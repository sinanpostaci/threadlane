use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::path::Path;

use threadlane::{WorkerAcceptanceState, WorkerSnapshot, WorkerStatus};
use threadlane_authority::{ApprovalRecord, ApprovedTaskRecord};
use threadlane_operations::{
    inspect_review_subject_readonly as inspect_review_subject, load_integration_receipt,
    load_review_receipt, verify_review_receipt_readonly as verify_review_receipt,
    InspectReviewSubjectRequest, IntegrationReceipt, IntegrationStrategy, ReviewPolicyBinding,
    ReviewReceipt, ReviewSubject, MAX_RECEIPT_BYTES, RECEIPTS_SUBDIR,
};
use threadlane_workers::{
    ensure_worktree_job_inactive_readonly, evaluate_diff_gate, is_commit_ancestor,
    is_worktree_clean, load_and_verify_manifest, resolve_exact_commit_sha, resolve_exact_tree_sha,
    run_sanitized_git, WorkerRunStatus, WorktreeKind, CHECKOUTS_SUBDIR,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS, MANIFESTS_SUBDIR,
};

use crate::client::compact_worktree_tag;
use crate::storage::{ensure_no_symlink_components, verify_existing_task_receipts};
use crate::types::{
    TaskEvidenceVerificationStatus, TaskExecutionStatus, TaskIntegrationLifecycleStatus,
    TaskLifecycleProjection, TaskReviewLifecycleStatus, TaskRunReceipt,
    VerifiedIntegrationReceiptSummary, VerifiedReviewReceiptSummary, MAX_LIFECYCLE_DETAIL_BYTES,
    MAX_RECEIPT_SCAN_ENTRIES, RUNTIME_STATE_SUBDIR, WORK_AREA_SUBDIR,
};

/// Load an immutable private Codex task report and verify its launch, snapshot and scope pins.
/// This is trusted-local evidence, never a requested-to-observed telemetry substitution.
pub fn load_verified_codex_task_report(
    record: &ApprovalRecord,
    session_dir: &Path,
    task: &ApprovedTaskRecord,
    worker: &WorkerSnapshot,
) -> Result<threadlane_workers::TaskExecutionReport, String> {
    use threadlane_workers::{TaskExecutionReport, WorkAcceptanceState};
    if !task.is_frozen_headless_worker()
        || task.provider != Some(threadlane_workers::WorkerProvider::CodexCli)
    {
        return Err("task has no frozen Codex worker authority".to_string());
    }
    let artifact_id = format!("art_{}", worker.worker_id);
    if worker
        .outcome
        .as_ref()
        .and_then(|o| o.report_artifact_id.as_deref())
        != Some(artifact_id.as_str())
        || crate::storage::validate_identifier("artifact_id", &artifact_id).is_err()
    {
        return Err("worker has no matching immutable report artifact".to_string());
    }
    let artifact_dir = session_dir
        .join(RUNTIME_STATE_SUBDIR)
        .join("worker_reports");
    crate::storage::verify_existing_private_dir(&artifact_dir).map_err(|e| e.to_string())?;
    let path = artifact_dir.join(format!("{artifact_id}.json"));
    ensure_no_symlink_components(&path).map_err(|e| e.to_string())?;
    let meta = fs::symlink_metadata(&path).map_err(|e| e.to_string())?;
    if !meta.is_file() || meta.len() > 16 * 1024 * 1024 {
        return Err("invalid bounded worker report file".to_string());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let parent_meta = fs::metadata(&artifact_dir).map_err(|e| e.to_string())?;
        if meta.permissions().mode() & 0o777 != 0o600 || meta.uid() != parent_meta.uid() {
            return Err("worker report must be private and owned by its store owner".to_string());
        }
    }
    let mut bytes = Vec::new();
    fs::File::open(&path)
        .map_err(|e| e.to_string())?
        .take(16 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() > 16 * 1024 * 1024 {
        return Err("worker report exceeds byte limit".to_string());
    }
    let report: TaskExecutionReport = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    let profile = crate::storage::build_validated_task_profile(
        record,
        &worker.binding_id,
        &session_dir.join(WORK_AREA_SUBDIR),
        task,
    )?;
    let manifest = load_and_verify_manifest(
        &session_dir.join(WORK_AREA_SUBDIR),
        &report.manifest.worktree_id,
    )
    .map_err(|e| e.to_string())?;
    if manifest.kind != WorktreeKind::Task
        || manifest.repository_path != record.repository_path
        || manifest.work_area_root != session_dir.join(WORK_AREA_SUBDIR)
        || manifest.run_id != crate::client::compact_worktree_tag(&worker.run_id, "r")
        || manifest.task_id.as_deref()
            != Some(crate::client::compact_worktree_tag(&worker.worker_id, "w").as_str())
        || manifest.worktree_id != worker.worktree.task_worktree_id.as_deref().unwrap_or("")
        || manifest.checkout_path.to_str() != worker.worktree.task_worktree_path.as_deref()
        || report.manifest.worktree_id != manifest.worktree_id
        || report.manifest.repository_path != manifest.repository_path
        || report.manifest.checkout_path != manifest.checkout_path
        || report.manifest.work_area_root != manifest.work_area_root
        || report.manifest.run_id != manifest.run_id
        || report.manifest.base_commit != manifest.base_commit
        || report.manifest.allowed_write_paths != manifest.allowed_write_paths
        || manifest.base_commit != worker.worktree.base_commit
        || manifest.allowed_write_paths != task.allowed_write_paths.iter().cloned().collect()
        || worker.profile_id != profile.profile_id
        || worker.profile_digest != profile.profile_digest
        || worker.requested_model != task.base_model
        || Some(worker.requested_effort.as_str()) != task.effort.as_deref()
        || worker.instruction != task.instruction
        || worker.provider != "codex_cli"
        || worker.status != WorkerStatus::Completed
        || worker.observed_model.as_deref() != report.worker_report.effective_observed_model()
        || worker.observed_effort.as_deref() != report.worker_report.effective_observed_effort()
        || !task.matches_codex_task_launch_report(
            &report.worker_report,
            &manifest.checkout_path,
            &manifest.base_commit,
        )
        || !report.diff_gate.passed
        || !report.diff_gate.evidence_complete
        || !report.diff_gate.violations.is_empty()
        || report.overall_acceptance_state != WorkAcceptanceState::DiffGatePassedPendingReview
    {
        return Err(
            "Codex worker report does not match the frozen profile, snapshot or worktree"
                .to_string(),
        );
    }
    Ok(report)
}

pub(crate) fn verify_worker_model_evidence(
    record: &ApprovalRecord,
    session_dir: &Path,
    task: &ApprovedTaskRecord,
    worker: &WorkerSnapshot,
) -> bool {
    if task.provider == Some(threadlane_workers::WorkerProvider::CodexCli) {
        load_verified_codex_task_report(record, session_dir, task, worker).is_ok()
    } else {
        worker.observed_model.as_deref() == Some(task.base_model.as_str())
            && task
                .effort
                .as_deref()
                .is_none_or(|effort| worker.observed_effort.as_deref() == Some(effort))
    }
}

/// Projects the truthful, read-only review and integration lifecycle for a task snapshot.
///
/// - Preserves the legacy execution invariant (`TaskStateSnapshot::accepted == false` and
///   `TaskExecutionStatus::ReadyForReview` for completed workers).
/// - Joins software-owned approval, task, run, worker, worktree, subject, and receipt identities.
/// - Performs no mutations, file creation, provider invocations, or host startup.
/// - Never trusts worker summary prose (`WorkerOutcomeSnapshot::final_summary`).
pub fn project_task_lifecycle(
    record: &ApprovalRecord,
    session_dir: &Path,
    task: &ApprovedTaskRecord,
    status: TaskExecutionStatus,
    run_receipt: Option<&TaskRunReceipt>,
    worker: Option<&WorkerSnapshot>,
) -> TaskLifecycleProjection {
    if status != TaskExecutionStatus::ReadyForReview {
        return TaskLifecycleProjection {
            review_status: TaskReviewLifecycleStatus::NotReady,
            integration_status: TaskIntegrationLifecycleStatus::NotIntegrated,
            evidence_status: TaskEvidenceVerificationStatus::NotApplicable,
            task_checkout_present: false,
            integration_checkout_present: false,
            review_receipt: None,
            integration_receipt: None,
            verification_detail: None,
        };
    }

    let (Some(run_receipt), Some(worker)) = (run_receipt, worker) else {
        return invalidated_projection(
            false,
            false,
            None,
            "ReadyForReview task is missing a sealed run receipt or runtime worker snapshot.",
        );
    };

    let Some(provider) = task.provider else {
        return invalidated_projection(
            false,
            false,
            None,
            "ReadyForReview task is missing a headless worker provider.",
        );
    };

    if run_receipt.task_id != task.task_id
        || run_receipt.task_digest != task.task_digest
        || Some(&run_receipt.profile_id) != task.profile_id.as_ref()
        || Some(&run_receipt.command_key) != task.command_key.as_ref()
        || worker.run_id != run_receipt.run_id
        || worker.worker_id != run_receipt.worker_id
        || worker.profile_id != run_receipt.profile_id
        || worker.provider != provider.as_str()
        || worker.requested_model != task.base_model
        || !verify_worker_model_evidence(record, session_dir, task, worker)
        || worker.instruction != task.instruction
        || worker.status != WorkerStatus::Completed
    {
        return invalidated_projection(
            false,
            false,
            None,
            "Worker snapshot or sealed run receipt does not match the frozen task contract.",
        );
    }

    if let Some(ref expected_effort) = task.effort {
        if worker.requested_effort != *expected_effort
            || (task.provider != Some(threadlane_workers::WorkerProvider::CodexCli)
                && worker.observed_effort.as_deref() != Some(expected_effort.as_str()))
        {
            return invalidated_projection(
                false,
                false,
                None,
                "Worker snapshot effort telemetry does not match the frozen task effort.",
            );
        }
    }

    let Some(ref outcome) = worker.outcome else {
        return invalidated_projection(
            false,
            false,
            None,
            "Completed worker snapshot is missing outcome evidence.",
        );
    };

    if outcome.acceptance_state != WorkerAcceptanceState::ReadyForReview
        || outcome.missing_model_evidence
        || outcome.missing_effort_evidence
        || outcome.provider_error
        || outcome.incomplete_cleanup
        || outcome.rejected_by_diff_gate
        || outcome.timed_out
        || outcome.cancelled
        || outcome.exit_code != Some(0)
    {
        return invalidated_projection(
            false,
            false,
            None,
            "Worker outcome evidence does not satisfy ReadyForReview verification gates.",
        );
    }

    let expected_base_commit = if task.depends_on.is_empty() {
        if run_receipt.execution_binding.is_some() || worker.execution_binding.is_some() {
            return invalidated_projection(
                false,
                false,
                None,
                "Root worker task unexpectedly carries a dependent execution binding.",
            );
        }
        record.base_commit.clone()
    } else {
        let (Some(ref receipt_binding), Some(ref worker_binding)) =
            (&run_receipt.execution_binding, &worker.execution_binding)
        else {
            return invalidated_projection(
                false,
                false,
                None,
                "Dependent worker task is missing its sealed execution binding.",
            );
        };
        if receipt_binding != worker_binding {
            return invalidated_projection(
                false,
                false,
                None,
                "Dependent worker snapshot execution binding does not match sealed run receipt.",
            );
        }
        receipt_binding.execution_base_commit.clone()
    };

    let (Some(ref task_worktree_id), Some(ref run_worktree_id)) = (
        &worker.worktree.task_worktree_id,
        &worker.worktree.run_worktree_id,
    ) else {
        return invalidated_projection(
            false,
            false,
            None,
            "Worker snapshot is missing task_worktree_id or run_worktree_id.",
        );
    };

    if worker.worktree.base_commit != expected_base_commit {
        return invalidated_projection(
            false,
            false,
            None,
            "Worker worktree base_commit does not match expected task execution base commit.",
        );
    }

    let Ok(canonical_repo) = validate_canonical_dir_readonly(&record.repository_path) else {
        return invalidated_projection(
            false,
            false,
            None,
            "Repository path failed canonical directory or symlink verification.",
        );
    };

    let work_area_dir = session_dir.join(WORK_AREA_SUBDIR);
    let Ok(canonical_work_area) = validate_private_dir_readonly(&work_area_dir) else {
        return invalidated_projection(
            false,
            false,
            None,
            "Session work-area directory failed private directory or symlink verification.",
        );
    };

    let task_checkout_path = canonical_work_area
        .join(CHECKOUTS_SUBDIR)
        .join("tasks")
        .join(task_worktree_id);
    let integration_checkout_path = canonical_work_area
        .join(CHECKOUTS_SUBDIR)
        .join("runs")
        .join(run_worktree_id);

    let task_checkout_present = match inspect_optional_dir_readonly(&task_checkout_path, false) {
        Ok(present) => present,
        Err(reason) => {
            return invalidated_projection(false, false, None, reason);
        }
    };
    let integration_checkout_present =
        match inspect_optional_dir_readonly(&integration_checkout_path, false) {
            Ok(present) => present,
            Err(reason) => {
                return invalidated_projection(task_checkout_present, false, None, reason);
            }
        };

    // Verify primary repository HEAD remains at the frozen approval base commit.
    match resolve_exact_commit_sha(&canonical_repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS) {
        Ok(current_repo_head) if current_repo_head == record.base_commit => {}
        Ok(_) => {
            return invalidated_projection(
                task_checkout_present,
                integration_checkout_present,
                None,
                "Primary repository HEAD moved away from the frozen approval base commit.",
            );
        }
        Err(_) => {
            return invalidated_projection(
                task_checkout_present,
                integration_checkout_present,
                None,
                "Failed to resolve primary repository HEAD commit.",
            );
        }
    }

    let expected_run_tag = compact_worktree_tag(&run_receipt.run_id, "r");
    let expected_task_tag = compact_worktree_tag(&run_receipt.worker_id, "w");
    let expected_allowed_write_paths: BTreeSet<String> =
        task.allowed_write_paths.iter().cloned().collect();

    let session_receipts =
        match verify_existing_task_receipts(session_dir, record, &worker.binding_id) {
            Ok(Some(ledger)) => ledger.receipts,
            Ok(None) => {
                return invalidated_projection(
                    task_checkout_present,
                    integration_checkout_present,
                    None,
                    "Session task receipts ledger is missing for a ReadyForReview task.",
                );
            }
            Err(_) => {
                return invalidated_projection(
                    task_checkout_present,
                    integration_checkout_present,
                    None,
                    "Session task receipts ledger failed integrity verification.",
                );
            }
        };

    let mut known_session_pairs = BTreeSet::new();
    known_session_pairs.insert((expected_run_tag.clone(), expected_task_tag.clone()));
    for sr in &session_receipts {
        known_session_pairs.insert((
            compact_worktree_tag(&sr.run_id, "r"),
            compact_worktree_tag(&sr.worker_id, "w"),
        ));
    }

    let scanned = match scan_session_receipts_bounded(
        &canonical_work_area,
        task_worktree_id,
        run_worktree_id,
        &expected_run_tag,
        &expected_task_tag,
        &known_session_pairs,
    ) {
        Ok(scanned) => scanned,
        Err(reason) => {
            return invalidated_projection(
                task_checkout_present,
                integration_checkout_present,
                None,
                reason,
            );
        }
    };

    // Case 1: No review receipt exists for this task yet.
    if scanned.review_receipts.is_empty() {
        if !scanned.integration_receipts.is_empty() {
            return invalidated_projection(
                task_checkout_present,
                integration_checkout_present,
                None,
                "Integration receipt exists for this task without a matching review receipt.",
            );
        }
        if !task_checkout_present {
            return invalidated_projection(
                false,
                integration_checkout_present,
                None,
                "Task worktree checkout is missing before review acceptance.",
            );
        }

        if let Err(reason) = verify_pending_review_live_worktree(
            &canonical_work_area,
            &canonical_repo,
            task_worktree_id,
            &expected_run_tag,
            &expected_task_tag,
            &expected_base_commit,
            &expected_allowed_write_paths,
            worker,
        ) {
            return invalidated_projection(true, integration_checkout_present, None, reason);
        }

        return TaskLifecycleProjection {
            review_status: TaskReviewLifecycleStatus::PendingReview,
            integration_status: TaskIntegrationLifecycleStatus::NotIntegrated,
            evidence_status: TaskEvidenceVerificationStatus::PendingReview,
            task_checkout_present: true,
            integration_checkout_present,
            review_receipt: None,
            integration_receipt: None,
            verification_detail: None,
        };
    }

    // Validate static task/scope/policy bindings on every review receipt targeting this task,
    // and select the latest non-conflicting review receipt.
    for receipt in &scanned.review_receipts {
        if let Err(reason) = verify_review_receipt_task_scope(
            receipt,
            &canonical_repo,
            &canonical_work_area,
            task_worktree_id,
            &expected_run_tag,
            &expected_task_tag,
            &expected_base_commit,
            &expected_allowed_write_paths,
            provider.as_str(),
            &task.base_model,
            task.effort.as_deref(),
        ) {
            return invalidated_projection(
                task_checkout_present,
                integration_checkout_present,
                None,
                reason,
            );
        }
    }

    let review_receipt = match select_latest_review_receipt(&scanned.review_receipts) {
        Ok(receipt) => receipt,
        Err(reason) => {
            return invalidated_projection(
                task_checkout_present,
                integration_checkout_present,
                None,
                reason,
            );
        }
    };

    // Case 2: Live task checkout is present on disk -> reconstruct live ReviewSubject and verify.
    if task_checkout_present {
        if let Err(reason) = verify_live_task_manifest_ownership(
            &canonical_work_area,
            &canonical_repo,
            task_worktree_id,
            &expected_run_tag,
            &expected_task_tag,
            &expected_base_commit,
            &expected_allowed_write_paths,
            worker,
        ) {
            return invalidated_projection(true, integration_checkout_present, None, reason);
        }

        let live_subject = match inspect_review_subject(&InspectReviewSubjectRequest {
            work_area_root: canonical_work_area.clone(),
            task_worktree_id: task_worktree_id.clone(),
            policy: review_receipt.policy.clone(),
            test_evidence: review_receipt.test_evidence.clone(),
            worker_status: Some(WorkerRunStatus::Succeeded),
        }) {
            Ok(subject) => subject,
            Err(_) => {
                return invalidated_projection(
                    true,
                    integration_checkout_present,
                    None,
                    "Live task worktree failed review subject verification (dirty checkout, untracked files, or diff gate violation after review).",
                );
            }
        };

        if verify_review_receipt(&live_subject, review_receipt).is_err() {
            return invalidated_projection(
                true,
                integration_checkout_present,
                None,
                "Live task worktree state diverged from the accepted review receipt (HEAD commit, tree, or diff digest changed).",
            );
        }

        let review_summary = build_review_receipt_summary(review_receipt);

        if scanned.integration_receipts.is_empty() {
            return TaskLifecycleProjection {
                review_status: TaskReviewLifecycleStatus::Accepted,
                integration_status: TaskIntegrationLifecycleStatus::ReadyToIntegrate,
                evidence_status: TaskEvidenceVerificationStatus::VerifiedLive,
                task_checkout_present: true,
                integration_checkout_present,
                review_receipt: Some(review_summary),
                integration_receipt: None,
                verification_detail: None,
            };
        }

        let integration_receipt = match verify_and_select_integration_receipt(
            &scanned.integration_receipts,
            Some(&live_subject),
            review_receipt,
            &canonical_repo,
            &canonical_work_area,
            task_worktree_id,
            run_worktree_id,
            &expected_run_tag,
            &expected_task_tag,
            &expected_base_commit,
            &record.base_commit,
            integration_checkout_present,
            worker,
        ) {
            Ok(receipt) => receipt,
            Err(reason) => {
                return invalidated_projection(
                    true,
                    integration_checkout_present,
                    Some(review_summary),
                    reason,
                );
            }
        };

        let integration_summary = build_integration_receipt_summary(integration_receipt);

        if integration_checkout_present {
            return TaskLifecycleProjection {
                review_status: TaskReviewLifecycleStatus::Accepted,
                integration_status: TaskIntegrationLifecycleStatus::Integrated,
                evidence_status: TaskEvidenceVerificationStatus::VerifiedLive,
                task_checkout_present: true,
                integration_checkout_present: true,
                review_receipt: Some(review_summary),
                integration_receipt: Some(integration_summary),
                verification_detail: None,
            };
        }

        return TaskLifecycleProjection {
            review_status: TaskReviewLifecycleStatus::Accepted,
            integration_status: TaskIntegrationLifecycleStatus::HistoricalIntegrated,
            evidence_status: TaskEvidenceVerificationStatus::RecordedHistoricalOnly,
            task_checkout_present: true,
            integration_checkout_present: false,
            review_receipt: Some(review_summary),
            integration_receipt: Some(integration_summary),
            verification_detail: Some(sanitize_lifecycle_detail(
                "Run integration checkout is no longer present on disk; integration receipt and repository commit/tree ancestry are verified from recorded history.",
            )),
        };
    }

    // Case 3: Historical task checkout (checkout directory was cleaned up / is absent on disk).
    if let Err(reason) = verify_historical_task_manifest_if_present(
        &canonical_work_area,
        &canonical_repo,
        task_worktree_id,
        &expected_run_tag,
        &expected_task_tag,
        &expected_base_commit,
        &expected_allowed_write_paths,
        &review_receipt.task_head_commit,
    ) {
        return invalidated_projection(false, integration_checkout_present, None, reason);
    }

    if let Err(reason) = verify_historical_review_in_repo(
        review_receipt,
        &canonical_repo,
        &expected_base_commit,
        &expected_allowed_write_paths,
    ) {
        return invalidated_projection(false, integration_checkout_present, None, reason);
    }

    let review_summary = build_review_receipt_summary(review_receipt);

    if scanned.integration_receipts.is_empty() {
        return TaskLifecycleProjection {
            review_status: TaskReviewLifecycleStatus::HistoricalAccepted,
            integration_status: TaskIntegrationLifecycleStatus::NotIntegrated,
            evidence_status: TaskEvidenceVerificationStatus::RecordedHistoricalOnly,
            task_checkout_present: false,
            integration_checkout_present,
            review_receipt: Some(review_summary),
            integration_receipt: None,
            verification_detail: Some(sanitize_lifecycle_detail(
                "Task checkout is no longer present on disk; review receipt and Git commit/tree history are verified, but live checkout inspection and branch integration are unavailable.",
            )),
        };
    }

    let integration_receipt = match verify_and_select_integration_receipt(
        &scanned.integration_receipts,
        None,
        review_receipt,
        &canonical_repo,
        &canonical_work_area,
        task_worktree_id,
        run_worktree_id,
        &expected_run_tag,
        &expected_task_tag,
        &expected_base_commit,
        &record.base_commit,
        integration_checkout_present,
        worker,
    ) {
        Ok(receipt) => receipt,
        Err(reason) => {
            return TaskLifecycleProjection {
                review_status: TaskReviewLifecycleStatus::HistoricalAccepted,
                integration_status: TaskIntegrationLifecycleStatus::Invalidated,
                evidence_status: TaskEvidenceVerificationStatus::Invalidated,
                task_checkout_present: false,
                integration_checkout_present,
                review_receipt: Some(review_summary),
                integration_receipt: None,
                verification_detail: Some(sanitize_lifecycle_detail(reason)),
            };
        }
    };

    let integration_summary = build_integration_receipt_summary(integration_receipt);

    TaskLifecycleProjection {
        review_status: TaskReviewLifecycleStatus::HistoricalAccepted,
        integration_status: TaskIntegrationLifecycleStatus::HistoricalIntegrated,
        evidence_status: TaskEvidenceVerificationStatus::RecordedHistoricalOnly,
        task_checkout_present: false,
        integration_checkout_present,
        review_receipt: Some(review_summary),
        integration_receipt: Some(integration_summary),
        verification_detail: Some(sanitize_lifecycle_detail(
            "Task checkout cleaned up; verified from recorded receipts and repository commit/tree history (live task checkout inspection unavailable).",
        )),
    }
}

struct ScannedTaskReceipts {
    review_receipts: Vec<ReviewReceipt>,
    integration_receipts: Vec<IntegrationReceipt>,
}

fn scan_session_receipts_bounded(
    canonical_work_area: &Path,
    task_worktree_id: &str,
    run_worktree_id: &str,
    expected_run_tag: &str,
    expected_task_tag: &str,
    known_session_pairs: &BTreeSet<(String, String)>,
) -> Result<ScannedTaskReceipts, &'static str> {
    let manifests_dir = canonical_work_area.join(MANIFESTS_SUBDIR);
    if !inspect_optional_private_dir_readonly(&manifests_dir)? {
        return Ok(ScannedTaskReceipts {
            review_receipts: Vec::new(),
            integration_receipts: Vec::new(),
        });
    }

    let receipts_dir = manifests_dir.join(RECEIPTS_SUBDIR);
    if !inspect_optional_private_dir_readonly(&receipts_dir)? {
        return Ok(ScannedTaskReceipts {
            review_receipts: Vec::new(),
            integration_receipts: Vec::new(),
        });
    }

    let read_dir =
        fs::read_dir(&receipts_dir).map_err(|_| "Failed to read session receipts directory.")?;

    let mut entry_names = BTreeMap::new();
    for (scanned_count, entry_res) in read_dir.enumerate() {
        if scanned_count >= MAX_RECEIPT_SCAN_ENTRIES {
            return Err("Session receipts directory exceeded the 256-entry scan budget.");
        }
        let entry = entry_res.map_err(|_| "Failed to inspect entry in receipts directory.")?;
        let file_type = entry
            .file_type()
            .map_err(|_| "Failed to inspect receipt entry file type.")?;
        if file_type.is_symlink() || !file_type.is_file() {
            return Err("Session receipts directory contains a symlink or non-regular file.");
        }
        let meta = fs::symlink_metadata(entry.path())
            .map_err(|_| "Failed to stat entry in session receipts directory.")?;
        if meta.file_type().is_symlink() || !meta.file_type().is_file() {
            return Err("Session receipts directory contains a symlink or non-regular file.");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if (meta.permissions().mode() & 0o777) != 0o600 {
                return Err("Receipt file in session work area does not have 0600 permissions.");
            }
        }
        if meta.len() == 0 || meta.len() > MAX_RECEIPT_BYTES {
            return Err("Receipt file in session work area is empty or exceeds the 64 KiB limit.");
        }

        let os_name = entry.file_name();
        let Some(name) = os_name.to_str() else {
            return Err("Session receipts directory contains a non-UTF-8 filename.");
        };
        entry_names.insert(name.to_string(), entry.path());
    }

    let mut review_receipts = Vec::new();
    let mut integration_receipts = Vec::new();

    for (name, _path) in entry_names {
        if is_transient_receipt_temp_file(&name) {
            continue;
        }
        if is_other_operation_receipt_file(&name) {
            continue;
        }
        if let Some(receipt_id) = parse_review_receipt_id(&name) {
            let receipt = load_review_receipt(canonical_work_area, receipt_id)
                .map_err(|_| "Malformed or tampered review receipt in session work area.")?;
            if !known_session_pairs.contains(&(receipt.run_id.clone(), receipt.task_id.clone())) {
                return Err(
                    "Foreign review receipt in session work area does not match any sealed task run.",
                );
            }
            let matches_worktree = receipt.task_worktree_id == task_worktree_id;
            let matches_tags =
                receipt.run_id == expected_run_tag && receipt.task_id == expected_task_tag;
            if matches_worktree != matches_tags {
                return Err(
                    "Review receipt worktree_id and run/task identity tags do not match for this task.",
                );
            }
            if matches_worktree {
                review_receipts.push(receipt);
            }
            continue;
        }
        if let Some(receipt_id) = parse_integration_receipt_id(&name) {
            let receipt = load_integration_receipt(canonical_work_area, receipt_id)
                .map_err(|_| "Malformed or tampered integration receipt in session work area.")?;
            if !known_session_pairs.contains(&(receipt.run_id.clone(), receipt.task_id.clone())) {
                return Err(
                    "Foreign integration receipt in session work area does not match any sealed task run.",
                );
            }
            let matches_task_wt = receipt.task_worktree_id == task_worktree_id;
            let matches_run_wt = receipt.integration_worktree_id == run_worktree_id;
            let matches_tags =
                receipt.run_id == expected_run_tag && receipt.task_id == expected_task_tag;
            if matches_task_wt != matches_tags || matches_run_wt != matches_tags {
                return Err(
                    "Integration receipt worktree IDs and run/task identity tags do not match for this task.",
                );
            }
            if matches_task_wt {
                integration_receipts.push(receipt);
            }
            continue;
        }

        return Err("Unexpected foreign file in session receipts directory.");
    }

    Ok(ScannedTaskReceipts {
        review_receipts,
        integration_receipts,
    })
}

fn parse_review_receipt_id(name: &str) -> Option<&str> {
    name.strip_suffix(".json")?
        .strip_prefix("review-")
        .filter(|id| !id.is_empty())
}

fn parse_integration_receipt_id(name: &str) -> Option<&str> {
    name.strip_suffix(".json")?
        .strip_prefix("integration-")
        .filter(|id| !id.is_empty())
}

fn is_transient_receipt_temp_file(name: &str) -> bool {
    (name.starts_with(".review-")
        || name.starts_with(".integration-")
        || name.starts_with(".push-")
        || name.starts_with(".remote-delete-"))
        && name.contains(".json.tmp.")
}

fn is_other_operation_receipt_file(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".json") else {
        return false;
    };
    (stem.starts_with("push-") && stem.len() > "push-".len())
        || (stem.starts_with("remote-delete-") && stem.len() > "remote-delete-".len())
}

#[allow(clippy::too_many_arguments)]
fn verify_pending_review_live_worktree(
    canonical_work_area: &Path,
    canonical_repo: &Path,
    task_worktree_id: &str,
    expected_run_tag: &str,
    expected_task_tag: &str,
    expected_base_commit: &str,
    expected_allowed_write_paths: &BTreeSet<String>,
    worker: &WorkerSnapshot,
) -> Result<(), &'static str> {
    verify_live_task_manifest_ownership(
        canonical_work_area,
        canonical_repo,
        task_worktree_id,
        expected_run_tag,
        expected_task_tag,
        expected_base_commit,
        expected_allowed_write_paths,
        worker,
    )?;

    let report = evaluate_diff_gate(canonical_work_area, task_worktree_id)
        .map_err(|_| "Live task worktree failed post-run diff gate inspection.")?;
    if !report.passed || !report.evidence_complete {
        return Err(
            "Live task worktree contains out-of-scope modifications or diff gate violations.",
        );
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn verify_live_task_manifest_ownership(
    canonical_work_area: &Path,
    canonical_repo: &Path,
    task_worktree_id: &str,
    expected_run_tag: &str,
    expected_task_tag: &str,
    expected_base_commit: &str,
    expected_allowed_write_paths: &BTreeSet<String>,
    worker: &WorkerSnapshot,
) -> Result<(), &'static str> {
    let manifest = load_and_verify_manifest(canonical_work_area, task_worktree_id)
        .map_err(|_| "Task worktree ownership manifest is missing or invalid.")?;

    if manifest.kind != WorktreeKind::Task
        || manifest.worktree_id != task_worktree_id
        || manifest.run_id != expected_run_tag
        || manifest.task_id.as_deref() != Some(expected_task_tag)
        || manifest.repository_path != canonical_repo
        || manifest.work_area_root != canonical_work_area
        || manifest.base_commit != expected_base_commit
        || &manifest.allowed_write_paths != expected_allowed_write_paths
        || manifest.job_active
        || manifest.partial_cleanup_accepted_head.is_some()
    {
        return Err("Task worktree manifest does not match frozen task identity or write scope.");
    }

    if worker.worktree.task_worktree_path.as_deref()
        != Some(manifest.checkout_path.to_string_lossy().as_ref())
        || worker.worktree.task_branch_name.as_deref() != Some(manifest.branch_name.as_str())
    {
        return Err("Task worktree manifest path or branch does not match worker snapshot.");
    }

    if is_worktree_job_lock_held_readonly(canonical_work_area, task_worktree_id)? {
        return Err("Task worktree has an active job lock.");
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn verify_historical_task_manifest_if_present(
    canonical_work_area: &Path,
    canonical_repo: &Path,
    task_worktree_id: &str,
    expected_run_tag: &str,
    expected_task_tag: &str,
    expected_base_commit: &str,
    expected_allowed_write_paths: &BTreeSet<String>,
    accepted_head_commit: &str,
) -> Result<(), &'static str> {
    if is_worktree_job_lock_held_readonly(canonical_work_area, task_worktree_id)? {
        return Err("Historical task worktree still holds an active job lock.");
    }

    let manifest_path = canonical_work_area
        .join(MANIFESTS_SUBDIR)
        .join(format!("{task_worktree_id}.json"));
    match fs::symlink_metadata(&manifest_path) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err("Failed to inspect historical task worktree manifest metadata."),
        Ok(_) => {
            let manifest = load_and_verify_manifest(canonical_work_area, task_worktree_id)
                .map_err(|_| "Historical task worktree manifest failed integrity verification.")?;
            if manifest.kind != WorktreeKind::Task
                || manifest.worktree_id != task_worktree_id
                || manifest.run_id != expected_run_tag
                || manifest.task_id.as_deref() != Some(expected_task_tag)
                || manifest.repository_path != canonical_repo
                || manifest.work_area_root != canonical_work_area
                || manifest.base_commit != expected_base_commit
                || &manifest.allowed_write_paths != expected_allowed_write_paths
                || manifest.job_active
            {
                return Err(
                    "Historical task worktree manifest does not match frozen task identity.",
                );
            }
            if let Some(ref partial_head) = manifest.partial_cleanup_accepted_head {
                if partial_head != accepted_head_commit {
                    return Err(
                        "Historical task worktree partial-cleanup head does not match review receipt.",
                    );
                }
                let branch_ref = format!("refs/heads/{}", manifest.branch_name);
                let resolved_branch = resolve_exact_commit_sha(
                    canonical_repo,
                    &branch_ref,
                    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                )
                .map_err(|_| {
                    "Preserved task branch ref is missing in repository after partial cleanup."
                })?;
                if resolved_branch != accepted_head_commit {
                    return Err(
                        "Preserved task branch ref moved away from accepted review commit.",
                    );
                }
            }
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn verify_review_receipt_task_scope(
    receipt: &ReviewReceipt,
    canonical_repo: &Path,
    canonical_work_area: &Path,
    task_worktree_id: &str,
    expected_run_tag: &str,
    expected_task_tag: &str,
    expected_base_commit: &str,
    expected_allowed_write_paths: &BTreeSet<String>,
    expected_provider: &str,
    expected_model: &str,
    expected_effort: Option<&str>,
) -> Result<(), &'static str> {
    if receipt.task_worktree_id != task_worktree_id
        || receipt.run_id != expected_run_tag
        || receipt.task_id != expected_task_tag
        || receipt.repository_path != canonical_repo
        || receipt.work_area_root != canonical_work_area
    {
        return Err("Review receipt worktree, run, task, or repository identity mismatch.");
    }

    if receipt.base_commit != expected_base_commit {
        return Err("Review receipt base_commit does not match expected task base commit.");
    }

    if &receipt.allowed_write_paths != expected_allowed_write_paths {
        return Err("Review receipt allowed_write_paths do not match frozen task write scope.");
    }

    if receipt.task_head_commit == receipt.base_commit {
        return Err("Review receipt task_head_commit must advance beyond base_commit.");
    }

    verify_policy_matches_frozen_task(
        &receipt.policy,
        expected_provider,
        expected_model,
        expected_effort,
    )?;

    if !receipt.test_evidence.all_checks_passed() {
        return Err("Review receipt test evidence does not have all required checks passing.");
    }

    Ok(())
}

fn verify_policy_matches_frozen_task(
    policy: &ReviewPolicyBinding,
    expected_provider: &str,
    expected_model: &str,
    expected_effort: Option<&str>,
) -> Result<(), &'static str> {
    if policy.validate(true).is_err() {
        return Err("Review receipt policy binding failed validation.");
    }
    if policy.requested_provider.as_deref() != Some(expected_provider) {
        return Err("Review receipt requested_provider does not match frozen task provider.");
    }
    if policy.requested_model.as_deref() != Some(expected_model)
        || (policy.observed_model.as_deref() != Some(expected_model)
            && !policy.has_verified_codex_task_launch())
    {
        return Err("Review receipt model evidence does not match frozen task model.");
    }
    if let Some(req_effort) = expected_effort {
        if policy.requested_effort.as_deref() != Some(req_effort)
            || !policy.require_effort_evidence
            || (policy.observed_effort.as_deref() != Some(req_effort)
                && !policy.has_verified_codex_task_launch())
        {
            return Err("Review receipt effort evidence does not match frozen task effort.");
        }
    }
    if !policy.require_passing_tests {
        return Err("Review receipt policy must require passing test evidence.");
    }
    Ok(())
}

fn select_latest_review_receipt(
    receipts: &[ReviewReceipt],
) -> Result<&ReviewReceipt, &'static str> {
    let Some(first) = receipts.first() else {
        return Err("No review receipt found for task.");
    };
    let mut latest = first;
    for candidate in &receipts[1..] {
        if candidate.approved_at_ms > latest.approved_at_ms {
            latest = candidate;
        }
    }
    for candidate in receipts {
        if candidate.receipt_id != latest.receipt_id
            && candidate.approved_at_ms >= latest.approved_at_ms
        {
            return Err("Conflicting review receipts exist with ambiguous approval timestamps.");
        }
    }
    Ok(latest)
}

fn verify_historical_review_in_repo(
    review_receipt: &ReviewReceipt,
    canonical_repo: &Path,
    expected_base_commit: &str,
    expected_allowed_write_paths: &BTreeSet<String>,
) -> Result<(), &'static str> {
    if review_receipt.verify_integrity().is_err() {
        return Err("Historical review receipt failed cryptographic integrity verification.");
    }

    let resolved_base = resolve_exact_commit_sha(
        canonical_repo,
        &review_receipt.base_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|_| "Historical review base_commit is missing from repository.")?;
    if resolved_base != expected_base_commit {
        return Err("Historical review base_commit does not match expected task base commit.");
    }

    let resolved_head = resolve_exact_commit_sha(
        canonical_repo,
        &review_receipt.task_head_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|_| "Accepted task_head_commit is missing from repository history.")?;
    if resolved_head != review_receipt.task_head_commit {
        return Err("Accepted task_head_commit resolution mismatch in repository.");
    }

    let resolved_tree = resolve_exact_tree_sha(
        canonical_repo,
        &review_receipt.task_head_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|_| "Failed to resolve tree SHA for accepted task_head_commit in repository.")?;
    if resolved_tree != review_receipt.task_tree_sha {
        return Err(
            "Accepted task_head_commit tree SHA in repository does not match review receipt.",
        );
    }

    let is_anc = is_commit_ancestor(
        canonical_repo,
        expected_base_commit,
        &review_receipt.task_head_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|_| "Failed to verify commit ancestry for historical review receipt.")?;
    if !is_anc {
        return Err("Historical review base_commit is not an ancestor of task_head_commit.");
    }

    let diff_names = run_sanitized_git(
        canonical_repo,
        &[
            "diff",
            "--name-only",
            "-z",
            expected_base_commit,
            &review_receipt.task_head_commit,
        ],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|_| "Failed to inspect changed paths for historical review commit.")?;

    let mut changed_count = 0usize;
    for path in diff_names.split('\0').filter(|p| !p.is_empty()) {
        changed_count += 1;
        if !is_path_within_allowed_scopes(path, expected_allowed_write_paths) {
            return Err("Historical review commit modifies paths outside allowed_write_paths.");
        }
    }
    if changed_count == 0 {
        return Err("Historical review commit contains no changed paths relative to base_commit.");
    }

    Ok(())
}

fn is_path_within_allowed_scopes(rel_path: &str, allowed_write_paths: &BTreeSet<String>) -> bool {
    let candidate = Path::new(rel_path);
    for allowed in allowed_write_paths {
        let trimmed = allowed.trim_end_matches('/');
        if allowed.ends_with('/') {
            if candidate.starts_with(Path::new(trimmed)) {
                return true;
            }
        } else if rel_path == trimmed {
            return true;
        }
    }
    false
}

#[allow(clippy::too_many_arguments)]
fn verify_and_select_integration_receipt<'a>(
    integration_receipts: &'a [IntegrationReceipt],
    live_subject: Option<&ReviewSubject>,
    review_receipt: &ReviewReceipt,
    canonical_repo: &Path,
    canonical_work_area: &Path,
    task_worktree_id: &str,
    run_worktree_id: &str,
    expected_run_tag: &str,
    expected_task_tag: &str,
    expected_base_commit: &str,
    approved_base_commit: &str,
    integration_checkout_present: bool,
    worker: &WorkerSnapshot,
) -> Result<&'a IntegrationReceipt, &'static str> {
    let Some(first) = integration_receipts.first() else {
        return Err("No integration receipt found for task.");
    };
    let expected_run_branch_prefix = format!("threadlane/run/{expected_run_tag}/");
    let expected_task_branch_prefix =
        format!("threadlane/task/{expected_run_tag}/{expected_task_tag}/");
    for candidate in integration_receipts {
        if candidate.task_worktree_id != task_worktree_id
            || candidate.integration_worktree_id != run_worktree_id
            || candidate.run_id != expected_run_tag
            || candidate.task_id != expected_task_tag
            || candidate.repository_path != canonical_repo
            || candidate.work_area_root != canonical_work_area
            || candidate.base_commit != expected_base_commit
            || candidate.source_head_commit != approved_base_commit
        {
            return Err(
                "Integration receipt worktree, run, task, or base commit identity mismatch.",
            );
        }
        if !candidate
            .integration_branch_name
            .starts_with(&expected_run_branch_prefix)
            || !candidate
                .task_branch_name
                .starts_with(&expected_task_branch_prefix)
        {
            return Err(
                "Integration receipt branch names do not match expected software branch prefixes.",
            );
        }
        if let Some(ref expected_task_branch) = worker.worktree.task_branch_name {
            if &candidate.task_branch_name != expected_task_branch {
                return Err("Integration receipt task_branch_name does not match worker snapshot.");
            }
        }
    }

    let mut latest = first;
    for candidate in &integration_receipts[1..] {
        if candidate.integrated_at_ms > latest.integrated_at_ms {
            latest = candidate;
        }
    }
    for candidate in integration_receipts {
        if candidate.receipt_id != latest.receipt_id
            && candidate.integrated_at_ms >= latest.integrated_at_ms
        {
            return Err(
                "Conflicting integration receipts exist with ambiguous integration timestamps.",
            );
        }
    }

    if let Some(subject) = live_subject {
        if latest
            .verify_against_review(subject, review_receipt)
            .is_err()
        {
            return Err(
                "Integration receipt does not match the verified review receipt and review subject.",
            );
        }
    } else {
        if review_receipt.verify_integrity().is_err() || latest.verify_integrity().is_err() {
            return Err(
                "Historical review or integration receipt failed cryptographic integrity verification.",
            );
        }
        if latest.run_id != review_receipt.run_id
            || latest.task_id != review_receipt.task_id
            || latest.task_worktree_id != review_receipt.task_worktree_id
            || latest.repository_path != review_receipt.repository_path
            || latest.work_area_root != review_receipt.work_area_root
            || latest.base_commit != review_receipt.base_commit
            || latest.accepted_task_head_commit != review_receipt.task_head_commit
            || latest.review_subject_digest != review_receipt.subject_digest
            || latest.review_receipt_id != review_receipt.receipt_id
            || latest.review_receipt_digest != review_receipt.receipt_digest
        {
            return Err(
                "Integration receipt does not match the verified historical review receipt.",
            );
        }
    }

    let resolved_accepted = resolve_exact_commit_sha(
        canonical_repo,
        &latest.accepted_task_head_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|_| "Integrated accepted_task_head_commit is missing from repository.")?;
    if resolved_accepted != latest.accepted_task_head_commit {
        return Err("Integrated accepted_task_head_commit resolution mismatch.");
    }

    let resolved_target = resolve_exact_commit_sha(
        canonical_repo,
        &latest.integrated_target_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|_| "Integrated target commit is missing from repository.")?;
    if resolved_target != latest.integrated_target_commit {
        return Err("Integrated target commit resolution mismatch.");
    }

    let resolved_target_tree = resolve_exact_tree_sha(
        canonical_repo,
        &latest.integrated_target_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|_| "Failed to resolve tree SHA for integrated_target_commit.")?;
    if resolved_target_tree != latest.integrated_tree_sha {
        return Err("Integrated target commit tree SHA does not match integration receipt.");
    }

    if latest.strategy == IntegrationStrategy::FastForwardOnly
        && (latest.integrated_target_commit != latest.accepted_task_head_commit
            || latest.integrated_tree_sha != review_receipt.task_tree_sha)
    {
        return Err(
            "Fast-forward integration receipt target commit or tree does not match accepted task head.",
        );
    }

    let base_is_anc = is_commit_ancestor(
        canonical_repo,
        expected_base_commit,
        &latest.accepted_task_head_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|_| "Failed to verify base commit ancestry for integration receipt.")?;
    if !base_is_anc {
        return Err("Task execution base commit is not an ancestor of accepted_task_head_commit.");
    }

    let task_is_anc = is_commit_ancestor(
        canonical_repo,
        &latest.accepted_task_head_commit,
        &latest.integrated_target_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|_| {
        "Failed to verify accepted_task_head_commit ancestry in integrated_target_commit."
    })?;
    if !task_is_anc {
        return Err("Accepted task commit is not an ancestor of integrated_target_commit.");
    }

    if integration_checkout_present {
        let int_manifest = load_and_verify_manifest(canonical_work_area, run_worktree_id)
            .map_err(|_| "Run integration worktree manifest is missing or invalid.")?;
        if int_manifest.kind != WorktreeKind::RunIntegration
            || int_manifest.worktree_id != run_worktree_id
            || int_manifest.run_id != expected_run_tag
            || int_manifest.task_id.is_some()
            || int_manifest.repository_path != canonical_repo
            || int_manifest.work_area_root != canonical_work_area
            || int_manifest.base_commit != expected_base_commit
            || int_manifest.branch_name != latest.integration_branch_name
            || int_manifest.checkout_path != latest.integration_checkout_path
            || int_manifest.job_active
            || int_manifest.partial_cleanup_accepted_head.is_some()
        {
            return Err(
                "Run integration worktree manifest does not match integration receipt or task identity.",
            );
        }

        if worker.worktree.run_worktree_path.as_deref()
            != Some(int_manifest.checkout_path.to_string_lossy().as_ref())
            || worker.worktree.run_branch_name.as_deref() != Some(int_manifest.branch_name.as_str())
        {
            return Err("Run integration worktree manifest does not match worker snapshot.");
        }

        if is_worktree_job_lock_held_readonly(canonical_work_area, run_worktree_id)? {
            return Err("Run integration worktree has an active job lock.");
        }

        let current_branch = run_sanitized_git(
            &int_manifest.checkout_path,
            &["symbolic-ref", "--quiet", "--short", "HEAD"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .map_err(|_| "Run integration worktree is detached or failed branch inspection.")?;
        if current_branch.trim() != int_manifest.branch_name {
            return Err("Run integration worktree branch does not match manifest branch_name.");
        }

        let clean = is_worktree_clean(&int_manifest.checkout_path, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .map_err(|_| "Failed to inspect run integration worktree cleanliness.")?;
        if !clean {
            return Err("Run integration worktree became dirty after integration.");
        }

        let live_int_head = resolve_exact_commit_sha(
            &int_manifest.checkout_path,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .map_err(|_| "Failed to resolve live HEAD commit in run integration worktree.")?;
        if live_int_head != latest.integrated_target_commit {
            return Err("Run integration worktree HEAD moved away from integrated_target_commit.");
        }

        let live_int_tree = resolve_exact_tree_sha(
            &int_manifest.checkout_path,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .map_err(|_| "Failed to resolve live tree SHA in run integration worktree.")?;
        if live_int_tree != latest.integrated_tree_sha {
            return Err("Run integration worktree tree SHA does not match integration receipt.");
        }
    }

    Ok(latest)
}

fn validate_canonical_dir_readonly(path: &Path) -> Result<std::path::PathBuf, &'static str> {
    if !path.is_absolute() {
        return Err("Path is not absolute.");
    }
    let checked =
        ensure_no_symlink_components(path).map_err(|_| "Path contains symlink components.")?;
    let meta = fs::symlink_metadata(&checked).map_err(|_| "Directory does not exist.")?;
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return Err("Path is not a real directory.");
    }
    fs::canonicalize(&checked).map_err(|_| "Failed to canonicalize directory.")
}

fn validate_private_dir_readonly(path: &Path) -> Result<std::path::PathBuf, &'static str> {
    let canonical = validate_canonical_dir_readonly(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta =
            fs::symlink_metadata(&canonical).map_err(|_| "Failed to stat private directory.")?;
        if (meta.permissions().mode() & 0o777) != 0o700 {
            return Err("Directory does not have private 0700 permissions.");
        }
    }
    Ok(canonical)
}

fn inspect_optional_private_dir_readonly(path: &Path) -> Result<bool, &'static str> {
    inspect_optional_dir_readonly(path, true)
}

// Git creates checkouts with the user's ordinary directory mode, commonly 0755.
// The already-validated 0700 work-area ancestor protects their contents. Private
// metadata remains 0700; checkouts must still reject group/world write access.
fn inspect_optional_dir_readonly(path: &Path, require_private: bool) -> Result<bool, &'static str> {
    if ensure_no_symlink_components(path).is_err() {
        return Err("Work area path contains a symlink component.");
    }
    match fs::symlink_metadata(path) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err("Failed to inspect directory metadata."),
        Ok(meta) => {
            if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
                return Err("Expected private directory is a symlink or non-directory.");
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = meta.permissions().mode() & 0o777;
                if require_private && mode != 0o700 {
                    return Err("Work area subdirectory does not have 0700 permissions.");
                }
                if !require_private && mode & 0o022 != 0 {
                    return Err("Task checkout grants group or world write permission.");
                }
            }
            Ok(true)
        }
    }
}

fn is_worktree_job_lock_held_readonly(
    canonical_work_area: &Path,
    worktree_id: &str,
) -> Result<bool, &'static str> {
    let manifest = load_and_verify_manifest(canonical_work_area, worktree_id)
        .map_err(|_| "Worktree manifest failed read-only lock verification.")?;
    match ensure_worktree_job_inactive_readonly(&manifest) {
        Ok(()) => Ok(false),
        Err(threadlane_workers::WorkerError::CleanupRefused {
            reason: threadlane_workers::CleanupRefusalReason::ActiveJob,
            ..
        }) => Ok(true),
        Err(_) => Err("Worktree job lock failed secure read-only verification."),
    }
}

fn build_review_receipt_summary(receipt: &ReviewReceipt) -> VerifiedReviewReceiptSummary {
    VerifiedReviewReceiptSummary {
        receipt_id: receipt.receipt_id.clone(),
        subject_digest: receipt.subject_digest.clone(),
        receipt_digest: receipt.receipt_digest.clone(),
        task_worktree_id: receipt.task_worktree_id.clone(),
        base_commit: receipt.base_commit.clone(),
        task_head_commit: receipt.task_head_commit.clone(),
        task_tree_sha: receipt.task_tree_sha.clone(),
        diff_digest: receipt.diff_digest.clone(),
        reviewer_id: receipt.reviewer_id.clone(),
        approved_at_ms: receipt.approved_at_ms,
    }
}

fn build_integration_receipt_summary(
    receipt: &IntegrationReceipt,
) -> VerifiedIntegrationReceiptSummary {
    VerifiedIntegrationReceiptSummary {
        receipt_id: receipt.receipt_id.clone(),
        receipt_digest: receipt.receipt_digest.clone(),
        review_receipt_id: receipt.review_receipt_id.clone(),
        integration_worktree_id: receipt.integration_worktree_id.clone(),
        strategy: receipt.strategy,
        accepted_task_head_commit: receipt.accepted_task_head_commit.clone(),
        integrated_target_commit: receipt.integrated_target_commit.clone(),
        integrated_tree_sha: receipt.integrated_tree_sha.clone(),
        operator_id: receipt.operator_id.clone(),
        integrated_at_ms: receipt.integrated_at_ms,
    }
}

fn invalidated_projection(
    task_checkout_present: bool,
    integration_checkout_present: bool,
    verified_review_receipt: Option<VerifiedReviewReceiptSummary>,
    detail: &str,
) -> TaskLifecycleProjection {
    let review_status = if verified_review_receipt.is_some() {
        if task_checkout_present {
            TaskReviewLifecycleStatus::Accepted
        } else {
            TaskReviewLifecycleStatus::HistoricalAccepted
        }
    } else {
        TaskReviewLifecycleStatus::Invalidated
    };

    let integration_status = if verified_review_receipt.is_some() {
        TaskIntegrationLifecycleStatus::Invalidated
    } else {
        TaskIntegrationLifecycleStatus::NotIntegrated
    };

    TaskLifecycleProjection {
        review_status,
        integration_status,
        evidence_status: TaskEvidenceVerificationStatus::Invalidated,
        task_checkout_present,
        integration_checkout_present,
        review_receipt: verified_review_receipt,
        integration_receipt: None,
        verification_detail: Some(sanitize_lifecycle_detail(detail)),
    }
}

fn sanitize_lifecycle_detail(raw: &str) -> String {
    let collapsed: String = raw
        .split_whitespace()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    if collapsed.len() <= MAX_LIFECYCLE_DETAIL_BYTES {
        return collapsed;
    }
    let mut end = MAX_LIFECYCLE_DETAIL_BYTES.saturating_sub(3);
    while end > 0 && !collapsed.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &collapsed[..end])
}
