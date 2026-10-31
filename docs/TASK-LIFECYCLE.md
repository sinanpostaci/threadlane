# Task Review and Integration Lifecycle Projection

This document specifies the read-only task lifecycle projection exposed by `threadlane-host` (`TaskStateSnapshot.lifecycle`) and rendered by the desktop session control panel (`apps/desktop/src/host/`).

## 1. Authority and Execution Invariant

Threadlane separates worker execution completion from operator review acceptance and branch integration:

1. **Legacy Execution Invariant Preserved**:
   - `TaskStateSnapshot.accepted` remains `false`.
   - A completed worker task whose post-run diff gate succeeded continues to report `TaskStateSnapshot.status = "ready_for_review"` (`TaskExecutionStatus::ReadyForReview`) even after trusted operator review and integration receipts exist.
   - Worker execution summaries (`WorkerSnapshot.outcome.final_summary`) or prose output are never trusted as review or integration evidence.
2. **Read-Only Projection**:
   - `ApprovedHostClient::get_task` (as well as `dispatch_task` and `cancel_task`) computes an optional additive `lifecycle: Option<TaskLifecycleProjection>` field on `TaskStateSnapshot`.
   - Projection reads are strictly side-effect-free: they never mutate session records, worktree manifests, lockfiles, or Git refs, never invoke model providers, and never start or adopt a host.
3. **Wire Compatibility**:
   - `TaskStateSnapshot.lifecycle` is annotated with `#[serde(default, skip_serializing_if = "Option::is_none")]`.
   - Optional nested fields (`review_receipt`, `integration_receipt`, `verification_detail`) are omitted when `None`. New readers accept legacy snapshots, and omission preserves their original shape. When the projection is present, use matching updated host and desktop versions; older strict readers may reject the additional field.

---

## 2. Host DTOs (`crates/threadlane-host`)

### `TaskLifecycleProjection`

```rust
pub const MAX_RECEIPT_SCAN_ENTRIES: usize = 256;
pub const MAX_LIFECYCLE_DETAIL_BYTES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskReviewLifecycleStatus {
    NotReady,
    PendingReview,
    Accepted,
    HistoricalAccepted,
    Invalidated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskIntegrationLifecycleStatus {
    NotIntegrated,
    ReadyToIntegrate,
    Integrated,
    HistoricalIntegrated,
    Invalidated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskEvidenceVerificationStatus {
    NotApplicable,
    PendingReview,
    VerifiedLive,
    RecordedHistoricalOnly,
    Invalidated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiedReviewReceiptSummary {
    pub receipt_id: String,
    pub subject_digest: String,
    pub receipt_digest: String,
    pub task_worktree_id: String,
    pub base_commit: String,
    pub task_head_commit: String,
    pub task_tree_sha: String,
    pub diff_digest: String,
    pub reviewer_id: String,
    pub approved_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiedIntegrationReceiptSummary {
    pub receipt_id: String,
    pub receipt_digest: String,
    pub review_receipt_id: String,
    pub integration_worktree_id: String,
    pub strategy: IntegrationStrategy,
    pub accepted_task_head_commit: String,
    pub integrated_target_commit: String,
    pub integrated_tree_sha: String,
    pub operator_id: String,
    pub integrated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskLifecycleProjection {
    pub review_status: TaskReviewLifecycleStatus,
    pub integration_status: TaskIntegrationLifecycleStatus,
    pub evidence_status: TaskEvidenceVerificationStatus,
    pub task_checkout_present: bool,
    pub integration_checkout_present: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_receipt: Option<VerifiedReviewReceiptSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integration_receipt: Option<VerifiedIntegrationReceiptSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_detail: Option<String>,
}
```

No raw filesystem paths, bearer tokens, environment values, or worker prose are included in `TaskLifecycleProjection`. `verification_detail` is single-line sanitized text truncated to `MAX_LIFECYCLE_DETAIL_BYTES` (`256` bytes).

---

## 3. Software-Owned Identity Join Rules

When `TaskStateSnapshot.status == TaskExecutionStatus::ReadyForReview`, the host joins the frozen approval record (`ApprovalRecord`), the frozen task contract (`ApprovedTaskRecord`), the sealed task run ledger (`TaskRunReceipt`), the verified worker snapshot (`WorkerSnapshot`), and operator receipts in `<work_area_dir>/manifests/receipts`:

1. **Worker and Run Binding Prerequisites**:
   - `TaskRunReceipt.approval_id == record.approval_id`, `TaskRunReceipt.task_id == task.task_id`, `TaskRunReceipt.task_digest == task.task_digest`.
   - `WorkerSnapshot.run_id == TaskRunReceipt.run_id`, `WorkerSnapshot.worker_id == TaskRunReceipt.worker_id`.
   - `WorkerSnapshot.status == WorkerStatus::Completed`, `outcome.run_status == WorkerRunStatus::Succeeded`, `outcome.acceptance_state == WorkAcceptanceState::DiffGatePassedPendingReview`, and `outcome.diff_gate_passed == Some(true)`.
   - `WorkerSnapshot.requested_model == task.base_model` and `WorkerSnapshot.observed_model == Some(task.base_model)`.
   - When `task.effort` is specified, `WorkerSnapshot.requested_effort` and `WorkerSnapshot.observed_effort` match `task.effort`.
   - Expected compact tags are derived deterministically via `compact_worktree_tag(&run_receipt.run_id, "r")` (`r_<12hex>`) and `compact_worktree_tag(&run_receipt.worker_id, "w")` (`w_<12hex>`).
   - Expected base commit is `record.base_commit` for root tasks (`task.depends_on.is_empty()`) or `run_receipt.execution_binding.execution_base_commit` for dependent tasks (which must match `worker.execution_binding` and `worker.worktree.base_commit`).

2. **Bounded Receipt Directory Scan**:
   - `<work_area_dir>/manifests` and `<work_area_dir>/manifests/receipts` must be real `0700` directories (never symlinks) when present.
   - Directory iteration is capped at `MAX_RECEIPT_SCAN_ENTRIES` (`256`). Exceeding the cap, encountering symlinks, non-regular files, non-`0600` permissions, files larger than `MAX_RECEIPT_BYTES` (`64 KiB`), malformed receipt JSON, or foreign receipt files whose `(run_id, task_id)` does not belong to any sealed `TaskRunReceipt` in the session immediately transitions `evidence_status` to `Invalidated`.
   - Review receipts (`review-<id>.json`) and integration receipts (`integration-<id>.json`) are loaded and integrity-checked through `threadlane_operations::load_review_receipt` and `threadlane_operations::load_integration_receipt`.

3. **Review Receipt Verification**:
   - `review_receipt.verify_integrity()` succeeds.
   - `review_receipt.task_worktree_id == worker.worktree.task_worktree_id`
   - `review_receipt.run_id == expected_run_tag` and `review_receipt.task_id == expected_task_tag`
   - `review_receipt.repository_path == canonical(record.repository_path)` and `review_receipt.work_area_root == canonical(work_area_dir)`
   - `review_receipt.base_commit == expected_base_commit`
   - `review_receipt.allowed_write_paths == BTreeSet::from_iter(task.allowed_write_paths)`
   - `review_receipt.task_head_commit != review_receipt.base_commit`
   - `review_receipt.policy` enforces `require_clean_worktree`, `require_diff_gate_passed`, `require_terminal_worker_success`, `require_passing_tests`, `requested_provider`, `requested_model == task.base_model`, `observed_model == task.base_model`, and `task.effort` (`require_effort_evidence`, `requested_effort`, `observed_effort`), and `review_receipt.test_evidence.all_checks_passed()` is `true`.

4. **Integration Receipt Verification**:
   - `integration_receipt.verify_integrity()` succeeds.
   - `integration_receipt.task_worktree_id == worker.worktree.task_worktree_id`
   - `integration_receipt.integration_worktree_id == worker.worktree.run_worktree_id`
   - `integration_receipt.run_id == expected_run_tag` and `integration_receipt.task_id == expected_task_tag`
   - `integration_receipt.repository_path == canonical(record.repository_path)` and `integration_receipt.work_area_root == canonical(work_area_dir)`
   - `integration_receipt.base_commit == expected_base_commit` and `integration_receipt.source_head_commit == record.base_commit`
   - `integration_receipt.review_receipt_id == review_receipt.receipt_id`
   - `integration_receipt.review_receipt_digest == review_receipt.receipt_digest`
   - `integration_receipt.review_subject_digest == review_receipt.subject_digest`
   - `integration_receipt.accepted_task_head_commit == review_receipt.task_head_commit`
   - `integration_receipt.accepted_task_tree_sha == review_receipt.task_tree_sha`
   - `integration_receipt.task_branch_name` has prefix `threadlane/task/<expected_run_tag>/<expected_task_tag>/`
   - `integration_receipt.integration_branch_name` has prefix `threadlane/run/<expected_run_tag>/`
   - When the live task checkout is present, `integration_receipt.verify_against_review(&live_subject, &review_receipt)` succeeds.

---

## 4. Live Checkout vs. Historical Post-Cleanup Verification

### Live Task Checkout Present (`task_checkout_present == true`)
- When `<work_area_dir>/checkouts/tasks/<task_worktree_id>` exists on disk:
  - If **no** review receipt exists for the task:
    - The host verifies the task worktree manifest (`load_and_verify_manifest`), confirms no active job lock exists, and checks that the worktree checkout still passes `evaluate_diff_gate` (or remains within `allowed_write_paths` without symlinks or moved `base_commit`).
    - Projects `review_status = PendingReview`, `integration_status = NotIntegrated`, `evidence_status = PendingReview`.
  - If a review receipt exists for the task:
    - The host reconstructs the live `ReviewSubject` using `threadlane_operations::inspect_review_subject` and verifies it against the receipt via `threadlane_operations::verify_review_receipt`.
    - Any subsequent uncommitted change (`DirtyWorktree`), untracked file, moved `HEAD` commit (`HeadMoved` / digest mismatch), out-of-scope modification, or symlink in the task checkout causes `inspect_review_subject` or `verify_review_receipt` to fail, projecting `review_status = Invalidated` and `evidence_status = Invalidated`.
    - If valid and no integration receipt exists: projects `review_status = Accepted`, `integration_status = ReadyToIntegrate`, `evidence_status = VerifiedLive`.
    - If an integration receipt exists: verifies `integration_receipt.verify_against_review(&live_subject, &review_receipt)`, primary repository `HEAD == record.base_commit`, Git commit/tree/ancestry for `accepted_task_head_commit` and `integrated_target_commit`, and (when the integration checkout is present) the integration worktree manifest, cleanliness, branch, and ancestry (`integrated_target_commit` is an ancestor of live integration `HEAD`). When all checks pass, projects `review_status = Accepted`, `integration_status = Integrated`, `evidence_status = VerifiedLive`.

### Missing Historical Checkout After Cleanup (`task_checkout_present == false`)
- After `cleanup_integrated_task_worktree` removes `<work_area_dir>/checkouts/tasks/<task_worktree_id>` (either full cleanup where manifest and branch ref are removed, or partial cleanup where `<task_worktree_id>.json` records `partial_cleanup_accepted_head` while the checkout directory is removed):
  - If no review receipt exists, removing the task checkout before review is invalid and projects `review_status = Invalidated`, `evidence_status = Invalidated`.
  - When a valid review receipt (and integration receipt) exist:
    1. If `<task_worktree_id>.json` is still present on disk (partial cleanup), the host verifies the manifest via `load_and_verify_manifest`, checks ownership (`run_id`, `task_id`, `base_commit`, `allowed_write_paths`, `!job_active`), and verifies `partial_cleanup_accepted_head` (when set) matches `review_receipt.task_head_commit`.
    2. The host verifies `review_receipt.verify_integrity()` and all contract/identity bindings against the frozen approval, task contract, and run receipt.
    3. The host verifies directly against the canonical repository (`record.repository_path`) that:
       - Primary repository `HEAD` remains at `record.base_commit`.
       - `review_receipt.base_commit` and `review_receipt.task_head_commit` exist as commits in the repository.
       - The exact tree SHA of `review_receipt.task_head_commit` equals `review_receipt.task_tree_sha`.
       - `review_receipt.base_commit` is an ancestor of `review_receipt.task_head_commit`.
    4. When an integration receipt is present, the host additionally verifies `integration_receipt.verify_integrity()`, cross-receipt bindings (`review_receipt_id`, `review_receipt_digest`, `review_subject_digest`, `accepted_task_head_commit`, `accepted_task_tree_sha`), confirms `integrated_target_commit` exists in the repository with exact tree `integrated_tree_sha` and descends from `accepted_task_head_commit`, and (if the integration worktree checkout is still present on disk) verifies the live integration worktree manifest, cleanliness, and ancestry.
    5. Because the live task worktree checkout directory is no longer on disk for working-tree inspection, the host **never** fabricates `VerifiedLive`. Instead it projects:
       - `review_status = HistoricalAccepted`
       - `integration_status = HistoricalIntegrated` (or `NotIntegrated` if only a review receipt existed)
       - `evidence_status = RecordedHistoricalOnly`
       - `verification_detail = Some("Task checkout cleaned up; verified from recorded receipts and repository commit/tree history (live task checkout inspection unavailable).")`

---

## 5. Desktop Session Rendering and Polling (`apps/desktop/src/host/`)

1. **Additive TypeScript Types (`types.ts`)**:
   - Adds `TaskReviewLifecycleStatus`, `TaskIntegrationLifecycleStatus`, `TaskEvidenceVerificationStatus`, `IntegrationStrategy`, `VerifiedReviewReceiptSummary`, `VerifiedIntegrationReceiptSummary`, and `TaskLifecycleProjection`, plus `lifecycle?: TaskLifecycleProjection | null` on `TaskStateSnapshot`.
2. **Pure Lifecycle Helpers (`session-logic.ts`)**:
   - `formatTaskReviewLifecycleStatus`, `formatTaskIntegrationLifecycleStatus`, `formatTaskEvidenceVerificationStatus`, `formatTaskLifecycleReceipts`, `taskReviewNote`, `formatTaskLifecycleDisplay`, and `syncTaskLifecycleTargets` map `TaskStateSnapshot` into accessible stage labels, semantic badge tones (`default`, `active`, `warn`), receipt metadata summaries, and verification limit / invalidation notes.
   - Legacy snapshots without `lifecycle` at `status === "ready_for_review"` fall back cleanly to `Review pending` / `Not integrated` / `Awaiting review receipt`.
   - `shouldPollTaskSnapshot(snapshot)` and `selectTaskIdsForRead(...)` keep visible `ready_for_review` tasks eligible for periodic `'active'` background polling so operator review, integration, later worktree modifications, or post-integration cleanup update the session view automatically on repeated polls.
3. **Narrow Session View (`session.ts`, `session.css`)**:
   - Each worker task row preserves its existing execution status pill (`Ready for review`, `Running`, etc.) and renders a dedicated accessible lifecycle group (`role="group"`, `aria-label="Lifecycle stages for <taskId>"`) when the task has reached `ready_for_review` or carries lifecycle state, displaying distinct stage pills for **Execution**, **Review**, **Integration**, and **Evidence**, followed by verified receipt/commit identifiers and any historical verification limit or invalidation detail.
   - Existing row DOM nodes are updated in place via `syncTaskLifecycleTargets`, preserving focus, `<details>` open state, and scroll position across poll refreshes.
