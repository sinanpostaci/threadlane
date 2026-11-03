# Multi-Worker to Single-Job Integration Bridge (`threadlane-operations`)

## Motivation

In multi-worker jobs, `ApprovedHostClient::dispatch_task` starts each supervised worker via
`ScopedClient::start_worker`, assigning a distinct `run_id` and `worker_id` to every worker task
worktree (`WorktreeKind::Task`). Legacy single-run integration (`propose_integration` /
`execute_integration`) enforces `integration_manifest.run_id == task_manifest.run_id == subject.run_id`,
which remains intact for single-run workflows and manual same-run operations.

To aggregate accepted task worktrees from separately owned worker runs into one owned
`WorktreeKind::RunIntegration` target worktree without relabeling task/receipt/run IDs, mutating
worktree manifests, or weakening legacy same-run validation, `threadlane-operations` provides an
explicit job-scoped aggregation API and typed receipt provenance.

## Public API

- **`JobIntegrationScopeProvenance`**: Binds `job_id`, `job_revision`, `approval_id`,
  `approval_subject_digest`, `target_integration_run_id`, and `aggregate_allowed_write_paths`.
- **`JobIntegrationTaskInput`**: Binds each task's `original_run_id`, `task_id`, `task_digest`,
  `task_worktree_id`, `ReviewSubject`, persisted `ReviewReceipt`, `expected_task_head_commit`,
  optional `dependency_input` (`JobTaskDependencyExecutionProof { policy, instruction }` for
  non-root dependent tasks), `IntegrationStrategy`, and optional `receipt_id`.
- **`propose_job_integration` (`ProposeJobIntegrationRequest` -> `JobIntegrationProposal`)**:
  Strictly read-only verification and merge-chain simulation across all task inputs and the target
  `RunIntegration` worktree.
- **`execute_job_integration` (`ExecuteJobIntegrationRequest` -> `JobIntegrationReport`)**:
  Verifies all manifests, live checkouts, persisted review receipts, write scopes, optional
  persisted `DependencyInputSeal` proofs for non-root dependent tasks, and pairwise conflict-free
  mergeability before recording a durable `JobIntegrationIntent` and executing merges inside the
  owned `RunIntegration` checkout.
- **`JobAggregationReceiptProvenance` (`IntegrationReceipt::job_aggregation`)**:
  Optional additive field on `IntegrationReceipt` (`#[serde(default, skip_serializing_if = "Option::is_none")]`).
  Legacy same-run receipts omit this field (`None`), preserving exact historical JSON serialization
  and SHA-256 `receipt_digest` values. Job-aggregated receipts bind both `original_task_run_id`
  (`IntegrationReceipt::run_id`) and `target_integration_run_id`, root `approved_base_commit`
  (while `IntegrationReceipt::base_commit` binds the task's actual execution base commit),
  optional `dependency_seal_digest` and `dependency_snapshot_ref` for non-root tasks, job/approval
  digests, task tree and diff digests, task and job write scopes, policy and passing check
  evidence, proposal digest, and intent/command keys.
- **Intent inspection and uncertainty helpers**:
  `record_job_integration_intent_before_effect`, `inspect_job_integration_intent_readonly`, and
  `mark_job_integration_intent_unknown`.

## Trust and Authority Boundaries

- **No self-approval or cryptographic authority claim**: The native controller or trusted local
  caller must separately verify that an immutable human-approved job and authority envelope permits
  local task commit and integration. `JobIntegrationScopeProvenance` and
  `JobAggregationReceiptProvenance` record provenance for deterministic audit and digest binding,
  and explicitly enforce:
  - `cryptographically_proves_user_authority == false`
  - `native_chat_identity_verified == false`
  - `grants_main_merge_or_push_authority == false`
- **Owned `RunIntegration` mutation only**: `execute_job_integration` mutates only the owned
  `WorktreeKind::RunIntegration` checkout. It never merges into `main`, pushes to remotes, deletes
  branches or worktrees, or modifies the primary source repository checkout.
- **No conflict auto-resolution**: Merge conflicts, dirty checkouts, detached branches, stale or
  superseded review receipts, or out-of-scope paths are refused before mutating the target
  `RunIntegration` checkout (never selecting `--ours` or `--theirs`).

## Intent-Before-Effect, Idempotent Reuse, and Recovery Limits

1. **Pre-effect intent journaling**: Before running any `git merge` in the target `RunIntegration`
   worktree, `execute_job_integration` writes a private (`0600`) `JobIntegrationIntent` in
   `JobIntegrationIntentState::InProgress` under
   `<work_area_root>/manifests/job-integrations/job-integration-intent-<integration_worktree_id>.json`.
2. **Duplicate receipt reuse**: Re-invoking `execute_job_integration` for an already completed batch
   verifies live checkout heads and all persisted `IntegrationReceipt`s on disk and returns
   `reused_existing_receipts: true` without creating new commits. Reusing an existing `command_key`
   with a different payload is rejected with `OperationsError::ReceiptInvalidated`.
3. **Receipt recovery vs. uncertainty fencing (`OutcomeUnknown`)**:
   - If a prior call crashed after persisting all planned `IntegrationReceipt`s and advancing the
     `RunIntegration` `HEAD` (leaving the intent in `InProgress`), a matching retry verifies the
     complete receipt chain against live Git state and promotes the intent to `Completed`.
   - If an `InProgress` intent does not have a complete verified receipt chain matching the live
     `RunIntegration` `HEAD` (for example, a crash before or mid-batch), `execute_job_integration`
     transitions the intent to `OutcomeUnknown` and refuses blind replay with
     `OperationsError::UncertainWorkerOutcome` across all `command_key`s.
   - **Remaining recovery limit**: Automatic recovery only completes when all planned task
     `IntegrationReceipt`s were durably written and match the live `RunIntegration` `HEAD`. Partial
     mid-batch merges where some tasks merged into `RunIntegration` before interruption are fenced
     as `OutcomeUnknown` and require explicit operator inspection/reset rather than automatic
     branch rollback or replay.

## Final Candidate Inspection (`inspect_final_candidate_readonly`)

- Rejects any `RunIntegration` worktree that has an unresolved (`InProgress` or `OutcomeUnknown`)
  `JobIntegrationIntent` on disk.
- Accepts predecessor tasks from distinct `original_task_run_id`s only when each predecessor's
  persisted `IntegrationReceipt` carries verified `JobAggregationReceiptProvenance` targeting the
  `RunIntegration` worktree's `run_id` and `worktree_id`, matching `task_digest`, and matching the
  candidate policy's `approval_id` and `approval_subject_digest`.
- Enforces that all job-aggregated predecessors in a candidate share identical job/approval scope
  provenance and forbids mixing legacy same-run and job-aggregated receipts on the same
  `RunIntegration` worktree.
- Preserves `verify_legacy_base_closure_insufficient` so a base-tree-only reservation can never
  satisfy final combined candidate closure.
