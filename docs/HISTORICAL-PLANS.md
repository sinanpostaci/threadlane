# Historical published plan reads

`ScopedWorkflow::get_plan` and `ScopedWorkflow::list_plans` build `PlanSummary`
headers by first attempting active compilation against the current
`RegistrySnapshot`. When the global registry revision advances past an active
published plan revision's pinned `profile_revision`, active compilation rejects
the revision with `profile_revision_mismatch`.

For active published revisions (`published`, `received`, or `applied`) whose
pinned `profile_revision` predates the current registry revision, `get_plan` and
`list_plans` consult the binding's existing durable workflow command journal
(`workflow-commands-<tag>.json`) in read-only mode to recover the frozen
compiled `plan_digest`.

## Verification requirements

The historical summary digest fallback returns `Some(plan_digest)` only when all
of the following hold:

- The persisted `PlanRevision` passes structural DAG and write-scope validation,
  belongs to the scoped `binding_id`, is in an active published state
  (`published`, `received`, or `applied`), and has
  `0 < profile_revision < current_registry_revision`.
- The per-binding command journal exists as a regular `0600` non-symlink file
  inside the `0700` `PlanStore` directory within the 1 MiB / 1,024-entry bounds,
  and its `binding_id` matches the scoped binding.
- No `Pending` journal entry and no contradictory `Failed` entry exists for
  `(plan_id, revision)`, and there is a single unambiguous `Completed` entry
  whose `resolved_plan_id`, `target_revision`, `created_revision`, `profile_id`,
  `profile_revision`, and `summary` match the persisted `PlanRevision`.
- Both `tasks_digest` and `payload_digest` recomputed directly from the
  persisted `PlanRevision` match the frozen journal entry, and both
  `entry.plan_digest` and `Completed.plan_digest` contain the identical valid
  64-character lowercase hex SHA-256 digest.

If the journal is missing, `Pending`, `Failed`, tampered, corrupt, cross-binding,
wrong-revision, or ambiguous, or if the plan revision is `draft` or
`superseded`, the fallback returns `plan_digest: None`.

## Preserved boundaries

- Read-only fallback never creates or opens `.lock` files, never mutates
  `plans.json`, `registry.json`, or the command journal, and never advances
  delivery receipts.
- Historical plans are never recompiled against changed role/model assignments
  from a newer registry revision.
- `save_draft`, `publish`, approval, and dispatch registry-drift gates remain
  strictly enforced against the current `RegistrySnapshot`.
