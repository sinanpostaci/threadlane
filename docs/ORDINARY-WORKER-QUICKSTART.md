# Ordinary Worker quickstart

One exact human-approved single-task scope. No execution before human approval.

1. Read `status` and `catalog` (plus `connect_current_chat`/`operating_guide` when paired). Keep actual IDs/revisions and repository/base; never invent IDs.

2. Publish the required plan; no model `Orchestrator` needed. Use exposed `save_draft`/`publish`: fresh command key, `expected_revision: 0`, actual profile/registry revision, one `worker` task with exact instruction/write paths (at most 4096 UTF-8 bytes).

3. Prepare `OperatorApprovalIntent` for the trusted-local route: replace every `<...>` and both revision zeros with actual plan/registry revisions and budgets. `prepare` compiles the normalized `OperatorApprovalRequest` preview; preparation grants no authority.

```json
{
  "binding_id": "<ACTUAL_BINDING_ID>", "thread_uuid": "<ACTUAL_THREAD_UUID>",
  "plan_id": "<ACTUAL_PLAN_ID>", "plan_revision": 0, "expected_registry_revision": 0,
  "repository_path": "<ACTUAL_ABSOLUTE_REPOSITORY_PATH>",
  "base_commit": "<ACTUAL_40_HEX_BASE_COMMIT>",
  "approved_task_ids": ["<ACTUAL_TASK_ID>"],
  "approved_write_paths": ["<ACTUAL_RELATIVE_WRITE_PATH>"],
  "max_parallel": 1,
  "budgets": {"timeout_ms": 600000, "max_stdout_bytes": 16777216,
    "max_stderr_bytes": 1048576, "max_line_bytes": 4194304, "max_events": 4096}
}
```

`prepare` compiles `expected_plan_digest`; do not invent it.

```sh
threadlane-context --context <CONTEXT_PATH> approve prepare --request <INTENT_JSON>
```

Present `preview.request`, `subject_digest`, and frozen tasks as the unchanged scope.

4. After explicit approval, save `preview.request` unchanged and record:

```sh
threadlane-context --context <CONTEXT_PATH> approve record --request <REQUEST_JSON> \
  --confirm-subject <PREVIEW_SUBJECT_DIGEST> --confirm-thread <ACTUAL_THREAD_UUID> \
  --operator-approved
```

Any changed scope/model/effort/quota/repository/base/task identity needs a fresh preview.

5. Create the storage-only `worker` backlog item first; creation grants no execution authority. Then call `backlog_worker_start` once with the current item revision and frozen approval/task IDs. COPY the literal recipes in the [guide](MCP-OPERATING-GUIDE.md#compact-ordinary-callback-closure) and bind only their one typed object (`APPROVED_TASK_CONTEXT` for prep, `ACCEPTANCE_CONTEXT` after the separate decision); the callback binds nothing. Never parse, compile, or evaluate Markdown or fences. Prep verifies the canonical `threadlane closure-template` capsule, retains the exact start DTO and binding, and owns the only guarded start.

   The fresh-isolate callback uses `createOrdinaryClosureClient.readStored()`: stale, mixed, or failed-prep state refuses before effect; full evidence/report/correlation stay private; only a compact view with identity, guard facts, verified event counts/ACK cursor, and exact physical text is texted. The unchanged read checks bindings, page order/limits, cross-page continuity, duplicate IDs, `step_seq`, `run_id`, and the inner 49152-byte UTF-8 batch before ACK; incomplete state keeps mapping/last ACK/cursor.

   Exact success precedes one bounded physical check: optional `threadlane inspect-delivery`; `HEAD` must equal the expected base; `--text-file` text is untrusted; an equally bounded physical read may record its limits.

   Separately check item semantics, then call `completeStored` once with one typed `ACCEPTANCE_CONTEXT` (fresh command key, bounded note/evidence). It keeps correlation/revision/ref/digest checks before effect, retains intent and the exact receipt, reuses only a matching validated Done, and never retries an unknown reply. Finish briefly; ACK only the real batch ID.
