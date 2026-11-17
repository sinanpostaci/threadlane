# Ordinary Worker quickstart

One exact human-approved single-task scope. No execution before human approval.

1. Call `connect_current_chat` `{}` once, `operating_guide` `{}` on startup/resume/uncertainty, and `status`/`catalog`/`backlog_list` as needed. Keep actual IDs/revisions and repo/base; never invent IDs, bypass checks, or set a fixed binding. Read `NATIVE-OPERATOR.md`/`PROFILE-FLOW.md`/legacy guides only when selected (not for `operator_context` alone).

2. Publish the required one-task plan; no `Orchestrator` needed. Use `save_draft`/`publish`: fresh command key, `expected_revision: 0`, actual profile/registry revision, one `worker` task with exact instruction/write paths (<=4096 UTF-8 bytes).

3. Save `OperatorApprovalIntent` in private `0600` `<INTENT_JSON>` (replace `<...>` and zeros with actual revisions/budgets; `prepare` compiles `expected_plan_digest`, grants no authority):

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

```sh
threadlane-context --context <CONTEXT_PATH> approve prepare --request <INTENT_JSON>
```

Parse `OperatorApprovalPreview` via built-in JSON tools, keep DTO/`subject_digest`/tasks, save ONLY unchanged `preview.request` (`OperatorApprovalRequest`) to private `0600` `<REQUEST_JSON>` via private-file tools (no `python3`/`node`/`jq`/extractor script; never rebuild intent or retype facts). Present `preview.request`, `subject_digest`, frozen tasks.

4. After exact human consent (reusing unchanged valid consent), pass `<REQUEST_JSON>` to `approve record`:

```sh
threadlane-context --context <CONTEXT_PATH> approve record --request <REQUEST_JSON> \
  --confirm-subject <PREVIEW_SUBJECT_DIGEST> --confirm-thread <ACTUAL_THREAD_UUID> \
  --operator-approved
```

Keep source/binding/identity/revision/digest checks; changed scope/model/effort/quota/repo/base/task needs fresh preview.

5. Create storage-only `worker` item first (`backlog_create`; no authority). Never run an unconditional `approved_task`/ready/host probe before start: guarded `backlog_worker_start` owns eligible auto host startup, and `host_not_ready` on an optional pre-start read is not an attempted launch. Start once with current revision and frozen approval/task IDs, or COPY the literal prep recipe in [guide](MCP-OPERATING-GUIDE.md#compact-ordinary-callback-closure) with only `APPROVED_TASK_CONTEXT` (callback binds nothing; never parse/eval Markdown). Keep real startup/refusal/unknown errors and original receipts; no retries/polling.

   Callback runs `createOrdinaryClosureClient.readStored()`: stale/mixed/failed-prep refuses before effect; full evidence/report/correlation stay private; compact view returns identity, guards, event counts/ACK cursor, physical text. Read checks bindings, page order/limits, continuity, duplicate IDs, `step_seq`, `run_id`, and 49152-byte inner batch before ACK; incomplete keeps mapping/last ACK/cursor.

   Exact success precedes one bounded physical check: optional `threadlane inspect-delivery` (`HEAD` equals base; `--text-file` untrusted; or equally bounded physical read).

   Separately check semantics, then call `completeStored` once with `ACCEPTANCE_CONTEXT` (fresh command key, bounded note/evidence). It validates correlation/revision/ref/digests before effect, retains intent/receipt, reuses only matching Done, and never retries unknown replies. Finish briefly; ACK only the real batch ID.
