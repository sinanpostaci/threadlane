# Ordinary Worker quickstart

No execution before human approval.

1. Call `connect_current_chat` `{}` then `operating_guide` `{}`; reconnect/refetch on resume. Retain/compare `thread_uuid`, `binding_id`, project, `cli_package`, `operator_context.content_sha256`. Refresh uncertain scoped state, including `catalog`. Use the connection's router/context; never guess IDs or search packages.

2. `prepare-task-exact` saves/publishes one task before preview. For a plan from `save_draft`/`publish`, use `prepare-private-exact` with an `OperatorApprovalIntent`: `binding_id`, `thread_uuid`, `plan_id`, `plan_revision`, `expected_registry_revision`, `repository_path`, `base_commit`, `approved_task_ids`, `approved_write_paths`, `max_parallel` and the same `budgets`.

3. In `functions.exec`, use the actual connection, task, profile and authorized budgets. Both commands reject omitted budgets and unknown fields, including `max_attempts`/`max_workers`. `max_parallel` is concurrency; `progress_watchdog` is optional.

```javascript
const request = {
  "binding_id":"<BINDING_ID>","thread_uuid":"<THREAD_UUID>",
  "repository_path":"<ABS_REPO_PATH>","base_commit":"<40_HEX_BASE_COMMIT>",
  "profile_id":"<PROFILE_ID>","profile_revision":1,"command_key":"<FRESH_KEY>",
  "summary":"<SUMMARY>","task":{"id":"<TASK_ID>","title":"<TITLE>",
    "instruction":"<INSTRUCTION>","role":"coder","difficulty":"easy",
    "depends_on":[],"allowed_write_paths":["STATUS.txt"]},
  "budgets":{"timeout_ms":90000,"max_line_bytes":16384,"max_stdout_bytes":2097152,
    "max_stderr_bytes":100000,"max_events":321}
};
const router = "<context_router_executable>";
const context = "<context_path>";
const q = s => "'" + s.replace(/'/g, "'\\''") + "'";
const cmd = "printf '%s' " + q(JSON.stringify(request)) + " | " + q(router) +
  " --context " + q(context) + " approve prepare-task-exact";
const r = await tools.exec_command({cmd, sandbox_permissions:"require_escalated",
  justification:"Allow this preview to reach the verified local supervisor?",
  yield_time_ms:30000});
text(JSON.stringify(r));
```

Paths: `operator_context.context_router_executable` and `operator_context.context_path`. Omit `sandbox_permissions`/`justification` when supervisor access permits. On refusal or `transport_unavailable` stop. Collect a running `session_id`; never retry unknown results. After partial preparation inspect the original plan/revision. Ceilings: timeout 3600000 ms, line 4194304 B, stdout 33554432 B, stderr 4194304 B, events 4096; each >= 1 and line <= stdout.

Both return `{preview_path,preview}`; the CLI verifies its `0600` preview in a private `0700` directory. Match the full normalized `preview.request` (`OperatorApprovalRequest`), task, model/effort, executable, budgets, paths and digests to human scope. Retain the path unchanged. Legacy `prepare-task`, `prepare-private`, `prepare --request` retain historical defaults.

4. After exact human consent, use the same router, quoting and access policy to run `approve record --preview <preview_path> --confirm-subject <subject_digest> --confirm-thread <thread_uuid> --operator-approved`. The CLI recompiles/matches the preview; recording starts no Worker. `record --request` requires unchanged `preview.request` in a private `0600` file.

5. Create a storage-only `worker` item. For exact-file review call `ordinary_review_scope({approval_id,task_id,item_id,source_files})`, then guarded `backlog_worker_start` once; no readiness/host pre-probe. List tracked inputs to preserve. For a standalone new file use `source_files: []`. On callback, `ordinary_review_notice({approval_id})` reconciles, ACKs events and prepares evidence. Decide semantics independently; call `ordinary_review_complete` only on a positive decision. Missing/unsupported scope stops; no automatic fallback. For legacy routes use their guide. Never redispatch unknown outcomes. Inspect delivery or use an equally bounded physical read; HEAD must equal the expected base. Test an exact-byte checkout with its own `CARGO_TARGET_DIR`.
