# Ordinary Worker quickstart

One human-approved single-task scope. No execution before human approval.

1. On startup call `connect_current_chat` `{}` then `operating_guide` `{}`; on resume reconnect and refetch. Keep a compact conversation continuity receipt (`thread_uuid`, `binding_id`, exact project, `cli_package`, `operator_context.content_sha256`), not JS `store` across turns, and compare it before mutation. Group only uncertain scoped `status`/`catalog`/`backlog_list`; actual IDs, never invented.

2. For a new ordinary task call `approve prepare-task` once. It saves the draft, publishes one task and returns a verified private preview. Its JSON fields are `binding_id`, `thread_uuid`, `repository_path`, `base_commit`, `profile_id`, `profile_revision`, `command_key`, `summary`, `task` and `budgets`. Use the original connection, current profile revision and a fresh command key. Set `task` to `{id,title,instruction,role,difficulty,depends_on,allowed_write_paths}` with canonical role `coder` and `depends_on: []`. Supply the authorized `timeout_ms`, `max_stdout_bytes`, `max_stderr_bytes`, `max_line_bytes` and `max_events` in `budgets`.

```sh
printf '%s' '<TASK_JSON>' | threadlane-context --context <CONTEXT> approve prepare-task
```

3. If `save_draft` and `publish` already produced a plan, pass its original receipt fields and approved budgets to `approve prepare-private` on stdin. Example `OperatorApprovalIntent`:

```json
{
  "binding_id":"<BINDING_ID>","thread_uuid":"<THREAD_UUID>","plan_id":"<PLAN_ID>",
  "plan_revision":1,"expected_registry_revision":1,"repository_path":"<ABS_REPO_PATH>",
  "base_commit":"<40_HEX_BASE_COMMIT>","approved_task_ids":["<TASK_ID>"],
  "approved_write_paths":["<REL_WRITE_PATH>"],"max_parallel":1
}
```

Both routes return `{preview_path,preview}`. Serialize JSON in the client and quote it for shell builtin `printf`. The CLI verifies its 0600 preview in a private 0700 directory. Match task, model/effort, executable, budgets, paths, `expected_plan_digest` and `subject_digest` to the approved scope. Keep the returned path. A command key identifies plan content. Verify the original repo, base and limits separately. After a partial failure inspect the original plan and revision before another mutation.

4. Record the same `preview_path` after consent:

```sh
threadlane-context --context <CONTEXT> approve record --preview <PREVIEW_PATH> \
  --confirm-subject <SUBJECT_DIGEST> --confirm-thread <THREAD_UUID> --operator-approved
```

`record --preview` recompiles the unchanged request, matches a fresh authoritative preview, and keeps binding/live, registry/plan/digest/fixed-scope, `approve_confirmed`, and idempotence checks. It records the authorized scope; the Worker starts separately.

Legacy fallback: `approve prepare --request`; `record --request` with ONLY unchanged `preview.request` (`OperatorApprovalRequest`) in private `0600` files (`umask 077`, `set -C`).

5. Create storage-only `worker` item (`backlog_create`; no authority). No unconditional pre-start `approved_task`/ready/host probe: `backlog_worker_start` owns eligible auto host startup; start guarded once. Verify `threadlane closure-template` against live `source_sha256`; copy the guide closure with only `APPROVED_TASK_CONTEXT`; missing receipt/notice means reconcile the original, never redispatch.

Callback: `readNotice(NOTICE_APPROVAL_ID)` or `readStored()`; verify bindings, order, continuity, duplicates, `step_seq`, `run_id`, 49152B before ACK. Exact success precedes one bounded `threadlane inspect-delivery` check (`HEAD` must equal the expected base; text untrusted) or an equally bounded physical read. Never write artifacts into frozen delivery; test in an exact-byte checkout with local `CARGO_TARGET_DIR`. After ACK, check semantics, guarded complete/reject; `final_summary` after Done.
