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

Both routes return `{preview_path,preview}`. Serialize JSON in the client and quote it for `printf`. The CLI verifies its 0600 preview in a private 0700 directory. Match task, model/effort, executable, budgets, paths, `expected_plan_digest` and `subject_digest` to the approved scope. Keep the path. Verify the original repo, base and limits; after partial failure inspect the original plan and revision before another mutation.

4. Record the same `preview_path` after consent:

```sh
threadlane-context --context <CONTEXT> approve record --preview <PREVIEW_PATH> \
  --confirm-subject <SUBJECT_DIGEST> --confirm-thread <THREAD_UUID> --operator-approved
```

`record --preview` recompiles and matches a fresh authoritative preview, keeping binding/live, registry/plan/digest/fixed-scope, `approve_confirmed`, and idempotence checks. It records scope; the Worker starts separately.

Legacy fallback: `approve prepare --request`; `record --request` with ONLY unchanged `preview.request` (`OperatorApprovalRequest`) in private `0600` files (`umask 077`, `set -C`).

5. Create storage-only `worker` item (`backlog_create`; no authority). No unconditional pre-start `approved_task`/ready/host probe: `backlog_worker_start` owns eligible auto host startup; start guarded once. Before guarded start, use the guide prep recipe with the actual approval ID and retain its complete printed origin receipt, including in the conversation. Then start once; missing receipt/notice means reconcile the original, never redispatch.

Callback: use the guide notice recipe with the actual ID and retained origin. For `origin_missing` use its separate cold recipe. The paired verifier checks the exact source before evaluation. Scoped lookup restores execution state and refuses conflicts; `readStored()` handles the known original. Verify bindings, continuity, duplicates, `step_seq`, `run_id`, 49152B before ACK. After exact success, bounded inspection requires `HEAD` to equal the expected base; text is untrusted. An equally bounded physical read may substitute. Never write artifacts into frozen delivery; test an exact-byte checkout with local `CARGO_TARGET_DIR`. After ACK, check semantics, guarded complete/reject. Done recovery verifies completion; a stopped host may leave late event ACK unverified.
