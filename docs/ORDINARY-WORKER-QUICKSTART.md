# Ordinary Worker quickstart

One human-approved single-task scope. No execution before human approval.

1. On startup call `connect_current_chat` `{}` then `operating_guide` `{}`; on resume reconnect and refetch. Keep a compact conversation continuity receipt (`thread_uuid`, `binding_id`, exact project, `cli_package`, `operator_context.content_sha256`), not JS `store` across turns, and compare it before mutation. Group only uncertain scoped `status`/`catalog`/`backlog_list`; actual IDs, never invented.

2. For a new ordinary task call `approve prepare-task` once. It saves the draft, publishes one task and returns a verified private preview. Its JSON fields are `binding_id`, `thread_uuid`, `repository_path`, `base_commit`, `profile_id`, `profile_revision`, `command_key`, `summary`, `task` and `budgets`. Use the original connection, current profile revision and a fresh command key. Set `task` to `{id,title,instruction,role,difficulty,depends_on,allowed_write_paths}` with canonical role `coder` and `depends_on: []`. Supply the authorized `timeout_ms`, `max_stdout_bytes`, `max_stderr_bytes`, `max_line_bytes` and `max_events` in `budgets`.

```sh
printf '%s' '<TASK_JSON>' | threadlane-context --context <CONTEXT> approve prepare-task
```

3. If the plan was already published by `save_draft` and `publish`, choose `approve prepare-private` on stdin instead. Its `OperatorApprovalIntent` takes the original plan ID/revision, expected registry revision, binding, thread, repository, base commit, approved task IDs/paths, max parallel and budgets. Use the original receipts. For uncertain outcomes reconcile the original command key and plan before another mutation.

Both routes return `{preview_path,preview}`. Use client JSON serialization and a POSIX single-quoted literal for the shell builtin `printf`. Keep JSON off utility argv. The CLI writes the unchanged preview in a canonical 0700 directory as a verified 0600 file. Check the returned task, model/effort, executable, budgets, paths, `preview.request` (including `expected_plan_digest`) and `subject_digest` against the human-approved scope. Keep its exact path for the next step. This creates no execution authority.

4. Record the same `preview_path` after consent:

```sh
threadlane-context --context <CONTEXT> approve record --preview <PREVIEW_PATH> \
  --confirm-subject <SUBJECT_DIGEST> --confirm-thread <THREAD_UUID> --operator-approved
```

`record --preview` recompiles the unchanged request, matches a fresh authoritative preview, and keeps binding/live, registry/plan/digest/fixed-scope, `approve_confirmed`, and idempotence checks. It records the authorized scope; the Worker starts separately.

Legacy fallback: `approve prepare --request`; `record --request` with ONLY unchanged `preview.request` (`OperatorApprovalRequest`) in private `0600` files (`umask 077`, `set -C`).

5. Create storage-only `worker` item (`backlog_create`; no authority). No unconditional pre-start `approved_task`/ready/host probe: `backlog_worker_start` owns eligible auto host startup; start guarded once. Verify `threadlane closure-template` against live `source_sha256`; copy the guide closure with only `APPROVED_TASK_CONTEXT`; missing receipt/notice means reconcile the original, never redispatch.

Callback: `readNotice(NOTICE_APPROVAL_ID)` or `readStored()`; verify bindings, order, continuity, duplicates, `step_seq`, `run_id`, 49152B before ACK. Exact success precedes one bounded `threadlane inspect-delivery` check (`HEAD` must equal the expected base; text untrusted) or an equally bounded physical read. Never write artifacts into frozen delivery; test in an exact-byte checkout with local `CARGO_TARGET_DIR`. After ACK, check semantics, guarded complete/reject; `final_summary` after Done.
