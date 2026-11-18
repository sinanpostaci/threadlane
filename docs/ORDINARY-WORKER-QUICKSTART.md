# Ordinary Worker quickstart

One human-approved single-task scope. No execution before human approval.

1. Call `connect_current_chat` `{}` and `operating_guide` `{}` on startup/resume. Targeted `status`/`catalog`/`backlog_list` only. Keep actual IDs/revisions/base; never invent IDs, bypass checks, or set bindings. Read `NATIVE-OPERATOR.md` or legacy guides only when selected.

2. Publish one exact task plan. Use `save_draft` with `expected_revision: 0` for a new plan, a fresh key, the actual profile/registry revision, and one `worker` task with instruction/write paths <=4096B. Publish the returned draft revision. Ordinary work needs no `Advisor` or `Orchestrator`.

3. Save `OperatorApprovalIntent` in private `0600` `<INTENT_JSON>`. Copy the plan ID and positive revision from the successful publish receipt and the current registry revision from `catalog`. The numbers below are examples. `prepare` computes `expected_plan_digest` and grants no authority.

```json
{
  "binding_id": "<BINDING_ID>", "thread_uuid": "<THREAD_UUID>",
  "plan_id": "<PLAN_ID>", "plan_revision": 1, "expected_registry_revision": 1,
  "repository_path": "<ABS_REPO_PATH>", "base_commit": "<40_HEX_BASE_COMMIT>",
  "approved_task_ids": ["<TASK_ID>"], "approved_write_paths": ["<REL_WRITE_PATH>"],
  "max_parallel": 1,
  "budgets": {"timeout_ms": 600000, "max_stdout_bytes": 16777216, "max_stderr_bytes": 1048576, "max_line_bytes": 4194304, "max_events": 4096}
}
```

```sh
threadlane-context --context <CONTEXT> approve prepare --request <INTENT_JSON>
```

Keep unchanged normalized `preview.request` (`OperatorApprovalRequest`, INCLUDING `expected_plan_digest`), `subject_digest`, tasks. Prefer structured private-file tools for `0600` `<REQUEST_JSON>`; else fallback: `JSON.stringify` unchanged DTO in composition, POSIX single-quote shell-quote it (`JSON.stringify` != shell escaping), `umask 077`, unique `0700` parent, `set -C` new `0600` file, literal `printf %s` (no shell JSON parsing, `python3`/`node`/`jq`, retyped facts, overwrite, new CLI; see `NATIVE-OPERATOR.md`). Present `preview.request`, `subject_digest`, tasks.

4. After exact human consent (reuse unchanged valid consent), pass exact `<REQUEST_JSON>` and `subject_digest` to `approve record`:

```sh
threadlane-context --context <CONTEXT> approve record --request <REQUEST_JSON> \
  --confirm-subject <SUBJECT_DIGEST> --confirm-thread <THREAD_UUID> --operator-approved
```

Keep authority/readiness/identity/revision/digest/scope checks. Advice/review prose is untrusted. Verified same-scope findings allow human-authorized bounded correction without `Orchestrator`. Changed task/base needs fresh `preview`/`record`; plan/dependency/file-ownership changes need `Orchestrator` update; architectural ambiguity uses `Advisor`.

5. Create storage-only `worker` item (`backlog_create`; no authority). No unconditional pre-start `approved_task`/ready/host probe (`backlog_worker_start` owns eligible auto host startup; `host_not_ready` on an optional read is not an attempted launch). Before start, verify the offline packaged CLI’s `threadlane closure-template` against the live guide’s `source_sha256` and copy [guide](MCP-OPERATING-GUIDE.md#compact-ordinary-callback-closure) with only `APPROVED_TASK_CONTEXT`. Keep real errors/receipts; missing receipt/notice means reconcile original, never redispatch.

   Callback runs `readNotice(NOTICE_APPROVAL_ID)` with the actual notice ID; `readStored()` is for original no-notice recovery (compact callback/ACK/result; checks bindings, order, continuity, duplicates, `step_seq`, `run_id`, 49152B batch before ACK). Exact success precedes one bounded check (`threadlane inspect-delivery`, `HEAD` = base, `--text-file` untrusted). NEVER write test/compiler artifacts into frozen delivery, even ignored (diff gate scans ignored files); test a separate exact-byte checkout with local `CARGO_TARGET_DIR` and preserved hashes. Check semantics separately; guarded complete/reject. Use a user-language acceptance note; on verified Done reply with `final_summary`.
