# Ordinary Worker quickstart

No execution before human approval.

1. Call `connect_current_chat` `{}` then `operating_guide` `{}`; reconnect/refetch on resume. Keep `thread_uuid, binding_id, project_root, cli_package, operator_context.content_sha256`; refresh uncertain state/`catalog`. Use its router/context and exact canonical paired `project_root` for `repository_path`; never guess IDs. In an authorized new empty test workspace initialize the fixture there before preview; keep nested clones/proofs/build products outside.

2. `prepare-task-exact` saves/publishes one task before preview. For `save_draft`/`publish` plans use `prepare-private-exact` (`OperatorApprovalIntent`: `binding_id, thread_uuid, plan_id, plan_revision, expected_registry_revision, repository_path, base_commit, approved_task_ids, approved_write_paths, max_parallel, budgets`).

3. Use actual connection/task/profile/budgets in `functions.exec`. Both reject omitted budgets and unknown fields (`max_attempts`/`max_workers`). `max_parallel` is concurrency; `progress_watchdog` optional.

```javascript
const req = {
  "binding_id":"<BINDING>","thread_uuid":"<THREAD>",
  "repository_path":"<PROJECT_ROOT>","base_commit":"<40_HEX_BASE>",
  "profile_id":"<PROFILE>","profile_revision":1,"command_key":"<KEY>",
  "summary":"<SUM>","task":{"id":"<ID>","title":"<TITLE>",
    "instruction":"<INSTR>","role":"coder","difficulty":"easy",
    "depends_on":[],"allowed_write_paths":["STATUS.txt"]},
  "budgets":{"timeout_ms":90000,"max_line_bytes":16384,"max_stdout_bytes":2097152,
    "max_stderr_bytes":100000,"max_events":321}
};
const q = s => "'" + s.replace(/'/g, "'\\''") + "'";
const cmd = "printf '%s' " + q(JSON.stringify(req)) + " | " + q("<router>") +
  " --context " + q("<context>") + " approve prepare-task-exact";
const r = await tools.exec_command({cmd, sandbox_permissions:"require_escalated",
  justification:"Allow supervisor preview?", yield_time_ms:30000});
text(JSON.stringify(r));
```

Paths: `operator_context` `context_router_executable`/`context_path`; omit `sandbox_permissions`/`justification` when loopback works. On `transport_unavailable` with ready context, run one read-only `flow grant-list` (`docs/NATIVE-OPERATOR.md`), request scoped executor permission (not human approval), retry read-only once; preserve ambiguity/refusals, stop on refusal/denied access, collect `session_id`, never retry unknowns. Max (>=1, line<=stdout): 3600000ms, line/stderr 4194304B, stdout 33554432B, 4096 events.

Both return `{preview_path,preview}` (`0600` in `0700`). Match `preview.request` (`OperatorApprovalRequest`), task/model/effort/executable/budgets/paths/digests to scope; keep `preview_path`. Legacy `prepare-task`/`prepare-private`/`prepare --request` stay.

4. After human consent, run `approve record --preview <path> --confirm-subject <digest> --confirm-thread <uuid> --operator-approved` (same router/quoting/access); starts no Worker. `record --request` needs unchanged `preview.request` in `0600`.

5. Create a storage-only `worker` item. Native review: only supported small exact-file text deliveries (every approved write path changes); before guarded start, `ordinary_review_scope` (`source_files`: original tracked source files to preserve even for dependent new output; `[]` only for a standalone new file needing no original source). Larger/unsupported tasks select their supported route up front (`inspect-delivery` or equally bounded physical read; HEAD must equal the expected base). `backlog_worker_start` once (no readiness/host pre-probe). On callback, `ordinary_review_notice` reconciles/ACKs to `ready_for_semantic_review` (`observed_model, observed_effort, attempt_count, base_commit, task_checkout_path`, `read_id`); reuse ready bytes/hashes (or hash-check copied test input against that read), test independently outside the checkout (own `CARGO_TARGET_DIR`), do not requery present facts. Decide semantics independently; `ordinary_review_complete` (original `read_id`) only on accept. Native refusal/missing scope stops (no automatic fallback); never redispatch unknowns.
