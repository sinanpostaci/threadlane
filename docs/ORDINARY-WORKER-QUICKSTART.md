# Ordinary Worker quickstart

One exact human-approved single-task scope. No execution before human approval.

1. Read actual `status` and `catalog` (plus `connect_current_chat`/`operating_guide` when
   paired). Keep actual IDs/revisions and repository/base; never invent IDs or reuse another
   chat's approval. An exported `operator_context` needs no CLI probe; paired preparation uses
   verified MCP tools. Never bypass connection, binding, readiness, revision, digest, or
   sandbox refusals.

2. Publish the required plan record; no model `Orchestrator` is mandatory. Use exposed
   `save_draft`/`publish` with actual schemas/IDs: fresh command key, `expected_revision: 0`,
   actual profile/registry revision, one `worker` task with exact instruction/write paths
   (at most 4096 UTF-8 bytes). Publish the returned plan ID and revision. Use the CLI only for
   explicit trusted-local preview/record.

3. Prepare the exact `OperatorApprovalIntent` for the trusted-local route, replacing every
   `<...>` and both revision zeros with actual positive plan and registry revisions; set
   every budget to the presented scope.
   `prepare` compiles the normalized `OperatorApprovalRequest` preview; preparation grants no
   authority.

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

5. Run the guarded Worker: create a `worker` item, call `backlog_worker_start` with the
   actual `expected_revision`, approval ID, and task ID; wait for the callback; pull and ACK
   the returned batch and bounded `has_more` pages before waiting; call `backlog_worker_result`;
   make one bounded physical and semantic
   check; then complete or reject with the returned `item_snapshot.revision`, `execution_ref`,
   and matching digests.

   For a small delivery, prefer one trusted-local `threadlane inspect-delivery` call with the
   actual root/base and allowed paths from authenticated worker evidence. It reports bounded
   HEAD, changed-path, and per-file regular-file/UTF8/final-LF/size/SHA-256 facts. The
   exact-HEAD helper requires `HEAD` to equal the expected base; only explicit small UTF-8
   `--text-file` contents are included, marked untrusted, and off by default. It
   never executes contents, refuses unsafe, staged, out-of-scope, special-file, or changing
   evidence, and grants no execution, review, acceptance, or `Done` authority. When the verified
   CLI is unavailable, make an equally bounded physical read through the available trusted-local
   route and record its limits and refusal conditions.

   ACK only the actual returned `batch_id`; ACK is not acceptance. Batch ACK/result only when
   the exposed tools permit, preserving ordering, refusals, and the actual batch ID. A
   successful complete returns immutable `Done`: report it briefly, do not rewrite the
   receipt. Keep a small durable Coworker item for multi-call work when useful.
