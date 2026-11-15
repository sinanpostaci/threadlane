# Ordinary Worker quickstart

One exact human-approved single-task scope. No execution before human approval.

1. Read actual `status` and `catalog` (plus `connect_current_chat`/`operating_guide` when
   paired). Keep the actual `binding_id`, thread UUID, profile ID, registry revision,
   repository path, and base commit. Never invent IDs or reuse another chat's approval.

2. Publish the required plan record. No model `Orchestrator` is mandatory. Use the exposed
   `save_draft` and `publish` with their exposed schemas and actual returned IDs. For a new
   draft use a fresh command key, `expected_revision: 0`, the actual profile/registry revision,
   one `worker` task, and its exact instruction/write paths. Task instructions must be nonempty
   and at most 4096 UTF-8 bytes. Publish the actual returned plan ID and revision. Read the
   longer operator reference only if these tools are unavailable; do not guess CLI flags.

3. Prepare the exact `OperatorApprovalIntent`. Put this strict JSON in a private file and
   replace every `<...>` with actual returned values. Replace both revision zeros with the
   actual positive published-plan and registry revisions. The budget numbers illustrate a
   bounded task; set every limit to the concrete scope being presented. Preparation grants no
   authority: the exact preview still needs explicit human approval before record/start.

```json
{
  "binding_id": "<ACTUAL_BINDING_ID>",
  "thread_uuid": "<ACTUAL_THREAD_UUID>",
  "plan_id": "<ACTUAL_PLAN_ID>",
  "plan_revision": 0,
  "expected_registry_revision": 0,
  "repository_path": "<ACTUAL_ABSOLUTE_REPOSITORY_PATH>",
  "base_commit": "<ACTUAL_40_HEX_BASE_COMMIT>",
  "approved_task_ids": ["<ACTUAL_TASK_ID>"],
  "approved_write_paths": ["<ACTUAL_RELATIVE_WRITE_PATH>"],
  "max_parallel": 1,
  "budgets": {
    "timeout_ms": 600000,
    "max_stdout_bytes": 16777216,
    "max_stderr_bytes": 1048576,
    "max_line_bytes": 4194304,
    "max_events": 4096
  }
}
```

`prepare` compiles `expected_plan_digest`; do not invent it.

```sh
threadlane-context --context <CONTEXT_PATH> approve prepare --request <INTENT_JSON>
```

The preview returns the normalized `OperatorApprovalRequest` as `preview.request`, plus
`subject_digest` and frozen tasks. Present that exact unchanged scope to the human.

4. After explicit approval, save `preview.request` unchanged and record:

```sh
threadlane-context --context <CONTEXT_PATH> approve record --request <REQUEST_JSON> \
  --confirm-subject <PREVIEW_SUBJECT_DIGEST> --confirm-thread <ACTUAL_THREAD_UUID> \
  --operator-approved
```

Any changed scope/model/effort/quota/repository/base/task identity needs a fresh preview.

5. Run the guarded Worker: create a `worker` item, call `backlog_worker_start` with the actual
   `expected_revision`, approval ID, and task ID, wait for the callback, pull and ACK the
   returned batch, call `backlog_worker_result`, make one bounded physical/semantic check, then
   complete or reject with the returned `item_snapshot.revision`, `execution_ref`, and matching
   digests. A successful complete returns immutable `Done`; report it briefly and do not rewrite
   the receipt. Prepare/record never invoke a Worker or accept work.
