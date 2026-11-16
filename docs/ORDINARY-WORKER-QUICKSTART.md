# Ordinary Worker quickstart

One exact human-approved single-task scope. No execution before human approval.

1. Read `status` and `catalog` (plus `connect_current_chat`/`operating_guide` when paired). Keep actual IDs/revisions and repository/base; never invent IDs or reuse another chat's approval. An exported `operator_context` needs no CLI probe; paired preparation uses verified MCP tools. Never bypass connection, binding, readiness, revision, digest, or sandbox refusals.

2. Publish the required plan; no model `Orchestrator` is mandatory. Use exposed `save_draft`/`publish` with actual schemas/IDs: fresh command key, `expected_revision: 0`, actual profile/registry revision, one `worker` task with exact instruction/write paths (at most 4096 UTF-8 bytes). Publish the returned ID and revision. Use the CLI only for explicit preview/record.

3. Prepare the exact `OperatorApprovalIntent` for the trusted-local route: replace every `<...>` and both revision zeros with actual positive plan/registry revisions; set every budget to the presented scope. `prepare` compiles the normalized `OperatorApprovalRequest` preview; preparation grants no authority.

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

5. Create a `worker` item and call `backlog_worker_start` once with actual revision, approval and task IDs. Wait for the callback. Follow the [compact closure](MCP-OPERATING-GUIDE.md#compact-ordinary-callback-closure): distinguish chat/control from dispatch-session bindings; validate real page ID/order/limits and event run ID. Events lack worker ID/task authority; validate those at the durable result against the original start receipt and item snapshot. Use only exposed tools and actual fields.

   If composition is available, set finite page/call budgets and a deadline. Sequentially pull/process/ACK advertised pages; false `has_more` ends pagination, then reconcile once via `backlog_worker_result`. Errors, unknown/stale/missing evidence or exhaustion return incomplete state with original mapping/last ACK/cursor. Resume advertised pages if needed. Without composition, use supported calls sequentially. Never poll, redispatch or invent IDs.

   Exact success precedes one bounded physical check: optional `threadlane inspect-delivery`, authenticated root/base and allowed paths. `HEAD` must equal the expected base; explicit small UTF-8 `--text-file` text is untrusted and off by default. Refuse unsafe/out-of-scope/ changing evidence; no authority is granted. An equally bounded physical read may record its limits.

   Separately check item semantics, then complete or reject with `item_snapshot.revision`, original `execution_ref` and checked matching result/delivery digests. No auto-acceptance, parallel dependent mutations or unknown-effect replay. ACK only the real batch ID; ACK is not acceptance. Report immutable `Done` briefly; do not rewrite receipts.
