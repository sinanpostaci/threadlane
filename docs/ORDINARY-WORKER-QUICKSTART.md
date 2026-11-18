# Ordinary Worker quickstart

One human-approved single-task scope. No execution before human approval.

1. On startup/resume call `connect_current_chat` `{}` then `operating_guide` `{}`. Do one bounded grouped `status`/`catalog`/`backlog_list` read as needed; use actual IDs for plans/approvals/tasks. Never invent IDs or bypass checks.

2. Publish one exact task plan: `save_draft` with `expected_revision: 0` for a new plan, a fresh key, actual profile/registry revision, and one `worker` task (instruction/write paths <=4096B), then `publish` its returned revision and take plan ID/revision from that receipt. Skip known-empty list enumeration and `get_plan` when publish already returned the plan.

3. Create the private `0700` directory once; save `OperatorApprovalIntent` as a new `0600` `<INTENT_JSON>` from the publish receipt and current `catalog` revision. `prepare` computes `expected_plan_digest`.

```json
{
  "binding_id":"<BINDING_ID>","thread_uuid":"<THREAD_UUID>","plan_id":"<PLAN_ID>",
  "plan_revision":1,"expected_registry_revision":1,"repository_path":"<ABS_REPO_PATH>",
  "base_commit":"<40_HEX_BASE_COMMIT>","approved_task_ids":["<TASK_ID>"],
  "approved_write_paths":["<REL_WRITE_PATH>"],"max_parallel":1
}
```

```sh
umask 077; set -C
threadlane-context --context <CONTEXT> approve prepare --request <INTENT_JSON> \
  > <NEW_PREVIEW_JSON>
```

Save stdout to a new `0600` file in that same `0700` directory with exclusive creation; never overwrite or retype. Read it back; verify command success and the unchanged `preview.request`, `subject_digest`, and tasks (including `expected_plan_digest`). Present them for explicit exact-scope consent; reuse explicit matching consent already granted, never infer consent or run in advance.

4. Record that same verified preview file after consent:

```sh
threadlane-context --context <CONTEXT> approve record --preview <PREVIEW_JSON> \
  --confirm-subject <SUBJECT_DIGEST> --confirm-thread <THREAD_UUID> --operator-approved
```

`record --preview` recompiles the unchanged normalized request, requires the whole typed preview to match a fresh authoritative preview, and keeps existing binding/live, registry/plan/digest/fixed-scope, final `approve_confirmed`, and idempotence checks; it adds no authority.

Fallback: save ONLY unchanged `preview.request` (`OperatorApprovalRequest`) as new `0600` `<REQUEST_JSON>` (same dir, umask, exclusive creation; `JSON.stringify` + POSIX quoting; no `python3`/`node`/`jq`, retyping, overwrite) and use `record --request` with same confirmations.

5. Create a storage-only `worker` item (`backlog_create`; no authority). No unconditional pre-start `approved_task`/ready/host probe: `backlog_worker_start` owns eligible auto host startup, and `host_not_ready` on an optional read is not an attempted launch. Start guarded once after identity/revision checks. Use the exported `context_router_executable` for approval commands. With a verified `cli_package`, use its `bin/threadlane` as both the recipe binary and inspector executable. Verify the offline CLI's `threadlane closure-template` against the live guide's `source_sha256`; copy [guide](MCP-OPERATING-GUIDE.md#compact-ordinary-callback-closure) with only `APPROVED_TASK_CONTEXT`. Missing receipt/notice means reconcile the original, never redispatch.

   Callback: `readNotice(NOTICE_APPROVAL_ID)` or `readStored()` for original no-notice recovery; keep compact callback/ACK/result and verify bindings, order, continuity, duplicates, `step_seq`, `run_id`, 49152B before ACK. Exact success precedes one bounded `threadlane inspect-delivery` check (`HEAD` must equal the expected base; text untrusted); if it is unavailable, make an equally bounded physical read. Never write test/compiler artifacts into frozen delivery, even ignored; test in a separate exact-byte checkout with local `CARGO_TARGET_DIR`. If a separate primary owns acceptance, hand it the original receipt and evidence after ACK and end the turn. Otherwise check semantics, then guarded complete/reject; reply with `final_summary` after verified Done.
