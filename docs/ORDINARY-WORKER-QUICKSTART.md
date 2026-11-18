# Ordinary Worker quickstart

One human-approved single-task scope. No execution before human approval.

1. On startup/resume call `connect_current_chat` `{}` then `operating_guide` `{}`. Do one bounded grouped `status`/`catalog`/`backlog_list` read as needed; use actual IDs; never invent IDs.

2. Publish one exact task plan: `save_draft` with `expected_revision: 0`, a fresh key, actual profile/registry revision, and one `worker` task (instruction/write paths <=4096B); `publish` its returned revision and take plan ID/revision from that receipt.

3. Compose the exact `OperatorApprovalIntent` below from the publish receipt and current `catalog` revision (`prepare` computes `expected_plan_digest`). Prefer stdin `prepare-private`; never put JSON on utility argv except the shell builtin `printf`:

```sh
printf '%s' '<INTENT_JSON>' | threadlane-context --context <CONTEXT> approve prepare-private
```

Client JS: `JSON.stringify` + POSIX single-quote replacement (`String.replace(/'/g, "'\\''")`); no `TextEncoder`, `python3`/`node`/`jq`, package/hash/path searches, retyping, or overwrite. Same prepare/live/fixed-scope logic; returns `{preview_path,preview}` after writing the unchanged typed preview + newline as `0600` `preview.json` in a new canonical `0700` dir. Verify path/mode/type/bytes/readback/success and full `preview.request` incl `expected_plan_digest`, `subject_digest`, tasks. Fallback: save the same intent as new `0600` `<INTENT_JSON>` in a private `0700` dir (`umask 077`, `set -C`, exclusive), run `approve prepare --request <INTENT_JSON> > <NEW_PREVIEW_JSON>`, verify the same. Consent, then record the same preview separately; reuse explicit matching consent; never infer or run ahead.

```json
{
  "binding_id":"<BINDING_ID>","thread_uuid":"<THREAD_UUID>","plan_id":"<PLAN_ID>",
  "plan_revision":1,"expected_registry_revision":1,"repository_path":"<ABS_REPO_PATH>",
  "base_commit":"<40_HEX_BASE_COMMIT>","approved_task_ids":["<TASK_ID>"],
  "approved_write_paths":["<REL_WRITE_PATH>"],"max_parallel":1
}
```

4. Record the same verified `preview_path` after consent:

```sh
threadlane-context --context <CONTEXT> approve record --preview <PREVIEW_PATH> \
  --confirm-subject <SUBJECT_DIGEST> --confirm-thread <THREAD_UUID> --operator-approved
```

`record --preview` recompiles the unchanged normalized request, requires the whole typed preview to match a fresh authoritative preview, and keeps existing binding/live, registry/plan/digest/fixed-scope, final `approve_confirmed`, and idempotence checks; it adds no authority.

Fallback: save ONLY unchanged `preview.request` (`OperatorApprovalRequest`) as new `0600` `<REQUEST_JSON>` (same dir, umask, exclusive creation; `JSON.stringify` + POSIX quoting; no `python3`/`node`/`jq`, retyping, overwrite) and use `record --request` with same confirmations.

5. Create a storage-only `worker` item (`backlog_create`; no authority). No unconditional pre-start `approved_task`/ready/host probe: `backlog_worker_start` owns eligible auto host startup, and `host_not_ready` on an optional read is not an attempted launch. Start guarded once after identity/revision checks. Use exported `context_router_executable` for approvals; a verified `cli_package` `bin/threadlane` is recipe binary and inspector. Verify `threadlane closure-template` against the live guide `source_sha256`; copy [guide](MCP-OPERATING-GUIDE.md#compact-ordinary-callback-closure) with only `APPROVED_TASK_CONTEXT`. Missing receipt/notice means reconcile the original, never redispatch.

   Callback: `readNotice(NOTICE_APPROVAL_ID)` or `readStored()` for original no-notice recovery; verify bindings, order, continuity, duplicates, `step_seq`, `run_id`, 49152B before ACK. Exact success precedes one bounded `threadlane inspect-delivery` check (`HEAD` must equal the expected base; text untrusted); if unavailable, make an equally bounded physical read. Never write test/compiler artifacts into frozen delivery; test in a separate exact-byte checkout with local `CARGO_TARGET_DIR`. With a separate primary acceptance owner, hand it the original receipt/evidence after ACK and end the turn; otherwise check semantics, then guarded complete/reject and reply with `final_summary` after verified Done.
