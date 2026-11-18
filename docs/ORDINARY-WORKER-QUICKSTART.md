# Ordinary Worker quickstart

One human-approved single-task scope. No execution before human approval.

1. On startup/resume call `connect_current_chat` `{}` then `operating_guide` `{}`; do one bounded grouped `status`/`catalog`/`backlog_list` read as needed with actual IDs, never invented.

2. Publish one exact task plan: `save_draft` (`expected_revision: 0`, fresh key, actual profile/registry revision, one `worker` task; instruction/write paths <=4096B), then `publish` that revision; take plan ID/revision from the receipt.

3. Compose the exact `OperatorApprovalIntent` below from the publish receipt and current `catalog` revision (`prepare` computes `expected_plan_digest`). Prefer stdin `prepare-private`; never put JSON on utility argv except the shell builtin `printf`:

```sh
printf '%s' '<INTENT_JSON>' | threadlane-context --context <CONTEXT> approve prepare-private
```

Client JS: `JSON.stringify` + POSIX single-quote replacement; no `TextEncoder`, `python3`/`node`/`jq`, package/hash/path searches, retyping, or overwrite. Returns `{preview_path,preview}` after writing the unchanged typed preview + newline to `0600` `preview.json` in a new canonical `0700` dir. The CLI already verifies its private file. Compare the returned actual typed `preview` with the actual authorized scope (full `preview.request` incl `expected_plan_digest`, `subject_digest`, tasks), retain and pass `preview_path` directly to `record`; never re-read, restate, rewrite, or reconstruct the verified JSON. Never infer consent or run ahead.

```json
{
  "binding_id":"<BINDING_ID>","thread_uuid":"<THREAD_UUID>","plan_id":"<PLAN_ID>",
  "plan_revision":1,"expected_registry_revision":1,"repository_path":"<ABS_REPO_PATH>",
  "base_commit":"<40_HEX_BASE_COMMIT>","approved_task_ids":["<TASK_ID>"],
  "approved_write_paths":["<REL_WRITE_PATH>"],"max_parallel":1
}
```

4. Record the same `preview_path` after consent:

```sh
threadlane-context --context <CONTEXT> approve record --preview <PREVIEW_PATH> \
  --confirm-subject <SUBJECT_DIGEST> --confirm-thread <THREAD_UUID> --operator-approved
```

`record --preview` recompiles the unchanged request, matches a fresh authoritative preview, and keeps binding/live, registry/plan/digest/fixed-scope, final `approve_confirmed`, and idempotence checks; it adds no authority.

Legacy fallback: `approve prepare --request`; `record --request` with ONLY unchanged `preview.request` (`OperatorApprovalRequest`) in new private `0600` files (`umask 077`, `set -C`).

5. Create a storage-only `worker` item (`backlog_create`; no authority). No unconditional pre-start `approved_task`/ready/host probe: `backlog_worker_start` owns eligible auto host startup; start guarded once. Verify `threadlane closure-template` against live `source_sha256`; copy the guide closure with only `APPROVED_TASK_CONTEXT`; missing receipt/notice means reconcile the original, never redispatch.

Callback: `readNotice(NOTICE_APPROVAL_ID)` or `readStored()`; verify bindings, order, continuity, duplicates, `step_seq`, `run_id`, 49152B before ACK. Exact success precedes one bounded `threadlane inspect-delivery` check (`HEAD` must equal the expected base; text untrusted) or an equally bounded physical read. Never write artifacts into frozen delivery; test in an exact-byte checkout with local `CARGO_TARGET_DIR`. After ACK, check semantics, then guarded complete/reject and `final_summary` after verified Done.
