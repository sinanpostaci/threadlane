# Ordinary Worker quickstart

No execution before human approval. Existing approved Worker item/correction (exact IDs, no title match): `backlog_get` pre-edit; `todo`+unstarted+approval -> `backlog_worker_start` once (reuse `ordinary_review_scope`), `doing`/unknown -> reconcile, Done/terminal execution -> retain; pre-start Blocked -> check the original blocker; never patch bound repo for item fix/test; Coworker unchanged.

1. Call `connect_current_chat` `{}`, `operating_guide` `{}` (refetch on resume); refresh uncertain state/`catalog`. Use exact canonical paired `project_root` for `repository_path`; never guess IDs. In an authorized new empty test workspace initialize the fixture there before preview; keep nested clones/proofs/build products outside.

2. `prepare-task-exact` publishes one task (opt `task.required_changed_paths`). `save_draft`/`publish` plans use `prepare-private-exact` (`OperatorApprovalIntent` or v2 `{schema_version:2,intent,task_required_changed_paths}`).

3. Use live scope; both reject omitted budgets and unknown fields (`max_attempts`/`max_workers`).

```js
const req = {
 "binding_id":"<B>","thread_uuid":"<T>","repository_path":"<ROOT>",
 "base_commit":"<SHA>","profile_id":"<P>","profile_revision":1,
 "command_key":"<K>","summary":"<S>","task":{"id":"<ID>","title":"<TI>",
 "instruction":"<IN>","role":"coder","difficulty":"easy","depends_on":[],
 "allowed_write_paths":["STATUS.txt"]},"budgets":{"timeout_ms":90000,
 "max_line_bytes":16384,"max_stdout_bytes":2097152,"max_stderr_bytes":100000,
 "max_events":321}
};
const q = s => "'" + s.replace(/'/g, "'\\''") + "'";
const cmd = "printf '%s' " + q(JSON.stringify(req)) + " | " + q("<router>") +
 " --context " + q("<ctx>") + " approve prepare-task-exact";
const r = await tools.exec_command({cmd, sandbox_permissions:"require_escalated",
 justification:"Allow supervisor preview?", yield_time_ms:30000});
text(JSON.stringify(r));
```

Paths: `operator_context` router/context; omit `sandbox_permissions` on working loopback. On `transport_unavailable` (ready context), run `flow grant-list` (`docs/NATIVE-OPERATOR.md`), request executor access (not approval), retry read-only once; preserve ambiguity/refusals, stop on denial, never retry unknowns.

Returns `{preview_path,preview}` (`0600`/`0700`); adds v2 `record_request` only for `Some(R)` (`None` unchanged). Match `preview.request` (`OperatorApprovalRequest`), task/model/effort/exe/budgets/paths/digests; legacy `prepare-task`/`prepare-private`/`prepare --request` stay.

4. After consent: `approve record --preview <path> --confirm-subject <digest> --confirm-thread <uuid> --operator-approved` (`None`/`Some(R)`; starts no Worker); `record --request` takes `record_request` (`Some(R)`) or `preview.request` (`0600`). Keep matched binaries on one store (`Some(R)` needs v2).

5. Create a storage-only `worker` item. Native review: only supported small exact-file text deliveries (every approved write path changes) or `required_changed_paths` (`R<=D<=A`, no loosening/out-of-scope edits; changed optionals checked as bounded UTF-8); before guarded start, `ordinary_review_scope` (`source_files`: original tracked source files to preserve even for dependent new output; `[]` only for a standalone new file needing no original source). Larger/unsupported tasks select their supported route up front (`inspect-delivery` or equally bounded physical read; HEAD must equal the expected base). `backlog_worker_start` once. On callback, `ordinary_review_notice` -> `ready_for_semantic_review` (`observed_model, observed_effort, attempt_count, base_commit, task_checkout_path`, `read_id`); reuse ready bytes/hashes (or hash-check copied test input against that read), test independently outside the checkout (own `CARGO_TARGET_DIR`), do not requery present facts. Decide semantics independently; `ordinary_review_complete` (original `read_id`) only on accept. Link verified task files after Done; applying to a named repo needs separate human authorization. Native refusal/missing scope stops (no automatic fallback); never redispatch unknowns.
