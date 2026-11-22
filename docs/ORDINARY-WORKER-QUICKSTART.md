# Ordinary Worker quickstart

No execution before human approval. Existing approved Worker (exact IDs): `backlog_get` pre-edit; `todo`+unstarted+approval->`backlog_worker_start` once (reuse `ordinary_review_scope`), `doing`/unknown->reconcile, Done/terminal->retain; Blocked->original blocker; never patch bound repo; Coworker unchanged.

1. `connect_current_chat`/`operating_guide` `{}` (refetch on resume), refresh `catalog`; use exact canonical paired `project_root` for `repository_path`; in an authorized new empty test workspace initialize the fixture there before preview; keep nested clones/proofs/build products outside.

2. `prepare-task-exact` publishes 1 task (opt `task.required_changed_paths`); include `forecast={component_count:1..32,coupled:bool,uncertain:bool,summary:sanitized shareable <=512 bytes}`; freeze independent `task.difficulty` (no extra classify/wait; profile model, same authority). `save_draft`/`publish` use `prepare-private-exact` (`OperatorApprovalIntent` or v2 `{schema_version:2,intent,task_required_changed_paths}`).

3. Both reject omitted budgets and unknown fields (`max_attempts`/`max_workers`).

```js
const req = {"binding_id":"<B>","thread_uuid":"<T>","repository_path":"<R>",
"base_commit":"<S>","profile_id":"<P>","profile_revision":1,"command_key":"<K>",
"summary":"<S>","task":{"id":"<I>","title":"<T>","instruction":"<I>",
"role":"coder","difficulty":"easy","depends_on":[],"allowed_write_paths":["STATUS.txt"]},
"forecast":{"component_count":1,"coupled":false,"uncertain":false,"summary":"<F>"},
"budgets":{"timeout_ms":90000,"max_line_bytes":16384,"max_stdout_bytes":2097152,
"max_stderr_bytes":100000,"max_events":321}};
const q=s=>"'"+s.replace(/'/g,"'\\''")+"'";
const cmd="printf '%s' "+q(JSON.stringify(req))+"|"+q("<rt>")+" --context "+q("<c>")+" approve prepare-task-exact";
text(JSON.stringify(await tools.exec_command({cmd,sandbox_permissions:"require_escalated",justification:"Preview",yield_time_ms:3e4})));
```

`operator_context`; omit `sandbox_permissions` on loopback. On `transport_unavailable`, run `flow grant-list` (`docs/NATIVE-OPERATOR.md`), request executor access (not approval), retry read once; preserve ambiguity/refusals, stop on denial, never retry unknowns.

Returns `{preview_path,preview}` (`0600`/`0700`), v2 `record_request` if `Some(R)`. Match `preview.request` (`OperatorApprovalRequest`), task/model/effort/exe/budgets/paths/digests; legacy `prepare-*` stay.

4. Consent: `approve record --preview <path> --confirm-subject <digest> --confirm-thread <uuid> --operator-approved` (no start); `record --request` takes `record_request` or `preview.request` (`0600`). Match binaries/store (`Some(R)`=v2).

5. Storage-only `worker` item. Native review: only supported small exact-file text deliveries (every approved write path changes) or `required_changed_paths` (`R<=D<=A`, bounded UTF-8 optionals); before guarded start, preflight behavior tests on scratch inputs; use `ordinary_review_scope` (`source_files`: original tracked source files to preserve even for dependent new output; `[]` only for a standalone new file needing no original source). Larger/unsupported tasks select their supported route up front (`inspect-delivery` or equally bounded physical read; HEAD must equal the expected base). `backlog_worker_start` once. Callback: `ordinary_review_notice`->`ready_for_semantic_review` (`observed_model, observed_effort, attempt_count, base_commit, task_checkout_path`, `read_id`); show semantic_review with changed code; start the prepared test now. reuse ready bytes/hashes (or hash-check copied test input against that read), test independently outside the checkout (`CARGO_TARGET_DIR`), do not requery present facts. Decide semantics independently; `ordinary_review_complete` (original `read_id`) only on accept. Link verified files after Done; repo apply needs separate human auth. Native refusal/missing scope stops (no automatic fallback); no unknown redispatch. After closure, only when comparing, run scoped `session forecast-report --item-id <id>` (never add to ordinary callback closure).
