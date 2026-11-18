# Ordinary Worker callback closure template

Version: `2026-11-16.v4`.

Use this audited client composition for an ordinary approved-task
callback. It adds no MCP tool, endpoint, controller, backend guard, or execution
authority. Supported one-step tool calls remain the fallback.

The trusted packaged source is
`skills/threadlane/references/ordinary-closure.js`. Run
`threadlane closure-template` once from the already verified canonical package
and match its version and source digest to the live guide's `template_version` and `source_sha256`. Its offline JSON document contains schema/version metadata, the exact source
SHA-256, the exported `createOrdinaryClosure` factory name, and the packaged
JavaScript source (which also exports `createOrdinaryClosureClient`). Prepare and
retain that serializable capsule **before**
`backlog_worker_start`; do not print the JavaScript. Do not add extra identity,
UI, or readiness probes. The real client may evaluate only this immutable trusted
source, never event, provider, notification, or other untrusted files or text.
Do not fetch, rewrite, or concatenate template source from an event.

The capsule preserves the legacy `createOrdinaryClosure` `read`/`doneView`
contract and adds a small `createOrdinaryClosureClient` serialized-state helper.
The helper accepts only explicit `load`, `store`, `pull`, `ack`, `result`, and
`inspect` callbacks plus an optional `complete` callback. It has no unrestricted
tools, ambient filesystem, network, environment, subprocess, dynamic loading, or
evaluation access. After verified Done, later reads keep their own report and raw evidence under `threadlane/closure-after-done/<execution_ref>/<sequence>`.
It exposes `readNotice(actualApprovalId)`, original-execution recovery via `readStored()`, and `completeStored(acceptance)`;
it has no start, reject, or automatic acceptance operation.

## functions.exec recipe

The recipes below use the exposed functions client directly. COPY each literal recipe and bind only
its one typed object (`APPROVED_TASK_CONTEXT` before the start, `ACCEPTANCE_CONTEXT` after the
separate semantic decision); bind the actual notice approval ID at callback. Never parse, compile, or evaluate
Markdown or fences, and do not repeatedly inspect declarations. `tools.exec_command` returns
`{exit_code, output, session_id?}`; `text(value)` and `store(key, value)` return undefined;
`load(key)` returns serialized data in a later fresh isolate. Ambient logging is unavailable, and
`store` cannot retain functions. Context is data, not execution authority; enrollment, frozen
approval, and server guards remain mandatory.

Create the storage-only `worker` backlog item with `backlog_create` before the prep recipe;
creation stores bounded intent and grants no execution authority. Its current `item_id` and
positive safe `expected_revision`, plus the frozen approval/task authority, supply every start
argument. Prepare and privately retain the capsule **before** starting the Worker. Invoke the actual
verified canonical binary by its POSIX-quoted path, never a PATH lookup. The command must exit `0`,
leave no live session, be untruncated, and return enough output for the packaged JavaScript
source:

```js
// functions.exec prep: COPY literally; bind the one typed input object.
const input = APPROVED_TASK_CONTEXT;
const a = input && input.start_args, s = input && input.scope;
if (!input || typeof input.binary !== "string" || input.binary.charAt(0) !== "/" ||
    typeof input.expected_source_sha256 !== "string" ||
    !/^[0-9a-f]{64}$/.test(input.expected_source_sha256) || !a || !s ||
    typeof s.base_commit !== "string" || !/^[0-9a-f]{40}$/.test(s.base_commit) ||
    typeof s.requested_model !== "string" || s.requested_model.length === 0 ||
    typeof s.requested_effort !== "string" || s.requested_effort.length === 0 ||
    !Array.isArray(s.write_paths) || s.write_paths.length === 0 ||
    !s.write_paths.every((p) => typeof p === "string" && p.length > 0 &&
      !p.includes("\u0000") && !p.includes("\\") && p.charAt(0) !== "/" &&
      p.split("/").every((segment) => segment && segment !== "." && segment !== "..")) ||
    !s.inspector || typeof s.inspector.executable !== "string" ||
    s.inspector.executable.charAt(0) !== "/" ||
    !Array.isArray(s.inspector.argv) || s.inspector.argv.length !== 1 ||
    s.inspector.argv[0] !== "inspect-delivery" ||
    !Number.isSafeInteger(a.expected_revision) || a.expected_revision <= 0 ||
    [a.command_key, a.item_id, a.approval_id, a.task_id]
      .some((v) => typeof v !== "string" || v.length === 0)) {
  throw new Error("invalid approved task context");
}
await store("threadlane/approved-task-context.json", input);
const quote = (value) => "'" + String(value).replace(/'/g, "'\\''") + "'";
const result = await tools.exec_command({
  cmd: quote(input.binary) + " 'closure-template'",
  max_output_tokens: 32000, yield_time_ms: 30000,
});
if (result.exit_code !== 0 || result.session_id ||
    result.output_truncated === true || result.truncated === true ||
    typeof result.output !== "string") throw new Error("canonical closure-template failed");
const template = JSON.parse(result.output);
if (template.tool !== "threadlane closure-template" ||
    template.version !== "2026-11-16.v4" ||
    template.source_path !== "skills/threadlane/references/ordinary-closure.js" ||
    template.factory_name !== "createOrdinaryClosure" ||
    template.source_sha256 !== input.expected_source_sha256 ||
    typeof template.javascript !== "string") throw new Error("untrusted closure template");
await store("threadlane/closure-template.json", template);
const started = await tools.mcp__threadlane__backlog_worker_start(a);
if (!started || typeof started !== "object") throw new Error("missing guarded start reply; use original-receipt fallback");
await store("threadlane/start-receipt.json", started);
await store("threadlane/start-binding.json", {input, start: started});
text(JSON.stringify({version: template.version, source_sha256: template.source_sha256}));
```

Prep stores the exact returned start DTO and its additive prep-owned binding of that input and DTO
only after the guarded start returns. A failed exec, thrown start, or missing reply leaves the prior
receipt and binding unchanged. Callback and complete require that stored binding to equal the current
retained input and raw start before any pull, ACK, result, inspection, or completion; they never mint
or repair it. The binding is data, never authority; a missing, changed, or partial binding refuses.

Each callback runs in a fresh V8 isolate. Recreate the factory from the retained
capsule; never evaluate event, provider, notification, or file text. The
metadata check and the exact version marker keep evaluation on the verified
packaged source; compare the digest/version/path/factory values with the
canonical package output you verified before the Worker started. The read wrapper
only loads the capsule/context and evaluates that trusted source; the helper
reloads and validates the exact current context, raw start, and prep binding
again before any effect. Store the full private evidence immediately on receipt.
Text only the compact view:

```js
// functions.exec callback: bind NOTICE_APPROVAL_ID from the actual notice.
const input = load("threadlane/approved-task-context.json");
if (!input) throw new Error("missing or changed retained context/start/binding; use original-receipt fallback");
const template = load("threadlane/closure-template.json");
if (!template || template.tool !== "threadlane closure-template" ||
    template.version !== "2026-11-16.v4" ||
    template.source_path !== "skills/threadlane/references/ordinary-closure.js" ||
    template.factory_name !== "createOrdinaryClosure" ||
    !/^[0-9a-f]{64}$/.test(template.source_sha256) ||
    template.source_sha256 !== input.expected_source_sha256 ||
    typeof template.javascript !== "string" ||
    template.javascript.indexOf('var CREATE_ORDINARY_CLOSURE_VERSION = "2026-11-16.v4";') === -1) {
  throw new Error("invalid closure capsule");
}
const createOrdinaryClosureClient = new Function(template.javascript + "\nreturn createOrdinaryClosureClient;")();
const quote = (value) => "'" + String(value).replace(/'/g, "'\\''") + "'";
const client = createOrdinaryClosureClient({
  load: (key) => load(key), store: (key, value) => store(key, value),
  pull: (a) => tools.mcp__threadlane__approved_pull_events(a),
  ack: (a) => tools.mcp__threadlane__approved_ack_events(a),
  result: (a) => tools.mcp__threadlane__backlog_worker_result(a),
  inspect: (r) => tools.exec_command({
    cmd: r.argv.map(quote).join(" "), max_output_tokens: 16000, yield_time_ms: 30000
  }),
});
text(JSON.stringify(await client.readNotice(NOTICE_APPROVAL_ID)));
```

`readStored()` loads the exact current context, raw SDK start, template capsule,
and prep-owned `{input, start}` binding. Any missing, stale, mixed, partial, or
failed-prep state refuses before a pull, ACK, result, inspection, or store. It
then delegates to the unchanged legacy `read` with fixed finite budgets
`{max_pages: 8, max_calls: 16, deadline_ms: 60000}`. The legacy composition retains
the full actual start/page/ACK/result/inspect DTOs in private evidence, while the
helper stores:

- `threadlane/closure-evidence.json`: the full raw evidence;
- `threadlane/closure-report.json`: the full legacy report;
- `threadlane/closure-read-correlation.json`: `{input, start, report}`.

The returned compact view keeps the current mapping, scope, result, batches,
technical checks, timing, and the exact physical file text required for the
separate semantic review. It replaces the repeated event array with verified
event counts, the first/last sequence, important count, and the last verified ACK
cursor, and adds the actual validated source SHA-256. It never hides required
physical content and never claims ready when the legacy read reported incomplete,
refused, oversized, or omitted material.

The factory itself uses only standard language builtins. `read` is one finite
closure composition; `evidence` retains the full actual start/page/ACK/result/
inspect DTOs for private storage, while the compact view is the bounded printable
form. The callback first requires the prep-owned start binding to match the
retained input and raw start, then stores the exact read context/start/report
correlation beside the report as private serialized data; it grants no authority.
A successful status is only `ready_for_semantic_review`; it never grants
completion, rejection, acceptance, or authority.

## Read behavior and corrections

`read` derives approval, task, item, execution, control binding, dispatch
session, run, worker, and profile identities from the actual start execution
authority and dispatch receipt. The outer/control binding and dispatch session
binding are checked separately. It also validates the expected tool and every
supported outer binding, including the Done receipt in `doneView`.

Publication and event handling fail closed on all of the following:

- explicit transport `isError: true` anywhere, including pull/ACK/result/inspect;
- a response with neither a valid structured envelope nor valid JSON text, or
  conflicting structured/text copies, including conflicting outer control
  bindings or tools;
- a skipped or repeated page range, a page `acked_seq` that differs from the
  retained verified ACK cursor, or a non-empty batch that does not start exactly
  at the inclusive `from_seq` after that cursor. A valid empty batch at the
  retained cursor, including a harmless repeated/duplicate empty batch, remains
  processable;
- an inner `EventBatch` serialized JSON larger than 49152 UTF-8 bytes, measured
  on the actual returned `batch` object before ACK; duplicate structured/text
  copies of the larger outer result do not change that inner check;
- a duplicate `event_id` within a page or across pages;
- a missing, non-numeric, negative, or unsafe-integer `event.step_seq`; zero is
  valid and every event is checked before ACK and before bounded printing;
- any changed physical path that is not in the frozen write-path set, even when
  the inspector itself reports `allowed: true`.

Each actual start/page/ACK/result/inspect DTO is retained in private evidence
immediately after the callback returns and before the returned value is
validated or transformed. Malformed or unknown ACK data, a callback failure, or
a conflicting envelope keeps the exact original mapping, the actual pending
batch, and the last verified ACK/cursor; nothing is retried, invented, or
converted into a result. A late receipt is still retained, but no new effect is
started after expiry and a late closure never reports `ready_for_semantic_review`.

The deadline is checked before and after every awaited callback, including the
final inspection. The template does not race or cancel an effect, and it never
treats a timeout as proof that no effect occurred.

The physical inspector result accepts the actual `exec_command` wrapper shape
`{exit_code, output, session_id?, ...}`. It requires exact exit `0`, no live
`session_id`, no truncation/omitted-output flag, and complete JSON output.
`running: true`, a live session, a non-zero exit, or truncated/omitted output
stops incomplete without reusing any printed JSON as a report. The inspection
request keeps `argv` as the no-shell form and `command` as the correctly quoted
display/fallback form, with the exact inspector root/base/allowed/text paths.

The read pipeline is:

- pull sequentially with an explicit page limit, ACK budget, call budget, and
  deadline;
- validate every actual batch ID, sequence range and continuity, contiguous
  event order, event binding, event ID uniqueness, and event `run_id`;
- ACK each actual returned `batch_id` and verify the ACK cursor/identity;
- continue only while the returned `has_more` is exactly `true`; `false` ends
  pagination and no further pull is made;
- call `backlog_worker_result` once, matching approval/task/run/worker/session/
  item/execution and current `item_snapshot` revision/ref;
- require exact `success`, exit `0`, requested and observed model/effort pins,
  and all missing/failure/cleanup/truncation flags false;
- require the result execution to preserve the start execution's
  `acceptance_condition_digest`, `delivery_description_digest`,
  `reservation_revision`, and `reserved_at_ms` exactly, before inspection; the
  item snapshot revision is a different mutable fact;
- retain and compare the optional dispatch `execution_binding_digest` plus the
  original authority subject/task digests;
- require the result and physical delivery digests to agree, and the delivery
  approval/task/subject/task-digest/binding/session/run/worker/profile/base to
  match;
- only then invoke the existing bounded `threadlane inspect-delivery` once on
  the actual `worker_evidence.task_worktree_path`, with exact write paths as
  allowed and text files. Inspected text is never executed.

A refusal, missing/Unknown/stale/changed identity, budget or deadline
exhaustion, or a still-running physical command returns `status: "incomplete"`
with the original mapping, last real ACK, cursor, and pending batch when
present. No result, inspection, retry, start, poll, acceptance, or later effect
follows. `readStored` exposes no complete, reject, start, or accept operation.

The compact view contains checked IDs, model/timing facts, current revision
and result/delivery digests, verified event counts/ACK cursor, and the actual
bounded untrusted physical file text. Full raw DTOs and page bodies stay only in
the separately stored evidence. Do not print raw provider logs, event payload
prose, or the full evidence file into the human report.

## Separate semantic decision and guarded completion

The compact view is physical/technical evidence, not acceptance. The Coworker must
separately check the item-specific acceptance condition against that view and
the untrusted physical text. Then, and only then, use `completeStored` with one
real typed semantic `ACCEPTANCE_CONTEXT` carrying the actual fresh `command_key`,
bounded `acceptance_note`, and `acceptance_evidence`. Copy the literal recipe
below in a new isolate after that separate decision:

```js
// functions.exec complete: NEW isolate after the separate Coworker semantic decision.
// COPY literally and bind one typed acceptance object.
const acceptance = ACCEPTANCE_CONTEXT;
const input = load("threadlane/approved-task-context.json");
if (!input) throw new Error("missing or changed retained context/start/binding; use original-receipt fallback");
const template = load("threadlane/closure-template.json");
if (!template || template.tool !== "threadlane closure-template" ||
    template.version !== "2026-11-16.v4" ||
    template.source_path !== "skills/threadlane/references/ordinary-closure.js" ||
    template.factory_name !== "createOrdinaryClosure" ||
    !/^[0-9a-f]{64}$/.test(template.source_sha256) ||
    template.source_sha256 !== input.expected_source_sha256 ||
    typeof template.javascript !== "string" ||
    template.javascript.indexOf('var CREATE_ORDINARY_CLOSURE_VERSION = "2026-11-16.v4";') === -1) {
  throw new Error("invalid closure capsule");
}
const createOrdinaryClosureClient = new Function(template.javascript + "\nreturn createOrdinaryClosureClient;")();
const unused = () => { throw new Error("callback not used"); };
const client = createOrdinaryClosureClient({
  load: (key) => load(key), store: (key, value) => store(key, value),
  pull: unused, ack: unused, result: unused, inspect: unused,
  complete: (a) => tools.mcp__threadlane__backlog_worker_complete(a),
});
text(JSON.stringify(await client.completeStored(acceptance)));
```

`completeStored` first validates the explicit semantic object, then loads the
exact current context/template/start/binding/correlation/report. It moves every
current literal check inside the helper before an effect: capsule metadata,
`{input, start}` binding, `{input, start, report}` correlation, ready status,
current item/execution identity, positive revision, and result/delivery digest
presence. It derives the guarded completion arguments solely from that verified
report. Missing, stale, mixed, partial, or failed-prep state refuses before any
completion callback.

Before the possible effect the helper stores
`threadlane/closure-completion-intent.json` with the source identity, acceptance
data, exact derived args, and raw start. It then invokes the existing guarded
`backlog_worker_complete` exactly once, retains the exact returned SDK receipt
immediately in `threadlane/closure-done-receipt.json`, and formats it through the
legacy pure `doneView`. A successful validated view is also retained privately in
`threadlane/closure-done-view.json`. A missing, thrown, error, or unknown reply
never retries and never fabricates `Done`; the retained intent makes a later call
refuse rather than repeat the effect.

A matching already validated retained Done may be formatted purely from the
retained receipt and current report without another pull, ACK, result,
inspection, or completion call. A changed current attempt, changed acceptance,
changed start, corrupt receipt, corrupt retained view, or new attempt cannot reuse
that result and refuses instead. `doneView` verifies the successful Done
receipt's expected tool, outer control binding, authority approval/task/subject/
task-digest, item/execution/run/worker/session IDs, current revision, completion
timestamps, the four immutable reservation fields, the optional dispatch
`execution_binding_digest`, and immutable result/delivery digests against the read
report. Missing or conflicting structured/text envelopes or transport errors fail
closed. It returns only bounded status/item/revision/time fields, never the full
receipt. If it reports `incompatible`, do not claim `Done`.

After `doneView` reports `Done`, finish with an immediate short final report.
Do not introspect the raw SDK receipt or repeat pull/ACK for the original notice
already processed by the read. An `approval_id` alone does not prove duplication;
a genuinely new notice still needs supported actual recovery.

## Failure and fallback

Stop on any incomplete read. Preserve the reported original mapping, last real
ACK, and cursor; do not manufacture a new mapping, ACK an absent batch,
redispatch, retry an unknown effect, or accept semantics from prose. Missing or
stale retained context/start/binding/report correlation uses the supported
original-receipt fallback; never fabricate a receipt or redispatch. If the
template or trusted client evaluation is unavailable, report the precise reason
and use the supported per-tool original result, physical check, and guarded
acceptance one step at a time under the same checks. Do not execute Markdown,
repeat approval, or probe UI, readiness, or status.
