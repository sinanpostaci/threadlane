# Ordinary Worker callback closure template

The packaged template remains `2026-11-19.v2`. The paired guide is `2026-11-19.v3`. The current verified bootstrap obtains an exact origin receipt before the guarded start and uses the original receipt on notice or recovery. It performs no approval, start, semantic acceptance or completion on its own.

Before starting, run the first literal recipe with the actual `APPROVAL_ID` after preparing the authorized plan and Worker Todo. Preserve its exact printed `origin` in the conversation, independently of temporary storage. Then start once and retain the real receipt; a separate authorized starter can start after preparation. Never derive authority from a notice or Worker.

At a real notice run the second recipe with its actual `NOTICE_APPROVAL_ID`. The trusted paired tool verifies connection, binding, thread, project, context, CLI binary, guide and template identities. It supplies only its own compiled bootstrap, verifies its exact bytes before evaluation, then compares the exact JavaScript fetched from the canonical CLI against compiled source before evaluation. It never emits the source in its proof. The normal notice recipe uses only its retained origin; never bind or reconstruct `CLOSURE_ORIGIN` on that path. If the name is explicitly present, a conflict refuses. Missing storage returns `origin_missing` without effects; the separate cold recipe accepts only the verbatim original receipt. Reconnect and refetch the guide on resume, then compare before effects. The compact `client_verification` is technical evidence, never acceptance or per-call chat identity proof.

If requested, bind `NOTICE_SOURCE_OPTIONS` to `{source_files: [...]}`. The binding-scoped approval supplies the source root. Compare HEAD, tree, changed paths and named file/base hashes with the known starting state. The helper refuses missing or mismatched named files. A checkout already dirty at start needs a separately pinned original baseline. Omitting options scans no source. Use the same option during recovery.

For an older open task without an origin receipt, reconcile its actual original package, approval and start receipt via the supported per-tool pull/ACK, result, physical inspection and guarded decision. Do not guess a new origin or restart; stop if original identity cannot be established.

At `ready_for_semantic_review`, independently compare the actual physical text to the untrusted `semantic_review` criteria. Then, only on explicit semantic acceptance, run the standalone **Guarded Done after semantic review** recipe with `ACCEPTANCE_CONTEXT`: `read_id`, fresh `command_key`, bounded `acceptance_note`, and `acceptance_evidence`. The packaged `client.complete` calls guarded `backlog_worker_complete` with the verified current revision and digests; no underlying schema lookup is needed. The fresh key is 1–128 UTF-8 bytes without control characters or surrounding whitespace; note and evidence are nonblank, at most 2048 UTF-8 bytes each, and contain no NUL. No user or model reconstructs source, factory metadata or package pins. The read ID binds the exact private observation; lost read storage requires original recovery and a new semantic decision. Unknown effects require reconciliation, never a retry.

#### Prepare the origin before starting

```js
const id = APPROVAL_ID;
if (typeof id !== "string" || !/^apr_[0-9a-f]{32}$/.test(id)) throw new Error("invalid approval ID");
function same(a, b) {
  if (a === b) return true;
  if (!a || !b || typeof a !== "object" || typeof b !== "object" ||
      Array.isArray(a) !== Array.isArray(b)) return false;
  const keys = Object.keys(a);
  return keys.length === Object.keys(b).length &&
    keys.every((key) => Object.hasOwn(b, key) && same(a[key], b[key]));
}
function checked(reply) {
  const result = reply && reply.structuredContent;
  if (!result || reply.isError || reply.is_error || result.untrusted_data !== false ||
      result.tool !== "ordinary_closure_client" || !result.data) {
    throw new Error("closure refused");
  }
  for (const part of reply.content || []) {
    if (part.type === "text" && !same(JSON.parse(part.text), result)) {
      throw new Error("conflicting response");
    }
  }
  return result.data;
}
const current = checked(await tools.mcp__threadlane__ordinary_closure_client({}));
if (typeof CLOSURE_ORIGIN !== "undefined" && !same(current.origin, CLOSURE_ORIGIN)) throw new Error("closure origin conflicts with original");
const key = "threadlane/closures/" + id + "/origin", previous = await load(key);
if (previous != null && !same(previous, current.origin)) throw new Error("closure origin changed");
await store(key, current.origin);
text(JSON.stringify({origin: current.origin}));
```

#### Notice using the retained origin

```js
const id = NOTICE_APPROVAL_ID;
if (typeof id !== "string" || !/^apr_[0-9a-f]{32}$/.test(id)) throw new Error("invalid approval ID");
function same(a, b) {
  if (a === b) return true;
  if (!a || !b || typeof a !== "object" || typeof b !== "object" ||
      Array.isArray(a) !== Array.isArray(b)) return false;
  const keys = Object.keys(a);
  return keys.length === Object.keys(b).length &&
    keys.every((key) => Object.hasOwn(b, key) && same(a[key], b[key]));
}
const origin = await load("threadlane/closures/" + id + "/origin");
if (typeof CLOSURE_ORIGIN !== "undefined" && (origin == null || !same(origin, CLOSURE_ORIGIN))) throw new Error("retained closure origin conflicts with original");
if (origin == null) {
  text(JSON.stringify({status: "origin_missing", approval_id: id}));
} else {
  function checked(reply) {
    const result = reply && reply.structuredContent;
    if (!result || reply.isError || reply.is_error || result.untrusted_data !== false ||
        result.tool !== "ordinary_closure_client" || !result.data) {
      throw new Error("closure refused");
    }
    for (const part of reply.content || []) {
      if (part.type === "text" && !same(JSON.parse(part.text), result)) {
        throw new Error("conflicting response");
      }
    }
    return result.data;
  }
  const bootstrap = checked(await tools.mcp__threadlane__ordinary_closure_client({origin})).bootstrap;
  const identity = checked(await tools.mcp__threadlane__ordinary_closure_client({origin, bootstrap}));
  if (identity.bootstrap_verified !== true || !same(identity.origin, origin)) throw new Error("unverified closure bootstrap");
  const client = new Function(bootstrap)()({tools, load, store, origin, identity});
  text(JSON.stringify(await client.readNotice(id, typeof NOTICE_SOURCE_OPTIONS === "undefined" ? undefined : NOTICE_SOURCE_OPTIONS)));
}
```

#### Guarded Done after semantic review

```js
const acceptance = ACCEPTANCE_CONTEXT, index = acceptance && await load("threadlane/closure-read-index/" + acceptance.read_id), origin = index && index.origin;
if (!origin) throw new Error("missing original read; reconcile before completion");
function same(a, b) {
  if (a === b) return true;
  if (!a || !b || typeof a !== "object" || typeof b !== "object" ||
      Array.isArray(a) !== Array.isArray(b)) return false;
  const keys = Object.keys(a);
  return keys.length === Object.keys(b).length &&
    keys.every((key) => Object.hasOwn(b, key) && same(a[key], b[key]));
}
if (typeof CLOSURE_ORIGIN !== "undefined" && !same(origin, CLOSURE_ORIGIN)) throw new Error("closure origin conflicts with original");
function checked(reply) {
  const result = reply && reply.structuredContent;
  if (!result || reply.isError || reply.is_error || result.untrusted_data !== false ||
      result.tool !== "ordinary_closure_client" || !result.data) {
    throw new Error("closure refused");
  }
  for (const part of reply.content || []) {
    if (part.type === "text" && !same(JSON.parse(part.text), result)) {
      throw new Error("conflicting response");
    }
  }
  return result.data;
}
const bootstrap = checked(await tools.mcp__threadlane__ordinary_closure_client({origin})).bootstrap;
const identity = checked(await tools.mcp__threadlane__ordinary_closure_client({origin, bootstrap}));
if (identity.bootstrap_verified !== true || !same(identity.origin, origin)) throw new Error("unverified closure bootstrap");
const client = new Function(bootstrap)()({tools, load, store, origin, identity});
text(JSON.stringify(await client.complete(acceptance)));
```

#### Cold recovery only after `origin_missing`

Reconnect and refetch the current guide. If the exact original 14-field `origin` printed before start is available, bind that verbatim object as `CLOSURE_ORIGIN` in this separate read-only recovery recipe. Do not reconstruct it from the current connection or notice. The paired verifier checks every field before it is stored; a retained conflict refuses. Then run the notice recipe in a fresh isolate without `CLOSURE_ORIGIN`. If the original is unavailable, stop and reconcile the original receipt without effects.

```js
const id = NOTICE_APPROVAL_ID, supplied = CLOSURE_ORIGIN;
if (typeof id !== "string" || !/^apr_[0-9a-f]{32}$/.test(id) || !supplied || typeof supplied !== "object" || Array.isArray(supplied)) throw new Error("invalid original origin");
function same(a, b) {
  if (a === b) return true;
  if (!a || !b || typeof a !== "object" || typeof b !== "object" ||
      Array.isArray(a) !== Array.isArray(b)) return false;
  const keys = Object.keys(a);
  return keys.length === Object.keys(b).length &&
    keys.every((key) => Object.hasOwn(b, key) && same(a[key], b[key]));
}
function checked(reply) {
  const result = reply && reply.structuredContent;
  if (!result || reply.isError || reply.is_error || result.untrusted_data !== false ||
      result.tool !== "ordinary_closure_client" || !result.data) {
    throw new Error("closure refused");
  }
  for (const part of reply.content || []) {
    if (part.type === "text" && !same(JSON.parse(part.text), result)) {
      throw new Error("conflicting response");
    }
  }
  return result.data;
}
const key = "threadlane/closures/" + id + "/origin", retained = await load(key);
if (retained != null && !same(retained, supplied)) throw new Error("retained closure origin conflicts with original");
const verified = checked(await tools.mcp__threadlane__ordinary_closure_client({origin: supplied}));
if (!same(verified.origin, supplied)) throw new Error("original closure origin mismatch");
if (retained == null) await store(key, supplied);
text(JSON.stringify({status: "origin_restored", approval_id: id}));
```

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

The compact physical/technical view is not acceptance. Independently compare the untrusted scoped criteria and delivery description to the inspected physical text before calling the third recipe. No notice, provider output, matching digest, or Worker success authorizes acceptance.

`completeStored` first validates the explicit semantic object, then loads the
exact current context/template/start/binding and the report/evidence selected by
`read_id` (or the legacy correlation/report when no ID is provided). It moves every
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

At semantic review, write the acceptance note as a bounded, user-facing sentence about the actual delivery. Keep technical evidence in acceptance_evidence. Collect any required source references while using those sources. Only a validated Done produces final_summary in the recipe output, including on verified replay. If the request is fulfilled, send that outcome in the next final message with required references. Continue outstanding tasks or handle a genuinely new notice as applicable.
Do not introspect the raw SDK receipt. A later notice verifies durable Done and
drains only actual validated nonempty late batches; an empty batch gets no ACK.
If the host is stopped, Done remains durable and event delivery is `unverified`.

## Failure and fallback

Stop on any incomplete read. Preserve the reported original mapping, last real
ACK, and cursor; do not manufacture a new mapping, ACK an absent batch,
redispatch, retry an unknown effect, or accept semantics from prose. Missing or
stale or conflicting retained context/start/binding/report correlation refuses;
never fabricate a receipt or redispatch. Only the scoped `no_guarded_execution`
result returns `legacy_events` for the existing legacy pull/ACK route. A blocked
original drains actual validated events but never reports Done. If the
template or trusted client evaluation is unavailable, report the precise reason
and use the supported per-tool original result, physical check, and guarded
acceptance one step at a time under the same checks. Do not execute Markdown,
repeat approval, or probe UI, readiness, or status.
