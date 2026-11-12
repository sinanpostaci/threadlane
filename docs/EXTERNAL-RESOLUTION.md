# External resolution receipts

`external_resolution_receipts_v1` preserves operator-attested evidence that an
existing job or legacy approved plan was resolved outside the automatic
controller. Receipts are appended to the existing per-binding private
`CoworkerBindingLedger::external_resolutions` array; there is no parallel job,
authority, or receipt store.

A receipt is historical evidence only. It is **not** review, integration,
`Closure`, dependency-seal, task-acceptance, or execution authority. Recording a
receipt never mutates job phase/revision, task stages/results, holds, approvals,
controller envelopes, host state, or Git/provider state.

## Supported trusted-local calls

All calls use the normal fixed `threadlane-flow` scope flags. When routed through
`threadlane-context`, the router injects the validated canonical `project_root`
and rejects caller-supplied scope overrides, including `--project-root`,
`--binding`, `--thread`, store paths, `--cache-dir`, and `--context-id`.

```sh
threadlane-flow \
  --state-dir /private/runtime --registry-dir /private/registry \
  --plans-dir /private/plans --authority-dir /private/authority \
  --sessions-dir /private/sessions --binding BINDING_ID --thread THREAD_UUID \
  external-resolution preview --request /private/request.json
```

`preview` is pure and read-only. It validates the request against the live
workflow/authority/plan/registry stores, proves the target project, binding,
thread, job or uniquely linked legacy plan, and task scope, and returns the
normalized `preview_digest`. It creates no journal record, lockfile, cache,
temporary file, or `fsync`, and starts no Git, provider, host, or worker effect.
Preview and inspect are intentionally excluded from both `threadlane-flow` and
`threadlane-context` observation journaling, including inherited observation
environments; the receipt ledger is the activity source.

```sh
threadlane-flow ... external-resolution record \
  --request /private/request.json \
  --confirm-thread THREAD_UUID \
  --confirm-preview PREVIEW_SHA256 \
  --operator-approved
```

`record` requires all three independent CLI confirmations. JSON cannot supply
attestation, thread, or preview confirmation; `--confirm-thread` must match the
fixed CLI thread and the request `thread_uuid`, and `--confirm-preview` must be a
64-character lowercase SHA-256 that the service matches against the exact
recomputed preview digest. A wrong chat/thread/project/task/digest, missing flag,
self-approving JSON, or hostile flag is refused before persistence. An exact
duplicate record returns the existing receipt when durability is already
witnessed; it does not create a second journal, lock, cache, or receipt.

```sh
threadlane-flow ... external-resolution inspect
threadlane-flow ... external-resolution inspect --request-key ORIGINAL_KEY
```

`inspect` is pure. It reports each preserved receipt with its current
`durability` and logical `applicability`. Logical applicability is a read-only
classification against live workflow/authority/plan/runtime state (applicable or
stale with reasons); local durability is only whether this process has witnessed
the file-and-directory `fsync`. The two are independent: a stale logical
classification does not delete history or reopen covered dispatch, and a local
durability uncertainty does not make the receipt logically absent. Inspection
never syncs, repairs, or writes bytes. A restarted process may report an unchanged
receipt as `persistence_uncertain` until an explicit existing-byte reconcile
establishes a local `fsync` witness.

```sh
threadlane-flow ... external-resolution record \
  --request /private/original-request.json \
  --confirm-thread THREAD_UUID \
  --confirm-preview ORIGINAL_PREVIEW_SHA256 \
  --operator-approved --reconcile-existing
```

After inspect reports uncertainty, `record --reconcile-existing` accepts only
the original `request_key` and original preview digest. It syncs the existing
unchanged ledger file and directory without rewriting bytes, advancing
`ledger_revision`, creating a missing receipt, changing consent/revisions, or
replaying a provider/Git/host effect. A missing or conflicting receipt is
refused. `PersistenceUncertain` and unknown outcomes are never reported as
success, and automatic reconcile or replay with a new key is not supported.

## Dispatch and advancement guard

The same read-only receipt predicate is enforced at all real effect boundaries:

- direct `threadlane-session dispatch` and paired MCP `start_approved_task`,
  through the shared conversation-advancement dispatch guard;
- `preview_execution` and `start_execution`;
- code-driver `ensure` and `run-owner`;
- controller admission and every controller step;
- `CoworkerWorkflowStore::reserve_task_outbox`, including attempt, review,
  integration, correction, and dependency-preparation reservations.

Direct dispatch rechecks coverage while holding the per-binding conversation
advancement lock used by receipt recording. Controller and driver boundaries
hold physical job ownership. Outbox reservation checks the immutable ledger under
the innermost workflow ledger lock. This prevents a record/dispatch race from
placing a new effect past a receipt that already covers the work without
inverting the existing ownership, advancement, host, controller, plan, or ledger
lock order. Read-only status, history, inspect, and narrow cancellation remain
available.

A `full_job` receipt closes job advancement: controller admission, execution
start, owner ensure/run, and every covered task dispatch are refused. A
task-scoped receipt blocks only its covered task IDs; unrelated same-job work
may continue. Staleness caused by unrelated same-job progress does not silently
reopen a covered task, and an uncertain local `fsync` witness is never treated as
absence of the receipt. If a partial external result cannot be safely consumed
by the approved DAG, the supported outcome is an explicit replan and fresh
operator reapproval; there is no new continuation framework, implicit dependency
seal, or automatic acceptance.

## Compatibility and operational limits

Older MCP readers reject a non-empty `external_resolutions` array. Real record
and reopen checks therefore use the exact new package with a disposable,
isolated private store; do not point them at an original binding whose ledger
must remain empty or omitted for an older reader. The original binding must stay
empty/omitted in compatibility tests. This is not a claim of live or
bidirectional compatibility with older readers, and it requires no Codex restart
or global configuration change.

Temporary preview/reconcile artifacts belong only in an existing ignored
`.local` directory or a private artifact directory outside the repository; do
not share caches between checkouts. Provider, host, Git, Closure, and live native
chat behavior remain separate qualifications.
