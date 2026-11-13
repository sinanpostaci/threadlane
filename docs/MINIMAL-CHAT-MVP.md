# Minimal chat-centered Threadlane MVP

**Status: human-approved product scope, 2026-11-13. Source-level core, runtime,
and guarded-facade work is available in the combined checkout; the full product
is unverified and not delivered.**

This document is the current product contract. It supersedes conflicting
multi-role workflow requirements in older planning documents while preserving
their historical pilot and acceptance evidence as historical. The independent
combined checkout now contains the additive core backlog, paired MCP backlog
tools, and the guarded Worker execution facade. That is source-level
implementation only. The desktop conversation stream and backlog drawer,
liveness and `NeedsYou` derivation, live provider/native-callback acceptance,
final Nix packaging, integrated formatting/Clippy/test validation, and the
package-closing review remain pending. Only the parent records integrated test
evidence.

## Human-approved direction

- Native Codex is the Coworker and owns the human conversation.
- Each chat has a small durable backlog with `Todo`, `Doing`, `Done`, and
  `Blocked` items, accessed through MCP.
- Ordinary new work starts in the backlog. It does not require a `Job`,
  controller, `Orchestrator`, `Architect`, `Reviewer`, or `Closure` stage.
- The desktop has one bottom-growing conversation stream and a backlog drawer.
- Actual Coworker and Worker liveness is separate from backlog state. `Doing` is
  committed workflow state, not proof that a process is running.
- `NeedsYou` represents actual open human requests. A merely `Blocked` item, a
  failed result, or a result waiting for Coworker checking is not automatically a
  human request.
- Existing single-task runner, authority, and isolation are retained. There is no
  mandatory Orchestrator, Reviewer, Closure, Job, or controller role and no
  hidden mandatory job/closure stage in this contract.
- One concrete scope approval covers technical prepare and launch for that
  unchanged scope. Threadlane never self-approves, and backlog creation or
  editing alone is not execution authority.
- The host persists a terminal result before sending a notice. The Coworker then
  reconciles that durable result into backlog evidence with
  `backlog_worker_result`; that call does not send the notice or run before it.
  If the notice is lost, the result is recovered by reading durable state.
  Unknown external effects are reconciled, never blindly replayed.
- `Done` requires checked accepted delivery conditions and durable completion.
- Independent tasks remain parallel-safe. There is no global one-Worker cap;
  one Worker is simply the first test configuration.
- Code, comments, and documentation are English. Agent-authored content stays
  in the user's language.

## Backlog contract

A backlog item keeps only the durable data needed for chat-visible work:

| Field | Meaning |
| --- | --- |
| `item_id`, `binding_id`, `revision` | Scoped identity and monotonic revision. |
| `title` | Bounded short description. |
| `acceptance_condition` | Bounded condition checked before `Done`. |
| `delivery_description` | Bounded description of the intended delivery. |
| `kind` | `Worker` or `Coworker`. |
| `status` | `Todo`, `Doing`, `Done`, or `Blocked`. |
| `blocked_reason` | Required while `Blocked` and cleared by `backlog_start`. |
| `current_execution_ref` | Current guarded Worker execution reference; absent before reservation or for Coworker items. |
| `completion_note`, `completion_evidence`, `completed_at_ms` | Bounded Coworker completion attestation. |

Backlog state is committed workflow state, not a process liveness cache. The
model does not store derived `NeedsYou` values, process health, or execution
success booleans.

### Allowed transitions

- `Todo -> Blocked` and `Doing -> Blocked` with a bounded reason.
- Coworker: `Todo -> Doing` and `Blocked -> Doing` through `backlog_start` at the
  item's current `expected_revision`.
- Coworker: `Doing -> Done` only with a bounded completion note and evidence.
- No completion from `Todo` or `Blocked`.
- `Done` is terminal. It cannot be reopened automatically or by an ordinary
  edit, block, `backlog_start`, or second completion.
- Worker items may be created and edited while `Todo`, and blocked with a
  reason. They cannot be started or completed through the ordinary Coworker
  commands and cannot be converted to Coworker. The guarded Worker facade below
  reserves, reconciles, and completes them.
- Ordinary edits are revision-checked. In `Doing`, only the title may be
  clarified; both the acceptance condition and delivery description are frozen.
  Ordinary edits of `Blocked` or `Done` items are rejected. Final completion
  evidence is immutable.

Every read and mutation is scoped by binding. A reused idempotency key in another
chat is independent; reusing a key with a changed payload in the same chat is a
conflict. An exact retry returns the original result without a new revision or
history entry. Binding validation happens before idempotency replay.

An item, its history entry, and its idempotency record commit atomically. The
backlog history is independent of generic events and runs: creating a `Todo`
never fabricates a `Run`, `Job`, Worker, event, approval, or human request.

## Invariant enforcement versus Coworker attestation

Invariant-enforced behavior:

- binding-scoped reads and writes, including history and idempotency records;
- monotonic revisions with stale-edit rejection;
- allowed status transitions and terminal `Done`;
- immutable item kind and no public status/kind bypass in ordinary edits;
- Worker manual `Doing`/`Done` rejection;
- guarded Worker reservation, dispatch receipt, result reconciliation, and
  completion receipt on the trusted paired route;
- bounded text, limits, cursors, and pagination;
- atomic item, history, and deduplication writes;
- preserved legacy binding, run, worker, event, and recovery behavior.

Coworker completion is an **attestation**, not proof of real Worker execution.
It records what the Coworker says was delivered and what evidence it checked. It
does not claim that a process ran, does not accept a caller-provided trusted
outcome, and does not attach a Worker invocation. Worker completion must use the
guarded path backed by a verified terminal result. The two must not be presented
as equivalent evidence.

A delivery-specific `Done` means the item-specific acceptance condition was
checked, the bounded delivery description was satisfied, and completion evidence
was durably recorded. For Coworker items, the Coworker command is the explicit
attestation boundary. For Worker items, the guarded completion must enforce its
own verified acceptance gate before recording completion.

## Guarded Worker execution contract

The paired, enrolled MCP facade exposes the stable guarded Worker contract:

- `backlog_worker_start(command_key, item_id, expected_revision, approval_id, task_id)`
- `backlog_worker_result(item_id, execution_ref)`
- `backlog_worker_complete(command_key, item_id, expected_revision, execution_ref, expected_result_digest, expected_delivery_digest, acceptance_note, acceptance_evidence)`

`backlog_worker_start` requires real enrollment and the exact frozen
approval/task identity. It records the reservation before any external effect and
returns the software-generated `execution_ref`. A reservation is not proof that a
process is running or that a provider accepted work. A caller must not fabricate a
`run_id`, `worker_id`, approval, task, or execution reference.

`backlog_worker_result` is a mutating evidence-reconciliation operation, not a
pure snapshot. The host durably persists the real terminal result first, then
sends the notice about that result; the Coworker invokes this operation later to
reconcile that durable result into backlog evidence. It does not send the notice
or run before it. A dropped reply or notice is not a failure and is not
permission to redispatch. If the result or status is uncertain, read the original
receipt, backlog item, and history; never blind replay the effect or mint a new
command key to bypass the uncertainty.

A successful result leaves the Worker item in `Doing`. The Coworker reads the
exact MCP result, independently verifies actual physical delivery and the
item-specific acceptance condition, and inspects the matching result and delivery
digests. Only then may `backlog_worker_complete` record `Done` with the item's
current `expected_revision`, the exact `execution_ref`, the previously inspected
matching `expected_result_digest` and `expected_delivery_digest`, a bounded
acceptance note, and bounded acceptance evidence. A callback, pull, read, or ACK
is not acceptance, and ACK alone leaves the item in `Doing`.

An ordinary approved-task callback identifies only `approval_id` (no job, item,
or execution ref); after `approved_pull_events`, recover the actual
item/`current_execution_ref`, reconcile that exact execution, match authority and
dispatch identity before acceptance, verify physical delivery, complete at the
current revision with matching digests, and ACK only the actual returned batch.
ACK is not completion. Missing mapping or evidence is reported precisely, never
filled with invented IDs, a success claim, `NeedsYou`, or a relaunch. A stopped
host permits supported read result/history, not history ACK or a forced restart.
Busy/idle callback handling is intended but not live-qualified.

Failed and Unknown outcomes cannot become `Done`. An Unknown outcome remains
unknown until reconciled through the original execution; it is not success and
cannot reserve a replacement. After a known failure, a new attempt requires a
fresh approval/task identity and the item's current revision. Raw outcome import
and manual Worker success shortcuts are not part of this contract. A `Done`
replay returns the immutable original completion receipt rather than creating a
new acceptance.

These are source-level contracts in the combined checkout. Live provider,
native-callback, UI, and installed-package acceptance are not established by the
source alone.

## Roles and approvals

- **Human** sets direction and grants concrete scope approvals. Approval is not
  inferred from backlog edits, worker output, a CLI flag, a model profile, or an
  MCP context.
- **Coworker** is the native Codex conversation. It plans, explains, edits the
  backlog, attests Coworker delivery, and checks Worker results. It is not
  dispatched as a Worker and has no Threadlane model-profile lock.
- **Worker** is the existing single-task execution route. The guarded facade
  reserves a backlog item, launches within the approved concrete scope, and
  records a verified terminal result before completion. A real Worker authority
  still freezes the selected execution profile and registry revision plus exact
  model/effort/canonical executable pins; only Coworker-only backlog work has no
  execution profile. The optional part is the multi-role stage, not the Worker
  profile.
- **Independent tasks** may execute in parallel when their scopes are
  independent. No controller role is required merely to satisfy this contract.
- **Optional advice** such as shadow profile or model advice is optional. It is
  never mandatory for backlog use or execution approval.

Existing authority still requires a published technical plan record for each
approved task. An operator may save and publish that record directly without a
model `Orchestrator`; publication alone grants no approval. The operator
`prepare`/`record` pair freezes the exact scope, and its `--operator-approved`
flag is an attestation, not authentication or self-approval. One explicit human
authorization of an unchanged concrete scope covers preparation and launch.
Broader authorization inside the stated intent is honored; changed scope,
model, effort, quota, repository, base, or task identity requires a new concrete
preview. Unchanged scope does not need a redundant re-ask.

## Reuse versus rewrite

The approved path reuses the existing `threadlane-core` SQLite connection and
sole-owner `Core`, binding scope and authority, single-task runner, worktree
isolation, receipts, and approval ledger. Backlog support is a small additive
module and additive tables, not a new store, process, event bus, or controller
framework. The guarded Worker facade is an additive trusted-paired boundary over
the same state. A big-bang rewrite is not required.

Optional advice, planning, or role-flow surfaces may remain available where they
already exist, but they are not mandatory steps in the new contract. Historical
multi-role jobs and pilots remain valid historical evidence; they do not create a
new mandatory stage.

## Implementation status

### Core backlog storage and current contract (source available)

Present in `threadlane-core` in the combined checkout:

- additive transactional tables for items, independent history, and
  binding-scoped backlog idempotency;
- `create_backlog_item`, `get_backlog_item`, `list_backlog_items`,
  `edit_backlog_item`, `block_backlog_item`, `start_backlog_item`,
  `complete_backlog_item`, and `list_backlog_history`;
- bounded text, keyset pagination, stable history order, revision checks,
  payload-conflict detection, and rejection-without-side-effects behavior.

This is source-level storage and workflow state. Parent validation remains open.

### Worker reservation and guarded completion (source available, unverified)

The combined checkout contains the guarded Worker linkage and paired facade:
reservation against an exact frozen approval/task identity, dispatch receipt
attachment, mutating terminal-result reconciliation, separate Coworker
acceptance completion, immutable `Done` replay, and explicit Failed/Unknown
handling. It reuses the existing runner and authority rather than creating a
second execution model.

It has not been accepted as live execution. Real provider behavior, native
callback delivery, process-loss recovery, and installed-package behavior remain
unverified.

### MCP, stream/drawer, liveness, and live acceptance (partial source)

The paired MCP backlog tools and guarded Worker facade exist at source level.
The desktop bottom-growing stream and backlog drawer, runtime liveness
projection, `NeedsYou` derivation, and end-to-end live acceptance remain open.
No UI or live-product claim is made here.

## What this source-level foundation does not deliver

- No desktop conversation-stream or backlog-drawer implementation.
- No verified liveness projection or `NeedsYou` derivation from actual open
  human requests.
- No verified live provider execution, native callback delivery, or
  installed-package acceptance.
- No final Nix packaging, cross-platform support, or release qualification.
- No full MVP claim, no package-closing review, and no test evidence recorded by
  this document.
- No schema-version bump, broad migration, or recovery-behavior change. The new
  tables are created idempotently by the existing sole-owner `Core` on open, so
  older bindings, runs, workers, events, approvals, and historical state remain
  compatible.

## Open acceptance gates

Source presence alone satisfies none of these gates. The parent records
integrated test evidence; the boxes remain open.

- [ ] Parent-run `cargo fmt` check, focused core/MCP/facade tests, and Clippy
      pass against the combined checkout and its own target directory.
- [ ] Guarded reservation, mutating result reconciliation, verified completion,
      and unknown-outcome recovery pass focused tests.
- [ ] Desktop bottom-growing stream and backlog drawer are implemented and
      reviewed.
- [ ] `NeedsYou` derives only from real open human requests, not from `Blocked`
      or awaiting-review states.
- [ ] Host result persistence before notice, later `backlog_worker_result`
      reconciliation, and notice-loss recovery are verified.
- [ ] Independent tasks run in parallel without a global one-Worker cap or
      mandatory multi-role job/closure stage.
- [ ] A live authorized Coworker/Worker acceptance conversation passes on the
      supported Linux/NixOS host, including approved-task callbacks in busy and
      idle turns.
- [ ] The authorized package-closing review is completed after integrated
      validation.

Historical pilot and multi-role acceptance reports remain evidence only for
their exact prior scope. They are not evidence that this new contract or its
open gates have been delivered.
