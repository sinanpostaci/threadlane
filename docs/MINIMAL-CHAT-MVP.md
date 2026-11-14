# Minimal chat-centered Threadlane MVP

**Status: accepted for the qualified Linux/NixOS environment, 2026-11-13.**

This is the current product contract. It supersedes conflicting historical
multi-role requirements while retaining their original evidence. The combined
runtime, minimal desktop, live native Coworker/Worker flow, parallel isolation,
recovery, package and independent NixOS application gates passed. The
[acceptance report](MINIMAL-CHAT-MVP-ACCEPTANCE-2026-11-13.md) identifies the exact
runtime checkpoint, artifact paths, checks, corrected defects and qualification
limits. Publication of this source does not activate a host upgrade.

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
  reserves, reconciles, completes, or rejects them. An acceptance rejection moves
  the item to `Blocked` while preserving its immutable successful technical
  outcome.
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
- guarded Worker reservation, dispatch receipt, result reconciliation,
  completion receipt, and separate acceptance rejection with immutable success
  preservation on the trusted paired route;
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
- `backlog_worker_reject(command_key, item_id, expected_revision, execution_ref, expected_result_digest, expected_delivery_digest, rejection_reason, rejection_evidence)`

This contract's paired facade totals 52 tools; the scoped surface remains 24.

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
exact MCP result, independently verifies that the recorded outcome is exact
technical success, verifies actual physical delivery and the item-specific
acceptance condition, and inspects the matching result and delivery digests. The
sequence is result reconciliation, then independent physical acceptance, then
exactly one of complete or reject. ACK of the real returned event batch is
independent of that decision and is never acceptance.

Only after a positive check may `backlog_worker_complete` record `Done` with the
item's current `expected_revision`, the exact `execution_ref`, the previously
inspected matching `expected_result_digest` and `expected_delivery_digest`, a
bounded acceptance note, and bounded acceptance evidence.

If exact technical success and physical delivery are independently confirmed but
the delivery fails Coworker acceptance, `backlog_worker_reject` separately
records the failed Coworker acceptance with a bounded reason and evidence and
moves the item to `Blocked`. It preserves the immutable technical `Success`; it
does not record execution failure or `Done`, create a human question or
`NeedsYou`, or automatically dispatch another attempt. Missing or mismatched
result or delivery evidence permits neither completion nor rejection: report the
exact uncertainty and keep the attempt unreconciled rather than inventing either
outcome.

An ordinary approved-task callback identifies only `approval_id` (no job, item,
or execution ref); after `approved_pull_events`, recover the actual
item/`current_execution_ref`, reconcile that exact execution, match authority and
dispatch identity before acceptance, verify physical delivery, then complete or
reject at the current revision with matching digests, and ACK only the actual
returned batch. ACK is not completion or rejection. Missing mapping or evidence is
reported precisely, never filled with invented IDs, a success claim, `NeedsYou`,
or a relaunch. A stopped host permits supported read result/history, not history
ACK or a forced restart. Busy and idle native callback handling passed the
qualified live cases; see the acceptance report for artifact and model boundaries.

A rejected attempt cannot complete or record another outcome. A
`backlog_worker_result` reread preserves the stored rejection. An exact replay of
the original rejection request returns the original rejection receipt even if a
later attempt now exists. A new attempt after rejection requires fresh authorized
approval/task identity and the item's current revision; old authority must never
be replayed.

Failed and Unknown outcomes cannot become `Done`. An Unknown outcome remains
unknown until reconciled through the original execution; it is not success and
cannot reserve a replacement. After a known failure or a recorded acceptance
rejection, a new attempt requires a fresh approval/task identity and the item's
current revision. Raw outcome import and manual Worker success shortcuts are not
part of this contract. A `Done` replay returns the immutable original completion
receipt rather than creating a new acceptance.

These contracts passed the combined source checks and the specific live
provider/native-callback, browser and packaged NixOS UI gates recorded in the
acceptance report. They do not establish every-provider, every-platform, or
installed-host upgrade acceptance.

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
already exist, but they are not mandatory steps in the new contract. The optional
legacy sections of the shared MCP guide are opt-in only and are not required for
ordinary backlog work. Historical multi-role jobs and pilots remain valid
historical evidence; they do not create a new mandatory stage.

## Implementation status

### Core backlog storage and guarded linkage (validated)

Present in `threadlane-core` in the combined checkout:

- additive transactional tables for items, independent history, and
  binding-scoped backlog idempotency;
- `create_backlog_item`, `get_backlog_item`, `list_backlog_items`,
  `edit_backlog_item`, `block_backlog_item`, `start_backlog_item`,
  `complete_backlog_item`, and `list_backlog_history`;
- guarded Worker reservation, dispatch receipt attachment, terminal-result
  reconciliation, separate acceptance completion or rejection, immutable
  `Done` replay, preserved rejection on reread, and fresh-authority retry rules;
- bounded text, keyset pagination, stable history order, revision checks,
  payload-conflict detection, and rejection-without-side-effects behavior.

Core backlog storage and the trusted-local operator routes passed focused and
combined parent-run validation; see the acceptance report.

### Worker reservation and guarded acceptance (validated)

The combined checkout contains the guarded Worker linkage and paired facade:
reservation against an exact frozen approval/task identity, dispatch receipt
attachment, mutating terminal-result reconciliation, separate Coworker
acceptance completion or rejection, immutable `Done` replay, and explicit
Failed/Unknown handling. A rejection requires exact technical success plus
independently checked physical delivery, blocks the item, and leaves the
successful technical outcome immutable. It reuses the existing runner and
authority rather than creating a second execution model.

The combined guarded facade passed eleven focused tests and all 166 native
library regressions; Clippy passed. Tests cover exact rejection and receipt
replay, changed or missing physical delivery, stale revisions, acceptance
refusal after rejection, and preservation of a newer authorized attempt. The
expanded fake-provider tests also discard an actual serialized MCP start reply,
reconnect through a new enrolled server, and verify the same receipt with one
provider invocation. Missed-notice recovery reads the retained result after a
clean host stop and authority reopen, checks delivery, and records Done. Negative
result/completion requests leave item, history, and host state unchanged. These
fixtures do not themselves prove live native callback delivery.

### Orphaned Ready terminal-history recovery (source contract)

An interrupted host start, reboot, or killed host process can leave a `Ready`
manifest with its old PID and instance after both `host.lock` and
`supervisor.lock` were released. The guarded host client may read the original
terminal result from that state only while holding the existing exclusive
`host.lock`, with `supervisor.lock` demonstrably released, and after revalidating
the exact manifest/instance/PID/gates/session binding, original sealed
task/run/worker receipt, and complete terminal worker evidence before and after
the read.

For that orphaned `Ready` case only, the read uses the existing bounded
`threadlane_core::observation::read_core_state_readonly` snapshot reader so
committed `state.db-wal`/`state.db-shm` sidecars left by the interrupted host are
respected rather than ignored. The snapshot must contain exactly one
binding/thread matching the manifest, exactly one sealed run and worker, a
terminal run status/revision matching the complete `WorkerSnapshot`, and the
original receipt identity. Missing or inconsistent sidecar pairs, SQLite read
errors, and missing, duplicate, malformed, mismatched, or incomplete selected
binding/run/worker evidence are refused. The reader uses SQLite's consistent
committed snapshot; it does not independently validate every WAL/SHM byte.
SQLite may ignore recoverable or uncommitted crash tails and WAL content already
checkpointed into the main database. The reader may update SQLite shared
memory read marks in `-shm`; durable `state.db`, `-wal`, manifest, receipts,
cursors, backlog state, and event batches remain unchanged. It refuses missing,
partial, changed, or nonterminal history, never promotes `Unknown` or `Failed`
to `Success`/`Done`, does not mark the session `Stopped`, rewrite manifest or
receipts, open a writable core, run migrations or recovery, checkpoint, delete
sidecars, dispatch a Worker, send or ACK notifications, or restart the host.
Clean `Stopped` history keeps its strict no-sidecar immutable reader. This is a
source-level read fallback; installed-reboot, live foreign-supervisor, and
non-Linux behavior remain separate unqualified gates, and a controlled fixture
crash is not power-loss proof.

### Desktop stream and read-only liveness (validated)

Ordinary Projects chats render one bottom-growing durable event stream and a
read-only Todo/Doing/Done/Blocked drawer. Current scope uses the complete source,
binding, thread, and canonical-project identity. The reader anchor survives event
head trimming and reflow; Working expires using original sample timestamps.
Backlog failures and awaiting checks do not create a human request.

Desktop host presence uses the static read-only authority/session route without
initializing stores or acquiring write locks. Acquisition retains at most 64
candidates before scheduling, runs at most eight calls concurrently, uses per-call
and overall deadlines, cancels pending work, and retains completed results.

Parent-run combined validation passed formatting, focused behavior and
all-target Clippy, 1,325 Rust workspace tests, the desktop suite, 36 frontend test
files, type checking and production build. The complete 36-check browser gate
passed using production components with explicitly mocked test IPC. The final
packaged app separately passed real Tauri IPC and Wayland/tray acceptance inside
an independent NixOS guest. Drawer opacity and text contrast were measured,
and actual final screenshots inspected. See the acceptance report for precise
counts and test boundaries.

### Live acceptance and qualification limits

The qualified real native route projected new paired chats before work, returned
Worker results to idle and already-active Codex turns, captured actual physical
Worker presence, and required the native Coworker to inspect physical delivery,
record durable Done and ACK the actual batch. Two independent real Workers on
one runtime artifact overlapped and completed in separate chats and worktrees.
The same final packaged desktop showed a new actual native chat before work,
then actual Worker Working and checked Done with cleared live/attention indicators.
Lost replies/notices, stopped-host recovery, duplicates, stale revisions and
cross-chat rejection passed the focused fixture matrix. Fixture recovery and
native guest UI evidence remain distinct from real-provider evidence.

The final CLI/MCP/host/session binaries are byte-identical to the live-qualified
runtime; the final desktop package includes the separately qualified UI fixes.
Astra performed one package-closing review and targeted correction rechecks.
No host activation, Codex restart, second backend, global configuration change,
or non-Linux qualification is claimed. Historical pilots retain their original
scope. The additive backlog tables retain the existing sole-owner Core and
binding/run compatibility; the report does not claim a broad migration acceptance.

## Acceptance gates (qualified Linux/NixOS scope)

Each completed gate below is supported by the dated acceptance report. Source
presence alone was not treated as acceptance.

- [x] Parent-run formatting, focused core/MCP/facade tests, combined workspace
      tests and Clippy pass with checkout-local build directories.
- [x] Guarded reservation, exact-success rejection, immutable Done/success,
      fresh-authority retry rules and unknown-outcome recovery pass.
- [x] One bottom-growing desktop stream and a small backlog drawer replace the
      normal Jobs/Messages/Activity surface; wide/narrow behavior is qualified.
- [x] Actual fresh activity drives Working; only a real open human request
      drives Needs you. Historical failure, Blocked and awaiting-check do not.
- [x] Results persist before notices; lost reply/notice and stopped-host result
      recovery preserve the original invocation without blind redispatch.
- [x] Independent real Workers run in parallel without a mandatory multi-role
      job/controller/closure stage.
- [x] Actual native Coworker idle/busy callback, physical delivery checking,
      durable Done, real event ACK and fresh-before-work projection pass.
- [x] Final Nix packages, real wide/narrow native app and Linux Wayland tray
      acceptance pass without host activation.
- [x] One authorized Astra High package-closing review and targeted fix rechecks
      are complete.

These gates qualify the dated Linux/NixOS MVP, not every provider, untested
platform, compositor, installed-host upgrade or unlimited outage duration.
