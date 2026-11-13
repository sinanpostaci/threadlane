# Minimal chat-centered Threadlane MVP

**Status: human-approved product scope, 2026-11-13.**

This document is the current product contract. It supersedes conflicting
multi-role workflow requirements in older planning documents while preserving
their historical pilot and acceptance evidence as historical. The first
implementation slice is core backlog storage plus this contract; it is not the
full MVP and it does not make Worker execution live.

## Human-approved direction

- Native Codex is the Coworker and owns the human conversation.
- Each chat has a small durable backlog with `Todo`, `Doing`, `Done`, and
  `Blocked` items, accessed through MCP.
- The desktop has one bottom-growing conversation stream and a backlog drawer.
- Actual Coworker and Worker liveness is separate from backlog state.
- `NeedsYou` represents actual open human requests. A merely `Blocked` item or a
  result waiting for review is not automatically a human request.
- Existing single-task runner, authority, and isolation are retained. There is
  no mandatory Orchestrator, Reviewer, Closure, Job, or controller role and no
  hidden mandatory job/closure stage in this contract.
- One concrete scope approval covers technical prepare and launch for that
  scope. Threadlane never self-approves, and backlog creation or editing alone
  is not execution authority.
- A result is persisted before a notice is sent. If the notice is lost, the
  result is recovered by reading durable state. Unknown external effects are
  reconciled, never blindly replayed.
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
| `blocked_reason` | Required while `Blocked` and cleared on resume. |
| `current_execution_ref` | Reserved for a future Worker reservation; unused in this slice. |
| `completion_note`, `completion_evidence`, `completed_at_ms` | Bounded Coworker completion attestation. |

Backlog state is committed workflow state, not a process liveness cache. The
model does not store derived `NeedsYou` values, process health, or execution
success booleans.

### Allowed transitions

- `Todo -> Blocked` and `Doing -> Blocked` with a bounded reason.
- Coworker: `Todo -> Doing`, `Blocked -> Doing` when explicitly revision-checked.
- Coworker: `Doing -> Done` only with a bounded completion note and evidence.
- No completion from `Todo` or `Blocked`.
- `Done` is terminal. It cannot be reopened automatically or by an ordinary
  edit, block, start, or second completion.
- Worker items may be created and edited while `Todo`, and blocked with a
  reason. They cannot be started or completed manually and cannot be converted
  to Coworker. Guarded Worker reservation and verified terminal completion are
  the next slice.
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
- bounded text, limits, cursors, and pagination;
- atomic item, history, and deduplication writes;
- preserved legacy binding, run, worker, event, and recovery behavior.

Coworker completion is an **attestation**, not proof of real Worker execution.
It records what the Coworker says was delivered and what evidence it checked. It
does not claim that a process ran, does not accept a caller-provided trusted
outcome, and does not attach a Worker invocation. Worker completion must use a
future guarded path backed by a verified terminal result. The two must not be
presented as equivalent evidence.

A delivery-specific `Done` means the item-specific acceptance condition was
checked, the bounded delivery description was satisfied, and completion evidence
was durably recorded. This slice has no separate acceptance flag or automatic
checker; the Coworker command is the explicit attestation boundary. A future
Worker path must enforce its own verified acceptance gate before recording
completion.

## Roles and approvals

- **Human** sets direction and grants concrete scope approvals. Approval is not
  inferred from backlog edits, worker output, a CLI flag, a model profile, or an
  MCP context.
- **Coworker** is the native Codex conversation. It plans, explains, edits the
  backlog, and attests Coworker delivery. It is not dispatched as a Worker and
  has no Threadlane model-profile lock.
- **Worker** is an existing single-task execution route. A future slice will
  reserve a backlog item, launch within the approved concrete scope, and read a
  verified terminal result before completion.
- **Independent tasks** may execute in parallel when their scopes are
  independent. No controller role is required merely to satisfy this contract.
- **Optional advice** such as shadow profile or model advice is optional. It is
  never mandatory for backlog use or execution approval.

## Reuse versus rewrite

The approved path reuses the existing `threadlane-core` SQLite connection and
sole-owner `Core`, binding scope and authority, single-task runner, worktree
isolation, receipts, and approval ledger. Backlog support is a small additive
module and additive tables, not a new store, process, event bus, or controller
framework. A big-bang rewrite is not required.

Optional advice, planning, or role-flow surfaces may remain available where they
already exist, but they are not mandatory steps in the new contract. Historical
multi-role jobs and pilots remain valid historical evidence; they do not create a
new mandatory stage.

## Three implementation slices

### Slice 1: Core backlog storage and current contract (this slice)

Implemented in `threadlane-core`:

- additive transactional tables for items, independent history, and
  binding-scoped backlog idempotency;
- `create_backlog_item`, `get_backlog_item`, `list_backlog_items`,
  `edit_backlog_item`, `block_backlog_item`, `start_backlog_item`,
  `complete_backlog_item`, and `list_backlog_history`;
- bounded text, keyset pagination, stable history order, revision checks,
  payload-conflict detection, and rejection-without-side-effects behavior.

This slice is **not** execution integration. It does not deliver MCP tools,
desktop UI, Worker reservation, verified Worker completion, liveness, or
`NeedsYou`. It is pending parent-run formatting, Clippy, and behavioral test
validation.

### Slice 2: Worker reservation and guarded completion (open)

Not implemented. Required direction:

- reserve a Worker backlog item atomically against the existing single-task
  runner and approved concrete scope;
- persist the execution reference before launch;
- treat process loss or an unknown effect as unknown until reconciled;
- read a verified terminal result and check the item-specific acceptance
  condition before recording `Done`;
- keep independent tasks parallel-safe and avoid blind replay;
- retain kind immutability and prevent manual Worker success.

### Slice 3: MCP, stream/drawer, liveness, and live acceptance (open)

Not implemented. Required direction:

- expose bounded backlog operations over MCP and render the bottom-growing
  stream plus backlog drawer;
- keep actual Coworker/Worker liveness separate from backlog status;
- derive `NeedsYou` only from actual open human requests;
- persist a result before notice and recover lost notices by result read;
- qualify a real Coworker conversation and Worker path end to end.

## What this slice does not deliver

- No MCP backlog tools, desktop drawer, stream changes, or notification changes.
- No Worker execution, reservation, verified result import, or guarded Worker
  completion.
- No liveness or `NeedsYou` derivation.
- No automatic human-request generation.
- No full MVP, live acceptance, cross-platform support, or release qualification.
- No schema-version bump, broad migration, or recovery-behavior change. The new
  tables are created idempotently by the existing sole-owner `Core` on open, so
  older bindings, runs, workers, events, approvals, and historical state remain
  compatible.

## Open acceptance gates

- [ ] Parent-run `cargo fmt` check, focused core tests, and Clippy pass against
      the combined checkout and its own target directory.
- [ ] Worker reservation, verified terminal read, guarded completion, and unknown
      outcome reconciliation pass focused tests.
- [ ] MCP backlog tools and desktop bottom-growing stream/backlog drawer are
      implemented and reviewed.
- [ ] `NeedsYou` derives only from real open human requests, not from `Blocked`
      or awaiting-review states.
- [ ] Result persistence before notice and notice-loss recovery by result read
      are verified.
- [ ] Independent tasks run in parallel without a global one-Worker cap or
      mandatory multi-role job/closure stage.
- [ ] A live authorized Coworker/Worker acceptance conversation passes on the
      supported Linux/NixOS host.
- [ ] The authorized package-closing review is completed after integrated
      validation.

Historical pilot and multi-role acceptance reports remain evidence only for
their exact prior scope. They are not evidence that this new contract or its
open gates have been delivered.
