# Crash recovery and persisted task state

Threadlane persists task state as it changes. It does not depend on keeping the
desktop window or a model's context alive to remember the approved plan.

## What is persisted

- Worker admission, claims, lifecycle transitions, bounded progress and terminal
  outcomes use the supervisor's SQLite transactions and ordered event stream.
  The core opens the database with WAL journaling and `synchronous=FULL`.
- Plans, frozen approvals, host manifests and dispatch receipts are persisted
  separately. Plan, approval and host storage use temporary-file writes, file
  synchronization, atomic rename and parent-directory synchronization.
- Native conversational changes, role handoffs, planning commands and artifacts,
  controller policy, workflow state, and notice intents/receipts use the shared
  private atomic writer. It synchronizes the temporary file before replacement
  and confirms parent-directory synchronization before reporting success.
  `persistence_uncertain` means replacement already occurred, but a later step
  failed: the new contents may be visible and must be reconciled. It does not
  mean rollback or permission to repeat an external effect. A failed durable
  reservation stops advancement before provider dispatch; receipt failures retain
  the prior intent and require evidence-based recovery.
- Owned worktrees and their on-disk changes are retained for inspection.
  Bounded progress can be coalesced; every generated token is not a durable
  checkpoint, and unsaved UI drafts are not committed task state.

## Reopening after interruption

Reopening the core preserves completed records. Workers left `queued`,
`claimed`, `running` or `cancel_requested` become `unknown` with a recovery
event. An in-flight notification also becomes unknown. Neither condition
automatically replays a provider request.

The trusted operator inspects the exact approval, host instance, task receipt,
worktree and event cursor before choosing the next action. Existing completed
work must not be regenerated merely because a notification was interrupted.
An unknown outcome is not proof of completion, failure or remote inference
termination.

Host restoration is explicit and preserves its no-replay boundary. It does not
silently restore notification flags or create a new execution approval. Use
[notification recovery](NOTIFICATION-RECOVERY.md) to inspect delivery and ACK
actual event batches. Use [native operator commands](NATIVE-OPERATOR.md) for
the host/session lifecycle and [profile flow](PROFILE-FLOW.md) for role receipts.

## Stale Ready manifests after interruption

A reboot, interrupted host start, or killed host process can leave
`host-manifest.json` at `ready` with its old PID and instance after both
process-lifetime locks were released. The guarded host client may read the
original terminal result through `approved_task`/`backlog_worker_result` from
that state only when the existing exclusive `host.lock` can be acquired,
`supervisor.lock` is demonstrably released, and the exact
manifest/instance/PID/gates/session binding, original sealed
task/run/worker receipt, and complete terminal worker evidence revalidate before
and after the read.

For this orphaned `Ready` case only, the client uses the existing bounded
`threadlane_core::observation::read_core_state_readonly` snapshot reader so
committed `state.db-wal`/`state.db-shm` frames left by the interrupted host are
respected instead of being ignored. The snapshot must contain exactly one
binding/thread matching the manifest, exactly one sealed run and worker, a
terminal run status/revision matching the complete `WorkerSnapshot`, and the
original receipt identity. Missing or inconsistent sidecar pairs, SQLite read
errors, and missing, duplicate, malformed, mismatched, or incomplete selected
binding/run/worker evidence are refused. This is not an independent integrity
check of every WAL/SHM byte: SQLite may ignore recoverable or uncommitted crash
tails and WAL content already checkpointed into the main database. The
missing-SHM and corrupt-WAL fixtures cover refusal when required terminal
evidence cannot be read, not every possible sidecar corruption. The reader may
update SQLite shared-memory read
marks in `-shm`; durable `state.db`, `-wal`, manifest, receipts, cursors, backlog
state, and event batches remain unchanged.

This orphan fallback is strictly read-only. It does not rewrite the manifest or
receipts, mark the session stopped, open a writable core, run migrations or
recovery, create directories or sidecars, checkpoint or delete WAL/SHM frames,
dispatch a Worker, send or ACK notifications, or restart the host. A held lock, a
live or foreign supervisor, a missing PID or instance identity on its own, a
changed receipt, or a missing, nonterminal, or `unknown` worker record is refused
rather than promoted. `unknown` and `failed` never become `success` or `Done`.
Clean `stopped` history keeps its strict immutable no-sidecar read behavior. A
controlled fixture process kill is not power-loss proof, and installed-reboot
behavior remains a separate operator/live gate.

## Current limits

This restores durable workflow state; it does not resume an interrupted LLM
call at the exact token where it stopped.

Activity now provides a read-only **Needs attention** overview of the selected
snapshot's recent runs, with links to existing run details. It distinguishes
uncertain outcomes, terminal execution and evidence review, marks stale
snapshots and reports bounded coverage. It does not enumerate every approval
or restore interrupted hosts. Guided continuation remains explicit operator work.

There is no implicitly installed OS service that restarts all approved hosts after reboot.

The stale `ready` orphan read is a bounded read-only recovery path. It does not
restore or restart the interrupted host, and installed-reboot behavior remains a
separate operator/live gate.

Reopen/restart fixtures verify preserved completed state, unknown in-flight
state and absence of automatic retries. Atomic-writer fault-injection tests
cover failures before rename and during directory open/sync after replacement,
including preservation of visible contents and private permissions. They do not
simulate physical power loss or guarantee the durability of the last unflushed
worker file edits.
