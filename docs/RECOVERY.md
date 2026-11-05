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

## Current limits

This restores durable workflow state; it does not resume an interrupted LLM
call at the exact token where it stopped.

Activity now provides a read-only **Needs attention** overview of the selected
snapshot's recent runs, with links to existing run details. It distinguishes
uncertain outcomes, terminal execution and evidence review, marks stale
snapshots and reports bounded coverage. It does not enumerate every approval
or restore interrupted hosts. Guided continuation remains explicit operator work.

There is no implicitly installed OS service that restarts all approved hosts after reboot.

Reopen/restart fixtures verify preserved completed state, unknown in-flight
state and absence of automatic retries. Atomic-writer fault-injection tests
cover failures before rename and during directory open/sync after replacement,
including preservation of visible contents and private permissions. They do not
simulate physical power loss or guarantee the durability of the last unflushed
worker file edits.
