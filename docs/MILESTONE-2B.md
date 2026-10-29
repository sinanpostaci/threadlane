# Milestone 2B: durable worker dispatch

This is the implemented durable-dispatch contract. The fixture checkpoint passed on
2026-10-29; its evidence and remaining gates are recorded in [VALIDATION.md](VALIDATION.md)
and [DISPATCH.md](DISPATCH.md). It is not full native real-worker acceptance.
Native Codex remains the conversation interface. The existing demo and bounded
history/replay contracts must continue to work.

## Vertical slice

- An optional private startup configuration defines binding-scoped worker profiles
  and a bounded concurrency limit. Profiles fix executable, provider, model/effort,
  repository, base, work area, write scope and execution budgets. Disabled is the
  default. This is not the later user-facing global Tools/Profiles registry.
- Scoped MCP/HTTP clients may start a configured profile with bounded instruction
  text and an idempotency key, inspect a compact durable worker snapshot, and
  request revision-checked cancellation. They cannot submit executable paths,
  arbitrary specs, models, repositories, work areas or permission overrides.
- The single SQLite owner persists spawn intent before an external effect. Duplicate
  command replay never launches another process. Persisted claims bind the exact
  profile digest, run, revision and attempt; stale completions cannot overwrite
  newer state. Uncertain outcomes after restart become explicit unknown outcomes
  without automatic resubmission or PID-based killing of an unverified process.
- Worker jobs use the ordered binding event stream and its existing delivery/ACK
  and coalesced wake-up machinery. Progress is bounded and redacted; important
  terminal/blocked/unknown outcomes wake Codex without copying worker prose into
  the fixed wake-up text.
- Cancellation requested, process stopped, execution finished and patch accepted
  are distinct states. Provider model/effort evidence stays separate from requested
  settings. A successful process or diff never grants review acceptance.
- Calls, events, replay and snapshots retain explicit byte/count limits. Large
  reports belong in private bounded local artifacts, not repeated MCP payloads.
- Every task has an owned worktree; independent tasks can run concurrently up to
  the configured limit. Preserve source checkout and dirty artifacts. No automatic
  merge, force cleanup, persistent service or global client configuration changes.

## Parallel ownership and worker observer contract

The dispatch implementation owns core/runtime code and tests plus DISPATCH.md.
A separate writer owns only threadlane-workers and its focused tests. That writer
adds these additive public entry points, preserving all existing functions:

- `run_worker_with_observer(spec, directory, cancel_rx, progress_tx)`
- `execute_worker_task_with_observer(spec, run_id, task_id, existing_worktree_id,
  cancel_rx, progress_tx)`

The last argument is `Option<tokio::sync::mpsc::Sender<NormalizedWorkerEvent>>`.
Live normalized events are sanitized/bounded before transmission, emitted before
process exit, and sent with nonblocking `try_send`. A full/closed observer never
blocks pipe drainage, cancellation or final evidence. Final reports record a
separate `dropped_live_events` count; observer drops do not erase terminal evidence.
The dispatcher bounds its channel, durable progress count and event text too.

## Explicitly deferred boundaries

Provider-interactive question answering and true in-session Steer are not yet
supported by the print-mode adapters. Worker calls must reject unsupported Answer
or Steer before any state change; the deterministic demo retains those features.
Do not pretend that recording an answer delivered it to a provider. Reviewed Git
integration, accepted-result cleanup, native busy-turn behavior and the desktop
UI require later integration checkpoints.

## Validation gate

Preserve all existing behavioral regressions. Add behavioral coverage for scoped profiles,
forbidden overrides, disabled dispatch, duplicate replay, concurrency limits,
observable progress before exit, cancel/finish races, shutdown/drop, uncertain
restart without replay, stale completions, bounded results, fake-provider errors
and unchanged source checkouts. Use disposable Git/process fixtures. Formatting,
workspace tests, Clippy with warnings denied, build and CLI smoke must pass before
merging. A small real Argon transport/coding fixture may follow with no other
model or unrelated chat invocation. Live OpenCode use remains unverified unless
separately exercised and reported.
