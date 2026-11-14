# Control-runtime compatibility and safe idle upgrade

This document describes the additive baseline control-runtime contract used to avoid the
confirmed installed defect where a background `threadlane serve` from an older package survived
desktop Quit/nrs, answered `GET /v1/bindings`, and then failed later on an unsupported scoped
request such as `backlog_list`.

The implementation is intentionally small and uses the existing authenticated loopback
supervisor, descriptor, `supervisor.lock`, sole-owner `Core`, and clean-shutdown machinery. It
does not add a dependency, a second Codex backend, a SQLite/ledger migration, an OS service, a
signal helper, or a new framework.

## Compatibility is capability-based, not package-version-based

A live supervisor advertises a bounded authenticated record through
`GET /v1/control-runtime/info`:

- control-runtime `protocol_version`;
- informational `package_version` (never a compatibility gate by itself);
- `role` (`baseline` or `approved_host`);
- lifecycle `state` (`ready` or `retiring`);
- a per-process random public `instance_id` (32 lowercase hex characters, not a secret);
- the exact `pid`;
- whether Codex wake-up notifications and worker profiles are configured;
- operation capabilities for `bindings`, `scoped_backlog`, `event_pull_ack`,
  `guarded_worker_backlog`, `idle_retire`, and `approved_host_control`.

`ensure_control_runtime` reuses a runtime only when the required baseline operation contract is
present. A package-version difference alone never restarts a healthy compatible runtime, so an
older package cannot cause restart ping-pong. A newer protocol is never retired by an older
caller; the caller fails with an actionable incompatible-runtime error instead.

An older caller that predates this endpoint still performs its original binding readiness check
and shares the same durable supervisor state. It does not trigger an automatic restart.

## Atomic idle retirement

A caller that needs a safe upgrade may request
`POST /v1/control-runtime/retire` with the exact observed `protocol_version`, `instance_id`, and
`pid`. The live supervisor verifies the request under the same `Core` mutex and `stopping` latch
used by the approved-host idle-stop path. Retirement is accepted only when all of the following
are true:

- the runtime is a `baseline` supervisor, not an approved host;
- Codex notifications, worker profiles, and host control are not configured;
- every binding has zero non-terminal runs and workers;
- there are zero live worker tasks;
- there are zero in-flight wake-up notices and zero pending wake-up notices across all bindings;
- the exact instance/protocol identity still matches the live process.

If activity wins the race, retirement refuses with a bounded busy error (counts included) and no
side effect. If retirement wins, the mutation latch is set before the `Core` mutex is released;
later mutations are refused, while read-only inspection may continue. Pending or in-flight
notices are never forced or dropped. Unknown notice outcomes are preserved untouched; retirement
never marks, retries, or replays them.

The supervisor does not signal itself. It latches the existing stop channel, and `threadlane
serve` exits through its normal clean-shutdown stop-request branch. `SupervisorHandle::wait_for_fatal`
remains fatal-only: a clean idle retirement does not fabricate an `InternalError` or record fatal
state. Descriptor removal happens through the existing locked `ActiveCore` drop path. The database, bindings, backlog items and
`Done` receipts, run history, event history, and approval/ledger files remain in place. Normal
startup performs its existing non-terminal-worker `Unknown` recovery and never re-dispatches work.

## Bounded upgrade flow

For an incompatible but safely retireable runtime, `ensure_control_runtime`:

1. acquires the in-process per-state-directory coordinator mutex;
2. acquires a bounded cross-process startup guard scoped to the canonical state path;
3. verifies the exact live descriptor, lock holder, PID, role, protocol, and capability contract;
4. sends the exact-instance retire request;
5. waits at most until the caller deadline for the owned `supervisor.lock` to be released;
6. never spawns while the owned lock is still held;
7. starts only the validated canonical `<executable> serve --state-dir <dir>` path;
8. verifies the successor's authenticated info and binding readiness before returning
   `upgraded`.

If the lock is not released within the deadline, the result is `retire_timed_out`: no replacement
is started and no process is force-killed. If another caller wins the lock, this caller waits for
a verified compatible winner instead of starting a second supervisor.

## Cross-process startup coordination

`ensure_control_runtime` already serializes callers inside one process, but that mutex cannot
coordinate separate desktop processes racing during first use, stale-descriptor recovery, or
retirement of the same idle baseline. The helper therefore also takes a minimal advisory `fs2`
lock on a persistent guard file:

- the guard path is a sibling of `state_dir`, derived from a SHA-256 digest of the canonical state
  path (`.threadlane-control-runtime-startup-<digest>.lock` in the state directory's parent);
- it is deliberately outside `state_dir`, so it is not added to the strict legacy state-directory
  entry allowlist and older callers never mistake it for runtime state;
- the file must be an empty non-symlink regular file owned by the current effective UID with private
  `0600` mode (no owner execute or group/world bits); open uses `O_NOFOLLOW` and the held file
  identity is rechecked against the path after locking. A pre-existing guard with the wrong owner,
  type, size, or permissions fails closed instead of being repaired or replaced;
- acquisition uses bounded `try_lock_exclusive` retries with async sleeps, never an unbounded
  blocking `flock`, and fails closed with `lock_busy` at the caller deadline;
- the guard is never unlinked while held or after release, because another process may have the
  same file open and waiting. Dropping the guard only releases the advisory lock.

The guard serializes only the bounded ensure flow. It does not signal, stop, kill, retire, or
downgrade any runtime by itself; all descriptor/token/lock ownership checks, capability checks,
and atomic idle-retire rules remain authoritative.

## Legacy runtime without the handshake

A legacy runtime that answers authenticated binding reads but has no control-info/idle-retire
endpoint cannot prove the scoped backlog/event contract and cannot be safely retired by this
caller. The helper fails early with `upgrade_required`. The sanitized message starts with the
short actionable instruction to stop that legacy runtime while it is idle through the normal
process manager and retry, before bounded PID/port/path diagnostics. Keeping the guidance first
means the existing 240-character diagnostic sanitizer cannot truncate the operator action.

The helper does not emulate a check-then-kill sequence, does not send SIGTERM/SIGKILL, does not
delete a descriptor or lock, and does not start a second supervisor alongside it. This is an
honest limitation, not seamless legacy replacement. The legacy runtime remains alive and intact
until the operator stops it.

## Operator guidance

- If `connect_current_chat` or the native MCP reports `upgrade_required`, stop the named legacy
  `threadlane serve` process while no runs or workers are active, then retry the connection.
  The old process must not be killed during active work; nothing in this upgrade path will force
  that.
- If the error is `runtime_busy`, finish, cancel, or reconcile the reported active work. Do not
  delete `state.db` or `supervisor.lock` to force an upgrade.
- If the error is `incompatible_runtime`, inspect the reported role/protocol. Approved host roles
  and newer protocols are deliberately never retired by the baseline helper. Use the matching
  package or the approved-host route.
- A successful upgrade preserves the existing state directory. Backlog `Done` items, accepted
  receipts, and completed runs are not replayed.

## Validation and limits

Focused fixtures are defined to cover startup and same-runtime reuse, compatible package
differences, legacy refusal, inactive upgrade with data/Done/history preservation, busy and
pending-notice refusal, concurrent ensure callers converging on one successor, stale
instance/protocol identity, approved-host and configured-notification refusal, malformed
information refusal, and retire/startup timeouts without forced kills. Cross-process fixtures use
per-invocation argument capture and atomic launch counting so concurrent launches cannot corrupt
each other's recorded argv. A bounded regression proves a clean idle retirement does not resolve
the fatal-only `wait_for_fatal` waiter. Tests use isolated private temporary directories and owned
processes only.

Source-level design and fixture presence are not a run result. The parent controller must run
formatting, focused tests, and Clippy; this document does not claim an unrun pass.

The behavior targets Linux, matching the existing supervisor descriptor/lock implementation.
macOS, Windows, installed-host upgrade automation, and live multi-host acceptance are not claimed
by this document.
