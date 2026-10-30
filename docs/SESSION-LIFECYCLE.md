# Approved session lifecycle

Native Codex remains the everyday work surface. Threadlane's window is an
optional control and observation panel, not a form that must be completed for
every task. Initial tool/profile configuration, native pairing, and trusted
approval are distinct setup boundaries; publication and worker output do not
grant execution authority.
Each new immutable execution scope needs trusted approval; tasks within an
already approved scope do not need another panel visit just to dispatch or read
their progress through MCP.

## Optional desktop controls

Open an approved plan's local session panel in the connected original chat.

- **Start local session** starts a new approval host. Starting it does not
  automatically dispatch any task.
- **Stop idle session** asks the authenticated host to stop only when no active
  run, queued/claimed/running/cancelling worker, live worker task, or in-flight
  notification remains. Pending notifications block only for enabled bindings.
  Use existing task cancellation first when work is active. There is no force
  stop or PID-based termination.
- **Restart stopped session** is a separate action available only after a clean,
  confirmed stop. It preserves existing task receipts and does not replay jobs.
  It creates a new instance identity and starts with notifications off unless
  explicitly selected again.

Closing the window does not stop an attached approval host. Reopening the window
can inspect and stop that host without an in-memory child-process handle.

Each start has an `hst_` instance identifier. Stop and restart compare the
expected identifier, so an old button response cannot control a newer instance.
Legacy sessions without this identity remain readable with lifecycle controls
unavailable. A failed or uncertain session is not silently restarted.

## Readiness and uncertain results

`Ready` requires a live authenticated response. `Stopping` disables new
mutations while the owner drains. Stop succeeds only after a matching durable
`Stopped` record and released host lock. A lost connection or response alone is
not success; refresh status and inspect the result before retrying.

The control routes use the same private loopback descriptor, bearer auth,
original approval, control binding, native thread, and session binding. Desktop
commands accept only approval/instance identifiers and an optional registered
notification tool. They accept no PID, executable path, owner identity, token,
arbitrary arguments, or timeout. Stop/restart are not new MCP tools.

## Native metadata list

Chats can explicitly load read-only native metadata from an enabled Codex CLI
tool. An omitted executable path uses the backend's normal PATH discovery.
Navigation and polling never query the CLI automatically. Changing tool,
registry revision, executable, filter, or visibility invalidates pending replies.
Pagination retains at most 100 distinct threads. Metadata and matching labels
are observations, not proof of enrollment or operator approval.

## Acceptance boundaries

The Linux package passed 238 workspace tests plus one separately executed real
CLI-process test, 23 Tauri helper tests, and 70 frontend tests. The process test
uses the actual Cargo-built host binary, authenticated idle stop, natural exit
zero, released locks, explicit restart, a rotated instance, and stale-instance
rejection. It never invokes a real coding provider. Root and Tauri Clippy run
with warnings denied; dependency versions remain unchanged.

At 390 pixels, browser checks confirmed preserved filter focus/selection during
polling, explicit-only metadata loading, cleared old results after filter changes,
correct semantic hiding, and no horizontal overflow. Those checks use marked
development fixtures.

Local fixture tests and browser preview checks are separate from live native
chat routing. Two-chat isolation, busy-turn delivery, interactive provider
continuation, product API access, and macOS/Windows release checks remain their
own acceptance gates. See [native routing](NATIVE-ROUTING.md),
[approved notifications](APPROVED-NOTIFICATIONS.md), and
[configuration](CONFIGURATION.md).
