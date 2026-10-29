# Foundation validation

Verified on 2026-10-29 on Linux/NixOS. This is a checkpoint report, not a claim
that every milestone or platform is supported.

## Reproduce locally

From the repository root:

```sh
nix develop
cargo build --workspace
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
python3 scripts/smoke.py
```

The committed Nix lock resolves Rust/Cargo 1.99.0. `Cargo.lock` pins the Rust
dependencies, including the official `rmcp` SDK at 3.8.0. The development shell
was actually opened; the checks below ran inside that shell.

## Results

| Check | Observed result |
| --- | --- |
| Core behavioural tests | 12 passed |
| Core review regressions | 6 passed |
| HTTP, CLI, stdio MCP, and fake-Codex integration | 13 passed |
| Independent runtime regressions | 3 passed |
| Formatting and Clippy with warnings denied | Passed |
| CLI smoke with real local processes | Start, question, event delivery, acknowledgement, answer, completion, and idle wake state passed |

The original independent regressions initially reproduced failures and passed after
implementation fixes. They cover acknowledgement races, an empty-batch boundary,
an old HTTP handler writing after its supervisor released ownership, symlinked
parent directories, and a never-ending chunked response exceeding the byte budget.

The runtime integration test also injects SQLite failures in an isolated fixture.
The service reports a fault, rejects later operations, and does not repeat an
external wake-up when recording its outcome fails. The stdio test initializes a
real MCP process, lists and calls all eight tools, rejects injected scope/time
arguments, and verifies that cross-binding requests have no effect.

The closure pass adds coverage for escaped instructions through start/steer,
completion and restart, additive v1 schema recovery, manual wake retry without
cursor changes, stale claim rejection and durable retry history. Runtime checks
cover offline MCP discovery followed by recovery in the same process, descriptor
refresh/security, preserving unrelated directories and nonzero CLI exit on a
fatal SQLite error. All 34 tests passed. A redundant `mut` in a new test was then
removed to satisfy Clippy; formatting, Clippy, build and the CLI smoke all passed.

## Native Codex experiment

**The deterministic native conversation loop passed on Linux/NixOS on 2026-10-29.**
The test used one disposable native Codex chat with explicit user authorization.
No other project chat or model, effort, approval or sandbox setting was changed.

Initial attempts delivered the fixed wake-up but the open chat reported
`MCP_UNAVAILABLE`. Public `config/read` diagnostics showed that its project-local
MCP configuration was disabled because the project was not trusted. A temporary,
user-approved trust entry enabled that layer. A standalone installed Codex
app-server could then discover all eight tools; that alone was not native proof.

After the user restarted the desktop app, its app-server launched the scoped
Threadlane MCP process for the test project. Although the user still did not see
it in the `/mcp` menu, a new turn in the existing test chat successfully called
`threadlane_probe.status`. The menu observation did not establish tool failure.
No NixOS package, library or system configuration fix was needed for this test.
The complete reason for the earlier UI omission was not established.

The final experiment independently verified native chat tool records and runtime
state, not only the exit status of the queue command:

1. The native chat cancelled the old waiting demo through MCP, pulled its four
   events and acknowledged the returned batch.
2. It started a new deterministic demo with `start_demo`, then ended its turn.
3. The supervisor was restarted with wake-up dispatch allowlisted to that same
   disposable chat. The already running MCP process recovered with the new private
   descriptor. The waiting run persisted across this restart.
4. The real queue adapter delivered the fixed question wake-up. The chat used
   `pull_events`, `ack_events`, `get_run` and revision-matched `answer` through MCP.
   Its answer was a preauthorized synthetic test response, not real task approval.
5. A second fixed wake-up delivered completion. The chat used `pull_events`,
   `ack_events` and `status` through MCP and confirmed the run was completed.
6. Final independent CLI status showed `active_runs = 0`, `delivered_seq = 9`,
   `acked_seq = 9`, wake-up `idle` and no last error. The cumulative submission
   count rose from one earlier failed-discovery experiment to three: two additional
   wake-ups for the accepted question/completion loop.

The owned supervisor/MCP processes and only the owned temporary trust/project
configuration entries were removed after the test. No persistent service was
installed, and unrelated Codex configuration was preserved.

To repeat in a dedicated trusted test project, configure the scoped stdio command
and verify a real native `status` call before running the loop:

```text
<absolute-threadlane-binary> mcp --state-dir <canonical-private-state-dir> --binding <test-binding-id>
```

Discovery works with the supervisor offline; actual calls require the supervisor.
A project configuration can be shared by chats in that project, so this fixture
was deliberately restricted to one chat. General per-chat routing remains later
integration work. Busy-turn steering and real external worker/process recovery
were not established by this deterministic native test.

Client setup reference: [Codex MCP configuration](https://learn.chatgpt.com/docs/extend/mcp).

## Deliberate limits

- External coding agents, Jev, worktree automation, and the Tauri UI are later work.
- The demo proves durable logical transitions, not recovery of real worker processes.
- macOS and Windows have not been tested. Windows runtime startup fails closed
  until suitable private file ACL support is implemented.
- Full run/mutation responses and retained history are not yet size-bounded;
  pagination, compact mutation results and retention are a gate before real workers.
- Secret files are private to the OS user. This does not isolate a malicious
  process already running as that same user.
