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

An existing disposable chat was used with explicit user authorization. No active
project chat was modified and no model, effort, approval, or sandbox override was
sent by the Threadlane wake-up adapter.

1. A simulated run reached its question and generated one pending wake-up.
2. The real adapter invoked the explicitly configured installed Codex executable.
3. The durable outcome became `submitted`, with `attempt_count = 1`.
4. Desktop chat history independently showed the fixed wake-up message and a new
   completed reply. Thus delivery was observed, not inferred from exit status.
5. The chat reported that Threadlane MCP tools were unavailable. It did not pull,
   answer, or acknowledge events through MCP.

Per-invocation `codex queue -c mcp_servers...` options did not make the new tools
appear in the already open chat. A temporary project-scoped MCP entry also did
not appear there. These observations do not establish a general Codex limitation
or a confirmed cause; configuration loading and client refresh still need an
explicit setup check. During the follow-up, public `config/read` diagnostics identified the test
project's local configuration as disabled because that project was not trusted.
The user explicitly approved a temporary trust entry for only that test folder;
the same API then showed the MCP entry enabled. The installed Codex app-server,
given an explicit entry, listed all eight tools through its real MCP client
without a model turn. The already open native chat still reported
`MCP_UNAVAILABLE`. Its supported client refresh and callback test remain open.
Test wake-up dispatch is off while waiting for that refresh. The temporary trust
and project MCP entries are owned test fixtures to remove when the test ends.

**Native round-trip acceptance remains open.** In a dedicated trusted test
project, register the stdio command below through the client's supported MCP
configuration, refresh it, and first verify that `status` is actually callable:

```text
<absolute-threadlane-binary> mcp --state-dir <canonical-private-state-dir> --binding <test-binding-id>
```

The MCP process can initialize and list tools with the supervisor offline;
actual tool calls require the supervisor. `threadlane bind` provides the binding
ID. Its wake-up adapter must be enabled only for that same disposable chat.
Complete a new demo using real MCP calls: start, receive the question wake-up,
pull and acknowledge the delivered batch, answer the current revision, receive
completion, and acknowledge it. Never fabricate a successful callback from CLI
state. A project-scoped configuration can be shared by chats in that project;
this fixture is deliberately limited to one chat, and general per-chat routing
is a later integration concern.

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
