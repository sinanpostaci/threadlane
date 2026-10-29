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
| Core behavioural tests | 9 passed |
| Independent core regressions | 5 passed |
| HTTP, CLI, stdio MCP, and fake-Codex integration | 8 passed |
| Independent runtime regressions | 3 passed |
| Formatting and Clippy with warnings denied | Passed |
| CLI smoke with real local processes | Start, question, event delivery, acknowledgement, answer, completion, and idle wake state passed |

The independent regressions initially reproduced failures and passed after
implementation fixes. They cover acknowledgement races, an empty-batch boundary,
an old HTTP handler writing after its supervisor released ownership, symlinked
parent directories, and a never-ending chunked response exceeding the byte budget.

The runtime integration test also injects SQLite failures in an isolated fixture.
The service reports a fault, rejects later operations, and does not repeat an
external wake-up when recording its outcome fails. The stdio test initializes a
real MCP process, lists and calls all eight tools, rejects injected scope/time
arguments, and verifies that cross-binding requests have no effect.

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
explicit setup check. No global Codex configuration was changed. Temporary test
supervisors are stopped after verification, so no unsolicited wake-up remains.

**Native round-trip acceptance remains open.** In a dedicated trusted test
project, register the stdio command below through the client's supported MCP
configuration, refresh it, and first verify that `status` is actually callable:

```text
<absolute-threadlane-binary> mcp --state-dir <canonical-private-state-dir> --binding <test-binding-id>
```

The supervisor must already be running. `threadlane bind` provides the binding
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
- Event retention and large-history performance have not been benchmarked.
- Secret files are private to the OS user. This does not isolate a malicious
  process already running as that same user.
