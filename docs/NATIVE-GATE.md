# Native Two-Chat Pairing Acceptance Gate (`threadlane-native-probe`)

`threadlane-native-probe` is the local operator acceptance CLI for inspecting native
Codex thread MCP readiness and verifying two-chat connection-epoch pairing isolation
over the documented public `codex app-server proxy` stdio interface.

## Protocol and Safety Invariants

- **Public proxy methods only**: Invokes `codex app-server proxy [--sock <PATH>]`
  using `NativeClient` with bounded timeouts (`15s` default), frame limits (`1 MiB`),
  and Unix process-group supervision:
  - `thread/read` with `includeTurns: false` (verifying authoritative `thread.id`
    and expected canonical `cwd`).
  - `mcpServerStatus/list` scoped to `threadId` and `serverName: "threadlane"`
    (`detail: "toolsAndAuthOnly"`).
  - `mcpServer/tool/call` scoped to `threadId` and `server: "threadlane"` for
    `connection_identity` and (when `--verify-status` is passed) read-only `status`.
- **No conversation or work execution mutation**: Never reads turns or transcripts, never
  calls `thread/resume`, never starts a second app-server, never sends user
  messages or `turn/start` / `turn/steer` / `queue`, and never calls worker,
  demo, or event-acknowledgement tools. The explicit `pair-check` mode does create
  test bindings and enrolls connection epochs; `inspect` remains read-only.
- **No secret or transcript disclosure**: Never prints `operator.token`,
  `connection_nonce`, `epoch_id`, raw MCP payloads, chat titles (`name`),
  `sessionId`, or conversation previews to `stdout`, `stderr`, or `Debug` output.
- **Honest verification gates**: Fixture runs, metadata checks, and read-only
  `status` probes never mark `restart_persistence_gate`, `busy_turn_delivery_gate`,
  or `overall_live_gate` as verified; those remain `needs_verification` until an
  explicitly authorized live host experiment.
- **Optional read-only managed WebSocket transport (`NativeClient`)**: On Linux,
  `NativeClient::new_managed_websocket` (or `with_managed_websocket`) accepts an
  explicit `threadlane::NativeReconnectConfig` (`0700` `descriptor_root` plus
  canonical `backend_executable` and `application_executable`), verifies `/proc`
  process ancestry, private socket identity, and `SO_PEERCRED`, and enforces a
  strict read-only JSON-RPC allowlist (`thread/loaded/list`, `thread/list` without `projectId`,
  `thread/read` with `includeTurns: false`, `mcpServerStatus/list` with
  `detail: "toolsAndAuthOnly"`, and `mcpServer/tool/call` only for server
  `threadlane`, tool `connection_identity`, and `{}` arguments). Observing a
  matching nonce/epoch or distinct server PIDs across read-only probes does not
  by itself prove general two-chat per-call isolation, which remains a separate
  verification gate.

The paired server advertises session-handshake MCP protocol versions through
2025-11-25, rather than the newer stateless version. Its connection-epoch
binding is process-scoped and does not yet authenticate the origin of every
individual tool call. On the tested Codex desktop version, a loaded v2
subagent was listed but rejected direct `mcpServer/tool/call`; its direct MCP
call used a different process. This is an observation, not a general guarantee.

On 2026-11-02, an ephemeral local CLI session using the bundled
`codex-cli 0.166.0-alpha.5.1` called a disposable read-only MCP tool. The
call's `_meta.threadId` matched that session's `thread.started` ID. This
establishes the field for that CLI test only, not for desktop, other versions,
or other platforms. Direct app-server callers can supply `_meta`, so it is an
untrusted hint, never enrollment authority. The read-only
`verify_thread_hint_candidate` helper cross-checks a hint against a thread-scoped
connection and rereads it, but does not establish exclusive ownership or
authenticate later calls. It does not perform enrollment.
In a separate read-only Linux desktop check, the same candidate helper matched
the current chat to its existing MCP process; no other chat was changed and
no enrollment or work dispatch occurred.

The separate `connect_current_chat` tool composes this check with managed-parent
endpoint discovery, baseline runtime readiness, idempotent binding resolution,
and private-store enrollment. It is available on the managed Linux desktop route
only. Later scoped calls require a matching host-provided thread ID. Its trust
boundary is the local OS user and the verified parent app-server; a same-user
process with direct access to that private socket or private state is not an
untrusted remote caller. Results continue to report
`native_per_call_identity_verified: false`. The complete automatic desktop path
still needs an installed-package acceptance test; read-only probes alone do not
establish that gate or macOS/Windows support.

## CLI Modes

### 1. `inspect` (Read-Only Single-Thread Probe)

```bash
cargo run -p threadlane-native --bin threadlane-native-probe -- inspect \
  --codex /abs/path/to/codex \
  [--socket /abs/path/to/proxy.sock] \
  --thread <CANONICAL-THREAD-UUID> \
  --project-root /canonical/abs/path/to/project
```

- Calls `NativeClient::thread` (`includeTurns: false`) and requires the returned
  `thread.id` to match `--thread` and `thread.cwd` to match `--project-root`.
- Calls `NativeClient::mcp_server_status(thread, "threadlane")` and requires a
  configured `threadlane` MCP server entry.
- Calls thread-scoped `mcpServer/tool/call` (`connection_identity`), parses and
  validates the `rmcp` result wrapper (`content[0].text` and/or `structuredContent`),
  and validates nonce, epoch, PID, and state shape without printing secrets or
  enrolling the connection.

### 2. `pair-check` (Two-Chat Enrollment and Isolation Probe)

```bash
cargo run -p threadlane-native --bin threadlane-native-probe -- pair-check \
  --codex /abs/path/to/codex \
  [--socket /abs/path/to/proxy.sock] \
  --pairing-dir /abs/path/to/private-pairing-dir \
  --state-dir /abs/path/to/running-supervisor-state-dir \
  --project-root /canonical/abs/path/to/project \
  --thread-a <CANONICAL-THREAD-A-UUID> \
  --thread-b <CANONICAL-THREAD-B-UUID> \
  --allow-enroll \
  [--verify-status]
```

- Requires explicit `--allow-enroll`, two distinct canonical non-nil thread UUIDs,
  and matching `cwd` (and `projectId`) metadata on both threads.
- Queries `connection_identity` via thread-scoped `mcpServer/tool/call` on both
  threads.
- Opens the private (`0700`/`0600`) `PairingStore` at `--pairing-dir`, reads the
  private operator token internally, creates two bindings in the running
  Threadlane supervisor at `--state-dir` with static labels
  (`native-probe-thread-a`, `native-probe-thread-b`), and enrolls the two probed
  connection identities separately.
- **Shared child connection**: If both threads resolve to the same MCP connection
  nonce or epoch, the second enrollment quarantines the session in `PairingStore`
  (`quarantined_shared_connection`), outputs a bounded JSON report with
  `blocked_shared_connection` for both `thread_a` and `thread_b` (and
  `two_chat_routing_gate` / `overall_live_gate`), exits non-zero, and never
  continues to `status` calls or claims isolation.
- **Distinct child connections without `--verify-status`**: Reports
  `outcome: "isolated_enrollment_observed"`, `isolated_enrollment_observed: true`,
  `status_routing_verified: false`, and `per_call_binding_routing: "needs_verification"`.
- **Distinct child connections with `--verify-status`**: Invokes read-only `status`
  via thread-scoped `mcpServer/tool/call` on both threads, parses the `rmcp`
  result wrapper, and asserts each returned structured `binding_id` matches that
  thread's own created binding and differs from the other (`outcome: "isolated_status_verified"`,
  `status_routing_verified: true`, `per_call_binding_routing: "status_binding_verified"`).
  `restart_persistence_gate`, `busy_turn_delivery_gate`, and `overall_live_gate`
  remain `needs_verification`.

## Operator Preparation vs. User Approval Boundary

### Prepared Locally Without Live Native Chat Activity

- Building `threadlane-native-mcp` and `threadlane-native-probe` and running the
  local fake JSON-RPC fixture test suite (`cargo test -p threadlane-native`).
- Starting an isolated local Threadlane supervisor (`threadlane serve --state-dir <DIR>`)
  and preparing a private `0700` temporary `--pairing-dir`.

### Requires Explicit User Approval Before Execution

- Creating, opening, or selecting live disposable native Codex chats in the target
  project.
- Adding or refreshing project/host MCP configuration to launch
  `threadlane-native-mcp --state-dir <DIR> --pairing-dir <DIR>`.
- Running `threadlane-native-probe inspect` or `threadlane-native-probe pair-check`
  against live native Codex threads, or conducting live restart/refresh and
  busy-turn `turn/steer` verification experiments.
