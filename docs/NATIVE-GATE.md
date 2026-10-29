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
- **No conversation or state mutation**: Never reads turns or transcripts, never
  calls `thread/resume`, never starts a second app-server, never sends user
  messages or `turn/start` / `turn/steer` / `queue`, and never calls worker,
  demo, or event-acknowledgement tools.
- **No secret or transcript disclosure**: Never prints `operator.token`,
  `connection_nonce`, `epoch_id`, raw MCP payloads, chat titles (`name`),
  `sessionId`, or conversation previews to `stdout`, `stderr`, or `Debug` output.
- **Honest verification gates**: Fixture runs, metadata checks, and read-only
  `status` probes never mark `restart_persistence_gate`, `busy_turn_delivery_gate`,
  or `overall_live_gate` as verified; those remain `needs_verification` until an
  explicitly authorized live host experiment.

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
