# Threadlane

Threadlane is a local execution middleware and control plane designed to work alongside a native Codex conversation. The user remains in their native Codex chat while Threadlane owns validated task state, durable event streams, revision checks, and process supervision.

> **Status:** Early Milestone 1 foundation, developed with AI assistance and reviewed before checkpointing. This is **not production ready**. On Linux/NixOS, 34 automated tests and the CLI smoke check pass. A real Codex chat received a runtime wake-up. The installed Codex MCP client can discover all eight tools, but the already open desktop test chat still needs a supported MCP refresh. The complete native conversation loop remains unverified. See [validation evidence](docs/VALIDATION.md) and the [closure review](docs/CLOSURE-REVIEW.md).

---

## Working Now (Milestone 1) vs. Planned Milestones

### Working in Milestone 1
- **`threadlane-core` (`crates/threadlane-core`)**: Transactional SQLite state engine (`SCHEMA_VERSION = 1`) with validated non-nil Codex thread UUID bindings, deterministic simulated demo runs, revision-checked `answer` / `steer` / `cancel` transitions, per-binding idempotent command deduplication, bounded event batches, high-water-mark event acknowledgements, and persistent wake-up notice coalescing and crash/reopen recovery.
- **`threadlane` runtime (`crates/threadlane`)**:
  - **Single-owner supervisor (`threadlane serve`)**: Holds an exclusive process-lifetime file lock (`supervisor.lock`) tied to the active state engine, owns the SQLite database (`state.db`) and timed demo progression ticks, binds only `127.0.0.1:0`, and writes an atomic private runtime descriptor (`supervisor.json`) containing a fresh 64-hex-character bearer token (244 random bits from two v4 UUIDs).
  - **Trusted CLI (`threadlane bind`, `list-bindings`, `status`, `call`, `retry-wakeup`)**: Configures chat bindings and executes binding-scoped operations over authenticated loopback HTTP with streamed response size enforcement.
  - **Scoped stdio MCP server (`threadlane mcp`)**: Built on the official Rust MCP SDK (`rmcp = "=3.8.0"`). Launched for a single pre-configured `binding_id`; discovery works while the supervisor is offline, and every tool call revalidates the current private descriptor. It exposes `status`, `get_run`, `start_demo`, `pull_events`, `ack_events`, `answer`, `steer`, and `cancel`.
  - **Opt-in Codex wake-up adapter (`--codex` + `--notify-binding`)**: Disabled by default. When explicitly configured with an absolute executable path and a binding allowlist, persists `InFlight` intent before invoking `["queue", "--thread", "<UUID>", "--message", FIXED_WAKEUP_MESSAGE]` with a bounded timeout and no model, effort, approval, or sandbox overrides.
  - **Self-contained CLI smoke script (`scripts/smoke.py`)**: Exercises the simulated loop in an isolated temporary directory without touching live chats or paid APIs.

### Planned for Later Milestones (Not Implemented Yet)
- **Milestone 2**: Real coding worker adapters (OpenCode v2/Go and Antigravity), requested/observed model and effort tracking, per-task and integration Git worktrees, scoped diff gates, and process-tree supervision.
- **Milestone 3**: Tauri narrow smoked-glass desktop control panel (`Activity`, `Chats`, `Tools`, `Profiles`, `Settings`) with an accessible opaque fallback.
- **Milestone 4**: Global tool/model/effort registry, role/difficulty profiles, and shadow-mode Jev classification.
- **Milestone 5**: Cross-platform release hardening, macOS/Windows native ACLs and secret storage, optional background service/tray packaging, and measured cost/notification budgets.

---

## Protocol, Security, and Acknowledgement Model

1. **Single State Owner & Explicit Fault Handling**:
   - Only `threadlane serve` opens `state.db`. A non-blocking exclusive `flock` on `supervisor.lock` rejects concurrent supervisor instances on the same state directory.
   - `Core` ownership and `supervisor.lock` are bundled together inside the supervisor's active state guard. Shutdown stops acceptance, gives the worker a bounded opportunity to persist its final outcome, then retires pending requests. Dropping the handle retires it immediately. The old engine cannot mutate after a new owner acquires the lock; descriptor cleanup occurs while ownership is still held.
   - If a background `tick`, wake-up claim, or wake-up result write encounters a SQLite persistence failure, the supervisor logs a bounded redacted diagnostic, stops the background loop without retrying uncertain external side effects, and fails subsequent requests closed with `storage_error`. The CLI then shuts down, releases ownership and exits nonzero; it does not stay alive as an apparently healthy service.
2. **Loopback IPC & Private Descriptors**:
   - The supervisor binds `127.0.0.1:0` only and generates a fresh 64-hex-character bearer secret (two v4 UUIDs providing 244 random bits) on every start.
   - All HTTP endpoints require `Authorization: Bearer <token>`, compared with a non-short-circuiting fixed-length byte loop (`std::hint::black_box` accumulator, without claiming hardware/compiler-guaranteed side-channel immunity).
   - An unrelated non-empty directory is rejected before its permissions or contents are changed. On Unix, the dedicated state directory is enforced as `0700` and state/descriptor/lock/database files as `0600`. Symlink components anywhere along the state directory path (or on the state files themselves) and pre-existing files with group/world permissions are rejected before reads or writes.
   - **Canonical path note**: Because every component of `--state-dir` is checked with `symlink_metadata`, pass a canonical non-symlink path on systems where `/tmp` or `/var` is a platform symlink (for example on macOS where `/tmp` links to `/private/tmp`). Relative paths are resolved against the current working directory before checking components. These filesystem checks protect against accidental misconfiguration and other local users via standard Unix permissions (`0700`/`0600`), and do not claim isolation against malicious processes already running under the same OS user account.
   - Before sending a bearer token, the client verifies that the state directory (`0700`) and `supervisor.json` (`0600`) are private non-symlink paths, checks descriptor version `1` and loopback host `127.0.0.1`, bypasses environment HTTP proxies, disables HTTP redirects, and enforces request timeouts and streamed chunk response size limits (`2 MiB`).
   - Bearer tokens are redacted in `Debug` and `Display` formatting and never logged.
3. **Binding Scope & Untrusted Worker Output**:
   - Chat bindings (`bnd_<uuid>`) are created exclusively via the trusted CLI (`threadlane bind`), never via MCP tools.
   - Each `threadlane mcp` process is locked to one `binding_id` at launch. MCP tool schemas reject unknown fields (`deny_unknown_fields`) and never accept `binding_id`, `thread_uuid`, or caller-supplied `now_ms`.
   - All run and event text payloads carry `"untrusted_data": true`. Worker output and event text are untrusted data and never grant permissions.
   - `status` returns a compact snapshot (binding metadata, wake-up notice state, and short recent run summaries without historical prompts/questions); full revision and question history is fetched via `get_run`.
4. **Idempotency & Injected Time**:
   - The supervisor injects current wall-clock `now_ms` on all commands.
   - Mutating commands (`start_demo`, `ack_events`, `answer`, `steer`, `cancel`) deduplicate by `(binding_id, command_key)` excluding `now_ms`. Retrying the same key and parameters returns the original result (including across supervisor restarts); reusing a key with different parameters returns `idempotency_conflict`.
5. **Ordered Events, Explicit Acknowledgements & Coalesced Wake-ups**:
   - `pull_events` returns a bounded batch of per-binding ordered events (`seq = 1..N`), records a server-generated `batch_id`, and advances `delivered_seq`. Reading events does not acknowledge them.
   - `ack_events` validates `batch_id` ownership and contiguous high-water-mark boundaries (`acked_seq`). Callers cannot acknowledge unseen, skipped, or cross-binding batches; repeated acknowledgements are harmless and never move `acked_seq` backwards.
   - At most one wake-up notice is active per binding. Important events (`question_asked`, `run_completed`) coalesce into that notice.
   - Wake-up claims transition `Pending -> InFlight` in SQLite before the external Codex queue command is spawned, then record `Submitted` (exit 0), `Failed` (spawn error), or `Unknown` (timeout, non-zero/signal termination, or crash recovery on reopen). Timed-out child processes are killed and reaped. `Submitted`, `Failed`, and `Unknown` claims are never automatically replayed; only a valid `ack_events` covering `covered_up_to_seq` releases the notice (transitioning to `Pending` if newer unacknowledged important events remain, or `Idle` otherwise).

---

## Demo Limits and Platform Support

- **Deterministic Simulation Only**: Milestone 1 runs a simulated state machine (`Running -> WaitingInput -> Completing -> Completed` or `Cancelled`). It does not invoke real coding agents or prove external worker crash recovery.
- **Live Native Codex Acceptance**: The real runtime submitted exactly one wake-up and the intended disposable desktop chat produced a reply. The chat reported that Threadlane MCP tools were unavailable, so native pull/answer/ack and full round-trip acceptance are still pending. CLI or synthetic MCP results are not counted as native chat delivery proof. See [validation evidence](docs/VALIDATION.md).
- **Linux / NixOS First**: Runtime permission checks (`0700` directories, `0600` files, symlink component rejection) are implemented for Unix and tested on Linux/NixOS. Non-Unix platforms fail closed with an explicit `unsupported_platform` error until native platform ACL enforcement (e.g., Windows DACLs) is implemented and verified in a later milestone. macOS and Windows have not yet been tested.

---

## Local Usage (From a Fresh Checkout)

### Development Shell (Nix)
```bash
nix develop
```

### Build the Binary
From the repository root, build the workspace so the binary is available at `./target/debug/threadlane` (or invoke commands via `cargo run -p threadlane -- <subcommand>`):
```bash
cargo build --workspace
```

### CLI Grammar
```bash
# 1. Start the local supervisor (Codex wake-up bridge is OFF by default)
./target/debug/threadlane serve --state-dir /path/to/state_dir

# Optional: enable Codex wake-ups for specific allowed bindings only
./target/debug/threadlane serve \
  --state-dir /path/to/state_dir \
  --codex /abs/path/to/codex \
  --notify-binding bnd_<uuid>

# 2. Create or list trusted bindings (CLI only)
./target/debug/threadlane bind --state-dir /path/to/state_dir --thread <UUID> --label "Project Chat"
./target/debug/threadlane list-bindings --state-dir /path/to/state_dir

# 3. Inspect compact binding status
./target/debug/threadlane status --state-dir /path/to/state_dir --binding bnd_<uuid>

# 4. Execute a scoped command via JSON
./target/debug/threadlane call \
  --state-dir /path/to/state_dir \
  --binding bnd_<uuid> \
  --request '{"op":"start_demo","command_key":"demo-1","instruction":"Simulated task","step_delay_ms":500}'

# 5. Launch the stdio MCP server for a single binding
./target/debug/threadlane mcp --state-dir /path/to/state_dir --binding bnd_<uuid>
```

### Recovering a failed wake-up

Inspect `threadlane status` first. Only an operator can re-arm a specific failed
claim; MCP tools cannot perform this action:

```sh
threadlane retry-wakeup --state-dir /path/to/state_dir \
  --binding bnd_<uuid> --expected-claim wcl_<uuid>
```

For an `unknown` outcome, add `--allow-unknown` only after deciding that a possible
duplicate delivery is acceptable. Submitted, pending or in-flight claims cannot
be forced through this command. A stale claim ID has no effect. The retry keeps
the attempt history and audit record, and does not acknowledge any events.
There are no automatic retries of failed or uncertain external effects.

Timed demo events carry compact run/revision references rather than repeatedly
copying the instruction. `get_run` retains the full instruction and history.
Long-history pagination and compact mutation responses remain a prerequisite
before real workers; event size limits alone do not bound full run snapshots.

### Running Tests and Smoke Check
```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
python3 scripts/smoke.py
```

---

## AI-Assisted Development Note

This codebase is developed with AI assistance under human direction and explicit architectural invariants: implementation slices and automated tests are reviewed and validated before checkpointing. AI-generated plans or reviews are not treated as runtime proof; only reproducible builds, tests, and observed execution artifacts count as verification.
