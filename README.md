# Threadlane

Threadlane is a local execution middleware and control plane designed to work alongside a native Codex conversation. The user remains in their native Codex chat while Threadlane owns validated task state, durable event streams, revision checks, and process supervision.

Everyday planning, questions, Steer, and work follow-up belong in Codex. The
desktop is an optional observer and configuration surface. Initial pairing and
trusted scope approval remain explicit setup boundaries; ordinary work should
not require switching to the panel for each task. See
[session lifecycle and its current limits](docs/SESSION-LIFECYCLE.md).

> **Status:** Early development with AI assistance, reviewed before checkpointing. The Linux desktop includes editable tools, model/effort catalogs, workflow profiles, durable plan drafts, appearance settings, and read-only activity history. CLI and Linux desktop Nix source builds pass. Immutable operator approvals, detached local hosts, frozen-task MCP dispatch and default-off approved-session notifications have local fixture coverage. Production multi-chat onboarding, live API entitlement, interactive provider steering, and other platforms remain separate acceptance gates. See the [M4 baseline and its validation limits](docs/M4-CHECKPOINT.md), [approved-session notification extension](docs/APPROVED-NOTIFICATIONS.md), [configuration](docs/CONFIGURATION.md), and [desktop usage](docs/DESKTOP.md).

---

## Working Now and Remaining Milestones

### Conversation Runtime and Bounded History (Milestones 1 and 2A)
- **`threadlane-core` (`crates/threadlane-core`)**: Transactional SQLite state engine (`SCHEMA_VERSION = 3`, with transactional v1/v2 upgrades) with validated non-nil Codex thread UUID bindings, deterministic simulated demo runs, revision-checked `answer` / `steer` / `cancel` transitions, per-binding compact idempotent command replay, bounded keyset-paginated run/revision/question history, bounded event batches, high-water-mark event acknowledgements, and persistent wake-up notice coalescing and crash/reopen recovery.
- **`threadlane` runtime (`crates/threadlane`)**:
  - **Single-owner supervisor (`threadlane serve`)**: Holds an exclusive process-lifetime file lock (`supervisor.lock`) tied to the active state engine, owns the SQLite database (`state.db`) and timed demo progression ticks, binds only `127.0.0.1:0`, and writes an atomic private runtime descriptor (`supervisor.json`) containing a fresh 64-hex-character bearer token (244 random bits from two v4 UUIDs).
  - **Trusted CLI (`threadlane bind`, `list-bindings`, `status`, `call`, `retry-wakeup`)**: Configures chat bindings and executes binding-scoped operations over authenticated loopback HTTP with streamed response size enforcement.
  - **Scoped stdio MCP server (`threadlane mcp`)**: Built on the official Rust MCP SDK (`rmcp = "=3.8.0"`). Launched for a single pre-configured `binding_id`; discovery works while the supervisor is offline, and every tool call revalidates the current private descriptor. It exposes 15 tools, including read-only `list_activity`, bounded run history, explicit event pull/acknowledgement, demo lifecycle operations, and worker start/query/cancellation. See [bounded API and schema changes](docs/BOUNDED-DATA.md).
  - **Opt-in Codex wake-up adapter (`--codex` + `--notify-binding`)**: Disabled by default. When explicitly configured with an absolute executable path and a binding allowlist, persists `InFlight` intent before invoking `["queue", "--thread", "<UUID>", "--message", FIXED_WAKEUP_MESSAGE]` with a bounded timeout and no model, effort, approval, or sandbox overrides.
  - **Self-contained CLI smoke script (`scripts/smoke.py`)**: Exercises the simulated loop in an isolated temporary directory without touching live chats or paid APIs.

### Worker Diagnostics (Milestone 2 Foundation)
- **`threadlane-workers`**: Separate library and trusted `threadlane-worker` diagnostic CLI for exact provider argv, incremental bounded output, requested/observed model and effort evidence, Unix process-group supervision, owned task/integration worktrees and post-run diff checks.
- **Acceptance stays explicit**: Exit zero and a passing diff are insufficient to accept a patch. Missing provider evidence, blocked work, incomplete cleanup and incomplete diff reports remain visible. Diff checks do not prevent filesystem writes while a worker runs.
- **Validation scope**: Linux process/Git regressions use isolated fixtures. One live Antigravity transport smoke confirmed the requested model; the provider did not report effort, so it remained unverified. OpenCode uses fixture and installed-CLI checks; live Go execution is still unverified. See [worker usage and limits](docs/WORKERS.md).

### Durable Worker Dispatch (Milestone 2B)

- **Trusted startup profiles**: Optional private `--workers-config` fixes provider, executable, model/effort, exact base commit, repository, work area, write scope and budgets. Callers select an authorized `profile_id`; they cannot override those settings. Dispatch is disabled by default.
- **Durable lifecycle**: SQLite claims precede external effects. Duplicate requests replay without another spawn; revision/claim checks reject stale callbacks. Uncertain jobs become `Unknown` on restart without automatic replay or unverified PID killing.
- **Scoped runtime operations**: `start_worker`, `get_worker` and `cancel_worker` support bounded live progress, per-task worktrees, configured concurrency (`1..=8`), private optional report artifacts and the existing event/ACK/wake-up path.
- **Acceptance stays separate**: A successful execution is only ready for review. Native print-mode worker Answer/Steer, live provider coding acceptance and reviewed merge/cleanup remain separate gates. See [dispatch usage and limits](docs/DISPATCH.md).

### Scoped Provider Permissions and Native Pilot (Milestone 2C)

- **Explicit opt-in**: Trusted profiles may select `file_permissions: "task_files"` for Antigravity. Session-only grants use the actual task directory and write allowlist. Default Inherit preserves the original argv and profile fingerprint.
- **Finite grants**: Ambiguous rule paths, existing symlink components, more than 128 write entries or 65,536 serialized settings bytes are rejected. These grants are not an OS sandbox; inherited provider policy and post-run diff checks still matter.
- **Measured native loop**: One authorized native chat launched a real Argon task and received its terminal event through MCP/wake-up/ACK. A real one-file edit and unchanged source were independently confirmed. Missing effort stayed Failed/ExecutionFailed; no patch was automatically accepted or merged. A separate local fake-process cancellation loop also passed.

- **Repeatable operator fixture**: [live_worker_pilot.py](scripts/live_worker_pilot.py) prints a plan by default and invokes a provider only with explicit `--allow-live`, executable, model and effort. It uses a new private disposable repository and preserves evidence; see [usage](docs/WORKERS.md#repeatable-operator-pilot).

### Remaining Milestones
- **Milestone 2 remaining integration**: Interactive provider question/Steer delivery, busy-turn and multi-chat acceptance, reconciliation of preserved uncertain artifacts, and reviewed Git integration/cleanup. The durable fixture checkpoint is not the full native workflow.
- **Desktop configuration and observation**: [Tauri desktop panel](docs/DESKTOP.md) (`Activity`, `Chats`, `Plans`, `Tools`, `Profiles`, `Settings`), editable global models/efforts and role/difficulty profiles, a separate local demo, read-only history, and configurable theme/accent/transparency.
- **Milestone 4 execution checkpoint**: Durable draft/publication MCP operations, immutable operator approvals, separate per-approval host processes, and caller-scoped frozen-task dispatch are implemented with local fixture coverage. Jev/OpenAI Decisions adapters provide explicit synthetic tests; live credentials and automatic workflow classification are separate gates. The [configuration guide](docs/CONFIGURATION.md) distinguishes approval, host startup, dispatch, and review.
- **Milestone 5**: Cross-platform release hardening, macOS/Windows native ACLs and secret storage, optional background service/tray packaging, and measured cost/notification budgets.

### Optional Approved-Session Controls

- **Codex-first initial startup**: An operator may opt in with
  `threadlane-native-mcp --auto-start-approved-hosts` and the configured stores.
  An explicit approved root-task dispatch can then launch its first fresh host
  and wait for authenticated readiness. It cannot create approval or restart an
  uncertain session. New-scope approval currently uses desktop confirmation;
  a trusted headless approval route remains planned. See the
  [native conversation workflow](docs/CODEX-WORKFLOW.md).
- **Explicit idle stop and stopped-session restart**: Operator controls use
  authenticated owner scope and per-start instance identity. Active jobs prevent
  idle stop; restart preserves receipts without replay and resets notification
  opt-in. Closing the window leaves the approval host running.
- **Native metadata context**: Read-only chat lists reject stale tool/filter/
  registry replies, preserve input focus, and retain at most 100 distinct threads.
  A missing executable path uses PATH discovery. Loading metadata never enrolls
  a chat or grants approval.
- **Validation limits**: Local tests include a real host CLI-process lifecycle.
  Native multi-chat delivery and real provider compatibility remain separate
  gates. No new lifecycle MCP tools are exposed. See
  [session lifecycle](docs/SESSION-LIFECYCLE.md).

OpenCode v2 [metadata validation](docs/OPENCODE-METADATA.md) is available as a pure library boundary using
bounded, already-loaded inputs and per-assistant model/effort fields. It performs
no session discovery or export and is not yet connected to the runtime worker
collector. The existing missing-evidence gate remains in force.

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
   - `status` returns a compact snapshot (binding metadata, wake-up notice state, and short recent run summaries without historical prompts/questions); the current snapshot is fetched via `get_run`, and historical records use the paginated history tools.
4. **Idempotency & Injected Time**:
   - The supervisor injects current wall-clock `now_ms` on all commands.
   - Mutating commands (`start_demo`, `ack_events`, `answer`, `steer`, `cancel`, `start_worker`, `cancel_worker`) deduplicate by `(binding_id, command_key)` excluding `now_ms`. Retrying the same key and parameters returns the original result (including across supervisor restarts); reusing a key with different parameters returns `idempotency_conflict`.
5. **Ordered Events, Explicit Acknowledgements & Coalesced Wake-ups**:
   - `pull_events` returns a bounded batch of per-binding ordered events (`seq = 1..N`), records a server-generated `batch_id`, and advances `delivered_seq`. Reading events does not acknowledge them.
   - `ack_events` validates `batch_id` ownership and contiguous high-water-mark boundaries (`acked_seq`). Callers cannot acknowledge unseen, skipped, or cross-binding batches; repeated acknowledgements are harmless and never move `acked_seq` backwards.
   - At most one wake-up notice is active per binding. Important demo and terminal/unknown worker events coalesce into that notice. Live progress alone does not produce a notification for every output line.
   - Wake-up claims transition `Pending -> InFlight` in SQLite before the external Codex queue command is spawned, then record `Submitted` (exit 0), `Failed` (spawn error), or `Unknown` (timeout, non-zero/signal termination, or crash recovery on reopen). Timed-out child processes are killed and reaped. `Submitted`, `Failed`, and `Unknown` claims are never automatically replayed; only a valid `ack_events` covering `covered_up_to_seq` releases the notice (transitioning to `Pending` if newer unacknowledged important events remain, or `Idle` otherwise).

---

## Demo Limits and Platform Support

- **Deterministic Simulation Only**: Milestone 1 runs a simulated state machine (`Running -> WaitingInput -> Completing -> Completed` or `Cancelled`). It does not invoke real coding agents or prove external worker crash recovery.
- **Live Native Codex Acceptance**: Passed on Linux/NixOS in one explicitly authorized disposable chat. Native chat history records real MCP calls and both question/completion wake-ups; final runtime state has no active runs, equal delivered/acknowledged cursors and an idle wake-up notice. The answer was preauthorized synthetic demo input, not a production user decision. Temporary configuration and test processes were removed afterward. See [validation evidence](docs/VALIDATION.md).
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
cargo build --workspace --locked
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
copying the instruction. `get_run` returns the current instruction and bounded active-question detail.
Use `list_run_revisions` and `list_run_questions` for historical records.
See [the bounded-data contract](docs/BOUNDED-DATA.md) for pagination and replay limits. To enable worker dispatch, create a binding first, then restart the supervisor with a private startup profile authorizing that binding; see [DISPATCH.md](docs/DISPATCH.md). Fixed-binding MCP configuration is not automatic native chat identity verification; see [NATIVE-ROUTING.md](docs/NATIVE-ROUTING.md).

### Running Tests and Smoke Check
```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked -- --test-threads=1
python3 scripts/smoke.py
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p test_live_worker_pilot.py -v
```

---

## AI-Assisted Development Note

This codebase is developed with AI assistance under human direction and explicit architectural invariants: implementation slices and automated tests are reviewed and validated before checkpointing. AI-generated plans or reviews are not treated as runtime proof; only reproducible builds, tests, and observed execution artifacts count as verification.
