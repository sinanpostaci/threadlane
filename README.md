# Threadlane

Threadlane is a local execution middleware and control plane designed to work alongside a native Codex conversation. The user remains in their native Codex chat while Threadlane owns validated task state, durable event streams, revision checks, and process supervision.

Everyday planning, questions, Steer, and work follow-up belong in Codex. The
desktop is an optional observer and configuration surface. Initial pairing and
trusted scope approval remain explicit setup boundaries; ordinary work should
not require switching to the panel for each task. See
[session lifecycle and its current limits](docs/SESSION-LIFECYCLE.md).

> **Current product:** the minimal chat-centered MVP passed its qualified
> Linux/NixOS acceptance on 2026-11-13. Native Codex remains the Coworker;
> Threadlane shows one chat event stream, a small Todo/Doing/Done/Blocked backlog,
> and real fresh activity. Ordinary work has no mandatory Jobs, Orchestrator,
> Reviewer or Closure stage. Read the [current contract](docs/MINIMAL-CHAT-MVP.md)
> and [dated acceptance report](docs/MINIMAL-CHAT-MVP-ACCEPTANCE-2026-11-13.md) for
> exact artifact/model/platform boundaries. Source publication does not activate
> a host system upgrade.

The milestone sections below preserve earlier implementation history and optional
capabilities. Their historical limitations and multi-role workflows do not
replace the current minimal product contract or impose mandatory roles on it.

---

## Earlier milestones and optional capabilities

### Conversation Runtime and Bounded History (Milestones 1 and 2A)
- **`threadlane-core` (`crates/threadlane-core`)**: Transactional SQLite state engine (`SCHEMA_VERSION = 4`, with transactional v1/v2/v3 upgrades) with validated non-nil Codex thread UUID bindings, deterministic simulated demo runs, revision-checked `answer` / `steer` / `cancel` transitions, per-binding compact idempotent command replay, bounded keyset-paginated run/revision/question history, bounded event batches, high-water-mark event acknowledgements, and persistent wake-up notice coalescing and crash/reopen recovery.
- **`threadlane` runtime (`crates/threadlane`)**:
  - **Single-owner supervisor (`threadlane serve`)**: Holds an exclusive process-lifetime file lock (`supervisor.lock`) tied to the active state engine, owns the SQLite database (`state.db`) and timed demo progression ticks, binds only `127.0.0.1:0`, and writes an atomic private runtime descriptor (`supervisor.json`) containing a fresh 64-hex-character bearer token (244 random bits from two v4 UUIDs).
  - **Trusted CLI (`threadlane bind`, `list-bindings`, `status`, `call`, `retry-wakeup`)**: Configures chat bindings and executes binding-scoped operations over authenticated loopback HTTP with streamed response size enforcement.
  - **Scoped stdio MCP server (`threadlane mcp`)**: Built on the official Rust MCP SDK (`rmcp = "=3.8.0"`). Launched for a single pre-configured `binding_id`; discovery works while the supervisor is offline, and every tool call revalidates the current private descriptor. It exposes 16 tools, including the versioned operating guide, read-only `list_activity`, bounded run history, explicit event pull/acknowledgement, demo lifecycle operations, and worker start/query/cancellation. See [bounded API and schema changes](docs/BOUNDED-DATA.md).
  - **Opt-in Codex wake-up adapter (`--codex` + `--notify-binding`)**: Disabled by default. When explicitly configured with an absolute executable path and a binding allowlist, persists `InFlight` intent before invoking `["queue", "--thread", "<UUID>", "--message", FIXED_WAKEUP_MESSAGE]` with a bounded timeout and no model, effort, approval, or sandbox overrides.
  - **Explicit approved-session tool-output transport**: An operator can select a private Unix WebSocket context with `--notify-tool-output-context`. Machine notices use `functionCallOutput` with empty user input, preserve the existing durable claim/ACK lifecycle, and never fall back to queue text. Controlled idle and busy delivery were confirmed in one Linux desktop test chat. A separate [qualified product pilot](docs/NATIVE-TOOL-OUTPUT-PILOT-2026-10-31.md) verified a real DeepSeek Go task, native Luna pull/ACK and scoped teardown. Multi-chat identity, restart recovery and other platforms remain separate gates. See [native tool-output notifications](docs/NATIVE-TOOL-OUTPUT.md) and the [optional Linux desktop bridge](tools/native-bridge/README.md).
  - **Opt-in Linux endpoint reconnect**: A frozen application/backend selector can follow the existing launcher's new private descriptor after restart. Bounded pre-write retries recheck process ancestry, kernel peer credentials and exact chat settings. A potentially submitted notification remains `Unknown` and is never automatically replayed. Host startup after reboot stays explicit. See [automatic endpoint refresh](docs/NATIVE-TOOL-OUTPUT.md#automatic-endpoint-refresh-on-linux).
  - **Self-contained CLI smoke script (`scripts/smoke.py`)**: Exercises the simulated loop in an isolated temporary directory without touching live chats or paid APIs.

### Worker Diagnostics (Milestone 2 Foundation)
- **`threadlane-workers`**: Separate library and trusted `threadlane-worker` diagnostic CLI for exact provider argv, incremental bounded output, requested/observed model and effort evidence, Unix process-group supervision, owned task/integration worktrees and post-run diff checks.
- **Acceptance stays explicit**: Exit zero and a passing diff are insufficient to accept a patch. Missing provider evidence, blocked work, incomplete cleanup and incomplete diff reports remain visible. Diff checks do not prevent filesystem writes while a worker runs.
- **Validation scope**: Linux process/Git regressions use isolated fixtures. One live Antigravity transport smoke confirmed the requested model; the provider did not report effort, so it remained unverified. A separate disposable three-task OpenCode Go pipeline completed with CLI-reported per-assistant model/effort evidence, review/integration receipts and eight focused integration checks on `0e94e17`; broader live provider compatibility remains unverified. See the earlier checkpoint note in the [2026-10-31 report](docs/NATIVE-CLI-PILOT-2026-10-31.md). See [worker usage and limits](docs/WORKERS.md).

### Durable Worker Dispatch (Milestone 2B)

- **Trusted startup profiles**: Optional private `--workers-config` fixes provider, executable, model/effort, exact base commit, repository, work area, write scope and budgets. Callers select an authorized `profile_id`; they cannot override those settings. Dispatch is disabled by default.
- **Durable lifecycle**: SQLite claims precede external effects. Duplicate requests replay without another spawn; revision/claim checks reject stale callbacks. Uncertain jobs become `Unknown` on restart without automatic replay or unverified PID killing.
- **Scoped runtime operations**: `start_worker`, `get_worker` and `cancel_worker` support bounded live progress, per-task worktrees, configured concurrency (`1..=8`), private optional report artifacts and the existing event/ACK/wake-up path.
- **Acceptance stays separate**: A successful execution is only ready for review. Native print-mode worker Answer/Steer, live provider coding acceptance, and automatic merge/push remain separate gates. Reviewed commit, acceptance, and same-run integration are explicit operator CLI actions; they do not auto-accept or publish. See [dispatch usage and limits](docs/DISPATCH.md) and the [operator workflow](docs/OPERATOR-WORKFLOW.md).

### Scoped Provider Permissions and Native Pilot (Milestone 2C)

- **Explicit opt-in**: Trusted profiles may select `file_permissions: "task_files"` for Antigravity. Session-only grants use the actual task directory and write allowlist. Default Inherit preserves the original argv and profile fingerprint.
- **Finite grants**: Ambiguous rule paths, existing symlink components, more than 128 write entries or 65,536 serialized settings bytes are rejected. These grants are not an OS sandbox; inherited provider policy and post-run diff checks still matter.
- **Measured native loop**: One authorized native chat launched a real Argon task and received its terminal event through MCP/wake-up/ACK. A real one-file edit and unchanged source were independently confirmed. Missing effort stayed Failed/ExecutionFailed; no patch was automatically accepted or merged. A separate local fake-process cancellation loop also passed.

- **Repeatable operator fixture**: [live_worker_pilot.py](scripts/live_worker_pilot.py) prints a plan by default and invokes a provider only with explicit `--allow-live`, executable, model and effort. It uses a new private disposable repository and preserves evidence; see [usage](docs/WORKERS.md#repeatable-operator-pilot).

### Remaining Milestones
- **Milestone 2 remaining integration**: Interactive provider question/Steer delivery, general per-call native identity and authenticated two-chat routing, and broader provider compatibility. Busy-turn callback passed in one trusted local operator CLI conversation; preserved uncertain artifacts still require explicit reconciliation. Receipt-bound dependency inputs and the trusted review/integration CLI are implemented with their own same-run and receipt checks; they do not auto-accept, merge, push, or start a child without a separate explicit dispatch. See [the operator CLI pilot](docs/NATIVE-CLI-PILOT-2026-10-31.md).
- **Desktop configuration and observation**: A React/Radix [desktop workspace](docs/DESKTOP.md#current-desktop-workspace) with a collapsible projects/chat sidebar, selected-chat activity, expandable job/task states, real profile lists and difficulty overrides. A small-window drawer, local reversible history archive, tray close preferences and validated Open in Codex action keep observation separate from execution authority. [Request observation](docs/OBSERVATION.md), editable tools/models and appearance settings remain available.
- **Milestone 4 execution checkpoint**: Durable draft/publication MCP operations, immutable operator approvals, separate per-approval host processes, and caller-scoped frozen-task dispatch are implemented with local fixture coverage. Jev/OpenAI Decisions adapters provide explicit synthetic tests; live credentials and automatic workflow classification are separate gates. The [configuration guide](docs/CONFIGURATION.md) distinguishes approval, host startup, dispatch, and review.
- **Milestone 5**: Linux release/lifecycle hardening, measured cost and notification budgets, then macOS native qualification. Shared source preparation does not establish macOS support; Windows implementation is deferred.

### Optional Approved-Session Controls

- **Installed CLI versions**: The desktop checks enabled local tools once per
  startup, with manual refresh and explicit System/Custom executable selection.
  Current Nix profile discovery avoids stale desktop-bundled Codex paths. See
  [tool installations](docs/TOOL-INSTALLATIONS.md).

- **First-use checks and recovery overview**: Settings → Connections shows a read-only local
  setup checklist for installed tools, model/effort assignments, saved profiles
  and the selected real runtime binding, plus the startup `mcp_servers.threadlane`
  registration status (`threadlane-codex-setup`) with explicit Recheck/Retry
  controls and separate conversation pairing status. Startup also installs or updates
  the owned user-level `$threadlane` skill, with optional `profile=...`, independently
  of MCP pairing and planning/execution approval. Manual skill edits are preserved.
  Activity highlights uncertain or unsuccessful recent runs and evidence review,
  without replaying work.
  See [first everyday use](docs/FIRST-USE.md) and [Codex MCP startup setup](docs/CODEX-SETUP.md).

- **Crash recovery**: Task state and events are persisted; interrupted workers
  become unknown and are reconciled before continuation. Provider requests are
  never blindly replayed. See [recovery and current limits](docs/RECOVERY.md).

- **Packaged context commands**: `threadlane-context --context /private/context.json
  check` validates the selected private context and live target. Use the same
  prefix with `workflow ...`, `flow ...`, `session ...` or `advice ...` for
  everyday commands. Executables are fixed package siblings and bound scope
  flags cannot be overridden. The router itself does not create stores, start a
  daemon, grant approval, or call a provider; the advice sibling contacts Jev
  only for explicit `classify` or `models`. See
  [native context commands](docs/NATIVE-CONTEXT.md).

- **Shadow difficulty advice**: `threadlane-advice` and the `advice` context
  family expose an enabled Jev tool's server catalog and explicit advisory
  classification. `preview` performs no provider call or vault read; `classify`
  is one request or a completed cache hit. Results are advisory-only and never
  change plans, profiles, difficulty, models, approvals, hosts, workers or Git.
  See [Jev shadow advice](docs/JEV-SHADOW.md).

- **Optional decision advisor**: Profiles may reference a global decision API
  model separately from the five execution roles (`Orchestrator`, `Architect`,
  `Worker` / canonical `coder`, `Reviewer`, and `Closure`). User-facing surfaces
  display `Worker` for task execution (`WorkflowRole::display_label`, `catalog`
  `role_label: "Worker"`, and CLI/`save_draft` `"worker"` input normalization) while
  retaining canonical `coder` (`WorkflowRole::Coder`) in stored profiles, plan/approval
  records, and v1 digests; tasks remain bounded to repository worktrees and
  repository-relative write scopes. Advisors default to off, have no effort setting,
  and provide shadow advice only. Saving a choice sends no provider request. See
  [profile decision advisors](docs/PROFILE-ADVISOR.md).

- **Profile-selected role cycles**: `threadlane-flow` validates only requested
  roles against an explicit model/effort policy, freezes an approved cycle, and
  records idempotent role handoffs. Native Codex handoffs remain manual routing;
  headless tasks use the existing approved host. A closure reservation prevents
  repeated closing handoffs for the same approved package candidate. See
  [profile flows](docs/PROFILE-FLOW.md) and [commands](docs/ROLE-FLOW-CLI.md).
- **Truthful progress and conversation changes**: The desktop distinguishes
  execution from verified review/integration receipts, including historical or
  invalidated evidence. `threadlane-session` records questions, clarifications,
  scope/model changes and explicit cancellation without claiming unsupported
  live provider steering. Blocking changes require a new plan/approval before
  advancement. See [task lifecycle](docs/TASK-LIFECYCLE.md) and
  [conversation changes](docs/CONVERSATION-CHANGES.md).

- **Trusted local review and integration**: `threadlane-operator` provides
  explicit task commit, review inspection/acceptance, and owned integration
  commands. It preserves the existing digest/receipt and same-run checks; no
  main merge, push or dependent-task dispatch follows automatically. See the
  [operator workflow](docs/OPERATOR-WORKFLOW.md).

- **Reviewed dependent tasks**: An explicit local
  `threadlane-host prepare-dependencies` call combines accepted predecessor heads into a frozen
  input commit. Approved child dispatch uses the same task-ID interface, with
  bound input proofs and no automatic release when a parent exits. See
  [dependency inputs](docs/DEPENDENCIES.md).

- **Codex-first initial startup**: An operator may opt in with
  `threadlane-native-mcp --auto-start-approved-hosts` and the configured stores.
  An explicit approved root-task dispatch can then launch its first fresh host
  and wait for authenticated readiness. It cannot create approval or restart an
  uncertain session. New-scope approval can use desktop confirmation or the
  trusted local `threadlane-approve` preview/record commands. See
  [native scope approval](docs/NATIVE-APPROVAL.md) and the
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

- **Native operator CLI pilot (2026-10-31)**: One Linux/NixOS trusted local
  operator CLI conversation received a real callback after a busy turn, pulled and
  acknowledged a six-event batch, canceled an OpenCode process, and passed
  explicit stop/restart without replay. The cancellation's `system_init`
  step-start is not model-output, effort, or remote-termination proof; no code was
  accepted. See [pilot evidence](docs/NATIVE-CLI-PILOT-2026-10-31.md).

OpenCode v2 [metadata validation](docs/OPENCODE-METADATA.md) is integrated into the
runtime worker path. New software-owned OpenCode sessions are allocated through the
bounded CLI collector, then validated from that exact session's info and sanitized
export after a clean process exit; existing-session jobs retain stream-only evidence.
The collector performs no session discovery, does not query IDs from model prose,
and keeps raw export content in memory. The missing-evidence gate remains in force,
and CLI-reported metadata is not cloud attestation.

Antigravity [catalog-variant evidence](docs/ANTIGRAVITY-EVIDENCE.md) verifies exact
known concrete model IDs from structured CLI initialization, preserving the
distinction between catalog selection and direct effort telemetry. Unknown IDs
and contradictory metadata still fail the evidence gate.

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
