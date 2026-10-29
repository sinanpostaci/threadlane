# Milestone 2B: Durable Worker Dispatch (`threadlane`)

This document specifies the Milestone 2B runtime worker dispatch architecture implemented in `crates/threadlane` on top of the single-writer SQLite engine in `crates/threadlane-core` and the process/worktree supervision foundation in `crates/threadlane-workers`.

---

## 1. Architecture Overview

Threadlane's supervisor (`crates/threadlane/src/supervisor.rs` and `crates/threadlane/src/workers.rs`) integrates bounded, real CLI worker execution into the existing single-writer `Core` state machine and durable event/wake-up pipeline:

1. **Single SQLite Writer (`threadlane-core`)**: All worker admissions (`StartWorker`), dispatcher claims (`claim_worker`), running transitions (`record_worker_running`), bounded live progress updates (`record_worker_progress`), terminal outcomes (`record_worker_outcome`), revision-checked cancellation requests (`CancelWorker`), and crash/shutdown uncertainty recoveries (`mark_active_workers_unknown`) flow through the single supervisor-owned `Core` instance and emit ordered events on the binding's durable event stream.
2. **No Lock Held Across External I/O**: The supervisor acquires the `Core` mutex only for brief, synchronous SQLite transactions. Git worktree creation (`WorktreeManager`), child process execution (`execute_worker_task_with_observer`), and post-run diff-gate evaluation execute asynchronously outside the `Core` mutex.
3. **Bounded Parallelism (`JoinSet`)**: The background worker loop owns a `tokio::task::JoinSet` of active worker invocations bounded by `max_parallel` (`1..=8`). Demo run ticks (`tick_due_runs`) never advance or mutate real worker runs.

---

## 2. Private Startup Worker Profile Configuration (`--workers-config`)

Worker dispatch is **disabled by default** (`SupervisorConfig::workers = None`). When disabled, any `start_worker` request over HTTP, CLI, or MCP is rejected with `ErrorCode::InvalidConfig` before admitting a run.

To enable worker dispatch, pass `--workers-config <PATH>` to `threadlane serve`.

### 2.1 File and Schema Validation Rules

`WorkersConfig::load_from_path` enforces the following invariants before starting the supervisor:

- **Canonical, non-symlink private file**: The path and all parent components must be free of symlinks and `..` traversal components. On Unix, the file must be a regular file with exact mode `0600` (verified both before and after opening with `dev`/`ino` consistency checks).
- **Bounded file size**: Rejects files larger than `MAX_WORKERS_CONFIG_FILE_BYTES` (`64 KiB`) using a bounded reader (`Read::take`).
- **Strict schema (`deny_unknown_fields`)**: Unknown fields are rejected at the root config level, profile level, `spec` template level, and `budgets` level.
- **No embedded credentials or environment overrides**: Any key or string value containing credentials (`api_key`, `secret`, `password`, `credential`, `auth_token`, `access_token`, `bearer`, `env`, `environment`, or token patterns caught by `sanitize_and_redact`) is rejected. Worker CLIs use their existing credential stores and the supervisor process environment; the profile file is not a secret store.
- **Concurrency and profile bounds**:
  - `max_parallel`: `1..=8` (defaults to `2`).
  - `profiles`: `1..=32` uniquely named profiles (`profile_id` `1..=64` ASCII identifier characters).
  - `allowed_binding_ids`: Non-empty set of binding IDs authorized to invoke the profile.
- **Exact 40-hex `base_commit` requirement**: Every profile's `base_commit` must be an exact 40-character hexadecimal Git commit SHA (`validate_exact_commit_sha`). Mutable symbolic refs such as `"HEAD"`, branch names (`"main"`), or abbreviated SHAs are rejected so an immutable profile digest cannot silently change its target commit as the source repository's `HEAD` moves.
- **Side-effect-free validation**: Startup config validation reads file/directory metadata and validates `WorkerSpec` arguments without creating `work_area_root`, without `chmod`ing existing directories, without creating Git worktrees, and without spawning any child processes.

### 2.2 Example `--workers-config` JSON

```json
{
  "max_parallel": 2,
  "profiles": [
    {
      "profile_id": "rust_backend",
      "allowed_binding_ids": ["bnd_0123456789abcdef0123456789abcdef"],
      "spec": {
        "provider": "antigravity",
        "executable": "/usr/local/bin/agy",
        "model": "gemini-4-argon-high",
        "effort": "high",
        "allowed_models": ["gemini-4-argon-high"],
        "standalone": false,
        "repository_path": "/home/user/project",
        "base_commit": "0123456789abcdef0123456789abcdef01234567",
        "work_area_root": "/home/user/.local/share/threadlane/work_area",
        "allowed_write_paths": ["crates/threadlane", "docs"],
        "budgets": {
          "timeout_ms": 300000,
          "max_line_bytes": 65536,
          "max_stdout_bytes": 2097152,
          "max_stderr_bytes": 262144,
          "max_events": 1024
        }
      }
    }
  ]
}
```

### 2.3 Immutable `profile_digest`

At validation time, each `ValidatedWorkerProfile` computes a canonical 64-character SHA-256 hex digest (`profile_digest`) over the complete trusted execution profile:

- `profile_id` and sorted `allowed_binding_ids`
- `provider`, canonical `executable`, `base_model`, `requested_effort`, `cli_model_arg`, sorted `allowed_models`, `standalone`, and `session_id`
- canonical `repository_path`, normalized 40-hex `base_commit`, `work_area_root`, and sorted `allowed_write_paths`
- `budgets` (`timeout_ms`, `max_line_bytes`, `max_stdout_bytes`, `max_stderr_bytes`, `max_events`)

The `profile_digest` is persisted on the worker record at admission time (`StartWorker`) and verified on `claim_worker` and all subsequent worker callbacks. A changed startup configuration with the same `profile_id` produces a different `profile_digest` and cannot silently execute a request queued under an older profile definition.

---

## 3. Scoped Operations & Security Boundaries

The runtime exposes three binding-scoped worker operations across HTTP (`POST /v1/bindings/{binding_id}/command`), `ScopedClient`, CLI (`threadlane call`), and stdio MCP (`ThreadlaneMcpServer`):

| Operation / MCP Tool | Mutating | Caller Arguments | Response |
| :--- | :--- | :--- | :--- |
| `start_worker` | Yes (`command_key`) | `command_key`, `profile_id`, `instruction` | `WorkerSnapshot` |
| `get_worker` | No | `run_id` (or alias `worker_id`) | `WorkerSnapshot` |
| `cancel_worker` | Yes (`command_key`) | `command_key`, `run_id` (or alias `worker_id`), `expected_revision`, optional `reason` | `WorkerSnapshot` |

### 3.1 Caller Restrictions

- **No untrusted overrides**: Callers cannot specify or override `executable`, `model`, `effort`, `repository_path`, `work_area_root`, `allowed_write_paths`, `budgets`, `base_commit`, `profile_digest`, `binding_id`, or `now_ms`. Extra fields in HTTP/CLI payloads are rejected with `ErrorCode::ValidationFailed`. The MCP SDK rejects unknown arguments before executing the tool handler, returning `isError: true` with parameter-deserialization error text rather than a structured application error.
- **Pre-admission authorization**: The supervisor verifies that worker dispatch is enabled, `profile_id` exists in the startup registry, and the request's `binding_id` is in `profile.allowed_binding_ids` before admitting a run into `Core`.
- **No dynamic profile registration**: Registering or mutating worker profiles via HTTP or MCP is forbidden.
- **Queue-independent idempotency**: Repeating `start_worker` with the same `(binding_id, command_key, profile_id, instruction)` returns the original `WorkerSnapshot` immediately without queuing a second run, even when all `max_parallel` worker slots are currently busy. Reusing a `command_key` with different parameters, a changed `profile_digest`, or a different operation fails with `ErrorCode::IdempotencyConflict`.
- **Accurate `demo_only` labeling**: MCP tool responses set `"demo_only": false` on `start_worker`, `get_worker`, `cancel_worker`, and shared query tools (`status`, `get_run`, `list_runs`, `list_run_revisions`, `list_run_questions`, `pull_events`, `ack_events`). `"demo_only": true` is restricted exclusively to simulated demo operations (`start_demo`, `answer`, `steer`, `cancel`).

---

## 4. Dispatch Lifecycle, Worktrees, and Live Progress

```
Queued ──(claim_worker)──► Claimed ──(record_worker_running)──► Running
  │                           │                                    │
  │                           ├─(CancelWorker)─► CancelRequested ◄─┤
  │                           │                         │          │
  ▼                           ▼                         ▼          ▼
Cancelled / Unknown        Unknown        Cancelled / Completed / Failed / TimedOut / Unknown
```

1. **Admission (`Queued`)**: `StartWorker` creates linked `runs` and `workers` records at `revision = 1`, emits `EventKind::WorkerQueued`, and notifies the background dispatcher.
2. **Claim (`Claimed`)**: When `worker_tasks.len() < max_parallel`, the dispatcher lists queued workers (`Core::list_queued_workers`), verifies the profile's `profile_digest`, and atomically claims the worker (`Core::claim_worker`, advancing `revision` and persisting `claim_id` and `attempt_count = 1`) **before** any filesystem or process side effect.
3. **Isolated Worktree Creation**: Inside `spawn_blocking`, `prepare_claim_worktrees` uses `WorktreeManager` to create:
   - an owned per-run integration worktree (`wt-run-<r_tag>-<token>`) at `base_commit`, and
   - an owned per-task execution worktree (`wt-task-<r_tag>-<w_tag>-<token>`) branched from the run worktree.
   Compact deterministic SHA-256 tags (`r_<12hex>` and `w_<12hex>`) ensure composite worktree IDs always stay within `threadlane-workers`'s 64-character identifier limit without prefix collisions.
4. **Running Transition (`Running`)**: `Core::record_worker_running` records the owned worktree IDs, paths, branch names, and resolved `base_commit`, and emits `EventKind::WorkerStarted`.
5. **Bounded Live Progress Streaming**:
   - `execute_claimed_worker` invokes `execute_worker_task_with_observer` with a bounded `mpsc::channel(64)` (`LIVE_OBSERVER_CHANNEL_CAPACITY`).
   - The stdout parser forwards normalized events via nonblocking `try_send` so a slow consumer can never block child stdout pipe draining; any overflow increments `dropped_live_events`.
   - The supervisor drains the channel in bounded batches, sanitizes and secret-redacts summaries (`sanitize_and_redact`), and calls `Core::record_worker_progress`.
   - Durable `EventKind::WorkerProgress` events are capped at `MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT` (`32`) per attempt; further progress updates increment `coalesced_progress_events` and update `last_summary` without growing the event log unboundedly.
6. **Post-Run Diff Gate & Terminal Outcome**:
   - After child exit and process-group cleanup, `execute_worker_task_with_observer` runs the post-run Git diff gate against `allowed_write_paths`.
   - `build_completion_record` constructs a compact `WorkerCompletionRecord` and writes a private (`0600`), finite-size (`<= 128 KiB`) local report artifact in `<state_dir>/worker_reports/art_<worker_id>.json` with `constructed_argv` cleared to avoid duplicating prompt arguments. The report directory must be private and free of symlink components. Existing files or links are never overwritten; an unsafe path or unavailable artifact leaves `report_artifact_id = null` without changing the genuine worker outcome.
   - `Core::record_worker_outcome` atomically verifies `(binding_id, run_id, worker_id, revision, claim_id, profile_digest)` and transitions the worker to `Completed`, `Failed`, `Cancelled`, `TimedOut`, or `Unknown`, emitting an important durable event (`WorkerCompleted`, `WorkerFailed`, `WorkerCancelled`, `WorkerTimedOut`, or `WorkerUnknown`) that coalesces into the binding's Codex wake-up notice.
   - **No Auto-Acceptance**: A clean exit (`0`), satisfied model/effort/result evidence, and passing diff gate set `WorkerAcceptanceState::ReadyForReview` (never automatically accepted).
   - **Worktree Preservation**: Source repositories and owned run/task worktrees (including dirty worktrees rejected by the diff gate) are preserved on disk for inspection and review.

---

## 5. Cancellation, Crash Recovery, and Fault Boundaries

- **Revision-Checked Cancellation & Completion Races**:
  - `cancel_worker` requires `expected_revision`. Calling `cancel_worker` with a stale revision fails with `ErrorCode::StaleRevision`.
  - When `cancel_worker` is accepted on an active worker, the worker transitions to `WorkerStatus::CancelRequested` and signals the worker task's cancellation watch channel.
  - **Cancellation vs. Natural Completion Race**: `WorkerStatus::CancelRequested` and proven `WorkerStatus::Cancelled` are distinct. If a worker process had already emitted complete valid completion evidence (`init`, verified `model`, verified `effort`, `result`, zero errors/blocks) and exited `0` without signal termination right as cancellation arrived, `build_completion_record` retains the genuine `WorkerStatus::Completed` (`ReadyForReview`) result while setting `outcome.cancel_requested_before_completion = true` and `outcome.cancelled = false`.
- **Crash/Restart Recovery (`Unknown`, Never Auto-Retried)**:
  - On supervisor startup (`Supervisor::start`) and shutdown/drop, `Core::mark_active_workers_unknown` transitions any worker in `Queued`, `Claimed`, `Running`, or `CancelRequested` to `WorkerStatus::Unknown` (`WorkerAcceptanceState::Unknown`), emits an important `EventKind::WorkerUnknown` event, and arms the binding's wake-up notice.
  - Uncertain workers are **never** automatically re-queued or re-invoked (`attempt_count <= 1`), and startup recovery never performs blind PID kills across restarts.
- **Shutdown, Drop, and Fatal SQLite Errors**:
  - Explicit shutdown signals active workers to cancel, waits for bounded `JoinSet` drainage, and marks interrupted non-user-cancelled workers `Unknown` before retiring ownership. Dropping the handle retires ownership immediately and signals cancellation; any remaining uncertain records become `Unknown` on reopen. Retired background tasks cannot mutate `Core`.
  - A fatal SQLite storage error immediately records a supervisor fault, cancels all active workers, and rejects subsequent mutating requests with the recorded `ErrorCode::StorageError`.
- **Wire Budgets & Secret Redaction**:
  - Compact responses (`WorkerSnapshot`, `StatusResponse`, `Run`) enforce `MAX_COMPACT_RESPONSE_BYTES` (`64 KiB`) and paged responses (`EventBatch`, `RunListPage`) enforce `MAX_PAGE_RESPONSE_BYTES` (`256 KiB`), accounting for JSON-escaped text expansion.
  - Raw stdout/stderr streams and argv arrays are not copied into snapshots or wake-up messages. The bounded caller instruction is intentionally retained in private state and authorized run/worker snapshots; it is not included in progress events or wake-up messages. Parsed worker summaries are bounded and redacted. Redaction is best effort and is not a secret-isolation boundary.

---

## 6. Explicitly Deferred and Unsupported Boundaries

1. **No Interactive `Answer`, `Steer`, or Generic Demo `Cancel` on Worker Runs**:
   - Print-mode CLI workers (`antigravity` and `opencode_v2`) execute non-interactively. Calling `answer`, `steer`, or the simulated demo `cancel` operation on a worker run is explicitly rejected with `ErrorCode::InvalidRunState` before any state mutation. Worker cancellation must use `cancel_worker` with `expected_revision`.
2. **Deferred Accepted-Result Merge and Worktree Cleanup**:
   - Automatic branch merge/push and post-review cleanup of accepted worktrees are deferred to a later milestone.
3. **Native Codex Busy-Turn Wake-Up Limitation**:
   - Wake-ups continue to use `codex queue --thread <UUID> --message <FIXED_WAKEUP_MESSAGE>`. An idle deterministic native conversation passed its earlier gate; real-worker busy-turn delivery and general multi-chat routing have not passed native acceptance. See [NATIVE-ROUTING.md](NATIVE-ROUTING.md).
4. **Provider Verification Scope**:
   - Both `antigravity` (`agy -p --output-format stream-json --model <model> --effort <effort>`) and `opencode_v2` (`opencode --standalone run --format json --model <provider/model#variant>`) are supported by the spec validator and NDJSON parser. Automated runtime integration tests use deterministic fake provider scripts and never invoke live paid model APIs.


## Validation and operating limits

Schema v3 upgrades existing v1/v2 state transactionally. Restart reconciliation
records uncertain workers as `Unknown`; it does not establish that an old process
has stopped. Do not automatically re-run or clean up such a task.

Startup profiles are an operator-controlled vertical slice, not the global
Tools/Profiles UI. Write scopes are a post-run diff gate, not an OS filesystem
sandbox. Provider file/tool permissions still apply; a fresh noninteractive AGY
worktree can be blocked without appropriately scoped provider permissions. The
current product adapter does not install the session-only settings used by the
development runner, and does not bypass provider permission checks.

The primary agent owns the acceptance report in [VALIDATION.md](VALIDATION.md).
Fixture transport, parser, and lifecycle success do not establish a live paid
OpenCode/Go run or native real-worker end-to-end acceptance. No provider's effort
is assumed from requested arguments alone.
