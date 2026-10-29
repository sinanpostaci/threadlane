# Threadlane M2 Worker Supervision and Git Worktree Foundation (`threadlane-workers`)

`threadlane-workers` provides the isolated Milestone 2 foundation for launching external
coding workers (`antigravity` and `opencode_v2`), capturing and verifying incremental
NDJSON stream evidence, supervising Unix process groups, and managing isolated Git
worktrees with post-run diff gates and ownership-verified cleanup.

## Scope and Non-Claims

- **Independent foundation crate**: `crates/threadlane-workers` is intentionally decoupled
  from `threadlane-core` and `threadlane` so it can be tested and reviewed
  independently of the durable supervisor/MCP dispatch described in [DISPATCH.md](DISPATCH.md).
- **Not a public MCP launch permission**: The included `threadlane-worker` binary is a
  narrow local diagnostic CLI for exercising specs, worktrees, process supervision, diff
  gates, and cleanup. It does not expose an MCP server, install a persistent service, or
  grant autonomous launch permissions to untrusted callers.
- **Separate completion and acceptance states**: A worker exiting `0` with verified stream
  evidence (`WorkerRunStatus::Succeeded`) and a passing diff gate
  (`WorkAcceptanceState::DiffGatePassedPendingReview`) are distinct from reviewed,
  accepted work. This crate never performs automatic `git merge`, `git rebase`, `git push`,
  `git reset`, or `git worktree remove --force`.

---

## Public Types and Library API

### 1. Worker Specification and Policy (`threadlane_workers::spec`)

- `WorkerProvider`:
  - `WorkerProvider::Antigravity` (canonical serde `"antigravity"`, alias `"agy"`)
  - `WorkerProvider::OpenCodeV2` (canonical serde `"opencode_v2"`, aliases `"open_code_v2"`, `"opencode-v2"`, `"opencode"`)
- `ExecutionBudgets`:
  - `timeout_ms` (`1..=3_600_000`, default `300_000`)
  - `max_line_bytes` (`1..=4_194_304`, default `262_144`)
  - `max_stdout_bytes` (`1..=33_554_432`, default `4_194_304`, must be `>= max_line_bytes`)
  - `max_stderr_bytes` (`1..=4_194_304`, default `262_144`)
  - `max_events` (`1..=4_096`, default `256`)
- `WorkerSpec`:
  - `provider`: `WorkerProvider`
  - `executable`: Absolute path to a regular, non-symlink, executable file.
  - `model`: Exact requested model identifier (`gemini-4-argon-high` for Antigravity, or
    `provider/model` / `provider/model#variant` for OpenCode v2). Validated as an arbitrary
    safe identifier rather than a frozen catalog; never silently substituted.
  - `effort` (JSON alias `variant`): Required for `antigravity`; optional for `opencode_v2`
    (where it formats `--model provider/model#variant`). Conflicting inline `#variant` and
    explicit `effort` values are rejected during validation.
  - `allowed_models`: Optional trusted allowlist of permitted model identifiers.
  - `session_id`: Optional `--session <ID>` (supported only for `opencode_v2`; rejected on
    `antigravity`).
  - `standalone`: Boolean flag prepending global `--standalone` before `run`. Must be `true`
    for `opencode_v2` (JSON OpenCode specs must explicitly set `"standalone": true` so each
    worker runs an owned private server rather than attaching to an unowned shared background
    server) and must be `false` (or omitted) on `antigravity`.
  - `repository_path`: Canonical path to the trusted Git repository (or linked Git worktree).
  - `base_commit`: Commit ref or 40-hex SHA resolved and verified prior to worktree creation.
  - `work_area_root`: Private directory outside `repository_path` (and its common Git directory)
    where owned checkouts and private ownership manifests reside.
  - `allowed_write_paths`: Non-empty set of normalized repository-relative paths. Protected
    control configuration paths (`.git`, `.gitignore`, `.gitattributes`, `.gitmodules`,
    `AGENTS.md`, `CLAUDE.md`, `.codex`, `.gemini`, `.opencode`, `.agents`, `.github`,
    `.cargo/config*`, `.env*`) are always rejected.
  - `prompt`: Untrusted task prompt (`1..=65_536` bytes; `MAX_PROMPT_BYTES = 65_536` / 64 KiB,
    kept conservatively below Linux's 128 KiB per-argument `MAX_ARG_STRLEN` `E2BIG` ceiling)
    passed strictly as data in the argument array.
  - `file_permissions`: `WorkerFilePermissions`, default `"inherit"`. Opt-in
    `"task_files"` resolves session-only AGY grants from the actual task directory.
  - `budgets`: `ExecutionBudgets`.
- `WorkerPolicy`: External allowlist of canonical executable paths (`allowed_executables`)
  and models (`allowed_models`), enforced via `WorkerSpec::validate_against_policy`.

### 2. Argument Array Construction (`WorkerSpec::build_argv`, `build_argv_for_workdir`)

All processes are spawned with explicit argument arrays (`Command::new(&executable).args(&argv)`),
never via a shell (`sh -c`). Neither `--dangerously-skip-permissions` nor `--auto` is ever
used, and unsupported flags such as `--variant` are never invented. OpenCode positional
prompts are preceded by the `--` option terminator so prompt text (e.g. `"--auto"`) can
never be interpreted as a CLI flag.

- **Antigravity (`agy`)**:
  ```text
  [
    "--model", "<model>",
    "--effort", "<effort>",
    "--mode", "accept-edits",
    "--output-format", "stream-json",
    "--print-timeout", "0",
    "--print", "<prompt>"
  ]
  ```
- **OpenCode v2 (`opencode`)**:
  ```text
  [
    "--standalone",
    "run",
    "--model", "<provider/model[#variant]>",
    "--format", "json",
    // "--session", "<session_id>" (when spec.session_id.is_some()),
    "--",
    "<prompt>"
  ]
  ```

### Scoped Antigravity file permissions (Milestone 2C)

`file_permissions: "inherit"` preserves the previous argv and default runtime
profile fingerprint. `"task_files"` is an operator-selected Antigravity-only
option: it adds session-only `--settings <inline-json>` granting task-tree reads
and exact/descendant file-edit rules for the existing write allowlist. It adds no
shell, network or MCP grants and does not modify global provider settings.
Inherited provider rules may still apply; this is not an OS sandbox or global
deny isolation.

The actual existing task working directory and existing components of each
allowlisted target must be canonical non-symlink paths. Validation does not
recursively scan all descendants for symlinks before granting a directory rule;
post-run diff checks remain mandatory. Glob/rule metacharacters and control
characters are rejected in interpolated paths. TaskFiles has at most 128 write
entries and 65,536 serialized settings bytes; excess is rejected before spawn,
not truncated. Inherit retains its previous path behavior.

`validate()` stays side-effect free and marks TaskFiles argv as an incomplete
static preview requiring working-directory resolution. `build_argv()` rejects
that unresolved mode; `build_argv_for_workdir()` returns the actual argv. The
supervisor executes and records this resolved argv, while private runtime report
artifacts continue to clear prompt argv. Missing observed model/effort evidence
is not repaired by the permission option.

### 3. Incremental NDJSON Evidence Parser (`threadlane_workers::stream`)

`NdjsonStreamParser` processes stdout line-by-line within bounded memory and produces a
`StreamEvidenceSummary`:

- `FieldEvidence`:
  - `VerifiedMatch { value }`: Stream emitted explicit evidence matching the requested value.
  - `Mismatch { requested, observed, detail }`: Stream emitted a value differing from the
    requested value or emitted conflicting values across events in the same stream.
  - `Missing { requested }`: A value was requested by `WorkerSpec`, but the stream never
    emitted evidence for it. Effort is never synthesized from requested CLI flags or model
    name suffixes.
  - `NotRequested { observed }`: No value was requested (e.g., optional `session_id` or
    optional OpenCode variant), recording whatever value was observed.
- **Supported NDJSON Event Shapes (Fake Fixture Verified)**:
  - **Installed Antigravity (`agy --output-format stream-json`) Envelope (`"event"` key)**:
    - Init: `{"event": "init", "conversation_id": "fixture-session", "init": {"model": "gemini-4-argon-high", "cwd": "/canonical/worktree", "permission_mode": "request-review"}}`
    - Step update: `{"event": "step_update", "step_update": {"conversation_id": "fixture-session", "step_index": 1, "state": "DONE", "step_type": "agent_response", "usage": {}}}`
    - Result (success): `{"event": "result", "result": {"conversation_id": "fixture-session", "status": "SUCCESS", "response": "Finished", "num_turns": 1}}`
    - Result (permission blocked via `denied_actions` even when `status == "SUCCESS"`):
      `{"event": "result", "result": {"conversation_id": "fixture-session", "status": "SUCCESS", "response": "", "denied_actions": [{"action": "read_file", "display_name": "ReadFile"}]}}`
    - Result (failure): `{"event": "result", "result": {"conversation_id": "fixture-session", "status": "FAILURE", "error": "..."}}`
    - Recoverable step tool errors (`step_update` with `state: "ERROR"` / `error` or `tool_result` with `is_error: true` without permission/input block) increment `recoverable_step_errors_count` and are retained in `retained_events` without poisoning a subsequent valid `result` event.
    - Note: Because the installed `agy` stream schema emits `init.model` and `conversation_id` but does not emit an `effort` field, `effort_evidence` honestly records `FieldEvidence::Missing { requested: "high" }`, resulting in `WorkerRunStatus::MissingEffortEvidence` and (when the diff gate passes) `WorkAcceptanceState::FinishedNeedsEvidenceAndReview`.
  - **OpenCode v2 (`opencode run --format json`) & Generic `"type"` Envelope**:
    - Init / step start: `{"type": "step_start", "sessionID": "...", "part": {"type": "step-start", "model": "provider/model#variant"}}`, `{"type": "system", "subtype": "init", ...}`, or `{"type": "session", "providerID": "...", "modelID": "...", "variant": "..."}`
    - Blocked: Events with `denied_actions` or `type` / `subtype` / `status` / `stop_reason` / `error.code` indicating `permission`, `permission_request`, `permission_required`, `permission_denied`, `approval_required` (`BlockedKind::PermissionRequired`) or `question`, `input_required`, `user_input_required`, `blocked` (`BlockedKind::UserInputRequired`).
    - Provider Error: Top-level error events (`event`/`type: "error"`, `subtype: "error"` / `"error_during_execution"`) or terminal result events with `status: "FAILURE"` / `"ERROR"` / `"FAILED"` or error payloads.
    - Terminal Result: `{"type": "step_finish", "part": {"type": "step-finish", "reason": "stop"}}` or `{"type": "result", "subtype": "success", ...}`.
- **Secret Redaction**:
  - Event summaries, malformed line snippets, and stderr summaries pass through
    `sanitize_and_redact`, replacing common credential patterns (`Bearer ...`, `sk-...`,
    `AIza...`, `ghp_...`, `github_pat_...`, `api_key=...`, `token=...`, `secret=...`,
    `password=...`) with `[REDACTED]` and truncating to fixed byte limits.

### 4. Unix Process-Group Supervision (`threadlane_workers::supervisor`)

`run_worker` and `run_worker_with_cancel` execute a validated `WorkerSpec` inside a
validated checkout directory:

1. Spawns the worker with `stdin` set to `Stdio::null()`, piped `stdout`/`stderr`, and
   `process_group(0)` so the worker becomes the leader of a dedicated process group
   (`pgid == child_pid`, verified `pgid > 1` and `pgid != caller_pgid()`).
2. Attaches a `ProcessGroupGuard` RAII drop guard to `pgid` immediately after spawn. If the
   `run_worker` / `run_worker_with_cancel` future is dropped or aborted before normal
   completion, `ProcessGroupGuard::drop` signals `-pgid` with `SIGTERM` and `SIGKILL` so
   descendant processes are not orphaned.
3. Drains `stdout` and `stderr` concurrently in `4096`-byte chunks, enforcing
   `max_line_bytes`, `max_stdout_bytes`, and `max_stderr_bytes` before buffering.
4. On timeout, explicit cancellation (`cancel_rx` transitioning to `true`; `false` updates
   are ignored), or stream output overflow:
   - Sends `SIGTERM` to `-pgid`, polls briefly for exit, escalates to `SIGKILL` on `-pgid`,
     reaps the direct child via `child.wait()`, and verifies no non-zombie process remains
     in `pgid`.
5. When the direct child exits:
   - Allows a brief drain window for buffered pipe data; if any descendant process in `pgid`
     survived the parent's exit, terminates `-pgid` (`SIGTERM` -> `SIGKILL`) and sets
     `descendants_terminated_after_exit = true`.
   - Enforces a bounded post-termination pipe join (`POST_EXIT_PIPE_DRAIN_TIMEOUT_MS = 250`).
     If an escaped/reparented descendant outside `pgid` still holds `stdout` or `stderr`
     pipes open after group termination, the reader tasks stop waiting, `cleanup_incomplete`
     is set to `true`, and the run is classified as `WorkerRunStatus::IncompleteCleanup`
     rather than hanging or succeeding.
6. Classifies `WorkerRunStatus`:
   - `Succeeded` requires exit code `0`, `cleanup_incomplete == false`, zero malformed
     lines, zero provider errors, zero blocked events, `saw_result_event == true`,
     `model_evidence == VerifiedMatch`, and satisfied `effort_evidence` and `session_evidence`.
   - Other outcomes: `Blocked`, `ProviderError`, `ModelMismatch`, `MissingModelEvidence`,
     `EffortMismatch`, `MissingEffortEvidence`, `SessionMismatch`, `MissingResultEvidence`,
     `MalformedStream`, `OutputOverflow`, `TimedOut`, `Cancelled`, `IncompleteCleanup`,
     `NonZeroExit`.

## Live progress observation

`run_worker_with_observer` and `execute_worker_task_with_observer` add an optional
bounded `tokio::sync::mpsc::Sender<NormalizedWorkerEvent>` as their final argument.
Existing entry points retain their signatures and delegate with no observer.
Normalized events are emitted while the worker is running. The sender uses
`try_send`, so a slow, full or closed observer cannot block stdout drainage,
timeout/cancellation or final model/result verification. The final report's
`dropped_live_events` counts undelivered live notifications separately from the
parser's retained-event truncation counter. With no observer it remains zero.

This library interface is connected to the durable supervisor/MCP dispatcher;
see [DISPATCH.md](DISPATCH.md) for its runtime profile and lifecycle contract.

### 5. Git Worktree Manager, Diff Gate, and Safe Cleanup (`threadlane_workers::git`)

- **Work-Area Ownership Marker and Preservation (`WorktreeManager::new`)**:
  - Resolves and validates `repository_path`, its common Git directory (`git rev-parse --git-common-dir`,
    supporting both standard repositories with a `.git` directory and linked worktrees with a
    `.git` file), and `base_commit` **before** performing any filesystem operations on
    `work_area_root`.
  - Refuses any pre-existing non-empty `work_area_root` that lacks a valid private (`0600`)
    `.threadlane-work-area.json` (`WorkAreaOwnershipMarker`) or contains unexpected entries,
    and refuses any existing directory whose permissions are not already `0700` **before**
    changing permissions or creating subdirectories.
- **Directory Layout inside `work_area_root` (`0700`)**:
  - `.threadlane-work-area.json` (`0600`): Private work-area ownership marker binding
    `work_area_root` to the repository's canonical common Git directory.
  - `manifests/<worktree_id>.json` (`0600`): Private `WorktreeManifest` stored **outside**
    the checkout so the worker cannot accidentally tamper with ownership metadata.
  - `manifests/<worktree_id>.lock` (`0600`): Exclusive `flock` held while a job is active.
  - `checkouts/runs/wt-run-<run_id>-<token>`: Per-run integration worktree on branch
    `threadlane/run/<run_id>/<token>`.
  - `checkouts/tasks/wt-task-<run_id>-<task_id>-<token>`: Per-task worktree on branch
    `threadlane/task/<run_id>/<task_id>/<token>`.
- **Post-Run Diff Gate & Bounded Git Process-Group Supervision (`evaluate_diff_gate` / `evaluate_diff_gate_with_limits` / `evaluate_diff_gate_with_timeout` / `evaluate_diff_gate_with_scan_limits`)**:
  - Every Git subprocess spawned by `threadlane-workers` runs in a dedicated Unix process
    group (`process_group(0)`) with nonblocking (`O_NONBLOCK` + `libc::poll`) concurrent
    `stdout`/`stderr` pipe drains, a finite wall-clock deadline
    (`DEFAULT_GIT_COMMAND_TIMEOUT_MS = 10_000`), and early overflow termination (`SIGTERM` ->
    `SIGKILL` on `-pgid`) before closing pipes and reaping the child. A Git command or hook
    that overflows `stdout` while holding `stderr` open, sleeps past the deadline, or spawns
    an escaped descendant that holds pipes open past `POST_EXIT_PIPE_DRAIN_TIMEOUT_MS`
    (`250 ms`) is terminated and bounded without blocking on `thread::scope` reader joins.
  - Executes diff-gate Git queries using bounded pipe capture (`DEFAULT_MAX_GIT_OUTPUT_BYTES = 4 MiB`),
    bounds the changed-path list (`DEFAULT_MAX_DIFF_CHANGED_PATHS = 4,096`), and bounds the
    non-following checkout directory scan by total visited entries (`DEFAULT_MAX_SCAN_ENTRIES = 32,768`)
    and recursion depth (`DEFAULT_MAX_SCAN_DEPTH = 64`). If any Git query, changed-path list,
    or checkout scan exceeds its byte/count/entry/depth budget, times out, or reports incomplete
    pipe/process-group cleanup, `DiffGateReport` sets `evidence_complete: false`,
    `passed: false`, `acceptance_state: RejectedByDiffGate`, and records a
    `DiffViolationKind::DiffBudgetExceeded` violation rather than silently truncating or
    passing partial diff evidence.
  - **Strict UTF-8 Path Encoding**: Repository and worktree relative paths must be valid UTF-8
    (including valid Unicode codepoints such as literal `U+FFFD`). Path-bearing Git metadata
    outputs (`git status`, `git diff -z`, `git ls-files -z`, `git ls-tree -z`) and checkout
    directory entries are validated strictly as UTF-8 rather than decoded with
    `String::from_utf8_lossy` / `to_string_lossy`. Any raw non-UTF-8 Unix filename fails
    closed (`evidence_complete: false`, `passed: false`, `acceptance_state: RejectedByDiffGate`,
    `DiffViolationKind::PathEscape`) so non-UTF-8 byte sequences can never collide with an
    allowed `U+FFFD` path.
  - Compares the owned checkout against `manifest.base_commit` (including staged, unstaged,
    committed, renamed, deleted, binary, and all untracked files without `--exclude-standard`
    so `.gitignore` cannot hide unauthorized files, plus a non-following directory walk for
    changed or newly created symlinks).
  - Queries tracked baseline symlinks (`mode == 120000`) in `base_commit` via
    `git ls-tree -r -z <base_commit>` so unchanged tracked baseline symlinks are not
    rejected, while modified baseline symlinks, newly created symlinks, and changes
    traversing a symlink are rejected with `DiffViolationKind::SymlinkDetected`.
  - Rejects `OutsideAllowedPaths`, `SymlinkDetected`, `PathEscape`,
    `ControlConfigModified`, and `DiffBudgetExceeded`.
  - Labels all reports with `isolation_level: PostRunDiffEvidence` and
    `preventive_sandbox: false`.
- **Safe Cleanup (`cleanup_worktree`)**:
  - Requires a `CleanupRequest { work_area_root, worktree_id }` backed by a verified private
    work-area marker and manifest in `<work_area_root>/manifests/<worktree_id>.json` (never
    deletes an arbitrary caller-supplied path).
  - Refuses cleanup with `WorkerError::CleanupRefused` when:
    - `MissingOrInvalidManifest`: Work-area marker or manifest is missing, malformed, or
      fails ownership checks.
    - `ActiveJob`: `job_active == true`, `active_job_pid` is alive, or `.lock` is held.
    - `UnexpectedBranchOrOwner`: Checkout HEAD is detached, on a different branch, or its
      resolved `git-common-dir` does not match `manifest.repository_path`'s resolved
      `git-common-dir`.
    - `DirtyWorktree`: Checkout has modified, staged, untracked, or ignored files, or `HEAD`
      diverges from `manifest.base_commit`.

---

## Diagnostic CLI Usage (`threadlane-worker`)

### Example `worker-spec.json` (Antigravity)

```json
{
  "provider": "antigravity",
  "executable": "/abs/path/to/agy",
  "model": "gemini-4-argon-high",
  "effort": "high",
  "allowed_models": ["gemini-4-argon-high"],
  "repository_path": "/abs/path/to/repo",
  "base_commit": "0123456789abcdef0123456789abcdef01234567",
  "work_area_root": "/abs/path/to/work-area",
  "allowed_write_paths": ["crates/threadlane-workers", "docs/WORKERS.md"],
  "prompt": "Implement the requested module changes.",
  "budgets": {
    "timeout_ms": 300000,
    "max_line_bytes": 262144,
    "max_stdout_bytes": 4194304,
    "max_stderr_bytes": 262144,
    "max_events": 256
  }
}
```

### Commands

```bash
# 1. Validate a JSON worker spec (TaskFiles shows a static preview/resolution note)
cargo run -p threadlane-workers --bin threadlane-worker -- \
  validate-spec --spec /abs/path/to/worker-spec.json

# 2. Create owned per-run integration and per-task worktrees
cargo run -p threadlane-workers --bin threadlane-worker -- \
  create-worktrees --spec /abs/path/to/worker-spec.json --run-id run-01 --task-id task-01

# 3. Run the configured worker inside its owned task worktree and evaluate the diff gate
cargo run -p threadlane-workers --bin threadlane-worker -- \
  run --spec /abs/path/to/worker-spec.json --run-id run-01 --task-id task-01 \
  --worktree-id wt-task-run-01-task-01-<token>

# 4. Re-evaluate the post-run diff gate on an owned worktree
cargo run -p threadlane-workers --bin threadlane-worker -- \
  diff-gate --work-area /abs/path/to/work-area --worktree-id wt-task-run-01-task-01-<token>

# 5. Explicitly clean up a clean, inactive, owned worktree
cargo run -p threadlane-workers --bin threadlane-worker -- \
  cleanup --work-area /abs/path/to/work-area --worktree-id wt-task-run-01-task-01-<token>
```

---

## Recovery, Isolation, and Verification Limits

1. **Post-run diff gate vs OS sandboxing**:
   Workers execute as standard Unix user processes with `current_dir` set to the owned task
   worktree. Because this milestone does not wrap workers in a Linux namespace/Landlock/bwrap
   sandbox, a worker process could write outside `allowed_write_paths` during execution.
   `DiffGateReport` explicitly records `preventive_sandbox: false` and
   `isolation_level: "post_run_diff_evidence"`.
2. **Crash recovery and no automatic replay**:
   If a worker run times out, overflows, crashes, or is interrupted, `threadlane-workers`
   never automatically replays the invocation. If the runner process itself crashes while a
   job is in flight, `job_active: true` remains persisted in
   `<work_area_root>/manifests/<worktree_id>.json`, causing `cleanup_worktree` and
   `begin_worktree_job` to refuse further mutation until the worktree is inspected.
3. **Provider verification boundary**:
   Automated tests in this crate use deterministic fake executable fixtures modeling the
   documented CLI flags and NDJSON schemas for `agy` (`--output-format stream-json`) and
   `opencode` (`run --model provider/model#variant --format json`). If a live provider CLI
   version omits `model` or `effort`/`variant` metadata from its JSON stream events,
   `threadlane-workers` will report `MissingModelEvidence` or `MissingEffortEvidence` rather
   than assuming unverified compliance. One live Argon transport smoke confirmed the model
   but returned `MissingEffortEvidence`, because effort was absent from the stream. Live
   OpenCode/Go execution is unverified. See [validation evidence](VALIDATION.md).
4. **Platform support**:
   Unix process-group signaling (`setpgid` / `kill(-pgid, ...)`) and `0700`/`0600` manifest
   permission checks are implemented for Unix and verified target-side on Linux/NixOS.
   macOS behavior has not been tested. Windows process-tree lifecycle support (such as
   Job Objects) and native ACL enforcement are not implemented; non-Unix execution returns
   `WorkerError::UnsupportedPlatform`. No cross-platform build or runtime claim is made.
