# Codex CLI Mutable Task Worker (`threadlane-task-files`)

This document describes the worker-layer execution contract for mutable headless `CodexCli`
(`WorkerProvider::CodexCli`) task-worktree runs in `threadlane-workers`.

## Scope and Distinction from Read-Only Advisory Runs

`threadlane-workers` supports two distinct, mutually exclusive `CodexCli` launch profiles:

1. **Read-Only Advisory (`AdvisoryWorkerSpec`, `WorkerFilePermissions::ReadOnly`)**:
   - Uses `default_permissions="threadlane-advisory"` and the static
     `CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG` inline profile (`"."="read"`, protected denies,
     `network.enabled=false`).
   - Verified by `is_pinned_codex_read_only_argv` and
     `WorkerExecutionReport::has_pinned_codex_read_only_launch_config`
     (`codex_launch_pin_evidence_mode() == Some("codex_read_only_launch_pin")`).
2. **Mutable Task Worktree (`WorkerSpec`, `WorkerFilePermissions::TaskFiles`)**:
   - Uses `--ignore-rules`, `default_permissions="threadlane-task-files"`, and a
     workdir/base-commit-resolved `permissions.threadlane-task-files={...}` inline profile
     constructed via `WorkerSpec::build_argv_for_workdir`.
   - Verified by `is_pinned_codex_task_files_argv`,
     `extract_pinned_codex_task_files_write_scopes`, and
     `WorkerExecutionReport::has_pinned_codex_task_files_launch_config`
     (`codex_launch_pin_evidence_mode() == Some("codex_task_files_launch_pin")`).
   - `WorkerFilePermissions::Inherit` is explicitly refused for `WorkerProvider::CodexCli` with
     `WorkerError::UnsupportedCapability`.

## Pinned CLI Launch Shape

Mutable `CodexCli` worker tasks are spawned without a shell in an isolated Git task worktree with
the exact argument vector:

```text
--no-daemon
exec
--json
--ephemeral
--ignore-user-config
--ignore-rules
--model <resolved_model>
-c model_reasoning_effort="<effort>"
-c approval_policy="never"
-c model_provider="openai"
-c default_permissions="threadlane-task-files"
-c permissions.threadlane-task-files={"filesystem"={"glob_scan_max_depth"=64,":minimal"="read",":workspace_roots"={"."="read",<approved-write-entries>,<protected-deny-entries>},"/nix/store"="read"},"network"={"enabled"=false}}
-c web_search="disabled"
--disable hooks
--disable plugins
--disable apps
--disable multi_agent
--disable browser_use
--disable computer_use
--disable in_app_browser
--disable image_generation
--disable memories
--disable skill_search
--disable skill_mcp_dependency_install
--disable shell_snapshot
-- <prompt>
```

Both launch profiles use the exact ordered `CODEX_ADVISORY_DISABLED_FEATURES` list above.
The installed Codex 0.167.0 `--no-daemon features list` recognizes all twelve names;
`hooks` is its stable key. This metadata check performs no inference and does not qualify
runtime capability enforcement.

Never combined with legacy `--sandbox` (`workspace-write`, `danger-full-access`), `--full-auto`,
`--dangerously-bypass-approvals-and-sandbox`, or persistent app-server daemons.

## Preventive Permission Profile vs. Post-Run Git Diff Gate

- **Preventive CLI configuration (`threadlane-task-files`)**:
  - Reads are bounded to the owned task checkout (`"."="read"`), `:minimal`, and `/nix/store`.
  - Writes are granted only to the finite repository-relative `allowed_write_paths` resolved
    against the pinned `base_commit` (`WriteScopeKind::ExactFile` or
    `WriteScopeKind::DirectorySubtree`). Bare entries always mean exact files; directory
    entries require a trailing `/`. A derived execution base that changes a bare file scope
    into a directory is rejected before launch; it cannot expand the frozen scope.
  - Write scopes are validated by `validate_codex_task_files_scope_str`, which rejects glob/rule
    metacharacters as well as TOML inline-table and quoted-string delimiters (`"`, `'`, `,`, `=`,
    `:`, `;`, `#`, `` ` ``, and whitespace) before interpolation.
  - Protected paths (`CODEX_TASK_FILES_PROTECTED_DENY_PATTERNS`: `.git`, `.codex`, `.agents`,
    `.aws`, `.env*`, `codex.toml`, `.codex.toml`, `AGENTS.md`, `CLAUDE.md`, `opencode.json*`,
    `.gemini`, `.opencode`, `.claude`, `.github`, `.cargo/config*`) are explicitly set to `"deny"`.
  - Pre-spawn workdir validation (`validate_codex_task_files_workdir`) rejects symlinks, project
    config markers (`.codex`, `codex.toml`, `.codex.toml`), and pathological directory
    trees (`depth > 62` or `entries > 32,768`). Nonexistent write scopes deeper than 62
    components are also rejected before profile rendering.
- **Post-run Git diff gate (`evaluate_diff_gate`)**:
  - After process exit, Threadlane evaluates the actual Git working tree and index against
    `base_commit`. Any modification or untracked file outside the approved write scopes or touching
    a protected control/config path fails the diff gate (`WorkAcceptanceState::RejectedByDiffGate`).

## Honest Launch-Pin vs. Runtime Observed Evidence and Bounded Turn Lifecycle

`codex exec --json` emits lifecycle JSONL events (`thread.started`, `turn.started`, `item.*`,
`turn.completed` / `turn.failed` / `error`) and token usage, but does **not** emit runtime
`model` or `effort` telemetry in the event stream.

- `WorkerExecutionReport::effective_observed_model()` and
  `WorkerExecutionReport::effective_observed_effort()` remain `None` when absent from the JSONL
  stream; Threadlane never copies `requested_model` into observed fields.
- `NdjsonStreamParser` tracks bounded O(1) Codex turn lifecycle state in
  `StreamEvidenceSummary::codex_turn_lifecycle` (`CodexTurnLifecycleEvidence`) without relying on
  `retained_events`.
- `has_satisfied_model_evidence()` and `has_satisfied_effort_evidence()` may use verified
  launch pins when the stream omits those fields; an observed mismatch still fails.
  `has_complete_natural_completion_evidence()` accepts `CodexCli` runs only when:
  1. `has_pinned_codex_launch_config()` is `true` (exact verified argv, model, effort, and
     validated `threadlane-advisory` or `threadlane-task-files` permission profile),
  2. no stream model or effort mismatch occurred, and
  3. `has_valid_codex_turn_lifecycle()` is `true` (an exact single `thread.started -> turn.started -> [item.*] -> turn.completed` sequence with a valid `thread_id`, zero partial/delta/truncation markers, zero duplicate or out-of-order lifecycle events, zero trailing events after `turn.completed`, zero provider errors/blocked events/malformed lines), and
  4. the process exited with code `0`, without signal, timeout, overflow, or incomplete cleanup.
- Strict observed-evidence rules for `Antigravity` and `OpenCodeV2` remain unchanged.

Command `aggregated_output` is opaque text: an output-limit notice inside that string does
not invalidate an otherwise complete lifecycle. Explicit partial/delta/truncation markers
on lifecycle or item metadata fail closed. No installed-schema evidence currently establishes
that a tool item's `truncated` flag represents only optional stdout truncation, so that
metadata shape remains unqualified. Terminal/lifecycle truncation always rejects completion.

Persisted read-only advisory artifacts from before the lifecycle/executable markers existed
use `has_complete_legacy_codex_advisory_evidence()` and
`AdvisoryStructuredExecution::validate_persisted_verified_response(max_bytes)` during immutable
integrity reconstruction. This path requires both markers to be absent, exact historical
read-only argv, and clean process/session/model/effort/result evidence. It preserves response
hash, byte budget, framing, session and usage checks without inventing observations or lifecycle
state. Current execution collection and `validate_verified_response` remain strictly framed;
mutable `TaskFiles` reports cannot use historical advisory compatibility.

## Qualification Status

- **Linux synthetic fixture qualification**: Covered by unit and fake-Codex process-group/worktree
  tests in `crates/threadlane-workers/tests/codex_coder.rs`. These include an approved file
  write, forbidden diff rejection, malformed lifecycle/identity/usage, cancellation and timeout
  descendant cleanup, and absent observed model/effort with genuine fixture token counts.
- **Live provider and non-Linux acceptance**: Pending live human-authorized acceptance testing;
  synthetic CLI fixtures verify Threadlane's launch construction, stream parser, and post-run Git
  diff gate, not upstream OpenAI/Codex CLI runtime sandbox enforcement.

## Frozen approved workflow

The approved Coworker controller path supports Codex CLI Coder tasks, including dependency-bound
tasks and authorized in-scope corrections. Exact provider/model/effort policy admission precedes
Coder normalization. Approval previews freeze provider, canonical executable, model and effort,
`standalone=false`, TaskFiles permissions and finite write scopes into the approval digest.
A new explicitly selected `gpt-6-luna` / `max` Coder can use this route; other explicitly admitted
Codex CLI targets use the same contract. No runtime registry or native Coworker model is changed.

Persisted native Codex approvals with `provider=None` retain their existing digest and manual
routing. They are never reinterpreted as writable worker contracts. Generic role-flow preflight
and manual handoffs also retain native semantics. Future-call profile adoption is separate.

For original and dependent runs, the host checks the immutable private task report against the
frozen executable/model/effort, exact preventive permission argv, profile digest, owned manifest,
repository/work-area, run/worker identity and write scope. Native attempt artifacts and digest-bound
review policies retain a sanitized launch report separately from optional observed model/effort: its
prompt is replaced, retained events and stderr summary removed, and its immutable source digest
recorded. Verification, review, integration and correction reread and match that source. Derived
attempts retain the verified correction sidecar identity/digest and reuse the original frozen scope.

Bare write paths remain exact files; trailing `/` denotes a directory subtree. A dependency or
correction base that changes an exact file into a directory cannot broaden preventive permissions.
Missing pins, malformed/failed lifecycle output, conflicting telemetry, missing source reports or
foreign execution scope fail closed. Checks, Reviewer acceptance, integration and exactly one
accepted Closure remain required. Unknown effects are reconciled without redispatch.

The Linux deterministic full-pipeline fixture in `tests/support/coworker_full_driver.rs` exercises
two parallel Codex roots, a rejected candidate with one correction, accepted predecessor receipts,
a dependent Codex task, checks, reviews, integration and one Closure. It also checks absent
observed telemetry, immutable proof failures and idempotent reopening. Live provider/sandbox
qualification and non-Linux acceptance remain separate gates.
