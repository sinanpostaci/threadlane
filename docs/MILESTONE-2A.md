# Milestone 2A: Bounded History, Paging, and Wire-Budget Contract

Milestone 2A (`M2A`) implements the bounded-data prerequisite identified in `docs/CLOSURE-REVIEW.md` and `docs/ROADMAP.md` ahead of Milestone 2B (`M2B`) real worker adapters (AGY and OpenCode). All run, history, status, mutation, and event routes exposed over the supervisor HTTP API, CLI, and MCP server are bounded in SQLite queries and on the wire.

## 1. Summary of Contract Changes

### 1.1 Constant-Size `Run` Snapshots
- `Run` no longer embeds `revision_history: Vec<RunRevision>` or `questions: Vec<Question>`.
- Instead, `Run` carries:
  - `active_question_id: Option<String>`
  - `active_question: Option<Question>` (populated when `active_question_id` is `Some`, providing the active question's `question_id`, `revision`, `prompt`, `status`, `created_at_ms`, and `untrusted_data: true` needed to answer without extra round-trips)
  - `untrusted_data: bool` (always `true`)
- `GetRun`, `StartDemo`, `Answer`, `Steer`, and `Cancel` return `CommandOutput::Run(Box<Run>)` (`ScopedResponse::Run(Run)`), bounded by `MAX_COMPACT_RESPONSE_BYTES = 65,536` bytes regardless of how many historical revisions or questions a run has accumulated.

### 1.2 Ascending Keyset-Paginated Run & History Endpoints
- The unbounded `Command::ListRuns -> CommandOutput::Runs(Vec<Run>)` variant is replaced by paged `Command::ListRuns { cursor, limit } -> CommandOutput::RunList(RunListPage)`.
- Three paged query operations are exposed across `threadlane_core::Core`, `Command` / `CommandOutput`, `ScopedRequest` / `ScopedResponse`, `ScopedClient`, `threadlane call`, and stdio MCP (`ThreadlaneMcpServer`):
  1. `list_runs` (`cursor: Option<String>`, `limit: Option<usize>`): binding-scoped ascending creation order (`ORDER BY rowid ASC`), returning `RunListPage { binding_id, runs: Vec<Run>, next_cursor, has_more, high_water_mark_rowid }`. Each element in `runs` is a compact current `Run` snapshot.
  2. `list_run_revisions` (`run_id: String`, `cursor: Option<String>`, `limit: Option<usize>`): binding-and-run-scoped ascending revision order (`ORDER BY revision ASC`), returning `RevisionHistoryPage { binding_id, run_id, current_revision, high_water_mark_revision, revisions: Vec<RunRevision>, next_cursor, has_more }`.
  3. `list_run_questions` (`run_id: String`, `cursor: Option<String>`, `limit: Option<usize>`): binding-and-run-scoped ascending creation order (`ORDER BY rowid ASC`), returning `QuestionHistoryPage { binding_id, run_id, current_revision, high_water_mark_rowid, questions: Vec<Question>, next_cursor, has_more }`.
- Page structs (`RunListPage`, `RevisionHistoryPage`, `QuestionHistoryPage`) do not carry a top-level `untrusted_data` field; `untrusted_data: true` is present on every `Run`, `RunRevision`, and `Question` item (as well as on `RunSummary`, `StatusResponse`, and the MCP tool response envelope).
- Full historical text (`RunRevision.instruction`, `Question.prompt`, `Question.answer`) is never truncated in `list_run_revisions` or `list_run_questions`; only `RunSummary.instruction_preview` in `status` uses a 120-character preview (`MAX_SUMMARY_TEXT_PREVIEW_CHARS = 120`).

### 1.3 Bounded SQL `status`
- `Core::get_status_run_summaries(&binding_id, MAX_STATUS_RECENT_RUNS)` computes `total_runs` and `active_runs` directly in SQLite (`COUNT(*)`) and loads at most `MAX_STATUS_RECENT_RUNS = 10` newest `RunSummary` rows via `ORDER BY rowid DESC LIMIT 10`.
- `Supervisor` serves `ScopedRequest::Status` using `get_status_run_summaries` without materializing all runs, revisions, or questions.

### 1.4 Two-Copy Wire Byte Budgets (`rmcp 3.8.0` `CallToolResult` Safe)
- In `rmcp 3.8.0`, `CallToolResult::structured(value)` serializes `value` twice in the JSON-RPC response (`structuredContent` as JSON and `content[0].text` as `value.to_string()`, which undergoes second-level JSON string literal escaping).
- `estimate_wire_response_bytes(value)` computes:
  ```text
  first_level_json.len()
    + serde_json::to_string(&first_level_json).len()
    + RESPONSE_WRAPPER_OVERHEAD_BYTES (1,024)
  ```
- Enforced ceilings:
  - Compact command/status snapshots (`Run`, `AckResult`, `StatusResponse`): `<= MAX_COMPACT_RESPONSE_BYTES = 65,536` (64 KiB), verified before transaction commit.
  - Paged responses (`RunListPage`, `RevisionHistoryPage`, `QuestionHistoryPage`): `<= MAX_PAGE_RESPONSE_BYTES = 262,144` (256 KiB), verified before advancing the page cursor so the first item that would exceed the budget is never skipped.
  - Event payload (`EventPayload`): `<= MAX_EVENT_PAYLOAD_BYTES = 16,384` (16 KiB).
  - HTTP request body (`axum` `DefaultBodyLimit`): `<= MAX_HTTP_BODY_BYTES = 65,536` (64 KiB); oversized authenticated requests return `413 Payload Too Large` (`ErrorCode::PayloadTooLarge`).
  - HTTP client response stream cap: `<= MAX_CLIENT_RESPONSE_BYTES = 2,097,152` (2 MiB).

### 1.5 Atomic Compact Idempotency & One-Time `v1 -> v2` Migration
- Mutations (`StartDemo`, `Answer`, `Steer`, `Cancel`, `AcknowledgeEvents`) persist their compact `CommandOutput` snapshot in `command_dedup` inside the same `rusqlite` `Connection::transaction()` as the state change.
- Retrying an accepted command with the same `(binding_id, command_key)` and identical parameters returns the **original mutation snapshot** (not the run's later mutated state). Reusing a key with different parameters returns `CoreError::IdempotencyConflict`.
- Opening a legacy `v1` SQLite database (`schema_meta.version = 1`) runs a one-time transactional migration (`migrate_legacy_command_dedup_tx`) to `SCHEMA_VERSION = 2`:
  - `command_dedup` rows are stepped one row at a time (`WHERE rowid > ?1 ORDER BY rowid ASC LIMIT 1`) with no all-rows `Vec` allocation.
  - Legacy `CommandOutput::Run` payloads discard `revision_history` via `serde::de::IgnoredAny` without allocating a `Value` array, stream `questions` via a `SeqAccess` visitor without allocating a `Vec<Question>`, reconstruct `active_question` from the legacy snapshot's `questions` sequence if `active_question_id` was set, and rewrite `response_json` in compact v2 form.
  - `command_key`, `operation`, `payload_hash`, and `created_at_ms` are preserved intact.
  - Reopening an already-migrated `v2` database (`schema_meta.version = 2`) does not rescan `command_dedup`.

## 2. Explicit Scope, Unpaged Trusted Helpers, and Deferred Items

- **Unpaged Trusted Internal Helpers**: Bounded pagination applies to the run, history, status, and event routes (`get_run`, `list_runs`, `list_run_revisions`, `list_run_questions`, `status`, `pull_events`). Trusted supervisor/CLI helpers `Core::list_bindings` and `Core::list_wakeup_retries` (manual wake audit) are not exposed over MCP and remain unpaged helpers.
- **Lifetime Disk Retention Deferred**: Milestone 2A bounds per-row, per-event, per-snapshot, per-page, and per-request memory/wire sizes and eliminates quadratic `O(N^2)` growth in `command_dedup`. Total lifetime SQLite disk growth over an unbounded number of historical runs, revisions, questions, events, and idempotency keys is deferred to a later operational milestone so history, ACK cursors, and idempotency keys are never silently deleted.
- **Real Worker Execution Deferred to M2B**: Real external worker adapters (AGY and OpenCode) are out of scope for M2A.

## 3. Coverage & Validation Status

| Area | Status | Location |
| --- | --- | --- |
| 210+ steer revisions & questions with max 4,096-byte control-escaped instructions and quote-escaped answers | Coverage added (pending parent execution) | `m2a_hundreds_of_revisions_and_questions_max_escaped_byte_budget_paging` in `crates/threadlane-core/tests/behavioral.rs` |
| Actual `rmcp::model::CallToolResult` serialized byte budget across multi-page escaped history (`<= 256 KiB` page, `<= 64 KiB` compact, zero skipped items) | Coverage added (pending parent execution) | `test_m2a_rmcp_call_tool_result_wire_budgets_stdio_mcp_paging_and_http_body_cap` in `crates/threadlane/tests/runtime_integration.rs` |
| Cursor validation (`tlc1:`): cross-binding, cross-run, cross-kind, malformed, leading-zero, out-of-order, non-existent anchor, `u64::MAX` / `i64::MAX + 1` overflow, invalid limits (`CoreError::Validation` / `validation_failed`) | Coverage added (pending parent execution) | `m2a_cursor_validation_scope_kind_misuse_and_overflow_rejection` in `crates/threadlane-core/tests/behavioral.rs` |
| High-water-mark window stability under concurrent appends + live-view status semantics | Coverage added (pending parent execution) | `m2a_stable_high_water_mark_window_under_concurrent_appends_and_live_view_semantics` in `crates/threadlane-core/tests/behavioral.rs` |
| Multi-run bounded SQL `status` counts (`COUNT(*)`) and `LIMIT 10` recent summaries + ascending paged `list_runs` | Coverage added (pending parent execution) | `m2a_multi_run_bounded_status_counts_and_paged_list_runs` in `crates/threadlane-core/tests/behavioral.rs` |
| Original mutation snapshot replay after later mutations/restarts + multi-row `v1 -> v2` legacy `command_dedup` migration + no rescan on `v2` reopen | Coverage added (pending parent execution) | `m2a_compact_idempotency_original_snapshot_replay_and_legacy_v1_migration` in `crates/threadlane-core/tests/behavioral.rs` |
| Real stdio MCP 11-tool discovery (`tools/list`), `list_runs` / `list_run_revisions` / `list_run_questions` calls, offline discovery, and CLI `smoke.py` | Coverage added (pending parent execution) | `crates/threadlane/tests/runtime_integration.rs` and `scripts/smoke.py` |
| HTTP 64 KiB request body limit (`413 Payload Too Large` after auth, `401 Unauthorized` before auth) | Coverage added (pending parent execution) | `test_m2a_rmcp_call_tool_result_wire_budgets_stdio_mcp_paging_and_http_body_cap` in `crates/threadlane/tests/runtime_integration.rs` |
| Windows / macOS platform-specific file locking and permissions | Untested | Linux (`x86_64-unknown-linux-gnu`) target only |
| Real AGY / OpenCode worker execution | Deferred (M2B) | Owned by separate worker milestone |

## 4. Exact Validation Commands for Parent Review

Run from the worktree root (`/home/sinan/Documents/Codex/2026-10-29/threadlane-m2-history`):

```bash
cargo fmt --all -- --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets -- -D warnings
python3 scripts/smoke.py
```
