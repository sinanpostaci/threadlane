# Threadlane Bounded-Data Architecture & Wire Protocol (`docs/BOUNDED-DATA.md`)

This document specifies Threadlane's bounded-data storage, ascending keyset pagination, cursor format, live-view semantics, two-copy wire-size accounting, and `v1 -> v2` schema migration rules implemented in Milestone 2A (`M2A`).

## 1. Size & Pagination Constants

Defined in `crates/threadlane-core/src/limits.rs` and `crates/threadlane/src/{supervisor,client}.rs`:

| Constant | Value | Enforcement Point |
| --- | --- | --- |
| `SCHEMA_VERSION` | `2` | `schema_meta.version` in SQLite (`LEGACY_SCHEMA_VERSION_V1 = 1` migrated once on open) |
| `DEFAULT_PAGE_LIMIT` | `20` | Default `limit` when omitted in MCP/HTTP/CLI `list_runs`, `list_run_revisions`, `list_run_questions` |
| `MAX_PAGE_LIMIT` | `50` | Maximum `limit` (`1..=50`); `0` or `> 50` fails with `CoreError::Validation` (`validation_failed`) |
| `MAX_CURSOR_LEN` | `256` bytes | Maximum cursor length; empty/whitespace or `> 256` bytes fails with `CoreError::Validation` |
| `MAX_STATUS_RECENT_RUNS` | `10` | Maximum `RunSummary` rows loaded in `status` (`ORDER BY rowid DESC LIMIT 10`) |
| `MAX_SUMMARY_TEXT_PREVIEW_CHARS` | `120` chars | Truncation threshold for `RunSummary.instruction_preview` (`...` appended if truncated) |
| `MAX_INSTRUCTION_LEN` | `4,096` bytes | Raw UTF-8 byte cap for `instruction` |
| `MAX_ANSWER_LEN` | `4,096` bytes | Raw UTF-8 byte cap for `answer` |
| `MAX_EVENT_PAYLOAD_BYTES` | `16,384` (16 KiB) | Serialized JSON byte cap for a single `EventPayload` |
| `MAX_COMPACT_RESPONSE_BYTES` | `65,536` (64 KiB) | Two-copy wire byte budget for compact snapshots (`Run`, `AckResult`, `StatusResponse`) |
| `MAX_PAGE_RESPONSE_BYTES` | `262,144` (256 KiB) | Two-copy wire byte budget for paged responses (`RunListPage`, `RevisionHistoryPage`, `QuestionHistoryPage`) |
| `RESPONSE_WRAPPER_OVERHEAD_BYTES` | `1,024` bytes | Envelope reserve included in `estimate_wire_response_bytes` |
| `MAX_HTTP_BODY_BYTES` | `65,536` (64 KiB) | HTTP request body cap enforced after bearer token authentication (`413 Payload Too Large`) |
| `MAX_CLIENT_RESPONSE_BYTES` | `2,097,152` (2 MiB) | `ScopedClient` / `SupervisorClient` streaming HTTP response cap |

## 2. Keyset Pagination & Stateless High-Water-Mark Cursors

Pagination does not insert persistent cursor state on reads. Cursors are stateless, scope-bound ASCII tokens (`tlc1:...`) validated against SQLite on each page request.

### 2.1 Cursor Formats (`tlc1`) and Ascending Ordering

Unlike `status.recent_runs` (which queries the 10 newest summaries via `ORDER BY rowid DESC LIMIT 10`), the paged endpoints `list_runs`, `list_run_revisions`, and `list_run_questions` return records in **ascending** creation/revision order within a pinned high-water-mark (`hwm`) window:

- **`list_runs` cursor** (`RunListPage { binding_id, runs: Vec<Run>, next_cursor, has_more, high_water_mark_rowid }`):
  ```text
  tlc1:runs:<binding_id>:<after_rowid>:<hwm_rowid>
  ```
  - Page 1 (`cursor = None`): snapshots `hwm_rowid = COALESCE(MAX(rowid), 0)` for `binding_id` (`after_rowid = 0`).
  - Queries `WHERE binding_id = ?1 AND rowid > ?2 AND rowid <= ?3 ORDER BY rowid ASC LIMIT ?4` (`limit + 1`), loading each row's compact current `Run` snapshot (including `active_question` when `active_question_id` is present).
  - Subsequent pages (`cursor = Some(...)`): validates that both `after_rowid` and `hwm_rowid` exist in `runs` for `binding_id` and `after_rowid < hwm_rowid`.

- **`list_run_revisions` cursor** (`RevisionHistoryPage { binding_id, run_id, current_revision, high_water_mark_revision, revisions: Vec<RunRevision>, next_cursor, has_more }`):
  ```text
  tlc1:revisions:<binding_id>:<run_id>:<after_revision>:<hwm_revision>
  ```
  - Page 1 (`cursor = None`): verifies `(binding_id, run_id)` exists and snapshots `hwm_revision = current_revision` (`after_revision = 0`).
  - Queries `WHERE run_id = ?1 AND revision > ?2 AND revision <= ?3 ORDER BY revision ASC LIMIT ?4` (`limit + 1`).
  - Subsequent pages (`cursor = Some(...)`): verifies `after_revision < hwm_revision` and `hwm_revision <= current_revision`.

- **`list_run_questions` cursor** (`QuestionHistoryPage { binding_id, run_id, current_revision, high_water_mark_rowid, questions: Vec<Question>, next_cursor, has_more }`):
  ```text
  tlc1:questions:<binding_id>:<run_id>:<after_rowid>:<hwm_rowid>
  ```
  - Page 1 (`cursor = None`): verifies `(binding_id, run_id)` exists and snapshots `hwm_rowid = COALESCE(MAX(rowid), 0)` for `run_id` (`after_rowid = 0`).
  - Queries `WHERE run_id = ?1 AND rowid > ?2 AND rowid <= ?3 ORDER BY rowid ASC LIMIT ?4` (`limit + 1`).
  - Subsequent pages (`cursor = Some(...)`): validates that both `after_rowid` and `hwm_rowid` exist in `questions` for `(binding_id, run_id)` and `after_rowid < hwm_rowid`.

### 2.2 Strict Cursor Validation Rules
`parse_and_validate_cursor` and the core query methods reject all of the following with `CoreError::Validation` (`ErrorCode::ValidationFailed` / `"validation_failed"`):
- Empty, whitespace-only, non-ASCII, or `> 256`-byte cursors.
- Wrong prefix (anything other than `tlc1`) or wrong colon-delimited segment count.
- Kind mismatch (e.g., passing a `revisions` cursor to `list_run_questions` or `list_runs`).
- Scope mismatch (passing a cursor from another `binding_id` or another `run_id`).
- Non-canonical integers (leading zeros such as `01`, non-digits, `0`, or values exceeding `i64::MAX as u64`).
- Out-of-order bounds (`after >= hwm`, since paging is strictly ascending and a follow-up page requires `after < hwm`).
- Forged anchors where `after` or `hwm` does not correspond to a real record owned by `(binding_id, run_id)`.

### 2.3 High-Water-Mark Window Membership vs Live-View Mutable Fields
- **Window Membership is Pinned**: Because Page 1 captures `hwm` and every query filters `rowid <= hwm` (or `revision <= hwm`), any new runs, steer revisions, or questions appended after Page 1 have `rowid > hwm` (or `revision > hwm`) and are excluded from that cursor chain. Walking a cursor chain to `has_more == false` never duplicates or skips rows from the initial window.
- **Mutable Status Fields are Live Views**: Historical rows are read directly from `runs`, `run_revisions`, and `questions`:
  - **Immutable fields** (`run_id`, `binding_id`, `created_at_ms`, `RunRevision.revision`, `RunRevision.instruction`, `Question.question_id`, `Question.revision`, `Question.prompt`) never change after creation.
  - **Mutable lifecycle fields** (`Run.status`, `Run.revision`, `Run.instruction`, `Run.active_question_id`, `Run.active_question`, `RunRevision.superseded_at_ms`, `Question.status`, `Question.answer`, `Question.resolved_at_ms`, `Question.superseded_by_revision`) reflect the **current live state** when the page is read.

## 3. Wire-Budget Accounting (`rmcp 3.8.0` Two-Copy Safe)

### 3.1 Why Two-Copy Accounting is Required
When `ThreadlaneMcpServer` returns a structured tool response via `rmcp::model::CallToolResult::structured(payload)`, `rmcp 3.8.0` constructs both `structuredContent: payload` and `content[0].text: payload.to_string()`. Inside `content[0].text`, every `"` and `\` in the first-level JSON is escaped a second time.

### 3.2 Conservative Wire Estimator
`estimate_wire_response_bytes(value)` in `crates/threadlane-core/src/limits.rs`:
1. Serializes `value` to `first_level_json: String`.
2. Serializes `first_level_json` as a JSON string literal (`second_level_string_literal = serde_json::to_string(&first_level_json)`).
3. Returns `first_level_json.len() + second_level_string_literal.len() + RESPONSE_WRAPPER_OVERHEAD_BYTES (1,024)`.

### 3.3 Budget Enforcement Points
- **Mutations (`StartDemo`, `Answer`, `Steer`, `Cancel`, `AcknowledgeEvents`)**:
  - Checked via `enforce_compact_response_budget(&output)` (`<= 65,536` bytes) before `tx.commit()` and before writing `command_dedup`.
  - Accepted instruction/answer values remain subject to the existing event payload budget. For example, 4,096 control characters can exceed the 16 KiB event limit even though their raw input fits the text limit; such a mutation is rejected transactionally. Compact response tests exercise maximum accepted escaped text rather than promising acceptance of every raw-length-valid string.
- **Paged Reads (`list_runs`, `list_run_revisions`, `list_run_questions`)**:
  - SQLite fetches at most `limit + 1` candidate rows (`limit <= 50`).
  - Candidates are appended one by one into the page vector. After adding a candidate, the page's two-copy wire size (using a conservative dummy cursor when `has_more` could be true) is checked against `MAX_PAGE_RESPONSE_BYTES = 262,144` (256 KiB).
  - If adding a candidate (beyond the first item on the page) would exceed `262,144` bytes, that candidate is excluded from the current page, `has_more` is set to `true`, and `next_cursor` is encoded using the **last included item's** key (`after`). The excluded candidate is the first item returned on the next page—never skipped.

## 4. Schema Version 2 & One-Time Legacy `command_dedup` Migration

- New databases initialize `schema_meta.version = 2` (`SCHEMA_VERSION = 2`).
- Existing `v1` databases (`schema_meta.version = 1`) are migrated once inside a `rusqlite` `Connection::transaction()` in `Core::init_schema_and_recover` via `migrate_legacy_command_dedup_tx`:
  - Rows in `command_dedup` are stepped one row at a time (`WHERE rowid > ?1 ORDER BY rowid ASC LIMIT 1`) so all rows are never materialized into a `Vec`.
  - Legacy `CommandOutput::Run` JSON objects are deserialized via `StoredDedupOutput::Run(Box<LegacyStoredRun>)`:
    - `revision_history` is skipped in place via `serde::de::IgnoredAny` (`IgnoredFieldPresence`) without allocating a `serde_json::Value` array.
    - `questions` is streamed via a `SeqAccess` visitor (`LegacyQuestionsField`) that retains only a pending/matching candidate question without allocating a `Vec<Question>`.
    - If `active_question_id` is set on the legacy snapshot (or inferred from a legacy `waiting_input` snapshot's pending question), `active_question` is reconstructed solely from the legacy snapshot's streamed `questions` field (it does not fall back to the live `questions` table).
    - The compacted `CommandOutput::Run(Box<Run>)` is checked against `enforce_compact_response_budget` and updated in place.
  - `schema_meta.version` is updated to `2` in the same transaction.
  - Opening an already-migrated `v2` database (`schema_meta.version = 2`) skips `migrate_legacy_command_dedup_tx` and never rescans `command_dedup`.
  - Any unknown schema version (`!= 1` and `!= 2`) fails closed with `CoreError::IncompatibleSchemaVersion`.

## 5. Unpaged Trusted Helpers & Deferred Disk Retention

- **Trusted Helpers**: `Core::list_bindings` and `Core::list_wakeup_retries` are internal/CLI trusted operations (not exposed as MCP tools) and remain unpaged.
- **Deferred Disk Retention**: Milestone 2A guarantees bounded memory and wire sizes per request/response and `O(1)` per-mutation idempotency storage in `command_dedup`. Total lifetime SQLite file size across historical runs, revisions, questions, events, and idempotency keys remains linear (`O(N)`) on disk; retention/archival pruning is deferred to a future milestone.
