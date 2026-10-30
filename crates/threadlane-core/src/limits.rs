use uuid::Uuid;

use crate::error::CoreError;
use crate::types::EventKind;

/// Current SQLite schema version owned by `threadlane-core`.
///
/// Version 2 introduced keyset pagination indexes (`idx_runs_binding_order`,
/// `idx_questions_binding_run_order`) and in-place compaction of legacy v1
/// full-run `command_dedup` responses. Version 3 adds the durable `workers`
/// ledger and reopen uncertainty recovery. Databases at version 1 or 2 are
/// migrated automatically and transactionally on open.
pub const SCHEMA_VERSION: u32 = 3;

/// Legacy Milestone 1 SQLite schema version supported for in-place upgrade on open.
pub(crate) const LEGACY_SCHEMA_VERSION_V1: u32 = 1;

/// Legacy Milestone 2A SQLite schema version supported for in-place upgrade on open.
pub(crate) const LEGACY_SCHEMA_VERSION_V2: u32 = 2;

/// Maximum number of named worker execution profiles permitted in startup configuration.
pub const MAX_WORKER_PROFILES: usize = 32;

/// Minimum global worker concurrency limit permitted in startup configuration.
pub const MIN_WORKER_MAX_PARALLEL: usize = 1;

/// Maximum global worker concurrency limit permitted in startup configuration.
pub const MAX_WORKER_MAX_PARALLEL: usize = 8;

/// Maximum byte length for a worker profile identifier (`profile_id`).
pub const MAX_WORKER_PROFILE_ID_LEN: usize = 64;

/// Exact hex byte length for an immutable SHA-256 trusted profile digest.
pub const MAX_WORKER_DIGEST_LEN: usize = 64;

/// Maximum number of durable `WorkerProgress` events persisted per worker attempt.
///
/// Additional live progress updates beyond this cap still update the worker's
/// latest progress summary and increment `coalesced_progress_events` without
/// growing the durable `events` table unboundedly.
pub const MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT: u32 = 32;

/// Maximum byte length for sanitized summary or diagnostic text stored in a worker snapshot.
pub const MAX_WORKER_SUMMARY_TEXT_BYTES: usize = 512;

/// Maximum number of relative changed file paths previewed in a compact worker outcome.
pub const MAX_WORKER_CHANGED_PATHS_PREVIEW: usize = 16;

/// Maximum number of diff-gate violation summaries included in a compact worker outcome.
pub const MAX_WORKER_VIOLATION_SUMMARIES: usize = 8;

/// Maximum byte length for a worktree path or relative path string in a compact worker snapshot.
pub const MAX_WORKER_PATH_BYTES: usize = 512;

/// Maximum byte length for software-generated or caller-supplied identifiers.
pub const MAX_ID_LEN: usize = 64;

/// Maximum byte length for a binding display label.
pub const MAX_LABEL_LEN: usize = 128;

/// Maximum byte length for an idempotent command key.
pub const MAX_COMMAND_KEY_LEN: usize = 128;

/// Maximum byte length for a task instruction (`StartDemo` or `Steer`).
pub const MAX_INSTRUCTION_LEN: usize = 4_096;

/// Maximum byte length for an answer payload (`Answer`).
pub const MAX_ANSWER_LEN: usize = 4_096;

/// Maximum byte length for an optional cancellation reason (`Cancel`).
pub const MAX_CANCEL_REASON_LEN: usize = 1_024;

/// Maximum byte length for a wake-up failure or unknown-outcome diagnostic string.
pub const MAX_WAKEUP_ERROR_LEN: usize = 1_024;

/// Maximum number of events returned in a single `PullEvents` batch.
pub const MAX_EVENT_BATCH_SIZE: usize = 100;

/// Default number of events returned when pulling a batch.
pub const DEFAULT_EVENT_BATCH_SIZE: usize = 50;

/// Minimum serialized JSON byte budget permitted for `PullEventsBounded` (1 KiB).
pub const MIN_EVENT_BATCH_BYTES: usize = 1_024;

/// Maximum serialized JSON byte budget permitted for `PullEventsBounded` (128 KiB).
pub const MAX_EVENT_BATCH_BYTES: usize = 131_072;

/// Maximum serialized JSON payload byte length for a single durable event.
pub const MAX_EVENT_PAYLOAD_BYTES: usize = 16_384;

/// Default number of items returned in a bounded run, revision, or question page.
pub const DEFAULT_PAGE_LIMIT: usize = 20;

/// Maximum number of items permitted in a single bounded run, revision, or question page request.
pub const MAX_PAGE_LIMIT: usize = 50;

/// Maximum byte length for an opaque keyset pagination cursor token.
pub const MAX_CURSOR_LEN: usize = 256;

/// Maximum serialized JSON byte budget (64 KiB, including outer protocol/MCP wrapper overhead)
/// for compact single-resource snapshots (`GetRun`, `StartDemo`, `Answer`, `Steer`, `Cancel`,
/// `AcknowledgeEvents`, and `Status`).
pub const MAX_COMPACT_RESPONSE_BYTES: usize = 64 * 1024;

/// Maximum serialized JSON byte budget (256 KiB, including outer protocol/MCP wrapper overhead)
/// for bounded history and list pages (`ListRuns`, `ListRunRevisions`, `ListRunQuestions`, `ListActivity`).
pub const MAX_PAGE_RESPONSE_BYTES: usize = 256 * 1024;

/// Reserved byte budget for outer HTTP/MCP response envelope fields around a serialized
/// [`crate::CommandOutput`] payload.
pub const RESPONSE_WRAPPER_OVERHEAD_BYTES: usize = 1_024;

/// Maximum number of recent run summaries included in a compact binding status snapshot.
pub const MAX_STATUS_RECENT_RUNS: usize = 10;

/// Maximum character count included in `RunSummary::instruction_preview`.
pub const MAX_SUMMARY_TEXT_PREVIEW_CHARS: usize = 120;

/// Default delay (in milliseconds) between deterministic demo run steps.
pub const DEFAULT_DEMO_STEP_DELAY_MS: i64 = 1_000;

/// Maximum permitted delay (in milliseconds) between deterministic demo run steps.
pub const MAX_DEMO_STEP_DELAY_MS: i64 = 3_600_000;

/// Fixed wake-up message sent to Codex.
///
/// Contains no instruction text, question prompt, worker output, or display label.
pub const FIXED_WAKEUP_MESSAGE: &str =
    "Threadlane has pending events. Pull and process Threadlane events via MCP.";

pub(crate) fn validate_thread_uuid(raw: &str) -> Result<String, CoreError> {
    if raw.is_empty() {
        return Err(CoreError::Validation {
            field: "thread_uuid",
            reason: "thread_uuid must not be empty".to_string(),
        });
    }
    if raw.len() > MAX_ID_LEN {
        return Err(CoreError::Validation {
            field: "thread_uuid",
            reason: format!("thread_uuid exceeds maximum length of {MAX_ID_LEN} bytes"),
        });
    }
    if raw.trim() != raw {
        return Err(CoreError::Validation {
            field: "thread_uuid",
            reason: "thread_uuid must not have leading or trailing whitespace".to_string(),
        });
    }
    let parsed = Uuid::parse_str(raw).map_err(|err| CoreError::Validation {
        field: "thread_uuid",
        reason: format!("invalid UUID format: {err}"),
    })?;
    if parsed.is_nil() {
        return Err(CoreError::Validation {
            field: "thread_uuid",
            reason: "thread_uuid must not be the nil UUID".to_string(),
        });
    }
    Ok(parsed.hyphenated().to_string())
}

pub(crate) fn validate_label(raw: &str) -> Result<String, CoreError> {
    if raw.len() > MAX_LABEL_LEN {
        return Err(CoreError::Validation {
            field: "label",
            reason: format!("label exceeds maximum length of {MAX_LABEL_LEN} bytes"),
        });
    }
    if raw.chars().any(char::is_control) {
        return Err(CoreError::Validation {
            field: "label",
            reason: "label must not contain control characters".to_string(),
        });
    }
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(CoreError::Validation {
            field: "label",
            reason: "label must not be empty".to_string(),
        });
    }
    Ok(trimmed.to_string())
}

pub(crate) fn validate_identifier(field: &'static str, raw: &str) -> Result<String, CoreError> {
    if raw.is_empty() {
        return Err(CoreError::Validation {
            field,
            reason: format!("{field} must not be empty"),
        });
    }
    if raw.len() > MAX_ID_LEN {
        return Err(CoreError::Validation {
            field,
            reason: format!("{field} exceeds maximum length of {MAX_ID_LEN} bytes"),
        });
    }
    if raw.trim() != raw || raw.chars().any(char::is_control) {
        return Err(CoreError::Validation {
            field,
            reason: format!(
                "{field} must not contain control characters or surrounding whitespace"
            ),
        });
    }
    Ok(raw.to_string())
}

pub(crate) fn validate_command_key(raw: &str) -> Result<String, CoreError> {
    if raw.is_empty() {
        return Err(CoreError::Validation {
            field: "command_key",
            reason: "command_key must not be empty".to_string(),
        });
    }
    if raw.len() > MAX_COMMAND_KEY_LEN {
        return Err(CoreError::Validation {
            field: "command_key",
            reason: format!("command_key exceeds maximum length of {MAX_COMMAND_KEY_LEN} bytes"),
        });
    }
    if raw.trim() != raw || raw.chars().any(char::is_control) {
        return Err(CoreError::Validation {
            field: "command_key",
            reason: "command_key must not contain control characters or surrounding whitespace"
                .to_string(),
        });
    }
    Ok(raw.to_string())
}

pub(crate) fn validate_bounded_text(
    field: &'static str,
    raw: &str,
    max_len: usize,
) -> Result<String, CoreError> {
    if raw.len() > max_len {
        return Err(CoreError::Validation {
            field,
            reason: format!("{field} exceeds maximum length of {max_len} bytes"),
        });
    }
    if raw.contains('\0') {
        return Err(CoreError::Validation {
            field,
            reason: format!("{field} must not contain NUL bytes"),
        });
    }
    if raw.trim().is_empty() {
        return Err(CoreError::Validation {
            field,
            reason: format!("{field} must not be empty"),
        });
    }
    Ok(raw.to_string())
}

pub(crate) fn validate_optional_text(
    field: &'static str,
    raw: Option<&str>,
    max_len: usize,
) -> Result<Option<String>, CoreError> {
    match raw {
        None => Ok(None),
        Some(value) => validate_bounded_text(field, value, max_len).map(Some),
    }
}

pub(crate) fn validate_timestamp_ms(field: &'static str, value: i64) -> Result<i64, CoreError> {
    if value < 0 {
        return Err(CoreError::Validation {
            field,
            reason: format!("{field} must be non-negative, got {value}"),
        });
    }
    Ok(value)
}

pub(crate) fn validate_step_delay_ms(delay_ms: Option<i64>) -> Result<i64, CoreError> {
    let resolved = delay_ms.unwrap_or(DEFAULT_DEMO_STEP_DELAY_MS);
    if !(0..=MAX_DEMO_STEP_DELAY_MS).contains(&resolved) {
        return Err(CoreError::Validation {
            field: "step_delay_ms",
            reason: format!(
                "step_delay_ms must be between 0 and {MAX_DEMO_STEP_DELAY_MS}, got {resolved}"
            ),
        });
    }
    Ok(resolved)
}

pub(crate) fn validate_batch_limit(limit: usize) -> Result<usize, CoreError> {
    if limit == 0 || limit > MAX_EVENT_BATCH_SIZE {
        return Err(CoreError::Validation {
            field: "limit",
            reason: format!("limit must be between 1 and {MAX_EVENT_BATCH_SIZE}, got {limit}"),
        });
    }
    Ok(limit)
}

pub(crate) fn validate_batch_byte_budget(max_batch_bytes: usize) -> Result<usize, CoreError> {
    if !(MIN_EVENT_BATCH_BYTES..=MAX_EVENT_BATCH_BYTES).contains(&max_batch_bytes) {
        return Err(CoreError::Validation {
            field: "max_batch_bytes",
            reason: format!(
                "max_batch_bytes must be between {MIN_EVENT_BATCH_BYTES} and {MAX_EVENT_BATCH_BYTES}, got {max_batch_bytes}"
            ),
        });
    }
    Ok(max_batch_bytes)
}

pub(crate) fn validate_page_limit(limit: usize) -> Result<usize, CoreError> {
    if limit == 0 || limit > MAX_PAGE_LIMIT {
        return Err(CoreError::Validation {
            field: "limit",
            reason: format!("limit must be between 1 and {MAX_PAGE_LIMIT}, got {limit}"),
        });
    }
    Ok(limit)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CursorKind {
    Runs,
    Revisions,
    Questions,
    Activity,
}

impl CursorKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Runs => "runs",
            Self::Revisions => "revisions",
            Self::Questions => "questions",
            Self::Activity => "activity",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedCursor {
    pub(crate) after: u64,
    pub(crate) high_water_mark: u64,
}

pub(crate) fn validate_activity_run_id(raw: &str) -> Result<String, CoreError> {
    let clean = validate_identifier("run_id", raw)?;
    if clean == "-" || clean.contains(':') {
        return Err(CoreError::Validation {
            field: "run_id",
            reason: "run_id contains invalid characters".to_string(),
        });
    }
    Ok(clean)
}

pub(crate) fn encode_runs_cursor(binding_id: &str, after_rowid: u64, hwm_rowid: u64) -> String {
    format!("tlc1:runs:{binding_id}:{after_rowid}:{hwm_rowid}")
}

pub(crate) fn encode_revisions_cursor(
    binding_id: &str,
    run_id: &str,
    after_revision: u64,
    hwm_revision: u64,
) -> String {
    format!("tlc1:revisions:{binding_id}:{run_id}:{after_revision}:{hwm_revision}")
}

pub(crate) fn encode_questions_cursor(
    binding_id: &str,
    run_id: &str,
    after_rowid: u64,
    hwm_rowid: u64,
) -> String {
    format!("tlc1:questions:{binding_id}:{run_id}:{after_rowid}:{hwm_rowid}")
}

pub(crate) fn encode_activity_cursor(
    binding_id: &str,
    run_id: Option<&str>,
    kind: Option<EventKind>,
    after_seq: u64,
    hwm_seq: u64,
) -> String {
    let run_part = run_id.unwrap_or("-");
    let kind_part = kind.map(EventKind::as_str).unwrap_or("-");
    format!("tlc1:activity:{binding_id}:{run_part}:{kind_part}:{after_seq}:{hwm_seq}")
}

fn parse_cursor_position(label: &str, raw: &str) -> Result<u64, CoreError> {
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) || raw.starts_with('0') {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: format!("cursor {label} must be a canonical positive integer"),
        });
    }
    let value = raw.parse::<u64>().map_err(|_| CoreError::Validation {
        field: "cursor",
        reason: format!("cursor {label} overflows 64-bit integer range"),
    })?;
    if value == 0 || value > i64::MAX as u64 {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: format!("cursor {label} exceeds valid signed 64-bit integer key range"),
        });
    }
    Ok(value)
}

pub(crate) fn parse_and_validate_cursor(
    raw: &str,
    expected_kind: CursorKind,
    expected_binding_id: &str,
    expected_run_id: Option<&str>,
) -> Result<ParsedCursor, CoreError> {
    if raw.is_empty() {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: "cursor must not be empty".to_string(),
        });
    }
    if raw.len() > MAX_CURSOR_LEN {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: format!("cursor exceeds maximum length of {MAX_CURSOR_LEN} bytes"),
        });
    }
    if raw.trim() != raw || raw.chars().any(char::is_control) {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: "cursor must not contain control characters or surrounding whitespace"
                .to_string(),
        });
    }

    let parts: Vec<&str> = raw.split(':').collect();
    if parts.first().copied() != Some("tlc1") {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: "unrecognized cursor version or format".to_string(),
        });
    }

    let actual_kind = match parts.get(1).copied() {
        Some("runs") => CursorKind::Runs,
        Some("revisions") => CursorKind::Revisions,
        Some("questions") => CursorKind::Questions,
        Some("activity") => CursorKind::Activity,
        _ => {
            return Err(CoreError::Validation {
                field: "cursor",
                reason: "unrecognized cursor resource kind".to_string(),
            });
        }
    };

    if actual_kind != expected_kind {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: format!(
                "cursor kind '{}' cannot be used for '{}' pagination",
                actual_kind.as_str(),
                expected_kind.as_str()
            ),
        });
    }

    let (after_raw, hwm_raw) = match (expected_kind, expected_run_id) {
        (CursorKind::Runs, None) => {
            if parts.len() != 5 {
                return Err(CoreError::Validation {
                    field: "cursor",
                    reason: "malformed runs cursor structure".to_string(),
                });
            }
            if parts[2] != expected_binding_id {
                return Err(CoreError::Validation {
                    field: "cursor",
                    reason: "cursor binding_id does not match request scope".to_string(),
                });
            }
            (parts[3], parts[4])
        }
        (CursorKind::Revisions | CursorKind::Questions, Some(expected_run)) => {
            if parts.len() != 6 {
                return Err(CoreError::Validation {
                    field: "cursor",
                    reason: format!("malformed {} cursor structure", expected_kind.as_str()),
                });
            }
            if parts[2] != expected_binding_id {
                return Err(CoreError::Validation {
                    field: "cursor",
                    reason: "cursor binding_id does not match request scope".to_string(),
                });
            }
            if parts[3] != expected_run {
                return Err(CoreError::Validation {
                    field: "cursor",
                    reason: "cursor run_id does not match requested run_id".to_string(),
                });
            }
            (parts[4], parts[5])
        }
        _ => {
            return Err(CoreError::Validation {
                field: "cursor",
                reason: "cursor scope mismatch".to_string(),
            });
        }
    };

    let after = parse_cursor_position("position", after_raw)?;
    let high_water_mark = parse_cursor_position("high_water_mark", hwm_raw)?;
    if after >= high_water_mark {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: format!(
                "cursor position ({after}) must be strictly less than high_water_mark ({high_water_mark})"
            ),
        });
    }

    Ok(ParsedCursor {
        after,
        high_water_mark,
    })
}

pub(crate) fn parse_and_validate_activity_cursor(
    raw: &str,
    expected_binding_id: &str,
    expected_run_id: Option<&str>,
    expected_event_kind: Option<EventKind>,
) -> Result<ParsedCursor, CoreError> {
    if raw.is_empty() {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: "cursor must not be empty".to_string(),
        });
    }
    if raw.len() > MAX_CURSOR_LEN {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: format!("cursor exceeds maximum length of {MAX_CURSOR_LEN} bytes"),
        });
    }
    if raw.trim() != raw || raw.chars().any(char::is_control) {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: "cursor must not contain control characters or surrounding whitespace"
                .to_string(),
        });
    }

    let parts: Vec<&str> = raw.split(':').collect();
    if parts.first().copied() != Some("tlc1") {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: "unrecognized cursor version or format".to_string(),
        });
    }

    let actual_kind = match parts.get(1).copied() {
        Some("runs") => CursorKind::Runs,
        Some("revisions") => CursorKind::Revisions,
        Some("questions") => CursorKind::Questions,
        Some("activity") => CursorKind::Activity,
        _ => {
            return Err(CoreError::Validation {
                field: "cursor",
                reason: "unrecognized cursor resource kind".to_string(),
            });
        }
    };

    if actual_kind != CursorKind::Activity {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: format!(
                "cursor kind '{}' cannot be used for 'activity' pagination",
                actual_kind.as_str()
            ),
        });
    }

    if parts.len() != 7 {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: "malformed activity cursor structure".to_string(),
        });
    }

    if parts[2] != expected_binding_id {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: "cursor binding_id does not match request scope".to_string(),
        });
    }

    let cursor_run_id = if parts[3] == "-" {
        None
    } else {
        let valid_run = validate_activity_run_id(parts[3]).map_err(|_| CoreError::Validation {
            field: "cursor",
            reason: "malformed activity cursor run_id filter".to_string(),
        })?;
        Some(valid_run)
    };
    if cursor_run_id.as_deref() != expected_run_id {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: "cursor run_id filter does not match requested run_id".to_string(),
        });
    }

    let cursor_event_kind = if parts[4] == "-" {
        None
    } else {
        let kind = EventKind::from_str(parts[4]).ok_or_else(|| CoreError::Validation {
            field: "cursor",
            reason: "malformed activity cursor event kind filter".to_string(),
        })?;
        Some(kind)
    };
    if cursor_event_kind != expected_event_kind {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: "cursor kind filter does not match requested kind".to_string(),
        });
    }

    let after = parse_cursor_position("position", parts[5])?;
    let high_water_mark = parse_cursor_position("high_water_mark", parts[6])?;
    if after >= high_water_mark {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: format!(
                "cursor position ({after}) must be strictly less than high_water_mark ({high_water_mark})"
            ),
        });
    }

    Ok(ParsedCursor {
        after,
        high_water_mark,
    })
}

pub(crate) fn truncate_preview(text: &str) -> (String, bool) {
    let mut chars = text.chars();
    let preview: String = chars
        .by_ref()
        .take(MAX_SUMMARY_TEXT_PREVIEW_CHARS)
        .collect();
    if chars.next().is_some() {
        (format!("{preview}..."), true)
    } else {
        (preview, false)
    }
}

/// Computes a conservative serialized wire byte estimate for `value` that accounts for both
/// `structuredContent` (first-level JSON) and `content[0].text` (second-level JSON string literal
/// of the first-level JSON) produced by `rmcp::model::CallToolResult::structured`, plus
/// [`RESPONSE_WRAPPER_OVERHEAD_BYTES`].
pub fn estimate_wire_response_bytes<T: serde::Serialize>(value: &T) -> Result<usize, CoreError> {
    let first_level_json = serde_json::to_string(value)?;
    let second_level_json_string = serde_json::to_string(&first_level_json)?;
    Ok(first_level_json
        .len()
        .saturating_add(second_level_json_string.len())
        .saturating_add(RESPONSE_WRAPPER_OVERHEAD_BYTES))
}
