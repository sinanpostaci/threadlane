use uuid::Uuid;

use crate::error::CoreError;

/// Current SQLite schema version owned by `threadlane-core`.
pub const SCHEMA_VERSION: u32 = 1;

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

/// Maximum serialized JSON payload byte length for a single durable event.
pub const MAX_EVENT_PAYLOAD_BYTES: usize = 16_384;

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
