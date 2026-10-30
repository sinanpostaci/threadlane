//! Pure validation of synthetic OpenCode v2 session metadata.
//!
//! The function in this module performs no I/O. It validates an already-loaded metadata response
//! and sanitized session export, then returns compact allowlisted evidence or one of a finite set
//! of static errors. It never canonicalizes paths, reads a session, invokes a command, or retains
//! titles, prompts, message content, reasoning, tool data, error prose, or filesystem paths.
//!
//! OpenCode v2 assistant messages are flat: `type == "assistant"` with a `model` object,
//! `finish`, `time.created`/`time.completed`, and an optional `error`. The older nested
//! `messages[].info.role` shape is intentionally not accepted. Session-level `model` is mutable
//! selection state and is never used as execution evidence.

use std::collections::BTreeSet;
use std::fmt;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::spec::{ResolvedModelTarget, MAX_IDENTIFIER_LEN};

/// Maximum accepted byte length of the authoritative `Session.Info` response (`64 KiB`).
pub const MAX_OPENCODE_INFO_BYTES: usize = 65_536;

/// Maximum accepted byte length of the sanitized session export (`1 MiB`).
pub const MAX_OPENCODE_EXPORT_BYTES: usize = 1_048_576;

/// Maximum accepted number of transfer messages, including non-assistant content (`512`).
pub const MAX_OPENCODE_MESSAGES: usize = 512;

/// Trusted ownership data supplied by the caller.
///
/// `canonical_workdir` is already canonical. Validation must not call `Path::canonicalize`; the
/// untrusted directory is compared strictly against this value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedOpenCodeSession {
    /// Software-owned session identifier that both metadata copies must match.
    pub session_id: String,
    /// Trusted canonical working directory that the real metadata must match exactly.
    pub canonical_workdir: PathBuf,
    /// Trusted session start time; session creation may not predate it.
    pub started_at_ms: u64,
}

/// Compact allowlisted evidence derived only from validated CLI assistant metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenCodeMetadataEvidence {
    /// Fixed evidence source marker (`opencode_sanitized_session_export`).
    pub source: String,
    /// Fixed evidence strength marker (`cli_reported_per_assistant`).
    pub strength: String,
    /// Software-owned session identifier validated against both metadata copies.
    pub session_id: String,
    /// Observed assistant model in `providerID/id` form.
    pub observed_model: String,
    /// Observed assistant variant/effort, when reported.
    #[serde(default)]
    pub observed_effort: Option<String>,
    /// Number of validated flat assistant messages.
    pub assistant_message_count: usize,
    /// Identifier of the final assistant in transfer order.
    pub terminal_message_id: String,
}

/// Finite static metadata validation error.
///
/// Variants carry no JSON, prose, paths, identifiers, or provider detail. Safe machine-readable
/// values are exposed only through [`Self::as_str`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpenCodeMetadataError {
    /// An input byte or message-count budget was exceeded.
    OutputOverflow,
    /// A required documented shape, identifier, timestamp, or field type was malformed.
    InvalidMetadata,
    /// Session ownership, working directory, or supplied message-session scope did not match.
    OwnershipMismatch,
    /// Session or assistant evidence predates the trusted session creation/start.
    StaleSession,
    /// No assistant model evidence was available.
    MissingModel,
    /// An assistant's provider or model did not match the immutable requested target.
    ModelMismatch,
    /// The requested effort had no matching per-assistant variant evidence.
    MissingEffort,
    /// An assistant variant differed from the requested effort.
    EffortMismatch,
    /// A succeeded session outcome, assistant completion, final `stop`, or idle time was missing.
    MissingCompletion,
    /// An assistant message carried a non-null provider error.
    ProviderError,
}

impl OpenCodeMetadataError {
    /// Returns the finite `snake_case` error code.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::OutputOverflow => "output_overflow",
            Self::InvalidMetadata => "invalid_metadata",
            Self::OwnershipMismatch => "ownership_mismatch",
            Self::StaleSession => "stale_session",
            Self::MissingModel => "missing_model",
            Self::ModelMismatch => "model_mismatch",
            Self::MissingEffort => "missing_effort",
            Self::EffortMismatch => "effort_mismatch",
            Self::MissingCompletion => "missing_completion",
            Self::ProviderError => "provider_error",
        }
    }
}

impl fmt::Display for OpenCodeMetadataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::error::Error for OpenCodeMetadataError {}

/// Validates synthetic OpenCode v2 metadata and returns compact allowlisted evidence.
///
/// The inputs are the exact info transport (`{ "data": Session.Info }`) and the exact sanitized
/// CLI transfer (`{ "info": Session.Info, "messages": [...] }`). Unknown/private fields are
/// dropped. No recursive wrapper search is performed.
///
/// Validation requires ownership, an exact canonical working directory, fresh matching session
/// creation times, succeeded session outcomes, an idle timestamp not before any retained assistant
/// completion, at least one per-assistant model/effort match, unique message identifiers, valid
/// completion times, and a final assistant `finish == "stop"`.
///
/// Both session outcomes must be the exact JSON string `succeeded`. The real-info idle timestamp
/// is read only from `info.time.idle` and must not precede any retained assistant completion.
pub fn validate_opencode_metadata(
    owned: &OwnedOpenCodeSession,
    info_response: &[u8],
    sanitized_export: &[u8],
    expected: &ResolvedModelTarget,
) -> Result<OpenCodeMetadataEvidence, OpenCodeMetadataError> {
    if info_response.len() > MAX_OPENCODE_INFO_BYTES {
        return Err(OpenCodeMetadataError::OutputOverflow);
    }
    if sanitized_export.len() > MAX_OPENCODE_EXPORT_BYTES {
        return Err(OpenCodeMetadataError::OutputOverflow);
    }

    let owned_workdir = validate_owned_session(owned)?;

    let info_root = parse_object(info_response)?;
    let info = info_root
        .get("data")
        .and_then(Value::as_object)
        .ok_or(OpenCodeMetadataError::InvalidMetadata)?;

    let export_root = parse_object(sanitized_export)?;
    let export_info = export_root
        .get("info")
        .and_then(Value::as_object)
        .ok_or(OpenCodeMetadataError::InvalidMetadata)?;
    let messages = export_root
        .get("messages")
        .and_then(Value::as_array)
        .ok_or(OpenCodeMetadataError::InvalidMetadata)?;
    if messages.len() > MAX_OPENCODE_MESSAGES {
        return Err(OpenCodeMetadataError::OutputOverflow);
    }

    validate_identity(info, export_info, owned)?;
    validate_real_workdir(info, owned_workdir)?;

    let info_created_ms = session_created_ms(info)?;
    let export_created_ms = session_created_ms(export_info)?;
    if info_created_ms != export_created_ms {
        return Err(OpenCodeMetadataError::InvalidMetadata);
    }
    if info_created_ms < owned.started_at_ms {
        return Err(OpenCodeMetadataError::StaleSession);
    }

    if !session_outcome_succeeded(info) || !session_outcome_succeeded(export_info) {
        return Err(OpenCodeMetadataError::MissingCompletion);
    }

    validate_message_envelopes(messages)?;

    let (expected_provider, expected_model) = split_expected_target(expected)?;
    if expected
        .effort
        .as_deref()
        .is_some_and(|effort| !is_safe_identifier(effort))
    {
        return Err(OpenCodeMetadataError::InvalidMetadata);
    }

    let mut assistant_message_count = 0usize;
    let mut terminal_message_id: Option<String> = None;
    let mut terminal_finish: Option<String> = None;
    let mut max_assistant_completed_ms = 0u64;
    let mut observed_model: Option<String> = None;
    let mut observed_effort: Option<String> = None;

    for message in messages {
        let message = message
            .as_object()
            .ok_or(OpenCodeMetadataError::InvalidMetadata)?;
        let message_type = message
            .get("type")
            .and_then(Value::as_str)
            .ok_or(OpenCodeMetadataError::InvalidMetadata)?;

        match message.get("sessionID") {
            None => {}
            Some(value) if value.is_null() => {}
            Some(value) => {
                let supplied_session_id = value
                    .as_str()
                    .ok_or(OpenCodeMetadataError::OwnershipMismatch)?;
                if supplied_session_id != owned.session_id {
                    return Err(OpenCodeMetadataError::OwnershipMismatch);
                }
            }
        }

        if message_type != "assistant" {
            continue;
        }

        let message_id = message
            .get("id")
            .and_then(Value::as_str)
            .ok_or(OpenCodeMetadataError::InvalidMetadata)?;
        if !is_safe_identifier(message_id) {
            return Err(OpenCodeMetadataError::InvalidMetadata);
        }

        if message
            .get("error")
            .is_some_and(|error_value| !error_value.is_null())
        {
            return Err(OpenCodeMetadataError::ProviderError);
        }

        let time = message
            .get("time")
            .and_then(Value::as_object)
            .ok_or(OpenCodeMetadataError::MissingCompletion)?;
        let created_ms = required_timestamp(
            time.get("created"),
            OpenCodeMetadataError::MissingCompletion,
        )?;
        let completed_ms = required_timestamp(
            time.get("completed"),
            OpenCodeMetadataError::MissingCompletion,
        )?;
        if completed_ms < created_ms {
            return Err(OpenCodeMetadataError::MissingCompletion);
        }
        if created_ms < info_created_ms {
            return Err(OpenCodeMetadataError::StaleSession);
        }
        max_assistant_completed_ms = max_assistant_completed_ms.max(completed_ms);

        let model = message
            .get("model")
            .and_then(Value::as_object)
            .ok_or(OpenCodeMetadataError::MissingModel)?;
        let provider_id = model
            .get("providerID")
            .ok_or(OpenCodeMetadataError::MissingModel)?
            .as_str()
            .ok_or(OpenCodeMetadataError::MissingModel)?;
        let model_id = model
            .get("id")
            .ok_or(OpenCodeMetadataError::MissingModel)?
            .as_str()
            .ok_or(OpenCodeMetadataError::MissingModel)?;
        let variant = match model.get("variant") {
            None => None,
            Some(value) if value.is_null() => None,
            Some(value) => Some(
                value
                    .as_str()
                    .ok_or(OpenCodeMetadataError::InvalidMetadata)?,
            ),
        };

        if !is_safe_identifier(provider_id) || !is_safe_model_identifier(model_id) {
            return Err(OpenCodeMetadataError::InvalidMetadata);
        }
        if variant.is_some_and(|value| !is_safe_identifier(value)) {
            return Err(OpenCodeMetadataError::InvalidMetadata);
        }

        if provider_id != expected_provider || model_id != expected_model {
            return Err(OpenCodeMetadataError::ModelMismatch);
        }
        match (expected.effort.as_deref(), variant) {
            (Some(_), None) => return Err(OpenCodeMetadataError::MissingEffort),
            (Some(expected_effort), Some(observed_variant))
                if expected_effort != observed_variant =>
            {
                return Err(OpenCodeMetadataError::EffortMismatch);
            }
            (None, Some(_)) => return Err(OpenCodeMetadataError::EffortMismatch),
            _ => {}
        }

        let model_string = format!("{provider_id}/{model_id}");
        if let Some(previous) = observed_model.as_ref() {
            if previous.as_str() != model_string.as_str() {
                return Err(OpenCodeMetadataError::ModelMismatch);
            }
        } else {
            observed_model = Some(model_string);
        }
        if observed_effort.is_none() {
            observed_effort = variant.map(|value| value.to_string());
        }

        let finish = message.get("finish").and_then(Value::as_str);

        assistant_message_count += 1;
        terminal_message_id = Some(message_id.to_string());
        terminal_finish = finish.map(|value| value.to_string());
    }

    let terminal_message_id = terminal_message_id.ok_or(OpenCodeMetadataError::MissingModel)?;
    let observed_model = observed_model.ok_or(OpenCodeMetadataError::MissingModel)?;
    if terminal_finish.as_deref() != Some("stop") {
        return Err(OpenCodeMetadataError::MissingCompletion);
    }

    let idle_ms = session_idle_ms(info)?;
    if idle_ms < info_created_ms || idle_ms < max_assistant_completed_ms {
        return Err(OpenCodeMetadataError::MissingCompletion);
    }

    Ok(OpenCodeMetadataEvidence {
        source: "opencode_sanitized_session_export".to_string(),
        strength: "cli_reported_per_assistant".to_string(),
        session_id: owned.session_id.clone(),
        observed_model,
        observed_effort,
        assistant_message_count,
        terminal_message_id,
    })
}

fn parse_object(bytes: &[u8]) -> Result<Map<String, Value>, OpenCodeMetadataError> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|_| OpenCodeMetadataError::InvalidMetadata)?;
    match value {
        Value::Object(map) => Ok(map),
        _ => Err(OpenCodeMetadataError::InvalidMetadata),
    }
}

fn validate_owned_session(owned: &OwnedOpenCodeSession) -> Result<&str, OpenCodeMetadataError> {
    if !is_safe_identifier(&owned.session_id) || !owned.canonical_workdir.is_absolute() {
        return Err(OpenCodeMetadataError::InvalidMetadata);
    }
    let workdir = owned
        .canonical_workdir
        .to_str()
        .ok_or(OpenCodeMetadataError::InvalidMetadata)?;
    if !is_clean_absolute_path(workdir) {
        return Err(OpenCodeMetadataError::InvalidMetadata);
    }
    Ok(workdir)
}

fn validate_identity(
    info: &Map<String, Value>,
    export_info: &Map<String, Value>,
    owned: &OwnedOpenCodeSession,
) -> Result<(), OpenCodeMetadataError> {
    let info_id = info
        .get("id")
        .and_then(Value::as_str)
        .ok_or(OpenCodeMetadataError::InvalidMetadata)?;
    let export_id = export_info
        .get("id")
        .and_then(Value::as_str)
        .ok_or(OpenCodeMetadataError::InvalidMetadata)?;
    if info_id != owned.session_id || export_id != owned.session_id {
        return Err(OpenCodeMetadataError::OwnershipMismatch);
    }
    Ok(())
}

fn validate_real_workdir(
    info: &Map<String, Value>,
    owned_workdir: &str,
) -> Result<(), OpenCodeMetadataError> {
    let location = info
        .get("location")
        .and_then(Value::as_object)
        .ok_or(OpenCodeMetadataError::InvalidMetadata)?;
    let directory = location
        .get("directory")
        .and_then(Value::as_str)
        .ok_or(OpenCodeMetadataError::InvalidMetadata)?;
    if !is_clean_absolute_path(directory) || directory != owned_workdir {
        return Err(OpenCodeMetadataError::OwnershipMismatch);
    }
    Ok(())
}

fn session_created_ms(info: &Map<String, Value>) -> Result<u64, OpenCodeMetadataError> {
    let time = info
        .get("time")
        .and_then(Value::as_object)
        .ok_or(OpenCodeMetadataError::InvalidMetadata)?;
    required_timestamp(time.get("created"), OpenCodeMetadataError::InvalidMetadata)
}

fn required_timestamp(
    value: Option<&Value>,
    missing_error: OpenCodeMetadataError,
) -> Result<u64, OpenCodeMetadataError> {
    match value {
        None => Err(missing_error),
        Some(value) if value.is_null() => Err(missing_error),
        Some(value) => value.as_u64().ok_or(OpenCodeMetadataError::InvalidMetadata),
    }
}

// The public v2 schema encodes outcome as a scalar enum. Both session copies must be the exact
// JSON string `succeeded`; tagged objects and every other shape fail completion validation.
fn session_outcome_succeeded(info: &Map<String, Value>) -> bool {
    info.get("outcome").and_then(Value::as_str) == Some("succeeded")
}

// The contract gives the real info an idle timestamp; `time.idle` is that authoritative field.
fn session_idle_ms(info: &Map<String, Value>) -> Result<u64, OpenCodeMetadataError> {
    let time = info
        .get("time")
        .and_then(Value::as_object)
        .ok_or(OpenCodeMetadataError::MissingCompletion)?;
    required_timestamp(time.get("idle"), OpenCodeMetadataError::MissingCompletion)
}

fn validate_message_envelopes(messages: &[Value]) -> Result<(), OpenCodeMetadataError> {
    let mut message_ids = BTreeSet::new();
    for message in messages {
        let message = message
            .as_object()
            .ok_or(OpenCodeMetadataError::InvalidMetadata)?;
        message
            .get("type")
            .and_then(Value::as_str)
            .ok_or(OpenCodeMetadataError::InvalidMetadata)?;
        if let Some(id_value) = message.get("id") {
            let message_id = id_value
                .as_str()
                .ok_or(OpenCodeMetadataError::InvalidMetadata)?;
            if !message_ids.insert(message_id.to_string()) {
                return Err(OpenCodeMetadataError::InvalidMetadata);
            }
        }
    }
    Ok(())
}

fn split_expected_target(
    expected: &ResolvedModelTarget,
) -> Result<(&str, &str), OpenCodeMetadataError> {
    if expected.base_model.len() > MAX_IDENTIFIER_LEN {
        return Err(OpenCodeMetadataError::InvalidMetadata);
    }
    let (provider_id, model_id) = expected
        .base_model
        .split_once('/')
        .ok_or(OpenCodeMetadataError::InvalidMetadata)?;
    if !is_safe_identifier(provider_id) || !is_safe_model_identifier(model_id) {
        return Err(OpenCodeMetadataError::InvalidMetadata);
    }
    Ok((provider_id, model_id))
}

fn is_clean_absolute_path(value: &str) -> bool {
    if value.is_empty()
        || value.chars().any(char::is_control)
        || !Path::new(value).is_absolute()
        || value.contains("//")
        || (value != "/" && value.ends_with('/'))
    {
        return false;
    }
    Path::new(value)
        .components()
        .all(|component| !matches!(component, Component::CurDir | Component::ParentDir))
}

fn is_safe_model_identifier(value: &str) -> bool {
    !value.is_empty() && value.split('/').all(is_safe_identifier)
}

fn is_safe_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_LEN
        && value.trim() == value
        && !value.starts_with('-')
        && !value.starts_with('.')
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    const SESSION_ID: &str = "ses_owned_123";
    const WORKDIR: &str = "/tmp/threadlane/work";

    fn owned() -> OwnedOpenCodeSession {
        OwnedOpenCodeSession {
            session_id: SESSION_ID.to_string(),
            canonical_workdir: PathBuf::from(WORKDIR),
            started_at_ms: 1_000,
        }
    }

    fn expected() -> ResolvedModelTarget {
        ResolvedModelTarget {
            base_model: "opencode-go/deepseek-v4.1-flash".to_string(),
            effort: Some("max".to_string()),
            cli_model_arg: "opencode-go/deepseek-v4.1-flash#max".to_string(),
        }
    }

    fn expected_without_effort() -> ResolvedModelTarget {
        ResolvedModelTarget {
            base_model: "opencode-go/deepseek-v4.1-flash".to_string(),
            effort: None,
            cli_model_arg: "opencode-go/deepseek-v4.1-flash".to_string(),
        }
    }

    fn assistant_message(id: &str, finish: &str, created: u64, completed: u64) -> Value {
        json!({
            "id": id,
            "type": "assistant",
            "model": {
                "providerID": "opencode-go",
                "id": "deepseek-v4.1-flash",
                "variant": "max"
            },
            "finish": finish,
            "time": {
                "created": created,
                "completed": completed
            },
            "error": null,
            "content": [
                {
                    "type": "reasoning",
                    "text": "PRIVATE_REASONING_SENTINEL"
                }
            ],
            "title": "PRIVATE_MESSAGE_TITLE_SENTINEL"
        })
    }

    fn info_document() -> Value {
        json!({
            "data": {
                "id": SESSION_ID,
                "location": {
                    "directory": WORKDIR,
                    "workspace": "PRIVATE_WORKSPACE_SENTINEL"
                },
                "time": {
                    "created": 2_000,
                    "idle": 5_000
                },
                "outcome": "succeeded",
                "model": {
                    "providerID": "mutable-provider",
                    "id": "mutable-selection",
                    "variant": "high"
                },
                "title": "PRIVATE_INFO_TITLE_SENTINEL",
                "private": {
                    "path": "/tmp/private/path",
                    "secret": "PRIVATE_INFO_SECRET_SENTINEL"
                }
            }
        })
    }

    fn export_document() -> Value {
        json!({
            "info": {
                "id": SESSION_ID,
                "location": {
                    "directory": "[redacted]"
                },
                "time": {
                    "created": 2_000,
                    "idle": 4_500
                },
                "outcome": "succeeded",
                "model": {
                    "providerID": "mutable-provider",
                    "id": "mutable-selection",
                    "variant": "high"
                }
            },
            "messages": [
                assistant_message("msg_1", "tool-calls", 2_500, 3_000),
                assistant_message("msg_2", "stop", 3_100, 4_000),
                {
                    "id": "msg_user",
                    "type": "user",
                    "content": "PRIVATE_USER_PROMPT_SENTINEL"
                }
            ],
            "private_export_field": "PRIVATE_EXPORT_SENTINEL"
        })
    }

    fn run_with(
        owned: &OwnedOpenCodeSession,
        expected: &ResolvedModelTarget,
        info: &Value,
        export: &Value,
    ) -> Result<OpenCodeMetadataEvidence, OpenCodeMetadataError> {
        let info_bytes = serde_json::to_vec(info).expect("serialize synthetic info");
        let export_bytes = serde_json::to_vec(export).expect("serialize synthetic export");
        validate_opencode_metadata(owned, &info_bytes, &export_bytes, expected)
    }

    fn run(
        info: &Value,
        export: &Value,
    ) -> Result<OpenCodeMetadataEvidence, OpenCodeMetadataError> {
        run_with(&owned(), &expected(), info, export)
    }

    fn remove_key(value: &mut Value, key: &str) {
        value
            .as_object_mut()
            .expect("object for remove_key")
            .remove(key);
    }

    fn nested_array(depth: usize) -> Value {
        let mut value = json!(0);
        for _ in 0..depth {
            value = Value::Array(vec![value]);
        }
        value
    }

    #[test]
    fn validates_flat_assistant_evidence_and_drops_private_fields() {
        let evidence = run(&info_document(), &export_document()).expect("valid flat metadata");

        assert_eq!(evidence.source, "opencode_sanitized_session_export");
        assert_eq!(evidence.strength, "cli_reported_per_assistant");
        assert_eq!(evidence.session_id, SESSION_ID);
        assert_eq!(evidence.observed_model, "opencode-go/deepseek-v4.1-flash");
        assert_eq!(evidence.observed_effort.as_deref(), Some("max"));
        assert_eq!(evidence.assistant_message_count, 2);
        assert_eq!(evidence.terminal_message_id, "msg_2");

        let serialized = serde_json::to_string(&evidence).expect("serialize evidence");
        for sentinel in [
            "PRIVATE_",
            "/tmp/threadlane/work",
            "/tmp/private",
            "mutable-provider",
            "mutable-selection",
            "redacted",
            "tool-calls",
            "msg_1",
            "PRIVATE_USER_PROMPT",
            "reasoning",
            "content",
            "title",
        ] {
            assert!(
                !serialized.contains(sentinel),
                "private field `{sentinel}` leaked into evidence: {serialized}"
            );
        }
        assert!(serialized.contains("opencode-go/deepseek-v4.1-flash"));
        assert!(serialized.contains("msg_2"));
    }

    #[test]
    fn rejects_ownership_cwd_and_scope_mismatches() {
        let mut info = info_document();
        info["data"]["id"] = json!("ses_other");
        assert_eq!(
            run(&info, &export_document()).unwrap_err(),
            OpenCodeMetadataError::OwnershipMismatch
        );

        let mut export = export_document();
        export["info"]["id"] = json!("ses_other");
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::OwnershipMismatch
        );

        for directory in [
            "/tmp/threadlane/other",
            "tmp/threadlane/work",
            "/tmp/threadlane/./work",
            "/tmp/threadlane/work/../work",
            "/tmp//threadlane/work",
            "/tmp/threadlane/work/",
            "/tmp/threadlane/work\n",
        ] {
            let mut info = info_document();
            info["data"]["location"]["directory"] = json!(directory);
            assert_eq!(
                run(&info, &export_document()).unwrap_err(),
                OpenCodeMetadataError::OwnershipMismatch,
                "directory `{directory}` must fail ownership"
            );
        }

        let mut info = info_document();
        remove_key(&mut info["data"], "location");
        assert_eq!(
            run(&info, &export_document()).unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );

        let mut export = export_document();
        export["messages"][0]["sessionID"] = json!(SESSION_ID);
        assert!(run(&info_document(), &export).is_ok());

        export["messages"][0]["sessionID"] = json!("ses_other");
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::OwnershipMismatch
        );

        export["messages"][0]["sessionID"] = json!(7);
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::OwnershipMismatch
        );

        let mut export = export_document();
        export["messages"][2]["sessionID"] = json!("ses_other");
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::OwnershipMismatch
        );

        let mut invalid_owned = owned();
        invalid_owned.session_id = "bad\nidentifier".to_string();
        assert_eq!(
            run_with(
                &invalid_owned,
                &expected(),
                &info_document(),
                &export_document()
            )
            .unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );

        invalid_owned.session_id = SESSION_ID.to_string();
        invalid_owned.canonical_workdir = PathBuf::from("relative/work");
        assert_eq!(
            run_with(
                &invalid_owned,
                &expected(),
                &info_document(),
                &export_document()
            )
            .unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );

        let mut sanitized_cwd_is_ignored = export_document();
        sanitized_cwd_is_ignored["info"]["location"]["directory"] = json!("PRIVATE_REDACTED_CWD");
        assert!(run(&info_document(), &sanitized_cwd_is_ignored).is_ok());
    }

    #[test]
    fn rejects_stale_and_inconsistent_times() {
        let mut info = info_document();
        let mut export = export_document();
        info["data"]["time"]["created"] = json!(500);
        export["info"]["time"]["created"] = json!(500);
        assert_eq!(
            run(&info, &export).unwrap_err(),
            OpenCodeMetadataError::StaleSession
        );

        let mut info = info_document();
        info["data"]["time"]["created"] = json!(500);
        assert_eq!(
            run(&info, &export_document()).unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );

        let mut info = info_document();
        info["data"]["time"]["created"] = json!("2000");
        assert_eq!(
            run(&info, &export_document()).unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );

        let mut info = info_document();
        info["data"]["time"]["idle"] = json!(3_500);
        assert_eq!(
            run(&info, &export_document()).unwrap_err(),
            OpenCodeMetadataError::MissingCompletion
        );

        let mut info = info_document();
        remove_key(&mut info["data"]["time"], "idle");
        assert_eq!(
            run(&info, &export_document()).unwrap_err(),
            OpenCodeMetadataError::MissingCompletion
        );

        let mut info = info_document();
        info["data"]["time"]["idle"] = json!("5000");
        assert_eq!(
            run(&info, &export_document()).unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );

        let mut export = export_document();
        export["messages"][0]["time"]["created"] = json!(1_500);
        export["messages"][0]["time"]["completed"] = json!(1_600);
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::StaleSession
        );

        let mut export = export_document();
        export["messages"][0]["time"]["completed"] = json!(2_400);
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::MissingCompletion
        );

        let mut export = export_document();
        remove_key(&mut export["messages"][1]["time"], "completed");
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::MissingCompletion
        );

        let mut export = export_document();
        remove_key(&mut export["messages"][1]["time"], "created");
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::MissingCompletion
        );

        let mut export = export_document();
        export["messages"][1]["time"]["created"] = json!(3_100.5);
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );

        let mut export = export_document();
        export["messages"][1]["time"]["completed"] = json!("4000");
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );

        let mut info = info_document();
        info["data"]["time"]["idle"] = json!(1_500);
        assert_eq!(
            run(&info, &export_document()).unwrap_err(),
            OpenCodeMetadataError::MissingCompletion
        );
    }

    #[test]
    fn rejects_assistant_completion_after_real_idle_even_when_final_is_earlier() {
        let mut export = export_document();
        export["messages"][0]["time"]["completed"] = json!(6_000);
        export["messages"][1]["time"]["created"] = json!(3_100);
        export["messages"][1]["time"]["completed"] = json!(4_000);
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::MissingCompletion
        );

        let mut info = info_document();
        info["data"]["time"]["idle"] = json!(6_000);
        assert!(run(&info, &export).is_ok());
    }

    #[test]
    fn rejects_duplicate_identifiers_before_model_checks() {
        let mut export = export_document();
        export["messages"][1]["id"] = json!("msg_1");
        export["messages"][1]["model"]["id"] = json!("wrong-model");
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );

        let mut export = export_document();
        export["messages"][2]["id"] = json!("msg_2");
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );

        let mut export = export_document();
        let mut second_user = json!({
            "id": "msg_user",
            "type": "user"
        });
        second_user["id"] = json!("msg_user");
        export["messages"]
            .as_array_mut()
            .expect("messages array")
            .push(second_user);
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );
    }

    #[test]
    fn rejects_missing_assistants_models_effort_and_completion() {
        let mut export = export_document();
        export["messages"] = json!([]);
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::MissingModel
        );

        let mut export = export_document();
        remove_key(&mut export["messages"][0], "model");
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::MissingModel
        );

        let mut export = export_document();
        remove_key(&mut export["messages"][0]["model"], "providerID");
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::MissingModel
        );

        let mut export = export_document();
        remove_key(&mut export["messages"][0]["model"], "id");
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::MissingModel
        );

        let mut export = export_document();
        remove_key(&mut export["messages"][0]["model"], "variant");
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::MissingEffort
        );

        for finish in ["tool-calls", "length", "error", "content-filter", "unknown"] {
            let mut export = export_document();
            export["messages"][1]["finish"] = json!(finish);
            assert_eq!(
                run(&info_document(), &export).unwrap_err(),
                OpenCodeMetadataError::MissingCompletion,
                "final finish `{finish}` must not establish completion"
            );
        }

        let mut export = export_document();
        remove_key(&mut export["messages"][1], "finish");
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::MissingCompletion
        );

        let mut info = info_document();
        remove_key(&mut info["data"], "outcome");
        assert_eq!(
            run(&info, &export_document()).unwrap_err(),
            OpenCodeMetadataError::MissingCompletion
        );

        let mut export = export_document();
        export["info"]["outcome"] = json!("failed");
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::MissingCompletion
        );

        let mut export = export_document();
        export["messages"][1]["error"] = json!({
            "message": "PRIVATE_PROVIDER_ERROR_SENTINEL"
        });
        let error = run(&info_document(), &export).unwrap_err();
        assert_eq!(error, OpenCodeMetadataError::ProviderError);
        assert!(!error.as_str().contains("PRIVATE_PROVIDER_ERROR_SENTINEL"));
        assert!(!error
            .to_string()
            .contains("PRIVATE_PROVIDER_ERROR_SENTINEL"));

        let no_effort_expected = expected_without_effort();
        let mut no_effort_export = export_document();
        remove_key(&mut no_effort_export["messages"][0]["model"], "variant");
        remove_key(&mut no_effort_export["messages"][1]["model"], "variant");
        let evidence = run_with(
            &owned(),
            &no_effort_expected,
            &info_document(),
            &no_effort_export,
        )
        .expect("no-effort target accepts absent variants");
        assert_eq!(evidence.observed_effort, None);

        let mut unexpected_effort = no_effort_export.clone();
        unexpected_effort["messages"][1]["model"]["variant"] = json!("max");
        assert_eq!(
            run_with(
                &owned(),
                &no_effort_expected,
                &info_document(),
                &unexpected_effort
            )
            .unwrap_err(),
            OpenCodeMetadataError::EffortMismatch
        );
    }

    #[test]
    fn mutable_session_selection_never_fills_assistant_fields() {
        let mut info = info_document();
        info["data"]["model"] = json!({
            "providerID": "opencode-go",
            "id": "deepseek-v4.1-flash",
            "variant": "max"
        });
        let mut export = export_document();
        export["info"]["model"] = info["data"]["model"].clone();
        remove_key(&mut export["messages"][0], "model");
        assert_eq!(
            run(&info, &export).unwrap_err(),
            OpenCodeMetadataError::MissingModel
        );

        let mut export = export_document();
        export["info"]["model"] = info["data"]["model"].clone();
        remove_key(&mut export["messages"][0]["model"], "variant");
        assert_eq!(
            run(&info, &export).unwrap_err(),
            OpenCodeMetadataError::MissingEffort
        );
    }

    #[test]
    fn rejects_mixed_models_and_variants() {
        let mut export = export_document();
        export["messages"][1]["model"]["providerID"] = json!("other-provider");
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::ModelMismatch
        );

        let mut export = export_document();
        export["messages"][1]["model"]["id"] = json!("other-model");
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::ModelMismatch
        );

        let mut export = export_document();
        export["messages"][1]["model"]["variant"] = json!("high");
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::EffortMismatch
        );

        let mut export = export_document();
        remove_key(&mut export["messages"][1]["model"], "variant");
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::MissingEffort
        );
    }

    #[test]
    fn requires_final_stop_and_rejects_provider_errors() {
        let mut export = export_document();
        export["messages"][0]["finish"] = json!("stop");
        export["messages"][1]["finish"] = json!("stop");
        assert!(run(&info_document(), &export).is_ok());

        let mut export = export_document();
        export["messages"][1]["finish"] = json!("tool-calls");
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::MissingCompletion
        );

        let mut export = export_document();
        export["messages"][0]["error"] = json!({
            "message": "intermediate failure"
        });
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::ProviderError
        );

        let mut export = export_document();
        export["messages"][1]["error"] = json!({
            "message": "terminal failure"
        });
        assert_eq!(
            run(&info_document(), &export).unwrap_err(),
            OpenCodeMetadataError::ProviderError
        );

        let mut export = export_document();
        export["messages"][1]["finish"] = json!("stop");
        export["messages"][1]["error"] = Value::Null;
        assert!(run(&info_document(), &export).is_ok());
    }

    #[test]
    fn requires_scalar_succeeded_outcome_and_real_idle() {
        assert!(run(&info_document(), &export_document()).is_ok());

        for tagged_outcome in [
            json!({
                "type": "succeeded"
            }),
            json!({
                "status": "succeeded"
            }),
            json!({
                "type": "succeeded",
                "status": "succeeded"
            }),
            json!({
                "type": "succeeded",
                "status": "failed"
            }),
        ] {
            let mut info = info_document();
            info["data"]["outcome"] = tagged_outcome.clone();
            assert_eq!(
                run(&info, &export_document()).unwrap_err(),
                OpenCodeMetadataError::MissingCompletion
            );

            let mut export = export_document();
            export["info"]["outcome"] = tagged_outcome;
            assert_eq!(
                run(&info_document(), &export).unwrap_err(),
                OpenCodeMetadataError::MissingCompletion
            );
        }

        for non_succeeded in ["failed", "interrupted"] {
            let mut info = info_document();
            info["data"]["outcome"] = json!(non_succeeded);
            assert_eq!(
                run(&info, &export_document()).unwrap_err(),
                OpenCodeMetadataError::MissingCompletion
            );
        }

        let mut missing_idle = info_document();
        remove_key(&mut missing_idle["data"]["time"], "idle");
        missing_idle["data"]["time"]["updated"] = json!(5_000);
        assert_eq!(
            run(&missing_idle, &export_document()).unwrap_err(),
            OpenCodeMetadataError::MissingCompletion
        );

        let mut invalid_idle = info_document();
        invalid_idle["data"]["time"]["idle"] = json!("not-a-time");
        invalid_idle["data"]["time"]["updated"] = json!(5_000);
        assert_eq!(
            run(&invalid_idle, &export_document()).unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );
    }

    #[test]
    fn enforces_budgets_json_depth_message_shape_and_string_limits() {
        let export_bytes = serde_json::to_vec(&export_document()).expect("export bytes");
        let oversized_info = vec![b' '; MAX_OPENCODE_INFO_BYTES + 1];
        assert_eq!(
            validate_opencode_metadata(&owned(), &oversized_info, &export_bytes, &expected())
                .unwrap_err(),
            OpenCodeMetadataError::OutputOverflow
        );

        let info_bytes = serde_json::to_vec(&info_document()).expect("info bytes");
        let oversized_export = vec![b' '; MAX_OPENCODE_EXPORT_BYTES + 1];
        assert_eq!(
            validate_opencode_metadata(&owned(), &info_bytes, &oversized_export, &expected())
                .unwrap_err(),
            OpenCodeMetadataError::OutputOverflow
        );

        let mut too_many = export_document();
        let mut messages = Vec::new();
        for index in 0..=MAX_OPENCODE_MESSAGES {
            messages.push(json!({
                "id": format!("msg_{index}"),
                "type": "user"
            }));
        }
        too_many["messages"] = Value::Array(messages);
        let too_many_bytes = serde_json::to_vec(&too_many).expect("many messages bytes");
        assert_eq!(
            validate_opencode_metadata(&owned(), &info_bytes, &too_many_bytes, &expected())
                .unwrap_err(),
            OpenCodeMetadataError::OutputOverflow
        );

        assert_eq!(
            validate_opencode_metadata(&owned(), b"not json", &export_bytes, &expected())
                .unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );
        assert_eq!(
            validate_opencode_metadata(&owned(), &info_bytes, b"not json", &expected())
                .unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );
        assert_eq!(
            validate_opencode_metadata(&owned(), b"[]", &export_bytes, &expected()).unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );

        let mut deep_info = info_document();
        deep_info["data"]["private"] = nested_array(200);
        let deep_info_bytes = serde_json::to_vec(&deep_info).expect("deep info bytes");
        assert_eq!(
            validate_opencode_metadata(&owned(), &deep_info_bytes, &export_bytes, &expected())
                .unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );

        let empty_object = json!({});
        assert_eq!(
            run(&empty_object, &export_document()).unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );

        let mut missing_messages = export_document();
        remove_key(&mut missing_messages, "messages");
        assert_eq!(
            run(&info_document(), &missing_messages).unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );

        let mut malformed_message = export_document();
        malformed_message["messages"][0] = json!(7);
        assert_eq!(
            run(&info_document(), &malformed_message).unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );

        let mut missing_type = export_document();
        remove_key(&mut missing_type["messages"][0], "type");
        assert_eq!(
            run(&info_document(), &missing_type).unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );

        let mut long_assistant_id = export_document();
        long_assistant_id["messages"][0]["id"] = json!("a".repeat(MAX_IDENTIFIER_LEN + 1));
        assert_eq!(
            run(&info_document(), &long_assistant_id).unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );

        let mut long_model_id = export_document();
        long_model_id["messages"][0]["model"]["id"] = json!("m".repeat(MAX_IDENTIFIER_LEN + 1));
        assert_eq!(
            run(&info_document(), &long_model_id).unwrap_err(),
            OpenCodeMetadataError::InvalidMetadata
        );
    }

    #[test]
    fn error_codes_are_finite_snake_case_and_do_not_echo_input() {
        let cases = [
            (OpenCodeMetadataError::OutputOverflow, "output_overflow"),
            (OpenCodeMetadataError::InvalidMetadata, "invalid_metadata"),
            (
                OpenCodeMetadataError::OwnershipMismatch,
                "ownership_mismatch",
            ),
            (OpenCodeMetadataError::StaleSession, "stale_session"),
            (OpenCodeMetadataError::MissingModel, "missing_model"),
            (OpenCodeMetadataError::ModelMismatch, "model_mismatch"),
            (OpenCodeMetadataError::MissingEffort, "missing_effort"),
            (OpenCodeMetadataError::EffortMismatch, "effort_mismatch"),
            (
                OpenCodeMetadataError::MissingCompletion,
                "missing_completion",
            ),
            (OpenCodeMetadataError::ProviderError, "provider_error"),
        ];
        for (error, code) in cases {
            assert_eq!(error.as_str(), code);
            assert_eq!(error.to_string(), code);
            assert!(!format!("{error:?}").contains("PRIVATE_"));
        }

        let private_json = b"{\"data\":{\"private\":\"PRIVATE_JSON_SENTINEL\"}} trailing";
        let error =
            validate_opencode_metadata(&owned(), private_json, b"{}", &expected()).unwrap_err();
        assert_eq!(error, OpenCodeMetadataError::InvalidMetadata);
        assert!(!error.as_str().contains("PRIVATE_JSON_SENTINEL"));
        assert!(!error.to_string().contains("PRIVATE_JSON_SENTINEL"));

        let mut wrong_model = export_document();
        wrong_model["messages"][0]["model"]["id"] = json!("PRIVATE_MODEL_SENTINEL");
        let error = run(&info_document(), &wrong_model).unwrap_err();
        assert_eq!(error, OpenCodeMetadataError::ModelMismatch);
        assert!(!error.as_str().contains("PRIVATE_MODEL_SENTINEL"));
        assert!(!error.to_string().contains("PRIVATE_MODEL_SENTINEL"));
    }

    #[test]
    fn ownership_and_completion_precedence_is_deterministic() {
        let mut info = info_document();
        info["data"]["id"] = json!("ses_other");
        remove_key(&mut info["data"]["time"], "idle");
        let mut export = export_document();
        export["messages"][1]["model"]["id"] = json!("wrong-model");
        assert_eq!(
            run(&info, &export).unwrap_err(),
            OpenCodeMetadataError::OwnershipMismatch
        );

        let mut info = info_document();
        let mut export = export_document();
        info["data"]["time"]["created"] = json!(500);
        export["info"]["time"]["created"] = json!(500);
        remove_key(&mut info["data"], "outcome");
        assert_eq!(
            run(&info, &export).unwrap_err(),
            OpenCodeMetadataError::StaleSession
        );

        let mut info = info_document();
        remove_key(&mut info["data"], "outcome");
        let mut export = export_document();
        remove_key(&mut export["messages"][0], "model");
        assert_eq!(
            run(&info, &export).unwrap_err(),
            OpenCodeMetadataError::MissingCompletion
        );
    }
}
