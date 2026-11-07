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

/// Finite, software-owned validation stage for OpenCode v2 metadata diagnostics.
///
/// Variants carry no raw JSON, transcript text, titles, content, reasoning, URLs,
/// filesystem paths, identifiers, tokens, or provider error strings. Safe machine-readable
/// literals are exposed only through [`Self::as_str`] and [`Self::qualified_error_code`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenCodeMetadataStage {
    /// Trusted caller inputs (`OwnedOpenCodeSession` or `ResolvedModelTarget`) failed identifier or canonical path validation.
    CallerContract,
    /// `session.create` stdout was not valid JSON or not a top-level JSON object.
    CreateResponseJson,
    /// `session.create` JSON root was missing a valid `data` envelope or required session creation fields.
    CreateResponseEnvelope,
    /// `session.get` (`info_response`) was not valid JSON or not a top-level JSON object.
    InfoResponseJson,
    /// `session.get` (`info_response`) JSON root was missing the `data` object envelope.
    InfoResponseEnvelope,
    /// `session export --standalone --sanitize` (`sanitized_export`) was not valid JSON or not a top-level JSON object.
    ExportJson,
    /// `sanitized_export` JSON root was missing the `info` object or `messages` array envelope.
    ExportEnvelope,
    /// Session `id` or `location.directory` field was missing or had an invalid JSON type in `info` or `export.info`.
    IdentityWorkdir,
    /// Session or assistant timestamp (`time`, `time.created`, `time.idle`, `time.completed`) was missing, non-integer, or inconsistent between `info` and `export.info`.
    Timestamps,
    /// An entry in `export.messages` was not a JSON object, lacked a string `type`, had a non-string or unsafe `id`, or reused a duplicate message `id`.
    MessageEnvelope,
    /// An assistant `model.providerID`, `model.id`, or `model.variant` field had an invalid JSON type or failed safe identifier validation.
    AssistantTarget,
    /// Terminal completion check (`outcome`, assistant count, final `finish`, or `time.idle` ordering) failed.
    TerminalCompletion,
}

impl OpenCodeMetadataStage {
    /// All finite validation stages in canonical declaration order.
    pub const ALL: [Self; 12] = [
        Self::CallerContract,
        Self::CreateResponseJson,
        Self::CreateResponseEnvelope,
        Self::InfoResponseJson,
        Self::InfoResponseEnvelope,
        Self::ExportJson,
        Self::ExportEnvelope,
        Self::IdentityWorkdir,
        Self::Timestamps,
        Self::MessageEnvelope,
        Self::AssistantTarget,
        Self::TerminalCompletion,
    ];

    /// Returns the finite `snake_case` stage code.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::CallerContract => "caller_contract",
            Self::CreateResponseJson => "create_response_json",
            Self::CreateResponseEnvelope => "create_response_envelope",
            Self::InfoResponseJson => "info_response_json",
            Self::InfoResponseEnvelope => "info_response_envelope",
            Self::ExportJson => "export_json",
            Self::ExportEnvelope => "export_envelope",
            Self::IdentityWorkdir => "identity_workdir",
            Self::Timestamps => "timestamps",
            Self::MessageEnvelope => "message_envelope",
            Self::AssistantTarget => "assistant_target",
            Self::TerminalCompletion => "terminal_completion",
        }
    }

    /// Returns the fixed stage-qualified `invalid_metadata:<stage>` error code.
    pub const fn qualified_error_code(&self) -> &'static str {
        match self {
            Self::CallerContract => "invalid_metadata:caller_contract",
            Self::CreateResponseJson => "invalid_metadata:create_response_json",
            Self::CreateResponseEnvelope => "invalid_metadata:create_response_envelope",
            Self::InfoResponseJson => "invalid_metadata:info_response_json",
            Self::InfoResponseEnvelope => "invalid_metadata:info_response_envelope",
            Self::ExportJson => "invalid_metadata:export_json",
            Self::ExportEnvelope => "invalid_metadata:export_envelope",
            Self::IdentityWorkdir => "invalid_metadata:identity_workdir",
            Self::Timestamps => "invalid_metadata:timestamps",
            Self::MessageEnvelope => "invalid_metadata:message_envelope",
            Self::AssistantTarget => "invalid_metadata:assistant_target",
            Self::TerminalCompletion => "invalid_metadata:terminal_completion",
        }
    }

    /// Parses a canonical `snake_case` stage literal, returning `None` for any unknown string.
    pub fn from_code(raw: &str) -> Option<Self> {
        match raw {
            "caller_contract" => Some(Self::CallerContract),
            "create_response_json" => Some(Self::CreateResponseJson),
            "create_response_envelope" => Some(Self::CreateResponseEnvelope),
            "info_response_json" => Some(Self::InfoResponseJson),
            "info_response_envelope" => Some(Self::InfoResponseEnvelope),
            "export_json" => Some(Self::ExportJson),
            "export_envelope" => Some(Self::ExportEnvelope),
            "identity_workdir" => Some(Self::IdentityWorkdir),
            "timestamps" => Some(Self::Timestamps),
            "message_envelope" => Some(Self::MessageEnvelope),
            "assistant_target" => Some(Self::AssistantTarget),
            "terminal_completion" => Some(Self::TerminalCompletion),
            _ => None,
        }
    }
}

impl fmt::Display for OpenCodeMetadataStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Privacy-safe OpenCode v2 metadata validation failure pairing a coarse
/// [`OpenCodeMetadataError`] with an optional finite [`OpenCodeMetadataStage`].
///
/// Contains only fixed software-owned enum values. Legacy undifferentiated `"invalid_metadata"`
/// reports remain readable via [`Self::from_error_code`] with `stage: None` (unknown/unavailable
/// stage detail, never an invented cause).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OpenCodeMetadataDiagnostic {
    /// Coarse semantic metadata validation error category.
    pub error: OpenCodeMetadataError,
    /// Finite validation stage when available (`None` for legacy undifferentiated `invalid_metadata` reports or un-staged errors).
    pub stage: Option<OpenCodeMetadataStage>,
}

impl OpenCodeMetadataDiagnostic {
    /// Creates a diagnostic pairing `error` with an optional finite `stage`.
    pub const fn new(error: OpenCodeMetadataError, stage: Option<OpenCodeMetadataStage>) -> Self {
        Self { error, stage }
    }

    /// Creates a diagnostic for `error` without a specific stage (`stage: None`).
    pub const fn from_error(error: OpenCodeMetadataError) -> Self {
        Self { error, stage: None }
    }

    /// Creates a diagnostic pairing `error` with `stage`.
    pub const fn staged(error: OpenCodeMetadataError, stage: OpenCodeMetadataStage) -> Self {
        Self {
            error,
            stage: Some(stage),
        }
    }

    /// Creates an [`OpenCodeMetadataError::InvalidMetadata`] diagnostic qualified by `stage`.
    pub const fn invalid_metadata(stage: OpenCodeMetadataStage) -> Self {
        Self {
            error: OpenCodeMetadataError::InvalidMetadata,
            stage: Some(stage),
        }
    }

    /// Returns the finite machine-readable error code.
    ///
    /// Preserves existing semantic error codes (`output_overflow`, `ownership_mismatch`,
    /// `stale_session`, `missing_model`, `model_mismatch`, `missing_effort`, `effort_mismatch`,
    /// `missing_completion`, `provider_error`), emits `"invalid_metadata:<stage>"` when
    /// `error == InvalidMetadata` and `stage` is `Some`, and emits `"invalid_metadata"` for
    /// legacy undifferentiated `InvalidMetadata` (`stage: None`).
    pub fn as_str(&self) -> &'static str {
        match (self.error, self.stage) {
            (OpenCodeMetadataError::InvalidMetadata, Some(stage)) => stage.qualified_error_code(),
            (error, _) => error.as_str(),
        }
    }

    /// Parses a stored metadata error code into a structured [`OpenCodeMetadataDiagnostic`].
    ///
    /// - `"invalid_metadata"` decodes with `error: InvalidMetadata` and `stage: None`
    ///   (preserving legacy reports with unknown/unavailable stage detail, never an invented cause).
    /// - `"invalid_metadata:<stage>"` decodes only known finite [`OpenCodeMetadataStage`] literals.
    /// - Semantic error codes decode with their corresponding [`OpenCodeMetadataError`] and `stage: None`.
    /// - Any unrecognized or arbitrary string returns `None`.
    pub fn from_error_code(raw: &str) -> Option<Self> {
        if let Some(stage_raw) = raw.strip_prefix("invalid_metadata:") {
            return OpenCodeMetadataStage::from_code(stage_raw).map(Self::invalid_metadata);
        }
        match raw {
            "output_overflow" => Some(Self::from_error(OpenCodeMetadataError::OutputOverflow)),
            "invalid_metadata" => Some(Self::from_error(OpenCodeMetadataError::InvalidMetadata)),
            "ownership_mismatch" => {
                Some(Self::from_error(OpenCodeMetadataError::OwnershipMismatch))
            }
            "stale_session" => Some(Self::from_error(OpenCodeMetadataError::StaleSession)),
            "missing_model" => Some(Self::from_error(OpenCodeMetadataError::MissingModel)),
            "model_mismatch" => Some(Self::from_error(OpenCodeMetadataError::ModelMismatch)),
            "missing_effort" => Some(Self::from_error(OpenCodeMetadataError::MissingEffort)),
            "effort_mismatch" => Some(Self::from_error(OpenCodeMetadataError::EffortMismatch)),
            "missing_completion" => {
                Some(Self::from_error(OpenCodeMetadataError::MissingCompletion))
            }
            "provider_error" => Some(Self::from_error(OpenCodeMetadataError::ProviderError)),
            _ => None,
        }
    }
}

impl fmt::Display for OpenCodeMetadataDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::error::Error for OpenCodeMetadataDiagnostic {}

impl From<OpenCodeMetadataDiagnostic> for OpenCodeMetadataError {
    fn from(diagnostic: OpenCodeMetadataDiagnostic) -> Self {
        diagnostic.error
    }
}

/// Validates synthetic OpenCode v2 metadata and returns compact allowlisted evidence.
///
/// Preserves the coarse [`OpenCodeMetadataError`] return signature for callers that do not
/// inspect stage-level provenance. Call [`validate_opencode_metadata_with_diagnostic`] for
/// stage-qualified [`OpenCodeMetadataDiagnostic`] results.
pub fn validate_opencode_metadata(
    owned: &OwnedOpenCodeSession,
    info_response: &[u8],
    sanitized_export: &[u8],
    expected: &ResolvedModelTarget,
) -> Result<OpenCodeMetadataEvidence, OpenCodeMetadataError> {
    validate_opencode_metadata_with_diagnostic(owned, info_response, sanitized_export, expected)
        .map_err(|diagnostic| diagnostic.error)
}

/// Validates synthetic OpenCode v2 metadata and returns compact allowlisted evidence or a
/// finite, privacy-safe [`OpenCodeMetadataDiagnostic`].
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
pub fn validate_opencode_metadata_with_diagnostic(
    owned: &OwnedOpenCodeSession,
    info_response: &[u8],
    sanitized_export: &[u8],
    expected: &ResolvedModelTarget,
) -> Result<OpenCodeMetadataEvidence, OpenCodeMetadataDiagnostic> {
    if info_response.len() > MAX_OPENCODE_INFO_BYTES {
        return Err(OpenCodeMetadataDiagnostic::from_error(
            OpenCodeMetadataError::OutputOverflow,
        ));
    }
    if sanitized_export.len() > MAX_OPENCODE_EXPORT_BYTES {
        return Err(OpenCodeMetadataDiagnostic::from_error(
            OpenCodeMetadataError::OutputOverflow,
        ));
    }

    let owned_workdir = validate_owned_session(owned)?;

    let info_root = parse_object(info_response, OpenCodeMetadataStage::InfoResponseJson)?;
    let info = info_root
        .get("data")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            OpenCodeMetadataDiagnostic::invalid_metadata(
                OpenCodeMetadataStage::InfoResponseEnvelope,
            )
        })?;

    let export_root = parse_object(sanitized_export, OpenCodeMetadataStage::ExportJson)?;
    let export_info = export_root
        .get("info")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            OpenCodeMetadataDiagnostic::invalid_metadata(OpenCodeMetadataStage::ExportEnvelope)
        })?;
    let messages = export_root
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            OpenCodeMetadataDiagnostic::invalid_metadata(OpenCodeMetadataStage::ExportEnvelope)
        })?;
    if messages.len() > MAX_OPENCODE_MESSAGES {
        return Err(OpenCodeMetadataDiagnostic::from_error(
            OpenCodeMetadataError::OutputOverflow,
        ));
    }

    validate_identity(info, export_info, owned)?;
    validate_real_workdir(info, owned_workdir)?;

    let info_created_ms = session_created_ms(info)?;
    let export_created_ms = session_created_ms(export_info)?;
    if info_created_ms != export_created_ms {
        return Err(OpenCodeMetadataDiagnostic::invalid_metadata(
            OpenCodeMetadataStage::Timestamps,
        ));
    }
    if info_created_ms < owned.started_at_ms {
        return Err(OpenCodeMetadataDiagnostic::staged(
            OpenCodeMetadataError::StaleSession,
            OpenCodeMetadataStage::Timestamps,
        ));
    }

    if !session_outcome_succeeded(info) || !session_outcome_succeeded(export_info) {
        return Err(OpenCodeMetadataDiagnostic::staged(
            OpenCodeMetadataError::MissingCompletion,
            OpenCodeMetadataStage::TerminalCompletion,
        ));
    }

    validate_message_envelopes(messages)?;

    let (expected_provider, expected_model) = split_expected_target(expected)?;
    if expected
        .effort
        .as_deref()
        .is_some_and(|effort| !is_safe_identifier(effort))
    {
        return Err(OpenCodeMetadataDiagnostic::invalid_metadata(
            OpenCodeMetadataStage::CallerContract,
        ));
    }

    let mut assistant_message_count = 0usize;
    let mut terminal_message_id: Option<String> = None;
    let mut terminal_finish: Option<String> = None;
    let mut max_assistant_completed_ms = 0u64;
    let mut observed_model: Option<String> = None;
    let mut observed_effort: Option<String> = None;

    for message in messages {
        let message = message.as_object().ok_or_else(|| {
            OpenCodeMetadataDiagnostic::invalid_metadata(OpenCodeMetadataStage::MessageEnvelope)
        })?;
        let message_type = message.get("type").and_then(Value::as_str).ok_or_else(|| {
            OpenCodeMetadataDiagnostic::invalid_metadata(OpenCodeMetadataStage::MessageEnvelope)
        })?;

        match message.get("sessionID") {
            None => {}
            Some(value) if value.is_null() => {}
            Some(value) => {
                let supplied_session_id = value.as_str().ok_or_else(|| {
                    OpenCodeMetadataDiagnostic::staged(
                        OpenCodeMetadataError::OwnershipMismatch,
                        OpenCodeMetadataStage::IdentityWorkdir,
                    )
                })?;
                if supplied_session_id != owned.session_id {
                    return Err(OpenCodeMetadataDiagnostic::staged(
                        OpenCodeMetadataError::OwnershipMismatch,
                        OpenCodeMetadataStage::IdentityWorkdir,
                    ));
                }
            }
        }

        if message_type != "assistant" {
            continue;
        }

        let message_id = message.get("id").and_then(Value::as_str).ok_or_else(|| {
            OpenCodeMetadataDiagnostic::invalid_metadata(OpenCodeMetadataStage::MessageEnvelope)
        })?;
        if !is_safe_identifier(message_id) {
            return Err(OpenCodeMetadataDiagnostic::invalid_metadata(
                OpenCodeMetadataStage::MessageEnvelope,
            ));
        }

        if message
            .get("error")
            .is_some_and(|error_value| !error_value.is_null())
        {
            return Err(OpenCodeMetadataDiagnostic::from_error(
                OpenCodeMetadataError::ProviderError,
            ));
        }

        let time = message
            .get("time")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                OpenCodeMetadataDiagnostic::staged(
                    OpenCodeMetadataError::MissingCompletion,
                    OpenCodeMetadataStage::Timestamps,
                )
            })?;
        let created_ms = required_timestamp(
            time.get("created"),
            OpenCodeMetadataDiagnostic::staged(
                OpenCodeMetadataError::MissingCompletion,
                OpenCodeMetadataStage::Timestamps,
            ),
        )?;
        let completed_ms = required_timestamp(
            time.get("completed"),
            OpenCodeMetadataDiagnostic::staged(
                OpenCodeMetadataError::MissingCompletion,
                OpenCodeMetadataStage::Timestamps,
            ),
        )?;
        if completed_ms < created_ms {
            return Err(OpenCodeMetadataDiagnostic::staged(
                OpenCodeMetadataError::MissingCompletion,
                OpenCodeMetadataStage::Timestamps,
            ));
        }
        if created_ms < info_created_ms {
            return Err(OpenCodeMetadataDiagnostic::staged(
                OpenCodeMetadataError::StaleSession,
                OpenCodeMetadataStage::Timestamps,
            ));
        }
        max_assistant_completed_ms = max_assistant_completed_ms.max(completed_ms);

        let model = message
            .get("model")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                OpenCodeMetadataDiagnostic::staged(
                    OpenCodeMetadataError::MissingModel,
                    OpenCodeMetadataStage::AssistantTarget,
                )
            })?;
        let provider_id = model
            .get("providerID")
            .ok_or_else(|| {
                OpenCodeMetadataDiagnostic::staged(
                    OpenCodeMetadataError::MissingModel,
                    OpenCodeMetadataStage::AssistantTarget,
                )
            })?
            .as_str()
            .ok_or_else(|| {
                OpenCodeMetadataDiagnostic::staged(
                    OpenCodeMetadataError::MissingModel,
                    OpenCodeMetadataStage::AssistantTarget,
                )
            })?;
        let model_id = model
            .get("id")
            .ok_or_else(|| {
                OpenCodeMetadataDiagnostic::staged(
                    OpenCodeMetadataError::MissingModel,
                    OpenCodeMetadataStage::AssistantTarget,
                )
            })?
            .as_str()
            .ok_or_else(|| {
                OpenCodeMetadataDiagnostic::staged(
                    OpenCodeMetadataError::MissingModel,
                    OpenCodeMetadataStage::AssistantTarget,
                )
            })?;
        let variant = match model.get("variant") {
            None => None,
            Some(value) if value.is_null() => None,
            Some(value) => Some(value.as_str().ok_or_else(|| {
                OpenCodeMetadataDiagnostic::invalid_metadata(OpenCodeMetadataStage::AssistantTarget)
            })?),
        };

        if !is_safe_identifier(provider_id) || !is_safe_model_identifier(model_id) {
            return Err(OpenCodeMetadataDiagnostic::invalid_metadata(
                OpenCodeMetadataStage::AssistantTarget,
            ));
        }
        if variant.is_some_and(|value| !is_safe_identifier(value)) {
            return Err(OpenCodeMetadataDiagnostic::invalid_metadata(
                OpenCodeMetadataStage::AssistantTarget,
            ));
        }

        if provider_id != expected_provider || model_id != expected_model {
            return Err(OpenCodeMetadataDiagnostic::staged(
                OpenCodeMetadataError::ModelMismatch,
                OpenCodeMetadataStage::AssistantTarget,
            ));
        }
        match (expected.effort.as_deref(), variant) {
            (Some(_), None) => {
                return Err(OpenCodeMetadataDiagnostic::staged(
                    OpenCodeMetadataError::MissingEffort,
                    OpenCodeMetadataStage::AssistantTarget,
                ));
            }
            (Some(expected_effort), Some(observed_variant))
                if expected_effort != observed_variant =>
            {
                return Err(OpenCodeMetadataDiagnostic::staged(
                    OpenCodeMetadataError::EffortMismatch,
                    OpenCodeMetadataStage::AssistantTarget,
                ));
            }
            (None, Some(_)) => {
                return Err(OpenCodeMetadataDiagnostic::staged(
                    OpenCodeMetadataError::EffortMismatch,
                    OpenCodeMetadataStage::AssistantTarget,
                ));
            }
            _ => {}
        }

        let model_string = format!("{provider_id}/{model_id}");
        if let Some(previous) = observed_model.as_ref() {
            if previous.as_str() != model_string.as_str() {
                return Err(OpenCodeMetadataDiagnostic::staged(
                    OpenCodeMetadataError::ModelMismatch,
                    OpenCodeMetadataStage::AssistantTarget,
                ));
            }
        } else {
            observed_model = Some(model_string);
        }
        if observed_effort.is_none() {
            observed_effort = variant.map(|value| value.to_string());
        }

        let finish = match message.get("finish") {
            None => None,
            Some(value) if value.is_null() => None,
            Some(value) => Some(value.as_str().ok_or_else(|| {
                OpenCodeMetadataDiagnostic::invalid_metadata(
                    OpenCodeMetadataStage::TerminalCompletion,
                )
            })?),
        };

        assistant_message_count += 1;
        terminal_message_id = Some(message_id.to_string());
        terminal_finish = finish.map(|value| value.to_string());
    }

    let terminal_message_id = terminal_message_id.ok_or_else(|| {
        OpenCodeMetadataDiagnostic::staged(
            OpenCodeMetadataError::MissingModel,
            OpenCodeMetadataStage::TerminalCompletion,
        )
    })?;
    let observed_model = observed_model.ok_or_else(|| {
        OpenCodeMetadataDiagnostic::staged(
            OpenCodeMetadataError::MissingModel,
            OpenCodeMetadataStage::TerminalCompletion,
        )
    })?;
    if terminal_finish.as_deref() != Some("stop") {
        return Err(OpenCodeMetadataDiagnostic::staged(
            OpenCodeMetadataError::MissingCompletion,
            OpenCodeMetadataStage::TerminalCompletion,
        ));
    }

    let idle_ms = session_idle_ms(info)?;
    if idle_ms < info_created_ms || idle_ms < max_assistant_completed_ms {
        return Err(OpenCodeMetadataDiagnostic::staged(
            OpenCodeMetadataError::MissingCompletion,
            OpenCodeMetadataStage::TerminalCompletion,
        ));
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

fn parse_object(
    bytes: &[u8],
    stage: OpenCodeMetadataStage,
) -> Result<Map<String, Value>, OpenCodeMetadataDiagnostic> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|_| OpenCodeMetadataDiagnostic::invalid_metadata(stage))?;
    match value {
        Value::Object(map) => Ok(map),
        _ => Err(OpenCodeMetadataDiagnostic::invalid_metadata(stage)),
    }
}

fn validate_owned_session(
    owned: &OwnedOpenCodeSession,
) -> Result<&str, OpenCodeMetadataDiagnostic> {
    if !is_safe_identifier(&owned.session_id) || !owned.canonical_workdir.is_absolute() {
        return Err(OpenCodeMetadataDiagnostic::invalid_metadata(
            OpenCodeMetadataStage::CallerContract,
        ));
    }
    let workdir = owned.canonical_workdir.to_str().ok_or_else(|| {
        OpenCodeMetadataDiagnostic::invalid_metadata(OpenCodeMetadataStage::CallerContract)
    })?;
    if !is_clean_absolute_path(workdir) {
        return Err(OpenCodeMetadataDiagnostic::invalid_metadata(
            OpenCodeMetadataStage::CallerContract,
        ));
    }
    Ok(workdir)
}

fn validate_identity(
    info: &Map<String, Value>,
    export_info: &Map<String, Value>,
    owned: &OwnedOpenCodeSession,
) -> Result<(), OpenCodeMetadataDiagnostic> {
    let info_id = info.get("id").and_then(Value::as_str).ok_or_else(|| {
        OpenCodeMetadataDiagnostic::invalid_metadata(OpenCodeMetadataStage::IdentityWorkdir)
    })?;
    let export_id = export_info
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            OpenCodeMetadataDiagnostic::invalid_metadata(OpenCodeMetadataStage::IdentityWorkdir)
        })?;
    if info_id != owned.session_id || export_id != owned.session_id {
        return Err(OpenCodeMetadataDiagnostic::staged(
            OpenCodeMetadataError::OwnershipMismatch,
            OpenCodeMetadataStage::IdentityWorkdir,
        ));
    }
    Ok(())
}

fn validate_real_workdir(
    info: &Map<String, Value>,
    owned_workdir: &str,
) -> Result<(), OpenCodeMetadataDiagnostic> {
    let location = info
        .get("location")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            OpenCodeMetadataDiagnostic::invalid_metadata(OpenCodeMetadataStage::IdentityWorkdir)
        })?;
    let directory = location
        .get("directory")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            OpenCodeMetadataDiagnostic::invalid_metadata(OpenCodeMetadataStage::IdentityWorkdir)
        })?;
    if !is_clean_absolute_path(directory) || directory != owned_workdir {
        return Err(OpenCodeMetadataDiagnostic::staged(
            OpenCodeMetadataError::OwnershipMismatch,
            OpenCodeMetadataStage::IdentityWorkdir,
        ));
    }
    Ok(())
}

fn session_created_ms(info: &Map<String, Value>) -> Result<u64, OpenCodeMetadataDiagnostic> {
    let time = info.get("time").and_then(Value::as_object).ok_or_else(|| {
        OpenCodeMetadataDiagnostic::invalid_metadata(OpenCodeMetadataStage::Timestamps)
    })?;
    required_timestamp(
        time.get("created"),
        OpenCodeMetadataDiagnostic::invalid_metadata(OpenCodeMetadataStage::Timestamps),
    )
}

fn required_timestamp(
    value: Option<&Value>,
    missing_error: OpenCodeMetadataDiagnostic,
) -> Result<u64, OpenCodeMetadataDiagnostic> {
    match value {
        None => Err(missing_error),
        Some(value) if value.is_null() => Err(missing_error),
        Some(value) => value.as_u64().ok_or_else(|| {
            OpenCodeMetadataDiagnostic::invalid_metadata(OpenCodeMetadataStage::Timestamps)
        }),
    }
}

// The public v2 schema encodes outcome as a scalar enum. Both session copies must be the exact
// JSON string `succeeded`; tagged objects and every other shape fail completion validation.
fn session_outcome_succeeded(info: &Map<String, Value>) -> bool {
    info.get("outcome").and_then(Value::as_str) == Some("succeeded")
}

// The contract gives the real info an idle timestamp; `time.idle` is that authoritative field.
fn session_idle_ms(info: &Map<String, Value>) -> Result<u64, OpenCodeMetadataDiagnostic> {
    let time = info.get("time").and_then(Value::as_object).ok_or_else(|| {
        OpenCodeMetadataDiagnostic::staged(
            OpenCodeMetadataError::MissingCompletion,
            OpenCodeMetadataStage::TerminalCompletion,
        )
    })?;
    required_timestamp(
        time.get("idle"),
        OpenCodeMetadataDiagnostic::staged(
            OpenCodeMetadataError::MissingCompletion,
            OpenCodeMetadataStage::TerminalCompletion,
        ),
    )
}

fn validate_message_envelopes(messages: &[Value]) -> Result<(), OpenCodeMetadataDiagnostic> {
    let mut message_ids = BTreeSet::new();
    for message in messages {
        let message = message.as_object().ok_or_else(|| {
            OpenCodeMetadataDiagnostic::invalid_metadata(OpenCodeMetadataStage::MessageEnvelope)
        })?;
        message.get("type").and_then(Value::as_str).ok_or_else(|| {
            OpenCodeMetadataDiagnostic::invalid_metadata(OpenCodeMetadataStage::MessageEnvelope)
        })?;
        if let Some(id_value) = message.get("id") {
            let message_id = id_value.as_str().ok_or_else(|| {
                OpenCodeMetadataDiagnostic::invalid_metadata(OpenCodeMetadataStage::MessageEnvelope)
            })?;
            if !message_ids.insert(message_id.to_string()) {
                return Err(OpenCodeMetadataDiagnostic::invalid_metadata(
                    OpenCodeMetadataStage::MessageEnvelope,
                ));
            }
        }
    }
    Ok(())
}

fn split_expected_target(
    expected: &ResolvedModelTarget,
) -> Result<(&str, &str), OpenCodeMetadataDiagnostic> {
    if expected.base_model.len() > MAX_IDENTIFIER_LEN {
        return Err(OpenCodeMetadataDiagnostic::invalid_metadata(
            OpenCodeMetadataStage::CallerContract,
        ));
    }
    let (provider_id, model_id) = expected.base_model.split_once('/').ok_or_else(|| {
        OpenCodeMetadataDiagnostic::invalid_metadata(OpenCodeMetadataStage::CallerContract)
    })?;
    if !is_safe_identifier(provider_id) || !is_safe_model_identifier(model_id) {
        return Err(OpenCodeMetadataDiagnostic::invalid_metadata(
            OpenCodeMetadataStage::CallerContract,
        ));
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

    fn run_with_diagnostic(
        info: &Value,
        export: &Value,
    ) -> Result<OpenCodeMetadataEvidence, OpenCodeMetadataDiagnostic> {
        let info_bytes = serde_json::to_vec(info).expect("serialize info");
        let export_bytes = serde_json::to_vec(export).expect("serialize export");
        validate_opencode_metadata_with_diagnostic(
            &owned(),
            &info_bytes,
            &export_bytes,
            &expected(),
        )
    }

    #[test]
    fn diagnostic_stages_distinguish_malformed_categories_and_never_echo_sentinels() {
        let sentinel = "PRIVATE_DIAGNOSTIC_SENTINEL_TOKEN_PATH_/tmp/secret_https://example.invalid";
        let info_bytes = serde_json::to_vec(&info_document()).expect("info bytes");
        let export_bytes = serde_json::to_vec(&export_document()).expect("export bytes");

        // 1. Caller contract stage (unsafe session ID, relative workdir, malformed target/effort).
        let mut bad_owned = owned();
        bad_owned.session_id = format!("ses_{sentinel}");
        let diag = validate_opencode_metadata_with_diagnostic(
            &bad_owned,
            &info_bytes,
            &export_bytes,
            &expected(),
        )
        .unwrap_err();
        assert_eq!(diag.error, OpenCodeMetadataError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::CallerContract));
        assert_eq!(diag.as_str(), "invalid_metadata:caller_contract");
        assert!(!diag.to_string().contains("PRIVATE_"));
        assert!(!format!("{diag:?}").contains("PRIVATE_"));

        let mut bad_target = expected();
        bad_target.effort = Some(format!("max {sentinel}"));
        let diag = validate_opencode_metadata_with_diagnostic(
            &owned(),
            &info_bytes,
            &export_bytes,
            &bad_target,
        )
        .unwrap_err();
        assert_eq!(diag.error, OpenCodeMetadataError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::CallerContract));
        assert_eq!(diag.as_str(), "invalid_metadata:caller_contract");

        // 2. Info response JSON stage (syntax error, non-object root, deep nesting).
        for raw_info in [
            format!("not-json-{sentinel}").into_bytes(),
            format!("[\"{sentinel}\"]").into_bytes(),
        ] {
            let diag = validate_opencode_metadata_with_diagnostic(
                &owned(),
                &raw_info,
                &export_bytes,
                &expected(),
            )
            .unwrap_err();
            assert_eq!(diag.error, OpenCodeMetadataError::InvalidMetadata);
            assert_eq!(diag.stage, Some(OpenCodeMetadataStage::InfoResponseJson));
            assert_eq!(diag.as_str(), "invalid_metadata:info_response_json");
            assert!(!diag.to_string().contains("PRIVATE_"));
            assert!(!format!("{diag:?}").contains("PRIVATE_"));
        }

        // 3. Info response envelope stage (missing or non-object `data`).
        for bad_info_env in [json!({ "error": sentinel }), json!({ "data": [sentinel] })] {
            let diag = run_with_diagnostic(&bad_info_env, &export_document()).unwrap_err();
            assert_eq!(diag.error, OpenCodeMetadataError::InvalidMetadata);
            assert_eq!(
                diag.stage,
                Some(OpenCodeMetadataStage::InfoResponseEnvelope)
            );
            assert_eq!(diag.as_str(), "invalid_metadata:info_response_envelope");
            assert!(!diag.to_string().contains("PRIVATE_"));
            assert!(!format!("{diag:?}").contains("PRIVATE_"));
        }

        // 4. Export JSON stage (syntax error, non-object root).
        for raw_export in [
            format!("not-json-{sentinel}").into_bytes(),
            format!("[\"{sentinel}\"]").into_bytes(),
        ] {
            let diag = validate_opencode_metadata_with_diagnostic(
                &owned(),
                &info_bytes,
                &raw_export,
                &expected(),
            )
            .unwrap_err();
            assert_eq!(diag.error, OpenCodeMetadataError::InvalidMetadata);
            assert_eq!(diag.stage, Some(OpenCodeMetadataStage::ExportJson));
            assert_eq!(diag.as_str(), "invalid_metadata:export_json");
            assert!(!diag.to_string().contains("PRIVATE_"));
            assert!(!format!("{diag:?}").contains("PRIVATE_"));
        }

        // 5. Export envelope stage (missing/non-object `info` or missing/non-array `messages`).
        let mut no_export_info = export_document();
        remove_key(&mut no_export_info, "info");
        no_export_info["secret"] = json!(sentinel);
        let diag = run_with_diagnostic(&info_document(), &no_export_info).unwrap_err();
        assert_eq!(diag.error, OpenCodeMetadataError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::ExportEnvelope));
        assert_eq!(diag.as_str(), "invalid_metadata:export_envelope");

        let mut bad_messages_env = export_document();
        bad_messages_env["messages"] = json!({ "leak": sentinel });
        let diag = run_with_diagnostic(&info_document(), &bad_messages_env).unwrap_err();
        assert_eq!(diag.error, OpenCodeMetadataError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::ExportEnvelope));
        assert_eq!(diag.as_str(), "invalid_metadata:export_envelope");

        // 6. Identity / workdir stage (missing/non-string `id` or `location.directory`).
        let mut missing_info_id = info_document();
        missing_info_id["data"]["id"] = json!({ "leak": sentinel });
        let diag = run_with_diagnostic(&missing_info_id, &export_document()).unwrap_err();
        assert_eq!(diag.error, OpenCodeMetadataError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::IdentityWorkdir));
        assert_eq!(diag.as_str(), "invalid_metadata:identity_workdir");

        let mut missing_dir = info_document();
        missing_dir["data"]["location"]["directory"] = json!(42);
        let diag = run_with_diagnostic(&missing_dir, &export_document()).unwrap_err();
        assert_eq!(diag.error, OpenCodeMetadataError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::IdentityWorkdir));
        assert_eq!(diag.as_str(), "invalid_metadata:identity_workdir");

        // 7. Timestamps stage (missing/non-u64 created/idle/completed or mismatched created).
        let mut mismatched_created = export_document();
        mismatched_created["info"]["time"]["created"] = json!(2_001);
        let diag = run_with_diagnostic(&info_document(), &mismatched_created).unwrap_err();
        assert_eq!(diag.error, OpenCodeMetadataError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::Timestamps));
        assert_eq!(diag.as_str(), "invalid_metadata:timestamps");

        let mut bad_idle_type = info_document();
        bad_idle_type["data"]["time"]["idle"] = json!(sentinel);
        let diag = run_with_diagnostic(&bad_idle_type, &export_document()).unwrap_err();
        assert_eq!(diag.error, OpenCodeMetadataError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::Timestamps));
        assert_eq!(diag.as_str(), "invalid_metadata:timestamps");

        let mut bad_asst_completed_type = export_document();
        bad_asst_completed_type["messages"][0]["time"]["completed"] = json!(sentinel);
        let diag = run_with_diagnostic(&info_document(), &bad_asst_completed_type).unwrap_err();
        assert_eq!(diag.error, OpenCodeMetadataError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::Timestamps));
        assert_eq!(diag.as_str(), "invalid_metadata:timestamps");

        // 8. Message envelope stage (non-object entry, missing type, unsafe or duplicate ID).
        let mut dup_id = export_document();
        dup_id["messages"][1]["id"] = dup_id["messages"][0]["id"].clone();
        let diag = run_with_diagnostic(&info_document(), &dup_id).unwrap_err();
        assert_eq!(diag.error, OpenCodeMetadataError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::MessageEnvelope));
        assert_eq!(diag.as_str(), "invalid_metadata:message_envelope");

        let mut unsafe_asst_id = export_document();
        unsafe_asst_id["messages"][0]["id"] = json!(format!("msg/{sentinel}"));
        let diag = run_with_diagnostic(&info_document(), &unsafe_asst_id).unwrap_err();
        assert_eq!(diag.error, OpenCodeMetadataError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::MessageEnvelope));
        assert_eq!(diag.as_str(), "invalid_metadata:message_envelope");

        // 9. Assistant target stage (non-string variant or unsafe provider/model/variant identifier).
        let mut bad_variant_type = export_document();
        bad_variant_type["messages"][0]["model"]["variant"] = json!({ "secret": sentinel });
        let diag = run_with_diagnostic(&info_document(), &bad_variant_type).unwrap_err();
        assert_eq!(diag.error, OpenCodeMetadataError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::AssistantTarget));
        assert_eq!(diag.as_str(), "invalid_metadata:assistant_target");

        let mut unsafe_model_id = export_document();
        unsafe_model_id["messages"][0]["model"]["id"] = json!(format!("deepseek {sentinel}"));
        let diag = run_with_diagnostic(&info_document(), &unsafe_model_id).unwrap_err();
        assert_eq!(diag.error, OpenCodeMetadataError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::AssistantTarget));
        assert_eq!(diag.as_str(), "invalid_metadata:assistant_target");
        assert!(!diag.to_string().contains("PRIVATE_"));
        assert!(!format!("{diag:?}").contains("PRIVATE_"));

        // 10. Terminal completion stage (malformed non-string assistant `finish`).
        let mut bad_finish_type = export_document();
        bad_finish_type["messages"][1]["finish"] = json!({ "reason": sentinel });
        let diag = run_with_diagnostic(&info_document(), &bad_finish_type).unwrap_err();
        assert_eq!(diag.error, OpenCodeMetadataError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::TerminalCompletion));
        assert_eq!(diag.as_str(), "invalid_metadata:terminal_completion");
        assert!(!diag.to_string().contains("PRIVATE_"));
        assert!(!format!("{diag:?}").contains("PRIVATE_"));
    }

    #[test]
    fn legacy_invalid_metadata_and_semantic_codes_round_trip_without_inventing_stage() {
        let legacy = OpenCodeMetadataDiagnostic::from_error_code("invalid_metadata")
            .expect("legacy invalid_metadata must remain readable");
        assert_eq!(legacy.error, OpenCodeMetadataError::InvalidMetadata);
        assert_eq!(
            legacy.stage, None,
            "legacy invalid_metadata must not invent a stage cause"
        );
        assert_eq!(legacy.as_str(), "invalid_metadata");

        let all_stages = [
            (OpenCodeMetadataStage::CallerContract, "caller_contract"),
            (
                OpenCodeMetadataStage::CreateResponseJson,
                "create_response_json",
            ),
            (
                OpenCodeMetadataStage::CreateResponseEnvelope,
                "create_response_envelope",
            ),
            (
                OpenCodeMetadataStage::InfoResponseJson,
                "info_response_json",
            ),
            (
                OpenCodeMetadataStage::InfoResponseEnvelope,
                "info_response_envelope",
            ),
            (OpenCodeMetadataStage::ExportJson, "export_json"),
            (OpenCodeMetadataStage::ExportEnvelope, "export_envelope"),
            (OpenCodeMetadataStage::IdentityWorkdir, "identity_workdir"),
            (OpenCodeMetadataStage::Timestamps, "timestamps"),
            (OpenCodeMetadataStage::MessageEnvelope, "message_envelope"),
            (OpenCodeMetadataStage::AssistantTarget, "assistant_target"),
            (
                OpenCodeMetadataStage::TerminalCompletion,
                "terminal_completion",
            ),
        ];
        for (stage, stage_str) in all_stages {
            assert_eq!(stage.as_str(), stage_str);
            let expected_qualified = format!("invalid_metadata:{stage_str}");
            assert_eq!(stage.qualified_error_code(), expected_qualified);
            let diag = OpenCodeMetadataDiagnostic::invalid_metadata(stage);
            assert_eq!(diag.as_str(), expected_qualified);
            let parsed = OpenCodeMetadataDiagnostic::from_error_code(diag.as_str())
                .expect("parse qualified stage code");
            assert_eq!(parsed, diag);
        }

        // Semantic non-InvalidMetadata errors preserve their exact coarse wire code in `as_str()`.
        let mut wrong_owner = info_document();
        wrong_owner["data"]["id"] = json!("ses_other");
        let owner_diag = run_with_diagnostic(&wrong_owner, &export_document()).unwrap_err();
        assert_eq!(owner_diag.error, OpenCodeMetadataError::OwnershipMismatch);
        assert_eq!(
            owner_diag.stage,
            Some(OpenCodeMetadataStage::IdentityWorkdir)
        );
        assert_eq!(owner_diag.as_str(), "ownership_mismatch");

        let mut incomplete = export_document();
        incomplete["messages"][1]["finish"] = json!("tool-calls");
        let incomplete_diag = run_with_diagnostic(&info_document(), &incomplete).unwrap_err();
        assert_eq!(
            incomplete_diag.error,
            OpenCodeMetadataError::MissingCompletion
        );
        assert_eq!(
            incomplete_diag.stage,
            Some(OpenCodeMetadataStage::TerminalCompletion)
        );
        assert_eq!(incomplete_diag.as_str(), "missing_completion");

        // Arbitrary or untrusted strings are rejected by `from_error_code`.
        assert!(
            OpenCodeMetadataDiagnostic::from_error_code("invalid_metadata:unknown_stage").is_none()
        );
        assert!(OpenCodeMetadataDiagnostic::from_error_code("invalid_metadata:").is_none());
        assert!(OpenCodeMetadataDiagnostic::from_error_code("arbitrary_secret").is_none());
    }
}
