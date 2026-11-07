//! Complete, byte-bounded structured advisory response capture for read-only [`AdvisoryWorkerSpec`](crate::AdvisoryWorkerSpec) runs.
//!
//! [`WorkerExecutionReport::evidence`](crate::WorkerExecutionReport::evidence) intentionally truncates
//! and secret-redacts `retained_events[].summary` at [`MAX_DIAGNOSTIC_SUMMARY_BYTES`](crate::stream::MAX_DIAGNOSTIC_SUMMARY_BYTES)
//! (`256` bytes) for safe status/UI diagnostics. That diagnostic summary stream cannot carry a complete
//! plan DAG or structured review payload and must never be parsed as a model response.
//!
//! This module provides an opt-in, byte-bounded terminal response collector that runs alongside
//! [`NdjsonStreamParser`](crate::NdjsonStreamParser) inside the supervised read-only execution path
//! ([`run_advisory_worker_with_structured_result`](crate::run_advisory_worker_with_structured_result)):
//!
//! - **Antigravity (`antigravity`)**: Captures only from a terminal `event: "result"` object whose
//!   `result.status` is `"SUCCESS"` and whose `result.response` is a non-empty UTF-8 string containing
//!   a valid top-level JSON object. Intermediate `step_update` messages, tool calls/results, and
//!   `ERROR`/`FAILURE` results (even when the process exits `0`) are never accepted.
//! - **OpenCode v2 (`opencode_v2`)**: Captures only from a complete assistant `type: "text"`
//!   (`part.type == "text"`) event inside an active `step_start`..`step_finish` frame that terminates
//!   with an explicit `step_finish` (`part.type == "step-finish"`, `reason == "stop"`). Partial/delta
//!   text snapshots, unfinished `part.time` records, duplicate or conflicting text parts in a step,
//!   tool-result spoofing, and unsupported event shapes are explicitly rejected.
//! - **Codex CLI (`codex_cli`)**: Captures only from the single final completed
//!   `type: "item.completed"` event whose `item.type == "agent_message"` contains a non-empty
//!   UTF-8 top-level JSON object `item.text` inside a `thread.started` -> `turn.started` ->
//!   `turn.completed` lifecycle. Bounded ordinary prose preamble `agent_message` items
//!   (up to [`MAX_CODEX_PROSE_PREAMBLES_PER_TURN`]) may precede the final structured JSON object,
//!   while terminal `turn.failed` and `error` events, missing `thread.started` or `turn.completed`
//!   framing, conflicting `thread_id` values, duplicate `item.id` values, prior malformed
//!   JSON-like or non-object JSON candidates, multiple/conflicting structured JSON candidates,
//!   trailing `agent_message` items after a structured JSON candidate, prose-only turns, and
//!   tool/command spoofing are rejected. Because `codex exec --json` does not emit runtime model
//!   or reasoning-effort telemetry on stdout, observed model/effort fields remain `None` unless
//!   genuinely emitted by the CLI while completion relies on the verified pinned read-only launch
//!   configuration (`--model` + `-c model_reasoning_effort`).
//! - **Untrusted model data**: A verified [`BoundedAdvisoryResponse`] proves only that the supervised
//!   provider process exited cleanly with matching CLI-reported or launch-pinned evidence
//!   and emitted a complete UTF-8 JSON object within [`AdvisoryResponseBudget`]. Parsing a valid JSON
//!   object grants no execution authority, approves no scope, and executes no tools.

use std::collections::BTreeSet;
use std::fmt;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::antigravity_models::antigravity_catalog_effort;
use crate::error::WorkerError;
use crate::fs_safety::sanitize_and_redact;
use crate::spec::{
    ResolvedModelTarget, WorkerProvider, DEFAULT_MAX_LINE_BYTES, MAX_IDENTIFIER_LEN, MAX_LINE_BYTES,
};
use crate::stream::{
    extract_genuine_turn_usage, has_object_error_field, trim_ascii_whitespace,
    EffortEvidenceSource, StreamTokenUsage, MAX_DIAGNOSTIC_SUMMARY_BYTES,
};
use crate::supervisor::{WorkerExecutionReport, WorkerRunStatus};

/// Default maximum UTF-8 byte length for a captured structured advisory response (`256 KiB`).
pub const DEFAULT_MAX_ADVISORY_RESPONSE_BYTES: usize = DEFAULT_MAX_LINE_BYTES;

/// Hard maximum configurable UTF-8 byte length for a captured structured advisory response (`4 MiB`).
pub const MAX_ADVISORY_RESPONSE_BYTES: usize = MAX_LINE_BYTES;

/// Maximum number of intermediate ordinary prose `agent_message` preambles permitted before the
/// single final structured JSON-object `agent_message` in a Codex CLI advisory turn (`32`).
pub const MAX_CODEX_PROSE_PREAMBLES_PER_TURN: usize = 32;

const fn default_max_advisory_response_bytes() -> usize {
    DEFAULT_MAX_ADVISORY_RESPONSE_BYTES
}

/// Explicit per-run byte budget for capturing a complete structured advisory response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdvisoryResponseBudget {
    /// Maximum UTF-8 bytes permitted for the complete captured advisory response
    /// (`1..=MAX_ADVISORY_RESPONSE_BYTES`).
    #[serde(default = "default_max_advisory_response_bytes")]
    pub max_response_bytes: usize,
}

impl Default for AdvisoryResponseBudget {
    fn default() -> Self {
        Self {
            max_response_bytes: DEFAULT_MAX_ADVISORY_RESPONSE_BYTES,
        }
    }
}

impl AdvisoryResponseBudget {
    /// Creates an [`AdvisoryResponseBudget`] with the given `max_response_bytes` limit.
    #[must_use]
    pub const fn new(max_response_bytes: usize) -> Self {
        Self { max_response_bytes }
    }

    /// Validates that `max_response_bytes` is non-zero and within [`MAX_ADVISORY_RESPONSE_BYTES`].
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.max_response_bytes == 0 || self.max_response_bytes > MAX_ADVISORY_RESPONSE_BYTES {
            return Err(WorkerError::InvalidSpec(format!(
                "advisory max_response_bytes must be between 1 and {MAX_ADVISORY_RESPONSE_BYTES} (got {})",
                self.max_response_bytes
            )));
        }
        Ok(())
    }
}

/// Truthful provider-specific stream source from which a [`BoundedAdvisoryResponse`] was captured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdvisoryResponseSource {
    /// Captured from Antigravity's final successful `event: "result"` (`result.response`) field.
    AntigravityFinalResultResponse,
    /// Captured from OpenCode v2's complete assistant `type: "text"` (`part.type == "text"`) event
    /// sealed by a terminal `type: "step_finish"` (`part.type == "step-finish"`, `reason == "stop"`).
    OpenCodeAssistantTextPartStop,
    /// Captured from Codex CLI's completed `type: "item.completed"` (`item.type == "agent_message"`)
    /// event sealed by a terminal `type: "turn.completed"` event.
    CodexAgentMessageTurnCompleted,
}

impl AdvisoryResponseSource {
    /// Returns the canonical `snake_case` identifier for this response source.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AntigravityFinalResultResponse => "antigravity_final_result_response",
            Self::OpenCodeAssistantTextPartStop => "opencode_assistant_text_part_stop",
            Self::CodexAgentMessageTurnCompleted => "codex_agent_message_turn_completed",
        }
    }
}

/// Truthful framing and binding provenance for a captured [`BoundedAdvisoryResponse`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdvisoryResponseFraming {
    /// 1-based stdout NDJSON line number where the response payload was emitted.
    pub response_line_number: usize,
    /// 1-based stdout NDJSON line number of the terminal completion event that sealed the response.
    pub terminal_line_number: usize,
    /// Verified session/conversation/thread ID bound to the response framing, when observed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Verified assistant message ID bound to the response framing, when observed (OpenCode v2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    /// Verified assistant text part ID bound to the response framing, when observed (OpenCode v2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub part_id: Option<String>,
    /// Verified completed item ID bound to the response framing, when observed (Codex CLI `item.id`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<String>,
    /// Provenance of observed stream effort evidence (`direct_stream_metadata` or
    /// `antigravity_catalog_model_variant`), when effort was verified from the stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort_evidence_source: Option<EffortEvidenceSource>,
    /// Genuine token usage telemetry observed on the terminal completion event, when emitted by the CLI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<StreamTokenUsage>,
}

/// Verified, byte-bounded complete UTF-8 structured advisory response from a read-only worker run.
///
/// The `text` payload is guaranteed to be valid UTF-8 within the configured
/// [`AdvisoryResponseBudget`] and to parse as a top-level JSON object (`serde_json::Value::Object`),
/// while remaining **untrusted model data** that grants no authority and executes no tools.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundedAdvisoryResponse {
    /// Exact full UTF-8 response text emitted by the provider's terminal framing.
    pub text: String,
    /// Exact UTF-8 byte count of `text` (`text.len()`), guaranteed within `1..=max_response_bytes`.
    pub byte_count: usize,
    /// Lowercase 64-character hexadecimal SHA-256 digest of `text.as_bytes()`.
    pub sha256_digest: String,
    /// Worker provider that produced the response.
    pub provider: WorkerProvider,
    /// Truthful provider-specific stream source classification.
    pub source: AdvisoryResponseSource,
    /// Line numbers, session/message/part bindings, and effort provenance for the captured response.
    pub framing: AdvisoryResponseFraming,
}

impl BoundedAdvisoryResponse {
    /// Returns the exact UTF-8 response text slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// Returns the exact UTF-8 byte length of the response text.
    #[must_use]
    pub const fn byte_len(&self) -> usize {
        self.byte_count
    }

    /// Returns the 64-character lowercase hexadecimal SHA-256 digest of `self.text.as_bytes()`.
    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.sha256_digest
    }

    /// Deserializes the untrusted JSON response text into `T`.
    ///
    /// Parsing a valid JSON value grants no execution authority and triggers no side effects.
    /// Callers loading persisted or cached responses must call [`Self::validate_integrity`]
    /// with their frozen per-call `max_response_bytes` budget to verify framing and digest
    /// integrity before trusting response metadata.
    pub fn parse_json<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_str(&self.text)
    }

    /// Parses the untrusted response text as a top-level JSON object map.
    ///
    /// Parsing a valid JSON object grants no execution authority and triggers no side effects.
    pub fn parse_json_object(&self) -> Result<Map<String, Value>, serde_json::Error> {
        serde_json::from_str(&self.text)
    }

    /// Validates that this response satisfies all byte-budget, SHA-256 digest, complete top-level
    /// JSON-object, and provider/source/framing coherence invariants under `max_response_bytes`.
    ///
    /// Callers reloading persisted or cached private advisory artifacts must pass their exact
    /// frozen per-call/grant `max_response_bytes` limit here. Even when `Ok(())` is returned,
    /// `self.text` remains untrusted model data and grants no execution or tool authority.
    pub fn validate_integrity(&self, max_response_bytes: usize) -> Result<(), WorkerError> {
        AdvisoryResponseBudget::new(max_response_bytes).validate()?;

        let actual_bytes = self.text.len();
        if actual_bytes == 0 || self.text.trim().is_empty() {
            return Err(WorkerError::InvalidSpec(
                "advisory response text must not be empty".to_string(),
            ));
        }
        if self.byte_count != actual_bytes {
            return Err(WorkerError::InvalidSpec(format!(
                "advisory response byte_count ({}) does not match actual UTF-8 byte length ({actual_bytes})",
                self.byte_count
            )));
        }
        if actual_bytes > max_response_bytes {
            return Err(WorkerError::InvalidSpec(format!(
                "advisory response byte length ({actual_bytes}) exceeds max_response_bytes ({max_response_bytes})"
            )));
        }

        if self.sha256_digest.len() != 64
            || !self
                .sha256_digest
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(WorkerError::InvalidSpec(
                "advisory response sha256_digest must be a 64-character lowercase hex SHA-256 digest"
                    .to_string(),
            ));
        }
        let computed_sha256 = compute_sha256_hex(self.text.as_bytes());
        if self.sha256_digest != computed_sha256 {
            return Err(WorkerError::InvalidSpec(format!(
                "advisory response sha256_digest ({}) does not match computed SHA-256 ({computed_sha256})",
                self.sha256_digest
            )));
        }

        validate_structured_json_object(&self.text, self.framing.response_line_number).map_err(
            |err| WorkerError::InvalidSpec(format!("advisory response invalid JSON object: {err}")),
        )?;

        if self.framing.response_line_number == 0
            || self.framing.terminal_line_number == 0
            || self.framing.terminal_line_number < self.framing.response_line_number
        {
            return Err(WorkerError::InvalidSpec(format!(
                "advisory response framing line numbers are invalid (response_line_number={}, terminal_line_number={})",
                self.framing.response_line_number, self.framing.terminal_line_number
            )));
        }

        for (field_name, id_opt) in [
            ("session_id", self.framing.session_id.as_deref()),
            ("message_id", self.framing.message_id.as_deref()),
            ("part_id", self.framing.part_id.as_deref()),
            ("item_id", self.framing.item_id.as_deref()),
        ] {
            if let Some(id_val) = id_opt {
                if !is_safe_stream_id(id_val) {
                    return Err(WorkerError::InvalidSpec(format!(
                        "advisory response framing `{field_name}` is malformed or exceeds bounds"
                    )));
                }
            }
        }

        match self.provider {
            WorkerProvider::Antigravity => {
                if self.source != AdvisoryResponseSource::AntigravityFinalResultResponse {
                    return Err(WorkerError::InvalidSpec(format!(
                        "Antigravity advisory response has incompatible source `{}`",
                        self.source.as_str()
                    )));
                }
                if self.framing.response_line_number < 2
                    || self.framing.response_line_number != self.framing.terminal_line_number
                {
                    return Err(WorkerError::InvalidSpec(
                        "Antigravity advisory response framing requires response_line_number == terminal_line_number >= 2"
                            .to_string(),
                    ));
                }
                if self.framing.message_id.is_some()
                    || self.framing.part_id.is_some()
                    || self.framing.item_id.is_some()
                {
                    return Err(WorkerError::InvalidSpec(
                        "Antigravity advisory response framing must not carry OpenCode message_id/part_id or Codex item_id"
                            .to_string(),
                    ));
                }
            }
            WorkerProvider::OpenCodeV2 => {
                if self.source != AdvisoryResponseSource::OpenCodeAssistantTextPartStop {
                    return Err(WorkerError::InvalidSpec(format!(
                        "OpenCode v2 advisory response has incompatible source `{}`",
                        self.source.as_str()
                    )));
                }
                if self.framing.response_line_number < 2
                    || self.framing.terminal_line_number <= self.framing.response_line_number
                {
                    return Err(WorkerError::InvalidSpec(
                        "OpenCode v2 advisory response framing requires 2 <= response_line_number < terminal_line_number"
                            .to_string(),
                    ));
                }
                if self.framing.item_id.is_some() {
                    return Err(WorkerError::InvalidSpec(
                        "OpenCode v2 advisory response framing must not carry Codex item_id"
                            .to_string(),
                    ));
                }
                if self.framing.effort_evidence_source
                    == Some(EffortEvidenceSource::AntigravityCatalogModelVariant)
                {
                    return Err(WorkerError::InvalidSpec(
                        "OpenCode v2 advisory response framing cannot use AntigravityCatalogModelVariant effort source"
                            .to_string(),
                    ));
                }
            }
            WorkerProvider::CodexCli => {
                if self.source != AdvisoryResponseSource::CodexAgentMessageTurnCompleted {
                    return Err(WorkerError::InvalidSpec(format!(
                        "Codex CLI advisory response has incompatible source `{}`",
                        self.source.as_str()
                    )));
                }
                if self.framing.response_line_number < 2
                    || self.framing.terminal_line_number <= self.framing.response_line_number
                {
                    return Err(WorkerError::InvalidSpec(
                        "Codex CLI advisory response framing requires 2 <= response_line_number < terminal_line_number"
                            .to_string(),
                    ));
                }
                if self.framing.session_id.is_none() {
                    return Err(WorkerError::InvalidSpec(
                        "Codex CLI advisory response framing requires a verified thread_id in session_id"
                            .to_string(),
                    ));
                }
                if self.framing.item_id.is_none() {
                    return Err(WorkerError::InvalidSpec(
                        "Codex CLI advisory response framing requires a verified item_id"
                            .to_string(),
                    ));
                }
                if self.framing.message_id.is_some() || self.framing.part_id.is_some() {
                    return Err(WorkerError::InvalidSpec(
                        "Codex CLI advisory response framing must not carry OpenCode message_id or part_id"
                            .to_string(),
                    ));
                }
                if self.framing.effort_evidence_source
                    == Some(EffortEvidenceSource::AntigravityCatalogModelVariant)
                {
                    return Err(WorkerError::InvalidSpec(
                        "Codex CLI advisory response framing cannot use AntigravityCatalogModelVariant effort source"
                            .to_string(),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Validates response integrity against an explicit [`AdvisoryResponseBudget`].
    pub fn validate_integrity_with_budget(
        &self,
        budget: AdvisoryResponseBudget,
    ) -> Result<(), WorkerError> {
        self.validate_integrity(budget.max_response_bytes)
    }

    /// Returns `true` if [`Self::validate_integrity`] succeeds for `max_response_bytes`.
    #[must_use]
    pub fn is_valid(&self, max_response_bytes: usize) -> bool {
        self.validate_integrity(max_response_bytes).is_ok()
    }
}

/// Machine-readable classification for why a structured advisory response was rejected or unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdvisoryResponseErrorCode {
    /// Underlying worker process execution did not succeed (e.g. non-zero exit, timeout,
    /// cancellation, permission/input block, or incomplete process-group cleanup).
    WorkerExecutionFailed,
    /// Provider NDJSON stream or terminal result reported an error (including `ERROR` with exit 0).
    ProviderError,
    /// Stdout line, stdout total, or stderr total exceeded `ExecutionBudgets`.
    StreamOutputOverflow,
    /// Stdout contained invalid UTF-8, non-JSON, or non-object NDJSON lines.
    MalformedStream,
    /// Requested model evidence was missing or conflicted/mismatched in the stream or metadata.
    ModelEvidenceFailed,
    /// Requested effort/variant evidence was missing or conflicted/mismatched in the stream or metadata.
    EffortEvidenceFailed,
    /// Session identity evidence was missing or conflicted across stream events or owned metadata.
    SessionEvidenceFailed,
    /// Stream exited without the required provider-specific terminal completion framing.
    MissingTerminalFraming,
    /// Terminal completion framing was present, but no non-empty assistant response text was emitted.
    MissingResponseOutput,
    /// Stream emitted an unsupported, incomplete, or unrecognized event shape for the provider.
    UnsupportedFraming,
    /// Stream emitted partial/delta text, an unfinished text part (`time.end` missing), or a
    /// truncated stop reason (`length` / `max_tokens`).
    IncompleteOrPartialOutput,
    /// A tool call/result or non-assistant event attempted to supply or masquerade as the response.
    SpoofedOrToolOutput,
    /// Multiple or contradictory terminal payloads, duplicate text snapshots, conflicting
    /// message/part bindings, or trailing events after terminal completion were observed.
    ConflictingTerminalOutput,
    /// Captured terminal response text exceeded `AdvisoryResponseBudget::max_response_bytes`.
    ResponseTooLarge,
    /// Captured terminal response text was not a valid, complete top-level UTF-8 JSON object.
    InvalidStructuredJson,
}

impl AdvisoryResponseErrorCode {
    /// Returns the canonical `snake_case` error code string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WorkerExecutionFailed => "worker_execution_failed",
            Self::ProviderError => "provider_error",
            Self::StreamOutputOverflow => "stream_output_overflow",
            Self::MalformedStream => "malformed_stream",
            Self::ModelEvidenceFailed => "model_evidence_failed",
            Self::EffortEvidenceFailed => "effort_evidence_failed",
            Self::SessionEvidenceFailed => "session_evidence_failed",
            Self::MissingTerminalFraming => "missing_terminal_framing",
            Self::MissingResponseOutput => "missing_response_output",
            Self::UnsupportedFraming => "unsupported_framing",
            Self::IncompleteOrPartialOutput => "incomplete_or_partial_output",
            Self::SpoofedOrToolOutput => "spoofed_or_tool_output",
            Self::ConflictingTerminalOutput => "conflicting_terminal_output",
            Self::ResponseTooLarge => "response_too_large",
            Self::InvalidStructuredJson => "invalid_structured_json",
        }
    }
}

impl fmt::Display for AdvisoryResponseErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Typed, bounded diagnostic explaining why a structured advisory response was not accepted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdvisoryResponseError {
    /// Machine-readable failure category.
    pub code: AdvisoryResponseErrorCode,
    /// Bounded, secret-redacted diagnostic message (`<= 256` bytes).
    pub message: String,
    /// 1-based stdout NDJSON line number associated with the failure, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_number: Option<usize>,
    /// Observed response byte length when `code == ResponseTooLarge`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_bytes: Option<usize>,
    /// Configured `max_response_bytes` limit when `code == ResponseTooLarge`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_response_bytes: Option<usize>,
}

impl AdvisoryResponseError {
    pub(crate) fn new(code: AdvisoryResponseErrorCode, message: impl AsRef<str>) -> Self {
        Self {
            code,
            message: sanitize_and_redact(message.as_ref(), MAX_DIAGNOSTIC_SUMMARY_BYTES),
            line_number: None,
            observed_bytes: None,
            max_response_bytes: None,
        }
    }

    pub(crate) fn at_line(
        code: AdvisoryResponseErrorCode,
        line_number: usize,
        message: impl AsRef<str>,
    ) -> Self {
        Self {
            code,
            message: sanitize_and_redact(message.as_ref(), MAX_DIAGNOSTIC_SUMMARY_BYTES),
            line_number: Some(line_number),
            observed_bytes: None,
            max_response_bytes: None,
        }
    }
}

impl fmt::Display for AdvisoryResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for AdvisoryResponseError {}

/// Combined outcome of a supervised read-only advisory worker invocation with opt-in
/// complete structured response capture.
///
/// The underlying [`WorkerExecutionReport`] is always preserved in `execution` even when the
/// provider process completed (`WorkerRunStatus::Succeeded`) but returned missing, oversized,
/// truncated, non-JSON, spoofed, or conflicting output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdvisoryStructuredExecution {
    /// Complete bounded execution report from the supervised read-only worker process.
    pub execution: WorkerExecutionReport,
    /// Verified complete structured advisory response, present only when `execution` succeeded
    /// with complete evidence and the terminal output satisfied all framing, binding, budget,
    /// and JSON-object requirements.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<BoundedAdvisoryResponse>,
    /// Typed bounded diagnostic explaining why `response` is `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_error: Option<AdvisoryResponseError>,
}

impl AdvisoryStructuredExecution {
    /// Validates that `execution` succeeded with complete natural completion evidence,
    /// `response_error` is `None`, and `response` passes [`BoundedAdvisoryResponse::validate_integrity`]
    /// under `max_response_bytes` while remaining coherent with `execution` (provider, line bounds,
    /// effort evidence source, session binding, and OpenCode metadata).
    ///
    /// This strict entry point requires current execution evidence. Historical persisted reports
    /// use [`Self::validate_persisted_verified_response`] with their exact frozen byte limit.
    pub fn validate_verified_response(
        &self,
        max_response_bytes: usize,
    ) -> Result<&BoundedAdvisoryResponse, WorkerError> {
        self.validate_verified_response_internal(max_response_bytes, false)
    }

    /// Validates a persisted or cached advisory artifact under its exact frozen response budget.
    ///
    /// Applies the same response integrity, framing, session, usage and execution-coherence
    /// checks as [`Self::validate_verified_response`], with a narrow compatibility path for
    /// historical read-only Codex reports lacking both current execution markers. The caller
    /// must verify the persisted artifact's own provenance and integrity. This method does not
    /// authorize fresh collection or execution, and it does not synthesize missing evidence.
    pub fn validate_persisted_verified_response(
        &self,
        max_response_bytes: usize,
    ) -> Result<&BoundedAdvisoryResponse, WorkerError> {
        self.validate_verified_response_internal(max_response_bytes, true)
    }

    fn validate_verified_response_internal(
        &self,
        max_response_bytes: usize,
        allow_legacy_codex_advisory: bool,
    ) -> Result<&BoundedAdvisoryResponse, WorkerError> {
        let complete = self.execution.has_complete_natural_completion_evidence()
            || (allow_legacy_codex_advisory
                && self.execution.has_complete_legacy_codex_advisory_evidence());
        if self.execution.status != WorkerRunStatus::Succeeded || !complete {
            return Err(WorkerError::InvalidSpec(
                "advisory execution does not have complete natural completion evidence".to_string(),
            ));
        }
        if self.response_error.is_some() {
            return Err(WorkerError::InvalidSpec(
                "advisory execution carries a response_error alongside response".to_string(),
            ));
        }
        let Some(ref response) = self.response else {
            return Err(WorkerError::InvalidSpec(
                "advisory execution is missing a captured response".to_string(),
            ));
        };

        response.validate_integrity(max_response_bytes)?;

        if response.provider != self.execution.provider {
            return Err(WorkerError::InvalidSpec(format!(
                "advisory response provider ({}) does not match execution.provider ({})",
                response.provider.as_str(),
                self.execution.provider.as_str()
            )));
        }
        if response.framing.terminal_line_number > self.execution.evidence.total_lines {
            return Err(WorkerError::InvalidSpec(format!(
                "advisory response terminal_line_number ({}) exceeds execution total_lines ({})",
                response.framing.terminal_line_number, self.execution.evidence.total_lines
            )));
        }
        if response.framing.effort_evidence_source != self.execution.evidence.effort_evidence_source
        {
            return Err(WorkerError::InvalidSpec(
                "advisory response effort_evidence_source does not match execution evidence"
                    .to_string(),
            ));
        }

        let expected_sid = self
            .execution
            .allocated_session_id
            .as_deref()
            .or(self.execution.requested_session_id.as_deref())
            .or(self.execution.evidence.observed_session_id.as_deref());
        if let Some(exp_sid) = expected_sid {
            if response.framing.session_id.as_deref() != Some(exp_sid) {
                return Err(WorkerError::InvalidSpec(format!(
                    "advisory response framing session_id ({:?}) does not match execution session ({exp_sid})",
                    response.framing.session_id
                )));
            }
        }

        if self.execution.provider == WorkerProvider::OpenCodeV2 {
            if self.execution.requested_session_id.is_none()
                && self.execution.validated_opencode_metadata().is_none()
            {
                return Err(WorkerError::InvalidSpec(
                    "owned OpenCode v2 advisory execution lacks validated session metadata"
                        .to_string(),
                ));
            }
            if let Some(meta) = self.execution.validated_opencode_metadata() {
                if let Some(ref msg_id) = response.framing.message_id {
                    if msg_id != &meta.terminal_message_id {
                        return Err(WorkerError::InvalidSpec(format!(
                            "advisory response framing message_id (`{msg_id}`) conflicts with OpenCode terminal_message_id (`{}`)",
                            meta.terminal_message_id
                        )));
                    }
                }
            }
        }

        if self.execution.provider == WorkerProvider::CodexCli {
            if !self.execution.has_pinned_codex_read_only_launch_config() {
                return Err(WorkerError::InvalidSpec(
                    "Codex CLI advisory execution lacks verified pinned read-only launch configuration"
                        .to_string(),
                ));
            }
            if self.execution.evidence.observed_session_id.is_none() {
                return Err(WorkerError::InvalidSpec(
                    "Codex CLI advisory execution lacks an observed thread_id in stream evidence"
                        .to_string(),
                ));
            }
            if response.framing.usage != self.execution.evidence.observed_usage {
                return Err(WorkerError::InvalidSpec(
                    "Codex CLI advisory response framing usage does not match execution observed_usage"
                        .to_string(),
                ));
            }
        }

        Ok(response)
    }

    /// Returns `true` only when the worker execution succeeded with complete evidence and
    /// produced a [`BoundedAdvisoryResponse`] that passes full integrity and execution-coherence
    /// validation within `max_response_bytes`.
    #[must_use]
    pub fn has_verified_response_within_budget(&self, max_response_bytes: usize) -> bool {
        self.validate_verified_response(max_response_bytes).is_ok()
    }

    /// Returns a reference to the verified [`BoundedAdvisoryResponse`] if and only if
    /// [`Self::validate_verified_response`] succeeds for `max_response_bytes`.
    #[must_use]
    pub fn verified_response_within_budget(
        &self,
        max_response_bytes: usize,
    ) -> Option<&BoundedAdvisoryResponse> {
        self.validate_verified_response(max_response_bytes).ok()
    }

    /// Returns `true` only when the worker execution succeeded with complete evidence and
    /// produced a [`BoundedAdvisoryResponse`] that passes full integrity and execution-coherence
    /// validation under [`MAX_ADVISORY_RESPONSE_BYTES`].
    ///
    /// Note: When reloading persisted or cached private advisory artifacts, callers must also
    /// reapply their exact frozen per-call/grant byte limit via
    /// [`Self::validate_persisted_verified_response`].
    #[must_use]
    pub fn has_verified_response(&self) -> bool {
        self.validate_verified_response(MAX_ADVISORY_RESPONSE_BYTES)
            .is_ok()
    }

    /// Returns a reference to the verified [`BoundedAdvisoryResponse`] if and only if
    /// [`Self::has_verified_response`] is `true`.
    ///
    /// Note: When reloading persisted or cached private advisory artifacts, callers must also
    /// reapply their exact frozen per-call/grant byte limit via
    /// [`Self::validate_persisted_verified_response`].
    #[must_use]
    pub fn verified_response(&self) -> Option<&BoundedAdvisoryResponse> {
        self.validate_verified_response(MAX_ADVISORY_RESPONSE_BYTES)
            .ok()
    }
}

#[derive(Debug, Clone)]
struct CapturedCandidate {
    text: String,
    response_line_number: usize,
    session_id: Option<String>,
    message_id: Option<String>,
    part_id: Option<String>,
    item_id: Option<String>,
}

/// Incremental stdout collector that captures complete terminal advisory output alongside
/// [`NdjsonStreamParser`](crate::NdjsonStreamParser).
pub(crate) struct AdvisoryResponseCollector {
    provider: WorkerProvider,
    requested_model: ResolvedModelTarget,
    expected_session_id: Option<String>,
    budget: AdvisoryResponseBudget,
    total_lines: usize,
    latched_error: Option<AdvisoryResponseError>,
    observed_session_id: Option<String>,
    observed_model: Option<String>,
    observed_effort: Option<String>,
    saw_tool_output_candidate: bool,
    agy_saw_init: bool,
    agy_candidate: Option<CapturedCandidate>,
    oc_in_step: bool,
    oc_step_message_id: Option<String>,
    oc_step_candidate: Option<CapturedCandidate>,
    oc_sealed_candidate: Option<(CapturedCandidate, usize)>,
    codex_saw_thread_started: bool,
    codex_in_turn: bool,
    codex_seen_agent_item_ids: BTreeSet<String>,
    codex_prose_preamble_count: usize,
    codex_segment_prose_line: Option<usize>,
    codex_turn_candidate: Option<CapturedCandidate>,
    codex_sealed_candidate: Option<(CapturedCandidate, usize, Option<StreamTokenUsage>)>,
}

impl AdvisoryResponseCollector {
    pub(crate) fn new(
        provider: WorkerProvider,
        requested_model: ResolvedModelTarget,
        expected_session_id: Option<String>,
        budget: AdvisoryResponseBudget,
    ) -> Self {
        Self {
            provider,
            requested_model,
            expected_session_id,
            budget,
            total_lines: 0,
            latched_error: None,
            observed_session_id: None,
            observed_model: None,
            observed_effort: None,
            saw_tool_output_candidate: false,
            agy_saw_init: false,
            agy_candidate: None,
            oc_in_step: false,
            oc_step_message_id: None,
            oc_step_candidate: None,
            oc_sealed_candidate: None,
            codex_saw_thread_started: false,
            codex_in_turn: false,
            codex_seen_agent_item_ids: BTreeSet::new(),
            codex_prose_preamble_count: 0,
            codex_segment_prose_line: None,
            codex_turn_candidate: None,
            codex_sealed_candidate: None,
        }
    }

    fn latch_error(&mut self, err: AdvisoryResponseError) {
        if self.latched_error.is_none() {
            self.latched_error = Some(err);
        }
    }

    pub(crate) fn ingest_line(&mut self, raw_line: &[u8]) {
        let trimmed = trim_ascii_whitespace(raw_line);
        if trimmed.is_empty() {
            return;
        }
        self.total_lines += 1;
        let line_number = self.total_lines;

        let Ok(line_str) = std::str::from_utf8(trimmed) else {
            self.latch_error(AdvisoryResponseError::at_line(
                AdvisoryResponseErrorCode::MalformedStream,
                line_number,
                "stdout NDJSON line contained invalid UTF-8 bytes",
            ));
            return;
        };

        let Ok(value) = serde_json::from_str::<Value>(line_str) else {
            self.latch_error(AdvisoryResponseError::at_line(
                AdvisoryResponseErrorCode::MalformedStream,
                line_number,
                "stdout NDJSON line is not valid JSON",
            ));
            return;
        };

        let Some(obj) = value.as_object() else {
            self.latch_error(AdvisoryResponseError::at_line(
                AdvisoryResponseErrorCode::MalformedStream,
                line_number,
                "stdout NDJSON line must be a JSON object",
            ));
            return;
        };

        if let Err(err) = self.verify_and_record_session_id(line_number, obj) {
            self.latch_error(err);
            return;
        }

        match self.provider {
            WorkerProvider::Antigravity => self.ingest_antigravity_event(line_number, obj),
            WorkerProvider::OpenCodeV2 => self.ingest_opencode_event(line_number, obj),
            WorkerProvider::CodexCli => self.ingest_codex_event(line_number, obj),
        }
    }

    fn verify_and_record_session_id(
        &mut self,
        line_number: usize,
        obj: &Map<String, Value>,
    ) -> Result<(), AdvisoryResponseError> {
        let mut found = BTreeSet::new();
        let scopes = [
            Some(obj),
            obj.get("init").and_then(Value::as_object),
            obj.get("step_update").and_then(Value::as_object),
            obj.get("result").and_then(Value::as_object),
            obj.get("part").and_then(Value::as_object),
            obj.get("item").and_then(Value::as_object),
            obj.get("metadata").and_then(Value::as_object),
        ];
        for scope in scopes.into_iter().flatten() {
            for key in ["conversation_id", "session_id", "sessionID", "thread_id"] {
                if let Some(val) = scope.get(key) {
                    let Some(s) = val.as_str() else {
                        return Err(AdvisoryResponseError::at_line(
                            AdvisoryResponseErrorCode::SessionEvidenceFailed,
                            line_number,
                            format!("session field `{key}` must be a string"),
                        ));
                    };
                    let clean = s.trim();
                    if !clean.is_empty() {
                        found.insert(clean.to_string());
                    }
                }
            }
        }
        if let Some(sess_id_val) = obj
            .get("session")
            .and_then(Value::as_object)
            .and_then(|s| s.get("id"))
        {
            let Some(s) = sess_id_val.as_str() else {
                return Err(AdvisoryResponseError::at_line(
                    AdvisoryResponseErrorCode::SessionEvidenceFailed,
                    line_number,
                    "session.id must be a string",
                ));
            };
            let clean = s.trim();
            if !clean.is_empty() {
                found.insert(clean.to_string());
            }
        }

        if found.len() > 1 {
            return Err(AdvisoryResponseError::at_line(
                AdvisoryResponseErrorCode::SessionEvidenceFailed,
                line_number,
                "conflicting session identifiers within a single stream event",
            ));
        }

        if let Some(sid) = found.into_iter().next() {
            if !is_safe_stream_id(&sid) {
                return Err(AdvisoryResponseError::at_line(
                    AdvisoryResponseErrorCode::SessionEvidenceFailed,
                    line_number,
                    "stream session identifier is malformed or exceeds bounds",
                ));
            }
            if let Some(ref expected) = self.expected_session_id {
                if &sid != expected {
                    return Err(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::SessionEvidenceFailed,
                        line_number,
                        format!(
                            "stream session `{sid}` does not match expected session `{expected}`"
                        ),
                    ));
                }
            }
            if let Some(ref prev) = self.observed_session_id {
                if prev != &sid {
                    return Err(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::SessionEvidenceFailed,
                        line_number,
                        format!("conflicting session identifiers in stream: `{prev}` vs `{sid}`"),
                    ));
                }
            } else {
                self.observed_session_id = Some(sid);
            }
        }

        Ok(())
    }

    fn verify_and_record_model_and_effort(
        &mut self,
        line_number: usize,
        obj: &Map<String, Value>,
        is_agy_structured_init: bool,
    ) -> Result<(), AdvisoryResponseError> {
        let mut models = BTreeSet::new();
        let mut efforts = BTreeSet::new();

        let scopes = [
            Some(obj),
            obj.get("init").and_then(Value::as_object),
            obj.get("result").and_then(Value::as_object),
            obj.get("step_update").and_then(Value::as_object),
            obj.get("part").and_then(Value::as_object),
            obj.get("message").and_then(Value::as_object),
            obj.get("metadata").and_then(Value::as_object),
        ];

        for scope in scopes.into_iter().flatten() {
            if let Some(m_str) = scope.get("model").and_then(Value::as_str) {
                let trimmed = m_str.trim();
                if !trimmed.is_empty() {
                    if let Some((base, variant)) = trimmed.split_once('#') {
                        if !base.trim().is_empty() {
                            models.insert(base.trim().to_string());
                        }
                        if !variant.trim().is_empty() {
                            efforts.insert(variant.trim().to_string());
                        }
                    } else {
                        models.insert(trimmed.to_string());
                    }
                }
            }
            if let Some(m_obj) = scope.get("model").and_then(Value::as_object) {
                if let Some(pm) = extract_provider_model_pair(m_obj) {
                    models.insert(pm);
                }
                for key in ["effort", "variant", "reasoning_effort", "thinking_effort"] {
                    if let Some(eff) = m_obj.get(key).and_then(Value::as_str) {
                        let clean = eff.trim();
                        if !clean.is_empty() {
                            efforts.insert(clean.to_string());
                        }
                    }
                }
            }
            if let Some(pm) = extract_provider_model_pair(scope) {
                models.insert(pm);
            }
            for key in ["effort", "variant", "reasoning_effort", "thinking_effort"] {
                if let Some(eff) = scope.get(key).and_then(Value::as_str) {
                    let clean = eff.trim();
                    if !clean.is_empty() {
                        efforts.insert(clean.to_string());
                    }
                }
            }
        }

        if models.len() > 1 {
            return Err(AdvisoryResponseError::at_line(
                AdvisoryResponseErrorCode::ModelEvidenceFailed,
                line_number,
                "conflicting model identifiers within a single stream event",
            ));
        }

        if let Some(model) = models.into_iter().next() {
            if model != self.requested_model.base_model
                && model != self.requested_model.cli_model_arg
            {
                return Err(AdvisoryResponseError::at_line(
                    AdvisoryResponseErrorCode::ModelEvidenceFailed,
                    line_number,
                    format!(
                        "stream reported model `{model}` (expected `{}`)",
                        self.requested_model.base_model
                    ),
                ));
            }
            if let Some(ref prev) = self.observed_model {
                if prev != &model {
                    return Err(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::ModelEvidenceFailed,
                        line_number,
                        format!("conflicting models in stream: `{prev}` vs `{model}`"),
                    ));
                }
            } else {
                self.observed_model = Some(model.clone());
            }

            if self.provider == WorkerProvider::Antigravity {
                if let Some(catalog_eff) = antigravity_catalog_effort(&model) {
                    if is_agy_structured_init && efforts.is_empty() {
                        efforts.insert(catalog_eff.to_string());
                    } else if efforts.iter().any(|e| e != catalog_eff) {
                        return Err(AdvisoryResponseError::at_line(
                            AdvisoryResponseErrorCode::EffortEvidenceFailed,
                            line_number,
                            format!(
                                "explicit stream effort conflicts with Antigravity catalog model `{model}` effort `{catalog_eff}`"
                            ),
                        ));
                    }
                }
            }
        }

        if efforts.len() > 1 {
            return Err(AdvisoryResponseError::at_line(
                AdvisoryResponseErrorCode::EffortEvidenceFailed,
                line_number,
                "conflicting effort/variant values within a single stream event",
            ));
        }

        if let Some(effort) = efforts.into_iter().next() {
            match self.requested_model.effort.as_deref() {
                Some(req_eff) if req_eff != effort => {
                    return Err(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::EffortEvidenceFailed,
                        line_number,
                        format!("stream reported effort/variant `{effort}` (expected `{req_eff}`)"),
                    ));
                }
                _ => {}
            }
            if let Some(ref prev_eff) = self.observed_effort {
                if prev_eff != &effort {
                    return Err(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::EffortEvidenceFailed,
                        line_number,
                        format!(
                            "conflicting effort/variant values in stream: `{prev_eff}` vs `{effort}`"
                        ),
                    ));
                }
            } else {
                self.observed_effort = Some(effort);
            }
        }

        Ok(())
    }

    fn ingest_antigravity_event(&mut self, line_number: usize, obj: &Map<String, Value>) {
        if self.agy_candidate.is_some() {
            self.agy_candidate = None;
            self.latch_error(AdvisoryResponseError::at_line(
                AdvisoryResponseErrorCode::ConflictingTerminalOutput,
                line_number,
                "Antigravity stream emitted additional event after terminal result event",
            ));
            return;
        }

        let ev_field = obj.get("event").and_then(Value::as_str);
        let ty_field = obj.get("type").and_then(Value::as_str);
        if let (Some(ev), Some(ty)) = (ev_field, ty_field) {
            if ev != ty && !(ev == "init" && ty == "system") {
                let code = if is_tool_keyword(ev) || is_tool_keyword(ty) {
                    AdvisoryResponseErrorCode::SpoofedOrToolOutput
                } else {
                    AdvisoryResponseErrorCode::UnsupportedFraming
                };
                self.latch_error(AdvisoryResponseError::at_line(
                    code,
                    line_number,
                    format!("conflicting `event` (`{ev}`) and `type` (`{ty}`) fields"),
                ));
                return;
            }
        }

        let event_type = ev_field.or(ty_field).unwrap_or("");
        let subtype = obj
            .get("subtype")
            .and_then(Value::as_str)
            .or_else(|| {
                obj.get("step_update")
                    .and_then(Value::as_object)
                    .and_then(|s| s.get("step_type"))
                    .and_then(Value::as_str)
            })
            .unwrap_or("");

        let is_tool = is_tool_keyword(event_type)
            || is_tool_keyword(subtype)
            || has_tool_markers(obj)
            || obj
                .get("step_update")
                .and_then(Value::as_object)
                .is_some_and(has_tool_markers);

        let is_structured_init = event_type == "init"
            && obj.get("init").is_some_and(Value::is_object)
            && !is_tool
            && !has_object_error_field(obj);

        if !is_tool {
            if let Err(err) =
                self.verify_and_record_model_and_effort(line_number, obj, is_structured_init)
            {
                self.latch_error(err);
                return;
            }
        }

        match event_type {
            "init" | "system" => {
                if event_type == "system" && subtype != "init" {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        format!("unsupported Antigravity system subtype `{subtype}`"),
                    ));
                    return;
                }
                if obj.get("is_error") == Some(&Value::Bool(true))
                    || has_object_error_field(obj)
                    || obj
                        .get("init")
                        .and_then(Value::as_object)
                        .is_some_and(has_object_error_field)
                {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::ProviderError,
                        line_number,
                        "Antigravity init event reported an error",
                    ));
                    return;
                }
                self.agy_saw_init = true;
            }
            "step_update" => {
                // Never capture step_update prose or tool output as the final advisory response.
                if obj.contains_key("result") {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::SpoofedOrToolOutput,
                        line_number,
                        "Antigravity step_update event must not embed a terminal `result` object",
                    ));
                    return;
                }
                if is_tool && has_candidate_text_payload(obj) {
                    self.saw_tool_output_candidate = true;
                }
            }
            "tool" | "tool_use" | "tool_result" | "tool_call" => {
                self.saw_tool_output_candidate = true;
                if obj.get("result").is_some_and(Value::is_object) {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::SpoofedOrToolOutput,
                        line_number,
                        "tool event attempted to embed a structured `result` object",
                    ));
                }
            }
            "error" => {
                self.latch_error(AdvisoryResponseError::at_line(
                    AdvisoryResponseErrorCode::ProviderError,
                    line_number,
                    "Antigravity stream emitted a provider error event",
                ));
            }
            "result" => {
                if is_tool
                    || obj.contains_key("step_update")
                    || obj
                        .get("result")
                        .and_then(Value::as_object)
                        .is_some_and(has_tool_markers)
                {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::SpoofedOrToolOutput,
                        line_number,
                        "Antigravity terminal result event contains tool or step_update markers",
                    ));
                    return;
                }

                if !self.agy_saw_init {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::MissingTerminalFraming,
                        line_number,
                        "Antigravity result event was not preceded by an init event",
                    ));
                    return;
                }

                let Some(result_obj) = obj.get("result").and_then(Value::as_object) else {
                    if has_any_error_indicator(obj, None) {
                        self.latch_error(AdvisoryResponseError::at_line(
                            AdvisoryResponseErrorCode::ProviderError,
                            line_number,
                            "Antigravity terminal result event reported an error",
                        ));
                    } else {
                        self.latch_error(AdvisoryResponseError::at_line(
                            AdvisoryResponseErrorCode::UnsupportedFraming,
                            line_number,
                            "Antigravity terminal result must be a JSON object at `result`",
                        ));
                    }
                    return;
                };

                if has_denied_actions(obj, Some(result_obj)) {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::WorkerExecutionFailed,
                        line_number,
                        "Antigravity terminal result reported denied_actions permission block",
                    ));
                    return;
                }

                if has_any_error_indicator(obj, Some(result_obj)) {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::ProviderError,
                        line_number,
                        "Antigravity terminal result reported an error or failure status",
                    ));
                    return;
                }

                if has_truncation_stop_reason(obj, Some(result_obj)) {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::IncompleteOrPartialOutput,
                        line_number,
                        "Antigravity terminal result reported a truncated stop/finished reason",
                    ));
                    return;
                }

                let Some(status_str) = result_obj.get("status").and_then(Value::as_str) else {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        "Antigravity `result.status` is required",
                    ));
                    return;
                };
                if !status_str.eq_ignore_ascii_case("SUCCESS") {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::ProviderError,
                        line_number,
                        format!(
                            "Antigravity `result.status` is `{status_str}` (expected `SUCCESS`)"
                        ),
                    ));
                    return;
                }

                if let Some(top_status) = obj.get("status").and_then(Value::as_str) {
                    if !top_status.eq_ignore_ascii_case("SUCCESS") {
                        self.latch_error(AdvisoryResponseError::at_line(
                            AdvisoryResponseErrorCode::ProviderError,
                            line_number,
                            format!(
                                "Antigravity top-level `status` is `{top_status}` (expected `SUCCESS`)"
                            ),
                        ));
                        return;
                    }
                }
                if let Some(top_sub) = obj.get("subtype").and_then(Value::as_str) {
                    if !top_sub.is_empty() && !top_sub.eq_ignore_ascii_case("SUCCESS") {
                        self.latch_error(AdvisoryResponseError::at_line(
                            AdvisoryResponseErrorCode::ProviderError,
                            line_number,
                            format!(
                                "Antigravity top-level `subtype` is `{top_sub}` (expected `SUCCESS`)"
                            ),
                        ));
                        return;
                    }
                }

                let Some(response_val) = result_obj.get("response") else {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::MissingResponseOutput,
                        line_number,
                        "Antigravity `result.response` is missing",
                    ));
                    return;
                };
                let Some(response_text) = response_val.as_str() else {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        "Antigravity `result.response` must be a UTF-8 string",
                    ));
                    return;
                };
                if response_text.trim().is_empty() {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::MissingResponseOutput,
                        line_number,
                        "Antigravity `result.response` is empty",
                    ));
                    return;
                }

                let byte_count = response_text.len();
                if byte_count > self.budget.max_response_bytes {
                    self.latch_error(AdvisoryResponseError {
                        code: AdvisoryResponseErrorCode::ResponseTooLarge,
                        message: sanitize_and_redact(
                            &format!(
                                "Antigravity `result.response` ({byte_count} bytes) exceeds max_response_bytes ({})",
                                self.budget.max_response_bytes
                            ),
                            MAX_DIAGNOSTIC_SUMMARY_BYTES,
                        ),
                        line_number: Some(line_number),
                        observed_bytes: Some(byte_count),
                        max_response_bytes: Some(self.budget.max_response_bytes),
                    });
                    return;
                }

                if let Err(err) = validate_structured_json_object(response_text, line_number) {
                    self.latch_error(err);
                    return;
                }

                self.agy_candidate = Some(CapturedCandidate {
                    text: response_text.to_string(),
                    response_line_number: line_number,
                    session_id: self.observed_session_id.clone(),
                    message_id: None,
                    part_id: None,
                    item_id: None,
                });
            }
            "assistant" | "message" | "text" | "content" => {
                // Ignored for response capture; Antigravity responses must come from final `result.response`.
            }
            other => {
                self.latch_error(AdvisoryResponseError::at_line(
                    AdvisoryResponseErrorCode::UnsupportedFraming,
                    line_number,
                    format!("unsupported Antigravity stream event `{other}`"),
                ));
            }
        }
    }

    fn ingest_opencode_event(&mut self, line_number: usize, obj: &Map<String, Value>) {
        if self.oc_sealed_candidate.is_some() {
            self.oc_sealed_candidate = None;
            self.latch_error(AdvisoryResponseError::at_line(
                AdvisoryResponseErrorCode::ConflictingTerminalOutput,
                line_number,
                "OpenCode stream emitted additional event after terminal step_finish stop",
            ));
            return;
        }

        let ty_field = obj.get("type").and_then(Value::as_str);
        let ev_field = obj.get("event").and_then(Value::as_str);
        if let (Some(ty), Some(ev)) = (ty_field, ev_field) {
            if ty != ev {
                let code = if is_tool_keyword(ty) || is_tool_keyword(ev) {
                    AdvisoryResponseErrorCode::SpoofedOrToolOutput
                } else {
                    AdvisoryResponseErrorCode::UnsupportedFraming
                };
                self.latch_error(AdvisoryResponseError::at_line(
                    code,
                    line_number,
                    format!("conflicting OpenCode `type` (`{ty}`) and `event` (`{ev}`) fields"),
                ));
                return;
            }
        }

        let event_type = ty_field.or(ev_field).unwrap_or("");
        let part_obj = obj.get("part").and_then(Value::as_object);
        let part_type = part_obj.and_then(|p| p.get("type")).and_then(Value::as_str);
        let subtype = obj.get("subtype").and_then(Value::as_str).or(part_type);

        if has_denied_actions(obj, part_obj) {
            self.latch_error(AdvisoryResponseError::at_line(
                AdvisoryResponseErrorCode::WorkerExecutionFailed,
                line_number,
                "OpenCode stream event reported denied_actions permission block",
            ));
            return;
        }

        let is_tool = is_tool_keyword(event_type)
            || subtype.is_some_and(is_tool_keyword)
            || has_tool_markers(obj)
            || part_obj.is_some_and(has_tool_markers);

        if !is_tool && has_any_error_indicator(obj, part_obj) {
            self.latch_error(AdvisoryResponseError::at_line(
                AdvisoryResponseErrorCode::ProviderError,
                line_number,
                "OpenCode stream event reported a provider error",
            ));
            return;
        }

        if !is_tool {
            if let Err(err) = self.verify_and_record_model_and_effort(line_number, obj, false) {
                self.latch_error(err);
                return;
            }
        }

        let event_message_id = match extract_and_validate_message_id(line_number, obj, part_obj) {
            Ok(mid) => mid,
            Err(err) => {
                self.latch_error(err);
                return;
            }
        };

        match event_type {
            "session" | "init" | "system" => {}
            "step_start" => {
                if is_tool {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::SpoofedOrToolOutput,
                        line_number,
                        "OpenCode step_start event contains tool markers",
                    ));
                    return;
                }
                if part_type != Some("step-start") {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        "OpenCode step_start event requires `part.type == \"step-start\"`",
                    ));
                    return;
                }
                if self.oc_in_step {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        "OpenCode emitted nested step_start before closing active step with step_finish",
                    ));
                    return;
                }
                self.oc_in_step = true;
                self.oc_step_message_id = event_message_id;
                self.oc_step_candidate = None;
            }
            "tool" | "tool_use" | "tool_result" | "tool_call" => {
                self.saw_tool_output_candidate = true;
                if part_type == Some("text") {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::SpoofedOrToolOutput,
                        line_number,
                        "OpenCode tool event attempted to masquerade as `part.type == \"text\"`",
                    ));
                }
            }
            "reasoning" => {
                if !self.oc_in_step {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        "OpenCode reasoning event occurred outside an active step",
                    ));
                }
            }
            "text" => {
                if is_tool || has_non_assistant_role(obj, part_obj) {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::SpoofedOrToolOutput,
                        line_number,
                        "OpenCode text event contains tool or non-assistant role markers",
                    ));
                    return;
                }
                if !self.oc_in_step {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        "OpenCode text event occurred outside an active step_start..step_finish frame",
                    ));
                    return;
                }
                let Some(part) = part_obj else {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        "OpenCode text event is missing `part` object",
                    ));
                    return;
                };
                if part_type != Some("text") {
                    let code = if part_type.is_some_and(is_tool_keyword) {
                        AdvisoryResponseErrorCode::SpoofedOrToolOutput
                    } else {
                        AdvisoryResponseErrorCode::UnsupportedFraming
                    };
                    self.latch_error(AdvisoryResponseError::at_line(
                        code,
                        line_number,
                        "OpenCode text event requires `part.type == \"text\"`",
                    ));
                    return;
                }
                if part.get("synthetic") == Some(&Value::Bool(true))
                    || obj.get("synthetic") == Some(&Value::Bool(true))
                {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        "OpenCode synthetic text part cannot be accepted as model advisory output",
                    ));
                    return;
                }
                if let Err(err) = validate_opencode_text_part_completeness(line_number, obj, part) {
                    self.latch_error(err);
                    return;
                }

                if self.oc_step_candidate.is_some() {
                    self.oc_step_candidate = None;
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::ConflictingTerminalOutput,
                        line_number,
                        "OpenCode step emitted duplicate or conflicting assistant text parts",
                    ));
                    return;
                }

                if let Some(ref mid) = event_message_id {
                    if let Some(ref step_mid) = self.oc_step_message_id {
                        if step_mid != mid {
                            self.latch_error(AdvisoryResponseError::at_line(
                                AdvisoryResponseErrorCode::ConflictingTerminalOutput,
                                line_number,
                                format!(
                                    "OpenCode text part messageID `{mid}` conflicts with step messageID `{step_mid}`"
                                ),
                            ));
                            return;
                        }
                    } else {
                        self.oc_step_message_id = Some(mid.clone());
                    }
                }

                let part_id = match extract_and_validate_part_id(line_number, obj, part) {
                    Ok(pid) => pid,
                    Err(err) => {
                        self.latch_error(err);
                        return;
                    }
                };

                let Some(text_val) = part.get("text") else {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::MissingResponseOutput,
                        line_number,
                        "OpenCode text part is missing `part.text`",
                    ));
                    return;
                };
                let Some(raw_text) = text_val.as_str() else {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        "OpenCode `part.text` must be a UTF-8 string",
                    ));
                    return;
                };
                if raw_text.trim().is_empty() {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::MissingResponseOutput,
                        line_number,
                        "OpenCode `part.text` is empty",
                    ));
                    return;
                }

                let byte_count = raw_text.len();
                if byte_count > self.budget.max_response_bytes {
                    self.latch_error(AdvisoryResponseError {
                        code: AdvisoryResponseErrorCode::ResponseTooLarge,
                        message: sanitize_and_redact(
                            &format!(
                                "OpenCode `part.text` ({byte_count} bytes) exceeds max_response_bytes ({})",
                                self.budget.max_response_bytes
                            ),
                            MAX_DIAGNOSTIC_SUMMARY_BYTES,
                        ),
                        line_number: Some(line_number),
                        observed_bytes: Some(byte_count),
                        max_response_bytes: Some(self.budget.max_response_bytes),
                    });
                    return;
                }

                self.oc_step_candidate = Some(CapturedCandidate {
                    text: raw_text.to_string(),
                    response_line_number: line_number,
                    session_id: self.observed_session_id.clone(),
                    message_id: self.oc_step_message_id.clone(),
                    part_id,
                    item_id: None,
                });
            }
            "step_finish" => {
                if is_tool {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::SpoofedOrToolOutput,
                        line_number,
                        "OpenCode step_finish event contains tool markers",
                    ));
                    return;
                }
                if !self.oc_in_step {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        "OpenCode step_finish occurred without an active step_start",
                    ));
                    return;
                }
                let Some(part) = part_obj else {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        "OpenCode step_finish event is missing `part` object",
                    ));
                    return;
                };
                if part_type != Some("step-finish") {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        "OpenCode step_finish event requires `part.type == \"step-finish\"`",
                    ));
                    return;
                }

                if let Some(ref mid) = event_message_id {
                    if let Some(ref step_mid) = self.oc_step_message_id {
                        if step_mid != mid {
                            self.latch_error(AdvisoryResponseError::at_line(
                                AdvisoryResponseErrorCode::ConflictingTerminalOutput,
                                line_number,
                                format!(
                                    "OpenCode step_finish messageID `{mid}` conflicts with step messageID `{step_mid}`"
                                ),
                            ));
                            return;
                        }
                    } else {
                        self.oc_step_message_id = Some(mid.clone());
                    }
                }

                let part_reason = part.get("reason").and_then(Value::as_str);
                let top_reason = obj.get("reason").and_then(Value::as_str);
                if let (Some(pr), Some(tr)) = (part_reason, top_reason) {
                    if pr != tr {
                        self.latch_error(AdvisoryResponseError::at_line(
                            AdvisoryResponseErrorCode::UnsupportedFraming,
                            line_number,
                            format!("conflicting OpenCode step_finish reasons `{pr}` vs `{tr}`"),
                        ));
                        return;
                    }
                }
                let Some(reason) = part_reason.or(top_reason) else {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        "OpenCode step_finish is missing explicit `reason`",
                    ));
                    return;
                };

                self.oc_in_step = false;
                let step_mid = self.oc_step_message_id.take();
                let step_cand = self.oc_step_candidate.take();

                match reason {
                    "tool-calls" | "tool_calls" => {
                        // Intermediate tool-call step completed; final step must still follow.
                    }
                    "stop" => {
                        let Some(mut candidate) = step_cand else {
                            let code = if self.saw_tool_output_candidate {
                                AdvisoryResponseErrorCode::SpoofedOrToolOutput
                            } else {
                                AdvisoryResponseErrorCode::MissingResponseOutput
                            };
                            self.latch_error(AdvisoryResponseError::at_line(
                                code,
                                line_number,
                                "OpenCode terminal step finished with `stop` without a complete assistant text part",
                            ));
                            return;
                        };
                        if candidate.message_id.is_none() {
                            candidate.message_id = step_mid;
                        }
                        if candidate.session_id.is_none() {
                            candidate.session_id = self.observed_session_id.clone();
                        }
                        if let Err(err) = validate_structured_json_object(
                            &candidate.text,
                            candidate.response_line_number,
                        ) {
                            self.latch_error(err);
                            return;
                        }
                        self.oc_sealed_candidate = Some((candidate, line_number));
                    }
                    "length" | "max_tokens" | "truncated" => {
                        self.latch_error(AdvisoryResponseError::at_line(
                            AdvisoryResponseErrorCode::IncompleteOrPartialOutput,
                            line_number,
                            format!(
                                "OpenCode step_finish reported truncated generation (`reason: {reason}`)"
                            ),
                        ));
                    }
                    "error" | "failed" | "failure" | "cancelled" | "canceled" | "aborted" => {
                        self.latch_error(AdvisoryResponseError::at_line(
                            AdvisoryResponseErrorCode::ProviderError,
                            line_number,
                            format!("OpenCode step_finish reported failure reason `{reason}`"),
                        ));
                    }
                    other => {
                        self.latch_error(AdvisoryResponseError::at_line(
                            AdvisoryResponseErrorCode::UnsupportedFraming,
                            line_number,
                            format!("unsupported OpenCode step_finish reason `{other}`"),
                        ));
                    }
                }
            }
            other => {
                self.latch_error(AdvisoryResponseError::at_line(
                    AdvisoryResponseErrorCode::UnsupportedFraming,
                    line_number,
                    format!(
                        "unsupported OpenCode stream event `{other}`; expected step_start, text, and step_finish"
                    ),
                ));
            }
        }
    }

    fn ingest_codex_event(&mut self, line_number: usize, obj: &Map<String, Value>) {
        if self.codex_sealed_candidate.is_some() {
            self.codex_sealed_candidate = None;
            self.latch_error(AdvisoryResponseError::at_line(
                AdvisoryResponseErrorCode::ConflictingTerminalOutput,
                line_number,
                "Codex CLI stream emitted additional event after terminal turn.completed",
            ));
            return;
        }

        let ty_field = obj.get("type").and_then(Value::as_str);
        let ev_field = obj.get("event").and_then(Value::as_str);
        if let (Some(ty), Some(ev)) = (ty_field, ev_field) {
            if ty != ev {
                let code = if is_tool_keyword(ty) || is_tool_keyword(ev) {
                    AdvisoryResponseErrorCode::SpoofedOrToolOutput
                } else {
                    AdvisoryResponseErrorCode::UnsupportedFraming
                };
                self.latch_error(AdvisoryResponseError::at_line(
                    code,
                    line_number,
                    format!("conflicting Codex `type` (`{ty}`) and `event` (`{ev}`) fields"),
                ));
                return;
            }
        }

        let event_type = ty_field.or(ev_field).unwrap_or("");
        let item_obj = obj.get("item").and_then(Value::as_object);
        let item_type = item_obj.and_then(|i| i.get("type")).and_then(Value::as_str);

        if has_denied_actions(obj, item_obj) {
            self.latch_error(AdvisoryResponseError::at_line(
                AdvisoryResponseErrorCode::WorkerExecutionFailed,
                line_number,
                "Codex CLI stream event reported denied_actions permission block",
            ));
            return;
        }

        let is_tool = is_tool_keyword(event_type)
            || item_type.is_some_and(is_codex_tool_item_type)
            || has_tool_markers(obj)
            || item_obj.is_some_and(has_tool_markers);

        if !is_tool {
            if let Err(err) = self.verify_and_record_model_and_effort(line_number, obj, false) {
                self.latch_error(err);
                return;
            }
        }

        match event_type {
            "thread.started" => {
                if is_tool || has_any_error_indicator(obj, None) {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::ProviderError,
                        line_number,
                        "Codex thread.started event reported an error or invalid tool marker",
                    ));
                    return;
                }
                if self.codex_saw_thread_started {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::ConflictingTerminalOutput,
                        line_number,
                        "Codex CLI stream emitted duplicate thread.started events",
                    ));
                    return;
                }
                if self.observed_session_id.is_none() {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::SessionEvidenceFailed,
                        line_number,
                        "Codex thread.started event is missing a valid `thread_id`",
                    ));
                    return;
                }
                self.codex_saw_thread_started = true;
            }
            "turn.started" => {
                if is_tool || has_any_error_indicator(obj, None) {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::ProviderError,
                        line_number,
                        "Codex turn.started event reported an error or invalid tool marker",
                    ));
                    return;
                }
                if !self.codex_saw_thread_started {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::MissingTerminalFraming,
                        line_number,
                        "Codex turn.started event was not preceded by thread.started",
                    ));
                    return;
                }
                if self.codex_in_turn {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        "Codex emitted nested turn.started before closing active turn",
                    ));
                    return;
                }
                self.codex_in_turn = true;
                self.codex_prose_preamble_count = 0;
                self.codex_segment_prose_line = None;
                self.codex_turn_candidate = None;
            }
            "item.started" | "item.updated" => {
                if !self.codex_saw_thread_started || !self.codex_in_turn {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        format!("Codex `{event_type}` occurred outside an active thread/turn"),
                    ));
                    return;
                }
                let Some(item) = item_obj else {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        format!("Codex `{event_type}` is missing `item` object"),
                    ));
                    return;
                };
                if item_type == Some("error")
                    || (!is_tool && has_any_error_indicator(obj, Some(item)))
                {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::ProviderError,
                        line_number,
                        format!("Codex `{event_type}` reported an error"),
                    ));
                    return;
                }
                if is_tool && has_codex_candidate_text(item) {
                    self.saw_tool_output_candidate = true;
                }
            }
            "item.completed" => {
                if !self.codex_saw_thread_started || !self.codex_in_turn {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        "Codex item.completed occurred outside an active thread.started/turn.started frame",
                    ));
                    return;
                }
                let Some(item) = item_obj else {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        "Codex item.completed event is missing `item` object",
                    ));
                    return;
                };
                let Some(ity) = item_type else {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        "Codex item.completed event is missing `item.type`",
                    ));
                    return;
                };

                match ity {
                    "agent_message" => {
                        if is_tool || has_non_assistant_role(obj, Some(item)) {
                            self.latch_error(AdvisoryResponseError::at_line(
                                AdvisoryResponseErrorCode::SpoofedOrToolOutput,
                                line_number,
                                "Codex agent_message item contains tool or non-assistant role markers",
                            ));
                            return;
                        }
                        if has_any_error_indicator(obj, Some(item)) {
                            self.latch_error(AdvisoryResponseError::at_line(
                                AdvisoryResponseErrorCode::ProviderError,
                                line_number,
                                "Codex agent_message item reported an error status",
                            ));
                            return;
                        }
                        if has_truncation_stop_reason(obj, Some(item))
                            || obj.contains_key("delta")
                            || item.contains_key("delta")
                            || item.get("partial") == Some(&Value::Bool(true))
                            || item.get("incomplete") == Some(&Value::Bool(true))
                        {
                            self.latch_error(AdvisoryResponseError::at_line(
                                AdvisoryResponseErrorCode::IncompleteOrPartialOutput,
                                line_number,
                                "Codex agent_message item carries partial, delta, or truncated markers",
                            ));
                            return;
                        }

                        let Some(raw_item_id) = item.get("id").and_then(Value::as_str) else {
                            self.latch_error(AdvisoryResponseError::at_line(
                                AdvisoryResponseErrorCode::UnsupportedFraming,
                                line_number,
                                "Codex agent_message item is missing string `item.id`",
                            ));
                            return;
                        };
                        let item_id = raw_item_id.trim();
                        if !is_safe_stream_id(item_id) {
                            self.latch_error(AdvisoryResponseError::at_line(
                                AdvisoryResponseErrorCode::UnsupportedFraming,
                                line_number,
                                "Codex `item.id` is malformed or exceeds bounds",
                            ));
                            return;
                        }
                        if !self.codex_seen_agent_item_ids.insert(item_id.to_string()) {
                            self.codex_turn_candidate = None;
                            self.latch_error(AdvisoryResponseError::at_line(
                                AdvisoryResponseErrorCode::ConflictingTerminalOutput,
                                line_number,
                                format!("Codex stream emitted duplicate completed agent_message id `{item_id}`"),
                            ));
                            return;
                        }

                        let Some(text_val) = item.get("text") else {
                            self.latch_error(AdvisoryResponseError::at_line(
                                AdvisoryResponseErrorCode::MissingResponseOutput,
                                line_number,
                                "Codex agent_message item is missing `item.text`",
                            ));
                            return;
                        };
                        let Some(raw_text) = text_val.as_str() else {
                            self.latch_error(AdvisoryResponseError::at_line(
                                AdvisoryResponseErrorCode::UnsupportedFraming,
                                line_number,
                                "Codex `item.text` must be a UTF-8 string",
                            ));
                            return;
                        };
                        if raw_text.trim().is_empty() {
                            self.latch_error(AdvisoryResponseError::at_line(
                                AdvisoryResponseErrorCode::MissingResponseOutput,
                                line_number,
                                "Codex `item.text` is empty",
                            ));
                            return;
                        }

                        let byte_count = raw_text.len();
                        if byte_count > self.budget.max_response_bytes {
                            self.latch_error(AdvisoryResponseError {
                                code: AdvisoryResponseErrorCode::ResponseTooLarge,
                                message: sanitize_and_redact(
                                    &format!(
                                        "Codex `item.text` ({byte_count} bytes) exceeds max_response_bytes ({})",
                                        self.budget.max_response_bytes
                                    ),
                                    MAX_DIAGNOSTIC_SUMMARY_BYTES,
                                ),
                                line_number: Some(line_number),
                                observed_bytes: Some(byte_count),
                                max_response_bytes: Some(self.budget.max_response_bytes),
                            });
                            return;
                        }

                        if self.codex_turn_candidate.is_some() {
                            self.codex_turn_candidate = None;
                            self.latch_error(AdvisoryResponseError::at_line(
                                AdvisoryResponseErrorCode::ConflictingTerminalOutput,
                                line_number,
                                "Codex turn emitted multiple completed agent_message items",
                            ));
                            return;
                        }

                        match classify_codex_agent_message_text(raw_text, line_number) {
                            CodexAgentMessageClassification::StructuredJsonObject => {
                                self.codex_turn_candidate = Some(CapturedCandidate {
                                    text: raw_text.to_string(),
                                    response_line_number: line_number,
                                    session_id: self.observed_session_id.clone(),
                                    message_id: None,
                                    part_id: None,
                                    item_id: Some(item_id.to_string()),
                                });
                            }
                            CodexAgentMessageClassification::OrdinaryProsePreamble => {
                                self.codex_prose_preamble_count += 1;
                                if self.codex_prose_preamble_count
                                    > MAX_CODEX_PROSE_PREAMBLES_PER_TURN
                                {
                                    self.latch_error(AdvisoryResponseError::at_line(
                                        AdvisoryResponseErrorCode::ConflictingTerminalOutput,
                                        line_number,
                                        format!(
                                            "Codex turn exceeded maximum ordinary prose preamble agent_message count ({MAX_CODEX_PROSE_PREAMBLES_PER_TURN})"
                                        ),
                                    ));
                                    return;
                                }
                                self.codex_segment_prose_line = Some(line_number);
                            }
                            CodexAgentMessageClassification::InvalidStructuredCandidate(err) => {
                                self.latch_error(err);
                            }
                        }
                    }
                    "reasoning" | "todo_list" => {
                        if has_any_error_indicator(obj, Some(item)) {
                            self.latch_error(AdvisoryResponseError::at_line(
                                AdvisoryResponseErrorCode::ProviderError,
                                line_number,
                                format!("Codex `{ity}` item reported an error"),
                            ));
                        }
                    }
                    "command_execution" | "mcp_tool_call" | "web_search" | "file_change" => {
                        self.saw_tool_output_candidate = true;
                        // A read-only shell command under the scoped permission profile may precede the
                        // final agent_message, so if an earlier agent_message was buffered in the same
                        // turn before a tool item completed, clear it so only the final post-tool
                        // agent_message can seal the turn.
                        self.codex_turn_candidate = None;
                        self.codex_segment_prose_line = None;
                    }
                    "error" => {
                        self.latch_error(AdvisoryResponseError::at_line(
                            AdvisoryResponseErrorCode::ProviderError,
                            line_number,
                            "Codex stream emitted an error item",
                        ));
                    }
                    other => {
                        let code = if is_codex_tool_item_type(other) {
                            AdvisoryResponseErrorCode::SpoofedOrToolOutput
                        } else {
                            AdvisoryResponseErrorCode::UnsupportedFraming
                        };
                        self.latch_error(AdvisoryResponseError::at_line(
                            code,
                            line_number,
                            format!("unsupported Codex item.completed type `{other}`"),
                        ));
                    }
                }
            }
            "turn.completed" => {
                if is_tool {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::SpoofedOrToolOutput,
                        line_number,
                        "Codex turn.completed event contains tool markers",
                    ));
                    return;
                }
                if !self.codex_saw_thread_started || !self.codex_in_turn {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::MissingTerminalFraming,
                        line_number,
                        "Codex turn.completed occurred without an active thread.started and turn.started",
                    ));
                    return;
                }
                if has_any_error_indicator(obj, None) {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::ProviderError,
                        line_number,
                        "Codex turn.completed event reported an error or failure status",
                    ));
                    return;
                }
                if has_truncation_stop_reason(obj, None) {
                    self.latch_error(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::IncompleteOrPartialOutput,
                        line_number,
                        "Codex turn.completed reported a truncated stop reason",
                    ));
                    return;
                }

                let usage = match extract_genuine_turn_usage(obj) {
                    Ok(u) => u,
                    Err(msg) => {
                        self.latch_error(AdvisoryResponseError::at_line(
                            AdvisoryResponseErrorCode::UnsupportedFraming,
                            line_number,
                            msg,
                        ));
                        return;
                    }
                };

                self.codex_in_turn = false;
                let prose_line = self.codex_segment_prose_line.take();
                let Some(mut candidate) = self.codex_turn_candidate.take() else {
                    if let Some(prose_line_number) = prose_line {
                        self.latch_error(AdvisoryResponseError::at_line(
                            AdvisoryResponseErrorCode::InvalidStructuredJson,
                            prose_line_number,
                            "Codex turn completed with ordinary prose agent_message output and no final structured JSON object",
                        ));
                        return;
                    }
                    let code = if self.saw_tool_output_candidate {
                        AdvisoryResponseErrorCode::SpoofedOrToolOutput
                    } else {
                        AdvisoryResponseErrorCode::MissingResponseOutput
                    };
                    self.latch_error(AdvisoryResponseError::at_line(
                        code,
                        line_number,
                        "Codex turn.completed occurred without a completed final agent_message item",
                    ));
                    return;
                };
                if candidate.session_id.is_none() {
                    candidate.session_id = self.observed_session_id.clone();
                }
                if let Err(err) =
                    validate_structured_json_object(&candidate.text, candidate.response_line_number)
                {
                    self.latch_error(err);
                    return;
                }
                self.codex_sealed_candidate = Some((candidate, line_number, usage));
            }
            "turn.failed" | "error" => {
                self.codex_in_turn = false;
                self.codex_segment_prose_line = None;
                self.codex_turn_candidate = None;
                self.latch_error(AdvisoryResponseError::at_line(
                    AdvisoryResponseErrorCode::ProviderError,
                    line_number,
                    format!("Codex stream emitted terminal failure event `{event_type}`"),
                ));
            }
            other => {
                self.latch_error(AdvisoryResponseError::at_line(
                    AdvisoryResponseErrorCode::UnsupportedFraming,
                    line_number,
                    format!(
                        "unsupported Codex CLI stream event `{other}`; expected thread.started, turn.started, item.*, and turn.completed"
                    ),
                ));
            }
        }
    }

    pub(crate) fn finalize(self, execution: WorkerExecutionReport) -> AdvisoryStructuredExecution {
        if let Some(err) = self.evaluate_failure(&execution) {
            return AdvisoryStructuredExecution {
                execution,
                response: None,
                response_error: Some(err),
            };
        }

        let (candidate, terminal_line_number, source, usage) = match self.provider {
            WorkerProvider::Antigravity => {
                let Some(cand) = self.agy_candidate else {
                    let code = if self.saw_tool_output_candidate {
                        AdvisoryResponseErrorCode::SpoofedOrToolOutput
                    } else {
                        AdvisoryResponseErrorCode::MissingTerminalFraming
                    };
                    return AdvisoryStructuredExecution {
                        execution,
                        response: None,
                        response_error: Some(AdvisoryResponseError::new(
                            code,
                            "Antigravity stream did not emit a verified terminal `result.response`",
                        )),
                    };
                };
                let line = cand.response_line_number;
                (
                    cand,
                    line,
                    AdvisoryResponseSource::AntigravityFinalResultResponse,
                    None,
                )
            }
            WorkerProvider::OpenCodeV2 => {
                if self.oc_in_step {
                    return AdvisoryStructuredExecution {
                        execution,
                        response: None,
                        response_error: Some(AdvisoryResponseError::new(
                            AdvisoryResponseErrorCode::MissingTerminalFraming,
                            "OpenCode stream ended before closing the active step with `step_finish` (`reason: \"stop\"`)",
                        )),
                    };
                }
                let Some((cand, term_line)) = self.oc_sealed_candidate else {
                    let code = if self.saw_tool_output_candidate {
                        AdvisoryResponseErrorCode::SpoofedOrToolOutput
                    } else {
                        AdvisoryResponseErrorCode::MissingTerminalFraming
                    };
                    return AdvisoryStructuredExecution {
                        execution,
                        response: None,
                        response_error: Some(AdvisoryResponseError::new(
                            code,
                            "OpenCode stream did not emit a complete assistant text part sealed by `step_finish` (`reason: \"stop\"`)",
                        )),
                    };
                };
                (
                    cand,
                    term_line,
                    AdvisoryResponseSource::OpenCodeAssistantTextPartStop,
                    None,
                )
            }
            WorkerProvider::CodexCli => {
                if self.codex_in_turn {
                    return AdvisoryStructuredExecution {
                        execution,
                        response: None,
                        response_error: Some(AdvisoryResponseError::new(
                            AdvisoryResponseErrorCode::MissingTerminalFraming,
                            "Codex CLI stream ended before closing the active turn with `turn.completed`",
                        )),
                    };
                }
                let Some((cand, term_line, turn_usage)) = self.codex_sealed_candidate else {
                    let code = if self.saw_tool_output_candidate {
                        AdvisoryResponseErrorCode::SpoofedOrToolOutput
                    } else {
                        AdvisoryResponseErrorCode::MissingTerminalFraming
                    };
                    return AdvisoryStructuredExecution {
                        execution,
                        response: None,
                        response_error: Some(AdvisoryResponseError::new(
                            code,
                            "Codex CLI stream did not emit a completed `agent_message` sealed by `turn.completed`",
                        )),
                    };
                };
                (
                    cand,
                    term_line,
                    AdvisoryResponseSource::CodexAgentMessageTurnCompleted,
                    turn_usage.or_else(|| execution.evidence.observed_usage.clone()),
                )
            }
        };

        let expected_sid = execution
            .allocated_session_id
            .as_deref()
            .or(execution.requested_session_id.as_deref());
        let final_session_id = candidate
            .session_id
            .or(self.observed_session_id)
            .or_else(|| execution.evidence.observed_session_id.clone());

        if let Some(exp_sid) = expected_sid {
            if final_session_id.as_deref() != Some(exp_sid) {
                let response_error = AdvisoryResponseError::new(
                    AdvisoryResponseErrorCode::SessionEvidenceFailed,
                    format!(
                        "terminal advisory response session `{:?}` did not match expected session `{exp_sid}`",
                        final_session_id
                    ),
                );
                return AdvisoryStructuredExecution {
                    execution,
                    response: None,
                    response_error: Some(response_error),
                };
            }
        }

        if self.provider == WorkerProvider::OpenCodeV2 {
            if execution.requested_session_id.is_none()
                && execution.validated_opencode_metadata().is_none()
            {
                return AdvisoryStructuredExecution {
                    execution,
                    response: None,
                    response_error: Some(AdvisoryResponseError::new(
                        AdvisoryResponseErrorCode::WorkerExecutionFailed,
                        "owned OpenCode v2 advisory run lacked validated session metadata",
                    )),
                };
            }
            if let Some(meta) = execution.validated_opencode_metadata() {
                if let Some(ref msg_id) = candidate.message_id {
                    if msg_id != &meta.terminal_message_id {
                        let response_error = AdvisoryResponseError::new(
                            AdvisoryResponseErrorCode::ConflictingTerminalOutput,
                            format!(
                                "OpenCode stream messageID `{msg_id}` conflicts with session metadata terminal_message_id `{}`",
                                meta.terminal_message_id
                            ),
                        );
                        return AdvisoryStructuredExecution {
                            execution,
                            response: None,
                            response_error: Some(response_error),
                        };
                    }
                }
            }
        }

        let byte_count = candidate.text.len();
        let sha256_digest = compute_sha256_hex(candidate.text.as_bytes());
        let effort_evidence_source = execution.evidence.effort_evidence_source;

        let response = BoundedAdvisoryResponse {
            text: candidate.text,
            byte_count,
            sha256_digest,
            provider: self.provider,
            source,
            framing: AdvisoryResponseFraming {
                response_line_number: candidate.response_line_number,
                terminal_line_number,
                session_id: final_session_id,
                message_id: candidate.message_id,
                part_id: candidate.part_id,
                item_id: candidate.item_id,
                effort_evidence_source,
                usage,
            },
        };

        AdvisoryStructuredExecution {
            execution,
            response: Some(response),
            response_error: None,
        }
    }

    fn evaluate_failure(&self, execution: &WorkerExecutionReport) -> Option<AdvisoryResponseError> {
        if execution.overflow.is_some() || execution.status == WorkerRunStatus::OutputOverflow {
            return Some(AdvisoryResponseError::new(
                AdvisoryResponseErrorCode::StreamOutputOverflow,
                format!(
                    "worker stream exceeded output budget ({:?})",
                    execution.overflow
                ),
            ));
        }

        if matches!(
            execution.status,
            WorkerRunStatus::Cancelled
                | WorkerRunStatus::TimedOut
                | WorkerRunStatus::IncompleteCleanup
                | WorkerRunStatus::Blocked
        ) || !execution.evidence.blocked_events.is_empty()
        {
            return Some(AdvisoryResponseError::new(
                AdvisoryResponseErrorCode::WorkerExecutionFailed,
                format!(
                    "worker execution did not complete normally (status: {})",
                    execution.status.as_str()
                ),
            ));
        }

        if execution.evidence.malformed_lines_count > 0
            || execution.status == WorkerRunStatus::MalformedStream
        {
            if let Some(ref err) = self.latched_error {
                if err.code == AdvisoryResponseErrorCode::MalformedStream {
                    return Some(err.clone());
                }
            }
            return Some(AdvisoryResponseError::new(
                AdvisoryResponseErrorCode::MalformedStream,
                format!(
                    "worker stdout contained {} malformed NDJSON line(s)",
                    execution.evidence.malformed_lines_count
                ),
            ));
        }

        if !execution.evidence.provider_errors.is_empty()
            || execution.status == WorkerRunStatus::ProviderError
            || execution.opencode_metadata_error.as_deref() == Some("provider_error")
        {
            if let Some(ref err) = self.latched_error {
                if err.code == AdvisoryResponseErrorCode::ProviderError {
                    return Some(err.clone());
                }
            }
            let detail = execution
                .evidence
                .provider_errors
                .first()
                .map(|e| e.message.as_str())
                .unwrap_or("provider reported an execution error");
            return Some(AdvisoryResponseError::new(
                AdvisoryResponseErrorCode::ProviderError,
                detail,
            ));
        }

        if execution.status == WorkerRunStatus::NonZeroExit
            || execution.exit_code != Some(0)
            || execution.signal.is_some()
        {
            return Some(AdvisoryResponseError::new(
                AdvisoryResponseErrorCode::WorkerExecutionFailed,
                format!(
                    "worker exited non-zero (status: {}, exit_code: {:?}, signal: {:?})",
                    execution.status.as_str(),
                    execution.exit_code,
                    execution.signal
                ),
            ));
        }

        if !execution.has_satisfied_model_evidence()
            || matches!(
                execution.status,
                WorkerRunStatus::ModelMismatch | WorkerRunStatus::MissingModelEvidence
            )
        {
            if let Some(ref err) = self.latched_error {
                if err.code == AdvisoryResponseErrorCode::ModelEvidenceFailed {
                    return Some(err.clone());
                }
            }
            return Some(AdvisoryResponseError::new(
                AdvisoryResponseErrorCode::ModelEvidenceFailed,
                format!(
                    "model evidence verification failed (status: {})",
                    execution.status.as_str()
                ),
            ));
        }

        if !execution.has_satisfied_effort_evidence()
            || matches!(
                execution.status,
                WorkerRunStatus::EffortMismatch | WorkerRunStatus::MissingEffortEvidence
            )
        {
            if let Some(ref err) = self.latched_error {
                if err.code == AdvisoryResponseErrorCode::EffortEvidenceFailed {
                    return Some(err.clone());
                }
            }
            return Some(AdvisoryResponseError::new(
                AdvisoryResponseErrorCode::EffortEvidenceFailed,
                format!(
                    "effort/variant evidence verification failed (status: {})",
                    execution.status.as_str()
                ),
            ));
        }

        if !execution.has_satisfied_session_evidence()
            || execution.status == WorkerRunStatus::SessionMismatch
        {
            if let Some(ref err) = self.latched_error {
                if err.code == AdvisoryResponseErrorCode::SessionEvidenceFailed {
                    return Some(err.clone());
                }
            }
            return Some(AdvisoryResponseError::new(
                AdvisoryResponseErrorCode::SessionEvidenceFailed,
                "session identity evidence verification failed",
            ));
        }

        if let Some(ref err) = self.latched_error {
            return Some(err.clone());
        }

        if !execution.evidence.saw_result_event
            || execution.status == WorkerRunStatus::MissingResultEvidence
        {
            if self.saw_tool_output_candidate {
                return Some(AdvisoryResponseError::new(
                    AdvisoryResponseErrorCode::SpoofedOrToolOutput,
                    "stream emitted tool output without a verified terminal assistant response",
                ));
            }
            return Some(AdvisoryResponseError::new(
                AdvisoryResponseErrorCode::MissingTerminalFraming,
                "worker exited without emitting a verified terminal completion event on stdout",
            ));
        }

        if execution.status != WorkerRunStatus::Succeeded
            || !execution.has_complete_natural_completion_evidence()
        {
            return Some(AdvisoryResponseError::new(
                AdvisoryResponseErrorCode::WorkerExecutionFailed,
                format!(
                    "worker execution did not succeed cleanly (status: {}, exit_code: {:?})",
                    execution.status.as_str(),
                    execution.exit_code
                ),
            ));
        }

        None
    }
}

fn compute_sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn validate_structured_json_object(
    raw_text: &str,
    line_number: usize,
) -> Result<(), AdvisoryResponseError> {
    match serde_json::from_str::<Value>(raw_text) {
        Ok(Value::Object(_)) => Ok(()),
        Ok(_) => Err(AdvisoryResponseError::at_line(
            AdvisoryResponseErrorCode::InvalidStructuredJson,
            line_number,
            "captured advisory response must be a top-level JSON object",
        )),
        Err(err) => Err(AdvisoryResponseError::at_line(
            AdvisoryResponseErrorCode::InvalidStructuredJson,
            line_number,
            format!("captured advisory response is not valid complete JSON: {err}"),
        )),
    }
}

enum CodexAgentMessageClassification {
    StructuredJsonObject,
    OrdinaryProsePreamble,
    InvalidStructuredCandidate(AdvisoryResponseError),
}

fn classify_codex_agent_message_text(
    raw_text: &str,
    line_number: usize,
) -> CodexAgentMessageClassification {
    match serde_json::from_str::<Value>(raw_text) {
        Ok(Value::Object(_)) => CodexAgentMessageClassification::StructuredJsonObject,
        Ok(_) => CodexAgentMessageClassification::InvalidStructuredCandidate(
            AdvisoryResponseError::at_line(
                AdvisoryResponseErrorCode::InvalidStructuredJson,
                line_number,
                "captured advisory response must be a top-level JSON object",
            ),
        ),
        Err(err) => {
            if looks_like_malformed_json_candidate(raw_text) {
                CodexAgentMessageClassification::InvalidStructuredCandidate(
                    AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::InvalidStructuredJson,
                        line_number,
                        format!("captured advisory response is not valid complete JSON: {err}"),
                    ),
                )
            } else {
                CodexAgentMessageClassification::OrdinaryProsePreamble
            }
        }
    }
}

fn looks_like_malformed_json_candidate(raw_text: &str) -> bool {
    let trimmed = raw_text.trim();
    trimmed.starts_with('{')
        || trimmed.starts_with('[')
        || trimmed.starts_with('"')
        || trimmed.ends_with('}')
        || trimmed.contains("```")
        || contains_json_object_opening(trimmed)
}

fn contains_json_object_opening(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'{' {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j == bytes.len() || matches!(bytes[j], b'"' | b'}' | b'{') {
                return true;
            }
        }
        i += 1;
    }
    false
}

fn validate_opencode_text_part_completeness(
    line_number: usize,
    obj: &Map<String, Value>,
    part: &Map<String, Value>,
) -> Result<(), AdvisoryResponseError> {
    if obj.contains_key("delta")
        || part.contains_key("delta")
        || obj.get("partial") == Some(&Value::Bool(true))
        || part.get("partial") == Some(&Value::Bool(true))
        || obj.get("incomplete") == Some(&Value::Bool(true))
        || part.get("incomplete") == Some(&Value::Bool(true))
    {
        return Err(AdvisoryResponseError::at_line(
            AdvisoryResponseErrorCode::IncompleteOrPartialOutput,
            line_number,
            "OpenCode text event carries delta or partial streaming markers",
        ));
    }

    for scope in [obj, part] {
        for key in ["status", "state"] {
            if let Some(st) = scope.get(key).and_then(Value::as_str) {
                let lower = st.to_ascii_lowercase();
                if matches!(
                    lower.as_str(),
                    "running" | "pending" | "in_progress" | "streaming" | "incomplete" | "partial"
                ) {
                    return Err(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::IncompleteOrPartialOutput,
                        line_number,
                        format!("OpenCode text event has non-terminal `{key}: {st}`"),
                    ));
                }
            }
        }
    }

    if let Some(time_val) = part.get("time") {
        let Some(time_obj) = time_val.as_object() else {
            return Err(AdvisoryResponseError::at_line(
                AdvisoryResponseErrorCode::IncompleteOrPartialOutput,
                line_number,
                "OpenCode `part.time` must be a JSON object when present",
            ));
        };
        let end_ms = time_obj
            .get("end")
            .or_else(|| time_obj.get("completed"))
            .and_then(Value::as_u64);
        let Some(end_ms) = end_ms else {
            return Err(AdvisoryResponseError::at_line(
                AdvisoryResponseErrorCode::IncompleteOrPartialOutput,
                line_number,
                "OpenCode `part.time` is missing a completed `end` timestamp",
            ));
        };
        if let Some(start_ms) = time_obj
            .get("start")
            .or_else(|| time_obj.get("created"))
            .and_then(Value::as_u64)
        {
            if end_ms < start_ms {
                return Err(AdvisoryResponseError::at_line(
                    AdvisoryResponseErrorCode::IncompleteOrPartialOutput,
                    line_number,
                    "OpenCode `part.time.end` precedes `part.time.start`",
                ));
            }
        }
    }

    Ok(())
}

fn extract_and_validate_message_id(
    line_number: usize,
    obj: &Map<String, Value>,
    part: Option<&Map<String, Value>>,
) -> Result<Option<String>, AdvisoryResponseError> {
    let mut ids = BTreeSet::new();
    for key in ["messageID", "message_id"] {
        if let Some(val) = obj.get(key) {
            let Some(s) = val.as_str() else {
                return Err(AdvisoryResponseError::at_line(
                    AdvisoryResponseErrorCode::UnsupportedFraming,
                    line_number,
                    format!("`{key}` must be a string"),
                ));
            };
            let clean = s.trim();
            if !clean.is_empty() {
                ids.insert(clean.to_string());
            }
        }
        if let Some(p) = part {
            if let Some(val) = p.get(key) {
                let Some(s) = val.as_str() else {
                    return Err(AdvisoryResponseError::at_line(
                        AdvisoryResponseErrorCode::UnsupportedFraming,
                        line_number,
                        format!("`part.{key}` must be a string"),
                    ));
                };
                let clean = s.trim();
                if !clean.is_empty() {
                    ids.insert(clean.to_string());
                }
            }
        }
    }

    if ids.len() > 1 {
        return Err(AdvisoryResponseError::at_line(
            AdvisoryResponseErrorCode::ConflictingTerminalOutput,
            line_number,
            "conflicting `messageID` values within a single OpenCode event",
        ));
    }

    if let Some(mid) = ids.into_iter().next() {
        if !is_safe_stream_id(&mid) {
            return Err(AdvisoryResponseError::at_line(
                AdvisoryResponseErrorCode::UnsupportedFraming,
                line_number,
                "OpenCode `messageID` is malformed or exceeds bounds",
            ));
        }
        Ok(Some(mid))
    } else {
        Ok(None)
    }
}

fn extract_and_validate_part_id(
    line_number: usize,
    obj: &Map<String, Value>,
    part: &Map<String, Value>,
) -> Result<Option<String>, AdvisoryResponseError> {
    let mut ids = BTreeSet::new();
    for key in ["partID", "part_id"] {
        if let Some(val) = obj.get(key) {
            let Some(s) = val.as_str() else {
                return Err(AdvisoryResponseError::at_line(
                    AdvisoryResponseErrorCode::UnsupportedFraming,
                    line_number,
                    format!("`{key}` must be a string"),
                ));
            };
            let clean = s.trim();
            if !clean.is_empty() {
                ids.insert(clean.to_string());
            }
        }
    }
    for key in ["id", "partID", "part_id"] {
        if let Some(val) = part.get(key) {
            let Some(s) = val.as_str() else {
                return Err(AdvisoryResponseError::at_line(
                    AdvisoryResponseErrorCode::UnsupportedFraming,
                    line_number,
                    format!("`part.{key}` must be a string"),
                ));
            };
            let clean = s.trim();
            if !clean.is_empty() {
                ids.insert(clean.to_string());
            }
        }
    }

    if ids.len() > 1 {
        return Err(AdvisoryResponseError::at_line(
            AdvisoryResponseErrorCode::ConflictingTerminalOutput,
            line_number,
            "conflicting part identifiers within a single OpenCode text event",
        ));
    }

    if let Some(pid) = ids.into_iter().next() {
        if !is_safe_stream_id(&pid) {
            return Err(AdvisoryResponseError::at_line(
                AdvisoryResponseErrorCode::UnsupportedFraming,
                line_number,
                "OpenCode part identifier is malformed or exceeds bounds",
            ));
        }
        Ok(Some(pid))
    } else {
        Ok(None)
    }
}

fn extract_provider_model_pair(obj: &Map<String, Value>) -> Option<String> {
    let provider_id = obj
        .get("providerID")
        .or_else(|| obj.get("provider_id"))
        .and_then(Value::as_str)?
        .trim();
    let model_id = obj
        .get("modelID")
        .or_else(|| obj.get("model_id"))
        .or_else(|| obj.get("id"))
        .and_then(Value::as_str)?
        .trim();
    if provider_id.is_empty() || model_id.is_empty() {
        None
    } else {
        Some(format!("{provider_id}/{model_id}"))
    }
}

fn is_safe_stream_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_LEN
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn is_tool_keyword(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "tool"
            | "tool_use"
            | "tool-use"
            | "tool_result"
            | "tool-result"
            | "tool_call"
            | "tool-call"
    ) || lower.contains("tool")
}

fn is_codex_tool_item_type(value: &str) -> bool {
    is_tool_keyword(value)
        || matches!(
            value,
            "command_execution" | "mcp_tool_call" | "web_search" | "file_change"
        )
}

fn has_codex_candidate_text(item: &Map<String, Value>) -> bool {
    for key in ["text", "aggregated_output", "output", "result"] {
        if item
            .get(key)
            .and_then(Value::as_str)
            .is_some_and(|s| !s.trim().is_empty())
        {
            return true;
        }
    }
    false
}

fn has_tool_markers(obj: &Map<String, Value>) -> bool {
    [
        "tool",
        "tool_name",
        "tool_use",
        "tool_result",
        "tool_call",
        "callID",
        "call_id",
        "toolCallId",
        "tool_use_id",
    ]
    .into_iter()
    .any(|key| obj.get(key).is_some_and(|v| !v.is_null()))
}

fn has_non_assistant_role(obj: &Map<String, Value>, sub: Option<&Map<String, Value>>) -> bool {
    for scope in [Some(obj), sub].into_iter().flatten() {
        if let Some(role) = scope.get("role").and_then(Value::as_str) {
            if !role.eq_ignore_ascii_case("assistant") {
                return true;
            }
        }
    }
    false
}

fn has_candidate_text_payload(obj: &Map<String, Value>) -> bool {
    for scope in [
        Some(obj),
        obj.get("step_update").and_then(Value::as_object),
        obj.get("part").and_then(Value::as_object),
        obj.get("result").and_then(Value::as_object),
    ]
    .into_iter()
    .flatten()
    {
        for key in ["response", "text", "result", "output", "message"] {
            if scope
                .get(key)
                .and_then(Value::as_str)
                .is_some_and(|s| !s.trim().is_empty())
            {
                return true;
            }
        }
    }
    false
}

fn has_denied_actions(obj: &Map<String, Value>, sub: Option<&Map<String, Value>>) -> bool {
    [Some(obj), sub].into_iter().flatten().any(|scope| {
        scope
            .get("denied_actions")
            .and_then(Value::as_array)
            .is_some_and(|a| !a.is_empty())
    })
}

fn has_any_error_indicator(obj: &Map<String, Value>, sub: Option<&Map<String, Value>>) -> bool {
    for scope in [Some(obj), sub].into_iter().flatten() {
        if scope.get("is_error") == Some(&Value::Bool(true)) || has_object_error_field(scope) {
            return true;
        }
        if scope.get("finished_reason").and_then(Value::as_str) == Some("print_timeout") {
            return true;
        }
        for key in [
            "status",
            "subtype",
            "state",
            "stop_reason",
            "finished_reason",
        ] {
            if let Some(val) = scope.get(key).and_then(Value::as_str) {
                let lower = val.to_ascii_lowercase();
                if matches!(
                    lower.as_str(),
                    "error"
                        | "error_during_execution"
                        | "failed"
                        | "failure"
                        | "cancelled"
                        | "canceled"
                        | "aborted"
                ) {
                    return true;
                }
            }
        }
    }
    false
}

fn has_truncation_stop_reason(obj: &Map<String, Value>, sub: Option<&Map<String, Value>>) -> bool {
    for scope in [Some(obj), sub].into_iter().flatten() {
        for key in ["stop_reason", "finished_reason", "reason"] {
            if let Some(val) = scope.get(key).and_then(Value::as_str) {
                let lower = val.to_ascii_lowercase();
                if matches!(lower.as_str(), "max_tokens" | "length" | "truncated") {
                    return true;
                }
            }
        }
    }
    false
}
