use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::antigravity_models::antigravity_catalog_effort;
use crate::fs_safety::sanitize_and_redact;
use crate::spec::{ResolvedModelTarget, WorkerProvider};

/// Maximum byte length of a redacted event summary or diagnostic snippet.
pub const MAX_DIAGNOSTIC_SUMMARY_BYTES: usize = 256;

/// Maximum number of malformed line diagnostics retained per run.
pub const MAX_MALFORMED_DIAGNOSTICS: usize = 32;

/// Maximum number of provider error records retained per run.
pub const MAX_PROVIDER_ERROR_RECORDS: usize = 32;

/// Maximum number of blocked event records retained per run.
pub const MAX_BLOCKED_RECORDS: usize = 32;

/// Verification state comparing a requested field against provider stream evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum FieldEvidence {
    /// Stream emitted explicit evidence matching the requested value.
    VerifiedMatch {
        /// Verified matching identifier.
        value: String,
    },
    /// Stream emitted a value that differed from the requested value or conflicted across events.
    Mismatch {
        /// Requested value from the worker spec, if any.
        requested: Option<String>,
        /// Observed value in the stream.
        observed: String,
        /// Explanation of the mismatch or intra-stream conflict.
        detail: String,
    },
    /// A value was requested in the worker spec, but the stream never emitted evidence for it.
    Missing {
        /// Requested value for which no stream evidence was observed.
        requested: String,
    },
    /// No specific value was requested by the spec.
    NotRequested {
        /// Value observed in the stream, if any.
        observed: Option<String>,
    },
}

impl FieldEvidence {
    /// Returns `true` if this field is satisfied (either `VerifiedMatch` or `NotRequested`).
    pub fn is_satisfied(&self) -> bool {
        matches!(self, Self::VerifiedMatch { .. } | Self::NotRequested { .. })
    }
}

/// Classification of why a worker run blocked on external authority or user interaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockedKind {
    /// Worker requested tool or edit permission/approval that was not granted.
    PermissionRequired,
    /// Worker requested interactive user input or a question response.
    UserInputRequired,
}

/// Structured evidence of a permission or user-input block emitted by the provider stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockedEvidence {
    /// Block category.
    pub kind: BlockedKind,
    /// 1-based stdout line number where the block event occurred.
    pub line_number: usize,
    /// Associated tool name, if reported by the provider.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    /// Bounded, secret-redacted description of the block.
    pub summary: String,
}

/// Structured evidence of an error event emitted by the provider NDJSON stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderErrorEvidence {
    /// 1-based stdout line number where the error event occurred.
    pub line_number: usize,
    /// Optional provider error code or error type name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Bounded, secret-redacted error message.
    pub message: String,
}

/// Diagnostic record for a malformed (non-JSON or non-object) stdout line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MalformedLineDiagnostic {
    /// 1-based stdout line number.
    pub line_number: usize,
    /// Parser error message.
    pub error: String,
    /// Bounded, secret-redacted snippet of the malformed line.
    pub redacted_snippet: String,
}

/// Normalized category for a parsed provider NDJSON event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NormalizedEventKind {
    /// Session or step initialization event (`init`, `system:init`, `step_start`, `session`).
    Init,
    /// Assistant message or progress output (`assistant`, `message`, `text`).
    Progress,
    /// Tool invocation or tool execution event (`tool_use`, `tool_result`, `tool`).
    Tool,
    /// Permission or user-input required block event.
    Blocked,
    /// Provider error event.
    Error,
    /// Terminal completion/result event (`result:success`, `step_finish:stop`).
    Result,
    /// Advisory attention generated only by the supervisor, never by provider event parsing.
    SupervisorAttention,
    /// Other valid JSON object event.
    Other,
}

/// Bounded, normalized representation of a single provider NDJSON event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedWorkerEvent {
    /// 1-based provider stdout line number; `0` is reserved for supervisor-generated events.
    pub line_number: usize,
    /// Normalized event kind.
    pub kind: NormalizedEventKind,
    /// Bounded raw event type/subtype label.
    pub raw_type: String,
    /// Session identifier observed on this event, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Base model identifier observed on this event, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Effort or variant identifier observed on this event, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    /// Tool name observed on this event, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    /// Bounded, sanitized, secret-redacted summary.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

/// Provenance of observed effort/variant evidence in a provider NDJSON stream.
///
/// Both variants represent CLI-reported selection metadata from the worker stream, **not**
/// independent cloud or reasoning-budget attestation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffortEvidenceSource {
    /// Effort or variant was explicitly emitted in stream metadata (a dedicated `effort`,
    /// `variant`, `reasoning_effort`, or `thinking_effort` field, or an inline `#variant` tag).
    DirectStreamMetadata,
    /// Effort was derived from an allowlisted concrete model identifier in a structured
    /// Antigravity `event: "init"` (`init.model`) event under the versioned Antigravity 1.4.12
    /// catalog contract. This records CLI-reported concrete model variant selection, **not**
    /// independent reasoning-budget attestation.
    AntigravityCatalogModelVariant,
}

/// Genuine token usage telemetry reported by a provider terminal event (such as Codex CLI `turn.completed`).
///
/// Never fabricated when a provider stream omits usage metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamTokenUsage {
    /// Input tokens consumed in the turn.
    pub input_tokens: u64,
    /// Cached input tokens read during the turn, when reported by the provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_input_tokens: Option<u64>,
    /// Cache-write input tokens recorded during the turn, when reported by the provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_input_tokens: Option<u64>,
    /// Output tokens generated in the turn.
    pub output_tokens: u64,
    /// Reasoning output tokens generated in the turn, when reported by the provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_output_tokens: Option<u64>,
}

/// Bounded single-turn lifecycle verification state for [`WorkerProvider::CodexCli`] `codex exec --json` streams.
///
/// Tracks the required `thread.started -> turn.started -> [item.*] -> turn.completed` lifecycle
/// in O(1) state without relying on `retained_events` (which is bounded by `max_events` and may
/// drop intermediate events).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CodexTurnLifecycleEvidence {
    /// Count of `thread.started` events observed in the stream.
    pub thread_started_count: usize,
    /// Count of `turn.started` events observed in the stream.
    pub turn_started_count: usize,
    /// Count of `turn.completed` events observed in the stream.
    pub turn_completed_count: usize,
    /// Count of `item.*` (`item.started`, `item.updated`, `item.completed`) events observed in the stream.
    pub item_events_count: usize,
    /// Count of events observed after the first terminal `turn.completed` event.
    pub trailing_events_after_completion_count: usize,
    /// First lifecycle framing, ordering, or truncation violation observed in the stream, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub violation: Option<String>,
}

impl CodexTurnLifecycleEvidence {
    /// Returns `true` if the stream observed an exact single `thread.started -> turn.started -> turn.completed`
    /// sequence with zero framing, ordering, or truncation violations and zero trailing events after
    /// `turn.completed`.
    #[must_use]
    pub fn is_valid_completed_turn(&self) -> bool {
        self.thread_started_count == 1
            && self.turn_started_count == 1
            && self.turn_completed_count == 1
            && self.trailing_events_after_completion_count == 0
            && self.violation.is_none()
    }
}

/// Aggregated evidence and normalized events parsed from a worker's stdout NDJSON stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamEvidenceSummary {
    /// Total non-empty lines processed on stdout.
    pub total_lines: usize,
    /// Total valid JSON object events parsed.
    pub valid_json_events: usize,
    /// Total malformed lines encountered on stdout.
    pub malformed_lines_count: usize,
    /// Bounded list of malformed line diagnostics.
    pub malformed_diagnostics: Vec<MalformedLineDiagnostic>,
    /// Whether an initialization / session start event was observed.
    pub saw_init_event: bool,
    /// Whether an explicit non-error terminal completion/result event was observed.
    pub saw_result_event: bool,
    /// Verification of requested vs observed model.
    pub model_evidence: FieldEvidence,
    /// Verification of requested vs observed effort/variant.
    pub effort_evidence: FieldEvidence,
    /// Provenance of observed stream effort evidence, when effort was observed without conflict.
    ///
    /// Distinguishes direct stream effort/variant metadata from known Antigravity catalog model
    /// variant selection in structured `event: "init"` (`init.model`). This records CLI-reported
    /// variant selection, **not** independent reasoning-budget attestation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort_evidence_source: Option<EffortEvidenceSource>,
    /// Verification of requested vs observed session ID.
    pub session_evidence: FieldEvidence,
    /// First observed session ID in the stream, if any.
    pub observed_session_id: Option<String>,
    /// Genuine token usage reported on the terminal completion event, when emitted by the provider.
    ///
    /// Never fabricated when the provider stream omits usage metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_usage: Option<StreamTokenUsage>,
    /// Bounded single-turn lifecycle verification state for [`WorkerProvider::CodexCli`] streams.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codex_turn_lifecycle: Option<CodexTurnLifecycleEvidence>,
    /// Provider error events observed in the stream.
    pub provider_errors: Vec<ProviderErrorEvidence>,
    /// Count of non-fatal step/tool errors observed during intermediate steps (`step_update` / `tool_result`).
    #[serde(default)]
    pub recoverable_step_errors_count: usize,
    /// Permission or user-input block events observed in the stream.
    pub blocked_events: Vec<BlockedEvidence>,
    /// Bounded list of normalized events (`<= max_events`).
    pub retained_events: Vec<NormalizedWorkerEvent>,
    /// Number of valid events dropped from `retained_events` due to `max_events` budget.
    pub dropped_events_count: usize,
}

impl StreamEvidenceSummary {
    /// Returns `true` if [`Self::codex_turn_lifecycle`] is present and verified as a complete
    /// single-turn `thread.started -> turn.started -> turn.completed` sequence with no framing,
    /// ordering, truncation, or trailing-event violations.
    #[must_use]
    pub fn has_valid_codex_turn_lifecycle(&self) -> bool {
        self.codex_turn_lifecycle
            .as_ref()
            .is_some_and(CodexTurnLifecycleEvidence::is_valid_completed_turn)
    }
}

/// Incremental stateful parser for Antigravity, OpenCode v2, and Codex CLI NDJSON streams.
pub struct NdjsonStreamParser {
    provider: Option<WorkerProvider>,
    requested_model: ResolvedModelTarget,
    requested_session_id: Option<String>,
    max_events: usize,
    total_lines: usize,
    valid_json_events: usize,
    malformed_lines_count: usize,
    malformed_diagnostics: Vec<MalformedLineDiagnostic>,
    saw_init_event: bool,
    saw_result_event: bool,
    observed_model: Option<String>,
    model_conflict: Option<String>,
    observed_effort: Option<String>,
    observed_effort_source: Option<EffortEvidenceSource>,
    effort_conflict: Option<String>,
    observed_session_id: Option<String>,
    session_conflict: Option<String>,
    observed_usage: Option<StreamTokenUsage>,
    codex_turn_lifecycle: CodexTurnLifecycleEvidence,
    codex_in_turn: bool,
    saw_codex_event: bool,
    provider_errors: Vec<ProviderErrorEvidence>,
    recoverable_step_errors_count: usize,
    blocked_events: Vec<BlockedEvidence>,
    retained_events: Vec<NormalizedWorkerEvent>,
    dropped_events_count: usize,
}

impl NdjsonStreamParser {
    /// Creates a new provider-neutral, strict incremental NDJSON stream parser for the given
    /// expected model, effort, and session.
    ///
    /// This default constructor remains provider-neutral and requires direct stream effort/variant
    /// metadata (`effort`, `variant`, `reasoning_effort`, `thinking_effort`, or inline `#variant`)
    /// whenever an effort/variant is requested. Use [`NdjsonStreamParser::new_for_provider`] or
    /// [`NdjsonStreamParser::with_provider`] to enable provider-specific stream rules.
    pub fn new(
        requested_model: ResolvedModelTarget,
        requested_session_id: Option<String>,
        max_events: usize,
    ) -> Self {
        Self {
            provider: None,
            requested_model,
            requested_session_id,
            max_events,
            total_lines: 0,
            valid_json_events: 0,
            malformed_lines_count: 0,
            malformed_diagnostics: Vec::new(),
            saw_init_event: false,
            saw_result_event: false,
            observed_model: None,
            model_conflict: None,
            observed_effort: None,
            observed_effort_source: None,
            effort_conflict: None,
            observed_session_id: None,
            session_conflict: None,
            observed_usage: None,
            codex_turn_lifecycle: CodexTurnLifecycleEvidence::default(),
            codex_in_turn: false,
            saw_codex_event: false,
            provider_errors: Vec::new(),
            recoverable_step_errors_count: 0,
            blocked_events: Vec::new(),
            retained_events: Vec::new(),
            dropped_events_count: 0,
        }
    }

    /// Creates a new provider-aware incremental NDJSON stream parser for `provider`.
    ///
    /// When `provider` is [`WorkerProvider::Antigravity`], valid structured `event: "init"` events
    /// carrying a known Antigravity 1.4.12 catalog model identifier in `init.model` can supply
    /// [`EffortEvidenceSource::AntigravityCatalogModelVariant`] evidence (unless contradicted by
    /// explicit effort/variant metadata or later stream conflicts).
    pub fn new_for_provider(
        provider: WorkerProvider,
        requested_model: ResolvedModelTarget,
        requested_session_id: Option<String>,
        max_events: usize,
    ) -> Self {
        Self::new(requested_model, requested_session_id, max_events).with_provider(provider)
    }

    /// Configures the worker provider context for this stream parser.
    #[must_use]
    pub fn with_provider(mut self, provider: WorkerProvider) -> Self {
        self.provider = Some(provider);
        self
    }

    /// Processes a single complete line from stdout.
    pub fn ingest_line(&mut self, raw_line: &[u8]) {
        let _ = self.ingest_line_internal(raw_line, false);
    }

    /// Processes a single complete line from stdout and returns the sanitized, bounded
    /// [`NormalizedWorkerEvent`] when the line is a valid JSON object event.
    pub fn ingest_line_with_event(&mut self, raw_line: &[u8]) -> Option<NormalizedWorkerEvent> {
        self.ingest_line_internal(raw_line, true)
    }

    fn ingest_line_internal(
        &mut self,
        raw_line: &[u8],
        emit_live_event: bool,
    ) -> Option<NormalizedWorkerEvent> {
        let trimmed = trim_ascii_whitespace(raw_line);
        if trimmed.is_empty() {
            return None;
        }
        self.total_lines += 1;
        let line_number = self.total_lines;

        let line_str = match std::str::from_utf8(trimmed) {
            Ok(s) => s,
            Err(err) => {
                self.record_malformed(
                    line_number,
                    format!("invalid UTF-8 on stdout line: {err}"),
                    &String::from_utf8_lossy(trimmed),
                );
                return None;
            }
        };

        let value: Value = match serde_json::from_str(line_str) {
            Ok(val) => val,
            Err(err) => {
                self.record_malformed(
                    line_number,
                    format!("invalid JSON on stdout line: {err}"),
                    line_str,
                );
                return None;
            }
        };

        let Some(obj) = value.as_object() else {
            self.record_malformed(
                line_number,
                "stdout NDJSON line must be a JSON object".to_string(),
                line_str,
            );
            return None;
        };

        self.valid_json_events += 1;
        let event = self.process_json_object(line_number, obj);
        self.retain_event(event, emit_live_event)
    }

    fn record_malformed(&mut self, line_number: usize, error: String, raw_snippet: &str) {
        self.malformed_lines_count += 1;
        if self.malformed_diagnostics.len() < MAX_MALFORMED_DIAGNOSTICS {
            self.malformed_diagnostics.push(MalformedLineDiagnostic {
                line_number,
                error: sanitize_and_redact(&error, MAX_DIAGNOSTIC_SUMMARY_BYTES),
                redacted_snippet: sanitize_and_redact(raw_snippet, MAX_DIAGNOSTIC_SUMMARY_BYTES),
            });
        }
    }

    fn process_json_object(
        &mut self,
        line_number: usize,
        obj: &Map<String, Value>,
    ) -> NormalizedWorkerEvent {
        let event_type = obj
            .get("event")
            .or_else(|| obj.get("type"))
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let subtype = obj
            .get("subtype")
            .and_then(Value::as_str)
            .or_else(|| {
                obj.get("part")
                    .and_then(Value::as_object)
                    .and_then(|p| p.get("type"))
                    .and_then(Value::as_str)
            })
            .or_else(|| {
                obj.get("step_update")
                    .and_then(Value::as_object)
                    .and_then(|s| s.get("step_type").or_else(|| s.get("state")))
                    .and_then(Value::as_str)
            })
            .or_else(|| {
                obj.get("result")
                    .and_then(Value::as_object)
                    .and_then(|r| r.get("status").or_else(|| r.get("subtype")))
                    .and_then(Value::as_str)
            })
            .or_else(|| {
                obj.get("item")
                    .and_then(Value::as_object)
                    .and_then(|i| i.get("type"))
                    .and_then(Value::as_str)
            });

        let raw_type = match subtype {
            Some(sub) if !sub.is_empty() && sub != event_type => {
                sanitize_and_redact(&format!("{event_type}:{sub}"), 64)
            }
            _ => sanitize_and_redact(event_type, 64),
        };

        let session_id = extract_session_id(obj);
        if event_type == "thread.started"
            && obj
                .get("thread_id")
                .and_then(Value::as_str)
                .is_none_or(|id| id.trim().is_empty())
            && self.session_conflict.is_none()
        {
            self.session_conflict = Some(
                "Codex `thread.started` event is missing a valid non-empty string `thread_id`"
                    .to_string(),
            );
        }
        if let Some(tid_val) = obj.get("thread_id") {
            if tid_val.as_str().is_none_or(|s| s.trim().is_empty())
                && self.session_conflict.is_none()
            {
                self.session_conflict = Some(
                    "stream `thread_id` field must be a non-empty string when present".to_string(),
                );
            }
        }
        if let Some(ref sid) = session_id {
            match &self.observed_session_id {
                None => self.observed_session_id = Some(sid.clone()),
                Some(prev) if prev != sid && self.session_conflict.is_none() => {
                    self.session_conflict = Some(format!(
                        "conflicting session IDs observed in stream: `{prev}` vs `{sid}`"
                    ));
                }
                _ => {}
            }
        }

        let mut tool_name = extract_tool_name(obj);
        let event_is_tool = is_tool_event(obj, event_type, subtype, tool_name.is_some());

        let (event_model, mut event_effort, inline_model_conflict, inline_effort_conflict) =
            if event_is_tool {
                (None, None, None, None)
            } else {
                extract_model_and_effort(obj)
            };
        let mut event_effort_source = event_effort
            .as_ref()
            .map(|_| EffortEvidenceSource::DirectStreamMetadata);

        if self.model_conflict.is_none() {
            self.model_conflict = inline_model_conflict;
        }
        if self.effort_conflict.is_none() {
            self.effort_conflict = inline_effort_conflict;
        }
        if let Some(ref model) = event_model {
            match &self.observed_model {
                None => self.observed_model = Some(model.clone()),
                Some(prev) if prev != model && self.model_conflict.is_none() => {
                    self.model_conflict = Some(format!(
                        "conflicting models observed in stream: `{prev}` vs `{model}`"
                    ));
                }
                _ => {}
            }
        }

        if self.provider == Some(WorkerProvider::Antigravity) {
            if let (Some(ref model), Some(ref explicit_eff)) = (&event_model, &event_effort) {
                if let Some(catalog_effort) = antigravity_catalog_effort(model) {
                    if explicit_eff != catalog_effort && self.effort_conflict.is_none() {
                        self.effort_conflict = Some(format!(
                            "explicit stream effort/variant `{explicit_eff}` conflicts with Antigravity catalog model `{model}` encoded effort `{catalog_effort}`"
                        ));
                    }
                }
            }
            if let (Some(ref model), Some(ref prev_eff)) = (&event_model, &self.observed_effort) {
                if let Some(catalog_effort) = antigravity_catalog_effort(model) {
                    if prev_eff != catalog_effort && self.effort_conflict.is_none() {
                        self.effort_conflict = Some(format!(
                            "conflicting effort/variant values observed in stream: `{prev_eff}` vs `{catalog_effort}`"
                        ));
                    }
                }
            }
        }

        let is_codex_event = self.provider == Some(WorkerProvider::CodexCli)
            || matches!(
                event_type,
                "thread.started"
                    | "turn.started"
                    | "item.started"
                    | "item.updated"
                    | "item.completed"
                    | "turn.completed"
                    | "turn.failed"
            );
        if is_codex_event {
            self.saw_codex_event = true;
            self.observe_codex_lifecycle_event(obj, event_type, session_id.as_deref());
        }

        // 1. Check for permission or user-input block first (including `denied_actions`)
        // so blocked states are never misclassified as success.
        if let Some((blocked_kind, blocked_tool, blocked_summary)) =
            detect_blocked_event(obj, event_type, subtype)
        {
            self.record_observed_event_effort(event_effort.as_deref(), event_effort_source);
            if tool_name.is_none() {
                tool_name = blocked_tool;
            }
            if self.blocked_events.len() < MAX_BLOCKED_RECORDS {
                self.blocked_events.push(BlockedEvidence {
                    kind: blocked_kind,
                    line_number,
                    tool_name: tool_name.clone(),
                    summary: blocked_summary.clone(),
                });
            }
            return NormalizedWorkerEvent {
                line_number,
                kind: NormalizedEventKind::Blocked,
                raw_type,
                session_id,
                model: event_model,
                effort: event_effort,
                tool_name,
                summary: Some(blocked_summary),
            };
        }

        // 2. Check for non-fatal recoverable step/tool errors (`step_update`, `tool_result`,
        // or provider-scoped Codex `item.completed` `command_execution` nonzero exits)
        // before terminal provider error detection so a recoverable step tool failure
        // does not poison a subsequent valid `result` event.
        if is_recoverable_step_error(obj, event_type, subtype)
            || is_recoverable_codex_command_execution_error(self.provider, obj, event_type, subtype)
        {
            self.recoverable_step_errors_count += 1;
        } else if let Some((err_code, err_msg)) = detect_error_event(obj, event_type, subtype) {
            self.record_observed_event_effort(event_effort.as_deref(), event_effort_source);
            if self.provider_errors.len() < MAX_PROVIDER_ERROR_RECORDS {
                self.provider_errors.push(ProviderErrorEvidence {
                    line_number,
                    code: err_code,
                    message: err_msg.clone(),
                });
            }
            return NormalizedWorkerEvent {
                line_number,
                kind: NormalizedEventKind::Error,
                raw_type,
                session_id,
                model: event_model,
                effort: event_effort,
                tool_name,
                summary: Some(err_msg),
            };
        } else if self.provider == Some(WorkerProvider::Antigravity)
            && !event_is_tool
            && is_structured_antigravity_init_event(obj, subtype)
            && self.model_conflict.is_none()
        {
            if let Some(init_model_raw) = obj
                .get("init")
                .and_then(Value::as_object)
                .and_then(|init_obj| init_obj.get("model"))
                .and_then(Value::as_str)
            {
                if let Some(catalog_effort) = antigravity_catalog_effort(init_model_raw) {
                    match &event_effort {
                        Some(explicit_eff) => {
                            if explicit_eff != catalog_effort && self.effort_conflict.is_none() {
                                self.effort_conflict = Some(format!(
                                    "explicit stream effort/variant `{explicit_eff}` conflicts with Antigravity init catalog model `{init_model_raw}` encoded effort `{catalog_effort}`"
                                ));
                            }
                        }
                        None => {
                            event_effort = Some(catalog_effort.to_string());
                            event_effort_source =
                                Some(EffortEvidenceSource::AntigravityCatalogModelVariant);
                        }
                    }
                }
            }
        }

        self.record_observed_event_effort(event_effort.as_deref(), event_effort_source);

        // 3. Classify Init, Result, Tool, Progress, or Other.
        let is_init = if is_codex_event {
            event_type == "thread.started"
                && session_id.is_some()
                && self.codex_turn_lifecycle.thread_started_count == 1
                && self.codex_turn_lifecycle.turn_started_count == 0
                && self.codex_turn_lifecycle.turn_completed_count == 0
                && self.codex_turn_lifecycle.item_events_count == 0
        } else {
            is_init_event(obj, event_type, subtype)
        };
        if is_init {
            self.saw_init_event = true;
        }

        let mut is_result = if is_codex_event {
            event_type == "turn.completed" && is_result_event(obj, event_type, subtype)
        } else {
            is_result_event(obj, event_type, subtype)
        };
        if event_type.eq_ignore_ascii_case("turn.completed") {
            match extract_genuine_turn_usage(obj) {
                Ok(Some(usage)) => {
                    if is_result && self.codex_turn_lifecycle.is_valid_completed_turn() {
                        self.observed_usage = Some(usage);
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    is_result = false;
                    self.record_codex_lifecycle_violation(format!(
                        "malformed `usage` on `turn.completed`: {err}"
                    ));
                    self.record_malformed(
                        line_number,
                        format!("malformed `usage` on `turn.completed`: {err}"),
                        &raw_type,
                    );
                }
            }
            if !self.codex_turn_lifecycle.is_valid_completed_turn() {
                is_result = false;
            }
        }
        if is_result {
            self.saw_result_event = true;
        }

        let step_is_tool = subtype.is_some_and(|s| s.to_ascii_lowercase().contains("tool"));

        let kind = if is_result {
            NormalizedEventKind::Result
        } else if is_init {
            NormalizedEventKind::Init
        } else if event_is_tool
            || tool_name.is_some()
            || step_is_tool
            || matches!(event_type, "tool" | "tool_use" | "tool_result")
            || matches!(subtype, Some("tool" | "tool_use" | "tool_result"))
        {
            NormalizedEventKind::Tool
        } else if matches!(
            event_type,
            "assistant" | "message" | "text" | "content" | "step_update"
        ) || matches!(
            subtype,
            Some("text" | "message" | "agent_response" | "agent_message" | "reasoning")
        ) {
            NormalizedEventKind::Progress
        } else {
            NormalizedEventKind::Other
        };

        let summary = extract_summary(obj);

        NormalizedWorkerEvent {
            line_number,
            kind,
            raw_type,
            session_id,
            model: event_model,
            effort: event_effort,
            tool_name,
            summary,
        }
    }

    fn record_observed_event_effort(
        &mut self,
        event_effort: Option<&str>,
        event_effort_source: Option<EffortEvidenceSource>,
    ) {
        let Some(effort) = event_effort else {
            return;
        };
        match &self.observed_effort {
            None => {
                self.observed_effort = Some(effort.to_string());
                self.observed_effort_source = event_effort_source;
            }
            Some(prev) if prev != effort => {
                if self.effort_conflict.is_none() {
                    self.effort_conflict = Some(format!(
                        "conflicting effort/variant values observed in stream: `{prev}` vs `{effort}`"
                    ));
                }
            }
            Some(_) => {
                if event_effort_source == Some(EffortEvidenceSource::DirectStreamMetadata) {
                    self.observed_effort_source = Some(EffortEvidenceSource::DirectStreamMetadata);
                }
            }
        }
    }

    fn record_codex_lifecycle_violation(&mut self, detail: impl Into<String>) {
        if self.codex_turn_lifecycle.violation.is_none() {
            self.codex_turn_lifecycle.violation = Some(sanitize_and_redact(
                &detail.into(),
                MAX_DIAGNOSTIC_SUMMARY_BYTES,
            ));
        }
    }

    fn observe_codex_lifecycle_event(
        &mut self,
        obj: &Map<String, Value>,
        event_type: &str,
        session_id: Option<&str>,
    ) {
        for key in ["thread_id", "session_id", "sessionID", "conversation_id"] {
            if let Some(value) = obj.get(key) {
                let valid_id = value.as_str().filter(|id| {
                    !id.trim().is_empty() && id.len() <= 128 && !id.chars().any(char::is_control)
                });
                if valid_id.is_none()
                    || valid_id.map(str::trim) != session_id
                    || valid_id.map(str::trim) != self.observed_session_id.as_deref()
                {
                    self.session_conflict = Some(
                        "Codex stream contains invalid or conflicting identity fields".to_string(),
                    );
                    self.record_codex_lifecycle_violation(
                        "Codex stream contains invalid or conflicting identity fields",
                    );
                }
            }
        }
        if let (Some(ty), Some(ev)) = (
            obj.get("type").and_then(Value::as_str),
            obj.get("event").and_then(Value::as_str),
        ) {
            if ty != ev {
                self.record_codex_lifecycle_violation(format!(
                    "conflicting Codex `type` (`{ty}`) and `event` (`{ev}`) fields"
                ));
            }
        }

        if self.codex_turn_lifecycle.turn_completed_count > 0 {
            self.codex_turn_lifecycle
                .trailing_events_after_completion_count = self
                .codex_turn_lifecycle
                .trailing_events_after_completion_count
                .saturating_add(1);
            match event_type {
                "thread.started" => {
                    self.codex_turn_lifecycle.thread_started_count = self
                        .codex_turn_lifecycle
                        .thread_started_count
                        .saturating_add(1);
                    if self.session_conflict.is_none() {
                        self.session_conflict = Some(
                            "Codex `thread.started` event occurred after terminal `turn.completed`"
                                .to_string(),
                        );
                    }
                }
                "turn.started" => {
                    self.codex_turn_lifecycle.turn_started_count = self
                        .codex_turn_lifecycle
                        .turn_started_count
                        .saturating_add(1);
                }
                "turn.completed" => {
                    self.codex_turn_lifecycle.turn_completed_count = self
                        .codex_turn_lifecycle
                        .turn_completed_count
                        .saturating_add(1);
                }
                "item.started" | "item.updated" | "item.completed" => {
                    self.codex_turn_lifecycle.item_events_count = self
                        .codex_turn_lifecycle
                        .item_events_count
                        .saturating_add(1);
                }
                _ => {}
            }
            self.record_codex_lifecycle_violation(format!(
                "Codex stream emitted `{event_type}` after terminal `turn.completed`"
            ));
            return;
        }

        match event_type {
            "thread.started" => {
                self.codex_turn_lifecycle.thread_started_count = self
                    .codex_turn_lifecycle
                    .thread_started_count
                    .saturating_add(1);
                if self.codex_turn_lifecycle.thread_started_count > 1 {
                    if self.session_conflict.is_none() {
                        self.session_conflict = Some(
                            "duplicate Codex `thread.started` event observed in stream".to_string(),
                        );
                    }
                    self.record_codex_lifecycle_violation(
                        "Codex stream emitted duplicate `thread.started` events",
                    );
                }
                if self.codex_turn_lifecycle.turn_started_count > 0
                    || self.codex_turn_lifecycle.item_events_count > 0
                {
                    if self.session_conflict.is_none() {
                        self.session_conflict = Some(
                            "Codex `thread.started` event occurred after turn or item events"
                                .to_string(),
                        );
                    }
                    self.record_codex_lifecycle_violation(
                        "Codex `thread.started` event occurred out of order after turn or item events",
                    );
                }
                if session_id.is_none() {
                    self.record_codex_lifecycle_violation(
                        "Codex `thread.started` event is missing a valid `thread_id`",
                    );
                }
            }
            "turn.started" => {
                self.codex_turn_lifecycle.turn_started_count = self
                    .codex_turn_lifecycle
                    .turn_started_count
                    .saturating_add(1);
                if self.codex_turn_lifecycle.thread_started_count == 0 {
                    if self.session_conflict.is_none() {
                        self.session_conflict = Some(
                            "Codex `turn.started` event was not preceded by `thread.started`"
                                .to_string(),
                        );
                    }
                    self.record_codex_lifecycle_violation(
                        "Codex `turn.started` event was not preceded by `thread.started`",
                    );
                } else if self.codex_turn_lifecycle.turn_started_count > 1 || self.codex_in_turn {
                    self.record_codex_lifecycle_violation(
                        "Codex stream emitted duplicate or nested `turn.started` event",
                    );
                } else {
                    self.codex_in_turn = true;
                }
            }
            "item.started" | "item.updated" | "item.completed" => {
                self.codex_turn_lifecycle.item_events_count = self
                    .codex_turn_lifecycle
                    .item_events_count
                    .saturating_add(1);
                if self.codex_turn_lifecycle.thread_started_count == 0 {
                    if self.session_conflict.is_none() {
                        self.session_conflict = Some(format!(
                            "Codex `{event_type}` event occurred before `thread.started`"
                        ));
                    }
                    self.record_codex_lifecycle_violation(format!(
                        "Codex `{event_type}` event occurred before `thread.started`"
                    ));
                } else if self.codex_turn_lifecycle.turn_started_count != 1 || !self.codex_in_turn {
                    self.record_codex_lifecycle_violation(format!(
                        "Codex `{event_type}` event occurred outside an active `turn.started` frame"
                    ));
                }
                let Some(item_obj) = obj.get("item").and_then(Value::as_object) else {
                    self.record_codex_lifecycle_violation(format!(
                        "Codex `{event_type}` event is missing `item` object"
                    ));
                    return;
                };
                if item_obj
                    .get("type")
                    .and_then(Value::as_str)
                    .is_none_or(|s| s.trim().is_empty())
                {
                    self.record_codex_lifecycle_violation(format!(
                        "Codex `{event_type}` event is missing non-empty string `item.type`"
                    ));
                }
                if item_obj
                    .get("id")
                    .and_then(Value::as_str)
                    .is_none_or(|s| s.trim().is_empty())
                {
                    self.record_codex_lifecycle_violation(
                        "Codex item event is missing non-empty string `item.id`",
                    );
                }
                if has_codex_partial_or_truncation_markers(obj, Some(item_obj)) {
                    self.record_codex_lifecycle_violation(format!(
                        "Codex `{event_type}` event carries partial, delta, or truncated markers"
                    ));
                }
            }
            "turn.completed" => {
                self.codex_turn_lifecycle.turn_completed_count = self
                    .codex_turn_lifecycle
                    .turn_completed_count
                    .saturating_add(1);
                if self.codex_turn_lifecycle.thread_started_count == 0 {
                    if self.session_conflict.is_none() {
                        self.session_conflict = Some(
                            "Codex `turn.completed` event was not preceded by `thread.started`"
                                .to_string(),
                        );
                    }
                    self.record_codex_lifecycle_violation(
                        "Codex `turn.completed` event was not preceded by `thread.started`",
                    );
                } else if self.codex_turn_lifecycle.turn_started_count != 1 || !self.codex_in_turn {
                    self.record_codex_lifecycle_violation(
                        "Codex `turn.completed` event was not preceded by an active `turn.started`",
                    );
                }
                if has_codex_partial_or_truncation_markers(obj, None) {
                    self.record_codex_lifecycle_violation(
                        "Codex `turn.completed` event reported partial, delta, or truncated output",
                    );
                }
                if obj.get("status").is_some_and(|value| !value.is_string()) {
                    self.record_codex_lifecycle_violation(
                        "Codex terminal status must be a string when present",
                    );
                }
                self.codex_in_turn = false;
            }
            "turn.failed" | "error" => {
                self.codex_in_turn = false;
                self.record_codex_lifecycle_violation(format!(
                    "Codex stream emitted error event `{event_type}`"
                ));
            }
            other => {
                if self.provider == Some(WorkerProvider::CodexCli)
                    && detect_blocked_event(obj, event_type, None).is_none()
                    && detect_error_event(obj, event_type, None).is_none()
                {
                    self.record_codex_lifecycle_violation(format!(
                        "unsupported Codex stream event type `{other}`"
                    ));
                }
            }
        }
    }

    fn retain_event(
        &mut self,
        event: NormalizedWorkerEvent,
        emit_live_event: bool,
    ) -> Option<NormalizedWorkerEvent> {
        if self.retained_events.len() < self.max_events {
            if emit_live_event {
                self.retained_events.push(event.clone());
                Some(event)
            } else {
                self.retained_events.push(event);
                None
            }
        } else {
            self.dropped_events_count += 1;
            emit_live_event.then_some(event)
        }
    }

    /// Finalizes stream parsing and computes requested-vs-observed evidence verification.
    pub fn finalize(mut self) -> StreamEvidenceSummary {
        let is_codex_stream =
            self.provider == Some(WorkerProvider::CodexCli) || self.saw_codex_event;
        if is_codex_stream {
            if self.malformed_lines_count > 0
                || !self.provider_errors.is_empty()
                || !self.blocked_events.is_empty()
                || self.session_conflict.is_some()
            {
                self.record_codex_lifecycle_violation(
                    "Codex stream contains malformed, error, blocked, or conflicting identity evidence",
                );
            }
            if !self.saw_result_event {
                self.record_codex_lifecycle_violation(
                    "Codex stream lacks valid terminal completion evidence",
                );
            }
            if self.codex_turn_lifecycle.thread_started_count != 1 {
                self.saw_init_event = false;
                if self.total_lines > 0 && self.session_conflict.is_none() {
                    self.session_conflict = Some(
                        "Codex stream did not emit an exact single `thread.started` event"
                            .to_string(),
                    );
                }
                self.record_codex_lifecycle_violation(
                    "Codex stream did not emit an exact single `thread.started` event",
                );
            }
            if self.codex_turn_lifecycle.turn_started_count == 0 {
                self.record_codex_lifecycle_violation(
                    "Codex stream did not emit a `turn.started` event",
                );
            }
            if self.codex_turn_lifecycle.turn_completed_count == 0 {
                self.record_codex_lifecycle_violation(
                    "Codex stream exited without a terminal `turn.completed` event",
                );
            }
            if !self.codex_turn_lifecycle.is_valid_completed_turn() {
                self.saw_result_event = false;
                self.observed_usage = None;
            }
        }

        if self.observed_effort_source == Some(EffortEvidenceSource::AntigravityCatalogModelVariant)
            && self.model_conflict.is_some()
            && self.effort_conflict.is_none()
        {
            self.effort_conflict = Some(
                "cannot verify Antigravity catalog model effort when stream reported conflicting models"
                    .to_string(),
            );
        }

        let model_evidence = evaluate_model_evidence(
            &self.requested_model,
            self.observed_model.as_deref(),
            self.model_conflict.as_deref(),
        );
        let effort_evidence = evaluate_effort_evidence(
            self.requested_model.effort.as_deref(),
            self.observed_effort.as_deref(),
            self.effort_conflict.as_deref(),
        );
        let effort_evidence_source = if self.effort_conflict.is_none() {
            self.observed_effort_source
        } else {
            None
        };
        let session_evidence = evaluate_session_evidence(
            self.requested_session_id.as_deref(),
            self.observed_session_id.as_deref(),
            self.session_conflict.as_deref(),
        );

        StreamEvidenceSummary {
            total_lines: self.total_lines,
            valid_json_events: self.valid_json_events,
            malformed_lines_count: self.malformed_lines_count,
            malformed_diagnostics: self.malformed_diagnostics,
            saw_init_event: self.saw_init_event,
            saw_result_event: self.saw_result_event,
            model_evidence,
            effort_evidence,
            effort_evidence_source,
            session_evidence,
            observed_session_id: self.observed_session_id,
            observed_usage: self.observed_usage,
            codex_turn_lifecycle: is_codex_stream.then_some(self.codex_turn_lifecycle),
            provider_errors: self.provider_errors,
            recoverable_step_errors_count: self.recoverable_step_errors_count,
            blocked_events: self.blocked_events,
            retained_events: self.retained_events,
            dropped_events_count: self.dropped_events_count,
        }
    }
}

fn evaluate_model_evidence(
    requested: &ResolvedModelTarget,
    observed: Option<&str>,
    conflict: Option<&str>,
) -> FieldEvidence {
    if let Some(conflict_msg) = conflict {
        return FieldEvidence::Mismatch {
            requested: Some(requested.base_model.clone()),
            observed: observed.unwrap_or("conflicting").to_string(),
            detail: conflict_msg.to_string(),
        };
    }

    match observed {
        None => FieldEvidence::Missing {
            requested: requested.base_model.clone(),
        },
        Some(obs) => {
            if obs == requested.base_model || obs == requested.cli_model_arg {
                FieldEvidence::VerifiedMatch {
                    value: requested.base_model.clone(),
                }
            } else {
                FieldEvidence::Mismatch {
                    requested: Some(requested.base_model.clone()),
                    observed: obs.to_string(),
                    detail: format!(
                        "requested model `{}` but provider stream reported `{obs}`",
                        requested.base_model
                    ),
                }
            }
        }
    }
}

fn evaluate_effort_evidence(
    requested_effort: Option<&str>,
    observed_effort: Option<&str>,
    conflict: Option<&str>,
) -> FieldEvidence {
    if let Some(conflict_msg) = conflict {
        return FieldEvidence::Mismatch {
            requested: requested_effort.map(str::to_string),
            observed: observed_effort.unwrap_or("conflicting").to_string(),
            detail: conflict_msg.to_string(),
        };
    }

    match (requested_effort, observed_effort) {
        (Some(req), Some(obs)) => {
            if req == obs {
                FieldEvidence::VerifiedMatch {
                    value: obs.to_string(),
                }
            } else {
                FieldEvidence::Mismatch {
                    requested: Some(req.to_string()),
                    observed: obs.to_string(),
                    detail: format!(
                        "requested effort/variant `{req}` but provider stream reported `{obs}`"
                    ),
                }
            }
        }
        (Some(req), None) => FieldEvidence::Missing {
            requested: req.to_string(),
        },
        (None, obs) => FieldEvidence::NotRequested {
            observed: obs.map(str::to_string),
        },
    }
}

fn evaluate_session_evidence(
    requested_session: Option<&str>,
    observed_session: Option<&str>,
    conflict: Option<&str>,
) -> FieldEvidence {
    if let Some(conflict_msg) = conflict {
        return FieldEvidence::Mismatch {
            requested: requested_session.map(str::to_string),
            observed: observed_session.unwrap_or("conflicting").to_string(),
            detail: conflict_msg.to_string(),
        };
    }

    match (requested_session, observed_session) {
        (Some(req), Some(obs)) => {
            if req == obs {
                FieldEvidence::VerifiedMatch {
                    value: obs.to_string(),
                }
            } else {
                FieldEvidence::Mismatch {
                    requested: Some(req.to_string()),
                    observed: obs.to_string(),
                    detail: format!(
                        "requested session `{req}` but provider stream reported `{obs}`"
                    ),
                }
            }
        }
        (Some(req), None) => FieldEvidence::Missing {
            requested: req.to_string(),
        },
        (None, obs) => FieldEvidence::NotRequested {
            observed: obs.map(str::to_string),
        },
    }
}

fn extract_session_from_sub(sub: &Map<String, Value>) -> Option<&str> {
    sub.get("conversation_id")
        .or_else(|| sub.get("session_id"))
        .or_else(|| sub.get("sessionID"))
        .or_else(|| sub.get("thread_id"))
        .and_then(Value::as_str)
}

fn extract_session_id(obj: &Map<String, Value>) -> Option<String> {
    let candidates = [
        obj.get("conversation_id").and_then(Value::as_str),
        obj.get("session_id").and_then(Value::as_str),
        obj.get("sessionID").and_then(Value::as_str),
        obj.get("thread_id").and_then(Value::as_str),
        obj.get("init")
            .and_then(Value::as_object)
            .and_then(extract_session_from_sub),
        obj.get("step_update")
            .and_then(Value::as_object)
            .and_then(extract_session_from_sub),
        obj.get("result")
            .and_then(Value::as_object)
            .and_then(extract_session_from_sub),
        obj.get("part")
            .and_then(Value::as_object)
            .and_then(extract_session_from_sub),
        obj.get("item")
            .and_then(Value::as_object)
            .and_then(extract_session_from_sub),
        obj.get("metadata")
            .and_then(Value::as_object)
            .and_then(extract_session_from_sub),
        obj.get("session")
            .and_then(Value::as_object)
            .and_then(|s| s.get("id"))
            .and_then(Value::as_str),
    ];

    for candidate in candidates.into_iter().flatten() {
        let trimmed = candidate.trim();
        if !trimmed.is_empty() {
            return Some(sanitize_and_redact(trimmed, 128));
        }
    }
    None
}

fn is_tool_event(
    obj: &Map<String, Value>,
    event_type: &str,
    subtype: Option<&str>,
    has_tool_name: bool,
) -> bool {
    let et = event_type.to_ascii_lowercase();
    let st = subtype.unwrap_or("").to_ascii_lowercase();
    if matches!(
        et.as_str(),
        "tool" | "tool_use" | "tool_result" | "tool_call"
    ) || matches!(
        st.as_str(),
        "tool"
            | "tool_use"
            | "tool_result"
            | "tool_call"
            | "command_execution"
            | "file_change"
            | "mcp_tool_call"
            | "web_search"
    ) || obj.contains_key("tool_use")
        || obj.contains_key("tool_result")
    {
        return true;
    }

    !matches!(
        et.as_str(),
        "init"
            | "result"
            | "system"
            | "step_start"
            | "step_finish"
            | "session"
            | "thread.started"
            | "turn.started"
            | "turn.completed"
    ) && (has_tool_name || st.contains("tool"))
}

fn is_structured_antigravity_init_event(obj: &Map<String, Value>, subtype: Option<&str>) -> bool {
    if obj.get("event").and_then(Value::as_str) != Some("init") {
        return false;
    }
    if obj
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|t| t != "init")
    {
        return false;
    }
    if subtype.is_some_and(|s| !s.is_empty() && s != "init") {
        return false;
    }
    let Some(init_obj) = obj.get("init").and_then(Value::as_object) else {
        return false;
    };
    !has_object_error_field(init_obj) && init_obj.get("is_error") != Some(&Value::Bool(true))
}

fn extract_model_and_effort(
    obj: &Map<String, Value>,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    let model_sources = [
        obj.get("model").and_then(Value::as_str).map(str::to_string),
        obj.get("init")
            .and_then(Value::as_object)
            .and_then(|i| i.get("model"))
            .and_then(Value::as_str)
            .map(str::to_string),
        obj.get("result")
            .and_then(Value::as_object)
            .and_then(|r| r.get("model"))
            .and_then(Value::as_str)
            .map(str::to_string),
        obj.get("step_update")
            .and_then(Value::as_object)
            .and_then(|s| s.get("model"))
            .and_then(Value::as_str)
            .map(str::to_string),
        obj.get("message")
            .and_then(Value::as_object)
            .and_then(|m| m.get("model"))
            .and_then(Value::as_str)
            .map(str::to_string),
        obj.get("part")
            .and_then(Value::as_object)
            .and_then(|p| p.get("model"))
            .and_then(Value::as_str)
            .map(str::to_string),
        obj.get("metadata")
            .and_then(Value::as_object)
            .and_then(|m| m.get("model"))
            .and_then(Value::as_str)
            .map(str::to_string),
        extract_provider_plus_model_id(obj),
        obj.get("init")
            .and_then(Value::as_object)
            .and_then(extract_provider_plus_model_id),
        obj.get("result")
            .and_then(Value::as_object)
            .and_then(extract_provider_plus_model_id),
        obj.get("part")
            .and_then(Value::as_object)
            .and_then(extract_provider_plus_model_id),
        obj.get("message")
            .and_then(Value::as_object)
            .and_then(extract_provider_plus_model_id),
        obj.get("model")
            .and_then(Value::as_object)
            .and_then(extract_provider_plus_model_id),
        obj.get("part")
            .and_then(Value::as_object)
            .and_then(|p| p.get("model"))
            .and_then(Value::as_object)
            .and_then(extract_provider_plus_model_id),
        obj.get("result")
            .and_then(Value::as_object)
            .and_then(|r| r.get("model"))
            .and_then(Value::as_object)
            .and_then(extract_provider_plus_model_id),
        obj.get("init")
            .and_then(Value::as_object)
            .and_then(|i| i.get("model"))
            .and_then(Value::as_object)
            .and_then(extract_provider_plus_model_id),
        obj.get("message")
            .and_then(Value::as_object)
            .and_then(|m| m.get("model"))
            .and_then(Value::as_object)
            .and_then(extract_provider_plus_model_id),
    ];

    let mut base_model: Option<String> = None;
    let mut inline_effort: Option<String> = None;
    let mut model_conflict: Option<String> = None;
    let mut effort_conflict: Option<String> = None;

    for raw_m in model_sources.into_iter().flatten() {
        let trimmed = raw_m.trim();
        if trimmed.is_empty() {
            continue;
        }
        let (cand_base, cand_inline) = match trimmed.split_once('#') {
            Some((base, variant)) => (
                (!base.is_empty()).then(|| sanitize_and_redact(base, 128)),
                (!variant.is_empty()).then(|| sanitize_and_redact(variant, 128)),
            ),
            None => (Some(sanitize_and_redact(trimmed, 128)), None),
        };
        if let Some(cb) = cand_base {
            match &base_model {
                None => base_model = Some(cb),
                Some(prev) if prev != &cb && model_conflict.is_none() => {
                    model_conflict = Some(format!(
                        "conflicting models observed on event: `{prev}` vs `{cb}`"
                    ));
                }
                _ => {}
            }
        }
        if let Some(ci) = cand_inline {
            match &inline_effort {
                None => inline_effort = Some(ci),
                Some(prev) if prev != &ci && effort_conflict.is_none() => {
                    effort_conflict = Some(format!(
                        "conflicting inline model variants observed on event: `{prev}` vs `{ci}`"
                    ));
                }
                _ => {}
            }
        }
    }

    let explicit_effort_scopes = [
        Some(obj),
        obj.get("init").and_then(Value::as_object),
        obj.get("result").and_then(Value::as_object),
        obj.get("step_update").and_then(Value::as_object),
        obj.get("part").and_then(Value::as_object),
        obj.get("message").and_then(Value::as_object),
        obj.get("metadata").and_then(Value::as_object),
        obj.get("model").and_then(Value::as_object),
        obj.get("part")
            .and_then(Value::as_object)
            .and_then(|p| p.get("model"))
            .and_then(Value::as_object),
        obj.get("result")
            .and_then(Value::as_object)
            .and_then(|r| r.get("model"))
            .and_then(Value::as_object),
        obj.get("init")
            .and_then(Value::as_object)
            .and_then(|i| i.get("model"))
            .and_then(Value::as_object),
        obj.get("message")
            .and_then(Value::as_object)
            .and_then(|m| m.get("model"))
            .and_then(Value::as_object),
    ];

    let mut explicit_effort: Option<String> = None;
    for exp_cand in explicit_effort_scopes
        .into_iter()
        .flatten()
        .flat_map(extract_explicit_efforts)
    {
        match &explicit_effort {
            None => explicit_effort = Some(exp_cand),
            Some(prev) if prev != &exp_cand && effort_conflict.is_none() => {
                effort_conflict = Some(format!(
                    "conflicting explicit effort/variant values observed on event: `{prev}` vs `{exp_cand}`"
                ));
            }
            _ => {}
        }
    }

    let resolved_effort = match (inline_effort, explicit_effort) {
        (Some(inl), Some(exp)) => {
            if inl != exp && effort_conflict.is_none() {
                effort_conflict = Some(format!(
                    "inline model variant `{inl}` conflicts with explicit event effort/variant `{exp}`"
                ));
            }
            Some(exp)
        }
        (Some(inl), None) => Some(inl),
        (None, Some(exp)) => Some(exp),
        (None, None) => None,
    };

    (base_model, resolved_effort, model_conflict, effort_conflict)
}

fn extract_provider_plus_model_id(obj: &Map<String, Value>) -> Option<String> {
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
        return None;
    }
    Some(format!("{provider_id}/{model_id}"))
}

fn extract_explicit_efforts(obj: &Map<String, Value>) -> impl Iterator<Item = String> + '_ {
    ["effort", "variant", "reasoning_effort", "thinking_effort"]
        .into_iter()
        .filter_map(|key| obj.get(key).and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| sanitize_and_redact(value, 128))
}

fn extract_tool_name(obj: &Map<String, Value>) -> Option<String> {
    let raw = obj
        .get("tool")
        .and_then(Value::as_str)
        .or_else(|| obj.get("tool_name").and_then(Value::as_str))
        .or_else(|| {
            if matches!(
                obj.get("event")
                    .or_else(|| obj.get("type"))
                    .and_then(Value::as_str),
                Some("tool_use" | "tool_result" | "permission" | "permission_request")
            ) {
                obj.get("name").and_then(Value::as_str)
            } else {
                None
            }
        })
        .or_else(|| {
            obj.get("step_update")
                .and_then(Value::as_object)
                .and_then(|s| s.get("tool").or_else(|| s.get("tool_name")))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            obj.get("part")
                .and_then(Value::as_object)
                .and_then(|p| p.get("tool").or_else(|| p.get("tool_name")))
                .and_then(Value::as_str)
        })?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(sanitize_and_redact(trimmed, 128))
    }
}

fn extract_denied_actions(obj: &Map<String, Value>) -> Option<(Option<String>, String)> {
    let arr = obj
        .get("denied_actions")
        .and_then(Value::as_array)
        .or_else(|| {
            obj.get("result")
                .and_then(Value::as_object)
                .and_then(|r| r.get("denied_actions"))
                .and_then(Value::as_array)
        })
        .or_else(|| {
            obj.get("step_update")
                .and_then(Value::as_object)
                .and_then(|s| s.get("denied_actions"))
                .and_then(Value::as_array)
        })
        .or_else(|| {
            obj.get("init")
                .and_then(Value::as_object)
                .and_then(|i| i.get("denied_actions"))
                .and_then(Value::as_array)
        })?;

    if arr.is_empty() {
        return None;
    }

    let mut first_tool: Option<String> = None;
    let mut descriptions: Vec<String> = Vec::new();

    for item in arr {
        match item {
            Value::Object(map) => {
                let action = map
                    .get("action")
                    .or_else(|| map.get("tool"))
                    .or_else(|| map.get("name"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                let display = map
                    .get("display_name")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty());

                if first_tool.is_none() {
                    first_tool = action.or(display).map(|act| sanitize_and_redact(act, 128));
                }

                match (action, display) {
                    (Some(a), Some(d)) if a != d => descriptions.push(format!("{a} ({d})")),
                    (Some(a), _) => descriptions.push(a.to_string()),
                    (None, Some(d)) => descriptions.push(d.to_string()),
                    (None, None) => descriptions.push("unknown_action".to_string()),
                }
            }
            Value::String(s) => {
                let trimmed = s.trim();
                if !trimmed.is_empty() {
                    if first_tool.is_none() {
                        first_tool = Some(sanitize_and_redact(trimmed, 128));
                    }
                    descriptions.push(trimmed.to_string());
                }
            }
            _ => {}
        }
    }

    let raw_summary = if descriptions.is_empty() {
        "permission denied via denied_actions".to_string()
    } else {
        format!("denied_actions: {}", descriptions.join(", "))
    };

    Some((
        first_tool,
        sanitize_and_redact(&raw_summary, MAX_DIAGNOSTIC_SUMMARY_BYTES),
    ))
}

fn detect_blocked_event(
    obj: &Map<String, Value>,
    event_type: &str,
    subtype: Option<&str>,
) -> Option<(BlockedKind, Option<String>, String)> {
    if let Some((denied_tool, denied_summary)) = extract_denied_actions(obj) {
        return Some((BlockedKind::PermissionRequired, denied_tool, denied_summary));
    }

    let et = event_type.to_ascii_lowercase();
    let st = subtype.unwrap_or("").to_ascii_lowercase();
    let nested_res = obj.get("result").and_then(Value::as_object);

    let nested_step = obj.get("step_update").and_then(Value::as_object);

    let status = obj
        .get("status")
        .and_then(Value::as_str)
        .or_else(|| {
            nested_res
                .and_then(|r| r.get("status"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            nested_step
                .and_then(|s| s.get("state"))
                .and_then(Value::as_str)
        })
        .unwrap_or("")
        .to_ascii_lowercase();
    let stop_reason = obj
        .get("stop_reason")
        .or_else(|| obj.get("reason"))
        .or_else(|| nested_res.and_then(|r| r.get("stop_reason").or_else(|| r.get("reason"))))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    let part_status = obj
        .get("part")
        .and_then(Value::as_object)
        .and_then(|p| {
            p.get("reason").and_then(Value::as_str).or_else(|| {
                p.get("state")
                    .and_then(Value::as_object)
                    .and_then(|s| s.get("status"))
                    .and_then(Value::as_str)
            })
        })
        .unwrap_or("")
        .to_ascii_lowercase();

    let err_code = extract_error_code(obj)
        .unwrap_or_default()
        .to_ascii_lowercase();

    let is_permission_block = matches!(
        et.as_str(),
        "permission"
            | "permission_request"
            | "permission_required"
            | "approval_required"
            | "tool_permission"
    ) || matches!(
        st.as_str(),
        "permission"
            | "permission_request"
            | "permission_required"
            | "permission_denied"
            | "approval_required"
    ) || matches!(
        status.as_str(),
        "permission_required"
            | "permission_denied"
            | "requires_permission"
            | "waiting_for_approval"
            | "pending_permission"
    ) || matches!(
        stop_reason.as_str(),
        "permission_required" | "permission_denied" | "approval_required"
    ) || matches!(
        part_status.as_str(),
        "permission_required" | "permission_denied" | "waiting_for_approval" | "pending_permission"
    ) || matches!(
        err_code.as_str(),
        "permission_denied"
            | "permission_required"
            | "approval_required"
            | "eacces_tool_permission"
            | "toolpermissionerror"
    ) || obj.get("permission_required") == Some(&Value::Bool(true))
        || obj.get("approval_required") == Some(&Value::Bool(true));

    if is_permission_block {
        let summary = extract_summary(obj)
            .unwrap_or_else(|| "worker blocked on tool or edit permission".to_string());
        return Some((BlockedKind::PermissionRequired, None, summary));
    }

    let is_input_block = matches!(
        et.as_str(),
        "input_required" | "user_input_required" | "question" | "ask_user" | "blocked"
    ) || matches!(
        st.as_str(),
        "input_required" | "user_input_required" | "question" | "blocked"
    ) || matches!(
        status.as_str(),
        "input_required"
            | "user_input_required"
            | "requires_input"
            | "waiting_for_input"
            | "question_pending"
            | "blocked"
    ) || matches!(
        stop_reason.as_str(),
        "input_required" | "user_input_required" | "question" | "blocked"
    ) || matches!(
        part_status.as_str(),
        "input_required" | "user_input_required" | "waiting_for_input" | "blocked"
    ) || matches!(
        err_code.as_str(),
        "input_required" | "user_input_required" | "userinputrequirederror"
    ) || obj.get("input_required") == Some(&Value::Bool(true))
        || obj.get("requires_user_input") == Some(&Value::Bool(true))
        || obj.get("is_blocked") == Some(&Value::Bool(true));

    if is_input_block {
        let summary = extract_summary(obj)
            .unwrap_or_else(|| "worker blocked waiting for user input".to_string());
        return Some((BlockedKind::UserInputRequired, None, summary));
    }

    None
}

pub(crate) fn has_object_error_field(obj: &Map<String, Value>) -> bool {
    match obj.get("error") {
        Some(Value::String(s)) => !s.trim().is_empty(),
        Some(Value::Object(_)) => true,
        Some(Value::Bool(b)) => *b,
        _ => false,
    }
}

fn is_recoverable_step_error(
    obj: &Map<String, Value>,
    event_type: &str,
    subtype: Option<&str>,
) -> bool {
    let et = event_type.to_ascii_lowercase();
    let st = subtype.unwrap_or("").to_ascii_lowercase();

    let is_step_or_tool_event = matches!(et.as_str(), "step_update" | "tool_result" | "tool")
        || matches!(st.as_str(), "tool_result" | "tool");
    if !is_step_or_tool_event {
        return false;
    }

    let step_obj = obj.get("step_update").and_then(Value::as_object);
    let step_state = step_obj
        .and_then(|s| s.get("state").or_else(|| s.get("status")))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    let top_status = obj
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();

    matches!(step_state.as_str(), "error" | "failed" | "failure")
        || matches!(top_status.as_str(), "error" | "failed" | "failure")
        || obj.get("is_error") == Some(&Value::Bool(true))
        || step_obj.and_then(|s| s.get("is_error")) == Some(&Value::Bool(true))
        || has_object_error_field(obj)
        || step_obj.is_some_and(has_object_error_field)
}

fn is_recoverable_codex_command_execution_error(
    provider: Option<WorkerProvider>,
    obj: &Map<String, Value>,
    event_type: &str,
    subtype: Option<&str>,
) -> bool {
    if provider != Some(WorkerProvider::CodexCli) || event_type != "item.completed" {
        return false;
    }
    if !subtype.is_none_or(|st| st == "command_execution") {
        return false;
    }
    if obj.contains_key("status")
        || obj.contains_key("is_error")
        || obj.contains_key("failure")
        || obj.contains_key("result")
        || obj.contains_key("part")
        || has_object_error_field(obj)
    {
        return false;
    }
    let Some(item) = obj.get("item").and_then(Value::as_object) else {
        return false;
    };
    if item.get("type").and_then(Value::as_str) != Some("command_execution") {
        return false;
    }
    if item.contains_key("state")
        || item.contains_key("is_error")
        || item.contains_key("failure")
        || has_object_error_field(item)
    {
        return false;
    }
    let Some(item_status) = item.get("status").and_then(Value::as_str) else {
        return false;
    };
    if !matches!(
        item_status.to_ascii_lowercase().as_str(),
        "failed" | "completed"
    ) {
        return false;
    }
    item.get("exit_code")
        .and_then(Value::as_i64)
        .is_some_and(|code| code != 0)
}

fn detect_error_event(
    obj: &Map<String, Value>,
    event_type: &str,
    subtype: Option<&str>,
) -> Option<(Option<String>, String)> {
    let et = event_type.to_ascii_lowercase();
    let st = subtype.unwrap_or("").to_ascii_lowercase();
    let nested_res = obj.get("result").and_then(Value::as_object);

    // The installed AGY CLI reports this terminal timeout alongside status SUCCESS.
    // A selected model variant cannot turn an incomplete provider run into completion.
    if et == "result"
        && [Some(obj), nested_res].into_iter().flatten().any(|scope| {
            scope.get("finished_reason").and_then(Value::as_str) == Some("print_timeout")
        })
    {
        return Some((
            Some("print_timeout".to_string()),
            "provider print deadline elapsed without confirmed completion".to_string(),
        ));
    }

    let status = obj
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    let nested_res_status = nested_res
        .and_then(|r| {
            r.get("status")
                .or_else(|| r.get("subtype"))
                .or_else(|| r.get("state"))
        })
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    let part_reason = obj
        .get("part")
        .and_then(Value::as_object)
        .and_then(|p| p.get("reason"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();

    let nested_item = obj.get("item").and_then(Value::as_object);
    let nested_item_status = nested_item
        .and_then(|i| i.get("status").or_else(|| i.get("state")))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    let nested_item_type = nested_item
        .and_then(|i| i.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    let has_error_field = has_object_error_field(obj)
        || nested_res.is_some_and(has_object_error_field)
        || nested_item.is_some_and(has_object_error_field);

    let is_error = et == "error"
        || et == "turn.failed"
        || st == "error"
        || st == "error_during_execution"
        || nested_item_type == "error"
        || matches!(status.as_str(), "error" | "failed" | "failure")
        || matches!(
            nested_res_status.as_str(),
            "error" | "failed" | "failure" | "cancelled" | "canceled" | "aborted"
        )
        || matches!(
            nested_item_status.as_str(),
            "error" | "failed" | "failure" | "cancelled" | "canceled" | "aborted"
        )
        || part_reason == "error"
        || obj.get("is_error") == Some(&Value::Bool(true))
        || nested_res.and_then(|r| r.get("is_error")) == Some(&Value::Bool(true))
        || nested_item.and_then(|i| i.get("is_error")) == Some(&Value::Bool(true))
        || (has_error_field && obj.get("is_error") != Some(&Value::Bool(false)));

    if !is_error {
        return None;
    }

    let code = extract_error_code(obj);
    let msg =
        extract_summary(obj).unwrap_or_else(|| "provider reported an error event".to_string());
    Some((code, msg))
}

fn extract_error_code(obj: &Map<String, Value>) -> Option<String> {
    let nested_res = obj.get("result").and_then(Value::as_object);
    let raw = obj
        .get("code")
        .and_then(Value::as_str)
        .or_else(|| {
            obj.get("error")
                .and_then(Value::as_object)
                .and_then(|e| {
                    e.get("code")
                        .or_else(|| e.get("name"))
                        .or_else(|| e.get("type"))
                })
                .and_then(Value::as_str)
        })
        .or_else(|| {
            nested_res
                .and_then(|r| r.get("code").or_else(|| r.get("error_code")))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            nested_res
                .and_then(|r| r.get("error"))
                .and_then(Value::as_object)
                .and_then(|e| {
                    e.get("code")
                        .or_else(|| e.get("name"))
                        .or_else(|| e.get("type"))
                })
                .and_then(Value::as_str)
        })?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(sanitize_and_redact(trimmed, 64))
    }
}

fn is_init_event(obj: &Map<String, Value>, event_type: &str, subtype: Option<&str>) -> bool {
    let et = event_type.to_ascii_lowercase();
    let st = subtype.unwrap_or("").to_ascii_lowercase();
    et == "init"
        || obj.get("init").is_some_and(Value::is_object)
        || (et == "system" && st == "init")
        || et == "step_start"
        || st == "step-start"
        || (et == "thread.started" && extract_session_id(obj).is_some())
        || (et == "session"
            && (st == "init"
                || st == "created"
                || obj.get("status").and_then(Value::as_str) == Some("started")
                || obj.contains_key("sessionID")
                || obj.contains_key("session_id")
                || obj.contains_key("conversation_id")
                || obj.contains_key("thread_id")))
}

fn is_result_event(obj: &Map<String, Value>, event_type: &str, subtype: Option<&str>) -> bool {
    let et = event_type.to_ascii_lowercase();
    let st = subtype.unwrap_or("").to_ascii_lowercase();

    if et == "result" {
        if obj.get("is_error") == Some(&Value::Bool(true)) || has_object_error_field(obj) {
            return false;
        }
        if let Some(nested_res) = obj.get("result").and_then(Value::as_object) {
            if nested_res.get("is_error") == Some(&Value::Bool(true))
                || has_object_error_field(nested_res)
            {
                return false;
            }
            let nested_status = nested_res
                .get("status")
                .or_else(|| nested_res.get("subtype"))
                .or_else(|| nested_res.get("state"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_ascii_lowercase();
            if !matches!(
                nested_status.as_str(),
                "" | "success" | "completed" | "ok" | "done"
            ) {
                return false;
            }
        }
        let status = obj
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_ascii_lowercase();
        return matches!(st.as_str(), "" | "success" | "completed" | "ok" | "done")
            && matches!(
                status.as_str(),
                "" | "success" | "completed" | "ok" | "done"
            );
    }

    if et == "step_finish" || st == "step-finish" {
        let reason = obj
            .get("part")
            .and_then(Value::as_object)
            .and_then(|p| p.get("reason"))
            .and_then(Value::as_str)
            .or_else(|| obj.get("reason").and_then(Value::as_str))
            .or_else(|| obj.get("status").and_then(Value::as_str))
            .unwrap_or("stop")
            .to_ascii_lowercase();
        return matches!(
            reason.as_str(),
            "stop" | "completed" | "success" | "end_turn"
        );
    }

    if et == "turn.completed" {
        if obj.get("is_error") == Some(&Value::Bool(true)) || has_object_error_field(obj) {
            return false;
        }
        let status = obj
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_ascii_lowercase();
        return matches!(
            status.as_str(),
            "" | "completed" | "success" | "ok" | "done"
        );
    }

    false
}

fn has_codex_partial_or_truncation_markers(
    obj: &Map<String, Value>,
    nested_item: Option<&Map<String, Value>>,
) -> bool {
    for scope in [Some(obj), nested_item].into_iter().flatten() {
        if scope.contains_key("delta")
            || scope.get("partial") == Some(&Value::Bool(true))
            || scope.get("incomplete") == Some(&Value::Bool(true))
            || scope.get("truncated") == Some(&Value::Bool(true))
        {
            return true;
        }
        for key in [
            "stop_reason",
            "finish_reason",
            "finished_reason",
            "reason",
            "status",
            "incomplete_reason",
        ] {
            if let Some(raw) = scope.get(key).and_then(Value::as_str) {
                let lowered = raw.trim().to_ascii_lowercase();
                if matches!(
                    lowered.as_str(),
                    "length"
                        | "max_tokens"
                        | "max_output_tokens"
                        | "truncated"
                        | "incomplete"
                        | "partial"
                        | "interrupted"
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

pub(crate) fn extract_genuine_turn_usage(
    obj: &Map<String, Value>,
) -> Result<Option<StreamTokenUsage>, String> {
    let Some(usage_val) = obj.get("usage") else {
        return Ok(None);
    };
    if usage_val.is_null() {
        return Ok(None);
    }
    let Some(usage_obj) = usage_val.as_object() else {
        return Err("`usage` must be a JSON object when present".to_string());
    };
    let input_tokens = usage_obj
        .get("input_tokens")
        .and_then(Value::as_u64)
        .ok_or_else(|| "`usage.input_tokens` must be a non-negative integer".to_string())?;
    let output_tokens = usage_obj
        .get("output_tokens")
        .and_then(Value::as_u64)
        .ok_or_else(|| "`usage.output_tokens` must be a non-negative integer".to_string())?;
    let cached_input_tokens = match usage_obj.get("cached_input_tokens") {
        None | Some(Value::Null) => None,
        Some(val) => Some(val.as_u64().ok_or_else(|| {
            "`usage.cached_input_tokens` must be a non-negative integer".to_string()
        })?),
    };
    let cache_write_input_tokens = match usage_obj.get("cache_write_input_tokens") {
        None | Some(Value::Null) => None,
        Some(val) => Some(val.as_u64().ok_or_else(|| {
            "`usage.cache_write_input_tokens` must be a non-negative integer".to_string()
        })?),
    };
    let reasoning_output_tokens = match usage_obj.get("reasoning_output_tokens") {
        None | Some(Value::Null) => None,
        Some(val) => Some(val.as_u64().ok_or_else(|| {
            "`usage.reasoning_output_tokens` must be a non-negative integer".to_string()
        })?),
    };
    Ok(Some(StreamTokenUsage {
        input_tokens,
        cached_input_tokens,
        cache_write_input_tokens,
        output_tokens,
        reasoning_output_tokens,
    }))
}

fn extract_summary(obj: &Map<String, Value>) -> Option<String> {
    let nested_err = obj.get("error").and_then(Value::as_object);
    let nested_err_data = nested_err
        .and_then(|e| e.get("data"))
        .and_then(Value::as_object);
    let nested_res = obj.get("result").and_then(Value::as_object);
    let nested_res_err = nested_res
        .and_then(|r| r.get("error"))
        .and_then(Value::as_object);
    let nested_step = obj.get("step_update").and_then(Value::as_object);
    let nested_part = obj.get("part").and_then(Value::as_object);
    let nested_item = obj.get("item").and_then(Value::as_object);
    let nested_msg = obj.get("message").and_then(Value::as_object);

    let candidates = [
        obj.get("message").and_then(Value::as_str),
        obj.get("result").and_then(Value::as_str),
        obj.get("response").and_then(Value::as_str),
        obj.get("text").and_then(Value::as_str),
        obj.get("summary").and_then(Value::as_str),
        obj.get("question").and_then(Value::as_str),
        obj.get("error").and_then(Value::as_str),
        nested_res.and_then(|r| {
            r.get("error")
                .and_then(Value::as_str)
                .or_else(|| r.get("response").and_then(Value::as_str))
                .or_else(|| r.get("message").and_then(Value::as_str))
                .or_else(|| r.get("summary").and_then(Value::as_str))
        }),
        nested_res_err
            .and_then(|e| e.get("message"))
            .and_then(Value::as_str),
        nested_step.and_then(|s| {
            s.get("error")
                .and_then(Value::as_str)
                .or_else(|| s.get("message").and_then(Value::as_str))
                .or_else(|| s.get("step_type").and_then(Value::as_str))
        }),
        nested_err
            .and_then(|e| e.get("message"))
            .and_then(Value::as_str),
        nested_err_data
            .and_then(|d| d.get("message"))
            .and_then(Value::as_str),
        nested_part
            .and_then(|p| p.get("text").or_else(|| p.get("message")))
            .and_then(Value::as_str),
        nested_item
            .and_then(|i| {
                i.get("text")
                    .or_else(|| i.get("message"))
                    .or_else(|| i.get("error"))
            })
            .and_then(Value::as_str),
        nested_msg
            .and_then(|m| m.get("text").or_else(|| m.get("content")))
            .and_then(Value::as_str),
    ];

    for candidate in candidates.into_iter().flatten() {
        let trimmed = candidate.trim();
        if !trimmed.is_empty() {
            return Some(sanitize_and_redact(trimmed, MAX_DIAGNOSTIC_SUMMARY_BYTES));
        }
    }
    None
}

pub(crate) fn trim_ascii_whitespace(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map_or(start, |idx| idx + 1);
    &bytes[start..end]
}
