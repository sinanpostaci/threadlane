use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::fs_safety::sanitize_and_redact;
use crate::spec::ResolvedModelTarget;

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
    /// Other valid JSON object event.
    Other,
}

/// Bounded, normalized representation of a single provider NDJSON event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedWorkerEvent {
    /// 1-based stdout line number.
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
    /// Verification of requested vs observed session ID.
    pub session_evidence: FieldEvidence,
    /// First observed session ID in the stream, if any.
    pub observed_session_id: Option<String>,
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

/// Incremental stateful parser for Antigravity and OpenCode v2 NDJSON streams.
pub struct NdjsonStreamParser {
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
    effort_conflict: Option<String>,
    observed_session_id: Option<String>,
    session_conflict: Option<String>,
    provider_errors: Vec<ProviderErrorEvidence>,
    recoverable_step_errors_count: usize,
    blocked_events: Vec<BlockedEvidence>,
    retained_events: Vec<NormalizedWorkerEvent>,
    dropped_events_count: usize,
}

impl NdjsonStreamParser {
    /// Creates a new incremental NDJSON stream parser for the given expected model, effort, and session.
    pub fn new(
        requested_model: ResolvedModelTarget,
        requested_session_id: Option<String>,
        max_events: usize,
    ) -> Self {
        Self {
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
            effort_conflict: None,
            observed_session_id: None,
            session_conflict: None,
            provider_errors: Vec::new(),
            recoverable_step_errors_count: 0,
            blocked_events: Vec::new(),
            retained_events: Vec::new(),
            dropped_events_count: 0,
        }
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
            });

        let raw_type = match subtype {
            Some(sub) if !sub.is_empty() && sub != event_type => {
                sanitize_and_redact(&format!("{event_type}:{sub}"), 64)
            }
            _ => sanitize_and_redact(event_type, 64),
        };

        let session_id = extract_session_id(obj);
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

        let (event_model, event_effort, inline_effort_conflict) = extract_model_and_effort(obj);
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
        if let Some(ref effort) = event_effort {
            match &self.observed_effort {
                None => self.observed_effort = Some(effort.clone()),
                Some(prev) if prev != effort && self.effort_conflict.is_none() => {
                    self.effort_conflict = Some(format!(
                        "conflicting effort/variant values observed in stream: `{prev}` vs `{effort}`"
                    ));
                }
                _ => {}
            }
        }

        let mut tool_name = extract_tool_name(obj);

        // 1. Check for permission or user-input block first (including `denied_actions`)
        // so blocked states are never misclassified as success.
        if let Some((blocked_kind, blocked_tool, blocked_summary)) =
            detect_blocked_event(obj, event_type, subtype)
        {
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

        // 2. Check for non-fatal recoverable step/tool errors (`step_update`, `tool_result`)
        // before terminal provider error detection so a recoverable step tool failure
        // does not poison a subsequent valid `result` event.
        if is_recoverable_step_error(obj, event_type, subtype) {
            self.recoverable_step_errors_count += 1;
        } else if let Some((err_code, err_msg)) = detect_error_event(obj, event_type, subtype) {
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
        }

        // 3. Classify Init, Result, Tool, Progress, or Other.
        let is_init = is_init_event(obj, event_type, subtype);
        if is_init {
            self.saw_init_event = true;
        }

        let is_result = is_result_event(obj, event_type, subtype);
        if is_result {
            self.saw_result_event = true;
        }

        let step_is_tool = subtype.is_some_and(|s| s.to_ascii_lowercase().contains("tool"));

        let kind = if is_result {
            NormalizedEventKind::Result
        } else if is_init {
            NormalizedEventKind::Init
        } else if tool_name.is_some()
            || step_is_tool
            || matches!(event_type, "tool" | "tool_use" | "tool_result")
            || matches!(subtype, Some("tool" | "tool_use" | "tool_result"))
        {
            NormalizedEventKind::Tool
        } else if matches!(
            event_type,
            "assistant" | "message" | "text" | "content" | "step_update"
        ) || matches!(subtype, Some("text" | "message" | "agent_response"))
        {
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
    pub fn finalize(self) -> StreamEvidenceSummary {
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
            session_evidence,
            observed_session_id: self.observed_session_id,
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
        .and_then(Value::as_str)
}

fn extract_session_id(obj: &Map<String, Value>) -> Option<String> {
    let candidates = [
        obj.get("conversation_id").and_then(Value::as_str),
        obj.get("session_id").and_then(Value::as_str),
        obj.get("sessionID").and_then(Value::as_str),
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

fn extract_model_and_effort(
    obj: &Map<String, Value>,
) -> (Option<String>, Option<String>, Option<String>) {
    let raw_model_str = obj
        .get("model")
        .and_then(Value::as_str)
        .or_else(|| {
            obj.get("init")
                .and_then(Value::as_object)
                .and_then(|i| i.get("model"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            obj.get("result")
                .and_then(Value::as_object)
                .and_then(|r| r.get("model"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            obj.get("step_update")
                .and_then(Value::as_object)
                .and_then(|s| s.get("model"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            obj.get("message")
                .and_then(Value::as_object)
                .and_then(|m| m.get("model"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            obj.get("part")
                .and_then(Value::as_object)
                .and_then(|p| p.get("model"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            obj.get("metadata")
                .and_then(Value::as_object)
                .and_then(|m| m.get("model"))
                .and_then(Value::as_str)
        })
        .map(str::to_string)
        .or_else(|| extract_provider_plus_model_id(obj))
        .or_else(|| {
            obj.get("init")
                .and_then(Value::as_object)
                .and_then(extract_provider_plus_model_id)
        })
        .or_else(|| {
            obj.get("part")
                .and_then(Value::as_object)
                .and_then(extract_provider_plus_model_id)
        })
        .or_else(|| {
            obj.get("model")
                .and_then(Value::as_object)
                .and_then(extract_provider_plus_model_id)
        });

    let explicit_effort = extract_explicit_effort(obj)
        .or_else(|| {
            obj.get("init")
                .and_then(Value::as_object)
                .and_then(extract_explicit_effort)
        })
        .or_else(|| {
            obj.get("result")
                .and_then(Value::as_object)
                .and_then(extract_explicit_effort)
        })
        .or_else(|| {
            obj.get("step_update")
                .and_then(Value::as_object)
                .and_then(extract_explicit_effort)
        })
        .or_else(|| {
            obj.get("part")
                .and_then(Value::as_object)
                .and_then(extract_explicit_effort)
        })
        .or_else(|| {
            obj.get("message")
                .and_then(Value::as_object)
                .and_then(extract_explicit_effort)
        })
        .or_else(|| {
            obj.get("metadata")
                .and_then(Value::as_object)
                .and_then(extract_explicit_effort)
        })
        .or_else(|| {
            obj.get("model")
                .and_then(Value::as_object)
                .and_then(extract_explicit_effort)
        });

    let mut base_model = None;
    let mut inline_effort = None;

    if let Some(raw_m) = raw_model_str {
        let trimmed = raw_m.trim();
        if !trimmed.is_empty() {
            if let Some((base, variant)) = trimmed.split_once('#') {
                if !base.is_empty() {
                    base_model = Some(sanitize_and_redact(base, 128));
                }
                if !variant.is_empty() {
                    inline_effort = Some(sanitize_and_redact(variant, 128));
                }
            } else {
                base_model = Some(sanitize_and_redact(trimmed, 128));
            }
        }
    }

    let mut conflict = None;
    let resolved_effort = match (inline_effort, explicit_effort) {
        (Some(inl), Some(exp)) => {
            if inl != exp {
                conflict = Some(format!(
                    "inline model variant `{inl}` conflicts with explicit event effort/variant `{exp}`"
                ));
            }
            Some(exp)
        }
        (Some(inl), None) => Some(inl),
        (None, Some(exp)) => Some(exp),
        (None, None) => None,
    };

    (base_model, resolved_effort, conflict)
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

fn extract_explicit_effort(obj: &Map<String, Value>) -> Option<String> {
    for key in ["effort", "variant", "reasoning_effort", "thinking_effort"] {
        if let Some(val) = obj.get(key).and_then(Value::as_str) {
            let trimmed = val.trim();
            if !trimmed.is_empty() {
                return Some(sanitize_and_redact(trimmed, 128));
            }
        }
    }
    None
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

fn has_object_error_field(obj: &Map<String, Value>) -> bool {
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

fn detect_error_event(
    obj: &Map<String, Value>,
    event_type: &str,
    subtype: Option<&str>,
) -> Option<(Option<String>, String)> {
    let et = event_type.to_ascii_lowercase();
    let st = subtype.unwrap_or("").to_ascii_lowercase();
    let nested_res = obj.get("result").and_then(Value::as_object);

    let status = obj
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    let nested_res_status = nested_res
        .and_then(|r| r.get("status").or_else(|| r.get("subtype")))
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

    let has_error_field =
        has_object_error_field(obj) || nested_res.is_some_and(has_object_error_field);

    let is_error = et == "error"
        || st == "error"
        || st == "error_during_execution"
        || matches!(status.as_str(), "error" | "failed" | "failure")
        || matches!(
            nested_res_status.as_str(),
            "error" | "failed" | "failure" | "cancelled" | "canceled" | "aborted"
        )
        || part_reason == "error"
        || obj.get("is_error") == Some(&Value::Bool(true))
        || nested_res.and_then(|r| r.get("is_error")) == Some(&Value::Bool(true))
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
        || (et == "session"
            && (st == "init"
                || st == "created"
                || obj.get("status").and_then(Value::as_str) == Some("started")
                || obj.contains_key("sessionID")
                || obj.contains_key("session_id")
                || obj.contains_key("conversation_id")))
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

    false
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

fn trim_ascii_whitespace(bytes: &[u8]) -> &[u8] {
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
