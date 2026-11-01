use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use threadlane_registry::{Difficulty, ModelDefinition};

use crate::error::DecisionError;
use crate::types::{
    DecisionAdvice, DecisionProvider, DecisionRequest, DEFAULT_OPENAI_DECISION_MODEL,
    MAX_JS_SAFE_INTEGER, MIN_CONFIDENCE_THRESHOLD,
};

/// Maximum length in bytes for `tool_id` and `model` identifiers.
pub const MAX_IDENTIFIER_BYTES: usize = 128;

/// Maximum length in bytes for `task_summary`.
pub const MAX_TASK_SUMMARY_BYTES: usize = 8 * 1024;

/// Maximum number of models accepted from a provider catalog response.
pub const MAX_CATALOG_MODELS: usize = 256;

pub(crate) const QUESTION_NAME: &str = "difficulty";
pub(crate) const JEV_INSTRUCTIONS: &str =
    "Select implementation difficulty from the supplied task evidence.";
const OPENAI_INSTRUCTIONS: &str = "Select implementation difficulty from the supplied evidence.";

pub(crate) const EASY_DESCRIPTION: &str = "Small isolated behavior-preserving edit.";
pub(crate) const MEDIUM_DESCRIPTION: &str =
    "Bounded multi-step change within known module boundaries.";
pub(crate) const HARD_DESCRIPTION: &str =
    "Substantial uncertainty, concurrency, or cross-module integration complexity.";
pub(crate) const EXTREME_DESCRIPTION: &str =
    "High-risk architectural, security-critical, or multi-system redesign.";

#[derive(Clone)]
pub(crate) struct ValidatedRequest {
    pub tool_id: String,
    pub provider: DecisionProvider,
    pub model: String,
    pub task_summary: String,
}

pub(crate) fn validate_tool_id(tool_id: &str) -> Result<String, DecisionError> {
    let trimmed = tool_id.trim();
    if trimmed.is_empty()
        || trimmed.len() > MAX_IDENTIFIER_BYTES
        || trimmed.chars().any(|ch| ch.is_control())
    {
        return Err(DecisionError::InvalidRequest);
    }
    Ok(trimmed.to_string())
}

pub(crate) fn validate_secret(secret: &str) -> Result<String, DecisionError> {
    let trimmed = secret.trim();
    if trimmed.is_empty() {
        return Err(DecisionError::NotConfigured);
    }
    if trimmed.len() > 4096 || trimmed.chars().any(|ch| ch.is_control()) {
        return Err(DecisionError::InvalidRequest);
    }
    Ok(trimmed.to_string())
}

pub(crate) fn validate_model_for_provider(
    provider: DecisionProvider,
    model: &str,
) -> Result<String, DecisionError> {
    let trimmed = model.trim();
    if !is_valid_model_token(trimmed) {
        return Err(DecisionError::InvalidRequest);
    }
    match provider {
        DecisionProvider::Jev => {
            if !is_supported_jev_model(trimmed) {
                return Err(DecisionError::InvalidRequest);
            }
        }
        DecisionProvider::Openai => {
            if !is_supported_openai_decision_model(trimmed) {
                return Err(DecisionError::InvalidRequest);
            }
        }
    }
    Ok(trimmed.to_string())
}

pub(crate) fn validate_request(
    request: &DecisionRequest,
) -> Result<ValidatedRequest, DecisionError> {
    let tool_id = validate_tool_id(&request.tool_id)?;
    let model = validate_model_for_provider(request.provider, &request.model)?;
    let task_summary = request.task_summary.trim();
    if task_summary.is_empty()
        || task_summary.len() > MAX_TASK_SUMMARY_BYTES
        || task_summary.contains('\0')
    {
        return Err(DecisionError::InvalidRequest);
    }
    Ok(ValidatedRequest {
        tool_id,
        provider: request.provider,
        model,
        task_summary: task_summary.to_string(),
    })
}

fn is_valid_model_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':'))
}

pub(crate) fn is_supported_jev_model(model: &str) -> bool {
    is_valid_model_token(model) && model.starts_with("jev-") && model.len() > 4
}

pub(crate) fn is_supported_openai_decision_model(model: &str) -> bool {
    is_valid_model_token(model)
        && (model == DEFAULT_OPENAI_DECISION_MODEL || model.starts_with("gpt-6-luna-"))
}

fn is_jev_alias(model: &str) -> bool {
    matches!(model, "jev-latest" | "jev-preview")
}

fn validate_model_echo(
    provider: DecisionProvider,
    requested_model: &str,
    echoed_model: &str,
) -> Result<String, DecisionError> {
    let echoed = echoed_model.trim();
    if !is_valid_model_token(echoed) {
        return Err(DecisionError::InvalidResponse);
    }

    match provider {
        DecisionProvider::Jev => {
            if !is_supported_jev_model(echoed) {
                return Err(DecisionError::InvalidResponse);
            }
            if is_jev_alias(requested_model) {
                // Jev aliases such as `jev-latest` or `jev-preview` may resolve to a concrete
                // versioned model (e.g. `jev-1.14.0`) or echo the alias itself.
                Ok(echoed.to_string())
            } else if echoed == requested_model {
                Ok(echoed.to_string())
            } else {
                Err(DecisionError::InvalidResponse)
            }
        }
        DecisionProvider::Openai => {
            if !is_supported_openai_decision_model(echoed) {
                return Err(DecisionError::InvalidResponse);
            }
            if echoed == requested_model
                || (requested_model == DEFAULT_OPENAI_DECISION_MODEL
                    && echoed.starts_with("gpt-6-luna-"))
            {
                Ok(echoed.to_string())
            } else {
                Err(DecisionError::InvalidResponse)
            }
        }
    }
}

fn parse_difficulty_choice(value: &str) -> Result<Difficulty, DecisionError> {
    match value {
        "easy" => Ok(Difficulty::Easy),
        "medium" => Ok(Difficulty::Medium),
        "hard" => Ok(Difficulty::Hard),
        "extreme" => Ok(Difficulty::Extreme),
        _ => Err(DecisionError::InvalidResponse),
    }
}

fn validate_unit_interval(value: f64) -> Result<f64, DecisionError> {
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(DecisionError::InvalidResponse);
    }
    Ok(value)
}

fn validate_four_way_probabilities(
    selected: Difficulty,
    entries: &[(String, f64)],
) -> Result<(), DecisionError> {
    if entries.len() != 4 {
        return Err(DecisionError::InvalidResponse);
    }

    let mut seen = HashSet::with_capacity(4);
    let mut sum = 0.0_f64;
    let mut selected_prob: Option<f64> = None;
    let mut max_prob = 0.0_f64;

    for (raw_key, raw_prob) in entries {
        let diff = parse_difficulty_choice(raw_key)?;
        if !seen.insert(raw_key.as_str()) {
            return Err(DecisionError::InvalidResponse);
        }
        let prob = validate_unit_interval(*raw_prob)?;
        sum += prob;
        if diff == selected {
            selected_prob = Some(prob);
        }
        if prob > max_prob {
            max_prob = prob;
        }
    }

    if !sum.is_finite() || !(0.95..=1.05).contains(&sum) {
        return Err(DecisionError::InvalidResponse);
    }

    match selected_prob {
        Some(chosen) if chosen + 1e-6 >= max_prob => Ok(()),
        _ => Err(DecisionError::InvalidResponse),
    }
}

fn validate_probabilities(
    selected: Difficulty,
    entries: &[(String, f64)],
) -> Result<(), DecisionError> {
    if entries.is_empty() || entries.len() > 4 {
        return Err(DecisionError::InvalidResponse);
    }

    let mut seen = HashSet::with_capacity(entries.len());
    let mut sum = 0.0_f64;
    let mut selected_prob: Option<f64> = None;
    let mut max_prob = 0.0_f64;

    for (raw_key, raw_prob) in entries {
        let diff = parse_difficulty_choice(raw_key)?;
        if !seen.insert(raw_key.as_str()) {
            return Err(DecisionError::InvalidResponse);
        }
        let prob = validate_unit_interval(*raw_prob)?;
        sum += prob;
        if diff == selected {
            selected_prob = Some(prob);
        }
        if prob > max_prob {
            max_prob = prob;
        }
    }

    if !sum.is_finite() || sum <= 0.0 || sum > 1.05 {
        return Err(DecisionError::InvalidResponse);
    }

    match selected_prob {
        Some(chosen) if chosen + 1e-6 >= max_prob => Ok(()),
        _ => Err(DecisionError::InvalidResponse),
    }
}

fn validate_token_count(tokens: Option<u64>) -> Result<Option<u64>, DecisionError> {
    if let Some(value) = tokens {
        if value > MAX_JS_SAFE_INTEGER {
            return Err(DecisionError::InvalidResponse);
        }
    }
    Ok(tokens)
}

fn build_classified_advice(
    provider: DecisionProvider,
    model: String,
    difficulty: Difficulty,
    confidence: f64,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
) -> DecisionAdvice {
    let needs_review = confidence < MIN_CONFIDENCE_THRESHOLD;
    let reason_code = if needs_review {
        "low_confidence".to_string()
    } else {
        "classified".to_string()
    };

    DecisionAdvice {
        provider,
        model,
        difficulty: Some(difficulty),
        confidence: Some(confidence),
        needs_review,
        reason_code,
        input_tokens,
        output_tokens,
    }
}

#[derive(Debug, Clone)]
struct UniqueStringMap<V>(Vec<(String, V)>);

impl<'de, V: Deserialize<'de>> Deserialize<'de> for UniqueStringMap<V> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MapVisitor<V>(std::marker::PhantomData<V>);

        impl<'de, V: Deserialize<'de>> serde::de::Visitor<'de> for MapVisitor<V> {
            type Value = UniqueStringMap<V>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object with unique keys")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut entries: Vec<(String, V)> = Vec::new();
                while let Some((key, value)) = map.next_entry::<String, V>()? {
                    if entries.iter().any(|(existing, _)| existing == &key) {
                        return Err(serde::de::Error::custom("duplicate object key"));
                    }
                    entries.push((key, value));
                }
                Ok(UniqueStringMap(entries))
            }
        }

        deserializer.deserialize_map(MapVisitor(std::marker::PhantomData))
    }
}

// ============================================================================
// Jev / TypeSafe System One Schema
// ============================================================================

#[derive(Serialize)]
struct JevClassifyRequest<'a> {
    model: &'a str,
    state: JevTaskState<'a>,
    questions: JevQuestions<'a>,
}

#[derive(Serialize)]
struct JevTaskState<'a> {
    task: &'a str,
}

#[derive(Serialize)]
struct JevQuestions<'a> {
    difficulty: JevDifficultyQuestion<'a>,
}

#[derive(Serialize)]
struct JevDifficultyQuestion<'a> {
    #[serde(rename = "type")]
    question_type: &'a str,
    instructions: &'a str,
    criteria: JevDifficultyCriteria<'a>,
}

#[derive(Serialize)]
struct JevDifficultyCriteria<'a> {
    easy: &'a str,
    medium: &'a str,
    hard: &'a str,
    extreme: &'a str,
}

pub(crate) fn build_jev_classify_body(
    model: &str,
    task_summary: &str,
) -> Result<String, DecisionError> {
    let payload = JevClassifyRequest {
        model,
        state: JevTaskState { task: task_summary },
        questions: JevQuestions {
            difficulty: JevDifficultyQuestion {
                question_type: "choice",
                instructions: JEV_INSTRUCTIONS,
                criteria: JevDifficultyCriteria {
                    easy: EASY_DESCRIPTION,
                    medium: MEDIUM_DESCRIPTION,
                    hard: HARD_DESCRIPTION,
                    extreme: EXTREME_DESCRIPTION,
                },
            },
        },
    };
    serde_json::to_string(&payload).map_err(|_| DecisionError::InvalidRequest)
}

/// Validated catalog model metadata returned by Jev `GET /v1/models`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JevCatalogEntry {
    /// Scoped registry model identifier (`"<tool_id>:<name>"`).
    pub id: String,
    /// Registry tool identifier that owns this model.
    pub tool_id: String,
    /// Upstream Jev model or alias name (`"<name>"`).
    pub name: String,
    /// Display label matching the upstream model name.
    pub label: String,
    /// Exact provider `model` argument to pass to Jev requests.
    pub model_arg: String,
    /// Optional bounded description returned by the Jev catalog.
    pub description: Option<String>,
    /// Optional release date string returned by the Jev catalog.
    pub release_date: Option<String>,
    /// Supported reasoning efforts (empty for Jev System One models).
    pub efforts: Vec<String>,
    /// Whether this model may be invoked without an effort parameter (`true` for Jev).
    pub allow_no_effort: bool,
}

impl JevCatalogEntry {
    /// Converts this catalog entry into a [`ModelDefinition`].
    pub fn into_model_definition(self) -> ModelDefinition {
        ModelDefinition {
            id: self.id,
            tool_id: self.tool_id,
            label: self.label,
            model_arg: self.model_arg,
            efforts: self.efforts,
            allow_no_effort: self.allow_no_effort,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JevModelsResponse {
    models: Vec<JevCatalogModel>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JevCatalogModel {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    release_date: Option<String>,
}

fn sanitize_optional_catalog_field(
    value: Option<String>,
    max_bytes: usize,
) -> Result<Option<String>, DecisionError> {
    let Some(raw) = value else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.len() > max_bytes || trimmed.chars().any(|ch| ch.is_control()) {
        return Err(DecisionError::InvalidResponse);
    }
    Ok(Some(trimmed.to_string()))
}

pub(crate) fn parse_jev_catalog_entries(
    tool_id: &str,
    bytes: &[u8],
) -> Result<Vec<JevCatalogEntry>, DecisionError> {
    let parsed: JevModelsResponse =
        serde_json::from_slice(bytes).map_err(|_| DecisionError::InvalidResponse)?;
    if parsed.models.len() > MAX_CATALOG_MODELS {
        return Err(DecisionError::InvalidResponse);
    }

    let mut seen = HashSet::with_capacity(parsed.models.len());
    let mut entries = Vec::with_capacity(parsed.models.len());

    for item in parsed.models {
        let name = item.name.trim();
        if !is_supported_jev_model(name) {
            return Err(DecisionError::InvalidResponse);
        }
        if !seen.insert(name.to_string()) {
            return Err(DecisionError::InvalidResponse);
        }
        let description = sanitize_optional_catalog_field(item.description, 1024)?;
        let release_date = sanitize_optional_catalog_field(item.release_date, 64)?;

        entries.push(JevCatalogEntry {
            id: format!("{tool_id}:{name}"),
            tool_id: tool_id.to_string(),
            name: name.to_string(),
            label: name.to_string(),
            model_arg: name.to_string(),
            description,
            release_date,
            efforts: Vec::new(),
            allow_no_effort: true,
        });
    }

    // Never inject any model or version that the server did not return.
    Ok(entries)
}

pub(crate) fn parse_jev_models_response(
    tool_id: &str,
    bytes: &[u8],
) -> Result<Vec<ModelDefinition>, DecisionError> {
    let entries = parse_jev_catalog_entries(tool_id, bytes)?;
    Ok(entries
        .into_iter()
        .map(JevCatalogEntry::into_model_definition)
        .collect())
}

#[derive(Deserialize)]
struct JevClassifyResponse {
    model: String,
    answers: UniqueStringMap<JevAnswerPayload>,
    #[serde(default)]
    usage: Option<JevUsage>,
}

#[derive(Deserialize)]
struct JevAnswerPayload {
    #[serde(rename = "type")]
    answer_type: String,
    #[serde(default)]
    choice: Option<String>,
    #[serde(default)]
    confidence: Option<f64>,
    #[serde(default)]
    probabilities: Option<UniqueStringMap<f64>>,
    #[serde(default)]
    noul: Option<f64>,
}

#[derive(Deserialize)]
struct JevUsage {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
}

pub(crate) fn parse_jev_classify_response(
    requested_model: &str,
    bytes: &[u8],
) -> Result<DecisionAdvice, DecisionError> {
    let parsed: JevClassifyResponse =
        serde_json::from_slice(bytes).map_err(|_| DecisionError::InvalidResponse)?;

    let model = validate_model_echo(DecisionProvider::Jev, requested_model, &parsed.model)?;

    if parsed.answers.0.len() != 1 {
        return Err(DecisionError::InvalidResponse);
    }
    let (question_name, answer) = &parsed.answers.0[0];
    if question_name != QUESTION_NAME {
        return Err(DecisionError::InvalidResponse);
    }

    let (input_tokens, output_tokens) = match parsed.usage {
        Some(usage) => (
            validate_token_count(usage.input_tokens)?,
            validate_token_count(usage.output_tokens)?,
        ),
        None => (None, None),
    };

    // Difficulty is a four-way Choice question (`easy`, `medium`, `hard`, `extreme`).
    // `noul` is a separate yes/no probability primitive in Jev, not abstention or confidence,
    // so any non-`"choice"` answer (including `"noul"`) is invalid for this request.
    if answer.answer_type != "choice" || answer.noul.is_some() {
        return Err(DecisionError::InvalidResponse);
    }

    let raw_choice = answer
        .choice
        .as_deref()
        .ok_or(DecisionError::InvalidResponse)?;
    let difficulty = parse_difficulty_choice(raw_choice)?;
    let raw_confidence = answer.confidence.ok_or(DecisionError::InvalidResponse)?;
    let confidence = validate_unit_interval(raw_confidence)?;

    let probabilities = answer
        .probabilities
        .as_ref()
        .ok_or(DecisionError::InvalidResponse)?;
    validate_four_way_probabilities(difficulty, &probabilities.0)?;

    Ok(build_classified_advice(
        DecisionProvider::Jev,
        model,
        difficulty,
        confidence,
        input_tokens,
        output_tokens,
    ))
}

// ============================================================================
// OpenAI Decisions API Schema
// ============================================================================

#[derive(Serialize)]
struct OpenAiDecisionsRequest<'a> {
    model: &'a str,
    input: &'a str,
    questions: [OpenAiChoiceQuestion<'a>; 1],
}

#[derive(Serialize)]
struct OpenAiChoiceQuestion<'a> {
    #[serde(rename = "type")]
    question_type: &'a str,
    name: &'a str,
    instructions: &'a str,
    choices: [OpenAiChoiceOption<'a>; 4],
}

#[derive(Serialize)]
struct OpenAiChoiceOption<'a> {
    value: &'a str,
    description: &'a str,
}

pub(crate) fn build_openai_classify_body(
    model: &str,
    task_summary: &str,
) -> Result<String, DecisionError> {
    let payload = OpenAiDecisionsRequest {
        model,
        input: task_summary,
        questions: [OpenAiChoiceQuestion {
            question_type: "choice",
            name: QUESTION_NAME,
            instructions: OPENAI_INSTRUCTIONS,
            choices: [
                OpenAiChoiceOption {
                    value: "easy",
                    description: EASY_DESCRIPTION,
                },
                OpenAiChoiceOption {
                    value: "medium",
                    description: MEDIUM_DESCRIPTION,
                },
                OpenAiChoiceOption {
                    value: "hard",
                    description: HARD_DESCRIPTION,
                },
                OpenAiChoiceOption {
                    value: "extreme",
                    description: EXTREME_DESCRIPTION,
                },
            ],
        }],
    };
    serde_json::to_string(&payload).map_err(|_| DecisionError::InvalidRequest)
}

#[derive(Deserialize)]
struct OpenAiModelsResponse {
    data: Vec<OpenAiCatalogEntry>,
}

#[derive(Deserialize)]
struct OpenAiCatalogEntry {
    id: String,
}

pub(crate) fn parse_openai_models_response(
    tool_id: &str,
    bytes: &[u8],
) -> Result<Vec<ModelDefinition>, DecisionError> {
    let parsed: OpenAiModelsResponse =
        serde_json::from_slice(bytes).map_err(|_| DecisionError::InvalidResponse)?;
    if parsed.data.len() > 1024 {
        return Err(DecisionError::InvalidResponse);
    }

    let mut seen = HashSet::with_capacity(parsed.data.len());
    let mut definitions = Vec::new();

    for entry in parsed.data {
        let id = entry.id.trim();
        if !is_valid_model_token(id) {
            return Err(DecisionError::InvalidResponse);
        }
        if !seen.insert(id.to_string()) {
            return Err(DecisionError::InvalidResponse);
        }
        if is_supported_openai_decision_model(id) {
            definitions.push(ModelDefinition {
                id: format!("{tool_id}:{id}"),
                tool_id: tool_id.to_string(),
                label: id.to_string(),
                model_arg: id.to_string(),
                efforts: Vec::new(),
                allow_no_effort: true,
            });
        }
    }

    Ok(definitions)
}

#[derive(Deserialize)]
struct OpenAiDecisionsResponse {
    model: String,
    answers: Vec<OpenAiAnswerPayload>,
    #[serde(default)]
    usage: Option<OpenAiUsage>,
}

#[derive(Deserialize)]
struct OpenAiAnswerPayload {
    #[serde(rename = "type")]
    answer_type: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    choice: Option<String>,
    #[serde(default)]
    confidence: Option<f64>,
    #[serde(default)]
    probabilities: Option<Vec<OpenAiProbabilityEntry>>,
}

#[derive(Deserialize)]
struct OpenAiProbabilityEntry {
    value: String,
    probability: f64,
}

#[derive(Deserialize)]
struct OpenAiUsage {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    total_tokens: Option<u64>,
}

pub(crate) fn parse_openai_classify_response(
    requested_model: &str,
    bytes: &[u8],
) -> Result<DecisionAdvice, DecisionError> {
    let parsed: OpenAiDecisionsResponse =
        serde_json::from_slice(bytes).map_err(|_| DecisionError::InvalidResponse)?;

    let model = validate_model_echo(DecisionProvider::Openai, requested_model, &parsed.model)?;

    if parsed.answers.is_empty() {
        return Err(DecisionError::InvalidResponse);
    }

    let mut seen_names = HashSet::with_capacity(parsed.answers.len());
    for item in &parsed.answers {
        if let Some(name) = item.name.as_deref() {
            if !seen_names.insert(name) {
                return Err(DecisionError::InvalidResponse);
            }
        }
    }

    if parsed.answers.len() != 1 {
        return Err(DecisionError::InvalidResponse);
    }

    let answer = &parsed.answers[0];
    if let Some(name) = answer.name.as_deref() {
        if name != QUESTION_NAME {
            return Err(DecisionError::InvalidResponse);
        }
    }

    let (input_tokens, output_tokens) = match parsed.usage {
        Some(usage) => {
            let _ = validate_token_count(usage.total_tokens)?;
            (
                validate_token_count(usage.input_tokens)?,
                validate_token_count(usage.output_tokens)?,
            )
        }
        None => (None, None),
    };

    match answer.answer_type.as_str() {
        "choice" => {
            let raw_choice = answer
                .choice
                .as_deref()
                .ok_or(DecisionError::InvalidResponse)?;
            let difficulty = parse_difficulty_choice(raw_choice)?;
            let raw_confidence = answer.confidence.ok_or(DecisionError::InvalidResponse)?;
            let confidence = validate_unit_interval(raw_confidence)?;

            if let Some(probabilities) = &answer.probabilities {
                let pairs: Vec<(String, f64)> = probabilities
                    .iter()
                    .map(|entry| (entry.value.clone(), entry.probability))
                    .collect();
                validate_probabilities(difficulty, &pairs)?;
            }

            Ok(build_classified_advice(
                DecisionProvider::Openai,
                model,
                difficulty,
                confidence,
                input_tokens,
                output_tokens,
            ))
        }
        "refusal" => {
            if answer.choice.is_some()
                || answer.confidence.is_some()
                || answer.probabilities.is_some()
            {
                return Err(DecisionError::InvalidResponse);
            }

            Ok(DecisionAdvice {
                provider: DecisionProvider::Openai,
                model,
                difficulty: None,
                confidence: None,
                needs_review: true,
                reason_code: "refusal".to_string(),
                input_tokens,
                output_tokens,
            })
        }
        _ => Err(DecisionError::InvalidResponse),
    }
}
