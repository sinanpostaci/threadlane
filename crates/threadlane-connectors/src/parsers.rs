use std::collections::BTreeSet;

use serde_json::Value;
use threadlane_registry::ModelDefinition;

use crate::credentials::validate_tool_id;
use crate::error::ConnectorError;

/// Maximum number of models retained from a single catalog probe (matches registry cap).
pub const MAX_CATALOG_MODELS: usize = 256;

/// Maximum byte length of a model identifier or argument.
pub const MAX_MODEL_ID_BYTES: usize = 128;

/// Maximum character length of a model display label.
pub const MAX_MODEL_LABEL_CHARS: usize = 128;

/// Maximum byte length of a reasoning effort identifier.
pub const MAX_EFFORT_BYTES: usize = 64;

/// Maximum byte length of a parsed CLI version string.
pub const MAX_VERSION_BYTES: usize = 64;

/// Auth status reported when a CLI explicitly confirms an active login session.
pub const AUTH_STATUS_AUTHENTICATED: &str = "authenticated";

/// Auth status reported when a CLI explicitly confirms no active login session exists.
pub const AUTH_STATUS_UNAUTHENTICATED: &str = "unauthenticated";

/// Auth status reported when login state cannot be safely verified or is not applicable.
pub const AUTH_STATUS_UNKNOWN: &str = "unknown";

/// Strips ANSI CSI/OSC escape sequences from CLI text output before deterministic parsing.
pub fn strip_ansi_codes(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            match chars.peek().copied() {
                Some('[') => {
                    let _ = chars.next();
                    for c in chars.by_ref() {
                        if ('@'..='~').contains(&c) {
                            break;
                        }
                    }
                }
                Some(']') => {
                    let _ = chars.next();
                    while let Some(c) = chars.next() {
                        if c == '\u{07}' {
                            break;
                        }
                        if c == '\u{1b}' && chars.peek() == Some(&'\\') {
                            let _ = chars.next();
                            break;
                        }
                    }
                }
                _ => {}
            }
        } else if ch != '\r' {
            out.push(ch);
        }
    }

    out
}

/// Validates that `model_id` is a safe, canonical model identifier (`slug` or `provider/model`).
pub fn validate_model_id(model_id: &str) -> Result<(), ConnectorError> {
    if model_id.is_empty() || model_id.len() > MAX_MODEL_ID_BYTES || model_id.trim() != model_id {
        return Err(ConnectorError::ParseError(
            "Catalog contained an empty or oversized model identifier.",
        ));
    }
    if model_id.starts_with('-')
        || model_id.starts_with('.')
        || model_id.starts_with('/')
        || model_id.ends_with('/')
        || model_id.contains("//")
        || model_id.contains("..")
    {
        return Err(ConnectorError::ParseError(
            "Catalog contained a malformed model identifier.",
        ));
    }
    if !model_id
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '/' | ':'))
    {
        return Err(ConnectorError::ParseError(
            "Catalog model identifier contained unsupported characters.",
        ));
    }
    Ok(())
}

/// Validates that `effort` is a safe reasoning effort token.
pub fn validate_effort_token(effort: &str) -> Result<(), ConnectorError> {
    if effort.is_empty() || effort.len() > MAX_EFFORT_BYTES || effort.trim() != effort {
        return Err(ConnectorError::ParseError(
            "Catalog contained an empty or oversized reasoning effort.",
        ));
    }
    if effort.starts_with('-') || effort.starts_with('.') {
        return Err(ConnectorError::ParseError(
            "Catalog reasoning effort must not start with '-' or '.'.",
        ));
    }
    if !effort
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
    {
        return Err(ConnectorError::ParseError(
            "Catalog reasoning effort contained invalid characters.",
        ));
    }
    Ok(())
}

fn sanitize_display_label(raw: &str, fallback: &str) -> String {
    let cleaned: String = strip_ansi_codes(raw)
        .chars()
        .filter(|ch| !ch.is_control())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    if cleaned.is_empty() {
        return fallback.to_string();
    }

    cleaned.chars().take(MAX_MODEL_LABEL_CHARS).collect()
}

fn is_codex_visibility_exposed(
    entry: &serde_json::Map<String, Value>,
) -> Result<bool, ConnectorError> {
    if entry.get("hidden").and_then(Value::as_bool) == Some(true) {
        return Ok(false);
    }
    if entry.get("enabled").and_then(Value::as_bool) == Some(false) {
        return Ok(false);
    }

    match entry.get("visibility") {
        None | Some(Value::Null) => Ok(true),
        Some(Value::String(vis)) => {
            let norm = vis.trim().to_ascii_lowercase();
            match norm.as_str() {
                "list" | "visible" | "public" | "show" | "default" => Ok(true),
                "hide" | "hidden" | "none" | "internal" | "private" | "unlisted" => Ok(false),
                _ => Ok(false),
            }
        }
        Some(_) => Err(ConnectorError::ParseError(
            "Codex catalog entry contained a non-string visibility field.",
        )),
    }
}

fn extract_effort_from_value(val: &Value) -> Result<Option<String>, ConnectorError> {
    match val {
        Value::Null => Ok(None),
        Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                validate_effort_token(trimmed)?;
                Ok(Some(trimmed.to_string()))
            }
        }
        Value::Object(map) => match map.get("effort") {
            Some(Value::String(s)) => {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    Ok(None)
                } else {
                    validate_effort_token(trimmed)?;
                    Ok(Some(trimmed.to_string()))
                }
            }
            Some(Value::Null) | None => Err(ConnectorError::ParseError(
                "Codex supported_reasoning_levels entry was missing an effort string.",
            )),
            Some(_) => Err(ConnectorError::ParseError(
                "Codex supported_reasoning_levels effort field must be a string.",
            )),
        },
        _ => Err(ConnectorError::ParseError(
            "Codex supported_reasoning_levels entry had an invalid type.",
        )),
    }
}

/// Parses `codex debug models --bundled` JSON output into [`ModelDefinition`] items.
///
/// Reads `models[].slug`, `display_name`, `supported_reasoning_levels[].effort`,
/// `default_reasoning_level`, and `visibility`, filtering out hidden catalog entries
/// (`visibility: "hide"` / `"hidden"` / `"none"`).
pub fn parse_codex_bundled_models(
    tool_id: &str,
    raw_json: &str,
) -> Result<Vec<ModelDefinition>, ConnectorError> {
    validate_tool_id(tool_id)?;
    let trimmed = raw_json.trim();
    if trimmed.is_empty() {
        return Err(ConnectorError::ParseError(
            "Codex bundled model catalog output was empty.",
        ));
    }

    let parsed: Value = serde_json::from_str(trimmed).map_err(|_| {
        ConnectorError::ParseError("Codex bundled model catalog output was not valid JSON.")
    })?;

    let items = match &parsed {
        Value::Object(map) => {
            map.get("models")
                .and_then(Value::as_array)
                .ok_or(ConnectorError::ParseError(
                    "Codex bundled model catalog JSON did not contain a 'models' array.",
                ))?
        }
        Value::Array(arr) => arr,
        _ => {
            return Err(ConnectorError::ParseError(
                "Codex bundled model catalog JSON root must be an object or array.",
            ));
        }
    };

    let mut models = Vec::new();
    let mut seen_ids = BTreeSet::new();

    for item in items {
        let obj = item.as_object().ok_or(ConnectorError::ParseError(
            "Codex model catalog entry must be a JSON object.",
        ))?;

        let slug = obj
            .get("slug")
            .and_then(Value::as_str)
            .map(str::trim)
            .ok_or(ConnectorError::ParseError(
                "Codex model catalog entry was missing a string 'slug' field.",
            ))?;
        validate_model_id(slug)?;

        if !is_codex_visibility_exposed(obj)? {
            continue;
        }

        if !seen_ids.insert(slug.to_string()) {
            continue;
        }

        let label = obj
            .get("display_name")
            .and_then(Value::as_str)
            .map(|name| sanitize_display_label(name, slug))
            .unwrap_or_else(|| slug.to_string());

        let mut efforts = Vec::new();
        let mut seen_efforts = BTreeSet::new();
        if let Some(levels_val) = obj.get("supported_reasoning_levels") {
            if !levels_val.is_null() {
                let levels = levels_val.as_array().ok_or(ConnectorError::ParseError(
                    "Codex supported_reasoning_levels field must be an array.",
                ))?;
                for level in levels {
                    if let Some(effort) = extract_effort_from_value(level)? {
                        if seen_efforts.insert(effort.clone()) {
                            efforts.push(effort);
                        }
                    }
                }
            }
        }

        let default_effort = match obj.get("default_reasoning_level") {
            Some(val) => extract_effort_from_value(val)?,
            None => None,
        };
        if let Some(ref def_eff) = default_effort {
            if !efforts.is_empty() && seen_efforts.insert(def_eff.clone()) {
                efforts.push(def_eff.clone());
            }
        }

        let allow_no_effort = default_effort.is_some() || efforts.is_empty();

        models.push(ModelDefinition {
            id: slug.to_string(),
            tool_id: tool_id.to_string(),
            label,
            model_arg: slug.to_string(),
            efforts,
            allow_no_effort,
        });

        if models.len() >= MAX_CATALOG_MODELS {
            break;
        }
    }

    Ok(models)
}

/// Classifies `codex login status` process outcome into a safe static status string
/// (`authenticated`, `unauthenticated`, or `unknown`) without exposing raw CLI output or tokens.
pub fn parse_codex_login_status(
    exit_code: Option<i32>,
    stdout: &str,
    stderr: &str,
) -> &'static str {
    let clean_out = strip_ansi_codes(stdout).to_ascii_lowercase();
    let clean_err = strip_ansi_codes(stderr).to_ascii_lowercase();
    let combined = format!("{clean_out}\n{clean_err}");

    const UNAUTH_MARKERS: &[&str] = &[
        "not logged in",
        "logged out",
        "unauthenticated",
        "not authenticated",
        "no active login",
        "login required",
        "please log in",
        "no credentials",
        "\"authenticated\":false",
        "\"authenticated\": false",
        "\"status\":\"unauthenticated\"",
        "\"status\": \"unauthenticated\"",
        "\"status\":\"logged_out\"",
        "\"status\": \"logged_out\"",
    ];

    if UNAUTH_MARKERS.iter().any(|m| combined.contains(m)) {
        return AUTH_STATUS_UNAUTHENTICATED;
    }

    if exit_code == Some(0) {
        const AUTH_MARKERS: &[&str] = &[
            "logged in",
            "authenticated",
            "active account",
            "chatgpt",
            "api key",
            "\"authenticated\":true",
            "\"authenticated\": true",
            "\"status\":\"authenticated\"",
            "\"status\": \"authenticated\"",
        ];
        if AUTH_MARKERS.iter().any(|m| combined.contains(m)) {
            return AUTH_STATUS_AUTHENTICATED;
        }
    }

    AUTH_STATUS_UNKNOWN
}

fn parse_unverified_json_catalog(
    tool_id: &str,
    trimmed: &str,
) -> Result<Vec<ModelDefinition>, ConnectorError> {
    let parsed: Value = serde_json::from_str(trimmed)
        .map_err(|_| ConnectorError::ParseError("Model catalog JSON output was malformed."))?;

    let items = match &parsed {
        Value::Object(map) => {
            map.get("models")
                .and_then(Value::as_array)
                .ok_or(ConnectorError::ParseError(
                    "Model catalog JSON object did not contain a 'models' array.",
                ))?
        }
        Value::Array(arr) => arr,
        _ => {
            return Err(ConnectorError::ParseError(
                "Model catalog JSON root must be an object or array.",
            ));
        }
    };

    let mut models = Vec::new();
    let mut seen_ids = BTreeSet::new();

    for item in items {
        let (model_id, label) = match item {
            Value::String(s) => {
                let id = s.trim();
                validate_model_id(id)?;
                (id.to_string(), id.to_string())
            }
            Value::Object(obj) => {
                let raw_id = obj
                    .get("id")
                    .or_else(|| obj.get("slug"))
                    .or_else(|| obj.get("name"))
                    .or_else(|| obj.get("model"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .ok_or(ConnectorError::ParseError(
                        "Model catalog JSON entry was missing a string identifier.",
                    ))?;
                validate_model_id(raw_id)?;
                let lbl = obj
                    .get("label")
                    .or_else(|| obj.get("display_name"))
                    .or_else(|| obj.get("name"))
                    .and_then(Value::as_str)
                    .map(|s| sanitize_display_label(s, raw_id))
                    .unwrap_or_else(|| raw_id.to_string());
                (raw_id.to_string(), lbl)
            }
            _ => {
                return Err(ConnectorError::ParseError(
                    "Model catalog JSON entry must be a string or object.",
                ));
            }
        };

        if seen_ids.insert(model_id.clone()) {
            models.push(ModelDefinition {
                id: model_id.clone(),
                tool_id: tool_id.to_string(),
                label,
                model_arg: model_id,
                // Catalog unverified efforts remain manual; no guessed per-model support.
                efforts: Vec::new(),
                allow_no_effort: true,
            });
            if models.len() >= MAX_CATALOG_MODELS {
                break;
            }
        }
    }

    Ok(models)
}

fn is_noise_header_or_separator(line: &str) -> bool {
    if line.chars().all(|c| matches!(c, '-' | '=' | '_')) {
        return true;
    }
    let lower = line.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "models"
            | "models:"
            | "available models"
            | "available models:"
            | "id"
            | "model"
            | "id name"
            | "model description"
    )
}

fn is_disallowed_noise_word(token: &str) -> bool {
    let lower = token.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "error"
            | "warning"
            | "fatal"
            | "usage"
            | "help"
            | "none"
            | "null"
            | "unknown"
            | "models"
            | "available"
    )
}

/// Parses `opencode models --standalone` output into [`ModelDefinition`] items.
///
/// Unverified per-model reasoning efforts are never guessed (`efforts` is empty and
/// `allow_no_effort` is `true` until configured by the user).
pub fn parse_opencode_models(
    tool_id: &str,
    raw_output: &str,
) -> Result<Vec<ModelDefinition>, ConnectorError> {
    validate_tool_id(tool_id)?;
    let cleaned = strip_ansi_codes(raw_output);
    let trimmed = cleaned.trim();
    if trimmed.is_empty() {
        return Err(ConnectorError::ParseError(
            "OpenCode model catalog output was empty.",
        ));
    }

    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return parse_unverified_json_catalog(tool_id, trimmed);
    }

    let mut models = Vec::new();
    let mut seen_ids = BTreeSet::new();

    for raw_line in trimmed.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') || is_noise_header_or_separator(line) {
            continue;
        }
        let candidate = line
            .strip_prefix("- ")
            .or_else(|| line.strip_prefix("* "))
            .unwrap_or(line)
            .trim();

        validate_model_id(candidate)?;
        if is_disallowed_noise_word(candidate) {
            return Err(ConnectorError::ParseError(
                "OpenCode model catalog contained an unrecognized status line.",
            ));
        }

        if seen_ids.insert(candidate.to_string()) {
            models.push(ModelDefinition {
                id: candidate.to_string(),
                tool_id: tool_id.to_string(),
                label: candidate.to_string(),
                model_arg: candidate.to_string(),
                efforts: Vec::new(),
                allow_no_effort: true,
            });
            if models.len() >= MAX_CATALOG_MODELS {
                break;
            }
        }
    }

    if models.is_empty() {
        return Err(ConnectorError::ParseError(
            "OpenCode model catalog output did not contain any valid models.",
        ));
    }

    Ok(models)
}

/// Parses Antigravity (`agy models`) output into [`ModelDefinition`] items.
///
/// Unverified per-model reasoning efforts are never guessed (`efforts` is empty and
/// `allow_no_effort` is `true` until configured by the user).
pub fn parse_antigravity_models(
    tool_id: &str,
    raw_output: &str,
) -> Result<Vec<ModelDefinition>, ConnectorError> {
    validate_tool_id(tool_id)?;
    let cleaned = strip_ansi_codes(raw_output);
    let trimmed = cleaned.trim();
    if trimmed.is_empty() {
        return Err(ConnectorError::ParseError(
            "Antigravity model catalog output was empty.",
        ));
    }

    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return parse_unverified_json_catalog(tool_id, trimmed);
    }

    let mut models = Vec::new();
    let mut seen_ids = BTreeSet::new();

    for raw_line in trimmed.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') || is_noise_header_or_separator(line) {
            continue;
        }

        let stripped = line
            .strip_prefix("- ")
            .or_else(|| line.strip_prefix("* "))
            .or_else(|| line.strip_prefix("• "))
            .unwrap_or(line)
            .trim();

        let (model_id, label) = match stripped.split_once(char::is_whitespace) {
            None => {
                validate_model_id(stripped)?;
                if is_disallowed_noise_word(stripped) {
                    return Err(ConnectorError::ParseError(
                        "Antigravity model catalog contained an unrecognized status line.",
                    ));
                }
                (stripped.to_string(), stripped.to_string())
            }
            Some((first, rest)) => {
                let id = first.trim();
                validate_model_id(id)?;
                // Require multi-token lines to begin with a lowercase/digit slug containing
                // model punctuation or digits so prose sentences are rejected.
                let starts_valid = id
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
                let has_slug_marker = id
                    .chars()
                    .any(|c| c.is_ascii_digit() || matches!(c, '-' | '_' | '.' | '/'));
                if !starts_valid || !has_slug_marker || is_disallowed_noise_word(id) {
                    return Err(ConnectorError::ParseError(
                        "Antigravity model catalog contained a non-model text line.",
                    ));
                }

                let raw_label = rest
                    .trim()
                    .trim_start_matches('-')
                    .trim_start_matches(':')
                    .trim();
                let unparenthesized = raw_label
                    .strip_prefix('(')
                    .and_then(|s| s.strip_suffix(')'))
                    .unwrap_or(raw_label)
                    .trim();
                let lbl = sanitize_display_label(unparenthesized, id);
                (id.to_string(), lbl)
            }
        };

        if seen_ids.insert(model_id.clone()) {
            models.push(ModelDefinition {
                id: model_id.clone(),
                tool_id: tool_id.to_string(),
                label,
                model_arg: model_id,
                efforts: Vec::new(),
                allow_no_effort: true,
            });
            if models.len() >= MAX_CATALOG_MODELS {
                break;
            }
        }
    }

    if models.is_empty() {
        return Err(ConnectorError::ParseError(
            "Antigravity model catalog output did not contain any valid models.",
        ));
    }

    Ok(models)
}

/// Extracts a clean, bounded version token from CLI `--version` or `-V` output.
///
/// Never returns raw multi-line output, file paths, or tokens that do not begin with a version digit.
pub fn parse_cli_version(stdout: &str, stderr: &str) -> Option<String> {
    let clean_out = strip_ansi_codes(stdout);
    let clean_err = strip_ansi_codes(stderr);
    let first_line = clean_out
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .or_else(|| clean_err.lines().map(str::trim).find(|l| !l.is_empty()))?;

    for raw_token in first_line.split_whitespace() {
        let trimmed_token =
            raw_token.trim_matches(|c: char| matches!(c, '(' | ')' | '[' | ']' | ',' | ';'));
        let token = trimmed_token
            .strip_prefix('v')
            .or_else(|| trimmed_token.strip_prefix('V'))
            .unwrap_or(trimmed_token);

        if token.is_empty() || token.len() > MAX_VERSION_BYTES {
            continue;
        }
        let first_char = token.chars().next()?;
        if !first_char.is_ascii_digit() {
            continue;
        }
        if token
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '+' | '_'))
        {
            return Some(token.to_string());
        }
    }

    None
}
