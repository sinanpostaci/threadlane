use serde::{Deserialize, Serialize};
use threadlane_registry::Difficulty;

/// Documented Jev alias model identifier.
///
/// `jev-latest` is an upstream alias and is never injected into `/v1/models` catalog responses
/// or used as an automatic fallback to rewrite an explicit model selection.
pub const DEFAULT_JEV_MODEL: &str = "jev-latest";

/// Supported OpenAI Decisions API model identifier.
pub const DEFAULT_OPENAI_DECISION_MODEL: &str = "gpt-6-luna";

/// Fixed synthetic task summary used for explicit UI decision tests.
pub const SYNTHETIC_TEST_TASK_SUMMARY: &str =
    "Rename one local variable without changing behavior.";

/// Minimum confidence threshold below which advisory classifications require review.
pub const MIN_CONFIDENCE_THRESHOLD: f64 = 0.75;

/// Maximum integer representable without precision loss in IEEE-754 double precision (`2^53 - 1`).
pub const MAX_JS_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// Supported external decision providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionProvider {
    /// TypeSafe Jev System One API (`https://api.typesafe.ai/v1/systemone`).
    Jev,
    /// OpenAI Decisions API (`https://api.openai.com/v1/decisions`).
    #[serde(rename = "openai", alias = "open_ai")]
    Openai,
}

impl DecisionProvider {
    /// CamelCase alias for [`DecisionProvider::Openai`].
    #[allow(non_upper_case_globals)]
    pub const OpenAi: Self = Self::Openai;

    /// Returns the canonical snake_case identifier for this provider.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Jev => "jev",
            Self::Openai => "openai",
        }
    }
}

impl std::fmt::Display for DecisionProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Advisory difficulty classification request.
///
/// `Debug` output intentionally redacts `task_summary` so raw task summaries do not leak into logs.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionRequest {
    /// Registry tool identifier used to look up the API key in the credential store.
    pub tool_id: String,
    /// Target decision provider.
    pub provider: DecisionProvider,
    /// Requested provider model identifier (e.g. `"jev-latest"`, `"jev-1.14.0"`, or `"gpt-6-luna"`).
    pub model: String,
    /// Bounded task summary to classify.
    pub task_summary: String,
}

impl std::fmt::Debug for DecisionRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecisionRequest")
            .field("tool_id", &self.tool_id)
            .field("provider", &self.provider)
            .field("model", &self.model)
            .field("task_summary", &"[REDACTED]")
            .finish()
    }
}

/// Validated advisory result returned by a decision provider.
///
/// Decision advice is strictly advisory and never constitutes automatic plan approval,
/// scope escalation, or model fallback.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionAdvice {
    /// Provider that produced this advice.
    pub provider: DecisionProvider,
    /// Validated model identifier echoed by the provider.
    pub model: String,
    /// Classified difficulty when the provider returned a valid choice answer, or `None` on provider refusal.
    pub difficulty: Option<Difficulty>,
    /// Validated finite confidence in `[0.0, 1.0]` when available.
    pub confidence: Option<f64>,
    /// `true` when the classification is low-confidence or refused and requires operator review.
    pub needs_review: bool,
    /// Machine-readable reason code (`"classified"`, `"low_confidence"`, or `"refusal"`).
    pub reason_code: String,
    /// Reported input token count when present and within JS-safe bounds.
    pub input_tokens: Option<u64>,
    /// Reported output token count when present and within JS-safe bounds.
    pub output_tokens: Option<u64>,
}
