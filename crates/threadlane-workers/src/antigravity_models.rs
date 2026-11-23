//! Versioned Antigravity (`agy`) CLI catalog model variant contract.
//!
//! This contract retains the 20 exact variants recorded through AGY 1.5.9 and adds the four
//! Gemini 4 Neon variants reported by AGY 1.5.12. Structured `init.model` identifies the
//! selected variant even when the stream omits a separate effort field.
//!
//! Unlabeled orchestrator IDs have no encoded reasoning effort.
//!
//! ## Security and Verification Invariants
//!
//! - **Exact allowlist only**: Only the exact recorded catalog identifiers match.
//!   Never performs suffix guessing (such as `*-high` or `*-xhigh`), substring matching, case
//!   folding, or family fallback.
//! - **Pure and offline**: Performs no filesystem, process, or network I/O and never falls back
//!   to caller-requested `WorkerSpec::effort`.
//! - **Provenance boundary**: Evidence derived from this table records CLI-reported concrete
//!   model variant selection in structured `event: "init"` (`init.model`), **not** independent
//!   cloud or reasoning-budget attestation.

/// Version identifier for the bounded Antigravity catalog model-variant evidence contract.
pub const ANTIGRAVITY_CATALOG_CONTRACT_VERSION: &str = "agy-1.5.12-catalog-v1";

/// Most recent Antigravity CLI version contributing verified catalog variants.
pub const ANTIGRAVITY_OBSERVED_CLI_VERSION: &str = "1.5.12";

/// Exact AGY catalog model IDs that may be selected without a reasoning effort.
/// This list does not assert an inferred provider reasoning budget.
pub const ANTIGRAVITY_NO_EFFORT_MODELS: &[&str] =
    &["gemini-pro-orchestrator-neon", "gemini-pro-orchestrator"];

/// Whether an exact AGY catalog model supports the separately approved no-effort route.
#[must_use]
pub fn antigravity_no_effort_model(model_id: &str) -> bool {
    ANTIGRAVITY_NO_EFFORT_MODELS.contains(&model_id)
}

/// Exact model IDs and their catalog-reported effort, including historical variants.
pub const ANTIGRAVITY_KNOWN_CATALOG_MODELS: &[(&str, &str)] = &[
    ("gemini-4-argon-low", "low"),
    ("gemini-4-argon-medium", "medium"),
    ("gemini-4-argon-high", "high"),
    ("gemini-3.8-flash-low", "low"),
    ("gemini-3.8-flash-medium", "medium"),
    ("gemini-3.8-flash-high", "high"),
    ("claude-opus-5-5-low", "low"),
    ("claude-opus-5-5-medium", "medium"),
    ("claude-opus-5-5-high", "high"),
    ("claude-sonnet-5-5-low", "low"),
    ("claude-sonnet-5-5-medium", "medium"),
    ("claude-sonnet-5-5-high", "high"),
    ("gpt-oss-120b-medium", "medium"),
    ("gemini-4-carbon-low", "low"),
    ("gemini-4-carbon-medium", "medium"),
    ("gemini-4-carbon-high", "high"),
    ("gemini-4-hydrogen-low", "low"),
    ("gemini-4-hydrogen-medium", "medium"),
    ("gemini-4-hydrogen-high", "high"),
    ("gemini-4-hydrogen-xhigh", "xhigh"),
    ("gemini-4-neon-low", "low"),
    ("gemini-4-neon-medium", "medium"),
    ("gemini-4-neon-high", "high"),
    ("gemini-4-neon-xhigh", "xhigh"),
];

/// Returns the canonical encoded effort (`"low"`, `"medium"`, `"high"`, or `"xhigh"`) for an
/// exact known Antigravity catalog model identifier, or `None` for any unknown identifier.
///
/// This function is a pure exact-match lookup over the recorded Antigravity catalog model IDs.
/// It never infers effort from arbitrary `*-low` / `*-medium` / `*-high` / `*-xhigh`
/// suffixes, never consults requested spec values, and represents CLI-reported model variant
/// selection rather than independent reasoning-budget attestation.
#[must_use]
pub fn antigravity_catalog_effort(model_id: &str) -> Option<&'static str> {
    ANTIGRAVITY_KNOWN_CATALOG_MODELS
        .iter()
        .find_map(|(known, effort)| (*known == model_id).then_some(*effort))
}
