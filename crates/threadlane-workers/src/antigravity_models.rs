//! Versioned Antigravity (`agy`) CLI catalog model variant contract.
//!
//! Antigravity 1.4.12 advertises 13 concrete model identifiers in `agy models` where each model
//! identifier is bound to a single reasoning-effort label (`(Low)`, `(Medium)`, or `(High)`),
//! while `--output-format stream-json` emits the selected concrete model identifier in
//! `{"event":"init","init":{"model":"<id>",...}}` without a separate `effort` field.
//!
//! This module defines the finite, versioned exact-match lookup table for those 13 observed
//! Antigravity 1.4.12 catalog model identifiers.
//!
//! ## Security and Verification Invariants
//!
//! - **Exact allowlist only**: Only the 13 observed Antigravity 1.4.12 catalog identifiers match.
//!   Never performs suffix guessing (such as `*-high`), substring matching, case folding, or
//!   family fallback.
//! - **Pure and offline**: Performs no filesystem, process, or network I/O and never falls back
//!   to caller-requested `WorkerSpec::effort`.
//! - **Provenance boundary**: Evidence derived from this table records CLI-reported concrete
//!   model variant selection in structured `event: "init"` (`init.model`), **not** independent
//!   cloud or reasoning-budget attestation.

/// Version identifier for the bounded Antigravity catalog model-variant evidence contract.
pub const ANTIGRAVITY_CATALOG_CONTRACT_VERSION: &str = "agy-1.4.12-catalog-v1";

/// Observed Antigravity CLI version from which the 13 concrete catalog model variants were recorded.
pub const ANTIGRAVITY_OBSERVED_CLI_VERSION: &str = "1.4.12";

/// Exhaustive table of the 13 observed `(model_id, effort)` pairs from Antigravity 1.4.12 `models`.
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
];

/// Returns the canonical encoded effort (`"low"`, `"medium"`, or `"high"`) for an exact known
/// Antigravity 1.4.12 catalog model identifier, or `None` for any unknown identifier.
///
/// This function is a pure exact-match lookup over the 13 observed Antigravity 1.4.12 catalog
/// model IDs. It never infers effort from arbitrary `*-low` / `*-medium` / `*-high` suffixes,
/// never consults requested spec values, and represents CLI-reported model variant selection
/// rather than independent reasoning-budget attestation.
#[must_use]
pub fn antigravity_catalog_effort(model_id: &str) -> Option<&'static str> {
    ANTIGRAVITY_KNOWN_CATALOG_MODELS
        .iter()
        .find_map(|(known, effort)| (*known == model_id).then_some(*effort))
}
