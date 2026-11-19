//! Offline, read-only packaging for the opt-in ordinary Worker callback closure template.
//!
//! [`render_closure_template`] returns the immutable packaged JavaScript factory source
//! together with its schema/version metadata and SHA-256 digest. It performs no network,
//! supervisor, provider, filesystem, or execution work. The real client may evaluate only
//! this trusted packaged source, never event, provider, or other untrusted text.
//!
//! The template itself derives callback identities from a retained real start receipt,
//! validates a finite sequential pull/ACK/page stream against the verified ACK cursor,
//! retains every actual callback DTO in private evidence, reconciles exact terminal success,
//! and runs exactly one bounded physical inspection with a clean `exec_command` wrapper.
//! A successful report is only `ready_for_semantic_review`; semantic acceptance and guarded
//! completion remain separate Coworker decisions. Clients prepare and privately retain this
//! serializable capsule before starting a Worker, then recreate the factory in each fresh
//! isolate rather than storing executable functions.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Stable schema version for the offline closure template JSON document.
pub const CLOSURE_TEMPLATE_SCHEMA_VERSION: u32 = 1;

/// Canonical offline CLI command name embedded in the template document.
pub const CLOSURE_TEMPLATE_TOOL: &str = "threadlane closure-template";

/// Canonical template version shared with the operating guide and skill metadata.
pub const CLOSURE_TEMPLATE_VERSION: &str = "2026-11-19.v1";

/// Repository-relative path to the trusted packaged JavaScript template source.
pub const CLOSURE_TEMPLATE_SOURCE_PATH: &str = "skills/threadlane/references/ordinary-closure.js";

/// Exported JavaScript factory name in the packaged template source.
pub const CLOSURE_TEMPLATE_FACTORY_NAME: &str = "createOrdinaryClosure";

/// Compile-time embedded trusted JavaScript template source.
pub const CLOSURE_TEMPLATE_JAVASCRIPT: &str =
    include_str!("../../../skills/threadlane/references/ordinary-closure.js");

/// Offline closure template document printed by `threadlane closure-template`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClosureTemplate {
    /// Stable schema version for this JSON document.
    pub schema_version: u32,
    /// Canonical offline CLI command name.
    pub tool: String,
    /// Canonical template version string.
    pub version: String,
    /// Repository-relative path to the packaged source.
    pub source_path: String,
    /// Lowercase SHA-256 digest of the exact embedded source bytes.
    pub source_sha256: String,
    /// Exported JavaScript factory name.
    pub factory_name: String,
    /// Complete trusted packaged JavaScript source text.
    pub javascript: String,
}

/// Returns the current offline closure template document with a freshly computed source digest.
pub fn render_closure_template() -> ClosureTemplate {
    ClosureTemplate {
        schema_version: CLOSURE_TEMPLATE_SCHEMA_VERSION,
        tool: CLOSURE_TEMPLATE_TOOL.to_string(),
        version: CLOSURE_TEMPLATE_VERSION.to_string(),
        source_path: CLOSURE_TEMPLATE_SOURCE_PATH.to_string(),
        source_sha256: closure_template_source_sha256(),
        factory_name: CLOSURE_TEMPLATE_FACTORY_NAME.to_string(),
        javascript: CLOSURE_TEMPLATE_JAVASCRIPT.to_string(),
    }
}

/// Returns the lowercase SHA-256 digest of the exact embedded template source bytes.
pub fn closure_template_source_sha256() -> String {
    let mut hasher = Sha256::new();
    hasher.update(CLOSURE_TEMPLATE_JAVASCRIPT.as_bytes());
    let mut hex = String::with_capacity(64);
    for byte in hasher.finalize() {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

/// Serializes the offline closure template document as compact JSON.
pub fn render_closure_template_json() -> Result<String, serde_json::Error> {
    serde_json::to_string(&render_closure_template())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_template_has_stable_metadata_and_digest() {
        let template = render_closure_template();
        assert_eq!(template.schema_version, 1);
        assert_eq!(template.tool, "threadlane closure-template");
        assert_eq!(template.version, "2026-11-19.v1");
        assert_eq!(
            template.source_path,
            "skills/threadlane/references/ordinary-closure.js"
        );
        assert_eq!(template.factory_name, "createOrdinaryClosure");
        assert_eq!(template.javascript, CLOSURE_TEMPLATE_JAVASCRIPT);
        assert_eq!(template.source_sha256, closure_template_source_sha256());
        assert_eq!(template.source_sha256.len(), 64);
        assert!(template
            .source_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
        assert!(template.javascript.contains("createOrdinaryClosure"));
        assert!(template.javascript.contains("ready_for_semantic_review"));
        assert!(template.javascript.contains("doneView"));
        assert!(template
            .javascript
            .contains("ORDINARY_CLOSURE_MAX_BATCH_BYTES = 49152"));
        assert!(template.javascript.contains("batch_too_large"));
        assert!(template.javascript.contains("immutable_execution_mismatch"));
        assert!(template.javascript.contains("utf8ByteLength"));
        assert!(template.javascript.contains("event step_seq"));
    }

    #[test]
    fn compact_json_round_trips_without_unknown_fields() {
        let serialized = render_closure_template_json().expect("serialize closure template");
        let decoded: ClosureTemplate =
            serde_json::from_str(&serialized).expect("deserialize closure template");
        assert_eq!(decoded, render_closure_template());
        assert!(!serialized.contains('\n'), "offline JSON is compact stdout");

        let mut value: serde_json::Value =
            serde_json::from_str(&serialized).expect("parse JSON value");
        value
            .as_object_mut()
            .expect("template JSON object")
            .insert("unexpected".to_string(), serde_json::Value::Bool(true));
        assert!(
            serde_json::from_value::<ClosureTemplate>(value).is_err(),
            "unknown fields must be rejected"
        );
    }
}
