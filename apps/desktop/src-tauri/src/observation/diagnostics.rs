use std::collections::BTreeSet;

use threadlane_core::observation::{
    sanitize_observation_summary, validate_correlation_id, ObservationWarningContext,
};

use super::{sanitize_and_bound_warnings, MAX_OBSERVATION_WARNINGS};

pub(crate) const WARNING_ORIGIN_REGISTRY: &str = "registry";
pub(crate) const WARNING_ORIGIN_JOBS: &str = "jobs";
pub(crate) const WARNING_ORIGIN_JOURNAL: &str = "journal";
pub(crate) const WARNING_ORIGIN_REQUESTS: &str = "requests";
pub(crate) const WARNING_ORIGIN_SNAPSHOT: &str = "snapshot";

/// Normalizes an observation warning origin token to one of the fixed safe subsystem identifiers.
pub(crate) fn sanitize_warning_origin(raw: &str) -> &'static str {
    match raw.trim() {
        WARNING_ORIGIN_REGISTRY => WARNING_ORIGIN_REGISTRY,
        WARNING_ORIGIN_JOBS => WARNING_ORIGIN_JOBS,
        WARNING_ORIGIN_JOURNAL => WARNING_ORIGIN_JOURNAL,
        WARNING_ORIGIN_REQUESTS => WARNING_ORIGIN_REQUESTS,
        WARNING_ORIGIN_SNAPSHOT => WARNING_ORIGIN_SNAPSHOT,
        _ => WARNING_ORIGIN_SNAPSHOT,
    }
}

/// Validates an optional observation source identifier, rejecting paths, whitespace, or oversized strings.
pub(crate) fn sanitize_warning_source_id(raw: Option<&str>) -> Option<String> {
    let candidate = raw?.trim();
    if candidate.is_empty() || candidate != raw? {
        return None;
    }
    validate_correlation_id(candidate)
}

/// Sanitizes, filters to surviving warnings, deduplicates, and bounds structured warning contexts.
pub(crate) fn sanitize_and_bound_warning_contexts(
    raw_contexts: impl IntoIterator<Item = ObservationWarningContext>,
    surviving_warnings: &[String],
) -> Vec<ObservationWarningContext> {
    let surviving_set: BTreeSet<&str> = surviving_warnings
        .iter()
        .map(|warning| warning.trim())
        .filter(|warning| !warning.is_empty())
        .collect();

    let mut seen = BTreeSet::new();
    let mut bounded = Vec::new();

    for raw in raw_contexts {
        let sanitized = sanitize_observation_summary(&raw.warning);
        let clean_warning = sanitized.trim();
        if clean_warning.is_empty() || !surviving_set.contains(clean_warning) {
            continue;
        }

        let clean_source_id = sanitize_warning_source_id(raw.source_id.as_deref());
        let clean_origin = sanitize_warning_origin(&raw.origin).to_string();
        let warning_owned = clean_warning.to_string();

        if seen.insert((
            warning_owned.clone(),
            clean_source_id.clone(),
            clean_origin.clone(),
        )) {
            bounded.push(ObservationWarningContext {
                warning: warning_owned,
                source_id: clean_source_id,
                origin: clean_origin,
            });
            if bounded.len() >= MAX_OBSERVATION_WARNINGS {
                break;
            }
        }
    }

    bounded
}

/// Accumulates raw warnings together with their verified source/origin provenance before snapshot bounding.
#[derive(Debug, Default, Clone)]
pub(crate) struct ObservationWarningCollector {
    raw_warnings: Vec<String>,
    raw_contexts: Vec<ObservationWarningContext>,
}

impl ObservationWarningCollector {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn extend_inventory(&mut self, warnings: impl IntoIterator<Item = String>) {
        for warning in warnings {
            self.raw_warnings.push(warning.clone());
            self.raw_contexts.push(ObservationWarningContext {
                warning,
                source_id: None,
                origin: WARNING_ORIGIN_REGISTRY.to_string(),
            });
        }
    }

    pub(crate) fn extend_source(
        &mut self,
        source_id: &str,
        origin: &str,
        warnings: impl IntoIterator<Item = String>,
    ) {
        let validated_source_id = sanitize_warning_source_id(Some(source_id));
        let validated_origin = sanitize_warning_origin(origin);
        for warning in warnings {
            self.raw_warnings.push(warning.clone());
            self.raw_contexts.push(ObservationWarningContext {
                warning,
                source_id: validated_source_id.clone(),
                origin: validated_origin.to_string(),
            });
        }
    }

    pub(crate) fn push_snapshot(&mut self, warning: String) {
        self.raw_warnings.push(warning.clone());
        self.raw_contexts.push(ObservationWarningContext {
            warning,
            source_id: None,
            origin: WARNING_ORIGIN_SNAPSHOT.to_string(),
        });
    }

    pub(crate) fn finish(self) -> (Vec<String>, Vec<ObservationWarningContext>) {
        let warnings = sanitize_and_bound_warnings(self.raw_warnings);
        let warning_contexts = sanitize_and_bound_warning_contexts(self.raw_contexts, &warnings);
        (warnings, warning_contexts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use threadlane_core::observation::{ObservationSnapshot, OBSERVATION_SCHEMA_VERSION};

    #[test]
    fn same_warning_from_two_sources_retains_both_provenances_and_deduplicates_identical_triples() {
        let shared_warning =
            "Skipped unreadable or invalid coworker workflow ledger in observation source."
                .to_string();
        let journal_warning =
            "Ignored unverified job, plan, task, or approval hint on journal record.".to_string();

        let mut collector = ObservationWarningCollector::new();
        collector.extend_source(
            "src_alpha",
            WARNING_ORIGIN_JOBS,
            vec![shared_warning.clone(), shared_warning.clone()],
        );
        collector.extend_source(
            "src_beta",
            WARNING_ORIGIN_JOBS,
            vec![shared_warning.clone()],
        );
        collector.extend_source(
            "src_alpha",
            WARNING_ORIGIN_JOURNAL,
            vec![journal_warning.clone()],
        );

        let (warnings, contexts) = collector.finish();
        assert_eq!(
            warnings,
            vec![shared_warning.clone(), journal_warning.clone()]
        );
        assert_eq!(
            contexts,
            vec![
                ObservationWarningContext {
                    warning: shared_warning.clone(),
                    source_id: Some("src_alpha".to_string()),
                    origin: "jobs".to_string(),
                },
                ObservationWarningContext {
                    warning: shared_warning,
                    source_id: Some("src_beta".to_string()),
                    origin: "jobs".to_string(),
                },
                ObservationWarningContext {
                    warning: journal_warning,
                    source_id: Some("src_alpha".to_string()),
                    origin: "journal".to_string(),
                },
            ]
        );
    }

    #[test]
    fn warning_contexts_are_sanitized_filtered_to_surviving_warnings_and_bounded() {
        let mut raw_warnings = vec![
            "Authorization: Bearer super-secret-token".to_string(),
            "   ".to_string(),
        ];
        let mut raw_contexts = vec![
            ObservationWarningContext {
                warning: "Authorization: Bearer super-secret-token".to_string(),
                source_id: Some("/etc/passwd".to_string()),
                origin: "/var/log/unsafe".to_string(),
            },
            ObservationWarningContext {
                warning: "Dropped because not in surviving warnings".to_string(),
                source_id: Some("src_alpha".to_string()),
                origin: "jobs".to_string(),
            },
        ];

        for idx in 0..(MAX_OBSERVATION_WARNINGS + 8) {
            let msg = format!("Repeated warning {idx}");
            raw_warnings.push(msg.clone());
            raw_contexts.push(ObservationWarningContext {
                warning: msg.clone(),
                source_id: Some("src_alpha".to_string()),
                origin: "requests".to_string(),
            });
            raw_contexts.push(ObservationWarningContext {
                warning: msg,
                source_id: Some("src_beta".to_string()),
                origin: "requests".to_string(),
            });
        }

        let surviving_warnings = sanitize_and_bound_warnings(raw_warnings);
        let bounded_contexts =
            sanitize_and_bound_warning_contexts(raw_contexts, &surviving_warnings);

        assert_eq!(surviving_warnings.len(), MAX_OBSERVATION_WARNINGS);
        assert_eq!(bounded_contexts.len(), MAX_OBSERVATION_WARNINGS);
        assert_eq!(
            bounded_contexts[0],
            ObservationWarningContext {
                warning: "[redacted sensitive metadata]".to_string(),
                source_id: None,
                origin: "snapshot".to_string(),
            }
        );
        assert!(bounded_contexts
            .iter()
            .all(|ctx| surviving_warnings.contains(&ctx.warning)));
        assert!(bounded_contexts
            .iter()
            .all(|ctx| ctx.warning != "Dropped because not in surviving warnings"));
        assert!(bounded_contexts
            .iter()
            .all(|ctx| ctx.warning != "Repeated warning 30"));
    }

    #[test]
    fn legacy_snapshot_deserializes_when_warning_contexts_field_is_absent() {
        let raw_json = r#"{
            "schema_version": 1,
            "generated_at_ms": 1700000000123,
            "sources": [],
            "jobs": [],
            "requests": [],
            "next_cursor": null,
            "has_more": false,
            "jobs_has_more": false,
            "warnings": ["Ignored unverified job, plan, task, or approval hint on journal record."]
        }"#;

        let parsed: ObservationSnapshot =
            serde_json::from_str(raw_json).expect("legacy snapshot JSON must deserialize");
        assert_eq!(parsed.schema_version, OBSERVATION_SCHEMA_VERSION);
        assert_eq!(parsed.warnings.len(), 1);
        assert!(parsed.warning_contexts.is_empty());
    }
}
