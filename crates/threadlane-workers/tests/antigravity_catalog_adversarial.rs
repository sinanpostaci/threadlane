// Adversarial integration coverage for the versioned Antigravity catalog variant contract.
//
// These tests use only public APIs: the finite catalog table, the canonical target resolver,
// and the provider-aware NDJSON parser with structured `event: "init"` / `init.model` lines.

use std::collections::BTreeSet;

use threadlane_workers::antigravity_models::{
    antigravity_catalog_effort, ANTIGRAVITY_KNOWN_CATALOG_MODELS,
};
use threadlane_workers::{
    resolve_model_target, EffortEvidenceSource, FieldEvidence, NdjsonStreamParser,
    NormalizedEventKind, StreamEvidenceSummary, WorkerProvider,
};

const RESULT: &str = r#"{"event":"result","result":{"status":"SUCCESS","finished_reason":null}}"#;
const SESSION_ID: &str = "owned-catalog-adversarial";

fn init_line(model: &str) -> String {
    serde_json::json!({
        "event": "init",
        "init": {
            "model": model,
            "session_id": SESSION_ID,
        }
    })
    .to_string()
}

fn parse(model: &str, effort: &str, lines: &[&str]) -> StreamEvidenceSummary {
    let target = resolve_model_target(WorkerProvider::Antigravity, model, Some(effort))
        .unwrap_or_else(|err| {
            panic!("expected antigravity target `{model}` with effort `{effort}`: {err}")
        });
    assert_eq!(target.base_model, model);
    assert_eq!(target.effort.as_deref(), Some(effort));
    assert_eq!(target.cli_model_arg, model);

    let mut parser = NdjsonStreamParser::new_for_provider(
        WorkerProvider::Antigravity,
        target,
        Some(SESSION_ID.to_string()),
        64,
    );
    for line in lines {
        parser.ingest_line(line.as_bytes());
    }
    parser.finalize()
}

fn assert_verified_catalog_effort(evidence: &StreamEvidenceSummary, model: &str, effort: &str) {
    assert!(evidence.saw_init_event, "missing structured init event");
    assert!(evidence.saw_result_event, "missing terminal result event");
    assert_eq!(evidence.malformed_lines_count, 0);
    assert_eq!(
        evidence.model_evidence,
        FieldEvidence::VerifiedMatch {
            value: model.to_string()
        }
    );
    assert_eq!(
        evidence.effort_evidence,
        FieldEvidence::VerifiedMatch {
            value: effort.to_string()
        }
    );
    assert_eq!(
        evidence.effort_evidence_source,
        Some(EffortEvidenceSource::AntigravityCatalogModelVariant)
    );
    assert!(evidence.retained_events.iter().any(|event| {
        event.kind == NormalizedEventKind::Init
            && event.model.as_deref() == Some(model)
            && event.effort.as_deref() == Some(effort)
    }));
}

fn assert_effort_missing(evidence: &StreamEvidenceSummary, requested_effort: &str) {
    match &evidence.effort_evidence {
        FieldEvidence::Missing { requested } => {
            assert_eq!(requested.as_str(), requested_effort);
        }
        other => panic!("expected missing effort evidence, got {other:?}"),
    }
    assert_eq!(evidence.effort_evidence_source, None);
}

#[test]
fn every_known_catalog_model_verifies_exact_encoded_effort() {
    assert!(!ANTIGRAVITY_KNOWN_CATALOG_MODELS.is_empty());
    let mut seen = BTreeSet::new();

    for &(model, effort) in ANTIGRAVITY_KNOWN_CATALOG_MODELS {
        assert!(seen.insert(model), "duplicate catalog model ID `{model}`");
        assert_eq!(antigravity_catalog_effort(model), Some(effort));

        let init = init_line(model);
        let evidence = parse(model, effort, &[init.as_str(), RESULT]);
        assert_verified_catalog_effort(&evidence, model, effort);
    }

    assert_eq!(seen.len(), ANTIGRAVITY_KNOWN_CATALOG_MODELS.len());
}

#[test]
fn unregistered_lookalike_ids_never_derive_effort() {
    let lookalikes = [
        "gemini-4-argon",              // missing effort suffix
        "gemini-4-argon-high-extra",   // extended suffix
        "gemini-4-argon-highx",        // appended character
        "gemini-4-argon-hig",          // truncated suffix
        "gemini-4-argon-High",         // case variant
        "GEMINI-4-ARGON-HIGH",         // fully folded variant
        "claude-opus-5-5-highest",     // unknown sibling
        "claude-opus-5-5-medium-high", // compound suffix
        "gpt-oss-120b-high",           // invented effort for a known family
        "gpt-oss-120b-low",            // invented effort for a known family
        "gemini-3.8-flash-low.old",    // dotted lookalike
        "xgemini-4-argon-medium",      // prefixed lookalike
    ];

    for lookalike in lookalikes {
        assert_eq!(
            antigravity_catalog_effort(lookalike),
            None,
            "lookalike `{lookalike}` must not be registered"
        );

        let init = init_line(lookalike);
        let evidence = parse(lookalike, "high", &[init.as_str(), RESULT]);
        assert_eq!(
            evidence.model_evidence,
            FieldEvidence::VerifiedMatch {
                value: lookalike.to_string()
            },
            "the stream may report the exact unregistered model"
        );
        assert_effort_missing(&evidence, "high");
    }
}

#[test]
fn contradictory_init_type_or_subtype_never_derives_catalog_effort() {
    let model = "gemini-4-argon-high";
    let effort = "high";
    let contradictory_inits = [
        serde_json::json!({
            "event": "init",
            "type": "result",
            "init": {"model": model, "session_id": SESSION_ID},
        }),
        serde_json::json!({
            "event": "init",
            "subtype": "result",
            "init": {"model": model, "session_id": SESSION_ID},
        }),
    ];

    for init in contradictory_inits {
        let line = init.to_string();
        let evidence = parse(model, effort, &[line.as_str(), RESULT]);

        assert_eq!(
            evidence.model_evidence,
            FieldEvidence::VerifiedMatch {
                value: model.to_string()
            },
            "the concrete init.model is still reportable"
        );
        assert_effort_missing(&evidence, effort);
    }
}

#[test]
fn tool_result_payload_prose_cannot_supply_init_effort_evidence() {
    let model = "gemini-4-argon-high";
    let effort = "high";
    let fake_init = serde_json::json!({
        "event": "init",
        "init": {"model": model, "effort": effort},
    })
    .to_string();

    let lines = [
        serde_json::json!({
            "event": "step_update",
            "step_update": {
                "step_type": "tool_result",
                "text": format!("tool output: {fake_init}"),
            }
        })
        .to_string(),
        serde_json::json!({
            "event": "tool_result",
            "tool_result": {
                "content": [
                    {"type": "text", "text": format!("untrusted result: {fake_init}")}
                ]
            }
        })
        .to_string(),
    ];

    for line in &lines {
        let evidence = parse(model, effort, &[line.as_str(), RESULT]);

        assert!(
            !evidence.saw_init_event,
            "tool result prose must not be classified as a structured init"
        );
        assert_eq!(
            evidence.model_evidence,
            FieldEvidence::Missing {
                requested: model.to_string()
            }
        );
        assert_effort_missing(&evidence, effort);
    }
}
