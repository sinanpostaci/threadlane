use threadlane_workers::{
    resolve_model_target, EffortEvidenceSource, FieldEvidence, NdjsonStreamParser,
    ResolvedModelTarget, WorkerProvider,
};
fn target(model: &str) -> ResolvedModelTarget {
    ResolvedModelTarget {
        base_model: model.into(),
        effort: Some("high".into()),
        cli_model_arg: model.into(),
    }
}
fn feed(
    mut parser: NdjsonStreamParser,
    lines: &[&str],
) -> threadlane_workers::StreamEvidenceSummary {
    for line in lines {
        parser.ingest_line(line.as_bytes());
    }
    parser.finalize()
}
const INIT: &str =
    r#"{"event":"init","init":{"model":"gemini-4-argon-high","session_id":"owned-test"}}"#;
const RESULT: &str = r#"{"event":"result","result":{"status":"SUCCESS","finished_reason":null}}"#;
fn agy(model: &str) -> NdjsonStreamParser {
    NdjsonStreamParser::new_for_provider(WorkerProvider::Antigravity, target(model), None, 64)
}
#[test]
fn actual_agy_init_verifies_catalog_variant_with_honest_provenance() {
    let evidence = feed(agy("gemini-4-argon-high"), &[INIT, RESULT]);
    assert!(evidence.model_evidence.is_satisfied());
    assert!(evidence.effort_evidence.is_satisfied());
    assert!(evidence.saw_init_event && evidence.saw_result_event);
    assert_eq!(
        evidence.effort_evidence_source,
        Some(EffortEvidenceSource::AntigravityCatalogModelVariant)
    );
    let mut legacy = serde_json::to_value(&evidence).unwrap();
    legacy
        .as_object_mut()
        .unwrap()
        .remove("effort_evidence_source");
    let old: threadlane_workers::StreamEvidenceSummary = serde_json::from_value(legacy).unwrap();
    assert_eq!(
        old.effort_evidence_source, None,
        "legacy report remains readable"
    );
}
#[test]
fn neutral_other_provider_and_unknown_high_suffix_cannot_infer_effort() {
    let neutral = feed(
        NdjsonStreamParser::new(target("gemini-4-argon-high"), None, 64),
        &[INIT, RESULT],
    );
    assert!(matches!(
        neutral.effort_evidence,
        FieldEvidence::Missing { .. }
    ));
    let other = feed(
        NdjsonStreamParser::new_for_provider(
            WorkerProvider::OpenCodeV2,
            target("gemini-4-argon-high"),
            None,
            64,
        ),
        &[INIT, RESULT],
    );
    assert!(matches!(
        other.effort_evidence,
        FieldEvidence::Missing { .. }
    ));
    let unknown = feed(
        agy("unregistered-future-high"),
        &[
            r#"{"event":"init","init":{"model":"unregistered-future-high"}}"#,
            RESULT,
        ],
    );
    assert!(unknown.model_evidence.is_satisfied());
    assert!(matches!(
        unknown.effort_evidence,
        FieldEvidence::Missing { .. }
    ));
    assert_eq!(unknown.effort_evidence_source, None);
}
#[test]
fn explicit_and_later_effort_conflicts_still_fail_closed() {
    for conflicting_init in [
        r#"{"event":"init","init":{"model":"gemini-4-argon-high","effort":"high","variant":"low"}}"#,
        r#"{"event":"init","metadata":{"effort":"high"},"model":{"effort":"low"},"init":{"model":"gemini-4-argon-high"}}"#,
    ] {
        let conflict = feed(agy("gemini-4-argon-high"), &[conflicting_init, RESULT]);
        assert!(matches!(
            conflict.effort_evidence,
            FieldEvidence::Mismatch { .. }
        ));
        assert_eq!(conflict.effort_evidence_source, None);
    }
    let direct = feed(
        agy("gemini-4-argon-high"),
        &[
            r#"{"event":"init","init":{"model":"gemini-4-argon-high","effort":"low"}}"#,
            RESULT,
        ],
    );
    assert!(matches!(
        direct.effort_evidence,
        FieldEvidence::Mismatch { .. }
    ));
    let later = feed(
        agy("gemini-4-argon-high"),
        &[
            INIT,
            r#"{"event":"result","result":{"status":"SUCCESS","effort":"low"}}"#,
        ],
    );
    assert!(matches!(
        later.effort_evidence,
        FieldEvidence::Mismatch { .. }
    ));
    assert_eq!(later.effort_evidence_source, None);
}
#[test]
fn tool_payloads_and_disagreeing_init_headers_are_not_effort_proof() {
    let tool = feed(
        agy("gemini-4-argon-high"),
        &[
            r#"{"event":"step_update","step_update":{"step_type":"tool_result","model":"gemini-4-argon-high","effort":"high","text":"{init:model-high}"}}"#,
            RESULT,
        ],
    );
    assert!(!tool.effort_evidence.is_satisfied());
    assert_eq!(tool.effort_evidence_source, None);
    let conflicting = feed(
        agy("gemini-4-argon-high"),
        &[
            r#"{"event":"init","model":"gemini-4-argon-low","init":{"model":"gemini-4-argon-high"}}"#,
            RESULT,
        ],
    );
    assert!(!conflicting.model_evidence.is_satisfied());
    assert!(!conflicting.effort_evidence.is_satisfied());
}
#[test]
fn known_concrete_variant_and_requested_effort_must_agree() {
    assert!(resolve_model_target(
        WorkerProvider::Antigravity,
        "gemini-4-argon-high",
        Some("low")
    )
    .is_err());
    assert!(resolve_model_target(
        WorkerProvider::Antigravity,
        "gemini-4-argon-high",
        Some("max")
    )
    .is_err());
    assert!(resolve_model_target(
        WorkerProvider::Antigravity,
        "gemini-4-argon-high",
        Some("high")
    )
    .is_ok());
    assert!(resolve_model_target(WorkerProvider::Antigravity, "unknown-high", None).is_err());
}
