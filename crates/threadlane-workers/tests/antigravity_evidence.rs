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
    for (model, effort, wrong) in [
        ("gemini-4-carbon-low", "low", "high"),
        ("gemini-4-carbon-medium", "medium", "low"),
        ("gemini-4-carbon-high", "high", "medium"),
    ] {
        let ok = resolve_model_target(WorkerProvider::Antigravity, model, Some(effort));
        assert!(ok.is_ok());
        let bad = resolve_model_target(WorkerProvider::Antigravity, model, Some(wrong));
        assert!(bad.is_err());
        let missing = resolve_model_target(WorkerProvider::Antigravity, model, None);
        assert!(missing.is_err());
    }
    assert!(resolve_model_target(WorkerProvider::Antigravity, "unknown-high", None).is_err());
}

#[test]
fn actual_agy_1_5_3_carbon_init_schema_verifies_catalog_model_variant_source() {
    let variants = [
        ("gemini-4-carbon-low", "low"),
        ("gemini-4-carbon-medium", "medium"),
        ("gemini-4-carbon-high", "high"),
    ];
    for (model, effort) in variants {
        let target_res = resolve_model_target(WorkerProvider::Antigravity, model, Some(effort));
        let resolved = target_res.unwrap();
        let init = serde_json::json!({
            "event": "init",
            "conversation_id": "owned-carbon-conv",
            "init": {
                "model": model,
                "cwd": "/canonical/worktree",
                "permission_mode": "accept-edits",
            }
        })
        .to_string();
        let step_update = serde_json::json!({
            "event": "step_update",
            "step_update": {
                "conversation_id": "owned-carbon-conv",
                "step_index": 1,
                "state": "DONE",
                "step_type": "agent_response",
                "usage": {},
            }
        })
        .to_string();
        let result = serde_json::json!({
            "event": "result",
            "result": {
                "conversation_id": "owned-carbon-conv",
                "status": "SUCCESS",
                "finished_reason": null,
                "response": "Finished",
                "num_turns": 1,
            }
        })
        .to_string();
        let lines = [init.as_str(), step_update.as_str(), result.as_str()];

        let parser = NdjsonStreamParser::new_for_provider(
            WorkerProvider::Antigravity,
            resolved.clone(),
            Some("owned-carbon-conv".to_string()),
            64,
        );
        let evidence = feed(parser, &lines);
        assert!(evidence.saw_init_event && evidence.saw_result_event);
        assert_eq!(evidence.malformed_lines_count, 0);
        assert!(evidence.provider_errors.is_empty());
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
            evidence.session_evidence,
            FieldEvidence::VerifiedMatch {
                value: "owned-carbon-conv".to_string()
            }
        );
        assert_eq!(
            evidence.effort_evidence_source,
            Some(EffortEvidenceSource::AntigravityCatalogModelVariant)
        );
        let wire = serde_json::to_value(&evidence).unwrap();
        assert_eq!(
            wire["effort_evidence_source"],
            "antigravity_catalog_model_variant"
        );

        let neutral_parser = NdjsonStreamParser::new(resolved.clone(), None, 64);
        let neutral = feed(neutral_parser, &lines);
        assert!(matches!(
            neutral.effort_evidence,
            FieldEvidence::Missing { .. }
        ));
        assert_eq!(neutral.effort_evidence_source, None);

        let other_parser = NdjsonStreamParser::new_for_provider(
            WorkerProvider::OpenCodeV2,
            resolved.clone(),
            Some("owned-carbon-conv".to_string()),
            64,
        );
        let other = feed(other_parser, &lines);
        assert!(matches!(
            other.effort_evidence,
            FieldEvidence::Missing { .. }
        ));
        assert_eq!(other.effort_evidence_source, None);

        let timeout_result = serde_json::json!({
            "event": "result",
            "result": {
                "conversation_id": "owned-carbon-conv",
                "status": "SUCCESS",
                "finished_reason": "print_timeout",
            }
        })
        .to_string();
        let timeout_parser = NdjsonStreamParser::new_for_provider(
            WorkerProvider::Antigravity,
            resolved,
            Some("owned-carbon-conv".to_string()),
            64,
        );
        let timeout_lines = [init.as_str(), timeout_result.as_str()];
        let timeout_ev = feed(timeout_parser, &timeout_lines);
        assert!(timeout_ev.effort_evidence.is_satisfied());
        assert!(!timeout_ev.saw_result_event);
        assert_eq!(timeout_ev.provider_errors.len(), 1);
        assert_eq!(
            timeout_ev.provider_errors[0].code.as_deref(),
            Some("print_timeout")
        );
    }
}

#[test]
fn provider_print_timeout_cannot_be_a_successful_terminal_result() {
    let evidence = feed(
        agy("gemini-4-argon-high"),
        &[
            INIT,
            r#"{"event":"result","result":{"status":"SUCCESS","finished_reason":"print_timeout"}}"#,
        ],
    );
    assert!(evidence.effort_evidence.is_satisfied());
    assert!(!evidence.saw_result_event);
    assert_eq!(evidence.provider_errors.len(), 1);
    assert_eq!(
        evidence.provider_errors[0].code.as_deref(),
        Some("print_timeout")
    );
}
