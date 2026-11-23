use threadlane_workers::{
    resolve_model_target, FieldEvidence, NdjsonStreamParser, ResolvedModelTarget, WorkerProvider,
};

const MODELS: [&str; 2] = ["gemini-pro-orchestrator-neon", "gemini-pro-orchestrator"];
const RESULT: &str = r#"{"event":"result","result":{"status":"SUCCESS"}}"#;

fn parse(
    provider: WorkerProvider,
    model: &str,
    lines: &[&str],
) -> threadlane_workers::StreamEvidenceSummary {
    let target = ResolvedModelTarget {
        base_model: model.into(),
        effort: None,
        cli_model_arg: model.into(),
    };
    let mut parser = NdjsonStreamParser::new_for_provider(provider, target, None, 64);
    for line in lines {
        parser.ingest_line(line.as_bytes());
    }
    parser.finalize()
}

#[test]
fn only_two_exact_agy_models_resolve_without_effort() {
    for model in MODELS {
        let target = resolve_model_target(WorkerProvider::Antigravity, model, None).unwrap();
        assert_eq!(target.base_model, model);
        assert_eq!(target.cli_model_arg, model);
        assert_eq!(target.effort, None);
        // Existing explicitly pinned approvals retain their former contract.
        assert_eq!(
            resolve_model_target(WorkerProvider::Antigravity, model, Some("high"))
                .unwrap()
                .effort
                .as_deref(),
            Some("high")
        );
    }
    for model in [
        "gemini-pro-orchestrator-argon",
        "gemini-pro-orchestrator-neon-high",
        "Gemini-pro-orchestrator",
        "gemini-4-argon-high",
    ] {
        assert!(
            resolve_model_target(WorkerProvider::Antigravity, model, None).is_err(),
            "{model}"
        );
    }
    assert!(resolve_model_target(WorkerProvider::CodexCli, MODELS[0], None).is_err());
}

#[test]
fn no_effort_requires_exact_structured_init_and_rejects_reported_effort() {
    for model in MODELS {
        let init = format!(r#"{{"event":"init","init":{{"model":"{model}","effort":null}}}}"#);
        let evidence = parse(WorkerProvider::Antigravity, model, &[&init, RESULT]);
        assert_eq!(
            evidence.model_evidence,
            FieldEvidence::VerifiedMatch {
                value: model.into()
            }
        );
        assert_eq!(
            evidence.effort_evidence,
            FieldEvidence::NotRequested { observed: None }
        );
        assert_eq!(evidence.effort_evidence_source, None);
        assert!(evidence.saw_result_event);

        let result_only =
            format!(r#"{{"event":"result","result":{{"model":"{model}","status":"SUCCESS"}}}}"#);
        let no_init = parse(WorkerProvider::Antigravity, model, &[&result_only]);
        assert!(matches!(
            no_init.model_evidence,
            FieldEvidence::Missing { .. }
        ));
        let other = if model == MODELS[0] {
            MODELS[1]
        } else {
            MODELS[0]
        };
        let wrong_init = format!(r#"{{"event":"init","init":{{"model":"{other}"}}}}"#);
        assert!(matches!(
            parse(WorkerProvider::Antigravity, model, &[&wrong_init, RESULT]).model_evidence,
            FieldEvidence::Mismatch { .. }
        ));
        let conflicting =
            format!(r#"{{"event":"result","result":{{"status":"SUCCESS","model":"{other}"}}}}"#);
        assert!(matches!(
            parse(WorkerProvider::Antigravity, model, &[&init, &conflicting]).model_evidence,
            FieldEvidence::Mismatch { .. }
        ));
        for non_exact in [format!("{model}#"), format!(" {model}")] {
            let later = serde_json::json!({"event":"step_update", "model":non_exact}).to_string();
            assert!(matches!(
                parse(WorkerProvider::Antigravity, model, &[&init, &later, RESULT]).model_evidence,
                FieldEvidence::Mismatch { .. }
            ));
        }
        for suffix in [
            r#"{"event":"step_update","effort":"high"}"#,
            r#"{"event":"result","result":{"status":"SUCCESS","effort":"default"}}"#,
        ] {
            assert!(matches!(
                parse(WorkerProvider::Antigravity, model, &[&init, suffix]).effort_evidence,
                FieldEvidence::Mismatch { .. }
            ));
        }
        // Untrusted tool payloads do not become model or effort telemetry.
        let tool =
            r#"{"event":"step_update","step_update":{"step_type":"tool_result","effort":"high"}}"#;
        assert_eq!(
            parse(WorkerProvider::Antigravity, model, &[&init, tool, RESULT]).effort_evidence,
            FieldEvidence::NotRequested { observed: None }
        );
        for malformed in [
            r#"{"event":"step_update","effort":""}"#,
            r#"{"event":"step_update","metadata":{"reasoning_effort":0}}"#,
            r#"{"event":"step_update","init":{"model":{"thinking_effort":false}}}"#,
        ] {
            let evidence = parse(
                WorkerProvider::Antigravity,
                model,
                &[&init, malformed, RESULT],
            );
            assert_eq!(evidence.malformed_lines_count, 1);
            assert_eq!(
                evidence.effort_evidence,
                FieldEvidence::NotRequested { observed: None }
            );
        }
        let tool_payload =
            r#"{"event":"step_update","step_update":{"step_type":"tool_result","effort":12}}"#;
        let message_text = r#"{"event":"message","message":{"text":"effort: 0"}}"#;
        assert_eq!(
            parse(
                WorkerProvider::Antigravity,
                model,
                &[&init, tool_payload, message_text, RESULT]
            )
            .malformed_lines_count,
            0
        );
    }
    // Provider-neutral paths keep their existing NotRequested semantics.
    let init = r#"{"event":"init","init":{"model":"gemini-pro-orchestrator","effort":"high"}}"#;
    assert!(matches!(
        parse(WorkerProvider::OpenCodeV2, MODELS[1], &[init, RESULT]).effort_evidence,
        FieldEvidence::NotRequested { observed: Some(_) }
    ));
}
