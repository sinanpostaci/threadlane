use std::collections::BTreeSet;
use std::fs;
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::sync::{mpsc, watch};

use threadlane_workers::{
    run_advisory_worker, run_advisory_worker_with_structured_result,
    run_advisory_worker_with_structured_result_and_cancel,
    run_advisory_worker_with_structured_result_and_observer, AdvisoryResponseBudget,
    AdvisoryResponseErrorCode, AdvisoryResponseSource, AdvisoryStructuredExecution,
    AdvisoryWorkerSpec, BoundedAdvisoryResponse, EffortEvidenceSource, ExecutionBudgets,
    NormalizedWorkerEvent, OutputOverflowKind, WorkerError, WorkerErrorCode, WorkerFilePermissions,
    WorkerProvider, WorkerRunStatus, DEFAULT_MAX_ADVISORY_RESPONSE_BYTES,
    MAX_ADVISORY_RESPONSE_BYTES,
};

fn write_executable_script(dir: &Path, name: &str, script_body: &str) -> PathBuf {
    let staging_path = dir.join(format!(".{name}.tmp"));
    let path = dir.join(name);
    {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o755)
            .open(&staging_path)
            .expect("open staging executable script");
        file.write_all(script_body.as_bytes())
            .expect("write executable script");
        file.sync_all().expect("sync executable script");
    }
    fs::rename(&staging_path, &path).expect("rename executable script into place");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755))
        .expect("chmod 0755 executable script");
    path
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SampleAdvisoryPlanPayload {
    schema_version: u32,
    plan_id: String,
    summary: String,
    nodes: Vec<SamplePlanNode>,
    review_findings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SamplePlanNode {
    node_id: String,
    title: String,
    depends_on: Vec<String>,
    rationale: String,
}

fn build_large_sample_plan_payload() -> (SampleAdvisoryPlanPayload, String) {
    let payload = SampleAdvisoryPlanPayload {
        schema_version: 1,
        plan_id: "plan-structured-advisory-001".to_string(),
        summary: "Complete multi-node DAG and review findings exceeding 256-byte diagnostic truncation.".to_string(),
        nodes: (1..=6)
            .map(|idx| SamplePlanNode {
                node_id: format!("node-{idx:02}"),
                title: format!("Validate subsystem boundary #{idx} with deterministic evidence"),
                depends_on: if idx == 1 {
                    Vec::new()
                } else {
                    vec![format!("node-{:02}", idx - 1)]
                },
                rationale: format!(
                    "Subsystem #{idx} requires complete UTF-8 JSON fidelity without reconstructing truncated 256-byte progress summaries (marker={idx})."
                ),
            })
            .collect(),
        review_findings: vec![
            "Retained event summaries remain strictly capped at 256 bytes for safe UI diagnostics.".to_string(),
            "Complete structured advisory responses are captured in-band from verified terminal framing.".to_string(),
            "Multibyte UTF-8 sequences such as café, €100, and 🦀 are preserved byte-for-byte.".to_string(),
        ],
    };
    let json = serde_json::to_string(&payload).expect("serialize sample plan payload");
    assert!(
        json.len() > 1200,
        "sample payload must significantly exceed 256-byte summary truncation (got {} bytes)",
        json.len()
    );
    (payload, json)
}

#[tokio::test]
async fn antigravity_supervised_structured_response_captures_full_json_above_256_bytes_and_preserves_truncated_summaries(
) {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let (expected_payload, payload_json) = build_large_sample_plan_payload();
    let payload_file = temp.path().join("agy_result_line.json");
    let result_event = serde_json::json!({
        "event": "result",
        "result": {
            "status": "SUCCESS",
            "response": payload_json
        }
    });
    fs::write(
        &payload_file,
        serde_json::to_string(&result_event).expect("serialize result event"),
    )
    .expect("write payload file");

    let script = format!(
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{{"event":"init","init":{{"session_id":"agy-sess-77","model":"gemini-4-argon-high"}}}}'
printf '%s\n' '{{"event":"step_update","step_update":{{"step_type":"thinking","message":"{{\"intermediate_only\":true,\"do_not_capture\":1}}"}}}}'
cat "{}"
printf '\n'
exit 0
"#,
        payload_file.display()
    );
    let fake_agy = write_executable_script(&bin_dir, "fake-agy-structured", &script);

    let spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::Antigravity,
        executable: fake_agy,
        model: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        allowed_models: BTreeSet::from(["gemini-4-argon-high".to_string()]),
        session_id: None,
        standalone: false,
        working_directory: workdir,
        allowed_write_paths: BTreeSet::new(),
        prompt: "Return structured JSON plan.".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    };

    let (tx, mut rx) = mpsc::channel::<NormalizedWorkerEvent>(16);
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let structured = run_advisory_worker_with_structured_result_and_observer(
        &spec,
        AdvisoryResponseBudget::default(),
        &mut cancel_rx,
        Some(tx),
    )
    .await
    .expect("structured advisory execution should succeed");

    assert!(structured.has_verified_response());
    assert!(structured.response_error.is_none());
    assert_eq!(structured.execution.status, WorkerRunStatus::Succeeded);
    assert!(structured
        .execution
        .has_complete_natural_completion_evidence());

    // Retained progress and live observer summaries remain strictly truncated at <= 256 bytes.
    for ev in &structured.execution.evidence.retained_events {
        if let Some(ref summary) = ev.summary {
            assert!(
                summary.len() <= 256,
                "retained summary must remain <= 256 bytes (got {})",
                summary.len()
            );
        }
    }
    let result_retained_summary = structured
        .execution
        .evidence
        .retained_events
        .last()
        .and_then(|e| e.summary.as_ref())
        .expect("result event should have truncated diagnostic summary");
    assert!(result_retained_summary.len() <= 256);
    assert!(
        serde_json::from_str::<SampleAdvisoryPlanPayload>(result_retained_summary).is_err(),
        "truncated 256-byte diagnostic summary cannot be parsed as the full plan DAG"
    );

    let mut live_events = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        if let Some(ref summary) = ev.summary {
            assert!(summary.len() <= 256);
        }
        live_events.push(ev);
    }
    assert_eq!(live_events.len(), 3);

    // Complete structured response is intact and matches exact UTF-8 bytes and SHA-256 digest.
    let response = structured
        .verified_response()
        .expect("verified response must be present");
    assert_eq!(response.text, payload_json);
    assert_eq!(response.byte_count, payload_json.len());
    assert_eq!(response.byte_len(), payload_json.len());
    assert_eq!(response.sha256_digest, sha256_hex(payload_json.as_bytes()));
    assert_eq!(response.provider, WorkerProvider::Antigravity);
    assert_eq!(
        response.source,
        AdvisoryResponseSource::AntigravityFinalResultResponse
    );
    assert_eq!(response.framing.response_line_number, 3);
    assert_eq!(response.framing.terminal_line_number, 3);
    assert_eq!(response.framing.session_id.as_deref(), Some("agy-sess-77"));
    assert_eq!(response.framing.message_id, None);
    assert_eq!(response.framing.part_id, None);
    assert_eq!(
        response.framing.effort_evidence_source,
        Some(EffortEvidenceSource::AntigravityCatalogModelVariant)
    );

    let parsed: SampleAdvisoryPlanPayload = response
        .parse_json()
        .expect("full structured JSON must deserialize into typed plan payload");
    assert_eq!(parsed, expected_payload);
}

#[tokio::test]
async fn opencode_supervised_structured_response_captures_owned_and_explicit_sessions_with_provenance(
) {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let (expected_payload, payload_json) = build_large_sample_plan_payload();
    let text_line_file = temp.path().join("oc_text_line.json");
    let text_event = serde_json::json!({
        "type": "text",
        "timestamp": 1775933708045u64,
        "sessionID": "ses_owned_adv_42",
        "part": {
            "id": "prt_plan_final_1",
            "sessionID": "ses_owned_adv_42",
            "messageID": "msg_asst_final_1",
            "type": "text",
            "text": payload_json,
            "time": {
                "start": 1775933708010u64,
                "end": 1775933708040u64
            }
        }
    });
    fs::write(
        &text_line_file,
        serde_json::to_string(&text_event).expect("serialize text event"),
    )
    .expect("write text line file");

    let script = format!(
        r#"#!/usr/bin/env sh
set -eu
NOW_MS=$(( $(date +%s) * 1000 + 2000 ))
DONE_MS=$(( NOW_MS + 100 ))

if [ "$1" = "api" ] && [ "$2" = "--standalone" ] && [ "$3" = "session.create" ]; then
    cat <<EOF
{{"data":{{"id":"ses_owned_adv_42","location":{{"directory":"$(pwd)"}},"time":{{"created":$NOW_MS}}}}}}
EOF
    exit 0
fi

if [ "$1" = "api" ] && [ "$2" = "--standalone" ] && [ "$3" = "session.get" ]; then
    cat <<EOF
{{"data":{{"id":"ses_owned_adv_42","outcome":"succeeded","location":{{"directory":"$(pwd)"}},"time":{{"created":$NOW_MS,"idle":$DONE_MS}}}}}}
EOF
    exit 0
fi

if [ "$1" = "session" ] && [ "$2" = "export" ] && [ "$3" = "--standalone" ] && [ "$4" = "--sanitize" ]; then
    cat <<EOF
{{"info":{{"id":"ses_owned_adv_42","outcome":"succeeded","location":{{"directory":"$(pwd)"}},"time":{{"created":$NOW_MS,"idle":$DONE_MS}}}},"messages":[{{"id":"msg_user_1","type":"user","sessionID":"ses_owned_adv_42"}},{{"id":"msg_asst_final_1","type":"assistant","sessionID":"ses_owned_adv_42","finish":"stop","time":{{"created":$NOW_MS,"completed":$DONE_MS}},"model":{{"providerID":"opencode-go","id":"deepseek-v4.1-flash","variant":"max"}}}}]}}
EOF
    exit 0
fi

printf '%s\n' '{{"type":"step_start","sessionID":"ses_owned_adv_42","part":{{"id":"prt_start_1","sessionID":"ses_owned_adv_42","messageID":"msg_asst_final_1","type":"step-start","model":{{"providerID":"opencode-go","id":"deepseek-v4.1-flash","variant":"max"}}}}}}'
cat "{}"
printf '\n'
printf '%s\n' '{{"type":"step_finish","sessionID":"ses_owned_adv_42","part":{{"id":"prt_finish_1","sessionID":"ses_owned_adv_42","messageID":"msg_asst_final_1","type":"step-finish","reason":"stop"}}}}'
exit 0
"#,
        text_line_file.display()
    );
    let fake_oc = write_executable_script(&bin_dir, "fake-opencode-owned", &script);

    let spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::OpenCodeV2,
        executable: fake_oc,
        model: "opencode-go/deepseek-v4.1-flash#max".to_string(),
        effort: None,
        allowed_models: BTreeSet::from(["opencode-go/deepseek-v4.1-flash#max".to_string()]),
        session_id: None,
        standalone: true,
        working_directory: workdir,
        allowed_write_paths: BTreeSet::new(),
        prompt: "Review repository and emit JSON findings.".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    };

    let structured =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("owned OpenCode structured run should succeed");

    assert!(structured.has_verified_response());
    assert!(structured.response_error.is_none());
    assert_eq!(structured.execution.status, WorkerRunStatus::Succeeded);
    assert_eq!(
        structured.execution.allocated_session_id.as_deref(),
        Some("ses_owned_adv_42")
    );
    assert!(structured.execution.validated_opencode_metadata().is_some());

    let response = structured
        .verified_response()
        .expect("verified OpenCode response must be present");
    assert_eq!(response.text, payload_json);
    assert_eq!(response.byte_count, payload_json.len());
    assert_eq!(response.sha256_digest, sha256_hex(payload_json.as_bytes()));
    assert_eq!(response.provider, WorkerProvider::OpenCodeV2);
    assert_eq!(
        response.source,
        AdvisoryResponseSource::OpenCodeAssistantTextPartStop
    );
    assert_eq!(response.framing.response_line_number, 2);
    assert_eq!(response.framing.terminal_line_number, 3);
    assert_eq!(
        response.framing.session_id.as_deref(),
        Some("ses_owned_adv_42")
    );
    assert_eq!(
        response.framing.message_id.as_deref(),
        Some("msg_asst_final_1")
    );
    assert_eq!(
        response.framing.part_id.as_deref(),
        Some("prt_plan_final_1")
    );
    assert_eq!(
        response.framing.effort_evidence_source,
        Some(EffortEvidenceSource::DirectStreamMetadata)
    );

    let parsed: SampleAdvisoryPlanPayload = response
        .parse_json()
        .expect("parse OpenCode structured JSON");
    assert_eq!(parsed, expected_payload);
}

#[tokio::test]
async fn multibyte_utf8_size_boundaries_enforce_exact_byte_budget_without_utf8_truncation() {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    // Construct a JSON object containing 2-byte (`é`), 3-byte (`€`), and 4-byte (`🦀`) UTF-8 sequences
    // exceeding 256 bytes so any truncation at a byte boundary would either split a codepoint or corrupt JSON.
    let multibyte_repeat = "é€🦀".repeat(40); // (2 + 3 + 4) * 40 = 360 bytes
    let full_json = serde_json::json!({
        "kind": "multibyte_boundary_plan",
        "utf8_markers": multibyte_repeat,
        "tail_sentinel": "complete_object_end"
    })
    .to_string();
    let exact_len = full_json.len();
    assert!(exact_len > 400);

    let result_line = serde_json::json!({
        "event": "result",
        "result": {
            "status": "SUCCESS",
            "response": full_json
        }
    })
    .to_string();
    let payload_file = temp.path().join("multibyte_result.json");
    fs::write(&payload_file, &result_line).expect("write multibyte result");

    let script = format!(
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{{"event":"init","init":{{"session_id":"agy-mb-1","model":"gemini-4-argon-high"}}}}'
cat "{}"
printf '\n'
exit 0
"#,
        payload_file.display()
    );
    let fake_agy = write_executable_script(&bin_dir, "fake-agy-multibyte", &script);

    let spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::Antigravity,
        executable: fake_agy,
        model: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        allowed_models: BTreeSet::new(),
        session_id: None,
        standalone: false,
        working_directory: workdir,
        allowed_write_paths: BTreeSet::new(),
        prompt: "multibyte boundary check".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    };

    // 1. Exact byte boundary (`max_response_bytes == exact_len`) succeeds with full multibyte text intact.
    let exact_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::new(exact_len))
            .await
            .expect("exact budget run");
    assert!(exact_out.has_verified_response());
    let resp = exact_out.verified_response().unwrap();
    assert_eq!(resp.byte_count, exact_len);
    assert_eq!(resp.text, full_json);

    // 2. `exact_len + 1` also succeeds.
    let plus_one_out = run_advisory_worker_with_structured_result(
        &spec,
        AdvisoryResponseBudget::new(exact_len + 1),
    )
    .await
    .expect("exact_len + 1 budget run");
    assert!(plus_one_out.has_verified_response());

    // 3. `exact_len - 1` and `exact_len - 3` (falling inside multibyte UTF-8 characters) fail closed
    //    with `ResponseTooLarge` while retaining the underlying `WorkerExecutionReport`.
    for under_budget in [exact_len - 1, exact_len - 3, 256] {
        let over_out = run_advisory_worker_with_structured_result(
            &spec,
            AdvisoryResponseBudget::new(under_budget),
        )
        .await
        .expect("oversize response must still return Ok(AdvisoryStructuredExecution)");
        assert!(!over_out.has_verified_response());
        assert!(over_out.response.is_none());
        assert_eq!(over_out.execution.status, WorkerRunStatus::Succeeded);
        let err = over_out
            .response_error
            .expect("response_error must be populated");
        assert_eq!(err.code, AdvisoryResponseErrorCode::ResponseTooLarge);
        assert_eq!(err.observed_bytes, Some(exact_len));
        assert_eq!(err.max_response_bytes, Some(under_budget));
        assert_eq!(err.line_number, Some(2));
    }
}

#[tokio::test]
async fn stream_overflow_incomplete_missing_and_invalid_json_retain_execution_report_and_fail_closed(
) {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    // Case 1: Stream line overflow (`StdoutLineExceeded`)
    let (_, large_json) = build_large_sample_plan_payload();
    let result_line = serde_json::json!({
        "event": "result",
        "result": {
            "status": "SUCCESS",
            "response": large_json
        }
    })
    .to_string();
    let overflow_file = temp.path().join("overflow_line.json");
    fs::write(&overflow_file, &result_line).expect("write overflow line");

    let overflow_script = format!(
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{{"event":"init","init":{{"session_id":"agy-ovf-1","model":"gemini-4-argon-high"}}}}'
cat "{}"
printf '\n'
exit 0
"#,
        overflow_file.display()
    );
    let fake_overflow = write_executable_script(&bin_dir, "fake-agy-overflow", &overflow_script);
    let mut overflow_spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::Antigravity,
        executable: fake_overflow,
        model: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        allowed_models: BTreeSet::new(),
        session_id: None,
        standalone: false,
        working_directory: workdir.clone(),
        allowed_write_paths: BTreeSet::new(),
        prompt: "overflow test".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets {
            max_line_bytes: 512,
            ..ExecutionBudgets::default()
        },
    };
    let ovf_out = run_advisory_worker_with_structured_result(
        &overflow_spec,
        AdvisoryResponseBudget::default(),
    )
    .await
    .expect("stream overflow run returns structured report");
    assert!(!ovf_out.has_verified_response());
    assert!(ovf_out.response.is_none());
    assert_eq!(ovf_out.execution.status, WorkerRunStatus::OutputOverflow);
    assert_eq!(
        ovf_out.execution.overflow,
        Some(OutputOverflowKind::StdoutLineExceeded {
            max_line_bytes: 512
        })
    );
    assert_eq!(
        ovf_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::StreamOutputOverflow)
    );

    // Case 2: Missing terminal result event (`MissingResultEvidence` / `MissingTerminalFraming`)
    let missing_res_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"event":"init","init":{"session_id":"agy-miss-1","model":"gemini-4-argon-high"}}'
printf '%s\n' '{"event":"step_update","step_update":{"step_type":"thinking","message":"{\"plan\":true}"}}'
exit 0
"#;
    overflow_spec.executable =
        write_executable_script(&bin_dir, "fake-agy-missing-res", missing_res_script);
    overflow_spec.budgets = ExecutionBudgets::default();
    let miss_out = run_advisory_worker_with_structured_result(
        &overflow_spec,
        AdvisoryResponseBudget::default(),
    )
    .await
    .expect("missing result run");
    assert!(!miss_out.has_verified_response());
    assert_eq!(
        miss_out.execution.status,
        WorkerRunStatus::MissingResultEvidence
    );
    assert_eq!(
        miss_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::MissingTerminalFraming)
    );

    // Case 3: Truncated / invalid JSON string inside a valid terminal `result.response`
    let bad_json_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"event":"init","init":{"session_id":"agy-badjson-1","model":"gemini-4-argon-high"}}'
printf '%s\n' '{"event":"result","result":{"status":"SUCCESS","response":"{\"nodes\":[{\"id\":\"n1\""}}'
exit 0
"#;
    overflow_spec.executable =
        write_executable_script(&bin_dir, "fake-agy-badjson", bad_json_script);
    let bad_json_out = run_advisory_worker_with_structured_result(
        &overflow_spec,
        AdvisoryResponseBudget::default(),
    )
    .await
    .expect("bad json run");
    assert!(!bad_json_out.has_verified_response());
    assert_eq!(bad_json_out.execution.status, WorkerRunStatus::Succeeded);
    assert_eq!(
        bad_json_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::InvalidStructuredJson)
    );

    // Case 4: OpenCode incomplete text part (`part.time` missing `end`) and truncated `reason: "length"`
    let oc_incomplete_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"step_start","sessionID":"ses_inc_1","part":{"type":"step-start","model":"opencode-go/deepseek-v4.1-flash#max"}}'
printf '%s\n' '{"type":"text","sessionID":"ses_inc_1","part":{"id":"prt_1","type":"text","text":"{\"plan\":1}","time":{"start":100}}}'
printf '%s\n' '{"type":"step_finish","sessionID":"ses_inc_1","part":{"type":"step-finish","reason":"stop"}}'
exit 0
"#;
    let fake_oc_inc = write_executable_script(&bin_dir, "fake-oc-incomplete", oc_incomplete_script);
    let oc_spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::OpenCodeV2,
        executable: fake_oc_inc,
        model: "opencode-go/deepseek-v4.1-flash#max".to_string(),
        effort: None,
        allowed_models: BTreeSet::new(),
        session_id: Some("ses_inc_1".to_string()),
        standalone: true,
        working_directory: workdir.clone(),
        allowed_write_paths: BTreeSet::new(),
        prompt: "incomplete check".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    };
    let oc_inc_out =
        run_advisory_worker_with_structured_result(&oc_spec, AdvisoryResponseBudget::default())
            .await
            .expect("oc incomplete run");
    assert!(!oc_inc_out.has_verified_response());
    assert_eq!(oc_inc_out.execution.status, WorkerRunStatus::Succeeded);
    assert_eq!(
        oc_inc_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::IncompleteOrPartialOutput)
    );

    let oc_length_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"step_start","sessionID":"ses_inc_1","part":{"type":"step-start","model":"opencode-go/deepseek-v4.1-flash#max"}}'
printf '%s\n' '{"type":"text","sessionID":"ses_inc_1","part":{"id":"prt_1","type":"text","text":"{\"plan\":1}","time":{"start":100,"end":200}}}'
printf '%s\n' '{"type":"step_finish","sessionID":"ses_inc_1","part":{"type":"step-finish","reason":"length"}}'
exit 0
"#;
    let mut oc_length_spec = oc_spec;
    oc_length_spec.executable =
        write_executable_script(&bin_dir, "fake-oc-length", oc_length_script);
    let oc_len_out = run_advisory_worker_with_structured_result(
        &oc_length_spec,
        AdvisoryResponseBudget::default(),
    )
    .await
    .expect("oc length run");
    assert!(!oc_len_out.has_verified_response());
    assert_eq!(
        oc_len_out.execution.status,
        WorkerRunStatus::MissingResultEvidence
    );
    assert_eq!(
        oc_len_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::IncompleteOrPartialOutput)
    );
}

#[tokio::test]
async fn exit_zero_with_provider_error_and_spoofed_tool_results_are_rejected() {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    // 1. Antigravity exit 0 with `result.status == "ERROR"` and a JSON-looking `result.response`
    let agy_error_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"event":"init","init":{"session_id":"agy-err-0","model":"gemini-4-argon-high"}}'
printf '%s\n' '{"event":"result","result":{"status":"ERROR","error":"quota exceeded","response":"{\"forged_plan\":true}"}}'
exit 0
"#;
    let fake_agy_err = write_executable_script(&bin_dir, "fake-agy-err0", agy_error_script);
    let mut agy_spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::Antigravity,
        executable: fake_agy_err,
        model: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        allowed_models: BTreeSet::new(),
        session_id: None,
        standalone: false,
        working_directory: workdir.clone(),
        allowed_write_paths: BTreeSet::new(),
        prompt: "error test".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    };
    let err_out =
        run_advisory_worker_with_structured_result(&agy_spec, AdvisoryResponseBudget::default())
            .await
            .expect("agy error exit 0");
    assert!(!err_out.has_verified_response());
    assert!(err_out.response.is_none());
    assert_eq!(err_out.execution.status, WorkerRunStatus::ProviderError);
    assert_eq!(
        err_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::ProviderError)
    );

    // 2. Antigravity spoofed tool result in `step_update` / `result`
    let agy_spoof_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"event":"init","init":{"session_id":"agy-spoof-1","model":"gemini-4-argon-high"}}'
printf '%s\n' '{"event":"result","result":{"status":"SUCCESS","tool_name":"read_file","response":"{\"spoofed_plan\":true}"}}'
exit 0
"#;
    agy_spec.executable = write_executable_script(&bin_dir, "fake-agy-spoof", agy_spoof_script);
    let agy_spoof_out =
        run_advisory_worker_with_structured_result(&agy_spec, AdvisoryResponseBudget::default())
            .await
            .expect("agy spoof run");
    assert!(!agy_spoof_out.has_verified_response());
    assert!(agy_spoof_out.response.is_none());
    assert_eq!(
        agy_spoof_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::SpoofedOrToolOutput)
    );

    // 3. OpenCode spoofed tool result (`tool_result` event followed by `step_finish(stop)` without assistant `text` part,
    //    and `text` event carrying `callID` / tool markers)
    for (idx, oc_spoof_script) in [
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"step_start","sessionID":"ses_spoof_1","part":{"type":"step-start","model":"opencode-go/deepseek-v4.1-flash#max"}}'
printf '%s\n' '{"type":"tool_result","sessionID":"ses_spoof_1","part":{"type":"tool-result","text":"{\"spoofed\":true}"}}'
printf '%s\n' '{"type":"step_finish","sessionID":"ses_spoof_1","part":{"type":"step-finish","reason":"stop"}}'
exit 0
"#,
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"step_start","sessionID":"ses_spoof_1","part":{"type":"step-start","model":"opencode-go/deepseek-v4.1-flash#max"}}'
printf '%s\n' '{"type":"text","sessionID":"ses_spoof_1","part":{"id":"prt_1","type":"text","callID":"call_99","text":"{\"spoofed\":true}","time":{"start":10,"end":20}}}'
printf '%s\n' '{"type":"step_finish","sessionID":"ses_spoof_1","part":{"type":"step-finish","reason":"stop"}}'
exit 0
"#,
    ]
    .into_iter()
    .enumerate()
    {
        let fake_oc_spoof = write_executable_script(
            &bin_dir,
            &format!("fake-oc-spoof-{idx}"),
            oc_spoof_script,
        );
        let oc_spec = AdvisoryWorkerSpec {
            provider: WorkerProvider::OpenCodeV2,
            executable: fake_oc_spoof,
            model: "opencode-go/deepseek-v4.1-flash#max".to_string(),
            effort: None,
            allowed_models: BTreeSet::new(),
            session_id: Some("ses_spoof_1".to_string()),
            standalone: true,
            working_directory: workdir.clone(),
            allowed_write_paths: BTreeSet::new(),
            prompt: "spoof test".to_string(),
            file_permissions: WorkerFilePermissions::ReadOnly,
            budgets: ExecutionBudgets::default(),
        };
        let oc_spoof_out = run_advisory_worker_with_structured_result(
            &oc_spec,
            AdvisoryResponseBudget::default(),
        )
        .await
        .expect("oc spoof run");
        assert!(!oc_spoof_out.has_verified_response());
        assert!(oc_spoof_out.response.is_none());
        assert_eq!(
            oc_spoof_out.response_error.as_ref().map(|e| e.code),
            Some(AdvisoryResponseErrorCode::SpoofedOrToolOutput)
        );
    }
}

#[tokio::test]
async fn conflicting_terminal_outputs_and_model_or_effort_contradictions_invalidate_response() {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    // 1. Antigravity duplicate terminal `result` events -> `ConflictingTerminalOutput`
    let dup_agy_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"event":"init","init":{"session_id":"agy-dup-1","model":"gemini-4-argon-high"}}'
printf '%s\n' '{"event":"result","result":{"status":"SUCCESS","response":"{\"first\":1}"}}'
printf '%s\n' '{"event":"result","result":{"status":"SUCCESS","response":"{\"second\":2}"}}'
exit 0
"#;
    let fake_dup_agy = write_executable_script(&bin_dir, "fake-agy-dup", dup_agy_script);
    let mut agy_spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::Antigravity,
        executable: fake_dup_agy,
        model: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        allowed_models: BTreeSet::new(),
        session_id: None,
        standalone: false,
        working_directory: workdir.clone(),
        allowed_write_paths: BTreeSet::new(),
        prompt: "dup result test".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    };
    let dup_agy_out =
        run_advisory_worker_with_structured_result(&agy_spec, AdvisoryResponseBudget::default())
            .await
            .expect("dup agy run");
    assert!(!dup_agy_out.has_verified_response());
    assert!(dup_agy_out.response.is_none());
    assert_eq!(
        dup_agy_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::ConflictingTerminalOutput)
    );

    // 2. Antigravity catalog model variant (`gemini-4-argon-high`) contradicted by explicit `effort: "low"` in result
    let effort_conflict_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"event":"init","init":{"session_id":"agy-eff-1","model":"gemini-4-argon-high"}}'
printf '%s\n' '{"event":"result","effort":"low","result":{"status":"SUCCESS","response":"{\"plan\":1}"}}'
exit 0
"#;
    agy_spec.executable =
        write_executable_script(&bin_dir, "fake-agy-eff-conflict", effort_conflict_script);
    let eff_conflict_out =
        run_advisory_worker_with_structured_result(&agy_spec, AdvisoryResponseBudget::default())
            .await
            .expect("effort conflict run");
    assert!(!eff_conflict_out.has_verified_response());
    assert!(eff_conflict_out.response.is_none());
    assert_eq!(
        eff_conflict_out.execution.status,
        WorkerRunStatus::EffortMismatch
    );
    assert_eq!(
        eff_conflict_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::EffortEvidenceFailed)
    );

    // 3. Antigravity model contradiction across init and result -> `ModelMismatch` / `ModelEvidenceFailed`
    let model_conflict_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"event":"init","init":{"session_id":"agy-mod-1","model":"gemini-4-argon-high"}}'
printf '%s\n' '{"event":"result","model":"gemini-4-flash","result":{"status":"SUCCESS","response":"{\"plan\":1}"}}'
exit 0
"#;
    agy_spec.executable =
        write_executable_script(&bin_dir, "fake-agy-mod-conflict", model_conflict_script);
    let mod_conflict_out =
        run_advisory_worker_with_structured_result(&agy_spec, AdvisoryResponseBudget::default())
            .await
            .expect("model conflict run");
    assert!(!mod_conflict_out.has_verified_response());
    assert!(mod_conflict_out.response.is_none());
    assert_eq!(
        mod_conflict_out.execution.status,
        WorkerRunStatus::ModelMismatch
    );
    assert_eq!(
        mod_conflict_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::ModelEvidenceFailed)
    );

    // 4. OpenCode duplicate text parts in a single step -> `ConflictingTerminalOutput`
    let oc_dup_text_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"step_start","sessionID":"ses_dup_1","part":{"type":"step-start","model":"opencode-go/deepseek-v4.1-flash#max"}}'
printf '%s\n' '{"type":"text","sessionID":"ses_dup_1","part":{"id":"prt_1","messageID":"msg_1","type":"text","text":"{\"a\":1}","time":{"start":10,"end":20}}}'
printf '%s\n' '{"type":"text","sessionID":"ses_dup_1","part":{"id":"prt_2","messageID":"msg_1","type":"text","text":"{\"b\":2}","time":{"start":21,"end":30}}}'
printf '%s\n' '{"type":"step_finish","sessionID":"ses_dup_1","part":{"type":"step-finish","messageID":"msg_1","reason":"stop"}}'
exit 0
"#;
    let fake_oc_dup = write_executable_script(&bin_dir, "fake-oc-dup-text", oc_dup_text_script);
    let oc_spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::OpenCodeV2,
        executable: fake_oc_dup,
        model: "opencode-go/deepseek-v4.1-flash#max".to_string(),
        effort: None,
        allowed_models: BTreeSet::new(),
        session_id: Some("ses_dup_1".to_string()),
        standalone: true,
        working_directory: workdir.clone(),
        allowed_write_paths: BTreeSet::new(),
        prompt: "dup text test".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    };
    let oc_dup_out =
        run_advisory_worker_with_structured_result(&oc_spec, AdvisoryResponseBudget::default())
            .await
            .expect("oc dup text run");
    assert!(!oc_dup_out.has_verified_response());
    assert_eq!(oc_dup_out.execution.status, WorkerRunStatus::Succeeded);
    assert_eq!(
        oc_dup_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::ConflictingTerminalOutput)
    );

    // 5. Owned OpenCode session export `terminal_message_id` conflicts with stream `messageID` -> `ConflictingTerminalOutput`
    let oc_msg_mismatch_script = r#"#!/usr/bin/env sh
set -eu
NOW_MS=$(( $(date +%s) * 1000 + 2000 ))
DONE_MS=$(( NOW_MS + 100 ))
if [ "$1" = "api" ] && [ "$2" = "--standalone" ] && [ "$3" = "session.create" ]; then
    printf '{"data":{"id":"ses_owned_mismatch","location":{"directory":"%s"},"time":{"created":%s}}}\n' "$(pwd)" "$NOW_MS"
    exit 0
fi
if [ "$1" = "api" ] && [ "$2" = "--standalone" ] && [ "$3" = "session.get" ]; then
    printf '{"data":{"id":"ses_owned_mismatch","outcome":"succeeded","location":{"directory":"%s"},"time":{"created":%s,"idle":%s}}}\n' "$(pwd)" "$NOW_MS" "$DONE_MS"
    exit 0
fi
if [ "$1" = "session" ] && [ "$2" = "export" ] && [ "$3" = "--standalone" ] && [ "$4" = "--sanitize" ]; then
    printf '{"info":{"id":"ses_owned_mismatch","outcome":"succeeded","location":{"directory":"%s"},"time":{"created":%s,"idle":%s}},"messages":[{"id":"msg_export_other","type":"assistant","sessionID":"ses_owned_mismatch","finish":"stop","time":{"created":%s,"completed":%s},"model":{"providerID":"opencode-go","id":"deepseek-v4.1-flash","variant":"max"}}]}\n' "$(pwd)" "$NOW_MS" "$DONE_MS" "$NOW_MS" "$DONE_MS"
    exit 0
fi
printf '%s\n' '{"type":"step_start","sessionID":"ses_owned_mismatch","part":{"id":"prt_s","sessionID":"ses_owned_mismatch","messageID":"msg_stream_1","type":"step-start","model":{"providerID":"opencode-go","id":"deepseek-v4.1-flash","variant":"max"}}}'
printf '%s\n' '{"type":"text","sessionID":"ses_owned_mismatch","part":{"id":"prt_t","sessionID":"ses_owned_mismatch","messageID":"msg_stream_1","type":"text","text":"{\"plan\":1}","time":{"start":10,"end":20}}}'
printf '%s\n' '{"type":"step_finish","sessionID":"ses_owned_mismatch","part":{"id":"prt_f","sessionID":"ses_owned_mismatch","messageID":"msg_stream_1","type":"step-finish","reason":"stop"}}'
exit 0
"#;
    let mut oc_mismatch_spec = oc_spec;
    oc_mismatch_spec.executable =
        write_executable_script(&bin_dir, "fake-oc-msg-mismatch", oc_msg_mismatch_script);
    oc_mismatch_spec.session_id = None;
    let oc_mismatch_out = run_advisory_worker_with_structured_result(
        &oc_mismatch_spec,
        AdvisoryResponseBudget::default(),
    )
    .await
    .expect("oc msg mismatch run");
    assert!(!oc_mismatch_out.has_verified_response());
    assert_eq!(oc_mismatch_out.execution.status, WorkerRunStatus::Succeeded);
    assert_eq!(
        oc_mismatch_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::ConflictingTerminalOutput)
    );
}

#[tokio::test]
async fn many_discarded_retained_progress_events_do_not_discard_final_structured_output() {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let (expected_payload, payload_json) = build_large_sample_plan_payload();
    let result_line = serde_json::json!({
        "event": "result",
        "result": {
            "status": "SUCCESS",
            "response": payload_json
        }
    })
    .to_string();
    let payload_file = temp.path().join("many_events_result.json");
    fs::write(&payload_file, &result_line).expect("write many_events_result");

    // Emit 1 init event + 25 step_update events + 1 final result event = 27 events while `max_events == 2`.
    let script = format!(
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{{"event":"init","init":{{"session_id":"agy-many-1","model":"gemini-4-argon-high"}}}}'
for i in $(seq 1 25); do
    printf '{{"event":"step_update","step_update":{{"step_type":"thinking","message":"progress step %d"}}}}\n' "$i"
done
cat "{}"
printf '\n'
exit 0
"#,
        payload_file.display()
    );
    let fake_agy = write_executable_script(&bin_dir, "fake-agy-many-events", &script);

    let spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::Antigravity,
        executable: fake_agy,
        model: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        allowed_models: BTreeSet::new(),
        session_id: None,
        standalone: false,
        working_directory: workdir,
        allowed_write_paths: BTreeSet::new(),
        prompt: "many events test".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets {
            max_events: 2,
            ..ExecutionBudgets::default()
        },
    };

    let out = run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
        .await
        .expect("many events run should succeed");

    assert!(out.has_verified_response());
    assert_eq!(out.execution.status, WorkerRunStatus::Succeeded);
    assert_eq!(out.execution.evidence.total_lines, 27);
    assert_eq!(out.execution.evidence.retained_events.len(), 2);
    assert_eq!(out.execution.evidence.dropped_events_count, 25);

    let resp = out
        .verified_response()
        .expect("final response must be captured");
    assert_eq!(resp.text, payload_json);
    assert_eq!(resp.framing.response_line_number, 27);
    assert_eq!(resp.framing.terminal_line_number, 27);
    let parsed: SampleAdvisoryPlanPayload = resp.parse_json().expect("parse JSON");
    assert_eq!(parsed, expected_payload);
}

#[tokio::test]
async fn pre_launch_invalid_budget_or_permissions_refuses_with_worker_error_and_legacy_report_wire_is_unchanged(
) {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"event":"init","init":{"session_id":"agy-leg-1","model":"gemini-4-argon-high"}}'
printf '%s\n' '{"event":"result","result":{"status":"SUCCESS","response":"{\"ok\":true}"}}'
exit 0
"#;
    let fake_agy = write_executable_script(&bin_dir, "fake-agy-legacy", script);

    let spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::Antigravity,
        executable: fake_agy,
        model: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        allowed_models: BTreeSet::new(),
        session_id: None,
        standalone: false,
        working_directory: workdir,
        allowed_write_paths: BTreeSet::new(),
        prompt: "legacy check".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    };

    assert_eq!(DEFAULT_MAX_ADVISORY_RESPONSE_BYTES, 262_144);
    assert_eq!(MAX_ADVISORY_RESPONSE_BYTES, 4_194_304);

    // 1. Invalid `AdvisoryResponseBudget` (0 or > MAX_ADVISORY_RESPONSE_BYTES) refuses pre-launch with `WorkerError::InvalidSpec`
    for bad_budget in [0usize, MAX_ADVISORY_RESPONSE_BYTES + 1] {
        let (_tx, mut cancel_rx) = watch::channel(false);
        let err = run_advisory_worker_with_structured_result_and_cancel(
            &spec,
            AdvisoryResponseBudget::new(bad_budget),
            &mut cancel_rx,
        )
        .await
        .expect_err("invalid response_budget must fail pre-launch");
        assert_eq!(err.code(), WorkerErrorCode::InvalidSpec);
        assert!(matches!(err, WorkerError::InvalidSpec(_)));
    }

    // 2. Non-read_only mode refuses pre-launch with `WorkerError::UnsupportedCapability`
    let mut bad_perm_spec = spec.clone();
    bad_perm_spec.file_permissions = WorkerFilePermissions::Inherit;
    let perm_err = run_advisory_worker_with_structured_result(
        &bad_perm_spec,
        AdvisoryResponseBudget::default(),
    )
    .await
    .expect_err("non-read_only advisory spec must fail pre-launch");
    assert_eq!(perm_err.code(), WorkerErrorCode::UnsupportedCapability);

    // 3. Legacy `run_advisory_worker` report wire format has no `response` or `response_error` fields
    //    and matches the `execution` field of `AdvisoryStructuredExecution`.
    let legacy_report = run_advisory_worker(&spec)
        .await
        .expect("legacy run_advisory_worker");
    let structured_report =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("structured run_advisory_worker");

    assert_eq!(legacy_report.status, WorkerRunStatus::Succeeded);
    assert_eq!(
        legacy_report.constructed_argv,
        structured_report.execution.constructed_argv
    );
    let legacy_json = serde_json::to_value(&legacy_report).expect("serialize legacy report");
    let legacy_obj = legacy_json.as_object().expect("legacy report object");
    assert!(!legacy_obj.contains_key("response"));
    assert!(!legacy_obj.contains_key("response_error"));
    assert!(!legacy_obj.contains_key("prompt"));
}

#[tokio::test]
async fn deserialized_response_integrity_and_tamper_resistance_enforces_hash_count_provider_and_budget(
) {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let (_, payload_json) = build_large_sample_payload_for_tamper_test();
    let result_line = serde_json::json!({
        "event": "result",
        "result": {
            "status": "SUCCESS",
            "response": payload_json
        }
    })
    .to_string();
    let payload_file = temp.path().join("tamper_result.json");
    fs::write(&payload_file, &result_line).expect("write tamper_result");

    let script = format!(
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{{"event":"init","init":{{"session_id":"agy-tamper-1","model":"gemini-4-argon-high"}}}}'
cat "{}"
printf '\n'
exit 0
"#,
        payload_file.display()
    );
    let fake_agy = write_executable_script(&bin_dir, "fake-agy-tamper", &script);

    let spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::Antigravity,
        executable: fake_agy,
        model: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        allowed_models: BTreeSet::new(),
        session_id: None,
        standalone: false,
        working_directory: workdir,
        allowed_write_paths: BTreeSet::new(),
        prompt: "tamper test".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    };

    let frozen_budget_bytes = payload_json.len() + 32;
    let genuine = run_advisory_worker_with_structured_result(
        &spec,
        AdvisoryResponseBudget::new(frozen_budget_bytes),
    )
    .await
    .expect("supervised run");

    // 1. Genuine supervised result round-trips through JSON and validates under frozen budget.
    let serialized_exec = serde_json::to_string(&genuine).expect("serialize structured execution");
    let roundtripped: AdvisoryStructuredExecution =
        serde_json::from_str(&serialized_exec).expect("deserialize structured execution");
    assert!(roundtripped.has_verified_response());
    assert!(roundtripped.has_verified_response_within_budget(frozen_budget_bytes));
    assert!(roundtripped.verified_response().is_some());
    assert!(roundtripped
        .verified_response_within_budget(frozen_budget_bytes)
        .is_some());

    let serialized_resp = serde_json::to_string(roundtripped.verified_response().unwrap())
        .expect("serialize bounded response");
    let roundtripped_resp: BoundedAdvisoryResponse =
        serde_json::from_str(&serialized_resp).expect("deserialize bounded response");
    roundtripped_resp
        .validate_integrity(frozen_budget_bytes)
        .expect("round-tripped response must validate");
    roundtripped_resp
        .validate_integrity_with_budget(AdvisoryResponseBudget::new(frozen_budget_bytes))
        .expect("round-tripped response must validate with budget");
    assert!(roundtripped_resp.is_valid(frozen_budget_bytes));

    // 2. Per-call byte limit is rechecked when loading persisted artifacts.
    let tighter_budget = payload_json.len() - 1;
    assert!(roundtripped_resp
        .validate_integrity(tighter_budget)
        .is_err());
    assert!(!roundtripped_resp.is_valid(tighter_budget));
    assert!(!roundtripped.has_verified_response_within_budget(tighter_budget));
    assert!(roundtripped
        .verified_response_within_budget(tighter_budget)
        .is_none());
    assert!(roundtripped_resp.validate_integrity(0).is_err());
    assert!(roundtripped_resp
        .validate_integrity(MAX_ADVISORY_RESPONSE_BYTES + 1)
        .is_err());

    // 3. Tampered `text` (even same-length valid JSON) is rejected by SHA-256 integrity check.
    let mut tampered_text = roundtripped.clone();
    let orig_text = tampered_text.response.as_ref().unwrap().text.clone();
    let altered_text = orig_text.replacen("\"node-01\"", "\"node-99\"", 1);
    assert_eq!(altered_text.len(), orig_text.len());
    tampered_text.response.as_mut().unwrap().text = altered_text;
    // `parse_json` still parses untrusted JSON, but integrity/verified status rejects the tamper!
    assert!(tampered_text
        .response
        .as_ref()
        .unwrap()
        .parse_json_object()
        .is_ok());
    assert!(!tampered_text.has_verified_response());
    assert!(tampered_text.verified_response().is_none());
    assert!(tampered_text
        .response
        .as_ref()
        .unwrap()
        .validate_integrity(frozen_budget_bytes)
        .is_err());

    // 4. Tampered `sha256_digest` is rejected.
    let mut tampered_hash = roundtripped.clone();
    tampered_hash.response.as_mut().unwrap().sha256_digest = "0".repeat(64);
    assert!(!tampered_hash.has_verified_response());
    assert!(tampered_hash.verified_response().is_none());

    // 5. Tampered `byte_count` is rejected.
    let mut tampered_count = roundtripped.clone();
    tampered_count.response.as_mut().unwrap().byte_count += 1;
    assert!(!tampered_count.has_verified_response());
    assert!(tampered_count.verified_response().is_none());

    // 6. Tampered `response.provider` (or `source` / `framing` mismatch against `execution`) is rejected.
    let mut tampered_provider = roundtripped.clone();
    tampered_provider.response.as_mut().unwrap().provider = WorkerProvider::OpenCodeV2;
    assert!(!tampered_provider.has_verified_response());
    assert!(tampered_provider.verified_response().is_none());

    let mut tampered_source_and_provider = roundtripped.clone();
    {
        let r = tampered_source_and_provider.response.as_mut().unwrap();
        r.provider = WorkerProvider::OpenCodeV2;
        r.source = AdvisoryResponseSource::OpenCodeAssistantTextPartStop;
        r.framing.response_line_number = 2;
        r.framing.terminal_line_number = 3;
        r.framing.effort_evidence_source = Some(EffortEvidenceSource::DirectStreamMetadata);
    }
    // `r` alone looks like OpenCode, but conflicts with `execution.provider == Antigravity`!
    assert!(!tampered_source_and_provider.has_verified_response());
    assert!(tampered_source_and_provider.verified_response().is_none());

    // 7. Tampered `framing.session_id` conflicting with `execution` observed session is rejected.
    let mut tampered_session = roundtripped;
    tampered_session
        .response
        .as_mut()
        .unwrap()
        .framing
        .session_id = Some("agy-other-session".to_string());
    assert!(!tampered_session.has_verified_response());
    assert!(tampered_session.verified_response().is_none());
}

fn build_large_sample_payload_for_tamper_test() -> (SampleAdvisoryPlanPayload, String) {
    build_large_sample_plan_payload()
}

#[tokio::test]
async fn codex_recoverable_command_execution_nonzero_exit_preserves_valid_turn_and_rejects_fatal_or_spoofed_cases(
) {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir(workdir.join(".git")).expect("isolate fixture project boundary");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let (expected_payload, payload_json) = build_large_sample_plan_payload();
    let payload_raw_file = temp.path().join("codex_final_payload.json");
    fs::write(&payload_raw_file, &payload_json).expect("write payload_raw_file");
    let final_msg_file = temp.path().join("codex_final_agent_msg.json");
    let final_msg_event = serde_json::json!({
        "type": "item.completed",
        "item": {
            "id": "item_msg_final_1",
            "type": "agent_message",
            "text": payload_json
        }
    });
    fs::write(
        &final_msg_file,
        serde_json::to_string(&final_msg_event).expect("serialize final agent_message"),
    )
    .expect("write final agent_message file");

    // 1. Valid Codex turn with a failing read-only command_execution item (exit_code: 1)
    //    and an intermediate completed agent_message before the designated final agent_message.
    let ok_script = format!(
        r#"#!/usr/bin/env sh
set -eu
OUT_LAST_MSG=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-last-message|-o) OUT_LAST_MSG="$2"; shift 2 ;;
        --) shift; break ;;
        *) shift 1 ;;
    esac
done
printf '%s\n' '{{"type":"thread.started","thread_id":"thread_codex_rec_1","model":"gpt-6-astra","reasoning_effort":"high"}}'
printf '%s\n' '{{"type":"turn.started"}}'
printf '%s\n' '{{"type":"item.started","item":{{"id":"item_cmd_1","type":"command_execution","command":"rg -n missing_symbol src/","status":"in_progress"}}}}'
printf '%s\n' '{{"type":"item.completed","item":{{"id":"item_cmd_1","type":"command_execution","command":"rg -n missing_symbol src/","aggregated_output":"","exit_code":1,"status":"failed"}}}}'
printf '%s\n' '{{"type":"item.completed","item":{{"id":"item_msg_intermediate_0","type":"agent_message","text":"{{\"step\":\"no_matches_found\",\"continuing\":true}}"}}}}'
cat "{}"
printf '\n'
if [ -n "$OUT_LAST_MSG" ]; then
    cp "{}" "$OUT_LAST_MSG"
fi
printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":140,"cached_input_tokens":32,"output_tokens":88}}}}'
exit 0
"#,
        final_msg_file.display(),
        payload_raw_file.display(),
    );
    let fake_codex_ok = write_executable_script(&bin_dir, "fake-codex-rec-ok", &ok_script);
    let mut codex_spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::CodexCli,
        executable: fake_codex_ok,
        model: "gpt-6-astra".to_string(),
        effort: Some("high".to_string()),
        allowed_models: BTreeSet::from(["gpt-6-astra".to_string()]),
        session_id: None,
        standalone: false,
        working_directory: workdir.clone(),
        allowed_write_paths: BTreeSet::new(),
        prompt: "Inspect repository and emit structured JSON plan.".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    };

    let ok_out =
        run_advisory_worker_with_structured_result(&codex_spec, AdvisoryResponseBudget::default())
            .await
            .expect("codex recoverable command run should succeed");
    assert!(ok_out.has_verified_response());
    assert!(ok_out.response_error.is_none());
    assert_eq!(ok_out.execution.status, WorkerRunStatus::Succeeded);
    assert!(ok_out.execution.has_complete_natural_completion_evidence());
    assert!(ok_out.execution.evidence.has_valid_codex_turn_lifecycle());
    assert_eq!(ok_out.execution.evidence.recoverable_step_errors_count, 1);
    assert!(ok_out.execution.evidence.provider_errors.is_empty());
    assert!(ok_out.execution.evidence.blocked_events.is_empty());
    let resp = ok_out.verified_response().expect("verified codex response");
    assert_eq!(resp.provider, WorkerProvider::CodexCli);
    assert_eq!(resp.text, payload_json);
    assert_eq!(resp.sha256_digest, sha256_hex(payload_json.as_bytes()));
    let parsed: SampleAdvisoryPlanPayload = resp.parse_json().expect("parse codex structured plan");
    assert_eq!(parsed, expected_payload);

    // 2. Fatal top-level error marker on item.completed still rejects as ProviderError.
    let top_err_script = format!(
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{{"type":"thread.started","thread_id":"thread_codex_top_err","model":"gpt-6-astra","reasoning_effort":"high"}}'
printf '%s\n' '{{"type":"turn.started"}}'
printf '%s\n' '{{"type":"item.completed","error":"fatal stream error","item":{{"id":"item_cmd_1","type":"command_execution","command":"rg -n missing src/","exit_code":1,"status":"failed"}}}}'
cat "{}"
printf '\n'
printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":10,"output_tokens":10}}}}'
exit 0
"#,
        final_msg_file.display()
    );
    codex_spec.executable =
        write_executable_script(&bin_dir, "fake-codex-top-err", &top_err_script);
    let top_err_out =
        run_advisory_worker_with_structured_result(&codex_spec, AdvisoryResponseBudget::default())
            .await
            .expect("codex top-level error run");
    assert!(!top_err_out.has_verified_response());
    assert_eq!(top_err_out.execution.status, WorkerRunStatus::ProviderError);
    assert_eq!(
        top_err_out.execution.evidence.recoverable_step_errors_count,
        0
    );
    assert!(!top_err_out.execution.evidence.provider_errors.is_empty());
    assert!(!top_err_out
        .execution
        .evidence
        .has_valid_codex_turn_lifecycle());
    assert_eq!(
        top_err_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::ProviderError)
    );

    // 3. turn.failed after a recoverable command failure still rejects as ProviderError.
    let turn_failed_script = format!(
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{{"type":"thread.started","thread_id":"thread_codex_tf","model":"gpt-6-astra","reasoning_effort":"high"}}'
printf '%s\n' '{{"type":"turn.started"}}'
printf '%s\n' '{{"type":"item.completed","item":{{"id":"item_cmd_1","type":"command_execution","command":"rg -n missing src/","exit_code":1,"status":"failed"}}}}'
cat "{}"
printf '\n'
printf '%s\n' '{{"type":"turn.failed","error":{{"message":"model turn failed"}}}}'
exit 0
"#,
        final_msg_file.display()
    );
    codex_spec.executable =
        write_executable_script(&bin_dir, "fake-codex-turn-failed", &turn_failed_script);
    let tf_out =
        run_advisory_worker_with_structured_result(&codex_spec, AdvisoryResponseBudget::default())
            .await
            .expect("codex turn.failed run");
    assert!(!tf_out.has_verified_response());
    assert_eq!(tf_out.execution.status, WorkerRunStatus::ProviderError);
    assert!(!tf_out.execution.evidence.provider_errors.is_empty());
    assert!(!tf_out.execution.evidence.has_valid_codex_turn_lifecycle());

    // 4. Denied action / permission block on command_execution stays Blocked first.
    let denied_script = format!(
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{{"type":"thread.started","thread_id":"thread_codex_blk","model":"gpt-6-astra","reasoning_effort":"high"}}'
printf '%s\n' '{{"type":"turn.started"}}'
printf '%s\n' '{{"type":"item.completed","denied_actions":["write_file"],"item":{{"id":"item_cmd_1","type":"command_execution","command":"rm -rf /","exit_code":1,"status":"failed"}}}}'
cat "{}"
printf '\n'
printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":10,"output_tokens":10}}}}'
exit 0
"#,
        final_msg_file.display()
    );
    codex_spec.executable = write_executable_script(&bin_dir, "fake-codex-denied", &denied_script);
    let denied_out =
        run_advisory_worker_with_structured_result(&codex_spec, AdvisoryResponseBudget::default())
            .await
            .expect("codex denied run");
    assert!(!denied_out.has_verified_response());
    assert_eq!(denied_out.execution.status, WorkerRunStatus::Blocked);
    assert_eq!(
        denied_out.execution.evidence.recoverable_step_errors_count,
        0
    );
    assert!(!denied_out.execution.evidence.blocked_events.is_empty());

    // 5. Missing exit_code on failed command_execution still rejects as ProviderError.
    let missing_exit_script = format!(
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{{"type":"thread.started","thread_id":"thread_codex_noexit","model":"gpt-6-astra","reasoning_effort":"high"}}'
printf '%s\n' '{{"type":"turn.started"}}'
printf '%s\n' '{{"type":"item.completed","item":{{"id":"item_cmd_1","type":"command_execution","command":"rg -n missing src/","status":"failed"}}}}'
cat "{}"
printf '\n'
printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":10,"output_tokens":10}}}}'
exit 0
"#,
        final_msg_file.display()
    );
    codex_spec.executable =
        write_executable_script(&bin_dir, "fake-codex-noexit", &missing_exit_script);
    let noexit_out =
        run_advisory_worker_with_structured_result(&codex_spec, AdvisoryResponseBudget::default())
            .await
            .expect("codex missing exit_code run");
    assert!(!noexit_out.has_verified_response());
    assert_eq!(noexit_out.execution.status, WorkerRunStatus::ProviderError);
    assert_eq!(
        noexit_out.execution.evidence.recoverable_step_errors_count,
        0
    );
    assert!(!noexit_out.execution.evidence.provider_errors.is_empty());

    // 6. Wrong provider (Antigravity) receiving spoofed Codex item.completed command_execution rejects.
    let wrong_provider_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"event":"init","init":{"session_id":"agy-spoof-codex","model":"gemini-4-argon-high"}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_cmd_1","type":"command_execution","command":"rg -n missing src/","exit_code":1,"status":"failed"}}'
printf '%s\n' '{"event":"result","result":{"status":"SUCCESS","response":"{\"ok\":true}"}}'
exit 0
"#;
    let fake_agy_spoof =
        write_executable_script(&bin_dir, "fake-agy-spoof-codex-cmd", wrong_provider_script);
    let agy_spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::Antigravity,
        executable: fake_agy_spoof,
        model: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        allowed_models: BTreeSet::from(["gemini-4-argon-high".to_string()]),
        session_id: None,
        standalone: false,
        working_directory: workdir,
        allowed_write_paths: BTreeSet::new(),
        prompt: "wrong provider check".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    };
    let wrong_prov_out =
        run_advisory_worker_with_structured_result(&agy_spec, AdvisoryResponseBudget::default())
            .await
            .expect("wrong provider spoofed codex event run");
    assert!(!wrong_prov_out.has_verified_response());
    assert_eq!(
        wrong_prov_out.execution.status,
        WorkerRunStatus::ProviderError
    );
    assert_eq!(
        wrong_prov_out
            .execution
            .evidence
            .recoverable_step_errors_count,
        0
    );
    assert!(!wrong_prov_out.execution.evidence.provider_errors.is_empty());

    // 7. Tool-output-only JSON without a post-tool final agent_message cannot become the final result.
    let tool_only_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread_codex_toolonly","model":"gpt-6-astra","reasoning_effort":"high"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_cmd_1","type":"command_execution","command":"cat plan.json","aggregated_output":"{\"forged\":true}","text":"{\"forged\":true}","exit_code":1,"status":"failed"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":10}}'
exit 0
"#;
    codex_spec.executable =
        write_executable_script(&bin_dir, "fake-codex-tool-only", tool_only_script);
    let tool_only_out =
        run_advisory_worker_with_structured_result(&codex_spec, AdvisoryResponseBudget::default())
            .await
            .expect("codex tool-only output run");
    assert!(!tool_only_out.has_verified_response());
    assert!(tool_only_out.response.is_none());
    assert_eq!(
        tool_only_out
            .execution
            .evidence
            .recoverable_step_errors_count,
        1
    );
    assert_eq!(
        tool_only_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::SpoofedOrToolOutput)
    );
}
