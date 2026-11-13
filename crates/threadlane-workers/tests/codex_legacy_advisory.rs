use std::collections::BTreeMap;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use threadlane_workers::{
    build_codex_task_files_permission_profile_config_for_scopes, is_pinned_codex_task_files_argv,
    AdvisoryStructuredExecution, CodexTurnLifecycleEvidence, WorkerRunStatus, WriteScopeKind,
    CODEX_ADVISORY_DISABLED_FEATURES, CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG,
};

/// Exact earlier bounded static read-only profile retained for successful historical planning
/// artifacts. This is a literal persisted-evidence vector, not an alias to the current constant.
const HISTORICAL_BOUNDED_READ_ONLY_PROFILE: &str = r#"permissions.threadlane-advisory={"filesystem"={"glob_scan_max_depth"=64,":minimal"="read",":workspace_roots"={"."="read","**/.env"="deny","**/.env.*"="deny","**/codex.toml"="deny","**/.codex.toml"="deny","**/.git"="deny","**/.git/**"="deny","**/.codex"="deny","**/.codex/**"="deny","**/.agents"="deny","**/.agents/**"="deny","**/.aws"="deny","**/.aws/**"="deny"},"/nix/store"="read"},"network"={"enabled"=false}}"#;

// Historical wire shape: neither executed_executable nor codex_turn_lifecycle existed.
// Launch pins and response/usage provenance are retained exactly; no current marker is fabricated.
fn historical_json() -> Value {
    let mut argv: Vec<String> = [
        "--no-daemon",
        "exec",
        "--json",
        "--ephemeral",
        "--ignore-user-config",
        "--model",
        "gpt-6.1-sol",
        "-c",
        "model_reasoning_effort=\"xhigh\"",
        "-c",
        "approval_policy=\"never\"",
        "-c",
        "model_provider=\"openai\"",
        "-c",
        "default_permissions=\"threadlane-advisory\"",
        "-c",
        CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG,
        "-c",
        "web_search=\"disabled\"",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    for feature in CODEX_ADVISORY_DISABLED_FEATURES {
        argv.extend(["--disable".to_string(), feature.to_string()]);
    }
    argv.extend([
        "--".to_string(),
        "[redacted historical advisory instruction]".to_string(),
    ]);
    let text = r#"{"verdict":"approved","summary":"Historical read-only review."}"#;
    let digest = format!("{:x}", Sha256::digest(text.as_bytes()));
    let usage = json!({"input_tokens":1420,"cached_input_tokens":1024,"output_tokens":318});
    json!({
        "execution": {
            "provider":"codex_cli",
            "requested_model":{"base_model":"gpt-6.1-sol","effort":"xhigh","cli_model_arg":"gpt-6.1-sol"},
            "constructed_argv":argv,
            "working_directory":"/tmp/threadlane-historical-advisory",
            "status":"succeeded","exit_code":0,"process_group_id":42,
            "duration_ms":25,"stdout_bytes_captured":900,"stderr_bytes_captured":0,
            "descendants_terminated_after_exit":false,"cleanup_incomplete":false,
            "dropped_live_events":0,
            "evidence":{
                "total_lines":4,"valid_json_events":4,"malformed_lines_count":0,
                "malformed_diagnostics":[],"saw_init_event":true,"saw_result_event":true,
                "model_evidence":{"status":"missing","requested":"gpt-6.1-sol"},
                "effort_evidence":{"status":"missing","requested":"xhigh"},
                "session_evidence":{"status":"not_requested","observed":"thread-history-1"},
                "observed_session_id":"thread-history-1","observed_usage":usage,
                "provider_errors":[],"blocked_events":[],"recoverable_step_errors_count":0,
                "retained_events":[
                    {"line_number":1,"kind":"init","raw_type":"thread.started","session_id":"thread-history-1"},
                    {"line_number":2,"kind":"other","raw_type":"turn.started"},
                    {"line_number":3,"kind":"progress","raw_type":"item.completed:agent_message","summary":text},
                    {"line_number":4,"kind":"result","raw_type":"turn.completed"}
                ],"dropped_events_count":0
            }
        },
        "response":{
            "text":text,"byte_count":text.len(),"sha256_digest":digest,
            "provider":"codex_cli","source":"codex_agent_message_turn_completed",
            "framing":{"response_line_number":3,"terminal_line_number":4,
                "session_id":"thread-history-1","item_id":"item-final-1","usage":usage}
        }
    })
}

fn deserialize(value: &Value) -> AdvisoryStructuredExecution {
    serde_json::from_slice(&serde_json::to_vec(value).expect("serialize historical JSON"))
        .expect("deserialize historical advisory artifact")
}

#[test]
fn historical_codex_advisory_json_remains_readable_only_through_persisted_validation() {
    let wire = historical_json();
    assert!(wire["execution"].get("executed_executable").is_none());
    assert!(wire["execution"]["evidence"]
        .get("codex_turn_lifecycle")
        .is_none());
    let artifact = deserialize(&wire);
    assert!(artifact.execution.executed_executable.is_none());
    assert!(artifact.execution.evidence.codex_turn_lifecycle.is_none());
    assert!(artifact
        .execution
        .has_complete_legacy_codex_advisory_evidence());
    assert!(!artifact
        .execution
        .has_complete_natural_completion_evidence());
    assert!(artifact.validate_verified_response(4096).is_err());
    assert!(!artifact.has_verified_response());
    assert!(artifact.validate_persisted_verified_response(4096).is_ok());
    assert_eq!(artifact.execution.effective_observed_model(), None);
    assert_eq!(artifact.execution.effective_observed_effort(), None);
    // Persistence validation has no mutation side effect or new serialized markers.
    let roundtrip = serde_json::to_value(&artifact).expect("serialize legacy artifact");
    assert!(roundtrip["execution"].get("executed_executable").is_none());
    assert!(roundtrip["execution"]["evidence"]
        .get("codex_turn_lifecycle")
        .is_none());
}

#[test]
fn historical_bounded_static_profile_remains_exact_legacy_evidence_only() {
    let mut wire = historical_json();
    wire["execution"]["constructed_argv"][16] = json!(HISTORICAL_BOUNDED_READ_ONLY_PROFILE);
    let artifact = deserialize(&wire);
    assert!(artifact
        .execution
        .has_complete_legacy_codex_advisory_evidence());
    assert!(artifact.validate_persisted_verified_response(4096).is_ok());

    for weakened in [
        HISTORICAL_BOUNDED_READ_ONLY_PROFILE.replace("\".\"=\"read\"", "\".\"=\"write\""),
        HISTORICAL_BOUNDED_READ_ONLY_PROFILE.replace(
            "network\"={\"enabled\"=false}",
            "network\"={\"enabled\"=true}",
        ),
        HISTORICAL_BOUNDED_READ_ONLY_PROFILE
            .replace("glob_scan_max_depth\"=64", "glob_scan_max_depth\"=128"),
        HISTORICAL_BOUNDED_READ_ONLY_PROFILE.replace("\"**/.env\"=\"deny\",", ""),
    ] {
        let mut wire = historical_json();
        wire["execution"]["constructed_argv"][16] = json!(weakened);
        let rejected = deserialize(&wire);
        assert!(!rejected
            .execution
            .has_complete_legacy_codex_advisory_evidence());
        assert!(rejected.validate_persisted_verified_response(4096).is_err());
    }
}

#[test]
fn legacy_codex_advisory_predicate_refuses_mutable_launches_current_markers_and_failed_evidence() {
    let original = deserialize(&historical_json());
    let mut with_executable = original.clone();
    with_executable.execution.executed_executable = Some("/tmp/codex".into());
    assert!(!with_executable
        .execution
        .has_complete_legacy_codex_advisory_evidence());
    assert!(with_executable
        .validate_persisted_verified_response(4096)
        .is_err());
    let mut with_lifecycle = original.clone();
    with_lifecycle.execution.evidence.codex_turn_lifecycle =
        Some(CodexTurnLifecycleEvidence::default());
    assert!(!with_lifecycle
        .execution
        .has_complete_legacy_codex_advisory_evidence());
    assert!(with_lifecycle
        .validate_persisted_verified_response(4096)
        .is_err());

    // A mutable report with both markers stripped still fails the exact historical read-only pin.
    let dir = tempfile::TempDir::new().expect("permission fixture");
    std::fs::create_dir(dir.path().join(".git")).expect("isolated project boundary");
    let config = build_codex_task_files_permission_profile_config_for_scopes(
        dir.path(),
        &BTreeMap::from([("file.rs".to_string(), WriteScopeKind::ExactFile)]),
    )
    .expect("task profile");
    let mut mutable = original.clone();
    mutable
        .execution
        .constructed_argv
        .insert(5, "--ignore-rules".to_string());
    mutable.execution.constructed_argv[15] =
        "default_permissions=\"threadlane-task-files\"".to_string();
    mutable.execution.constructed_argv[17] = config;
    assert!(is_pinned_codex_task_files_argv(
        &mutable.execution.constructed_argv,
        &mutable.execution.requested_model
    ));
    assert!(!mutable
        .execution
        .has_complete_legacy_codex_advisory_evidence());
    assert!(mutable.validate_persisted_verified_response(4096).is_err());

    for status in [
        WorkerRunStatus::Cancelled,
        WorkerRunStatus::TimedOut,
        WorkerRunStatus::ProviderError,
        WorkerRunStatus::MissingResultEvidence,
        WorkerRunStatus::ModelMismatch,
        WorkerRunStatus::EffortMismatch,
    ] {
        let mut failed = original.clone();
        failed.execution.status = status;
        assert!(
            !failed
                .execution
                .has_complete_legacy_codex_advisory_evidence(),
            "{status:?}"
        );
        assert!(
            failed.validate_persisted_verified_response(4096).is_err(),
            "{status:?}"
        );
    }
    for (pointer, replacement) in [
        ("/execution/provider", json!("antigravity")),
        ("/execution/requested_session_id", json!("foreign-session")),
        ("/execution/allocated_session_id", json!("foreign-session")),
        (
            "/execution/opencode_metadata_error",
            json!("invalid_metadata"),
        ),
        (
            "/execution/opencode_metadata",
            json!({
                "source":"opencode_sanitized_session_export","strength":"cli_reported_per_assistant",
                "session_id":"foreign-session","observed_model":"openai/gpt-6.1-sol",
                "observed_effort":"xhigh","assistant_message_count":1,"terminal_message_id":"message-1"
            }),
        ),
        ("/execution/exit_code", json!(1)),
        ("/execution/signal", json!(15)),
        ("/execution/cleanup_incomplete", json!(true)),
        (
            "/execution/overflow",
            json!({"kind":"stdout_total_exceeded","max_stdout_bytes":512}),
        ),
        ("/execution/evidence/saw_init_event", json!(false)),
        ("/execution/evidence/saw_result_event", json!(false)),
        ("/execution/evidence/observed_session_id", Value::Null),
        ("/execution/evidence/malformed_lines_count", json!(1)),
        (
            "/execution/evidence/provider_errors",
            json!([{"line_number":3,"message":"failed"}]),
        ),
        (
            "/execution/evidence/blocked_events",
            json!([{"kind":"permission_required","line_number":3,"summary":"blocked"}]),
        ),
        (
            "/execution/evidence/model_evidence",
            json!({"status":"mismatch","requested":"gpt-6.1-sol","observed":"wrong","detail":"conflict"}),
        ),
        (
            "/execution/evidence/effort_evidence",
            json!({"status":"mismatch","requested":"xhigh","observed":"low","detail":"conflict"}),
        ),
        (
            "/execution/evidence/session_evidence",
            json!({"status":"mismatch","requested":null,"observed":"wrong","detail":"conflict"}),
        ),
    ] {
        let mut wire = historical_json();
        let (parent, key) = pointer.rsplit_once('/').expect("pointer parent");
        wire.pointer_mut(parent).expect("fixture path")[key] = replacement;
        let rejected = deserialize(&wire);
        assert!(
            !rejected
                .execution
                .has_complete_legacy_codex_advisory_evidence(),
            "{pointer}"
        );
        assert!(
            rejected.validate_persisted_verified_response(4096).is_err(),
            "{pointer}"
        );
    }
}

#[test]
fn persisted_legacy_validation_preserves_response_hash_budget_framing_session_and_usage_checks() {
    for (pointer, replacement) in [
        ("/response/sha256_digest", json!("0".repeat(64))),
        ("/response/byte_count", json!(1)),
        ("/response/framing/response_line_number", json!(0)),
        ("/response/framing/terminal_line_number", json!(5)),
        ("/response/framing/session_id", json!("wrong-thread")),
        ("/response/framing/item_id", Value::Null),
        ("/response/framing/usage/output_tokens", json!(319)),
        (
            "/response_error",
            json!({"code":"incomplete_or_partial_output","message":"truncated response"}),
        ),
        ("/response", Value::Null),
    ] {
        let mut wire = historical_json();
        let (parent, key) = pointer.rsplit_once('/').expect("pointer parent");
        wire.pointer_mut(parent).expect("fixture path")[key] = replacement;
        let rejected = deserialize(&wire);
        assert!(
            rejected
                .execution
                .has_complete_legacy_codex_advisory_evidence(),
            "report unchanged {pointer}"
        );
        assert!(
            rejected.validate_persisted_verified_response(4096).is_err(),
            "{pointer}"
        );
    }
    let artifact = deserialize(&historical_json());
    let bytes = artifact.response.as_ref().expect("response").byte_count;
    assert!(artifact.validate_persisted_verified_response(bytes).is_ok());
    assert!(artifact
        .validate_persisted_verified_response(bytes - 1)
        .is_err());
}
