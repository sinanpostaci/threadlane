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
    is_pinned_codex_read_only_argv, resolve_model_target, run_advisory_worker,
    run_advisory_worker_with_structured_result,
    run_advisory_worker_with_structured_result_and_cancel,
    run_advisory_worker_with_structured_result_and_observer, validate_advisory_provider_and_mode,
    validate_codex_advisory_workdir, AdvisoryResponseBudget, AdvisoryResponseErrorCode,
    AdvisoryResponseSource, AdvisoryStructuredExecution, AdvisoryWorkerSpec,
    BoundedAdvisoryResponse, ExecutionBudgets, NormalizedWorkerEvent, OutputOverflowKind,
    StreamTokenUsage, WorkerError, WorkerErrorCode, WorkerFilePermissions, WorkerProvider,
    WorkerRunStatus, WorkerSpec, CODEX_ADVISORY_DISABLED_FEATURES,
    CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG, CODEX_ADVISORY_PERMISSION_PROFILE_NAME,
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
struct SampleCodexReviewPayload {
    schema_version: u32,
    review_id: String,
    verdict: String,
    summary: String,
    checked_invariants: Vec<String>,
}

fn build_large_codex_review_payload() -> (SampleCodexReviewPayload, String) {
    let payload = SampleCodexReviewPayload {
        schema_version: 1,
        review_id: "codex-adv-review-001".to_string(),
        verdict: "approved".to_string(),
        summary: "Read-only Codex CLI advisory review exceeding the 256-byte diagnostic summary cap while preserving multibyte UTF-8 (café, €100, 🦀).".to_string(),
        checked_invariants: (1..=8)
            .map(|idx| {
                format!(
                    "Invariant #{idx:02}: verified read-only permission profile, no-daemon flag, and honest requested-vs-observed evidence boundary."
                )
            })
            .collect(),
    };
    let json = serde_json::to_string(&payload).expect("serialize sample review payload");
    assert!(
        json.len() > 800,
        "sample payload must exceed 256-byte summary truncation (got {} bytes)",
        json.len()
    );
    (payload, json)
}

fn sample_codex_advisory_spec(
    executable: PathBuf,
    working_directory: PathBuf,
    prompt: &str,
) -> AdvisoryWorkerSpec {
    AdvisoryWorkerSpec {
        provider: WorkerProvider::CodexCli,
        executable,
        model: "gpt-6.1-sol".to_string(),
        effort: Some("xhigh".to_string()),
        allowed_models: BTreeSet::from(["gpt-6.1-sol".to_string()]),
        session_id: None,
        standalone: false,
        working_directory,
        allowed_write_paths: BTreeSet::new(),
        prompt: prompt.to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    }
}

#[test]
fn codex_cli_exact_argv_permission_profile_and_mutable_or_invalid_spec_refusals() {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workspace");
    let work_area = temp.path().join("work-area");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(&work_area).expect("create work_area");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let fake_codex = write_executable_script(&bin_dir, "fake-codex", "#!/usr/bin/env sh\nexit 0\n");

    // 1. Serde aliases and canonical representation for WorkerProvider::CodexCli
    assert_eq!(WorkerProvider::CodexCli.as_str(), "codex_cli");
    assert_eq!(
        serde_json::to_string(&WorkerProvider::CodexCli).expect("serialize CodexCli"),
        "\"codex_cli\""
    );
    for raw in ["\"codex_cli\"", "\"codex-cli\"", "\"codex\""] {
        let parsed: WorkerProvider = serde_json::from_str(raw).expect("deserialize CodexCli");
        assert_eq!(parsed, WorkerProvider::CodexCli);
    }

    // 2. Exact named permission profile TOML string check
    assert_eq!(
        CODEX_ADVISORY_PERMISSION_PROFILE_NAME,
        "threadlane-advisory"
    );
    assert_eq!(
        CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG,
        r#"permissions.threadlane-advisory={"filesystem"={"glob_scan_max_depth"=64,":minimal"="read",":workspace_roots"={"."="read","**/.env"="deny","**/.env.*"="deny","**/codex.toml"="deny","**/.codex.toml"="deny","**/.git"="deny","**/.git/**"="deny","**/.codex"="deny","**/.codex/**"="deny","**/.agents"="deny","**/.agents/**"="deny","**/.aws"="deny","**/.aws/**"="deny"},"/nix/store"="read"},"network"={"enabled"=false}}"#
    );

    // 3. Exact constructed argv and empty child_env on valid read-only AdvisoryWorkerSpec
    let spec = sample_codex_advisory_spec(
        fake_codex.clone(),
        workdir.clone(),
        "Inspect workspace and return JSON.",
    );
    let validation = spec.validate().expect("validate CodexCli advisory spec");
    assert_eq!(validation.provider, WorkerProvider::CodexCli);
    assert_eq!(validation.file_permissions, WorkerFilePermissions::ReadOnly);
    assert_eq!(validation.resolved_model.base_model, "gpt-6.1-sol");
    assert_eq!(validation.resolved_model.effort.as_deref(), Some("xhigh"));
    assert_eq!(validation.resolved_model.cli_model_arg, "gpt-6.1-sol");
    assert!(validation.child_env.is_empty());
    assert!(validation.allowed_write_paths.is_empty());
    assert!(is_pinned_codex_read_only_argv(
        &validation.constructed_argv,
        &validation.resolved_model
    ));

    let mut expected_argv = vec![
        "--no-daemon".to_string(),
        "exec".to_string(),
        "--json".to_string(),
        "--ephemeral".to_string(),
        "--ignore-user-config".to_string(),
        "--model".to_string(),
        "gpt-6.1-sol".to_string(),
        "-c".to_string(),
        "model_reasoning_effort=\"xhigh\"".to_string(),
        "-c".to_string(),
        "approval_policy=\"never\"".to_string(),
        "-c".to_string(),
        "model_provider=\"openai\"".to_string(),
        "-c".to_string(),
        "default_permissions=\"threadlane-advisory\"".to_string(),
        "-c".to_string(),
        CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG.to_string(),
        "-c".to_string(),
        "web_search=\"disabled\"".to_string(),
    ];
    for feature in CODEX_ADVISORY_DISABLED_FEATURES {
        expected_argv.push("--disable".to_string());
        expected_argv.push((*feature).to_string());
    }
    expected_argv.push("--".to_string());
    expected_argv.push("Inspect workspace and return JSON.".to_string());

    assert_eq!(validation.constructed_argv, expected_argv);
    assert!(
        !validation.constructed_argv.iter().any(|arg| {
            arg == "--sandbox"
                || arg == "workspace-write"
                || arg == "danger-full-access"
                || arg.contains("bypass")
                || arg.contains("full-auto")
        }),
        "Codex advisory argv must never include --sandbox or any write/bypass/auto flags"
    );

    // 4. Reject Inherit and TaskFiles on AdvisoryWorkerSpec and validate_advisory_provider_and_mode
    for mode in [
        WorkerFilePermissions::Inherit,
        WorkerFilePermissions::TaskFiles,
    ] {
        let mut bad_mode_spec = spec.clone();
        bad_mode_spec.file_permissions = mode;
        let err = bad_mode_spec
            .validate()
            .expect_err("non-read_only mode must be rejected for CodexCli AdvisoryWorkerSpec");
        assert_eq!(err.code(), WorkerErrorCode::UnsupportedCapability);
        assert!(matches!(err, WorkerError::UnsupportedCapability { .. }));

        let mode_err = validate_advisory_provider_and_mode("codex_cli", mode)
            .expect_err("validate_advisory_provider_and_mode must reject non-read_only mode");
        assert_eq!(mode_err.code(), WorkerErrorCode::UnsupportedCapability);
    }

    // 5. Reject non-empty allowed_write_paths on ReadOnly AdvisoryWorkerSpec
    let mut non_empty_writes_spec = spec.clone();
    non_empty_writes_spec
        .allowed_write_paths
        .insert("src/lib.rs".to_string());
    let writes_err = non_empty_writes_spec
        .validate()
        .expect_err("read_only AdvisoryWorkerSpec must reject non-empty allowed_write_paths");
    assert_eq!(writes_err.code(), WorkerErrorCode::InvalidSpec);

    // 6. Reject session_id and standalone on CodexCli AdvisoryWorkerSpec
    let mut with_session = spec.clone();
    with_session.session_id = Some("thread-123".to_string());
    assert_eq!(
        with_session.validate().unwrap_err().code(),
        WorkerErrorCode::InvalidSpec
    );

    let mut with_standalone = spec.clone();
    with_standalone.standalone = true;
    assert_eq!(
        with_standalone.validate().unwrap_err().code(),
        WorkerErrorCode::InvalidSpec
    );

    // 7. Reject Inherit and ReadOnly on WorkerSpec for CodexCli before process effect; allow TaskFiles
    for (mode, write_paths) in [
        (
            WorkerFilePermissions::Inherit,
            BTreeSet::from(["src/lib.rs".to_string()]),
        ),
        (
            WorkerFilePermissions::ReadOnly,
            BTreeSet::from(["src/lib.rs".to_string()]),
        ),
        (WorkerFilePermissions::ReadOnly, BTreeSet::new()),
    ] {
        let worker_spec = WorkerSpec {
            provider: WorkerProvider::CodexCli,
            executable: fake_codex.clone(),
            model: "gpt-6.1-sol".to_string(),
            effort: Some("xhigh".to_string()),
            allowed_models: BTreeSet::new(),
            session_id: None,
            standalone: false,
            repository_path: workdir.clone(),
            base_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
            work_area_root: work_area.clone(),
            allowed_write_paths: write_paths,
            prompt: "attempt mutable coder task".to_string(),
            file_permissions: mode,
            budgets: ExecutionBudgets::default(),
        };
        let err = worker_spec
            .validate()
            .expect_err("WorkerSpec must reject Inherit and ReadOnly for CodexCli");
        if mode != WorkerFilePermissions::ReadOnly {
            assert_eq!(err.code(), WorkerErrorCode::UnsupportedCapability);
        } else {
            assert_eq!(err.code(), WorkerErrorCode::InvalidSpec);
        }
    }

    // 8. Model/effort resolver rejects inline #variant, provider/model slash, missing effort, and unsafe identifiers
    for (bad_model, bad_effort) in [
        ("gpt-6.1-sol#xhigh", Some("xhigh")),
        ("openai/gpt-6.1-sol", Some("xhigh")),
        ("gpt-6.1-sol", None),
        ("gpt-6.1-sol", Some("")),
        ("gpt-6.1-sol", Some("high\"inject")),
        ("gpt 6.1", Some("xhigh")),
    ] {
        let err = resolve_model_target(WorkerProvider::CodexCli, bad_model, bad_effort)
            .expect_err("invalid Codex model/effort must be rejected");
        assert_eq!(err.code(), WorkerErrorCode::InvalidSpec);
    }
}

#[test]
fn codex_cli_workdir_fails_closed_when_project_codex_config_markers_exist() {
    let temp = TempDir::new().expect("tempdir");
    let repo_root = temp.path().join("repo");
    let nested_workdir = repo_root.join("crates/subcrate");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(repo_root.join(".git")).expect("create .git");
    fs::create_dir_all(&nested_workdir).expect("create nested_workdir");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let fake_codex = write_executable_script(&bin_dir, "fake-codex", "#!/usr/bin/env sh\nexit 0\n");
    let spec = sample_codex_advisory_spec(fake_codex, nested_workdir.clone(), "review");

    validate_codex_advisory_workdir(&nested_workdir)
        .expect("clean workdir without project Codex markers should pass");
    spec.validate()
        .expect("spec should validate in clean workdir");

    for marker in [".codex", "codex.toml", ".codex.toml"] {
        let marker_path = repo_root.join(marker);
        if marker == ".codex" {
            fs::create_dir_all(&marker_path).expect("create .codex dir");
        } else {
            fs::write(&marker_path, "# project codex config\n").expect("write marker file");
        }

        let err = validate_codex_advisory_workdir(&nested_workdir)
            .expect_err("ancestor project Codex config marker must fail closed");
        assert_eq!(err.code(), WorkerErrorCode::InvalidSpec);
        assert!(spec.validate().is_err());

        if marker == ".codex" {
            fs::remove_dir_all(&marker_path).expect("remove .codex dir");
        } else {
            fs::remove_file(&marker_path).expect("remove marker file");
        }
    }
}

#[tokio::test]
async fn codex_cli_supervised_advisory_run_captures_large_final_agent_message_and_genuine_usage_without_fabricating_telemetry(
) {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let (expected_payload, payload_json) = build_large_codex_review_payload();
    let item_event = serde_json::json!({
        "type": "item.completed",
        "item": {
            "id": "item_final_42",
            "type": "agent_message",
            "text": payload_json
        }
    });
    let item_line_file = temp.path().join("codex_agent_message.json");
    fs::write(
        &item_line_file,
        serde_json::to_string(&item_event).expect("serialize item event"),
    )
    .expect("write item line file");

    let payload_file = temp.path().join("codex_final_payload.json");
    fs::write(&payload_file, &payload_json).expect("write payload file");
    let argv_dump_file = temp.path().join("codex_argv.txt");
    let script = format!(
        r#"#!/usr/bin/env sh
set -eu
OUT_LAST_MSG=""
for arg in "$@"; do
    printf '%s\n' "$arg" >> "{}"
done
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-last-message|-o)
            OUT_LAST_MSG="$2"
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *)
            shift 1
            ;;
    esac
done
printf '%s\n' '{{"type":"thread.started","thread_id":"019a52d7-7c00-7111-8222-codexthread01"}}'
printf '%s\n' '{{"type":"turn.started"}}'
printf '%s\n' '{{"type":"item.completed","item":{{"id":"item_reason_1","type":"reasoning","text":" Inspecting workspace under read-only profile. "}}}}'
printf '%s\n' '{{"type":"item.completed","item":{{"id":"item_cmd_1","type":"command_execution","command":"git status --short","aggregated_output":"{{\"not_the_final_response\":true}}","exit_code":0,"status":"completed"}}}}'
cat "{}"
printf '\n'
if [ -n "$OUT_LAST_MSG" ]; then
    cp "{}" "$OUT_LAST_MSG"
fi
printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":1420,"cached_input_tokens":1024,"cache_write_input_tokens":64,"output_tokens":318,"reasoning_output_tokens":192}}}}'
exit 0
"#,
        argv_dump_file.display(),
        item_line_file.display(),
        payload_file.display(),
    );
    let fake_codex = write_executable_script(&bin_dir, "fake-codex-structured", &script);
    let spec = sample_codex_advisory_spec(
        fake_codex,
        workdir,
        "Review workspace and emit structured JSON.",
    );

    let (tx, mut rx) = mpsc::channel::<NormalizedWorkerEvent>(16);
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let structured = run_advisory_worker_with_structured_result_and_observer(
        &spec,
        AdvisoryResponseBudget::default(),
        &mut cancel_rx,
        Some(tx),
    )
    .await
    .expect("supervised CodexCli structured advisory execution should succeed");

    // Verify actual argv received by the fake Codex process matches constructed_argv
    let received_argv: Vec<String> = fs::read_to_string(&argv_dump_file)
        .expect("read argv dump")
        .lines()
        .map(str::to_string)
        .collect();
    assert_eq!(received_argv, structured.execution.constructed_argv);
    assert_eq!(
        received_argv.first().map(String::as_str),
        Some("--no-daemon")
    );

    assert!(structured.has_verified_response());
    assert!(structured.response_error.is_none());
    assert_eq!(structured.execution.status, WorkerRunStatus::Succeeded);
    assert!(structured
        .execution
        .has_pinned_codex_read_only_launch_config());
    let pinned_capture_path =
        threadlane_workers::spec::extract_pinned_codex_read_only_output_last_message_path(
            &structured.execution.constructed_argv,
            &structured.execution.requested_model,
        )
        .expect("supervised Codex advisory argv must pin --output-last-message");
    assert!(!Path::new(pinned_capture_path).exists());
    assert!(
        !Path::new(pinned_capture_path)
            .parent()
            .expect("capture parent")
            .exists(),
        "invocation-private --output-last-message directory must be removed after completion"
    );
    assert!(structured
        .execution
        .has_complete_natural_completion_evidence());

    // Honest requested-vs-observed boundary: Codex CLI did not emit model/effort telemetry,
    // so observed model and effort MUST remain None (never fabricated from requested_model).
    assert_eq!(
        structured.execution.requested_model.base_model,
        "gpt-6.1-sol"
    );
    assert_eq!(
        structured.execution.requested_model.effort.as_deref(),
        Some("xhigh")
    );
    assert_eq!(structured.execution.effective_observed_model(), None);
    assert_eq!(structured.execution.effective_observed_effort(), None);
    assert_eq!(structured.execution.evidence.effort_evidence_source, None);
    assert_eq!(
        structured.execution.evidence.observed_session_id.as_deref(),
        Some("019a52d7-7c00-7111-8222-codexthread01")
    );

    let expected_usage = StreamTokenUsage {
        input_tokens: 1420,
        cached_input_tokens: Some(1024),
        cache_write_input_tokens: Some(64),
        output_tokens: 318,
        reasoning_output_tokens: Some(192),
    };
    assert_eq!(
        structured.execution.evidence.observed_usage,
        Some(expected_usage.clone())
    );

    // Retained diagnostic event summaries remain capped at <= 256 bytes
    for ev in &structured.execution.evidence.retained_events {
        if let Some(ref summary) = ev.summary {
            assert!(summary.len() <= 256);
        }
    }
    let mut live_events = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        if let Some(ref summary) = ev.summary {
            assert!(summary.len() <= 256);
        }
        live_events.push(ev);
    }
    assert_eq!(live_events.len(), 6);

    let response = structured
        .verified_response()
        .expect("verified CodexCli response must be present");
    assert_eq!(response.text, payload_json);
    assert_eq!(response.byte_count, payload_json.len());
    assert_eq!(response.sha256_digest, sha256_hex(payload_json.as_bytes()));
    assert_eq!(response.provider, WorkerProvider::CodexCli);
    assert_eq!(
        response.source,
        AdvisoryResponseSource::CodexAgentMessageTurnCompleted
    );
    assert_eq!(response.framing.response_line_number, 5);
    assert_eq!(response.framing.terminal_line_number, 6);
    assert_eq!(
        response.framing.session_id.as_deref(),
        Some("019a52d7-7c00-7111-8222-codexthread01")
    );
    assert_eq!(response.framing.item_id.as_deref(), Some("item_final_42"));
    assert_eq!(response.framing.message_id, None);
    assert_eq!(response.framing.part_id, None);
    assert_eq!(response.framing.effort_evidence_source, None);
    assert_eq!(response.framing.usage, Some(expected_usage));

    let parsed: SampleCodexReviewPayload = response
        .parse_json()
        .expect("deserialize full Codex structured review JSON");
    assert_eq!(parsed, expected_payload);

    // Round-trip through serde JSON and verify integrity + tamper detection
    let serialized = serde_json::to_string(&structured).expect("serialize structured execution");
    let roundtripped: AdvisoryStructuredExecution =
        serde_json::from_str(&serialized).expect("deserialize structured execution");
    assert!(roundtripped.has_verified_response());
    let roundtripped_resp: BoundedAdvisoryResponse =
        serde_json::from_str(&serde_json::to_string(response).unwrap()).unwrap();
    roundtripped_resp
        .validate_integrity(payload_json.len())
        .expect("validate integrity at exact byte count");
}

#[tokio::test]
async fn codex_cli_rejects_turn_failed_error_missing_framing_conflicting_threads_and_tool_only_output(
) {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    // 1. `turn.failed` even when process exits 0 -> `ProviderError`
    let turn_failed_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-fail-1"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"turn.failed","error":{"message":"model quota exceeded"}}'
exit 0
"#;
    let fake_failed =
        write_executable_script(&bin_dir, "fake-codex-turn-failed", turn_failed_script);
    let mut spec = sample_codex_advisory_spec(fake_failed, workdir.clone(), "prompt");
    let failed_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("turn.failed run");
    assert!(!failed_out.has_verified_response());
    assert_eq!(failed_out.execution.status, WorkerRunStatus::ProviderError);
    assert_eq!(
        failed_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::ProviderError)
    );

    // 2. Top-level `error` event even when process exits 0 -> `ProviderError`
    let error_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-err-1"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"error","message":"stream disconnected"}'
exit 0
"#;
    spec.executable = write_executable_script(&bin_dir, "fake-codex-error", error_script);
    let err_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("error event run");
    assert!(!err_out.has_verified_response());
    assert_eq!(err_out.execution.status, WorkerRunStatus::ProviderError);
    assert_eq!(
        err_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::ProviderError)
    );

    // 3. Missing `turn.completed` -> `MissingResultEvidence` / `MissingTerminalFraming`
    let missing_turn_completed_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-incomplete-1"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_1","type":"agent_message","text":"{\"ok\":true}"}}'
exit 0
"#;
    spec.executable = write_executable_script(
        &bin_dir,
        "fake-codex-missing-turn-completed",
        missing_turn_completed_script,
    );
    let miss_term_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("missing turn.completed run");
    assert!(!miss_term_out.has_verified_response());
    assert_eq!(
        miss_term_out.execution.status,
        WorkerRunStatus::MissingResultEvidence
    );
    assert_eq!(
        miss_term_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::MissingTerminalFraming)
    );

    // 4. Missing `thread.started` -> `SessionMismatch` / `SessionEvidenceFailed`
    let missing_thread_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_1","type":"agent_message","text":"{\"ok\":true}"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":5}}'
exit 0
"#;
    spec.executable =
        write_executable_script(&bin_dir, "fake-codex-missing-thread", missing_thread_script);
    let miss_thread_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("missing thread.started run");
    assert!(!miss_thread_out.has_verified_response());
    assert_eq!(
        miss_thread_out.execution.status,
        WorkerRunStatus::SessionMismatch
    );
    assert_eq!(
        miss_thread_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::SessionEvidenceFailed)
    );

    // 5. Conflicting `thread_id` across events -> `SessionMismatch` / `SessionEvidenceFailed`
    let conflicting_thread_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-alpha"}'
printf '%s\n' '{"type":"turn.started","thread_id":"thread-beta"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_1","type":"agent_message","text":"{\"ok\":true}"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":5}}'
exit 0
"#;
    spec.executable = write_executable_script(
        &bin_dir,
        "fake-codex-conflict-thread",
        conflicting_thread_script,
    );
    let conflict_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("conflicting thread_id run");
    assert!(!conflict_out.has_verified_response());
    assert_eq!(
        conflict_out.execution.status,
        WorkerRunStatus::SessionMismatch
    );
    assert_eq!(
        conflict_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::SessionEvidenceFailed)
    );

    // 6. Tool-only JSON (`command_execution` without a subsequent final `agent_message`) -> `SpoofedOrToolOutput`
    let tool_only_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-tool-only"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_cmd","type":"command_execution","command":"cat fixture.json","aggregated_output":"{\"forged\":true}","exit_code":0,"status":"completed"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":5}}'
exit 0
"#;
    spec.executable = write_executable_script(&bin_dir, "fake-codex-tool-only", tool_only_script);
    let tool_only_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("tool-only run");
    assert!(!tool_only_out.has_verified_response());
    assert_eq!(
        tool_only_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::SpoofedOrToolOutput)
    );

    // 7. Malformed NDJSON line -> `MalformedStream`
    let malformed_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-malformed"}'
printf '%s\n' 'not-valid-json-line'
exit 0
"#;
    spec.executable = write_executable_script(&bin_dir, "fake-codex-malformed", malformed_script);
    let malformed_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("malformed line run");
    assert!(!malformed_out.has_verified_response());
    assert_eq!(
        malformed_out.execution.status,
        WorkerRunStatus::MalformedStream
    );
    assert_eq!(
        malformed_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::MalformedStream)
    );

    // 8. Truncated/non-object JSON inside `agent_message.text` -> `InvalidStructuredJson`
    let bad_json_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-badjson"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_1","type":"agent_message","text":"{\"unterminated\":1"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":5}}'
exit 0
"#;
    spec.executable = write_executable_script(&bin_dir, "fake-codex-badjson", bad_json_script);
    let bad_json_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("bad json run");
    assert!(!bad_json_out.has_verified_response());
    assert_eq!(bad_json_out.execution.status, WorkerRunStatus::Succeeded);
    assert_eq!(
        bad_json_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::InvalidStructuredJson)
    );
}

#[tokio::test]
async fn codex_cli_enforces_overflow_cancellation_timeout_and_preserves_legacy_provider_strictness()
{
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    // 1. Stdout line overflow (`StdoutLineExceeded`)
    let (_, large_json) = build_large_codex_review_payload();
    let item_line = serde_json::json!({
        "type": "item.completed",
        "item": {
            "id": "item_ovf",
            "type": "agent_message",
            "text": large_json
        }
    })
    .to_string();
    let overflow_file = temp.path().join("codex_overflow_line.json");
    fs::write(&overflow_file, &item_line).expect("write overflow line");

    let overflow_script = format!(
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{{"type":"thread.started","thread_id":"thread-ovf-1"}}'
printf '%s\n' '{{"type":"turn.started"}}'
cat "{}"
printf '\n'
exit 0
"#,
        overflow_file.display()
    );
    let fake_ovf = write_executable_script(&bin_dir, "fake-codex-overflow", &overflow_script);
    let mut ovf_spec = sample_codex_advisory_spec(fake_ovf, workdir.clone(), "overflow test");
    ovf_spec.budgets.max_line_bytes = 512;
    let ovf_out =
        run_advisory_worker_with_structured_result(&ovf_spec, AdvisoryResponseBudget::default())
            .await
            .expect("overflow run");
    assert!(!ovf_out.has_verified_response());
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

    // 2. Cancellation reaps process group and reports `Cancelled`
    let slow_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-cancel-1"}'
sleep 30 &
wait
"#;
    let fake_slow = write_executable_script(&bin_dir, "fake-codex-slow", slow_script);
    let cancel_spec = sample_codex_advisory_spec(fake_slow.clone(), workdir.clone(), "cancel test");
    let (cancel_tx, mut cancel_rx) = watch::channel(true);
    let _ = cancel_tx.send(true);
    let cancel_out = run_advisory_worker_with_structured_result_and_cancel(
        &cancel_spec,
        AdvisoryResponseBudget::default(),
        &mut cancel_rx,
    )
    .await
    .expect("cancelled run");
    assert!(!cancel_out.has_verified_response());
    assert_eq!(cancel_out.execution.status, WorkerRunStatus::Cancelled);
    assert!(!cancel_out.execution.cleanup_incomplete);
    assert_eq!(
        cancel_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::WorkerExecutionFailed)
    );

    // 3. Timeout reaps process group and reports `TimedOut`
    let mut timeout_spec = sample_codex_advisory_spec(fake_slow, workdir.clone(), "timeout test");
    timeout_spec.budgets.timeout_ms = 150;
    let timeout_out = run_advisory_worker_with_structured_result(
        &timeout_spec,
        AdvisoryResponseBudget::default(),
    )
    .await
    .expect("timed out run");
    assert!(!timeout_out.has_verified_response());
    assert_eq!(timeout_out.execution.status, WorkerRunStatus::TimedOut);
    assert!(!timeout_out.execution.cleanup_incomplete);
    assert_eq!(
        timeout_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::WorkerExecutionFailed)
    );

    // 4. Existing Antigravity strict observed model evidence rule remains unchanged:
    //    omitting observed model in Antigravity stream still fails with `MissingModelEvidence`.
    let agy_no_model_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"event":"init","init":{"session_id":"agy-nomodel-1"}}'
printf '%s\n' '{"event":"result","result":{"status":"SUCCESS","response":"{\"ok\":true}"}}'
exit 0
"#;
    let fake_agy_no_model =
        write_executable_script(&bin_dir, "fake-agy-nomodel", agy_no_model_script);
    let agy_spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::Antigravity,
        executable: fake_agy_no_model,
        model: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        allowed_models: BTreeSet::new(),
        session_id: None,
        standalone: false,
        working_directory: workdir,
        allowed_write_paths: BTreeSet::new(),
        prompt: "verify agy strictness".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    };
    let agy_report = run_advisory_worker(&agy_spec)
        .await
        .expect("run antigravity without model telemetry");
    assert_eq!(agy_report.status, WorkerRunStatus::MissingModelEvidence);
}

#[tokio::test]
async fn codex_cli_accepts_multiple_ordinary_prose_preambles_and_prose_tools_prose_before_single_final_json_object(
) {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let (expected_payload, payload_json) = build_large_codex_review_payload();
    let payload_file = temp.path().join("codex_prose_payload.json");
    fs::write(&payload_file, &payload_json).expect("write payload_file");
    let final_item_line_4 = serde_json::json!({
        "type": "item.completed",
        "item": {
            "id": "item_final_json_4",
            "type": "agent_message",
            "text": payload_json
        }
    });
    let final_line_4_file = temp.path().join("codex_final_line_4.json");
    fs::write(
        &final_line_4_file,
        serde_json::to_string(&final_item_line_4).expect("serialize final item line 4"),
    )
    .expect("write final_line_4_file");

    // 1. Multiple ordinary prose preambles in a single tool-free turn followed by one final JSON object
    let multi_prose_script = format!(
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
printf '%s\n' '{{"type":"thread.started","thread_id":"019a52d7-7c00-7111-8222-codexprose01"}}'
printf '%s\n' '{{"type":"turn.started"}}'
printf '%s\n' '{{"type":"item.completed","item":{{"id":"item_prose_1","type":"agent_message","text":"I will inspect `docs/ROADMAP.md` and `crates/threadlane-workers/src/advisory_response.rs` [read-only]."}}}}'
printf '%s\n' '{{"type":"item.completed","item":{{"id":"item_prose_2","type":"agent_message","text":"1. Verified launch flags and workdir rules.\n2. Preparing the final structured review JSON."}}}}'
cat "{}"
printf '\n'
if [ -n "$OUT_LAST_MSG" ]; then
    cp "{}" "$OUT_LAST_MSG"
fi
printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":810,"cached_input_tokens":512,"output_tokens":245}}}}'
exit 0
"#,
        final_line_4_file.display(),
        payload_file.display(),
    );
    let fake_multi_prose =
        write_executable_script(&bin_dir, "fake-codex-multi-prose", &multi_prose_script);
    let mut spec = sample_codex_advisory_spec(
        fake_multi_prose,
        workdir.clone(),
        "Review workspace after prose updates.",
    );

    let multi_prose_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("multi-prose + single JSON final run should succeed");
    assert!(multi_prose_out.has_verified_response());
    assert!(multi_prose_out.response_error.is_none());
    assert_eq!(multi_prose_out.execution.status, WorkerRunStatus::Succeeded);
    assert_eq!(multi_prose_out.execution.effective_observed_model(), None);
    assert_eq!(multi_prose_out.execution.effective_observed_effort(), None);

    let resp_a = multi_prose_out
        .verified_response()
        .expect("verified response for multi-prose turn");
    assert_eq!(resp_a.text, payload_json);
    assert_eq!(resp_a.framing.response_line_number, 5);
    assert_eq!(resp_a.framing.terminal_line_number, 6);
    assert_eq!(resp_a.framing.item_id.as_deref(), Some("item_final_json_4"));
    assert_eq!(
        resp_a.framing.session_id.as_deref(),
        Some("019a52d7-7c00-7111-8222-codexprose01")
    );
    let parsed_a: SampleCodexReviewPayload =
        resp_a.parse_json().expect("deserialize multi-prose JSON");
    assert_eq!(parsed_a, expected_payload);

    // 2. Structural shape with prose (line 3) -> reasoning/tools (lines 4..17) -> post-tool prose (line 18) -> final JSON (line 19) -> turn.completed (line 20)
    let final_item_line_19 = serde_json::json!({
        "type": "item.completed",
        "item": {
            "id": "item_final_19",
            "type": "agent_message",
            "text": payload_json
        }
    });
    let final_line_19_file = temp.path().join("codex_final_line_19.json");
    fs::write(
        &final_line_19_file,
        serde_json::to_string(&final_item_line_19).expect("serialize final item line 19"),
    )
    .expect("write final_line_19_file");

    let prose_tools_prose_script = format!(
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
printf '%s\n' '{{"type":"thread.started","thread_id":"019a52d7-7c00-7111-8222-codexprose02"}}'
printf '%s\n' '{{"type":"turn.started"}}'
printf '%s\n' '{{"type":"item.completed","item":{{"id":"item_3","type":"agent_message","text":"I will inspect the repository structure and read the relevant files before producing the plan."}}}}'
for i in $(seq 4 17); do
    if [ $((i % 2)) -eq 0 ]; then
        printf '{{"type":"item.completed","item":{{"id":"item_%d","type":"reasoning","text":"Reasoning step %d"}}}}\n' "$i" "$i"
    else
        printf '{{"type":"item.completed","item":{{"id":"item_%d","type":"command_execution","command":"git status --short","aggregated_output":"{{\\"tool_output\\":%d}}","exit_code":0,"status":"completed"}}}}\n' "$i" "$i"
    fi
done
printf '%s\n' '{{"type":"item.completed","item":{{"id":"item_18","type":"agent_message","text":"I have completed the read-only inspection and will now emit the structured JSON response."}}}}'
cat "{}"
printf '\n'
if [ -n "$OUT_LAST_MSG" ]; then
    cp "{}" "$OUT_LAST_MSG"
fi
printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":2048,"cached_input_tokens":1536,"output_tokens":512}}}}'
exit 0
"#,
        final_line_19_file.display(),
        payload_file.display(),
    );
    spec.executable = write_executable_script(
        &bin_dir,
        "fake-codex-prose-tools-prose",
        &prose_tools_prose_script,
    );

    let ptp_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("prose -> tools -> prose -> final JSON run should succeed");
    assert!(ptp_out.has_verified_response());
    assert!(ptp_out.response_error.is_none());
    assert_eq!(ptp_out.execution.status, WorkerRunStatus::Succeeded);
    assert_eq!(ptp_out.execution.evidence.total_lines, 20);

    let resp_b = ptp_out
        .verified_response()
        .expect("verified response for prose-tools-prose-final turn");
    assert_eq!(resp_b.text, payload_json);
    assert_eq!(resp_b.framing.response_line_number, 19);
    assert_eq!(resp_b.framing.terminal_line_number, 20);
    assert_eq!(resp_b.framing.item_id.as_deref(), Some("item_final_19"));
    assert_eq!(
        resp_b.framing.session_id.as_deref(),
        Some("019a52d7-7c00-7111-8222-codexprose02")
    );
    let parsed_b: SampleCodexReviewPayload = resp_b
        .parse_json()
        .expect("deserialize prose-tools-prose JSON");
    assert_eq!(parsed_b, expected_payload);

    // 3. Exact 32-preamble boundary (`MAX_CODEX_PROSE_PREAMBLES_PER_TURN = 32`) followed by one final JSON object
    let mut boundary_32_lines = vec![
        serde_json::to_string(&serde_json::json!({
            "type": "thread.started",
            "thread_id": "019a52d7-7c00-7111-8222-codexprose32"
        }))
        .expect("serialize thread.started"),
        serde_json::to_string(&serde_json::json!({
            "type": "turn.started"
        }))
        .expect("serialize turn.started"),
    ];
    for idx in 1..=32 {
        boundary_32_lines.push(
            serde_json::to_string(&serde_json::json!({
                "type": "item.completed",
                "item": {
                    "id": format!("item_preamble_{idx}"),
                    "type": "agent_message",
                    "text": format!("Ordinary read-only inspection note {idx} of 32.")
                }
            }))
            .expect("serialize prose preamble"),
        );
    }
    boundary_32_lines.push(
        serde_json::to_string(&serde_json::json!({
            "type": "item.completed",
            "item": {
                "id": "item_final_after_32",
                "type": "agent_message",
                "text": payload_json
            }
        }))
        .expect("serialize final JSON item"),
    );
    boundary_32_lines.push(
        serde_json::to_string(&serde_json::json!({
            "type": "turn.completed",
            "usage": {
                "input_tokens": 1024,
                "cached_input_tokens": 512,
                "output_tokens": 256
            }
        }))
        .expect("serialize turn.completed"),
    );
    let boundary_32_ndjson = temp.path().join("codex_boundary_32.ndjson");
    fs::write(
        &boundary_32_ndjson,
        format!("{}\n", boundary_32_lines.join("\n")),
    )
    .expect("write boundary_32_ndjson");
    let boundary_32_script = format!(
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
cat "{}"
if [ -n "$OUT_LAST_MSG" ]; then
    cp "{}" "$OUT_LAST_MSG"
fi
exit 0
"#,
        boundary_32_ndjson.display(),
        payload_file.display(),
    );
    spec.executable =
        write_executable_script(&bin_dir, "fake-codex-boundary-32", &boundary_32_script);

    let boundary_32_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("32 prose preambles + 1 final JSON run should succeed");
    assert!(boundary_32_out.has_verified_response());
    assert!(boundary_32_out.response_error.is_none());
    assert_eq!(boundary_32_out.execution.status, WorkerRunStatus::Succeeded);
    let resp_32 = boundary_32_out
        .verified_response()
        .expect("verified response at 32-preamble boundary");
    assert_eq!(resp_32.text, payload_json);
    assert_eq!(resp_32.framing.response_line_number, 35);
    assert_eq!(resp_32.framing.terminal_line_number, 36);
    assert_eq!(
        resp_32.framing.item_id.as_deref(),
        Some("item_final_after_32")
    );
}

#[tokio::test]
async fn codex_cli_rejects_prose_only_missing_final_duplicate_ids_conflicting_finals_and_malformed_json_like_prior_candidates(
) {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let dummy_exe =
        write_executable_script(&bin_dir, "fake-codex-init", "#!/usr/bin/env sh\nexit 0\n");
    let mut spec = sample_codex_advisory_spec(dummy_exe, workdir, "regression checks");

    // 1. Prose-only turn (multiple normal prose messages with no final JSON object) -> `InvalidStructuredJson`
    let prose_only_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-prose-only-1"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_1","type":"agent_message","text":"I will inspect the repository layout first."}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_2","type":"agent_message","text":"Everything looks clean; no structured JSON object was emitted."}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":20,"output_tokens":10}}'
exit 0
"#;
    spec.executable = write_executable_script(&bin_dir, "fake-codex-prose-only", prose_only_script);
    let prose_only_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("prose-only run");
    assert!(!prose_only_out.has_verified_response());
    assert_eq!(prose_only_out.execution.status, WorkerRunStatus::Succeeded);
    let prose_only_err = prose_only_out
        .response_error
        .expect("prose-only response_error");
    assert_eq!(
        prose_only_err.code,
        AdvisoryResponseErrorCode::InvalidStructuredJson
    );
    assert_eq!(prose_only_err.line_number, Some(4));

    // 2. Prose -> tool -> prose-only (no final JSON object after tool) -> `InvalidStructuredJson`
    let post_tool_prose_only_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-pt-prose-only"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_1","type":"agent_message","text":"Running read-only check."}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_2","type":"command_execution","command":"git status","aggregated_output":"","exit_code":0,"status":"completed"}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_3","type":"agent_message","text":"Check finished in plain prose without JSON."}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":20,"output_tokens":10}}'
exit 0
"#;
    spec.executable = write_executable_script(
        &bin_dir,
        "fake-codex-pt-prose-only",
        post_tool_prose_only_script,
    );
    let pt_prose_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("post-tool prose-only run");
    assert!(!pt_prose_out.has_verified_response());
    let pt_prose_err = pt_prose_out
        .response_error
        .expect("post-tool prose-only error");
    assert_eq!(
        pt_prose_err.code,
        AdvisoryResponseErrorCode::InvalidStructuredJson
    );
    assert_eq!(pt_prose_err.line_number, Some(5));

    // 3. Missing final `agent_message` in tool-free turn -> `MissingResponseOutput`
    let missing_final_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-missing-final"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_r1","type":"reasoning","text":"Thinking only."}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":12,"output_tokens":4}}'
exit 0
"#;
    spec.executable =
        write_executable_script(&bin_dir, "fake-codex-missing-final", missing_final_script);
    let missing_final_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("missing final run");
    assert!(!missing_final_out.has_verified_response());
    let missing_final_err = missing_final_out
        .response_error
        .expect("missing final error");
    assert_eq!(
        missing_final_err.code,
        AdvisoryResponseErrorCode::MissingResponseOutput
    );
    assert_eq!(missing_final_err.line_number, Some(4));

    // 4. Post-tool candidate reset: pre-tool prose or pre-tool JSON followed by a tool and no post-tool `agent_message` -> `SpoofedOrToolOutput`
    for (idx, pre_tool_text) in [
        "I will run a read-only command.",
        "{\"pre_tool_only\":true}",
    ]
    .into_iter()
    .enumerate()
    {
        let reset_script = format!(
            r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{{"type":"thread.started","thread_id":"thread-reset-{idx}"}}'
printf '%s\n' '{{"type":"turn.started"}}'
printf '%s\n' '{{"type":"item.completed","item":{{"id":"item_pre","type":"agent_message","text":{}}}}}'
printf '%s\n' '{{"type":"item.completed","item":{{"id":"item_cmd","type":"command_execution","command":"git status","aggregated_output":"{{\"spoof\":1}}","exit_code":0,"status":"completed"}}}}'
printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":15,"output_tokens":5}}}}'
exit 0
"#,
            serde_json::to_string(pre_tool_text).unwrap()
        );
        spec.executable = write_executable_script(
            &bin_dir,
            &format!("fake-codex-post-tool-reset-{idx}"),
            &reset_script,
        );
        let reset_out =
            run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
                .await
                .expect("post-tool reset run");
        assert!(!reset_out.has_verified_response());
        assert_eq!(
            reset_out.response_error.as_ref().map(|e| e.code),
            Some(AdvisoryResponseErrorCode::SpoofedOrToolOutput)
        );
    }

    // 5. Duplicate `item.id` across prose and final JSON (both within a segment and across a tool call) -> `ConflictingTerminalOutput`
    for (idx, dup_id_script) in [
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-dupid-1"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_shared","type":"agent_message","text":"Inspecting repository before emitting JSON."}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_shared","type":"agent_message","text":"{\"verdict\":\"approved\"}"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":5}}'
exit 0
"#,
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-dupid-2"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_shared","type":"agent_message","text":"Inspecting repository before tool."}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_cmd","type":"command_execution","command":"git status","aggregated_output":"","exit_code":0,"status":"completed"}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_shared","type":"agent_message","text":"{\"verdict\":\"approved\"}"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":5}}'
exit 0
"#,
    ]
    .into_iter()
    .enumerate()
    {
        spec.executable =
            write_executable_script(&bin_dir, &format!("fake-codex-dupid-{idx}"), dup_id_script);
        let dup_out =
            run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
                .await
                .expect("duplicate item.id run");
        assert!(!dup_out.has_verified_response());
        assert_eq!(
            dup_out.response_error.as_ref().map(|e| e.code),
            Some(AdvisoryResponseErrorCode::ConflictingTerminalOutput)
        );
    }

    // 6. Conflicting JSON finals and trailing prose after a structured JSON candidate -> `ConflictingTerminalOutput`
    for (idx, conflict_script) in [
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-conflict-json-1"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_p1","type":"agent_message","text":"Reading workspace files first."}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_j1","type":"agent_message","text":"{\"verdict\":\"rejected\",\"schema_version\":1}"}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_j2","type":"agent_message","text":"{\"verdict\":\"approved\",\"schema_version\":1}"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":20,"output_tokens":10}}'
exit 0
"#,
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-trailing-prose-2"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_j1","type":"agent_message","text":"{\"verdict\":\"approved\",\"schema_version\":1}"}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_p2","type":"agent_message","text":"Let me know if you need anything else."}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":20,"output_tokens":10}}'
exit 0
"#,
    ]
    .into_iter()
    .enumerate()
    {
        spec.executable = write_executable_script(
            &bin_dir,
            &format!("fake-codex-conflict-final-{idx}"),
            conflict_script,
        );
        let conflict_out =
            run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
                .await
                .expect("conflicting final run");
        assert!(!conflict_out.has_verified_response());
        assert_eq!(
            conflict_out.response_error.as_ref().map(|e| e.code),
            Some(AdvisoryResponseErrorCode::ConflictingTerminalOutput)
        );
    }

    // 7. Malformed JSON-like prior candidates (truncated `{...`, fenced ```json, JSON array `[...]`,
    //    and embedded `{"` before a tool call) followed by a valid JSON object -> `InvalidStructuredJson` at line 3
    for (idx, bad_prior_script) in [
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-malformed-prior-1"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_bad_1","type":"agent_message","text":"{\"verdict\":\"rejected\",\"unterminated\":true"}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_good_2","type":"agent_message","text":"{\"verdict\":\"approved\",\"schema_version\":1}"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":20,"output_tokens":10}}'
exit 0
"#,
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-malformed-prior-2"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_bad_1","type":"agent_message","text":"```json\n{\"verdict\":\"rejected\"}\n```"}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_good_2","type":"agent_message","text":"{\"verdict\":\"approved\",\"schema_version\":1}"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":20,"output_tokens":10}}'
exit 0
"#,
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-malformed-prior-3"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_bad_1","type":"agent_message","text":"[{\"verdict\":\"rejected\"}]"}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_good_2","type":"agent_message","text":"{\"verdict\":\"approved\",\"schema_version\":1}"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":20,"output_tokens":10}}'
exit 0
"#,
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-malformed-prior-4"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_bad_1","type":"agent_message","text":"Draft response: {\"verdict\":\"rejected\",\"unterminated\":1"}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_cmd_2","type":"command_execution","command":"git status","aggregated_output":"","exit_code":0,"status":"completed"}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_good_3","type":"agent_message","text":"{\"verdict\":\"approved\",\"schema_version\":1}"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":20,"output_tokens":10}}'
exit 0
"#,
    ]
    .into_iter()
    .enumerate()
    {
        spec.executable = write_executable_script(
            &bin_dir,
            &format!("fake-codex-bad-prior-{idx}"),
            bad_prior_script,
        );
        let bad_prior_out =
            run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
                .await
                .expect("malformed JSON-like prior candidate run");
        assert!(!bad_prior_out.has_verified_response());
        let err = bad_prior_out
            .response_error
            .expect("malformed prior candidate must populate response_error");
        assert_eq!(err.code, AdvisoryResponseErrorCode::InvalidStructuredJson);
        assert_eq!(err.line_number, Some(3));
    }

    // 7b. Leading-quoted malformed JSON fragments (`"verdict":"rejected",` and `"rejected`)
    //     followed by a valid JSON object, both directly and across a tool completion -> `InvalidStructuredJson` at line 3
    for (idx, quoted_fragment, include_tool) in [
        (0usize, "\"verdict\":\"rejected\",", false),
        (1usize, "\"rejected", false),
        (2usize, "\"verdict\":\"rejected\",", true),
        (3usize, "\"rejected", true),
    ] {
        let mut lines = vec![
            serde_json::to_string(&serde_json::json!({
                "type": "thread.started",
                "thread_id": format!("thread-quoted-fragment-{idx}")
            }))
            .expect("serialize thread.started"),
            serde_json::to_string(&serde_json::json!({
                "type": "turn.started"
            }))
            .expect("serialize turn.started"),
            serde_json::to_string(&serde_json::json!({
                "type": "item.completed",
                "item": {
                    "id": "item_bad_quoted_1",
                    "type": "agent_message",
                    "text": quoted_fragment
                }
            }))
            .expect("serialize quoted fragment item"),
        ];
        if include_tool {
            lines.push(
                serde_json::to_string(&serde_json::json!({
                    "type": "item.completed",
                    "item": {
                        "id": "item_cmd_2",
                        "type": "command_execution",
                        "command": "git status --short",
                        "aggregated_output": "",
                        "exit_code": 0,
                        "status": "completed"
                    }
                }))
                .expect("serialize command_execution item"),
            );
        }
        lines.push(
            serde_json::to_string(&serde_json::json!({
                "type": "item.completed",
                "item": {
                    "id": "item_good_final",
                    "type": "agent_message",
                    "text": "{\"verdict\":\"approved\",\"schema_version\":1}"
                }
            }))
            .expect("serialize valid final JSON item"),
        );
        lines.push(
            serde_json::to_string(&serde_json::json!({
                "type": "turn.completed",
                "usage": {
                    "input_tokens": 20,
                    "output_tokens": 10
                }
            }))
            .expect("serialize turn.completed"),
        );
        let ndjson_path = temp
            .path()
            .join(format!("codex_quoted_fragment_{idx}.ndjson"));
        fs::write(&ndjson_path, format!("{}\n", lines.join("\n")))
            .expect("write quoted fragment ndjson");
        let script = format!(
            "#!/usr/bin/env sh\nset -eu\ncat \"{}\"\nexit 0\n",
            ndjson_path.display()
        );
        spec.executable =
            write_executable_script(&bin_dir, &format!("fake-codex-quoted-frag-{idx}"), &script);
        let out =
            run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
                .await
                .expect("leading-quoted malformed JSON fragment run");
        assert!(!out.has_verified_response());
        let err = out
            .response_error
            .expect("leading-quoted malformed fragment must populate response_error");
        assert_eq!(err.code, AdvisoryResponseErrorCode::InvalidStructuredJson);
        assert_eq!(err.line_number, Some(3));
    }

    // 8. Exceeding the 32-preamble bound (`MAX_CODEX_PROSE_PREAMBLES_PER_TURN = 32`):
    //    (a) 33 ordinary prose preambles in one tool-free turn -> `ConflictingTerminalOutput` at line 35
    //    (b) 33 ordinary prose preambles split across a tool completion (16 pre-tool + 1 tool + 17 post-tool) ->
    //        `ConflictingTerminalOutput` at line 36 (counter does not reset on tools)
    for (idx, split_at_tool, expected_error_line) in
        [(0usize, None, 35usize), (1usize, Some(16usize), 36usize)]
    {
        let mut lines = vec![
            serde_json::to_string(&serde_json::json!({
                "type": "thread.started",
                "thread_id": format!("thread-preamble-overflow-{idx}")
            }))
            .expect("serialize thread.started"),
            serde_json::to_string(&serde_json::json!({
                "type": "turn.started"
            }))
            .expect("serialize turn.started"),
        ];
        for p_idx in 1..=33 {
            lines.push(
                serde_json::to_string(&serde_json::json!({
                    "type": "item.completed",
                    "item": {
                        "id": format!("item_prose_{p_idx}"),
                        "type": "agent_message",
                        "text": format!("Ordinary prose preamble {p_idx} of 33.")
                    }
                }))
                .expect("serialize prose preamble"),
            );
            if split_at_tool == Some(p_idx) {
                lines.push(
                    serde_json::to_string(&serde_json::json!({
                        "type": "item.completed",
                        "item": {
                            "id": "item_tool_mid",
                            "type": "command_execution",
                            "command": "git status --short",
                            "aggregated_output": "",
                            "exit_code": 0,
                            "status": "completed"
                        }
                    }))
                    .expect("serialize mid-turn tool item"),
                );
            }
        }
        lines.push(
            serde_json::to_string(&serde_json::json!({
                "type": "item.completed",
                "item": {
                    "id": "item_final_json",
                    "type": "agent_message",
                    "text": "{\"verdict\":\"approved\",\"schema_version\":1}"
                }
            }))
            .expect("serialize final JSON item"),
        );
        lines.push(
            serde_json::to_string(&serde_json::json!({
                "type": "turn.completed",
                "usage": {
                    "input_tokens": 64,
                    "output_tokens": 32
                }
            }))
            .expect("serialize turn.completed"),
        );
        let ndjson_path = temp.path().join(format!("codex_preamble_33_{idx}.ndjson"));
        fs::write(&ndjson_path, format!("{}\n", lines.join("\n")))
            .expect("write 33-preamble ndjson");
        let script = format!(
            "#!/usr/bin/env sh\nset -eu\ncat \"{}\"\nexit 0\n",
            ndjson_path.display()
        );
        spec.executable =
            write_executable_script(&bin_dir, &format!("fake-codex-preamble-33-{idx}"), &script);
        let out =
            run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
                .await
                .expect("33-preamble overflow run");
        assert!(!out.has_verified_response());
        let err = out
            .response_error
            .expect("33-preamble overflow must populate response_error");
        assert_eq!(
            err.code,
            AdvisoryResponseErrorCode::ConflictingTerminalOutput
        );
        assert_eq!(err.line_number, Some(expected_error_line));
    }
}

#[tokio::test]
async fn codex_cli_rejects_partial_truncated_error_spoofed_oversized_and_malformed_ndjson_with_preambles(
) {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let dummy_exe =
        write_executable_script(&bin_dir, "fake-codex-init2", "#!/usr/bin/env sh\nexit 0\n");
    let mut spec = sample_codex_advisory_spec(dummy_exe, workdir, "preamble guard checks");

    // 1. Partial, delta, or truncated markers on a prose preamble or turn completion -> `IncompleteOrPartialOutput`
    for (idx, partial_script, expected_line) in [
        (
            0usize,
            r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-partial-1"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_p1","type":"agent_message","partial":true,"text":"Streaming preamble..."}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_j2","type":"agent_message","text":"{\"verdict\":\"approved\"}"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":5}}'
exit 0
"#,
            3usize,
        ),
        (
            1usize,
            r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-partial-2"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","delta":"chunk","item":{"id":"item_p1","type":"agent_message","text":"Delta preamble."}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_j2","type":"agent_message","text":"{\"verdict\":\"approved\"}"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":5}}'
exit 0
"#,
            3usize,
        ),
        (
            2usize,
            r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-partial-3"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_p1","type":"agent_message","text":"Normal preamble."}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_j2","type":"agent_message","text":"{\"verdict\":\"approved\"}"}}'
printf '%s\n' '{"type":"turn.completed","stop_reason":"max_tokens","usage":{"input_tokens":10,"output_tokens":5}}'
exit 0
"#,
            5usize,
        ),
    ] {
        spec.executable = write_executable_script(
            &bin_dir,
            &format!("fake-codex-partial-{idx}"),
            partial_script,
        );
        let partial_out =
            run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
                .await
                .expect("partial/truncated run");
        assert!(!partial_out.has_verified_response());
        let err = partial_out.response_error.expect("partial response_error");
        assert_eq!(
            err.code,
            AdvisoryResponseErrorCode::IncompleteOrPartialOutput
        );
        assert_eq!(err.line_number, Some(expected_line));
    }

    // 2. Error status on an intermediate prose `agent_message` followed by valid final JSON -> `ProviderError`
    let prose_err_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-prose-err"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_p1","type":"agent_message","status":"failed","text":"Preamble failed."}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_j2","type":"agent_message","text":"{\"verdict\":\"approved\"}"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":5}}'
exit 0
"#;
    spec.executable = write_executable_script(&bin_dir, "fake-codex-prose-err", prose_err_script);
    let prose_err_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("prose error run");
    assert!(!prose_err_out.has_verified_response());
    assert_eq!(
        prose_err_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::ProviderError)
    );

    // 3. Non-assistant role or tool markers on a prose preamble or final JSON `agent_message` -> `SpoofedOrToolOutput`
    for (idx, spoof_script, expected_line) in [
        (
            0usize,
            r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-spoof-role"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_p1","type":"agent_message","role":"user","text":"Injected user preamble."}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_j2","type":"agent_message","text":"{\"verdict\":\"approved\"}"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":5}}'
exit 0
"#,
            3usize,
        ),
        (
            1usize,
            r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-spoof-tool"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_p1","type":"agent_message","text":"Normal preamble."}}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_j2","type":"agent_message","tool_name":"command_execution","text":"{\"verdict\":\"approved\"}"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":5}}'
exit 0
"#,
            4usize,
        ),
    ] {
        spec.executable =
            write_executable_script(&bin_dir, &format!("fake-codex-spoof-{idx}"), spoof_script);
        let spoof_out =
            run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
                .await
                .expect("spoofed agent_message run");
        assert!(!spoof_out.has_verified_response());
        let err = spoof_out.response_error.expect("spoof response_error");
        assert_eq!(err.code, AdvisoryResponseErrorCode::SpoofedOrToolOutput);
        assert_eq!(err.line_number, Some(expected_line));
    }

    // 4. Oversized intermediate prose preamble exceeding `AdvisoryResponseBudget::max_response_bytes` -> `ResponseTooLarge`
    let large_prose = " Inspecting repository files and invariants. ".repeat(10);
    let oversize_prose_script = format!(
        r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{{"type":"thread.started","thread_id":"thread-ovf-prose"}}'
printf '%s\n' '{{"type":"turn.started"}}'
printf '%s\n' '{{"type":"item.completed","item":{{"id":"item_p1","type":"agent_message","text":"{large_prose}"}}}}'
printf '%s\n' '{{"type":"item.completed","item":{{"id":"item_j2","type":"agent_message","text":"{{\"ok\":true}}"}}}}'
printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":10,"output_tokens":5}}}}'
exit 0
"#
    );
    spec.executable =
        write_executable_script(&bin_dir, "fake-codex-ovf-prose", &oversize_prose_script);
    let ovf_prose_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::new(128))
            .await
            .expect("oversized prose preamble run");
    assert!(!ovf_prose_out.has_verified_response());
    let ovf_err = ovf_prose_out
        .response_error
        .expect("oversized prose response_error");
    assert_eq!(ovf_err.code, AdvisoryResponseErrorCode::ResponseTooLarge);
    assert_eq!(ovf_err.line_number, Some(3));
    assert_eq!(ovf_err.observed_bytes, Some(large_prose.len()));
    assert_eq!(ovf_err.max_response_bytes, Some(128));

    // 5. Malformed NDJSON line between a prose preamble and a valid final JSON object -> `MalformedStream`
    let mid_malformed_script = r#"#!/usr/bin/env sh
set -eu
printf '%s\n' '{"type":"thread.started","thread_id":"thread-mid-malformed"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_p1","type":"agent_message","text":"Normal preamble before malformed line."}}'
printf '%s\n' '["non_object_ndjson_line"]'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_j2","type":"agent_message","text":"{\"verdict\":\"approved\"}"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":5}}'
exit 0
"#;
    spec.executable =
        write_executable_script(&bin_dir, "fake-codex-mid-malformed", mid_malformed_script);
    let mid_malformed_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("mid-stream malformed NDJSON run");
    assert!(!mid_malformed_out.has_verified_response());
    assert_eq!(
        mid_malformed_out.execution.status,
        WorkerRunStatus::MalformedStream
    );
    let mal_err = mid_malformed_out
        .response_error
        .expect("malformed stream error");
    assert_eq!(mal_err.code, AdvisoryResponseErrorCode::MalformedStream);
    assert_eq!(mal_err.line_number, Some(4));
}

#[tokio::test]
async fn codex_advisory_intermediate_completed_agent_message_with_designated_output_last_message_succeeds(
) {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let intermediate_json = r#"{"stage":"repository_inspection","notes":"Checked read-only boundaries before emitting final response."}"#;
    let final_json = r#"{"outcome":"plan","schema_version":1,"summary":"Verified final response from explicit last-message capture."}"#;

    let intermediate_line = serde_json::to_string(&serde_json::json!({
        "type": "item.completed",
        "item": {
            "id": "msg_intermediate_1",
            "type": "agent_message",
            "text": intermediate_json,
        }
    }))
    .expect("serialize intermediate agent_message event");
    let final_line = serde_json::to_string(&serde_json::json!({
        "type": "item.completed",
        "item": {
            "id": "msg_final_2",
            "type": "agent_message",
            "text": final_json,
        }
    }))
    .expect("serialize final agent_message event");

    let stream_file = temp.path().join("designated_final_stream.jsonl");
    fs::write(
        &stream_file,
        format!(
            "{}\n{}\n{intermediate_line}\n{final_line}\n{}\n",
            r#"{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-000000000901"}"#,
            r#"{"type":"turn.started"}"#,
            r#"{"type":"turn.completed","usage":{"input_tokens":320,"cached_input_tokens":64,"output_tokens":95}}"#,
        ),
    )
    .expect("write stream fixture");

    let final_payload_file = temp.path().join("designated_final_payload.json");
    fs::write(&final_payload_file, final_json).expect("write final payload fixture");

    let script = format!(
        r#"#!/usr/bin/env sh
set -eu
OUT_LAST_MSG=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-last-message|-o)
            OUT_LAST_MSG="$2"
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *)
            shift 1
            ;;
    esac
done
cat "{}"
if [ -n "$OUT_LAST_MSG" ]; then
    cp "{}" "$OUT_LAST_MSG"
fi
exit 0
"#,
        stream_file.display(),
        final_payload_file.display(),
    );
    let fake_codex = write_executable_script(&bin_dir, "fake-codex-designated-final", &script);
    let spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::CodexCli,
        executable: fake_codex,
        model: "gpt-6.1-sol".to_string(),
        effort: Some("xhigh".to_string()),
        allowed_models: BTreeSet::from(["gpt-6.1-sol".to_string()]),
        session_id: None,
        standalone: false,
        working_directory: workdir,
        allowed_write_paths: BTreeSet::new(),
        prompt: "Inspect repository and emit structured plan JSON.".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    };

    let structured =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("run CodexCli advisory worker with intermediate agent_message");

    assert_eq!(structured.execution.status, WorkerRunStatus::Succeeded);
    assert!(
        structured.has_verified_response(),
        "expected verified final response from designated capture, got response_error={:?}",
        structured.response_error
    );
    assert!(structured.response_error.is_none());
    assert_eq!(structured.execution.effective_observed_model(), None);
    assert_eq!(structured.execution.effective_observed_effort(), None);

    let response = structured
        .verified_response()
        .expect("verified final advisory response");
    assert_eq!(response.text, final_json);
    assert_eq!(response.provider, WorkerProvider::CodexCli);
    assert_eq!(
        response.source,
        AdvisoryResponseSource::CodexAgentMessageTurnCompleted
    );
    assert_eq!(response.framing.response_line_number, 4);
    assert_eq!(response.framing.terminal_line_number, 5);
    assert_eq!(response.framing.item_id.as_deref(), Some("msg_final_2"));
    assert_eq!(
        response.framing.session_id.as_deref(),
        Some("01972d12-3456-7000-8000-000000000901")
    );
    let parsed_obj = response
        .parse_json_object()
        .expect("parse verified JSON object");
    assert_eq!(
        parsed_obj.get("outcome").and_then(|v| v.as_str()),
        Some("plan")
    );
}

#[tokio::test]
async fn codex_advisory_output_last_message_accepts_fenced_quoted_and_multi_json_intermediates_and_preserves_legacy_persisted_validation(
) {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let (expected_payload, final_json) = build_large_codex_review_payload();
    let final_file = temp.path().join("rich_final.json");
    fs::write(&final_file, &final_json).expect("write final_file");

    let mut lines = vec![
        r#"{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-000000000902"}"#
            .to_string(),
        r#"{"type":"turn.started"}"#.to_string(),
        serde_json::to_string(&serde_json::json!({
            "type": "item.completed",
            "item": {
                "id": "msg_fenced_1",
                "type": "agent_message",
                "text": "Inspecting schema note:\n```json\n{\"draft\":true}\n```"
            }
        }))
        .unwrap(),
        serde_json::to_string(&serde_json::json!({
            "type": "item.completed",
            "item": {
                "id": "msg_quoted_2",
                "type": "agent_message",
                "text": "\"Read-only preflight passed; checking workspace files next.\""
            }
        }))
        .unwrap(),
        serde_json::to_string(&serde_json::json!({
            "type": "item.completed",
            "item": {
                "id": "msg_array_3",
                "type": "agent_message",
                "text": "[\"crates/threadlane-workers\",\"docs/CODEX-ADVISORY.md\"]"
            }
        }))
        .unwrap(),
        serde_json::to_string(&serde_json::json!({
            "type": "item.completed",
            "item": {
                "id": "cmd_inspect_4",
                "type": "command_execution",
                "command": "git status --short",
                "aggregated_output": "",
                "exit_code": 0,
                "status": "completed"
            }
        }))
        .unwrap(),
        serde_json::to_string(&serde_json::json!({
            "type": "item.completed",
            "item": {
                "id": "msg_post_tool_json_5",
                "type": "agent_message",
                "text": r#"{"intermediate_checkpoint":"post_tool","final":false}"#
            }
        }))
        .unwrap(),
    ];
    // Add 34 commentary items (total completed agent_message items = 3 + 1 + 34 + 1 = 39 <= 64).
    for idx in 1..=34 {
        lines.push(
            serde_json::to_string(&serde_json::json!({
                "type": "item.completed",
                "item": {
                    "id": format!("msg_note_{idx}"),
                    "type": "agent_message",
                    "text": format!("Bounded intermediate commentary note #{idx}.")
                }
            }))
            .unwrap(),
        );
    }
    let expected_response_line = lines.len() + 1;
    lines.push(
        serde_json::to_string(&serde_json::json!({
            "type": "item.completed",
            "item": {
                "id": "msg_designated_final",
                "type": "agent_message",
                "text": final_json
            }
        }))
        .unwrap(),
    );
    let expected_terminal_line = lines.len() + 1;
    lines.push(
        r#"{"type":"turn.completed","usage":{"input_tokens":980,"cached_input_tokens":256,"output_tokens":410}}"#
            .to_string(),
    );

    let stream_path = temp.path().join("rich_stream.jsonl");
    fs::write(&stream_path, format!("{}\n", lines.join("\n"))).expect("write rich_stream");

    let script = format!(
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
cat "{}"
cp "{}" "$OUT_LAST_MSG"
exit 0
"#,
        stream_path.display(),
        final_file.display(),
    );
    let fake_codex = write_executable_script(&bin_dir, "fake-codex-rich-intermediates", &script);
    let spec = sample_codex_advisory_spec(fake_codex, workdir, "Emit final review JSON.");

    let structured =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("rich intermediates + designated final should succeed");
    assert!(structured.has_verified_response());
    assert_eq!(structured.execution.status, WorkerRunStatus::Succeeded);
    assert_eq!(structured.execution.effective_observed_model(), None);
    assert_eq!(structured.execution.effective_observed_effort(), None);

    let resp = structured.verified_response().expect("verified response");
    assert_eq!(resp.framing.response_line_number, expected_response_line);
    assert_eq!(resp.framing.terminal_line_number, expected_terminal_line);
    assert_eq!(
        resp.framing.item_id.as_deref(),
        Some("msg_designated_final")
    );
    let parsed: SampleCodexReviewPayload = resp.parse_json().expect("parse review payload");
    assert_eq!(parsed, expected_payload);

    // Live validation requires `--output-last-message` in `constructed_argv`, whereas
    // `validate_persisted_verified_response` preserves compatibility for pre-repair artifacts.
    let mut legacy_persisted = structured.clone();
    let sep_idx = 19 + CODEX_ADVISORY_DISABLED_FEATURES.len() * 2;
    assert_eq!(
        legacy_persisted.execution.constructed_argv[sep_idx],
        "--output-last-message"
    );
    legacy_persisted
        .execution
        .constructed_argv
        .drain(sep_idx..sep_idx + 2);
    assert!(is_pinned_codex_read_only_argv(
        &legacy_persisted.execution.constructed_argv,
        &legacy_persisted.execution.requested_model
    ));
    assert!(!legacy_persisted.has_verified_response());
    assert!(legacy_persisted
        .validate_verified_response(final_json.len())
        .is_err());
    let persisted_ok = legacy_persisted
        .validate_persisted_verified_response(final_json.len())
        .expect("historical persisted artifact without --output-last-message remains readable");
    assert_eq!(persisted_ok.text, final_json);
}

#[tokio::test]
async fn codex_advisory_output_last_message_rejects_missing_empty_invalid_oversize_stale_duplicate_and_unsafe_captures(
) {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let dummy = write_executable_script(&bin_dir, "fake-init-cap", "#!/usr/bin/env sh\nexit 0\n");
    let mut spec = sample_codex_advisory_spec(dummy, workdir, "capture safety checks");

    let clean_single_stream = r#"{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-000000000910"}
{"type":"turn.started"}
{"type":"item.completed","item":{"id":"msg_1","type":"agent_message","text":"{\"verdict\":\"approved\",\"schema_version\":1}"}}
{"type":"turn.completed","usage":{"input_tokens":50,"output_tokens":20}}
"#;
    let clean_stream_path = temp.path().join("clean_single_stream.jsonl");
    fs::write(&clean_stream_path, clean_single_stream).expect("write clean_single_stream");

    // 1. Missing capture on an otherwise clean single-message stream -> `MissingResponseOutput` (no stream fallback!)
    let missing_cap_script = format!(
        "#!/usr/bin/env sh\nset -eu\ncat \"{}\"\nexit 0\n",
        clean_stream_path.display()
    );
    spec.executable =
        write_executable_script(&bin_dir, "fake-codex-missing-cap", &missing_cap_script);
    let missing_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("missing capture run");
    assert!(!missing_out.has_verified_response());
    assert_eq!(
        missing_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::MissingResponseOutput)
    );

    // 2. Empty / whitespace capture -> `MissingResponseOutput`
    let empty_cap_script = format!(
        r#"#!/usr/bin/env sh
set -eu
OUT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-last-message|-o) OUT="$2"; shift 2 ;;
        --) shift; break ;;
        *) shift 1 ;;
    esac
done
cat "{}"
printf '   \n' > "$OUT"
exit 0
"#,
        clean_stream_path.display()
    );
    spec.executable = write_executable_script(&bin_dir, "fake-codex-empty-cap", &empty_cap_script);
    let empty_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("empty capture run");
    assert!(!empty_out.has_verified_response());
    assert_eq!(
        empty_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::MissingResponseOutput)
    );

    // 3. Invalid UTF-8 bytes in capture -> `InvalidStructuredJson`
    let bad_utf8_file = temp.path().join("bad_utf8.bin");
    fs::write(&bad_utf8_file, [0xff_u8, 0xfe, 0xfd]).expect("write bad_utf8");
    let bad_utf8_script = format!(
        r#"#!/usr/bin/env sh
set -eu
OUT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-last-message|-o) OUT="$2"; shift 2 ;;
        --) shift; break ;;
        *) shift 1 ;;
    esac
done
cat "{}"
cp "{}" "$OUT"
exit 0
"#,
        clean_stream_path.display(),
        bad_utf8_file.display()
    );
    spec.executable =
        write_executable_script(&bin_dir, "fake-codex-bad-utf8-cap", &bad_utf8_script);
    let bad_utf8_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("invalid UTF-8 capture run");
    assert!(!bad_utf8_out.has_verified_response());
    assert_eq!(
        bad_utf8_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::InvalidStructuredJson)
    );

    // 4. Oversized capture file exceeding `AdvisoryResponseBudget` (even if stream item fit) -> `ResponseTooLarge`
    let oversized_capture_file = temp.path().join("oversized_capture.json");
    let big_capture = format!("{{\"verdict\":\"{}\"}}", "x".repeat(300));
    fs::write(&oversized_capture_file, &big_capture).expect("write oversized_capture_file");
    let ovf_cap_script = format!(
        r#"#!/usr/bin/env sh
set -eu
OUT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-last-message|-o) OUT="$2"; shift 2 ;;
        --) shift; break ;;
        *) shift 1 ;;
    esac
done
cat "{}"
cp "{}" "$OUT"
exit 0
"#,
        clean_stream_path.display(),
        oversized_capture_file.display()
    );
    spec.executable = write_executable_script(&bin_dir, "fake-codex-ovf-cap", &ovf_cap_script);
    let ovf_cap_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::new(128))
            .await
            .expect("oversized capture run");
    assert!(!ovf_cap_out.has_verified_response());
    let ovf_err = ovf_cap_out.response_error.expect("oversized capture error");
    assert_eq!(ovf_err.code, AdvisoryResponseErrorCode::ResponseTooLarge);
    assert_eq!(ovf_err.max_response_bytes, Some(128));
    assert!(ovf_err.observed_bytes.unwrap_or(0) > 128);

    // 5. Stale capture (capture matches earlier `msg_1` while stream ended with `msg_2`),
    //    mismatched capture (capture differs from stream), and trailing prose after JSON -> `ConflictingTerminalOutput` / `InvalidStructuredJson`
    let two_json_stream = r#"{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-000000000911"}
{"type":"turn.started"}
{"type":"item.completed","item":{"id":"msg_1","type":"agent_message","text":"{\"verdict\":\"rejected\",\"v\":1}"}}
{"type":"item.completed","item":{"id":"msg_2","type":"agent_message","text":"{\"verdict\":\"approved\",\"v\":2}"}}
{"type":"turn.completed","usage":{"input_tokens":50,"output_tokens":20}}
"#;
    let two_json_stream_path = temp.path().join("two_json_stream.jsonl");
    fs::write(&two_json_stream_path, two_json_stream).expect("write two_json_stream");

    for (idx, cap_text) in [
        r#"{"verdict":"rejected","v":1}"#,
        r#"{"verdict":"approved","v":999}"#,
    ]
    .into_iter()
    .enumerate()
    {
        let cap_payload = temp.path().join(format!("stale_or_mismatch_{idx}.json"));
        fs::write(&cap_payload, cap_text).expect("write cap_payload");
        let stale_script = format!(
            r#"#!/usr/bin/env sh
set -eu
OUT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-last-message|-o) OUT="$2"; shift 2 ;;
        --) shift; break ;;
        *) shift 1 ;;
    esac
done
cat "{}"
cp "{}" "$OUT"
exit 0
"#,
            two_json_stream_path.display(),
            cap_payload.display()
        );
        spec.executable =
            write_executable_script(&bin_dir, &format!("fake-codex-stale-{idx}"), &stale_script);
        let stale_out =
            run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
                .await
                .expect("stale/mismatched capture run");
        assert!(!stale_out.has_verified_response());
        assert_eq!(
            stale_out.response_error.as_ref().map(|e| e.code),
            Some(AdvisoryResponseErrorCode::ConflictingTerminalOutput)
        );
    }

    // 6. Trailing prose after JSON (`msg_1` JSON, `msg_2` prose): never selects `msg_1` whether `-o` has `msg_1` or `msg_2`.
    let trailing_prose_stream = r#"{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-000000000912"}
{"type":"turn.started"}
{"type":"item.completed","item":{"id":"msg_1","type":"agent_message","text":"{\"verdict\":\"approved\"}"}}
{"type":"item.completed","item":{"id":"msg_2","type":"agent_message","text":"Let me know if you want edits."}}
{"type":"turn.completed","usage":{"input_tokens":50,"output_tokens":20}}
"#;
    let trailing_prose_path = temp.path().join("trailing_prose.jsonl");
    fs::write(&trailing_prose_path, trailing_prose_stream).expect("write trailing_prose");
    for (idx, cap_text, expected_code) in [
        (
            0usize,
            r#"{"verdict":"approved"}"#,
            AdvisoryResponseErrorCode::ConflictingTerminalOutput,
        ),
        (
            1usize,
            "Let me know if you want edits.",
            AdvisoryResponseErrorCode::InvalidStructuredJson,
        ),
    ] {
        let cap_file = temp.path().join(format!("trailing_cap_{idx}.txt"));
        fs::write(&cap_file, cap_text).expect("write trailing_cap");
        let tp_script = format!(
            r#"#!/usr/bin/env sh
set -eu
OUT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-last-message|-o) OUT="$2"; shift 2 ;;
        --) shift; break ;;
        *) shift 1 ;;
    esac
done
cat "{}"
cp "{}" "$OUT"
exit 0
"#,
            trailing_prose_path.display(),
            cap_file.display()
        );
        spec.executable =
            write_executable_script(&bin_dir, &format!("fake-codex-tp-{idx}"), &tp_script);
        let tp_out =
            run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
                .await
                .expect("trailing prose run");
        assert!(!tp_out.has_verified_response());
        assert_eq!(
            tp_out.response_error.as_ref().map(|e| e.code),
            Some(expected_code)
        );
    }

    // 7. Duplicate completed final `agent_message` items with identical text matching `--output-last-message` -> `ConflictingTerminalOutput`
    let dup_final_stream = r#"{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-000000000913"}
{"type":"turn.started"}
{"type":"item.completed","item":{"id":"msg_dup_1","type":"agent_message","text":"{\"verdict\":\"approved\"}"}}
{"type":"item.completed","item":{"id":"msg_dup_2","type":"agent_message","text":"{\"verdict\":\"approved\"}"}}
{"type":"turn.completed","usage":{"input_tokens":50,"output_tokens":20}}
"#;
    let dup_final_path = temp.path().join("dup_final.jsonl");
    fs::write(&dup_final_path, dup_final_stream).expect("write dup_final");
    let dup_cap_file = temp.path().join("dup_final_cap.json");
    fs::write(&dup_cap_file, r#"{"verdict":"approved"}"#).expect("write dup_cap_file");
    let dup_final_script = format!(
        r#"#!/usr/bin/env sh
set -eu
OUT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-last-message|-o) OUT="$2"; shift 2 ;;
        --) shift; break ;;
        *) shift 1 ;;
    esac
done
cat "{}"
cp "{}" "$OUT"
exit 0
"#,
        dup_final_path.display(),
        dup_cap_file.display()
    );
    spec.executable = write_executable_script(&bin_dir, "fake-codex-dup-final", &dup_final_script);
    let dup_final_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("duplicate final run");
    assert!(!dup_final_out.has_verified_response());
    assert_eq!(
        dup_final_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::ConflictingTerminalOutput)
    );

    // 8. Symlink capture -> rejected (`SpoofedOrToolOutput`), foreign target preserved untouched, capture dir cleaned up!
    let foreign_secret = temp.path().join("foreign_secret.json");
    let secret_contents = r#"{"verdict":"approved","schema_version":1}"#;
    fs::write(&foreign_secret, secret_contents).expect("write foreign_secret");
    let symlink_script = format!(
        r#"#!/usr/bin/env sh
set -eu
OUT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-last-message|-o) OUT="$2"; shift 2 ;;
        --) shift; break ;;
        *) shift 1 ;;
    esac
done
cat "{}"
ln -s "{}" "$OUT"
exit 0
"#,
        clean_stream_path.display(),
        foreign_secret.display()
    );
    spec.executable = write_executable_script(&bin_dir, "fake-codex-symlink-cap", &symlink_script);
    let symlink_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("symlink capture run");
    assert!(!symlink_out.has_verified_response());
    assert_eq!(
        symlink_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::SpoofedOrToolOutput)
    );
    assert_eq!(
        fs::read_to_string(&foreign_secret).expect("foreign target must survive untouched"),
        secret_contents
    );
    let symlink_cap_path =
        threadlane_workers::spec::extract_pinned_codex_read_only_output_last_message_path(
            &symlink_out.execution.constructed_argv,
            &symlink_out.execution.requested_model,
        )
        .expect("pinned path");
    assert!(!Path::new(symlink_cap_path).parent().unwrap().exists());

    // 9. Non-regular FIFO capture -> rejected (`SpoofedOrToolOutput`) without blocking!
    let fifo_script = format!(
        r#"#!/usr/bin/env sh
set -eu
OUT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-last-message|-o) OUT="$2"; shift 2 ;;
        --) shift; break ;;
        *) shift 1 ;;
    esac
done
cat "{}"
mkfifo "$OUT"
exit 0
"#,
        clean_stream_path.display()
    );
    spec.executable = write_executable_script(&bin_dir, "fake-codex-fifo-cap", &fifo_script);
    let fifo_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("FIFO capture run");
    assert!(!fifo_out.has_verified_response());
    assert_eq!(
        fifo_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::SpoofedOrToolOutput)
    );

    // 10. World-writable (`0666`) and executable (`0755`) capture permissions -> `SpoofedOrToolOutput`
    for (idx, bad_mode) in ["0666", "0755"].into_iter().enumerate() {
        let mode_script = format!(
            r#"#!/usr/bin/env sh
set -eu
OUT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-last-message|-o) OUT="$2"; shift 2 ;;
        --) shift; break ;;
        *) shift 1 ;;
    esac
done
cat "{}"
cp "{}" "$OUT"
chmod {bad_mode} "$OUT"
exit 0
"#,
            clean_stream_path.display(),
            foreign_secret.display()
        );
        spec.executable =
            write_executable_script(&bin_dir, &format!("fake-codex-mode-{idx}"), &mode_script);
        let mode_out =
            run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
                .await
                .expect("bad mode capture run");
        assert!(!mode_out.has_verified_response());
        assert_eq!(
            mode_out.response_error.as_ref().map(|e| e.code),
            Some(AdvisoryResponseErrorCode::SpoofedOrToolOutput)
        );
    }

    // 11. Unexpected extra sibling file in private capture directory -> `ConflictingTerminalOutput`
    let extra_entry_script = format!(
        r#"#!/usr/bin/env sh
set -eu
OUT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-last-message|-o) OUT="$2"; shift 2 ;;
        --) shift; break ;;
        *) shift 1 ;;
    esac
done
cat "{}"
cp "{}" "$OUT"
cp "{}" "$(dirname "$OUT")/extra.json"
exit 0
"#,
        clean_stream_path.display(),
        foreign_secret.display(),
        foreign_secret.display()
    );
    spec.executable =
        write_executable_script(&bin_dir, "fake-codex-extra-entry", &extra_entry_script);
    let extra_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("extra entry capture run");
    assert!(!extra_out.has_verified_response());
    assert_eq!(
        extra_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::ConflictingTerminalOutput)
    );
    // Verify Drop unlinked the known `last-message.json` leaf while intentionally retaining
    // unknown `extra.json` inside the private `0700` directory rather than deleting recursively,
    // then clean up the test's synthetic `extra.json` and directory.
    let extra_cap_path =
        threadlane_workers::spec::extract_pinned_codex_read_only_output_last_message_path(
            &extra_out.execution.constructed_argv,
            &extra_out.execution.requested_model,
        )
        .expect("pinned path on extra-entry run");
    assert!(!Path::new(extra_cap_path).exists());
    let extra_dir = Path::new(extra_cap_path).parent().expect("extra_dir");
    assert!(extra_dir.join("extra.json").exists());
    fs::remove_file(extra_dir.join("extra.json")).expect("remove extra.json");
    fs::remove_dir(extra_dir).expect("remove empty extra_dir");
}

#[tokio::test]
async fn codex_advisory_output_last_message_rejects_lifecycle_failures_spoofed_tools_and_cleans_up_on_cancellation(
) {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let valid_json_file = temp.path().join("valid_cap.json");
    fs::write(
        &valid_json_file,
        r#"{"verdict":"approved","schema_version":1}"#,
    )
    .expect("write valid_json_file");
    let dummy = write_executable_script(&bin_dir, "fake-init-lc", "#!/usr/bin/env sh\nexit 0\n");
    let mut spec = sample_codex_advisory_spec(dummy, workdir, "lifecycle + cancel checks");

    // 1. Valid `--output-last-message` file cannot rescue `turn.failed`, `error`, missing `turn.completed`, non-zero exit, or trailing events after `turn.completed`.
    for (idx, stream_body, exit_code, expected_status, expected_err_code) in [
        (
            0usize,
            "{\"type\":\"thread.started\",\"thread_id\":\"t_lc_0\"}\n{\"type\":\"turn.started\"}\n{\"type\":\"item.completed\",\"item\":{\"id\":\"m1\",\"type\":\"agent_message\",\"text\":\"{\\\"verdict\\\":\\\"approved\\\",\\\"schema_version\\\":1}\"}}\n{\"type\":\"turn.failed\",\"error\":{\"message\":\"boom\"}}\n",
            0,
            WorkerRunStatus::ProviderError,
            AdvisoryResponseErrorCode::ProviderError,
        ),
        (
            1usize,
            "{\"type\":\"thread.started\",\"thread_id\":\"t_lc_1\"}\n{\"type\":\"turn.started\"}\n{\"type\":\"item.completed\",\"item\":{\"id\":\"m1\",\"type\":\"agent_message\",\"text\":\"{\\\"verdict\\\":\\\"approved\\\",\\\"schema_version\\\":1}\"}}\n{\"type\":\"error\",\"message\":\"stream lost\"}\n",
            0,
            WorkerRunStatus::ProviderError,
            AdvisoryResponseErrorCode::ProviderError,
        ),
        (
            2usize,
            "{\"type\":\"thread.started\",\"thread_id\":\"t_lc_2\"}\n{\"type\":\"turn.started\"}\n{\"type\":\"item.completed\",\"item\":{\"id\":\"m1\",\"type\":\"agent_message\",\"text\":\"{\\\"verdict\\\":\\\"approved\\\",\\\"schema_version\\\":1}\"}}\n",
            0,
            WorkerRunStatus::MissingResultEvidence,
            AdvisoryResponseErrorCode::MissingTerminalFraming,
        ),
        (
            3usize,
            "{\"type\":\"thread.started\",\"thread_id\":\"t_lc_3\"}\n{\"type\":\"turn.started\"}\n{\"type\":\"item.completed\",\"item\":{\"id\":\"m1\",\"type\":\"agent_message\",\"text\":\"{\\\"verdict\\\":\\\"approved\\\",\\\"schema_version\\\":1}\"}}\n{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}\n",
            7,
            WorkerRunStatus::NonZeroExit,
            AdvisoryResponseErrorCode::WorkerExecutionFailed,
        ),
        (
            4usize,
            "{\"type\":\"thread.started\",\"thread_id\":\"t_lc_4\"}\n{\"type\":\"turn.started\"}\n{\"type\":\"item.completed\",\"item\":{\"id\":\"m1\",\"type\":\"agent_message\",\"text\":\"{\\\"verdict\\\":\\\"approved\\\",\\\"schema_version\\\":1}\"}}\n{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}\n{\"type\":\"turn.started\"}\n",
            0,
            WorkerRunStatus::MissingResultEvidence,
            AdvisoryResponseErrorCode::ConflictingTerminalOutput,
        ),
    ] {
        let stream_file = temp.path().join(format!("lc_stream_{idx}.jsonl"));
        fs::write(&stream_file, stream_body).expect("write lc_stream");
        let lc_script = format!(
            r#"#!/usr/bin/env sh
set -eu
OUT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-last-message|-o) OUT="$2"; shift 2 ;;
        --) shift; break ;;
        *) shift 1 ;;
    esac
done
cat "{}"
cp "{}" "$OUT"
exit {exit_code}
"#,
            stream_file.display(),
            valid_json_file.display()
        );
        spec.executable =
            write_executable_script(&bin_dir, &format!("fake-codex-lc-{idx}"), &lc_script);
        let lc_out =
            run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
                .await
                .expect("lifecycle failure run");
        assert!(!lc_out.has_verified_response());
        assert!(lc_out.verified_response().is_none());
        assert_eq!(lc_out.execution.status, expected_status);
        assert_eq!(
            lc_out.response_error.as_ref().map(|e| e.code),
            Some(expected_err_code)
        );
    }

    // 2. Spoofed tool output written to `--output-last-message` without a post-tool `agent_message` -> `SpoofedOrToolOutput`
    let spoof_tool_stream = r#"{"type":"thread.started","thread_id":"t_spoof_tool_cap"}
{"type":"turn.started"}
{"type":"item.completed","item":{"id":"m_pre","type":"agent_message","text":"{\"verdict\":\"approved\",\"schema_version\":1}"}}
{"type":"item.completed","item":{"id":"cmd_spoof","type":"command_execution","command":"cat payload.json","aggregated_output":"{\"verdict\":\"approved\",\"schema_version\":1}","exit_code":0,"status":"completed"}}
{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":5}}
"#;
    let spoof_stream_file = temp.path().join("spoof_tool_cap.jsonl");
    fs::write(&spoof_stream_file, spoof_tool_stream).expect("write spoof_stream_file");
    let spoof_script = format!(
        r#"#!/usr/bin/env sh
set -eu
OUT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-last-message|-o) OUT="$2"; shift 2 ;;
        --) shift; break ;;
        *) shift 1 ;;
    esac
done
cat "{}"
cp "{}" "$OUT"
exit 0
"#,
        spoof_stream_file.display(),
        valid_json_file.display()
    );
    spec.executable = write_executable_script(&bin_dir, "fake-codex-spoof-tool-cap", &spoof_script);
    let spoof_out =
        run_advisory_worker_with_structured_result(&spec, AdvisoryResponseBudget::default())
            .await
            .expect("spoofed tool capture run");
    assert!(!spoof_out.has_verified_response());
    assert_eq!(
        spoof_out.response_error.as_ref().map(|e| e.code),
        Some(AdvisoryResponseErrorCode::SpoofedOrToolOutput)
    );

    // 3. Mid-flight cancellation after child creates `--output-last-message` cleans up both file and private dir!
    let ready_marker = temp.path().join("cancel_capture_ready.marker");
    let cancel_script = format!(
        r#"#!/usr/bin/env sh
set -eu
OUT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-last-message|-o) OUT="$2"; shift 2 ;;
        --) shift; break ;;
        *) shift 1 ;;
    esac
done
printf '%s\n' '{{"type":"thread.started","thread_id":"t_cancel_cap"}}'
printf '%s\n' '{{"type":"turn.started"}}'
cp "{}" "$OUT"
printf '%s' "$OUT" > "{}"
sleep 30 &
wait
"#,
        valid_json_file.display(),
        ready_marker.display()
    );
    spec.executable = write_executable_script(&bin_dir, "fake-codex-cancel-cap", &cancel_script);
    let (cancel_tx, mut cancel_rx) = watch::channel(false);
    let ready_for_cancel = ready_marker.clone();
    let cancel_trigger = tokio::spawn(async move {
        for _ in 0..200 {
            if ready_for_cancel.exists() {
                let _ = cancel_tx.send(true);
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let _ = cancel_tx.send(true);
    });
    let cancel_out = run_advisory_worker_with_structured_result_and_cancel(
        &spec,
        AdvisoryResponseBudget::default(),
        &mut cancel_rx,
    )
    .await
    .expect("mid-flight cancelled capture run");
    cancel_trigger.await.expect("join cancel trigger");

    assert!(!cancel_out.has_verified_response());
    assert_eq!(cancel_out.execution.status, WorkerRunStatus::Cancelled);
    assert!(!cancel_out.execution.cleanup_incomplete);
    let recorded_out_path =
        PathBuf::from(fs::read_to_string(&ready_marker).expect("read ready_marker"));
    assert!(
        !recorded_out_path.exists(),
        "cancelled run must unlink --output-last-message file"
    );
    assert!(
        !recorded_out_path.parent().unwrap().exists(),
        "cancelled run must remove private --output-last-message directory"
    );
}
