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
        r#"permissions.threadlane-advisory={"filesystem"={":minimal"="read",":workspace_roots"={"."="read","**/.env"="deny","**/.env.*"="deny","**/codex.toml"="deny","**/.codex.toml"="deny","**/.git"="deny","**/.git/**"="deny","**/.codex"="deny","**/.codex/**"="deny","**/.agents"="deny","**/.agents/**"="deny","**/.aws"="deny","**/.aws/**"="deny"},"/nix/store"="read"},"network"={"enabled"=false}}"#
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

    // 7. Reject mutable WorkerSpec for CodexCli across all permission modes before process effect
    for (mode, write_paths) in [
        (
            WorkerFilePermissions::Inherit,
            BTreeSet::from(["src/lib.rs".to_string()]),
        ),
        (
            WorkerFilePermissions::TaskFiles,
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
            .expect_err("mutable WorkerSpec must reject CodexCli");
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

    let argv_dump_file = temp.path().join("codex_argv.txt");
    let script = format!(
        r#"#!/usr/bin/env sh
set -eu
for arg in "$@"; do
    printf '%s\n' "$arg" >> "{}"
done
printf '%s\n' '{{"type":"thread.started","thread_id":"019a52d7-7c00-7111-8222-codexthread01"}}'
printf '%s\n' '{{"type":"turn.started"}}'
printf '%s\n' '{{"type":"item.completed","item":{{"id":"item_reason_1","type":"reasoning","text":" Inspecting workspace under read-only profile. "}}}}'
printf '%s\n' '{{"type":"item.completed","item":{{"id":"item_cmd_1","type":"command_execution","command":"git status --short","aggregated_output":"{{\"not_the_final_response\":true}}","exit_code":0,"status":"completed"}}}}'
cat "{}"
printf '\n'
printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":1420,"cached_input_tokens":1024,"cache_write_input_tokens":64,"output_tokens":318,"reasoning_output_tokens":192}}}}'
exit 0
"#,
        argv_dump_file.display(),
        item_line_file.display()
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
