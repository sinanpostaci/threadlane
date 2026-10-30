#![cfg(unix)]

use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;

use threadlane_workers::{
    run_worker, run_worker_with_cancel, ExecutionBudgets, FieldEvidence, OpenCodeMetadataEvidence,
    WorkerFilePermissions, WorkerProvider, WorkerRunStatus, WorkerSpec,
};

static SCRIPT_COUNTER: AtomicU64 = AtomicU64::new(1);

fn current_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as u64
}

fn write_executable_script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let seq = SCRIPT_COUNTER.fetch_add(1, Ordering::Relaxed);
    let final_path = dir.join(format!("{name}-{seq}.sh"));
    let staging_path = dir.join(format!(".{name}-{seq}.sh.tmp"));

    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o700)
        .open(&staging_path)
        .expect("create staging script");
    file.write_all(body.as_bytes()).expect("write script body");
    file.sync_all().expect("sync script");
    drop(file);

    fs::set_permissions(&staging_path, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
    fs::rename(&staging_path, &final_path).expect("atomic rename script");
    final_path
}

fn setup_test_env(temp: &Path) -> (PathBuf, PathBuf, PathBuf) {
    let repo_dir = temp.join("repo");
    let work_area = temp.join("work-area");
    let workdir = temp.join("workdir");
    fs::create_dir_all(&repo_dir).expect("create repo_dir");
    fs::create_dir_all(&work_area).expect("create work_area");
    fs::create_dir_all(&workdir).expect("create workdir");
    fs::set_permissions(&work_area, fs::Permissions::from_mode(0o700)).expect("chmod work_area");
    (
        fs::canonicalize(&repo_dir).expect("canonical repo"),
        fs::canonicalize(&work_area).expect("canonical work_area"),
        fs::canonicalize(&workdir).expect("canonical workdir"),
    )
}

fn make_opencode_spec(
    executable: PathBuf,
    repo_dir: PathBuf,
    work_area: PathBuf,
    session_id: Option<String>,
) -> WorkerSpec {
    let mut allowed_models = BTreeSet::new();
    allowed_models.insert("opencode-go/deepseek-v4.1-flash".to_string());
    let mut allowed_write_paths = BTreeSet::new();
    allowed_write_paths.insert("src/lib.rs".to_string());

    WorkerSpec {
        provider: WorkerProvider::OpenCodeV2,
        executable,
        model: "opencode-go/deepseek-v4.1-flash".to_string(),
        effort: Some("max".to_string()),
        allowed_models,
        prompt: "Implement the bounded change.".to_string(),
        standalone: true,
        session_id,
        repository_path: repo_dir,
        base_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
        work_area_root: work_area,
        allowed_write_paths,
        file_permissions: WorkerFilePermissions::Inherit,
        budgets: ExecutionBudgets::default(),
    }
}

fn read_invocations(log_path: &Path) -> Vec<String> {
    let Ok(raw) = fs::read_to_string(log_path) else {
        return Vec::new();
    };
    raw.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

#[tokio::test]
async fn new_opencode_run_allocates_session_supplements_missing_stream_fields_and_redacts_raw_export(
) {
    let temp = tempfile::tempdir().expect("tempdir");
    let (repo_dir, work_area, workdir) = setup_test_env(temp.path());
    let log_path = temp.path().join("invocations.log");
    let base_ts = current_epoch_ms().saturating_add(15_000);
    let workdir_str = workdir.to_str().expect("utf8 workdir");

    let script_body = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
LOG="{log}"
WORKDIR="{workdir}"
BASE_TS={base_ts}

if [[ "${{1:-}}" == "api" && "${{2:-}}" == "--standalone" && "${{3:-}}" == "session.create" ]]; then
  echo "create" >> "$LOG"
  printf '{{"data":{{"id":"ses_owned_alpha","location":{{"directory":"%s"}},"time":{{"created":%s}}}}}}\n' "$WORKDIR" "$BASE_TS"
  exit 0
fi

if [[ "${{1:-}}" == "--standalone" && "${{2:-}}" == "run" ]]; then
  shift
  SID=""
  while [[ $# -gt 0 ]]; do
    if [[ "$1" == "--session" ]]; then
      SID="$2"
      shift 2
    else
      shift
    fi
  done
  echo "run:$SID" >> "$LOG"
  printf '{{"type":"step_start","sessionID":"%s","part":{{"type":"step-start"}}}}\n' "$SID"
  printf '{{"type":"text","sessionID":"%s","part":{{"type":"text","text":"completed task"}}}}\n' "$SID"
  exit 0
fi

if [[ "${{1:-}}" == "api" && "${{2:-}}" == "--standalone" && "${{3:-}}" == "session.get" ]]; then
  PARAM="${{5:-}}"
  SID="${{PARAM#sessionID=}}"
  echo "get:$SID" >> "$LOG"
  printf '{{"data":{{"id":"%s","location":{{"directory":"%s"}},"outcome":"succeeded","time":{{"created":%s,"updated":%s,"idle":%s}}}}}}\n' \
    "$SID" "$WORKDIR" "$BASE_TS" "$((BASE_TS + 20))" "$((BASE_TS + 30))"
  exit 0
fi

if [[ "${{1:-}}" == "session" && "${{2:-}}" == "export" ]]; then
  SID="${{5:-}}"
  echo "export:$SID" >> "$LOG"
  printf '{{"info":{{"id":"%s","title":"RAW_EXPORT_SENTINEL_TITLE_DO_NOT_LEAK","location":{{"directory":"%s"}},"outcome":"succeeded","time":{{"created":%s,"updated":%s,"idle":%s}}}},"messages":[{{"id":"msg_user_1","sessionID":"%s","type":"user","time":{{"created":%s}},"parts":[{{"type":"text","text":"RAW_EXPORT_SENTINEL_PROMPT_DO_NOT_LEAK"}}]}},{{"id":"msg_ast_1","sessionID":"%s","type":"assistant","model":{{"providerID":"opencode-go","id":"deepseek-v4.1-flash","variant":"max"}},"finish":"stop","time":{{"created":%s,"completed":%s}},"parts":[{{"type":"text","text":"RAW_EXPORT_SENTINEL_BODY_DO_NOT_LEAK"}}]}} ]}}\n' \
    "$SID" "$WORKDIR" "$BASE_TS" "$((BASE_TS + 20))" "$((BASE_TS + 30))" "$SID" "$((BASE_TS + 5))" "$SID" "$((BASE_TS + 10))" "$((BASE_TS + 20))"
  exit 0
fi

echo "unexpected:$*" >> "$LOG"
exit 2
"#,
        log = log_path.display(),
        workdir = workdir_str,
        base_ts = base_ts,
    );

    let executable = write_executable_script(temp.path(), "fake-opencode-ok", &script_body);
    let spec = make_opencode_spec(executable, repo_dir, work_area, None);

    let report = run_worker(&spec, &workdir).await.expect("run_worker");

    assert!(
        spec.session_id.is_none(),
        "original WorkerSpec must not be mutated"
    );
    assert!(report.requested_session_id.is_none());
    assert_eq!(
        report.allocated_session_id.as_deref(),
        Some("ses_owned_alpha")
    );
    assert!(report
        .constructed_argv
        .windows(2)
        .any(|pair| pair == ["--session", "ses_owned_alpha"]));
    assert_eq!(report.status, WorkerRunStatus::Succeeded);

    // Stream evidence stays untouched even though metadata supplemented missing fields.
    assert!(!report.evidence.saw_result_event);
    assert!(matches!(
        report.evidence.model_evidence,
        FieldEvidence::Missing { .. }
    ));
    assert!(matches!(
        report.evidence.effort_evidence,
        FieldEvidence::Missing { .. }
    ));
    assert_eq!(
        report.evidence.session_evidence,
        FieldEvidence::VerifiedMatch {
            value: "ses_owned_alpha".to_string()
        }
    );

    // Report helpers reflect validated metadata supplementation.
    assert!(report.has_complete_natural_completion_evidence());
    assert!(report.has_satisfied_model_evidence());
    assert!(report.has_satisfied_effort_evidence());
    assert!(report.has_satisfied_session_evidence());
    assert!(report.has_satisfied_result_evidence());
    assert_eq!(
        report.effective_observed_model(),
        Some("opencode-go/deepseek-v4.1-flash")
    );
    assert_eq!(report.effective_observed_effort(), Some("max"));
    assert!(report.opencode_metadata_error.is_none());

    let meta = report
        .opencode_metadata
        .as_ref()
        .expect("opencode_metadata present");
    assert_eq!(meta.session_id, "ses_owned_alpha");
    assert_eq!(meta.observed_model, "opencode-go/deepseek-v4.1-flash");
    assert_eq!(meta.observed_effort.as_deref(), Some("max"));
    assert_eq!(meta.assistant_message_count, 1);
    assert_eq!(meta.terminal_message_id, "msg_ast_1");

    let serialized = serde_json::to_string(&report).expect("serialize report");
    assert!(
        !serialized.contains("RAW_EXPORT_SENTINEL"),
        "serialized report must never contain raw export content"
    );

    assert_eq!(
        read_invocations(&log_path),
        vec![
            "create".to_string(),
            "run:ses_owned_alpha".to_string(),
            "get:ses_owned_alpha".to_string(),
            "export:ses_owned_alpha".to_string(),
        ]
    );
}

#[tokio::test]
async fn negative_metadata_table_rejects_invalid_info_export_and_never_replays_inference() {
    let temp = tempfile::tempdir().expect("tempdir");
    let (repo_dir, work_area, workdir) = setup_test_env(temp.path());
    let workdir_str = workdir.to_str().expect("utf8 workdir");

    let cases: &[(&str, WorkerRunStatus, &str)] = &[
        (
            "wrong_info_id",
            WorkerRunStatus::SessionMismatch,
            "ownership_mismatch",
        ),
        (
            "wrong_export_id",
            WorkerRunStatus::SessionMismatch,
            "ownership_mismatch",
        ),
        (
            "wrong_cwd",
            WorkerRunStatus::SessionMismatch,
            "ownership_mismatch",
        ),
        (
            "wrong_model",
            WorkerRunStatus::ModelMismatch,
            "model_mismatch",
        ),
        (
            "wrong_variant",
            WorkerRunStatus::EffortMismatch,
            "effort_mismatch",
        ),
        (
            "absent_variant",
            WorkerRunStatus::MissingEffortEvidence,
            "missing_effort",
        ),
        ("stale_ts", WorkerRunStatus::NonZeroExit, "stale_session"),
        (
            "no_stop",
            WorkerRunStatus::MissingResultEvidence,
            "missing_completion",
        ),
    ];

    for &(mode, expected_status, expected_error_code) in cases {
        let base_ts = current_epoch_ms().saturating_add(15_000);
        let log_path = temp.path().join(format!("invocations-{mode}.log"));
        let script_body = format!(
            r#"#!/usr/bin/env bash
set -euo pipefail
LOG="{log}"
WORKDIR="{workdir}"
BASE_TS={base_ts}
MODE="{mode}"

if [[ "${{1:-}}" == "api" && "${{2:-}}" == "--standalone" && "${{3:-}}" == "session.create" ]]; then
  echo "create" >> "$LOG"
  printf '{{"data":{{"id":"ses_neg_01","location":{{"directory":"%s"}},"time":{{"created":%s}}}}}}\n' "$WORKDIR" "$BASE_TS"
  exit 0
fi

if [[ "${{1:-}}" == "--standalone" && "${{2:-}}" == "run" ]]; then
  shift
  echo "run" >> "$LOG"
  printf '{{"type":"text","sessionID":"ses_neg_01","part":{{"type":"text","text":"ran"}}}}\n'
  exit 0
fi

if [[ "${{1:-}}" == "api" && "${{2:-}}" == "--standalone" && "${{3:-}}" == "session.get" ]]; then
  echo "get" >> "$LOG"
  INFO_ID="ses_neg_01"
  INFO_DIR="$WORKDIR"
  INFO_CREATED="$BASE_TS"
  if [[ "$MODE" == "wrong_info_id" ]]; then INFO_ID="ses_other_99"; fi
  if [[ "$MODE" == "wrong_cwd" ]]; then INFO_DIR="/tmp/unrelated-opencode-dir"; fi
  if [[ "$MODE" == "stale_ts" ]]; then INFO_CREATED="1000"; fi
  printf '{{"data":{{"id":"%s","location":{{"directory":"%s"}},"outcome":"succeeded","time":{{"created":%s,"updated":%s,"idle":%s}}}}}}\n' \
    "$INFO_ID" "$INFO_DIR" "$INFO_CREATED" "$((BASE_TS + 20))" "$((BASE_TS + 30))"
  exit 0
fi

if [[ "${{1:-}}" == "session" && "${{2:-}}" == "export" ]]; then
  echo "export" >> "$LOG"
  EXP_ID="ses_neg_01"
  EXP_DIR="$WORKDIR"
  EXP_CREATED="$BASE_TS"
  MODEL_JSON='{{"providerID":"opencode-go","id":"deepseek-v4.1-flash","variant":"max"}}'
  FINISH="stop"

  if [[ "$MODE" == "wrong_export_id" ]]; then EXP_ID="ses_other_99"; fi
  if [[ "$MODE" == "wrong_cwd" ]]; then EXP_DIR="/tmp/unrelated-opencode-dir"; fi
  if [[ "$MODE" == "stale_ts" ]]; then EXP_CREATED="1000"; fi
  if [[ "$MODE" == "wrong_model" ]]; then
    MODEL_JSON='{{"providerID":"opencode-go","id":"wrong-model","variant":"max"}}'
  fi
  if [[ "$MODE" == "wrong_variant" ]]; then
    MODEL_JSON='{{"providerID":"opencode-go","id":"deepseek-v4.1-flash","variant":"low"}}'
  fi
  if [[ "$MODE" == "absent_variant" ]]; then
    MODEL_JSON='{{"providerID":"opencode-go","id":"deepseek-v4.1-flash"}}'
  fi
  if [[ "$MODE" == "no_stop" ]]; then FINISH="length"; fi

  printf '{{"info":{{"id":"%s","location":{{"directory":"%s"}},"outcome":"succeeded","time":{{"created":%s,"updated":%s,"idle":%s}}}},"messages":[{{"id":"msg_ast_1","sessionID":"%s","type":"assistant","model":%s,"finish":"%s","time":{{"created":%s,"completed":%s}}}}]}}\n' \
    "$EXP_ID" "$EXP_DIR" "$EXP_CREATED" "$((BASE_TS + 20))" "$((BASE_TS + 30))" "$EXP_ID" "$MODEL_JSON" "$FINISH" "$((BASE_TS + 10))" "$((BASE_TS + 20))"
  exit 0
fi

exit 2
"#,
            log = log_path.display(),
            workdir = workdir_str,
            base_ts = base_ts,
            mode = mode,
        );

        let executable = write_executable_script(
            temp.path(),
            &format!("fake-opencode-neg-{mode}"),
            &script_body,
        );
        let spec = make_opencode_spec(executable, repo_dir.clone(), work_area.clone(), None);
        let report = run_worker(&spec, &workdir)
            .await
            .expect("run_worker report");

        assert_eq!(
            report.status, expected_status,
            "unexpected status for negative metadata mode `{mode}`"
        );
        assert!(
            !report.has_complete_natural_completion_evidence(),
            "negative metadata mode `{mode}` must not report complete natural completion"
        );
        assert!(report.opencode_metadata.is_none());
        assert_eq!(
            report.opencode_metadata_error.as_deref(),
            Some(expected_error_code),
            "unexpected metadata error code for mode `{mode}`"
        );
        assert_eq!(
            read_invocations(&log_path),
            vec![
                "create".to_string(),
                "run".to_string(),
                "get".to_string(),
                "export".to_string(),
            ],
            "mode `{mode}` must never replay inference after metadata failure"
        );
    }
}

#[tokio::test]
async fn stream_conflicts_errors_and_blocks_skip_metadata_export_and_cannot_be_rescued() {
    let temp = tempfile::tempdir().expect("tempdir");
    let (repo_dir, work_area, workdir) = setup_test_env(temp.path());
    let workdir_str = workdir.to_str().expect("utf8 workdir");

    let stream_cases: &[(&str, &str, WorkerRunStatus)] = &[
        (
            "model_conflict",
            r#"{"type":"step_start","sessionID":"ses_stream_01","model":"opencode-go/wrong-model"}"#,
            WorkerRunStatus::ModelMismatch,
        ),
        (
            "effort_conflict",
            r#"{"type":"step_start","sessionID":"ses_stream_01","variant":"low"}"#,
            WorkerRunStatus::EffortMismatch,
        ),
        (
            "session_conflict",
            r#"{"type":"step_start","sessionID":"ses_wrong_999"}"#,
            WorkerRunStatus::SessionMismatch,
        ),
        (
            "provider_error",
            r#"{"type":"error","sessionID":"ses_stream_01","message":"upstream quota exceeded"}"#,
            WorkerRunStatus::ProviderError,
        ),
        (
            "blocked_event",
            r#"{"type":"permission_required","sessionID":"ses_stream_01","summary":"write approval needed"}"#,
            WorkerRunStatus::Blocked,
        ),
        (
            "malformed_line",
            "not-a-valid-json-line",
            WorkerRunStatus::MalformedStream,
        ),
    ];

    for &(case_name, stream_line, expected_status) in stream_cases {
        let base_ts = current_epoch_ms().saturating_add(15_000);
        let log_path = temp
            .path()
            .join(format!("invocations-stream-{case_name}.log"));
        let script_body = format!(
            r#"#!/usr/bin/env bash
set -euo pipefail
LOG="{log}"
WORKDIR="{workdir}"
BASE_TS={base_ts}

if [[ "${{1:-}}" == "api" && "${{2:-}}" == "--standalone" && "${{3:-}}" == "session.create" ]]; then
  echo "create" >> "$LOG"
  printf '{{"data":{{"id":"ses_stream_01","location":{{"directory":"%s"}},"time":{{"created":%s}}}}}}\n' "$WORKDIR" "$BASE_TS"
  exit 0
fi

if [[ "${{1:-}}" == "--standalone" && "${{2:-}}" == "run" ]]; then
  shift
  echo "run" >> "$LOG"
  cat <<'EOF'
{stream_line}
EOF
  exit 0
fi

echo "unexpected_metadata_call:$*" >> "$LOG"
exit 1
"#,
            log = log_path.display(),
            workdir = workdir_str,
            base_ts = base_ts,
            stream_line = stream_line,
        );

        let executable = write_executable_script(
            temp.path(),
            &format!("fake-opencode-stream-{case_name}"),
            &script_body,
        );
        let spec = make_opencode_spec(executable, repo_dir.clone(), work_area.clone(), None);
        let mut report = run_worker(&spec, &workdir)
            .await
            .expect("run_worker stream case");

        assert_eq!(
            report.status, expected_status,
            "unexpected status for stream case `{case_name}`"
        );
        assert!(report.opencode_metadata.is_none());
        assert!(!report.has_complete_natural_completion_evidence());
        assert_eq!(
            read_invocations(&log_path),
            vec!["create".to_string(), "run".to_string()],
            "stream case `{case_name}` must skip session.get and session export"
        );

        // Even if valid metadata is injected into the report and deserialized, contradictory
        // stream evidence remains authoritative and cannot be rescued.
        report.opencode_metadata = Some(OpenCodeMetadataEvidence {
            source: "opencode_sanitized_session_export".to_string(),
            strength: "cli_reported_per_assistant".to_string(),
            session_id: "ses_stream_01".to_string(),
            observed_model: "opencode-go/deepseek-v4.1-flash".to_string(),
            observed_effort: Some("max".to_string()),
            assistant_message_count: 1,
            terminal_message_id: "msg_ast_1".to_string(),
        });
        let roundtripped: threadlane_workers::WorkerExecutionReport =
            serde_json::from_str(&serde_json::to_string(&report).expect("serialize"))
                .expect("deserialize");
        assert!(
            roundtripped.validated_opencode_metadata().is_none(),
            "contradictory stream in `{case_name}` must invalidate serialized metadata"
        );
        assert!(
            !roundtripped.has_complete_natural_completion_evidence(),
            "contradictory stream in `{case_name}` must never be rescued by metadata"
        );
    }
}

#[tokio::test]
async fn allocation_and_metadata_failures_timeouts_overflow_and_cancel_never_replay_or_export_after_cancel(
) {
    let temp = tempfile::tempdir().expect("tempdir");
    let (repo_dir, work_area, workdir) = setup_test_env(temp.path());
    let workdir_str = workdir.to_str().expect("utf8 workdir");

    // 0. Pre-cancelled before session.create performs zero CLI invocations and leaves empty argv.
    {
        let log_path = temp.path().join("precancel.log");
        let script = format!(
            "#!/usr/bin/env bash\necho \"${{3:-$1}}\" >> \"{}\"\nexit 0\n",
            log_path.display()
        );
        let exe = write_executable_script(temp.path(), "precancel", &script);
        let spec = make_opencode_spec(exe, repo_dir.clone(), work_area.clone(), None);
        let (_cancel_tx, mut cancel_rx) = watch::channel(true);
        let report = run_worker_with_cancel(&spec, &workdir, &mut cancel_rx)
            .await
            .expect("report");
        assert_eq!(report.status, WorkerRunStatus::Cancelled);
        assert_eq!(report.opencode_metadata_error.as_deref(), Some("cancelled"));
        assert!(report.allocated_session_id.is_none());
        assert!(
            report.constructed_argv.is_empty(),
            "no inference argv should be recorded when cancelled before allocation"
        );
        assert!(
            read_invocations(&log_path).is_empty(),
            "pre-cancelled run must not spawn session.create or inference"
        );
    }

    // 1. session.create non-zero exit never starts inference and records empty argv.
    {
        let log_path = temp.path().join("create-nonzero.log");
        let script = format!(
            "#!/usr/bin/env bash\necho \"${{3:-$1}}\" >> \"{}\"\nexit 1\n",
            log_path.display()
        );
        let exe = write_executable_script(temp.path(), "create-nonzero", &script);
        let spec = make_opencode_spec(exe, repo_dir.clone(), work_area.clone(), None);
        let report = run_worker(&spec, &workdir).await.expect("report");
        assert_eq!(report.status, WorkerRunStatus::NonZeroExit);
        assert_eq!(
            report.opencode_metadata_error.as_deref(),
            Some("nonzero_exit")
        );
        assert!(report.allocated_session_id.is_none());
        assert!(report.constructed_argv.is_empty());
        assert_eq!(read_invocations(&log_path), vec!["session.create"]);
    }

    // 2. session.create stdout overflow (> 64 KiB) never starts inference and records empty argv.
    {
        let log_path = temp.path().join("create-overflow.log");
        let script = format!(
            "#!/usr/bin/env bash\necho \"${{3:-$1}}\" >> \"{}\"\nhead -c 70000 /dev/zero | tr '\\0' 'a'\nexit 0\n",
            log_path.display()
        );
        let exe = write_executable_script(temp.path(), "create-overflow", &script);
        let spec = make_opencode_spec(exe, repo_dir.clone(), work_area.clone(), None);
        let report = run_worker(&spec, &workdir).await.expect("report");
        assert_eq!(report.status, WorkerRunStatus::OutputOverflow);
        assert_eq!(
            report.opencode_metadata_error.as_deref(),
            Some("output_overflow")
        );
        assert!(report.allocated_session_id.is_none());
        assert!(report.constructed_argv.is_empty());
        assert_eq!(read_invocations(&log_path), vec!["session.create"]);
    }

    // 3. session.create timeout never starts inference and records empty argv.
    {
        let log_path = temp.path().join("create-timeout.log");
        let script = format!(
            "#!/usr/bin/env bash\necho \"${{3:-$1}}\" >> \"{}\"\nsleep 5\n",
            log_path.display()
        );
        let exe = write_executable_script(temp.path(), "create-timeout", &script);
        let mut spec = make_opencode_spec(exe, repo_dir.clone(), work_area.clone(), None);
        spec.budgets.timeout_ms = 120;
        let report = run_worker(&spec, &workdir).await.expect("report");
        assert_eq!(report.status, WorkerRunStatus::TimedOut);
        assert_eq!(report.opencode_metadata_error.as_deref(), Some("timed_out"));
        assert!(report.allocated_session_id.is_none());
        assert!(report.constructed_argv.is_empty());
        assert_eq!(read_invocations(&log_path), vec!["session.create"]);
    }

    // 4. Shared overall timeout budget: session.create consumes part of the budget, and the model
    // run consumes only the remaining budget rather than restarting a full timeout window.
    {
        let base_ts = current_epoch_ms().saturating_add(15_000);
        let log_path = temp.path().join("shared-budget.log");
        let script = format!(
            r#"#!/usr/bin/env bash
set -euo pipefail
LOG="{log}"
WORKDIR="{workdir}"
BASE_TS={base_ts}

if [[ "${{1:-}}" == "api" && "${{2:-}}" == "--standalone" && "${{3:-}}" == "session.create" ]]; then
  echo "create" >> "$LOG"
  sleep 0.16
  printf '{{"data":{{"id":"ses_budget_01","location":{{"directory":"%s"}},"time":{{"created":%s}}}}}}\n' "$WORKDIR" "$BASE_TS"
  exit 0
fi

if [[ "${{1:-}}" == "--standalone" && "${{2:-}}" == "run" ]]; then
  shift
  echo "run" >> "$LOG"
  sleep 0.20
  printf '{{"type":"text","sessionID":"ses_budget_01","part":{{"type":"text","text":"late"}}}}\n'
  exit 0
fi

echo "unexpected_after_timeout:$*" >> "$LOG"
exit 1
"#,
            log = log_path.display(),
            workdir = workdir_str,
            base_ts = base_ts,
        );
        let exe = write_executable_script(temp.path(), "shared-budget", &script);
        let mut spec = make_opencode_spec(exe, repo_dir.clone(), work_area.clone(), None);
        spec.budgets.timeout_ms = 260;
        let report = run_worker(&spec, &workdir).await.expect("report");
        assert_eq!(report.status, WorkerRunStatus::TimedOut);
        assert_eq!(
            report.allocated_session_id.as_deref(),
            Some("ses_budget_01")
        );
        assert!(report.opencode_metadata.is_none());
        assert_eq!(
            read_invocations(&log_path),
            vec!["create".to_string(), "run".to_string()],
            "shared deadline expiry during run must not invoke metadata collection"
        );
    }

    // 5. Cancellation during session.get stops immediately and never runs session export or replays inference.
    {
        let base_ts = current_epoch_ms().saturating_add(15_000);
        let log_path = temp.path().join("cancel-during-get.log");
        let get_started_path = temp.path().join("get.started");
        let script = format!(
            r#"#!/usr/bin/env bash
set -euo pipefail
LOG="{log}"
WORKDIR="{workdir}"
BASE_TS={base_ts}

if [[ "${{1:-}}" == "api" && "${{2:-}}" == "--standalone" && "${{3:-}}" == "session.create" ]]; then
  echo "create" >> "$LOG"
  printf '{{"data":{{"id":"ses_cancel_01","location":{{"directory":"%s"}},"time":{{"created":%s}}}}}}\n' "$WORKDIR" "$BASE_TS"
  exit 0
fi

if [[ "${{1:-}}" == "--standalone" && "${{2:-}}" == "run" ]]; then
  shift
  echo "run" >> "$LOG"
  printf '{{"type":"text","sessionID":"ses_cancel_01","part":{{"type":"text","text":"done"}}}}\n'
  exit 0
fi

if [[ "${{1:-}}" == "api" && "${{2:-}}" == "--standalone" && "${{3:-}}" == "session.get" ]]; then
  echo "get" >> "$LOG"
  touch "{started}"
  sleep 5
  exit 0
fi

if [[ "${{1:-}}" == "session" && "${{2:-}}" == "export" ]]; then
  echo "export" >> "$LOG"
  exit 0
fi
exit 2
"#,
            log = log_path.display(),
            workdir = workdir_str,
            base_ts = base_ts,
            started = get_started_path.display(),
        );
        let exe = write_executable_script(temp.path(), "cancel-during-get", &script);
        let spec = make_opencode_spec(exe, repo_dir.clone(), work_area.clone(), None);
        let (cancel_tx, mut cancel_rx) = watch::channel(false);

        let cancel_trigger = tokio::spawn(async move {
            for _ in 0..200 {
                if get_started_path.exists() {
                    let _ = cancel_tx.send(true);
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let _ = cancel_tx.send(true);
        });

        let report = run_worker_with_cancel(&spec, &workdir, &mut cancel_rx)
            .await
            .expect("report");
        cancel_trigger.await.expect("join cancel trigger");

        assert_eq!(report.status, WorkerRunStatus::Cancelled);
        assert_eq!(report.opencode_metadata_error.as_deref(), Some("cancelled"));
        assert!(report.opencode_metadata.is_none());
        assert!(!report.has_complete_natural_completion_evidence());
        assert_eq!(
            read_invocations(&log_path),
            vec!["create".to_string(), "run".to_string(), "get".to_string()],
            "session export must never run after cancellation"
        );
    }
}

#[tokio::test]
async fn explicit_preexisting_session_id_never_invokes_metadata_commands() {
    let temp = tempfile::tempdir().expect("tempdir");
    let (repo_dir, work_area, workdir) = setup_test_env(temp.path());
    let log_path = temp.path().join("preexisting.log");

    let script = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
LOG="{log}"

if [[ "${{1:-}}" == "--standalone" && "${{2:-}}" == "run" ]]; then
  shift
  SID=""
  while [[ $# -gt 0 ]]; do
    if [[ "$1" == "--session" ]]; then
      SID="$2"
      shift 2
    else
      shift
    fi
  done
  echo "run:$SID" >> "$LOG"
  printf '{{"type":"step_start","sessionID":"%s","model":"opencode-go/deepseek-v4.1-flash","variant":"max"}}\n' "$SID"
  printf '{{"type":"step_finish","sessionID":"%s","part":{{"type":"step-finish","reason":"stop"}}}}\n' "$SID"
  exit 0
fi

echo "forbidden_metadata:$*" >> "$LOG"
exit 1
"#,
        log = log_path.display()
    );

    let exe = write_executable_script(temp.path(), "preexisting-opencode", &script);
    let spec = make_opencode_spec(
        exe,
        repo_dir,
        work_area,
        Some("ses_preexisting_777".to_string()),
    );

    let report = run_worker(&spec, &workdir).await.expect("run_worker");
    assert_eq!(report.status, WorkerRunStatus::Succeeded);
    assert_eq!(
        report.requested_session_id.as_deref(),
        Some("ses_preexisting_777")
    );
    assert!(report.allocated_session_id.is_none());
    assert!(report.opencode_metadata.is_none());
    assert!(report.opencode_metadata_error.is_none());
    assert!(report.has_complete_natural_completion_evidence());
    assert_eq!(
        read_invocations(&log_path),
        vec!["run:ses_preexisting_777".to_string()],
        "explicit preexisting session_id must never invoke session.create, session.get, or session export"
    );
}
