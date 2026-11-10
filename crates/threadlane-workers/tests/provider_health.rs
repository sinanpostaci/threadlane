#![forbid(unsafe_code)]

use threadlane_workers::antigravity_diagnostics::{
    AntigravityDiagnosticParser, MAX_DIAGNOSTIC_BYTES, MAX_DIAGNOSTIC_LINE_BYTES,
};
use threadlane_workers::provider_health::{
    ProviderDiagnosticAvailability as Availability,
    ProviderDiagnosticUnavailableReason as Unavailable, ProviderHealthEvidenceClass,
    ProviderHealthRecovery, MAX_PROVIDER_HEALTH_RECORDS,
};

const FIXTURE: &str = include_str!("fixtures/agy-stream-idle-sanitized.log");

#[test]
fn five_internal_retries_use_fake_monotonic_time_and_preserve_history() {
    let mut parser = AntigravityDiagnosticParser::new();
    let mut offset = 0;
    for (i, line) in FIXTURE.split_inclusive('\n').enumerate() {
        let now = (i as u64 + 1) * 300_000;
        parser.ingest_at(offset, &line.as_bytes()[..17], now);
        parser.ingest_at(offset + 17, &line.as_bytes()[17..], now);
        let snapshot = parser.snapshot();
        assert_eq!(snapshot.retry_count, i as u32 + 1);
        assert_eq!(snapshot.first_failure.unwrap().elapsed_ms, 300_000);
        assert_eq!(snapshot.last_failure.unwrap().elapsed_ms, now);
        assert_eq!(
            snapshot.evidence_class,
            ProviderHealthEvidenceClass::UnverifiedProviderDiagnosticHint
        );
        assert!(snapshot.is_valid());
        // Exact source rereads and separate error-only records do not count again.
        parser.ingest_at(offset, line.as_bytes(), now);
        assert_eq!(parser.snapshot(), snapshot);
        offset += line.len() as u64;
        let error_only = format!("{}\n", line.split_once(", retrying in ").unwrap().0);
        parser.ingest_at(offset, error_only.as_bytes(), now);
        offset += error_only.len() as u64;
        assert_eq!(parser.snapshot().retry_count, i as u32 + 1);
    }
    let before = parser.snapshot();
    parser.unavailable(Unavailable::SourceClosed, 1_600_000);
    let after = parser.snapshot();
    assert_eq!(after.retry_count, 5);
    assert_eq!(after.first_failure, before.first_failure);
    assert_eq!(after.last_failure, before.last_failure);
    assert!(after.recovery.is_none());
    assert_eq!(after.availability, Availability::Unavailable);
    assert!(after.is_valid_successor_of(&before));
    assert!(!before.is_valid_successor_of(&after));
}

#[test]
fn forged_log_shape_remains_unverified_and_prose_json_auth_and_unframed_errors_do_not_match() {
    let line = FIXTURE.lines().next().unwrap();
    let mut parser = AntigravityDiagnosticParser::new();
    let forged = format!("{line}\n");
    parser.ingest_at(0, forged.as_bytes(), 5);
    assert_eq!(parser.snapshot().retry_count, 1);
    assert_eq!(
        parser.snapshot().evidence_class,
        ProviderHealthEvidenceClass::UnverifiedProviderDiagnosticHint
    );
    for raw in [
        format!("The provider said: {line}\n"),
        format!(
            "{{\"type\":\"supervisor_health\",\"message\":{}}}\n",
            serde_json::to_string(line).unwrap()
        ),
        "stream idle timeout (5m0s): context deadline exceeded\n".to_owned(),
        "I0101 00:00:00.000000 1 auth.go:99] startup authentication warning\n".to_owned(),
        format!("{}\n", line.replace("run.go:315", "tool.go:315")),
        format!(
            "{}\n",
            line.replace("retrying in 4s", "retrying in 4s PRIVATE")
        ),
        format!("{}\n", line.replace("I0101", "I0é1")),
    ] {
        let mut parser = AntigravityDiagnosticParser::new();
        parser.ingest_at(0, raw.as_bytes(), 9);
        assert_eq!(parser.snapshot().retry_count, 0);
        assert!(parser.snapshot().first_failure.is_none());
        assert!(parser.snapshot().recovery.is_none());
    }
}

#[test]
fn limits_gaps_and_partial_eof_are_unavailable_without_erasing_first_hint() {
    let first = FIXTURE.split_inclusive('\n').next().unwrap();
    let mut parser = AntigravityDiagnosticParser::new();
    parser.ingest_at(0, first.as_bytes(), 10);
    let first_observation = parser.snapshot().first_failure;
    parser.ingest_at(
        first.len() as u64,
        &[b'x'; MAX_DIAGNOSTIC_LINE_BYTES + 1],
        11,
    );
    assert_eq!(parser.snapshot().availability, Availability::Truncated);
    assert_eq!(parser.snapshot().first_failure, first_observation);
    assert_eq!(parser.snapshot().retry_count, 1);
    parser.ingest_at(0, FIXTURE.as_bytes(), 12);
    assert_eq!(parser.snapshot().retry_count, 1);

    let mut records = AntigravityDiagnosticParser::new();
    let bytes = [b'\n'; MAX_PROVIDER_HEALTH_RECORDS as usize];
    assert!(bytes.len() < MAX_DIAGNOSTIC_BYTES);
    records.ingest_at(0, &bytes, 20);
    assert_eq!(records.snapshot().availability, Availability::Truncated);

    let mut total = AntigravityDiagnosticParser::new();
    let mut line = vec![b'x'; 1023];
    line.push(b'\n');
    for i in 0..(MAX_DIAGNOSTIC_BYTES / line.len()) {
        total.ingest_at((i * line.len()) as u64, &line, 21);
    }
    assert_eq!(total.snapshot().availability, Availability::Truncated);

    let mut gap = AntigravityDiagnosticParser::new();
    gap.ingest_at(1, first.as_bytes(), 30);
    assert_eq!(
        gap.snapshot().unavailable_reason,
        Some(Unavailable::SourceLost)
    );
    assert_eq!(gap.snapshot().retry_count, 0);

    let mut partial = AntigravityDiagnosticParser::new();
    partial.ingest_at(0, first.trim_end().as_bytes(), 40);
    partial.unavailable(Unavailable::SourceClosed, 41);
    assert_eq!(partial.snapshot().retry_count, 0);
    assert_eq!(partial.snapshot().availability, Availability::Unavailable);
}

#[test]
fn overlapping_rereads_and_regressing_clock_cannot_duplicate_or_rewind() {
    let mut parser = AntigravityDiagnosticParser::new();
    let first = FIXTURE.split_inclusive('\n').next().unwrap();
    parser.ingest_at(0, &first.as_bytes()[..20], 100);
    parser.ingest_at(0, first.as_bytes(), 100);
    let before = parser.snapshot();
    let second = FIXTURE.split_inclusive('\n').nth(1).unwrap();
    parser.ingest_at(first.len() as u64, second.as_bytes(), 1);
    let after = parser.snapshot();
    assert_eq!(after.retry_count, 2);
    assert_eq!(after.elapsed_ms, 100);
    assert_eq!(after.first_failure, before.first_failure);
    assert!(after.is_valid_successor_of(&before));
    let mut tampered = after.clone();
    tampered.first_failure.as_mut().unwrap().elapsed_ms = 99;
    assert!(!tampered.is_valid_successor_of(&after));
    let mut json = serde_json::to_value(after).unwrap();
    json["diagnostic_path"] = serde_json::json!("/private/sentinel");
    assert!(serde_json::from_value::<threadlane_workers::ProviderHealthSnapshot>(json).is_err());
}

#[test]
fn distinct_source_positions_preserve_retries_when_attempt_numbers_repeat() {
    let line = FIXTURE.split_inclusive('\n').next().unwrap();
    let mut parser = AntigravityDiagnosticParser::new();
    parser.ingest_at(0, line.as_bytes(), 10);
    parser.ingest_at(0, line.as_bytes(), 11);
    assert_eq!(parser.snapshot().retry_count, 1);
    parser.ingest_at(line.len() as u64, line.as_bytes(), 20);
    let snapshot = parser.snapshot();
    assert_eq!(snapshot.retry_count, 2);
    assert_eq!(snapshot.retry_high_water, Some(1));
    assert_eq!(snapshot.first_failure.unwrap().elapsed_ms, 10);
    assert_eq!(snapshot.last_failure.unwrap().elapsed_ms, 20);
    assert_eq!(
        snapshot.evidence_class,
        ProviderHealthEvidenceClass::UnverifiedProviderDiagnosticHint
    );
}

#[cfg(target_os = "linux")]
mod supervised {
    use super::*;
    use std::collections::BTreeSet;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::time::Duration;
    use tempfile::TempDir;
    use threadlane_workers::{
        run_worker_with_health_observer, ExecutionBudgets, NormalizedWorkerEvent,
        OutputOverflowKind, WorkerExecutionReport, WorkerFilePermissions, WorkerProvider,
        WorkerRunStatus, WorkerSpec,
    };
    use tokio::sync::{mpsc, watch};

    struct Fake {
        _root: TempDir,
        spec: WorkerSpec,
    }

    impl Fake {
        fn new(body: &str) -> Self {
            let root = tempfile::tempdir().unwrap();
            let path = fs::canonicalize(root.path()).unwrap();
            let repository_path = path.join("repo");
            fs::create_dir(&repository_path).unwrap();
            let executable = path.join("worker");
            let script = format!(
                "#!/bin/sh\nset -eu\nlog=''\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = '--log-file' ]; then log=$2; shift 2; else shift; fi\ndone\nprintf '%s\\n' '{{\"type\":\"init\",\"model\":\"gemini-4-argon-high\",\"effort\":\"high\"}}'\n{body}\nprintf '%s\\n' '{{\"type\":\"result\",\"subtype\":\"success\",\"model\":\"gemini-4-argon-high\",\"effort\":\"high\"}}'\n"
            );
            fs::write(&executable, script).unwrap();
            fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
            Self {
                _root: root,
                spec: WorkerSpec {
                    provider: WorkerProvider::Antigravity,
                    executable,
                    model: "gemini-4-argon-high".to_owned(),
                    effort: Some("high".to_owned()),
                    allowed_models: BTreeSet::new(),
                    session_id: None,
                    standalone: false,
                    repository_path,
                    base_commit: "HEAD".to_owned(),
                    work_area_root: path.join("work"),
                    allowed_write_paths: BTreeSet::from(["result.txt".to_owned()]),
                    prompt: "PRIVATE_PROMPT_SENTINEL".to_owned(),
                    file_permissions: WorkerFilePermissions::Inherit,
                    budgets: ExecutionBudgets {
                        timeout_ms: 3_000,
                        ..ExecutionBudgets::default()
                    },
                },
            }
        }

        fn hints() -> String {
            let args = FIXTURE
                .lines()
                .map(|line| format!("'{line}'"))
                .collect::<Vec<_>>()
                .join(" ");
            format!("printf '%s\\n' {args} > \"$log\"\nprintf '%s\\n' 'PRIVATE_DIAGNOSTIC_SENTINEL' > \"$log\"\n")
        }
    }

    #[tokio::test]
    async fn legacy_calls_preserve_exact_argv_and_omit_unrequested_diagnostics() {
        let fake = Fake::new("sleep 0.03");
        let first = threadlane_workers::run_worker(&fake.spec, &fake.spec.repository_path)
            .await
            .unwrap();
        let second = threadlane_workers::run_worker(&fake.spec, &fake.spec.repository_path)
            .await
            .unwrap();
        assert_eq!(first.constructed_argv, second.constructed_argv);
        assert!(!first.constructed_argv.iter().any(|arg| arg == "--log-file"));
        assert!(first.provider_health.is_none());
        assert!(!serde_json::to_string(&first)
            .unwrap()
            .contains("provider_health"));

        let (_cancel, mut cancelled) = watch::channel(false);
        let (observer, _) = watch::channel(None);
        let observed = run_worker_with_health_observer(
            &fake.spec,
            &fake.spec.repository_path,
            &mut cancelled,
            None,
            Some(observer),
        )
        .await
        .unwrap();
        assert!(observed
            .constructed_argv
            .iter()
            .any(|arg| arg == "--log-file"));
        assert!(observed.provider_health.is_some());
        assert_eq!(first.requested_model, observed.requested_model);
        assert_eq!(first.status, observed.status);
    }

    #[tokio::test]
    async fn closed_live_consumers_preserve_final_diagnostics_and_execution_evidence() {
        let fake = Fake::new(&format!("{}\nsleep 0.15", Fake::hints()));
        let (_cancel, mut cancelled) = watch::channel(false);
        let (progress_tx, progress_rx) = mpsc::channel::<NormalizedWorkerEvent>(1);
        let (health_tx, health_rx) = watch::channel(None);
        drop(progress_rx);
        drop(health_rx);
        let report = run_worker_with_health_observer(
            &fake.spec,
            &fake.spec.repository_path,
            &mut cancelled,
            Some(progress_tx),
            Some(health_tx),
        )
        .await
        .unwrap();
        assert_eq!(report.status, WorkerRunStatus::Succeeded);
        assert!(report.dropped_live_events > 0);
        let health = report.provider_health.unwrap();
        assert_eq!(health.retry_count, 5);
        assert_eq!(
            health.evidence_class,
            ProviderHealthEvidenceClass::UnverifiedProviderDiagnosticHint
        );
        assert!(
            health.first_failure.unwrap().update_sequence
                < health.last_failure.unwrap().update_sequence
        );
        assert_eq!(
            health.recovery,
            Some(ProviderHealthRecovery::VerifiedTerminalOutcome)
        );
    }

    #[tokio::test]
    async fn full_progress_channel_keeps_cumulative_first_last_and_five_retries_in_one_invocation()
    {
        let fake = Fake::new(&format!(
            "{}\ni=0\nwhile [ \"$i\" -lt 4000 ]; do printf '%s\\n' '{{\"type\":\"tool_use\",\"name\":\"read\"}}'; i=$((i + 1)); done\nsleep 0.15",
            Fake::hints()
        ));
        let (_cancel, mut cancelled) = watch::channel(false);
        let (progress_tx, _held_progress) = mpsc::channel::<NormalizedWorkerEvent>(1);
        let (health_tx, mut health_rx) = watch::channel(None);
        let run = run_worker_with_health_observer(
            &fake.spec,
            &fake.spec.repository_path,
            &mut cancelled,
            Some(progress_tx),
            Some(health_tx),
        );
        tokio::pin!(run);
        let report = loop {
            tokio::select! {
                report = &mut run => break report.unwrap(),
                changed = health_rx.changed() => {
                    changed.unwrap();
                    let snapshot = health_rx.borrow_and_update().clone().unwrap();
                    assert!(snapshot.is_valid());
                }
            }
        };
        assert_eq!(report.status, WorkerRunStatus::Succeeded);
        assert!(report.dropped_live_events > 0);
        let health = report.provider_health.as_ref().unwrap();
        assert_eq!(health.retry_count, 5);
        assert!(
            health.first_failure.unwrap().update_sequence
                < health.last_failure.unwrap().update_sequence
        );
        assert_eq!(
            health.recovery,
            Some(ProviderHealthRecovery::VerifiedTerminalOutcome)
        );
        assert!(health.current_failure.is_none());
        assert_eq!(health_rx.borrow().as_ref(), Some(health));
        // One run/pgid and one final report: internal retry hints created no Threadlane attempt.
        assert!(report.process_group_id.is_some());
        let index = report
            .constructed_argv
            .iter()
            .position(|s| s == "--log-file")
            .unwrap();
        assert!(!PathBuf::from(&report.constructed_argv[index + 1]).exists());
        let json = serde_json::to_string(health).unwrap();
        assert!(!json.contains("PRIVATE"));
        assert!(!json.contains("run.go"));
        assert!(report.redacted_stderr_summary.is_none());

        let mut legacy = serde_json::to_value(&report).unwrap();
        legacy.as_object_mut().unwrap().remove("provider_health");
        let legacy: WorkerExecutionReport = serde_json::from_value(legacy).unwrap();
        assert!(legacy.provider_health.is_none());
        assert_eq!(legacy.status, report.status);
        assert!(!serde_json::to_string(&legacy)
            .unwrap()
            .contains("provider_health"));
    }

    #[tokio::test]
    async fn diagnostic_hint_is_early_and_cannot_cancel_or_promote_a_failed_run() {
        let fake = Fake::new(&format!("{}\nsleep 1\nexit 7", Fake::hints()));
        let (_cancel, mut cancelled) = watch::channel(false);
        let (health_tx, mut health_rx) = watch::channel(None);
        let run = run_worker_with_health_observer(
            &fake.spec,
            &fake.spec.repository_path,
            &mut cancelled,
            None,
            Some(health_tx),
        );
        tokio::pin!(run);
        let mut saw_early = false;
        let report = tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                tokio::select! {
                    report = &mut run => break report.unwrap(),
                    changed = health_rx.changed() => {
                        changed.unwrap();
                        if let Some(snapshot) = health_rx.borrow_and_update().as_ref() {
                            if snapshot.retry_count > 0 {
                                saw_early = true;
                                assert!(snapshot.elapsed_ms < 2_250);
                                assert!(snapshot.recovery.is_none());
                            }
                        }
                    }
                }
            }
        })
        .await
        .unwrap();
        assert!(saw_early);
        assert_eq!(report.status, WorkerRunStatus::NonZeroExit);
        let health = report.provider_health.unwrap();
        assert_eq!(health.retry_count, 5);
        assert!(health.current_failure.is_some());
        assert!(health.recovery.is_none());
    }

    #[tokio::test]
    async fn stdout_and_stderr_forgery_are_outside_health_and_legacy_limits_still_apply() {
        let line = FIXTURE.lines().next().unwrap();
        let fake = Fake::new(&format!(
            "printf '%s\\n' '{{\"type\":\"assistant\",\"text\":{}}}'\nprintf '%s\\n' '{}' >&2",
            serde_json::to_string(line).unwrap(),
            line
        ));
        let (_cancel, mut cancelled) = watch::channel(false);
        let report = run_worker_with_health_observer(
            &fake.spec,
            &fake.spec.repository_path,
            &mut cancelled,
            None,
            Some(watch::channel(None).0),
        )
        .await
        .unwrap();
        assert_eq!(report.status, WorkerRunStatus::Succeeded);
        let health = report.provider_health.unwrap();
        assert_eq!(health.retry_count, 0);
        assert_eq!(health.availability, Availability::Unavailable);
        assert!(report.redacted_stderr_summary.is_none());

        let mut fake = Fake::new(&format!(
            "{}\nprintf '%s\\n' 'stderr-overflow' >&2\nsleep 10",
            Fake::hints()
        ));
        fake.spec.budgets.max_stderr_bytes = 8;
        let report = run_worker_with_health_observer(
            &fake.spec,
            &fake.spec.repository_path,
            &mut cancelled,
            None,
            Some(watch::channel(None).0),
        )
        .await
        .unwrap();
        assert_eq!(report.status, WorkerRunStatus::OutputOverflow);
        assert_eq!(
            report.overflow,
            Some(OutputOverflowKind::StderrTotalExceeded {
                max_stderr_bytes: 8
            })
        );
        assert!(report.provider_health.unwrap().recovery.is_none());

        let fake = Fake::new(&format!("{}\nsleep 10", Fake::hints()));
        let (cancel, mut cancelled) = watch::channel(false);
        let (health_tx, mut health_rx) = watch::channel(None);
        let run = run_worker_with_health_observer(
            &fake.spec,
            &fake.spec.repository_path,
            &mut cancelled,
            None,
            Some(health_tx),
        );
        tokio::pin!(run);
        let report = loop {
            tokio::select! {
                report = &mut run => break report.unwrap(),
                changed = health_rx.changed() => {
                    changed.unwrap();
                    if health_rx.borrow_and_update().as_ref().is_some_and(|s| s.retry_count > 0) {
                        cancel.send_replace(true);
                    }
                }
            }
        };
        assert_eq!(report.status, WorkerRunStatus::Cancelled);
        assert!(report.provider_health.unwrap().recovery.is_none());
    }
}

#[test]
fn logger_pid_padding_is_narrow_and_does_not_normalize_untrusted_prose() {
    let line = FIXTURE.split_inclusive('\n').next().unwrap();
    for padded in ["1", "      1"] {
        let line = line.replace("      1 run.go", &format!("{padded} run.go"));
        let mut parser = AntigravityDiagnosticParser::new();
        parser.ingest_at(0, line.as_bytes(), 10);
        assert_eq!(parser.snapshot().retry_count, 1);
    }
    for bad in [
        format!(" {line}"),
        line.replace("      1", "\t1"),
        line.replace("1 run.go", "1  run.go"),
    ] {
        let mut parser = AntigravityDiagnosticParser::new();
        parser.ingest_at(0, bad.as_bytes(), 10);
        assert_eq!(parser.snapshot().retry_count, 0);
    }
}

#[test]
fn retry_ordinal_high_water_does_not_count_missing_reports_or_reset_on_lower_ordinals() {
    let first = FIXTURE.split_inclusive('\n').next().unwrap();
    let seventh = first.replace("attempt 1 failed", "attempt 7 failed");
    let second = first.replace("attempt 1 failed", "attempt 2 failed");
    let mut parser = AntigravityDiagnosticParser::new();
    parser.ingest_at(0, seventh.as_bytes(), 100);
    parser.ingest_at(seventh.len() as u64, second.as_bytes(), 200);
    assert_eq!(parser.snapshot().retry_count, 2);
    assert_eq!(parser.snapshot().retry_high_water, Some(7));
    assert!(parser.snapshot().is_valid());
}

#[test]
fn qualified_logger_terminal_period_is_a_bounded_unverified_match() {
    let mut parser = AntigravityDiagnosticParser::new();
    let line = b"I0101 00:00:01.000000 1 run.go:315] Run: attempt 1 failed (stream reading error: stream idle timeout (5m0s): context deadline exceeded), retrying in 4s.\n";
    parser.ingest_at(0, line, 1);
    let snapshot = parser.snapshot();
    assert_eq!(snapshot.retry_count, 1);
    assert_eq!(
        snapshot.evidence_class,
        ProviderHealthEvidenceClass::UnverifiedProviderDiagnosticHint
    );
}

#[test]
fn measured_startup_line_does_not_hide_a_later_padded_unverified_retry() {
    let mut parser = AntigravityDiagnosticParser::new();
    // Sanitized size-only reproduction; no real provider log is retained.
    let mut startup = vec![b'x'; 4173];
    startup.push(b'\n');
    parser.ingest_at(0, &startup, 1);
    assert_eq!(parser.snapshot().availability, Availability::Available);
    assert_eq!(parser.snapshot().retry_count, 0);
    let retry = b"I0101 00:00:01.000000     1 run.go:315] Run: attempt 1 failed (stream reading error: stream idle timeout (5m0s): context deadline exceeded), retrying in 4s.\n";
    let offset = startup.len() as u64;
    for (index, chunk) in retry.chunks(7).enumerate() {
        parser.ingest_at(offset + (index * 7) as u64, chunk, 300_000);
    }
    let snapshot = parser.snapshot();
    assert_eq!(snapshot.retry_count, 1);
    assert_eq!(snapshot.retry_high_water, Some(1));
    assert_eq!(snapshot.availability, Availability::Available);
    assert_eq!(
        snapshot.evidence_class,
        ProviderHealthEvidenceClass::UnverifiedProviderDiagnosticHint
    );
    assert!(snapshot.is_valid());
    parser.ingest_at(offset, retry, 300_001);
    assert_eq!(parser.snapshot(), snapshot);
}
