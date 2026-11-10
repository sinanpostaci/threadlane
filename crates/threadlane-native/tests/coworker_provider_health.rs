#![cfg(unix)]
//! Scope and trust contract regressions; real provider calls are never made.
use std::fs;
use std::os::unix::fs::PermissionsExt;

use threadlane_native::coworker_workflow::{CoworkerJobOwnershipKey, CoworkerJobOwnershipStore};
use threadlane_native::provider_health::{
    supervise_health, ProviderHealthInvocation, ScopedProviderHealth,
};
use threadlane_workers::antigravity_diagnostics::AntigravityDiagnosticParser;
use threadlane_workers::provider_health::{ProviderHealthEvidenceClass, ProviderHealthSnapshot};
use threadlane_workflow::WorkflowRole;

fn forged_hint(attempt: u32, elapsed: u64) -> ProviderHealthSnapshot {
    let mut parser = AntigravityDiagnosticParser::new();
    let text = format!(
        "I0101 00:00:01.000000 1 run.go:315] Run: attempt {attempt} failed (stream reading error: stream idle timeout (5m0s): context deadline exceeded), retrying in 4s.\n"
    );
    parser.ingest_at(0, text.as_bytes(), elapsed);
    parser.snapshot()
}

#[test]
fn scope_successors_reject_wrong_owner_reservation_subject_and_attempt_without_authority() {
    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let store = CoworkerJobOwnershipStore::new(root.path()).unwrap();
    let key = CoworkerJobOwnershipKey::new(
        "bnd_health",
        "550e8400-e29b-41d4-a716-446655440000",
        "job_health",
    )
    .unwrap();
    let guard = store
        .try_acquire_job(&key, 1)
        .unwrap()
        .into_guard()
        .unwrap();
    let record = ScopedProviderHealth {
        ownership_key: key.clone(),
        owner: guard.validate_before_effect(&key).unwrap().clone(),
        lease_epoch: Some(1),
        invocation: ProviderHealthInvocation::Controller {
            approval_id: "apr_health".into(),
            envelope_digest: "a".repeat(64),
            effect_id: "eff_health".into(),
            subject_digest: "b".repeat(64),
            task_id: Some("task_health".into()),
            attempt_no: Some(2),
            external_correlation_id: None,
            role: WorkflowRole::Reviewer,
        },
        snapshot: forged_hint(7, 10),
        terminal_observed: false,
    };
    record.validate().unwrap();
    assert_eq!(record.snapshot.retry_count, 1);
    assert_eq!(record.snapshot.retry_high_water, Some(7));
    assert_eq!(
        record.snapshot.evidence_class,
        ProviderHealthEvidenceClass::UnverifiedProviderDiagnosticHint
    );
    assert!(!record.owner.grants_execution_authority);
    let mut terminal = record.clone();
    terminal.terminal_observed = true;
    assert!(terminal.is_valid_successor_of(&record));
    assert!(!record.is_valid_successor_of(&terminal));

    // Ending finite coverage retains all hint history even while the invocation is nonterminal.
    use threadlane_workers::provider_health::ProviderDiagnosticUnavailableReason as Reason;
    for reason in [
        Reason::SourceClosed,
        Reason::SourceLost,
        Reason::ResourceLimit,
        Reason::CaptureSetupFailed,
        Reason::UnsupportedProvider,
    ] {
        let mut parser = AntigravityDiagnosticParser::new();
        let text = b"I0101 00:00:01.000000 1 run.go:315] Run: attempt 7 failed (stream reading error: stream idle timeout (5m0s): context deadline exceeded), retrying in 4s.\n";
        parser.ingest_at(0, text, 10);
        parser.unavailable(reason, 11);
        let mut history = record.clone();
        history.snapshot = parser.snapshot();
        history.validate().unwrap();
        assert!(history.is_valid_successor_of(&record));
        assert!(!history.terminal_observed);
        assert_eq!(history.snapshot.unavailable_reason, Some(reason));
        assert_eq!(
            history.snapshot.first_failure,
            record.snapshot.first_failure
        );
        assert_eq!(history.snapshot.last_failure, record.snapshot.last_failure);
        assert_eq!(
            history.snapshot.current_failure,
            record.snapshot.current_failure
        );
        assert_eq!(history.snapshot.retry_count, record.snapshot.retry_count);
        assert_eq!(
            history.snapshot.retry_high_water,
            record.snapshot.retry_high_water
        );
        assert!(history.snapshot.recovery.is_none());
    }

    for field in [
        "effect_id",
        "subject_digest",
        "task_id",
        "attempt_no",
        "approval_id",
        "envelope_digest",
    ] {
        let mut value = serde_json::to_value(&record).unwrap();
        value["invocation"][field] = match field {
            "attempt_no" => serde_json::json!(3),
            "subject_digest" | "envelope_digest" => serde_json::json!("f".repeat(64)),
            _ => serde_json::json!("wrong_identity"),
        };
        let wrong: ScopedProviderHealth = serde_json::from_value(value).unwrap();
        assert!(!wrong.is_valid_successor_of(&record), "{field}");
    }
    let mut wrong_epoch = record.clone();
    wrong_epoch.lease_epoch = Some(2);
    assert!(!wrong_epoch.is_valid_successor_of(&record));
    let mut wrong_key = record.clone();
    wrong_key.ownership_key.job_id = "another_job".into();
    assert!(wrong_key.validate().is_err());

    let mut planning = record.clone();
    planning.lease_epoch = None;
    planning.invocation = ProviderHealthInvocation::Planning {
        grant_id: "pgr_health".into(),
        reservation_id: "prs_health".into(),
        request_key: "psr_health".into(),
        context_digest: "c".repeat(64),
        role: WorkflowRole::Architect,
    };
    planning.validate().unwrap();
    let mut wrong_reservation = planning.clone();
    if let ProviderHealthInvocation::Planning { reservation_id, .. } =
        &mut wrong_reservation.invocation
    {
        *reservation_id = "prs_other".into();
    }
    assert!(!wrong_reservation.is_valid_successor_of(&planning));

    // Logger-shaped text has no fields capable of granting lifecycle or execution authority.
    let mut injected = serde_json::to_value(record).unwrap();
    injected["worker_status"] = serde_json::json!("failed");
    assert!(serde_json::from_value::<ScopedProviderHealth>(injected).is_err());
}

#[tokio::test]
async fn direct_unowned_supervision_retains_final_report_without_inventing_persistence_or_callbacks(
) {
    let terminal = forged_hint(5, 50);
    let expected = terminal.clone();
    let outcome = supervise_health(
        None,
        |observer| async move {
            observer.send_replace(Some(terminal.clone()));
            tokio::task::yield_now().await;
            // A hint cannot override the independent non-success result.
            Ok::<_, ()>((false, terminal))
        },
        |result| Some(&result.1),
    )
    .await
    .unwrap();
    assert!(!outcome.0);
    assert_eq!(outcome.1, expected);
    assert_eq!(
        outcome.1.evidence_class,
        ProviderHealthEvidenceClass::UnverifiedProviderDiagnosticHint
    );
}

#[test]
fn opt_in_fake_role_fixture_emits_one_recognized_unverified_line_and_no_stdout_hint() {
    use std::process::Command;

    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/support/fake-coworker-roles.mjs");
    let diagnostic = root.path().join("diagnostic.log");

    // The opt-in gate must be required even when the diagnostic path is supplied.
    let ordinary = Command::new("node")
        .arg(&fixture)
        .arg(root.path())
        .args(["--model", "gemini-4-argon-high", "--effort", "high"])
        .arg("--log-file")
        .arg(&diagnostic)
        .current_dir(root.path())
        .output()
        .unwrap();
    assert!(ordinary.status.success());
    assert!(!diagnostic.exists());
    fs::write(
        root.path().join("emit_provider_health_hint.gate"),
        b"enabled\n",
    )
    .unwrap();
    let opted_in = Command::new("node")
        .arg(&fixture)
        .arg(root.path())
        .args(["--model", "gemini-4-argon-high", "--effort", "high"])
        .arg("--log-file")
        .arg(&diagnostic)
        .current_dir(root.path())
        .output()
        .unwrap();
    assert!(opted_in.status.success());
    let bytes = fs::read(&diagnostic).unwrap();
    assert_eq!(bytes.last(), Some(&b'\n'));
    assert!(!bytes.ends_with(b"\\n"));
    assert_eq!(bytes.iter().filter(|&&byte| byte == b'\n').count(), 1);
    assert_eq!(
        fs::metadata(&diagnostic).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let mut parser = AntigravityDiagnosticParser::new();
    parser.ingest_at(0, &bytes, 10);
    let hint = parser.snapshot();
    assert_eq!(hint.retry_count, 1);
    assert_eq!(hint.retry_high_water, Some(1));
    assert!(hint.current_failure.is_some());
    assert_eq!(
        hint.evidence_class,
        ProviderHealthEvidenceClass::UnverifiedProviderDiagnosticHint
    );

    // Ordinary provider output remains outside the owned diagnostic source.
    assert!(!String::from_utf8(opted_in.stdout)
        .unwrap()
        .contains("run.go:315"));
    let ordinary_prose = b"The run had a stream idle timeout (5m0s), retrying in 4s.\n";
    let mut parser = AntigravityDiagnosticParser::new();
    parser.ingest_at(0, ordinary_prose, 10);
    assert_eq!(parser.snapshot().retry_count, 0);
}
