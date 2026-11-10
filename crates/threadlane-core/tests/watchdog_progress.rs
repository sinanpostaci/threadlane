//! Durable software-supervision attention remains distinct from provider progress.

use tempfile::TempDir;
use threadlane_core::{
    Command, Core, CoreError, WorkerProgressAttentionReason, WorkerProgressSummary,
    WorkerProgressUpdate, WorkerProgressWatchdogSnapshot, WorkerProgressWatchdogStatus,
    MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT,
};

const THREAD: &str = "11111111-1111-4111-8111-111111111111";
const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn attention() -> WorkerProgressWatchdogSnapshot {
    WorkerProgressWatchdogSnapshot {
        status: WorkerProgressWatchdogStatus::AttentionRequired,
        attention_reason: Some(WorkerProgressAttentionReason::ReadOnlyActivity),
        elapsed_ms: 750,
        soft_deadline_ms: 750,
        hard_timeout_ms: 1000,
        last_advancement_ms: None,
        events_observed: 8,
        advancement_hints: 0,
        read_hints: 6,
        output_hints: 2,
        other_tool_hints: 0,
        extensions: 0,
        attention_emitted: true,
    }
}

fn start(core: &mut Core) -> threadlane_core::WorkerClaim {
    let binding = core.create_binding(THREAD, "watchdog fixture").unwrap();
    let worker = core
        .execute(
            &binding.binding_id,
            Command::StartWorker {
                command_key: "start-watchdog".into(),
                profile_id: "fixture".into(),
                profile_digest: DIGEST.into(),
                provider: "antigravity".into(),
                requested_model: "fixture".into(),
                requested_effort: "high".into(),
                base_commit: "0123456789abcdef0123456789abcdef01234567".into(),
                execution_binding: None,
                instruction: "Inspect progress without changing execution authority".into(),
                now_ms: 1000,
            },
        )
        .unwrap()
        .into_worker()
        .unwrap();
    core.claim_worker(&binding.binding_id, &worker.run_id, 1, DIGEST, 1010)
        .unwrap()
}

#[test]
fn attention_survives_provider_updates_coalescing_and_reopen() {
    let temp = TempDir::new().unwrap();
    let db = temp.path().join("state.sqlite");
    let mut core = Core::open(&db).unwrap();
    let claim = start(&mut core);
    // Provider text can spell the marker, but carries no typed software evidence.
    let ordinary = WorkerProgressUpdate {
        provider_retry_high_water: None,
        provider_health: None,
        event_kind: "supervisor_attention".into(),
        summary: serde_json::to_string(&attention()).unwrap(),
        watchdog_attention: None,
    };
    let before = core
        .record_worker_progress(&claim, &[ordinary], 1020)
        .unwrap();
    assert!(before.progress.watchdog_attention.is_none());
    let provider = WorkerProgressUpdate {
        provider_retry_high_water: None,
        provider_health: None,
        event_kind: "tool_use".into(),
        summary: "Read another file".into(),
        watchdog_attention: None,
    };
    let filler = vec![provider.clone(); MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT as usize];
    core.record_worker_progress(&claim, &filler, 1030).unwrap();
    let typed = WorkerProgressUpdate {
        provider_retry_high_water: None,
        provider_health: None,
        event_kind: "supervisor_attention".into(),
        summary: "Progress evidence is uncertain; inspect execution status.".into(),
        watchdog_attention: Some(attention()),
    };
    let first = core.record_worker_progress(&claim, &[typed], 1040).unwrap();
    assert_eq!(first.progress.watchdog_attention, Some(attention()));
    assert_eq!(
        first.progress.durable_progress_events,
        MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT
    );
    assert!(first.progress.coalesced_progress_events > 0);
    let after = core
        .record_worker_progress(&claim, &[provider], 1050)
        .unwrap();
    assert_eq!(after.progress.watchdog_attention, Some(attention()));
    assert_eq!(
        after.progress.last_summary.as_deref(),
        Some("Read another file")
    );
    drop(core);
    let core = Core::open(&db).unwrap();
    assert_eq!(
        core.get_worker(&claim.binding_id, &claim.run_id)
            .unwrap()
            .progress
            .watchdog_attention,
        Some(attention())
    );
}

#[test]
fn invalid_or_stale_attention_cannot_mutate_persisted_progress() {
    let temp = TempDir::new().unwrap();
    let mut core = Core::open(temp.path().join("state.sqlite")).unwrap();
    let claim = start(&mut core);
    let mut malformed = attention();
    malformed.elapsed_ms = malformed.hard_timeout_ms;
    let update = WorkerProgressUpdate {
        provider_retry_high_water: None,
        provider_health: None,
        event_kind: "supervisor_attention".into(),
        summary: "attention".into(),
        watchdog_attention: Some(malformed),
    };
    assert!(matches!(
        core.record_worker_progress(&claim, &[update], 1020),
        Err(CoreError::Validation { .. })
    ));
    assert!(core
        .get_worker(&claim.binding_id, &claim.run_id)
        .unwrap()
        .progress
        .watchdog_attention
        .is_none());
    let update = WorkerProgressUpdate {
        provider_retry_high_water: None,
        provider_health: None,
        event_kind: "provider_output".into(),
        summary: "attention".into(),
        watchdog_attention: Some(attention()),
    };
    assert!(matches!(
        core.record_worker_progress(&claim, &[update], 1030),
        Err(CoreError::Validation { .. })
    ));
    let mut stale = claim.clone();
    stale.claim_id = "different-claim".into();
    let update = WorkerProgressUpdate {
        provider_retry_high_water: None,
        provider_health: None,
        event_kind: "supervisor_attention".into(),
        summary: "attention".into(),
        watchdog_attention: Some(attention()),
    };
    assert!(core
        .record_worker_progress(&stale, &[update], 1040)
        .is_err());
    assert!(core
        .get_worker(&claim.binding_id, &claim.run_id)
        .unwrap()
        .progress
        .watchdog_attention
        .is_none());
}

#[test]
fn legacy_progress_bytes_and_strict_attention_shape_are_preserved() {
    let legacy = r#"{"durable_progress_events":0,"coalesced_progress_events":0,"dropped_live_events":0,"last_event_kind":null,"last_summary":null,"last_event_at_ms":null}"#;
    let decoded: WorkerProgressSummary = serde_json::from_str(legacy).unwrap();
    assert!(decoded.watchdog_attention.is_none());
    assert_eq!(serde_json::to_string(&decoded).unwrap(), legacy);
    let mut snapshot = serde_json::to_value(attention()).unwrap();
    snapshot["provider_instruction"] = "retry with another model".into();
    assert!(serde_json::from_value::<WorkerProgressWatchdogSnapshot>(snapshot).is_err());
    let mut malformed = attention();
    malformed.read_hints = 99;
    assert!(malformed.validate_attention().is_err());
    malformed = attention();
    malformed.attention_reason = Some(WorkerProgressAttentionReason::AdvancementNotRecent);
    assert!(malformed.validate_attention().is_err());
}

use threadlane_core::provider_health::{
    ProviderDiagnosticAvailability, ProviderHealthCategory, ProviderHealthEvidenceClass,
    ProviderHealthObservation, ProviderHealthRecovery, ProviderHealthSnapshot,
    MAX_PROVIDER_HEALTH_ELAPSED_MS, MAX_PROVIDER_HEALTH_RECORDS, MAX_PROVIDER_HEALTH_UPDATES,
};
use threadlane_core::{
    WorkerAcceptanceState, WorkerCompletionRecord, WorkerOutcomeSummary, WorkerStatus,
};

fn diagnostic(retries: u32) -> ProviderHealthSnapshot {
    let first = ProviderHealthObservation {
        category: ProviderHealthCategory::StreamIdleRetry,
        elapsed_ms: 300_000,
        update_sequence: 2,
    };
    let last = ProviderHealthObservation {
        elapsed_ms: 300_000 * u64::from(retries),
        update_sequence: retries + 1,
        ..first
    };
    ProviderHealthSnapshot {
        schema_version: 1,
        evidence_class: ProviderHealthEvidenceClass::UnverifiedProviderDiagnosticHint,
        availability: ProviderDiagnosticAvailability::Available,
        unavailable_reason: None,
        elapsed_ms: last.elapsed_ms,
        update_sequence: last.update_sequence,
        retry_count: retries,
        first_failure: Some(first),
        last_failure: Some(last),
        current_failure: Some(last.category),
        recovery: None,
    }
}

fn diagnostic_update(snapshot: ProviderHealthSnapshot) -> WorkerProgressUpdate {
    WorkerProgressUpdate {
        provider_retry_high_water: Some(7),
        provider_health: Some(snapshot),
        watchdog_attention: None,
        event_kind: "provider_diagnostic_hint".into(),
        summary: String::new(),
    }
}

fn completion(
    snapshot: Option<ProviderHealthSnapshot>,
    status: WorkerStatus,
) -> WorkerCompletionRecord {
    WorkerCompletionRecord {
        status,
        observed_model: None,
        observed_effort: None,
        head_commit: None,
        dropped_live_events: 97,
        outcome: WorkerOutcomeSummary {
            provider_retry_high_water: snapshot.as_ref().map(|_| 7),
            provider_health: snapshot,
            exit_code: Some(if status == WorkerStatus::Completed {
                0
            } else {
                7
            }),
            signal: None,
            duration_ms: Some(2_000_000),
            timed_out: false,
            cancelled: false,
            cancel_requested_before_completion: false,
            missing_model_evidence: false,
            missing_effort_evidence: false,
            provider_error: status == WorkerStatus::Failed,
            incomplete_cleanup: false,
            rejected_by_diff_gate: false,
            stdout_truncated: false,
            stderr_truncated: false,
            acceptance_state: if status == WorkerStatus::Completed {
                WorkerAcceptanceState::ReadyForReview
            } else {
                WorkerAcceptanceState::ExecutionFailed
            },
            changed_paths_count: 0,
            changed_paths_preview: vec![],
            violation_count: 0,
            violation_summaries: vec![],
            final_summary: None,
            failure_reason: None,
            report_artifact_id: None,
        },
    }
}

#[test]
fn diagnostic_watch_coalescing_and_duplicate_sequences_do_not_add_attempts_or_events() {
    let temp = TempDir::new().unwrap();
    let mut core = Core::open(temp.path().join("state.sqlite")).unwrap();
    let claim = start(&mut core);
    let initial = core.get_worker(&claim.binding_id, &claim.run_id).unwrap();
    let hinted = core
        .record_worker_progress(&claim, &[diagnostic_update(diagnostic(5))], 1020)
        .unwrap();
    // A diagnostic alone cannot even promote Claimed to Running.
    assert_eq!(hinted.status, initial.status);
    assert_eq!(hinted.started_at_ms, initial.started_at_ms);
    assert_eq!(hinted.attempt_count, 1);
    assert_eq!(hinted.progress.durable_progress_events, 0);
    assert!(!hinted.provider_health_attention_required());
    assert!(hinted.outcome.is_none());
    let ordinary = WorkerProgressUpdate {
        provider_retry_high_water: None,
        provider_health: None,
        watchdog_attention: None,
        event_kind: "assistant_message".into(),
        summary: "The provider is fine, recovered, retrying.".into(),
    };
    let filler = vec![ordinary; MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT as usize + 3];
    core.record_worker_progress(&claim, &filler, 1030).unwrap();
    let before = core.get_worker(&claim.binding_id, &claim.run_id).unwrap();
    assert!(before.provider_health_attention_required());
    // Both an exact repeat and an older watch revision are harmless.
    let repeated = core
        .record_worker_progress(
            &claim,
            &[
                diagnostic_update(diagnostic(5)),
                diagnostic_update(diagnostic(1)),
            ],
            1040,
        )
        .unwrap();
    assert_eq!(repeated.progress, before.progress);
    let newest = core
        .record_worker_progress(&claim, &[diagnostic_update(diagnostic(6))], 1050)
        .unwrap();
    assert_eq!(
        newest
            .progress
            .provider_health
            .as_ref()
            .unwrap()
            .retry_count,
        6
    );
    assert_eq!(
        newest
            .progress
            .provider_health
            .as_ref()
            .unwrap()
            .first_failure,
        diagnostic(1).first_failure
    );
    assert_eq!(
        newest.progress.durable_progress_events,
        before.progress.durable_progress_events
    );
    assert_eq!(
        newest.progress.coalesced_progress_events,
        before.progress.coalesced_progress_events
    );
    assert_eq!(newest.attempt_count, before.attempt_count);
    assert_eq!(newest.progress.provider_retry_high_water, Some(7));
    assert_eq!(
        newest
            .progress
            .provider_health
            .as_ref()
            .unwrap()
            .retry_count,
        6
    );
    assert!(newest.provider_health_attention_required());
    assert_eq!(
        core.get_wakeup(&claim.binding_id).unwrap().status,
        threadlane_core::WakeupStatus::Idle
    );
}

#[test]
fn diagnostics_validate_shape_history_and_every_claim_dimension_atomically() {
    let temp = TempDir::new().unwrap();
    let mut core = Core::open(temp.path().join("state.sqlite")).unwrap();
    let claim = start(&mut core);
    core.record_worker_progress(&claim, &[diagnostic_update(diagnostic(2))], 1020)
        .unwrap();
    let expected = core.get_worker(&claim.binding_id, &claim.run_id).unwrap();

    let mut claims = Vec::new();
    let mut bad = claim.clone();
    bad.binding_id = "bnd_wrong".into();
    claims.push(bad);
    let mut bad = claim.clone();
    bad.run_id = "run_wrong".into();
    claims.push(bad);
    let mut bad = claim.clone();
    bad.worker_id = "wrk_wrong".into();
    claims.push(bad);
    let mut bad = claim.clone();
    bad.claim_id = "wclm_wrong".into();
    claims.push(bad);
    let mut bad = claim.clone();
    bad.revision += 1;
    claims.push(bad);
    let mut bad = claim.clone();
    bad.attempt_count += 1;
    claims.push(bad);
    let mut bad = claim.clone();
    bad.profile_id = "other".into();
    claims.push(bad);
    let mut bad = claim.clone();
    bad.profile_digest = "b".repeat(64);
    claims.push(bad);
    for bad in claims {
        assert!(core
            .record_worker_progress(&bad, &[diagnostic_update(diagnostic(3))], 1030)
            .is_err());
        assert!(core
            .record_worker_outcome(
                &bad,
                completion(Some(diagnostic(3)), WorkerStatus::Failed),
                1030
            )
            .is_err());
        assert_eq!(
            core.get_worker(&claim.binding_id, &claim.run_id).unwrap(),
            expected
        );
    }

    let mut invalid = Vec::new();
    let mut bad = diagnostic(3);
    bad.schema_version = 2;
    invalid.push(bad);
    let mut bad = diagnostic(3);
    bad.retry_count = MAX_PROVIDER_HEALTH_RECORDS + 1;
    invalid.push(bad);
    let mut bad = diagnostic(3);
    bad.update_sequence = MAX_PROVIDER_HEALTH_UPDATES + 1;
    invalid.push(bad);
    let mut bad = diagnostic(3);
    bad.elapsed_ms = MAX_PROVIDER_HEALTH_ELAPSED_MS + 1;
    invalid.push(bad);
    let mut bad = diagnostic(3);
    bad.first_failure.as_mut().unwrap().elapsed_ms += 1;
    invalid.push(bad);
    let mut bad = diagnostic(3);
    bad.last_failure.as_mut().unwrap().elapsed_ms = 1;
    invalid.push(bad);
    let mut bad = diagnostic(3);
    bad.unavailable_reason =
        Some(threadlane_core::provider_health::ProviderDiagnosticUnavailableReason::SourceLost);
    invalid.push(bad);
    let mut bad = diagnostic(2);
    bad.elapsed_ms += 1;
    invalid.push(bad); // same sequence, changed data
    let mut bad = diagnostic(3);
    bad.last_failure = diagnostic(2).last_failure;
    invalid.push(bad);
    for bad in invalid {
        assert!(core
            .record_worker_progress(&claim, &[diagnostic_update(bad.clone())], 1040)
            .is_err());
        assert_eq!(
            core.get_worker(&claim.binding_id, &claim.run_id).unwrap(),
            expected
        );
    }
    let mut wrong_channel = diagnostic_update(diagnostic(3));
    wrong_channel.event_kind = "assistant_message".into();
    assert!(core
        .record_worker_progress(&claim, &[wrong_channel], 1040)
        .is_err());
    let mut mixed = diagnostic_update(diagnostic(3));
    mixed.watchdog_attention = Some(attention());
    assert!(core.record_worker_progress(&claim, &[mixed], 1040).is_err());
    assert_eq!(
        core.get_worker(&claim.binding_id, &claim.run_id).unwrap(),
        expected
    );
}

#[test]
fn terminal_report_completes_missing_live_history_without_new_live_attention_and_survives_reopen() {
    for status in [
        WorkerStatus::Completed,
        WorkerStatus::Failed,
        WorkerStatus::Unknown,
    ] {
        let temp = TempDir::new().unwrap();
        let db = temp.path().join("state.sqlite");
        let mut core = Core::open(&db).unwrap();
        let claim = start(&mut core);
        let mut health = diagnostic(5);
        if status == WorkerStatus::Completed {
            health.update_sequence += 1;
            health.elapsed_ms += 100;
            health.current_failure = None;
            health.recovery = Some(ProviderHealthRecovery::VerifiedTerminalOutcome);
        }
        let terminal = core
            .record_worker_outcome(&claim, completion(Some(health.clone()), status), 1060)
            .unwrap();
        assert_eq!(terminal.status, status);
        assert_eq!(terminal.attempt_count, 1);
        assert_eq!(terminal.progress.provider_health.as_ref(), Some(&health));
        assert_eq!(
            terminal.outcome.as_ref().unwrap().provider_health.as_ref(),
            Some(&health)
        );
        assert!(!terminal.progress.provider_health_live_observed);
        assert!(!terminal.provider_health_attention_required());
        assert_eq!(terminal.progress.dropped_live_events, 97);
        assert_eq!(terminal.progress.provider_retry_high_water, Some(7));
        assert_eq!(
            terminal.outcome.as_ref().unwrap().provider_retry_high_water,
            Some(7)
        );
        assert_eq!(terminal.progress.durable_progress_events, 0);
        drop(core);
        let mut core = Core::open(&db).unwrap();
        assert_eq!(
            core.get_worker(&claim.binding_id, &claim.run_id).unwrap(),
            terminal
        );
        assert!(core
            .record_worker_progress(&claim, &[diagnostic_update(diagnostic(6))], 1070)
            .is_err());
    }
}

#[test]
fn terminal_merge_preserves_live_first_observation_and_real_failure() {
    let temp = TempDir::new().unwrap();
    let db = temp.path().join("state.sqlite");
    let mut core = Core::open(&db).unwrap();
    let claim = start(&mut core);
    let first = core
        .record_worker_progress(&claim, &[diagnostic_update(diagnostic(1))], 1020)
        .unwrap();
    let finished = core
        .record_worker_outcome(
            &claim,
            completion(Some(diagnostic(5)), WorkerStatus::Failed),
            1030,
        )
        .unwrap();
    assert_eq!(
        finished
            .progress
            .provider_health
            .as_ref()
            .unwrap()
            .first_failure,
        first
            .progress
            .provider_health
            .as_ref()
            .unwrap()
            .first_failure
    );
    assert_eq!(
        finished
            .progress
            .provider_health
            .as_ref()
            .unwrap()
            .retry_count,
        5
    );
    assert!(finished.progress.provider_health_live_observed);
    assert!(!finished.provider_health_attention_required());
    assert_eq!(finished.status, WorkerStatus::Failed);
    assert!(finished.outcome.as_ref().unwrap().provider_error);
    // Legacy outcome data remains byte-compatible with omitted diagnostic fields.
    let outcome = completion(None, WorkerStatus::Failed).outcome;
    let encoded = serde_json::to_string(&outcome).unwrap();
    assert!(!encoded.contains("provider_health"));
    assert_eq!(
        serde_json::from_str::<WorkerOutcomeSummary>(&encoded).unwrap(),
        outcome
    );
}

#[test]
fn explicit_later_hint_closes_only_current_advisory_and_keeps_observations() {
    let temp = TempDir::new().unwrap();
    let mut core = Core::open(temp.path().join("state.sqlite")).unwrap();
    let claim = start(&mut core);
    core.record_worker_progress(&claim, &[diagnostic_update(diagnostic(1))], 1020)
        .unwrap();
    let mut closed = diagnostic(1);
    closed.update_sequence += 1;
    closed.elapsed_ms += 100;
    closed.current_failure = None;
    closed.recovery = Some(ProviderHealthRecovery::LaterDiagnosticHint);
    let snapshot = core
        .record_worker_progress(&claim, &[diagnostic_update(closed.clone())], 1030)
        .unwrap();
    assert_eq!(snapshot.progress.provider_health.as_ref(), Some(&closed));
    assert!(!snapshot.provider_health_attention_required());
    let mut terminal_only = closed;
    terminal_only.update_sequence += 1;
    terminal_only.recovery = Some(ProviderHealthRecovery::VerifiedTerminalOutcome);
    assert!(core
        .record_worker_progress(&claim, &[diagnostic_update(terminal_only)], 1040)
        .is_err());
}

#[test]
fn coalesced_closed_source_snapshot_cannot_mint_current_live_attention() {
    let temp = TempDir::new().unwrap();
    let mut core = Core::open(temp.path().join("state.sqlite")).unwrap();
    let claim = start(&mut core);
    let mut closed = diagnostic(5);
    closed.update_sequence += 1;
    closed.availability = ProviderDiagnosticAvailability::Unavailable;
    closed.unavailable_reason =
        Some(threadlane_core::provider_health::ProviderDiagnosticUnavailableReason::SourceClosed);
    let final_only = core
        .record_worker_progress(&claim, &[diagnostic_update(closed.clone())], 1020)
        .unwrap();
    assert!(!final_only.progress.provider_health_live_observed);
    assert!(!final_only.provider_health_attention_required());
    assert_eq!(final_only.progress.provider_health.as_ref(), Some(&closed));
    closed.update_sequence += 1;
    closed.current_failure = None;
    closed.recovery = Some(ProviderHealthRecovery::VerifiedTerminalOutcome);
    let done = core
        .record_worker_progress(&claim, &[diagnostic_update(closed.clone())], 1030)
        .unwrap();
    assert!(!done.progress.provider_health_live_observed);
    assert!(!done.provider_health_attention_required());
    let terminal = core
        .record_worker_outcome(
            &claim,
            completion(Some(closed), WorkerStatus::Completed),
            1040,
        )
        .unwrap();
    assert!(!terminal.progress.provider_health_live_observed);
    assert!(!terminal.provider_health_attention_required());
}

#[test]
fn retry_high_water_cannot_advance_independently_or_replace_observed_counts() {
    let temp = TempDir::new().unwrap();
    let mut core = Core::open(temp.path().join("state.sqlite")).unwrap();
    let claim = start(&mut core);
    let expected = core
        .record_worker_progress(&claim, &[diagnostic_update(diagnostic(2))], 1020)
        .unwrap();
    for value in [0, 6, MAX_PROVIDER_HEALTH_RECORDS + 1] {
        let mut bad = diagnostic_update(diagnostic(3));
        bad.provider_retry_high_water = Some(value);
        assert!(core.record_worker_progress(&claim, &[bad], 1030).is_err());
        assert_eq!(
            core.get_worker(&claim.binding_id, &claim.run_id).unwrap(),
            expected
        );
    }
    let mut bad = diagnostic_update(diagnostic(2));
    bad.provider_retry_high_water = Some(8);
    assert!(core.record_worker_progress(&claim, &[bad], 1030).is_err());
    let mut future_coverage = diagnostic(2);
    future_coverage.update_sequence += 1;
    future_coverage.elapsed_ms += 1;
    let mut bad = diagnostic_update(future_coverage);
    bad.provider_retry_high_water = Some(8);
    assert!(core.record_worker_progress(&claim, &[bad], 1030).is_err());
    assert_eq!(
        core.get_worker(&claim.binding_id, &claim.run_id).unwrap(),
        expected
    );
}

#[test]
fn malformed_terminal_diagnostics_cannot_block_real_failure_or_replace_history() {
    let temp = TempDir::new().unwrap();
    let mut core = Core::open(temp.path().join("state.sqlite")).unwrap();
    let claim = start(&mut core);
    core.record_worker_progress(&claim, &[diagnostic_update(diagnostic(1))], 1020)
        .unwrap();
    let mut bad = diagnostic(5);
    bad.first_failure.as_mut().unwrap().elapsed_ms += 1;
    let failed = core
        .record_worker_outcome(&claim, completion(Some(bad), WorkerStatus::Failed), 1030)
        .unwrap();
    assert_eq!(failed.status, WorkerStatus::Failed);
    assert_eq!(failed.progress.provider_health, Some(diagnostic(1)));
    assert_eq!(
        failed.outcome.as_ref().unwrap().provider_health,
        Some(diagnostic(1))
    );
    assert!(failed.outcome.as_ref().unwrap().provider_error);
    assert!(!failed.provider_health_attention_required());
}

#[test]
fn inconsistent_terminal_clock_is_unknown_diagnostics_and_cannot_create_failure_or_acceptance() {
    let temp = TempDir::new().unwrap();
    let mut core = Core::open(temp.path().join("state.sqlite")).unwrap();
    let claim = start(&mut core);
    let mut result = completion(Some(diagnostic(5)), WorkerStatus::Failed);
    result.outcome.duration_ms = Some(1);
    let failed = core.record_worker_outcome(&claim, result, 1030).unwrap();
    assert_eq!(failed.status, WorkerStatus::Failed);
    assert!(failed.progress.provider_health.is_none());
    assert!(failed.outcome.as_ref().unwrap().provider_health.is_none());
    assert!(!failed.provider_health_attention_required());
    assert_eq!(
        failed.outcome.as_ref().unwrap().acceptance_state,
        WorkerAcceptanceState::ExecutionFailed
    );
}
