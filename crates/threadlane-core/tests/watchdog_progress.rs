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
        event_kind: "supervisor_attention".into(),
        summary: serde_json::to_string(&attention()).unwrap(),
        watchdog_attention: None,
    };
    let before = core
        .record_worker_progress(&claim, &[ordinary], 1020)
        .unwrap();
    assert!(before.progress.watchdog_attention.is_none());
    let provider = WorkerProgressUpdate {
        event_kind: "tool_use".into(),
        summary: "Read another file".into(),
        watchdog_attention: None,
    };
    let filler = vec![provider.clone(); MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT as usize];
    core.record_worker_progress(&claim, &filler, 1030).unwrap();
    let typed = WorkerProgressUpdate {
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
