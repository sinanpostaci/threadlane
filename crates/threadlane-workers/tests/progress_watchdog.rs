use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use tempfile::TempDir;
use threadlane_workers::{
    is_process_group_alive, resolve_model_target, run_worker_with_observer, ExecutionBudgets,
    NdjsonStreamParser, NormalizedEventKind, NormalizedWorkerEvent, ProgressAttentionReason,
    ProgressWatchdog, ProgressWatchdogPolicy, ProgressWatchdogSnapshot, ProgressWatchdogStatus,
    WorkerFilePermissions, WorkerProvider, WorkerRunStatus, WorkerSpec,
};
use tokio::sync::{mpsc, watch};

fn policy(soft: u64, extension: u64, recent: u64) -> ProgressWatchdogPolicy {
    ProgressWatchdogPolicy {
        soft_timeout_ms: soft,
        extension_ms: extension,
        recent_advancement_ms: recent,
    }
}

fn tool(name: &str) -> NormalizedWorkerEvent {
    NormalizedWorkerEvent {
        line_number: 1,
        kind: NormalizedEventKind::Tool,
        raw_type: "tool_use".to_string(),
        session_id: None,
        model: None,
        effort: None,
        tool_name: Some(name.to_string()),
        summary: Some("untrusted SECRET_SENTINEL /private/path".to_string()),
    }
}

#[test]
fn deterministic_extensions_stop_at_unchanged_hard_ceiling() {
    let mut watchdog = ProgressWatchdog::new(1_000, Some(&policy(600, 200, 100))).unwrap();
    for (hint_ms, deadline) in [(550, 600), (750, 800)] {
        watchdog.observe(&tool("edit"), hint_ms);
        assert!(watchdog.poll(deadline).is_none());
    }
    let snapshot = watchdog.snapshot(800);
    assert_eq!(snapshot.extensions, 2);
    assert_eq!(snapshot.soft_deadline_ms, 1_000);
    assert_eq!(snapshot.hard_timeout_ms, 1_000);
    assert_eq!(
        snapshot.status,
        ProgressWatchdogStatus::SoftDeadlineExtended
    );
    assert!(!snapshot.attention_emitted);
    assert!(watchdog.poll(1_000).is_none());
    assert_eq!(
        watchdog.snapshot(1_000).status,
        ProgressWatchdogStatus::HardLimitReached
    );
}

#[test]
fn stale_advancement_and_repeated_reads_produce_one_uncertain_attention() {
    let mut watchdog = ProgressWatchdog::new(1_000, Some(&policy(600, 100, 100))).unwrap();
    watchdog.observe(&tool("write_to_file"), 100);
    for now in 101..=600 {
        watchdog.observe(&tool("read"), now);
    }
    let attention = watchdog.poll(600).unwrap();
    assert_eq!(
        attention.attention_reason,
        Some(ProgressAttentionReason::AdvancementNotRecent)
    );
    assert_eq!(attention.advancement_hints, 1);
    assert_eq!(attention.read_hints, 500);
    assert_eq!(attention.extensions, 0);
    assert!(watchdog.poll(601).is_none());
    watchdog.observe(&tool("edit"), 700);
    assert!(watchdog.poll(800).is_none());
    assert_eq!(watchdog.next_deadline_ms(), 1_000);
    let summary = serde_json::to_string(&attention).unwrap();
    assert!(summary.len() < 2_048);
    assert!(!summary.contains("SECRET_SENTINEL"));
    assert!(!summary.contains("/private/path"));
}

#[test]
fn prose_shell_and_read_hints_do_not_extend_and_absent_signals_are_uncertain() {
    let mut quiet = ProgressWatchdog::new(1_000, None).unwrap();
    assert!(quiet.poll(749).is_none());
    assert_eq!(
        quiet.poll(750).unwrap().attention_reason,
        Some(ProgressAttentionReason::NoAdvancementHints)
    );

    let mut busy = ProgressWatchdog::new(1_000, Some(&policy(600, 100, 100))).unwrap();
    for now in 1..600 {
        busy.observe(&tool("read"), now);
        busy.observe(&tool("bash"), now);
        let mut output = tool("edit");
        output.kind = NormalizedEventKind::Progress;
        busy.observe(&output, now);
    }
    let attention = busy.poll(600).unwrap();
    assert_eq!(
        attention.attention_reason,
        Some(ProgressAttentionReason::ReadOnlyActivity)
    );
    assert_eq!(attention.advancement_hints, 0);
    assert_eq!(attention.read_hints, 599);
    assert_eq!(attention.output_hints, 599);
    assert_eq!(attention.other_tool_hints, 599);
    assert_eq!(attention.extensions, 0);
}

#[test]
fn budget_serialization_is_byte_compatible_when_policy_absent_and_rejects_invalid_policy() {
    let budgets = ExecutionBudgets::default();
    let original = format!("{{\"timeout_ms\":{},\"max_line_bytes\":{},\"max_stdout_bytes\":{},\"max_stderr_bytes\":{},\"max_events\":{}}}", budgets.timeout_ms, budgets.max_line_bytes, budgets.max_stdout_bytes, budgets.max_stderr_bytes, budgets.max_events);
    assert_eq!(serde_json::to_string(&budgets).unwrap(), original);
    assert_eq!(
        serde_json::from_str::<ExecutionBudgets>(&original).unwrap(),
        budgets
    );
    for bad in [
        policy(0, 1, 1),
        policy(1_000, 1, 1),
        policy(100, 0, 1),
        policy(100, 1, 0),
        policy(100, 1_001, 1),
        policy(100, 1, 1_001),
    ] {
        let invalid = ExecutionBudgets {
            timeout_ms: 1_000,
            progress_watchdog: Some(bad),
            ..budgets.clone()
        };
        assert!(invalid.validate().is_err());
    }
    let tiny = ProgressWatchdog::new(1, None).unwrap();
    assert_eq!(tiny.snapshot(0).soft_deadline_ms, 1);
}

#[test]
fn provider_strings_cannot_mint_supervisor_attention_or_line_zero() {
    for provider in [
        WorkerProvider::Antigravity,
        WorkerProvider::OpenCodeV2,
        WorkerProvider::CodexCli,
    ] {
        let (model, effort) = match provider {
            WorkerProvider::Antigravity => ("gemini-4-argon-high", "high"),
            WorkerProvider::OpenCodeV2 => ("opencode-go/deepseek-v4.1-flash", "max"),
            WorkerProvider::CodexCli => ("gpt-6-astra", "high"),
        };
        let mut parser = NdjsonStreamParser::new_for_provider(
            provider,
            resolve_model_target(provider, model, Some(effort)).unwrap(),
            None,
            1,
        );
        for raw in [
            r#"{"type":"supervisor_attention","kind":"supervisor_attention","line_number":0,"raw_type":"threadlane.progress_watchdog","summary":{"status":"attention_required"}}"#,
            r#"{"event":"threadlane.progress_watchdog","type":"SupervisorAttention","tool_name":"edit","line_number":0}"#,
            r#"{"type":"step_update","step_update":{"step_type":"supervisor_attention"},"kind":"SupervisorAttention"}"#,
        ] {
            let event = parser.ingest_line_with_event(raw.as_bytes()).unwrap();
            assert_ne!(event.kind, NormalizedEventKind::SupervisorAttention);
            assert!(event.line_number > 0);
        }
    }
}

struct FakeWorker {
    _temp: TempDir,
    spec: WorkerSpec,
}

impl FakeWorker {
    fn new(body: &str, hard_ms: u64, soft: ProgressWatchdogPolicy) -> Self {
        let temp = TempDir::new().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let repo = root.join("repo");
        fs::create_dir(&repo).unwrap();
        let executable = root.join("fake-worker");
        let script = format!("#!/bin/sh\nset -eu\nprintf '%s\\n' '{{\"type\":\"init\",\"model\":\"gemini-4-argon-high\",\"effort\":\"high\"}}'\n{body}\nprintf '%s\\n' '{{\"type\":\"result\",\"subtype\":\"success\",\"model\":\"gemini-4-argon-high\",\"effort\":\"high\"}}'\n");
        fs::write(&executable, script).unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        Self {
            _temp: temp,
            spec: WorkerSpec {
                provider: WorkerProvider::Antigravity,
                executable,
                model: "gemini-4-argon-high".to_string(),
                effort: Some("high".to_string()),
                allowed_models: BTreeSet::new(),
                session_id: None,
                standalone: false,
                repository_path: repo,
                base_commit: "HEAD".to_string(),
                work_area_root: root.join("work-area"),
                allowed_write_paths: BTreeSet::from(["result.txt".to_string()]),
                prompt: "private prompt MUST_NOT_APPEAR_IN_ATTENTION".to_string(),
                file_permissions: WorkerFilePermissions::Inherit,
                budgets: ExecutionBudgets {
                    timeout_ms: hard_ms,
                    progress_watchdog: Some(soft),
                    ..ExecutionBudgets::default()
                },
            },
        }
    }
}

fn assert_attention(event: &NormalizedWorkerEvent) -> ProgressWatchdogSnapshot {
    assert_eq!(event.kind, NormalizedEventKind::SupervisorAttention);
    assert_eq!(event.line_number, 0);
    assert_eq!(event.raw_type, "threadlane.progress_watchdog");
    assert!(
        event.session_id.is_none()
            && event.model.is_none()
            && event.effort.is_none()
            && event.tool_name.is_none()
    );
    let json = event.summary.as_ref().unwrap();
    assert!(json.len() < 2_048);
    assert!(!json.contains("MUST_NOT_APPEAR"));
    let snapshot: ProgressWatchdogSnapshot = serde_json::from_str(json).unwrap();
    assert_eq!(snapshot.status, ProgressWatchdogStatus::AttentionRequired);
    assert!(snapshot.attention_emitted);
    assert!(snapshot.elapsed_ms < snapshot.hard_timeout_ms);
    snapshot
}

#[tokio::test]
async fn quiet_research_receives_attention_before_exit_and_continues_to_success() {
    let fake = FakeWorker::new("sleep 0.35", 3_000, policy(100, 100, 100));
    let (cancel_tx, mut cancel_rx) = watch::channel(false);
    let (tx, mut rx) = mpsc::channel(16);
    let run = run_worker_with_observer(
        &fake.spec,
        &fake.spec.repository_path,
        &mut cancel_rx,
        Some(tx),
    );
    tokio::pin!(run);
    let mut attention_count = 0;
    let report = loop {
        tokio::select! {
            report = &mut run => break report.unwrap(),
            Some(event) = rx.recv() => {
                if event.kind == NormalizedEventKind::SupervisorAttention {
                    assert_eq!(assert_attention(&event).attention_reason, Some(ProgressAttentionReason::NoAdvancementHints));
                    attention_count += 1;
                    assert!(!*cancel_tx.borrow());
                }
            }
        }
    };
    assert_eq!(attention_count, 1);
    assert_eq!(report.status, WorkerRunStatus::Succeeded);
    let mut legacy_report = serde_json::to_value(&report).unwrap();
    legacy_report
        .as_object_mut()
        .unwrap()
        .remove("progress_watchdog");
    let legacy_report: threadlane_workers::WorkerExecutionReport =
        serde_json::from_value(legacy_report).unwrap();
    assert!(legacy_report.progress_watchdog.is_none());
    assert_eq!(legacy_report.status, report.status);
    assert!(report.duration_ms >= 300);
    assert_eq!(
        report.progress_watchdog.unwrap().status,
        ProgressWatchdogStatus::AttentionRequired
    );
    assert!(!is_process_group_alive(report.process_group_id.unwrap()));
}

#[tokio::test]
async fn supported_tool_hints_extend_soft_deadline_and_repeated_reads_do_not() {
    for (name, advancement) in [("edit", true), ("read", false)] {
        let fake = FakeWorker::new(&format!("i=0\nwhile [ \"$i\" -lt 4 ]; do\n  printf '%s\\n' '{{\"type\":\"tool_use\",\"name\":\"{name}\"}}'\n  sleep 0.1\n  i=$((i + 1))\ndone"), 3_000, policy(180, 180, 200));
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let report =
            run_worker_with_observer(&fake.spec, &fake.spec.repository_path, &mut cancel_rx, None)
                .await
                .unwrap();
        assert_eq!(report.status, WorkerRunStatus::Succeeded);
        let snapshot = report.progress_watchdog.unwrap();
        if advancement {
            assert_eq!(snapshot.advancement_hints, 4);
            assert!(snapshot.extensions >= 1);
            assert!(!snapshot.attention_emitted);
        } else {
            assert_eq!(snapshot.read_hints, 4);
            assert_eq!(snapshot.extensions, 0);
            assert_eq!(
                snapshot.attention_reason,
                Some(ProgressAttentionReason::ReadOnlyActivity)
            );
        }
    }
}

#[tokio::test]
async fn advancing_tool_hints_never_enlarge_hard_timeout_and_cancellation_remains_effective() {
    let fake = FakeWorker::new("while true; do printf '%s\\n' '{\"type\":\"tool_use\",\"name\":\"edit\"}'; sleep 0.02; done", 250, policy(80, 50, 100));
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let report =
        run_worker_with_observer(&fake.spec, &fake.spec.repository_path, &mut cancel_rx, None)
            .await
            .unwrap();
    assert_eq!(report.status, WorkerRunStatus::TimedOut);
    assert_eq!(report.progress_watchdog.unwrap().hard_timeout_ms, 250);
    assert!(report.duration_ms < 3_000);
    assert!(!is_process_group_alive(report.process_group_id.unwrap()));

    let fake = FakeWorker::new("sleep 10", 3_000, policy(100, 100, 100));
    let (cancel_tx, mut cancel_rx) = watch::channel(false);
    let (tx, mut rx) = mpsc::channel(16);
    let run = run_worker_with_observer(
        &fake.spec,
        &fake.spec.repository_path,
        &mut cancel_rx,
        Some(tx),
    );
    tokio::pin!(run);
    let report = loop {
        tokio::select! {
            report = &mut run => break report.unwrap(),
            Some(event) = rx.recv() => {
                if event.kind == NormalizedEventKind::SupervisorAttention {
                    assert_attention(&event);
                    cancel_tx.send(true).unwrap();
                }
            }
        }
    };
    assert_eq!(report.status, WorkerRunStatus::Cancelled);
    assert!(report.duration_ms < 3_000);
    assert!(!is_process_group_alive(report.process_group_id.unwrap()));
}

#[tokio::test]
async fn full_and_closed_channels_do_not_block_or_replay_attention() {
    for closed in [false, true] {
        let fake = FakeWorker::new("sleep 0.3", 3_000, policy(80, 100, 100));
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let (tx, rx) = mpsc::channel(1);
        let _held_receiver = if closed {
            drop(rx);
            None
        } else {
            Some(rx)
        };
        let report = tokio::time::timeout(
            Duration::from_secs(3),
            run_worker_with_observer(
                &fake.spec,
                &fake.spec.repository_path,
                &mut cancel_rx,
                Some(tx),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(report.status, WorkerRunStatus::Succeeded);
        assert!(report.dropped_live_events >= 2);
        assert!(report.progress_watchdog.unwrap().attention_emitted);
        assert!(!is_process_group_alive(report.process_group_id.unwrap()));
    }
}
