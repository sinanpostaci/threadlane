use tempfile::TempDir;
use threadlane_core::{
    estimate_wire_response_bytes, Command, CommandOutput, Core, WorkerAcceptanceState,
    WorkerCompletionRecord, WorkerOutcomeSummary, WorkerStatus, MAX_COMPACT_RESPONSE_BYTES,
    MAX_INSTRUCTION_LEN,
};

#[test]
fn terminal_worker_evidence_stays_readable_with_admitted_escaped_instruction_and_paths() {
    let temp = TempDir::new().unwrap();
    let database_path = temp.path().join("state.db");
    let mut core = Core::open(&database_path).unwrap();
    let binding = core
        .create_binding("11111111-2222-4333-8444-555555555555", "Review")
        .unwrap();
    let digest = "a".repeat(64);
    let instruction = "\"".repeat(MAX_INSTRUCTION_LEN);
    let started = core
        .execute(
            &binding.binding_id,
            Command::StartWorker {
                command_key: "review-escaped-start".into(),
                profile_id: "fixture".into(),
                profile_digest: digest.clone(),
                provider: "antigravity".into(),
                requested_model: "gemini-4-argon-high".into(),
                requested_effort: "high".into(),
                base_commit: "a".repeat(40),
                instruction: instruction.clone(),
                execution_binding: None,
                now_ms: 1_000,
            },
        )
        .expect("escaped instruction must be admitted")
        .into_worker()
        .unwrap();
    let claim = core
        .claim_worker(&binding.binding_id, &started.run_id, 1, &digest, 1_001)
        .unwrap();
    let paths = (0..16)
        .map(|i| format!("src/{}/{}/file-{i}.rs", "\"".repeat(240), "\"".repeat(240)))
        .collect();
    let outcome = WorkerOutcomeSummary {
        exit_code: Some(1),
        signal: None,
        duration_ms: Some(5),
        timed_out: false,
        cancelled: false,
        cancel_requested_before_completion: false,
        missing_model_evidence: false,
        missing_effort_evidence: false,
        provider_error: false,
        incomplete_cleanup: false,
        rejected_by_diff_gate: true,
        stdout_truncated: false,
        stderr_truncated: false,
        acceptance_state: WorkerAcceptanceState::RejectedByDiffGate,
        changed_paths_count: 16,
        changed_paths_preview: paths,
        violation_count: 8,
        violation_summaries: vec!["\"".repeat(512); 8],
        final_summary: Some("\"".repeat(512)),
        failure_reason: Some("\"".repeat(512)),
        report_artifact_id: None,
    };
    let finished = core.record_worker_outcome(&claim, WorkerCompletionRecord {
        status: WorkerStatus::Failed, observed_model: None, observed_effort: None,
        head_commit: Some("a".repeat(40)), dropped_live_events: 0, outcome,
    }, 1_002).expect("a finished process must not remain active because its compact evidence expands in JSON");
    assert_eq!(finished.status, WorkerStatus::Failed);
    assert_eq!(finished.instruction, instruction);
    let projected = finished.outcome.as_ref().unwrap();
    assert_eq!(projected.changed_paths_count, 16);
    assert_eq!(projected.violation_count, 8);
    assert!(projected.rejected_by_diff_gate);
    assert_eq!(
        projected.acceptance_state,
        WorkerAcceptanceState::RejectedByDiffGate
    );
    assert!(projected.changed_paths_preview.len() < 16);
    let conn = rusqlite::Connection::open(&database_path).unwrap();
    let stored: String = conn
        .query_row(
            "SELECT outcome_json FROM workers WHERE run_id = ?1",
            [&started.run_id],
            |row| row.get(0),
        )
        .unwrap();
    let original: WorkerOutcomeSummary = serde_json::from_str(&stored).unwrap();
    assert_eq!(original.changed_paths_preview.len(), 16);
    assert_eq!(original.violation_summaries.len(), 8);
    assert_eq!(
        original.final_summary.as_deref(),
        Some("\"".repeat(512).as_str())
    );
    let wire =
        estimate_wire_response_bytes(&CommandOutput::Worker(Box::new(finished.clone()))).unwrap();
    assert!(wire <= MAX_COMPACT_RESPONSE_BYTES, "wire was {wire}");
    assert_eq!(
        core.get_worker(&binding.binding_id, &started.run_id)
            .unwrap(),
        finished
    );
}
