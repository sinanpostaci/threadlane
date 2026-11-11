//! Behavioral contract tests for the shared Workspace Observation Contract (`WorkspaceSnapshot`).

use threadlane_core::observation::*;

const VALID_UUID_A: &str = "0196ed00-0000-7000-8000-000000000042";
const VALID_UUID_B: &str = "0196ed00-0000-7000-8000-000000000043";

fn sample_identity() -> WorkspaceChatIdentity {
    WorkspaceChatIdentity::new(
        "default",
        "bnd_chat_alpha",
        VALID_UUID_A,
        "/home/dev/repos/alpha-app",
    )
    .expect("valid sample chat identity")
}

fn sample_process_instance(pid: u32, start_ticks: u64) -> ObservationProcessInstance {
    ObservationProcessInstance {
        pid,
        boot_id: "boot-linux-01".to_string(),
        start_ticks,
    }
}

fn sample_native_live(
    identity: WorkspaceChatIdentity,
    epoch: &str,
    process: Option<ObservationProcessInstance>,
    sequence: u64,
    observed_at_ms: i64,
) -> WorkspaceNativeSample {
    WorkspaceNativeSample {
        chat_identity: identity,
        producer_epoch: epoch.to_string(),
        backend_process_instance: process,
        sequence,
        observed_at_ms: Some(observed_at_ms),
        last_success_at_ms: Some(observed_at_ms),
        availability: WorkspaceAvailabilityState::Available,
        activity: WorkspaceActivityState::Working,
        connection: WorkspaceSampleConnection {
            state: WorkspaceConnectionState::Connected,
            freshness: WorkspaceFreshnessState::Fresh,
            route: Some("managed_linux_desktop".to_string()),
            diagnostic: None,
        },
        capabilities: WorkspaceNativeCapabilities {
            exact_thread_read: WorkspaceCapabilityState::Supported,
            activity_detail: WorkspaceCapabilityState::Supported,
            configured_selection: WorkspaceCapabilityState::Supported,
            wakeup_delivery: WorkspaceCapabilityState::Supported,
            per_call_identity: WorkspaceCapabilityState::Supported,
        },
        configured_selection: WorkspaceConfiguredSelection::verified(Some("gpt-5.4"), Some("high")),
    }
}

fn sample_legacy_observation_snapshot() -> ObservationSnapshot {
    // Explicit struct literals verify backward compatibility of ObservationActor and ObservationSnapshot.
    let actor = ObservationActor {
        label: Some("Codex · Alpha Chat".to_string()),
        role: Some("coworker".to_string()),
        source: Some("paired_mcp".to_string()),
        model: None,
        effort: None,
        selected_model: None,
        selected_effort: None,
        evidence: ObservationActorEvidence::Configured,
    };

    let request = ObservationRequest {
        source_id: "default".to_string(),
        id: "obsreq_01".to_string(),
        job_id: Some("plan_alpha_01".to_string()),
        parent_request_id: None,
        title: "workflow get_plan".to_string(),
        operation: "mcp.get_plan".to_string(),
        project_label: "alpha-app".to_string(),
        binding_id: Some("bnd_chat_alpha".to_string()),
        thread_uuid: Some(VALID_UUID_A.to_string()),
        thread_label: Some("Alpha Chat".to_string()),
        state: ObservationCallState::Completed,
        started_at_ms: Some(1_700_000_000_000),
        finished_at_ms: Some(1_700_000_000_050),
        updated_at_ms: 1_700_000_000_050,
        duration_ms: Some(50),
        actor,
        executor: Some(ObservationExecutor::local_mcp()),
        usage: None,
        delivery: Vec::new(),
        children: Vec::new(),
        children_count: 0,
        summary: Some("Fetched active plan.".to_string()),
    };

    let job = ObservationJob {
        source_id: "default".to_string(),
        id: "plan_alpha_01".to_string(),
        project_label: "alpha-app".to_string(),
        project_path: Some("/home/dev/repos/alpha-app".to_string()),
        title: "Workspace observation contract".to_string(),
        thread_uuid: Some(VALID_UUID_A.to_string()),
        thread_label: Some("Alpha Chat".to_string()),
        binding_id: Some("bnd_chat_alpha".to_string()),
        plan_id: Some("plan_alpha_01".to_string()),
        plan_revision: Some(2),
        state: ObservationJobState::Running,
        approval_status: "approved".to_string(),
        execution_status: "running".to_string(),
        review_state: Some("pending_review".to_string()),
        updated_at_ms: Some(1_700_000_000_100),
        active_call_count: 1,
        tasks: vec![ObservationTask {
            id: "task_contract".to_string(),
            title: "Define contract".to_string(),
            status: "running".to_string(),
            depends_on: Vec::new(),
        }],
        summary: Some("Implementing shared Rust and TypeScript contract.".to_string()),
    };

    ObservationSnapshot {
        schema_version: OBSERVATION_SCHEMA_VERSION,
        generated_at_ms: 1_700_000_000_200,
        sources: vec![ObservationSource {
            id: "default".to_string(),
            label: "alpha-app".to_string(),
            project_path: Some("/home/dev/repos/alpha-app".to_string()),
            availability: ObservationSourceAvailability::Available,
            message: None,
        }],
        jobs: vec![job],
        requests: vec![request],
        next_cursor: None,
        has_more: false,
        jobs_has_more: false,
        warnings: Vec::new(),
        warning_contexts: Vec::new(),
    }
}

#[test]
fn legacy_observation_snapshot_and_actor_preserve_struct_literals_and_wire_compatibility() {
    let legacy = sample_legacy_observation_snapshot();
    let legacy_json = serde_json::to_string(&legacy).expect("serialize legacy ObservationSnapshot");

    // Optional selection fields are omitted when None so existing consumers see unchanged JSON.
    assert!(!legacy_json.contains("selected_model"));
    assert!(!legacy_json.contains("selected_effort"));
    assert!(!legacy_json.contains("native_samples"));

    let decoded_legacy: ObservationSnapshot =
        serde_json::from_str(&legacy_json).expect("round-trip legacy ObservationSnapshot");
    assert_eq!(decoded_legacy, legacy);

    // Upgrading a bare legacy ObservationSnapshot via parse_workspace_snapshot_json degrades
    // missing workspace-only fields safely to unknown / unavailable.
    let upgraded = parse_workspace_snapshot_json(&legacy_json)
        .expect("upgrade bare ObservationSnapshot into WorkspaceSnapshot");
    assert_eq!(upgraded.schema_version, WORKSPACE_SNAPSHOT_SCHEMA_VERSION);
    assert_eq!(upgraded.observation, legacy);
    assert_eq!(upgraded.native_samples.len(), 0);
    assert_eq!(
        upgraded.messages.availability,
        WorkspaceAvailabilityState::Unavailable
    );
    assert!(!upgraded.messages.requires_transcript);
    assert!(!upgraded.messages.allows_synthetic_user_messages);

    assert_eq!(upgraded.chats.len(), 1);
    assert_eq!(upgraded.chats[0].identity, sample_identity());
    assert_eq!(
        upgraded.chats[0].availability,
        WorkspaceAvailabilityState::Unavailable
    );
    assert_eq!(upgraded.chats[0].activity, WorkspaceActivityState::Unknown);
    assert_eq!(
        upgraded.chats[0].configured_selection.evidence,
        WorkspaceSelectionEvidence::Unavailable
    );
    assert!(
        !upgraded.chats[0]
            .configured_selection
            .counts_as_runtime_telemetry
    );

    assert_eq!(upgraded.job_histories.len(), 1);
    assert_eq!(
        upgraded.job_histories[0].acceptance_proof.status,
        WorkspaceAcceptanceStatus::Unknown
    );
}

#[test]
fn missing_and_unrecognized_workspace_fields_degrade_to_unknown_or_unavailable() {
    let partial_json = r#"{
        "generated_at_ms": 1700000005000,
        "sampling_policy": {
            "interval_ms": 0,
            "metadata_timeout_ms": 0,
            "stale_threshold_ms": 0,
            "snapshot_resets_sample_age": true
        },
        "chats": [
            {
                "identity": {
                    "source_id": "default",
                    "binding_id": "bnd_chat_alpha",
                    "thread_uuid": "0196ed00-0000-7000-8000-000000000042",
                    "canonical_project_scope": "/home/dev/repos/alpha-app"
                },
                "thread_label": "Alpha",
                "project_label": "alpha-app",
                "availability": "future_transport_mode",
                "activity": "quantum_thinking",
                "configured_selection": {
                    "selected_model": "gpt-6-astra",
                    "selected_effort": "high",
                    "evidence": "guessed_from_profile",
                    "counts_as_runtime_telemetry": true
                }
            }
        ],
        "native_samples": [
            {
                "chat_identity": {
                    "source_id": "default",
                    "binding_id": "bnd_chat_alpha",
                    "thread_uuid": "0196ed00-0000-7000-8000-000000000042",
                    "canonical_project_scope": "/home/dev/repos/alpha-app"
                },
                "producer_epoch": "epoch_01",
                "sequence": 4,
                "observed_at_ms": 1700000004000,
                "availability": "future_state",
                "activity": "working",
                "capabilities": {
                    "exact_thread_read": "experimental"
                }
            }
        ],
        "job_histories": [
            {
                "job": {
                    "source_id": "default",
                    "job_id": "job_01",
                    "state": "future_job_state"
                },
                "acceptance_proof": {
                    "status": "future_acceptance_state"
                }
            }
        ],
        "messages": {
            "availability": null,
            "requires_transcript": true,
            "allows_synthetic_user_messages": true,
            "items": [
                {
                    "item_id": "bad_1",
                    "source_kind": "raw_transcript",
                    "source_record_id": "rec_1",
                    "scope": { "source_id": "default" },
                    "summary": "Should be rejected because raw_transcript is forbidden."
                }
            ]
        }
    }"#;

    let parsed = parse_workspace_snapshot_json(partial_json).expect("parse sparse workspace json");

    assert_eq!(
        parsed.sampling_policy.interval_ms,
        WORKSPACE_SAMPLING_INTERVAL_MS
    );
    assert_eq!(
        parsed.sampling_policy.metadata_timeout_ms,
        WORKSPACE_METADATA_TIMEOUT_MS
    );
    assert_eq!(
        parsed.sampling_policy.stale_threshold_ms,
        WORKSPACE_SAMPLE_STALE_THRESHOLD_MS
    );
    assert!(!parsed.sampling_policy.snapshot_resets_sample_age);

    assert_eq!(parsed.chats.len(), 1);
    assert_eq!(
        parsed.chats[0].availability,
        WorkspaceAvailabilityState::Unavailable
    );
    assert_eq!(parsed.chats[0].activity, WorkspaceActivityState::Unknown);
    assert_eq!(
        parsed.chats[0].configured_selection,
        WorkspaceConfiguredSelection::unavailable()
    );

    assert_eq!(parsed.native_samples.len(), 1);
    assert_eq!(
        parsed.native_samples[0].availability,
        WorkspaceAvailabilityState::Unavailable
    );
    assert_eq!(
        parsed.native_samples[0].activity,
        WorkspaceActivityState::Unknown
    );
    assert_eq!(
        parsed.native_samples[0].capabilities.exact_thread_read,
        WorkspaceCapabilityState::Unknown
    );
    assert_eq!(
        parsed.native_samples[0].capabilities.activity_detail,
        WorkspaceCapabilityState::Unknown
    );

    assert_eq!(parsed.job_histories.len(), 1);
    assert_eq!(
        parsed.job_histories[0].job.state,
        ObservationJobState::Unknown
    );
    assert_eq!(
        parsed.job_histories[0].acceptance_proof.status,
        WorkspaceAcceptanceStatus::Unknown
    );

    assert_eq!(
        parsed.messages.availability,
        WorkspaceAvailabilityState::Unavailable
    );
    assert!(!parsed.messages.requires_transcript);
    assert!(!parsed.messages.allows_synthetic_user_messages);
    assert!(parsed.messages.items.is_empty());
    assert_eq!(parsed.messages.omitted_count, 1);
    assert!(parsed.messages.truncated);
}

#[test]
fn chat_identity_requires_verified_four_tuple_and_rejects_substitutes() {
    let id = sample_identity();
    let expected_key =
        "wchat:v1:7:default:14:bnd_chat_alpha:36:0196ed00-0000-7000-8000-000000000042:25:/home/dev/repos/alpha-app";
    assert_eq!(id.canonical_key().as_deref(), Some(expected_key));

    // Title, sessionId, and job_id/plan_id must never substitute for identity components.
    for bad_source in [
        "",
        "Alpha Chat",
        "sess_abc123",
        "sessionId:1",
        "job_42",
        "plan_7",
    ] {
        assert!(
            WorkspaceChatIdentity::new(
                bad_source,
                "bnd_chat_alpha",
                VALID_UUID_A,
                "/home/dev/repos/alpha-app",
            )
            .is_err(),
            "expected source_id `{bad_source}` to be rejected"
        );
    }

    for bad_binding in [
        "",
        "Alpha Chat",
        "sess_abc123",
        "session_99",
        "sessionId:xyz",
        "job_42",
        "plan_7",
        " bnd_trimmed ",
    ] {
        assert!(
            WorkspaceChatIdentity::new(
                "default",
                bad_binding,
                VALID_UUID_A,
                "/home/dev/repos/alpha-app",
            )
            .is_err(),
            "expected binding_id `{bad_binding}` to be rejected"
        );
    }

    for bad_uuid in [
        "",
        "Fix login bug",
        "sess_0196ed00",
        "job_0196ed00",
        "00000000-0000-0000-0000-000000000000",
        "0196ED00-0000-7000-8000-000000000042",
        "0196ed00000070008000000000000042",
    ] {
        assert!(
            WorkspaceChatIdentity::new(
                "default",
                "bnd_chat_alpha",
                bad_uuid,
                "/home/dev/repos/alpha-app",
            )
            .is_err(),
            "expected thread_uuid `{bad_uuid}` to be rejected"
        );
    }

    for bad_scope in [
        "",
        "repos/alpha-app",
        "/home/dev/repos/alpha-app/",
        "/home/dev//repos/alpha-app",
        "/home/dev/../repos/alpha-app",
        "/home/dev/repos/.",
        "/home/dev/sk-secret123456",
    ] {
        assert!(
            WorkspaceChatIdentity::new("default", "bnd_chat_alpha", VALID_UUID_A, bad_scope)
                .is_err(),
            "expected canonical_project_scope `{bad_scope}` to be rejected"
        );
    }

    // Renaming a chat title never alters canonical chat identity.
    let chat_before = WorkspaceChatSummary {
        identity: id.clone(),
        thread_label: "Initial Title".to_string(),
        project_label: "alpha-app".to_string(),
        availability: WorkspaceAvailabilityState::Available,
        activity: WorkspaceActivityState::Idle,
        configured_selection: WorkspaceConfiguredSelection::unavailable(),
        active_job_count: 0,
        attention_count: 0,
        updated_at_ms: Some(1_000),
    }
    .normalized();

    let chat_renamed = WorkspaceChatSummary {
        thread_label: "Renamed Conversational Thread".to_string(),
        active_job_count: 3,
        ..chat_before.clone()
    }
    .normalized();

    assert_eq!(
        chat_before.identity.canonical_key(),
        chat_renamed.identity.canonical_key()
    );
}

#[test]
fn native_sample_epoch_process_instance_and_monotonic_sequence_reconciliation() {
    let id = sample_identity();
    let proc_1 = Some(sample_process_instance(4242, 900_001));
    let proc_2 = Some(sample_process_instance(5151, 905_000));

    let s1 = sample_native_live(id.clone(), "epoch_a", proc_1.clone(), 1, 10_000);
    assert_eq!(
        reconcile_workspace_native_sample(None, &s1),
        WorkspaceSampleReconcileDecision::AcceptedInitial
    );

    let s2 = sample_native_live(id.clone(), "epoch_a", proc_1.clone(), 2, 12_000);
    assert_eq!(
        reconcile_workspace_native_sample(Some(&s1), &s2),
        WorkspaceSampleReconcileDecision::AcceptedNextSequence
    );

    // Duplicate and stale sequences within the same epoch are rejected.
    let s2_dup = sample_native_live(id.clone(), "epoch_a", proc_1.clone(), 2, 12_500);
    assert_eq!(
        reconcile_workspace_native_sample(Some(&s2), &s2_dup),
        WorkspaceSampleReconcileDecision::RejectedDuplicateSequence
    );

    let s1_stale = sample_native_live(id.clone(), "epoch_a", proc_1.clone(), 1, 13_000);
    assert_eq!(
        reconcile_workspace_native_sample(Some(&s2), &s1_stale),
        WorkspaceSampleReconcileDecision::RejectedStaleSequence
    );

    // Changing OS process instance without rotating producer_epoch is rejected.
    let mismatch_proc = sample_native_live(id.clone(), "epoch_a", proc_2.clone(), 3, 14_000);
    assert_eq!(
        reconcile_workspace_native_sample(Some(&s2), &mismatch_proc),
        WorkspaceSampleReconcileDecision::RejectedEpochProcessMismatch
    );

    // Rotating producer_epoch with an older timestamp is rejected as a stale epoch.
    let older_epoch = sample_native_live(id.clone(), "epoch_b", proc_2.clone(), 1, 11_000);
    assert_eq!(
        reconcile_workspace_native_sample(Some(&s2), &older_epoch),
        WorkspaceSampleReconcileDecision::RejectedStaleEpoch
    );

    // Rotating producer_epoch on backend restart resets sequence to 1 and is accepted at >= timestamp.
    let restarted_epoch = sample_native_live(id.clone(), "epoch_b", proc_2, 1, 14_000);
    assert_eq!(
        reconcile_workspace_native_sample(Some(&s2), &restarted_epoch),
        WorkspaceSampleReconcileDecision::AcceptedNewEpoch
    );

    // Different chat identity is rejected for single-slot reconciliation.
    let other_id = WorkspaceChatIdentity::new(
        "default",
        "bnd_chat_beta",
        VALID_UUID_B,
        "/home/dev/repos/alpha-app",
    )
    .expect("valid second identity");
    let other_chat_sample = sample_native_live(other_id, "epoch_b", proc_1.clone(), 2, 15_000);
    assert_eq!(
        reconcile_workspace_native_sample(Some(&restarted_epoch), &other_chat_sample),
        WorkspaceSampleReconcileDecision::RejectedMismatchedIdentity
    );

    // Invalid timestamp order (last_success_at_ms > observed_at_ms) is rejected.
    let mut invalid_ts = sample_native_live(id, "epoch_b", proc_1, 2, 15_000);
    invalid_ts.last_success_at_ms = Some(16_000);
    assert_eq!(
        reconcile_workspace_native_sample(Some(&restarted_epoch), &invalid_ts),
        WorkspaceSampleReconcileDecision::RejectedInvalidSample
    );
}

#[test]
fn native_sample_separates_activity_from_availability_and_labels_selection_as_configuration() {
    let id = sample_identity();
    let proc_inst = Some(sample_process_instance(4242, 900_001));

    for activity in [
        WorkspaceActivityState::Working,
        WorkspaceActivityState::Idle,
        WorkspaceActivityState::WaitingForApproval,
        WorkspaceActivityState::WaitingForInput,
        WorkspaceActivityState::Unknown,
    ] {
        let mut live = sample_native_live(id.clone(), "epoch_a", proc_inst.clone(), 1, 50_000);
        live.activity = activity;
        let eval = live.evaluate_at(52_000, WORKSPACE_SAMPLE_STALE_THRESHOLD_MS);
        assert_eq!(
            eval.effective_availability,
            WorkspaceAvailabilityState::Available
        );
        assert_eq!(eval.effective_activity, activity);
        assert_eq!(
            eval.effective_configured_selection.evidence,
            WorkspaceSelectionEvidence::VerifiedConfiguration
        );
        assert!(
            !eval
                .effective_configured_selection
                .counts_as_runtime_telemetry
        );
    }

    // Disconnected, Stale, and Unavailable availability states never leak a live working/idle claim
    // or stale configured model selection as current.
    for non_available in [
        WorkspaceAvailabilityState::Disconnected,
        WorkspaceAvailabilityState::Stale,
        WorkspaceAvailabilityState::Unavailable,
    ] {
        let mut degraded = sample_native_live(id.clone(), "epoch_a", proc_inst.clone(), 2, 50_000);
        degraded.availability = non_available;
        degraded.activity = WorkspaceActivityState::Working;
        let norm = degraded.normalized();
        assert_eq!(norm.availability, non_available);
        assert_eq!(norm.activity, WorkspaceActivityState::Unknown);
        assert_eq!(
            norm.configured_selection,
            WorkspaceConfiguredSelection::unavailable()
        );

        let eval = degraded.evaluate_at(52_000, WORKSPACE_SAMPLE_STALE_THRESHOLD_MS);
        assert_eq!(eval.effective_availability, non_available);
        assert_eq!(eval.effective_activity, WorkspaceActivityState::Unknown);
        assert_eq!(
            eval.effective_configured_selection,
            WorkspaceConfiguredSelection::unavailable()
        );
    }

    // Secret-like selection tokens are scrubbed and degrade evidence to Unavailable.
    let scrubbed = WorkspaceConfiguredSelection::verified(Some("sk-live-secret-123456"), None);
    assert_eq!(scrubbed, WorkspaceConfiguredSelection::unavailable());

    // ObservationActor configured selection never populates runtime-observed model/effort.
    let actor =
        ObservationActor::paired_coworker_with_selection("Alpha", Some("gpt-5.4"), Some("high"));
    assert_eq!(actor.model, None);
    assert_eq!(actor.effort, None);
    assert_eq!(actor.selected_model.as_deref(), Some("gpt-5.4"));
    assert_eq!(actor.selected_effort.as_deref(), Some("high"));
    assert_eq!(actor.evidence, ObservationActorEvidence::Configured);
}

#[test]
fn sampling_policy_enforces_2s_750ms_10s_and_snapshot_regeneration_never_resets_sample_age() {
    let policy = WorkspaceSamplingPolicy::default();
    assert_eq!(policy.interval_ms, 2_000);
    assert_eq!(policy.metadata_timeout_ms, 750);
    assert_eq!(policy.stale_threshold_ms, 10_000);
    assert!(!policy.snapshot_resets_sample_age);

    let id = sample_identity();
    let sample = sample_native_live(
        id.clone(),
        "epoch_live",
        Some(sample_process_instance(777, 12_345)),
        9,
        100_000,
    );

    let mut snap = WorkspaceSnapshot::new(110_000, sample_legacy_observation_snapshot());
    snap.native_samples = vec![sample];
    snap = snap.normalized();

    // At exactly 10,000 ms age (110_000 - 100_000 == 10_000), sample is still fresh and Available.
    assert_eq!(snap.chats.len(), 1);
    assert_eq!(
        snap.chats[0].availability,
        WorkspaceAvailabilityState::Available
    );
    assert_eq!(snap.chats[0].activity, WorkspaceActivityState::Working);
    assert_eq!(snap.native_samples[0].observed_at_ms, Some(100_000));
    assert_eq!(snap.native_samples[0].last_success_at_ms, Some(100_000));

    // Regenerating the snapshot 1 ms later (age = 10,001 ms > 10,000 ms) transitions chat availability
    // to Stale and activity to Unknown WITHOUT mutating sample timestamps.
    let snap_later = snap.regenerate_at(110_001);
    assert_eq!(snap_later.generated_at_ms, 110_001);
    assert_eq!(snap_later.native_samples[0].observed_at_ms, Some(100_000));
    assert_eq!(
        snap_later.native_samples[0].last_success_at_ms,
        Some(100_000)
    );
    assert_eq!(
        snap_later.chats[0].availability,
        WorkspaceAvailabilityState::Stale
    );
    assert_eq!(
        snap_later.chats[0].activity,
        WorkspaceActivityState::Unknown
    );
    assert_eq!(
        snap_later.chats[0].configured_selection,
        WorkspaceConfiguredSelection::unavailable()
    );
}

#[test]
fn job_history_separates_records_links_failures_attention_and_acceptance_proof() {
    let id = sample_identity();

    // Unbacked VerifiedAccepted claims (worker completed, but missing review/closure verdict IDs)
    // must downgrade to Unknown rather than treating worker completion as acceptance.
    let unbacked_task = WorkspaceTaskHistory {
        task_id: "task_impl".to_string(),
        title: "Implement envelope".to_string(),
        role: Some("coder".to_string()),
        difficulty: Some("hard".to_string()),
        stage: Some("verifying".to_string()),
        status: "completed".to_string(),
        depends_on: Vec::new(),
        current_attempt_no: 2,
        attempts: vec![
            WorkspaceTaskAttempt {
                attempt_no: 1,
                status: "failed".to_string(),
                run_id: Some("run_01".to_string()),
                worker_id: Some("w_01".to_string()),
                outbox_id: Some("outbox_01".to_string()),
                executor: None,
                started_at_ms: Some(1_000),
                finished_at_ms: Some(2_000),
                summary: Some("First attempt hit timeout.".to_string()),
            },
            WorkspaceTaskAttempt {
                attempt_no: 2,
                status: "completed".to_string(),
                run_id: Some("run_02".to_string()),
                worker_id: Some("w_02".to_string()),
                outbox_id: Some("outbox_02".to_string()),
                executor: None,
                started_at_ms: Some(3_000),
                finished_at_ms: Some(4_500),
                summary: Some("Worker exited 0 and produced candidate.".to_string()),
            },
        ],
        candidates: vec![WorkspaceTaskCandidate {
            candidate_id: "cand_02".to_string(),
            attempt_no: 2,
            worker_id: Some("w_02".to_string()),
            head_commit_sha: Some("abc1234def5678".to_string()),
            tree_sha: Some("tree9876543210".to_string()),
            verification_passed: Some(true),
            review_status: "pending".to_string(),
            review_verdict_id: None,
            review_receipt_id: None,
            produced_at_ms: Some(4_500),
            summary: Some("Candidate sealed awaiting Reviewer.".to_string()),
        }],
        active_candidate_id: Some("cand_02".to_string()),
        accepted_candidate_id: None,
        historical_failure: Some(WorkspaceHistoricalFailure {
            failure_id: Some("fail_01".to_string()),
            stage: "worker_execution".to_string(),
            failed_at_ms: Some(2_000),
            task_id: Some("task_impl".to_string()),
            attempt_no: Some(1),
            run_id: Some("run_01".to_string()),
            outbox_id: Some("outbox_01".to_string()),
            reason: "Attempt 1 timed out before producing candidate.".to_string(),
            superseded_or_resolved: true,
        }),
        current_attention: Some(WorkspaceAttentionState {
            kind: WorkspaceAttentionKind::ReviewRequired,
            reason: "Candidate cand_02 requires Reviewer verdict.".to_string(),
            next_step: Some("Dispatch Reviewer on cand_02.".to_string()),
            blocking_hold_ids: Vec::new(),
            blocking_outbox_ids: Vec::new(),
            question_id: None,
            updated_at_ms: Some(4_600),
        }),
        acceptance_proof: WorkspaceAcceptanceProof {
            status: WorkspaceAcceptanceStatus::VerifiedAccepted,
            accepted_candidate_id: Some("cand_02".to_string()),
            // Missing review_verdict_id and review_receipt_id!
            ..WorkspaceAcceptanceProof::unknown()
        },
    };

    let unbacked_history = WorkspaceJobHistory {
        job: WorkspaceJobSummary {
            source_id: "default".to_string(),
            job_id: "job_alpha_01".to_string(),
            binding_id: Some("bnd_chat_alpha".to_string()),
            chat_identity: Some(id.clone()),
            title: "Workspace Contract Job".to_string(),
            job_revision: Some(7),
            state: ObservationJobState::Reviewing,
            phase: Some("integrating".to_string()),
            approval_status: "approved".to_string(),
            execution_status: "ready_for_review".to_string(),
            review_state: Some("pending_review".to_string()),
            created_at_ms: Some(900),
            updated_at_ms: Some(4_700),
            summary: Some("Awaiting verified review and closure.".to_string()),
        },
        tasks: vec![unbacked_task],
        runs: vec![WorkspaceRunRecord {
            run_id: "run_02".to_string(),
            worker_id: Some("w_02".to_string()),
            binding_id: "bnd_chat_alpha".to_string(),
            task_id: Some("task_impl".to_string()),
            attempt_no: Some(2),
            role: Some("coder".to_string()),
            status: "completed".to_string(),
            executor: None,
            started_at_ms: Some(3_000),
            finished_at_ms: Some(4_500),
            updated_at_ms: Some(4_500),
        }],
        outbox: vec![WorkspaceOutboxRecord {
            outbox_id: "outbox_02".to_string(),
            request_key: Some("req_coder_attempt_2".to_string()),
            kind: "coder_dispatch".to_string(),
            status: "completed".to_string(),
            task_id: Some("task_impl".to_string()),
            attempt_no: Some(2),
            role: Some("coder".to_string()),
            run_id: Some("run_02".to_string()),
            worker_id: Some("w_02".to_string()),
            receipt_id: Some("rcpt_02".to_string()),
            reserved_at_job_revision: Some(5),
            updated_at_job_revision: Some(6),
            diagnostic: None,
        }],
        combined_candidates: vec![WorkspaceCombinedCandidate {
            combined_candidate_id: "ccand_01".to_string(),
            job_revision: Some(7),
            status: "ready_for_closure".to_string(),
            final_commit_sha: Some("fedcba987654".to_string()),
            final_tree_sha: Some("112233445566".to_string()),
            candidate_digest: Some("sha256_digest_01".to_string()),
            closure_reservation_id: Some("clos_res_01".to_string()),
            check_count: 4,
            produced_at_ms: Some(4_650),
        }],
        result: Some(WorkspaceJobResult {
            outcome: "ready_for_review".to_string(),
            summary: "Candidate integrated; awaiting Closure verdict.".to_string(),
            produced_at_ms: Some(4_650),
            delivery_stage: ObservationDeliveryStage::ResultReady,
            closure_verdict_id: None,
        }),
        links: WorkspaceRecordLinks {
            source_plan_id: Some("plan_alpha_01".to_string()),
            source_plan_revision: Some(2),
            source_approval_id: Some("apr_alpha_01".to_string()),
            source_work_brief_id: Some("brief_alpha_01".to_string()),
            planning_grant_id: Some("grant_01".to_string()),
            planning_reservation_id: Some("pres_01".to_string()),
            supersedes_job_id: Some("job_alpha_00".to_string()),
            superseded_by_job_id: None,
            parent_job_id: None,
            follow_up_job_ids: vec!["job_alpha_02".to_string()],
            architect_advice_ids: vec!["adv_01".to_string()],
            source_request_ids: vec!["obsreq_01".to_string()],
        },
        historical_failure: Some(WorkspaceHistoricalFailure {
            failure_id: Some("fail_01".to_string()),
            stage: "worker_execution".to_string(),
            failed_at_ms: Some(2_000),
            task_id: Some("task_impl".to_string()),
            attempt_no: Some(1),
            run_id: Some("run_01".to_string()),
            outbox_id: Some("outbox_01".to_string()),
            reason: "Attempt 1 failed prior to retry.".to_string(),
            superseded_or_resolved: true,
        }),
        current_attention: Some(WorkspaceAttentionState {
            kind: WorkspaceAttentionKind::ReviewRequired,
            reason: "Awaiting Reviewer and Closure verdicts.".to_string(),
            next_step: Some("Run closing review.".to_string()),
            blocking_hold_ids: Vec::new(),
            blocking_outbox_ids: Vec::new(),
            question_id: None,
            updated_at_ms: Some(4_700),
        }),
        acceptance_proof: WorkspaceAcceptanceProof {
            status: WorkspaceAcceptanceStatus::VerifiedAccepted,
            combined_candidate_id: Some("ccand_01".to_string()),
            // Missing closure_verdict_id!
            ..WorkspaceAcceptanceProof::unknown()
        },
        truncation: WorkspaceJobHistoryTruncation::default(),
    };

    let norm_unbacked = unbacked_history.normalized();
    assert_eq!(norm_unbacked.tasks[0].current_attempt_no, 2);
    assert_eq!(norm_unbacked.tasks[0].attempts.len(), 2);
    assert!(
        norm_unbacked
            .historical_failure
            .as_ref()
            .expect("historical_failure preserved")
            .superseded_or_resolved
    );
    assert_eq!(
        norm_unbacked
            .current_attention
            .as_ref()
            .expect("current_attention preserved")
            .kind,
        WorkspaceAttentionKind::ReviewRequired
    );
    assert_eq!(
        norm_unbacked.tasks[0].acceptance_proof.status,
        WorkspaceAcceptanceStatus::Unknown
    );
    assert_eq!(
        norm_unbacked.acceptance_proof.status,
        WorkspaceAcceptanceStatus::Unknown
    );

    // Supplying verified review and closure verdict receipts preserves VerifiedAccepted.
    let mut backed = unbacked_history;
    backed.tasks[0].acceptance_proof.review_verdict_id = Some("rverd_01".to_string());
    backed.tasks[0].acceptance_proof.verified_at_ms = Some(4_800);
    backed.acceptance_proof.closure_verdict_id = Some("cverd_01".to_string());
    backed.acceptance_proof.verified_at_ms = Some(4_900);

    let norm_backed = backed.normalized();
    assert_eq!(
        norm_backed.tasks[0].acceptance_proof.status,
        WorkspaceAcceptanceStatus::VerifiedAccepted
    );
    assert_eq!(
        norm_backed.acceptance_proof.status,
        WorkspaceAcceptanceStatus::VerifiedAccepted
    );
}

#[test]
fn messages_projection_accepts_only_safe_sources_and_preserves_order_scope_and_completeness() {
    for allowed in [
        "work_brief_summary",
        "plan_summary",
        "question_prompt",
        "question_answer",
        "task_result_summary",
        "review_verdict_summary",
        "closure_summary",
        "job_result_summary",
        "attention_summary",
        "journal_summary",
    ] {
        assert!(
            validate_safe_workspace_message_source(allowed).is_ok(),
            "expected `{allowed}` to be permitted"
        );
    }

    for forbidden in [
        "transcript",
        "raw_transcript",
        "provider_transcript",
        "synthetic_user_message",
        "user_chat_turn",
        "unknown",
        "",
    ] {
        assert!(
            matches!(
                validate_safe_workspace_message_source(forbidden),
                Err(WorkspaceContractError::ForbiddenMessageSource(_))
            ),
            "expected `{forbidden}` to be rejected"
        );
    }

    let full_scope = WorkspaceMessageScope {
        source_id: "default".to_string(),
        binding_id: Some("bnd_chat_alpha".to_string()),
        thread_uuid: Some(VALID_UUID_A.to_string()),
        canonical_project_scope: Some("/home/dev/repos/alpha-app".to_string()),
        chat_identity: None,
        job_id: Some("job_alpha_01".to_string()),
        task_id: Some("task_impl".to_string()),
        run_id: Some("run_02".to_string()),
        attempt_no: Some(2),
    };

    let partial_scope = WorkspaceMessageScope {
        source_id: "default".to_string(),
        binding_id: None,
        thread_uuid: None,
        canonical_project_scope: None,
        chat_identity: None,
        job_id: Some("job_alpha_01".to_string()),
        task_id: None,
        run_id: None,
        attempt_no: None,
    };

    let long_summary = "A".repeat(260);
    let window = project_safe_workspace_messages(vec![
        WorkspaceMessageCandidateInput {
            source_kind: "question_answer".to_string(),
            source_record_id: "q_01".to_string(),
            source_order: WorkspaceMessageSourceOrder {
                recorded_at_ms: Some(2_000),
                source_seq: 10,
                sub_index: 1,
            },
            scope: full_scope.clone(),
            actor_role: Some("operator".to_string()),
            raw_summary: "Approved scoped implementation bounds.".to_string(),
            upstream_truncated: false,
            incomplete_reason: None,
        },
        WorkspaceMessageCandidateInput {
            source_kind: "question_prompt".to_string(),
            source_record_id: "q_01".to_string(),
            source_order: WorkspaceMessageSourceOrder {
                recorded_at_ms: Some(2_000),
                source_seq: 10,
                sub_index: 0,
            },
            scope: full_scope,
            actor_role: Some("orchestrator".to_string()),
            raw_summary: long_summary,
            upstream_truncated: false,
            incomplete_reason: None,
        },
        WorkspaceMessageCandidateInput {
            source_kind: "closure_summary".to_string(),
            source_record_id: "cverd_01".to_string(),
            source_order: WorkspaceMessageSourceOrder {
                recorded_at_ms: None,
                source_seq: 1,
                sub_index: 0,
            },
            scope: partial_scope,
            actor_role: Some("closure".to_string()),
            raw_summary: "Authorization: Bearer super-secret-token".to_string(),
            upstream_truncated: true,
            incomplete_reason: Some("Missing live binding scope.".to_string()),
        },
    ])
    .expect("project safe messages");

    assert_eq!(window.availability, WorkspaceAvailabilityState::Available);
    assert!(!window.requires_transcript);
    assert!(!window.allows_synthetic_user_messages);
    assert_eq!(window.items.len(), 3);

    // Items without recorded_at_ms sort first (with missing_fields recorded), followed by
    // deterministic (recorded_at_ms, source_seq, sub_index) ordering.
    assert_eq!(
        window.items[0].source_kind,
        WorkspaceMessageSourceKind::ClosureSummary
    );
    assert!(window.items[0].completeness.redacted);
    assert_eq!(window.items[0].summary, "[redacted sensitive metadata]");
    assert!(window.items[0].completeness.summary_truncated);
    assert_eq!(window.items[0].scope.chat_identity, None);
    let missing = &window.items[0].completeness.missing_fields;
    assert!(missing.contains(&"binding_id".to_string()));
    assert!(missing.contains(&"recorded_at_ms".to_string()));

    assert_eq!(
        window.items[1].source_kind,
        WorkspaceMessageSourceKind::QuestionPrompt
    );
    assert_eq!(
        window.items[1].item_id,
        "wmsg:v1:7:default:15:question_prompt:4:q_01:0"
    );
    assert_eq!(window.items[1].scope.chat_identity, Some(sample_identity()));
    assert!(window.items[1].completeness.summary_truncated);
    assert_eq!(window.items[1].completeness.original_char_count, Some(260));
    assert_eq!(
        window.items[1].summary.chars().count(),
        MAX_OBSERVATION_SUMMARY_CHARS
    );

    assert_eq!(
        window.items[2].source_kind,
        WorkspaceMessageSourceKind::QuestionAnswer
    );
    assert_eq!(
        window.items[2].item_id,
        "wmsg:v1:7:default:15:question_answer:4:q_01:1"
    );
    assert!(!window.items[2].completeness.summary_truncated);
}

#[test]
fn unicode_boundaries_preserve_valid_utf8_and_exact_scalar_and_byte_counts() {
    // Multi-byte Turkish, CJK, and 4-byte emoji characters at exact 240-char and 245-char boundaries.
    let exact_240: String = "🦀".repeat(240);
    let exact_meta = sanitize_workspace_summary_with_meta(&exact_240);
    assert!(!exact_meta.truncated);
    assert_eq!(exact_meta.original_char_count, Some(240));
    assert_eq!(exact_meta.summary.chars().count(), 240);

    let over_245 = format!("ÖÜŞİĞÇöüşığç日本{}", "🦀".repeat(235));
    assert_eq!(over_245.chars().count(), 249);
    let over_meta = sanitize_workspace_summary_with_meta(&over_245);
    assert!(over_meta.truncated);
    assert_eq!(over_meta.original_char_count, Some(249));
    assert_eq!(over_meta.summary.chars().count(), 240);
    assert!(over_meta.summary.ends_with("..."));

    // Canonical key prefixes exact UTF-8 byte lengths (not UTF-16 code units or scalar count).
    let unicode_path = "/home/sinan/çalışma/🦀-proje";
    assert_eq!(unicode_path.len(), 33);
    let unicode_id = WorkspaceChatIdentity::new("default", "bnd_utf8", VALID_UUID_A, unicode_path)
        .expect("valid UTF-8 canonical project scope");
    let expected_unicode_key =
        "wchat:v1:7:default:8:bnd_utf8:36:0196ed00-0000-7000-8000-000000000042:33:/home/sinan/çalışma/🦀-proje";
    assert_eq!(
        unicode_id.canonical_key().as_deref(),
        Some(expected_unicode_key)
    );
}

#[test]
fn numeric_bounds_enforce_existing_and_new_ledger_and_sidecar_capacities() {
    // Verify pinned contract constants.
    assert_eq!(DEFAULT_OBSERVATION_REQUEST_PAGE_LIMIT, 50);
    assert_eq!(MAX_OBSERVATION_REQUEST_PAGE_LIMIT, 200);
    assert_eq!(MAX_OBSERVATION_JOBS, 500);
    assert_eq!(MAX_OBSERVATION_FRONTEND_REQUEST_WINDOW, 1_000);
    assert_eq!(MAX_OBSERVATION_SUMMARY_CHARS, 240);
    assert_eq!(MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES, 65_536);
    assert_eq!(MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE, 128);
    assert_eq!(MAX_WORKSPACE_MESSAGES_WINDOW, 1_000);
    assert_eq!(MAX_WORKSPACE_JOBS_PER_BINDING, 64);
    assert_eq!(MAX_WORKSPACE_TASKS_PER_JOB, 64);
    assert_eq!(MAX_WORKSPACE_ATTEMPTS_PER_TASK, 16);
    assert_eq!(MAX_WORKSPACE_CANDIDATES_PER_TASK, 16);
    assert_eq!(MAX_WORKSPACE_RUNS_PER_JOB, 256);
    assert_eq!(MAX_WORKSPACE_OUTBOX_PER_JOB, 256);
    assert_eq!(MAX_WORKSPACE_COMBINED_CANDIDATES_PER_JOB, 32);

    // 1. Native sample sidecar 64 KiB byte cap.
    let valid_sample = sample_native_live(
        sample_identity(),
        "epoch_sidecar",
        Some(sample_process_instance(100, 200)),
        1,
        5_000,
    );
    let sidecar_bytes =
        serialize_workspace_native_sample_sidecar(&valid_sample).expect("serialize 64 KiB sidecar");
    assert!(sidecar_bytes.len() <= MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES);
    let parsed_sidecar =
        parse_workspace_native_sample_sidecar(&sidecar_bytes).expect("parse valid sidecar");
    assert_eq!(parsed_sidecar.sequence, 1);

    let oversized_bytes = vec![b' '; MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES + 1];
    assert!(matches!(
        parse_workspace_native_sample_sidecar(&oversized_bytes),
        Err(WorkspaceContractError::SidecarTooLarge {
            actual_bytes: 65_537,
            max_bytes: 65_536,
        })
    ));

    // 2. Max 128 native samples per source.
    let mut many_samples = Vec::new();
    for idx in 0..130u32 {
        let uuid = format!("0196ed00-0000-7000-8000-{:012x}", idx + 1);
        let id = WorkspaceChatIdentity::new(
            "default",
            &format!("bnd_{idx}"),
            &uuid,
            "/home/dev/repos/alpha-app",
        )
        .expect("identity");
        many_samples.push(sample_native_live(
            id,
            "epoch_cap",
            None,
            u64::from(idx) + 1,
            10_000 + i64::from(idx),
        ));
    }
    let second_source_id = WorkspaceChatIdentity::new(
        "ctx_secondary",
        "bnd_sec_1",
        VALID_UUID_B,
        "/home/dev/repos/beta-app",
    )
    .expect("secondary identity");
    many_samples.push(sample_native_live(
        second_source_id,
        "epoch_cap",
        None,
        1,
        20_000,
    ));

    let (retained_samples, omitted_samples) =
        bound_workspace_native_samples_per_source(many_samples);
    assert_eq!(retained_samples.len(), 129); // 128 for `default` + 1 for `ctx_secondary`
    assert_eq!(omitted_samples, 2);

    // 3. Max 1,000 items in Messages window.
    let mut msg_inputs = Vec::new();
    for seq in 0..1_005u64 {
        msg_inputs.push(WorkspaceMessageCandidateInput {
            source_kind: "journal_summary".to_string(),
            source_record_id: format!("obsrec_{seq}"),
            source_order: WorkspaceMessageSourceOrder {
                recorded_at_ms: Some(1_000 + i64::try_from(seq).expect("i64")),
                source_seq: seq,
                sub_index: 0,
            },
            scope: WorkspaceMessageScope {
                source_id: "default".to_string(),
                ..WorkspaceMessageScope::default()
            },
            actor_role: None,
            raw_summary: format!("Journal entry {seq}"),
            upstream_truncated: false,
            incomplete_reason: None,
        });
    }
    let bounded_msgs = project_safe_workspace_messages(msg_inputs).expect("project messages");
    assert_eq!(bounded_msgs.items.len(), 1_000);
    assert_eq!(bounded_msgs.omitted_count, 5);
    assert!(bounded_msgs.has_more);
    assert!(bounded_msgs.truncated);
    assert_eq!(bounded_msgs.items[0].source_record_id, "obsrec_5");
    assert_eq!(bounded_msgs.items[999].source_record_id, "obsrec_1004");

    // 4. Max 64 jobs/binding and nested 64 task/job, 16 attempt/task, 16 candidate/task,
    //    256 run/job, 256 outbox/job, 32 combined_candidate/job bounds.
    let oversized_task = WorkspaceTaskHistory {
        task_id: "task_0".to_string(),
        title: "Oversized task".to_string(),
        status: "running".to_string(),
        attempts: (1..=20)
            .map(|n| WorkspaceTaskAttempt {
                attempt_no: n,
                status: "completed".to_string(),
                ..WorkspaceTaskAttempt::default()
            })
            .collect(),
        candidates: (1..=19)
            .map(|n| WorkspaceTaskCandidate {
                candidate_id: format!("cand_{n}"),
                attempt_no: n,
                review_status: "pending".to_string(),
                ..WorkspaceTaskCandidate::default()
            })
            .collect(),
        ..WorkspaceTaskHistory::default()
    };

    let mut tasks = vec![oversized_task];
    for idx in 1..70 {
        tasks.push(WorkspaceTaskHistory {
            task_id: format!("task_{idx}"),
            title: format!("Task {idx}"),
            status: "pending".to_string(),
            ..WorkspaceTaskHistory::default()
        });
    }

    let oversized_history = WorkspaceJobHistory {
        job: WorkspaceJobSummary {
            source_id: "default".to_string(),
            job_id: "job_oversized".to_string(),
            binding_id: Some("bnd_chat_alpha".to_string()),
            title: "Oversized Job".to_string(),
            updated_at_ms: Some(99_000),
            ..WorkspaceJobSummary::default()
        },
        tasks,
        runs: (0..260)
            .map(|n| WorkspaceRunRecord {
                run_id: format!("run_{n}"),
                binding_id: "bnd_chat_alpha".to_string(),
                status: "completed".to_string(),
                ..WorkspaceRunRecord::default()
            })
            .collect(),
        outbox: (0..265)
            .map(|n| WorkspaceOutboxRecord {
                outbox_id: format!("outbox_{n}"),
                kind: "coder_dispatch".to_string(),
                status: "completed".to_string(),
                ..WorkspaceOutboxRecord::default()
            })
            .collect(),
        combined_candidates: (0..35)
            .map(|n| WorkspaceCombinedCandidate {
                combined_candidate_id: format!("ccand_{n}"),
                status: "ready_for_closure".to_string(),
                ..WorkspaceCombinedCandidate::default()
            })
            .collect(),
        ..WorkspaceJobHistory::default()
    }
    .normalized();

    assert_eq!(oversized_history.tasks.len(), 64);
    assert_eq!(oversized_history.tasks[0].attempts.len(), 16);
    assert_eq!(oversized_history.tasks[0].attempts[0].attempt_no, 5);
    assert_eq!(oversized_history.tasks[0].candidates.len(), 16);
    assert_eq!(
        oversized_history.tasks[0].candidates[0].candidate_id,
        "cand_4"
    );
    assert_eq!(oversized_history.runs.len(), 256);
    assert_eq!(oversized_history.outbox.len(), 256);
    assert_eq!(oversized_history.combined_candidates.len(), 32);
    assert_eq!(oversized_history.truncation.tasks_omitted, 6);
    assert_eq!(oversized_history.truncation.attempts_omitted, 4);
    assert_eq!(oversized_history.truncation.candidates_omitted, 3);
    assert_eq!(oversized_history.truncation.runs_omitted, 4);
    assert_eq!(oversized_history.truncation.outbox_omitted, 9);
    assert_eq!(oversized_history.truncation.combined_candidates_omitted, 3);

    let mut per_binding_jobs = Vec::new();
    for idx in 0..70 {
        per_binding_jobs.push(WorkspaceJobHistory {
            job: WorkspaceJobSummary {
                source_id: "default".to_string(),
                job_id: format!("job_b_{idx}"),
                binding_id: Some("bnd_chat_alpha".to_string()),
                title: format!("Job {idx}"),
                updated_at_ms: Some(1_000 + i64::from(idx)),
                ..WorkspaceJobSummary::default()
            },
            ..WorkspaceJobHistory::default()
        });
    }
    let (bounded_jobs, jobs_omitted) = bound_workspace_job_histories(per_binding_jobs);
    assert_eq!(bounded_jobs.len(), 64);
    assert_eq!(jobs_omitted, 6);

    // 5. Existing 200 requests/page and 500 jobs caps on embedded ObservationSnapshot.
    let mut obs = sample_legacy_observation_snapshot();
    let template_req = obs.requests[0].clone();
    let template_job = obs.jobs[0].clone();
    obs.requests = (0..205)
        .map(|idx| {
            let mut r = template_req.clone();
            r.id = format!("obsreq_{idx}");
            r
        })
        .collect();
    obs.jobs = (0..502)
        .map(|idx| {
            let mut j = template_job.clone();
            j.id = format!("plan_{idx}");
            j
        })
        .collect();

    let capped_ws = WorkspaceSnapshot::new(200_000, obs);
    assert_eq!(capped_ws.observation.requests.len(), 200);
    assert!(capped_ws.observation.has_more);
    assert!(capped_ws.bounds.requests_capped);
    assert_eq!(capped_ws.observation.jobs.len(), 500);
    assert!(capped_ws.observation.jobs_has_more);
    assert!(capped_ws.bounds.jobs_capped);
}

#[test]
fn canonical_golden_workspace_snapshot_matches_typescript_wire_contract() {
    // Shared golden JSON fixture mirrored 1:1 in `apps/desktop/src/workspace-contract.test.ts`.
    let golden_json = r#"{
        "schema_version": 1,
        "generated_at_ms": 1700000010000,
        "sampling_policy": {
            "interval_ms": 2000,
            "metadata_timeout_ms": 750,
            "stale_threshold_ms": 10000,
            "snapshot_resets_sample_age": false
        },
        "observation": {
            "schema_version": 1,
            "generated_at_ms": 1700000010000,
            "sources": [
                {
                    "id": "default",
                    "label": "alpha-app",
                    "project_path": "/home/dev/repos/alpha-app",
                    "availability": "available",
                    "message": null
                }
            ],
            "jobs": [],
            "requests": [],
            "next_cursor": null,
            "has_more": false,
            "jobs_has_more": false,
            "warnings": []
        },
        "chats": [
            {
                "identity": {
                    "source_id": "default",
                    "binding_id": "bnd_chat_alpha",
                    "thread_uuid": "0196ed00-0000-7000-8000-000000000042",
                    "canonical_project_scope": "/home/dev/repos/alpha-app"
                },
                "thread_label": "Alpha Chat",
                "project_label": "alpha-app",
                "availability": "available",
                "activity": "waiting_for_input",
                "configured_selection": {
                    "selected_model": "gpt-5.4",
                    "selected_effort": "high",
                    "evidence": "verified_configuration",
                    "counts_as_runtime_telemetry": false
                },
                "active_job_count": 1,
                "attention_count": 1,
                "updated_at_ms": 1700000008000
            }
        ],
        "native_samples": [
            {
                "chat_identity": {
                    "source_id": "default",
                    "binding_id": "bnd_chat_alpha",
                    "thread_uuid": "0196ed00-0000-7000-8000-000000000042",
                    "canonical_project_scope": "/home/dev/repos/alpha-app"
                },
                "producer_epoch": "epoch_golden_01",
                "backend_process_instance": {
                    "pid": 4242,
                    "boot_id": "boot-linux-01",
                    "start_ticks": 900001
                },
                "sequence": 7,
                "observed_at_ms": 1700000008000,
                "last_success_at_ms": 1700000008000,
                "availability": "available",
                "activity": "waiting_for_input",
                "connection": {
                    "state": "connected",
                    "freshness": "fresh",
                    "route": "managed_linux_desktop",
                    "diagnostic": null
                },
                "capabilities": {
                    "exact_thread_read": "supported",
                    "activity_detail": "supported",
                    "configured_selection": "supported",
                    "wakeup_delivery": "supported",
                    "per_call_identity": "supported"
                },
                "configured_selection": {
                    "selected_model": "gpt-5.4",
                    "selected_effort": "high",
                    "evidence": "verified_configuration",
                    "counts_as_runtime_telemetry": false
                }
            }
        ],
        "job_histories": [
            {
                "job": {
                    "source_id": "default",
                    "job_id": "job_golden_01",
                    "binding_id": "bnd_chat_alpha",
                    "chat_identity": {
                        "source_id": "default",
                        "binding_id": "bnd_chat_alpha",
                        "thread_uuid": "0196ed00-0000-7000-8000-000000000042",
                        "canonical_project_scope": "/home/dev/repos/alpha-app"
                    },
                    "title": "Golden Contract Package",
                    "job_revision": 4,
                    "state": "completed",
                    "phase": "completed",
                    "approval_status": "approved",
                    "execution_status": "accepted",
                    "review_state": "review_passed",
                    "created_at_ms": 1700000001000,
                    "updated_at_ms": 1700000007500,
                    "summary": "Verified shared contract."
                },
                "tasks": [
                    {
                        "task_id": "task_golden_01",
                        "title": "Define WorkspaceSnapshot",
                        "role": "coder",
                        "difficulty": "hard",
                        "stage": "accepted",
                        "status": "accepted",
                        "depends_on": [],
                        "current_attempt_no": 1,
                        "attempts": [
                            {
                                "attempt_no": 1,
                                "status": "completed",
                                "run_id": "run_golden_01",
                                "worker_id": "w_golden_01",
                                "outbox_id": "outbox_golden_01",
                                "executor": null,
                                "started_at_ms": 1700000002000,
                                "finished_at_ms": 1700000005000,
                                "summary": "Implemented contract types."
                            }
                        ],
                        "candidates": [
                            {
                                "candidate_id": "cand_golden_01",
                                "attempt_no": 1,
                                "worker_id": "w_golden_01",
                                "head_commit_sha": "111122223333",
                                "tree_sha": "444455556666",
                                "verification_passed": true,
                                "review_status": "accepted",
                                "review_verdict_id": "rverd_golden_01",
                                "review_receipt_id": "rrcpt_golden_01",
                                "produced_at_ms": 1700000005000,
                                "summary": "Candidate passed checks."
                            }
                        ],
                        "active_candidate_id": null,
                        "accepted_candidate_id": "cand_golden_01",
                        "historical_failure": null,
                        "current_attention": null,
                        "acceptance_proof": {
                            "status": "verified_accepted",
                            "accepted_candidate_id": "cand_golden_01",
                            "combined_candidate_id": null,
                            "review_verdict_id": "rverd_golden_01",
                            "review_receipt_id": "rrcpt_golden_01",
                            "integration_receipt_id": null,
                            "closure_reservation_id": null,
                            "closure_verdict_id": null,
                            "verified_at_ms": 1700000006000
                        }
                    }
                ],
                "runs": [],
                "outbox": [],
                "combined_candidates": [
                    {
                        "combined_candidate_id": "ccand_golden_01",
                        "job_revision": 4,
                        "status": "accepted",
                        "final_commit_sha": "777788889999",
                        "final_tree_sha": "aaaabbbbcccc",
                        "candidate_digest": "digest_golden_01",
                        "closure_reservation_id": "clos_res_golden_01",
                        "check_count": 3,
                        "produced_at_ms": 1700000006500
                    }
                ],
                "result": {
                    "outcome": "completed",
                    "summary": "Workspace contract accepted by Closure.",
                    "produced_at_ms": 1700000007500,
                    "delivery_stage": "result_ready",
                    "closure_verdict_id": "cverd_golden_01"
                },
                "links": {
                    "source_plan_id": "plan_golden_01",
                    "source_plan_revision": 1,
                    "source_approval_id": "apr_golden_01",
                    "source_work_brief_id": "brief_golden_01",
                    "planning_grant_id": null,
                    "planning_reservation_id": null,
                    "supersedes_job_id": null,
                    "superseded_by_job_id": null,
                    "parent_job_id": null,
                    "follow_up_job_ids": [],
                    "architect_advice_ids": [],
                    "source_request_ids": []
                },
                "historical_failure": null,
                "current_attention": null,
                "acceptance_proof": {
                    "status": "verified_accepted",
                    "accepted_candidate_id": null,
                    "combined_candidate_id": "ccand_golden_01",
                    "review_verdict_id": null,
                    "review_receipt_id": null,
                    "integration_receipt_id": "ircpt_golden_01",
                    "closure_reservation_id": "clos_res_golden_01",
                    "closure_verdict_id": "cverd_golden_01",
                    "verified_at_ms": 1700000007500
                },
                "truncation": {
                    "tasks_omitted": 0,
                    "attempts_omitted": 0,
                    "candidates_omitted": 0,
                    "runs_omitted": 0,
                    "outbox_omitted": 0,
                    "combined_candidates_omitted": 0
                }
            }
        ],
        "messages": {
            "availability": "available",
            "items": [
                {
                    "item_id": "wmsg:v1:7:default:15:closure_summary:15:cverd_golden_01:0",
                    "source_kind": "closure_summary",
                    "source_record_id": "cverd_golden_01",
                    "source_order": {
                        "recorded_at_ms": 1700000007500,
                        "source_seq": 4,
                        "sub_index": 0
                    },
                    "scope": {
                        "source_id": "default",
                        "binding_id": "bnd_chat_alpha",
                        "thread_uuid": "0196ed00-0000-7000-8000-000000000042",
                        "canonical_project_scope": "/home/dev/repos/alpha-app",
                        "chat_identity": {
                            "source_id": "default",
                            "binding_id": "bnd_chat_alpha",
                            "thread_uuid": "0196ed00-0000-7000-8000-000000000042",
                            "canonical_project_scope": "/home/dev/repos/alpha-app"
                        },
                        "job_id": "job_golden_01",
                        "task_id": null,
                        "run_id": null,
                        "attempt_no": null
                    },
                    "actor_role": "closure",
                    "summary": "Workspace contract accepted by Closure.",
                    "completeness": {
                        "summary_truncated": false,
                        "original_char_count": 39,
                        "max_summary_chars": 240,
                        "redacted": false,
                        "missing_fields": [],
                        "incomplete_reason": null
                    }
                }
            ],
            "max_items": 1000,
            "omitted_count": 0,
            "has_more": false,
            "truncated": false,
            "requires_transcript": false,
            "allows_synthetic_user_messages": false
        },
        "bounds": {
            "native_samples_omitted": 0,
            "job_histories_omitted": 0,
            "messages_omitted": 0,
            "requests_capped": false,
            "jobs_capped": false
        }
    }"#;

    let snap = parse_workspace_snapshot_json(golden_json).expect("parse golden WorkspaceSnapshot");
    assert_eq!(snap.schema_version, 1);
    assert_eq!(snap.chats.len(), 1);
    let expected_golden_key =
        "wchat:v1:7:default:14:bnd_chat_alpha:36:0196ed00-0000-7000-8000-000000000042:25:/home/dev/repos/alpha-app";
    assert_eq!(
        snap.chats[0].identity.canonical_key().as_deref(),
        Some(expected_golden_key)
    );
    assert_eq!(
        snap.chats[0].availability,
        WorkspaceAvailabilityState::Available
    );
    assert_eq!(
        snap.chats[0].activity,
        WorkspaceActivityState::WaitingForInput
    );
    assert_eq!(
        snap.chats[0].configured_selection.selected_model.as_deref(),
        Some("gpt-5.4")
    );
    assert_eq!(
        snap.chats[0].configured_selection.evidence,
        WorkspaceSelectionEvidence::VerifiedConfiguration
    );
    assert!(
        !snap.chats[0]
            .configured_selection
            .counts_as_runtime_telemetry
    );

    let eval = snap.native_samples[0].evaluate_at(snap.generated_at_ms, 10_000);
    assert_eq!(eval.sample_age_ms, Some(2_000));
    assert_eq!(eval.freshness, WorkspaceFreshnessState::Fresh);
    assert_eq!(
        eval.effective_availability,
        WorkspaceAvailabilityState::Available
    );
    assert_eq!(
        eval.effective_activity,
        WorkspaceActivityState::WaitingForInput
    );

    assert_eq!(
        snap.job_histories[0].acceptance_proof.status,
        WorkspaceAcceptanceStatus::VerifiedAccepted
    );
    assert_eq!(
        snap.job_histories[0].tasks[0].acceptance_proof.status,
        WorkspaceAcceptanceStatus::VerifiedAccepted
    );
    assert_eq!(
        snap.messages.items[0].item_id,
        "wmsg:v1:7:default:15:closure_summary:15:cverd_golden_01:0"
    );

    // Round-trip serialization produces an identical WorkspaceSnapshot.
    let reserialized = serde_json::to_string(&snap).expect("reserialize golden WorkspaceSnapshot");
    let reparsed =
        parse_workspace_snapshot_json(&reserialized).expect("reparse golden WorkspaceSnapshot");
    assert_eq!(reparsed, snap);
}

#[test]
fn legacy_observation_upgrade_keeps_unknown_completed_and_failed_out_of_attention_count() {
    let cases = [
        (
            ObservationJobState::Unknown,
            "unknown",
            "unknown",
            0_usize,
            0_usize,
        ),
        (
            ObservationJobState::Completed,
            "approved",
            "completed",
            0_usize,
            0_usize,
        ),
        (
            ObservationJobState::Failed,
            "rejected",
            "failed",
            0_usize,
            0_usize,
        ),
        (
            ObservationJobState::NeedsUser,
            "pending",
            "blocked",
            1_usize,
            1_usize,
        ),
    ];

    for (state, approval, execution, exp_active, exp_attention) in cases {
        let mut legacy = sample_legacy_observation_snapshot();
        legacy.requests.clear();
        legacy.jobs[0].state = state;
        legacy.jobs[0].approval_status = approval.to_string();
        legacy.jobs[0].execution_status = execution.to_string();
        legacy.jobs[0].review_state = None;
        legacy.jobs[0].active_call_count = 0;
        legacy.jobs[0].tasks.clear();

        let direct = WorkspaceSnapshot::from_legacy_observation(legacy.clone());
        let raw_json = serde_json::to_string(&legacy).expect("serialize legacy snapshot");
        let parsed = parse_workspace_snapshot_json(&raw_json).expect("parse legacy snapshot");
        assert_eq!(parsed, direct);

        assert_eq!(direct.chats.len(), 1);
        assert_eq!(direct.chats[0].active_job_count, exp_active);
        assert_eq!(direct.chats[0].attention_count, exp_attention);

        assert_eq!(direct.job_histories.len(), 1);
        assert_eq!(direct.job_histories[0].job.state, state);
        assert!(direct.job_histories[0].current_attention.is_none());
        assert!(direct.job_histories[0].historical_failure.is_none());
    }
}
