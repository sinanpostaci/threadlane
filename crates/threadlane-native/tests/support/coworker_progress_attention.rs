//! Offline progress attention traverses a real fake process, host ledger, owner, and native WS.
use super::*;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use threadlane::NativeToolOutputConfig;
use threadlane_native::coworker_driver::{
    CoworkerCodeDriver, CoworkerDriverExecutionMode, CoworkerDriverFailureStage,
    CoworkerDriverOwnerState, CoworkerDriverRunOptions, CoworkerDriverStateRecord,
};
use threadlane_native::coworker_notice::{
    coworker_notice_dir, CoworkerNoticeConfig, CoworkerNoticeStore, NoticeDeliveryStatus,
};
use threadlane_native::coworker_workflow::{
    ApplyWorkflowHoldRequest, CoworkerControllerPhase1Status, CoworkerJobLockState,
    CoworkerJobOwnershipKey, WorkflowHoldReason,
};
use threadlane_workers::ProgressWatchdogPolicy;
use tokio::net::UnixListener;
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[cfg(target_os = "linux")]
async fn health_callback(
    listener: &UnixListener,
    repo: &str,
) -> (
    tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>,
    Value,
) {
    let (stream, _) = listener.accept().await.unwrap();
    let mut ws = accept_async(stream).await.unwrap();
    while let Some(frame) = ws.next().await {
        let Message::Text(text) = frame.unwrap() else {
            continue;
        };
        let request: Value = serde_json::from_str(&text).unwrap();
        let result = match request["method"].as_str().unwrap() {
            "initialized" => continue,
            "initialize" => json!({}),
            "thread/read" | "thread/resume" => json!({
                "thread": {"id": THREAD_UUID_A, "cwd": repo, "status": {"type": "idle"}},
                "cwd": repo,
            }),
            "turn/start" => return (ws, request),
            method => panic!("unexpected health notice method {method}"),
        };
        ws.send(Message::Text(
            json!({"id": request["id"], "result": result})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    }
    panic!("missing diagnostic availability callback");
}

#[cfg(target_os = "linux")]
async fn accept_health_callback(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>,
    request: &Value,
) {
    ws.send(Message::Text(
        json!({"id": request["id"], "result": {
            "turn": {"id": "0196ed00-0000-7000-8000-0000000000aa", "status": "inProgress"}
        }})
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
}

/// Emits five synthetic five-minute patterns immediately; no real provider or wall-clock wait.
#[cfg(target_os = "linux")]
fn health_hint_script() -> &'static str {
    r#"
health_log=''
health_previous=''
for health_argument in "$@"; do
    if [ "$health_previous" = '--log-file' ]; then health_log=$health_argument; break; fi
    health_previous=$health_argument
done
if [ -n "$health_log" ]; then
    health_attempt=1
    while [ "$health_attempt" -le 5 ]; do
        printf 'I0101 00:00:01.000000     1 run.go:315] Run: attempt %s failed (stream reading error: stream idle timeout (5m0s): context deadline exceeded), retrying in 4s.\n' "$health_attempt"
        health_attempt=$((health_attempt + 1))
    done > "$health_log"
fi
"#
}

#[cfg(target_os = "linux")]
#[test]
fn original_worker_health_notifies_before_soft_deadline_and_deduplicates_submitted_or_unknown() {
    run_with_16mib_test_stack(|| async move {
        for unknown in [false, true] {
            let fixture = setup_fixture();
            let (job, approval_id) = create_planned_approved_and_frozen_job(
                &fixture,
                "brf_health_original",
                "plan-health-original",
                vec![PlanTask {
                    id: "task-health".into(),
                    title: "Original diagnostic hint".into(),
                    instruction: "Read fixture state until explicitly cancelled".into(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/a.rs".into()],
                }],
                BTreeMap::new(),
                vec!["src/a.rs".into()],
                1,
                ExecutionBudgets {
                    timeout_ms: 30_000,
                    progress_watchdog: Some(ProgressWatchdogPolicy {
                        soft_timeout_ms: 20_000,
                        extension_ms: 1_000,
                        recent_advancement_ms: 100,
                    }),
                    ..ExecutionBudgets::default()
                },
                ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 1),
                IntegrationStrategy::MergeNoFf,
                true,
            )
            .await;
            let marker = fixture.root.join("health-original-starts");
            write_executable_script(&fixture.fake_agy_exe, &format!(
                "#!/bin/sh\nset -eu\nprintf 'started\\n' >> '{}'\n{}\nprintf '%s\\n' '{{\"event\":\"init\",\"init\":{{\"session_id\":\"fixture\",\"model\":\"gemini-4-argon-high\"}}}}'\nwhile :; do sleep 0.02; done\n",
                marker.display(), health_hint_script()));
            let host = Box::pin(ApprovedHost::start(ApprovedHostConfig::new(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                &approval_id,
            )))
            .await
            .unwrap();
            let client = ApprovedHostClient::new(
                Arc::clone(&fixture.authority),
                &fixture.sessions_dir,
                BINDING_A,
                THREAD_UUID_A,
            )
            .unwrap();
            let now = || {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
            };
            let session = fixture
                .controller
                .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, now())
                .unwrap();
            session.step(now()).await.unwrap();
            session.release().unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let snapshot = client.get_task(&approval_id, "task-health").await.unwrap();
                if snapshot
                    .worker
                    .as_ref()
                    .and_then(|worker| worker.progress.provider_health.as_ref())
                    .is_some_and(|health| health.retry_count == 5)
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "health never reached host ledger"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let driver = CoworkerCodeDriver::from_controller(
                fixture.root.join("runtime"),
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                Arc::clone(&fixture.authority),
                fixture.controller.clone(),
            )
            .unwrap();
            let key = CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_A, &job.job_id).unwrap();
            let digest = fixture
                .policy_store
                .get_job_runtime_readonly(BINDING_A, &job.job_id)
                .unwrap()
                .unwrap()
                .envelope
                .envelope_digest;
            let sockets = fixture.root.join("health-native");
            fs::create_dir(&sockets).unwrap();
            fs::set_permissions(&sockets, fs::Permissions::from_mode(0o700)).unwrap();
            let socket = sockets.join("native.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
            let notices = coworker_notice_dir(&fixture.plans_dir);
            CoworkerNoticeStore::new(&notices)
                .unwrap()
                .configure(
                    CoworkerNoticeConfig::new(
                        key.clone(),
                        digest.clone(),
                        NativeToolOutputConfig::new_user_message_steer(
                            socket,
                            THREAD_UUID_A.into(),
                            fixture.repo_dir.clone(),
                        ),
                    )
                    .unwrap(),
                )
                .unwrap();
            let peer_controller = fixture.controller.clone();
            let peer_client = client.clone();
            let peer_approval = approval_id.clone();
            let peer_key = key.clone();
            let repo = fixture.repo_dir.to_string_lossy().into_owned();
            let peer = tokio::spawn(async move {
                let (mut ws, request) = health_callback(&listener, &repo).await;
                let snapshot = peer_client
                    .get_task(&peer_approval, "task-health")
                    .await
                    .unwrap();
                assert_eq!(snapshot.status, TaskExecutionStatus::Running);
                let worker = snapshot.worker.unwrap();
                assert!(
                    worker.progress.watchdog_attention.is_none(),
                    "notice must precede soft deadline"
                );
                let durable = peer_controller
                    .policy_store()
                    .get_job_runtime_readonly(BINDING_A, &peer_key.job_id)
                    .unwrap()
                    .unwrap();
                let health = &durable.original_worker_provider_health["task-health"];
                assert_eq!(health.snapshot.retry_count, 5);
                assert_eq!(health.snapshot.evidence_class,
                    threadlane_core::provider_health::ProviderHealthEvidenceClass::UnverifiedProviderDiagnosticHint);
                assert!(!durable.stop_requested);
                assert!(matches!(
                    peer_controller
                        .ownership_store()
                        .inspect_job_lock(&peer_key)
                        .unwrap(),
                    CoworkerJobLockState::HeldByActiveProcess { .. }
                ));
                assert_eq!(
                    request["params"]["input"][0]["text"],
                    threadlane::format_coworker_job_wakeup_message(&peer_key.job_id).unwrap()
                );
                if !unknown {
                    accept_health_callback(&mut ws, &request).await;
                }
            });
            let opts = CoworkerDriverRunOptions {
                max_steps: Some(1),
                poll_interval_ms: 5,
                execution_mode: Some(CoworkerDriverExecutionMode::Phase1InitialDispatchOnly),
                ..Default::default()
            };
            let report = tokio::time::timeout(
                Duration::from_secs(10),
                Box::pin(driver.run_owner(BINDING_A, THREAD_UUID_A, &job.job_id, opts)),
            )
            .await
            .unwrap()
            .unwrap();
            peer.await.unwrap();
            assert!(!report.stop_requested);
            let metadata = CoworkerNoticeStore::inspect(&notices, &key, &digest).unwrap();
            assert_eq!(metadata.attempt_count, 1);
            assert_eq!(
                metadata.latest.unwrap().status,
                if unknown {
                    NoticeDeliveryStatus::Unknown
                } else {
                    NoticeDeliveryStatus::Submitted
                }
            );
            Box::pin(driver.run_owner(BINDING_A, THREAD_UUID_A, &job.job_id, opts))
                .await
                .unwrap();
            assert_eq!(
                CoworkerNoticeStore::inspect(&notices, &key, &digest)
                    .unwrap()
                    .attempt_count,
                1
            );
            assert_eq!(fs::read_to_string(&marker).unwrap().lines().count(), 1);
            let running = client.get_task(&approval_id, "task-health").await.unwrap();
            assert_eq!(running.status, TaskExecutionStatus::Running);
            client
                .cancel_task(
                    &approval_id,
                    "task-health",
                    running.worker.unwrap().revision,
                    Some("fixture teardown".into()),
                )
                .await
                .unwrap();
            host.shutdown().await.unwrap();
        }
    });
}

fn run_with_16mib_test_stack<F, Fut>(make_future: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()>,
{
    std::thread::Builder::new()
        .name("coworker-progress-attention-16m".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(16 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("build 16MiB test tokio runtime");
            rt.block_on(make_future());
        })
        .expect("spawn 16MiB test thread")
        .join()
        .expect("16MiB test thread panicked");
}

#[cfg(target_os = "linux")]
#[test]
fn reviewer_derived_correction_and_closure_send_durable_advisories_without_changing_authority() {
    run_with_16mib_test_stack(|| async move {
        let fixture = setup_fixture();
        let (job, approval_id) = create_planned_approved_and_frozen_job(
            &fixture,
            "brf_health_roles",
            "plan-health-roles",
            vec![PlanTask {
                id: "task-root-a".into(),
                title: "One bounded correction".into(),
                instruction: "CTRL_TASK_A implement src/a.rs".into(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/a.rs".into()],
            }],
            BTreeMap::new(),
            vec!["src/a.rs".into()],
            1,
            ExecutionBudgets::default(),
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 2),
            IntegrationStrategy::MergeNoFf,
            true,
        )
        .await;
        let original_script = fs::read_to_string(&fixture.fake_agy_exe).unwrap();
        let exec_node = original_script
            .lines()
            .find(|line| line.starts_with("exec node "))
            .unwrap();
        let gate = fixture.root.join("health-role-await-callback");
        let health_wrapper = format!(
            "#!/bin/sh\nset -eu\ncase \"$*\" in\n*EXPECTED_TASK_REVIEW_IDENTITY_JSON:*|*StructuredClosureModelResponse*|*'THREADLANE IN-SCOPE CORRECTION DIRECTIVE'*)\nprintf 'held\\n' > '{}'\n{}\nwhile [ -f '{}' ]; do sleep 0.01; done\n;;\nesac\n{}\n",
            gate.display(), health_hint_script(), gate.display(), exec_node);
        write_executable_script(&fixture.fake_agy_exe, &health_wrapper);
        fs::write(
            fixture
                .root
                .join("reviewer_override_task-root-a_attempt_1.json"),
            json!({"outcome": "correct_in_scope", "findings": ["Expected a() to return 101"],
                "required_in_scope_fixes": ["Update src/a.rs to return 101"],
                "summary": "One approved in-scope correction"})
            .to_string(),
        )
        .unwrap();
        let key = CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_A, &job.job_id).unwrap();
        let before = fixture
            .policy_store
            .get_job_runtime_readonly(BINDING_A, &job.job_id)
            .unwrap()
            .unwrap();
        let digest = before.envelope.envelope_digest.clone();
        let notices = coworker_notice_dir(&fixture.plans_dir);
        let sockets = fixture.root.join("health-role-native");
        fs::create_dir(&sockets).unwrap();
        fs::set_permissions(&sockets, fs::Permissions::from_mode(0o700)).unwrap();
        let socket = sockets.join("native.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
        CoworkerNoticeStore::new(&notices)
            .unwrap()
            .configure(
                CoworkerNoticeConfig::new(
                    key.clone(),
                    digest.clone(),
                    NativeToolOutputConfig::new_user_message_steer(
                        socket,
                        THREAD_UUID_A.into(),
                        fixture.repo_dir.clone(),
                    ),
                )
                .unwrap(),
            )
            .unwrap();
        let peer_controller = fixture.controller.clone();
        let peer_key = key.clone();
        let repo = fixture.repo_dir.to_string_lossy().into_owned();
        let peer = tokio::spawn(async move {
            let mut seen = std::collections::BTreeSet::new();
            for expected_role in [
                WorkflowRole::Reviewer,
                WorkflowRole::Coder,
                WorkflowRole::Reviewer,
                WorkflowRole::Closure,
            ] {
                let (mut ws, request) = health_callback(&listener, &repo).await;
                let state = peer_controller
                    .policy_store()
                    .get_job_runtime_readonly(BINDING_A, &peer_key.job_id)
                    .unwrap()
                    .unwrap();
                let current = state
                    .provider_health
                    .values()
                    .find(|health| {
                        !health.terminal_observed && health.snapshot.current_failure.is_some()
                    })
                    .expect("current advisory must be DURABLE before callback");
                assert!((1..=5).contains(&current.snapshot.retry_count));
                assert_eq!(current.snapshot.evidence_class,
                    threadlane_workers::provider_health::ProviderHealthEvidenceClass::UnverifiedProviderDiagnosticHint);
                let threadlane_native::provider_health::ProviderHealthInvocation::Controller {
                    effect_id,
                    role,
                    ..
                } = &current.invocation
                else {
                    panic!("expected controller invocation");
                };
                assert_eq!(*role, expected_role);
                assert!(seen.insert(effect_id.clone()), "no repeated retry callback");
                assert_eq!(
                    state.pending_effects[effect_id].status,
                    PendingControllerEffectStatus::ReservedBeforeEffect
                );
                assert!(!state.stop_requested);
                assert!(matches!(
                    peer_controller
                        .ownership_store()
                        .inspect_job_lock(&peer_key)
                        .unwrap(),
                    CoworkerJobLockState::HeldByActiveProcess { .. }
                ));
                assert_eq!(
                    request["params"]["input"][0]["text"],
                    threadlane::format_coworker_job_wakeup_message(&peer_key.job_id).unwrap()
                );
                accept_health_callback(&mut ws, &request).await;
                fs::remove_file(&gate).unwrap();
            }
        });
        let host = Box::pin(ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            &approval_id,
        )))
        .await
        .unwrap();
        let session = fixture
            .pipeline
            .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 20_000)
            .unwrap();
        let report = tokio::time::timeout(
            Duration::from_secs(120),
            Box::pin(session.drive_until_settled(20_000)),
        )
        .await
        .unwrap()
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), peer)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.status, CoworkerPipelineStatus::Completed);
        let final_state = fixture
            .policy_store
            .get_job_runtime_readonly(BINDING_A, &job.job_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            final_state.envelope, before.envelope,
            "diagnostics never alter immutable approval"
        );
        assert_eq!(final_state.provider_health.len(), 4);
        for view in threadlane_native::provider_health::controller_health_views(&final_state, None)
        {
            assert!(!view.live);
            assert!(view.record.terminal_observed);
            assert_eq!(view.record.snapshot.retry_count, 5);
            assert!(view.record.snapshot.first_failure.is_some());
            assert!(view.record.snapshot.last_failure.is_some());
            assert!(view.record.snapshot.current_failure.is_none());
        }
        assert_eq!(
            CoworkerNoticeStore::inspect(&notices, &key, &digest)
                .unwrap()
                .attempt_count,
            4
        );
        session.release().unwrap();
        let after_release = fixture
            .policy_store
            .get_job_runtime_readonly(BINDING_A, &job.job_id)
            .unwrap()
            .unwrap();
        assert_eq!(after_release.provider_health, final_state.provider_health);
        assert_eq!(
            CoworkerNoticeStore::inspect(&notices, &key, &digest)
                .unwrap()
                .attempt_count,
            4
        );
        host.shutdown().await.unwrap();
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn progress_attention_is_delivered_once_while_coder_remains_active_and_teardown_failure_is_reported(
) {
    Box::pin(async {
        for scenario in ["submitted", "unknown", "teardown"] {
            let teardown_error = scenario == "teardown";
            let unknown = scenario == "unknown";
            let fixture = setup_fixture();
            let budgets = ExecutionBudgets {
                timeout_ms: 15_000,
                progress_watchdog: Some(ProgressWatchdogPolicy {
                    soft_timeout_ms: 100, extension_ms: 50, recent_advancement_ms: 50,
                }),
                ..ExecutionBudgets::default()
            };
            let (job, approval_id) = create_planned_approved_and_frozen_job(
                &fixture, "brf_progress_attention", "plan-progress-attention",
                vec![PlanTask {
                    id: "task-progress".into(), title: "Offline progress attention".into(),
                    instruction: "Read fixture state until explicitly cancelled".into(), role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard, depends_on: Vec::new(), allowed_write_paths: vec!["src/a.rs".into()],
                }], BTreeMap::new(), vec!["src/a.rs".into()], 1, budgets,
                ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 1),
                IntegrationStrategy::MergeNoFf, true,
            ).await;
            let marker = fixture.root.join("active-coder-starts");
            write_executable_script(&fixture.fake_agy_exe, &format!(
                "#!/bin/sh\nset -eu\nprintf 'started\\n' >> '{}'\nprintf '%s\\n' '{{\"event\":\"init\",\"init\":{{\"session_id\":\"fixture\",\"model\":\"gemini-4-argon-high\"}}}}'\nwhile :; do printf '%s\\n' '{{\"event\":\"tool_use\",\"tool_name\":\"read_file\"}}'; sleep 0.02; done\n",
                marker.display()));
            let host = Box::pin(ApprovedHost::start(ApprovedHostConfig::new(
                &fixture.registry_dir, &fixture.plans_dir, &fixture.authority_dir,
                &fixture.sessions_dir, &approval_id,
            ))).await.unwrap();
            let client = ApprovedHostClient::new(Arc::clone(&fixture.authority), &fixture.sessions_dir, BINDING_A, THREAD_UUID_A).unwrap();
            let now = || std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
            let session = fixture.controller.open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, now()).unwrap();
            session.step(now()).await.unwrap();
            session.release().unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            let attention = loop {
                let snap = client.get_task(&approval_id, "task-progress").await.unwrap();
                if let Some(attention) = snap.worker.as_ref().and_then(|worker| worker.progress.watchdog_attention.clone()) {
                    assert_eq!(snap.status, TaskExecutionStatus::Running);
                    assert!(attention.read_hints > 0, "read-only fixture must remain advisory");
                    break attention;
                }
                assert!(Instant::now() < deadline, "live attention did not reach actual host snapshot");
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            let driver = CoworkerCodeDriver::from_controller(
                fixture.root.join("runtime"), &fixture.registry_dir, &fixture.plans_dir,
                &fixture.authority_dir, &fixture.sessions_dir, Arc::clone(&fixture.authority), fixture.controller.clone(),
            ).unwrap();
            let key = CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_A, &job.job_id).unwrap();
            let digest = fixture.policy_store.get_job_runtime_readonly(BINDING_A, &job.job_id).unwrap().unwrap().envelope.envelope_digest;
            let notices = coworker_notice_dir(&fixture.plans_dir);
            let sockets = fixture.root.join("progress-native");
            fs::create_dir(&sockets).unwrap(); fs::set_permissions(&sockets, fs::Permissions::from_mode(0o700)).unwrap();
            let socket = sockets.join("native.sock");
            let listener = UnixListener::bind(&socket).unwrap(); fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
            CoworkerNoticeStore::new(&notices).unwrap().configure(CoworkerNoticeConfig::new(
                key.clone(), digest.clone(), NativeToolOutputConfig::new_user_message_steer(socket, THREAD_UUID_A.into(), fixture.repo_dir.clone()),
            ).unwrap()).unwrap();
            let peer_driver = driver.clone(); let peer_key = key.clone();
            let peer_client = client.clone(); let peer_approval = approval_id.clone();
            let repo = fixture.repo_dir.to_string_lossy().to_string();
            let peer = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap(); drop(listener);
                let mut ws = accept_async(stream).await.unwrap();
                while let Some(frame) = ws.next().await {
                    let Message::Text(text) = frame.unwrap() else { continue; };
                    let request: Value = serde_json::from_str(&text).unwrap();
                    let method = request["method"].as_str().unwrap(); if method == "initialized" { continue; }
                    let result = match method {
                        "initialize" => json!({}),
                        "thread/read" | "thread/resume" => json!({"thread": {"id": THREAD_UUID_A, "cwd": repo, "status": {"type": "idle"}}, "cwd": repo}),
                        "turn/start" => {
                            let actual = peer_client.get_task(&peer_approval, "task-progress").await.unwrap();
                            assert_eq!(actual.status, TaskExecutionStatus::Running, "notice must precede worker completion");
                            assert!(actual.worker.unwrap().progress.watchdog_attention.is_some());
                            assert!(matches!(peer_driver.controller().ownership_store().inspect_job_lock(&peer_key).unwrap(), CoworkerJobLockState::HeldByActiveProcess { .. }));
                            let state = peer_driver.read_driver_state_optional(&peer_key).unwrap().unwrap();
                            assert!(state.last_step_report.unwrap().tasks.iter().any(|task| task.watchdog_attention.is_some()), "health must be durable before external send");
                            assert_eq!(request["params"]["input"][0]["text"], threadlane::format_coworker_job_wakeup_message(&peer_key.job_id).unwrap());
                            let live = peer_driver.status(BINDING_A, THREAD_UUID_A, &peer_key.job_id).await.unwrap();
                            assert!(live.tasks.iter().any(|task| task.watchdog_attention.is_some()), "execution status must expose durable attention while the owner is active");
                            if unknown { return request; }
                            if teardown_error { fs::set_permissions(peer_driver.state_file_path_for_key(&peer_key).unwrap(), fs::Permissions::from_mode(0o644)).unwrap(); }
                            let response = json!({"turn": {"id": "0196ed00-0000-7000-8000-0000000000aa", "status": "inProgress"}});
                            ws.send(Message::Text(json!({"id": request["id"], "result": response}).to_string().into())).await.unwrap();
                            return request;
                        }
                        other => panic!("unexpected native method {other}"),
                    };
                    ws.send(Message::Text(json!({"id": request["id"], "result": result}).to_string().into())).await.unwrap();
                }
                panic!("no progress wakeup");
            });
            let opts = CoworkerDriverRunOptions {
                max_steps: Some(1), poll_interval_ms: 5,
                execution_mode: Some(CoworkerDriverExecutionMode::Phase1InitialDispatchOnly), ..Default::default()
            };
            let result = tokio::time::timeout(Duration::from_secs(10), Box::pin(driver.run_owner(BINDING_A, THREAD_UUID_A, &job.job_id, opts))).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), peer).await.unwrap().unwrap();
            if teardown_error {
                assert_eq!(result.unwrap_err().code, NativeErrorCode::SecurityViolation);
                let failure = driver.read_owner_failure_optional(&key).unwrap().unwrap();
                assert_eq!(failure.stage, CoworkerDriverFailureStage::OwnerTeardown);
                fs::set_permissions(driver.state_file_path_for_key(&key).unwrap(), fs::Permissions::from_mode(0o600)).unwrap();
                let metadata = CoworkerNoticeStore::inspect(&notices, &key, &digest).unwrap();
                assert_eq!(metadata.attempt_count, 2, "separate progress and fatal fingerprints");
            } else {
                let report = result.unwrap();
                assert_eq!(report.tasks[0].watchdog_attention, Some(attention.clone()));
                assert!(!report.stop_requested, "soft attention cannot cancel execution");
                assert!(driver.read_owner_failure_optional(&key).unwrap().is_none());
                let metadata = CoworkerNoticeStore::inspect(&notices, &key, &digest).unwrap();
                assert_eq!(metadata.attempt_count, 1); assert_eq!(metadata.latest.unwrap().status, if unknown { NoticeDeliveryStatus::Unknown } else { NoticeDeliveryStatus::Submitted });
                // Reopening owner changes neither invocation identity nor the deduplicated notice.
                let duplicate = Box::pin(driver.run_owner(BINDING_A, THREAD_UUID_A, &job.job_id, opts)).await.unwrap();
                assert_eq!(duplicate.tasks[0].watchdog_attention, Some(attention));
                assert_eq!(CoworkerNoticeStore::inspect(&notices, &key, &digest).unwrap().attempt_count, 1);
            }
            let running = client.get_task(&approval_id, "task-progress").await.unwrap();
            assert_eq!(running.status, TaskExecutionStatus::Running);
            assert_eq!(fs::read_to_string(marker).unwrap().lines().count(), 1, "attention/reopen must not replay a provider");
            client.cancel_task(&approval_id, "task-progress", running.worker.unwrap().revision, Some("fixture teardown".into())).await.unwrap();
            host.shutdown().await.unwrap();
        }
    }).await;
}

#[test]
fn failed_root_delivers_early_attention_while_sibling_runs_then_fenced_sibling_completion_settles_without_duplicate_notice_or_promotion(
) {
    run_with_16mib_test_stack(|| async move {
        for scenario in ["submitted", "unknown"] {
            let unknown = scenario == "unknown";
            let fixture = setup_fixture();
            let tasks = vec![
                PlanTask {
                    id: "task-root-a".into(),
                    title: "Root task A".into(),
                    instruction: "CTRL_TASK_A implement src/a.rs".into(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/a.rs".into()],
                },
                PlanTask {
                    id: "task-root-b".into(),
                    title: "Root task B".into(),
                    instruction: "CTRL_TASK_B implement src/b.rs".into(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/b.rs".into()],
                },
                PlanTask {
                    id: "task-dep-c".into(),
                    title: "Dependent task C".into(),
                    instruction: "CTRL_TASK_C implement src/c.rs".into(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: vec!["task-root-a".into(), "task-root-b".into()],
                    allowed_write_paths: vec!["src/c.rs".into()],
                },
            ];
            let mut ifaces = BTreeMap::new();
            ifaces.insert(
                "task-root-a".into(),
                TaskInterfaceOwnershipSpec {
                    owned_interfaces: vec!["iface_a".into()],
                    consumed_interfaces: Vec::new(),
                },
            );
            ifaces.insert(
                "task-root-b".into(),
                TaskInterfaceOwnershipSpec {
                    owned_interfaces: vec!["iface_b".into()],
                    consumed_interfaces: Vec::new(),
                },
            );
            ifaces.insert(
                "task-dep-c".into(),
                TaskInterfaceOwnershipSpec {
                    owned_interfaces: vec!["iface_c".into()],
                    consumed_interfaces: vec!["iface_a".into(), "iface_b".into()],
                },
            );
            let (job, approval_id) = create_planned_approved_and_frozen_job(
                &fixture,
                &format!("brf_sibling_failure_{scenario}"),
                &format!("plan-sibling-failure-{scenario}"),
                tasks,
                ifaces,
                vec!["src/a.rs".into(), "src/b.rs".into(), "src/c.rs".into()],
                2,
                // Finite worker hard deadline must exceed the 40s owner/control window plus setup
                // and terminal reconciliation.
                ExecutionBudgets {
                    timeout_ms: 120_000,
                    ..ExecutionBudgets::default()
                },
                ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 3),
                IntegrationStrategy::MergeNoFf,
                false,
            )
            .await;

            let original_agy = fs::read_to_string(&fixture.fake_agy_exe).unwrap();
            let exec_node = original_agy
                .lines()
                .find(|line| line.starts_with("exec node "))
                .unwrap()
                .to_string();
            let root_a_starts = fixture.root.join("root-a-starts");
            let root_b_starts = fixture.root.join("root-b-starts");
            let hold_b = fixture.root.join("hold-root-b.gate");
            fs::write(&hold_b, "hold").unwrap();
            write_executable_script(
                &fixture.fake_agy_exe,
                &format!(
                    "#!/bin/sh\nset -eu\ncase \"$*\" in\n  *CTRL_TASK_A*)\n    printf 'started\\n' >> '{}'\n    printf '%s\\n' '{{\"event\":\"init\",\"init\":{{\"session_id\":\"fixture-a\",\"model\":\"gemini-4-argon-high\"}}}}'\n    printf 'invalid_metadata: simulated root A failure\\n' >&2\n    exit 1\n    ;;\n  *CTRL_TASK_B*)\n    printf 'started\\n' >> '{}'\n    while [ -f '{}' ]; do sleep 0.02; done\n    {}\n    ;;\n  *)\n    {}\n    ;;\nesac\n",
                    root_a_starts.display(),
                    root_b_starts.display(),
                    hold_b.display(),
                    exec_node,
                    exec_node,
                ),
            );

            let host = Box::pin(ApprovedHost::start(ApprovedHostConfig::new(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                &approval_id,
            )))
            .await
            .unwrap();
            let client = ApprovedHostClient::new(
                Arc::clone(&fixture.authority),
                &fixture.sessions_dir,
                BINDING_A,
                THREAD_UUID_A,
            )
            .unwrap();
            let now = || {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
            };
            let session = fixture
                .controller
                .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, now())
                .unwrap();
            session.step(now()).await.unwrap();

            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let snap_a = client.get_task(&approval_id, "task-root-a").await.unwrap();
                let snap_b = client.get_task(&approval_id, "task-root-b").await.unwrap();
                if snap_a.status == TaskExecutionStatus::Failed
                    && snap_b.status == TaskExecutionStatus::Running
                    && root_b_starts.is_file()
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for root A failure while root B remains gated running"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            // Reconcile root A's failed host snapshot into the workflow ledger before testing notice verification.
            session.step(now()).await.unwrap();
            session.release().unwrap();

            let driver = CoworkerCodeDriver::from_controller(
                fixture.root.join("runtime"),
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                Arc::clone(&fixture.authority),
                fixture.controller.clone(),
            )
            .unwrap();
            let key = CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_A, &job.job_id).unwrap();
            let digest = fixture
                .policy_store
                .get_job_runtime_readonly(BINDING_A, &job.job_id)
                .unwrap()
                .unwrap()
                .envelope
                .envelope_digest;
            let notices = coworker_notice_dir(&fixture.plans_dir);
            let sockets = fixture.root.join("sibling-failure-native");
            fs::create_dir(&sockets).unwrap();
            fs::set_permissions(&sockets, fs::Permissions::from_mode(0o700)).unwrap();
            let socket = sockets.join("native.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
            CoworkerNoticeStore::new(&notices)
                .unwrap()
                .configure(
                    CoworkerNoticeConfig::new(
                        key.clone(),
                        digest.clone(),
                        NativeToolOutputConfig::new_user_message_steer(
                            socket,
                            THREAD_UUID_A.into(),
                            fixture.repo_dir.clone(),
                        ),
                    )
                    .unwrap(),
                )
                .unwrap();

            let owner_opts = CoworkerDriverRunOptions {
                max_steps: None,
                poll_interval_ms: 5,
                idle_linger_ms: Some(0),
                execution_mode: Some(CoworkerDriverExecutionMode::FullAutomaticPipeline),
            };

            let peer_driver = driver.clone();
            let peer_key = key.clone();
            let peer_client = client.clone();
            let peer_approval = approval_id.clone();
            let repo = fixture.repo_dir.to_string_lossy().to_string();
            let peer = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                drop(listener);
                let mut ws = accept_async(stream).await.unwrap();
                while let Some(frame) = ws.next().await {
                    let Message::Text(text) = frame.unwrap() else {
                        continue;
                    };
                    let request: Value = serde_json::from_str(&text).unwrap();
                    let method = request["method"].as_str().unwrap();
                    if method == "initialized" {
                        continue;
                    }
                    let result = match method {
                        "initialize" => json!({}),
                        "thread/read" | "thread/resume" => json!({
                            "thread": {"id": THREAD_UUID_A, "cwd": repo, "status": {"type": "idle"}},
                            "cwd": repo
                        }),
                        "turn/start" => {
                            let actual_a = peer_client
                                .get_task(&peer_approval, "task-root-a")
                                .await
                                .unwrap();
                            let actual_b = peer_client
                                .get_task(&peer_approval, "task-root-b")
                                .await
                                .unwrap();
                            assert_eq!(actual_a.status, TaskExecutionStatus::Failed);
                            assert_eq!(
                                actual_b.status,
                                TaskExecutionStatus::Running,
                                "early failed-worker attention notice must arrive while independent sibling is still physically running"
                            );
                            assert!(matches!(
                                peer_driver
                                    .controller()
                                    .ownership_store()
                                    .inspect_job_lock(&peer_key)
                                    .unwrap(),
                                CoworkerJobLockState::HeldByActiveProcess { .. }
                            ));
                            assert_eq!(
                                request["params"]["input"][0]["text"],
                                threadlane::format_coworker_job_wakeup_message(&peer_key.job_id)
                                    .unwrap()
                            );
                            let live = peer_driver
                                .status(BINDING_A, THREAD_UUID_A, &peer_key.job_id)
                                .await
                                .unwrap();
                            assert_eq!(
                                live.overall_status,
                                CoworkerControllerPhase1Status::RunningTasks,
                                "real running sibling must keep live job status RunningTasks"
                            );
                            assert_eq!(
                                live.pipeline_status,
                                Some(CoworkerPipelineStatus::Running),
                                "real running sibling must not be prematurely declared settled"
                            );
                            if unknown {
                                return request;
                            }
                            let response = json!({
                                "turn": {
                                    "id": "0196ed00-0000-7000-8000-0000000000bb",
                                    "status": "inProgress"
                                }
                            });
                            ws.send(Message::Text(
                                json!({"id": request["id"], "result": response})
                                    .to_string()
                                    .into(),
                            ))
                            .await
                            .unwrap();
                            return request;
                        }
                        other => panic!("unexpected native method {other}"),
                    };
                    ws.send(Message::Text(
                        json!({"id": request["id"], "result": result})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
                }
                panic!("no worker-failure attention wakeup");
            });

            let state_path = driver.state_file_path_for_key(&key).unwrap();
            let owner_fut =
                Box::pin(driver.run_owner(BINDING_A, THREAD_UUID_A, &job.job_id, owner_opts));
            let control_fut = async {
                tokio::time::timeout(Duration::from_secs(10), peer)
                    .await
                    .expect(
                        "early worker-failure notice handshake must finish while root B is gated",
                    )
                    .unwrap();

                let tick_deadline = Instant::now() + Duration::from_secs(10);
                let live_state = loop {
                    if let Ok(bytes) = fs::read(&state_path) {
                        if let Ok(state) =
                            serde_json::from_slice::<CoworkerDriverStateRecord>(&bytes)
                        {
                            if state.owner_active && state.steps_completed >= 2 {
                                break state;
                            }
                        }
                    }
                    assert!(
                        Instant::now() < tick_deadline,
                        "timed out waiting for active uncapped owner to complete at least 2 ticks while root B is gated"
                    );
                    tokio::time::sleep(Duration::from_millis(5)).await;
                };

                let live_a = client.get_task(&approval_id, "task-root-a").await.unwrap();
                let live_b = client.get_task(&approval_id, "task-root-b").await.unwrap();
                assert_eq!(live_a.status, TaskExecutionStatus::Failed);
                assert_eq!(
                    live_b.status,
                    TaskExecutionStatus::Running,
                    "gated sibling B must remain physically Running on the host during live owner ticks"
                );
                assert!(live_state.owner_active);
                assert!(matches!(
                    driver
                        .controller()
                        .ownership_store()
                        .inspect_job_lock(&key)
                        .unwrap(),
                    CoworkerJobLockState::HeldByActiveProcess { .. }
                ));
                assert_eq!(
                    live_state.last_overall_status,
                    CoworkerControllerPhase1Status::RunningTasks
                );
                assert_eq!(
                    live_state.last_pipeline_status,
                    Some(CoworkerPipelineStatus::Running)
                );
                let metadata = CoworkerNoticeStore::inspect(&notices, &key, &digest).unwrap();
                assert_eq!(metadata.attempt_count, 1);
                assert_eq!(
                    metadata.latest.unwrap().status,
                    if unknown {
                        NoticeDeliveryStatus::Unknown
                    } else {
                        NoticeDeliveryStatus::Submitted
                    }
                );
                let live_runtime = fixture
                    .policy_store
                    .get_job_runtime_readonly(BINDING_A, &job.job_id)
                    .unwrap()
                    .unwrap();
                assert_eq!(live_runtime.total_task_attempts_reserved, 2);
                assert_eq!(
                    fs::read_to_string(&root_a_starts).unwrap().lines().count(),
                    1
                );
                assert_eq!(
                    fs::read_to_string(&root_b_starts).unwrap().lines().count(),
                    1
                );
                assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0);
                assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
                assert_eq!(read_invocation_count(&fixture.check_count_file), 0);

                // Observe at least one additional live owner tick while root B remains gated
                // to prove deduplication under the same retained active owner.
                let next_tick_deadline = Instant::now() + Duration::from_secs(10);
                let next_live_state = loop {
                    if let Ok(bytes) = fs::read(&state_path) {
                        if let Ok(state) =
                            serde_json::from_slice::<CoworkerDriverStateRecord>(&bytes)
                        {
                            if state.owner_active
                                && state.steps_completed > live_state.steps_completed
                            {
                                break state;
                            }
                        }
                    }
                    assert!(
                        Instant::now() < next_tick_deadline,
                        "timed out waiting for another observed owner tick while root B remains gated"
                    );
                    tokio::time::sleep(Duration::from_millis(5)).await;
                };
                assert!(next_live_state.owner_active);
                assert_eq!(
                    next_live_state.last_overall_status,
                    CoworkerControllerPhase1Status::RunningTasks
                );
                assert_eq!(
                    next_live_state.last_pipeline_status,
                    Some(CoworkerPipelineStatus::Running)
                );
                let live_a_again = client.get_task(&approval_id, "task-root-a").await.unwrap();
                let live_b_again = client.get_task(&approval_id, "task-root-b").await.unwrap();
                assert_eq!(live_a_again.status, TaskExecutionStatus::Failed);
                assert_eq!(live_b_again.status, TaskExecutionStatus::Running);
                let repeat_metadata =
                    CoworkerNoticeStore::inspect(&notices, &key, &digest).unwrap();
                assert_eq!(
                    repeat_metadata.attempt_count, 1,
                    "subsequent owned ticks while sibling runs must not resend unknown or submitted notice"
                );
                assert_eq!(
                    repeat_metadata.latest.unwrap().status,
                    if unknown {
                        NoticeDeliveryStatus::Unknown
                    } else {
                        NoticeDeliveryStatus::Submitted
                    }
                );
                assert_eq!(
                    fs::read_to_string(&root_a_starts).unwrap().lines().count(),
                    1
                );
                assert_eq!(
                    fs::read_to_string(&root_b_starts).unwrap().lines().count(),
                    1
                );
                assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0);
                assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
                assert_eq!(read_invocation_count(&fixture.check_count_file), 0);

                // Release gated root B and let the same active owner reconcile its terminal
                // receipt under the sibling-failure fence and naturally settle.
                fs::remove_file(&hold_b).unwrap();
            };

            let (owner_result, ()) = tokio::time::timeout(Duration::from_secs(40), async {
                tokio::join!(owner_fut, control_fut)
            })
            .await
            .expect(
                "retained uncapped owner must settle and exit once gated sibling B is released",
            );
            let settled_report = owner_result.unwrap();

            assert_eq!(
                settled_report.owner_state,
                CoworkerDriverOwnerState::NoActiveOwner
            );
            assert_eq!(
                settled_report.overall_status,
                CoworkerControllerPhase1Status::BlockedOrFailed
            );
            assert_eq!(
                settled_report.pipeline_status,
                Some(CoworkerPipelineStatus::BlockedOrFailed)
            );
            assert!(!settled_report.blocked_on_unknown_effect);

            let task_a = settled_report
                .tasks
                .iter()
                .find(|t| t.task_id == "task-root-a")
                .unwrap();
            assert_eq!(task_a.stage, CoworkerTaskStage::Failed);
            assert_eq!(task_a.host_status, TaskExecutionStatus::Failed);

            let task_b = settled_report
                .tasks
                .iter()
                .find(|t| t.task_id == "task-root-b")
                .unwrap();
            assert_eq!(
                task_b.stage,
                CoworkerTaskStage::Running,
                "fenced sibling must not be promoted to ReadyForReview/Accepted/Integrated"
            );
            assert_eq!(task_b.host_status, TaskExecutionStatus::ReadyForReview);
            assert!(task_b.fenced);
            assert!(task_b.run_id.is_some());
            assert!(task_b.worker_id.is_some());
            assert!(task_b.task_worktree_id.is_some());
            assert_eq!(
                task_b.observed_model.as_deref(),
                Some("gemini-4-argon-high")
            );
            assert_eq!(task_b.observed_effort.as_deref(), Some("high"));

            let task_c = settled_report
                .tasks
                .iter()
                .find(|t| t.task_id == "task-dep-c")
                .unwrap();
            assert_eq!(task_c.stage, CoworkerTaskStage::PendingDependencies);

            let runtime_snapshot = fixture
                .policy_store
                .get_job_runtime_readonly(BINDING_A, &job.job_id)
                .unwrap()
                .unwrap();
            assert_eq!(runtime_snapshot.total_task_attempts_reserved, 2);
            let artifacts_b = runtime_snapshot
                .get_attempt_artifacts("task-root-b", 1)
                .expect("completed fenced sibling must persist inspectable attempt artifacts");
            assert!(artifacts_b.committed_head_commit.is_some());
            assert!(artifacts_b.committed_tree_sha.is_some());
            assert_eq!(
                artifacts_b.observed_model.as_deref(),
                Some("gemini-4-argon-high")
            );
            assert_eq!(artifacts_b.observed_effort.as_deref(), Some("high"));
            assert!(artifacts_b.review_subject.is_none());
            assert!(artifacts_b.review_receipt.is_none());
            assert!(artifacts_b.integration_receipt.is_none());
            assert!(runtime_snapshot
                .pending_effects
                .values()
                .filter(|eff| eff.task_id.as_deref() == Some("task-root-b"))
                .all(|eff| eff.status == PendingControllerEffectStatus::EffectConfirmed));

            // No dependent dispatch, review, check, or Closure calls occurred.
            assert_eq!(
                fs::read_to_string(&root_a_starts).unwrap().lines().count(),
                1
            );
            assert_eq!(
                fs::read_to_string(&root_b_starts).unwrap().lines().count(),
                1
            );
            assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 1);
            assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0);
            assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
            assert_eq!(read_invocation_count(&fixture.check_count_file), 0);

            let settled_notice = CoworkerNoticeStore::inspect(&notices, &key, &digest).unwrap();
            assert_eq!(
                settled_notice.attempt_count, 1,
                "settling after early worker-failure notice must not send a second wakeup"
            );

            let _ = host.shutdown().await;

            // Read-only status and exact repeat/reopen after host shutdown preserve reconciled
            // ReadyForReview host_status and do not restart the host or resend notices.
            let post_shutdown_status = driver
                .status(BINDING_A, THREAD_UUID_A, &job.job_id)
                .await
                .unwrap();
            assert_eq!(
                post_shutdown_status.overall_status,
                CoworkerControllerPhase1Status::BlockedOrFailed
            );
            assert_eq!(
                post_shutdown_status.pipeline_status,
                Some(CoworkerPipelineStatus::BlockedOrFailed)
            );
            let post_b = post_shutdown_status
                .tasks
                .iter()
                .find(|t| t.task_id == "task-root-b")
                .unwrap();
            assert_eq!(post_b.host_status, TaskExecutionStatus::ReadyForReview);
            assert!(post_b.fenced);

            let reopened_driver = CoworkerCodeDriver::open_from_dirs(
                fixture.root.join("runtime"),
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
            )
            .unwrap();
            let reopened = reopened_driver
                .status(BINDING_A, THREAD_UUID_A, &job.job_id)
                .await
                .unwrap();
            assert_eq!(
                reopened.overall_status,
                CoworkerControllerPhase1Status::BlockedOrFailed
            );
            assert_eq!(reopened.host_phase, Some(HostPhase::Stopped));
            assert_eq!(
                CoworkerNoticeStore::inspect(&notices, &key, &digest)
                    .unwrap()
                    .attempt_count,
                1,
                "reopening settled job must not resend unknown or submitted notice"
            );
        }
    });
}

#[test]
fn both_roots_terminal_with_one_failure_settles_and_notifies_once_without_promoting_fenced_sibling()
{
    run_with_16mib_test_stack(|| async move {
        let fixture = setup_fixture();
        let tasks = vec![
            PlanTask {
                id: "task-root-a".into(),
                title: "Root task A".into(),
                instruction: "CTRL_TASK_A implement src/a.rs".into(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/a.rs".into()],
            },
            PlanTask {
                id: "task-root-b".into(),
                title: "Root task B".into(),
                instruction: "CTRL_TASK_B implement src/b.rs".into(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/b.rs".into()],
            },
        ];
        let mut ifaces = BTreeMap::new();
        ifaces.insert(
            "task-root-a".into(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: vec!["iface_a".into()],
                consumed_interfaces: Vec::new(),
            },
        );
        ifaces.insert(
            "task-root-b".into(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: vec!["iface_b".into()],
                consumed_interfaces: Vec::new(),
            },
        );
        let (job, approval_id) = create_planned_approved_and_frozen_job(
            &fixture,
            "brf_both_terminal_sibling_failure",
            "plan-both-terminal-sibling-failure",
            tasks,
            ifaces,
            vec!["src/a.rs".into(), "src/b.rs".into()],
            2,
            ExecutionBudgets {
                timeout_ms: 15_000,
                ..ExecutionBudgets::default()
            },
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 2),
            IntegrationStrategy::MergeNoFf,
            false,
        )
        .await;

        let original_agy = fs::read_to_string(&fixture.fake_agy_exe).unwrap();
        let exec_node = original_agy
            .lines()
            .find(|line| line.starts_with("exec node "))
            .unwrap()
            .to_string();
        let hold_b = fixture.root.join("hold-both-terminal-root-b.gate");
        fs::write(&hold_b, "hold").unwrap();
        write_executable_script(
            &fixture.fake_agy_exe,
            &format!(
                "#!/bin/sh\nset -eu\ncase \"$*\" in\n  *CTRL_TASK_A*)\n    printf 'invalid_metadata: root A failed\\n' >&2\n    exit 1\n    ;;\n  *CTRL_TASK_B*)\n    while [ -f '{}' ]; do sleep 0.02; done\n    {}\n    ;;\n  *)\n    {}\n    ;;\nesac\n",
                hold_b.display(),
                exec_node,
                exec_node,
            ),
        );

        let host = Box::pin(ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            &approval_id,
        )))
        .await
        .unwrap();
        let client = ApprovedHostClient::new(
            Arc::clone(&fixture.authority),
            &fixture.sessions_dir,
            BINDING_A,
            THREAD_UUID_A,
        )
        .unwrap();
        let now = || {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        };
        let session = fixture
            .controller
            .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, now())
            .unwrap();
        session.step(now()).await.unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let snap_a = client.get_task(&approval_id, "task-root-a").await.unwrap();
            let snap_b = client.get_task(&approval_id, "task-root-b").await.unwrap();
            if snap_a.status == TaskExecutionStatus::Failed
                && snap_b.status == TaskExecutionStatus::Running
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for root A failure while root B runs"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Reconcile root A's failed state into the workflow ledger while root B is still running,
        // then let root B complete on the host before driver.run_owner runs.
        session.step(now()).await.unwrap();
        session.release().unwrap();

        fs::remove_file(&hold_b).unwrap();
        let b_deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let snap_b = client.get_task(&approval_id, "task-root-b").await.unwrap();
            if snap_b.status == TaskExecutionStatus::ReadyForReview {
                break;
            }
            assert!(
                Instant::now() < b_deadline,
                "timed out waiting for root B to reach ReadyForReview on the host"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let driver = CoworkerCodeDriver::from_controller(
            fixture.root.join("runtime"),
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            Arc::clone(&fixture.authority),
            fixture.controller.clone(),
        )
        .unwrap();
        let key = CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_A, &job.job_id).unwrap();
        let digest = fixture
            .policy_store
            .get_job_runtime_readonly(BINDING_A, &job.job_id)
            .unwrap()
            .unwrap()
            .envelope
            .envelope_digest;
        let notices = coworker_notice_dir(&fixture.plans_dir);
        let sockets = fixture.root.join("both-terminal-native");
        fs::create_dir(&sockets).unwrap();
        fs::set_permissions(&sockets, fs::Permissions::from_mode(0o700)).unwrap();
        let socket = sockets.join("native.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
        CoworkerNoticeStore::new(&notices)
            .unwrap()
            .configure(
                CoworkerNoticeConfig::new(
                    key.clone(),
                    digest.clone(),
                    NativeToolOutputConfig::new_user_message_steer(
                        socket,
                        THREAD_UUID_A.into(),
                        fixture.repo_dir.clone(),
                    ),
                )
                .unwrap(),
            )
            .unwrap();

        let repo = fixture.repo_dir.to_string_lossy().to_string();
        let expected_wakeup = threadlane::format_coworker_job_wakeup_message(&job.job_id).unwrap();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            drop(listener);
            let mut ws = accept_async(stream).await.unwrap();
            while let Some(frame) = ws.next().await {
                let Message::Text(text) = frame.unwrap() else {
                    continue;
                };
                let request: Value = serde_json::from_str(&text).unwrap();
                let method = request["method"].as_str().unwrap();
                if method == "initialized" {
                    continue;
                }
                let result = match method {
                    "initialize" => json!({}),
                    "thread/read" | "thread/resume" => json!({
                        "thread": {"id": THREAD_UUID_A, "cwd": repo, "status": {"type": "idle"}},
                        "cwd": repo
                    }),
                    "turn/start" => {
                        assert_eq!(request["params"]["input"][0]["text"], expected_wakeup);
                        let response = json!({
                            "turn": {
                                "id": "0196ed00-0000-7000-8000-0000000000cc",
                                "status": "inProgress"
                            }
                        });
                        ws.send(Message::Text(
                            json!({"id": request["id"], "result": response})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .unwrap();
                        return;
                    }
                    other => panic!("unexpected native method {other}"),
                };
                ws.send(Message::Text(
                    json!({"id": request["id"], "result": result})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            }
            panic!("expected settled attention wakeup");
        });

        let report = tokio::time::timeout(
            Duration::from_secs(10),
            Box::pin(driver.run_owner(
                BINDING_A,
                THREAD_UUID_A,
                &job.job_id,
                CoworkerDriverRunOptions {
                    max_steps: None,
                    poll_interval_ms: 5,
                    idle_linger_ms: Some(0),
                    execution_mode: Some(CoworkerDriverExecutionMode::FullAutomaticPipeline),
                },
            )),
        )
        .await
        .expect("both-terminal job must settle immediately")
        .unwrap();

        tokio::time::timeout(Duration::from_secs(5), peer)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(report.owner_state, CoworkerDriverOwnerState::NoActiveOwner);
        assert_eq!(
            report.overall_status,
            CoworkerControllerPhase1Status::BlockedOrFailed
        );
        assert_eq!(
            report.pipeline_status,
            Some(CoworkerPipelineStatus::BlockedOrFailed)
        );
        let task_b = report
            .tasks
            .iter()
            .find(|t| t.task_id == "task-root-b")
            .unwrap();
        assert_eq!(task_b.stage, CoworkerTaskStage::Running);
        assert_eq!(task_b.host_status, TaskExecutionStatus::ReadyForReview);
        assert!(task_b.fenced);
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0);
        assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
        assert_eq!(read_invocation_count(&fixture.check_count_file), 0);
        assert_eq!(
            CoworkerNoticeStore::inspect(&notices, &key, &digest)
                .unwrap()
                .attempt_count,
            1
        );
        let _ = host.shutdown().await;
    });
}

#[test]
fn held_running_sibling_prevents_premature_quiescent_settlement_until_terminal_receipt_is_reconciled(
) {
    run_with_16mib_test_stack(|| async move {
        let fixture = setup_fixture();
        let tasks = vec![
            PlanTask {
                id: "task-root-a".into(),
                title: "Root task A".into(),
                instruction: "CTRL_TASK_A implement src/a.rs".into(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/a.rs".into()],
            },
            PlanTask {
                id: "task-root-b".into(),
                title: "Root task B".into(),
                instruction: "CTRL_TASK_B implement src/b.rs".into(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/b.rs".into()],
            },
            PlanTask {
                id: "task-dep-c".into(),
                title: "Dependent task C".into(),
                instruction: "CTRL_TASK_C implement src/c.rs".into(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: vec!["task-root-a".into(), "task-root-b".into()],
                allowed_write_paths: vec!["src/c.rs".into()],
            },
        ];
        let mut ifaces = BTreeMap::new();
        ifaces.insert(
            "task-root-a".into(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: vec!["iface_a".into()],
                consumed_interfaces: Vec::new(),
            },
        );
        ifaces.insert(
            "task-root-b".into(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: vec!["iface_b".into()],
                consumed_interfaces: Vec::new(),
            },
        );
        ifaces.insert(
            "task-dep-c".into(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: vec!["iface_c".into()],
                consumed_interfaces: vec!["iface_a".into(), "iface_b".into()],
            },
        );
        let (job, approval_id) = create_planned_approved_and_frozen_job(
            &fixture,
            "brf_held_running_sibling_liveness",
            "plan-held-running-sibling-liveness",
            tasks,
            ifaces,
            vec!["src/a.rs".into(), "src/b.rs".into(), "src/c.rs".into()],
            2,
            // Finite worker hard deadline must exceed the 40s owner/control window plus setup and
            // terminal reconciliation.
            ExecutionBudgets {
                timeout_ms: 120_000,
                ..ExecutionBudgets::default()
            },
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 3),
            IntegrationStrategy::MergeNoFf,
            true,
        )
        .await;

        let original_agy = fs::read_to_string(&fixture.fake_agy_exe).unwrap();
        let exec_node = original_agy
            .lines()
            .find(|line| line.starts_with("exec node "))
            .unwrap()
            .to_string();
        let root_a_starts = fixture.root.join("held-root-a-starts");
        let root_b_starts = fixture.root.join("held-root-b-starts");
        let hold_b = fixture.root.join("hold-held-root-b.gate");
        fs::write(&hold_b, "hold").unwrap();
        write_executable_script(
            &fixture.fake_agy_exe,
            &format!(
                "#!/bin/sh\nset -eu\ncase \"$*\" in\n  *CTRL_TASK_A*)\n    printf 'started\\n' >> '{}'\n    printf 'invalid_metadata: root A failed\\n' >&2\n    exit 1\n    ;;\n  *CTRL_TASK_B*)\n    printf 'started\\n' >> '{}'\n    while [ -f '{}' ]; do sleep 0.02; done\n    {}\n    ;;\n  *)\n    {}\n    ;;\nesac\n",
                root_a_starts.display(),
                root_b_starts.display(),
                hold_b.display(),
                exec_node,
                exec_node,
            ),
        );

        let host = Box::pin(ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            &approval_id,
        )))
        .await
        .unwrap();
        let client = ApprovedHostClient::new(
            Arc::clone(&fixture.authority),
            &fixture.sessions_dir,
            BINDING_A,
            THREAD_UUID_A,
        )
        .unwrap();
        let now = || {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        };
        let session = fixture
            .controller
            .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, now())
            .unwrap();
        session.step(now()).await.unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let snap_a = client.get_task(&approval_id, "task-root-a").await.unwrap();
            let snap_b = client.get_task(&approval_id, "task-root-b").await.unwrap();
            if snap_a.status == TaskExecutionStatus::Failed
                && snap_b.status == TaskExecutionStatus::Running
                && root_b_starts.is_file()
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for root A failure while root B runs"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Reconcile root A's failure and root B's running state into the workflow ledger.
        session.step(now()).await.unwrap();
        session.release().unwrap();

        // Apply a narrow workflow hold to running root B while failed root A stays Failed.
        let current_job = fixture
            .workflow_store
            .read_binding_ledger(BINDING_A)
            .unwrap()
            .jobs
            .into_iter()
            .find(|j| j.job_id == job.job_id)
            .unwrap();
        let (held_job, _) = fixture
            .workflow_store
            .apply_workflow_hold(&ApplyWorkflowHoldRequest {
                binding_id: BINDING_A.into(),
                thread_uuid: THREAD_UUID_A.into(),
                job_id: job.job_id.clone(),
                expected_job_revision: current_job.job_revision,
                expected_ledger_revision: None,
                request_key: "hold_root_b_narrow".into(),
                reason: WorkflowHoldReason::ConversationClarificationPending,
                detail: "narrow hold on root B while running".into(),
                directly_affected_task_ids: vec!["task-root-b".into()],
                affected_write_paths: vec!["src/b.rs".into()],
                affected_interfaces: vec!["iface_b".into()],
                holds_entire_job: false,
            })
            .unwrap();
        let held_task_a = held_job
            .tasks
            .iter()
            .find(|t| t.task_id == "task-root-a")
            .unwrap();
        let held_task_b = held_job
            .tasks
            .iter()
            .find(|t| t.task_id == "task-root-b")
            .unwrap();
        assert_eq!(held_task_a.stage, CoworkerTaskStage::Failed);
        assert_eq!(held_task_b.stage, CoworkerTaskStage::Held);

        let driver = CoworkerCodeDriver::from_controller(
            fixture.root.join("runtime"),
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            Arc::clone(&fixture.authority),
            fixture.controller.clone(),
        )
        .unwrap();

        // While Held root B is still physically running on the host after ControllerSession::release
        // and before any driver owner exists, read-only status keeps overall_status RunningTasks and
        // conservatively projects pipeline_status BlockedOnUnknownEffect for the ownerless reserved effect.
        let live_status = driver
            .status(BINDING_A, THREAD_UUID_A, &job.job_id)
            .await
            .unwrap();
        assert_eq!(
            live_status.overall_status,
            CoworkerControllerPhase1Status::RunningTasks,
            "Held task with active running attempt must keep overall_status RunningTasks"
        );
        assert_eq!(
            live_status.pipeline_status,
            Some(CoworkerPipelineStatus::BlockedOnUnknownEffect),
            "read-only ownerless status before driver owner exists must conservatively project BlockedOnUnknownEffect for reserved running effect"
        );

        // Run a bounded owner for two ticks while Held root B remains physically running on the
        // host: the precheck must not settle after tick 1, so the owner completes both ticks and
        // exits only because of the explicit max_steps=2 cap. Once it releases physical ownership,
        // the returned post-owner report projects BlockedOnUnknownEffect for the now-ownerless
        // reserved effect while physical worker liveness remains RunningTasks.
        let step_report = Box::pin(driver.run_owner(
            BINDING_A,
            THREAD_UUID_A,
            &job.job_id,
            CoworkerDriverRunOptions {
                max_steps: Some(2),
                poll_interval_ms: 5,
                execution_mode: Some(CoworkerDriverExecutionMode::FullAutomaticPipeline),
                ..Default::default()
            },
        ))
        .await
        .unwrap();
        let key = CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_A, &job.job_id).unwrap();
        let persisted_state: CoworkerDriverStateRecord = serde_json::from_slice(
            &fs::read(driver.state_file_path_for_key(&key).unwrap()).unwrap(),
        )
        .unwrap();
        assert_eq!(
            persisted_state.steps_completed, 2,
            "precheck must not settle after one tick while Held root B is physically running; exit must be due to the max_steps=2 cap"
        );
        assert!(
            !persisted_state.owner_active,
            "bounded owner must release active ownership on graceful cap exit"
        );
        assert!(
            persisted_state.graceful_exit_completed,
            "bounded owner cap exit must record graceful_exit_completed"
        );
        assert_eq!(
            step_report.overall_status,
            CoworkerControllerPhase1Status::RunningTasks
        );
        assert_eq!(
            step_report.pipeline_status,
            Some(CoworkerPipelineStatus::BlockedOnUnknownEffect),
            "post-bounded-owner report after owner exit must project BlockedOnUnknownEffect for the ownerless reserved effect while physical worker liveness stays RunningTasks"
        );
        let live_b = step_report
            .tasks
            .iter()
            .find(|t| t.task_id == "task-root-b")
            .unwrap();
        assert_eq!(live_b.stage, CoworkerTaskStage::Held);
        assert_eq!(live_b.host_status, TaskExecutionStatus::Running);
        assert!(live_b.fenced);

        // Release fake root B so its original worker completes on the host.
        fs::remove_file(&hold_b).unwrap();
        let b_deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let snap_b = client.get_task(&approval_id, "task-root-b").await.unwrap();
            if snap_b.status == TaskExecutionStatus::ReadyForReview {
                break;
            }
            assert!(
                Instant::now() < b_deadline,
                "timed out waiting for Held root B to reach ReadyForReview on the host"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Now run_owner must reconcile Held root B's terminal receipt without promoting/accepting
        // the Held output, running review/checks, or dispatching dependent task C, and then settle.
        let settled_report = tokio::time::timeout(
            Duration::from_secs(10),
            Box::pin(driver.run_owner(
                BINDING_A,
                THREAD_UUID_A,
                &job.job_id,
                CoworkerDriverRunOptions {
                    max_steps: None,
                    poll_interval_ms: 5,
                    idle_linger_ms: Some(0),
                    execution_mode: Some(CoworkerDriverExecutionMode::FullAutomaticPipeline),
                },
            )),
        )
        .await
        .expect("owner must settle once Held sibling's terminal receipt is reconciled")
        .unwrap();

        assert_eq!(
            settled_report.owner_state,
            CoworkerDriverOwnerState::NoActiveOwner
        );
        assert_eq!(
            settled_report.overall_status,
            CoworkerControllerPhase1Status::Held
        );
        assert_eq!(
            settled_report.pipeline_status,
            Some(CoworkerPipelineStatus::Held)
        );

        let settled_b = settled_report
            .tasks
            .iter()
            .find(|t| t.task_id == "task-root-b")
            .unwrap();
        assert_eq!(
            settled_b.stage,
            CoworkerTaskStage::Held,
            "terminal Held output must remain Held and unaccepted"
        );
        assert_eq!(settled_b.host_status, TaskExecutionStatus::ReadyForReview);
        assert!(settled_b.fenced);
        assert_eq!(
            settled_b.observed_model.as_deref(),
            Some("gemini-4-argon-high")
        );
        assert_eq!(settled_b.observed_effort.as_deref(), Some("high"));

        let settled_c = settled_report
            .tasks
            .iter()
            .find(|t| t.task_id == "task-dep-c")
            .unwrap();
        // The narrow hold on root B and iface_b propagates to dependent task C because C consumes iface_b.
        assert_eq!(
            settled_c.stage,
            CoworkerTaskStage::Held,
            "narrow hold on root B and iface_b must propagate to dependent task C without dispatching it"
        );

        let runtime_snapshot = fixture
            .policy_store
            .get_job_runtime_readonly(BINDING_A, &job.job_id)
            .unwrap()
            .unwrap();
        assert_eq!(runtime_snapshot.total_task_attempts_reserved, 2);
        let artifacts_b = runtime_snapshot
            .get_attempt_artifacts("task-root-b", 1)
            .expect("Held sibling terminal receipt must persist inspectable attempt artifacts");
        assert!(artifacts_b.committed_head_commit.is_some());
        assert!(artifacts_b.committed_tree_sha.is_some());
        assert_eq!(
            artifacts_b.observed_model.as_deref(),
            Some("gemini-4-argon-high")
        );
        assert_eq!(artifacts_b.observed_effort.as_deref(), Some("high"));
        assert!(artifacts_b.review_subject.is_none());
        assert!(artifacts_b.review_receipt.is_none());
        assert!(artifacts_b.integration_receipt.is_none());
        assert!(runtime_snapshot
            .pending_effects
            .values()
            .filter(|eff| eff.task_id.as_deref() == Some("task-root-b"))
            .all(|eff| eff.status == PendingControllerEffectStatus::EffectConfirmed));

        assert_eq!(
            fs::read_to_string(&root_a_starts).unwrap().lines().count(),
            1
        );
        assert_eq!(
            fs::read_to_string(&root_b_starts).unwrap().lines().count(),
            1
        );
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0);
        assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
        assert_eq!(read_invocation_count(&fixture.check_count_file), 0);

        let _ = host.shutdown().await;
    });
}

#[cfg(target_os = "linux")]
#[test]
fn superseded_role_health_retains_history_and_result_without_reserving_a_new_notice() {
    run_with_16mib_test_stack(|| async move {
        use threadlane_native::coworker_workflow::deterministic_controller_instance_id;
        use threadlane_native::provider_health::{supervise_health, ScopedHealthSink};

        // The unchanged baseline proves this exact reservation/configuration is notice eligible.
        for superseded in [false, true] {
            let fixture = setup_fixture();
            let (job, _) = create_planned_approved_and_frozen_job(
                &fixture,
                "brf_health_supersession",
                "plan-health-supersession",
                vec![PlanTask {
                    id: "task-health".into(),
                    title: "Health scope".into(),
                    instruction: "Read bounded fixture state".into(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: vec![],
                    allowed_write_paths: vec!["src/a.rs".into()],
                }],
                BTreeMap::new(),
                vec!["src/a.rs".into()],
                1,
                ExecutionBudgets::default(),
                ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 1),
                IntegrationStrategy::MergeNoFf,
                true,
            )
            .await;
            let session = fixture
                .controller
                .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 20_000)
                .unwrap();
            let key = session.guard().key();
            let instance = deterministic_controller_instance_id(
                BINDING_A,
                &job.job_id,
                session.guard().instance_id(),
            );
            let lease = fixture
                .policy_store
                .acquire_or_renew_lease(BINDING_A, &job.job_id, &instance, 20_000, 30_000)
                .unwrap();
            let effect_id = "eff_11111111111111111111111111111111";
            let subject = "a".repeat(64);
            fixture
                .policy_store
                .mutate_job_runtime(
                    BINDING_A,
                    &job.job_id,
                    &instance,
                    lease.lease_epoch,
                    |state| {
                        let _ = state.reserve_role_call(WorkflowRole::Reviewer)?;
                        state.pending_effects.insert(
                            effect_id.into(),
                            PendingControllerEffect {
                                effect_id: effect_id.into(),
                                request_key: "req_health_supersession".into(),
                                job_id: job.job_id.clone(),
                                task_id: Some("task-health".into()),
                                attempt_no: Some(1),
                                kind: PendingControllerEffectKind::ReviewerInvocation,
                                status: PendingControllerEffectStatus::ReservedBeforeEffect,
                                subject_digest: subject.clone(),
                                external_correlation_id: None,
                                created_at_ms: 20_000,
                                updated_at_ms: 20_000,
                                diagnostic: None,
                            },
                        );
                        Ok(())
                    },
                )
                .unwrap();
            // Construct the live sink before amendment, as an already running role would.
            let sink = ScopedHealthSink::controller(
                &fixture.controller,
                session.guard(),
                effect_id,
                WorkflowRole::Reviewer,
                Some("task-health"),
                Some(1),
                &subject,
            )
            .unwrap();
            let before = fixture
                .policy_store
                .get_job_runtime_readonly(BINDING_A, &job.job_id)
                .unwrap()
                .unwrap();
            if superseded {
                let mut brief = job.work_brief.clone();
                brief.brief_id = "brf_health_amendment".into();
                brief.kind = WorkBriefKind::Amendment {
                    target_job_id: job.job_id.clone(),
                    target_job_revision: job.job_revision,
                    amendment_reason: "Refine fixture requirements".into(),
                };
                fixture
                    .workflow_store
                    .create_or_amend_job(&CreateCoworkerJobRequest {
                        binding_id: BINDING_A.into(),
                        thread_uuid: THREAD_UUID_A.into(),
                        request_key: "req_health_amendment".into(),
                        expected_ledger_revision: None,
                        work_brief: brief,
                        amended_target_task_ids: vec![],
                        amended_write_paths: vec![],
                        amended_interfaces: vec![],
                    })
                    .unwrap();
            }
            let directory = coworker_notice_dir(&fixture.plans_dir);
            let sockets = fixture.root.join("health-supersession-native");
            fs::create_dir(&sockets).unwrap();
            fs::set_permissions(&sockets, fs::Permissions::from_mode(0o700)).unwrap();
            let socket = sockets.join("health-refused.sock");
            // Keep the socket inode but close its listener: a wrongly eligible notice would
            // still reserve a durable attempt and fail promptly, with no external endpoint.
            drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());
            fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
            let digest = &before.envelope.envelope_digest;
            CoworkerNoticeStore::new(&directory)
                .unwrap()
                .configure(
                    CoworkerNoticeConfig::new(
                        key.clone(),
                        digest.clone(),
                        NativeToolOutputConfig::new_user_message_steer(
                            socket,
                            THREAD_UUID_A.into(),
                            fixture.repo_dir.clone(),
                        ),
                    )
                    .unwrap(),
                )
                .unwrap();

            let mut parser =
                threadlane_workers::antigravity_diagnostics::AntigravityDiagnosticParser::new();
            parser.ingest_at(0, b"I0101 00:00:01.000000 1 run.go:315] Run: attempt 1 failed (stream reading error: stream idle timeout (5m0s): context deadline exceeded), retrying in 4s.\n", 10);
            let hint = parser.snapshot();
            let expected = hint.clone();
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                supervise_health(
                    Some(&sink),
                    |observer| {
                        let fixture = &fixture;
                        let job = &job;
                        let directory = &directory;
                        let key = &key;
                        let hint = hint.clone();
                        let expected = expected.clone();
                        async move {
                            observer.send_replace(Some(hint.clone()));
                            let deadline = Instant::now() + Duration::from_secs(2);
                            loop {
                                let state = fixture
                                    .policy_store
                                    .get_job_runtime_readonly(BINDING_A, &job.job_id)
                                    .unwrap()
                                    .unwrap();
                                if let Some(record) = state.provider_health.get(effect_id) {
                                    assert!(!record.terminal_observed);
                                    assert_eq!(record.snapshot, expected);
                                    break;
                                }
                                assert!(
                                    Instant::now() < deadline,
                                    "missing durable diagnostic history"
                                );
                                tokio::task::yield_now().await;
                            }
                            if superseded {
                                // Allow the notice consumer to run while the invocation remains live.
                                tokio::time::sleep(Duration::from_millis(250)).await;
                            } else {
                                while CoworkerNoticeStore::inspect(directory, key, digest)
                                    .unwrap()
                                    .attempt_count
                                    == 0
                                {
                                    assert!(
                                        Instant::now() < deadline,
                                        "baseline did not reserve a notice"
                                    );
                                    tokio::task::yield_now().await;
                                }
                            }
                            Ok::<_, ()>((false, hint.clone()))
                        }
                    },
                    |result| Some(&result.1),
                ),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(
                !result.0,
                "diagnostics cannot replace the independent provider result"
            );
            assert_eq!(result.1, expected);
            let after = fixture
                .policy_store
                .get_job_runtime_readonly(BINDING_A, &job.job_id)
                .unwrap()
                .unwrap();
            assert_eq!(after.envelope, before.envelope);
            assert_eq!(after.pending_effects, before.pending_effects);
            assert_eq!(after.stop_requested, before.stop_requested);
            assert_eq!(after.provider_health[effect_id].snapshot, expected);
            assert!(after.provider_health[effect_id].terminal_observed);
            assert_eq!(
                CoworkerNoticeStore::inspect(&directory, &key, digest)
                    .unwrap()
                    .attempt_count,
                if superseded { 0 } else { 1 },
            );
            drop(sink);
            session.release().unwrap();
        }
    });
}

#[cfg(target_os = "linux")]
#[test]
fn controller_first_closed_lost_or_truncated_hint_is_history_while_cleanup_is_held() {
    run_with_16mib_test_stack(|| async move {
        use futures_util::poll;
        use threadlane_native::coworker_workflow::deterministic_controller_instance_id;
        use threadlane_native::provider_health::{
            controller_health_views, supervise_health, ScopedHealthSink,
        };
        use threadlane_workers::antigravity_diagnostics::AntigravityDiagnosticParser;
        use threadlane_workers::provider_health::ProviderDiagnosticUnavailableReason as Reason;

        for role in [
            WorkflowRole::Reviewer,
            WorkflowRole::Coder,
            WorkflowRole::Closure,
        ] {
            for reason in [
                Reason::SourceClosed,
                Reason::SourceLost,
                Reason::ResourceLimit,
            ] {
                let fixture = setup_fixture();
                let (job, _) = create_planned_approved_and_frozen_job(
                    &fixture,
                    "brf_health_closed",
                    "plan-health-closed",
                    vec![PlanTask {
                        id: "task-health".into(),
                        title: "Final drain hint".into(),
                        instruction: "Read bounded fixture state".into(),
                        role: WorkflowRole::Coder,
                        difficulty: Difficulty::Hard,
                        depends_on: vec![],
                        allowed_write_paths: vec!["src/a.rs".into()],
                    }],
                    BTreeMap::new(),
                    vec!["src/a.rs".into()],
                    1,
                    ExecutionBudgets::default(),
                    ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 1),
                    IntegrationStrategy::MergeNoFf,
                    true,
                )
                .await;
                let session = fixture
                    .controller
                    .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 20_000)
                    .unwrap();
                let key = session.guard().key();
                let instance = deterministic_controller_instance_id(
                    BINDING_A,
                    &job.job_id,
                    session.guard().instance_id(),
                );
                let lease = fixture
                    .policy_store
                    .acquire_or_renew_lease(BINDING_A, &job.job_id, &instance, 20_000, 30_000)
                    .unwrap();
                let effect_id = "eff_11111111111111111111111111111111";
                let subject = "a".repeat(64);
                let task_id = (role != WorkflowRole::Closure).then_some("task-health");
                let attempt_no = task_id.map(|_| 2);
                let kind = match role {
                    WorkflowRole::Reviewer => PendingControllerEffectKind::ReviewerInvocation,
                    WorkflowRole::Coder => PendingControllerEffectKind::CoderDispatch,
                    WorkflowRole::Closure => PendingControllerEffectKind::ClosureInvocation,
                    _ => unreachable!(),
                };
                fixture
                    .policy_store
                    .mutate_job_runtime(
                        BINDING_A,
                        &job.job_id,
                        &instance,
                        lease.lease_epoch,
                        |state| {
                            let _ = state.reserve_role_call(role)?;
                            state.pending_effects.insert(
                                effect_id.into(),
                                PendingControllerEffect {
                                    effect_id: effect_id.into(),
                                    request_key: "req_health_closed".into(),
                                    job_id: job.job_id.clone(),
                                    task_id: task_id.map(str::to_owned),
                                    attempt_no,
                                    kind,
                                    status: PendingControllerEffectStatus::ReservedBeforeEffect,
                                    subject_digest: subject.clone(),
                                    external_correlation_id: None,
                                    created_at_ms: 20_000,
                                    updated_at_ms: 20_000,
                                    diagnostic: None,
                                },
                            );
                            Ok(())
                        },
                    )
                    .unwrap();
                let sink = ScopedHealthSink::controller(
                    &fixture.controller,
                    session.guard(),
                    effect_id,
                    role,
                    task_id,
                    attempt_no,
                    &subject,
                )
                .unwrap();
                let before = fixture
                    .policy_store
                    .get_job_runtime_readonly(BINDING_A, &job.job_id)
                    .unwrap()
                    .unwrap();
                let directory = coworker_notice_dir(&fixture.plans_dir);
                let sockets = fixture.root.join("health-closed-native");
                fs::create_dir(&sockets).unwrap();
                fs::set_permissions(&sockets, fs::Permissions::from_mode(0o700)).unwrap();
                let socket = sockets.join("health.sock");
                let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
                listener.set_nonblocking(true).unwrap();
                fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
                let digest = &before.envelope.envelope_digest;
                CoworkerNoticeStore::new(&directory)
                    .unwrap()
                    .configure(
                        CoworkerNoticeConfig::new(
                            key.clone(),
                            digest.clone(),
                            NativeToolOutputConfig::new_user_message_steer(
                                socket,
                                THREAD_UUID_A.into(),
                                fixture.repo_dir.clone(),
                            ),
                        )
                        .unwrap(),
                    )
                    .unwrap();

                let mut parser = AntigravityDiagnosticParser::new();
                let quiet = parser.snapshot();
                parser.ingest_at(0, b"I0101 00:00:01.000000 1 run.go:315] Run: attempt 7 failed (stream reading error: stream idle timeout (5m0s): context deadline exceeded), retrying in 4s.\n", 10);
                parser.unavailable(reason, 11);
                parser.unavailable(Reason::SourceClosed, 12);
                let hint = parser.snapshot();
                assert_eq!(hint.unavailable_reason, Some(reason));
                assert!(hint.is_valid());
                let expected = hint.clone();
                let (drain_tx, drain_rx) = tokio::sync::oneshot::channel();
                let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
                let mut supervision = Box::pin(supervise_health(
                    Some(&sink),
                    |observer| async move {
                        observer.send_replace(Some(quiet));
                        drain_rx.await.unwrap();
                        // No hint was visible until coverage had already ended.
                        observer.send_replace(Some(hint.clone()));
                        cleanup_rx.await.unwrap();
                        if role == WorkflowRole::Closure {
                            Err("injected provider error after final drain")
                        } else {
                            Ok((false, hint))
                        }
                    },
                    |result| Some(&result.1),
                ));
                assert!(poll!(supervision.as_mut()).is_pending());
                assert!(poll!(supervision.as_mut()).is_pending());
                assert!(poll!(supervision.as_mut()).is_pending());
                let quiet_state = fixture
                    .policy_store
                    .get_job_runtime_readonly(BINDING_A, &job.job_id)
                    .unwrap()
                    .unwrap();
                let owner = session.guard().validate_before_effect(&key).unwrap();
                let quiet_view = controller_health_views(&quiet_state, Some(owner));
                assert_eq!(quiet_view.len(), 1);
                assert!(
                    quiet_view[0].live,
                    "available coverage can be live without a hint"
                );
                assert!(quiet_view[0].record.snapshot.current_failure.is_none());

                drain_tx.send(()).unwrap();
                assert!(poll!(supervision.as_mut()).is_pending());
                assert!(poll!(supervision.as_mut()).is_pending());
                assert!(poll!(supervision.as_mut()).is_pending());
                let held = fixture
                    .policy_store
                    .get_job_runtime_readonly(BINDING_A, &job.job_id)
                    .unwrap()
                    .unwrap();
                assert_eq!(held.provider_health[effect_id].snapshot, expected);
                assert!(!held.provider_health[effect_id].terminal_observed);
                assert_eq!(held.pending_effects, before.pending_effects);
                assert_eq!(held.lease, before.lease);
                let owner = session.guard().validate_before_effect(&key).unwrap();
                let views = controller_health_views(&held, Some(owner));
                assert_eq!(views.len(), 1, "closed coverage remains readable history");
                assert!(!views[0].live, "{role:?}/{reason:?}");
                let notices = CoworkerNoticeStore::inspect(&directory, &key, digest).unwrap();
                assert_eq!(
                    notices.attempt_count, 0,
                    "no early intent for {role:?}/{reason:?}"
                );
                assert!(notices.latest.is_none());
                assert_eq!(
                    listener.accept().unwrap_err().kind(),
                    std::io::ErrorKind::WouldBlock
                );

                cleanup_tx.send(()).unwrap();
                let outcome = tokio::time::timeout(Duration::from_secs(5), supervision)
                    .await
                    .unwrap();
                if role == WorkflowRole::Closure {
                    assert_eq!(outcome, Err("injected provider error after final drain"));
                } else {
                    let outcome = outcome.unwrap();
                    assert!(
                        !outcome.0,
                        "coverage cannot change the provider's non-success result"
                    );
                    assert_eq!(outcome.1, expected);
                }
                let after = fixture
                    .policy_store
                    .get_job_runtime_readonly(BINDING_A, &job.job_id)
                    .unwrap()
                    .unwrap();
                assert_eq!(after.envelope, before.envelope);
                assert_eq!(after.pending_effects, before.pending_effects);
                assert_eq!(after.stop_requested, before.stop_requested);
                assert_eq!(after.provider_health[effect_id].snapshot, expected);
                assert!(after.provider_health[effect_id].terminal_observed);
                assert!(after.provider_health[effect_id]
                    .snapshot
                    .current_failure
                    .is_some());
                assert!(after.provider_health[effect_id].snapshot.recovery.is_none());
                assert_eq!(after.provider_health[effect_id].snapshot.retry_count, 1);
                assert_eq!(
                    after.provider_health[effect_id].snapshot.retry_high_water,
                    Some(7)
                );
                assert_eq!(
                    CoworkerNoticeStore::inspect(&directory, &key, digest)
                        .unwrap()
                        .attempt_count,
                    0
                );
                assert_eq!(
                    listener.accept().unwrap_err().kind(),
                    std::io::ErrorKind::WouldBlock
                );
                drop(sink);
                session.release().unwrap();
            }
        }
    });
}
