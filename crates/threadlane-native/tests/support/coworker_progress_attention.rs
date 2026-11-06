//! Offline progress attention traverses a real fake process, host ledger, owner, and native WS.
use super::*;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use threadlane::NativeToolOutputConfig;
use threadlane_native::coworker_driver::{
    CoworkerCodeDriver, CoworkerDriverExecutionMode, CoworkerDriverFailureStage,
    CoworkerDriverRunOptions,
};
use threadlane_native::coworker_notice::{
    coworker_notice_dir, CoworkerNoticeConfig, CoworkerNoticeStore, NoticeDeliveryStatus,
};
use threadlane_native::coworker_workflow::{CoworkerJobLockState, CoworkerJobOwnershipKey};
use threadlane_workers::ProgressWatchdogPolicy;
use tokio::net::UnixListener;
use tokio_tungstenite::{accept_async, tungstenite::Message};

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
