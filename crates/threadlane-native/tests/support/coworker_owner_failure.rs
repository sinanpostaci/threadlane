//! Offline owner-error regression over real ownership, admission, persistence, and native transport.
use super::*;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use threadlane::NativeToolOutputConfig;
use threadlane_native::coworker_driver::{
    CoworkerCodeDriver, CoworkerDriverFailureStage, CoworkerDriverRunOptions,
};
use threadlane_native::coworker_notice::{
    coworker_notice_dir, CoworkerNoticeConfig, CoworkerNoticeStore, NoticeDeliveryStatus,
};
use threadlane_native::coworker_workflow::{CoworkerJobLockState, CoworkerJobOwnershipKey};
use tokio::net::UnixListener;
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_loop_failure_is_durable_before_one_native_attempt_even_after_stop() {
    Box::pin(async {
        for outcome in ["submitted", "unknown", "pre_send_failure", "legacy"] {
            let fixture = setup_fixture();
            let (job, _) = create_planned_approved_and_frozen_job(
                &fixture, "brf_owner_failure", "plan-owner-failure",
                vec![PlanTask {
                    id: "task-owner-failure".into(), title: "Offline owner failure".into(),
                    instruction: "CTRL_TASK_A implement src/a.rs".into(), role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard, depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/a.rs".into()],
                }], BTreeMap::new(), vec!["src/a.rs".into()], 1, ExecutionBudgets::default(),
                ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 1),
                IntegrationStrategy::MergeNoFf, true,
            ).await;
            let driver = CoworkerCodeDriver::from_controller(
                fixture.root.join("runtime"), &fixture.registry_dir, &fixture.plans_dir,
                &fixture.authority_dir, &fixture.sessions_dir, Arc::clone(&fixture.authority),
                fixture.controller.clone(),
            ).unwrap();
            let key = CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_A, &job.job_id).unwrap();
            let digest = fixture.policy_store.get_job_runtime_readonly(BINDING_A, &job.job_id)
                .unwrap().unwrap().envelope.envelope_digest;
            let notice_root = coworker_notice_dir(&fixture.plans_dir);
            let socket_dir = fixture.root.join("native-socket");
            fs::create_dir(&socket_dir).unwrap();
            fs::set_permissions(&socket_dir, fs::Permissions::from_mode(0o700)).unwrap();
            let socket = socket_dir.join("fatal.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
            if outcome != "legacy" {
                CoworkerNoticeStore::new(&notice_root).unwrap().configure(CoworkerNoticeConfig::new(
                    key.clone(), digest.clone(), NativeToolOutputConfig::new_user_message_steer(
                        socket, THREAD_UUID_A.to_string(), fixture.repo_dir.clone()),
                ).unwrap()).unwrap();
            }
            let peer = if matches!(outcome, "pre_send_failure" | "legacy") {
                drop(listener);
                None
            } else {
                let repo = fixture.repo_dir.to_string_lossy().to_string();
                let peer_driver = driver.clone();
                let peer_key = key.clone();
                Some(tokio::spawn(async move {
                    let (stream, _) = listener.accept().await.unwrap();
                    drop(listener);
                    let mut ws = accept_async(stream).await.unwrap();
                    while let Some(frame) = ws.next().await {
                        let Message::Text(text) = frame.unwrap() else { continue; };
                        let request: Value = serde_json::from_str(&text).unwrap();
                        let method = request["method"].as_str().unwrap();
                        if method == "initialized" { continue; }
                        let result = match method {
                            "initialize" => json!({}),
                            "thread/read" | "thread/resume" => json!({
                                "thread": {"id": THREAD_UUID_A, "cwd": repo, "status": {"type": "idle"}},
                                "cwd": repo,
                            }),
                            "turn/start" => {
                                // Diagnosis must be private and durable before the external effect;
                                // the physical owner must still be held at this point.
                                assert!(peer_driver.read_owner_failure_optional(&peer_key).unwrap().is_some());
                                assert!(matches!(peer_driver.controller().ownership_store().inspect_job_lock(&peer_key).unwrap(),
                                    CoworkerJobLockState::HeldByActiveProcess { .. }));
                                assert_eq!(request["params"]["input"][0]["text"],
                                    threadlane::format_coworker_job_wakeup_message(&peer_key.job_id).unwrap());
                                if outcome == "unknown" { return request; }
                                let result = json!({"turn": {"id": "0196ed00-0000-7000-8000-0000000000aa", "status": "inProgress"}});
                                ws.send(Message::Text(json!({"id": request["id"], "result": result}).to_string().into())).await.unwrap();
                                return request;
                            }
                            other => panic!("unexpected native method {other}"),
                        };
                        ws.send(Message::Text(json!({"id": request["id"], "result": result}).to_string().into())).await.unwrap();
                    }
                    panic!("native peer did not receive one turn/start");
                }))
            };
            let state_file = driver.state_file_path_for_key(&key).unwrap();
            let run = Box::pin(driver.run_owner(BINDING_A, THREAD_UUID_A, &job.job_id, CoworkerDriverRunOptions {
                poll_interval_ms: 500, ..CoworkerDriverRunOptions::default()
            }));
            let force_failure = async {
                let deadline = Instant::now() + Duration::from_secs(15);
                while !state_file.is_file() {
                    assert!(Instant::now() < deadline, "initial owner state was not created");
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                if outcome != "submitted" {
                    fixture.policy_store.mutate_job_runtime_for_operator(BINDING_A, &job.job_id, |state| {
                        state.stop_requested = true;
                        Ok(())
                    }).unwrap();
                }
                // Fail the real next step's private-file check without changing execution gates.
                fs::set_permissions(&state_file, fs::Permissions::from_mode(0o644)).unwrap();
                driver.read_driver_state_optional(&key).unwrap_err()
            };
            let (result, primary_error) = tokio::join!(run, force_failure);
            assert_eq!(result.unwrap_err(), primary_error, "notice errors must not replace the original owner error");
            assert_eq!(primary_error.code, NativeErrorCode::SecurityViolation);
            fs::set_permissions(&state_file, fs::Permissions::from_mode(0o600)).unwrap();
            if let Some(peer) = peer {
                tokio::time::timeout(Duration::from_secs(5), peer).await.unwrap().unwrap();
            }
            let failure = driver.read_owner_failure_optional(&key).unwrap().unwrap();
            assert_eq!(failure.code, primary_error.code);
            assert_eq!(failure.stage, CoworkerDriverFailureStage::PipelineTick);
            assert!(!failure.message.contains(&primary_error.message));
            let status = driver.status(BINDING_A, THREAD_UUID_A, &job.job_id).await.unwrap();
            assert_eq!(status.fatal_error, Some(failure.clone()));
            let mut legacy_status = serde_json::to_value(&status).unwrap();
            legacy_status.as_object_mut().unwrap().remove("fatal_error");
            assert!(serde_json::from_value::<threadlane_native::CoworkerDriverStatusReport>(legacy_status).unwrap().fatal_error.is_none());
            assert_ne!(status.job_phase, CoworkerJobPhase::Completed);
            assert_eq!(status.stop_requested, outcome != "submitted");
            let metadata = CoworkerNoticeStore::inspect(&notice_root, &key, &digest).unwrap();
            if outcome == "legacy" {
                assert!(!metadata.configured);
                assert_eq!(metadata.attempt_count, 0);
                assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 0);
                continue;
            }
            assert_eq!(metadata.attempt_count, 1);
            assert_eq!(metadata.latest.unwrap().status, match outcome {
                "submitted" => NoticeDeliveryStatus::Submitted,
                "unknown" => NoticeDeliveryStatus::Unknown,
                _ => NoticeDeliveryStatus::Failed,
            });
            // A new physical owner, changed revision, and persisted Unknown/Failed/Submitted all
            // retain the same fatal fingerprint; no provider call or native replay is permitted.
            let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
            let session = fixture.controller.open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, now_ms).unwrap();
            let mut changed = failure;
            changed.job_revision += 1;
            let duplicate = CoworkerNoticeStore::new(&notice_root).unwrap().notify_fatal_error(&session, &changed).await.unwrap().unwrap();
            assert!(duplicate.was_duplicate);
            assert_eq!(duplicate.attempt_count, 1);
            session.release().unwrap();
            assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 0);
            assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0);
            assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
        }
    }).await;
}
