use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;
use tempfile::TempDir;
use threadlane::descriptor::{
    prepare_state_dir, write_descriptor_atomic, RuntimeDescriptor, SecretToken, DESCRIPTOR_VERSION,
    LOOPBACK_HOST,
};
use threadlane::{
    ApiError, BindingListResponse, CreateBindingRequest, ErrorCode, ScopedRequest, ScopedResponse,
    StatusResponse, Supervisor, SupervisorClient, SupervisorConfig,
};
use threadlane_core::{
    ActivityPage, Binding, Event, EventKind, EventPayload, Question, QuestionStatus, Run,
    RunStatus, RunSummary, WakeupNotice, WakeupStatus, WorkerAcceptanceState, WorkerOutcomeSummary,
    WorkerProgressSummary, WorkerSnapshot, WorkerStatus, WorkerWorktreeSummary,
    FIXED_WAKEUP_MESSAGE,
};
use threadlane_desktop_bridge::{
    discover_tool_status_from_path, AttachmentMode, BridgeError, ConnectionState, DesktopBridge,
    TaskKind, ToolAvailability, WindowAction, DEMO_ANSWER, DEMO_BINDING_LABEL, DEMO_INSTRUCTION,
    MAX_BINDINGS,
};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use uuid::Uuid;

type ActivityCall = (
    String,
    Option<u64>,
    Option<String>,
    Option<usize>,
    Option<String>,
    Option<EventKind>,
);

#[derive(Default)]
struct FakeServerData {
    expected_token: String,
    bindings: Vec<Binding>,
    statuses: BTreeMap<String, StatusResponse>,
    workers: BTreeMap<(String, String), Result<WorkerSnapshot, (StatusCode, ApiError)>>,
    runs: BTreeMap<(String, String), Result<Run, (StatusCode, ApiError)>>,
    activities: BTreeMap<String, Vec<Event>>,
    status_calls: Vec<String>,
    get_worker_calls: Vec<(String, String)>,
    get_run_calls: Vec<(String, String)>,
    list_activity_calls: Vec<ActivityCall>,
    list_activity_entered_notify: Option<Arc<Notify>>,
    delay_list_activity_notify: Option<Arc<Notify>>,
    pull_or_ack_calls: usize,
    start_demo_calls: Vec<(String, String)>,
    answer_calls: Vec<(String, String, String, u64, String)>,
    cancel_calls: Vec<(String, String, Option<String>)>,
    next_seq: usize,
}

#[derive(Clone)]
struct FakeServerState {
    inner: Arc<Mutex<FakeServerData>>,
    force_unauthorized: Arc<AtomicBool>,
}

impl FakeServerState {
    fn new(token: SecretToken) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FakeServerData {
                expected_token: token.as_str().to_string(),
                ..FakeServerData::default()
            })),
            force_unauthorized: Arc::new(AtomicBool::new(false)),
        }
    }
}

struct RunningFakeSupervisor {
    state: FakeServerState,
    token: SecretToken,
    handle: JoinHandle<()>,
}

impl RunningFakeSupervisor {
    async fn start(state_dir: &Path) -> Self {
        let token = SecretToken::generate();
        let state = FakeServerState::new(token.clone());
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake supervisor listener");
        let port = listener.local_addr().expect("listener addr").port();

        prepare_state_dir(state_dir).expect("prepare state dir");
        let descriptor = RuntimeDescriptor {
            version: DESCRIPTOR_VERSION,
            host: LOOPBACK_HOST.to_string(),
            port,
            pid: std::process::id(),
            token: token.clone(),
        };
        write_descriptor_atomic(state_dir, &descriptor).expect("write descriptor");

        let app = Router::new()
            .route(
                "/v1/bindings",
                get(handle_list_bindings).post(handle_create_binding),
            )
            .route("/v1/bindings/{binding_id}/command", post(handle_command))
            .with_state(state.clone());

        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Self {
            state,
            token,
            handle,
        }
    }

    fn stop(self) {
        self.handle.abort();
    }
}

fn check_auth(headers: &HeaderMap, state: &FakeServerState) -> Result<(), Box<Response>> {
    if state.force_unauthorized.load(Ordering::SeqCst) {
        return Err(Box::new(
            (
                StatusCode::UNAUTHORIZED,
                Json(ApiError::new(
                    ErrorCode::Unauthorized,
                    "forced unauthorized bearer token check failure",
                )),
            )
                .into_response(),
        ));
    }
    let expected = {
        let guard = state.inner.lock().unwrap();
        format!("Bearer {}", guard.expected_token)
    };
    let provided = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    if provided != expected {
        return Err(Box::new(
            (
                StatusCode::UNAUTHORIZED,
                Json(ApiError::new(
                    ErrorCode::Unauthorized,
                    "missing or invalid bearer token",
                )),
            )
                .into_response(),
        ));
    }
    Ok(())
}

async fn handle_list_bindings(
    State(state): State<FakeServerState>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = check_auth(&headers, &state) {
        return *resp;
    }
    let bindings = {
        let guard = state.inner.lock().unwrap();
        guard.bindings.clone()
    };
    Json(BindingListResponse { bindings }).into_response()
}

async fn handle_create_binding(
    State(state): State<FakeServerState>,
    headers: HeaderMap,
    Json(req): Json<CreateBindingRequest>,
) -> Response {
    if let Err(resp) = check_auth(&headers, &state) {
        return *resp;
    }
    let mut guard = state.inner.lock().unwrap();
    guard.next_seq += 1;
    let binding_id = format!("bnd_demo_{}", guard.next_seq);
    let binding = Binding {
        binding_id: binding_id.clone(),
        thread_uuid: req.thread_uuid,
        label: req.label,
        delivered_seq: 0,
        acked_seq: 0,
    };
    guard.bindings.push(binding.clone());
    guard.statuses.insert(
        binding_id.clone(),
        StatusResponse::from_summary_counts(
            binding.clone(),
            idle_wakeup(&binding_id, &binding.thread_uuid),
            0,
            0,
            Vec::new(),
        ),
    );
    Json(binding).into_response()
}

async fn handle_command(
    State(state): State<FakeServerState>,
    AxumPath(binding_id): AxumPath<String>,
    headers: HeaderMap,
    Json(req): Json<ScopedRequest>,
) -> Response {
    if let Err(resp) = check_auth(&headers, &state) {
        return *resp;
    }

    if let ScopedRequest::ListActivity {
        after_seq,
        cursor,
        limit,
        run_id,
        kind,
    } = &req
    {
        let (entered, delay) = {
            let mut guard = state.inner.lock().unwrap();
            guard.list_activity_calls.push((
                binding_id.clone(),
                *after_seq,
                cursor.clone(),
                *limit,
                run_id.clone(),
                *kind,
            ));
            (
                guard.list_activity_entered_notify.clone(),
                guard.delay_list_activity_notify.clone(),
            )
        };
        if let Some(notify) = entered {
            notify.notify_waiters();
        }
        if let Some(wait_notify) = delay {
            wait_notify.notified().await;
        }
    }

    let mut guard = state.inner.lock().unwrap();
    match req {
        ScopedRequest::Status => {
            guard.status_calls.push(binding_id.clone());
            match guard.statuses.get(&binding_id).cloned() {
                Some(status) => Json(ScopedResponse::Status(status)).into_response(),
                None => (
                    StatusCode::NOT_FOUND,
                    Json(ApiError::new(
                        ErrorCode::BindingNotFound,
                        "binding not found",
                    )),
                )
                    .into_response(),
            }
        }
        ScopedRequest::GetWorker { run_id } => {
            guard
                .get_worker_calls
                .push((binding_id.clone(), run_id.clone()));
            match guard
                .workers
                .get(&(binding_id.clone(), run_id.clone()))
                .cloned()
            {
                Some(Ok(worker)) => Json(ScopedResponse::Worker(worker)).into_response(),
                Some(Err((code, api_err))) => (code, Json(api_err)).into_response(),
                None => (
                    StatusCode::NOT_FOUND,
                    Json(ApiError::new(ErrorCode::WorkerNotFound, "worker not found")),
                )
                    .into_response(),
            }
        }
        ScopedRequest::GetRun { run_id } => {
            guard
                .get_run_calls
                .push((binding_id.clone(), run_id.clone()));
            match guard.runs.get(&(binding_id, run_id)).cloned() {
                Some(Ok(run)) => Json(ScopedResponse::Run(run)).into_response(),
                Some(Err((code, api_err))) => (code, Json(api_err)).into_response(),
                None => (
                    StatusCode::NOT_FOUND,
                    Json(ApiError::new(ErrorCode::RunNotFound, "run not found")),
                )
                    .into_response(),
            }
        }
        ScopedRequest::StartDemo {
            instruction,
            step_delay_ms,
            ..
        } => {
            guard.next_seq += 1;
            let run_id = format!("run_demo_{}", guard.next_seq);
            guard
                .start_demo_calls
                .push((binding_id.clone(), instruction.clone()));
            let run = Run {
                untrusted_data: true,
                run_id: run_id.clone(),
                binding_id: binding_id.clone(),
                revision: 1,
                status: RunStatus::Running,
                instruction: instruction.clone(),
                step_seq: 1,
                step_delay_ms: step_delay_ms.unwrap_or(200),
                next_step_at_ms: Some(1_700_000_000_200),
                active_question_id: None,
                active_question: None,
                started_at_ms: 1_700_000_000_000,
                updated_at_ms: 1_700_000_000_000,
                completed_at_ms: None,
                cancelled_at_ms: None,
                cancel_reason: None,
            };
            guard
                .runs
                .insert((binding_id.clone(), run_id.clone()), Ok(run.clone()));
            if let Some(status) = guard.statuses.get_mut(&binding_id) {
                status.total_runs += 1;
                status.active_runs += 1;
                status.recent_runs.insert(
                    0,
                    RunSummary {
                        untrusted_data: true,
                        run_id: run_id.clone(),
                        binding_id: binding_id.clone(),
                        revision: 1,
                        status: RunStatus::Running,
                        instruction_preview: instruction,
                        instruction_truncated: false,
                        active_question_id: None,
                        started_at_ms: 1_700_000_000_000,
                        updated_at_ms: 1_700_000_000_000,
                    },
                );
            }
            Json(ScopedResponse::Run(run)).into_response()
        }
        ScopedRequest::Answer {
            run_id,
            question_id,
            expected_revision,
            answer,
            ..
        } => {
            guard.answer_calls.push((
                binding_id.clone(),
                run_id.clone(),
                question_id.clone(),
                expected_revision,
                answer,
            ));
            let Some(Ok(mut run)) = guard
                .runs
                .get(&(binding_id.clone(), run_id.clone()))
                .cloned()
            else {
                return (
                    StatusCode::NOT_FOUND,
                    Json(ApiError::new(ErrorCode::RunNotFound, "run not found")),
                )
                    .into_response();
            };
            if run.revision != expected_revision {
                return (
                    StatusCode::CONFLICT,
                    Json(ApiError::new(ErrorCode::StaleRevision, "stale revision")),
                )
                    .into_response();
            }
            if run.status != RunStatus::WaitingInput {
                return (
                    StatusCode::CONFLICT,
                    Json(ApiError::new(
                        ErrorCode::InvalidRunState,
                        "run not waiting input",
                    )),
                )
                    .into_response();
            }
            run.status = RunStatus::Completing;
            run.active_question_id = None;
            run.active_question = None;
            run.updated_at_ms += 100;
            guard
                .runs
                .insert((binding_id.clone(), run_id.clone()), Ok(run.clone()));
            if let Some(status) = guard.statuses.get_mut(&binding_id) {
                for item in &mut status.recent_runs {
                    if item.run_id == run_id {
                        item.status = RunStatus::Completing;
                        item.active_question_id = None;
                    }
                }
            }
            Json(ScopedResponse::Run(run)).into_response()
        }
        ScopedRequest::Cancel { run_id, reason, .. } => {
            guard
                .cancel_calls
                .push((binding_id.clone(), run_id.clone(), reason.clone()));
            let Some(Ok(mut run)) = guard
                .runs
                .get(&(binding_id.clone(), run_id.clone()))
                .cloned()
            else {
                return (
                    StatusCode::NOT_FOUND,
                    Json(ApiError::new(ErrorCode::RunNotFound, "run not found")),
                )
                    .into_response();
            };
            if run.status.is_terminal() {
                return (
                    StatusCode::CONFLICT,
                    Json(ApiError::new(
                        ErrorCode::InvalidRunState,
                        "run already terminal",
                    )),
                )
                    .into_response();
            }
            run.status = RunStatus::Cancelled;
            run.active_question_id = None;
            run.active_question = None;
            run.cancel_reason = reason;
            run.cancelled_at_ms = Some(1_700_000_000_900);
            run.updated_at_ms = 1_700_000_000_900;
            guard
                .runs
                .insert((binding_id.clone(), run_id.clone()), Ok(run.clone()));
            if let Some(status) = guard.statuses.get_mut(&binding_id) {
                status.active_runs = status.active_runs.saturating_sub(1);
                for item in &mut status.recent_runs {
                    if item.run_id == run_id {
                        item.status = RunStatus::Cancelled;
                        item.active_question_id = None;
                    }
                }
            }
            Json(ScopedResponse::Run(run)).into_response()
        }
        ScopedRequest::ListActivity {
            after_seq,
            cursor,
            limit,
            run_id,
            kind,
        } => {
            if !guard.bindings.iter().any(|b| b.binding_id == binding_id) {
                return (
                    StatusCode::NOT_FOUND,
                    Json(ApiError::new(
                        ErrorCode::BindingNotFound,
                        "binding not found",
                    )),
                )
                    .into_response();
            }

            let all_events = guard
                .activities
                .get(&binding_id)
                .cloned()
                .unwrap_or_default();
            let high_water_mark_seq = all_events.last().map(|e| e.seq).unwrap_or(0);
            let resolved_after_seq = match cursor.as_deref() {
                Some(cur) => cur
                    .strip_prefix("cur:")
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0),
                None => after_seq.unwrap_or(0),
            };
            let page_limit = limit.unwrap_or(20);
            let filtered: Vec<Event> = all_events
                .into_iter()
                .filter(|e| e.seq > resolved_after_seq && e.seq <= high_water_mark_seq)
                .filter(|e| run_id.as_ref().is_none_or(|r| &e.run_id == r))
                .filter(|e| kind.is_none_or(|k| e.kind == k))
                .collect();
            let has_more = filtered.len() > page_limit;
            let events: Vec<Event> = filtered.into_iter().take(page_limit).collect();
            let next_cursor = if has_more {
                events.last().map(|last| format!("cur:{}", last.seq))
            } else {
                None
            };
            Json(ScopedResponse::Activity(ActivityPage {
                binding_id,
                events,
                next_cursor,
                has_more,
                high_water_mark_seq,
                after_seq: resolved_after_seq,
            }))
            .into_response()
        }
        ScopedRequest::PullEvents { .. } | ScopedRequest::AckEvents { .. } => {
            guard.pull_or_ack_calls += 1;
            (
                StatusCode::BAD_REQUEST,
                Json(ApiError::new(
                    ErrorCode::InvalidRequest,
                    "desktop bridge must never call pull_events or ack_events",
                )),
            )
                .into_response()
        }
        _ => (
            StatusCode::BAD_REQUEST,
            Json(ApiError::new(ErrorCode::InvalidRequest, "unsupported op")),
        )
            .into_response(),
    }
}

fn idle_wakeup(binding_id: &str, thread_uuid: &str) -> WakeupNotice {
    WakeupNotice {
        binding_id: binding_id.to_string(),
        thread_uuid: thread_uuid.to_string(),
        status: WakeupStatus::Idle,
        claim_id: None,
        pending_since_seq: None,
        latest_important_seq: 0,
        covered_up_to_seq: 0,
        coalesced_count: 0,
        attempt_count: 0,
        last_error: None,
        claimed_at_ms: None,
        resolved_at_ms: None,
        message: FIXED_WAKEUP_MESSAGE.to_string(),
    }
}

fn sample_worker_snapshot(
    binding_id: &str,
    run_id: &str,
    status: WorkerStatus,
    acceptance: WorkerAcceptanceState,
    missing_model_evidence: bool,
    missing_effort_evidence: bool,
) -> WorkerSnapshot {
    WorkerSnapshot {
        untrusted_data: true,
        worker_id: format!("wrk_{run_id}"),
        run_id: run_id.to_string(),
        binding_id: binding_id.to_string(),
        revision: 1,
        status,
        profile_id: "coding_hard".to_string(),
        profile_digest: "a".repeat(64),
        execution_binding: None,
        provider: "antigravity".to_string(),
        requested_model: "gemini-4-argon-high".to_string(),
        requested_effort: "high".to_string(),
        observed_model: if missing_model_evidence {
            None
        } else {
            Some("gemini-4-argon-high".to_string())
        },
        observed_effort: if missing_effort_evidence {
            None
        } else {
            Some("high".to_string())
        },
        instruction: "Implement typed desktop bridge with strict contract verification."
            .to_string(),
        claim_id: Some("wclm_1".to_string()),
        attempt_count: 1,
        worktree: WorkerWorktreeSummary {
            run_worktree_id: Some(format!("run_{run_id}")),
            run_worktree_path: Some("/tmp/worktrees/run".to_string()),
            run_branch_name: Some("tl/run/main".to_string()),
            task_worktree_id: Some(format!("task_{run_id}")),
            task_worktree_path: Some("/tmp/worktrees/task".to_string()),
            task_branch_name: Some("tl/task/m3-bridge".to_string()),
            base_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
            head_commit: Some("fedcba9876543210fedcba9876543210fedcba98".to_string()),
        },
        progress: WorkerProgressSummary {
            durable_progress_events: 4,
            coalesced_progress_events: 2,
            dropped_live_events: 0,
            last_event_kind: Some("tool_use".to_string()),
            last_summary: Some("Verified cargo workspace boundaries.".to_string()),
            last_event_at_ms: Some(1_700_000_000_500),
        },
        outcome: Some(WorkerOutcomeSummary {
            exit_code: Some(0),
            signal: None,
            duration_ms: Some(1200),
            timed_out: false,
            cancelled: false,
            cancel_requested_before_completion: false,
            missing_model_evidence,
            missing_effort_evidence,
            provider_error: false,
            incomplete_cleanup: false,
            rejected_by_diff_gate: false,
            stdout_truncated: false,
            stderr_truncated: false,
            acceptance_state: acceptance,
            changed_paths_count: 2,
            changed_paths_preview: vec!["crates/threadlane-desktop-bridge/src/lib.rs".to_string()],
            violation_count: 0,
            violation_summaries: Vec::new(),
            final_summary: Some("Completed bridge implementation.".to_string()),
            failure_reason: if missing_model_evidence || missing_effort_evidence {
                Some("Missing required provider effort telemetry.".to_string())
            } else {
                None
            },
            report_artifact_id: Some("art_123".to_string()),
        }),
        created_at_ms: 1_700_000_000_000,
        updated_at_ms: 1_700_000_000_600,
        started_at_ms: Some(1_700_000_000_100),
        completed_at_ms: Some(1_700_000_000_600),
        cancel_requested_at_ms: None,
        cancel_reason: None,
    }
}

#[tokio::test]
async fn unconfigured_bridge_returns_honest_empty_state_and_rejects_mutations() {
    let bridge = DesktopBridge::new(None, None).expect("create unconfigured bridge");
    let snap = bridge.snapshot(None).await.expect("unconfigured snapshot");

    assert_eq!(snap.attachment.mode, AttachmentMode::Unconfigured);
    assert_eq!(snap.attachment.connection, ConnectionState::Unconfigured);
    assert_eq!(snap.attachment.state_dir, None);
    assert!(snap.bindings.is_empty());
    assert_eq!(snap.selected_binding_id, None);
    assert!(snap.tasks.is_empty());
    assert_eq!(snap.total_runs, 0);
    assert_eq!(snap.active_runs, 0);
    assert!(!snap.has_more);
    assert!(!snap.demo_controls.available);

    assert_eq!(
        bridge
            .run_detail("bnd_1".to_string(), "run_1".to_string())
            .await,
        Err(BridgeError::Unconfigured)
    );
    assert_eq!(
        bridge
            .activity_history("bnd_1".to_string(), None, None, None, None, None)
            .await,
        Err(BridgeError::Unconfigured)
    );
    assert_eq!(bridge.start_demo().await, Err(BridgeError::DemoUnavailable));
    assert_eq!(
        bridge.answer_demo(1).await,
        Err(BridgeError::DemoUnavailable)
    );
    assert_eq!(
        bridge.cancel_demo(1).await,
        Err(BridgeError::DemoUnavailable)
    );
}

#[tokio::test]
async fn scope_cap_100_no_status_fanout_and_worker_not_found_fallback_only() {
    let temp = TempDir::new().expect("tempdir");
    let runtime_dir = temp.path().join("runtime");
    let server = RunningFakeSupervisor::start(&runtime_dir).await;

    {
        let mut guard = server.state.inner.lock().unwrap();
        for idx in 0..105 {
            let binding_id = format!("bnd_{idx:03}");
            let thread_uuid = Uuid::new_v4().to_string();
            let binding = Binding {
                binding_id: binding_id.clone(),
                thread_uuid: thread_uuid.clone(),
                label: format!("Chat {idx}"),
                delivered_seq: 0,
                acked_seq: 0,
            };
            guard.bindings.push(binding.clone());

            if idx == 1 {
                let worker = sample_worker_snapshot(
                    &binding_id,
                    "run_worker_1",
                    WorkerStatus::Completed,
                    WorkerAcceptanceState::ReadyForReview,
                    false,
                    false,
                );
                let missing_effort_worker = sample_worker_snapshot(
                    &binding_id,
                    "run_worker_missing_effort",
                    WorkerStatus::Completed,
                    WorkerAcceptanceState::ReadyForReview,
                    false,
                    true,
                );
                let demo_run = Run {
                    untrusted_data: true,
                    run_id: "run_demo_fallback".to_string(),
                    binding_id: binding_id.clone(),
                    revision: 2,
                    status: RunStatus::WaitingInput,
                    instruction: "Demo fallback instruction".to_string(),
                    step_seq: 3,
                    step_delay_ms: 100,
                    next_step_at_ms: None,
                    active_question_id: Some("qst_9".to_string()),
                    active_question: Some(Question {
                        untrusted_data: true,
                        question_id: "qst_9".to_string(),
                        binding_id: binding_id.clone(),
                        run_id: "run_demo_fallback".to_string(),
                        revision: 2,
                        status: QuestionStatus::Pending,
                        prompt: "Proceed with demo step?".to_string(),
                        answer: None,
                        superseded_by_revision: None,
                        created_at_ms: 1_700_000_000_200,
                        resolved_at_ms: None,
                    }),
                    started_at_ms: 1_700_000_000_000,
                    updated_at_ms: 1_700_000_000_300,
                    completed_at_ms: None,
                    cancelled_at_ms: None,
                    cancel_reason: None,
                };

                guard
                    .workers
                    .insert((binding_id.clone(), "run_worker_1".to_string()), Ok(worker));
                guard.workers.insert(
                    (binding_id.clone(), "run_worker_missing_effort".to_string()),
                    Ok(missing_effort_worker),
                );
                // `run_demo_fallback` has no worker entry -> returns WorkerNotFound -> falls back to get_run!
                guard.runs.insert(
                    (binding_id.clone(), "run_demo_fallback".to_string()),
                    Ok(demo_run),
                );

                let summaries = vec![
                    RunSummary {
                        untrusted_data: true,
                        run_id: "run_worker_1".to_string(),
                        binding_id: binding_id.clone(),
                        revision: 1,
                        status: RunStatus::Completed,
                        instruction_preview: "Worker 1".to_string(),
                        instruction_truncated: false,
                        active_question_id: None,
                        started_at_ms: 1_700_000_000_000,
                        updated_at_ms: 1_700_000_000_600,
                    },
                    RunSummary {
                        untrusted_data: true,
                        run_id: "run_worker_missing_effort".to_string(),
                        binding_id: binding_id.clone(),
                        revision: 1,
                        status: RunStatus::Completed,
                        instruction_preview: "Worker missing effort".to_string(),
                        instruction_truncated: false,
                        active_question_id: None,
                        started_at_ms: 1_700_000_000_000,
                        updated_at_ms: 1_700_000_000_600,
                    },
                    RunSummary {
                        untrusted_data: true,
                        run_id: "run_demo_fallback".to_string(),
                        binding_id: binding_id.clone(),
                        revision: 2,
                        status: RunStatus::WaitingInput,
                        instruction_preview: "Demo fallback instruction".to_string(),
                        instruction_truncated: false,
                        active_question_id: Some("qst_9".to_string()),
                        started_at_ms: 1_700_000_000_000,
                        updated_at_ms: 1_700_000_000_300,
                    },
                ];

                guard.statuses.insert(
                    binding_id.clone(),
                    StatusResponse::from_summary_counts(
                        binding.clone(),
                        idle_wakeup(&binding_id, &thread_uuid),
                        12,
                        1,
                        summaries,
                    ),
                );
            } else {
                guard.statuses.insert(
                    binding_id.clone(),
                    StatusResponse::from_summary_counts(
                        binding.clone(),
                        idle_wakeup(&binding_id, &thread_uuid),
                        0,
                        0,
                        Vec::new(),
                    ),
                );
            }
        }
    }

    let bridge =
        DesktopBridge::new(Some(runtime_dir.clone()), None).expect("create runtime bridge");

    // 1. Requesting a nonexistent binding scopes selection to existing bindings and never calls status on the unknown ID.
    let snap_unknown = bridge
        .snapshot(Some("bnd_nonexistent".to_string()))
        .await
        .expect("snapshot with unknown binding");
    assert_eq!(snap_unknown.bindings.len(), MAX_BINDINGS);
    assert_eq!(snap_unknown.selected_binding_id.as_deref(), Some("bnd_000"));
    {
        let guard = server.state.inner.lock().unwrap();
        assert_eq!(guard.status_calls, vec!["bnd_000".to_string()]);
        assert_eq!(guard.pull_or_ack_calls, 0);
    }

    // 2. Selecting `bnd_001` fetches status only for `bnd_001` and enriches its 3 runs.
    let snap = bridge
        .snapshot(Some("bnd_001".to_string()))
        .await
        .expect("snapshot bnd_001");
    assert_eq!(snap.attachment.mode, AttachmentMode::Runtime);
    assert_eq!(snap.attachment.connection, ConnectionState::Connected);
    assert_eq!(snap.selected_binding_id.as_deref(), Some("bnd_001"));
    assert_eq!(snap.total_runs, 12);
    assert_eq!(snap.active_runs, 1);
    assert!(snap.has_more);
    assert_eq!(snap.tasks.len(), 3);

    // Task 0: normal completed worker
    assert_eq!(snap.tasks[0].run_id, "run_worker_1");
    assert_eq!(snap.tasks[0].kind, TaskKind::Worker);
    assert_eq!(snap.tasks[0].status, "completed");
    assert!(!snap.tasks[0].needs_evidence);
    assert_eq!(snap.tasks[0].profile_id.as_deref(), Some("coding_hard"));

    // Task 1: worker missing effort evidence must stay "failed" with needs_evidence = true!
    assert_eq!(snap.tasks[1].run_id, "run_worker_missing_effort");
    assert_eq!(snap.tasks[1].kind, TaskKind::Worker);
    assert_eq!(snap.tasks[1].status, "failed");
    assert!(snap.tasks[1].needs_evidence);

    // Task 2: WorkerNotFound fell back to get_run -> Demo run
    assert_eq!(snap.tasks[2].run_id, "run_demo_fallback");
    assert_eq!(snap.tasks[2].kind, TaskKind::Demo);
    assert_eq!(snap.tasks[2].status, "waiting_input");
    assert_eq!(
        snap.tasks[2].last_summary.as_deref(),
        Some("Proceed with demo step?")
    );

    // Verify detail for missing-effort worker preserves Failed / ExecutionFailed
    let detail_missing = bridge
        .run_detail(
            "bnd_001".to_string(),
            "run_worker_missing_effort".to_string(),
        )
        .await
        .expect("run_detail missing effort worker");
    assert_eq!(detail_missing.task.status, "failed");
    assert_eq!(
        detail_missing.acceptance_state.as_deref(),
        Some("execution_failed")
    );
    assert!(!detail_missing.missing_model_evidence);
    assert!(detail_missing.missing_effort_evidence);
    assert_eq!(detail_missing.counts.durable_progress_events, 4);
    assert_eq!(detail_missing.counts.coalesced_progress_events, 2);

    // Verify detail for demo run via WorkerNotFound fallback
    let detail_demo = bridge
        .run_detail("bnd_001".to_string(), "run_demo_fallback".to_string())
        .await
        .expect("run_detail demo fallback");
    assert_eq!(detail_demo.task.kind, TaskKind::Demo);
    assert_eq!(
        detail_demo
            .question
            .as_ref()
            .map(|q| q.question_id.as_str()),
        Some("qst_9")
    );

    // 3. Now inject a non-WorkerNotFound error (InternalError) on get_worker and verify it NEVER falls back to get_run!
    {
        let mut guard = server.state.inner.lock().unwrap();
        guard.get_run_calls.clear();
        guard.workers.insert(
            ("bnd_001".to_string(), "run_demo_fallback".to_string()),
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiError::new(ErrorCode::InternalError, "simulated storage corruption"),
            )),
        );
    }

    let snap_err = bridge
        .snapshot(Some("bnd_001".to_string()))
        .await
        .expect("snapshot with internal error on get_worker");
    assert_eq!(snap_err.attachment.connection, ConnectionState::Blocked);
    assert!(snap_err.tasks.is_empty());
    {
        let guard = server.state.inner.lock().unwrap();
        assert!(
            guard.get_run_calls.is_empty(),
            "non-WorkerNotFound error must never fall back to get_run"
        );
        assert_eq!(guard.pull_or_ack_calls, 0);
    }

    server.stop();
}

#[tokio::test]
async fn local_demo_lifecycle_enforces_owned_tuple_revision_checks_and_replay_denial() {
    let temp = TempDir::new().expect("tempdir");
    let demo_dir = temp.path().join("demo-runtime");
    let runtime_dir = temp.path().join("real-runtime");

    let demo_server = RunningFakeSupervisor::start(&demo_dir).await;
    let real_server = RunningFakeSupervisor::start(&runtime_dir).await;

    {
        let mut guard = real_server.state.inner.lock().unwrap();
        let binding = Binding {
            binding_id: "bnd_real_1".to_string(),
            thread_uuid: Uuid::new_v4().to_string(),
            label: "Real Codex Thread".to_string(),
            delivered_seq: 0,
            acked_seq: 0,
        };
        guard.bindings.push(binding.clone());
        guard.statuses.insert(
            binding.binding_id.clone(),
            StatusResponse::from_summary_counts(
                binding.clone(),
                idle_wakeup(&binding.binding_id, &binding.thread_uuid),
                0,
                0,
                Vec::new(),
            ),
        );
    }

    // Default attaches demo_dir when state_dir is None, without creating bindings automatically.
    let bridge =
        DesktopBridge::new(None, Some(demo_dir.clone())).expect("create bridge with demo dir");
    let initial = bridge.snapshot(None).await.expect("initial demo snapshot");
    assert_eq!(initial.attachment.mode, AttachmentMode::Demo);
    assert_eq!(initial.attachment.connection, ConnectionState::Connected);
    assert!(initial.bindings.is_empty());
    assert!(initial.demo_controls.available);
    assert_eq!(initial.demo_controls.run_id, None);
    assert!(!initial.demo_controls.can_answer);
    assert!(!initial.demo_controls.can_cancel);

    // Start demo run 1
    let started = bridge.start_demo().await.expect("start_demo 1");
    assert_eq!(started.attachment.mode, AttachmentMode::Demo);
    assert_eq!(started.bindings.len(), 1);
    assert_eq!(started.bindings[0].label, DEMO_BINDING_LABEL);
    assert!(started.bindings[0].is_local_demo);
    let demo_binding_1 = started.bindings[0].binding_id.clone();
    let demo_run_1 = started
        .demo_controls
        .run_id
        .clone()
        .expect("owned demo run_id");
    assert_eq!(started.demo_controls.revision, Some(1));
    assert!(!started.demo_controls.can_answer);
    assert!(started.demo_controls.can_cancel);

    {
        let guard = demo_server.state.inner.lock().unwrap();
        assert_eq!(
            guard.start_demo_calls,
            vec![(demo_binding_1.clone(), DEMO_INSTRUCTION.to_string())]
        );
    }

    // Repeated start_demo while demo_run_1 is still Running must be rejected
    assert!(matches!(
        bridge.start_demo().await,
        Err(BridgeError::InvalidState(_))
    ));
    {
        let guard = demo_server.state.inner.lock().unwrap();
        assert_eq!(guard.start_demo_calls.len(), 1);
    }

    // Advance demo run 1 on the server to WaitingInput with a pending question
    {
        let mut guard = demo_server.state.inner.lock().unwrap();
        let run = guard
            .runs
            .get_mut(&(demo_binding_1.clone(), demo_run_1.clone()))
            .unwrap()
            .as_mut()
            .unwrap();
        run.status = RunStatus::WaitingInput;
        run.active_question_id = Some("qst_demo_1".to_string());
        run.active_question = Some(Question {
            untrusted_data: true,
            question_id: "qst_demo_1".to_string(),
            binding_id: demo_binding_1.clone(),
            run_id: demo_run_1.clone(),
            revision: 1,
            status: QuestionStatus::Pending,
            prompt: "Confirm local demo step?".to_string(),
            answer: None,
            superseded_by_revision: None,
            created_at_ms: 1_700_000_000_100,
            resolved_at_ms: None,
        });
        if let Some(status) = guard.statuses.get_mut(&demo_binding_1) {
            status.recent_runs[0].status = RunStatus::WaitingInput;
            status.recent_runs[0].active_question_id = Some("qst_demo_1".to_string());
        }
    }

    let waiting_snap = bridge.snapshot(None).await.expect("waiting snapshot");
    assert!(waiting_snap.demo_controls.can_answer);
    assert!(waiting_snap.demo_controls.can_cancel);
    assert_eq!(waiting_snap.demo_controls.revision, Some(1));

    // Stale revision on answer_demo and cancel_demo must fail BEFORE calling server mutation
    assert_eq!(bridge.answer_demo(2).await, Err(BridgeError::StaleRevision));
    assert_eq!(bridge.cancel_demo(2).await, Err(BridgeError::StaleRevision));
    {
        let guard = demo_server.state.inner.lock().unwrap();
        assert!(guard.answer_calls.is_empty());
        assert!(guard.cancel_calls.is_empty());
    }

    // Valid answer_demo(1) sends fixed answer "Continue"
    let answered_snap = bridge.answer_demo(1).await.expect("answer_demo(1)");
    assert!(!answered_snap.demo_controls.can_answer);
    assert!(answered_snap.demo_controls.can_cancel);
    {
        let guard = demo_server.state.inner.lock().unwrap();
        assert_eq!(guard.answer_calls.len(), 1);
        assert_eq!(guard.answer_calls[0].4, DEMO_ANSWER);
    }

    // Replaying answer_demo(1) is rejected without calling the server a second time
    assert!(matches!(
        bridge.answer_demo(1).await,
        Err(BridgeError::InvalidState(_))
    ));
    {
        let guard = demo_server.state.inner.lock().unwrap();
        assert_eq!(guard.answer_calls.len(), 1);
    }

    // Repeated start_demo while demo_run_1 is still Completing (non-terminal) must also be rejected
    assert!(matches!(
        bridge.start_demo().await,
        Err(BridgeError::InvalidState(_))
    ));
    {
        let guard = demo_server.state.inner.lock().unwrap();
        assert_eq!(guard.start_demo_calls.len(), 1);
    }

    // Attach to real runtime observer: switches view to Runtime without losing demo identity
    let attached_real = bridge
        .attach(runtime_dir.to_string_lossy().into_owned())
        .await
        .expect("attach real runtime");
    assert_eq!(attached_real.attachment.mode, AttachmentMode::Runtime);
    assert_eq!(
        attached_real.selected_binding_id.as_deref(),
        Some("bnd_real_1")
    );
    assert_eq!(
        attached_real.demo_controls.run_id.as_deref(),
        Some(demo_run_1.as_str())
    );

    // Transition demo_run_1 on the demo server to Completed WITHOUT polling snapshot first;
    // start_demo() must refresh live state via get_run, recognize terminal completion, and start demo_run_2.
    {
        let mut guard = demo_server.state.inner.lock().unwrap();
        let run = guard
            .runs
            .get_mut(&(demo_binding_1.clone(), demo_run_1.clone()))
            .unwrap()
            .as_mut()
            .unwrap();
        run.status = RunStatus::Completed;
        run.completed_at_ms = Some(1_700_000_000_500);
        run.updated_at_ms = 1_700_000_000_500;
        if let Some(status) = guard.statuses.get_mut(&demo_binding_1) {
            status.active_runs = status.active_runs.saturating_sub(1);
            status.recent_runs[0].status = RunStatus::Completed;
        }
    }

    let started_2 = bridge
        .start_demo()
        .await
        .expect("start_demo 2 after completion");
    assert_eq!(started_2.attachment.mode, AttachmentMode::Demo);
    let demo_binding_2 = started_2
        .demo_controls
        .binding_id
        .clone()
        .expect("owned demo binding 2");
    let demo_run_2 = started_2
        .demo_controls
        .run_id
        .clone()
        .expect("owned demo run 2");
    assert_ne!(demo_run_1, demo_run_2);

    // Old demo run detail must have can_answer = false and can_cancel = false
    let old_detail = bridge
        .run_detail(demo_binding_1.clone(), demo_run_1.clone())
        .await
        .expect("detail for old demo run");
    assert!(!old_detail.demo_controls.can_answer);
    assert!(!old_detail.demo_controls.can_cancel);

    // Externally steer demo_run_2 on the server to revision 2 BEFORE cancel_demo(1) WITHOUT polling;
    // cancel_demo(1) must fetch get_run, detect revision 2 != 1, update cached owned revision,
    // and reject with StaleRevision without cancelling the newer revision!
    {
        let mut guard = demo_server.state.inner.lock().unwrap();
        let run = guard
            .runs
            .get_mut(&(demo_binding_2.clone(), demo_run_2.clone()))
            .unwrap()
            .as_mut()
            .unwrap();
        run.revision = 2;
        run.instruction = "Steered instruction on revision 2".to_string();
        run.updated_at_ms += 50;
        if let Some(status) = guard.statuses.get_mut(&demo_binding_2) {
            status.recent_runs[0].revision = 2;
        }
    }

    assert_eq!(bridge.cancel_demo(1).await, Err(BridgeError::StaleRevision));
    {
        let guard = demo_server.state.inner.lock().unwrap();
        assert!(
            guard.cancel_calls.is_empty(),
            "stale cancel_demo(1) must not cancel revision 2 on the server"
        );
        let run = guard
            .runs
            .get(&(demo_binding_2.clone(), demo_run_2.clone()))
            .unwrap()
            .as_ref()
            .unwrap();
        assert_eq!(run.status, RunStatus::Running);
        assert_eq!(run.revision, 2);
    }

    // Cancel the active demo_run_2 at its actual revision 2
    let cancelled_snap = bridge.cancel_demo(2).await.expect("cancel_demo 2");
    assert!(!cancelled_snap.demo_controls.can_cancel);
    assert!(!cancelled_snap.demo_controls.can_answer);
    assert_eq!(cancelled_snap.demo_controls.revision, Some(2));
    {
        let guard = demo_server.state.inner.lock().unwrap();
        assert_eq!(guard.cancel_calls.len(), 1);
    }

    // Replaying cancel_demo(2) after cancellation is denied
    assert!(matches!(
        bridge.cancel_demo(2).await,
        Err(BridgeError::InvalidState(_))
    ));

    demo_server.stop();
    real_server.stop();
}

#[tokio::test]
async fn descriptor_refresh_offline_recovery_and_security_redaction() {
    let temp = TempDir::new().expect("tempdir");
    let runtime_dir = temp.path().join("refresh-runtime");

    let bridge =
        DesktopBridge::new(Some(runtime_dir.clone()), None).expect("create lazy runtime bridge");

    // 1. Before supervisor starts, snapshot reports Disconnected honestly.
    let offline_snap = bridge.snapshot(None).await.expect("offline snapshot");
    assert_eq!(offline_snap.attachment.mode, AttachmentMode::Runtime);
    assert_eq!(
        offline_snap.attachment.connection,
        ConnectionState::Disconnected
    );

    // 2. Start fake supervisor #1 -> snapshot automatically connects.
    let server1 = RunningFakeSupervisor::start(&runtime_dir).await;
    let connected_snap = bridge.snapshot(None).await.expect("connected snapshot");
    assert_eq!(
        connected_snap.attachment.connection,
        ConnectionState::Connected
    );
    assert_eq!(connected_snap.attachment.message, None);
    let token1_secret = server1.token.as_str().to_string();
    server1.stop();

    // 3. Start fake supervisor #2 on a new port with a rotated token -> snapshot refreshes descriptor.
    let server2 = RunningFakeSupervisor::start(&runtime_dir).await;
    assert_ne!(token1_secret, server2.token.as_str());
    let refreshed_snap = bridge.snapshot(None).await.expect("refreshed snapshot");
    assert_eq!(
        refreshed_snap.attachment.connection,
        ConnectionState::Connected
    );

    // 4. Force HTTP 401 Unauthorized -> surfaces Blocked without leaking token.
    server2
        .state
        .force_unauthorized
        .store(true, Ordering::SeqCst);
    let unauth_snap = bridge.snapshot(None).await.expect("unauthorized snapshot");
    assert_eq!(unauth_snap.attachment.connection, ConnectionState::Blocked);
    let msg = unauth_snap.attachment.message.unwrap_or_default();
    assert!(!msg.contains(server2.token.as_str()));

    let detail_err = bridge
        .run_detail("bnd_1".to_string(), "run_1".to_string())
        .await
        .expect_err("run_detail must reject when unauthorized");
    assert!(!detail_err.to_string().contains(server2.token.as_str()));

    server2.stop();

    // 5. Insecure descriptor permissions (0644) -> surfaces Blocked without leaking descriptor or token.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let desc_path = runtime_dir.join("supervisor.json");
        fs::set_permissions(&desc_path, fs::Permissions::from_mode(0o644))
            .expect("chmod 0644 descriptor");
        let blocked_snap = bridge.snapshot(None).await.expect("blocked snapshot");
        assert_eq!(blocked_snap.attachment.connection, ConnectionState::Blocked);
        let blocked_msg = blocked_snap.attachment.message.unwrap_or_default();
        assert!(!blocked_msg.contains(&token1_secret));
        assert!(!blocked_msg.contains("supervisor.json"));
    }
}

#[test]
fn tool_status_discovers_path_executables_without_spawning_and_marks_jev_not_configured() {
    let temp = TempDir::new().expect("tempdir");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&bin_dir).expect("create bin dir");

    // Create executable `codex`, `agy`, `git`, and even `jev` (which must still report not_configured),
    // plus a non-executable `opencode` (0644).
    for name in ["codex", "agy", "git", "jev"] {
        let p = bin_dir.join(name);
        fs::write(&p, "#!/bin/sh\nexit 99\n").expect("write fake bin");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&p, fs::Permissions::from_mode(0o755)).expect("chmod 755");
        }
    }
    let non_exec = bin_dir.join("opencode");
    fs::write(&non_exec, "not executable").expect("write non-exec opencode");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&non_exec, fs::Permissions::from_mode(0o644)).expect("chmod 644");
    }

    let path_os = OsString::from(bin_dir.as_os_str());
    let statuses = discover_tool_status_from_path(Some(&path_os));

    let by_id: BTreeMap<String, _> = statuses.into_iter().map(|s| (s.id.clone(), s)).collect();

    assert_eq!(by_id["codex"].availability, ToolAvailability::Found);
    assert!(by_id["codex"]
        .executable_path
        .as_deref()
        .unwrap()
        .ends_with("codex"));

    assert_eq!(by_id["antigravity"].availability, ToolAvailability::Found);
    assert!(by_id["antigravity"]
        .executable_path
        .as_deref()
        .unwrap()
        .ends_with("agy"));

    assert_eq!(by_id["git"].availability, ToolAvailability::Found);

    #[cfg(unix)]
    {
        assert_eq!(by_id["opencode"].availability, ToolAvailability::NotFound);
        assert_eq!(by_id["opencode"].executable_path, None);
    }

    assert_eq!(by_id["jev"].availability, ToolAvailability::NotConfigured);
    assert_eq!(by_id["jev"].executable_path, None);
}

#[test]
fn dto_serde_json_shapes_match_exact_contract() {
    let bridge = DesktopBridge::new(None, None).expect("bridge");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let snap = rt.block_on(bridge.snapshot(None)).expect("snapshot");
    let val = serde_json::to_value(&snap).expect("serialize DesktopSnapshot");

    assert_eq!(val["attachment"]["mode"], json!("unconfigured"));
    assert_eq!(val["attachment"]["connection"], json!("unconfigured"));
    assert!(val["attachment"].get("state_dir").is_some());
    assert!(val["attachment"].get("message").is_some());
    assert!(val["attachment"].get("updated_at_ms").is_some());
    assert!(val.get("bindings").is_some());
    assert!(val.get("selected_binding_id").is_some());
    assert!(val.get("tasks").is_some());
    assert!(val.get("total_runs").is_some());
    assert!(val.get("active_runs").is_some());
    assert!(val.get("has_more").is_some());
    assert!(val["demo_controls"].get("available").is_some());
    assert!(val["demo_controls"].get("run_id").is_some());
    assert!(val["demo_controls"].get("binding_id").is_some());
    assert!(val["demo_controls"].get("can_answer").is_some());
    assert!(val["demo_controls"].get("can_cancel").is_some());
    assert!(val["demo_controls"].get("revision").is_some());

    let close: WindowAction = serde_json::from_value(json!("close")).expect("deserialize close");
    let minimize: WindowAction =
        serde_json::from_value(json!("minimize")).expect("deserialize minimize");
    assert_eq!(close, WindowAction::Close);
    assert_eq!(minimize, WindowAction::Minimize);
}

fn sample_activity_event(
    binding_id: &str,
    seq: u64,
    run_id: &str,
    kind: EventKind,
    summary: &str,
) -> Event {
    Event {
        event_id: format!("evt_{binding_id}_{seq}"),
        binding_id: binding_id.to_string(),
        seq,
        run_id: run_id.to_string(),
        revision: 1,
        step_seq: seq,
        kind,
        important: matches!(
            kind,
            EventKind::QuestionAsked
                | EventKind::RunCompleted
                | EventKind::WorkerCompleted
                | EventKind::WorkerFailed
        ),
        payload: EventPayload {
            untrusted_data: true,
            summary: summary.to_string(),
            question_id: None,
            question_prompt: None,
            answer: None,
            instruction: None,
            cancel_reason: None,
        },
        created_at_ms: 1_700_000_000_000 + (seq as i64 * 100),
    }
}

#[tokio::test]
async fn activity_history_fake_supervisor_enforces_selection_isolation_no_ack_and_discards_stale_epoch(
) {
    let temp = TempDir::new().expect("tempdir");
    let runtime_dir = temp.path().join("runtime-history");
    let server = RunningFakeSupervisor::start(&runtime_dir).await;

    {
        let mut guard = server.state.inner.lock().unwrap();
        for idx in 0..105 {
            let binding_id = format!("bnd_{idx:03}");
            let thread_uuid = Uuid::new_v4().to_string();
            let binding = Binding {
                binding_id: binding_id.clone(),
                thread_uuid: thread_uuid.clone(),
                label: format!("Chat {idx}"),
                delivered_seq: 0,
                acked_seq: 0,
            };
            guard.bindings.push(binding.clone());
            guard.statuses.insert(
                binding_id.clone(),
                StatusResponse::from_summary_counts(
                    binding,
                    idle_wakeup(&binding_id, &thread_uuid),
                    0,
                    0,
                    Vec::new(),
                ),
            );
        }

        guard.activities.insert(
            "bnd_000".to_string(),
            vec![sample_activity_event(
                "bnd_000",
                1,
                "run_000_a",
                EventKind::RunStarted,
                "Started on bnd_000",
            )],
        );
        guard.activities.insert(
            "bnd_001".to_string(),
            vec![
                sample_activity_event(
                    "bnd_001",
                    1,
                    "run_001_a",
                    EventKind::WorkerStarted,
                    "Worker started on bnd_001",
                ),
                sample_activity_event(
                    "bnd_001",
                    2,
                    "run_001_a",
                    EventKind::WorkerProgress,
                    "Worker progress 1 on bnd_001",
                ),
                sample_activity_event(
                    "bnd_001",
                    3,
                    "run_001_b",
                    EventKind::WorkerProgress,
                    "Worker progress on run_001_b",
                ),
                sample_activity_event(
                    "bnd_001",
                    4,
                    "run_001_a",
                    EventKind::WorkerProgress,
                    "Worker progress 2 on bnd_001",
                ),
            ],
        );
    }

    let bridge = Arc::new(
        DesktopBridge::new(Some(runtime_dir.clone()), None).expect("create runtime bridge"),
    );

    // 1. Select `bnd_001` via snapshot.
    let snap = bridge
        .snapshot(Some("bnd_001".to_string()))
        .await
        .expect("select bnd_001");
    assert_eq!(snap.selected_binding_id.as_deref(), Some("bnd_001"));

    // 2. Requesting unselected binding `bnd_000`, capped binding `bnd_104`, or nonexistent binding
    //    is rejected with `BridgeError::NotFound` and makes ZERO `ListActivity` calls on the server.
    for forbidden_binding in ["bnd_000", "bnd_104", "bnd_nonexistent"] {
        let err = bridge
            .activity_history(forbidden_binding.to_string(), None, None, None, None, None)
            .await
            .expect_err("unselected or out-of-scope binding must be rejected");
        assert!(
            matches!(err, BridgeError::NotFound(_)),
            "expected NotFound for {forbidden_binding}, got {err:?}"
        );
    }
    {
        let guard = server.state.inner.lock().unwrap();
        assert!(
            guard.list_activity_calls.is_empty(),
            "rejected binding requests must never call ListActivity on the supervisor"
        );
        assert_eq!(guard.pull_or_ack_calls, 0);
    }

    // 3. Invalid arguments (both after_seq and cursor, out-of-range limit, invalid cursor) are rejected locally.
    assert!(matches!(
        bridge
            .activity_history(
                "bnd_001".to_string(),
                Some(1),
                Some("cur:1".to_string()),
                None,
                None,
                None
            )
            .await,
        Err(BridgeError::InvalidInput(_))
    ));
    assert!(matches!(
        bridge
            .activity_history("bnd_001".to_string(), None, None, Some(0), None, None)
            .await,
        Err(BridgeError::InvalidInput(_))
    ));
    assert!(matches!(
        bridge
            .activity_history("bnd_001".to_string(), None, None, Some(51), None, None)
            .await,
        Err(BridgeError::InvalidInput(_))
    ));
    assert!(matches!(
        bridge
            .activity_history(
                "bnd_001".to_string(),
                None,
                Some("bad\ncursor".to_string()),
                None,
                None,
                None
            )
            .await,
        Err(BridgeError::InvalidInput(_))
    ));

    // 4. Valid `activity_history` on selected `bnd_001` forwards filters and cursor pagination without pull/ack.
    let page_1 = bridge
        .activity_history(
            "bnd_001".to_string(),
            Some(1),
            None,
            Some(1),
            Some("run_001_a".to_string()),
            Some(EventKind::WorkerProgress),
        )
        .await
        .expect("page 1 for bnd_001");
    assert_eq!(page_1.binding_id, "bnd_001");
    assert_eq!(page_1.after_seq, 1);
    assert_eq!(page_1.high_water_mark_seq, 4);
    assert!(page_1.has_more);
    assert_eq!(page_1.next_cursor.as_deref(), Some("cur:2"));
    assert_eq!(page_1.events.len(), 1);
    assert_eq!(page_1.events[0].seq, 2);
    assert_eq!(page_1.events[0].run_id, "run_001_a");
    assert_eq!(page_1.events[0].kind, EventKind::WorkerProgress);

    let page_2 = bridge
        .activity_history(
            "bnd_001".to_string(),
            None,
            page_1.next_cursor.clone(),
            Some(1),
            Some("run_001_a".to_string()),
            Some(EventKind::WorkerProgress),
        )
        .await
        .expect("page 2 for bnd_001");
    assert_eq!(page_2.binding_id, "bnd_001");
    assert_eq!(page_2.after_seq, 2);
    assert!(!page_2.has_more);
    assert_eq!(page_2.next_cursor, None);
    assert_eq!(page_2.events.len(), 1);
    assert_eq!(page_2.events[0].seq, 4);

    {
        let guard = server.state.inner.lock().unwrap();
        assert_eq!(guard.list_activity_calls.len(), 2);
        assert_eq!(
            guard.list_activity_calls[0],
            (
                "bnd_001".to_string(),
                Some(1),
                None,
                Some(1),
                Some("run_001_a".to_string()),
                Some(EventKind::WorkerProgress),
            )
        );
        assert_eq!(
            guard.list_activity_calls[1],
            (
                "bnd_001".to_string(),
                None,
                Some("cur:2".to_string()),
                Some(1),
                Some("run_001_a".to_string()),
                Some(EventKind::WorkerProgress),
            )
        );
        assert_eq!(guard.pull_or_ack_calls, 0);
    }

    // 5. Stale observer context completion: pause `ListActivity` on `bnd_001`, switch selection
    //    to `bnd_000` via `snapshot(Some("bnd_000"))`, release `ListActivity`, and verify the stale
    //    `activity_history` completion is discarded with `BridgeError::InvalidState`.
    let entered_notify = Arc::new(Notify::new());
    let release_notify = Arc::new(Notify::new());
    {
        let mut guard = server.state.inner.lock().unwrap();
        guard.list_activity_entered_notify = Some(entered_notify.clone());
        guard.delay_list_activity_notify = Some(release_notify.clone());
    }

    let bridge_clone = bridge.clone();
    let in_flight = tokio::spawn(async move {
        bridge_clone
            .activity_history("bnd_001".to_string(), None, None, None, None, None)
            .await
    });

    entered_notify.notified().await;

    // Switch selected binding to `bnd_000` while `in_flight` is still awaiting `ListActivity`.
    let switched_snap = bridge
        .snapshot(Some("bnd_000".to_string()))
        .await
        .expect("switch selection to bnd_000");
    assert_eq!(
        switched_snap.selected_binding_id.as_deref(),
        Some("bnd_000")
    );

    {
        let mut guard = server.state.inner.lock().unwrap();
        guard.list_activity_entered_notify = None;
        guard.delay_list_activity_notify = None;
    }
    release_notify.notify_waiters();

    let stale_res = in_flight.await.expect("join in_flight");
    assert!(
        matches!(stale_res, Err(BridgeError::InvalidState(_))),
        "stale observer context completion must be discarded: {stale_res:?}"
    );

    // Active selection is now `bnd_000`, so `activity_history` on `bnd_000` succeeds.
    let page_000 = bridge
        .activity_history("bnd_000".to_string(), None, None, None, None, None)
        .await
        .expect("activity_history for newly selected bnd_000");
    assert_eq!(page_000.binding_id, "bnd_000");
    assert_eq!(page_000.events.len(), 1);
    assert_eq!(page_000.events[0].run_id, "run_000_a");

    {
        let guard = server.state.inner.lock().unwrap();
        assert_eq!(guard.pull_or_ack_calls, 0);
    }

    server.stop();
}

#[tokio::test]
async fn activity_history_real_supervisor_preserves_readonly_delivery_and_ack_cursors_across_pages()
{
    let temp = TempDir::new().expect("tempdir");
    let state_dir = temp.path().join("real-supervisor-state");

    let sup_handle = Supervisor::start(SupervisorConfig {
        state_dir: state_dir.clone(),
        tick_interval: std::time::Duration::from_millis(50),
        codex: None,
        workers: None,
    })
    .await
    .expect("start real supervisor");

    let sup_client = SupervisorClient::from_state_dir(&state_dir).expect("supervisor client");
    let binding_a = sup_client
        .create_binding("0196ed00-0000-7000-8000-00000000020a", "Chat A")
        .await
        .expect("create binding A");
    let binding_b = sup_client
        .create_binding("0196ed00-0000-7000-8000-00000000020b", "Chat B")
        .await
        .expect("create binding B");

    let scoped_a = sup_client
        .clone()
        .scoped(binding_a.binding_id.clone())
        .expect("scoped A");
    let run_a = scoped_a
        .start_demo(
            "cmd-bridge-hist-start".to_string(),
            "Bridge history integration test".to_string(),
            Some(5000),
        )
        .await
        .expect("start demo A");
    let steered_a = scoped_a
        .steer(
            "cmd-bridge-hist-steer".to_string(),
            run_a.run_id.clone(),
            run_a.revision,
            "Updated bridge history instruction".to_string(),
        )
        .await
        .expect("steer demo A");
    let _cancelled_a = scoped_a
        .cancel(
            "cmd-bridge-hist-cancel".to_string(),
            run_a.run_id.clone(),
            Some("Done testing".to_string()),
        )
        .await
        .expect("cancel demo A");
    assert_eq!(steered_a.revision, 2);

    let bridge =
        DesktopBridge::new(Some(state_dir.clone()), None).expect("create bridge for real runtime");
    let snap_a = bridge
        .snapshot(Some(binding_a.binding_id.clone()))
        .await
        .expect("select binding A");
    assert_eq!(
        snap_a.selected_binding_id.as_deref(),
        Some(binding_a.binding_id.as_str())
    );

    // 1. Cross-binding call to `binding_b` while `binding_a` is selected is rejected.
    let cross_err = bridge
        .activity_history(binding_b.binding_id.clone(), None, None, None, None, None)
        .await
        .expect_err("unselected binding B must be rejected");
    assert!(matches!(cross_err, BridgeError::NotFound(_)));

    // 2. Page through binding A's activity with limit = 1 using keyset cursor.
    let first_page = bridge
        .activity_history(
            binding_a.binding_id.clone(),
            None,
            None,
            Some(1),
            Some(run_a.run_id.clone()),
            None,
        )
        .await
        .expect("first page");
    assert_eq!(first_page.binding_id, binding_a.binding_id);
    assert_eq!(first_page.events.len(), 1);
    assert_eq!(first_page.events[0].seq, 1);
    assert_eq!(first_page.events[0].kind, EventKind::RunStarted);
    assert!(first_page.has_more);
    assert!(first_page.next_cursor.is_some());
    assert!(first_page.high_water_mark_seq >= 3);

    let second_page = bridge
        .activity_history(
            binding_a.binding_id.clone(),
            None,
            first_page.next_cursor.clone(),
            Some(2),
            Some(run_a.run_id.clone()),
            None,
        )
        .await
        .expect("second page");
    assert_eq!(second_page.binding_id, binding_a.binding_id);
    assert!(!second_page.events.is_empty());
    assert!(second_page.events.iter().all(|e| e.seq > 1));
    assert!(second_page.events.iter().all(|e| e.run_id == run_a.run_id));

    // 3. Filter by specific `EventKind::RunCancelled`.
    let cancelled_only = bridge
        .activity_history(
            binding_a.binding_id.clone(),
            None,
            None,
            Some(10),
            Some(run_a.run_id.clone()),
            Some(EventKind::RunCancelled),
        )
        .await
        .expect("cancelled kind filter");
    assert_eq!(cancelled_only.events.len(), 1);
    assert_eq!(cancelled_only.events[0].kind, EventKind::RunCancelled);

    // 4. Verify read-only invariant: `delivered_seq`, `acked_seq`, and wakeup cursors are untouched!
    let status_after = sup_client
        .status(&binding_a.binding_id)
        .await
        .expect("status after activity_history");
    assert_eq!(status_after.binding.delivered_seq, 0);
    assert_eq!(status_after.binding.acked_seq, 0);
    assert_eq!(status_after.wakeup.covered_up_to_seq, 0);

    sup_handle.shutdown().await;
}
