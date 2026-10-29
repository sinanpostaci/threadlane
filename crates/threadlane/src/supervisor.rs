use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::{DefaultBodyLimit, Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use threadlane_core::{
    estimate_wire_response_bytes, Command, Core, CoreError, WorkerClaim, WorkerStatus,
    MAX_COMPACT_RESPONSE_BYTES, MAX_STATUS_RECENT_RUNS,
};
use threadlane_workers::NormalizedWorkerEvent;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch, Mutex, MutexGuard, Notify};
use tokio::task::{JoinHandle, JoinSet};

use crate::codex::{invoke_codex_wakeup, CodexWakeupConfig};
use crate::descriptor::{
    enforce_database_permissions, prepare_database_path, remove_owned_descriptor,
    write_descriptor_atomic, RuntimeDescriptor, SecretToken, StateLock, DESCRIPTOR_VERSION,
    LOOPBACK_HOST,
};
use crate::error::{ApiError, Error, ErrorCode};
use crate::protocol::{
    BindingListResponse, CreateBindingRequest, RetryWakeupRequest, ScopedRequest, ScopedResponse,
    StatusResponse,
};
use crate::workers::{
    build_completion_record, build_pre_spawn_cancelled_completion, execute_claimed_worker,
    map_live_event_to_progress, prepare_claim_worktrees, ValidatedWorkerProfile,
    ValidatedWorkersConfig, WorkersConfig, LIVE_OBSERVER_CHANNEL_CAPACITY,
};

/// Default background tick interval (ms) for advancing due demo runs and checking wake-up claims.
pub const DEFAULT_TICK_INTERVAL_MS: u64 = 50;

/// Maximum allowed HTTP request body size (64 KiB).
pub const MAX_HTTP_BODY_BYTES: usize = 64 * 1024;

/// Bounded wait time for background worker shutdown (including killing/reaping in-flight Codex sender).
const WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

/// Bounded wait time for graceful HTTP connection drain on shutdown.
const SERVER_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(250);

/// Maximum character length for a single logged fatal diagnostic line.
const MAX_DIAGNOSTIC_CHARS: usize = 256;

/// Configuration for starting a [`Supervisor`] instance.
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    /// Path to the private state directory owned by this supervisor.
    pub state_dir: PathBuf,
    /// Interval between background demo ticks.
    pub tick_interval: Duration,
    /// Optional Codex wake-up queue bridge configuration (disabled by default).
    pub codex: Option<CodexWakeupConfig>,
    /// Optional startup worker profile configuration (disabled by default).
    pub workers: Option<WorkersConfig>,
}

impl SupervisorConfig {
    /// Creates a new [`SupervisorConfig`] for `state_dir` with default tick interval and Codex/workers disabled.
    pub fn new(state_dir: impl Into<PathBuf>) -> Self {
        Self {
            state_dir: state_dir.into(),
            tick_interval: Duration::from_millis(DEFAULT_TICK_INTERVAL_MS),
            codex: None,
            workers: None,
        }
    }

    /// Attaches a [`WorkersConfig`] to this [`SupervisorConfig`].
    pub fn with_workers(mut self, workers: WorkersConfig) -> Self {
        self.workers = Some(workers);
        self
    }

    /// Validates supervisor configuration parameters without side effects.
    pub fn validate(&self) -> Result<(), Error> {
        let tick_ms = self.tick_interval.as_millis();
        if tick_ms == 0 || tick_ms > 60_000 {
            return Err(Error::InvalidConfig(
                "tick_interval must be between 1 and 60000 ms".to_string(),
            ));
        }
        if let Some(codex) = &self.codex {
            codex.validate()?;
        }
        if let Some(workers) = &self.workers {
            let _ = workers.validate()?;
        }
        Ok(())
    }
}

/// Bundles `Core`, the descriptor identity, and `StateLock` so `Core` can never outlive
/// the exclusive file lock, and descriptor removal always occurs while `StateLock` is held.
struct ActiveCore {
    core: Core,
    state_dir: PathBuf,
    descriptor: RuntimeDescriptor,
    _lock: StateLock,
}

impl Drop for ActiveCore {
    fn drop(&mut self) {
        // `self._lock` is still held while `Drop::drop` runs and is dropped last
        // after `self.core` closes its SQLite connection.
        remove_owned_descriptor(&self.state_dir, &self.descriptor);
    }
}

struct ActiveWorkerControl {
    claim: WorkerClaim,
    cancel_tx: watch::Sender<bool>,
    user_cancel_requested: Arc<AtomicBool>,
}

struct ActiveWorkerGuard {
    state: Arc<SharedState>,
    run_id: String,
}

impl Drop for ActiveWorkerGuard {
    fn drop(&mut self) {
        self.state.unregister_active_worker(&self.run_id);
    }
}

struct SharedState {
    state_dir: PathBuf,
    active: Mutex<Option<ActiveCore>>,
    retired: AtomicBool,
    fatal_error: std::sync::Mutex<Option<ApiError>>,
    fatal_tx: watch::Sender<Option<ApiError>>,
    token: SecretToken,
    tick_interval: Duration,
    codex: Option<CodexWakeupConfig>,
    workers: Option<ValidatedWorkersConfig>,
    active_workers: std::sync::Mutex<BTreeMap<String, ActiveWorkerControl>>,
    wake_notify: Notify,
}

struct CoreAccessGuard<'a> {
    state: &'a SharedState,
    guard: MutexGuard<'a, Option<ActiveCore>>,
}

impl CoreAccessGuard<'_> {
    fn core_mut(&mut self) -> &mut Core {
        &mut self
            .guard
            .as_mut()
            .expect("ActiveCore verified present in CoreAccessGuard")
            .core
    }
}

impl Drop for CoreAccessGuard<'_> {
    fn drop(&mut self) {
        if self.state.retired.load(Ordering::SeqCst) {
            let _ = self.guard.take();
        }
    }
}

impl SharedState {
    fn register_active_worker(
        &self,
        claim: WorkerClaim,
        cancel_tx: watch::Sender<bool>,
        user_cancel_requested: Arc<AtomicBool>,
    ) {
        let mut map = self
            .active_workers
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        map.insert(
            claim.run_id.clone(),
            ActiveWorkerControl {
                claim,
                cancel_tx,
                user_cancel_requested,
            },
        );
    }

    fn unregister_active_worker(&self, run_id: &str) {
        let mut map = self
            .active_workers
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        map.remove(run_id);
    }

    fn request_worker_cancellation(&self, run_id: &str, worker_id: &str) {
        let map = self
            .active_workers
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for entry in map.values() {
            if entry.claim.run_id == run_id || entry.claim.worker_id == worker_id {
                entry.user_cancel_requested.store(true, Ordering::SeqCst);
                let _ = entry.cancel_tx.send(true);
            }
        }
    }

    fn cancel_all_active_workers(&self) {
        let map = self
            .active_workers
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for entry in map.values() {
            let _ = entry.cancel_tx.send(true);
        }
    }

    fn fatal_error(&self) -> Option<ApiError> {
        self.fatal_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn record_fatal_error(&self, api_err: ApiError) -> ApiError {
        let mut slot = self.fatal_error.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = slot.as_ref() {
            return existing.clone();
        }
        let redacted_msg = truncate_diagnostic(&api_err.message);
        eprintln!(
            "threadlane supervisor fatal error [{:?}]: {}",
            api_err.code, redacted_msg
        );
        *slot = Some(api_err.clone());
        let _ = self.fatal_tx.send_replace(Some(api_err.clone()));
        self.cancel_all_active_workers();
        self.wake_notify.notify_waiters();
        api_err
    }

    fn record_core_fatal_error(&self, context: &str, err: &CoreError) -> ApiError {
        let (_, mut api_err) = ApiError::from_core_error(err);
        api_err.message = format!("{context}: {}", truncate_diagnostic(&api_err.message));
        self.record_fatal_error(api_err)
    }

    fn check_core_err_fatal(&self, context: &str, err: &CoreError) {
        if matches!(
            err,
            CoreError::Storage(_)
                | CoreError::CorruptState(_)
                | CoreError::IncompatibleSchemaVersion { .. }
        ) {
            let _ = self.record_core_fatal_error(context, err);
        }
    }

    async fn acquire_for_request(
        &self,
    ) -> Result<CoreAccessGuard<'_>, (StatusCode, Json<ApiError>)> {
        if let Some(fatal) = self.fatal_error() {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(fatal)));
        }
        if self.retired.load(Ordering::SeqCst) {
            return Err(retired_response());
        }

        let mut guard = self.active.lock().await;
        if let Some(fatal) = self.fatal_error() {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(fatal)));
        }
        if self.retired.load(Ordering::SeqCst) {
            let _ = guard.take();
            return Err(retired_response());
        }
        if guard.is_none() {
            return Err(retired_response());
        }

        Ok(CoreAccessGuard { state: self, guard })
    }

    async fn acquire_for_worker(&self, allow_when_retiring: bool) -> Option<CoreAccessGuard<'_>> {
        if self.fatal_error().is_some() {
            return None;
        }
        if !allow_when_retiring && self.retired.load(Ordering::SeqCst) {
            return None;
        }

        let mut guard = self.active.lock().await;
        if self.fatal_error().is_some() {
            return None;
        }
        if !allow_when_retiring && self.retired.load(Ordering::SeqCst) {
            let _ = guard.take();
            return None;
        }
        if guard.is_none() {
            return None;
        }

        Some(CoreAccessGuard { state: self, guard })
    }
}

fn retired_response() -> (StatusCode, Json<ApiError>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ApiError::new(
            ErrorCode::InternalError,
            "supervisor is shutting down and no longer accepting requests",
        )),
    )
}

fn truncate_diagnostic(input: &str) -> String {
    let mut chars = input.chars().map(|c| if c.is_control() { ' ' } else { c });
    let out: String = chars.by_ref().take(MAX_DIAGNOSTIC_CHARS).collect();
    if chars.next().is_some() {
        format!("{out}...")
    } else {
        out
    }
}

/// Active handle to a running local [`Supervisor`].
///
/// Ties the process-lifetime `supervisor.lock` file lock to `Core` and manages the HTTP server
/// and background tick/wake-up task.
pub struct SupervisorHandle {
    descriptor: RuntimeDescriptor,
    state_dir: PathBuf,
    shared: Arc<SharedState>,
    shutdown_tx: watch::Sender<bool>,
    server_task: Option<JoinHandle<()>>,
    worker_task: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for SupervisorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SupervisorHandle")
            .field("descriptor", &self.descriptor)
            .field("state_dir", &self.state_dir)
            .finish()
    }
}

impl SupervisorHandle {
    /// Returns the active [`RuntimeDescriptor`] (token is redacted in `Debug`).
    pub fn descriptor(&self) -> &RuntimeDescriptor {
        &self.descriptor
    }

    /// Returns the state directory path owned by this supervisor.
    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    /// Returns the fatal persistence or runtime error if the supervisor has faulted.
    pub fn fatal_error(&self) -> Option<ApiError> {
        self.shared.fatal_error()
    }

    /// Waits until the supervisor records a fatal persistence or runtime error and returns it.
    pub async fn wait_for_fatal(&self) -> ApiError {
        let mut rx = self.shared.fatal_tx.subscribe();
        if let Some(err) = self.fatal_error() {
            return err;
        }
        loop {
            if let Some(err) = rx.borrow_and_update().clone() {
                return err;
            }
            if rx.changed().await.is_err() {
                if let Some(err) = self.fatal_error() {
                    return err;
                }
                std::future::pending::<()>().await;
            }
        }
    }

    /// Gracefully shuts down the supervisor and returns any recorded fatal error.
    pub async fn shutdown_and_take_fatal(self) -> Option<ApiError> {
        let shared = Arc::clone(&self.shared);
        self.shutdown().await;
        shared.fatal_error()
    }

    /// Gracefully stops the HTTP server and background worker, cancels/reaps any in-flight
    /// worker or Codex processes, removes `supervisor.json`, and releases the state directory lock.
    pub async fn shutdown(mut self) {
        let _ = self.shutdown_tx.send(true);
        self.shared.cancel_all_active_workers();
        self.shared.wake_notify.notify_waiters();

        // Allow the background worker task to finish reaping any in-flight worker/Codex
        // child processes and recording `Unknown` outcomes before retiring `ActiveCore`.
        if let Some(mut worker) = self.worker_task.take() {
            if tokio::time::timeout(WORKER_SHUTDOWN_TIMEOUT, &mut worker)
                .await
                .is_err()
            {
                worker.abort();
                let _ = worker.await;
            }
        }

        if self.shared.fatal_error().is_none() {
            if let Some(mut access) = self.shared.acquire_for_worker(true).await {
                if let Err(err) = access.core_mut().mark_active_workers_unknown(
                    current_time_ms(),
                    "supervisor shut down before worker completion proof",
                ) {
                    self.shared
                        .check_core_err_fatal("mark_active_workers_unknown on shutdown", &err);
                }
            }
        }

        self.shared.retired.store(true, Ordering::SeqCst);

        if let Some(mut server) = self.server_task.take() {
            if tokio::time::timeout(SERVER_SHUTDOWN_TIMEOUT, &mut server)
                .await
                .is_err()
            {
                server.abort();
                let _ = server.await;
            }
        }

        let mut active_guard = self.shared.active.lock().await;
        let _ = active_guard.take();
    }
}

impl Drop for SupervisorHandle {
    fn drop(&mut self) {
        self.shared.retired.store(true, Ordering::SeqCst);
        let _ = self.shutdown_tx.send(true);
        self.shared.cancel_all_active_workers();
        self.shared.wake_notify.notify_waiters();

        if let Some(worker) = self.worker_task.take() {
            worker.abort();
        }
        if let Some(server) = self.server_task.take() {
            server.abort();
        }

        // If no Core operation is currently holding the mutex, mark any active workers
        // Unknown and drop ActiveCore (and StateLock) immediately. If a Core operation is
        // mid-transaction, CoreAccessGuard::drop will drop ActiveCore (and StateLock) as
        // soon as that transaction finishes.
        if let Ok(mut guard) = self.shared.active.try_lock() {
            if self.shared.fatal_error().is_none() {
                if let Some(active) = guard.as_mut() {
                    let _ = active.core.mark_active_workers_unknown(
                        current_time_ms(),
                        "supervisor dropped while worker was active",
                    );
                }
            }
            let _ = guard.take();
        }
    }
}

/// Local single-owner state and execution supervisor.
pub struct Supervisor;

impl Supervisor {
    /// Starts the supervisor on `127.0.0.1:0`, acquiring the exclusive state directory lock,
    /// opening/recovering SQLite state, and atomically writing `supervisor.json`.
    pub async fn start(config: SupervisorConfig) -> Result<SupervisorHandle, Error> {
        config.validate()?;
        let validated_workers = match &config.workers {
            Some(w) => Some(w.validate()?),
            None => None,
        };

        let lock = StateLock::acquire(&config.state_dir)?;
        let db_path = prepare_database_path(&config.state_dir)?;
        let mut core = Core::open(&db_path)?;
        core.mark_active_workers_unknown(
            current_time_ms(),
            "supervisor restarted while worker was in an uncertain non-terminal state",
        )?;
        enforce_database_permissions(&config.state_dir)?;

        let listener = TcpListener::bind((LOOPBACK_HOST, 0)).await?;
        let local_addr = listener.local_addr()?;

        let token = SecretToken::generate();
        let descriptor = RuntimeDescriptor {
            version: DESCRIPTOR_VERSION,
            host: LOOPBACK_HOST.to_string(),
            port: local_addr.port(),
            pid: std::process::id(),
            token: token.clone(),
        };

        write_descriptor_atomic(&config.state_dir, &descriptor)?;

        let active_core = ActiveCore {
            core,
            state_dir: config.state_dir.clone(),
            descriptor: descriptor.clone(),
            _lock: lock,
        };

        let (fatal_tx, _fatal_rx) = watch::channel(None);

        let shared = Arc::new(SharedState {
            state_dir: config.state_dir.clone(),
            active: Mutex::new(Some(active_core)),
            retired: AtomicBool::new(false),
            fatal_error: std::sync::Mutex::new(None),
            fatal_tx,
            token,
            tick_interval: config.tick_interval,
            codex: config.codex,
            workers: validated_workers,
            active_workers: std::sync::Mutex::new(BTreeMap::new()),
            wake_notify: Notify::new(),
        });

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let worker_state = Arc::clone(&shared);
        let worker_shutdown_rx = shutdown_rx.clone();
        let worker_task = tokio::spawn(async move {
            run_background_worker(worker_state, worker_shutdown_rx).await;
        });

        let server_state = Arc::clone(&shared);
        let app = build_router(Arc::clone(&shared));
        let mut server_shutdown_rx = shutdown_rx.clone();
        let server_task = tokio::spawn(async move {
            let serve_res = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    while !*server_shutdown_rx.borrow_and_update() {
                        if server_shutdown_rx.changed().await.is_err() {
                            break;
                        }
                    }
                })
                .await;
            if let Err(err) = serve_res {
                let _ = server_state.record_fatal_error(ApiError::new(
                    ErrorCode::TransportError,
                    format!("local supervisor HTTP server failed: {err}"),
                ));
            }
        });

        Ok(SupervisorHandle {
            descriptor,
            state_dir: config.state_dir,
            shared,
            shutdown_tx,
            server_task: Some(server_task),
            worker_task: Some(worker_task),
        })
    }
}

fn build_router(state: Arc<SharedState>) -> Router {
    Router::new()
        .route(
            "/v1/bindings",
            post(handle_create_binding).get(handle_list_bindings),
        )
        .route("/v1/bindings/{binding_id}", get(handle_get_binding))
        .route(
            "/v1/bindings/{binding_id}/command",
            post(handle_scoped_command),
        )
        .route(
            "/v1/bindings/{binding_id}/retry-wakeup",
            post(handle_retry_wakeup),
        )
        .fallback(handle_fallback)
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
        .with_state(state)
}

fn verify_bearer_auth(
    headers: &HeaderMap,
    expected: &SecretToken,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    let Some(raw_header) = headers.get(axum::http::header::AUTHORIZATION) else {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ApiError::new(
                ErrorCode::Unauthorized,
                "missing Authorization header",
            )),
        ));
    };
    let Ok(header_str) = raw_header.to_str() else {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ApiError::new(
                ErrorCode::Unauthorized,
                "invalid Authorization header encoding",
            )),
        ));
    };
    let Some(candidate) = header_str.strip_prefix("Bearer ") else {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ApiError::new(
                ErrorCode::Unauthorized,
                "expected Bearer authentication scheme",
            )),
        ));
    };
    if candidate.is_empty() || !expected.constant_time_eq(candidate) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ApiError::new(
                ErrorCode::Unauthorized,
                "invalid bearer token",
            )),
        ));
    }
    Ok(())
}

fn parse_json_body<T: DeserializeOwned>(
    body: Result<Bytes, BytesRejection>,
) -> Result<T, (StatusCode, Json<ApiError>)> {
    let bytes = body.map_err(|rejection| {
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(ApiError::with_details(
                    ErrorCode::PayloadTooLarge,
                    format!(
                        "request body exceeds maximum allowed size of {MAX_HTTP_BODY_BYTES} bytes"
                    ),
                    serde_json::json!({ "max_bytes": MAX_HTTP_BODY_BYTES }),
                )),
            )
        } else {
            (
                StatusCode::BAD_REQUEST,
                Json(ApiError::new(
                    ErrorCode::InvalidRequest,
                    format!("failed to read request body: {rejection}"),
                )),
            )
        }
    })?;
    serde_json::from_slice(&bytes).map_err(|err| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::new(
                ErrorCode::InvalidRequest,
                format!("invalid request JSON: {err}"),
            )),
        )
    })
}

fn map_core_error(
    state: &SharedState,
    context: &str,
    err: CoreError,
) -> (StatusCode, Json<ApiError>) {
    state.check_core_err_fatal(context, &err);
    let (status, api_err) = ApiError::from_core_error(&err);
    (status, Json(api_err))
}

async fn handle_fallback(State(state): State<Arc<SharedState>>, headers: HeaderMap) -> Response {
    if let Err(resp) = verify_bearer_auth(&headers, &state.token) {
        return resp.into_response();
    }
    if let Some(fatal) = state.fatal_error() {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(fatal)).into_response();
    }
    if state.retired.load(Ordering::SeqCst) {
        return retired_response().into_response();
    }
    (
        StatusCode::NOT_FOUND,
        Json(ApiError::new(
            ErrorCode::InvalidRequest,
            "endpoint not found",
        )),
    )
        .into_response()
}

async fn handle_create_binding(
    State(state): State<Arc<SharedState>>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Result<(StatusCode, Json<threadlane_core::Binding>), (StatusCode, Json<ApiError>)> {
    verify_bearer_auth(&headers, &state.token)?;
    let req: CreateBindingRequest = parse_json_body(body)?;

    let mut access = state.acquire_for_request().await?;
    let binding = access
        .core_mut()
        .create_binding(&req.thread_uuid, &req.label)
        .map_err(|err| map_core_error(&state, "create_binding failed", err))?;
    Ok((StatusCode::CREATED, Json(binding)))
}

async fn handle_list_bindings(
    State(state): State<Arc<SharedState>>,
    headers: HeaderMap,
) -> Result<Json<BindingListResponse>, (StatusCode, Json<ApiError>)> {
    verify_bearer_auth(&headers, &state.token)?;

    let mut access = state.acquire_for_request().await?;
    let bindings = access
        .core_mut()
        .list_bindings()
        .map_err(|err| map_core_error(&state, "list_bindings failed", err))?;
    Ok(Json(BindingListResponse { bindings }))
}

async fn handle_get_binding(
    State(state): State<Arc<SharedState>>,
    headers: HeaderMap,
    AxumPath(binding_id): AxumPath<String>,
) -> Result<Json<threadlane_core::Binding>, (StatusCode, Json<ApiError>)> {
    verify_bearer_auth(&headers, &state.token)?;

    let mut access = state.acquire_for_request().await?;
    let binding = access
        .core_mut()
        .get_binding(&binding_id)
        .map_err(|err| map_core_error(&state, "get_binding failed", err))?;
    Ok(Json(binding))
}

async fn handle_scoped_command(
    State(state): State<Arc<SharedState>>,
    headers: HeaderMap,
    AxumPath(binding_id): AxumPath<String>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<ScopedResponse>, (StatusCode, Json<ApiError>)> {
    verify_bearer_auth(&headers, &state.token)?;
    let req: ScopedRequest = parse_json_body(body)?;
    let may_schedule = req.may_affect_scheduling();

    let now = current_time_ms();
    let mut notify_worker = may_schedule;

    let response = {
        let mut access = state.acquire_for_request().await?;
        if !matches!(req, ScopedRequest::ListActivity { .. }) {
            let report = access
                .core_mut()
                .tick(now)
                .map_err(|err| map_core_error(&state, "demo tick persistence failed", err))?;
            if report.emitted_events > 0 {
                notify_worker = true;
            }
        }

        match req {
            ScopedRequest::Status => {
                let binding = access
                    .core_mut()
                    .get_binding(&binding_id)
                    .map_err(|err| map_core_error(&state, "status get_binding failed", err))?;
                let wakeup = access
                    .core_mut()
                    .get_wakeup(&binding_id)
                    .map_err(|err| map_core_error(&state, "status get_wakeup failed", err))?;
                let (total_runs, active_runs, recent_runs) = access
                    .core_mut()
                    .get_status_run_summaries(&binding_id, MAX_STATUS_RECENT_RUNS)
                    .map_err(|err| map_core_error(&state, "status summary query failed", err))?;
                let status_resp = ScopedResponse::Status(StatusResponse::from_summary_counts(
                    binding,
                    wakeup,
                    total_runs,
                    active_runs,
                    recent_runs,
                ));
                let serialized_len = estimate_wire_response_bytes(&status_resp)
                    .map_err(|err| map_core_error(&state, "status serialization failed", err))?;
                if serialized_len > MAX_COMPACT_RESPONSE_BYTES {
                    return Err(map_core_error(
                        &state,
                        "status budget check failed",
                        CoreError::ResponseTooLarge {
                            size_bytes: serialized_len,
                            max_bytes: MAX_COMPACT_RESPONSE_BYTES,
                        },
                    ));
                }
                status_resp
            }
            ScopedRequest::StartWorker {
                command_key,
                profile_id,
                instruction,
            } => {
                let _ = access.core_mut().get_binding(&binding_id).map_err(|err| {
                    map_core_error(&state, "start_worker get_binding failed", err)
                })?;

                let Some(workers_cfg) = &state.workers else {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        Json(ApiError::new(
                            ErrorCode::InvalidConfig,
                            "worker dispatch is disabled (start supervisor with --workers-config)",
                        )),
                    ));
                };

                let Some(profile) = workers_cfg.profiles.get(&profile_id) else {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        Json(ApiError::with_details(
                            ErrorCode::ValidationFailed,
                            format!("unknown worker profile_id `{profile_id}`"),
                            serde_json::json!({ "field": "profile_id" }),
                        )),
                    ));
                };

                if !profile.allowed_binding_ids.contains(&binding_id) {
                    return Err((
                        StatusCode::FORBIDDEN,
                        Json(ApiError::with_details(
                            ErrorCode::ValidationFailed,
                            format!(
                                "binding `{binding_id}` is not authorized to use worker profile `{profile_id}`"
                            ),
                            serde_json::json!({
                                "field": "profile_id",
                                "binding_id": binding_id,
                                "profile_id": profile_id,
                            }),
                        )),
                    ));
                }

                let cmd = Command::StartWorker {
                    command_key,
                    profile_id: profile.profile_id.clone(),
                    profile_digest: profile.profile_digest.clone(),
                    provider: profile.provider_str().to_string(),
                    requested_model: profile.requested_model_str().to_string(),
                    requested_effort: profile.requested_effort.clone(),
                    base_commit: profile.template.base_commit.clone(),
                    instruction,
                    now_ms: now,
                };
                let output = access
                    .core_mut()
                    .execute(&binding_id, cmd)
                    .map_err(|err| map_core_error(&state, "start_worker execution failed", err))?;
                ScopedResponse::from(output)
            }
            other_req => {
                let is_cancel_worker = matches!(other_req, ScopedRequest::CancelWorker { .. });
                let cmd = other_req
                    .into_core_command(now)
                    .expect("non-status/start_worker request converts to core Command");
                let output = access.core_mut().execute(&binding_id, cmd).map_err(|err| {
                    map_core_error(&state, "scoped command execution failed", err)
                })?;
                let resp = ScopedResponse::from(output);
                if is_cancel_worker {
                    if let ScopedResponse::Worker(ref snapshot) = resp {
                        state.request_worker_cancellation(&snapshot.run_id, &snapshot.worker_id);
                    }
                }
                resp
            }
        }
    };

    if notify_worker {
        state.wake_notify.notify_one();
    }

    Ok(Json(response))
}

async fn handle_retry_wakeup(
    State(state): State<Arc<SharedState>>,
    headers: HeaderMap,
    AxumPath(binding_id): AxumPath<String>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<threadlane_core::WakeupNotice>, (StatusCode, Json<ApiError>)> {
    verify_bearer_auth(&headers, &state.token)?;
    let req: RetryWakeupRequest = parse_json_body(body)?;
    let now = current_time_ms();

    let notice = {
        let mut access = state.acquire_for_request().await?;
        access
            .core_mut()
            .retry_wakeup(&binding_id, &req.expected_claim_id, req.allow_unknown, now)
            .map_err(|err| map_core_error(&state, "retry_wakeup failed", err))?
    };

    state.wake_notify.notify_one();
    Ok(Json(notice))
}

async fn run_background_worker(state: Arc<SharedState>, mut shutdown_rx: watch::Receiver<bool>) {
    let mut worker_tasks: JoinSet<String> = JoinSet::new();

    loop {
        while worker_tasks.try_join_next().is_some() {}

        if *shutdown_rx.borrow()
            || state.retired.load(Ordering::SeqCst)
            || state.fatal_error().is_some()
        {
            break;
        }

        {
            let Some(mut access) = state.acquire_for_worker(false).await else {
                break;
            };
            if let Err(err) = access.core_mut().tick(current_time_ms()) {
                let _ =
                    state.record_core_fatal_error("background demo tick persistence failed", &err);
                break;
            }
        }

        if let Some(workers_cfg) = &state.workers {
            let mut fatal_occurred = false;
            while worker_tasks.len() < workers_cfg.max_parallel {
                if *shutdown_rx.borrow()
                    || state.retired.load(Ordering::SeqCst)
                    || state.fatal_error().is_some()
                {
                    break;
                }

                let available_slots = workers_cfg.max_parallel.saturating_sub(worker_tasks.len());
                if available_slots == 0 {
                    break;
                }

                let claimed_batch = {
                    let Some(mut access) = state.acquire_for_worker(false).await else {
                        break;
                    };
                    let queued = match access.core_mut().list_queued_workers(available_slots) {
                        Ok(list) => list,
                        Err(err) => {
                            let _ =
                                state.record_core_fatal_error("list_queued_workers failed", &err);
                            fatal_occurred = true;
                            break;
                        }
                    };
                    if queued.is_empty() {
                        break;
                    }

                    let mut newly_claimed = Vec::new();
                    for snap in queued {
                        if worker_tasks.len() + newly_claimed.len() >= workers_cfg.max_parallel {
                            break;
                        }
                        let Some(profile) = workers_cfg.profiles.get(&snap.profile_id) else {
                            continue;
                        };
                        if profile.profile_digest != snap.profile_digest
                            || !profile.allowed_binding_ids.contains(&snap.binding_id)
                        {
                            continue;
                        }

                        match access.core_mut().claim_worker(
                            &snap.binding_id,
                            &snap.run_id,
                            snap.revision,
                            &profile.profile_digest,
                            current_time_ms(),
                        ) {
                            Ok(claim) => {
                                let (cancel_tx, cancel_rx) = watch::channel(false);
                                let user_cancel_requested = Arc::new(AtomicBool::new(false));
                                state.register_active_worker(
                                    claim.clone(),
                                    cancel_tx.clone(),
                                    Arc::clone(&user_cancel_requested),
                                );
                                newly_claimed.push((
                                    profile.clone(),
                                    claim,
                                    cancel_tx,
                                    cancel_rx,
                                    user_cancel_requested,
                                ));
                            }
                            Err(
                                CoreError::StaleRevision { .. }
                                | CoreError::InvalidRunState { .. }
                                | CoreError::RunNotFound { .. }
                                | CoreError::BindingNotFound { .. },
                            ) => {
                                continue;
                            }
                            Err(err) => {
                                let _ = state.record_core_fatal_error("claim_worker failed", &err);
                                fatal_occurred = true;
                                break;
                            }
                        }
                    }
                    newly_claimed
                };

                if fatal_occurred || claimed_batch.is_empty() {
                    break;
                }

                for (profile, claim, cancel_tx, cancel_rx, user_cancel_requested) in claimed_batch {
                    let task_state = Arc::clone(&state);
                    let task_shutdown_rx = shutdown_rx.clone();
                    worker_tasks.spawn(async move {
                        run_single_claimed_worker(
                            task_state,
                            profile,
                            claim,
                            cancel_tx,
                            cancel_rx,
                            user_cancel_requested,
                            task_shutdown_rx,
                        )
                        .await
                    });
                }
            }

            if fatal_occurred {
                break;
            }
        }

        if let Some(codex_cfg) = &state.codex {
            let mut fatal_occurred = false;
            loop {
                if *shutdown_rx.borrow()
                    || state.retired.load(Ordering::SeqCst)
                    || state.fatal_error().is_some()
                {
                    break;
                }

                let next_claim = {
                    let Some(mut access) = state.acquire_for_worker(false).await else {
                        break;
                    };
                    let now = current_time_ms();
                    let mut claimed = None;
                    for binding_id in &codex_cfg.notify_bindings {
                        match access.core_mut().claim_wakeup_for_binding(binding_id, now) {
                            Ok(Some(claim)) => {
                                claimed = Some(claim);
                                break;
                            }
                            Ok(None) | Err(CoreError::BindingNotFound { .. }) => {
                                // Deliberate no-op when binding has no pending wake-up or does not exist yet.
                            }
                            Err(err) => {
                                let _ = state.record_core_fatal_error(
                                    &format!("failed to claim wake-up for binding `{binding_id}`"),
                                    &err,
                                );
                                fatal_occurred = true;
                                break;
                            }
                        }
                    }
                    claimed
                };

                if fatal_occurred {
                    break;
                }

                let Some(claim) = next_claim else {
                    break;
                };

                let outcome = invoke_codex_wakeup(codex_cfg, &claim, &mut shutdown_rx).await;

                // Persist the wake-up outcome (even if shutdown started while the child process was running).
                // If persisting the result fails, record a fatal supervisor error and stop without
                // retrying the uncertain external side effect.
                let Some(mut access) = state.acquire_for_worker(true).await else {
                    break;
                };
                if let Err(err) = access.core_mut().record_wakeup_result(
                    &claim.claim_id,
                    outcome,
                    current_time_ms(),
                ) {
                    let _ = state.record_core_fatal_error(
                        &format!(
                            "failed to persist wake-up outcome for claim `{}` (external effect not retried)",
                            claim.claim_id
                        ),
                        &err,
                    );
                    fatal_occurred = true;
                    break;
                }
            }

            if fatal_occurred {
                break;
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(state.tick_interval) => {}
            _ = state.wake_notify.notified() => {}
            Some(_) = worker_tasks.join_next(), if !worker_tasks.is_empty() => {}
            res = shutdown_rx.changed() => {
                if res.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
        }
    }

    // Cancel and drain or abort all active worker executions before exiting.
    state.cancel_all_active_workers();
    if !worker_tasks.is_empty() {
        let drain_fut = async { while worker_tasks.join_next().await.is_some() {} };
        if tokio::time::timeout(WORKER_SHUTDOWN_TIMEOUT, drain_fut)
            .await
            .is_err()
        {
            worker_tasks.abort_all();
            while worker_tasks.join_next().await.is_some() {}
        }
    }

    if state.fatal_error().is_none() {
        if let Some(mut access) = state.acquire_for_worker(true).await {
            if let Err(err) = access.core_mut().mark_active_workers_unknown(
                current_time_ms(),
                "supervisor background loop stopped while worker was active",
            ) {
                state.check_core_err_fatal("mark_active_workers_unknown on background exit", &err);
            }
        }
    }
}

async fn run_single_claimed_worker(
    state: Arc<SharedState>,
    profile: ValidatedWorkerProfile,
    claim: WorkerClaim,
    cancel_tx: watch::Sender<bool>,
    mut cancel_rx: watch::Receiver<bool>,
    user_cancel_requested: Arc<AtomicBool>,
    shutdown_rx: watch::Receiver<bool>,
) -> String {
    let run_id = claim.run_id.clone();
    let _active_guard = ActiveWorkerGuard {
        state: Arc::clone(&state),
        run_id: run_id.clone(),
    };

    // 1. Prepare owned per-run and per-task worktrees without holding the Core mutex.
    let prep_profile = profile.clone();
    let prep_claim = claim.clone();
    let worktree_res =
        tokio::task::spawn_blocking(move || prepare_claim_worktrees(&prep_profile, &prep_claim))
            .await;

    let worktree_update = match worktree_res {
        Ok(Ok(update)) => update,
        Ok(Err(worker_err)) => {
            if state.fatal_error().is_none() && !*shutdown_rx.borrow() {
                let completion = build_completion_record(
                    &state.state_dir,
                    &claim,
                    Err(worker_err),
                    user_cancel_requested.load(Ordering::SeqCst),
                );
                if let Some(mut access) = state.acquire_for_worker(true).await {
                    if let Err(err) = access.core_mut().record_worker_outcome(
                        &claim,
                        completion,
                        current_time_ms(),
                    ) {
                        state.check_core_err_fatal("record_worker_outcome after prep error", &err);
                    }
                    drop(access);
                    state.wake_notify.notify_one();
                }
            }
            return run_id;
        }
        Err(join_err) => {
            if state.fatal_error().is_none() && !*shutdown_rx.borrow() {
                let completion = build_completion_record(
                    &state.state_dir,
                    &claim,
                    Err(threadlane_workers::WorkerError::Io(std::io::Error::other(
                        format!("worktree preparation task panicked: {join_err}"),
                    ))),
                    user_cancel_requested.load(Ordering::SeqCst),
                );
                if let Some(mut access) = state.acquire_for_worker(true).await {
                    if let Err(err) = access.core_mut().record_worker_outcome(
                        &claim,
                        completion,
                        current_time_ms(),
                    ) {
                        state.check_core_err_fatal("record_worker_outcome after join error", &err);
                    }
                    drop(access);
                    state.wake_notify.notify_one();
                }
            }
            return run_id;
        }
    };

    // 2. Record `Running` with owned worktree metadata.
    let mut cancelled_before_spawn = user_cancel_requested.load(Ordering::SeqCst);
    {
        let Some(mut access) = state.acquire_for_worker(false).await else {
            return run_id;
        };
        match access.core_mut().record_worker_running(
            &claim,
            worktree_update.clone(),
            current_time_ms(),
        ) {
            Ok(snapshot) => {
                if snapshot.status == WorkerStatus::CancelRequested
                    || snapshot.cancel_requested_at_ms.is_some()
                {
                    user_cancel_requested.store(true, Ordering::SeqCst);
                    let _ = cancel_tx.send(true);
                    cancelled_before_spawn = true;
                }
            }
            Err(CoreError::StaleRevision { .. } | CoreError::InvalidRunState { .. }) => {
                if user_cancel_requested.load(Ordering::SeqCst) {
                    cancelled_before_spawn = true;
                } else {
                    return run_id;
                }
            }
            Err(CoreError::RunNotFound { .. }) => {
                return run_id;
            }
            Err(err) => {
                state.check_core_err_fatal("record_worker_running failed", &err);
                return run_id;
            }
        }
        if cancelled_before_spawn {
            let completion = build_pre_spawn_cancelled_completion(Some(&worktree_update));
            if let Err(err) =
                access
                    .core_mut()
                    .record_worker_outcome(&claim, completion, current_time_ms())
            {
                if !matches!(
                    err,
                    CoreError::StaleRevision { .. }
                        | CoreError::InvalidRunState { .. }
                        | CoreError::RunNotFound { .. }
                ) {
                    state
                        .check_core_err_fatal("record_worker_outcome after pre-spawn cancel", &err);
                }
            }
            drop(access);
            state.wake_notify.notify_one();
            return run_id;
        }
    }
    state.wake_notify.notify_one();

    // 3. Execute worker process + post-run diff gate while forwarding bounded live progress.
    let (progress_tx, mut progress_rx) =
        mpsc::channel::<NormalizedWorkerEvent>(LIVE_OBSERVER_CHANNEL_CAPACITY);

    let progress_state = Arc::clone(&state);
    let progress_claim = claim.clone();
    let progress_fut = async move {
        while let Some(first_event) = progress_rx.recv().await {
            let mut batch = Vec::with_capacity(16);
            batch.push(map_live_event_to_progress(&first_event));
            while batch.len() < 32 {
                match progress_rx.try_recv() {
                    Ok(ev) => batch.push(map_live_event_to_progress(&ev)),
                    Err(_) => break,
                }
            }

            let Some(mut access) = progress_state.acquire_for_worker(false).await else {
                while progress_rx.recv().await.is_some() {}
                break;
            };
            match access.core_mut().record_worker_progress(
                &progress_claim,
                &batch,
                current_time_ms(),
            ) {
                Ok(_) => {
                    drop(access);
                    progress_state.wake_notify.notify_one();
                }
                Err(
                    CoreError::StaleRevision { .. }
                    | CoreError::InvalidRunState { .. }
                    | CoreError::RunNotFound { .. },
                ) => {
                    drop(access);
                    while progress_rx.recv().await.is_some() {}
                    break;
                }
                Err(err) => {
                    progress_state.check_core_err_fatal("record_worker_progress failed", &err);
                    drop(access);
                    while progress_rx.recv().await.is_some() {}
                    break;
                }
            }
        }
    };

    let exec_fut = execute_claimed_worker(
        &profile,
        &claim,
        &worktree_update.task_worktree_id,
        &mut cancel_rx,
        progress_tx,
    );
    let (exec_res, ()) = tokio::join!(exec_fut, progress_fut);

    // 4. Persist terminal outcome (unless stopped solely due to supervisor shutdown/fault).
    if state.fatal_error().is_some() {
        return run_id;
    }

    let user_cancelled = user_cancel_requested.load(Ordering::SeqCst);
    let shutdown_interrupted = (*shutdown_rx.borrow() || state.retired.load(Ordering::SeqCst))
        && !user_cancelled
        && matches!(
            &exec_res,
            Ok(rep) if rep.worker_report.status == threadlane_workers::WorkerRunStatus::Cancelled
        );

    if shutdown_interrupted {
        // Leave for `mark_active_workers_unknown` on shutdown so it is recorded as `Unknown`
        // rather than a user-requested `Cancelled`.
        return run_id;
    }

    let completion = build_completion_record(&state.state_dir, &claim, exec_res, user_cancelled);

    let Some(mut access) = state.acquire_for_worker(true).await else {
        return run_id;
    };
    match access
        .core_mut()
        .record_worker_outcome(&claim, completion, current_time_ms())
    {
        Ok(_) => {
            drop(access);
            state.wake_notify.notify_one();
        }
        Err(
            CoreError::StaleRevision { .. }
            | CoreError::InvalidRunState { .. }
            | CoreError::RunNotFound { .. },
        ) => {}
        Err(err) => {
            state.check_core_err_fatal("record_worker_outcome failed", &err);
        }
    }

    run_id
}

/// Returns the current wall-clock timestamp in milliseconds since the Unix epoch.
pub fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
