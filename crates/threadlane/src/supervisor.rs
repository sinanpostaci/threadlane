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
    estimate_wire_response_bytes, BacklogItemEdit, Command, Core, CoreError, NewBacklogItem,
    WakeupStatus, WorkerClaim, WorkerStatus, DEFAULT_PAGE_LIMIT, MAX_COMPACT_RESPONSE_BYTES,
    MAX_STATUS_RECENT_RUNS,
};
use threadlane_workers::{NormalizedWorkerEvent, OwnedProcessSnapshot};
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
use crate::operator_backlog::{
    enforce_operator_worker_backlog_response_budget, execute_operator_worker_backlog,
    OperatorWorkerBacklogRequest, OperatorWorkerBacklogResponse, OPERATOR_WORKER_BACKLOG_PATH,
};
use crate::presence::{
    apply_physical_verification, evaluate_cached_presence, same_process_identity,
    unavailable_from_worker, verify_linux_process, PresenceEvaluation, WorkerPresenceCache,
    WorkerPresenceSnapshot, WorkerPresenceUnavailableReason,
};
use crate::protocol::{
    BindingListResponse, CreateBindingRequest, GetOrCreateBindingRequest,
    GetOrCreateBindingResponse, HostControlBusyCounts, HostControlConfig, HostControlInfoRequest,
    HostControlOwnerScope, HostControlRuntimeState, HostControlStatusResponse,
    RearmHostWakeupRequest, RearmHostWakeupResponse, RetryWakeupRequest, ScopedRequest,
    ScopedResponse, StatusResponse, StopIdleHostRequest,
};
use crate::workers::{
    build_completion_record, build_pre_spawn_cancelled_completion, execute_claimed_worker,
    map_live_event_to_progress, map_provider_health_to_progress, prepare_claim_worktrees,
    resolve_worker_execution, ResolvedWorkerExecution, ValidatedWorkersConfig, WorkersConfig,
    LIVE_OBSERVER_CHANNEL_CAPACITY,
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
    presence: Arc<WorkerPresenceCache>,
}

struct ActiveWorkerGuard {
    state: Arc<SharedState>,
    claim: WorkerClaim,
}

impl Drop for ActiveWorkerGuard {
    fn drop(&mut self) {
        self.state.unregister_active_worker(&self.claim);
    }
}

struct SharedState {
    state_dir: PathBuf,
    active: Mutex<Option<ActiveCore>>,
    retired: AtomicBool,
    stopping: AtomicBool,
    stop_tx: watch::Sender<bool>,
    host_control: Option<HostControlConfig>,
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
        presence: Arc<WorkerPresenceCache>,
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
                presence,
            },
        );
    }

    fn unregister_active_worker(&self, claim: &WorkerClaim) {
        let mut map = self
            .active_workers
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if map
            .get(&claim.run_id)
            .is_some_and(|entry| entry.claim == *claim)
        {
            map.remove(&claim.run_id);
        }
    }

    fn active_worker_presence(
        &self,
        binding_id: &str,
        run_id: &str,
    ) -> Option<(WorkerClaim, Arc<WorkerPresenceCache>)> {
        let map = self
            .active_workers
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let entry = map.get(run_id)?;
        if entry.claim.binding_id != binding_id {
            return None;
        }
        Some((entry.claim.clone(), Arc::clone(&entry.presence)))
    }

    fn presence_owner(&self, binding_id: &str) -> Option<HostControlConfig> {
        self.host_control
            .as_ref()
            .filter(|control| {
                control.session_binding_id == binding_id || control.control_binding_id == binding_id
            })
            .cloned()
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
        if !allow_when_retiring
            && (self.retired.load(Ordering::SeqCst) || self.stopping.load(Ordering::SeqCst))
        {
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
        if !allow_when_retiring && self.stopping.load(Ordering::SeqCst) {
            return None;
        }
        if guard.is_none() {
            return None;
        }

        Some(CoreAccessGuard { state: self, guard })
    }

    async fn resolve_worker_presence(
        &self,
        binding_id: &str,
        worker: threadlane_core::WorkerSnapshot,
    ) -> Result<WorkerPresenceSnapshot, (StatusCode, Json<ApiError>)> {
        // Terminal, stopped, stale, and unavailable cache outcomes need no Core access. No await
        // occurs before this evaluation, so its flags and cache are current.
        let owner = self.presence_owner(binding_id);
        let retired = self.retired.load(Ordering::SeqCst) || self.fatal_error().is_some();
        let stopping = self.stopping.load(Ordering::SeqCst);
        let active = self.active_worker_presence(binding_id, &worker.run_id);
        let cached = active.as_ref().map(|(_, cache)| cache.read());
        if let PresenceEvaluation::Final(presence) = evaluate_cached_presence(
            &worker,
            active.as_ref().map(|(claim, _)| claim),
            cached.as_ref(),
            owner.as_ref(),
            stopping,
            retired,
        ) {
            return Ok(presence);
        }

        // Physical proof is only valid for the sample observed after the final Core wait.
        let mut access = match self.acquire_for_request().await {
            Ok(access) => access,
            Err(_) => {
                let owner = self.presence_owner(binding_id);
                let retired = self.retired.load(Ordering::SeqCst) || self.fatal_error().is_some();
                let stopping = self.stopping.load(Ordering::SeqCst);
                let reason = if retired {
                    WorkerPresenceUnavailableReason::SupervisorRetired
                } else if stopping {
                    WorkerPresenceUnavailableReason::SupervisorStopping
                } else {
                    WorkerPresenceUnavailableReason::SupervisorUnavailable
                };
                return Ok(unavailable_from_worker(&worker, owner.as_ref(), reason));
            }
        };

        // Refresh mutable inputs after the awaited acquisition, then read the scoped worker and
        // its exact claim/cache under the same Core guard used for physical inspection.
        let owner = self.presence_owner(binding_id);
        let retired = self.retired.load(Ordering::SeqCst) || self.fatal_error().is_some();
        let stopping = self.stopping.load(Ordering::SeqCst);
        let latest = access
            .core_mut()
            .get_worker(binding_id, &worker.run_id)
            .map_err(|err| map_core_error(self, "worker presence physical recheck failed", err))?;
        let active = self.active_worker_presence(binding_id, &latest.run_id);
        let cached = active.as_ref().map(|(_, cache)| cache.read());
        let (_base, process) = match evaluate_cached_presence(
            &latest,
            active.as_ref().map(|(claim, _)| claim),
            cached.as_ref(),
            owner.as_ref(),
            stopping,
            retired,
        ) {
            PresenceEvaluation::Final(presence) => return Ok(presence),
            PresenceEvaluation::RequiresPhysicalCheck(base, process) => (base, process),
        };

        // Bounded synchronous `/proc` inspection. Nothing is carried across an await.
        let verification = verify_linux_process(&process);

        // The observer and shutdown flags can move while inspection runs. Re-evaluate them before
        // replying and apply proof only to the same claim and process identity.
        let owner_now = self.presence_owner(binding_id);
        let retired_now = self.retired.load(Ordering::SeqCst) || self.fatal_error().is_some();
        let stopping_now = self.stopping.load(Ordering::SeqCst);
        let active_now = self.active_worker_presence(binding_id, &latest.run_id);
        let cached_now = active_now.as_ref().map(|(_, cache)| cache.read());
        match evaluate_cached_presence(
            &latest,
            active_now.as_ref().map(|(claim, _)| claim),
            cached_now.as_ref(),
            owner_now.as_ref(),
            stopping_now,
            retired_now,
        ) {
            PresenceEvaluation::Final(presence) => Ok(presence),
            PresenceEvaluation::RequiresPhysicalCheck(post_base, post_process) => {
                let same_claim = match (active.as_ref(), active_now.as_ref()) {
                    (Some((before, _)), Some((after, _))) => before == after,
                    (None, None) => true,
                    _ => false,
                };
                if !same_claim || !same_process_identity(&process, &post_process) {
                    return Ok(unavailable_from_worker(
                        &latest,
                        owner_now.as_ref(),
                        WorkerPresenceUnavailableReason::ObservationChanged,
                    ));
                }
                // A concurrent heartbeat may advance the sample while the bounded inspection
                // runs. Re-inspect only when it is a different observation of the same process.
                let post_verification = if post_process == process {
                    verification
                } else {
                    verify_linux_process(&post_process)
                };
                Ok(apply_physical_verification(post_base, post_verification))
            }
        }
    }

    fn ensure_not_stopping(&self) -> Result<(), (StatusCode, Json<ApiError>)> {
        if self.stopping.load(Ordering::SeqCst) {
            return Err(stopping_response());
        }
        Ok(())
    }
}

fn new_process_presence_channel() -> (
    watch::Sender<Option<OwnedProcessSnapshot>>,
    Arc<WorkerPresenceCache>,
) {
    let (process_tx, process_rx) = watch::channel::<Option<OwnedProcessSnapshot>>(None);
    (process_tx, Arc::new(WorkerPresenceCache::new(process_rx)))
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

fn stopping_response() -> (StatusCode, Json<ApiError>) {
    (
        StatusCode::CONFLICT,
        Json(ApiError::new(
            ErrorCode::HostStopping,
            "host session is stopping and no longer accepting mutations",
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

    /// Waits until an authenticated, owner-scoped idle stop request is latched on this supervisor.
    ///
    /// If host control is not configured on this supervisor, this future remains pending indefinitely.
    pub async fn wait_for_stop_request(&self) {
        if self.shared.host_control.is_none() {
            std::future::pending::<()>().await;
            return;
        }
        let mut rx = self.shared.stop_tx.subscribe();
        loop {
            if *rx.borrow_and_update() || self.shared.stopping.load(Ordering::SeqCst) {
                return;
            }
            if rx.changed().await.is_err() {
                if self.shared.stopping.load(Ordering::SeqCst) {
                    return;
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
    ///
    /// Host session control is disabled by default on supervisors started via [`Supervisor::start`].
    pub async fn start(config: SupervisorConfig) -> Result<SupervisorHandle, Error> {
        Self::start_inner(config, None).await
    }

    /// Starts the supervisor on `127.0.0.1:0` with owner-scoped [`HostControlConfig`] installed.
    pub async fn start_with_host_control(
        config: SupervisorConfig,
        control: HostControlConfig,
    ) -> Result<SupervisorHandle, Error> {
        validate_host_control_config(&control)?;
        Self::start_inner(config, Some(control)).await
    }

    async fn start_inner(
        config: SupervisorConfig,
        host_control: Option<HostControlConfig>,
    ) -> Result<SupervisorHandle, Error> {
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
        let (stop_tx, _stop_rx) = watch::channel(false);

        let shared = Arc::new(SharedState {
            state_dir: config.state_dir.clone(),
            active: Mutex::new(Some(active_core)),
            retired: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            stop_tx,
            host_control,
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

fn validate_host_control_config(control: &HostControlConfig) -> Result<(), Error> {
    validate_host_control_id("approval_id", &control.approval_id)?;
    if control.subject_digest.len() != 64
        || !control
            .subject_digest
            .chars()
            .all(|ch| matches!(ch, '0'..='9' | 'a'..='f'))
    {
        return Err(Error::InvalidConfig(
            "host control `subject_digest` must be 64 lowercase hex characters".to_string(),
        ));
    }
    validate_host_control_id("control_binding_id", &control.control_binding_id)?;
    validate_host_control_id("session_binding_id", &control.session_binding_id)?;
    validate_host_control_id("thread_uuid", &control.thread_uuid)?;
    let Some(hex_part) = control.instance_id.strip_prefix("hst_") else {
        return Err(Error::InvalidConfig(format!(
            "invalid host control instance_id `{}`: must match `hst_<32 lowercase hex chars>`",
            control.instance_id
        )));
    };
    if hex_part.len() != 32
        || !hex_part
            .chars()
            .all(|ch| matches!(ch, '0'..='9' | 'a'..='f'))
    {
        return Err(Error::InvalidConfig(format!(
            "invalid host control instance_id `{}`: must match `hst_<32 lowercase hex chars>`",
            control.instance_id
        )));
    }
    Ok(())
}

fn validate_host_control_id(field: &'static str, value: &str) -> Result<(), Error> {
    if value.is_empty()
        || value.len() > threadlane_core::MAX_ID_LEN
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(Error::InvalidConfig(format!(
            "invalid host control `{field}` identifier `{value}`"
        )));
    }
    Ok(())
}

fn build_router(state: Arc<SharedState>) -> Router {
    Router::new()
        .route(
            "/v1/bindings",
            post(handle_create_binding).get(handle_list_bindings),
        )
        .route(
            "/v1/bindings/get-or-create",
            post(handle_get_or_create_binding),
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
        .route("/v1/host-control/info", post(handle_host_control_info))
        .route("/v1/host-control/stop", post(handle_stop_idle_host))
        .route(
            "/v1/host-control/rearm-wakeup",
            post(handle_rearm_host_wakeup),
        )
        .route(
            OPERATOR_WORKER_BACKLOG_PATH,
            post(handle_operator_worker_backlog),
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
    state.ensure_not_stopping()?;
    let binding = access
        .core_mut()
        .create_binding(&req.thread_uuid, &req.label)
        .map_err(|err| map_core_error(&state, "create_binding failed", err))?;
    Ok((StatusCode::CREATED, Json(binding)))
}

async fn handle_get_or_create_binding(
    State(state): State<Arc<SharedState>>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Result<(StatusCode, Json<GetOrCreateBindingResponse>), (StatusCode, Json<ApiError>)> {
    verify_bearer_auth(&headers, &state.token)?;
    let req: GetOrCreateBindingRequest = parse_json_body(body)?;

    let mut access = state.acquire_for_request().await?;
    state.ensure_not_stopping()?;
    let outcome = access
        .core_mut()
        .get_or_create_binding(&req.thread_uuid, &req.label)
        .map_err(|err| map_core_error(&state, "get_or_create_binding failed", err))?;
    let status = if outcome.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(outcome)))
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
        if req.is_mutating() {
            state.ensure_not_stopping()?;
        }
        if !state.stopping.load(Ordering::SeqCst)
            && !matches!(
                req,
                ScopedRequest::ListActivity { .. }
                    | ScopedRequest::GetWorkerPresence { .. }
                    | ScopedRequest::GetWorker { .. }
            )
            && !req.is_backlog()
        {
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
            ScopedRequest::BacklogCreate {
                command_key,
                title,
                acceptance_condition,
                delivery_description,
                kind,
            } => {
                let item = access
                    .core_mut()
                    .create_backlog_item(
                        &binding_id,
                        &command_key,
                        NewBacklogItem {
                            title,
                            acceptance_condition,
                            delivery_description,
                            kind,
                        },
                        now,
                    )
                    .map_err(|err| map_core_error(&state, "backlog_create failed", err))?;
                ScopedResponse::BacklogItem(item)
            }
            ScopedRequest::BacklogGet { item_id } => {
                let item = access
                    .core_mut()
                    .get_backlog_item(&binding_id, &item_id)
                    .map_err(|err| map_core_error(&state, "backlog_get failed", err))?;
                ScopedResponse::BacklogItem(item)
            }
            ScopedRequest::BacklogList { cursor, limit } => {
                let page = access
                    .core_mut()
                    .list_backlog_items(
                        &binding_id,
                        cursor.as_deref(),
                        limit.unwrap_or(DEFAULT_PAGE_LIMIT),
                    )
                    .map_err(|err| map_core_error(&state, "backlog_list failed", err))?;
                ScopedResponse::BacklogItemPage(page)
            }
            ScopedRequest::BacklogEdit {
                command_key,
                item_id,
                expected_revision,
                title,
                acceptance_condition,
                delivery_description,
            } => {
                let item = access
                    .core_mut()
                    .edit_backlog_item(
                        &binding_id,
                        &command_key,
                        &item_id,
                        expected_revision,
                        BacklogItemEdit {
                            title,
                            acceptance_condition,
                            delivery_description,
                        },
                        now,
                    )
                    .map_err(|err| map_core_error(&state, "backlog_edit failed", err))?;
                ScopedResponse::BacklogItem(item)
            }
            ScopedRequest::BacklogBlock {
                command_key,
                item_id,
                expected_revision,
                reason,
            } => {
                let item = access
                    .core_mut()
                    .block_backlog_item(
                        &binding_id,
                        &command_key,
                        &item_id,
                        expected_revision,
                        &reason,
                        now,
                    )
                    .map_err(|err| map_core_error(&state, "backlog_block failed", err))?;
                ScopedResponse::BacklogItem(item)
            }
            ScopedRequest::BacklogStart {
                command_key,
                item_id,
                expected_revision,
            } => {
                let item = access
                    .core_mut()
                    .start_backlog_item(&binding_id, &command_key, &item_id, expected_revision, now)
                    .map_err(|err| map_core_error(&state, "backlog_start failed", err))?;
                ScopedResponse::BacklogItem(item)
            }
            ScopedRequest::BacklogComplete {
                command_key,
                item_id,
                expected_revision,
                note,
                evidence,
            } => {
                let item = access
                    .core_mut()
                    .complete_backlog_item(
                        &binding_id,
                        &command_key,
                        &item_id,
                        expected_revision,
                        &note,
                        &evidence,
                        now,
                    )
                    .map_err(|err| map_core_error(&state, "backlog_complete failed", err))?;
                ScopedResponse::BacklogItem(item)
            }
            ScopedRequest::BacklogHistory {
                item_id,
                cursor,
                limit,
            } => {
                let page = access
                    .core_mut()
                    .list_backlog_history(
                        &binding_id,
                        &item_id,
                        cursor.as_deref(),
                        limit.unwrap_or(DEFAULT_PAGE_LIMIT),
                    )
                    .map_err(|err| map_core_error(&state, "backlog_history failed", err))?;
                ScopedResponse::BacklogHistory(page)
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

                let resolved = resolve_worker_execution(profile, &command_key, instruction.clone())
                    .map_err(|err| {
                        let safe_msg = threadlane_workers::sanitize_and_redact(
                            &err.to_string(),
                            threadlane_core::MAX_WORKER_SUMMARY_TEXT_BYTES,
                        );
                        (
                            StatusCode::BAD_REQUEST,
                            Json(ApiError::with_details(
                                ErrorCode::ValidationFailed,
                                safe_msg,
                                serde_json::json!({
                                    "field": "profile_id",
                                    "profile_id": profile.profile_id,
                                }),
                            )),
                        )
                    })?;

                let cmd = Command::StartWorker {
                    command_key,
                    profile_id: profile.profile_id.clone(),
                    profile_digest: profile.profile_digest.clone(),
                    provider: profile.provider_str().to_string(),
                    requested_model: profile.requested_model_str().to_string(),
                    requested_effort: profile.requested_effort.clone(),
                    base_commit: resolved.spec.base_commit.clone(),
                    instruction,
                    execution_binding: resolved.execution_binding.clone(),
                    now_ms: now,
                };
                let output = access
                    .core_mut()
                    .execute(&binding_id, cmd)
                    .map_err(|err| map_core_error(&state, "start_worker execution failed", err))?;
                ScopedResponse::from(output)
            }
            ScopedRequest::GetWorkerPresence { run_id } => {
                let worker = access
                    .core_mut()
                    .get_worker(&binding_id, &run_id)
                    .map_err(|err| {
                        map_core_error(&state, "worker presence get_worker failed", err)
                    })?;
                drop(access);
                let presence = state.resolve_worker_presence(&binding_id, worker).await?;
                let response = ScopedResponse::WorkerPresence(presence);
                let serialized_len = estimate_wire_response_bytes(&response).map_err(|err| {
                    map_core_error(&state, "worker presence serialization failed", err)
                })?;
                if serialized_len > MAX_COMPACT_RESPONSE_BYTES {
                    return Err(map_core_error(
                        &state,
                        "worker presence budget check failed",
                        CoreError::ResponseTooLarge {
                            size_bytes: serialized_len,
                            max_bytes: MAX_COMPACT_RESPONSE_BYTES,
                        },
                    ));
                }
                response
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

async fn handle_operator_worker_backlog(
    State(state): State<Arc<SharedState>>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<OperatorWorkerBacklogResponse>, (StatusCode, Json<ApiError>)> {
    verify_bearer_auth(&headers, &state.token)?;
    let req: OperatorWorkerBacklogRequest = parse_json_body(body)?;
    let now = current_time_ms();

    let mut access = state.acquire_for_request().await?;
    if req.is_mutating() {
        state.ensure_not_stopping()?;
    }
    let response = execute_operator_worker_backlog(access.core_mut(), now, req)
        .map_err(|err| map_core_error(&state, "operator worker backlog failed", err))?;
    enforce_operator_worker_backlog_response_budget(&response).map_err(|err| {
        map_core_error(
            &state,
            "operator worker backlog response budget failed",
            err,
        )
    })?;
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
        state.ensure_not_stopping()?;
        access
            .core_mut()
            .retry_wakeup(&binding_id, &req.expected_claim_id, req.allow_unknown, now)
            .map_err(|err| map_core_error(&state, "retry_wakeup failed", err))?
    };

    state.wake_notify.notify_one();
    Ok(Json(notice))
}

fn verify_host_control_scope<'a>(
    state: &'a SharedState,
    req: &HostControlOwnerScope,
) -> Result<&'a HostControlConfig, (StatusCode, Json<ApiError>)> {
    let Some(control) = state.host_control.as_ref() else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::new(
                ErrorCode::HostControlNotConfigured,
                "host session control is not configured on this supervisor",
            )),
        ));
    };

    let scope_checks = [
        ("approval_id", &control.approval_id, &req.approval_id),
        (
            "subject_digest",
            &control.subject_digest,
            &req.subject_digest,
        ),
        (
            "control_binding_id",
            &control.control_binding_id,
            &req.control_binding_id,
        ),
        ("thread_uuid", &control.thread_uuid, &req.thread_uuid),
        (
            "session_binding_id",
            &control.session_binding_id,
            &req.session_binding_id,
        ),
    ];

    for (field, expected, actual) in scope_checks {
        if expected != actual {
            return Err((
                StatusCode::CONFLICT,
                Json(ApiError::with_details(
                    ErrorCode::HostScopeMismatch,
                    format!(
                        "host control scope mismatch on `{field}`: expected `{expected}`, got `{actual}`"
                    ),
                    serde_json::json!({
                        "field": field,
                        "expected": expected,
                        "actual": actual,
                    }),
                )),
            ));
        }
    }

    if req.expected_instance_id != control.instance_id {
        return Err((
            StatusCode::CONFLICT,
            Json(ApiError::with_details(
                ErrorCode::HostInstanceMismatch,
                format!(
                    "host instance mismatch for approval `{}`: expected `{}`, actual `{}`",
                    control.approval_id, req.expected_instance_id, control.instance_id
                ),
                serde_json::json!({
                    "approval_id": control.approval_id,
                    "expected_instance_id": req.expected_instance_id,
                    "actual_instance_id": control.instance_id,
                }),
            )),
        ));
    }

    Ok(control)
}

fn inspect_host_busy_counts(
    state: &SharedState,
    access: &mut CoreAccessGuard<'_>,
) -> Result<HostControlBusyCounts, (StatusCode, Json<ApiError>)> {
    let inspection = access
        .core_mut()
        .inspect_runtime_idle()
        .map_err(|err| map_core_error(state, "inspect_runtime_idle failed", err))?;
    let mut pending_enabled_wakeups = 0usize;
    if let Some(codex_cfg) = &state.codex {
        for binding_id in &codex_cfg.notify_bindings {
            match access.core_mut().get_wakeup(binding_id) {
                Ok(wakeup) => {
                    if wakeup.status == WakeupStatus::Pending {
                        pending_enabled_wakeups += 1;
                    }
                }
                Err(CoreError::BindingNotFound { .. }) => {
                    // Deliberate no-op when an allowlisted binding does not exist yet,
                    // matching the background notifier loop.
                }
                Err(err) => {
                    return Err(map_core_error(
                        state,
                        &format!("get_wakeup failed for notify binding `{binding_id}`"),
                        err,
                    ));
                }
            }
        }
    }
    let live_worker_tasks = state
        .active_workers
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .len();
    Ok(HostControlBusyCounts {
        non_terminal_runs: inspection.non_terminal_runs,
        non_terminal_workers: inspection.non_terminal_workers,
        live_worker_tasks,
        in_flight_wakeups: inspection.in_flight_wakeups,
        pending_enabled_wakeups,
    })
}

fn build_host_control_status_response(
    control: &HostControlConfig,
    runtime_state: HostControlRuntimeState,
    busy: HostControlBusyCounts,
) -> HostControlStatusResponse {
    HostControlStatusResponse {
        approval_id: control.approval_id.clone(),
        subject_digest: control.subject_digest.clone(),
        control_binding_id: control.control_binding_id.clone(),
        thread_uuid: control.thread_uuid.clone(),
        session_binding_id: control.session_binding_id.clone(),
        instance_id: control.instance_id.clone(),
        state: runtime_state,
        idle: busy.is_idle(),
        busy,
    }
}

async fn handle_host_control_info(
    State(state): State<Arc<SharedState>>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<HostControlStatusResponse>, (StatusCode, Json<ApiError>)> {
    verify_bearer_auth(&headers, &state.token)?;
    let req: HostControlInfoRequest = parse_json_body(body)?;

    let mut access = state.acquire_for_request().await?;
    let control = verify_host_control_scope(&state, &req)?;
    let busy = inspect_host_busy_counts(&state, &mut access)?;
    let runtime_state = if state.stopping.load(Ordering::SeqCst) {
        HostControlRuntimeState::Stopping
    } else {
        HostControlRuntimeState::Ready
    };

    Ok(Json(build_host_control_status_response(
        control,
        runtime_state,
        busy,
    )))
}

async fn handle_stop_idle_host(
    State(state): State<Arc<SharedState>>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<HostControlStatusResponse>, (StatusCode, Json<ApiError>)> {
    verify_bearer_auth(&headers, &state.token)?;
    let req: StopIdleHostRequest = parse_json_body(body)?;

    let mut access = state.acquire_for_request().await?;
    let control = verify_host_control_scope(&state, &req)?;
    let busy = inspect_host_busy_counts(&state, &mut access)?;

    if state.stopping.load(Ordering::SeqCst) {
        return Ok(Json(build_host_control_status_response(
            control,
            HostControlRuntimeState::Stopping,
            busy,
        )));
    }

    if !busy.is_idle() {
        return Err((
            StatusCode::CONFLICT,
            Json(ApiError::with_details(
                ErrorCode::HostNotIdle,
                format!(
                    "host session for approval `{}` is not idle (non_terminal_runs={}, non_terminal_workers={}, live_worker_tasks={}, in_flight_wakeups={}, pending_enabled_wakeups={})",
                    control.approval_id,
                    busy.non_terminal_runs,
                    busy.non_terminal_workers,
                    busy.live_worker_tasks,
                    busy.in_flight_wakeups,
                    busy.pending_enabled_wakeups,
                ),
                serde_json::to_value(busy).unwrap_or_default(),
            )),
        ));
    }

    state.stopping.store(true, Ordering::SeqCst);
    let _ = state.stop_tx.send_replace(true);
    state.wake_notify.notify_waiters();

    Ok(Json(build_host_control_status_response(
        control,
        HostControlRuntimeState::Stopping,
        busy,
    )))
}

async fn handle_rearm_host_wakeup(
    State(state): State<Arc<SharedState>>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<RearmHostWakeupResponse>, (StatusCode, Json<ApiError>)> {
    verify_bearer_auth(&headers, &state.token)?;
    let req: RearmHostWakeupRequest = parse_json_body(body)?;
    let now = current_time_ms();

    let response = {
        let mut access = state.acquire_for_request().await?;
        state.ensure_not_stopping()?;
        let control = verify_host_control_scope(&state, &req.scope)?;
        let session_binding_id = control.session_binding_id.clone();
        let instance_id = control.instance_id.clone();
        let Some(codex_cfg) = state.codex.as_ref() else {
            return Err((
                StatusCode::CONFLICT,
                Json(ApiError::new(
                    ErrorCode::InvalidConfig,
                    "host wake-up notifications are not enabled on this supervisor",
                )),
            ));
        };
        if !codex_cfg.notify_bindings.contains(&session_binding_id) {
            return Err((
                StatusCode::CONFLICT,
                Json(ApiError::new(
                    ErrorCode::InvalidConfig,
                    "host wake-up notifications are not enabled for this session binding",
                )),
            ));
        }
        let binding = access
            .core_mut()
            .get_binding(&session_binding_id)
            .map_err(|err| map_core_error(&state, "rearm_host_wakeup get_binding failed", err))?;
        let wakeup = access
            .core_mut()
            .rearm_failed_wakeup(&session_binding_id, &req.expected_claim_id, now)
            .map_err(|err| map_core_error(&state, "rearm_host_wakeup failed", err))?;
        RearmHostWakeupResponse {
            instance_id,
            binding,
            wakeup,
        }
    };

    state.wake_notify.notify_one();
    Ok(Json(response))
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

        if !state.stopping.load(Ordering::SeqCst) {
            if let Some(mut access) = state.acquire_for_worker(false).await {
                if let Err(err) = access.core_mut().tick(current_time_ms()) {
                    let _ = state
                        .record_core_fatal_error("background demo tick persistence failed", &err);
                    break;
                }
            }
        }

        if !state.stopping.load(Ordering::SeqCst) {
            if let Some(workers_cfg) = &state.workers {
                let mut fatal_occurred = false;
                while worker_tasks.len() < workers_cfg.max_parallel {
                    if *shutdown_rx.borrow()
                        || state.retired.load(Ordering::SeqCst)
                        || state.stopping.load(Ordering::SeqCst)
                        || state.fatal_error().is_some()
                    {
                        break;
                    }

                    let available_slots =
                        workers_cfg.max_parallel.saturating_sub(worker_tasks.len());
                    if available_slots == 0 {
                        break;
                    }

                    let mut rejected_any = false;
                    let claimed_batch = {
                        let Some(mut access) = state.acquire_for_worker(false).await else {
                            break;
                        };
                        let queued = match access.core_mut().list_queued_workers(available_slots) {
                            Ok(list) => list,
                            Err(err) => {
                                let _ = state
                                    .record_core_fatal_error("list_queued_workers failed", &err);
                                fatal_occurred = true;
                                break;
                            }
                        };
                        if queued.is_empty() {
                            break;
                        }

                        let mut newly_claimed = Vec::new();
                        for snap in queued {
                            if worker_tasks.len() + newly_claimed.len() >= workers_cfg.max_parallel
                            {
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

                            let claim_command_key = profile
                                .dependency_input
                                .as_ref()
                                .map(|policy| policy.command_key.as_str())
                                .unwrap_or("root-claim");

                            let resolved_res = if profile.dependency_input.is_some()
                                != snap.execution_binding.is_some()
                            {
                                Err(format!(
                                    "queued worker `{}` dependency policy presence does not match recorded execution_binding",
                                    snap.worker_id
                                ))
                            } else {
                                match resolve_worker_execution(
                                    profile,
                                    claim_command_key,
                                    snap.instruction.clone(),
                                ) {
                                    Ok(resolved) => {
                                        if resolved.execution_binding != snap.execution_binding {
                                            Err(format!(
                                                "queued worker `{}` execution_binding no longer matches current dependency input seal",
                                                snap.worker_id
                                            ))
                                        } else if resolved.spec.base_commit
                                            != snap.worktree.base_commit
                                        {
                                            Err(format!(
                                                "queued worker `{}` base_commit `{}` does not match resolved execution base `{}`",
                                                snap.worker_id,
                                                snap.worktree.base_commit,
                                                resolved.spec.base_commit
                                            ))
                                        } else {
                                            Ok(resolved)
                                        }
                                    }
                                    Err(err) => Err(format!(
                                        "queued worker `{}` dependency input validation failed at claim: {err}",
                                        snap.worker_id
                                    )),
                                }
                            };

                            let resolved = match resolved_res {
                                Ok(resolved) => resolved,
                                Err(raw_reason) => {
                                    let safe_reason = threadlane_workers::sanitize_and_redact(
                                        &raw_reason,
                                        threadlane_core::MAX_WORKER_SUMMARY_TEXT_BYTES,
                                    );
                                    let reason = if safe_reason.trim().is_empty() {
                                        "queued worker dependency input rejected before claim"
                                            .to_string()
                                    } else {
                                        safe_reason
                                    };
                                    match access.core_mut().reject_queued_worker(
                                        &snap.binding_id,
                                        &snap.run_id,
                                        snap.revision,
                                        &profile.profile_digest,
                                        &reason,
                                        current_time_ms(),
                                    ) {
                                        Ok(_) => {
                                            rejected_any = true;
                                            continue;
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
                                            let _ = state.record_core_fatal_error(
                                                "reject_queued_worker failed",
                                                &err,
                                            );
                                            fatal_occurred = true;
                                            break;
                                        }
                                    }
                                }
                            };

                            match access.core_mut().claim_worker_with_execution_binding(
                                &snap.binding_id,
                                &snap.run_id,
                                snap.revision,
                                &profile.profile_digest,
                                snap.execution_binding.as_ref(),
                                current_time_ms(),
                            ) {
                                Ok(claim) => {
                                    let (cancel_tx, cancel_rx) = watch::channel(false);
                                    let user_cancel_requested = Arc::new(AtomicBool::new(false));
                                    let (process_tx, presence) = new_process_presence_channel();
                                    state.register_active_worker(
                                        claim.clone(),
                                        cancel_tx.clone(),
                                        Arc::clone(&user_cancel_requested),
                                        Arc::clone(&presence),
                                    );
                                    newly_claimed.push((
                                        resolved,
                                        claim,
                                        cancel_tx,
                                        cancel_rx,
                                        user_cancel_requested,
                                        process_tx,
                                        presence,
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
                                    let _ =
                                        state.record_core_fatal_error("claim_worker failed", &err);
                                    fatal_occurred = true;
                                    break;
                                }
                            }
                        }
                        newly_claimed
                    };

                    if fatal_occurred {
                        break;
                    }
                    if claimed_batch.is_empty() {
                        if rejected_any {
                            continue;
                        }
                        break;
                    }

                    for (
                        resolved,
                        claim,
                        cancel_tx,
                        cancel_rx,
                        user_cancel_requested,
                        process_tx,
                        presence,
                    ) in claimed_batch
                    {
                        let task_state = Arc::clone(&state);
                        let task_shutdown_rx = shutdown_rx.clone();
                        worker_tasks.spawn(async move {
                            run_single_claimed_worker(
                                task_state,
                                resolved,
                                claim,
                                cancel_tx,
                                cancel_rx,
                                user_cancel_requested,
                                task_shutdown_rx,
                                process_tx,
                                presence,
                            )
                            .await
                        });
                    }
                }

                if fatal_occurred {
                    break;
                }
            }
        }

        if !state.stopping.load(Ordering::SeqCst) {
            if let Some(codex_cfg) = &state.codex {
                let mut fatal_occurred = false;
                loop {
                    if *shutdown_rx.borrow()
                        || state.retired.load(Ordering::SeqCst)
                        || state.stopping.load(Ordering::SeqCst)
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
                                        &format!(
                                            "failed to claim wake-up for binding `{binding_id}`"
                                        ),
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

#[allow(clippy::too_many_arguments)]
async fn run_single_claimed_worker(
    state: Arc<SharedState>,
    resolved: ResolvedWorkerExecution,
    claim: WorkerClaim,
    cancel_tx: watch::Sender<bool>,
    mut cancel_rx: watch::Receiver<bool>,
    user_cancel_requested: Arc<AtomicBool>,
    shutdown_rx: watch::Receiver<bool>,
    process_tx: watch::Sender<Option<OwnedProcessSnapshot>>,
    presence: Arc<WorkerPresenceCache>,
) -> String {
    let run_id = claim.run_id.clone();
    let _active_guard = ActiveWorkerGuard {
        state: Arc::clone(&state),
        claim: claim.clone(),
    };

    // 1. Prepare owned per-run and per-task worktrees without holding the Core mutex, while
    // retaining `resolved` (and its dependency seal shared lock lease) in scope.
    let prep_spec = resolved.spec.clone();
    let prep_claim = claim.clone();
    let worktree_res =
        tokio::task::spawn_blocking(move || prepare_claim_worktrees(&prep_spec, &prep_claim)).await;

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

    let (health_tx, mut health_rx) = watch::channel(None);
    let health_state = Arc::clone(&state);
    let health_claim = claim.clone();
    let health_fut = async move {
        while health_rx.changed().await.is_ok() {
            // Release the watch borrow before taking the core mutex. Updates coalesce
            // cumulatively while durable persistence is busy; ordinary progress cannot drop them.
            let snapshot = health_rx.borrow_and_update().clone();
            let Some(snapshot) = snapshot else { continue };
            let update = match map_provider_health_to_progress(&snapshot) {
                Ok(update) => update,
                Err(_) => continue, // Malformed diagnostics have no execution authority.
            };
            let Some(mut access) = health_state.acquire_for_worker(false).await else {
                break;
            };
            let persisted = access.core_mut().record_worker_progress(
                &health_claim,
                &[update],
                current_time_ms(),
            );
            drop(access);
            if !finish_optional_health_persistence(&health_state, persisted) {
                break;
            }
        }
    };

    // The runner and its observer must be polled concurrently: awaiting only for a watch
    // change would leave the runner pinned and never produce the next process sample.
    let process_fut = presence.watch_changes();
    let exec_fut = execute_claimed_worker(
        &resolved,
        &claim,
        &worktree_update.task_worktree_id,
        &mut cancel_rx,
        progress_tx,
        health_tx,
        Some(process_tx),
    );
    let (exec_res, (), (), ()) = tokio::join!(exec_fut, progress_fut, health_fut, process_fut);

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

// A failed optional write has no durable notice evidence and no authority to poison the
// independent terminal-result write. Critical terminal persistence keeps its existing fencing.
fn finish_optional_health_persistence<T>(
    state: &SharedState,
    result: Result<T, CoreError>,
) -> bool {
    if result.is_ok() {
        state.wake_notify.notify_one();
        true
    } else {
        false
    }
}

/// Returns the current wall-clock timestamp in milliseconds since the Unix epoch.
pub fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::os::unix::fs::PermissionsExt;

    use crate::client::SupervisorClient;

    const APPROVAL_ID: &str = "apr_11111111111111111111111111111111";
    const SUBJECT_DIGEST: &str = "2222222222222222222222222222222222222222222222222222222222222222";
    const THREAD_UUID: &str = "33333333-3333-4333-8333-333333333333";
    const OTHER_THREAD_UUID: &str = "44444444-4444-4444-8444-444444444444";
    const INSTANCE_ID: &str = "hst_55555555555555555555555555555555";
    const STALE_INSTANCE_ID: &str = "hst_66666666666666666666666666666666";

    #[tokio::test]
    async fn diagnostic_persistence_failure_keeps_terminal_recording_available_and_terminal_failure_still_fences(
    ) {
        use threadlane_core::{
            WorkerAcceptanceState, WorkerCompletionRecord, WorkerOutcomeSummary,
        };
        let root = tempfile::tempdir().expect("private root");
        let state_dir = root.path().join("state");
        let handle = Supervisor::start(SupervisorConfig::new(&state_dir))
            .await
            .expect("start");
        let client = SupervisorClient::from_state_dir(&state_dir).expect("client");
        let binding = client
            .create_binding(THREAD_UUID, "health persistence regression")
            .await
            .expect("binding");
        let digest = "a".repeat(64);
        let mut access = handle.shared.acquire_for_request().await.expect("core");
        let worker = access
            .core_mut()
            .execute(
                &binding.binding_id,
                Command::StartWorker {
                    command_key: "health-terminal-regression".into(),
                    profile_id: "prof_test".into(),
                    profile_digest: digest.clone(),
                    provider: "antigravity".into(),
                    requested_model: "gemini-4-argon-high".into(),
                    requested_effort: "high".into(),
                    base_commit: "1".repeat(40),
                    instruction: "test optional metadata persistence".into(),
                    execution_binding: None,
                    now_ms: current_time_ms(),
                },
            )
            .expect("queue")
            .into_worker()
            .expect("worker");
        let claim = access
            .core_mut()
            .claim_worker(
                &binding.binding_id,
                &worker.run_id,
                worker.revision,
                &digest,
                current_time_ms(),
            )
            .expect("claim");
        drop(access);

        // Inject a persistence-class error at the same policy boundary as the live consumer.
        let diagnostic_error =
            CoreError::CorruptState("injected diagnostic-only persistence fault".into());
        assert!(!finish_optional_health_persistence::<()>(
            &handle.shared,
            Err(diagnostic_error)
        ));
        assert!(handle.fatal_error().is_none());
        let mut access = handle
            .shared
            .acquire_for_worker(true)
            .await
            .expect("terminal core stays available");
        let terminal = access
            .core_mut()
            .record_worker_outcome(
                &claim,
                WorkerCompletionRecord {
                    status: WorkerStatus::Completed,
                    observed_model: Some("gemini-4-argon-high".into()),
                    observed_effort: Some("high".into()),
                    head_commit: Some("2".repeat(40)),
                    dropped_live_events: 0,
                    outcome: WorkerOutcomeSummary {
                        provider_retry_high_water: None,
                        provider_health: None,
                        exit_code: Some(0),
                        signal: None,
                        duration_ms: Some(10),
                        timed_out: false,
                        cancelled: false,
                        cancel_requested_before_completion: false,
                        missing_model_evidence: false,
                        missing_effort_evidence: false,
                        provider_error: false,
                        incomplete_cleanup: false,
                        rejected_by_diff_gate: false,
                        stdout_truncated: false,
                        stderr_truncated: false,
                        acceptance_state: WorkerAcceptanceState::ReadyForReview,
                        changed_paths_count: 0,
                        changed_paths_preview: Vec::new(),
                        violation_count: 0,
                        violation_summaries: Vec::new(),
                        final_summary: None,
                        failure_reason: None,
                        report_artifact_id: None,
                    },
                },
                current_time_ms(),
            )
            .expect("independent proven outcome is recorded");
        assert_eq!(terminal.status, WorkerStatus::Completed);
        assert!(terminal.progress.provider_health.is_none());
        drop(access);
        assert!(handle.fatal_error().is_none());

        handle.shared.check_core_err_fatal(
            "terminal write regression",
            &CoreError::CorruptState("critical terminal fault".into()),
        );
        assert!(
            handle.fatal_error().is_some(),
            "critical terminal fencing must remain"
        );
        handle.shutdown().await;
    }

    fn sample_host_control(session_binding_id: &str) -> HostControlConfig {
        HostControlConfig {
            approval_id: APPROVAL_ID.to_string(),
            subject_digest: SUBJECT_DIGEST.to_string(),
            control_binding_id: "bnd_control_001".to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            session_binding_id: session_binding_id.to_string(),
            instance_id: INSTANCE_ID.to_string(),
        }
    }

    fn owner_scope_from_config(cfg: &HostControlConfig) -> HostControlOwnerScope {
        HostControlOwnerScope {
            approval_id: cfg.approval_id.clone(),
            subject_digest: cfg.subject_digest.clone(),
            control_binding_id: cfg.control_binding_id.clone(),
            thread_uuid: cfg.thread_uuid.clone(),
            session_binding_id: cfg.session_binding_id.clone(),
            expected_instance_id: cfg.instance_id.clone(),
        }
    }

    // Cancellation alone is not an important event. Seed a real QuestionAsked through the Core
    // timing path (start -> tick to WaitingInput -> cancel), leaving a Pending wakeup and no
    // non-terminal run rows, without touching SQLite or persisted status directly.
    fn seed_pending_important_wakeup(core: &mut Core, binding_id: &str, command_prefix: &str) {
        let now = current_time_ms();
        let seeded_run = core
            .execute(
                binding_id,
                Command::StartDemo {
                    command_key: format!("{command_prefix}-start"),
                    instruction: "seed important question event before cancellation".to_string(),
                    step_delay_ms: Some(1),
                    now_ms: now,
                },
            )
            .expect("start seeded demo")
            .into_run()
            .expect("seeded demo run snapshot");

        core.tick(now + 2)
            .expect("tick seeded demo to WaitingInput");

        let waiting_run = core
            .execute(
                binding_id,
                Command::GetRun {
                    run_id: seeded_run.run_id.clone(),
                },
            )
            .expect("get seeded demo after tick")
            .into_run()
            .expect("seeded waiting run snapshot");
        assert_eq!(waiting_run.status, threadlane_core::RunStatus::WaitingInput);
        assert!(
            waiting_run.active_question.is_some(),
            "seeded tick must emit an important QuestionAsked event"
        );

        let cancelled_run = core
            .execute(
                binding_id,
                Command::Cancel {
                    command_key: format!("{command_prefix}-cancel"),
                    run_id: seeded_run.run_id,
                    reason: Some("seed pending wakeup via QuestionAsked".to_string()),
                    now_ms: now + 3,
                },
            )
            .expect("cancel seeded demo")
            .into_run()
            .expect("cancelled seeded run snapshot");
        assert_eq!(cancelled_run.status, threadlane_core::RunStatus::Cancelled);
    }

    #[tokio::test]
    async fn host_control_disabled_by_default_on_legacy_start() {
        let temp = tempfile::tempdir().expect("tempdir");
        let state_dir = temp.path().join("state");
        let handle = Supervisor::start(SupervisorConfig::new(&state_dir))
            .await
            .expect("start supervisor");
        let client = SupervisorClient::from_state_dir(&state_dir).expect("client");

        let scope = owner_scope_from_config(&sample_host_control("bnd_session_001"));
        let info_err = client
            .host_control_info(&scope)
            .await
            .expect_err("info should be disabled");
        match info_err {
            Error::Remote(api_err) => assert_eq!(api_err.code, ErrorCode::HostControlNotConfigured),
            other => panic!("unexpected error: {other:?}"),
        }

        let stop_err = client
            .stop_idle_host(&scope)
            .await
            .expect_err("stop should be disabled");
        match stop_err {
            Error::Remote(api_err) => assert_eq!(api_err.code, ErrorCode::HostControlNotConfigured),
            other => panic!("unexpected error: {other:?}"),
        }

        // wait_for_stop_request remains pending when host control is disabled
        let wait_res =
            tokio::time::timeout(Duration::from_millis(40), handle.wait_for_stop_request()).await;
        assert!(wait_res.is_err());

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn host_control_validates_auth_unknown_fields_owner_scope_and_instance_id() {
        let temp = tempfile::tempdir().expect("tempdir");
        let state_dir = temp.path().join("state");
        let control = sample_host_control("bnd_session_001");
        let handle =
            Supervisor::start_with_host_control(SupervisorConfig::new(&state_dir), control.clone())
                .await
                .expect("start with host control");
        let client = SupervisorClient::from_state_dir(&state_dir).expect("client");

        // Unauthenticated raw HTTP request fails with Unauthorized
        let base_url = handle.descriptor().base_url().expect("base_url");
        let raw_http = reqwest::Client::builder().no_proxy().build().expect("http");
        let unauth_resp = raw_http
            .post(format!("{base_url}/v1/host-control/info"))
            .json(&owner_scope_from_config(&control))
            .send()
            .await
            .expect("send unauth");
        assert_eq!(unauth_resp.status(), reqwest::StatusCode::UNAUTHORIZED);

        // Unknown field in JSON body is rejected with InvalidRequest
        let unknown_field_resp = raw_http
            .post(format!("{base_url}/v1/host-control/stop"))
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", handle.descriptor().token.as_str()),
            )
            .body(
                serde_json::to_string(&serde_json::json!({
                    "approval_id": APPROVAL_ID,
                    "subject_digest": SUBJECT_DIGEST,
                    "control_binding_id": "bnd_control_001",
                    "thread_uuid": THREAD_UUID,
                    "session_binding_id": "bnd_session_001",
                    "expected_instance_id": INSTANCE_ID,
                    "force": true
                }))
                .expect("json"),
            )
            .send()
            .await
            .expect("send unknown field");
        assert_eq!(
            unknown_field_resp.status(),
            reqwest::StatusCode::BAD_REQUEST
        );
        let api_err: ApiError = unknown_field_resp.json().await.expect("api_err");
        assert_eq!(api_err.code, ErrorCode::InvalidRequest);

        // Owner scope mismatches fail with HostScopeMismatch
        let mut wrong_approval = owner_scope_from_config(&control);
        wrong_approval.approval_id = "apr_99999999999999999999999999999999".to_string();
        match client
            .stop_idle_host(&wrong_approval)
            .await
            .expect_err("wrong approval")
        {
            Error::Remote(err) => assert_eq!(err.code, ErrorCode::HostScopeMismatch),
            other => panic!("unexpected: {other:?}"),
        }

        let mut wrong_thread = owner_scope_from_config(&control);
        wrong_thread.thread_uuid = OTHER_THREAD_UUID.to_string();
        match client
            .host_control_info(&wrong_thread)
            .await
            .expect_err("wrong thread")
        {
            Error::Remote(err) => assert_eq!(err.code, ErrorCode::HostScopeMismatch),
            other => panic!("unexpected: {other:?}"),
        }

        // Instance mismatch fails with HostInstanceMismatch
        let mut stale_scope = owner_scope_from_config(&control);
        stale_scope.expected_instance_id = STALE_INSTANCE_ID.to_string();
        match client
            .stop_idle_host(&stale_scope)
            .await
            .expect_err("stale instance")
        {
            Error::Remote(err) => assert_eq!(err.code, ErrorCode::HostInstanceMismatch),
            other => panic!("unexpected: {other:?}"),
        }

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn stop_idle_host_rejects_active_demo_or_pending_enabled_wakeup_and_latches_when_idle() {
        use threadlane_core::WakeupOutcome;

        let temp = tempfile::tempdir().expect("tempdir");
        let state_dir = temp.path().join("state");

        let bin_dir = temp.path().join("bin");
        std::fs::create_dir_all(&bin_dir).expect("bin_dir");
        let fake_codex = bin_dir.join("fake-codex");
        std::fs::write(&fake_codex, "#!/bin/sh\nexit 1\n").expect("write fake codex");
        std::fs::set_permissions(&fake_codex, std::fs::Permissions::from_mode(0o755))
            .expect("chmod");

        // Long tick interval so we can deterministically inspect Pending and InFlight wakeup states
        // without background notifier timer races.
        let mut sup_cfg = SupervisorConfig::new(&state_dir);
        sup_cfg.tick_interval = Duration::from_secs(30);

        // Bootstrap bindings
        let boot = Supervisor::start(SupervisorConfig::new(&state_dir))
            .await
            .expect("boot");
        let boot_client = SupervisorClient::from_state_dir(&state_dir).expect("boot client");
        let session_binding = boot_client
            .create_binding(THREAD_UUID, "session")
            .await
            .expect("create session binding");
        let aux_binding = boot_client
            .create_binding(OTHER_THREAD_UUID, "aux")
            .await
            .expect("create aux binding");
        boot.shutdown().await;

        let mut notify_bindings = BTreeSet::new();
        notify_bindings.insert(session_binding.binding_id.clone());
        // Include a nonexistent binding ID in notify_bindings to verify BindingNotFound is ignored
        // identically to the background notifier loop.
        notify_bindings.insert("bnd_nonexistent_notify_target".to_string());
        sup_cfg.codex = Some(CodexWakeupConfig {
            executable: fake_codex,
            notify_bindings,
            timeout: Duration::from_secs(5),
            approved_session: None,
        });

        let control = sample_host_control(&session_binding.binding_id);
        let scope = owner_scope_from_config(&control);
        let mut handle = Supervisor::start_with_host_control(sup_cfg, control.clone())
            .await
            .expect("start final");
        // Freeze this fixture's background worker/notifier effects so Pending/InFlight core
        // states are deterministic; live provider/sender behavior is covered by other tests.
        let worker_task = handle.worker_task.take().expect("background worker task");
        worker_task.abort();
        let _ = worker_task.await;
        let client = SupervisorClient::from_state_dir(&state_dir).expect("client");

        // Initially idle
        let info0 = client.host_control_info(&scope).await.expect("info0");
        assert_eq!(info0.state, HostControlRuntimeState::Ready);
        assert!(info0.idle);

        // 1. Start a demo run on aux_binding (not in notify_bindings) -> runtime-wide idle check
        //    must still count non_terminal_runs == 1 globally.
        let aux_scoped = client
            .clone()
            .scoped(aux_binding.binding_id.clone())
            .expect("aux scoped");
        let demo_run = aux_scoped
            .start_demo(
                "demo-aux-1".to_string(),
                "test demo".to_string(),
                Some(60_000),
            )
            .await
            .expect("start demo");

        let busy_err = client
            .stop_idle_host(&scope)
            .await
            .expect_err("should reject active demo run on aux binding");
        match busy_err {
            Error::Remote(err) => {
                assert_eq!(err.code, ErrorCode::HostNotIdle);
                let details = err.details.expect("busy details");
                assert_eq!(details["non_terminal_runs"], 1);
            }
            other => panic!("unexpected: {other:?}"),
        }

        // 2. Cancel the demo run on aux_binding -> non_terminal_runs becomes 0. Cancellation alone
        //    is not important, so seed a real QuestionAsked below; because aux_binding is NOT in
        //    notify_bindings, pending_enabled_wakeups must be 0 and the host must be idle!
        aux_scoped
            .cancel(
                "cancel-aux-1".to_string(),
                demo_run.run_id,
                Some("cleanup".to_string()),
            )
            .await
            .expect("cancel demo");

        // Cancellation alone leaves the wakeup Idle, so seed a real important QuestionAsked on
        // aux_binding through Core timing before inspecting Pending.
        {
            let mut access = handle
                .shared
                .acquire_for_request()
                .await
                .expect("acquire for aux pending seed");
            seed_pending_important_wakeup(
                access.core_mut(),
                &aux_binding.binding_id,
                "seed-aux-pending",
            );
        }

        let aux_status = client
            .status(&aux_binding.binding_id)
            .await
            .expect("aux status");
        assert_eq!(aux_status.wakeup.status, WakeupStatus::Pending);

        let info_idle = client.host_control_info(&scope).await.expect("info_idle");
        assert!(info_idle.idle);
        assert_eq!(info_idle.busy.pending_enabled_wakeups, 0);
        assert_eq!(info_idle.busy.in_flight_wakeups, 0);

        // 3. Deterministically create a Pending wakeup on the allowlisted session_binding under
        //    SharedState::active without waking the background notifier loop, and verify that
        //    pending_enabled_wakeups == 1 blocks stop_idle_host.
        {
            let mut access = handle
                .shared
                .acquire_for_request()
                .await
                .expect("acquire active");
            seed_pending_important_wakeup(
                access.core_mut(),
                &session_binding.binding_id,
                "sess-seed",
            );
            let busy_under_lock =
                inspect_host_busy_counts(&handle.shared, &mut access).expect("inspect busy");
            assert_eq!(busy_under_lock.non_terminal_runs, 0);
            assert_eq!(busy_under_lock.pending_enabled_wakeups, 1);
            assert!(!busy_under_lock.is_idle());
        }

        let info_pending = client
            .host_control_info(&scope)
            .await
            .expect("info_pending");
        assert!(!info_pending.idle);
        assert_eq!(info_pending.busy.pending_enabled_wakeups, 1);

        let pending_err = client
            .stop_idle_host(&scope)
            .await
            .expect_err("allowlisted Pending wakeup must reject stop_idle_host");
        match pending_err {
            Error::Remote(err) => {
                assert_eq!(err.code, ErrorCode::HostNotIdle);
                let details = err.details.expect("busy details");
                assert_eq!(details["pending_enabled_wakeups"], 1);
                assert_eq!(details["in_flight_wakeups"], 0);
            }
            other => panic!("unexpected: {other:?}"),
        }

        // 4. Claim both session_binding and aux_binding wakeups into InFlight under the active
        //    lock, proving in_flight_wakeups is counted globally across all bindings while
        //    pending_enabled_wakeups drops to 0.
        let (session_claim, aux_claim) = {
            let mut access = handle
                .shared
                .acquire_for_request()
                .await
                .expect("acquire active");
            let now = current_time_ms();
            let s_claim = access
                .core_mut()
                .claim_wakeup_for_binding(&session_binding.binding_id, now)
                .expect("claim session wakeup")
                .expect("session wakeup was Pending");
            let a_claim = access
                .core_mut()
                .claim_wakeup_for_binding(&aux_binding.binding_id, now)
                .expect("claim aux wakeup")
                .expect("aux wakeup was Pending");
            (s_claim, a_claim)
        };

        let inflight_err = client
            .stop_idle_host(&scope)
            .await
            .expect_err("InFlight wakeups must reject stop_idle_host");
        match inflight_err {
            Error::Remote(err) => {
                assert_eq!(err.code, ErrorCode::HostNotIdle);
                let details = err.details.expect("busy details");
                assert_eq!(details["pending_enabled_wakeups"], 0);
                assert_eq!(details["in_flight_wakeups"], 2);
            }
            other => panic!("unexpected: {other:?}"),
        }

        // Resolve both InFlight wakeup claims to Submitted. The later aux cancellation adds no
        // important event, so only terminal rows remain when stop_idle_host is attempted below.
        {
            let mut access = handle
                .shared
                .acquire_for_request()
                .await
                .expect("acquire active");
            let now = current_time_ms();
            access
                .core_mut()
                .record_wakeup_result(&session_claim.claim_id, WakeupOutcome::Submitted, now)
                .expect("resolve session claim");
            access
                .core_mut()
                .record_wakeup_result(&aux_claim.claim_id, WakeupOutcome::Submitted, now)
                .expect("resolve aux claim");
        }
        let aux_demo_2 = aux_scoped
            .start_demo(
                "demo-aux-2".to_string(),
                "second aux demo".to_string(),
                Some(60_000),
            )
            .await
            .expect("start aux demo 2");
        aux_scoped
            .cancel(
                "cancel-aux-2".to_string(),
                aux_demo_2.run_id,
                Some("cleanup 2".to_string()),
            )
            .await
            .expect("cancel aux demo 2");

        // 5. Now stop_idle_host succeeds and latches Stopping!
        let stop_resp = client.stop_idle_host(&scope).await.expect("stop_idle_host");
        assert_eq!(stop_resp.state, HostControlRuntimeState::Stopping);
        assert!(stop_resp.idle);

        // wait_for_stop_request(&self) resolves immediately
        tokio::time::timeout(Duration::from_millis(200), handle.wait_for_stop_request())
            .await
            .expect("wait_for_stop_request should resolve");

        // Duplicate stop_idle_host for the same instance is idempotent
        let dup_stop = client
            .stop_idle_host(&scope)
            .await
            .expect("duplicate stop_idle_host");
        assert_eq!(dup_stop.state, HostControlRuntimeState::Stopping);

        // Read-only queries still succeed while Stopping before owner calls shutdown
        let bindings = client
            .list_bindings()
            .await
            .expect("list_bindings while stopping");
        assert_eq!(bindings.len(), 2);
        let status = client
            .status(&session_binding.binding_id)
            .await
            .expect("status while stopping");
        assert_eq!(status.binding.binding_id, session_binding.binding_id);

        // All mutating endpoints are rejected with HostStopping before shutdown
        let create_err = client
            .create_binding(THREAD_UUID, "blocked")
            .await
            .expect_err("create_binding blocked");
        match create_err {
            Error::Remote(err) => assert_eq!(err.code, ErrorCode::HostStopping),
            other => panic!("unexpected: {other:?}"),
        }

        let start_demo_err = aux_scoped
            .start_demo("demo-blocked".to_string(), "blocked".to_string(), None)
            .await
            .expect_err("start_demo blocked");
        match start_demo_err {
            Error::Remote(err) => assert_eq!(err.code, ErrorCode::HostStopping),
            other => panic!("unexpected: {other:?}"),
        }

        let pull_err = aux_scoped
            .pull_events(None, Some(10))
            .await
            .expect_err("pull_events blocked");
        match pull_err {
            Error::Remote(err) => assert_eq!(err.code, ErrorCode::HostStopping),
            other => panic!("unexpected: {other:?}"),
        }

        let fatal = handle.shutdown_and_take_fatal().await;
        assert!(fatal.is_none());
    }

    #[tokio::test]
    async fn stop_idle_host_with_disabled_sender_ignores_pending_wakeup_and_blocks_nonterminal_workers_and_live_tasks(
    ) {
        use threadlane_core::{
            WorkerAcceptanceState, WorkerCompletionRecord, WorkerOutcomeSummary,
        };

        let temp = tempfile::tempdir().expect("tempdir");
        let state_dir = temp.path().join("state");

        // Start supervisor with Codex sender disabled (codex: None) and workers dispatcher disabled.
        let mut sup_cfg = SupervisorConfig::new(&state_dir);
        sup_cfg.tick_interval = Duration::from_secs(30);

        let boot = Supervisor::start(SupervisorConfig::new(&state_dir))
            .await
            .expect("boot");
        let boot_client = SupervisorClient::from_state_dir(&state_dir).expect("boot client");
        let session_binding = boot_client
            .create_binding(THREAD_UUID, "session")
            .await
            .expect("create session binding");
        boot.shutdown().await;

        let control = sample_host_control(&session_binding.binding_id);
        let scope = owner_scope_from_config(&control);
        let handle = Supervisor::start_with_host_control(sup_cfg, control.clone())
            .await
            .expect("start with host control");
        let client = SupervisorClient::from_state_dir(&state_dir).expect("client");
        let scoped = client
            .clone()
            .scoped(session_binding.binding_id.clone())
            .expect("scoped");

        // 1. With sender disabled (codex: None), a Pending wakeup on session_binding itself
        //    produces pending_enabled_wakeups == 0 and does not block idle stop.
        let demo = scoped
            .start_demo(
                "demo-disabled-sender".to_string(),
                "demo".to_string(),
                Some(60_000),
            )
            .await
            .expect("start demo");
        scoped
            .cancel(
                "cancel-disabled-sender".to_string(),
                demo.run_id,
                Some("cancel".to_string()),
            )
            .await
            .expect("cancel demo");

        // Cancellation alone leaves the wakeup Idle, so seed a real important QuestionAsked on
        // session_binding through Core timing before asserting the disabled sender ignores Pending.
        {
            let mut access = handle
                .shared
                .acquire_for_request()
                .await
                .expect("acquire for disabled-sender pending seed");
            seed_pending_important_wakeup(
                access.core_mut(),
                &session_binding.binding_id,
                "seed-disabled-sender",
            );
        }

        let st_pending = client
            .status(&session_binding.binding_id)
            .await
            .expect("status");
        assert_eq!(st_pending.wakeup.status, WakeupStatus::Pending);

        let info_disabled = client
            .host_control_info(&scope)
            .await
            .expect("info with disabled sender");
        assert!(info_disabled.idle);
        assert_eq!(info_disabled.busy.pending_enabled_wakeups, 0);

        // Pull and acknowledge the batch (EventBatch.batch_id is String) to return wakeup to Idle.
        let batch = scoped
            .pull_events(None, Some(10))
            .await
            .expect("pull_events");
        let batch_id: String = batch.batch_id;
        scoped
            .ack_events("ack-disabled-sender".to_string(), batch_id)
            .await
            .expect("ack_events");

        // 2. Non-terminal worker rows in Queued, Claimed, and CancelRequested states must each
        //    prevent stop_idle_host even when no background worker task is running.
        let profile_digest = "a".repeat(64);
        let base_commit = "0123456789abcdef0123456789abcdef01234567".to_string();

        let queued_worker = {
            let mut access = handle
                .shared
                .acquire_for_request()
                .await
                .expect("acquire active");
            access
                .core_mut()
                .execute(
                    &session_binding.binding_id,
                    Command::StartWorker {
                        command_key: "wk-nonterminal-1".to_string(),
                        profile_id: "prof_test".to_string(),
                        profile_digest: profile_digest.clone(),
                        provider: "antigravity".to_string(),
                        requested_model: "gemini-4-argon-high".to_string(),
                        requested_effort: "high".to_string(),
                        base_commit,
                        instruction: "verify non-terminal worker states block idle stop"
                            .to_string(),
                        execution_binding: None,
                        now_ms: current_time_ms(),
                    },
                )
                .expect("start_worker in core")
                .into_worker()
                .expect("worker snapshot")
        };
        assert_eq!(queued_worker.status, WorkerStatus::Queued);

        match client
            .stop_idle_host(&scope)
            .await
            .expect_err("Queued worker must block stop_idle_host")
        {
            Error::Remote(err) => {
                assert_eq!(err.code, ErrorCode::HostNotIdle);
                let details = err.details.expect("details");
                assert_eq!(details["non_terminal_runs"], 1);
                assert_eq!(details["non_terminal_workers"], 1);
                assert_eq!(details["live_worker_tasks"], 0);
            }
            other => panic!("unexpected: {other:?}"),
        }

        // Transition Queued -> Claimed via 5-argument Core::claim_worker
        let claim = {
            let mut access = handle
                .shared
                .acquire_for_request()
                .await
                .expect("acquire active");
            access
                .core_mut()
                .claim_worker(
                    &session_binding.binding_id,
                    &queued_worker.run_id,
                    queued_worker.revision,
                    &profile_digest,
                    current_time_ms(),
                )
                .expect("claim_worker")
        };

        match client
            .stop_idle_host(&scope)
            .await
            .expect_err("Claimed worker must block stop_idle_host")
        {
            Error::Remote(err) => {
                assert_eq!(err.code, ErrorCode::HostNotIdle);
                let details = err.details.expect("details");
                assert_eq!(details["non_terminal_runs"], 1);
                assert_eq!(details["non_terminal_workers"], 1);
            }
            other => panic!("unexpected: {other:?}"),
        }

        // Transition Claimed -> CancelRequested using current revision
        let cancel_requested = {
            let mut access = handle
                .shared
                .acquire_for_request()
                .await
                .expect("acquire active");
            access
                .core_mut()
                .execute(
                    &session_binding.binding_id,
                    Command::CancelWorker {
                        command_key: "cancel-wk-nonterminal-1".to_string(),
                        run_id: claim.run_id.clone(),
                        expected_revision: claim.revision,
                        reason: Some("request cancel before outcome".to_string()),
                        now_ms: current_time_ms(),
                    },
                )
                .expect("cancel_worker")
                .into_worker()
                .expect("worker snapshot")
        };
        assert_eq!(cancel_requested.status, WorkerStatus::CancelRequested);

        match client
            .stop_idle_host(&scope)
            .await
            .expect_err("CancelRequested worker must block stop_idle_host")
        {
            Error::Remote(err) => {
                assert_eq!(err.code, ErrorCode::HostNotIdle);
                let details = err.details.expect("details");
                assert_eq!(details["non_terminal_runs"], 1);
                assert_eq!(details["non_terminal_workers"], 1);
            }
            other => panic!("unexpected: {other:?}"),
        }

        // Resolve worker to terminal Cancelled in Core
        {
            let mut access = handle
                .shared
                .acquire_for_request()
                .await
                .expect("acquire active");
            let terminal = access
                .core_mut()
                .record_worker_outcome(
                    &claim,
                    WorkerCompletionRecord {
                        status: WorkerStatus::Cancelled,
                        observed_model: None,
                        observed_effort: None,
                        head_commit: None,
                        dropped_live_events: 0,
                        outcome: WorkerOutcomeSummary {
                            provider_retry_high_water: None,
                            provider_health: None,
                            exit_code: None,
                            signal: None,
                            duration_ms: Some(5),
                            timed_out: false,
                            cancelled: true,
                            cancel_requested_before_completion: true,
                            missing_model_evidence: false,
                            missing_effort_evidence: false,
                            provider_error: false,
                            incomplete_cleanup: false,
                            rejected_by_diff_gate: false,
                            stdout_truncated: false,
                            stderr_truncated: false,
                            acceptance_state: WorkerAcceptanceState::ExecutionFailed,
                            changed_paths_count: 0,
                            changed_paths_preview: Vec::new(),
                            violation_count: 0,
                            violation_summaries: Vec::new(),
                            final_summary: None,
                            failure_reason: Some("cancelled in test".to_string()),
                            report_artifact_id: None,
                        },
                    },
                    current_time_ms(),
                )
                .expect("record_worker_outcome");
            assert_eq!(terminal.status, WorkerStatus::Cancelled);
        }

        // 3. Even when all persisted SQLite rows are terminal, a registered entry in
        //    SharedState::active_workers (live_worker_tasks == 1) must prevent stop_idle_host.
        let (cancel_tx, _cancel_rx) = watch::channel(false);
        let (_process_tx, presence) = new_process_presence_channel();
        handle.shared.register_active_worker(
            claim.clone(),
            cancel_tx,
            Arc::new(AtomicBool::new(false)),
            presence,
        );

        match client
            .stop_idle_host(&scope)
            .await
            .expect_err("registered active_workers entry must block stop_idle_host")
        {
            Error::Remote(err) => {
                assert_eq!(err.code, ErrorCode::HostNotIdle);
                let details = err.details.expect("details");
                assert_eq!(details["non_terminal_runs"], 0);
                assert_eq!(details["non_terminal_workers"], 0);
                assert_eq!(details["live_worker_tasks"], 1);
            }
            other => panic!("unexpected: {other:?}"),
        }

        let replacement = WorkerClaim {
            claim_id: "wclm_99999999-9999-4999-8999-999999999999".to_string(),
            ..claim.clone()
        };
        let (_replacement_process_tx, replacement_presence) = new_process_presence_channel();
        let (replacement_cancel_tx, _replacement_cancel_rx) = watch::channel(false);
        handle.shared.register_active_worker(
            replacement.clone(),
            replacement_cancel_tx,
            Arc::new(AtomicBool::new(false)),
            replacement_presence,
        );
        drop(ActiveWorkerGuard {
            state: Arc::clone(&handle.shared),
            claim: claim.clone(),
        });
        {
            let map = handle
                .shared
                .active_workers
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            assert_eq!(
                map.get(&claim.run_id)
                    .map(|entry| entry.claim.claim_id.as_str()),
                Some(replacement.claim_id.as_str()),
                "an old guard must not remove its replacement claim"
            );
        }
        handle.shared.unregister_active_worker(&replacement);

        let stopped = client
            .stop_idle_host(&scope)
            .await
            .expect("stop_idle_host succeeds once active_workers entry is removed");
        assert_eq!(stopped.state, HostControlRuntimeState::Stopping);
        assert!(stopped.idle);

        let fatal = handle.shutdown_and_take_fatal().await;
        assert!(fatal.is_none());
    }

    #[tokio::test]
    async fn concurrent_start_demo_and_stop_idle_host_admits_exclusively_one_winner() {
        for round in 0..4u32 {
            let temp = tempfile::tempdir().expect("tempdir");
            let state_dir = temp.path().join("state");

            let boot = Supervisor::start(SupervisorConfig::new(&state_dir))
                .await
                .expect("boot");
            let boot_client = SupervisorClient::from_state_dir(&state_dir).expect("boot client");
            let session_binding = boot_client
                .create_binding(THREAD_UUID, "session")
                .await
                .expect("create session binding");
            boot.shutdown().await;

            let mut sup_cfg = SupervisorConfig::new(&state_dir);
            sup_cfg.tick_interval = Duration::from_secs(30);
            let control = sample_host_control(&session_binding.binding_id);
            let scope = owner_scope_from_config(&control);
            let handle = Supervisor::start_with_host_control(sup_cfg, control)
                .await
                .expect("start with host control");

            let client_stop = SupervisorClient::from_state_dir(&state_dir).expect("client stop");
            let scoped_start = SupervisorClient::from_state_dir(&state_dir)
                .expect("client start")
                .scoped(session_binding.binding_id.clone())
                .expect("scoped start");

            // Hold SharedState::active briefly so both HTTP requests arrive and contend on the
            // active mutex simultaneously, then release it.
            let gate = handle.shared.active.lock().await;
            let scope_clone = scope.clone();
            let start_task = tokio::spawn(async move {
                scoped_start
                    .start_demo(
                        format!("race-demo-{round}"),
                        "race instruction".to_string(),
                        Some(60_000),
                    )
                    .await
            });
            let stop_task =
                tokio::spawn(async move { client_stop.stop_idle_host(&scope_clone).await });

            tokio::time::sleep(Duration::from_millis(15)).await;
            drop(gate);

            let start_res = start_task.await.expect("join start_task");
            let stop_res = stop_task.await.expect("join stop_task");

            match (start_res, stop_res) {
                (Ok(run), Err(Error::Remote(stop_err))) => {
                    // StartDemo won the mutex race: stop_idle_host MUST fail with HostNotIdle,
                    // and the supervisor must remain in Ready state until the run is cancelled.
                    assert_eq!(stop_err.code, ErrorCode::HostNotIdle);
                    let info = SupervisorClient::from_state_dir(&state_dir)
                        .expect("client")
                        .host_control_info(&scope)
                        .await
                        .expect("info after start won");
                    assert_eq!(info.state, HostControlRuntimeState::Ready);
                    assert!(!info.idle);
                    assert_eq!(info.busy.non_terminal_runs, 1);

                    let cleanup_scoped = SupervisorClient::from_state_dir(&state_dir)
                        .expect("client")
                        .scoped(session_binding.binding_id.clone())
                        .expect("scoped");
                    cleanup_scoped
                        .cancel(
                            format!("race-cancel-{round}"),
                            run.run_id,
                            Some("cleanup after race".to_string()),
                        )
                        .await
                        .expect("cancel winning demo");

                    let latched = SupervisorClient::from_state_dir(&state_dir)
                        .expect("client")
                        .stop_idle_host(&scope)
                        .await
                        .expect("stop after cancel");
                    assert_eq!(latched.state, HostControlRuntimeState::Stopping);
                }
                (Err(Error::Remote(start_err)), Ok(stop_status)) => {
                    // stop_idle_host won the mutex race: StartDemo MUST fail with HostStopping,
                    // and zero active runs may exist.
                    assert_eq!(stop_status.state, HostControlRuntimeState::Stopping);
                    assert!(stop_status.idle);
                    assert_eq!(start_err.code, ErrorCode::HostStopping);
                }
                other => panic!(
                    "exclusively one of StartDemo or stop_idle_host must win; got: {other:?}"
                ),
            }

            // Post-latch verification: read-only status succeeds while new mutations are fenced.
            let verify_client = SupervisorClient::from_state_dir(&state_dir).expect("client");
            let status_after = verify_client
                .status(&session_binding.binding_id)
                .await
                .expect("read-only status allowed after Stopping latch");
            assert_eq!(status_after.active_runs, 0);

            let fenced_err = verify_client
                .scoped(session_binding.binding_id.clone())
                .expect("scoped")
                .start_demo(format!("post-latch-{round}"), "fenced".to_string(), None)
                .await
                .expect_err("post-latch start_demo must be fenced");
            match fenced_err {
                Error::Remote(err) => assert_eq!(err.code, ErrorCode::HostStopping),
                other => panic!("unexpected post-latch error: {other:?}"),
            }

            let fatal = handle.shutdown_and_take_fatal().await;
            assert!(fatal.is_none());
        }
    }

    #[tokio::test]
    async fn rearm_host_wakeup_requires_live_codex_config_and_allowlisted_owner_session_binding() {
        let temp = tempfile::tempdir().expect("tempdir");
        let state_dir = temp.path().join("state");
        let bin_dir = temp.path().join("bin");
        std::fs::create_dir_all(&bin_dir).expect("create bin dir");
        let fake_codex = bin_dir.join("fake-codex-rearm");
        std::fs::write(&fake_codex, "#!/bin/sh\nexit 0\n").expect("write fake codex");
        std::fs::set_permissions(&fake_codex, std::fs::Permissions::from_mode(0o755))
            .expect("chmod fake codex");

        let boot = Supervisor::start(SupervisorConfig::new(&state_dir))
            .await
            .expect("boot");
        let boot_client = SupervisorClient::from_state_dir(&state_dir).expect("boot client");
        let session_binding = boot_client
            .create_binding(THREAD_UUID, "session")
            .await
            .expect("create session binding");
        let aux_binding = boot_client
            .create_binding(OTHER_THREAD_UUID, "aux")
            .await
            .expect("create aux binding");
        boot.shutdown().await;

        let control = sample_host_control(&session_binding.binding_id);
        let scope = owner_scope_from_config(&control);

        // 1. Start supervisor with `codex: None` and seed a Failed wakeup claim on `session_binding`.
        let mut cfg_disabled = SupervisorConfig::new(&state_dir);
        cfg_disabled.tick_interval = Duration::from_secs(30);
        let handle_disabled = Supervisor::start_with_host_control(cfg_disabled, control.clone())
            .await
            .expect("start disabled codex supervisor");
        let failed_claim = {
            let mut access = handle_disabled
                .shared
                .acquire_for_request()
                .await
                .expect("acquire active");
            seed_pending_important_wakeup(
                access.core_mut(),
                &session_binding.binding_id,
                "seed-failed-rearm-gate",
            );
            let now = current_time_ms();
            let claim = access
                .core_mut()
                .claim_wakeup_for_binding(&session_binding.binding_id, now)
                .expect("claim wakeup")
                .expect("pending wakeup present");
            access
                .core_mut()
                .record_wakeup_result(
                    &claim.claim_id,
                    threadlane_core::WakeupOutcome::Failed {
                        error: "pre-send failure before stop/restart".to_string(),
                    },
                    now,
                )
                .expect("record Failed wakeup");
            claim
        };

        let client_disabled =
            SupervisorClient::from_state_dir(&state_dir).expect("client disabled");
        let disabled_err = client_disabled
            .rearm_host_wakeup(&RearmHostWakeupRequest {
                scope: scope.clone(),
                expected_claim_id: failed_claim.claim_id.clone(),
            })
            .await
            .expect_err("rearm_host_wakeup must fail when codex config is None");
        match disabled_err {
            Error::Remote(err) => assert_eq!(err.code, ErrorCode::InvalidConfig),
            other => panic!("unexpected error when codex is None: {other:?}"),
        }

        let st_after_disabled = client_disabled
            .status(&session_binding.binding_id)
            .await
            .expect("status after disabled refusal");
        assert_eq!(st_after_disabled.wakeup.status, WakeupStatus::Failed);
        assert_eq!(
            st_after_disabled.wakeup.claim_id.as_deref(),
            Some(failed_claim.claim_id.as_str())
        );
        handle_disabled.shutdown().await;

        // 2. Start supervisor with `codex: Some(...)` where `notify_bindings` only contains
        //    `aux_binding` (not `control.session_binding_id`). Rearm must still be refused before
        //    mutating Core.
        let mut cfg_unlisted = SupervisorConfig::new(&state_dir);
        cfg_unlisted.tick_interval = Duration::from_secs(30);
        let mut aux_only_bindings = BTreeSet::new();
        aux_only_bindings.insert(aux_binding.binding_id.clone());
        cfg_unlisted.codex = Some(CodexWakeupConfig {
            executable: fake_codex.clone(),
            notify_bindings: aux_only_bindings,
            timeout: Duration::from_secs(5),
            approved_session: None,
        });
        let handle_unlisted = Supervisor::start_with_host_control(cfg_unlisted, control.clone())
            .await
            .expect("start unlisted binding supervisor");
        let client_unlisted =
            SupervisorClient::from_state_dir(&state_dir).expect("client unlisted");
        let unlisted_err = client_unlisted
            .rearm_host_wakeup(&RearmHostWakeupRequest {
                scope: scope.clone(),
                expected_claim_id: failed_claim.claim_id.clone(),
            })
            .await
            .expect_err(
                "rearm_host_wakeup must fail when session_binding_id is not in notify_bindings",
            );
        match unlisted_err {
            Error::Remote(err) => assert_eq!(err.code, ErrorCode::InvalidConfig),
            other => panic!("unexpected error when binding not allowlisted: {other:?}"),
        }

        let st_after_unlisted = client_unlisted
            .status(&session_binding.binding_id)
            .await
            .expect("status after unlisted refusal");
        assert_eq!(st_after_unlisted.wakeup.status, WakeupStatus::Failed);
        assert_eq!(
            st_after_unlisted.wakeup.claim_id.as_deref(),
            Some(failed_claim.claim_id.as_str())
        );
        handle_unlisted.shutdown().await;

        // 3. Start supervisor with `session_binding_id` allowlisted in `notify_bindings` (with
        //    background worker paused to inspect the immediate post-rearm state). Rearm succeeds.
        let mut cfg_allowed = SupervisorConfig::new(&state_dir);
        cfg_allowed.tick_interval = Duration::from_secs(30);
        let mut allowed_bindings = BTreeSet::new();
        allowed_bindings.insert(session_binding.binding_id.clone());
        cfg_allowed.codex = Some(CodexWakeupConfig {
            executable: fake_codex,
            notify_bindings: allowed_bindings,
            timeout: Duration::from_secs(5),
            approved_session: None,
        });
        let mut handle_allowed = Supervisor::start_with_host_control(cfg_allowed, control)
            .await
            .expect("start allowed binding supervisor");
        let worker_task = handle_allowed.worker_task.take().expect("worker task");
        worker_task.abort();
        let _ = worker_task.await;

        let client_allowed = SupervisorClient::from_state_dir(&state_dir).expect("client allowed");
        let rearmed = client_allowed
            .rearm_host_wakeup(&RearmHostWakeupRequest {
                scope,
                expected_claim_id: failed_claim.claim_id,
            })
            .await
            .expect("rearm_host_wakeup succeeds when session_binding_id is allowlisted");
        assert_eq!(rearmed.wakeup.status, WakeupStatus::Pending);
        assert!(rearmed.wakeup.claim_id.is_none());

        let fatal = handle_allowed.shutdown_and_take_fatal().await;
        assert!(fatal.is_none());
    }
    #[cfg(target_os = "linux")]
    struct PresenceTestChild {
        child: Option<std::process::Child>,
    }

    #[cfg(target_os = "linux")]
    impl PresenceTestChild {
        fn spawn() -> Self {
            use std::os::unix::process::CommandExt;

            let mut command = std::process::Command::new("sleep");
            command.arg("30").process_group(0);
            Self {
                child: Some(command.spawn().expect("spawn presence regression child")),
            }
        }

        fn pid(&self) -> u32 {
            self.child.as_ref().expect("live test child").id()
        }

        fn terminate(&mut self) {
            if let Some(mut child) = self.child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    #[cfg(target_os = "linux")]
    impl Drop for PresenceTestChild {
        fn drop(&mut self) {
            self.terminate();
        }
    }

    #[cfg(target_os = "linux")]
    fn presence_test_observation(child_pid: u32) -> OwnedProcessSnapshot {
        use threadlane_workers::{OwnedProcessOrigin, OwnedProcessState};

        let stat = std::fs::read_to_string(format!("/proc/{child_pid}/stat"))
            .expect("read presence regression child stat");
        let command_end = stat.rfind(')').expect("command terminator");
        let fields: Vec<&str> = stat[command_end + 1..].split_whitespace().collect();
        let process_group_id = fields[2]
            .parse::<i32>()
            .expect("presence regression child process group");
        let start_ticks = fields[19]
            .parse::<u64>()
            .expect("presence regression child start ticks");
        OwnedProcessSnapshot {
            schema_version: 1,
            origin: OwnedProcessOrigin::RunnerOwnedChild,
            invocation_id: "00000000-0000-4000-8000-000000000001".to_string(),
            state: OwnedProcessState::Running,
            unknown_reason: None,
            sequence: 1,
            elapsed_ms: 1_000,
            heartbeat_interval_ms: 1_000,
            child_pid: Some(child_pid),
            process_group_id: Some(process_group_id),
            linux_process_start_ticks: Some(start_ticks),
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "current_thread")]
    async fn presence_never_carries_physical_proof_across_a_core_wait() {
        let temp = tempfile::tempdir().expect("private root");
        let state_dir = temp.path().join("state");
        let mut config = SupervisorConfig::new(&state_dir);
        config.tick_interval = Duration::from_secs(30);
        let mut handle = Supervisor::start(config).await.expect("start");
        // Isolate the presence await boundary from the ordinary background dispatcher.
        let worker_task = handle.worker_task.take().expect("worker task");
        worker_task.abort();
        let _ = worker_task.await;
        let client = SupervisorClient::from_state_dir(&state_dir).expect("client");
        let binding = client
            .create_binding(THREAD_UUID, "presence await regression")
            .await
            .expect("binding");
        let digest = "a".repeat(64);
        let (worker, claim) = {
            let mut access = handle.shared.acquire_for_request().await.expect("core");
            let worker = access
                .core_mut()
                .execute(
                    &binding.binding_id,
                    Command::StartWorker {
                        command_key: "presence-await-core".to_string(),
                        profile_id: "prof_test".to_string(),
                        profile_digest: digest.clone(),
                        provider: "antigravity".to_string(),
                        requested_model: "gemini-4-argon-high".to_string(),
                        requested_effort: "high".to_string(),
                        base_commit: "1".repeat(40),
                        instruction: "presence await regression".to_string(),
                        execution_binding: None,
                        now_ms: current_time_ms(),
                    },
                )
                .expect("queue worker")
                .into_worker()
                .expect("queued worker");
            let claim = access
                .core_mut()
                .claim_worker(
                    &binding.binding_id,
                    &worker.run_id,
                    worker.revision,
                    &digest,
                    current_time_ms(),
                )
                .expect("claim worker");
            let current = access
                .core_mut()
                .get_worker(&binding.binding_id, &worker.run_id)
                .expect("current worker");
            (current, claim)
        };

        let mut child = PresenceTestChild::spawn();
        let (process_tx, presence) = new_process_presence_channel();
        let (cancel_tx, _cancel_rx) = watch::channel(false);
        handle.shared.register_active_worker(
            claim.clone(),
            cancel_tx,
            Arc::new(AtomicBool::new(false)),
            Arc::clone(&presence),
        );
        let observer = tokio::spawn(Arc::clone(&presence).watch_changes());
        let observation = presence_test_observation(child.pid());
        assert!(
            observation.is_valid(),
            "presence regression child observation must be valid"
        );
        process_tx
            .send(Some(observation))
            .expect("send running observation");

        let live = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let current = handle
                    .shared
                    .resolve_worker_presence(&binding.binding_id, worker.clone())
                    .await
                    .expect("presence while child is running");
                if current.live {
                    break current;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("running silent child must become live");
        assert_eq!(live.claim_id.as_deref(), Some(claim.claim_id.as_str()));

        // A shutdown latch set while the presence request waits for Core must be applied after the
        // wait rather than relying on flags captured beside the earlier physical proof.
        let gate = handle.shared.active.lock().await;
        let request_started = Arc::new(AtomicBool::new(false));
        let shared = Arc::clone(&handle.shared);
        let binding_id = binding.binding_id.clone();
        let request_worker = worker.clone();
        let started = Arc::clone(&request_started);
        let pending = tokio::spawn(async move {
            started.store(true, Ordering::SeqCst);
            shared
                .resolve_worker_presence(&binding_id, request_worker)
                .await
        });
        while !request_started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        handle.shared.stopping.store(true, Ordering::SeqCst);
        drop(gate);
        let stopping = tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .expect("stopping presence request bound")
            .expect("stopping presence task")
            .expect("stopping presence result");
        assert!(!stopping.live);
        assert_eq!(
            stopping.unavailable_reason,
            Some(WorkerPresenceUnavailableReason::SupervisorStopping)
        );
        handle.shared.stopping.store(false, Ordering::SeqCst);

        // Ending the actual child only while the request waits for Core must not leave a stale
        // successful `/proc` result that is applied to the post-wait sample.
        let gate = handle.shared.active.lock().await;
        let request_started = Arc::new(AtomicBool::new(false));
        let shared = Arc::clone(&handle.shared);
        let binding_id = binding.binding_id.clone();
        let request_worker = worker.clone();
        let started = Arc::clone(&request_started);
        let pending = tokio::spawn(async move {
            started.store(true, Ordering::SeqCst);
            shared
                .resolve_worker_presence(&binding_id, request_worker)
                .await
        });
        while !request_started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        child.terminate();
        drop(gate);
        let after_exit = tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .expect("exited-child presence request bound")
            .expect("exited-child presence task")
            .expect("exited-child presence result");
        assert!(
            !after_exit.live,
            "an exited child must never be reported live"
        );
        assert_ne!(
            after_exit.availability,
            crate::presence::WorkerPresenceAvailability::Live
        );
        assert!(matches!(
            after_exit.unavailable_reason,
            Some(
                WorkerPresenceUnavailableReason::ProcessMissing
                    | WorkerPresenceUnavailableReason::ProcessExited
                    | WorkerPresenceUnavailableReason::ProcessZombie
                    | WorkerPresenceUnavailableReason::ProcessIdentityMismatch
                    | WorkerPresenceUnavailableReason::ProcessGroupMismatch
            )
        ));
        assert_eq!(
            after_exit.claim_id.as_deref(),
            Some(claim.claim_id.as_str())
        );

        drop(process_tx);
        observer.await.expect("presence observer task");
        handle.shared.unregister_active_worker(&claim);
        handle.shutdown().await;
    }
    #[tokio::test]
    async fn get_worker_reads_never_tick_due_demo_or_other_binding_state() {
        let temp = tempfile::tempdir().expect("private root");
        let state_dir = temp.path().join("state");
        let mut config = SupervisorConfig::new(&state_dir);
        // Pause the real background tick so a stray pre-read tick is unmistakable.
        config.tick_interval = Duration::from_secs(30);
        let mut handle = Supervisor::start(config).await.expect("start");
        let worker_task = handle.worker_task.take().expect("background worker task");
        worker_task.abort();
        let _ = worker_task.await;

        let client = SupervisorClient::from_state_dir(&state_dir).expect("client");
        let due_binding = client
            .create_binding(THREAD_UUID, "due demo read isolation")
            .await
            .expect("due binding");
        let other_binding = client
            .create_binding(OTHER_THREAD_UUID, "worker read isolation")
            .await
            .expect("other binding");
        let due_scoped = client
            .clone()
            .scoped(&due_binding.binding_id)
            .expect("due scoped client");
        let other_scoped = client
            .scoped(&other_binding.binding_id)
            .expect("other scoped client");

        let due_run = due_scoped
            .start_demo(
                "read-isolation-due-demo".to_string(),
                "due demo state must survive worker reads".to_string(),
                Some(0),
            )
            .await
            .expect("start due demo");

        let digest = "a".repeat(64);
        let worker = {
            let mut access = handle.shared.acquire_for_request().await.expect("core");
            access
                .core_mut()
                .execute(
                    &due_binding.binding_id,
                    Command::StartWorker {
                        command_key: "read-isolation-worker".to_string(),
                        profile_id: "prof_read_isolation".to_string(),
                        profile_digest: digest,
                        provider: "antigravity".to_string(),
                        requested_model: "gemini-4-argon-high".to_string(),
                        requested_effort: "high".to_string(),
                        base_commit: "1".repeat(40),
                        instruction: "owned worker read fixture".to_string(),
                        execution_binding: None,
                        now_ms: current_time_ms(),
                    },
                )
                .expect("queue owned worker")
                .into_worker()
                .expect("owned worker snapshot")
        };

        let (run_before, questions_before, activity_before, wakeup_before) = {
            let mut access = handle.shared.acquire_for_request().await.expect("core");
            (
                access
                    .core_mut()
                    .get_run(&due_binding.binding_id, &due_run.run_id)
                    .expect("due run before reads"),
                access
                    .core_mut()
                    .list_run_questions(&due_binding.binding_id, &due_run.run_id, None, 50)
                    .expect("due questions before reads"),
                access
                    .core_mut()
                    .list_activity(&due_binding.binding_id, None, None, 50, None, None)
                    .expect("due activity before reads"),
                access
                    .core_mut()
                    .get_wakeup(&due_binding.binding_id)
                    .expect("due wakeup before reads"),
            )
        };
        let (other_activity_before, other_wakeup_before) = {
            let mut access = handle.shared.acquire_for_request().await.expect("core");
            (
                access
                    .core_mut()
                    .list_activity(&other_binding.binding_id, None, None, 50, None, None)
                    .expect("other activity before reads"),
                access
                    .core_mut()
                    .get_wakeup(&other_binding.binding_id)
                    .expect("other wakeup before reads"),
            )
        };
        assert_eq!(run_before.status, threadlane_core::RunStatus::Running);
        assert_eq!(run_before.revision, due_run.revision);
        let due_at_ms = run_before
            .next_step_at_ms
            .expect("due demo must schedule a next step");
        assert!(
            due_at_ms <= current_time_ms(),
            "fixture must be due before the read-only requests"
        );

        // A successful owned-worker read must stay read-only even with another due demo.
        let read_worker = due_scoped
            .get_worker(worker.run_id.clone())
            .await
            .expect("owned worker read");
        assert_eq!(read_worker.run_id, worker.run_id);
        assert_eq!(read_worker.worker_id, worker.worker_id);
        assert_eq!(read_worker.binding_id, due_binding.binding_id);
        assert_eq!(read_worker.revision, worker.revision);
        assert_eq!(read_worker.status, WorkerStatus::Queued);

        let presence = due_scoped
            .get_worker_presence(worker.run_id.clone())
            .await
            .expect("owned worker presence read");
        assert_eq!(presence.run_id, worker.run_id);
        assert_eq!(presence.binding_id, due_binding.binding_id);
        assert!(!presence.live);

        // A missing or foreign worker read from another binding must not tick the due demo in
        // the first binding, and strict scope must still return a normal WorkerNotFound.
        let missing_run_id = "run_00000000-0000-4000-8000-000000000000".to_string();
        let missing = other_scoped
            .get_worker(missing_run_id)
            .await
            .expect_err("missing worker read must fail");
        assert_eq!(missing.to_api_error().code, ErrorCode::WorkerNotFound);

        let foreign = other_scoped
            .get_worker(worker.run_id.clone())
            .await
            .expect_err("foreign worker read must fail");
        assert_eq!(foreign.to_api_error().code, ErrorCode::WorkerNotFound);

        let foreign_presence = other_scoped
            .get_worker_presence(worker.run_id.clone())
            .await
            .expect_err("foreign worker presence read must fail");
        assert_eq!(
            foreign_presence.to_api_error().code,
            ErrorCode::WorkerNotFound
        );

        let (run_after, questions_after, activity_after, wakeup_after) = {
            let mut access = handle.shared.acquire_for_request().await.expect("core");
            (
                access
                    .core_mut()
                    .get_run(&due_binding.binding_id, &due_run.run_id)
                    .expect("due run after reads"),
                access
                    .core_mut()
                    .list_run_questions(&due_binding.binding_id, &due_run.run_id, None, 50)
                    .expect("due questions after reads"),
                access
                    .core_mut()
                    .list_activity(&due_binding.binding_id, None, None, 50, None, None)
                    .expect("due activity after reads"),
                access
                    .core_mut()
                    .get_wakeup(&due_binding.binding_id)
                    .expect("due wakeup after reads"),
            )
        };
        let (other_activity_after, other_wakeup_after) = {
            let mut access = handle.shared.acquire_for_request().await.expect("core");
            (
                access
                    .core_mut()
                    .list_activity(&other_binding.binding_id, None, None, 50, None, None)
                    .expect("other activity after reads"),
                access
                    .core_mut()
                    .get_wakeup(&other_binding.binding_id)
                    .expect("other wakeup after reads"),
            )
        };
        assert_eq!(run_after, run_before, "worker reads changed the due run");
        assert_eq!(
            questions_after, questions_before,
            "worker reads changed due questions"
        );
        assert_eq!(
            activity_after, activity_before,
            "worker reads changed due activity sequence"
        );
        assert_eq!(
            wakeup_after, wakeup_before,
            "worker reads changed the due wakeup"
        );
        assert_eq!(
            other_activity_after, other_activity_before,
            "worker reads changed other-binding activity"
        );
        assert_eq!(
            other_wakeup_after, other_wakeup_before,
            "worker reads changed the other-binding wakeup"
        );

        // The fixture was actually due: a normal explicit tick still advances it.
        {
            let mut access = handle.shared.acquire_for_request().await.expect("core");
            let report = access
                .core_mut()
                .tick(current_time_ms())
                .expect("explicit tick");
            assert_eq!(report.advanced_runs, vec![due_run.run_id.clone()]);
            assert_eq!(report.emitted_events, 2);
        }
        let (advanced_run, advanced_questions, advanced_wakeup) = {
            let mut access = handle.shared.acquire_for_request().await.expect("core");
            (
                access
                    .core_mut()
                    .get_run(&due_binding.binding_id, &due_run.run_id)
                    .expect("advanced due run"),
                access
                    .core_mut()
                    .list_run_questions(&due_binding.binding_id, &due_run.run_id, None, 50)
                    .expect("advanced due questions"),
                access
                    .core_mut()
                    .get_wakeup(&due_binding.binding_id)
                    .expect("advanced due wakeup"),
            )
        };
        assert_eq!(
            advanced_run.status,
            threadlane_core::RunStatus::WaitingInput
        );
        assert_eq!(advanced_run.revision, due_run.revision);
        assert!(advanced_run.active_question_id.is_some());
        assert_eq!(advanced_questions.questions.len(), 1);
        assert_eq!(advanced_wakeup.status, WakeupStatus::Pending);

        handle.shutdown().await;
    }
}
