use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use threadlane::{ScopedClient, SupervisorClient};
use threadlane_core::{
    ActivityPage, Binding, EventKind, QuestionStatus, Run, RunStatus, MAX_CURSOR_LEN, MAX_ID_LEN,
    MAX_PAGE_LIMIT, MAX_STATUS_RECENT_RUNS,
};
use uuid::Uuid;

use crate::error::BridgeError;
use crate::tools::discover_tool_status;
use crate::types::{
    Attachment, AttachmentMode, BindingSummary, ConnectionState, DemoControls, DesktopSnapshot,
    RunDetail, TaskSummary, ToolStatus,
};

/// Maximum number of bindings returned in a [`DesktopSnapshot`].
pub const MAX_BINDINGS: usize = 100;

/// Maximum number of recent tasks enriched per [`DesktopSnapshot`] (`10`).
pub const MAX_RECENT_TASKS: usize = MAX_STATUS_RECENT_RUNS;

/// Fixed display label used when creating a synthetic local demo binding.
pub const DEMO_BINDING_LABEL: &str = "Local demo";

/// Fixed instruction used when starting a deterministic local demo run.
pub const DEMO_INSTRUCTION: &str =
    "Verify deterministic local demo lifecycle (progress, question, answer, completion, and cancellation) without external model calls.";

/// Fixed answer submitted by `answer_demo`.
pub const DEMO_ANSWER: &str = "Continue";

/// Fixed cancellation reason submitted by `cancel_demo`.
pub const DEMO_CANCEL_REASON: &str = "Cancelled from Threadlane desktop local demo.";

#[derive(Clone)]
struct ClientTarget {
    state_dir: PathBuf,
    client: Result<SupervisorClient, BridgeError>,
}

impl ClientTarget {
    fn from_lazy_path(path: PathBuf) -> Self {
        let client = SupervisorClient::from_state_dir_lazy(&path)
            .map_err(|err| BridgeError::from_threadlane_error(&err));
        Self {
            state_dir: path,
            client,
        }
    }

    fn display_path(&self) -> String {
        self.state_dir.to_string_lossy().into_owned()
    }
}

#[derive(Debug, Clone)]
struct OwnedDemoTuple {
    binding_id: String,
    run_id: String,
    revision: u64,
    status: RunStatus,
    active_question_id: Option<String>,
    answered_revision: Option<u64>,
}

impl OwnedDemoTuple {
    fn from_run(run: &Run) -> Self {
        let active_question_id = extract_pending_question_id(run);
        Self {
            binding_id: run.binding_id.clone(),
            run_id: run.run_id.clone(),
            revision: run.revision,
            status: run.status,
            active_question_id,
            answered_revision: None,
        }
    }

    fn update_from_run(&mut self, run: &Run) {
        if self.binding_id != run.binding_id || self.run_id != run.run_id {
            return;
        }
        self.revision = run.revision;
        self.status = run.status;
        self.active_question_id = extract_pending_question_id(run);
    }

    fn can_answer(&self, available: bool) -> bool {
        available
            && self.status == RunStatus::WaitingInput
            && self.active_question_id.is_some()
            && self.answered_revision != Some(self.revision)
    }

    fn can_cancel(&self, available: bool) -> bool {
        available && !self.status.is_terminal()
    }
}

fn extract_pending_question_id(run: &Run) -> Option<String> {
    if run.status != RunStatus::WaitingInput {
        return None;
    }
    if let Some(q) = &run.active_question {
        if q.status == QuestionStatus::Pending && q.revision == run.revision {
            return Some(q.question_id.clone());
        }
        return None;
    }
    run.active_question_id.clone()
}

struct BridgeState {
    observer_mode: AttachmentMode,
    runtime_target: Option<ClientTarget>,
    demo_target: Option<ClientTarget>,
    selected_binding_id: Option<String>,
    owned_demo: Option<OwnedDemoTuple>,
    local_demo_binding_ids: BTreeSet<String>,
    observer_epoch: u64,
    selection_epoch: u64,
}

#[derive(Clone)]
struct ObserverContext {
    mode: AttachmentMode,
    target: Option<ClientTarget>,
    selected_binding_id: Option<String>,
    demo_available: bool,
    owned_demo: Option<OwnedDemoTuple>,
    local_demo_binding_ids: BTreeSet<String>,
    observer_epoch: u64,
    selection_epoch: u64,
}

/// Pure Rust typed bridge between the desktop shell and `threadlane::SupervisorClient`.
///
/// Uses a short-held interior [`std::sync::Mutex`] for state transitions (never held across
/// `.await` points) and a separate [`tokio::sync::Mutex`] to serialize local demo mutations.
pub struct DesktopBridge {
    state: Mutex<BridgeState>,
    demo_mutation_lock: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for DesktopBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        f.debug_struct("DesktopBridge")
            .field("observer_mode", &guard.observer_mode)
            .field("selected_binding_id", &guard.selected_binding_id)
            .field("has_runtime_target", &guard.runtime_target.is_some())
            .field("has_demo_target", &guard.demo_target.is_some())
            .finish()
    }
}

impl DesktopBridge {
    /// Creates a new [`DesktopBridge`] with optional real runtime `state_dir` and optional
    /// owned `demo_state_dir`.
    ///
    /// - If `state_dir` is `Some`, the observer initially attaches in [`AttachmentMode::Runtime`].
    /// - Else if `demo_state_dir` is `Some`, the observer initially attaches in [`AttachmentMode::Demo`].
    /// - Otherwise, the observer starts in [`AttachmentMode::Unconfigured`].
    ///
    /// Never automatically creates directories, files, bindings, or runs at construction time.
    pub fn new(
        state_dir: Option<PathBuf>,
        demo_state_dir: Option<PathBuf>,
    ) -> Result<Self, BridgeError> {
        let runtime_target = match state_dir {
            Some(dir) => {
                validate_non_empty_path(&dir)?;
                let client = SupervisorClient::from_state_dir_lazy(&dir)
                    .map_err(|err| BridgeError::from_threadlane_error(&err))?;
                Some(ClientTarget {
                    state_dir: dir,
                    client: Ok(client),
                })
            }
            None => None,
        };

        let demo_target = match demo_state_dir {
            Some(dir) => {
                validate_non_empty_path(&dir)?;
                let client = SupervisorClient::from_state_dir_lazy(&dir)
                    .map_err(|err| BridgeError::from_threadlane_error(&err))?;
                Some(ClientTarget {
                    state_dir: dir,
                    client: Ok(client),
                })
            }
            None => None,
        };

        let observer_mode = if runtime_target.is_some() {
            AttachmentMode::Runtime
        } else if demo_target.is_some() {
            AttachmentMode::Demo
        } else {
            AttachmentMode::Unconfigured
        };

        Ok(Self {
            state: Mutex::new(BridgeState {
                observer_mode,
                runtime_target,
                demo_target,
                selected_binding_id: None,
                owned_demo: None,
                local_demo_binding_ids: BTreeSet::new(),
                observer_epoch: 1,
                selection_epoch: 1,
            }),
            demo_mutation_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// Attaches the observer to an existing external runtime `state_dir` (`AttachmentMode::Runtime`)
    /// and returns a fresh [`DesktopSnapshot`].
    ///
    /// Never modifies the dedicated demo runtime configuration or owned demo tuple.
    pub async fn attach(&self, state_dir: String) -> Result<DesktopSnapshot, BridgeError> {
        let trimmed = state_dir.trim();
        if trimmed.is_empty() || trimmed.contains('\0') {
            return Err(BridgeError::InvalidInput(
                "State directory path must not be empty.",
            ));
        }

        let path = PathBuf::from(trimmed);
        let target = ClientTarget::from_lazy_path(path);

        {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            guard.runtime_target = Some(target);
            guard.observer_mode = AttachmentMode::Runtime;
            guard.selected_binding_id = None;
            guard.observer_epoch = guard.observer_epoch.saturating_add(1);
            guard.selection_epoch = guard.selection_epoch.saturating_add(1);
        }

        self.snapshot(None).await
    }

    /// Fetches a bounded [`DesktopSnapshot`] for the currently attached runtime.
    ///
    /// Lists up to [`MAX_BINDINGS`] (`100`) bindings, scopes `selected_binding_id` to an
    /// existing binding in that list without fanning out `status` to unselected bindings,
    /// and enriches up to [`MAX_RECENT_TASKS`] (`10`) recent runs via `get_worker` with
    /// typed fallback to `get_run` only on `WorkerNotFound`.
    pub async fn snapshot(
        &self,
        binding_id: Option<String>,
    ) -> Result<DesktopSnapshot, BridgeError> {
        let requested_binding_id = normalize_optional_binding_id(binding_id)?;

        let ctx = {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if requested_binding_id.is_some() {
                guard.selection_epoch = guard.selection_epoch.saturating_add(1);
            }
            self.capture_observer_context(&guard)
        };

        let now = now_ms();

        if ctx.mode == AttachmentMode::Unconfigured {
            return Ok(DesktopSnapshot {
                attachment: Attachment {
                    mode: AttachmentMode::Unconfigured,
                    connection: ConnectionState::Unconfigured,
                    state_dir: None,
                    message: Some("No runtime state directory attached.".to_string()),
                    updated_at_ms: now,
                },
                bindings: Vec::new(),
                selected_binding_id: None,
                tasks: Vec::new(),
                total_runs: 0,
                active_runs: 0,
                has_more: false,
                demo_controls: self.build_demo_controls(false),
            });
        }

        let Some(target) = ctx.target.clone() else {
            return Ok(DesktopSnapshot {
                attachment: Attachment {
                    mode: AttachmentMode::Unconfigured,
                    connection: ConnectionState::Unconfigured,
                    state_dir: None,
                    message: Some("No runtime state directory attached.".to_string()),
                    updated_at_ms: now,
                },
                bindings: Vec::new(),
                selected_binding_id: None,
                tasks: Vec::new(),
                total_runs: 0,
                active_runs: 0,
                has_more: false,
                demo_controls: self.build_demo_controls(false),
            });
        };

        let display_dir = target.display_path();
        let client = match target.client {
            Ok(client) => client,
            Err(bridge_err) => {
                let (conn, msg) = BridgeError::classify_bridge_error_for_snapshot(&bridge_err);
                let demo_available = ctx.demo_available
                    && (ctx.mode != AttachmentMode::Demo || conn == ConnectionState::Connected);
                return Ok(self.empty_snapshot_for_state(
                    ctx.mode,
                    conn,
                    Some(display_dir),
                    Some(msg.to_string()),
                    now,
                    demo_available,
                ));
            }
        };

        let raw_bindings = match client.list_bindings().await {
            Ok(list) => list,
            Err(err) => {
                let (conn, msg) = BridgeError::classify_snapshot_error(&err);
                let demo_available = ctx.demo_available
                    && (ctx.mode != AttachmentMode::Demo || conn == ConnectionState::Connected);
                return Ok(self.empty_snapshot_for_state(
                    ctx.mode,
                    conn,
                    Some(display_dir),
                    Some(msg.to_string()),
                    now,
                    demo_available,
                ));
            }
        };

        let capped_bindings: Vec<Binding> = raw_bindings.into_iter().take(MAX_BINDINGS).collect();
        let binding_summaries: Vec<BindingSummary> = capped_bindings
            .iter()
            .map(|b| {
                let is_local_demo = ctx.local_demo_binding_ids.contains(&b.binding_id)
                    || ctx
                        .owned_demo
                        .as_ref()
                        .is_some_and(|d| d.binding_id == b.binding_id)
                    || (ctx.mode == AttachmentMode::Demo && b.label == DEMO_BINDING_LABEL);
                BindingSummary {
                    binding_id: b.binding_id.clone(),
                    thread_uuid: b.thread_uuid.clone(),
                    label: b.label.clone(),
                    is_local_demo,
                }
            })
            .collect();

        let resolved_binding_id = resolve_selected_binding_id(
            &capped_bindings,
            requested_binding_id.as_deref(),
            ctx.selected_binding_id.as_deref(),
            ctx.owned_demo.as_ref().map(|d| d.binding_id.as_str()),
            ctx.mode,
        );

        let mut tasks = Vec::new();
        let mut total_runs = 0usize;
        let mut active_runs = 0usize;
        let mut has_more = false;
        let mut refreshed_demo_run: Option<Run> = None;

        if let Some(ref selected_id) = resolved_binding_id {
            let status_resp = match client.status(selected_id).await {
                Ok(status) => status,
                Err(err) => {
                    if BridgeError::is_binding_not_found(&err) {
                        // Binding disappeared between list_bindings and status; return honest empty tasks.
                        return Ok(DesktopSnapshot {
                            attachment: Attachment {
                                mode: ctx.mode,
                                connection: ConnectionState::Connected,
                                state_dir: Some(display_dir),
                                message: None,
                                updated_at_ms: now,
                            },
                            bindings: binding_summaries,
                            selected_binding_id: None,
                            tasks: Vec::new(),
                            total_runs: 0,
                            active_runs: 0,
                            has_more: false,
                            demo_controls: self.build_demo_controls(ctx.demo_available),
                        });
                    }
                    let (conn, msg) = BridgeError::classify_snapshot_error(&err);
                    let demo_available = ctx.demo_available
                        && (ctx.mode != AttachmentMode::Demo || conn == ConnectionState::Connected);
                    return Ok(self.empty_snapshot_for_state(
                        ctx.mode,
                        conn,
                        Some(display_dir),
                        Some(msg.to_string()),
                        now,
                        demo_available,
                    ));
                }
            };

            if status_resp.binding.binding_id != *selected_id {
                return Ok(self.empty_snapshot_for_state(
                    ctx.mode,
                    ConnectionState::Blocked,
                    Some(display_dir),
                    Some("Runtime supervisor returned a mismatched binding scope.".to_string()),
                    now,
                    false,
                ));
            }

            let scoped = match client.clone().scoped(selected_id.clone()) {
                Ok(scoped) => scoped,
                Err(err) => {
                    let (conn, msg) = BridgeError::classify_snapshot_error(&err);
                    return Ok(self.empty_snapshot_for_state(
                        ctx.mode,
                        conn,
                        Some(display_dir),
                        Some(msg.to_string()),
                        now,
                        false,
                    ));
                }
            };

            total_runs = status_resp.total_runs;
            active_runs = status_resp.active_runs;

            for run_summary in status_resp.recent_runs.iter().take(MAX_RECENT_TASKS) {
                if run_summary.binding_id != *selected_id {
                    return Ok(self.empty_snapshot_for_state(
                        ctx.mode,
                        ConnectionState::Blocked,
                        Some(display_dir),
                        Some(
                            "Runtime supervisor returned a cross-binding task summary.".to_string(),
                        ),
                        now,
                        false,
                    ));
                }

                match enrich_single_task(&scoped, selected_id, &run_summary.run_id).await {
                    Ok((task_summary, maybe_demo_run)) => {
                        if let Some(demo_run) = maybe_demo_run {
                            if ctx.owned_demo.as_ref().is_some_and(|d| {
                                d.binding_id == demo_run.binding_id && d.run_id == demo_run.run_id
                            }) {
                                refreshed_demo_run = Some(demo_run);
                            }
                        }
                        tasks.push(task_summary);
                    }
                    Err(bridge_err) => {
                        let (conn, msg) =
                            BridgeError::classify_bridge_error_for_snapshot(&bridge_err);
                        let demo_available = ctx.demo_available
                            && (ctx.mode != AttachmentMode::Demo
                                || conn == ConnectionState::Connected);
                        return Ok(self.empty_snapshot_for_state(
                            ctx.mode,
                            conn,
                            Some(display_dir),
                            Some(msg.to_string()),
                            now,
                            demo_available,
                        ));
                    }
                }
            }

            has_more = total_runs > tasks.len();
        }

        // If we are in Demo mode and have a non-terminal owned demo run that was not already
        // refreshed in the selected binding loop, refresh it from the demo client so DemoControls stay accurate.
        if refreshed_demo_run.is_none() {
            if let Some(ref owned) = ctx.owned_demo {
                if !owned.status.is_terminal() {
                    if let Some(demo_client) = self.demo_client_ok() {
                        if let Ok(scoped_demo) = demo_client.scoped(owned.binding_id.clone()) {
                            if let Ok(run) = scoped_demo.get_run(owned.run_id.clone()).await {
                                if run.binding_id == owned.binding_id && run.run_id == owned.run_id
                                {
                                    refreshed_demo_run = Some(run);
                                }
                            }
                        }
                    }
                }
            }
        }

        let demo_controls = {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(ref run) = refreshed_demo_run {
                if let Some(ref mut owned) = guard.owned_demo {
                    owned.update_from_run(run);
                }
            }
            if guard.observer_epoch == ctx.observer_epoch
                && guard.selection_epoch == ctx.selection_epoch
            {
                guard.selected_binding_id = resolved_binding_id.clone();
            }
            Self::compute_demo_controls_locked(&guard, ctx.demo_available)
        };

        Ok(DesktopSnapshot {
            attachment: Attachment {
                mode: ctx.mode,
                connection: ConnectionState::Connected,
                state_dir: Some(display_dir),
                message: None,
                updated_at_ms: now,
            },
            bindings: binding_summaries,
            selected_binding_id: resolved_binding_id,
            tasks,
            total_runs,
            active_runs,
            has_more,
            demo_controls,
        })
    }

    /// Fetches detailed overview, instruction, and evidence metadata for `(binding_id, run_id)`
    /// within the currently attached runtime.
    pub async fn run_detail(
        &self,
        binding_id: String,
        run_id: String,
    ) -> Result<RunDetail, BridgeError> {
        let clean_binding_id = validate_id_input(&binding_id, "Invalid binding identifier.")?;
        let clean_run_id = validate_id_input(&run_id, "Invalid task run identifier.")?;

        let ctx = {
            let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            self.capture_observer_context(&guard)
        };

        if ctx.mode == AttachmentMode::Unconfigured {
            return Err(BridgeError::Unconfigured);
        }

        let target = ctx.target.ok_or(BridgeError::Unconfigured)?;
        let client = target.client?;
        let scoped = client
            .scoped(clean_binding_id.clone())
            .map_err(|err| BridgeError::from_threadlane_error(&err))?;

        match scoped.get_worker(clean_run_id.clone()).await {
            Ok(worker) => {
                if worker.binding_id != clean_binding_id || worker.run_id != clean_run_id {
                    return Err(BridgeError::Protocol(
                        "Runtime supervisor returned a mismatched task scope.",
                    ));
                }
                let demo_controls = {
                    let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    if guard.observer_epoch != ctx.observer_epoch
                        || guard.selection_epoch != ctx.selection_epoch
                        || guard.observer_mode != ctx.mode
                    {
                        return Err(BridgeError::InvalidState(
                            "Observer attachment or binding selection changed during request.",
                        ));
                    }
                    Self::compute_demo_controls_locked(&guard, ctx.demo_available)
                };
                Ok(RunDetail::from_worker(&worker, demo_controls))
            }
            Err(err) if BridgeError::is_worker_not_found(&err) => {
                let run = scoped
                    .get_run(clean_run_id.clone())
                    .await
                    .map_err(|e| BridgeError::from_threadlane_error(&e))?;
                if run.binding_id != clean_binding_id || run.run_id != clean_run_id {
                    return Err(BridgeError::Protocol(
                        "Runtime supervisor returned a mismatched task scope.",
                    ));
                }
                let demo_controls = {
                    let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    if guard.observer_epoch != ctx.observer_epoch
                        || guard.selection_epoch != ctx.selection_epoch
                        || guard.observer_mode != ctx.mode
                    {
                        return Err(BridgeError::InvalidState(
                            "Observer attachment or binding selection changed during request.",
                        ));
                    }
                    if let Some(ref mut owned) = guard.owned_demo {
                        owned.update_from_run(&run);
                    }
                    Self::compute_demo_controls_locked(&guard, ctx.demo_available)
                };
                Ok(RunDetail::from_demo_run(&run, demo_controls))
            }
            Err(err) => Err(BridgeError::from_threadlane_error(&err)),
        }
    }

    /// Fetches a bounded, read-only keyset-paginated [`ActivityPage`] of durable activity events
    /// for the active selected binding in the currently attached runtime.
    ///
    /// Resolves the active binding exclusively from the captured trusted observer attachment
    /// context, validates that `binding_id` is a member of the active attachment's binding list
    /// (capped to [`MAX_BINDINGS`]) and matches the resolved selected binding, and discards
    /// stale completions if the observer or selection epoch changes while the request is in flight.
    /// Never opens `state.db`, calls `pull_events` or `ack_events`, modifies wake-up cursors, or
    /// probes executables.
    pub async fn activity_history(
        &self,
        binding_id: String,
        after_seq: Option<u64>,
        cursor: Option<String>,
        limit: Option<usize>,
        run_id: Option<String>,
        kind: Option<EventKind>,
    ) -> Result<ActivityPage, BridgeError> {
        let clean_binding_id = validate_id_input(&binding_id, "Invalid binding identifier.")?;
        let clean_run_id = normalize_optional_id_input(run_id, "Invalid task run identifier.")?;
        let clean_cursor = normalize_optional_cursor_input(cursor)?;

        if after_seq.is_some() && clean_cursor.is_some() {
            return Err(BridgeError::InvalidInput(
                "Cannot specify both after_seq and cursor.",
            ));
        }

        if let Some(lim) = limit {
            if !(1..=MAX_PAGE_LIMIT).contains(&lim) {
                return Err(BridgeError::InvalidInput(
                    "Activity page limit must be between 1 and 50.",
                ));
            }
        }

        let ctx = {
            let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            self.capture_observer_context(&guard)
        };

        if ctx.mode == AttachmentMode::Unconfigured {
            return Err(BridgeError::Unconfigured);
        }

        if let Some(ref selected_id) = ctx.selected_binding_id {
            if selected_id != &clean_binding_id {
                return Err(BridgeError::NotFound(
                    "Selected chat binding was not found.",
                ));
            }
        }

        let target = ctx.target.ok_or(BridgeError::Unconfigured)?;
        let client = target.client?;

        let raw_bindings = client
            .list_bindings()
            .await
            .map_err(|err| BridgeError::from_threadlane_error(&err))?;
        let capped_bindings: Vec<Binding> = raw_bindings.into_iter().take(MAX_BINDINGS).collect();

        if !capped_bindings
            .iter()
            .any(|b| b.binding_id == clean_binding_id)
        {
            return Err(BridgeError::NotFound(
                "Selected chat binding was not found.",
            ));
        }

        let resolved_binding_id = resolve_selected_binding_id(
            &capped_bindings,
            None,
            ctx.selected_binding_id.as_deref(),
            ctx.owned_demo.as_ref().map(|d| d.binding_id.as_str()),
            ctx.mode,
        );

        let Some(selected_binding_id) = resolved_binding_id else {
            return Err(BridgeError::NotFound(
                "Selected chat binding was not found.",
            ));
        };

        if selected_binding_id != clean_binding_id {
            return Err(BridgeError::NotFound(
                "Selected chat binding was not found.",
            ));
        }

        let scoped = client
            .scoped(selected_binding_id.clone())
            .map_err(|err| BridgeError::from_threadlane_error(&err))?;

        let page = scoped
            .list_activity(after_seq, clean_cursor, limit, clean_run_id.clone(), kind)
            .await
            .map_err(|err| BridgeError::from_threadlane_error(&err))?;

        if page.binding_id != selected_binding_id
            || page
                .events
                .iter()
                .any(|event| event.binding_id != selected_binding_id)
        {
            return Err(BridgeError::Protocol(
                "Runtime supervisor returned a mismatched binding scope.",
            ));
        }

        if let Some(ref expected_run_id) = clean_run_id {
            if page
                .events
                .iter()
                .any(|event| &event.run_id != expected_run_id)
            {
                return Err(BridgeError::Protocol(
                    "Runtime supervisor returned a mismatched task scope.",
                ));
            }
        }

        if let Some(expected_kind) = kind {
            if page.events.iter().any(|event| event.kind != expected_kind) {
                return Err(BridgeError::Protocol(
                    "Runtime supervisor returned a mismatched event kind.",
                ));
            }
        }

        {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if guard.observer_epoch != ctx.observer_epoch
                || guard.selection_epoch != ctx.selection_epoch
                || guard.observer_mode != ctx.mode
                || guard
                    .selected_binding_id
                    .as_deref()
                    .is_some_and(|current| current != selected_binding_id)
            {
                return Err(BridgeError::InvalidState(
                    "Observer attachment or binding selection changed during request.",
                ));
            }
            if guard.selected_binding_id.is_none() {
                guard.selected_binding_id = Some(selected_binding_id);
            }
        }

        Ok(page)
    }

    /// Starts a new deterministic local demo run on the dedicated demo runtime:
    /// creates a synthetic v4 UUID binding labeled `"Local demo"`, starts a demo run with
    /// the fixed demo instruction, records the owned `(binding_id, run_id)` tuple, switches
    /// the observer view to [`AttachmentMode::Demo`], and returns the resulting [`DesktopSnapshot`].
    ///
    /// Rejects starting a new demo run if the currently owned demo run is still non-terminal
    /// after refreshing its live state via `get_run`.
    pub async fn start_demo(&self) -> Result<DesktopSnapshot, BridgeError> {
        let _mutation_guard = self.demo_mutation_lock.lock().await;

        let demo_client = self.require_demo_client()?;

        let existing_owned = {
            let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            guard.owned_demo.clone()
        };

        if let Some(owned) = existing_owned {
            if !owned.status.is_terminal() {
                let scoped_existing = demo_client
                    .clone()
                    .scoped(owned.binding_id.clone())
                    .map_err(|err| BridgeError::from_threadlane_error(&err))?;
                match scoped_existing.get_run(owned.run_id.clone()).await {
                    Ok(live_run) => {
                        if live_run.binding_id != owned.binding_id
                            || live_run.run_id != owned.run_id
                        {
                            return Err(BridgeError::Protocol(
                                "Local demo runtime returned a mismatched task scope.",
                            ));
                        }
                        {
                            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
                            if let Some(ref mut slot) = guard.owned_demo {
                                slot.update_from_run(&live_run);
                            }
                        }
                        if !live_run.status.is_terminal() {
                            return Err(BridgeError::InvalidState(
                                "A local demo run is already active; finish or cancel it first.",
                            ));
                        }
                    }
                    Err(err)
                        if BridgeError::is_binding_not_found(&err)
                            || matches!(
                                BridgeError::from_threadlane_error(&err),
                                BridgeError::NotFound(_)
                            ) =>
                    {
                        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
                        guard.owned_demo = None;
                    }
                    Err(err) => return Err(BridgeError::from_threadlane_error(&err)),
                }
            }
        }

        let thread_uuid = Uuid::new_v4().to_string();
        let binding = demo_client
            .create_binding(&thread_uuid, DEMO_BINDING_LABEL)
            .await
            .map_err(|err| BridgeError::from_threadlane_error(&err))?;

        let scoped = demo_client
            .scoped(binding.binding_id.clone())
            .map_err(|err| BridgeError::from_threadlane_error(&err))?;
        let command_key = format!("desktop_start_demo_{}", Uuid::new_v4().simple());
        let run = scoped
            .start_demo(command_key, DEMO_INSTRUCTION.to_string(), None)
            .await
            .map_err(|err| BridgeError::from_threadlane_error(&err))?;

        if run.binding_id != binding.binding_id {
            return Err(BridgeError::Protocol(
                "Local demo runtime returned a mismatched binding scope.",
            ));
        }

        let binding_id = run.binding_id.clone();
        {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            guard.local_demo_binding_ids.insert(binding_id.clone());
            guard.owned_demo = Some(OwnedDemoTuple::from_run(&run));
            guard.observer_mode = AttachmentMode::Demo;
            guard.selected_binding_id = Some(binding_id.clone());
            guard.observer_epoch = guard.observer_epoch.saturating_add(1);
            guard.selection_epoch = guard.selection_epoch.saturating_add(1);
        }

        self.snapshot(Some(binding_id)).await
    }

    /// Submits the fixed `"Continue"` answer for the currently owned local demo run after
    /// verifying `expected_revision` and denying replay/stale mutations.
    pub async fn answer_demo(
        &self,
        expected_revision: u64,
    ) -> Result<DesktopSnapshot, BridgeError> {
        let _mutation_guard = self.demo_mutation_lock.lock().await;

        if expected_revision == 0 {
            return Err(BridgeError::StaleRevision);
        }

        let demo_client = self.require_demo_client()?;
        let owned = self.get_owned_demo_for_answer(expected_revision)?;

        let scoped = demo_client
            .scoped(owned.binding_id.clone())
            .map_err(|err| BridgeError::from_threadlane_error(&err))?;

        let question_id = match owned.active_question_id.clone() {
            Some(qid) if owned.status == RunStatus::WaitingInput => qid,
            _ => {
                let live_run = scoped
                    .get_run(owned.run_id.clone())
                    .await
                    .map_err(|err| BridgeError::from_threadlane_error(&err))?;
                if live_run.binding_id != owned.binding_id || live_run.run_id != owned.run_id {
                    return Err(BridgeError::Protocol(
                        "Local demo runtime returned a mismatched task scope.",
                    ));
                }
                {
                    let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(ref mut slot) = guard.owned_demo {
                        slot.update_from_run(&live_run);
                    }
                }
                if live_run.revision != expected_revision {
                    return Err(BridgeError::StaleRevision);
                }
                if live_run.status != RunStatus::WaitingInput {
                    return Err(BridgeError::InvalidState(
                        "Local demo run is not waiting for an answer.",
                    ));
                }
                extract_pending_question_id(&live_run).ok_or(BridgeError::InvalidState(
                    "Local demo run has no pending question to answer.",
                ))?
            }
        };

        let command_key = format!("desktop_answer_demo_{}", Uuid::new_v4().simple());
        let updated_run = scoped
            .answer(
                command_key,
                owned.run_id.clone(),
                question_id,
                expected_revision,
                DEMO_ANSWER.to_string(),
            )
            .await
            .map_err(|err| BridgeError::from_threadlane_error(&err))?;

        if updated_run.binding_id != owned.binding_id || updated_run.run_id != owned.run_id {
            return Err(BridgeError::Protocol(
                "Local demo runtime returned a mismatched task scope.",
            ));
        }

        let binding_id = owned.binding_id.clone();
        {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(ref mut slot) = guard.owned_demo {
                slot.update_from_run(&updated_run);
                slot.answered_revision = Some(expected_revision);
            }
            guard.observer_mode = AttachmentMode::Demo;
            guard.selected_binding_id = Some(binding_id.clone());
            guard.observer_epoch = guard.observer_epoch.saturating_add(1);
            guard.selection_epoch = guard.selection_epoch.saturating_add(1);
        }

        self.snapshot(Some(binding_id)).await
    }

    /// Cancels the currently owned local demo run after verifying `expected_revision`
    /// and denying replay/terminal mutations.
    ///
    /// Because `ScopedRequest::Cancel` does not carry an atomic server-side `expected_revision`
    /// check, this method serializes demo mutations under `demo_mutation_lock`, fetches the live
    /// [`Run`] via `get_run` immediately before calling `cancel`, verifies the owned
    /// `(binding_id, run_id)` tuple, non-terminal status, and `live_run.revision == expected_revision`,
    /// and updates the owned tuple from the verified server response.
    pub async fn cancel_demo(
        &self,
        expected_revision: u64,
    ) -> Result<DesktopSnapshot, BridgeError> {
        let _mutation_guard = self.demo_mutation_lock.lock().await;

        if expected_revision == 0 {
            return Err(BridgeError::StaleRevision);
        }

        let demo_client = self.require_demo_client()?;
        let owned = {
            let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let current = guard.owned_demo.as_ref().ok_or(BridgeError::InvalidState(
                "No active local demo run is owned by this session.",
            ))?;
            if current.status.is_terminal() {
                return Err(BridgeError::InvalidState(
                    "Local demo run has already finished.",
                ));
            }
            current.clone()
        };

        let scoped = demo_client
            .scoped(owned.binding_id.clone())
            .map_err(|err| BridgeError::from_threadlane_error(&err))?;

        let live_run = scoped
            .get_run(owned.run_id.clone())
            .await
            .map_err(|err| BridgeError::from_threadlane_error(&err))?;
        if live_run.binding_id != owned.binding_id || live_run.run_id != owned.run_id {
            return Err(BridgeError::Protocol(
                "Local demo runtime returned a mismatched task scope.",
            ));
        }
        {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(ref mut slot) = guard.owned_demo {
                slot.update_from_run(&live_run);
            }
        }
        if live_run.status.is_terminal() {
            return Err(BridgeError::InvalidState(
                "Local demo run has already finished.",
            ));
        }
        if live_run.revision != expected_revision {
            return Err(BridgeError::StaleRevision);
        }

        let command_key = format!("desktop_cancel_demo_{}", Uuid::new_v4().simple());
        let updated_run = scoped
            .cancel(
                command_key,
                owned.run_id.clone(),
                Some(DEMO_CANCEL_REASON.to_string()),
            )
            .await
            .map_err(|err| BridgeError::from_threadlane_error(&err))?;

        if updated_run.binding_id != owned.binding_id || updated_run.run_id != owned.run_id {
            return Err(BridgeError::Protocol(
                "Local demo runtime returned a mismatched task scope.",
            ));
        }

        let binding_id = owned.binding_id.clone();
        {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(ref mut slot) = guard.owned_demo {
                slot.update_from_run(&updated_run);
            }
            guard.observer_mode = AttachmentMode::Demo;
            guard.selected_binding_id = Some(binding_id.clone());
            guard.observer_epoch = guard.observer_epoch.saturating_add(1);
            guard.selection_epoch = guard.selection_epoch.saturating_add(1);
        }

        self.snapshot(Some(binding_id)).await
    }

    /// Passively discovers local tool executables on `PATH` without spawning subprocesses.
    pub fn tool_status(&self) -> Vec<ToolStatus> {
        discover_tool_status()
    }

    fn capture_observer_context(&self, guard: &BridgeState) -> ObserverContext {
        let target = match guard.observer_mode {
            AttachmentMode::Demo => guard.demo_target.clone(),
            AttachmentMode::Runtime => guard.runtime_target.clone(),
            AttachmentMode::Unconfigured => None,
        };
        let demo_available = guard.demo_target.as_ref().is_some_and(|t| t.client.is_ok());

        ObserverContext {
            mode: guard.observer_mode,
            target,
            selected_binding_id: guard.selected_binding_id.clone(),
            demo_available,
            owned_demo: guard.owned_demo.clone(),
            local_demo_binding_ids: guard.local_demo_binding_ids.clone(),
            observer_epoch: guard.observer_epoch,
            selection_epoch: guard.selection_epoch,
        }
    }

    fn require_demo_client(&self) -> Result<SupervisorClient, BridgeError> {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let target = guard
            .demo_target
            .as_ref()
            .ok_or(BridgeError::DemoUnavailable)?;
        target.client.clone()
    }

    fn demo_client_ok(&self) -> Option<SupervisorClient> {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        guard
            .demo_target
            .as_ref()
            .and_then(|t| t.client.as_ref().ok().cloned())
    }

    fn get_owned_demo_for_answer(
        &self,
        expected_revision: u64,
    ) -> Result<OwnedDemoTuple, BridgeError> {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let owned = guard.owned_demo.as_ref().ok_or(BridgeError::InvalidState(
            "No active local demo run is owned by this session.",
        ))?;

        if owned.answered_revision == Some(expected_revision)
            || owned.status == RunStatus::Completing
            || owned.status.is_terminal()
        {
            return Err(BridgeError::InvalidState(
                "Local demo question has already been answered or closed.",
            ));
        }

        if owned.revision != expected_revision {
            return Err(BridgeError::StaleRevision);
        }

        Ok(owned.clone())
    }

    fn build_demo_controls(&self, available: bool) -> DemoControls {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Self::compute_demo_controls_locked(&guard, available)
    }

    fn compute_demo_controls_locked(guard: &BridgeState, available: bool) -> DemoControls {
        if !available {
            return DemoControls {
                available: false,
                run_id: guard.owned_demo.as_ref().map(|d| d.run_id.clone()),
                binding_id: guard.owned_demo.as_ref().map(|d| d.binding_id.clone()),
                can_answer: false,
                can_cancel: false,
                revision: guard.owned_demo.as_ref().map(|d| d.revision),
            };
        }

        match &guard.owned_demo {
            Some(owned) => DemoControls {
                available: true,
                run_id: Some(owned.run_id.clone()),
                binding_id: Some(owned.binding_id.clone()),
                can_answer: owned.can_answer(true),
                can_cancel: owned.can_cancel(true),
                revision: Some(owned.revision),
            },
            None => DemoControls {
                available: true,
                run_id: None,
                binding_id: None,
                can_answer: false,
                can_cancel: false,
                revision: None,
            },
        }
    }

    fn empty_snapshot_for_state(
        &self,
        mode: AttachmentMode,
        connection: ConnectionState,
        state_dir: Option<String>,
        message: Option<String>,
        updated_at_ms: i64,
        demo_available: bool,
    ) -> DesktopSnapshot {
        DesktopSnapshot {
            attachment: Attachment {
                mode,
                connection,
                state_dir,
                message,
                updated_at_ms,
            },
            bindings: Vec::new(),
            selected_binding_id: None,
            tasks: Vec::new(),
            total_runs: 0,
            active_runs: 0,
            has_more: false,
            demo_controls: self.build_demo_controls(demo_available),
        }
    }
}

async fn enrich_single_task(
    scoped: &ScopedClient,
    expected_binding_id: &str,
    run_id: &str,
) -> Result<(TaskSummary, Option<Run>), BridgeError> {
    match scoped.get_worker(run_id.to_string()).await {
        Ok(worker) => {
            if worker.binding_id != expected_binding_id || worker.run_id != run_id {
                return Err(BridgeError::Protocol(
                    "Runtime supervisor returned a mismatched worker scope.",
                ));
            }
            Ok((TaskSummary::from_worker(&worker), None))
        }
        Err(err) if BridgeError::is_worker_not_found(&err) => {
            let run = scoped
                .get_run(run_id.to_string())
                .await
                .map_err(|e| BridgeError::from_threadlane_error(&e))?;
            if run.binding_id != expected_binding_id || run.run_id != run_id {
                return Err(BridgeError::Protocol(
                    "Runtime supervisor returned a mismatched task scope.",
                ));
            }
            let summary = TaskSummary::from_demo_run(&run);
            Ok((summary, Some(run)))
        }
        Err(err) => Err(BridgeError::from_threadlane_error(&err)),
    }
}

fn resolve_selected_binding_id(
    bindings: &[Binding],
    requested: Option<&str>,
    prior_selected: Option<&str>,
    owned_demo_binding: Option<&str>,
    mode: AttachmentMode,
) -> Option<String> {
    if bindings.is_empty() {
        return None;
    }

    if let Some(req_id) = requested {
        if bindings.iter().any(|b| b.binding_id == req_id) {
            return Some(req_id.to_string());
        }
    }

    if let Some(prior_id) = prior_selected {
        if bindings.iter().any(|b| b.binding_id == prior_id) {
            return Some(prior_id.to_string());
        }
    }

    if mode == AttachmentMode::Demo {
        if let Some(demo_id) = owned_demo_binding {
            if bindings.iter().any(|b| b.binding_id == demo_id) {
                return Some(demo_id.to_string());
            }
        }
    }

    bindings.first().map(|b| b.binding_id.clone())
}

fn normalize_optional_binding_id(raw: Option<String>) -> Result<Option<String>, BridgeError> {
    normalize_optional_id_input(raw, "Invalid binding identifier.")
}

fn normalize_optional_id_input(
    raw: Option<String>,
    err_msg: &'static str,
) -> Result<Option<String>, BridgeError> {
    let Some(value) = raw else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let validated = validate_id_input(trimmed, err_msg)?;
    Ok(Some(validated))
}

fn normalize_optional_cursor_input(raw: Option<String>) -> Result<Option<String>, BridgeError> {
    let Some(value) = raw else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.len() > MAX_CURSOR_LEN || trimmed != value || trimmed.chars().any(char::is_control) {
        return Err(BridgeError::InvalidInput("Invalid pagination cursor."));
    }
    Ok(Some(trimmed.to_string()))
}

fn validate_id_input(raw: &str, err_msg: &'static str) -> Result<String, BridgeError> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.len() > MAX_ID_LEN
        || trimmed != raw
        || trimmed
            .chars()
            .any(|c| c.is_control() || matches!(c, '/' | '\\' | '?' | '#' | '%'))
    {
        return Err(BridgeError::InvalidInput(err_msg));
    }
    Ok(trimmed.to_string())
}

fn validate_non_empty_path(path: &Path) -> Result<(), BridgeError> {
    let s = path.as_os_str().to_string_lossy();
    if s.trim().is_empty() || s.contains('\0') {
        return Err(BridgeError::InvalidInput(
            "State directory path must not be empty.",
        ));
    }
    Ok(())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}
