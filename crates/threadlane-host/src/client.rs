use std::path::{Path, PathBuf};
use std::sync::Arc;

use threadlane::{
    ActivityPage, ErrorCode as RuntimeErrorCode, EventKind, ScopedClient, SupervisorClient,
    WorkerSnapshot,
};
use threadlane_authority::{ApprovalRecord, ApprovedTaskRecord, AuthorityStore};
use threadlane_core::{AckResult, Binding, EventBatch};
use threadlane_plans::PlanStore;
use threadlane_registry::{RegistryStore, ToolAdapter};

use crate::error::HostError;
use crate::storage::{
    derive_ack_command_key, derive_cancel_command_key, is_host_lock_held, now_epoch_ms,
    read_host_manifest_optional, read_task_ledger_entry, record_task_dispatch_intent,
    record_task_receipt, validate_approval_id, validate_canonical_thread_uuid, validate_identifier,
    validate_sessions_root, verify_existing_private_dir, verify_existing_session_dir,
};
use crate::types::{
    HostPhase, HostSessionSummary, TaskDispatchIntent, TaskExecutionStatus, TaskRunReceipt,
    TaskStateSnapshot, APPROVED_EVENT_BATCH_MAX_BYTES, APPROVED_EVENT_BATCH_MAX_EVENTS,
    RUNTIME_STATE_SUBDIR, WORK_AREA_SUBDIR,
};

struct VerifiedSession {
    record: ApprovalRecord,
    session_dir: PathBuf,
    work_area_dir: PathBuf,
    live_binding: Binding,
    scoped_client: ScopedClient,
}

/// Authoritative caller-scoped client for dispatching approved root worker tasks, inspecting task
/// status and bounded activity, and requesting revision-checked cancellation against a running
/// [`crate::ApprovedHost`] session.
///
/// ## Authority & Scope Invariants
///
/// - Constructed from trusted stores (`AuthorityStore` + `sessions_dir`) and the caller's
///   authoritative `control_binding_id` and `native_thread_uuid`.
/// - On every call, `approval_id` is treated strictly as a lookup key: the persisted
///   [`ApprovalRecord`] is loaded from [`AuthorityStore`] and both `record.binding_id` and
///   `record.thread_uuid` are compared against the caller's authoritative scope **before**
///   accessing the host session directory.
/// - `dispatch_task(approval_id, task_id)` accepts no instruction, model, effort, executable,
///   write scope, profile, or idempotency key override from the caller.
/// - Never exposes supervisor bearer tokens or local filesystem paths in return values or `Debug`.
#[derive(Clone)]
pub struct ApprovedHostClient {
    authority: Arc<AuthorityStore>,
    sessions_dir: PathBuf,
    control_binding_id: String,
    native_thread_uuid: String,
}

impl std::fmt::Debug for ApprovedHostClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApprovedHostClient")
            .field("control_binding_id", &self.control_binding_id)
            .field("native_thread_uuid", &self.native_thread_uuid)
            .finish()
    }
}

impl ApprovedHostClient {
    /// Opens the global stores (`registry_dir`, `plans_dir`, `authority_dir`) and constructs an
    /// [`ApprovedHostClient`] bound to the caller's authoritative `control_binding_id` and
    /// `native_thread_uuid`.
    pub fn open(
        registry_dir: impl AsRef<Path>,
        plans_dir: impl AsRef<Path>,
        authority_dir: impl AsRef<Path>,
        sessions_dir: impl AsRef<Path>,
        control_binding_id: impl Into<String>,
        native_thread_uuid: impl Into<String>,
    ) -> Result<Self, HostError> {
        let registry = Arc::new(RegistryStore::open(registry_dir.as_ref())?);
        let plans = Arc::new(PlanStore::open(plans_dir.as_ref())?);
        let authority = Arc::new(AuthorityStore::open(
            authority_dir.as_ref(),
            Arc::clone(&registry),
            Arc::clone(&plans),
        )?);
        validate_sessions_root(
            sessions_dir.as_ref(),
            &[
                ("registry_dir", registry.config_dir()),
                ("plans_dir", plans.dir()),
                ("authority_dir", authority.dir()),
            ],
        )?;
        Self::new(
            authority,
            sessions_dir,
            control_binding_id,
            native_thread_uuid,
        )
    }

    /// Constructs an [`ApprovedHostClient`] from an opened [`AuthorityStore`], `sessions_dir`,
    /// and the caller's authoritative `control_binding_id` and `native_thread_uuid`.
    pub fn new(
        authority: Arc<AuthorityStore>,
        sessions_dir: impl AsRef<Path>,
        control_binding_id: impl Into<String>,
        native_thread_uuid: impl Into<String>,
    ) -> Result<Self, HostError> {
        let control_binding_id = control_binding_id.into();
        let native_thread_uuid = native_thread_uuid.into();
        validate_identifier("control_binding_id", &control_binding_id)?;
        validate_canonical_thread_uuid("native_thread_uuid", &native_thread_uuid)?;

        let canonical_sessions_dir =
            validate_sessions_root(sessions_dir.as_ref(), &[("authority_dir", authority.dir())])?;

        Ok(Self {
            authority,
            sessions_dir: canonical_sessions_dir,
            control_binding_id,
            native_thread_uuid,
        })
    }

    /// Returns the caller's authoritative control-plane `binding_id`.
    pub fn control_binding_id(&self) -> &str {
        &self.control_binding_id
    }

    /// Returns the caller's authoritative canonical native Codex `thread_uuid`.
    pub fn native_thread_uuid(&self) -> &str {
        &self.native_thread_uuid
    }

    /// Returns a safe, token-free, path-free [`HostSessionSummary`] for `approval_id` after
    /// verifying that the approval belongs to this caller's `control_binding_id` and
    /// `native_thread_uuid`.
    pub fn session_summary(&self, approval_id: &str) -> Result<HostSessionSummary, HostError> {
        let record = self.resolve_authorized_approval(approval_id)?;
        let session_dir = verify_existing_session_dir(&self.sessions_dir, &record.approval_id)?;
        let manifest = read_host_manifest_optional(&session_dir, &record)?.ok_or_else(|| {
            HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: None,
                reason: "host-manifest.json does not exist".to_string(),
            }
        })?;
        let lock_held = is_host_lock_held(&session_dir)?;
        let mut summary = manifest.to_summary();
        if !lock_held {
            summary.gates.supervisor_authenticated_ready = false;
        }
        Ok(summary)
    }

    /// Dispatches an approved root headless worker task (`antigravity` or `opencode_v2`) using
    /// only `(approval_id, task_id)`.
    ///
    /// - Verifies caller `control_binding_id` and `native_thread_uuid` before accessing session state.
    /// - Rejects tasks with non-empty `depends_on` with [`HostError::DependencyInputsUnverified`].
    /// - Rejects manual `codex_cli` tasks with [`HostError::NativeRouteRequired`].
    /// - Materializes the frozen approval against the active host's single `session_binding`,
    ///   commits a validated [`TaskDispatchIntent`] to `task-receipts.json` **before** invoking
    ///   `ScopedClient::start_worker`, and calls `ScopedClient::start_worker` with the frozen
    ///   deterministic `command_key`, `profile_id`, and `instruction`.
    /// - Persists the actual returned `WorkerSnapshot::run_id` and `worker_id` in private
    ///   `task-receipts.json` and never mutates plan status to `Received` or `Applied`.
    pub async fn dispatch_task(
        &self,
        approval_id: &str,
        task_id: &str,
    ) -> Result<TaskStateSnapshot, HostError> {
        validate_identifier("task_id", task_id)?;
        let session = self.verify_active_ready_session(approval_id).await?;

        let task = session
            .record
            .tasks
            .iter()
            .find(|t| t.task_id == task_id)
            .ok_or_else(|| HostError::UnknownTask {
                approval_id: session.record.approval_id.clone(),
                task_id: task_id.to_string(),
            })?;

        if !task.depends_on.is_empty() {
            return Err(HostError::DependencyInputsUnverified {
                approval_id: session.record.approval_id.clone(),
                task_id: task.task_id.clone(),
                depends_on: task.depends_on.clone(),
            });
        }
        if task.provider.is_none() || task.adapter == ToolAdapter::CodexCli {
            return Err(HostError::NativeRouteRequired {
                approval_id: session.record.approval_id.clone(),
                task_id: task.task_id.clone(),
                adapter: task.adapter,
            });
        }

        let materialized = self.authority.materialize(
            &session.record.approval_id,
            &session.live_binding,
            &session.work_area_dir,
        )?;

        let dispatch = materialized
            .task_dispatch
            .iter()
            .find(|d| d.task_id == task_id)
            .ok_or_else(|| HostError::CorruptSessionState {
                reason: format!(
                    "materialized plan is missing dispatch entry for root worker task `{task_id}`"
                ),
            })?;

        if dispatch.task_digest != task.task_digest
            || Some(&dispatch.profile_id) != task.profile_id.as_ref()
            || Some(&dispatch.command_key) != task.command_key.as_ref()
            || dispatch.instruction != task.instruction
        {
            return Err(HostError::CorruptSessionState {
                reason: format!(
                    "materialized dispatch entry for task `{task_id}` does not match frozen approval task"
                ),
            });
        }

        let intent = TaskDispatchIntent {
            task_id: task.task_id.clone(),
            task_digest: dispatch.task_digest.clone(),
            profile_id: dispatch.profile_id.clone(),
            command_key: dispatch.command_key.clone(),
            recorded_at_ms: now_epoch_ms(),
        };
        let recorded_intent = record_task_dispatch_intent(
            &session.session_dir,
            &session.record,
            &session.live_binding.binding_id,
            intent,
        )?;

        let started = session
            .scoped_client
            .start_worker(
                recorded_intent.command_key.clone(),
                recorded_intent.profile_id.clone(),
                dispatch.instruction.clone(),
            )
            .await?;

        if started.binding_id != session.live_binding.binding_id {
            return Err(HostError::ScopeMismatch {
                field: "session_binding_id".to_string(),
                expected: session.live_binding.binding_id.clone(),
                actual: started.binding_id,
                reason: "worker snapshot returned mismatched session_binding_id".to_string(),
            });
        }

        let receipt = TaskRunReceipt {
            task_id: task.task_id.clone(),
            task_digest: recorded_intent.task_digest.clone(),
            profile_id: recorded_intent.profile_id.clone(),
            command_key: recorded_intent.command_key.clone(),
            run_id: started.run_id.clone(),
            worker_id: started.worker_id.clone(),
            recorded_at_ms: now_epoch_ms(),
        };
        let sealed = record_task_receipt(
            &session.session_dir,
            &session.record,
            &session.live_binding.binding_id,
            receipt,
        )?;

        let live_worker = match session
            .scoped_client
            .get_worker(sealed.run_id.clone())
            .await
        {
            Ok(fresh) => fresh,
            Err(_) => started,
        };

        Ok(build_task_snapshot(
            &session.record,
            &session.live_binding.binding_id,
            task,
            Some(&recorded_intent),
            Some(&sealed),
            Some(live_worker),
        ))
    }

    /// Queries the current execution status of `task_id` in `approval_id`.
    ///
    /// - Strictly read-only: never dispatches a worker or writes to session storage.
    /// - Never invents a `run_id` from a `command_key`.
    /// - If a root worker task has neither a durable dispatch intent nor a sealed receipt, returns
    ///   [`TaskExecutionStatus::NotStarted`].
    /// - If a root worker task has a durable dispatch intent without a sealed receipt, returns
    ///   [`TaskExecutionStatus::Unknown`].
    /// - If a task has non-empty `depends_on`, returns [`TaskExecutionStatus::DependencyInputsUnverified`].
    /// - If a task uses `codex_cli`, returns [`TaskExecutionStatus::NativeRouteRequired`].
    /// - If a worker completed, reports [`TaskExecutionStatus::ReadyForReview`] (`accepted: false`).
    pub async fn get_task(
        &self,
        approval_id: &str,
        task_id: &str,
    ) -> Result<TaskStateSnapshot, HostError> {
        validate_identifier("task_id", task_id)?;
        let session = self.verify_active_ready_session(approval_id).await?;

        let task = session
            .record
            .tasks
            .iter()
            .find(|t| t.task_id == task_id)
            .ok_or_else(|| HostError::UnknownTask {
                approval_id: session.record.approval_id.clone(),
                task_id: task_id.to_string(),
            })?;

        if !task.depends_on.is_empty() {
            return Ok(TaskStateSnapshot {
                approval_id: session.record.approval_id.clone(),
                subject_digest: session.record.subject_digest.clone(),
                binding_id: session.record.binding_id.clone(),
                thread_uuid: session.record.thread_uuid.clone(),
                session_binding_id: session.live_binding.binding_id.clone(),
                task_id: task.task_id.clone(),
                task_digest: task.task_digest.clone(),
                adapter: task.adapter,
                provider: task.provider,
                depends_on: task.depends_on.clone(),
                profile_id: task.profile_id.clone(),
                command_key: task.command_key.clone(),
                status: TaskExecutionStatus::DependencyInputsUnverified,
                run_id: None,
                worker_id: None,
                worker: None,
                accepted: false,
            });
        }

        if task.provider.is_none() || task.adapter == ToolAdapter::CodexCli {
            return Ok(TaskStateSnapshot {
                approval_id: session.record.approval_id.clone(),
                subject_digest: session.record.subject_digest.clone(),
                binding_id: session.record.binding_id.clone(),
                thread_uuid: session.record.thread_uuid.clone(),
                session_binding_id: session.live_binding.binding_id.clone(),
                task_id: task.task_id.clone(),
                task_digest: task.task_digest.clone(),
                adapter: task.adapter,
                provider: task.provider,
                depends_on: task.depends_on.clone(),
                profile_id: task.profile_id.clone(),
                command_key: task.command_key.clone(),
                status: TaskExecutionStatus::NativeRouteRequired,
                run_id: None,
                worker_id: None,
                worker: None,
                accepted: false,
            });
        }

        let (intent, receipt) = read_task_ledger_entry(
            &session.session_dir,
            &session.record,
            &session.live_binding.binding_id,
            task_id,
        )?;

        let Some(receipt) = receipt else {
            return Ok(build_task_snapshot(
                &session.record,
                &session.live_binding.binding_id,
                task,
                intent.as_ref(),
                None,
                None,
            ));
        };

        match session
            .scoped_client
            .get_worker(receipt.run_id.clone())
            .await
        {
            Ok(worker) => {
                if worker.binding_id != session.live_binding.binding_id {
                    return Err(HostError::ScopeMismatch {
                        field: "session_binding_id".to_string(),
                        expected: session.live_binding.binding_id.clone(),
                        actual: worker.binding_id,
                        reason: "worker snapshot binding_id mismatch".to_string(),
                    });
                }
                Ok(build_task_snapshot(
                    &session.record,
                    &session.live_binding.binding_id,
                    task,
                    intent.as_ref(),
                    Some(&receipt),
                    Some(worker),
                ))
            }
            Err(threadlane::Error::Remote(api_err))
                if api_err.code == RuntimeErrorCode::WorkerNotFound =>
            {
                Ok(build_task_snapshot(
                    &session.record,
                    &session.live_binding.binding_id,
                    task,
                    intent.as_ref(),
                    Some(&receipt),
                    None,
                ))
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Requests revision-checked cancellation of an in-flight root worker task using the sealed
    /// `task_id -> run_id` receipt and a deterministic cancellation command key.
    pub async fn cancel_task(
        &self,
        approval_id: &str,
        task_id: &str,
        expected_revision: u64,
        reason: Option<String>,
    ) -> Result<TaskStateSnapshot, HostError> {
        validate_identifier("task_id", task_id)?;
        if expected_revision == 0 {
            return Err(HostError::validation(
                "expected_revision",
                "must be a positive revision >= 1",
            ));
        }

        let session = self.verify_active_ready_session(approval_id).await?;

        let task = session
            .record
            .tasks
            .iter()
            .find(|t| t.task_id == task_id)
            .ok_or_else(|| HostError::UnknownTask {
                approval_id: session.record.approval_id.clone(),
                task_id: task_id.to_string(),
            })?;

        if !task.depends_on.is_empty() {
            return Err(HostError::DependencyInputsUnverified {
                approval_id: session.record.approval_id.clone(),
                task_id: task.task_id.clone(),
                depends_on: task.depends_on.clone(),
            });
        }
        if task.provider.is_none() || task.adapter == ToolAdapter::CodexCli {
            return Err(HostError::NativeRouteRequired {
                approval_id: session.record.approval_id.clone(),
                task_id: task.task_id.clone(),
                adapter: task.adapter,
            });
        }

        let (intent, receipt) = read_task_ledger_entry(
            &session.session_dir,
            &session.record,
            &session.live_binding.binding_id,
            task_id,
        )?;

        let Some(receipt) = receipt else {
            if let Some(intent) = intent {
                return Err(HostError::InvalidRunState {
                    run_id: task.task_id.clone(),
                    action: "cancel_task".to_string(),
                    reason: format!(
                        "task `{}` in approval `{}` has an unresolved dispatch intent (command_key `{}`) in unknown state without a sealed run_id receipt",
                        task.task_id, session.record.approval_id, intent.command_key
                    ),
                });
            }
            return Err(HostError::TaskNotStarted {
                approval_id: session.record.approval_id.clone(),
                task_id: task.task_id.clone(),
            });
        };

        let cancel_key = derive_cancel_command_key(
            &session.record.subject_digest,
            &task.task_id,
            &receipt.run_id,
            expected_revision,
        );

        let cancelled = session
            .scoped_client
            .cancel_worker(
                cancel_key,
                receipt.run_id.clone(),
                expected_revision,
                reason,
            )
            .await?;

        let live_worker = match session
            .scoped_client
            .get_worker(receipt.run_id.clone())
            .await
        {
            Ok(fresh) => fresh,
            Err(_) => cancelled,
        };

        Ok(build_task_snapshot(
            &session.record,
            &session.live_binding.binding_id,
            task,
            intent.as_ref(),
            Some(&receipt),
            Some(live_worker),
        ))
    }

    /// Lists bounded read-only activity events for the host session's runtime binding.
    pub async fn list_activity(
        &self,
        approval_id: &str,
        after_seq: Option<u64>,
        cursor: Option<String>,
        limit: Option<usize>,
        kind: Option<EventKind>,
    ) -> Result<ActivityPage, HostError> {
        let session = self.verify_active_ready_session(approval_id).await?;
        let page = session
            .scoped_client
            .list_activity(after_seq, cursor, limit, None, kind)
            .await?;
        Ok(page)
    }

    /// Lists bounded read-only activity events filtered to the sealed `run_id` of `task_id`.
    pub async fn list_task_activity(
        &self,
        approval_id: &str,
        task_id: &str,
        after_seq: Option<u64>,
        cursor: Option<String>,
        limit: Option<usize>,
        kind: Option<EventKind>,
    ) -> Result<ActivityPage, HostError> {
        validate_identifier("task_id", task_id)?;
        let session = self.verify_active_ready_session(approval_id).await?;

        let task = session
            .record
            .tasks
            .iter()
            .find(|t| t.task_id == task_id)
            .ok_or_else(|| HostError::UnknownTask {
                approval_id: session.record.approval_id.clone(),
                task_id: task_id.to_string(),
            })?;

        if !task.depends_on.is_empty() {
            return Err(HostError::DependencyInputsUnverified {
                approval_id: session.record.approval_id.clone(),
                task_id: task.task_id.clone(),
                depends_on: task.depends_on.clone(),
            });
        }
        if task.provider.is_none() || task.adapter == ToolAdapter::CodexCli {
            return Err(HostError::NativeRouteRequired {
                approval_id: session.record.approval_id.clone(),
                task_id: task.task_id.clone(),
                adapter: task.adapter,
            });
        }

        let (intent, receipt) = read_task_ledger_entry(
            &session.session_dir,
            &session.record,
            &session.live_binding.binding_id,
            task_id,
        )?;

        let Some(receipt) = receipt else {
            if let Some(intent) = intent {
                return Err(HostError::InvalidRunState {
                    run_id: task.task_id.clone(),
                    action: "list_task_activity".to_string(),
                    reason: format!(
                        "task `{}` in approval `{}` has an unresolved dispatch intent (command_key `{}`) in unknown state without a sealed run_id receipt",
                        task.task_id, session.record.approval_id, intent.command_key
                    ),
                });
            }
            return Err(HostError::TaskNotStarted {
                approval_id: session.record.approval_id.clone(),
                task_id: task.task_id.clone(),
            });
        };

        let page = session
            .scoped_client
            .list_activity(after_seq, cursor, limit, Some(receipt.run_id), kind)
            .await?;
        Ok(page)
    }

    /// Pulls a bounded batch of events from the approved session's full runtime event stream
    /// and advances `delivered_seq` on the session binding.
    ///
    /// - Enforces default/max `limit = 20` ([`APPROVED_EVENT_BATCH_MAX_EVENTS`]); rejects `limit == Some(0)`
    ///   or `limit > Some(20)` with [`HostError::Validation`] before any runtime mutation.
    /// - Enforces the `49_152`-byte ([`APPROVED_EVENT_BATCH_MAX_BYTES`]) serialized batch budget via
    ///   [`ScopedClient::pull_events_bounded`].
    /// - Verifies active ready session state and original caller `control_binding_id` + `native_thread_uuid`
    ///   on every call.
    pub async fn pull_events(
        &self,
        approval_id: &str,
        after_seq: Option<u64>,
        limit: Option<usize>,
    ) -> Result<EventBatch, HostError> {
        let resolved_limit = limit.unwrap_or(APPROVED_EVENT_BATCH_MAX_EVENTS);
        if resolved_limit == 0 || resolved_limit > APPROVED_EVENT_BATCH_MAX_EVENTS {
            return Err(HostError::validation(
                "limit",
                format!(
                    "limit must be between 1 and {APPROVED_EVENT_BATCH_MAX_EVENTS} (got {resolved_limit})"
                ),
            ));
        }

        let session = self.verify_active_ready_session(approval_id).await?;
        let batch = session
            .scoped_client
            .pull_events_bounded(
                after_seq,
                Some(resolved_limit),
                APPROVED_EVENT_BATCH_MAX_BYTES,
            )
            .await?;

        if batch.binding_id != session.live_binding.binding_id {
            return Err(HostError::ScopeMismatch {
                field: "session_binding_id".to_string(),
                expected: session.live_binding.binding_id.clone(),
                actual: batch.binding_id,
                reason: "event batch returned mismatched session_binding_id".to_string(),
            });
        }

        Ok(batch)
    }

    /// Acknowledges a previously delivered event batch (`batch_id`) on the approved session's
    /// runtime binding, advancing `acked_seq` and releasing or re-arming wake-up coverage.
    ///
    /// - Derives a deterministic idempotency command key internally from the approval's
    ///   `subject_digest`, `session_binding_id`, and `batch_id` (no arbitrary caller keys).
    /// - Verifies active ready session state and original caller `control_binding_id` + `native_thread_uuid`
    ///   on every call.
    pub async fn ack_events(
        &self,
        approval_id: &str,
        batch_id: String,
    ) -> Result<AckResult, HostError> {
        validate_identifier("batch_id", &batch_id)?;
        let session = self.verify_active_ready_session(approval_id).await?;
        let command_key = derive_ack_command_key(
            &session.record.subject_digest,
            &session.live_binding.binding_id,
            &batch_id,
        );

        let ack = session
            .scoped_client
            .ack_events(command_key, batch_id)
            .await?;

        if ack.binding_id != session.live_binding.binding_id {
            return Err(HostError::ScopeMismatch {
                field: "session_binding_id".to_string(),
                expected: session.live_binding.binding_id.clone(),
                actual: ack.binding_id,
                reason: "ack result returned mismatched session_binding_id".to_string(),
            });
        }

        Ok(ack)
    }

    fn resolve_authorized_approval(&self, approval_id: &str) -> Result<ApprovalRecord, HostError> {
        validate_approval_id(approval_id)?;
        let record = self.authority.get_for_operator(approval_id)?;

        // Authoritative caller scope check BEFORE accessing the host session directory.
        if record.binding_id != self.control_binding_id {
            return Err(HostError::ScopeMismatch {
                field: "control_binding_id".to_string(),
                expected: self.control_binding_id.clone(),
                actual: record.binding_id,
                reason: format!(
                    "approval `{}` does not belong to caller control_binding_id `{}`",
                    record.approval_id, self.control_binding_id
                ),
            });
        }
        if record.thread_uuid != self.native_thread_uuid {
            return Err(HostError::ScopeMismatch {
                field: "thread_uuid".to_string(),
                expected: self.native_thread_uuid.clone(),
                actual: record.thread_uuid,
                reason: format!(
                    "approval `{}` does not belong to caller native thread_uuid `{}`",
                    record.approval_id, self.native_thread_uuid
                ),
            });
        }

        validate_sessions_root(
            &self.sessions_dir,
            &[
                ("authority_dir", self.authority.dir()),
                ("repository_path", record.repository_path.as_path()),
            ],
        )?;

        Ok(record)
    }

    async fn verify_active_ready_session(
        &self,
        approval_id: &str,
    ) -> Result<VerifiedSession, HostError> {
        let record = self.resolve_authorized_approval(approval_id)?;
        let session_dir = verify_existing_session_dir(&self.sessions_dir, &record.approval_id)?;

        let manifest = read_host_manifest_optional(&session_dir, &record)?.ok_or_else(|| {
            HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: None,
                reason: "host-manifest.json does not exist".to_string(),
            }
        })?;

        if manifest.phase != HostPhase::Ready || !manifest.gates.supervisor_authenticated_ready {
            return Err(HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: Some(manifest.phase),
                reason: manifest
                    .last_error
                    .clone()
                    .unwrap_or_else(|| format!("host session is in phase `{}`", manifest.phase)),
            });
        }

        if !is_host_lock_held(&session_dir)? {
            return Err(HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: Some(manifest.phase),
                reason: "host.lock is not held by an active host process".to_string(),
            });
        }

        let session_binding_id =
            manifest
                .session_binding_id
                .clone()
                .ok_or_else(|| HostError::CorruptManifest {
                    path: session_dir.display().to_string(),
                    reason: "ready manifest is missing session_binding_id".to_string(),
                })?;

        let runtime_state_dir =
            verify_existing_private_dir(&session_dir.join(RUNTIME_STATE_SUBDIR))?;
        let work_area_dir = verify_existing_private_dir(&session_dir.join(WORK_AREA_SUBDIR))?;

        let sup_client =
            SupervisorClient::from_state_dir(&runtime_state_dir).map_err(|err| match err {
                threadlane::Error::SecurityViolation(reason) => HostError::SecurityViolation {
                    path: runtime_state_dir.display().to_string(),
                    reason,
                },
                other => HostError::HostNotReady {
                    approval_id: record.approval_id.clone(),
                    phase: Some(manifest.phase),
                    reason: format!("supervisor descriptor unavailable: {other}"),
                },
            })?;

        let live_binding = sup_client
            .get_binding(&session_binding_id)
            .await
            .map_err(|err| match err {
                threadlane::Error::Transport(msg) => HostError::HostNotReady {
                    approval_id: record.approval_id.clone(),
                    phase: Some(manifest.phase),
                    reason: format!("supervisor IPC unreachable: {msg}"),
                },
                other => other.into(),
            })?;

        if live_binding.binding_id != session_binding_id {
            return Err(HostError::ScopeMismatch {
                field: "session_binding_id".to_string(),
                expected: session_binding_id,
                actual: live_binding.binding_id,
                reason: "live supervisor session_binding_id mismatch".to_string(),
            });
        }
        if live_binding.thread_uuid != self.native_thread_uuid {
            return Err(HostError::ScopeMismatch {
                field: "thread_uuid".to_string(),
                expected: self.native_thread_uuid.clone(),
                actual: live_binding.thread_uuid,
                reason: "live supervisor thread_uuid mismatch".to_string(),
            });
        }

        let scoped_client = ScopedClient::from_state_dir(&runtime_state_dir, &session_binding_id)?;

        Ok(VerifiedSession {
            record,
            session_dir,
            work_area_dir,
            live_binding,
            scoped_client,
        })
    }
}

fn build_task_snapshot(
    record: &ApprovalRecord,
    session_binding_id: &str,
    task: &ApprovedTaskRecord,
    intent: Option<&TaskDispatchIntent>,
    receipt: Option<&TaskRunReceipt>,
    worker: Option<WorkerSnapshot>,
) -> TaskStateSnapshot {
    let status = match (intent, receipt, &worker) {
        (_, _, Some(w)) => TaskExecutionStatus::from_worker_snapshot(w),
        (_, Some(_), None) | (Some(_), None, None) => TaskExecutionStatus::Unknown,
        (None, None, None) => TaskExecutionStatus::NotStarted,
    };

    TaskStateSnapshot {
        approval_id: record.approval_id.clone(),
        subject_digest: record.subject_digest.clone(),
        binding_id: record.binding_id.clone(),
        thread_uuid: record.thread_uuid.clone(),
        session_binding_id: session_binding_id.to_string(),
        task_id: task.task_id.clone(),
        task_digest: task.task_digest.clone(),
        adapter: task.adapter,
        provider: task.provider,
        depends_on: task.depends_on.clone(),
        profile_id: task.profile_id.clone(),
        command_key: task.command_key.clone(),
        status,
        run_id: receipt.map(|r| r.run_id.clone()),
        worker_id: receipt.map(|r| r.worker_id.clone()),
        worker,
        accepted: false,
    }
}
