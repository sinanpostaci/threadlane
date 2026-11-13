use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use threadlane::presence::{
    WorkerPresenceAvailability, WorkerPresenceFreshness, WorkerPresenceSnapshot,
};
use threadlane::{
    resolve_worker_execution, ActivityPage, ErrorCode as RuntimeErrorCode, EventKind,
    HostControlInfoRequest, HostControlOwnerScope, HostControlRuntimeState,
    HostControlStatusResponse, RearmHostWakeupRequest, ScopedClient, StopIdleHostRequest,
    SupervisorClient, ValidatedWorkersConfig, WorkerAcceptanceState, WorkerSnapshot, WorkerStatus,
};
use threadlane_authority::{
    validate_executable_worker_instruction, ApprovalRecord, ApprovedTaskRecord, AuthorityStore,
};
use threadlane_core::{
    observation::ReadonlySqliteError, AckResult, Binding, Core, CoreError, EventBatch,
    WorkerExecutionBinding, DEFAULT_PAGE_LIMIT,
};
use threadlane_operations::{
    inspect_dependency_input_seal_readonly, prepare_and_seal_dependency_input,
    verify_dependency_predecessor_proof, DependencyPredecessorCandidate, OperationsError,
};
use threadlane_plans::PlanStore;
use threadlane_registry::RegistryStore;
use threadlane_workers::{
    is_commit_ancestor, load_and_verify_manifest, read_dependency_input, DependencyInputSeal,
    DependencyPredecessorInput, WorktreeKind, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};

use crate::delivery::{
    inspect_delivery_for_verified_task, recheck_delivery_for_verified_task, DeliveryInspection,
};
use crate::error::HostError;
use crate::lifecycle::project_task_lifecycle;
use crate::storage::{
    build_validated_task_profile, derive_ack_command_key, derive_cancel_command_key,
    ensure_no_symlink_components, is_host_lock_held, now_epoch_ms, read_host_manifest_optional,
    read_session_task_ledger_readonly, read_task_ledger_entry, record_task_dispatch_intent,
    record_task_receipt, validate_approval_id, validate_canonical_thread_uuid,
    validate_existing_sessions_root, validate_hex_digest, validate_host_instance_id,
    validate_identifier, validate_sessions_root, verify_all_transitive_predecessors_in_approval,
    verify_existing_private_dir, verify_existing_session_dir,
};
use crate::types::{
    DependencyInputSummary, DependencyPredecessorSelection, HostNotificationDeliveryStatus,
    HostNotificationRearmReport, HostNotificationStatusReport, HostPhase, HostSessionManifest,
    HostSessionSummary, PrepareDependencyInputsRequest, TaskDispatchIntent, TaskExecutionStatus,
    TaskRunReceipt, TaskStateSnapshot, APPROVED_EVENT_BATCH_MAX_BYTES,
    APPROVED_EVENT_BATCH_MAX_EVENTS, HOST_LOCK_FILENAME, RUNTIME_STATE_SUBDIR, WORK_AREA_SUBDIR,
};

const STOP_CONFIRM_TIMEOUT: Duration = Duration::from_secs(5);
const STOP_CONFIRM_POLL_INTERVAL: Duration = Duration::from_millis(25);

struct VerifiedSession {
    record: ApprovalRecord,
    manifest: HostSessionManifest,
    session_dir: PathBuf,
    work_area_dir: PathBuf,
    live_binding: Binding,
    scoped_client: ScopedClient,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadySessionAccess {
    /// Existing active operations preserve their blocking `host.lock` ownership probe and
    /// `HostNotReady` errors.
    Active,
    /// Presence reads use only noncreating probes and map normal non-live conditions to
    /// `Ok(None)` instead of `HostNotReady`.
    ReadonlyPresence,
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

    /// Returns a safe, token-free, path-free [`HostSessionSummary`] for `approval_id` with
    /// additive live (or truthful unavailable) [`HostNotificationDeliveryStatus`] populated in
    /// `summary.notification_delivery`.
    pub async fn session_summary_with_notifications(
        &self,
        approval_id: &str,
    ) -> Result<HostSessionSummary, HostError> {
        let mut summary = self.session_summary(approval_id)?;
        let notif_report = self.notification_status(approval_id).await?;
        summary.notification_delivery = Some(notif_report.delivery);
        Ok(summary)
    }

    /// Queries bounded, path-free, secret-free notification delivery status for `approval_id`.
    ///
    /// - Validates `approval_id`, caller `control_binding_id` and `native_thread_uuid`, private
    ///   session directories, and `host-manifest.json`.
    /// - When the host is live (`Ready` or `Stopping` with `host.lock` held and `instance_id`
    ///   present), verifies live supervisor owner scope + `instance_id` via `host_control_info` and
    ///   fetches authenticated scoped runtime `status` (never reading SQLite directly or exposing
    ///   bearer tokens, paths, or raw error strings).
    /// - When the host is stopped, failed, bootstrapping, unlocked, or unreachable, reports
    ///   `availability = unavailable` and `wakeup_status = unavailable` (never faking `Idle`).
    pub async fn notification_status(
        &self,
        approval_id: &str,
    ) -> Result<HostNotificationStatusReport, HostError> {
        let record = self.resolve_authorized_approval(approval_id)?;
        let session_dir = verify_existing_session_dir(&self.sessions_dir, &record.approval_id)?;
        let manifest = read_host_manifest_optional(&session_dir, &record)?.ok_or_else(|| {
            HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: None,
                reason: "host-manifest.json does not exist".to_string(),
            }
        })?;

        let runtime_candidate = session_dir.join(RUNTIME_STATE_SUBDIR);
        if std::fs::symlink_metadata(&runtime_candidate).is_ok() {
            let _ = verify_existing_private_dir(&runtime_candidate)?;
        }
        let work_area_candidate = session_dir.join(WORK_AREA_SUBDIR);
        if std::fs::symlink_metadata(&work_area_candidate).is_ok() {
            let _ = verify_existing_private_dir(&work_area_candidate)?;
        }

        let lock_held = is_host_lock_held(&session_dir)?;
        let build_report =
            |delivery: HostNotificationDeliveryStatus| HostNotificationStatusReport {
                approval_id: record.approval_id.clone(),
                subject_digest: record.subject_digest.clone(),
                binding_id: record.binding_id.clone(),
                thread_uuid: record.thread_uuid.clone(),
                session_binding_id: manifest.session_binding_id.clone(),
                instance_id: manifest.instance_id.clone(),
                phase: manifest.phase,
                notifications: manifest.notifications.clone(),
                delivery,
            };

        if !lock_held
            || !matches!(manifest.phase, HostPhase::Ready | HostPhase::Stopping)
            || (manifest.phase == HostPhase::Ready
                && !manifest.gates.supervisor_authenticated_ready)
        {
            return Ok(build_report(HostNotificationDeliveryStatus::unavailable()));
        }

        let session_binding_id =
            manifest
                .session_binding_id
                .clone()
                .ok_or_else(|| HostError::CorruptManifest {
                    path: session_dir.display().to_string(),
                    reason: format!("{} manifest is missing session_binding_id", manifest.phase),
                })?;

        let Some(ref instance_id) = manifest.instance_id else {
            return Ok(build_report(HostNotificationDeliveryStatus::unavailable()));
        };

        let runtime_state_dir =
            verify_existing_private_dir(&session_dir.join(RUNTIME_STATE_SUBDIR))?;
        let _work_area_dir = verify_existing_private_dir(&session_dir.join(WORK_AREA_SUBDIR))?;
        let sup_client = match SupervisorClient::from_state_dir(&runtime_state_dir) {
            Ok(client) => client,
            Err(threadlane::Error::SecurityViolation(reason)) => {
                return Err(HostError::SecurityViolation {
                    path: runtime_state_dir.display().to_string(),
                    reason,
                });
            }
            Err(_) => {
                return Ok(build_report(HostNotificationDeliveryStatus::unavailable()));
            }
        };

        let info = match sup_client
            .host_control_info(&HostControlInfoRequest {
                approval_id: record.approval_id.clone(),
                subject_digest: record.subject_digest.clone(),
                control_binding_id: self.control_binding_id.clone(),
                thread_uuid: self.native_thread_uuid.clone(),
                session_binding_id: session_binding_id.clone(),
                expected_instance_id: instance_id.clone(),
            })
            .await
        {
            Ok(info) => info,
            Err(threadlane::Error::Remote(api_err))
                if api_err.code == RuntimeErrorCode::HostControlNotConfigured =>
            {
                return Ok(build_report(HostNotificationDeliveryStatus::unavailable()));
            }
            Err(threadlane::Error::Transport(_)) => {
                return Ok(build_report(HostNotificationDeliveryStatus::unavailable()));
            }
            Err(other) => return Err(other.into()),
        };

        if info.approval_id != record.approval_id
            || info.subject_digest != record.subject_digest
            || info.control_binding_id != self.control_binding_id
            || info.thread_uuid != self.native_thread_uuid
            || info.session_binding_id != session_binding_id
            || &info.instance_id != instance_id
        {
            return Err(HostError::CorruptSessionState {
                reason: "live supervisor host_control_info returned mismatched owner scope"
                    .to_string(),
            });
        }

        let scoped_client =
            match ScopedClient::from_state_dir(&runtime_state_dir, &session_binding_id) {
                Ok(client) => client,
                Err(threadlane::Error::SecurityViolation(reason)) => {
                    return Err(HostError::SecurityViolation {
                        path: runtime_state_dir.display().to_string(),
                        reason,
                    });
                }
                Err(_) => {
                    return Ok(build_report(HostNotificationDeliveryStatus::unavailable()));
                }
            };

        let status = match scoped_client.status().await {
            Ok(st) => st,
            Err(threadlane::Error::Transport(_)) => {
                return Ok(build_report(HostNotificationDeliveryStatus::unavailable()));
            }
            Err(other) => return Err(other.into()),
        };

        if status.binding.binding_id != session_binding_id
            || status.wakeup.binding_id != session_binding_id
        {
            return Err(HostError::ScopeMismatch {
                field: "session_binding_id".to_string(),
                expected: session_binding_id,
                actual: status.binding.binding_id,
                reason: "authenticated scoped status returned mismatched session_binding_id"
                    .to_string(),
            });
        }
        if status.binding.thread_uuid != self.native_thread_uuid
            || status.wakeup.thread_uuid != self.native_thread_uuid
        {
            return Err(HostError::ScopeMismatch {
                field: "thread_uuid".to_string(),
                expected: self.native_thread_uuid.clone(),
                actual: status.binding.thread_uuid,
                reason: "authenticated scoped status returned mismatched thread_uuid".to_string(),
            });
        }

        let mut delivery =
            HostNotificationDeliveryStatus::from_live(&status.binding, &status.wakeup);
        delivery.rearm_eligible &= manifest.notifications.enabled;
        Ok(build_report(delivery))
    }

    /// Explicitly re-arms a `WakeupStatus::Failed` notification claim matching `expected_claim_id`
    /// on an active `Ready` host session after verifying `confirm_approval_digest` against the
    /// frozen approval's `subject_digest`.
    ///
    /// - Strictly limited to `WakeupStatus::Failed`: `WakeupStatus::Unknown` and all other wake-up
    ///   statuses are refused with [`HostError::InvalidWakeupState`].
    /// - Compare-and-swap checked on `expected_claim_id` and the live host `instance_id` inside the
    ///   supervisor's core transaction so stale or duplicate requests are deterministically refused.
    pub async fn rearm_failed_notification(
        &self,
        approval_id: &str,
        expected_claim_id: &str,
        confirm_approval_digest: &str,
    ) -> Result<HostNotificationRearmReport, HostError> {
        self.rearm_failed_notification_with_instance(
            approval_id,
            expected_claim_id,
            confirm_approval_digest,
            None,
        )
        .await
    }

    /// Explicitly re-arms a `WakeupStatus::Failed` notification claim matching `expected_claim_id`
    /// on an active `Ready` host session, optionally pinning an caller-supplied `expected_instance_id`.
    pub async fn rearm_failed_notification_with_instance(
        &self,
        approval_id: &str,
        expected_claim_id: &str,
        confirm_approval_digest: &str,
        expected_instance_id: Option<&str>,
    ) -> Result<HostNotificationRearmReport, HostError> {
        let record = self.resolve_authorized_approval(approval_id)?;
        validate_identifier("expected_claim_id", expected_claim_id)?;
        validate_hex_digest("confirm_approval", confirm_approval_digest)?;
        if let Some(exp_inst) = expected_instance_id {
            validate_host_instance_id("expected_instance_id", exp_inst)?;
        }

        if !constant_time_digest_eq(&record.subject_digest, confirm_approval_digest) {
            return Err(HostError::ScopeMismatch {
                field: "confirm_approval".to_string(),
                expected: record.subject_digest.clone(),
                actual: confirm_approval_digest.to_string(),
                reason: format!(
                    "confirm_approval digest does not match frozen subject_digest for approval `{}`",
                    record.approval_id
                ),
            });
        }

        let session_dir = verify_existing_session_dir(&self.sessions_dir, &record.approval_id)?;
        let manifest = read_host_manifest_optional(&session_dir, &record)?.ok_or_else(|| {
            HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: None,
                reason: "host-manifest.json does not exist".to_string(),
            }
        })?;

        if let Some(exp_inst) = expected_instance_id {
            if manifest.instance_id.as_deref() != Some(exp_inst) {
                return Err(HostError::InstanceMismatch {
                    approval_id: record.approval_id.clone(),
                    expected_instance_id: exp_inst.to_string(),
                    actual_instance_id: manifest.instance_id.clone(),
                });
            }
        }

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

        let Some(ref instance_id) = manifest.instance_id else {
            return Err(HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: Some(manifest.phase),
                reason: "ready manifest is missing instance_id".to_string(),
            });
        };

        let session_binding_id =
            manifest
                .session_binding_id
                .clone()
                .ok_or_else(|| HostError::CorruptManifest {
                    path: session_dir.display().to_string(),
                    reason: "ready manifest is missing session_binding_id".to_string(),
                })?;

        if !manifest.notifications.enabled {
            return Err(HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: Some(manifest.phase),
                reason: "host session notifications are disabled".to_string(),
            });
        }

        let runtime_state_dir =
            verify_existing_private_dir(&session_dir.join(RUNTIME_STATE_SUBDIR))?;
        let _work_area_dir = verify_existing_private_dir(&session_dir.join(WORK_AREA_SUBDIR))?;

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

        let resp = match sup_client
            .rearm_host_wakeup(&RearmHostWakeupRequest {
                scope: HostControlOwnerScope {
                    approval_id: record.approval_id.clone(),
                    subject_digest: record.subject_digest.clone(),
                    control_binding_id: self.control_binding_id.clone(),
                    thread_uuid: self.native_thread_uuid.clone(),
                    session_binding_id: session_binding_id.clone(),
                    expected_instance_id: instance_id.clone(),
                },
                expected_claim_id: expected_claim_id.to_string(),
            })
            .await
        {
            Ok(resp) => resp,
            Err(threadlane::Error::Remote(api_err))
                if matches!(
                    api_err.code,
                    RuntimeErrorCode::HostStopping
                        | RuntimeErrorCode::HostControlNotConfigured
                        | RuntimeErrorCode::InvalidConfig
                ) =>
            {
                return Err(HostError::HostNotReady {
                    approval_id: record.approval_id.clone(),
                    phase: Some(manifest.phase),
                    reason: api_err.message,
                });
            }
            Err(threadlane::Error::Transport(_)) => {
                return Err(HostError::MutationOutcomeUncertain {
                    operation: "rearm_failed_notification".to_string(),
                    detail: "rearm_host_wakeup transport outcome uncertain; inspect notification status before retrying"
                        .to_string(),
                });
            }
            Err(other) => return Err(other.into()),
        };

        if &resp.instance_id != instance_id
            || resp.binding.binding_id != session_binding_id
            || resp.wakeup.binding_id != session_binding_id
            || resp.binding.thread_uuid != self.native_thread_uuid
            || resp.wakeup.thread_uuid != self.native_thread_uuid
        {
            return Err(HostError::MutationOutcomeUncertain {
                operation: "rearm_failed_notification".to_string(),
                detail: "rearm_host_wakeup returned mismatched session or instance scope; inspect notification status before retrying"
                    .to_string(),
            });
        }

        let mut delivery = HostNotificationDeliveryStatus::from_live(&resp.binding, &resp.wakeup);
        delivery.rearm_eligible &= manifest.notifications.enabled;
        Ok(HostNotificationRearmReport {
            approval_id: record.approval_id,
            subject_digest: record.subject_digest,
            binding_id: record.binding_id,
            thread_uuid: record.thread_uuid,
            session_binding_id,
            instance_id: instance_id.clone(),
            phase: manifest.phase,
            notifications: manifest.notifications,
            rearmed_from_claim_id: expected_claim_id.to_string(),
            delivery,
        })
    }

    /// Queries live authenticated host control status (`Ready` or `Stopping` + idle/busy counts)
    /// for `approval_id` after verifying the approval record, caller authority
    /// (`control_binding_id` and `native_thread_uuid`), and manifest integrity.
    ///
    /// Returns `Ok(None)` when live control is unavailable by design: legacy manifests without an
    /// `instance_id` (read-only compatibility; lifecycle actions stay unsupported), lock-free
    /// `Stopped`/`Failed` sessions with no live control, or a running supervisor without host
    /// control configured. `Ready`/`Stopping` without a held `host.lock`, `Bootstrapping`, and other
    /// non-live phases remain `HostNotReady`; this method never fabricates readiness or stop proof.
    pub async fn host_control_status(
        &self,
        approval_id: &str,
    ) -> Result<Option<HostControlStatusResponse>, HostError> {
        let record = self.resolve_authorized_approval(approval_id)?;
        let session_dir = verify_existing_session_dir(&self.sessions_dir, &record.approval_id)?;
        let manifest = read_host_manifest_optional(&session_dir, &record)?.ok_or_else(|| {
            HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: None,
                reason: "host-manifest.json does not exist".to_string(),
            }
        })?;

        let Some(ref instance_id) = manifest.instance_id else {
            return Ok(None);
        };

        let lock_held = is_host_lock_held(&session_dir)?;
        if !lock_held && matches!(manifest.phase, HostPhase::Stopped | HostPhase::Failed) {
            return Ok(None);
        }

        if !lock_held || !matches!(manifest.phase, HostPhase::Ready | HostPhase::Stopping) {
            return Err(HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: Some(manifest.phase),
                reason: manifest
                    .last_error
                    .clone()
                    .unwrap_or_else(|| format!("host session is in phase `{}`", manifest.phase)),
            });
        }

        let session_binding_id =
            manifest
                .session_binding_id
                .clone()
                .ok_or_else(|| HostError::CorruptManifest {
                    path: session_dir.display().to_string(),
                    reason: format!("{} manifest is missing session_binding_id", manifest.phase),
                })?;

        let runtime_state_dir =
            verify_existing_private_dir(&session_dir.join(RUNTIME_STATE_SUBDIR))?;
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

        let info = match sup_client
            .host_control_info(&HostControlInfoRequest {
                approval_id: record.approval_id.clone(),
                subject_digest: record.subject_digest.clone(),
                control_binding_id: self.control_binding_id.clone(),
                thread_uuid: self.native_thread_uuid.clone(),
                session_binding_id: session_binding_id.clone(),
                expected_instance_id: instance_id.clone(),
            })
            .await
        {
            Ok(info) => info,
            Err(threadlane::Error::Remote(api_err))
                if api_err.code == RuntimeErrorCode::HostControlNotConfigured =>
            {
                return Ok(None);
            }
            Err(threadlane::Error::Transport(msg)) => {
                return Err(HostError::HostNotReady {
                    approval_id: record.approval_id.clone(),
                    phase: Some(manifest.phase),
                    reason: format!("supervisor IPC unreachable: {msg}"),
                });
            }
            Err(other) => return Err(other.into()),
        };

        if info.approval_id != record.approval_id
            || info.subject_digest != record.subject_digest
            || info.control_binding_id != self.control_binding_id
            || info.thread_uuid != self.native_thread_uuid
            || info.session_binding_id != session_binding_id
            || &info.instance_id != instance_id
        {
            return Err(HostError::CorruptSessionState {
                reason: "live supervisor host_control_info returned mismatched owner scope"
                    .to_string(),
            });
        }

        Ok(Some(info))
    }

    /// Requests an authenticated, owner-scoped idle stop for `approval_id` at `expected_instance_id`
    /// and waits for durable `HostPhase::Stopped` + released `host.lock` proof (`5s` timeout, `25ms` poll).
    pub async fn stop_idle_session(
        &self,
        approval_id: &str,
        expected_instance_id: &str,
    ) -> Result<HostSessionSummary, HostError> {
        let record = self.resolve_authorized_approval(approval_id)?;
        validate_host_instance_id("expected_instance_id", expected_instance_id)?;

        let session_dir = verify_existing_session_dir(&self.sessions_dir, &record.approval_id)?;
        let manifest = read_host_manifest_optional(&session_dir, &record)?.ok_or_else(|| {
            HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: None,
                reason: "host-manifest.json does not exist".to_string(),
            }
        })?;

        if manifest.instance_id.as_deref() != Some(expected_instance_id) {
            return Err(HostError::InstanceMismatch {
                approval_id: record.approval_id.clone(),
                expected_instance_id: expected_instance_id.to_string(),
                actual_instance_id: manifest.instance_id.clone(),
            });
        }

        let lock_held = is_host_lock_held(&session_dir)?;
        if manifest.phase == HostPhase::Stopped && !lock_held {
            return Ok(manifest.to_summary());
        }

        if !lock_held || !matches!(manifest.phase, HostPhase::Ready | HostPhase::Stopping) {
            return Err(HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: Some(manifest.phase),
                reason: manifest.last_error.clone().unwrap_or_else(|| {
                    format!(
                        "host session is in phase `{}` (lock_held={lock_held})",
                        manifest.phase
                    )
                }),
            });
        }

        let session_binding_id =
            manifest
                .session_binding_id
                .clone()
                .ok_or_else(|| HostError::CorruptManifest {
                    path: session_dir.display().to_string(),
                    reason: format!("{} manifest is missing session_binding_id", manifest.phase),
                })?;

        let runtime_state_dir =
            verify_existing_private_dir(&session_dir.join(RUNTIME_STATE_SUBDIR))?;

        match SupervisorClient::from_state_dir(&runtime_state_dir) {
            Ok(sup_client) => {
                let stop_req = StopIdleHostRequest {
                    approval_id: record.approval_id.clone(),
                    subject_digest: record.subject_digest.clone(),
                    control_binding_id: self.control_binding_id.clone(),
                    thread_uuid: self.native_thread_uuid.clone(),
                    session_binding_id: session_binding_id.clone(),
                    expected_instance_id: expected_instance_id.to_string(),
                };
                match sup_client.stop_idle_host(&stop_req).await {
                    Ok(resp) => {
                        if resp.approval_id != record.approval_id
                            || resp.subject_digest != record.subject_digest
                            || resp.control_binding_id != self.control_binding_id
                            || resp.thread_uuid != self.native_thread_uuid
                            || resp.session_binding_id != session_binding_id
                            || resp.instance_id != expected_instance_id
                            || resp.state != HostControlRuntimeState::Stopping
                        {
                            return Err(post_stop_outcome_uncertain(
                                &record.approval_id,
                                "stop_idle_host response did not match Stopping owner scope",
                            ));
                        }
                    }
                    Err(threadlane::Error::Remote(api_err)) => match api_err.code {
                        RuntimeErrorCode::HostNotIdle => {
                            return Err(HostError::HostNotIdle {
                                approval_id: record.approval_id.clone(),
                                detail: api_err.message,
                            });
                        }
                        RuntimeErrorCode::HostInstanceMismatch
                        | RuntimeErrorCode::HostScopeMismatch => {
                            return Err(HostError::from(threadlane::Error::Remote(api_err)));
                        }
                        RuntimeErrorCode::HostControlNotConfigured => {
                            return Err(HostError::HostNotReady {
                                approval_id: record.approval_id.clone(),
                                phase: Some(manifest.phase),
                                reason: api_err.message,
                            });
                        }
                        RuntimeErrorCode::SecurityViolation => {
                            return Err(HostError::SecurityViolation {
                                path: runtime_state_dir.display().to_string(),
                                reason: api_err.message,
                            });
                        }
                        _ => {
                            // Fall through to bounded durable confirmation loop if the supervisor
                            // retired concurrently while draining.
                        }
                    },
                    Err(threadlane::Error::SecurityViolation(reason)) => {
                        return Err(HostError::SecurityViolation {
                            path: runtime_state_dir.display().to_string(),
                            reason,
                        });
                    }
                    Err(threadlane::Error::Transport(_)) => {
                        // A lost HTTP response or connection closed mid-shutdown may only be
                        // reconciled by durable `Stopped` + released `host.lock` + matching
                        // `expected_instance_id` in the bounded confirmation loop below.
                    }
                    Err(_) => {
                        // Any other post-request client or decoding failure after submitting
                        // `stop_idle_host` is reconciled only via durable proof below.
                    }
                }
            }
            Err(threadlane::Error::SecurityViolation(reason)) => {
                return Err(HostError::SecurityViolation {
                    path: runtime_state_dir.display().to_string(),
                    reason,
                });
            }
            Err(_) => {
                // Descriptor may have been removed mid-shutdown; reconcile only via durable proof below.
            }
        }

        let deadline = tokio::time::Instant::now() + STOP_CONFIRM_TIMEOUT;
        loop {
            let current_manifest =
                read_host_manifest_optional(&session_dir, &record).map_err(|_| {
                    post_stop_outcome_uncertain(
                        &record.approval_id,
                        "failed to verify host session manifest after stop request",
                    )
                })?;
            let current_lock_held = is_host_lock_held(&session_dir).map_err(|_| {
                post_stop_outcome_uncertain(
                    &record.approval_id,
                    "failed to verify host session lock after stop request",
                )
            })?;

            if let Some(m) = current_manifest {
                if classify_post_stop_manifest(
                    &record.approval_id,
                    expected_instance_id,
                    m.instance_id.as_deref(),
                    m.phase,
                    current_lock_held,
                )? {
                    return Ok(m.to_summary());
                }
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(post_stop_outcome_uncertain(
                    &record.approval_id,
                    "timed out waiting for durable Stopped manifest and released host.lock",
                ));
            }

            tokio::time::sleep(STOP_CONFIRM_POLL_INTERVAL).await;
        }
    }

    /// Verifies receipt-proven predecessor outputs and seals a dependent task's deterministic
    /// dependency input commit and [`DependencyInputSeal`].
    pub async fn prepare_dependency_inputs(
        &self,
        request: PrepareDependencyInputsRequest,
    ) -> Result<DependencyInputSummary, HostError> {
        validate_identifier("task_id", &request.task_id)?;
        validate_hex_digest(
            "expected_approval_digest",
            &request.expected_approval_digest,
        )?;
        let session = self
            .verify_active_ready_session(&request.approval_id)
            .await?;

        if request.expected_approval_digest != session.record.subject_digest {
            return Err(HostError::ScopeMismatch {
                field: "expected_approval_digest".to_string(),
                expected: session.record.subject_digest.clone(),
                actual: request.expected_approval_digest,
                reason: format!(
                    "expected_approval_digest does not match frozen subject_digest for approval `{}`",
                    session.record.approval_id
                ),
            });
        }

        let task = session
            .record
            .tasks
            .iter()
            .find(|t| t.task_id == request.task_id)
            .ok_or_else(|| HostError::UnknownTask {
                approval_id: session.record.approval_id.clone(),
                task_id: request.task_id.clone(),
            })?;

        if !task.is_frozen_headless_worker() {
            return Err(HostError::NativeRouteRequired {
                approval_id: session.record.approval_id.clone(),
                task_id: task.task_id.clone(),
                adapter: task.adapter,
            });
        }

        if task.depends_on.is_empty() {
            return Err(HostError::validation(
                "task_id",
                format!(
                    "task `{}` is a root task (depends_on is empty) and does not use dependency inputs",
                    task.task_id
                ),
            ));
        }

        if session.manifest.gates.is_legacy_root_only() {
            return Err(HostError::DependencyInputsUnverified {
                approval_id: session.record.approval_id.clone(),
                task_id: task.task_id.clone(),
                depends_on: task.depends_on.clone(),
            });
        }

        // Verify that the frozen ApprovalRecord includes all direct AND transitive predecessors
        // as headless worker tasks. Legacy partial approvals fail closed with missing predecessor IDs.
        if verify_all_transitive_predecessors_in_approval(&session.record, &task.task_id).is_err() {
            let missing_ids =
                collect_missing_predecessors_in_approval(&session.record, &task.task_id);
            return Err(HostError::DependencyInputsUnverified {
                approval_id: session.record.approval_id.clone(),
                task_id: task.task_id.clone(),
                depends_on: if missing_ids.is_empty() {
                    task.depends_on.clone()
                } else {
                    missing_ids
                },
            });
        }

        // Verify that the active source plan is still Published and materialize the frozen profiles.
        let materialized = self.authority.materialize(
            &session.record.approval_id,
            &session.live_binding,
            &session.work_area_dir,
        )?;
        let workers_cfg = materialized
            .workers
            .ok_or_else(|| HostError::CorruptSessionState {
                reason: format!(
                    "materialized plan for approval `{}` has no headless worker profiles",
                    session.record.approval_id
                ),
            })?;
        let validated_workers = workers_cfg.validate()?;

        // Require `request.predecessors` to match `task.depends_on` as an exact duplicate-free set.
        let mut selection_by_task: BTreeMap<String, DependencyPredecessorSelection> =
            BTreeMap::new();
        for sel in &request.predecessors {
            validate_identifier("task_id", &sel.task_id)?;
            validate_identifier("review_receipt_id", &sel.review_receipt_id)?;
            validate_identifier("integration_receipt_id", &sel.integration_receipt_id)?;
            if selection_by_task
                .insert(sel.task_id.clone(), sel.clone())
                .is_some()
            {
                return Err(HostError::validation(
                    "predecessors",
                    format!(
                        "duplicate predecessor task_id `{}` in prepare_dependency_inputs request",
                        sel.task_id
                    ),
                ));
            }
        }

        let expected_preds: BTreeSet<String> = task.depends_on.iter().cloned().collect();
        let actual_preds: BTreeSet<String> = selection_by_task.keys().cloned().collect();
        if expected_preds != actual_preds || selection_by_task.len() != task.depends_on.len() {
            return Err(HostError::validation(
                "predecessors",
                format!(
                    "predecessors selection {:?} does not match task `{}` depends_on {:?}",
                    actual_preds, task.task_id, task.depends_on
                ),
            ));
        }

        let mut visiting = BTreeSet::new();
        let mut memo = BTreeMap::new();
        let mut predecessor_inputs = Vec::with_capacity(selection_by_task.len());

        for (pred_task_id, sel) in &selection_by_task {
            let proof = self
                .verify_predecessor_closure_member(
                    &session,
                    &validated_workers,
                    pred_task_id,
                    Some(sel),
                    &mut visiting,
                    &mut memo,
                )
                .await?;
            predecessor_inputs.push(proof);
        }

        let profile_id =
            task.profile_id
                .as_deref()
                .ok_or_else(|| HostError::CorruptSessionState {
                    reason: format!(
                        "dependent worker task `{}` is missing profile_id",
                        task.task_id
                    ),
                })?;
        let validated_profile = validated_workers.profiles.get(profile_id).ok_or_else(|| {
            HostError::CorruptSessionState {
                reason: format!(
                    "validated profile `{profile_id}` is missing from materialized config"
                ),
            }
        })?;
        let policy = validated_profile
            .dependency_input
            .as_ref()
            .ok_or_else(|| HostError::CorruptSessionState {
                reason: format!(
                    "validated profile `{profile_id}` for dependent task `{}` is missing dependency_input policy",
                    task.task_id
                ),
            })?;

        let seal = prepare_and_seal_dependency_input(
            policy,
            &session.record.repository_path,
            &session.work_area_dir,
            &session.record.base_commit,
            &task.instruction,
            predecessor_inputs,
        )?;

        Ok(DependencyInputSummary::from_seal(&seal))
    }

    /// Dispatches an approved root or prepared dependent headless worker task (`antigravity` or
    /// `opencode_v2`) using only `(approval_id, task_id)`.
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

        if !task.is_frozen_headless_worker() {
            return Err(HostError::NativeRouteRequired {
                approval_id: session.record.approval_id.clone(),
                task_id: task.task_id.clone(),
                adapter: task.adapter,
            });
        }

        // 1. Read `read_task_ledger_entry` FIRST (after task lookup and native-route check).
        let (existing_intent, existing_receipt) = read_task_ledger_entry(
            &session.session_dir,
            &session.record,
            &session.live_binding.binding_id,
            task_id,
        )?;

        let materialized = self.authority.materialize(
            &session.record.approval_id,
            &session.live_binding,
            &session.work_area_dir,
        )?;
        let workers_cfg = materialized
            .workers
            .ok_or_else(|| HostError::CorruptSessionState {
                reason: format!(
                    "materialized plan for approval `{}` has no headless worker profiles",
                    session.record.approval_id
                ),
            })?;
        let validated_workers = workers_cfg.validate()?;

        let dispatch = materialized
            .task_dispatch
            .iter()
            .find(|d| d.task_id == task_id)
            .ok_or_else(|| HostError::CorruptSessionState {
                reason: format!(
                    "materialized plan is missing dispatch entry for worker task `{task_id}`"
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

        let validated_profile = validated_workers
            .profiles
            .get(&dispatch.profile_id)
            .ok_or_else(|| HostError::CorruptSessionState {
                reason: format!(
                    "materialized validated_workers is missing profile `{}`",
                    dispatch.profile_id
                ),
            })?;

        // A sealed receipt proves that admission already happened. Observe that exact run
        // instead of asking the runtime to admit it again against mutable predecessor state.
        if existing_receipt.is_some() {
            return self.get_task(approval_id, task_id).await;
        }

        // Reject oversized, NUL-containing, or empty executable worker instructions as a typed
        // pre-effect validation refusal ONLY when no prior dispatch intent exists. If a prior
        // dispatch intent already exists without a sealed receipt, its outcome remains uncertain
        // (`Unknown`) and must never be reclassified as a fresh pre-effect refusal.
        if let Err(err) = validate_executable_worker_instruction(&dispatch.instruction) {
            if existing_intent.is_some() {
                return Err(HostError::MutationOutcomeUncertain {
                    operation: "dispatch_task".to_string(),
                    detail: format!(
                        "pre-existing dispatch intent without sealed receipt for task `{task_id}` remains outcome-uncertain; refusing redispatch on invalid executable instruction"
                    ),
                });
            }
            return Err(err.into());
        }

        // 2. Determine the dispatch intent to commit/replay:
        //    - For root tasks (`task.depends_on.is_empty()`), reuse an existing pending intent if
        //      present, or create a new root intent with `execution_binding: None`.
        //    - For dependent tasks without a sealed receipt (including retries of a pending
        //      intent), always enforce non-legacy gates, full predecessor closure in the approval
        //      record, `resolve_worker_execution` (retaining the shared seal lease through the
        //      dispatch response), full `execution_binding` equality against any existing pending
        //      intent (including raw seal byte digest), and live predecessor receipts/worktrees
        //      before invoking runtime `start_worker`.
        let mut _resolved_execution_lease = None;
        let intent = if task.depends_on.is_empty() {
            if let Some(existing) = existing_intent {
                existing
            } else {
                TaskDispatchIntent {
                    task_id: task.task_id.clone(),
                    task_digest: dispatch.task_digest.clone(),
                    profile_id: dispatch.profile_id.clone(),
                    command_key: dispatch.command_key.clone(),
                    execution_binding: None,
                    recorded_at_ms: now_epoch_ms(),
                }
            }
        } else {
            if session.manifest.gates.is_legacy_root_only()
                || verify_all_transitive_predecessors_in_approval(&session.record, &task.task_id)
                    .is_err()
            {
                let missing_ids =
                    collect_missing_predecessors_in_approval(&session.record, &task.task_id);
                return Err(HostError::DependencyInputsUnverified {
                    approval_id: session.record.approval_id.clone(),
                    task_id: task.task_id.clone(),
                    depends_on: if missing_ids.is_empty() {
                        task.depends_on.clone()
                    } else {
                        missing_ids
                    },
                });
            }

            let resolved = match resolve_worker_execution(
                validated_profile,
                &dispatch.command_key,
                dispatch.instruction.clone(),
            ) {
                Ok(res) => res,
                Err(threadlane::Error::SecurityViolation(reason)) => {
                    return Err(HostError::SecurityViolation {
                        path: session.work_area_dir.display().to_string(),
                        reason,
                    });
                }
                Err(_) => {
                    return Err(HostError::DependencyInputsUnverified {
                        approval_id: session.record.approval_id.clone(),
                        task_id: task.task_id.clone(),
                        depends_on: task.depends_on.clone(),
                    });
                }
            };

            let Some(exec_binding) = resolved.execution_binding.clone() else {
                return Err(HostError::DependencyInputsUnverified {
                    approval_id: session.record.approval_id.clone(),
                    task_id: task.task_id.clone(),
                    depends_on: task.depends_on.clone(),
                });
            };
            _resolved_execution_lease = Some(resolved);

            if let Some(ref existing) = existing_intent {
                if existing.execution_binding.as_ref() != Some(&exec_binding) {
                    return Err(HostError::DependencyInputsUnverified {
                        approval_id: session.record.approval_id.clone(),
                        task_id: task.task_id.clone(),
                        depends_on: task.depends_on.clone(),
                    });
                }
            }

            let seal = match self.inspect_readonly_verified_dependency_seal(&session, task)? {
                Some(s) => s,
                None => {
                    return Err(HostError::DependencyInputsUnverified {
                        approval_id: session.record.approval_id.clone(),
                        task_id: task.task_id.clone(),
                        depends_on: task.depends_on.clone(),
                    });
                }
            };

            if exec_binding.input_digest != seal.seal_digest
                || exec_binding.execution_base_commit != seal.execution_base_commit
            {
                return Err(HostError::DependencyInputsUnverified {
                    approval_id: session.record.approval_id.clone(),
                    task_id: task.task_id.clone(),
                    depends_on: task.depends_on.clone(),
                });
            }

            let expected_preds: BTreeSet<&str> =
                task.depends_on.iter().map(String::as_str).collect();
            let actual_preds: BTreeSet<&str> = seal
                .predecessors
                .iter()
                .map(|p| p.task_id.as_str())
                .collect();
            if expected_preds != actual_preds || seal.predecessors.len() != task.depends_on.len() {
                return Err(HostError::DependencyInputsUnverified {
                    approval_id: session.record.approval_id.clone(),
                    task_id: task.task_id.clone(),
                    depends_on: task.depends_on.clone(),
                });
            }

            let mut visiting = BTreeSet::new();
            let mut memo = BTreeMap::new();
            for pred_proof in &seal.predecessors {
                let sel = DependencyPredecessorSelection {
                    task_id: pred_proof.task_id.clone(),
                    review_receipt_id: pred_proof.review_receipt_id.clone(),
                    integration_receipt_id: pred_proof.integration_receipt_id.clone(),
                };
                let live_proof = match self
                    .verify_predecessor_closure_member(
                        &session,
                        &validated_workers,
                        &pred_proof.task_id,
                        Some(&sel),
                        &mut visiting,
                        &mut memo,
                    )
                    .await
                {
                    Ok(proof) => proof,
                    Err(HostError::SecurityViolation { path, reason }) => {
                        return Err(HostError::SecurityViolation { path, reason });
                    }
                    Err(HostError::Io { operation, reason }) => {
                        return Err(HostError::Io { operation, reason });
                    }
                    Err(_) => {
                        return Err(HostError::DependencyInputsUnverified {
                            approval_id: session.record.approval_id.clone(),
                            task_id: task.task_id.clone(),
                            depends_on: task.depends_on.clone(),
                        });
                    }
                };
                if live_proof != *pred_proof {
                    return Err(HostError::DependencyInputsUnverified {
                        approval_id: session.record.approval_id.clone(),
                        task_id: task.task_id.clone(),
                        depends_on: task.depends_on.clone(),
                    });
                }
            }

            if let Some(existing) = existing_intent {
                existing
            } else {
                TaskDispatchIntent {
                    task_id: task.task_id.clone(),
                    task_digest: dispatch.task_digest.clone(),
                    profile_id: dispatch.profile_id.clone(),
                    command_key: dispatch.command_key.clone(),
                    execution_binding: Some(exec_binding),
                    recorded_at_ms: now_epoch_ms(),
                }
            }
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

        verify_post_dispatch_worker_snapshot(
            &started.binding_id,
            &session.live_binding.binding_id,
            started.execution_binding.as_ref(),
            recorded_intent.execution_binding.as_ref(),
        )?;

        let receipt = TaskRunReceipt {
            task_id: task.task_id.clone(),
            task_digest: recorded_intent.task_digest.clone(),
            profile_id: recorded_intent.profile_id.clone(),
            command_key: recorded_intent.command_key.clone(),
            run_id: started.run_id.clone(),
            worker_id: started.worker_id.clone(),
            execution_binding: recorded_intent.execution_binding.clone(),
            recorded_at_ms: now_epoch_ms(),
        };
        let sealed = record_task_receipt(
            &session.session_dir,
            &session.record,
            &session.live_binding.binding_id,
            receipt,
        )
        .map_err(|err| map_post_dispatch_receipt_error(&err))?;

        let live_worker = match session
            .scoped_client
            .get_worker(sealed.run_id.clone())
            .await
        {
            Ok(fresh) => fresh,
            Err(_) => started,
        };

        let dependency_input =
            self.historical_dependency_summary(&session, task, sealed.execution_binding.as_ref());

        Ok(build_task_snapshot(
            &session.record,
            &session.session_dir,
            &session.live_binding.binding_id,
            task,
            Some(&recorded_intent),
            Some(&sealed),
            Some(live_worker),
            dependency_input,
        ))
    }

    /// Queries the current execution status of `task_id` in `approval_id`.
    ///
    /// - Strictly read-only: never dispatches a worker or writes to session storage.
    /// - Never invents a `run_id` from a `command_key`.
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

        if !task.is_frozen_headless_worker() {
            let status = TaskExecutionStatus::NativeRouteRequired;
            let lifecycle = Some(project_task_lifecycle(
                &session.record,
                &session.session_dir,
                task,
                status,
                None,
                None,
            ));
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
                dependency_input: None,
                profile_id: task.profile_id.clone(),
                command_key: task.command_key.clone(),
                status,
                run_id: None,
                worker_id: None,
                worker: None,
                accepted: false,
                lifecycle,
            });
        }

        let (intent, receipt) = read_task_ledger_entry(
            &session.session_dir,
            &session.record,
            &session.live_binding.binding_id,
            task_id,
        )?;

        if intent.is_none() && receipt.is_none() && !task.depends_on.is_empty() {
            let dep_seal = if session.manifest.gates.is_legacy_root_only()
                || verify_all_transitive_predecessors_in_approval(&session.record, &task.task_id)
                    .is_err()
            {
                None
            } else {
                self.inspect_readonly_verified_dependency_seal(&session, task)?
            };

            let (status, dependency_input) = match dep_seal {
                Some(seal) => (
                    TaskExecutionStatus::DependencyInputsReady,
                    Some(DependencyInputSummary::from_seal(&seal)),
                ),
                None => (TaskExecutionStatus::DependencyInputsUnverified, None),
            };
            let lifecycle = Some(project_task_lifecycle(
                &session.record,
                &session.session_dir,
                task,
                status,
                None,
                None,
            ));

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
                dependency_input,
                profile_id: task.profile_id.clone(),
                command_key: task.command_key.clone(),
                status,
                run_id: None,
                worker_id: None,
                worker: None,
                accepted: false,
                lifecycle,
            });
        }

        let pinned_binding = receipt
            .as_ref()
            .and_then(|r| r.execution_binding.as_ref())
            .or_else(|| intent.as_ref().and_then(|i| i.execution_binding.as_ref()));
        let dependency_input = self.historical_dependency_summary(&session, task, pinned_binding);

        let Some(receipt) = receipt else {
            return Ok(build_task_snapshot(
                &session.record,
                &session.session_dir,
                &session.live_binding.binding_id,
                task,
                intent.as_ref(),
                None,
                None,
                dependency_input,
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
                    &session.session_dir,
                    &session.live_binding.binding_id,
                    task,
                    intent.as_ref(),
                    Some(&receipt),
                    Some(worker),
                    dependency_input,
                ))
            }
            Err(threadlane::Error::Remote(api_err))
                if api_err.code == RuntimeErrorCode::WorkerNotFound =>
            {
                Ok(build_task_snapshot(
                    &session.record,
                    &session.session_dir,
                    &session.live_binding.binding_id,
                    task,
                    intent.as_ref(),
                    Some(&receipt),
                    None,
                    dependency_input,
                ))
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Returns the truthful task result on a live authenticated `Ready` host or on a verifiably
    /// stopped host whose checkpointed session history is still readable.
    ///
    /// - `Ready` delegates to the existing authenticated, strictly read-only [`Self::get_task`],
    ///   then re-verifies the returned snapshot against the exact current session, frozen task,
    ///   sealed receipt, and worker identity before returning it.
    /// - `Stopped` re-verifies the caller scope, private session path, stopped manifest, existing
    ///   `host.lock`, released supervisor lock, session binding/thread, exact frozen task and
    ///   sealed receipt, owned work area, and worker identity before reading one bounded
    ///   [`WorkerSnapshot`] from the checkpointed `state.db` without opening `Core`, running
    ///   migrations/recovery, creating directories, or writing rows, cursors, receipts or sidecars.
    /// - `Bootstrapping`, `Stopping`, and `Failed` remain explicit [`HostError::HostNotReady`]
    ///   refusals; this method never starts or restarts the host or a provider.
    ///
    /// A worker `Completed` result remains `ReadyForReview` with `accepted: false`. A missing
    /// receipt reports `NotStarted` (or `Unknown` when a dispatch intent exists) and never invents
    /// a `run_id`. Availability of a result is not review acceptance or backlog completion.
    pub async fn get_task_live_or_stopped(
        &self,
        approval_id: &str,
        task_id: &str,
    ) -> Result<TaskStateSnapshot, HostError> {
        validate_identifier("task_id", task_id)?;
        let record = self.resolve_authorized_approval(approval_id)?;
        let session_dir = verify_existing_session_dir(&self.sessions_dir, &record.approval_id)?;
        let manifest = read_host_manifest_optional(&session_dir, &record)?.ok_or_else(|| {
            HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: None,
                reason: "host-manifest.json does not exist".to_string(),
            }
        })?;

        match manifest.phase {
            HostPhase::Ready => {
                let snapshot = self.get_task(approval_id, task_id).await?;
                // Re-verify the returned snapshot against the exact current authenticated
                // session, frozen task and sealed receipt before exposing the new result API.
                self.verify_live_task_result(approval_id, task_id, &snapshot)
                    .await?;
                Ok(snapshot)
            }
            HostPhase::Stopped => {
                self.query_stopped_session_task(&record, &session_dir, &manifest, task_id)
            }
            HostPhase::Bootstrapping | HostPhase::Stopping | HostPhase::Failed => {
                Err(HostError::HostNotReady {
                    approval_id: record.approval_id.clone(),
                    phase: Some(manifest.phase),
                    reason: manifest.last_error.clone().unwrap_or_else(|| {
                        format!(
                            "task result requires a Ready or verifiably Stopped host session, found `{}`",
                            manifest.phase
                        )
                    }),
                })
            }
        }
    }

    /// Returns the runtime's actual, bounded worker-process presence for `task_id` in
    /// `approval_id` when an active authenticated host session currently owns that exact run.
    ///
    /// Delegates to [`Self::get_task_presence_readonly_at`] with this client's authority directory
    /// and authoritative caller scope so there is only one presence implementation.
    pub async fn get_task_presence(
        &self,
        approval_id: &str,
        task_id: &str,
    ) -> Result<Option<WorkerPresenceSnapshot>, HostError> {
        Self::get_task_presence_readonly_at(
            self.authority.dir(),
            &self.sessions_dir,
            &self.control_binding_id,
            &self.native_thread_uuid,
            approval_id,
            task_id,
        )
        .await
    }

    /// Returns the runtime's actual, bounded worker-process presence for `task_id` in
    /// `approval_id` without constructing registry/plan stores or mutating any store or session.
    ///
    /// - Strictly read-only: never starts, restarts, dispatches, retries, cancels, or records
    ///   receipts. It never creates or chmods the authority/sessions roots, session directories,
    ///   `host.lock`, `task-receipts.lock`, or any other file.
    /// - Reads and integrity-checks the frozen [`ApprovalRecord`] directly from `authority_dir`
    ///   without acquiring `authority.lock`, then verifies the explicit caller
    ///   `control_binding_id`, native thread, and approval identity before any session access.
    /// - Returns `Ok(None)` for an unstarted task, a missing session directory, a non-`Ready`
    ///   session, a missing/non-live `host.lock`, a non-sealed receipt, or any observation that is
    ///   not live, fresh, and bound to the current manifest instance, sealed receipt, worker
    ///   claim, and attempt. Corrupt or foreign observation identities are explicit errors.
    /// - External `authority.lock`/`task-receipts.lock` holders do not block this read because it
    ///   never acquires those locks.
    /// - The returned snapshot is only the runtime DTO. It carries no path, provider prose, raw
    ///   error, or credential. A live observation is not a claim that the physical child process
    ///   remains alive after this call returns.
    pub async fn get_task_presence_readonly_at(
        authority_dir: &Path,
        sessions_dir: &Path,
        control_binding_id: &str,
        native_thread_uuid: &str,
        approval_id: &str,
        task_id: &str,
    ) -> Result<Option<WorkerPresenceSnapshot>, HostError> {
        validate_identifier("control_binding_id", control_binding_id)?;
        validate_canonical_thread_uuid("native_thread_uuid", native_thread_uuid)?;
        validate_identifier("task_id", task_id)?;
        validate_approval_id(approval_id)?;

        let record = AuthorityStore::get_for_operator_readonly_at(authority_dir, approval_id)?;

        // Authoritative caller scope checks BEFORE accessing any session path.
        if record.binding_id != control_binding_id {
            return Err(HostError::ScopeMismatch {
                field: "control_binding_id".to_string(),
                expected: control_binding_id.to_string(),
                actual: record.binding_id,
                reason: format!(
                    "approval `{}` does not belong to caller control_binding_id `{}`",
                    record.approval_id, control_binding_id
                ),
            });
        }
        if record.thread_uuid != native_thread_uuid {
            return Err(HostError::ScopeMismatch {
                field: "thread_uuid".to_string(),
                expected: native_thread_uuid.to_string(),
                actual: record.thread_uuid,
                reason: format!(
                    "approval `{}` does not belong to caller native thread_uuid `{}`",
                    record.approval_id, native_thread_uuid
                ),
            });
        }

        let sessions_dir = validate_existing_sessions_root(
            sessions_dir,
            &[
                ("authority_dir", authority_dir),
                ("repository_path", record.repository_path.as_path()),
            ],
        )?;

        let task = record
            .tasks
            .iter()
            .find(|t| t.task_id == task_id)
            .ok_or_else(|| HostError::UnknownTask {
                approval_id: record.approval_id.clone(),
                task_id: task_id.to_string(),
            })?;
        if !task.is_frozen_headless_worker() {
            return Err(HostError::NativeRouteRequired {
                approval_id: record.approval_id.clone(),
                task_id: task.task_id.clone(),
                adapter: task.adapter,
            });
        }

        let session_candidate = sessions_dir.join(&record.approval_id);
        match std::fs::symlink_metadata(&session_candidate) {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(HostError::Io {
                    operation: "symlink_metadata(presence session_dir)".to_string(),
                    reason: err.to_string(),
                });
            }
        }
        let session_dir = verify_existing_session_dir(&sessions_dir, &record.approval_id)?;
        let Some(initial_manifest) = read_host_manifest_optional(&session_dir, &record)? else {
            return Ok(None);
        };
        if initial_manifest.phase != HostPhase::Ready
            || !initial_manifest.gates.supervisor_authenticated_ready
        {
            return Ok(None);
        }
        let Some(expected_instance_id) = initial_manifest.instance_id.clone() else {
            return Ok(None);
        };

        let Some(session) = Self::verify_ready_session_for_record(
            &record,
            &sessions_dir,
            native_thread_uuid,
            Some(&expected_instance_id),
            ReadySessionAccess::ReadonlyPresence,
        )
        .await?
        else {
            return Ok(None);
        };
        let task = session
            .record
            .tasks
            .iter()
            .find(|t| t.task_id == task_id)
            .ok_or_else(|| HostError::UnknownTask {
                approval_id: session.record.approval_id.clone(),
                task_id: task_id.to_string(),
            })?;
        if !task.is_frozen_headless_worker() {
            return Err(HostError::NativeRouteRequired {
                approval_id: session.record.approval_id.clone(),
                task_id: task.task_id.clone(),
                adapter: task.adapter,
            });
        }

        if session.manifest.instance_id.as_deref() != Some(expected_instance_id.as_str()) {
            return Ok(None);
        }
        let instance_id = expected_instance_id;
        let session_binding_id = session.manifest.session_binding_id.clone().ok_or_else(|| {
            HostError::CorruptManifest {
                path: session.session_dir.display().to_string(),
                reason: "ready manifest is missing session_binding_id".to_string(),
            }
        })?;

        let Some(receipt) = read_exact_task_receipt_readonly(
            &session.session_dir,
            &session.record,
            &session_binding_id,
            task_id,
        )?
        else {
            return Ok(None);
        };
        if receipt.task_id != task.task_id
            || receipt.task_digest != task.task_digest
            || Some(receipt.profile_id.as_str()) != task.profile_id.as_deref()
            || Some(receipt.command_key.as_str()) != task.command_key.as_deref()
        {
            return Err(HostError::CorruptSessionState {
                reason: format!(
                    "sealed receipt for task `{}` does not match the frozen task identity",
                    task.task_id
                ),
            });
        }

        let worker_before = match session
            .scoped_client
            .get_worker(receipt.run_id.clone())
            .await
        {
            Ok(worker) => worker,
            Err(threadlane::Error::Remote(api_err))
                if api_err.code == RuntimeErrorCode::WorkerNotFound =>
            {
                return Ok(None);
            }
            Err(err) => return Err(err.into()),
        };
        verify_exact_task_worker(
            &session.record,
            &session.live_binding.binding_id,
            &session.work_area_dir,
            task,
            &receipt,
            &worker_before,
        )?;

        // Start the monotonic age extension before the presence request. The runtime's
        // server-side sample age does not cover response transit or deserialization, and a
        // backwards wall clock during that delay must not make an expired sample look fresh.
        // Conservatively include the entire elapsed interval through the post-sample worker,
        // manifest, and receipt checks; the runtime's original sample timestamp and expiry remain
        // authoritative and are never renewed.
        let presence_request_started = Instant::now();
        let mut presence = match session
            .scoped_client
            .get_worker_presence(receipt.run_id.clone())
            .await
        {
            Ok(presence) => presence,
            Err(threadlane::Error::Remote(api_err))
                if api_err.code == RuntimeErrorCode::WorkerNotFound =>
            {
                return Ok(None);
            }
            Err(err) => return Err(err.into()),
        };

        // Reconcile once against the current worker after the presence sample. A newer claim,
        // attempt, revision, or lifecycle transition invalidates the older observation instead of
        // letting this adapter adopt it.
        let worker_after = match session
            .scoped_client
            .get_worker(receipt.run_id.clone())
            .await
        {
            Ok(worker) => worker,
            Err(threadlane::Error::Remote(api_err))
                if api_err.code == RuntimeErrorCode::WorkerNotFound =>
            {
                return Ok(None);
            }
            Err(err) => return Err(err.into()),
        };
        verify_exact_task_worker(
            &session.record,
            &session.live_binding.binding_id,
            &session.work_area_dir,
            task,
            &receipt,
            &worker_after,
        )?;

        if !verify_live_task_presence(
            &session.record,
            &session.live_binding.binding_id,
            &instance_id,
            &receipt,
            &worker_after,
            &presence,
        )? {
            return Ok(None);
        }

        // Final checks are synchronous and use only freshly read, noncreating state after the
        // last await: the exact manifest instance/session binding, the noncreating host.lock
        // ownership probe, and the same exact sealed task receipt.
        let final_manifest = read_host_manifest_optional(&session.session_dir, &session.record)?;
        let Some(final_manifest) = final_manifest else {
            return Ok(None);
        };
        if final_manifest.phase != HostPhase::Ready
            || !final_manifest.gates.supervisor_authenticated_ready
            || final_manifest.instance_id.as_deref() != Some(instance_id.as_str())
            || final_manifest.session_binding_id.as_deref() != Some(session_binding_id.as_str())
            || !is_host_lock_held_readonly(&session.session_dir)?
        {
            return Ok(None);
        }
        let final_receipt = read_exact_task_receipt_readonly(
            &session.session_dir,
            &session.record,
            &session_binding_id,
            task_id,
        )?;
        if final_receipt.as_ref() != Some(&receipt) {
            return Ok(None);
        }

        let elapsed_ms =
            u64::try_from(presence_request_started.elapsed().as_millis()).unwrap_or(u64::MAX);
        if !advance_presence_sample_age(&mut presence, elapsed_ms) {
            return Ok(None);
        }
        Ok(Some(presence))
    }

    /// Inspects the physical delivery of the exact terminal result for `task_id` in
    /// `approval_id`.
    ///
    /// The caller supplies only existing identifiers. This method first resolves the persisted
    /// approval and verifies this client's authoritative `control_binding_id` and
    /// `native_thread_uuid`, then obtains the exact terminal [`TaskStateSnapshot`] through
    /// [`Self::get_task_live_or_stopped`] so the existing live/stopped readiness, session,
    /// receipt, worker, and inactive-job checks remain authoritative. It resolves the private
    /// canonical session and work area internally through the existing storage helpers and calls
    /// [`inspect_delivery_for_verified_task`] with that genuine record, snapshot, and work area.
    ///
    /// The returned path-free [`DeliveryInspection`] contains the actual terminal `result_digest`
    /// and the current artifact `delivery_digest`. It never records `Done`, accepts the task,
    /// mutates the checkout, restarts the host, or invokes a provider.
    ///
    /// Physical inspection runs on `spawn_blocking` because the bounded Git and file reads must
    /// not occupy the Tokio executor. It is a stable read, not atomic future immutability: a
    /// malicious same-UID process can change files during or after inspection. Use
    /// [`Self::recheck_task_delivery`] immediately before relying on the receipt.
    pub async fn inspect_task_delivery(
        &self,
        approval_id: &str,
        task_id: &str,
    ) -> Result<DeliveryInspection, HostError> {
        let (record, snapshot, work_area_root) = self
            .resolve_task_delivery_inputs(approval_id, task_id)
            .await?;

        tokio::task::spawn_blocking(move || {
            inspect_delivery_for_verified_task(&record, &snapshot, &work_area_root)
        })
        .await
        .map_err(|err| HostError::Io {
            operation: "spawn_blocking(inspect_task_delivery)".to_string(),
            reason: err.to_string(),
        })?
    }

    /// Rechecks a prior [`DeliveryInspection`] against the current exact terminal result and
    /// physical checkout.
    ///
    /// This method repeats the full caller-scope resolution and fetches a fresh terminal snapshot
    /// through the same guarded live/stopped route before recomputing the inspection. A supplied
    /// digest or receipt is never trusted on its own; any change in the terminal result or current
    /// artifact delivery is a typed [`HostError::SecurityViolation`] refusal. It never accepts,
    /// records `Done`, mutates the checkout, restarts the host, or invokes a provider.
    ///
    /// Recheck narrows the stable-read window but inherits its same-UID/TOCTOU limit; it does not
    /// promise atomic future immutability.
    pub async fn recheck_task_delivery(
        &self,
        approval_id: &str,
        task_id: &str,
        expected: &DeliveryInspection,
    ) -> Result<(), HostError> {
        let (record, snapshot, work_area_root) = self
            .resolve_task_delivery_inputs(approval_id, task_id)
            .await?;
        let expected = expected.clone();

        tokio::task::spawn_blocking(move || {
            recheck_delivery_for_verified_task(&record, &snapshot, &work_area_root, &expected)
        })
        .await
        .map_err(|err| HostError::Io {
            operation: "spawn_blocking(recheck_task_delivery)".to_string(),
            reason: err.to_string(),
        })?
    }

    async fn resolve_task_delivery_inputs(
        &self,
        approval_id: &str,
        task_id: &str,
    ) -> Result<(ApprovalRecord, TaskStateSnapshot, PathBuf), HostError> {
        validate_identifier("task_id", task_id)?;
        let record = self.resolve_authorized_approval(approval_id)?;
        let snapshot = self.get_task_live_or_stopped(approval_id, task_id).await?;
        let session_dir = verify_existing_session_dir(&self.sessions_dir, &record.approval_id)?;
        let work_area_root = verify_existing_private_dir(&session_dir.join(WORK_AREA_SUBDIR))?;
        Ok((record, snapshot, work_area_root))
    }

    /// Requests revision-checked cancellation of an in-flight worker task using the sealed
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

        if !task.is_frozen_headless_worker() {
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
            if !task.depends_on.is_empty() {
                let prepared = !session.manifest.gates.is_legacy_root_only()
                    && verify_all_transitive_predecessors_in_approval(
                        &session.record,
                        &task.task_id,
                    )
                    .is_ok()
                    && self
                        .inspect_readonly_verified_dependency_seal(&session, task)?
                        .is_some();
                if !prepared {
                    return Err(HostError::DependencyInputsUnverified {
                        approval_id: session.record.approval_id.clone(),
                        task_id: task.task_id.clone(),
                        depends_on: task.depends_on.clone(),
                    });
                }
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

        let dependency_input =
            self.historical_dependency_summary(&session, task, receipt.execution_binding.as_ref());

        Ok(build_task_snapshot(
            &session.record,
            &session.session_dir,
            &session.live_binding.binding_id,
            task,
            intent.as_ref(),
            Some(&receipt),
            Some(live_worker),
            dependency_input,
        ))
    }

    /// Lists bounded read-only activity events for the host session's runtime binding.
    ///
    /// Serves live authenticated `Ready` sessions over supervisor IPC and verifiably `Stopped`
    /// sessions directly from the existing read-only `state.db` while holding the existing
    /// exclusive `host.lock` guard. Never restarts a host or mutates cursors, wake-ups, or files.
    pub async fn list_activity(
        &self,
        approval_id: &str,
        after_seq: Option<u64>,
        cursor: Option<String>,
        limit: Option<usize>,
        kind: Option<EventKind>,
    ) -> Result<ActivityPage, HostError> {
        self.query_activity_live_or_stopped(approval_id, None, after_seq, cursor, limit, kind)
            .await
    }

    /// Verifies that the approved host session is currently in live authenticated `Ready` state
    /// over supervisor IPC and returns its verified live runtime [`Binding`].
    ///
    /// Never falls back to stopped SQLite history and never mutates cursors, batches, or wake-ups.
    pub async fn verify_live_ready_binding(&self, approval_id: &str) -> Result<Binding, HostError> {
        let session = self.verify_active_ready_session(approval_id).await?;
        Ok(session.live_binding)
    }

    /// Lists bounded read-only activity events filtered to the sealed `run_id` of `task_id`.
    ///
    /// Serves live authenticated `Ready` sessions over supervisor IPC and verifiably `Stopped`
    /// sessions directly from the existing read-only `state.db` while holding the existing
    /// exclusive `host.lock` guard. Never restarts a host or mutates cursors, wake-ups, or files.
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
        self.query_activity_live_or_stopped(
            approval_id,
            Some(task_id),
            after_seq,
            cursor,
            limit,
            kind,
        )
        .await
    }

    async fn query_activity_live_or_stopped(
        &self,
        approval_id: &str,
        task_id: Option<&str>,
        after_seq: Option<u64>,
        cursor: Option<String>,
        limit: Option<usize>,
        kind: Option<EventKind>,
    ) -> Result<ActivityPage, HostError> {
        let record = self.resolve_authorized_approval(approval_id)?;
        let session_dir = verify_existing_session_dir(&self.sessions_dir, &record.approval_id)?;
        let manifest = read_host_manifest_optional(&session_dir, &record)?.ok_or_else(|| {
            HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: None,
                reason: "host-manifest.json does not exist".to_string(),
            }
        })?;

        match manifest.phase {
            HostPhase::Ready => {
                let session = self.verify_active_ready_session(approval_id).await?;
                let run_id = match task_id {
                    Some(tid) => Some(self.resolve_started_task_run_id(
                        &session.record,
                        &session.session_dir,
                        &session.work_area_dir,
                        &session.manifest,
                        &session.live_binding.binding_id,
                        tid,
                    )?),
                    None => None,
                };
                let page = session
                    .scoped_client
                    .list_activity(after_seq, cursor, limit, run_id, kind)
                    .await?;
                Ok(page)
            }
            HostPhase::Stopped => self.query_stopped_session_activity(
                &record,
                &session_dir,
                &manifest,
                task_id,
                after_seq,
                cursor,
                limit,
                kind,
            ),
            HostPhase::Bootstrapping | HostPhase::Stopping | HostPhase::Failed => {
                Err(HostError::HostNotReady {
                    approval_id: record.approval_id.clone(),
                    phase: Some(manifest.phase),
                    reason: manifest.last_error.clone().unwrap_or_else(|| {
                        format!("host session is in phase `{}`", manifest.phase)
                    }),
                })
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn query_stopped_session_activity(
        &self,
        record: &ApprovalRecord,
        initial_session_dir: &Path,
        initial_manifest: &HostSessionManifest,
        task_id: Option<&str>,
        after_seq: Option<u64>,
        cursor: Option<String>,
        limit: Option<usize>,
        kind: Option<EventKind>,
    ) -> Result<ActivityPage, HostError> {
        if initial_manifest.phase != HostPhase::Stopped
            || initial_manifest.gates.supervisor_authenticated_ready
            || initial_manifest.pid.is_some()
        {
            return Err(HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: Some(initial_manifest.phase),
                reason: "stopped host manifest still reports active readiness or pid".to_string(),
            });
        }

        let session_binding_id = initial_manifest.session_binding_id.clone().ok_or_else(|| {
            HostError::CorruptManifest {
                path: initial_session_dir.display().to_string(),
                reason: "stopped manifest is missing session_binding_id".to_string(),
            }
        })?;

        let host_lock_guard = ExistingHostLockGuard::acquire(
            initial_session_dir,
            &record.approval_id,
            HostPhase::Stopped,
        )?;

        let session_dir = verify_existing_session_dir(&self.sessions_dir, &record.approval_id)?;
        let locked_manifest =
            read_host_manifest_optional(&session_dir, record)?.ok_or_else(|| {
                HostError::HostNotReady {
                    approval_id: record.approval_id.clone(),
                    phase: None,
                    reason: "host-manifest.json disappeared during stopped-session read"
                        .to_string(),
                }
            })?;

        if locked_manifest.phase != HostPhase::Stopped
            || locked_manifest.gates.supervisor_authenticated_ready
            || locked_manifest.pid.is_some()
            || locked_manifest.instance_id != initial_manifest.instance_id
            || locked_manifest.session_binding_id.as_deref() != Some(session_binding_id.as_str())
        {
            return Err(HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: Some(locked_manifest.phase),
                reason: format!(
                    "host session changed state to `{}` before stopped-history read",
                    locked_manifest.phase
                ),
            });
        }

        host_lock_guard.verify_identity(&session_dir)?;
        let runtime_state_dir =
            verify_existing_private_dir(&session_dir.join(RUNTIME_STATE_SUBDIR))?;
        let work_area_dir = verify_existing_private_dir(&session_dir.join(WORK_AREA_SUBDIR))?;
        verify_stopped_supervisor_lock_not_held(
            &runtime_state_dir,
            &record.approval_id,
            HostPhase::Stopped,
        )?;

        let run_id = match task_id {
            Some(tid) => Some(self.resolve_started_task_run_id(
                record,
                &session_dir,
                &work_area_dir,
                &locked_manifest,
                &session_binding_id,
                tid,
            )?),
            None => None,
        };

        let db_path = runtime_state_dir.join("state.db");
        let page = Core::list_activity_readonly(
            &db_path,
            &session_binding_id,
            &self.native_thread_uuid,
            after_seq,
            cursor.as_deref(),
            limit.unwrap_or(DEFAULT_PAGE_LIMIT),
            run_id.as_deref(),
            kind,
        )
        .map_err(|err| {
            map_readonly_sqlite_read_error(
                err,
                &record.approval_id,
                &db_path,
                &session_binding_id,
                &self.native_thread_uuid,
            )
        })?;

        let post_manifest =
            read_host_manifest_optional(&session_dir, record)?.ok_or_else(|| {
                HostError::HostNotReady {
                    approval_id: record.approval_id.clone(),
                    phase: None,
                    reason: "host-manifest.json disappeared during stopped-session read"
                        .to_string(),
                }
            })?;
        if post_manifest.phase != HostPhase::Stopped
            || post_manifest.gates.supervisor_authenticated_ready
            || post_manifest.pid.is_some()
            || post_manifest.instance_id != locked_manifest.instance_id
            || post_manifest.session_binding_id.as_deref() != Some(session_binding_id.as_str())
        {
            return Err(HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: Some(post_manifest.phase),
                reason: format!(
                    "host session changed state to `{}` during stopped-history read",
                    post_manifest.phase
                ),
            });
        }

        host_lock_guard.verify_identity(&session_dir)?;
        Ok(page)
    }

    fn query_stopped_session_task(
        &self,
        record: &ApprovalRecord,
        initial_session_dir: &Path,
        initial_manifest: &HostSessionManifest,
        task_id: &str,
    ) -> Result<TaskStateSnapshot, HostError> {
        if initial_manifest.phase != HostPhase::Stopped
            || initial_manifest.gates.supervisor_authenticated_ready
            || initial_manifest.pid.is_some()
        {
            return Err(HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: Some(initial_manifest.phase),
                reason: "stopped host manifest still reports active readiness or pid".to_string(),
            });
        }

        let session_binding_id = initial_manifest.session_binding_id.clone().ok_or_else(|| {
            HostError::CorruptManifest {
                path: initial_session_dir.display().to_string(),
                reason: "stopped manifest is missing session_binding_id".to_string(),
            }
        })?;

        // Hold the existing session lifetime lock while reading durable history. A still-held
        // `host.lock` means the host did not stop cleanly and no result may be served from it.
        let host_lock_guard = ExistingHostLockGuard::acquire(
            initial_session_dir,
            &record.approval_id,
            HostPhase::Stopped,
        )?;

        let session_dir = verify_existing_session_dir(&self.sessions_dir, &record.approval_id)?;
        let locked_manifest =
            read_host_manifest_optional(&session_dir, record)?.ok_or_else(|| {
                HostError::HostNotReady {
                    approval_id: record.approval_id.clone(),
                    phase: None,
                    reason: "host-manifest.json disappeared during stopped-result read".to_string(),
                }
            })?;

        if locked_manifest.phase != HostPhase::Stopped
            || locked_manifest.gates.supervisor_authenticated_ready
            || locked_manifest.pid.is_some()
            || locked_manifest.instance_id != initial_manifest.instance_id
            || locked_manifest.session_binding_id.as_deref() != Some(session_binding_id.as_str())
        {
            return Err(HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: Some(locked_manifest.phase),
                reason: format!(
                    "host session changed state to `{}` before stopped-result read",
                    locked_manifest.phase
                ),
            });
        }

        host_lock_guard.verify_identity(&session_dir)?;
        let runtime_state_dir =
            verify_existing_private_dir(&session_dir.join(RUNTIME_STATE_SUBDIR))?;
        let work_area_dir = verify_existing_private_dir(&session_dir.join(WORK_AREA_SUBDIR))?;
        verify_stopped_supervisor_lock_not_held(
            &runtime_state_dir,
            &record.approval_id,
            HostPhase::Stopped,
        )?;

        let task = record
            .tasks
            .iter()
            .find(|t| t.task_id == task_id)
            .ok_or_else(|| HostError::UnknownTask {
                approval_id: record.approval_id.clone(),
                task_id: task_id.to_string(),
            })?;

        if !task.is_frozen_headless_worker() {
            return Err(HostError::NativeRouteRequired {
                approval_id: record.approval_id.clone(),
                task_id: task.task_id.clone(),
                adapter: task.adapter,
            });
        }

        let (intent, receipt) =
            read_task_ledger_entry(&session_dir, record, &session_binding_id, task_id)?;

        let snapshot = match receipt {
            None => {
                // No sealed task-to-run receipt means no result exists. Preserve the explicit
                // NotStarted/Unknown split without inventing a run from the dispatch intent or
                // command key.
                build_task_snapshot(
                    record,
                    &session_dir,
                    &session_binding_id,
                    task,
                    intent.as_ref(),
                    None,
                    None,
                    None,
                )
            }
            Some(receipt) => {
                let db_path = runtime_state_dir.join("state.db");
                let worker = Core::get_worker_readonly(
                    &db_path,
                    &session_binding_id,
                    &self.native_thread_uuid,
                    &receipt.run_id,
                )
                .map_err(|err| {
                    map_readonly_sqlite_read_error(
                        err,
                        &record.approval_id,
                        &db_path,
                        &session_binding_id,
                        &self.native_thread_uuid,
                    )
                })?;

                verify_exact_task_worker(
                    record,
                    &session_binding_id,
                    &work_area_dir,
                    task,
                    &receipt,
                    &worker,
                )?;

                let dependency_input = self.historical_dependency_summary_in_workspace(
                    record,
                    &session_binding_id,
                    &work_area_dir,
                    task,
                    receipt.execution_binding.as_ref(),
                );

                build_task_snapshot(
                    record,
                    &session_dir,
                    &session_binding_id,
                    task,
                    intent.as_ref(),
                    Some(&receipt),
                    Some(worker),
                    dependency_input,
                )
            }
        };

        // Recheck the manifest, session lifetime lock, and released supervisor lock after the
        // read so a concurrent restart or replacement cannot be mistaken for stopped history.
        let post_manifest =
            read_host_manifest_optional(&session_dir, record)?.ok_or_else(|| {
                HostError::HostNotReady {
                    approval_id: record.approval_id.clone(),
                    phase: None,
                    reason: "host-manifest.json disappeared during stopped-result read".to_string(),
                }
            })?;
        if post_manifest.phase != HostPhase::Stopped
            || post_manifest.gates.supervisor_authenticated_ready
            || post_manifest.pid.is_some()
            || post_manifest.instance_id != locked_manifest.instance_id
            || post_manifest.session_binding_id.as_deref() != Some(session_binding_id.as_str())
        {
            return Err(HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: Some(post_manifest.phase),
                reason: format!(
                    "host session changed state to `{}` during stopped-result read",
                    post_manifest.phase
                ),
            });
        }
        host_lock_guard.verify_identity(&session_dir)?;
        verify_stopped_supervisor_lock_not_held(
            &runtime_state_dir,
            &record.approval_id,
            HostPhase::Stopped,
        )?;

        Ok(snapshot)
    }

    async fn verify_live_task_result(
        &self,
        approval_id: &str,
        task_id: &str,
        snapshot: &TaskStateSnapshot,
    ) -> Result<(), HostError> {
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
        let corrupt = |reason: String| HostError::CorruptSessionState { reason };

        if snapshot.approval_id != session.record.approval_id
            || snapshot.subject_digest != session.record.subject_digest
            || snapshot.binding_id != session.record.binding_id
            || snapshot.thread_uuid != self.native_thread_uuid
            || snapshot.session_binding_id != session.live_binding.binding_id
            || snapshot.task_id != task.task_id
            || snapshot.task_digest != task.task_digest
            || snapshot.adapter != task.adapter
            || snapshot.provider != task.provider
            || snapshot.depends_on != task.depends_on
            || snapshot.profile_id != task.profile_id
            || snapshot.command_key != task.command_key
            || snapshot.accepted
        {
            return Err(corrupt(
                "live task result does not match the current authorized approval, session, or frozen task identity"
                    .to_string(),
            ));
        }

        if !task.is_frozen_headless_worker() {
            if snapshot.status != TaskExecutionStatus::NativeRouteRequired
                || snapshot.run_id.is_some()
                || snapshot.worker_id.is_some()
                || snapshot.worker.is_some()
            {
                return Err(corrupt(
                    "native-route task result unexpectedly carries worker or completion identity"
                        .to_string(),
                ));
            }
            return Ok(());
        }

        let (_, receipt) = read_task_ledger_entry(
            &session.session_dir,
            &session.record,
            &session.live_binding.binding_id,
            task_id,
        )?;

        let Some(receipt) = receipt else {
            if snapshot.run_id.is_some()
                || snapshot.worker_id.is_some()
                || snapshot.worker.is_some()
                || !matches!(
                    snapshot.status,
                    TaskExecutionStatus::NotStarted
                        | TaskExecutionStatus::Unknown
                        | TaskExecutionStatus::DependencyInputsUnverified
                        | TaskExecutionStatus::DependencyInputsReady
                )
            {
                return Err(corrupt(
                    "live task result carries run/worker or completion state without a current sealed receipt"
                        .to_string(),
                ));
            }
            return Ok(());
        };

        if snapshot.run_id.as_deref() != Some(receipt.run_id.as_str())
            || snapshot.worker_id.as_deref() != Some(receipt.worker_id.as_str())
        {
            return Err(corrupt(
                "live task result run/worker identity does not match the current sealed receipt"
                    .to_string(),
            ));
        }
        let Some(worker) = snapshot.worker.as_ref() else {
            return Err(corrupt(
                "live task result is missing the exact worker snapshot for its current sealed receipt"
                    .to_string(),
            ));
        };
        if snapshot.status != TaskExecutionStatus::from_worker_snapshot(worker) {
            return Err(corrupt(
                "live task result status does not match its verified worker snapshot".to_string(),
            ));
        }
        verify_exact_task_worker(
            &session.record,
            &session.live_binding.binding_id,
            &session.work_area_dir,
            task,
            &receipt,
            worker,
        )
    }

    fn resolve_started_task_run_id(
        &self,
        record: &ApprovalRecord,
        session_dir: &Path,
        work_area_dir: &Path,
        manifest: &HostSessionManifest,
        session_binding_id: &str,
        task_id: &str,
    ) -> Result<String, HostError> {
        let task = record
            .tasks
            .iter()
            .find(|t| t.task_id == task_id)
            .ok_or_else(|| HostError::UnknownTask {
                approval_id: record.approval_id.clone(),
                task_id: task_id.to_string(),
            })?;

        if !task.is_frozen_headless_worker() {
            return Err(HostError::NativeRouteRequired {
                approval_id: record.approval_id.clone(),
                task_id: task.task_id.clone(),
                adapter: task.adapter,
            });
        }

        let (intent, receipt) =
            read_task_ledger_entry(session_dir, record, session_binding_id, task_id)?;

        let Some(receipt) = receipt else {
            if let Some(intent) = intent {
                return Err(HostError::InvalidRunState {
                    run_id: task.task_id.clone(),
                    action: "list_task_activity".to_string(),
                    reason: format!(
                        "task `{}` in approval `{}` has an unresolved dispatch intent (command_key `{}`) in unknown state without a sealed run_id receipt",
                        task.task_id, record.approval_id, intent.command_key
                    ),
                });
            }
            if !task.depends_on.is_empty() {
                let prepared = !manifest.gates.is_legacy_root_only()
                    && verify_all_transitive_predecessors_in_approval(record, &task.task_id)
                        .is_ok()
                    && self
                        .inspect_readonly_verified_dependency_seal_in_work_area(
                            record,
                            session_binding_id,
                            work_area_dir,
                            task,
                        )?
                        .is_some();
                if !prepared {
                    return Err(HostError::DependencyInputsUnverified {
                        approval_id: record.approval_id.clone(),
                        task_id: task.task_id.clone(),
                        depends_on: task.depends_on.clone(),
                    });
                }
            }
            return Err(HostError::TaskNotStarted {
                approval_id: record.approval_id.clone(),
                task_id: task.task_id.clone(),
            });
        };

        Ok(receipt.run_id)
    }

    // Historical reads remain useful after parents move; only show input details that still
    // match the run's exact persisted binding. Never relabel a started run from a replacement seal.
    fn historical_dependency_summary(
        &self,
        session: &VerifiedSession,
        task: &ApprovedTaskRecord,
        binding: Option<&WorkerExecutionBinding>,
    ) -> Option<DependencyInputSummary> {
        self.historical_dependency_summary_in_workspace(
            &session.record,
            &session.live_binding.binding_id,
            &session.work_area_dir,
            task,
            binding,
        )
    }

    fn historical_dependency_summary_in_workspace(
        &self,
        record: &ApprovalRecord,
        session_binding_id: &str,
        work_area_dir: &Path,
        task: &ApprovedTaskRecord,
        binding: Option<&WorkerExecutionBinding>,
    ) -> Option<DependencyInputSummary> {
        let binding = binding?;
        let profile =
            build_validated_task_profile(record, session_binding_id, work_area_dir, task).ok()?;
        let policy = profile.dependency_input.as_ref()?;
        let verified = read_dependency_input(
            policy,
            &record.repository_path,
            work_area_dir,
            &record.base_commit,
            &task.instruction,
            &policy.command_key,
        )
        .ok()?;
        let expected = WorkerExecutionBinding::new(
            &profile.profile_digest,
            &task.instruction,
            &verified.seal.seal_digest,
            &verified.seal_file_digest,
            &verified.seal.execution_base_commit,
        )
        .ok()?;
        if &expected != binding {
            return None;
        }
        Some(DependencyInputSummary::from_seal(&verified.seal))
    }

    fn inspect_readonly_verified_dependency_seal(
        &self,
        session: &VerifiedSession,
        task: &ApprovedTaskRecord,
    ) -> Result<Option<DependencyInputSeal>, HostError> {
        self.inspect_readonly_verified_dependency_seal_in_work_area(
            &session.record,
            &session.live_binding.binding_id,
            &session.work_area_dir,
            task,
        )
    }

    fn inspect_readonly_verified_dependency_seal_in_work_area(
        &self,
        record: &ApprovalRecord,
        session_binding_id: &str,
        work_area_dir: &Path,
        task: &ApprovedTaskRecord,
    ) -> Result<Option<DependencyInputSeal>, HostError> {
        let Ok(profile) =
            build_validated_task_profile(record, session_binding_id, work_area_dir, task)
        else {
            return Ok(None);
        };
        let Some(ref policy) = profile.dependency_input else {
            return Ok(None);
        };

        match inspect_dependency_input_seal_readonly(
            policy,
            &record.repository_path,
            work_area_dir,
            &record.base_commit,
            &task.instruction,
            &policy.command_key,
        ) {
            Ok(Some(seal)) => {
                let expected_preds: BTreeSet<&str> =
                    task.depends_on.iter().map(String::as_str).collect();
                let actual_preds: BTreeSet<&str> = seal
                    .predecessors
                    .iter()
                    .map(|p| p.task_id.as_str())
                    .collect();
                if expected_preds != actual_preds
                    || seal.predecessors.len() != task.depends_on.len()
                {
                    return Ok(None);
                }
                Ok(Some(seal))
            }
            Ok(None) => Ok(None),
            Err(OperationsError::SecurityViolation(reason)) => Err(HostError::SecurityViolation {
                path: work_area_dir.display().to_string(),
                reason,
            }),
            Err(_) => Ok(None),
        }
    }

    async fn verify_predecessor_closure_member(
        &self,
        session: &VerifiedSession,
        validated_workers: &ValidatedWorkersConfig,
        pred_task_id: &str,
        direct_selection: Option<&DependencyPredecessorSelection>,
        visiting: &mut BTreeSet<String>,
        memo: &mut BTreeMap<String, DependencyPredecessorInput>,
    ) -> Result<DependencyPredecessorInput, HostError> {
        if let Some(cached) = memo.get(pred_task_id) {
            return verify_cached_predecessor_selection(cached, direct_selection);
        }
        if memo.len() + visiting.len() > session.record.tasks.len()
            || !visiting.insert(pred_task_id.to_string())
        {
            return Err(HostError::CorruptSessionState {
                reason: format!(
                    "dependency cycle or bound exceeded while verifying predecessor `{pred_task_id}`"
                ),
            });
        }

        let pred_task = session
            .record
            .tasks
            .iter()
            .find(|t| t.task_id == pred_task_id)
            .ok_or_else(|| HostError::DependencyInputsUnverified {
                approval_id: session.record.approval_id.clone(),
                task_id: pred_task_id.to_string(),
                depends_on: Vec::new(),
            })?;

        let Some(pred_provider) = pred_task.provider else {
            return Err(HostError::NativeRouteRequired {
                approval_id: session.record.approval_id.clone(),
                task_id: pred_task.task_id.clone(),
                adapter: pred_task.adapter,
            });
        };
        if !pred_task.is_frozen_headless_worker() {
            return Err(HostError::NativeRouteRequired {
                approval_id: session.record.approval_id.clone(),
                task_id: pred_task.task_id.clone(),
                adapter: pred_task.adapter,
            });
        }

        let p_profile_id = pred_task.profile_id.as_deref().ok_or_else(|| {
            HostError::DependencyInputsUnverified {
                approval_id: session.record.approval_id.clone(),
                task_id: pred_task.task_id.clone(),
                depends_on: pred_task.depends_on.clone(),
            }
        })?;
        let p_command_key = pred_task.command_key.as_deref().ok_or_else(|| {
            HostError::DependencyInputsUnverified {
                approval_id: session.record.approval_id.clone(),
                task_id: pred_task.task_id.clone(),
                depends_on: pred_task.depends_on.clone(),
            }
        })?;
        let p_profile = validated_workers
            .profiles
            .get(p_profile_id)
            .ok_or_else(|| HostError::DependencyInputsUnverified {
                approval_id: session.record.approval_id.clone(),
                task_id: pred_task.task_id.clone(),
                depends_on: pred_task.depends_on.clone(),
            })?;
        let host_p_profile = build_validated_task_profile(
            &session.record,
            &session.live_binding.binding_id,
            &session.work_area_dir,
            pred_task,
        )
        .map_err(|_| HostError::DependencyInputsUnverified {
            approval_id: session.record.approval_id.clone(),
            task_id: pred_task.task_id.clone(),
            depends_on: pred_task.depends_on.clone(),
        })?;
        if p_profile.profile_id != host_p_profile.profile_id
            || p_profile.profile_digest != host_p_profile.profile_digest
        {
            return Err(HostError::DependencyInputsUnverified {
                approval_id: session.record.approval_id.clone(),
                task_id: pred_task.task_id.clone(),
                depends_on: pred_task.depends_on.clone(),
            });
        }

        let (p_intent, p_receipt) = read_task_ledger_entry(
            &session.session_dir,
            &session.record,
            &session.live_binding.binding_id,
            pred_task_id,
        )?;
        let Some(p_receipt) = p_receipt else {
            return Err(HostError::DependencyInputsUnverified {
                approval_id: session.record.approval_id.clone(),
                task_id: pred_task.task_id.clone(),
                depends_on: pred_task.depends_on.clone(),
            });
        };
        if let Some(ref intent) = p_intent {
            if intent.execution_binding != p_receipt.execution_binding {
                return Err(HostError::CorruptReceipts {
                    path: session.session_dir.display().to_string(),
                    reason: format!(
                        "predecessor task `{pred_task_id}` intent and receipt execution_binding mismatch"
                    ),
                });
            }
        }

        let worker = session
            .scoped_client
            .get_worker(p_receipt.run_id.clone())
            .await
            .map_err(|_| HostError::DependencyInputsUnverified {
                approval_id: session.record.approval_id.clone(),
                task_id: pred_task.task_id.clone(),
                depends_on: pred_task.depends_on.clone(),
            })?;

        if worker.binding_id != session.live_binding.binding_id {
            return Err(HostError::ScopeMismatch {
                field: "session_binding_id".to_string(),
                expected: session.live_binding.binding_id.clone(),
                actual: worker.binding_id,
                reason: "predecessor worker snapshot binding_id mismatch".to_string(),
            });
        }

        if p_receipt.task_id != pred_task.task_id
            || p_receipt.task_digest != pred_task.task_digest
            || p_receipt.profile_id != p_profile_id
            || p_receipt.command_key != p_command_key
            || worker.run_id != p_receipt.run_id
            || worker.worker_id != p_receipt.worker_id
            || worker.profile_id != p_receipt.profile_id
            || worker.profile_id != p_profile.profile_id
            || worker.profile_digest != p_profile.profile_digest
            || worker.provider != pred_provider.as_str()
            || worker.provider != p_profile.provider_str()
            || worker.requested_model != pred_task.base_model
            || worker.requested_model != p_profile.requested_model_str()
            || worker.requested_effort != p_profile.requested_effort
            || worker.instruction != pred_task.instruction
            || worker.status != WorkerStatus::Completed
        {
            return Err(HostError::DependencyInputsUnverified {
                approval_id: session.record.approval_id.clone(),
                task_id: pred_task.task_id.clone(),
                depends_on: pred_task.depends_on.clone(),
            });
        }
        if let Some(ref expected_effort) = pred_task.effort {
            if worker.requested_effort != *expected_effort {
                return Err(HostError::DependencyInputsUnverified {
                    approval_id: session.record.approval_id.clone(),
                    task_id: pred_task.task_id.clone(),
                    depends_on: pred_task.depends_on.clone(),
                });
            }
        }

        let Some(ref outcome) = worker.outcome else {
            return Err(HostError::DependencyInputsUnverified {
                approval_id: session.record.approval_id.clone(),
                task_id: pred_task.task_id.clone(),
                depends_on: pred_task.depends_on.clone(),
            });
        };
        if outcome.acceptance_state != WorkerAcceptanceState::ReadyForReview
            || outcome.missing_model_evidence
            || outcome.missing_effort_evidence
            || outcome.provider_error
            || outcome.incomplete_cleanup
            || outcome.rejected_by_diff_gate
            || outcome.timed_out
            || outcome.cancelled
            || outcome.exit_code != Some(0)
            || !crate::lifecycle::verify_worker_model_evidence(
                &session.record,
                &session.session_dir,
                pred_task,
                &worker,
            )
        {
            return Err(HostError::DependencyInputsUnverified {
                approval_id: session.record.approval_id.clone(),
                task_id: pred_task.task_id.clone(),
                depends_on: pred_task.depends_on.clone(),
            });
        }
        if let Some(ref expected_effort) = pred_task.effort {
            if pred_provider != threadlane_workers::WorkerProvider::CodexCli
                && worker.observed_effort.as_deref() != Some(expected_effort.as_str())
            {
                return Err(HostError::DependencyInputsUnverified {
                    approval_id: session.record.approval_id.clone(),
                    task_id: pred_task.task_id.clone(),
                    depends_on: pred_task.depends_on.clone(),
                });
            }
        }

        let (Some(ref task_worktree_id), Some(ref integration_worktree_id)) = (
            &worker.worktree.task_worktree_id,
            &worker.worktree.run_worktree_id,
        ) else {
            return Err(HostError::DependencyInputsUnverified {
                approval_id: session.record.approval_id.clone(),
                task_id: pred_task.task_id.clone(),
                depends_on: pred_task.depends_on.clone(),
            });
        };

        let mut upstream_accepted_heads = Vec::new();
        let expected_execution_base_commit = if pred_task.depends_on.is_empty() {
            if p_profile.dependency_input.is_some() {
                return Err(HostError::DependencyInputsUnverified {
                    approval_id: session.record.approval_id.clone(),
                    task_id: pred_task.task_id.clone(),
                    depends_on: pred_task.depends_on.clone(),
                });
            }
            if p_receipt.execution_binding.is_some() || worker.execution_binding.is_some() {
                return Err(HostError::CorruptReceipts {
                    path: session.session_dir.display().to_string(),
                    reason: format!(
                        "root predecessor task `{pred_task_id}` must not have an execution_binding"
                    ),
                });
            }
            session.record.base_commit.clone()
        } else {
            let Some(ref p_binding) = p_receipt.execution_binding else {
                return Err(HostError::DependencyInputsUnverified {
                    approval_id: session.record.approval_id.clone(),
                    task_id: pred_task.task_id.clone(),
                    depends_on: pred_task.depends_on.clone(),
                });
            };
            if worker.execution_binding.as_ref() != Some(p_binding) {
                return Err(HostError::DependencyInputsUnverified {
                    approval_id: session.record.approval_id.clone(),
                    task_id: pred_task.task_id.clone(),
                    depends_on: pred_task.depends_on.clone(),
                });
            }

            let p_policy = p_profile.dependency_input.as_ref().ok_or_else(|| {
                HostError::DependencyInputsUnverified {
                    approval_id: session.record.approval_id.clone(),
                    task_id: pred_task.task_id.clone(),
                    depends_on: pred_task.depends_on.clone(),
                }
            })?;

            p_binding
                .verify_against(
                    &p_profile.profile_digest,
                    &pred_task.instruction,
                    &p_binding.execution_base_commit,
                )
                .map_err(|_| HostError::DependencyInputsUnverified {
                    approval_id: session.record.approval_id.clone(),
                    task_id: pred_task.task_id.clone(),
                    depends_on: pred_task.depends_on.clone(),
                })?;

            let p_verified = read_dependency_input(
                p_policy,
                &session.record.repository_path,
                &session.work_area_dir,
                &session.record.base_commit,
                &pred_task.instruction,
                p_command_key,
            )
            .map_err(|err| match err {
                threadlane_workers::WorkerError::SecurityViolation(reason) => {
                    HostError::SecurityViolation {
                        path: session.work_area_dir.display().to_string(),
                        reason,
                    }
                }
                _ => HostError::DependencyInputsUnverified {
                    approval_id: session.record.approval_id.clone(),
                    task_id: pred_task.task_id.clone(),
                    depends_on: pred_task.depends_on.clone(),
                },
            })?;
            let p_seal = &p_verified.seal;

            let expected_p_binding = WorkerExecutionBinding::new(
                &p_profile.profile_digest,
                &pred_task.instruction,
                &p_seal.seal_digest,
                &p_verified.seal_file_digest,
                &p_seal.execution_base_commit,
            )
            .map_err(|_| HostError::DependencyInputsUnverified {
                approval_id: session.record.approval_id.clone(),
                task_id: pred_task.task_id.clone(),
                depends_on: pred_task.depends_on.clone(),
            })?;

            if p_binding != &expected_p_binding {
                return Err(HostError::DependencyInputsUnverified {
                    approval_id: session.record.approval_id.clone(),
                    task_id: pred_task.task_id.clone(),
                    depends_on: pred_task.depends_on.clone(),
                });
            }

            let expected_up: BTreeSet<&str> =
                pred_task.depends_on.iter().map(String::as_str).collect();
            let actual_up: BTreeSet<&str> = p_seal
                .predecessors
                .iter()
                .map(|p| p.task_id.as_str())
                .collect();
            if expected_up != actual_up || p_seal.predecessors.len() != pred_task.depends_on.len() {
                return Err(HostError::DependencyInputsUnverified {
                    approval_id: session.record.approval_id.clone(),
                    task_id: pred_task.task_id.clone(),
                    depends_on: pred_task.depends_on.clone(),
                });
            }

            for up_proof in &p_seal.predecessors {
                let up_sel = DependencyPredecessorSelection {
                    task_id: up_proof.task_id.clone(),
                    review_receipt_id: up_proof.review_receipt_id.clone(),
                    integration_receipt_id: up_proof.integration_receipt_id.clone(),
                };
                let live_up = Box::pin(self.verify_predecessor_closure_member(
                    session,
                    validated_workers,
                    &up_proof.task_id,
                    Some(&up_sel),
                    visiting,
                    memo,
                ))
                .await?;
                if live_up != *up_proof {
                    return Err(HostError::DependencyInputsUnverified {
                        approval_id: session.record.approval_id.clone(),
                        task_id: pred_task.task_id.clone(),
                        depends_on: pred_task.depends_on.clone(),
                    });
                }
                upstream_accepted_heads.push(up_proof.accepted_task_head_commit.clone());
            }

            p_binding.execution_base_commit.clone()
        };

        if worker.worktree.base_commit != expected_execution_base_commit {
            return Err(HostError::DependencyInputsUnverified {
                approval_id: session.record.approval_id.clone(),
                task_id: pred_task.task_id.clone(),
                depends_on: pred_task.depends_on.clone(),
            });
        }

        let task_manifest = load_and_verify_manifest(&session.work_area_dir, task_worktree_id)
            .map_err(|err| match err {
                threadlane_workers::WorkerError::SecurityViolation(reason) => {
                    HostError::SecurityViolation {
                        path: session.work_area_dir.display().to_string(),
                        reason,
                    }
                }
                _ => HostError::DependencyInputsUnverified {
                    approval_id: session.record.approval_id.clone(),
                    task_id: pred_task.task_id.clone(),
                    depends_on: pred_task.depends_on.clone(),
                },
            })?;
        let integration_manifest =
            load_and_verify_manifest(&session.work_area_dir, integration_worktree_id).map_err(
                |err| match err {
                    threadlane_workers::WorkerError::SecurityViolation(reason) => {
                        HostError::SecurityViolation {
                            path: session.work_area_dir.display().to_string(),
                            reason,
                        }
                    }
                    _ => HostError::DependencyInputsUnverified {
                        approval_id: session.record.approval_id.clone(),
                        task_id: pred_task.task_id.clone(),
                        depends_on: pred_task.depends_on.clone(),
                    },
                },
            )?;

        let expected_run_tag = compact_worktree_tag(&p_receipt.run_id, "r");
        let expected_task_tag = compact_worktree_tag(&p_receipt.worker_id, "w");

        if task_manifest.kind != WorktreeKind::Task
            || integration_manifest.kind != WorktreeKind::RunIntegration
            || task_manifest.worktree_id != *task_worktree_id
            || integration_manifest.worktree_id != *integration_worktree_id
            || task_manifest.run_id != expected_run_tag
            || integration_manifest.run_id != expected_run_tag
            || task_manifest.task_id.as_deref() != Some(expected_task_tag.as_str())
            || integration_manifest.task_id.is_some()
            || task_manifest.base_commit != expected_execution_base_commit
            || integration_manifest.base_commit != expected_execution_base_commit
            || worker.worktree.task_worktree_path.as_deref()
                != Some(task_manifest.checkout_path.to_string_lossy().as_ref())
            || worker.worktree.run_worktree_path.as_deref()
                != Some(
                    integration_manifest
                        .checkout_path
                        .to_string_lossy()
                        .as_ref(),
                )
            || worker.worktree.task_branch_name.as_deref()
                != Some(task_manifest.branch_name.as_str())
            || worker.worktree.run_branch_name.as_deref()
                != Some(integration_manifest.branch_name.as_str())
        {
            return Err(HostError::DependencyInputsUnverified {
                approval_id: session.record.approval_id.clone(),
                task_id: pred_task.task_id.clone(),
                depends_on: pred_task.depends_on.clone(),
            });
        }

        let sel = direct_selection.ok_or_else(|| HostError::DependencyInputsUnverified {
            approval_id: session.record.approval_id.clone(),
            task_id: pred_task.task_id.clone(),
            depends_on: pred_task.depends_on.clone(),
        })?;

        let candidate = DependencyPredecessorCandidate {
            logical_task_id: pred_task.task_id.clone(),
            logical_task_digest: pred_task.task_digest.clone(),
            runtime_run_id: p_receipt.run_id.clone(),
            worker_id: p_receipt.worker_id.clone(),
            integration_worktree_id: integration_worktree_id.clone(),
            task_worktree_id: task_worktree_id.clone(),
            repository_path: session.record.repository_path.clone(),
            work_area_root: session.work_area_dir.clone(),
            approved_base_commit: session.record.base_commit.clone(),
            expected_execution_base_commit,
            allowed_write_paths: pred_task.allowed_write_paths.iter().cloned().collect(),
            expected_provider: pred_provider.as_str().to_string(),
            expected_model: pred_task.base_model.clone(),
            expected_effort: pred_task.effort.clone(),
            review_receipt_id: sel.review_receipt_id.clone(),
            integration_receipt_id: sel.integration_receipt_id.clone(),
        };

        let proof = verify_dependency_predecessor_proof(&candidate)?;

        for ancestor_head in &upstream_accepted_heads {
            let is_anc = is_commit_ancestor(
                &session.record.repository_path,
                ancestor_head,
                &proof.accepted_task_head_commit,
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )
            .map_err(|err| HostError::Runtime {
                code: "ancestry_mismatch".to_string(),
                message: err.to_string(),
            })?;
            if !is_anc {
                return Err(HostError::Runtime {
                    code: "ancestry_mismatch".to_string(),
                    message: format!(
                        "ancestor accepted_task_head_commit `{ancestor_head}` is not a Git ancestor of `{}`",
                        proof.accepted_task_head_commit
                    ),
                });
            }
        }

        visiting.remove(pred_task_id);
        memo.insert(pred_task_id.to_string(), proof.clone());
        Ok(proof)
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
        Self::verify_ready_session_for_record(
            &record,
            &self.sessions_dir,
            &self.native_thread_uuid,
            None,
            ReadySessionAccess::Active,
        )
        .await?
        .ok_or_else(|| HostError::HostNotReady {
            approval_id: record.approval_id.clone(),
            phase: None,
            reason: "host session is not ready".to_string(),
        })
    }

    /// Shared Ready-session verification for active operations and strictly read-only presence.
    ///
    /// The caller must already have authorized `record` for `native_thread_uuid`. Active access
    /// preserves the existing blocking `host.lock` probe and `HostNotReady` errors. Read-only
    /// presence uses only noncreating probes and maps normal non-live conditions to `Ok(None)`.
    async fn verify_ready_session_for_record(
        record: &ApprovalRecord,
        sessions_dir: &Path,
        native_thread_uuid: &str,
        expected_instance_id: Option<&str>,
        access: ReadySessionAccess,
    ) -> Result<Option<VerifiedSession>, HostError> {
        if record.thread_uuid != native_thread_uuid {
            return Err(HostError::ScopeMismatch {
                field: "thread_uuid".to_string(),
                expected: native_thread_uuid.to_string(),
                actual: record.thread_uuid.clone(),
                reason: format!(
                    "approval `{}` does not belong to caller native thread_uuid `{}`",
                    record.approval_id, native_thread_uuid
                ),
            });
        }

        let session_dir = match verify_existing_session_dir(sessions_dir, &record.approval_id) {
            Ok(session_dir) => session_dir,
            Err(HostError::HostNotReady { .. })
                if access == ReadySessionAccess::ReadonlyPresence =>
            {
                return Ok(None);
            }
            Err(err) => return Err(err),
        };

        let manifest = match read_host_manifest_optional(&session_dir, record)? {
            Some(manifest) => manifest,
            None if access == ReadySessionAccess::ReadonlyPresence => return Ok(None),
            None => {
                return Err(HostError::HostNotReady {
                    approval_id: record.approval_id.clone(),
                    phase: None,
                    reason: "host-manifest.json does not exist".to_string(),
                });
            }
        };

        if manifest.phase != HostPhase::Ready || !manifest.gates.supervisor_authenticated_ready {
            if access == ReadySessionAccess::ReadonlyPresence {
                return Ok(None);
            }
            return Err(HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: Some(manifest.phase),
                reason: manifest
                    .last_error
                    .clone()
                    .unwrap_or_else(|| format!("host session is in phase `{}`", manifest.phase)),
            });
        }

        if expected_instance_id
            .is_some_and(|expected| manifest.instance_id.as_deref() != Some(expected))
        {
            if access == ReadySessionAccess::ReadonlyPresence {
                return Ok(None);
            }
            return Err(HostError::CorruptManifest {
                path: session_dir.display().to_string(),
                reason: "ready manifest instance_id changed during session verification"
                    .to_string(),
            });
        }

        let lock_held = match access {
            ReadySessionAccess::Active => is_host_lock_held(&session_dir)?,
            ReadySessionAccess::ReadonlyPresence => is_host_lock_held_readonly(&session_dir)?,
        };
        if !lock_held {
            if access == ReadySessionAccess::ReadonlyPresence {
                return Ok(None);
            }
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
        if live_binding.thread_uuid != native_thread_uuid {
            return Err(HostError::ScopeMismatch {
                field: "thread_uuid".to_string(),
                expected: native_thread_uuid.to_string(),
                actual: live_binding.thread_uuid,
                reason: "live supervisor thread_uuid mismatch".to_string(),
            });
        }

        let scoped_client = ScopedClient::from_state_dir(&runtime_state_dir, &session_binding_id)?;

        Ok(Some(VerifiedSession {
            record: record.clone(),
            manifest,
            session_dir,
            work_area_dir,
            live_binding,
            scoped_client,
        }))
    }
}

struct ExistingHostLockGuard {
    file: std::fs::File,
}

impl ExistingHostLockGuard {
    fn acquire(session_dir: &Path, approval_id: &str, phase: HostPhase) -> Result<Self, HostError> {
        use fs2::FileExt;

        let lock_path = session_dir.join(HOST_LOCK_FILENAME);
        let pre_meta = match std::fs::symlink_metadata(&lock_path) {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(HostError::HostNotReady {
                    approval_id: approval_id.to_string(),
                    phase: Some(phase),
                    reason: "host.lock does not exist".to_string(),
                });
            }
            Err(err) => {
                return Err(HostError::Io {
                    operation: "symlink_metadata(host.lock)".to_string(),
                    reason: err.to_string(),
                });
            }
        };
        verify_existing_lock_file_metadata(&lock_path, &pre_meta)?;

        let file = open_existing_lock_file_readonly(&lock_path, &pre_meta)?;
        if file.try_lock_exclusive().is_err() {
            return Err(HostError::HostNotReady {
                approval_id: approval_id.to_string(),
                phase: Some(phase),
                reason: "host.lock is held by an active host process".to_string(),
            });
        }

        let guard = Self { file };
        guard.verify_identity(session_dir)?;
        Ok(guard)
    }

    fn verify_identity(&self, session_dir: &Path) -> Result<(), HostError> {
        let lock_path = session_dir.join(HOST_LOCK_FILENAME);
        let path_meta = std::fs::symlink_metadata(&lock_path).map_err(|err| HostError::Io {
            operation: "recheck(host.lock)".to_string(),
            reason: err.to_string(),
        })?;
        verify_existing_lock_file_metadata(&lock_path, &path_meta)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let held_meta = self.file.metadata().map_err(|err| HostError::Io {
                operation: "fstat(host.lock)".to_string(),
                reason: err.to_string(),
            })?;
            if held_meta.dev() != path_meta.dev() || held_meta.ino() != path_meta.ino() {
                return Err(HostError::SecurityViolation {
                    path: lock_path.display().to_string(),
                    reason: "host.lock identity changed while held".to_string(),
                });
            }
        }
        Ok(())
    }
}

impl Drop for ExistingHostLockGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn verify_stopped_supervisor_lock_not_held(
    runtime_state_dir: &Path,
    approval_id: &str,
    phase: HostPhase,
) -> Result<(), HostError> {
    use fs2::FileExt;

    let sup_lock_path = runtime_state_dir.join("supervisor.lock");
    let pre_meta = match std::fs::symlink_metadata(&sup_lock_path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(HostError::Io {
                operation: "symlink_metadata(supervisor.lock)".to_string(),
                reason: err.to_string(),
            });
        }
    };
    verify_existing_lock_file_metadata(&sup_lock_path, &pre_meta)?;

    let file = open_existing_lock_file_readonly(&sup_lock_path, &pre_meta)?;
    match file.try_lock_exclusive() {
        Ok(()) => {
            let _ = file.unlock();
            Ok(())
        }
        Err(_) => Err(HostError::HostNotReady {
            approval_id: approval_id.to_string(),
            phase: Some(phase),
            reason: "supervisor.lock is held by an active supervisor process".to_string(),
        }),
    }
}

fn verify_existing_lock_file_metadata(
    lock_path: &Path,
    meta: &std::fs::Metadata,
) -> Result<(), HostError> {
    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
        return Err(HostError::SecurityViolation {
            path: lock_path.display().to_string(),
            reason: "lock file must be a non-symlink regular file".to_string(),
        });
    }
    if meta.len() != 0 {
        return Err(HostError::SecurityViolation {
            path: lock_path.display().to_string(),
            reason: format!(
                "lock file must be empty (0 bytes), found {} bytes",
                meta.len()
            ),
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let mode = meta.permissions().mode() & 0o777;
        if (mode & 0o177) != 0 {
            return Err(HostError::SecurityViolation {
                path: lock_path.display().to_string(),
                reason: format!(
                    "lock file permissions {mode:04o} are too permissive (expected 0600)"
                ),
            });
        }
        if let Ok(self_meta) = std::fs::metadata("/proc/self") {
            if meta.uid() != self_meta.uid() {
                return Err(HostError::SecurityViolation {
                    path: lock_path.display().to_string(),
                    reason: "lock file owner does not match current process owner".to_string(),
                });
            }
        }
    }
    Ok(())
}

fn open_existing_lock_file_readonly(
    lock_path: &Path,
    pre_meta: &std::fs::Metadata,
) -> Result<std::fs::File, HostError> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(false).create(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(lock_path).map_err(|err| {
        #[cfg(unix)]
        if err.raw_os_error() == Some(libc::ELOOP) {
            return HostError::SecurityViolation {
                path: lock_path.display().to_string(),
                reason: "lock file must not be a symlink".to_string(),
            };
        }
        HostError::Io {
            operation: "open(lock_file)".to_string(),
            reason: err.to_string(),
        }
    })?;
    let post_meta = file.metadata().map_err(|err| HostError::Io {
        operation: "fstat(lock_file)".to_string(),
        reason: err.to_string(),
    })?;
    verify_existing_lock_file_metadata(lock_path, &post_meta)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if pre_meta.dev() != post_meta.dev() || pre_meta.ino() != post_meta.ino() {
            return Err(HostError::SecurityViolation {
                path: lock_path.display().to_string(),
                reason: "lock file identity changed during open".to_string(),
            });
        }
    }
    #[cfg(not(unix))]
    {
        let _ = pre_meta;
    }
    Ok(file)
}

fn is_host_lock_held_readonly(session_dir: &Path) -> Result<bool, HostError> {
    use fs2::FileExt;

    let lock_path = session_dir.join(HOST_LOCK_FILENAME);
    let checked = ensure_no_symlink_components(&lock_path)?;
    let pre_meta = match std::fs::symlink_metadata(&checked) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(HostError::Io {
                operation: "symlink_metadata(host.lock)".to_string(),
                reason: err.to_string(),
            });
        }
    };
    verify_existing_lock_file_metadata(&checked, &pre_meta)?;

    let file = open_existing_lock_file_readonly(&checked, &pre_meta)?;
    match file.try_lock_exclusive() {
        Ok(()) => {
            let _ = file.unlock();
            Ok(false)
        }
        Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => Ok(true),
        Err(err) => Err(HostError::Io {
            operation: "try_lock_exclusive(host.lock)".to_string(),
            reason: err.to_string(),
        }),
    }
}

fn read_exact_task_receipt_readonly(
    session_dir: &Path,
    record: &ApprovalRecord,
    session_binding_id: &str,
    task_id: &str,
) -> Result<Option<TaskRunReceipt>, HostError> {
    let Some((_intents, receipts)) =
        read_session_task_ledger_readonly(session_dir, record, session_binding_id)?
    else {
        return Ok(None);
    };
    Ok(receipts
        .into_iter()
        .find(|receipt| receipt.task_id == task_id))
}

fn verify_live_task_presence(
    record: &ApprovalRecord,
    session_binding_id: &str,
    instance_id: &str,
    receipt: &TaskRunReceipt,
    worker: &WorkerSnapshot,
    presence: &WorkerPresenceSnapshot,
) -> Result<bool, HostError> {
    if presence.availability != WorkerPresenceAvailability::Live
        || presence.freshness != WorkerPresenceFreshness::Fresh
        || !presence.live
        || presence.max_sample_age_ms == 0
    {
        return Ok(false);
    }

    if presence.binding_id != session_binding_id {
        return Err(HostError::ScopeMismatch {
            field: "session_binding_id".to_string(),
            expected: session_binding_id.to_string(),
            actual: presence.binding_id.clone(),
            reason: "live worker presence returned a mismatched session_binding_id".to_string(),
        });
    }
    if presence.run_id != receipt.run_id || presence.worker_id != receipt.worker_id {
        return Err(HostError::CorruptSessionState {
            reason: "live worker presence run/worker identity does not match the sealed receipt"
                .to_string(),
        });
    }
    if presence.profile_id != receipt.profile_id
        || presence.profile_id != worker.profile_id
        || presence.profile_digest != worker.profile_digest
    {
        return Err(HostError::CorruptSessionState {
            reason:
                "live worker presence profile identity does not match the sealed receipt or worker"
                    .to_string(),
        });
    }
    if presence.execution_binding.as_ref() != receipt.execution_binding.as_ref()
        || worker.execution_binding.as_ref() != receipt.execution_binding.as_ref()
    {
        return Err(HostError::CorruptSessionState {
            reason:
                "live worker presence execution binding does not match the sealed receipt or worker"
                    .to_string(),
        });
    }

    let expected_owner = threadlane::HostControlConfig {
        approval_id: record.approval_id.clone(),
        subject_digest: record.subject_digest.clone(),
        control_binding_id: record.binding_id.clone(),
        thread_uuid: record.thread_uuid.clone(),
        session_binding_id: session_binding_id.to_string(),
        instance_id: instance_id.to_string(),
    };
    match presence.owner.as_ref() {
        Some(owner) if owner == &expected_owner => {}
        Some(_) => {
            return Err(HostError::CorruptSessionState {
                reason: "live worker presence owner does not match the current host session"
                    .to_string(),
            });
        }
        None => {
            return Err(HostError::CorruptSessionState {
                reason: "live worker presence is missing its owner scope".to_string(),
            });
        }
    }

    // A concurrent claim or lifecycle transition invalidates the sampled observation instead of
    // making this adapter adopt an older claim, attempt, revision, or status. A terminal worker is
    // never presented as live presence.
    if worker.status.is_terminal()
        || presence.worker_revision != worker.revision
        || presence.worker_status != worker.status
        || presence.claim_id != worker.claim_id
        || presence.attempt_count != worker.attempt_count
    {
        return Ok(false);
    }

    if !presence
        .process
        .as_ref()
        .is_some_and(|process| process.is_valid() && process.is_live())
    {
        return Err(HostError::CorruptSessionState {
            reason: "live worker presence is missing a valid runner-owned process snapshot"
                .to_string(),
        });
    }
    Ok(true)
}

fn advance_presence_sample_age(presence: &mut WorkerPresenceSnapshot, elapsed_ms: u64) -> bool {
    let Some(age) = compute_advanced_presence_sample_age(
        presence.sample_age_ms,
        presence.sampled_at_ms,
        presence.expires_at_ms,
        presence.max_sample_age_ms,
        now_epoch_ms(),
        elapsed_ms,
    ) else {
        return false;
    };
    presence.sample_age_ms = Some(age);
    true
}

fn compute_advanced_presence_sample_age(
    sample_age_ms: Option<u64>,
    sampled_at_ms: Option<i64>,
    expires_at_ms: Option<i64>,
    max_sample_age_ms: u64,
    now_ms: i64,
    elapsed_ms: u64,
) -> Option<u64> {
    if max_sample_age_ms == 0 {
        return None;
    }
    let sampled_at_ms = sampled_at_ms?;
    let wall_age = now_ms.checked_sub(sampled_at_ms)?;
    let wall_age = u64::try_from(wall_age).ok()?;
    let mut age = match sample_age_ms {
        Some(age) => age.checked_add(elapsed_ms)?,
        None => wall_age,
    };
    age = age.max(wall_age);
    if age > max_sample_age_ms {
        return None;
    }
    if expires_at_ms.is_some_and(|expires_at_ms| now_ms >= expires_at_ms) {
        return None;
    }
    Some(age)
}

fn verify_exact_task_worker(
    record: &ApprovalRecord,
    session_binding_id: &str,
    work_area_dir: &Path,
    task: &ApprovedTaskRecord,
    receipt: &TaskRunReceipt,
    worker: &WorkerSnapshot,
) -> Result<(), HostError> {
    let corrupt = |reason: String| HostError::CorruptSessionState { reason };

    if worker.binding_id != session_binding_id {
        return Err(HostError::ScopeMismatch {
            field: "session_binding_id".to_string(),
            expected: session_binding_id.to_string(),
            actual: worker.binding_id.clone(),
            reason: "worker snapshot binding_id mismatch".to_string(),
        });
    }
    if worker.run_id != receipt.run_id || worker.worker_id != receipt.worker_id {
        return Err(corrupt(format!(
            "worker snapshot identity `{}`/`{}` does not match sealed receipt `{}`/`{}`",
            worker.run_id, worker.worker_id, receipt.run_id, receipt.worker_id
        )));
    }
    if task.task_digest != receipt.task_digest
        || task.profile_id.as_deref() != Some(receipt.profile_id.as_str())
        || task.command_key.as_deref() != Some(receipt.command_key.as_str())
    {
        return Err(corrupt(format!(
            "sealed receipt for task `{}` does not match the frozen task digest, profile, or command key",
            task.task_id
        )));
    }

    let profile = build_validated_task_profile(record, session_binding_id, work_area_dir, task)
        .map_err(|reason| {
            corrupt(format!(
                "failed to validate frozen worker profile for task `{}`: {reason}",
                task.task_id
            ))
        })?;

    if worker.profile_id != profile.profile_id
        || worker.profile_digest != profile.profile_digest
        || worker.provider != profile.provider_str()
        || worker.requested_model != profile.requested_model_str()
        || worker.requested_effort != profile.requested_effort
        || worker.instruction != task.instruction
    {
        return Err(corrupt(format!(
            "worker `{}` profile, model, effort, or instruction does not match the frozen task identity",
            worker.worker_id
        )));
    }

    if worker.execution_binding.as_ref() != receipt.execution_binding.as_ref() {
        return Err(corrupt(format!(
            "worker `{}` execution binding does not match the sealed task receipt",
            worker.worker_id
        )));
    }

    if let Some(binding) = worker.execution_binding.as_ref() {
        binding
            .verify_against(
                &profile.profile_digest,
                &task.instruction,
                &worker.worktree.base_commit,
            )
            .map_err(|err| {
                corrupt(format!(
                    "worker `{}` execution binding verification failed: {err}",
                    worker.worker_id
                ))
            })?;
    } else if !task.depends_on.is_empty() {
        return Err(corrupt(format!(
            "dependent worker `{}` is missing its required execution binding",
            worker.worker_id
        )));
    } else if worker.worktree.base_commit != record.base_commit {
        return Err(corrupt(format!(
            "worker `{}` worktree base commit does not match the frozen approval base commit",
            worker.worker_id
        )));
    }

    let run_tag = compact_worktree_tag(&worker.run_id, "r");
    let task_tag = compact_worktree_tag(&worker.worker_id, "w");
    if worker
        .worktree
        .run_worktree_id
        .as_deref()
        .is_some_and(|id| !id.starts_with(&format!("wt-run-{run_tag}-")))
    {
        return Err(corrupt(format!(
            "worker `{}` run worktree id does not match its run identity",
            worker.worker_id
        )));
    }
    if worker
        .worktree
        .task_worktree_id
        .as_deref()
        .is_some_and(|id| !id.starts_with(&format!("wt-task-{run_tag}-{task_tag}-")))
    {
        return Err(corrupt(format!(
            "worker `{}` task worktree id does not match its run/worker identity",
            worker.worker_id
        )));
    }
    for path in [
        worker.worktree.run_worktree_path.as_deref(),
        worker.worktree.task_worktree_path.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if !Path::new(path).starts_with(work_area_dir) {
            return Err(corrupt(format!(
                "worker `{}` worktree path is outside the verified owned work area",
                worker.worker_id
            )));
        }
    }

    Ok(())
}

fn map_readonly_sqlite_read_error(
    err: ReadonlySqliteError,
    approval_id: &str,
    db_path: &Path,
    expected_binding_id: &str,
    expected_thread_uuid: &str,
) -> HostError {
    match err {
        ReadonlySqliteError::SnapshotUnavailable(reason) => HostError::HostNotReady {
            approval_id: approval_id.to_string(), phase: Some(HostPhase::Stopped), reason,
        },
        ReadonlySqliteError::SecurityViolation(reason) => HostError::SecurityViolation {
            path: db_path.display().to_string(),
            reason,
        },
        ReadonlySqliteError::DatabaseTooLarge => HostError::SecurityViolation {
            path: db_path.display().to_string(),
            reason: "state.db exceeds maximum size limit".to_string(),
        },
        ReadonlySqliteError::Io(io_err) if io_err.kind() == std::io::ErrorKind::NotFound => {
            HostError::CorruptSessionState {
                reason: "stopped host session runtime-state is missing state.db".to_string(),
            }
        }
        ReadonlySqliteError::Io(io_err) => HostError::CorruptSessionState {
            reason: format!("stopped host session state.db read failed: {io_err}"),
        },
        ReadonlySqliteError::Core(CoreError::BindingNotFound { binding_id }) => {
            HostError::ScopeMismatch {
                field: "session_binding_id".to_string(),
                expected: expected_binding_id.to_string(),
                actual: binding_id,
                reason: "stopped session state.db binding_id does not match manifest session_binding_id".to_string(),
            }
        }
        ReadonlySqliteError::Core(CoreError::WorkerNotFound {
            binding_id,
            worker_id,
        }) => HostError::CorruptSessionState {
            reason: format!(
                "sealed task receipt references missing worker `{worker_id}` on stopped session binding `{binding_id}`"
            ),
        },
        ReadonlySqliteError::Core(CoreError::Validation {
            field: "thread_uuid",
            reason,
        }) => HostError::ScopeMismatch {
            field: "thread_uuid".to_string(),
            expected: expected_thread_uuid.to_string(),
            actual: "<mismatched>".to_string(),
            reason,
        },
        ReadonlySqliteError::Core(CoreError::CorruptState(reason)) => {
            HostError::CorruptSessionState { reason }
        }
        ReadonlySqliteError::Core(CoreError::IncompatibleSchemaVersion { found, expected }) => {
            HostError::CorruptSessionState {
                reason: format!(
                    "stopped session state.db has incompatible schema version {found} (expected {expected})"
                ),
            }
        }
        ReadonlySqliteError::Core(CoreError::Storage(err)) => HostError::CorruptSessionState {
            reason: format!("stopped host session state.db storage error: {err}"),
        },
        ReadonlySqliteError::Core(other_core_err) => {
            HostError::from(threadlane::Error::from(other_core_err))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn build_task_snapshot(
    record: &ApprovalRecord,
    session_dir: &Path,
    session_binding_id: &str,
    task: &ApprovedTaskRecord,
    intent: Option<&TaskDispatchIntent>,
    receipt: Option<&TaskRunReceipt>,
    worker: Option<WorkerSnapshot>,
    dependency_input: Option<DependencyInputSummary>,
) -> TaskStateSnapshot {
    let status = match (intent, receipt, &worker) {
        (_, _, Some(w)) => TaskExecutionStatus::from_worker_snapshot(w),
        (_, Some(_), None) | (Some(_), None, None) => TaskExecutionStatus::Unknown,
        (None, None, None) => TaskExecutionStatus::NotStarted,
    };

    let lifecycle = Some(project_task_lifecycle(
        record,
        session_dir,
        task,
        status,
        receipt,
        worker.as_ref(),
    ));

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
        dependency_input,
        profile_id: task.profile_id.clone(),
        command_key: task.command_key.clone(),
        status,
        run_id: receipt.map(|r| r.run_id.clone()),
        worker_id: receipt.map(|r| r.worker_id.clone()),
        worker,
        accepted: false,
        lifecycle,
    }
}

fn collect_missing_predecessors_in_approval(record: &ApprovalRecord, task_id: &str) -> Vec<String> {
    let tasks_by_id: BTreeMap<&str, &ApprovedTaskRecord> = record
        .tasks
        .iter()
        .map(|t| (t.task_id.as_str(), t))
        .collect();
    let mut visited = BTreeSet::new();
    let mut missing = BTreeSet::new();
    let mut stack = vec![task_id];

    while let Some(current_id) = stack.pop() {
        if !visited.insert(current_id) {
            continue;
        }
        if let Some(t) = tasks_by_id.get(current_id) {
            for dep_id in &t.depends_on {
                if tasks_by_id.contains_key(dep_id.as_str()) {
                    stack.push(dep_id.as_str());
                } else {
                    missing.insert(dep_id.clone());
                }
            }
        }
    }

    missing.into_iter().collect()
}

pub(crate) fn compact_worktree_tag(id: &str, prefix: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(id.as_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(12);
    for byte in &digest[..6] {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    format!("{prefix}_{hex}")
}

pub(crate) fn verify_cached_predecessor_selection(
    cached: &DependencyPredecessorInput,
    direct_selection: Option<&DependencyPredecessorSelection>,
) -> Result<DependencyPredecessorInput, HostError> {
    if let Some(sel) = direct_selection {
        if sel.task_id != cached.task_id
            || sel.review_receipt_id != cached.review_receipt_id
            || sel.integration_receipt_id != cached.integration_receipt_id
        {
            return Err(HostError::from(OperationsError::ReceiptInvalidated(format!(
                "predecessor `{}` selection (review `{}`, integration `{}`) does not match verified predecessor proof (review `{}`, integration `{}`)",
                cached.task_id,
                sel.review_receipt_id,
                sel.integration_receipt_id,
                cached.review_receipt_id,
                cached.integration_receipt_id
            ))));
        }
    }
    Ok(cached.clone())
}

fn post_stop_outcome_uncertain(approval_id: &str, detail: &'static str) -> HostError {
    HostError::StopOutcomeUncertain {
        approval_id: approval_id.to_string(),
        detail: detail.to_string(),
    }
}

fn constant_time_digest_eq(a: &str, b: &str) -> bool {
    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();
    let max_len = a_bytes.len().max(b_bytes.len());
    let mut diff = a_bytes.len() ^ b_bytes.len();
    for idx in 0..max_len {
        let lhs = a_bytes.get(idx).copied().unwrap_or(0);
        let rhs = b_bytes.get(idx).copied().unwrap_or(0);
        diff |= usize::from(lhs ^ rhs);
    }
    diff == 0
}

fn classify_post_stop_manifest(
    approval_id: &str,
    expected_instance_id: &str,
    observed_instance_id: Option<&str>,
    phase: HostPhase,
    current_lock_held: bool,
) -> Result<bool, HostError> {
    if observed_instance_id != Some(expected_instance_id) {
        return Err(post_stop_outcome_uncertain(
            approval_id,
            "host session instance changed before durable stop confirmation",
        ));
    }
    if phase == HostPhase::Stopped && !current_lock_held {
        return Ok(true);
    }
    if phase == HostPhase::Failed {
        return Err(post_stop_outcome_uncertain(
            approval_id,
            "host session entered failed phase during shutdown",
        ));
    }
    Ok(false)
}

fn post_dispatch_outcome_uncertain(detail: &'static str) -> HostError {
    HostError::MutationOutcomeUncertain {
        operation: "dispatch_task".to_string(),
        detail: detail.to_string(),
    }
}

fn verify_post_dispatch_worker_snapshot(
    started_binding_id: &str,
    expected_session_binding_id: &str,
    started_execution_binding: Option<&WorkerExecutionBinding>,
    expected_execution_binding: Option<&WorkerExecutionBinding>,
) -> Result<(), HostError> {
    if started_binding_id != expected_session_binding_id {
        return Err(post_dispatch_outcome_uncertain(
            "worker started but returned snapshot session_binding_id did not match session scope; reconcile task state before retrying",
        ));
    }
    if started_execution_binding != expected_execution_binding {
        return Err(post_dispatch_outcome_uncertain(
            "worker started but returned snapshot execution_binding did not match dispatch intent; reconcile task state before retrying",
        ));
    }
    Ok(())
}

fn map_post_dispatch_receipt_error(error: &HostError) -> HostError {
    let detail = match error {
        HostError::SecurityViolation { .. } => {
            "worker started but task receipt sealing failed security validation; reconcile task state before retrying"
        }
        HostError::CorruptReceipts { .. } | HostError::CorruptSessionState { .. } => {
            "worker started but task receipt ledger failed integrity verification; reconcile task state before retrying"
        }
        HostError::Io { .. } => {
            "worker started but task receipt persistence failed with an I/O error; reconcile task state before retrying"
        }
        _ => {
            "worker started but task receipt sealing could not be completed; reconcile task state before retrying"
        }
    };
    post_dispatch_outcome_uncertain(detail)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_APPROVAL_ID: &str = "apr_0123456789abcdef0123456789abcdef";
    const TEST_INSTANCE_A: &str = "hst_0123456789abcdef0123456789abcdef";
    const TEST_INSTANCE_B: &str = "hst_fedcba9876543210fedcba9876543210";
    const SECRET_DETAIL: &str = "/secret/private/path/or/provider/diagnostic";

    #[test]
    fn post_stop_manifest_classifier_distinguishes_stopped_polling_and_concurrent_replacement() {
        let confirmed = classify_post_stop_manifest(
            TEST_APPROVAL_ID,
            TEST_INSTANCE_A,
            Some(TEST_INSTANCE_A),
            HostPhase::Stopped,
            false,
        )
        .expect("matching stopped manifest with released lock confirms stop");
        assert!(confirmed);

        let stopping = classify_post_stop_manifest(
            TEST_APPROVAL_ID,
            TEST_INSTANCE_A,
            Some(TEST_INSTANCE_A),
            HostPhase::Stopping,
            true,
        )
        .expect("matching stopping manifest continues polling");
        assert!(!stopping);

        let locked_stopped = classify_post_stop_manifest(
            TEST_APPROVAL_ID,
            TEST_INSTANCE_A,
            Some(TEST_INSTANCE_A),
            HostPhase::Stopped,
            true,
        )
        .expect("matching stopped manifest with lock still held continues polling");
        assert!(!locked_stopped);

        let replaced_err = classify_post_stop_manifest(
            TEST_APPROVAL_ID,
            TEST_INSTANCE_A,
            Some(TEST_INSTANCE_B),
            HostPhase::Ready,
            true,
        )
        .expect_err("concurrent replacement instance after stop request must be uncertain");
        assert_eq!(replaced_err.code(), "host_stop_outcome_uncertain");
        match replaced_err {
            HostError::StopOutcomeUncertain {
                approval_id,
                detail,
            } => {
                assert_eq!(approval_id, TEST_APPROVAL_ID);
                assert!(!detail.contains(TEST_INSTANCE_B));
            }
            other => panic!("expected StopOutcomeUncertain, got {other:?}"),
        }

        let missing_instance_err = classify_post_stop_manifest(
            TEST_APPROVAL_ID,
            TEST_INSTANCE_A,
            None,
            HostPhase::Stopped,
            false,
        )
        .expect_err("missing instance_id after stop request must be uncertain");
        assert_eq!(missing_instance_err.code(), "host_stop_outcome_uncertain");

        let failed_phase_err = classify_post_stop_manifest(
            TEST_APPROVAL_ID,
            TEST_INSTANCE_A,
            Some(TEST_INSTANCE_A),
            HostPhase::Failed,
            false,
        )
        .expect_err("failed phase during post-stop confirmation must be uncertain");
        assert_eq!(failed_phase_err.code(), "host_stop_outcome_uncertain");
    }

    #[test]
    fn post_dispatch_stage_maps_receipt_and_snapshot_errors_to_outcome_unknown_without_leaks() {
        let receipt_errors = [
            HostError::SecurityViolation {
                path: SECRET_DETAIL.to_string(),
                reason: SECRET_DETAIL.to_string(),
            },
            HostError::CorruptReceipts {
                path: SECRET_DETAIL.to_string(),
                reason: SECRET_DETAIL.to_string(),
            },
            HostError::Io {
                operation: "record_task_receipt".to_string(),
                reason: SECRET_DETAIL.to_string(),
            },
        ];

        for source_err in &receipt_errors {
            let mapped = map_post_dispatch_receipt_error(source_err);
            assert_eq!(mapped.code(), "outcome_unknown");
            match mapped {
                HostError::MutationOutcomeUncertain { operation, detail } => {
                    assert_eq!(operation, "dispatch_task");
                    assert!(!detail.contains(SECRET_DETAIL));
                    assert!(detail.contains("reconcile"));
                }
                other => panic!("expected MutationOutcomeUncertain, got {other:?}"),
            }
        }

        assert!(
            verify_post_dispatch_worker_snapshot("bnd_sess_1", "bnd_sess_1", None, None).is_ok()
        );

        let scope_err =
            verify_post_dispatch_worker_snapshot(SECRET_DETAIL, "bnd_sess_1", None, None)
                .expect_err("mismatched post-start session_binding_id must be uncertain");
        assert_eq!(scope_err.code(), "outcome_unknown");
        assert!(!scope_err.to_string().contains(SECRET_DETAIL));

        let binding = WorkerExecutionBinding::new(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "instruction",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "0123456789abcdef0123456789abcdef01234567",
        )
        .expect("valid WorkerExecutionBinding");
        let exec_err =
            verify_post_dispatch_worker_snapshot("bnd_sess_1", "bnd_sess_1", None, Some(&binding))
                .expect_err("mismatched post-start execution_binding must be uncertain");
        assert_eq!(exec_err.code(), "outcome_unknown");
    }

    #[test]
    fn presence_sample_age_rejects_delayed_response_at_freshness_boundary() {
        let now_ms = 1_700_000_000_000_i64;
        let sampled_at_ms = now_ms - 100;
        let expires_at_ms = now_ms + 60_000;

        assert_eq!(
            compute_advanced_presence_sample_age(
                Some(999),
                Some(sampled_at_ms),
                Some(expires_at_ms),
                1_000,
                now_ms,
                2,
            ),
            None,
            "a near-bound server age plus delayed response/check time must fail closed"
        );
        assert_eq!(
            compute_advanced_presence_sample_age(
                Some(900),
                Some(sampled_at_ms),
                Some(expires_at_ms),
                1_000,
                now_ms,
                101,
            ),
            None,
            "elapsed time that crosses the freshness bound must fail closed"
        );
        assert_eq!(
            compute_advanced_presence_sample_age(
                Some(900),
                Some(sampled_at_ms),
                Some(expires_at_ms),
                1_000,
                now_ms,
                100,
            ),
            Some(1_000),
            "exactly at the freshness bound remains valid"
        );
    }

    #[test]
    fn presence_sample_age_rejects_missing_future_expired_and_overflowing_samples() {
        let now_ms = 1_700_000_000_000_i64;
        let max_sample_age_ms = 1_000;

        assert_eq!(
            compute_advanced_presence_sample_age(None, None, None, max_sample_age_ms, now_ms, 0),
            None,
            "a missing wall-clock sample time must fail closed"
        );
        assert_eq!(
            compute_advanced_presence_sample_age(
                Some(1),
                None,
                Some(now_ms + 60_000),
                max_sample_age_ms,
                now_ms,
                0,
            ),
            None,
            "a server age without a wall-clock sample time must fail closed"
        );
        assert_eq!(
            compute_advanced_presence_sample_age(
                Some(1),
                Some(now_ms + 1),
                Some(now_ms + 60_000),
                max_sample_age_ms,
                now_ms,
                0,
            ),
            None,
            "a future wall-clock sample time must fail closed"
        );
        assert_eq!(
            compute_advanced_presence_sample_age(
                Some(1),
                Some(now_ms - 1),
                Some(now_ms),
                max_sample_age_ms,
                now_ms,
                0,
            ),
            None,
            "an expired sample must fail closed at its original expiry"
        );
        assert_eq!(
            compute_advanced_presence_sample_age(
                Some(u64::MAX),
                Some(now_ms - 1),
                None,
                max_sample_age_ms,
                now_ms,
                1,
            ),
            None,
            "elapsed overflow must fail closed"
        );
        assert_eq!(
            compute_advanced_presence_sample_age(Some(0), Some(now_ms), None, 0, now_ms, 0),
            None,
            "a zero maximum sample age must fail closed"
        );
    }

    #[test]
    fn presence_sample_age_within_ttl_advances_without_renewing_the_sample() {
        let now_ms = 1_700_000_000_000_i64;
        let sampled_at_ms = now_ms - 250;
        let expires_at_ms = now_ms + 60_000;

        let first = compute_advanced_presence_sample_age(
            Some(400),
            Some(sampled_at_ms),
            Some(expires_at_ms),
            1_000,
            now_ms,
            100,
        )
        .expect("first within-TTL sample remains fresh");
        let second = compute_advanced_presence_sample_age(
            Some(first),
            Some(sampled_at_ms),
            Some(expires_at_ms),
            1_000,
            now_ms,
            200,
        )
        .expect("second within-TTL sample remains fresh");
        let third = compute_advanced_presence_sample_age(
            Some(second),
            Some(sampled_at_ms),
            Some(expires_at_ms),
            1_000,
            now_ms,
            300,
        )
        .expect("third within-TTL sample remains fresh");

        assert_eq!(first, 500);
        assert_eq!(second, 700);
        assert_eq!(third, 1_000);
        assert!(
            first <= second && second <= third,
            "advanced sample ages must be nondecreasing"
        );
    }
}
