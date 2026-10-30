use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use threadlane::{
    ApprovedSessionWakeupContext, CodexWakeupConfig, ScopedClient, Supervisor, SupervisorClient,
    SupervisorConfig, SupervisorHandle,
};
use threadlane_authority::{ApprovalRecord, AuthorityStore, MaterializedPlan};
use threadlane_core::Binding;
use threadlane_plans::PlanStore;
use threadlane_registry::RegistryStore;

use crate::error::HostError;
use crate::storage::{
    ensure_private_dir, now_epoch_ms, prepare_session_dir, read_host_manifest_optional,
    validate_approval_id, validate_sessions_root, write_host_manifest, HostSessionLock,
};
use crate::types::{
    ApprovedHostNotificationConfig, HostExecutionGates, HostNotificationSummary, HostPhase,
    HostSessionManifest, HostSessionSummary, RUNTIME_STATE_SUBDIR, WORK_AREA_SUBDIR,
};

/// Default supervisor tick interval (`50ms`) used by [`ApprovedHostConfig`].
pub const DEFAULT_HOST_TICK_INTERVAL: Duration = Duration::from_millis(50);

/// Explicit operator-selected configuration for starting a per-approval [`ApprovedHost`].
#[derive(Debug, Clone)]
pub struct ApprovedHostConfig {
    /// Global operator tool registry directory (`RegistryStore`).
    pub registry_dir: PathBuf,
    /// Global operator plan store directory (`PlanStore`).
    pub plans_dir: PathBuf,
    /// Global operator workflow authority directory (`AuthorityStore`).
    pub authority_dir: PathBuf,
    /// Private root directory under which per-approval host session directories live.
    pub sessions_dir: PathBuf,
    /// Deterministic approval identifier (`apr_<32hex>`) to host.
    pub approval_id: String,
    /// Background tick interval for the session supervisor.
    pub tick_interval: Duration,
    /// Optional explicit opt-in Codex wake-up notification configuration (`None` / disabled by default).
    pub notifications: Option<ApprovedHostNotificationConfig>,
}

impl ApprovedHostConfig {
    /// Creates a new [`ApprovedHostConfig`] with [`DEFAULT_HOST_TICK_INTERVAL`] and notifications disabled (`None`).
    pub fn new(
        registry_dir: impl Into<PathBuf>,
        plans_dir: impl Into<PathBuf>,
        authority_dir: impl Into<PathBuf>,
        sessions_dir: impl Into<PathBuf>,
        approval_id: impl Into<String>,
    ) -> Self {
        Self {
            registry_dir: registry_dir.into(),
            plans_dir: plans_dir.into(),
            authority_dir: authority_dir.into(),
            sessions_dir: sessions_dir.into(),
            approval_id: approval_id.into(),
            tick_interval: DEFAULT_HOST_TICK_INTERVAL,
            notifications: None,
        }
    }

    /// Overrides the supervisor background tick interval.
    pub fn with_tick_interval(mut self, tick_interval: Duration) -> Self {
        self.tick_interval = tick_interval;
        self
    }

    /// Enables optional Codex wake-up notifications for this host session.
    pub fn with_notifications(mut self, config: ApprovedHostNotificationConfig) -> Self {
        self.notifications = Some(config);
        self
    }
}

/// Entrypoint for starting a separately owned per-approval local execution host.
///
/// `ApprovedHost` reads a persisted [`ApprovalRecord`] from [`AuthorityStore`] (never calling
/// `approve`), acquires an exclusive process-lifetime lock `<sessions_dir>/<approval_id>/host.lock`,
/// executes the two-phase supervisor bootstrap and persisted binding recovery, materializes the
/// frozen worker profiles against the session's single software-generated binding and private work
/// area, and commits a `Ready` manifest only after authenticated supervisor readiness is verified.
#[derive(Debug)]
pub struct ApprovedHost;

impl ApprovedHost {
    /// Opens the global stores in `config` and starts the host session for `config.approval_id`.
    pub async fn start(config: ApprovedHostConfig) -> Result<ApprovedHostHandle, HostError> {
        validate_approval_id(&config.approval_id)?;
        let registry = Arc::new(RegistryStore::open(&config.registry_dir)?);
        let plans = Arc::new(PlanStore::open(&config.plans_dir)?);
        let authority = Arc::new(AuthorityStore::open(
            &config.authority_dir,
            Arc::clone(&registry),
            Arc::clone(&plans),
        )?);
        validate_sessions_root(
            &config.sessions_dir,
            &[
                ("registry_dir", registry.config_dir()),
                ("plans_dir", plans.dir()),
                ("authority_dir", authority.dir()),
            ],
        )?;
        Self::start_with_authority_and_notifications(
            authority,
            &config.sessions_dir,
            &config.approval_id,
            config.tick_interval,
            config.notifications,
        )
        .await
    }

    /// Starts the host session for `approval_id` using an already opened [`AuthorityStore`]
    /// with notifications disabled (`None`).
    pub async fn start_with_authority(
        authority: Arc<AuthorityStore>,
        sessions_dir: impl AsRef<Path>,
        approval_id: impl AsRef<str>,
        tick_interval: Duration,
    ) -> Result<ApprovedHostHandle, HostError> {
        Self::start_with_authority_and_notifications(
            authority,
            sessions_dir,
            approval_id,
            tick_interval,
            None,
        )
        .await
    }

    /// Starts the host session for `approval_id` using an already opened [`AuthorityStore`]
    /// and an optional explicit [`ApprovedHostNotificationConfig`].
    pub async fn start_with_authority_and_notifications(
        authority: Arc<AuthorityStore>,
        sessions_dir: impl AsRef<Path>,
        approval_id: impl AsRef<str>,
        tick_interval: Duration,
        notifications: Option<ApprovedHostNotificationConfig>,
    ) -> Result<ApprovedHostHandle, HostError> {
        let approval_id = approval_id.as_ref();
        validate_approval_id(approval_id)?;

        let validated_notifications = match notifications {
            Some(cfg) => Some(ApprovedHostNotificationConfig::new(
                cfg.executable,
                cfg.timeout_ms,
            )?),
            None => None,
        };
        let notify_summary = validated_notifications
            .as_ref()
            .map(ApprovedHostNotificationConfig::summary)
            .unwrap_or_default();

        // Read and integrity-verify the persisted immutable approval record.
        // Host startup never calls `approve`.
        let record = authority.get_for_operator(approval_id)?;

        let canonical_sessions_root = validate_sessions_root(
            sessions_dir.as_ref(),
            &[
                ("authority_dir", authority.dir()),
                ("repository_path", record.repository_path.as_path()),
            ],
        )?;

        let session_dir = prepare_session_dir(&canonical_sessions_root, &record.approval_id)?;
        let host_lock = HostSessionLock::acquire(&session_dir, &record.approval_id)?;

        // Read and validate any pre-existing manifest before touching state.
        // If the manifest on disk is corrupt, preserve it untouched and release host_lock.
        // Note: notification config is NEVER restored from an old manifest on restart;
        // notification opt-in is strictly an explicit startup choice on each host start.
        let existing_manifest = read_host_manifest_optional(&session_dir, &record)?;
        let prior_session_binding_id = existing_manifest
            .as_ref()
            .and_then(|m| m.session_binding_id.clone());
        let created_at_ms = existing_manifest
            .as_ref()
            .map(|m| m.created_at_ms)
            .unwrap_or_else(now_epoch_ms);
        let pid = std::process::id();

        let boot_manifest = HostSessionManifest::from_approval_record(
            &record,
            prior_session_binding_id.clone(),
            HostPhase::Bootstrapping,
            Some(pid),
            None,
            created_at_ms,
            now_epoch_ms(),
        )
        .with_notifications(notify_summary.clone());
        write_host_manifest(&session_dir, &boot_manifest)?;

        let mut known_session_binding_id = prior_session_binding_id;

        let startup_result = Self::bootstrap_and_launch(
            &authority,
            &record,
            &session_dir,
            created_at_ms,
            pid,
            tick_interval,
            validated_notifications.as_ref(),
            &notify_summary,
            &mut known_session_binding_id,
        )
        .await;

        match startup_result {
            Ok((
                supervisor_handle,
                runtime_state_dir,
                work_area_dir,
                materialized,
                session_binding,
                ready_manifest,
            )) => Ok(ApprovedHostHandle {
                host_lock,
                supervisor_handle,
                session_dir,
                runtime_state_dir,
                work_area_dir,
                approval: record,
                materialized,
                session_binding,
                manifest: ready_manifest,
            }),
            Err(err) => {
                // Record failure evidence without fabricating Ready state, then drop host_lock.
                let failed_manifest = HostSessionManifest::from_approval_record(
                    &record,
                    known_session_binding_id,
                    HostPhase::Failed,
                    None,
                    Some(sanitize_error_diagnostic(&err)),
                    created_at_ms,
                    now_epoch_ms(),
                )
                .with_notifications(notify_summary);
                let _ = write_host_manifest(&session_dir, &failed_manifest);
                drop(host_lock);
                Err(err)
            }
        }
    }

    // Bootstrap and launch receives the validated host startup context in one place.
    #[allow(clippy::too_many_arguments)]
    async fn bootstrap_and_launch(
        authority: &AuthorityStore,
        record: &ApprovalRecord,
        session_dir: &Path,
        created_at_ms: i64,
        pid: u32,
        tick_interval: Duration,
        notifications: Option<&ApprovedHostNotificationConfig>,
        notify_summary: &HostNotificationSummary,
        known_session_binding_id: &mut Option<String>,
    ) -> Result<
        (
            SupervisorHandle,
            PathBuf,
            PathBuf,
            MaterializedPlan,
            Binding,
            HostSessionManifest,
        ),
        HostError,
    > {
        let runtime_state_dir = ensure_private_dir(&session_dir.join(RUNTIME_STATE_SUBDIR))?;
        let work_area_dir = ensure_private_dir(&session_dir.join(WORK_AREA_SUBDIR))?;

        // Pre-validate source plan publication status and repository/base_commit before starting
        // any supervisor so superseded plans or missing repositories fail fast.
        let preview_binding = Binding {
            binding_id: known_session_binding_id
                .clone()
                .unwrap_or_else(|| "bnd_bootstrap_preview".to_string()),
            thread_uuid: record.thread_uuid.clone(),
            label: "bootstrap-preview".to_string(),
            delivered_seq: 0,
            acked_seq: 0,
        };
        authority.materialize(&record.approval_id, &preview_binding, &work_area_dir)?;

        // Phase 1: Start temporary bootstrap supervisor with `workers: None`.
        // Because `workers` is `None`, no worker can be claimed or spawned, and any prior
        // in-flight runs in `state.db` are marked `Unknown` by `Supervisor::start`.
        let bootstrap_config = SupervisorConfig {
            state_dir: runtime_state_dir.clone(),
            tick_interval,
            codex: None,
            workers: None,
        };
        let bootstrap_handle = Supervisor::start(bootstrap_config).await?;

        let phase1_result = async {
            let sup_client = SupervisorClient::from_state_dir(&runtime_state_dir)?;
            let mut bindings = sup_client.list_bindings().await?;
            match bindings.len() {
                0 => {
                    if let Some(ref expected_id) = *known_session_binding_id {
                        return Err(HostError::CorruptSessionState {
                            reason: format!(
                                "host manifest recorded session_binding_id `{expected_id}`, but runtime-state contains 0 bindings"
                            ),
                        });
                    }
                    let label = format!("host-{}-{}", record.approval_id, record.plan_id);
                    let created = sup_client
                        .create_binding(&record.thread_uuid, &label)
                        .await?;
                    if created.thread_uuid != record.thread_uuid {
                        return Err(HostError::ScopeMismatch {
                            field: "thread_uuid".to_string(),
                            expected: record.thread_uuid.clone(),
                            actual: created.thread_uuid,
                            reason: "created runtime session binding thread_uuid mismatch"
                                .to_string(),
                        });
                    }
                    Ok(created)
                }
                1 => {
                    let existing = bindings.pop().expect("single binding checked");
                    if existing.thread_uuid != record.thread_uuid {
                        return Err(HostError::ScopeMismatch {
                            field: "thread_uuid".to_string(),
                            expected: record.thread_uuid.clone(),
                            actual: existing.thread_uuid,
                            reason: "recovered runtime session binding thread_uuid does not match approval"
                                .to_string(),
                        });
                    }
                    if let Some(ref expected_id) = *known_session_binding_id {
                        if &existing.binding_id != expected_id {
                            return Err(HostError::CorruptSessionState {
                                reason: format!(
                                    "host manifest session_binding_id `{expected_id}` does not match runtime-state binding `{}`",
                                    existing.binding_id
                                ),
                            });
                        }
                    }
                    Ok(existing)
                }
                count => Err(HostError::CorruptSessionState {
                    reason: format!(
                        "runtime-state must contain at most 1 session binding, found {count}"
                    ),
                }),
            }
        }
        .await;

        let bootstrap_fatal = bootstrap_handle.shutdown_and_take_fatal().await;
        let session_binding = phase1_result?;
        if let Some(fatal_err) = bootstrap_fatal {
            return Err(fatal_err.into());
        }

        *known_session_binding_id = Some(session_binding.binding_id.clone());

        // Record the single recovered/created session_binding_id in the Bootstrapping manifest
        // before starting Phase 2 so restart recovery always pins this exact binding ID.
        let mid_manifest = HostSessionManifest::from_approval_record(
            record,
            Some(session_binding.binding_id.clone()),
            HostPhase::Bootstrapping,
            Some(pid),
            None,
            created_at_ms,
            now_epoch_ms(),
        )
        .with_notifications(notify_summary.clone());
        write_host_manifest(session_dir, &mid_manifest)?;

        // Phase 2: Materialize the frozen approval against the actual session Binding and
        // private work area, then start the final supervisor on the same runtime-state directory.
        let materialized =
            authority.materialize(&record.approval_id, &session_binding, &work_area_dir)?;
        if materialized.approval_id != record.approval_id
            || materialized.subject_digest != record.subject_digest
        {
            return Err(HostError::CorruptSessionState {
                reason: "materialized plan identity does not match approval record".to_string(),
            });
        }

        let final_codex = match notifications {
            Some(notify_cfg) => {
                let approved_ctx = ApprovedSessionWakeupContext::new(record.approval_id.clone())?;
                let codex_cfg = CodexWakeupConfig {
                    executable: notify_cfg.executable.clone(),
                    notify_bindings: BTreeSet::from([session_binding.binding_id.clone()]),
                    timeout: Duration::from_millis(notify_cfg.timeout_ms),
                    approved_session: Some(approved_ctx),
                };
                codex_cfg.validate()?;
                Some(codex_cfg)
            }
            None => None,
        };

        let final_config = SupervisorConfig {
            state_dir: runtime_state_dir.clone(),
            tick_interval,
            codex: final_codex,
            workers: materialized.workers.clone(),
        };
        let final_handle = Supervisor::start(final_config).await?;

        // Verify actual authenticated supervisor readiness before writing READY manifest.
        let readiness_check = async {
            let sup_client = SupervisorClient::from_state_dir(&runtime_state_dir)?;
            let all_bindings = sup_client.list_bindings().await?;
            if all_bindings.len() != 1 {
                return Err(HostError::CorruptSessionState {
                    reason: format!(
                        "authenticated supervisor reported {} bindings (expected exactly 1)",
                        all_bindings.len()
                    ),
                });
            }
            let verified_binding = sup_client.get_binding(&session_binding.binding_id).await?;
            if verified_binding.binding_id != session_binding.binding_id {
                return Err(HostError::ScopeMismatch {
                    field: "session_binding_id".to_string(),
                    expected: session_binding.binding_id.clone(),
                    actual: verified_binding.binding_id,
                    reason: "authenticated supervisor returned mismatched session_binding_id"
                        .to_string(),
                });
            }
            if verified_binding.thread_uuid != record.thread_uuid {
                return Err(HostError::ScopeMismatch {
                    field: "thread_uuid".to_string(),
                    expected: record.thread_uuid.clone(),
                    actual: verified_binding.thread_uuid,
                    reason: "authenticated supervisor returned mismatched thread_uuid".to_string(),
                });
            }

            let scoped_client =
                ScopedClient::from_state_dir(&runtime_state_dir, &session_binding.binding_id)?;
            let status = scoped_client.status().await?;
            if status.binding.binding_id != session_binding.binding_id {
                return Err(HostError::ScopeMismatch {
                    field: "session_binding_id".to_string(),
                    expected: session_binding.binding_id.clone(),
                    actual: status.binding.binding_id,
                    reason: "authenticated scoped status returned mismatched session_binding_id"
                        .to_string(),
                });
            }
            if status.binding.thread_uuid != record.thread_uuid {
                return Err(HostError::ScopeMismatch {
                    field: "thread_uuid".to_string(),
                    expected: record.thread_uuid.clone(),
                    actual: status.binding.thread_uuid,
                    reason: "authenticated scoped status returned mismatched thread_uuid"
                        .to_string(),
                });
            }

            let ready_manifest = HostSessionManifest::from_approval_record(
                record,
                Some(session_binding.binding_id.clone()),
                HostPhase::Ready,
                Some(pid),
                None,
                created_at_ms,
                now_epoch_ms(),
            )
            .with_notifications(notify_summary.clone());
            write_host_manifest(session_dir, &ready_manifest)?;
            Ok(ready_manifest)
        }
        .await;

        match readiness_check {
            Ok(ready_manifest) => Ok((
                final_handle,
                runtime_state_dir,
                work_area_dir,
                materialized,
                session_binding,
                ready_manifest,
            )),
            Err(err) => {
                let _ = final_handle.shutdown_and_take_fatal().await;
                Err(err)
            }
        }
    }
}

/// Handle to a running per-approval [`ApprovedHost`] session.
///
/// Owns both the exclusive [`HostSessionLock`] (`host.lock`) and the active [`SupervisorHandle`].
/// Closing or dropping an [`crate::ApprovedHostClient`] has no effect on this handle or its jobs.
pub struct ApprovedHostHandle {
    // Fields drop in declaration order. The supervisor must be torn down before the exclusive
    // `host.lock` is released, so `host_lock` is deliberately the last field.
    supervisor_handle: SupervisorHandle,
    session_dir: PathBuf,
    runtime_state_dir: PathBuf,
    work_area_dir: PathBuf,
    approval: ApprovalRecord,
    materialized: MaterializedPlan,
    session_binding: Binding,
    manifest: HostSessionManifest,
    host_lock: HostSessionLock,
}

impl std::fmt::Debug for ApprovedHostHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApprovedHostHandle")
            .field("approval_id", &self.approval.approval_id)
            .field("subject_digest", &self.approval.subject_digest)
            .field("control_binding_id", &self.approval.binding_id)
            .field("thread_uuid", &self.approval.thread_uuid)
            .field("session_binding_id", &self.session_binding.binding_id)
            .field("phase", &self.manifest.phase)
            .finish()
    }
}

impl ApprovedHostHandle {
    /// Returns a safe, token-free, path-free [`HostSessionSummary`] for this running host session.
    pub fn summary(&self) -> HostSessionSummary {
        self.manifest.to_summary()
    }

    /// Returns the durable [`HostSessionManifest`] committed when this host became ready.
    pub fn manifest(&self) -> &HostSessionManifest {
        &self.manifest
    }

    /// Returns the frozen [`ApprovalRecord`] governing this host session.
    pub fn approval(&self) -> &ApprovalRecord {
        &self.approval
    }

    /// Returns the [`MaterializedPlan`] compiled for this host session.
    pub fn materialized_plan(&self) -> &MaterializedPlan {
        &self.materialized
    }

    /// Returns the single software-generated runtime [`Binding`] in `runtime-state`.
    pub fn session_binding(&self) -> &Binding {
        &self.session_binding
    }

    /// Returns the single software-generated runtime `session_binding_id`.
    pub fn session_binding_id(&self) -> &str {
        &self.session_binding.binding_id
    }

    /// Returns the deterministic `approval_id` (`apr_<32hex>`).
    pub fn approval_id(&self) -> &str {
        &self.approval.approval_id
    }

    /// Returns the authoritative control-plane `binding_id`.
    pub fn control_binding_id(&self) -> &str {
        &self.approval.binding_id
    }

    /// Returns the canonical native Codex `thread_uuid`.
    pub fn thread_uuid(&self) -> &str {
        &self.approval.thread_uuid
    }

    /// Returns the canonical path to `<sessions_dir>/<approval_id>`.
    pub fn session_dir(&self) -> &Path {
        &self.session_dir
    }

    /// Returns the canonical path to `<sessions_dir>/<approval_id>/runtime-state`.
    pub fn runtime_state_dir(&self) -> &Path {
        &self.runtime_state_dir
    }

    /// Returns the canonical path to `<sessions_dir>/<approval_id>/work-area`.
    pub fn work_area_dir(&self) -> &Path {
        &self.work_area_dir
    }

    /// Waits until the underlying supervisor records a fatal background error and returns it.
    pub async fn wait_for_fatal(&self) -> threadlane::ApiError {
        self.supervisor_handle.wait_for_fatal().await
    }

    /// Gracefully shuts down the session supervisor, updates `host-manifest.json` to
    /// [`HostPhase::Stopped`] (or [`HostPhase::Failed`] if the supervisor faulted), and releases
    /// `host.lock`.
    pub async fn shutdown(mut self) -> Result<(), HostError> {
        let fatal = self.supervisor_handle.shutdown_and_take_fatal().await;
        match fatal {
            Some(err) => {
                let host_err: HostError = err.into();
                self.manifest.phase = HostPhase::Failed;
                self.manifest.pid = None;
                self.manifest.gates = HostExecutionGates::for_ready_state(false);
                self.manifest.last_error = Some(sanitize_error_diagnostic(&host_err));
                self.manifest.updated_at_ms = now_epoch_ms();
                let _ = write_host_manifest(&self.session_dir, &self.manifest);
                drop(self.host_lock);
                Err(host_err)
            }
            None => {
                self.manifest.phase = HostPhase::Stopped;
                self.manifest.pid = None;
                self.manifest.gates = HostExecutionGates::for_ready_state(false);
                self.manifest.last_error = None;
                self.manifest.updated_at_ms = now_epoch_ms();
                let write_res = write_host_manifest(&self.session_dir, &self.manifest);
                drop(self.host_lock);
                write_res
            }
        }
    }
}

fn sanitize_error_diagnostic(err: &HostError) -> String {
    const MAX_DIAGNOSTIC_CHARS: usize = 512;
    let raw = format!("[{}] {}", err.code(), err);
    if raw.len() <= MAX_DIAGNOSTIC_CHARS {
        raw
    } else {
        let mut truncated: String = raw.chars().take(MAX_DIAGNOSTIC_CHARS).collect();
        truncated.push_str("...");
        truncated
    }
}
