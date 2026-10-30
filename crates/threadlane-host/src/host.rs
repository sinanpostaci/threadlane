use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use threadlane::{
    ApprovedSessionWakeupContext, CodexWakeupConfig, HostControlConfig, HostControlInfoRequest,
    HostControlRuntimeState, ScopedClient, Supervisor, SupervisorClient, SupervisorConfig,
    SupervisorHandle,
};
use threadlane_authority::{ApprovalRecord, AuthorityStore, MaterializedPlan};
use threadlane_core::Binding;
use threadlane_plans::PlanStore;
use threadlane_registry::RegistryStore;
use threadlane_workers::{WorkerError, WorktreeManager};

use crate::error::HostError;
use crate::storage::{
    ensure_fresh_session_dir_locked, ensure_private_dir, generate_host_instance_id, now_epoch_ms,
    prepare_session_dir, read_host_manifest_optional, validate_approval_id,
    validate_host_instance_id, validate_sessions_root, verify_existing_task_receipts,
    write_host_manifest, HostSessionLock,
};
use crate::types::{
    ApprovedHostNotificationConfig, HostExecutionGates, HostNotificationSummary, HostPhase,
    HostSessionManifest, HostSessionSummary, HOST_MANIFEST_FILENAME, RUNTIME_STATE_SUBDIR,
    WORK_AREA_SUBDIR,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartSessionMode {
    TrustedStartOrRecover,
    FreshOnly,
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
        Self::start_session_internal(
            authority,
            sessions_dir.as_ref(),
            approval_id.as_ref(),
            tick_interval,
            notifications,
            StartSessionMode::TrustedStartOrRecover,
        )
        .await
    }

    /// Opens the global stores in `config` and starts a brand-new host session for
    /// `config.approval_id`, refusing under `host.lock` any pre-existing manifest, task receipts,
    /// runtime state, or work area.
    pub async fn start_fresh_only(
        config: ApprovedHostConfig,
    ) -> Result<ApprovedHostHandle, HostError> {
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
        Self::start_fresh_only_with_authority_and_notifications(
            authority,
            &config.sessions_dir,
            &config.approval_id,
            config.tick_interval,
            config.notifications,
        )
        .await
    }

    /// Starts a brand-new host session for `approval_id` using an already opened
    /// [`AuthorityStore`] with notifications disabled (`None`), refusing under `host.lock` any
    /// pre-existing manifest, task receipts, runtime state, or work area.
    pub async fn start_fresh_only_with_authority(
        authority: Arc<AuthorityStore>,
        sessions_dir: impl AsRef<Path>,
        approval_id: impl AsRef<str>,
        tick_interval: Duration,
    ) -> Result<ApprovedHostHandle, HostError> {
        Self::start_fresh_only_with_authority_and_notifications(
            authority,
            sessions_dir,
            approval_id,
            tick_interval,
            None,
        )
        .await
    }

    /// Starts a brand-new host session for `approval_id` using an already opened
    /// [`AuthorityStore`] and optional [`ApprovedHostNotificationConfig`], refusing under
    /// `host.lock` any pre-existing manifest, task receipts, runtime state, or work area while
    /// preserving all existing files untouched.
    pub async fn start_fresh_only_with_authority_and_notifications(
        authority: Arc<AuthorityStore>,
        sessions_dir: impl AsRef<Path>,
        approval_id: impl AsRef<str>,
        tick_interval: Duration,
        notifications: Option<ApprovedHostNotificationConfig>,
    ) -> Result<ApprovedHostHandle, HostError> {
        Self::start_session_internal(
            authority,
            sessions_dir.as_ref(),
            approval_id.as_ref(),
            tick_interval,
            notifications,
            StartSessionMode::FreshOnly,
        )
        .await
    }

    async fn start_session_internal(
        authority: Arc<AuthorityStore>,
        sessions_dir: &Path,
        approval_id: &str,
        tick_interval: Duration,
        notifications: Option<ApprovedHostNotificationConfig>,
        mode: StartSessionMode,
    ) -> Result<ApprovedHostHandle, HostError> {
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
            sessions_dir,
            &[
                ("authority_dir", authority.dir()),
                ("repository_path", record.repository_path.as_path()),
            ],
        )?;

        let session_dir = prepare_session_dir(&canonical_sessions_root, &record.approval_id)?;
        let host_lock = HostSessionLock::acquire(&session_dir, &record.approval_id)?;

        let (prior_session_binding_id, created_at_ms) = match mode {
            StartSessionMode::TrustedStartOrRecover => {
                // Read and validate any pre-existing manifest before touching state.
                // If the manifest on disk is corrupt, preserve it untouched and release host_lock.
                // Note: notification config is NEVER restored from an old manifest on restart;
                // notification opt-in is strictly an explicit startup choice on each host start.
                let existing_manifest = read_host_manifest_optional(&session_dir, &record)?;
                let prior_binding = existing_manifest
                    .as_ref()
                    .and_then(|m| m.session_binding_id.clone());
                let created_ms = existing_manifest
                    .as_ref()
                    .map(|m| m.created_at_ms)
                    .unwrap_or_else(now_epoch_ms);
                (prior_binding, created_ms)
            }
            StartSessionMode::FreshOnly => {
                // Under `host.lock`, refuse any existing manifest, task-receipts file, runtime
                // state, or work area (including Stopped, Failed, Ready, Bootstrapping, or
                // corrupt/orphaned state) before writing Bootstrapping or creating subdirectories.
                ensure_fresh_session_dir_locked(&session_dir, &record)?;

                let precheck_binding = Binding {
                    binding_id: "bnd_fresh_start_precheck".to_string(),
                    thread_uuid: record.thread_uuid.clone(),
                    label: "fresh-start-precheck".to_string(),
                    delivered_seq: 0,
                    acked_seq: 0,
                };
                authority.materialize(
                    &record.approval_id,
                    &precheck_binding,
                    &session_dir.join(WORK_AREA_SUBDIR),
                )?;

                (None, now_epoch_ms())
            }
        };

        let pid = std::process::id();
        let instance_id = generate_host_instance_id();

        let boot_manifest = HostSessionManifest::from_approval_record(
            &record,
            prior_session_binding_id.clone(),
            HostPhase::Bootstrapping,
            Some(pid),
            None,
            created_at_ms,
            now_epoch_ms(),
        )
        .with_instance_id(Some(instance_id.clone()))
        .with_notifications(notify_summary.clone());
        write_host_manifest(&session_dir, &boot_manifest)?;

        let mut known_session_binding_id = prior_session_binding_id;

        let startup_result = Self::bootstrap_and_launch(
            &authority,
            &record,
            &session_dir,
            created_at_ms,
            pid,
            &instance_id,
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
                instance_id,
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
                .with_instance_id(Some(instance_id))
                .with_notifications(notify_summary);
                let _ = write_host_manifest(&session_dir, &failed_manifest);
                drop(host_lock);
                Err(err)
            }
        }
    }

    /// Explicitly restarts a cleanly [`HostPhase::Stopped`] host session after verifying
    /// `expected_prior_instance_id`, current authority/publication/registry state, and any existing
    /// task receipts under `host.lock` before writing `Bootstrapping`.
    pub async fn restart_stopped(
        config: ApprovedHostConfig,
        expected_prior_instance_id: &str,
    ) -> Result<ApprovedHostHandle, HostError> {
        validate_approval_id(&config.approval_id)?;
        validate_host_instance_id("expected_prior_instance_id", expected_prior_instance_id)?;
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
        Self::restart_stopped_with_authority_and_notifications(
            authority,
            &config.sessions_dir,
            &config.approval_id,
            expected_prior_instance_id,
            config.tick_interval,
            config.notifications,
        )
        .await
    }

    /// Explicitly restarts a cleanly [`HostPhase::Stopped`] host session using an already opened
    /// [`AuthorityStore`] and optional explicit [`ApprovedHostNotificationConfig`] (defaulting to
    /// disabled; never restoring prior notification policy from the stopped manifest).
    pub async fn restart_stopped_with_authority_and_notifications(
        authority: Arc<AuthorityStore>,
        sessions_dir: impl AsRef<Path>,
        approval_id: impl AsRef<str>,
        expected_prior_instance_id: impl AsRef<str>,
        tick_interval: Duration,
        notifications: Option<ApprovedHostNotificationConfig>,
    ) -> Result<ApprovedHostHandle, HostError> {
        let approval_id = approval_id.as_ref();
        let expected_prior_instance_id = expected_prior_instance_id.as_ref();
        validate_approval_id(approval_id)?;
        validate_host_instance_id("expected_prior_instance_id", expected_prior_instance_id)?;

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

        let record = authority.get_for_operator(approval_id)?;

        let canonical_sessions_root = validate_sessions_root(
            sessions_dir.as_ref(),
            &[
                ("authority_dir", authority.dir()),
                ("repository_path", record.repository_path.as_path()),
            ],
        )?;

        let candidate_session_dir = canonical_sessions_root.join(&record.approval_id);
        if std::fs::symlink_metadata(&candidate_session_dir).is_err() {
            return Err(HostError::RestartNotAllowed {
                approval_id: record.approval_id.clone(),
                phase: None,
                detail: "host session directory does not exist".to_string(),
            });
        }

        let session_dir = prepare_session_dir(&canonical_sessions_root, &record.approval_id)?;
        let host_lock = HostSessionLock::acquire(&session_dir, &record.approval_id)?;

        // Under `host.lock`, validate clean `Stopped` phase, `expected_prior_instance_id`,
        // existing `task-receipts.json` identity, and current approval/publication/registry
        // BEFORE writing `Bootstrapping` so any precheck failure leaves `Stopped` untouched.
        let existing_manifest =
            read_host_manifest_optional(&session_dir, &record)?.ok_or_else(|| {
                HostError::RestartNotAllowed {
                    approval_id: record.approval_id.clone(),
                    phase: None,
                    detail: "host-manifest.json does not exist".to_string(),
                }
            })?;

        if existing_manifest.phase != HostPhase::Stopped {
            return Err(HostError::RestartNotAllowed {
                approval_id: record.approval_id.clone(),
                phase: Some(existing_manifest.phase),
                detail: format!(
                    "host session is in phase `{}` (expected `stopped`)",
                    existing_manifest.phase
                ),
            });
        }

        if existing_manifest.instance_id.as_deref() != Some(expected_prior_instance_id) {
            return Err(HostError::InstanceMismatch {
                approval_id: record.approval_id.clone(),
                expected_instance_id: expected_prior_instance_id.to_string(),
                actual_instance_id: existing_manifest.instance_id.clone(),
            });
        }

        let prior_session_binding_id =
            existing_manifest
                .session_binding_id
                .clone()
                .ok_or_else(|| HostError::CorruptManifest {
                    path: session_dir
                        .join(HOST_MANIFEST_FILENAME)
                        .display()
                        .to_string(),
                    reason: "Stopped manifest is missing session_binding_id".to_string(),
                })?;

        let _ = verify_existing_task_receipts(&session_dir, &record, &prior_session_binding_id)?;

        let work_area_dir = ensure_private_dir(&session_dir.join(WORK_AREA_SUBDIR))?;
        let precheck_binding = Binding {
            binding_id: prior_session_binding_id.clone(),
            thread_uuid: record.thread_uuid.clone(),
            label: "restart-precheck".to_string(),
            delivered_seq: 0,
            acked_seq: 0,
        };
        authority.materialize(&record.approval_id, &precheck_binding, &work_area_dir)?;
        initialize_and_verify_work_area(&record, &work_area_dir)?;

        let created_at_ms = existing_manifest.created_at_ms;
        let pid = std::process::id();
        let mut instance_id = generate_host_instance_id();
        while instance_id == expected_prior_instance_id {
            instance_id = generate_host_instance_id();
        }

        let boot_manifest = HostSessionManifest::from_approval_record(
            &record,
            Some(prior_session_binding_id.clone()),
            HostPhase::Bootstrapping,
            Some(pid),
            None,
            created_at_ms,
            now_epoch_ms(),
        )
        .with_instance_id(Some(instance_id.clone()))
        .with_notifications(notify_summary.clone());
        write_host_manifest(&session_dir, &boot_manifest)?;

        let mut known_session_binding_id = Some(prior_session_binding_id);

        let startup_result = Self::bootstrap_and_launch(
            &authority,
            &record,
            &session_dir,
            created_at_ms,
            pid,
            &instance_id,
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
                instance_id,
                manifest: ready_manifest,
            }),
            Err(err) => {
                let failed_manifest = HostSessionManifest::from_approval_record(
                    &record,
                    known_session_binding_id,
                    HostPhase::Failed,
                    None,
                    Some(sanitize_error_diagnostic(&err)),
                    created_at_ms,
                    now_epoch_ms(),
                )
                .with_instance_id(Some(instance_id))
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
        instance_id: &str,
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
        // Initialize and verify the host's owned work area (`.threadlane-work-area.json` ownership
        // marker bound to `repository_path`'s common Git directory plus `manifests/` and
        // `checkouts/` subdirectories) BEFORE starting any supervisor or advertising `Ready`.
        // This guarantees concurrent `start_approved_task` dispatches never race an uninitialized
        // lock-only work area during `AuthorityStore::materialize`.
        initialize_and_verify_work_area(record, &work_area_dir)?;

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
        .with_instance_id(Some(instance_id.to_string()))
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
        let host_control = HostControlConfig {
            approval_id: record.approval_id.clone(),
            subject_digest: record.subject_digest.clone(),
            control_binding_id: record.binding_id.clone(),
            thread_uuid: record.thread_uuid.clone(),
            session_binding_id: session_binding.binding_id.clone(),
            instance_id: instance_id.to_string(),
        };
        let final_handle = Supervisor::start_with_host_control(final_config, host_control).await?;

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

            let hc_info = sup_client
                .host_control_info(&HostControlInfoRequest {
                    approval_id: record.approval_id.clone(),
                    subject_digest: record.subject_digest.clone(),
                    control_binding_id: record.binding_id.clone(),
                    thread_uuid: record.thread_uuid.clone(),
                    session_binding_id: session_binding.binding_id.clone(),
                    expected_instance_id: instance_id.to_string(),
                })
                .await?;
            if hc_info.approval_id != record.approval_id
                || hc_info.subject_digest != record.subject_digest
                || hc_info.control_binding_id != record.binding_id
                || hc_info.thread_uuid != record.thread_uuid
                || hc_info.session_binding_id != session_binding.binding_id
                || hc_info.instance_id != instance_id
                || hc_info.state != HostControlRuntimeState::Ready
            {
                return Err(HostError::CorruptSessionState {
                    reason:
                        "authenticated supervisor host_control_info did not match Ready owner scope"
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
            .with_instance_id(Some(instance_id.to_string()))
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
    instance_id: String,
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
            .field("instance_id", &self.instance_id)
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

    /// Returns the public per-start host `instance_id` (`hst_<32hex>`).
    pub fn instance_id(&self) -> &str {
        &self.instance_id
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

    /// Waits until an authenticated, owner-scoped idle stop request is latched on this host's supervisor.
    pub async fn wait_for_stop_request(&self) {
        self.supervisor_handle.wait_for_stop_request().await;
    }

    /// Waits until the underlying supervisor records a fatal background error and returns it.
    pub async fn wait_for_fatal(&self) -> threadlane::ApiError {
        self.supervisor_handle.wait_for_fatal().await
    }

    /// Gracefully shuts down the session supervisor, recording [`HostPhase::Stopping`] first,
    /// then updating `host-manifest.json` to [`HostPhase::Stopped`] (or [`HostPhase::Failed`] if
    /// the supervisor faulted), and finally releasing `host.lock`.
    pub async fn shutdown(mut self) -> Result<(), HostError> {
        self.manifest.phase = HostPhase::Stopping;
        self.manifest.gates = HostExecutionGates::for_ready_state(false);
        self.manifest.updated_at_ms = now_epoch_ms();
        if let Err(stopping_write_err) = write_host_manifest(&self.session_dir, &self.manifest) {
            let _ = self.supervisor_handle.shutdown_and_take_fatal().await;
            self.manifest.phase = HostPhase::Failed;
            self.manifest.pid = None;
            self.manifest.gates = HostExecutionGates::for_ready_state(false);
            self.manifest.last_error = Some(sanitize_error_diagnostic(&stopping_write_err));
            self.manifest.updated_at_ms = now_epoch_ms();
            let _ = write_host_manifest(&self.session_dir, &self.manifest);
            drop(self.host_lock);
            return Err(stopping_write_err);
        }

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

fn initialize_and_verify_work_area(
    record: &ApprovalRecord,
    work_area_dir: &Path,
) -> Result<(), HostError> {
    let manager = WorktreeManager::new(&record.repository_path, work_area_dir, &record.base_commit)
        .map_err(|err| map_work_area_init_error(work_area_dir, err))?;
    if manager.resolved_base_commit() != record.base_commit {
        return Err(HostError::ScopeMismatch {
            field: "base_commit".to_string(),
            expected: record.base_commit.clone(),
            actual: manager.resolved_base_commit().to_string(),
            reason: "work-area manager resolved base_commit does not match frozen approval"
                .to_string(),
        });
    }
    if manager.work_area_root() != work_area_dir {
        return Err(HostError::SecurityViolation {
            path: work_area_dir.display().to_string(),
            reason: format!(
                "canonical work-area root `{}` does not match `{}`",
                manager.work_area_root().display(),
                work_area_dir.display()
            ),
        });
    }
    Ok(())
}

fn map_work_area_init_error(work_area_dir: &Path, err: WorkerError) -> HostError {
    match err {
        WorkerError::SecurityViolation(reason) => HostError::SecurityViolation {
            path: work_area_dir.display().to_string(),
            reason,
        },
        WorkerError::Io(io_err) => HostError::Io {
            operation: "initialize_work_area".to_string(),
            reason: io_err.to_string(),
        },
        WorkerError::GitCommandFailed {
            command,
            status,
            stderr,
        } => HostError::Authority {
            code: "invalid_repository".to_string(),
            message: format!("git command `{command}` failed (status: {status:?}): {stderr}"),
        },
        other => HostError::Authority {
            code: "invalid_work_area".to_string(),
            message: other.to_string(),
        },
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
