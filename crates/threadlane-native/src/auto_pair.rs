//! Local managed-desktop pairing. Request metadata is only a hint until the existing
//! parent app-server corroborates the exact process-generated connection identity.

use serde::Serialize;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use threadlane::{NativeReconnectConfig, SupervisorClient};
use threadlane_core::observation::VerifiedMcpObservationContext;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::context_export::{
    bootstrap_verified_operator_context, OperatorContextBootstrap, OperatorContextExportRequest,
    OperatorContextServerConfig,
};
use crate::fs_safety::constant_time_str_eq;
use crate::native_chat_observation::{
    ManagedParentEnvironment, NativeChatSamplerOwner, SamplerStartRequest,
};
use crate::{
    ensure_control_runtime, verify_thread_hint_candidate, ConnectionIdentity, NativeClient,
    NativeError, NativeErrorCode, NativeThread, OperatorEnrollRequest, PairingStore,
};

pub(crate) const OBSERVATION_METADATA_REFRESH_TIMEOUT: Duration = Duration::from_millis(750);

#[cfg(test)]
type TestMetadataRefreshFn = Arc<dyn Fn(&str) -> Result<NativeThread, NativeError> + Send + Sync>;

#[derive(Clone)]
enum ObservationMetadataSource {
    #[cfg(test)]
    Static,
    ManagedParent {
        reconnect: NativeReconnectConfig,
        backend_pid: u32,
        parent_env: ManagedParentEnvironment,
    },
    #[cfg(test)]
    TestRefresh(TestMetadataRefreshFn),
}

impl fmt::Debug for ObservationMetadataSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(test)]
            Self::Static => f.write_str("Static"),
            Self::ManagedParent {
                reconnect,
                backend_pid,
                parent_env,
            } => f
                .debug_struct("ManagedParent")
                .field("reconnect", reconnect)
                .field("backend_pid", backend_pid)
                .field("parent_env", parent_env)
                .finish(),
            #[cfg(test)]
            Self::TestRefresh(_) => f.write_str("TestRefresh(..)"),
        }
    }
}

tokio::task_local! {
    pub(crate) static CALL_THREAD_HINT: Option<String>;
}

#[derive(Debug, Clone)]
struct CachedObservationContext {
    connection_nonce: String,
    epoch_id: String,
    server_pid: u32,
    canonical_cwd: PathBuf,
    project_id: Option<String>,
    metadata_source: ObservationMetadataSource,
    context: VerifiedMcpObservationContext,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct AutomaticPairing {
    pub(crate) require_call_identity: Arc<AtomicBool>,
    coordinator: Arc<Mutex<()>>,
    observation_context: Arc<std::sync::Mutex<Option<CachedObservationContext>>>,
}

pub(crate) struct AutomaticPairingRuntime<'a> {
    pub(crate) context_config: OperatorContextServerConfig<'a>,
    pub(crate) parent_env: ManagedParentEnvironment,
    pub(crate) sampler_owner: &'a NativeChatSamplerOwner,
}

#[derive(Debug, Serialize)]
pub(crate) struct AutomaticPairingResult {
    connected: bool,
    binding_id: String,
    thread_id: String,
    binding_created: bool,
    conversation_role: &'static str,
    native_per_call_identity_verified: bool,
    operator_context: OperatorContextBootstrap,
}

pub(crate) fn parse_thread_hint(value: Option<&str>) -> Result<String, NativeError> {
    let value = value.ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::NeedsVerification,
            "Codex did not provide a thread ID; use the trusted local operator connection.",
        )
    })?;
    let id = Uuid::parse_str(value).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Codex thread ID is malformed",
        )
    })?;
    if id.is_nil() || id.hyphenated().to_string() != value {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Codex thread ID is not canonical",
        ));
    }
    Ok(value.to_owned())
}

pub(crate) fn validate_refreshed_thread_metadata(
    thread: &NativeThread,
    expected_thread_id: &str,
    expected_cwd: &Path,
    expected_project_id: Option<&str>,
) -> bool {
    let Some(expected_cwd_str) = expected_cwd.to_str() else {
        return false;
    };
    thread.id == expected_thread_id
        && matches!(thread.status.trim(), "idle" | "active")
        && thread.cwd == expected_cwd_str
        && thread.project_id.as_deref() == expected_project_id
}

async fn refresh_managed_parent_thread_metadata(
    reconnect: &NativeReconnectConfig,
    backend_pid: u32,
    parent_env: &ManagedParentEnvironment,
    expected_thread_id: &str,
    expected_cwd: &Path,
    expected_project_id: Option<&str>,
) -> Result<NativeThread, NativeError> {
    let (current_reconnect, current_pid) = parent_env.discover_reconnect(
        NativeErrorCode::ExpiredEpoch,
        "Codex parent connection changed",
    )?;
    if &current_reconnect != reconnect || current_pid != backend_pid {
        return Err(NativeError::new(
            NativeErrorCode::ExpiredEpoch,
            "Codex parent connection changed",
        ));
    }
    let native = NativeClient::new_managed_websocket(reconnect.clone())?
        .with_expected_backend_pid(backend_pid)
        .with_timeout(OBSERVATION_METADATA_REFRESH_TIMEOUT)?;
    let thread = native.thread(expected_thread_id).await?;
    if !validate_refreshed_thread_metadata(
        &thread,
        expected_thread_id,
        expected_cwd,
        expected_project_id,
    ) {
        return Err(NativeError::new(
            NativeErrorCode::NeedsVerification,
            "Refreshed Codex thread metadata did not match verified chat scope",
        ));
    }
    Ok(thread)
}

impl AutomaticPairing {
    pub(crate) fn verify_call_hint_present_if_required(&self) -> Result<(), NativeError> {
        if !self.require_call_identity.load(Ordering::Acquire) {
            return Ok(());
        }
        let present = CALL_THREAD_HINT
            .try_with(|hint| hint.is_some())
            .unwrap_or(false);
        if !present {
            return Err(NativeError::new(
                NativeErrorCode::SharedConnectionRejected,
                "This MCP call does not match the connected Codex chat.",
            ));
        }
        Ok(())
    }

    pub(crate) fn verify_call(&self, expected_thread: &str) -> Result<(), NativeError> {
        if !self.require_call_identity.load(Ordering::Acquire) {
            return Ok(());
        }
        let matches = CALL_THREAD_HINT
            .try_with(|hint| hint.as_deref() == Some(expected_thread))
            .unwrap_or(false);
        if !matches {
            return Err(NativeError::new(
                NativeErrorCode::SharedConnectionRejected,
                "This MCP call does not match the connected Codex chat.",
            ));
        }
        Ok(())
    }

    pub(crate) fn clear_observation_context(&self) {
        if let Ok(mut guard) = self.observation_context.lock() {
            *guard = None;
        }
    }

    pub(crate) fn invalidate_sampler_observation_context(
        &self,
        connection_nonce: &str,
        epoch_id: &str,
        server_pid: u32,
    ) {
        let mut guard = self
            .observation_context
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let matches = guard.as_ref().is_some_and(|cached| {
            cached.server_pid == server_pid
                && constant_time_str_eq(&cached.connection_nonce, connection_nonce)
                && constant_time_str_eq(&cached.epoch_id, epoch_id)
        });
        if matches {
            *guard = None;
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_sampler_thread_metadata(
        &self,
        connection_nonce: &str,
        epoch_id: &str,
        server_pid: u32,
        binding_id: &str,
        thread_id: &str,
        title: Option<&str>,
        selected_model: Option<&str>,
        selected_effort: Option<&str>,
    ) {
        let mut guard = self
            .observation_context
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(cached) = guard.as_mut() else {
            return;
        };
        if cached.server_pid != server_pid
            || !constant_time_str_eq(&cached.connection_nonce, connection_nonce)
            || !constant_time_str_eq(&cached.epoch_id, epoch_id)
            || cached
                .context
                .matching_scope(binding_id, thread_id)
                .is_none()
        {
            return;
        }
        cached
            .context
            .refresh_thread_metadata(title, selected_model, selected_effort);
    }

    pub(crate) fn clear_sampler_selected_model(
        &self,
        connection_nonce: &str,
        epoch_id: &str,
        server_pid: u32,
        binding_id: &str,
        thread_id: &str,
    ) {
        let mut guard = self
            .observation_context
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(cached) = guard.as_mut() else {
            return;
        };
        if cached.server_pid != server_pid
            || !constant_time_str_eq(&cached.connection_nonce, connection_nonce)
            || !constant_time_str_eq(&cached.epoch_id, epoch_id)
            || cached
                .context
                .matching_scope(binding_id, thread_id)
                .is_none()
        {
            return;
        }
        cached.context.clear_selected_model();
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_verified_observation_context(
        &self,
        connection_nonce: &str,
        epoch_id: &str,
        server_pid: u32,
        binding_id: &str,
        thread_id: &str,
        thread_title: &str,
        canonical_cwd: &Path,
        project_id: Option<&str>,
    ) {
        self.record_verified_observation_context_with_selection(
            connection_nonce,
            epoch_id,
            server_pid,
            binding_id,
            thread_id,
            thread_title,
            canonical_cwd,
            project_id,
            None,
            None,
        );
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_verified_observation_context_with_selection(
        &self,
        connection_nonce: &str,
        epoch_id: &str,
        server_pid: u32,
        binding_id: &str,
        thread_id: &str,
        thread_title: &str,
        canonical_cwd: &Path,
        project_id: Option<&str>,
        selected_model: Option<&str>,
        selected_effort: Option<&str>,
    ) {
        self.record_observation_context_with_source(
            connection_nonce,
            epoch_id,
            server_pid,
            binding_id,
            thread_id,
            thread_title,
            canonical_cwd,
            project_id,
            selected_model,
            selected_effort,
            ObservationMetadataSource::Static,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn record_managed_observation_context(
        &self,
        connection_nonce: &str,
        epoch_id: &str,
        server_pid: u32,
        binding_id: &str,
        thread_id: &str,
        thread_title: &str,
        canonical_cwd: &Path,
        project_id: Option<&str>,
        selected_model: Option<&str>,
        selected_effort: Option<&str>,
        reconnect: NativeReconnectConfig,
        backend_pid: u32,
        parent_env: ManagedParentEnvironment,
    ) {
        self.record_observation_context_with_source(
            connection_nonce,
            epoch_id,
            server_pid,
            binding_id,
            thread_id,
            thread_title,
            canonical_cwd,
            project_id,
            selected_model,
            selected_effort,
            ObservationMetadataSource::ManagedParent {
                reconnect,
                backend_pid,
                parent_env,
            },
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn record_observation_context_with_source(
        &self,
        connection_nonce: &str,
        epoch_id: &str,
        server_pid: u32,
        binding_id: &str,
        thread_id: &str,
        thread_title: &str,
        canonical_cwd: &Path,
        project_id: Option<&str>,
        selected_model: Option<&str>,
        selected_effort: Option<&str>,
        metadata_source: ObservationMetadataSource,
    ) {
        self.require_call_identity.store(true, Ordering::Release);
        let context = VerifiedMcpObservationContext::new(
            binding_id,
            thread_id,
            thread_title,
            canonical_cwd,
            project_id,
        )
        .with_selected_model(selected_model, selected_effort);
        let mut guard = self
            .observation_context
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = Some(CachedObservationContext {
            connection_nonce: connection_nonce.to_string(),
            epoch_id: epoch_id.to_string(),
            server_pid,
            canonical_cwd: canonical_cwd.to_path_buf(),
            project_id: project_id.map(str::to_string),
            metadata_source,
            context,
        });
    }

    #[cfg(test)]
    pub(crate) fn set_test_metadata_refresher<F>(&self, refresher: F)
    where
        F: Fn(&str) -> Result<NativeThread, NativeError> + Send + Sync + 'static,
    {
        let mut guard = self
            .observation_context
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(cached) = guard.as_mut() {
            cached.metadata_source = ObservationMetadataSource::TestRefresh(Arc::new(refresher));
        }
    }

    pub(crate) fn observation_context_for_call(
        &self,
        connection_nonce: &str,
        epoch_id: &str,
        server_pid: u32,
        binding_id: &str,
        thread_id: &str,
    ) -> Option<VerifiedMcpObservationContext> {
        if !self.require_call_identity.load(Ordering::Acquire) {
            return None;
        }
        if self.verify_call(thread_id).is_err() {
            return None;
        }
        let guard = self
            .observation_context
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let cached = guard.as_ref()?;
        if cached.server_pid != server_pid
            || !constant_time_str_eq(&cached.connection_nonce, connection_nonce)
            || !constant_time_str_eq(&cached.epoch_id, epoch_id)
        {
            return None;
        }
        cached
            .context
            .matching_scope(binding_id, thread_id)
            .cloned()
    }

    #[cfg(test)]
    pub(crate) async fn refresh_observation_context_for_call(
        &self,
        connection_nonce: &str,
        epoch_id: &str,
        server_pid: u32,
        binding_id: &str,
        thread_id: &str,
    ) -> Option<VerifiedMcpObservationContext> {
        self.refresh_observation_context_for_call_until(
            connection_nonce,
            epoch_id,
            server_pid,
            binding_id,
            thread_id,
            None,
        )
        .await
    }

    pub(crate) async fn refresh_observation_context_for_call_until(
        &self,
        connection_nonce: &str,
        epoch_id: &str,
        server_pid: u32,
        binding_id: &str,
        thread_id: &str,
        deadline: Option<Instant>,
    ) -> Option<VerifiedMcpObservationContext> {
        if !self.require_call_identity.load(Ordering::Acquire) {
            return None;
        }
        if self.verify_call(thread_id).is_err() {
            return None;
        }
        let (metadata_source, canonical_cwd, project_id) = {
            let guard = self
                .observation_context
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let cached = guard.as_ref()?;
            if cached.server_pid != server_pid
                || !constant_time_str_eq(&cached.connection_nonce, connection_nonce)
                || !constant_time_str_eq(&cached.epoch_id, epoch_id)
            {
                return None;
            }
            #[cfg(test)]
            {
                let matched = cached.context.matching_scope(binding_id, thread_id)?;
                if matches!(cached.metadata_source, ObservationMetadataSource::Static) {
                    return Some(matched.clone());
                }
            }
            #[cfg(not(test))]
            cached.context.matching_scope(binding_id, thread_id)?;
            (
                cached.metadata_source.clone(),
                cached.canonical_cwd.clone(),
                cached.project_id.clone(),
            )
        };

        let refresh_budget = match deadline {
            Some(dl) => dl
                .checked_duration_since(Instant::now())
                .map(|rem| rem.min(OBSERVATION_METADATA_REFRESH_TIMEOUT)),
            None => Some(OBSERVATION_METADATA_REFRESH_TIMEOUT),
        };

        let refreshed = match refresh_budget {
            None => Ok(Err(NativeError::new(
                NativeErrorCode::TimedOut,
                "Observation metadata refresh deadline expired",
            ))),
            Some(budget) => match metadata_source {
                #[cfg(test)]
                ObservationMetadataSource::Static => return None,
                ObservationMetadataSource::ManagedParent {
                    reconnect,
                    backend_pid,
                    parent_env,
                } => {
                    tokio::time::timeout(
                        budget,
                        refresh_managed_parent_thread_metadata(
                            &reconnect,
                            backend_pid,
                            &parent_env,
                            thread_id,
                            &canonical_cwd,
                            project_id.as_deref(),
                        ),
                    )
                    .await
                }
                #[cfg(test)]
                ObservationMetadataSource::TestRefresh(refresher) => {
                    let thread_id_for_call = thread_id.to_string();
                    let expected_thread_id = thread_id.to_string();
                    let canonical_cwd_clone = canonical_cwd.clone();
                    let project_id_clone = project_id.clone();
                    tokio::time::timeout(budget, async move {
                        let thread =
                            tokio::task::spawn_blocking(move || refresher(&thread_id_for_call))
                                .await
                                .map_err(|_| {
                                    NativeError::new(
                                        NativeErrorCode::IoError,
                                        "Observation metadata refresher failed to join",
                                    )
                                })??;
                        if !validate_refreshed_thread_metadata(
                            &thread,
                            &expected_thread_id,
                            &canonical_cwd_clone,
                            project_id_clone.as_deref(),
                        ) {
                            return Err(NativeError::new(
                                NativeErrorCode::NeedsVerification,
                                "Refreshed Codex thread metadata did not match verified chat scope",
                            ));
                        }
                        Ok(thread)
                    })
                    .await
                }
            },
        };

        if !self.require_call_identity.load(Ordering::Acquire)
            || self.verify_call(thread_id).is_err()
        {
            return None;
        }
        let mut guard = self
            .observation_context
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let cached = guard.as_mut()?;
        if cached.server_pid != server_pid
            || !constant_time_str_eq(&cached.connection_nonce, connection_nonce)
            || !constant_time_str_eq(&cached.epoch_id, epoch_id)
        {
            return None;
        }
        cached.context.matching_scope(binding_id, thread_id)?;
        match refreshed {
            Ok(Ok(thread)) => {
                cached.context.refresh_thread_metadata(
                    thread.title.as_deref(),
                    thread.model.as_deref(),
                    thread.effort.as_deref(),
                );
            }
            Ok(Err(_)) | Err(_) => {
                cached.context.clear_selected_model();
            }
        }
        cached
            .context
            .matching_scope(binding_id, thread_id)
            .cloned()
    }

    pub(crate) async fn connect_with_context_config(
        &self,
        hint: String,
        state_dir: &Path,
        store: &PairingStore,
        identity: &ConnectionIdentity,
        runtime: AutomaticPairingRuntime<'_>,
    ) -> Result<AutomaticPairingResult, NativeError> {
        let AutomaticPairingRuntime {
            context_config,
            parent_env,
            sampler_owner,
        } = runtime;
        let operation = async {
            let _guard = self.coordinator.lock().await;
            let current = store.inspect_epoch(&identity.connection_nonce, &identity.epoch_id)?;
            if current.server_pid != std::process::id() {
                return Err(NativeError::new(
                    NativeErrorCode::ExpiredEpoch,
                    "MCP connection changed before pairing",
                ));
            }
            let identity = &current;
            if let Ok(enrolled) =
                store.verify_enrolled_binding(&identity.connection_nonce, &identity.epoch_id)
            {
                if enrolled.thread_id != hint {
                    return Err(NativeError::new(
                        NativeErrorCode::SharedConnectionRejected,
                        "MCP connection already belongs to another chat",
                    ));
                }
            }
            let (reconnect, backend_pid) = parent_env.discover_reconnect(
                NativeErrorCode::NotConfigured,
                "Automatic pairing requires the managed Codex desktop connection on Linux.",
            )?;
            let native = NativeClient::new_managed_websocket(reconnect.clone())?
                .with_expected_backend_pid(backend_pid);
            let cwd = parent_env.resolve_project_cwd()?;
            let candidate = verify_thread_hint_candidate(&native, &hint, identity, &cwd).await?;
            let metadata_envelope = native.thread_metadata(&candidate.thread_id).await?;
            let metadata = metadata_envelope.thread.clone();
            let mut label = String::new();
            for ch in metadata.title.as_deref().unwrap_or("Codex chat").chars() {
                if !ch.is_control() && label.len() + ch.len_utf8() <= threadlane_core::MAX_LABEL_LEN
                {
                    label.push(ch);
                }
            }
            let label = if label.trim().is_empty() {
                "Codex chat"
            } else {
                label.trim()
            };
            let own_exe = std::env::current_exe().map_err(|_| {
                NativeError::new(
                    NativeErrorCode::NotConfigured,
                    "Cannot locate the Threadlane package",
                )
            })?;
            let default_executable = own_exe
                .parent()
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::NotConfigured,
                        "Cannot locate the Threadlane package",
                    )
                })?
                .join("threadlane");
            let executable = match context_config
                .sibling_anchor_exe
                .and_then(Path::parent)
                .map(|dir| dir.join("threadlane"))
            {
                Some(candidate) if candidate.exists() => candidate,
                _ => default_executable,
            };
            ensure_control_runtime(state_dir, &executable, Duration::from_secs(5)).await?;
            let supervisor = SupervisorClient::from_state_dir(state_dir).map_err(|_| {
                NativeError::new(
                    NativeErrorCode::TransportUnavailable,
                    "Threadlane control runtime is unavailable",
                )
            })?;
            // A repeat is safe even if a prior response was lost: binding creation is idempotent.
            let binding = supervisor
                .get_or_create_binding(&candidate.thread_id, label)
                .await
                .map_err(|_| {
                    NativeError::new(
                        NativeErrorCode::NeedsVerification,
                        "Cannot resolve a unique Threadlane binding for this chat",
                    )
                })?;
            let checked = verify_thread_hint_candidate(&native, &hint, identity, &cwd).await?;
            let (last_config, last_pid) = parent_env.discover_reconnect(
                NativeErrorCode::ExpiredEpoch,
                "Codex parent connection changed",
            )?;
            if last_config != reconnect
                || last_pid != backend_pid
                || checked.thread_id != candidate.thread_id
                || checked.cwd != candidate.cwd
                || checked.project_id != candidate.project_id
                || checked.server_pid != candidate.server_pid
                || checked.epoch_state != candidate.epoch_state
                || checked.enrolled_binding_id != candidate.enrolled_binding_id
            {
                return Err(NativeError::new(
                    NativeErrorCode::ExpiredEpoch,
                    "Codex connection changed during pairing",
                ));
            }
            let verified_cwd = checked.cwd.clone();
            let verified_project_id = checked.project_id.clone();
            let (selected_model, selected_effort) = if validate_refreshed_thread_metadata(
                &metadata,
                &checked.thread_id,
                &verified_cwd,
                verified_project_id.as_deref(),
            ) {
                (metadata.model.as_deref(), metadata.effort.as_deref())
            } else {
                (None, None)
            };
            // Enable the per-call guard before enrollment becomes visible to another handler.
            self.require_call_identity.store(true, Ordering::Release);
            let token = store.operator_token()?;
            let enrolled = store.enroll(
                &token,
                OperatorEnrollRequest {
                    connection_nonce: identity.connection_nonce.clone(),
                    epoch_id: identity.epoch_id.clone(),
                    binding_id: binding.binding.binding_id,
                    thread_id: checked.thread_id,
                    project_id: checked.project_id,
                },
            )?;
            self.record_managed_observation_context(
                &identity.connection_nonce,
                &identity.epoch_id,
                identity.server_pid,
                &enrolled.binding_id,
                &enrolled.thread_id,
                label,
                &verified_cwd,
                verified_project_id.as_deref(),
                selected_model,
                selected_effort,
                reconnect.clone(),
                backend_pid,
                parent_env.clone(),
            );
            sampler_owner
                .ensure_started_after_verified_connect(SamplerStartRequest {
                    connection_nonce: &identity.connection_nonce,
                    epoch_id: &identity.epoch_id,
                    server_pid: identity.server_pid,
                    binding_id: &enrolled.binding_id,
                    thread_uuid: &enrolled.thread_id,
                    verified_cwd: &verified_cwd,
                    verified_project_id: verified_project_id.as_deref(),
                    initial_metadata: &metadata_envelope,
                    reconnect,
                    backend_pid,
                    parent_env,
                    pairing_store: store.clone(),
                    automatic_pairing: self.clone(),
                    context_config: &context_config,
                })
                .await?;
            let operator_context =
                bootstrap_verified_operator_context(&OperatorContextExportRequest {
                    binding_id: &enrolled.binding_id,
                    thread_uuid: &enrolled.thread_id,
                    verified_project_root: &verified_cwd,
                    server_config: context_config,
                });
            Ok(AutomaticPairingResult {
                connected: true,
                binding_id: enrolled.binding_id,
                thread_id: enrolled.thread_id,
                binding_created: binding.created,
                conversation_role: "coworker",
                native_per_call_identity_verified: false,
                operator_context,
            })
        };
        tokio::time::timeout(Duration::from_secs(25), operation)
            .await
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::TimedOut,
                    "Automatic pairing timed out; inspect connection state before retrying.",
                )
            })?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn thread_id_hints_require_canonical_ids_and_never_accept_session_fallbacks() {
        assert!(parse_thread_hint(None).is_err());
        assert!(parse_thread_hint(Some("session-id")).is_err());
        assert!(parse_thread_hint(Some("00000000-0000-0000-0000-000000000000")).is_err());
        assert!(parse_thread_hint(Some("0196ed00-0000-7000-8000-00000000000a")).is_ok());
    }
    #[tokio::test]
    async fn automatic_connections_reject_missing_or_other_chat_context() {
        let state = AutomaticPairing::default();
        state.require_call_identity.store(true, Ordering::Release);
        assert!(state.verify_call("chat-a").is_err());
        CALL_THREAD_HINT
            .scope(Some("chat-b".into()), async {
                assert!(state.verify_call("chat-a").is_err());
            })
            .await;
        CALL_THREAD_HINT
            .scope(Some("chat-a".into()), async {
                assert!(state.verify_call("chat-a").is_ok());
            })
            .await;
    }

    #[tokio::test]
    async fn cached_observation_context_enforces_exact_connection_binding_thread_and_call_scope() {
        let state = AutomaticPairing::default();
        let nonce = "nonce_0196ed00-0000-7000-8000-000000000001";
        let epoch = "ep_0196ed00-0000-7000-8000-000000000001";
        let pid = std::process::id();
        let binding_a = "bnd_0196ed00-0000-7000-8000-000000000001";
        let thread_a = "0196ed00-0000-7000-8000-00000000000a";

        state.record_verified_observation_context(
            nonce,
            epoch,
            pid,
            binding_a,
            thread_a,
            "Architecture Chat",
            Path::new("/tmp/workspace/threadlane"),
            Some("proj_threadlane"),
        );

        // Without matching CALL_THREAD_HINT, observation context is never returned.
        assert!(state
            .observation_context_for_call(nonce, epoch, pid, binding_a, thread_a)
            .is_none());

        CALL_THREAD_HINT
            .scope(Some("0196ed00-0000-7000-8000-00000000000b".into()), async {
                assert!(state
                    .observation_context_for_call(nonce, epoch, pid, binding_a, thread_a)
                    .is_none());
            })
            .await;

        CALL_THREAD_HINT
            .scope(Some(thread_a.into()), async {
                let ctx = state
                    .observation_context_for_call(nonce, epoch, pid, binding_a, thread_a)
                    .expect("matching verified call context");
                assert_eq!(ctx.thread_label, "Architecture Chat");
                assert_eq!(ctx.project_label, "threadlane");
                assert_eq!(
                    ctx.project_path.as_deref(),
                    Some("/tmp/workspace/threadlane")
                );
                assert_eq!(
                    ctx.actor.label.as_deref(),
                    Some("Codex · Architecture Chat")
                );
                assert_eq!(ctx.actor.role.as_deref(), Some("coworker"));
                assert_eq!(ctx.actor.source.as_deref(), Some("paired_mcp"));
                assert_eq!(ctx.actor.model, None);
                assert_eq!(ctx.actor.effort, None);

                // Cross-binding, cross-thread, wrong nonce, wrong epoch, or wrong PID must never inherit context.
                assert!(state
                    .observation_context_for_call(nonce, epoch, pid, "bnd_other", thread_a)
                    .is_none());
                assert!(state
                    .observation_context_for_call(
                        nonce,
                        epoch,
                        pid,
                        binding_a,
                        "0196ed00-0000-7000-8000-00000000000b"
                    )
                    .is_none());
                assert!(state
                    .observation_context_for_call("nonce_other", epoch, pid, binding_a, thread_a)
                    .is_none());
                assert!(state
                    .observation_context_for_call(nonce, "ep_other", pid, binding_a, thread_a)
                    .is_none());
                assert!(state
                    .observation_context_for_call(
                        nonce,
                        epoch,
                        pid.saturating_add(1),
                        binding_a,
                        thread_a
                    )
                    .is_none());
            })
            .await;

        state.clear_observation_context();
        CALL_THREAD_HINT
            .scope(Some(thread_a.into()), async {
                assert!(state
                    .observation_context_for_call(nonce, epoch, pid, binding_a, thread_a)
                    .is_none());
            })
            .await;

        let result_json = serde_json::to_value(AutomaticPairingResult {
            connected: true,
            binding_id: binding_a.to_string(),
            thread_id: thread_a.to_string(),
            binding_created: true,
            conversation_role: "coworker",
            native_per_call_identity_verified: false,
            operator_context: OperatorContextBootstrap::unavailable(&NativeError::new(
                NativeErrorCode::NotConfigured,
                "not configured in unit test",
            )),
        })
        .expect("serialize AutomaticPairingResult");
        assert_eq!(result_json["conversation_role"], "coworker");
        assert_eq!(result_json["native_per_call_identity_verified"], false);
        assert_eq!(result_json["operator_context"]["available"], false);
        assert_eq!(
            result_json["operator_context"]["route"],
            "trusted_local_operator"
        );
        assert_eq!(
            result_json["operator_context"]["native_per_call_identity_verified"],
            false
        );
        assert_eq!(
            result_json["operator_context"]["human_approval_verified"],
            false
        );
    }

    #[tokio::test]
    async fn metadata_alone_cannot_create_a_binding_or_start_a_runtime() {
        let tmp = tempfile::TempDir::new().expect("temporary directory");
        let runtime = tmp.path().join("runtime");
        let pairing = PairingStore::open(tmp.path().join("pairing")).expect("pairing store");
        let server = crate::PairedMcpServer::new(&runtime, pairing).expect("server");
        for meta in [
            None,
            Some(serde_json::json!({"threadId": "0196ed00-0000-7000-8000-00000000000a"})),
        ] {
            let error = server
                .invoke_tool_json("connect_current_chat", serde_json::json!({}), meta)
                .await
                .expect_err("unverified host must refuse pairing");
            assert_eq!(error.is_error, Some(true));
            assert!(
                !runtime.exists(),
                "unverified metadata must not start a control runtime"
            );
        }
        let identity = server
            .pairing_store()
            .inspect_epoch(server.connection_nonce(), server.epoch_id())
            .expect("identity");
        assert_eq!(identity.state, crate::ConnectionEpochState::Unbound);
    }

    #[tokio::test]
    async fn refresh_observation_context_tracks_model_switch_and_clears_on_failure_or_scope_mismatch(
    ) {
        let state = AutomaticPairing::default();
        let nonce = "nonce_0196ed00-0000-7000-8000-000000000010";
        let epoch = "ep_0196ed00-0000-7000-8000-000000000010";
        let pid = std::process::id();
        let binding_a = "bnd_0196ed00-0000-7000-8000-000000000010";
        let thread_a = "0196ed00-0000-7000-8000-00000000001a";
        let cwd = Path::new("/tmp/workspace/threadlane");

        state.record_verified_observation_context_with_selection(
            nonce,
            epoch,
            pid,
            binding_a,
            thread_a,
            "Architecture Chat",
            cwd,
            Some("proj_threadlane"),
            Some("GPT-5.4"),
            Some("high"),
        );

        CALL_THREAD_HINT
            .scope(Some(thread_a.into()), async {
                let initial = state
                    .refresh_observation_context_for_call(nonce, epoch, pid, binding_a, thread_a)
                    .await
                    .expect("initial context");
                assert_eq!(initial.actor.selected_model.as_deref(), Some("GPT-5.4"));
                assert_eq!(initial.actor.selected_effort.as_deref(), Some("high"));
                assert_eq!(initial.actor.model, None);
                assert_eq!(initial.actor.effort, None);

                // Human switches Codex model to Sol / xhigh on the connected thread.
                state.set_test_metadata_refresher(move |requested_thread| {
                    assert_eq!(requested_thread, thread_a);
                    Ok(NativeThread {
                        id: thread_a.to_string(),
                        project_id: Some("proj_threadlane".to_string()),
                        title: Some("Architecture Chat".to_string()),
                        cwd: "/tmp/workspace/threadlane".to_string(),
                        status: "idle".to_string(),
                        model: Some("Sol".to_string()),
                        effort: Some("xhigh".to_string()),
                        can_accept_direct_input: Some(true),
                    })
                });
                let switched = state
                    .refresh_observation_context_for_call(nonce, epoch, pid, binding_a, thread_a)
                    .await
                    .expect("switched context");
                assert_eq!(switched.actor.selected_model.as_deref(), Some("Sol"));
                assert_eq!(switched.actor.selected_effort.as_deref(), Some("xhigh"));
                assert_eq!(switched.actor.model, None);
                assert_eq!(switched.actor.effort, None);

                // Refresh failure clears selected_model/selected_effort rather than keeping stale Sol.
                state.set_test_metadata_refresher(|_| {
                    Err(NativeError::new(
                        NativeErrorCode::TransportUnavailable,
                        "managed backend unavailable",
                    ))
                });
                let failed_refresh = state
                    .refresh_observation_context_for_call(nonce, epoch, pid, binding_a, thread_a)
                    .await
                    .expect("context remains usable on refresh failure");
                assert_eq!(failed_refresh.actor.selected_model, None);
                assert_eq!(failed_refresh.actor.selected_effort, None);
                assert_eq!(
                    failed_refresh.actor.label.as_deref(),
                    Some("Codex · Architecture Chat")
                );

                // Restore valid selection, then test scope mismatches (wrong thread, wrong cwd, wrong project_id).
                state.set_test_metadata_refresher(move |_| {
                    Ok(NativeThread {
                        id: thread_a.to_string(),
                        project_id: Some("proj_threadlane".to_string()),
                        title: Some("Architecture Chat".to_string()),
                        cwd: "/tmp/workspace/threadlane".to_string(),
                        status: "active".to_string(),
                        model: Some("GPT-5.4".to_string()),
                        effort: Some("high".to_string()),
                        can_accept_direct_input: Some(true),
                    })
                });
                let restored = state
                    .refresh_observation_context_for_call(nonce, epoch, pid, binding_a, thread_a)
                    .await
                    .expect("restored context");
                assert_eq!(restored.actor.selected_model.as_deref(), Some("GPT-5.4"));

                // Wrong thread ID in metadata response must never leak model/title and must clear selection.
                state.set_test_metadata_refresher(|_| {
                    Ok(NativeThread {
                        id: "0196ed00-0000-7000-8000-000000000099".to_string(),
                        project_id: Some("proj_threadlane".to_string()),
                        title: Some("Other Chat".to_string()),
                        cwd: "/tmp/workspace/threadlane".to_string(),
                        status: "idle".to_string(),
                        model: Some("LeakedModel".to_string()),
                        effort: Some("low".to_string()),
                        can_accept_direct_input: Some(true),
                    })
                });
                let wrong_thread = state
                    .refresh_observation_context_for_call(nonce, epoch, pid, binding_a, thread_a)
                    .await
                    .expect("wrong thread metadata clears selection");
                assert_eq!(wrong_thread.thread_label, "Architecture Chat");
                assert_eq!(wrong_thread.actor.selected_model, None);
                assert_eq!(wrong_thread.actor.selected_effort, None);
            })
            .await;
    }
}
