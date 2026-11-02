//! Local managed-desktop pairing. Request metadata is only a hint until the existing
//! parent app-server corroborates the exact process-generated connection identity.

use serde::Serialize;
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use threadlane::{NativeReconnectConfig, SupervisorClient};
use threadlane_core::observation::VerifiedMcpObservationContext;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::fs_safety::constant_time_str_eq;
use crate::{
    ensure_control_runtime, verify_thread_hint_candidate, ConnectionIdentity, NativeClient,
    NativeError, NativeErrorCode, OperatorEnrollRequest, PairingStore,
};

tokio::task_local! {
    pub(crate) static CALL_THREAD_HINT: Option<String>;
}

#[derive(Debug, Clone)]
struct CachedObservationContext {
    connection_nonce: String,
    epoch_id: String,
    server_pid: u32,
    context: VerifiedMcpObservationContext,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct AutomaticPairing {
    pub(crate) require_call_identity: Arc<AtomicBool>,
    coordinator: Arc<Mutex<()>>,
    observation_context: Arc<std::sync::Mutex<Option<CachedObservationContext>>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct AutomaticPairingResult {
    connected: bool,
    binding_id: String,
    thread_id: String,
    binding_created: bool,
    conversation_role: &'static str,
    native_per_call_identity_verified: bool,
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

impl AutomaticPairing {
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
        self.require_call_identity.store(true, Ordering::Release);
        let context = VerifiedMcpObservationContext::new(
            binding_id,
            thread_id,
            thread_title,
            canonical_cwd,
            project_id,
        );
        let mut guard = self
            .observation_context
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = Some(CachedObservationContext {
            connection_nonce: connection_nonce.to_string(),
            epoch_id: epoch_id.to_string(),
            server_pid,
            context,
        });
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

    pub(crate) async fn connect(
        &self,
        hint: String,
        state_dir: &Path,
        store: &PairingStore,
        identity: &ConnectionIdentity,
    ) -> Result<AutomaticPairingResult, NativeError> {
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
            let (reconnect, backend_pid) = NativeReconnectConfig::discover_for_mcp_parent()
                .map_err(|_| {
                    NativeError::new(
                        NativeErrorCode::NotConfigured,
                        "Automatic pairing requires the managed Codex desktop connection on Linux.",
                    )
                })?;
            let native = NativeClient::new_managed_websocket(reconnect.clone())?
                .with_expected_backend_pid(backend_pid);
            let cwd = std::env::current_dir().map_err(|_| {
                NativeError::new(
                    NativeErrorCode::InvalidConfig,
                    "Cannot determine the MCP project directory",
                )
            })?;
            let candidate = verify_thread_hint_candidate(&native, &hint, identity, &cwd).await?;
            let metadata = native.thread(&candidate.thread_id).await?;
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
            let executable = own_exe
                .parent()
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::NotConfigured,
                        "Cannot locate the Threadlane package",
                    )
                })?
                .join("threadlane");
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
            let (last_config, last_pid) = NativeReconnectConfig::discover_for_mcp_parent()
                .map_err(|_| {
                    NativeError::new(
                        NativeErrorCode::ExpiredEpoch,
                        "Codex parent connection changed",
                    )
                })?;
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
            self.record_verified_observation_context(
                &identity.connection_nonce,
                &identity.epoch_id,
                identity.server_pid,
                &enrolled.binding_id,
                &enrolled.thread_id,
                label,
                &verified_cwd,
                verified_project_id.as_deref(),
            );
            Ok(AutomaticPairingResult {
                connected: true,
                binding_id: enrolled.binding_id,
                thread_id: enrolled.thread_id,
                binding_created: binding.created,
                conversation_role: "coworker",
                native_per_call_identity_verified: false,
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
        })
        .expect("serialize AutomaticPairingResult");
        assert_eq!(result_json["conversation_role"], "coworker");
        assert_eq!(result_json["native_per_call_identity_verified"], false);
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
}
