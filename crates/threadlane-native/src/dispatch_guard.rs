//! Shared conversation-change advancement guard for direct native task dispatch (`threadlane-session dispatch`
//! and MCP `start_approved_task`).
//!
//! Serializes direct task admission and dispatch against per-binding `record-change` and
//! `resolve-change` mutations using [`acquire_conversation_advancement_guard`] and
//! [`inspect_unresolved_blocking_changes`]. Refuses unresolved blocking changes (`scope_change`,
//! `clarification`, `model_or_profile_change`, `stop_or_cancel`), superseded approvals, and
//! uncertain cancellation markers (`ambiguous_cancels`) before any host auto-start, plan
//! materialization, `TaskDispatchIntent`, or provider spawn can occur, and holds the lock until
//! the dispatch receipt is durably sealed.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use threadlane_authority::validation::{
    validate_canonical_thread_uuid, validate_identifier, validate_sha256_hex,
};
use threadlane_authority::{ApprovalRecord, AuthorityError, AuthorityStore};
use threadlane_host::{ApprovedHostClient, HostError, TaskStateSnapshot};
use threadlane_workflow::ToolAdapter;

use crate::conversation_change::{
    acquire_conversation_advancement_guard, inspect_unresolved_blocking_changes,
    ConversationChangeError, ConversationChangeStore,
};
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{constant_time_str_eq, CrossProcessLockGuard};

const HOST_NOT_READY_TIMEOUT_MESSAGE: &str =
    "Approved host session is not running or has not reached authenticated ready state.";

tokio::task_local! {
    static ACTIVE_DISPATCH_GUARD: (PathBuf, String);
}

/// Categorized error returned by guarded native task dispatch helpers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardedDispatchError {
    /// Deterministic refusal before any host launch, plan materialization, dispatch intent, or worker spawn.
    PreEffect {
        /// Stable machine-readable `snake_case` error code.
        code: &'static str,
        /// Sanitized static human-safe error message.
        message: &'static str,
    },
    /// Error returned by [`ApprovedHostClient::dispatch_task`] after passing the conversation-change gate.
    Host(HostError),
}

impl GuardedDispatchError {
    /// Constructs a deterministic pre-effect refusal error.
    pub const fn pre_effect(code: &'static str, message: &'static str) -> Self {
        Self::PreEffect { code, message }
    }
}

/// Validates approval/task/binding/thread scope, acquires the per-binding conversation advancement
/// lock (unless already held by an enclosing guarded native dispatch scope on the current task),
/// verifies no unresolved blocking conversational changes apply to `(binding_id, approval_id, task_id)`,
/// and executes `op` while keeping the lock alive until `op` completes.
pub async fn with_guarded_native_dispatch<T, F, Fut>(
    authority: &Arc<AuthorityStore>,
    binding_id: &str,
    thread_uuid: &str,
    approval_id: &str,
    task_id: &str,
    expected_subject_digest: Option<&str>,
    op: F,
) -> Result<T, GuardedDispatchError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, GuardedDispatchError>>,
{
    with_guarded_native_dispatch_until(
        authority,
        binding_id,
        thread_uuid,
        approval_id,
        task_id,
        expected_subject_digest,
        None,
        op,
    )
    .await
}

/// Like [`with_guarded_native_dispatch`], but bounds pre-effect authority metadata loading,
/// conversation-change lock acquisition, and blocker inspection by `admission_deadline` when
/// supplied. Never moves `op` into a detached background task.
#[allow(clippy::too_many_arguments)]
pub async fn with_guarded_native_dispatch_until<T, F, Fut>(
    authority: &Arc<AuthorityStore>,
    binding_id: &str,
    thread_uuid: &str,
    approval_id: &str,
    task_id: &str,
    expected_subject_digest: Option<&str>,
    admission_deadline: Option<Instant>,
    op: F,
) -> Result<T, GuardedDispatchError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, GuardedDispatchError>>,
{
    let checked_approval_id = validate_approval_id(approval_id)?;
    let checked_task_id = validate_identifier("task_id", task_id).map_err(|_| {
        GuardedDispatchError::pre_effect(
            "invalid_input",
            "Parameter `task_id` failed identifier syntax validation.",
        )
    })?;
    let checked_binding_id = validate_identifier("binding_id", binding_id).map_err(|_| {
        GuardedDispatchError::pre_effect(
            "invalid_input",
            "Parameter `binding_id` failed identifier syntax validation.",
        )
    })?;
    let checked_thread_uuid =
        validate_canonical_thread_uuid("thread_uuid", thread_uuid).map_err(|_| {
            GuardedDispatchError::pre_effect(
                "invalid_input",
                "Parameter `thread_uuid` must be a canonical lowercase UUID.",
            )
        })?;
    let checked_digest = match expected_subject_digest {
        Some(raw) => Some(validate_sha256_hex("confirm_approval", raw).map_err(|_| {
            GuardedDispatchError::pre_effect(
                "invalid_input",
                "Confirmation digest must be a 64-character lowercase hexadecimal SHA-256 digest.",
            )
        })?),
        None => None,
    };

    let record =
        load_approval_record_within_deadline(authority, &checked_approval_id, admission_deadline)
            .await?;

    if record.binding_id != checked_binding_id || record.thread_uuid != checked_thread_uuid {
        return Err(GuardedDispatchError::pre_effect(
            "scope_mismatch",
            "Approval record or host session does not belong to the enrolled chat binding and native thread.",
        ));
    }

    if let Some(ref digest) = checked_digest {
        if !constant_time_str_eq(&record.subject_digest, digest) {
            return Err(GuardedDispatchError::pre_effect(
                "subject_mismatch",
                "Approval confirmation digest does not match the frozen approval record's subject_digest.",
            ));
        }
    }

    let Some(task) = record.tasks.iter().find(|t| t.task_id == checked_task_id) else {
        return Err(GuardedDispatchError::pre_effect(
            "unknown_task",
            "Task was not found in the frozen approval record.",
        ));
    };

    if task.provider.is_none()
        || task.adapter == ToolAdapter::CodexCli
        || task.profile_id.is_none()
        || task.command_key.is_none()
    {
        return Err(GuardedDispatchError::pre_effect(
            "native_route_required",
            "Task targets a manual native route and cannot be dispatched as a background worker.",
        ));
    }

    let plans_dir = authority.plans().dir().to_path_buf();
    let already_guarded = ACTIVE_DISPATCH_GUARD
        .try_with(|(active_dir, active_binding)| {
            active_dir == &plans_dir && active_binding == &checked_binding_id
        })
        .unwrap_or(false);

    if already_guarded {
        verify_no_blocking_conversation_changes_within_deadline(
            &plans_dir,
            &checked_binding_id,
            &record.approval_id,
            &checked_task_id,
            admission_deadline,
        )
        .await?;
        ensure_admission_deadline_not_expired(admission_deadline)?;
        op().await
    } else {
        let _advancement_guard = acquire_advancement_guard_within_deadline(
            &plans_dir,
            &checked_binding_id,
            admission_deadline,
        )
        .await?;
        verify_no_blocking_conversation_changes_within_deadline(
            &plans_dir,
            &checked_binding_id,
            &record.approval_id,
            &checked_task_id,
            admission_deadline,
        )
        .await?;
        ensure_admission_deadline_not_expired(admission_deadline)?;
        ACTIVE_DISPATCH_GUARD
            .scope((plans_dir, checked_binding_id), async move {
                let result = op().await;
                drop(_advancement_guard);
                result
            })
            .await
    }
}

/// Validates scope, acquires the per-binding conversation advancement guard, verifies no
/// unresolved blocking conversational changes exist for `(binding_id, approval_id, task_id)`,
/// runs `pre_dispatch`, and invokes [`ApprovedHostClient::dispatch_task`] while holding the guard
/// through the sealed dispatch receipt.
#[allow(clippy::too_many_arguments)] // Frozen identity fields remain explicit at both native entrypoints.
pub async fn dispatch_approved_task_guarded<F, Fut>(
    authority: &Arc<AuthorityStore>,
    client: &ApprovedHostClient,
    binding_id: &str,
    thread_uuid: &str,
    approval_id: &str,
    task_id: &str,
    expected_subject_digest: Option<&str>,
    pre_dispatch: F,
) -> Result<TaskStateSnapshot, GuardedDispatchError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), GuardedDispatchError>>,
{
    if client.control_binding_id() != binding_id || client.native_thread_uuid() != thread_uuid {
        return Err(GuardedDispatchError::pre_effect(
            "scope_mismatch",
            "Approval record or host session does not belong to the enrolled chat binding and native thread.",
        ));
    }

    with_guarded_native_dispatch(
        authority,
        binding_id,
        thread_uuid,
        approval_id,
        task_id,
        expected_subject_digest,
        || async move {
            pre_dispatch().await?;
            client
                .dispatch_task(approval_id, task_id)
                .await
                .map_err(GuardedDispatchError::Host)
        },
    )
    .await
}

/// Inspects the binding's conversation-change ledger under the advancement lock and refuses
/// unresolved blocking changes, superseded approvals, or uncertain cancellation markers.
pub fn verify_no_blocking_conversation_changes(
    plans_dir: &Path,
    binding_id: &str,
    approval_id: &str,
    task_id: &str,
) -> Result<(), GuardedDispatchError> {
    match inspect_unresolved_blocking_changes(
        plans_dir,
        binding_id,
        Some(approval_id),
        Some(task_id),
    ) {
        Ok(summary) => {
            if !summary.blocked {
                return Ok(());
            }
            let reason = if summary.approval_superseded {
                "Approved task dispatch is blocked because this approval was superseded by a resolved conversational change; use the newly linked plan revision and fresh approval."
            } else if summary.replan_required || summary.new_approval_required {
                "Approved task dispatch is blocked by an unresolved conversational change requiring a new plan revision and fresh operator approval."
            } else {
                "Approved task dispatch is blocked by an unresolved conversational clarification; resolve the change before dispatching affected work."
            };
            let err = ConversationChangeError::BlockedByConversationChange { reason };
            Err(GuardedDispatchError::pre_effect(
                err.code(),
                err.safe_message(),
            ))
        }
        Err(native_err) => {
            if let Ok(store) = ConversationChangeStore::open_readonly(plans_dir) {
                if let Err(change_err) =
                    store.unresolved_blocking_changes(binding_id, Some(approval_id), Some(task_id))
                {
                    return Err(GuardedDispatchError::pre_effect(
                        change_err.code(),
                        change_err.safe_message(),
                    ));
                }
            }
            Err(map_native_error(&native_err))
        }
    }
}

const fn startup_timeout_pre_effect() -> GuardedDispatchError {
    GuardedDispatchError::pre_effect("host_not_ready", HOST_NOT_READY_TIMEOUT_MESSAGE)
}

fn ensure_admission_deadline_not_expired(
    admission_deadline: Option<Instant>,
) -> Result<(), GuardedDispatchError> {
    if let Some(deadline) = admission_deadline {
        if Instant::now() >= deadline {
            return Err(startup_timeout_pre_effect());
        }
    }
    Ok(())
}

async fn run_within_admission_deadline<T, Fut>(
    admission_deadline: Option<Instant>,
    fut: Fut,
) -> Result<T, GuardedDispatchError>
where
    Fut: Future<Output = Result<T, GuardedDispatchError>>,
{
    match admission_deadline {
        None => fut.await,
        Some(deadline) => {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return Err(startup_timeout_pre_effect());
            };
            match tokio::time::timeout(remaining, fut).await {
                Ok(res) => res,
                Err(_elapsed) => Err(startup_timeout_pre_effect()),
            }
        }
    }
}

async fn load_approval_record_within_deadline(
    authority: &Arc<AuthorityStore>,
    approval_id: &str,
    admission_deadline: Option<Instant>,
) -> Result<ApprovalRecord, GuardedDispatchError> {
    let auth = Arc::clone(authority);
    let approval_id = approval_id.to_string();
    run_within_admission_deadline(admission_deadline, async move {
        tokio::task::spawn_blocking(move || {
            auth.get_for_operator(&approval_id)
                .map_err(|err| map_authority_error(&err))
        })
        .await
        .map_err(|_| {
            GuardedDispatchError::pre_effect(
                "io_error",
                "Filesystem I/O error occurred while accessing the authority store.",
            )
        })?
    })
    .await
}

async fn acquire_advancement_guard_within_deadline(
    plans_dir: &Path,
    binding_id: &str,
    admission_deadline: Option<Instant>,
) -> Result<CrossProcessLockGuard, GuardedDispatchError> {
    run_within_admission_deadline(admission_deadline, async move {
        acquire_conversation_advancement_guard(plans_dir, binding_id)
            .await
            .map_err(|err| map_native_error(&err))
    })
    .await
}

async fn verify_no_blocking_conversation_changes_within_deadline(
    plans_dir: &Path,
    binding_id: &str,
    approval_id: &str,
    task_id: &str,
    admission_deadline: Option<Instant>,
) -> Result<(), GuardedDispatchError> {
    let plans_dir = plans_dir.to_path_buf();
    let binding_id = binding_id.to_string();
    let approval_id = approval_id.to_string();
    let task_id = task_id.to_string();
    run_within_admission_deadline(admission_deadline, async move {
        tokio::task::spawn_blocking(move || {
            verify_no_blocking_conversation_changes(&plans_dir, &binding_id, &approval_id, &task_id)
        })
        .await
        .map_err(|_| {
            GuardedDispatchError::pre_effect(
                "io_error",
                "Filesystem I/O error occurred while accessing the conversation-change store.",
            )
        })?
    })
    .await
}

fn validate_approval_id(raw: &str) -> Result<String, GuardedDispatchError> {
    let trimmed = raw.trim();
    let Some(hex_part) = trimmed.strip_prefix("apr_") else {
        return Err(GuardedDispatchError::pre_effect(
            "invalid_input",
            "Parameter `approval_id` must be `apr_<32 lowercase hex>`.",
        ));
    };
    if trimmed != raw
        || hex_part.len() != 32
        || !hex_part
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(GuardedDispatchError::pre_effect(
            "invalid_input",
            "Parameter `approval_id` must be `apr_<32 lowercase hex>`.",
        ));
    }
    Ok(trimmed.to_string())
}

fn map_authority_error(err: &AuthorityError) -> GuardedDispatchError {
    let (code, message) = crate::approved_mcp::map_authority_error_to_mcp(err);
    GuardedDispatchError::pre_effect(code, message)
}

fn map_native_error(err: &NativeError) -> GuardedDispatchError {
    let (code, message) = match err.code {
        NativeErrorCode::SecurityViolation => (
            "security_violation",
            "Conversation-change store violated private permission or symlink security checks.",
        ),
        NativeErrorCode::UnsupportedPlatform => (
            "unsupported_platform",
            "Conversation change routing is only supported on Unix platforms.",
        ),
        NativeErrorCode::AmbiguousTransportFailure => (
            "ambiguous_outcome_pending_reconciliation",
            "A prior conversation-change cancellation had an ambiguous outcome; inspect task and change state before retrying.",
        ),
        NativeErrorCode::IoError => (
            "io_error",
            "Filesystem I/O error occurred while accessing the conversation-change store.",
        ),
        NativeErrorCode::PersistenceUncertain => (
            "persistence_uncertain",
            "A conversation-change replacement may be visible but its durability is unconfirmed; reconcile stored state before retrying.",
        ),
        _ => (
            "invalid_input",
            "Conversation-change store path or parameter failed validation.",
        ),
    };
    GuardedDispatchError::pre_effect(code, message)
}
