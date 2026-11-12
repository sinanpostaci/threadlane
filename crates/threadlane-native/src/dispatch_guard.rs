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
use crate::coworker_workflow::{CoworkerBindingLedger, CoworkerJobRecord, CoworkerWorkflowStore};
use crate::error::{NativeError, NativeErrorCode};
use crate::external_resolution::{
    ExternalResolutionReceipt, ExternalResolutionScope, ExternalResolutionTarget,
};
use crate::fs_safety::{constant_time_str_eq, CrossProcessLockGuard};

/// Stable pre-effect refusal code used when an operator-attested external resolution receipt
/// covers the requested task or full job.
pub const EXTERNAL_RESOLUTION_DISPATCH_BLOCKED: &str = "external_resolution_dispatch_blocked";

/// Why a stored external resolution receipt blocks a task or full-job dispatch effect.
///
/// The receipt is only preserved evidence. It is never review/integration/closure acceptance, a
/// dependency seal, or authorization to dispatch new work. Coverage remains blocking regardless
/// of stale applicability or local `fsync` witness state, including receipts reported as
/// [`crate::external_resolution::ExternalResolutionDurability::PersistenceUncertain`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalResolutionDispatchBlock {
    /// The receipt covers every task in the target job/legacy plan and closes job advancement.
    pub full_job: bool,
    /// Deterministic `exr_<32hex>` receipt identifier.
    pub receipt_id: String,
    /// Immutable operator request key of the blocking receipt.
    pub request_key: String,
    /// Concrete covered task identifiers when known for a job target (empty for legacy plans).
    pub covered_task_ids: Vec<String>,
}

/// Returns a blocking external resolution receipt when one covers `task_id`.
///
/// Only the already-loaded immutable binding ledger is inspected so callers can run this under
/// their innermost ledger lock without acquiring any additional store lock or creating state.
/// A matching `Job` receipt is resolved by explicit `job_id` or, for direct approved-task
/// dispatch, by the job's frozen `approval_id`/`plan_id` linkage. A `LegacyPlan` receipt matches
/// the approved plan identifier. Receipts are never treated as absent because they are stale or
/// because durability was not witnessed in the current service instance.
#[must_use]
pub fn external_resolution_blocks_task(
    ledger: &CoworkerBindingLedger,
    job_id: Option<&str>,
    plan_id: Option<&str>,
    approval_id: Option<&str>,
    task_id: &str,
) -> Option<ExternalResolutionDispatchBlock> {
    for receipt in &ledger.external_resolutions {
        if !receipt_matches_target(ledger, receipt, job_id, plan_id, approval_id) {
            continue;
        }
        let full_job = matches!(receipt.scope, ExternalResolutionScope::FullJob);
        let covers_task = match &receipt.scope {
            ExternalResolutionScope::FullJob => true,
            ExternalResolutionScope::Tasks { task_ids } => {
                task_ids.iter().any(|covered| covered == task_id)
            }
        };
        if covers_task {
            return Some(block_from_receipt(ledger, receipt, full_job));
        }
    }
    None
}

/// Returns a blocking external resolution receipt when a `FullJob` receipt covers the target.
///
/// `Task`-scoped receipts deliberately do not block unrelated same-job progress or controller
/// admission; their covered tasks are fenced individually by the dispatch boundary.
#[must_use]
pub fn external_resolution_blocks_full_job(
    ledger: &CoworkerBindingLedger,
    job_id: Option<&str>,
    plan_id: Option<&str>,
    approval_id: Option<&str>,
) -> Option<ExternalResolutionDispatchBlock> {
    for receipt in &ledger.external_resolutions {
        if !matches!(receipt.scope, ExternalResolutionScope::FullJob) {
            continue;
        }
        if receipt_matches_target(ledger, receipt, job_id, plan_id, approval_id) {
            return Some(block_from_receipt(ledger, receipt, true));
        }
    }
    None
}

fn receipt_matches_target(
    ledger: &CoworkerBindingLedger,
    receipt: &ExternalResolutionReceipt,
    job_id: Option<&str>,
    plan_id: Option<&str>,
    approval_id: Option<&str>,
) -> bool {
    match &receipt.target {
        ExternalResolutionTarget::Job {
            job_id: target_job_id,
        } => {
            if let Some(expected_job_id) = job_id {
                return target_job_id == expected_job_id;
            }
            let Some(job) = ledger
                .jobs
                .iter()
                .find(|candidate| &candidate.job_id == target_job_id)
            else {
                return false;
            };
            job_matches_target(job, plan_id, approval_id)
        }
        ExternalResolutionTarget::LegacyPlan {
            plan_id: target_plan_id,
            ..
        } => plan_id.is_some_and(|expected_plan_id| expected_plan_id == target_plan_id),
        ExternalResolutionTarget::CommitOnly { .. } => false,
    }
}

fn job_matches_target(
    job: &CoworkerJobRecord,
    plan_id: Option<&str>,
    approval_id: Option<&str>,
) -> bool {
    if let Some(expected_approval_id) = approval_id {
        if job
            .approval_linkage
            .as_ref()
            .is_some_and(|linkage| linkage.approval_id == expected_approval_id)
        {
            return true;
        }
    }
    if let Some(expected_plan_id) = plan_id {
        let approval_plan = job
            .approval_linkage
            .as_ref()
            .is_some_and(|linkage| linkage.plan_id == expected_plan_id);
        let planning_plan = job
            .planning_result
            .as_ref()
            .is_some_and(|planning| planning.plan_id == expected_plan_id);
        if approval_plan || planning_plan {
            return true;
        }
    }
    false
}

fn block_from_receipt(
    ledger: &CoworkerBindingLedger,
    receipt: &ExternalResolutionReceipt,
    full_job: bool,
) -> ExternalResolutionDispatchBlock {
    let covered_task_ids = match &receipt.target {
        ExternalResolutionTarget::Job { job_id } if full_job => ledger
            .jobs
            .iter()
            .find(|job| &job.job_id == job_id)
            .map(|job| {
                let mut ids: Vec<String> =
                    job.tasks.iter().map(|task| task.task_id.clone()).collect();
                ids.sort();
                ids
            })
            .unwrap_or_default(),
        _ => match &receipt.scope {
            ExternalResolutionScope::Tasks { task_ids } => task_ids.clone(),
            ExternalResolutionScope::FullJob => Vec::new(),
        },
    };
    ExternalResolutionDispatchBlock {
        full_job,
        receipt_id: receipt.receipt_id.clone(),
        request_key: receipt.request_key.clone(),
        covered_task_ids,
    }
}

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
        verify_no_external_resolution_dispatch_block_within_deadline(
            &plans_dir,
            &checked_binding_id,
            &record.plan_id,
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
        // Recheck immutable external-resolution coverage under the same per-binding advancement
        // lock used by `ExternalResolutionService::record`, so a new receipt cannot linearize
        // between this check and the dispatch effect.
        verify_no_external_resolution_dispatch_block_within_deadline(
            &plans_dir,
            &checked_binding_id,
            &record.plan_id,
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

fn verify_no_external_resolution_dispatch_block(
    plans_dir: &Path,
    binding_id: &str,
    plan_id: &str,
    approval_id: &str,
    task_id: &str,
) -> Result<(), GuardedDispatchError> {
    let store = CoworkerWorkflowStore::new(plans_dir).map_err(|err| map_native_error(&err))?;
    let ledger = store
        .read_binding_ledger(binding_id)
        .map_err(|err| map_native_error(&err))?;
    if let Some(block) =
        external_resolution_blocks_task(&ledger, None, Some(plan_id), Some(approval_id), task_id)
    {
        return Err(GuardedDispatchError::pre_effect(
            EXTERNAL_RESOLUTION_DISPATCH_BLOCKED,
            if block.full_job {
                "Approved task dispatch is blocked because an operator-attested external resolution receipt covers the full job. The receipt is not review, integration, closure, or dependency acceptance; the exact scope requires replan and explicit reapproval before new dispatch."
            } else {
                "Approved task dispatch is blocked because an operator-attested external resolution receipt covers this task. The receipt is not review, integration, closure, or dependency acceptance; the exact scope requires replan and explicit reapproval before new dispatch."
            },
        ));
    }
    Ok(())
}

async fn verify_no_external_resolution_dispatch_block_within_deadline(
    plans_dir: &Path,
    binding_id: &str,
    plan_id: &str,
    approval_id: &str,
    task_id: &str,
    admission_deadline: Option<Instant>,
) -> Result<(), GuardedDispatchError> {
    let plans_dir = plans_dir.to_path_buf();
    let binding_id = binding_id.to_string();
    let plan_id = plan_id.to_string();
    let approval_id = approval_id.to_string();
    let task_id = task_id.to_string();
    run_within_admission_deadline(admission_deadline, async move {
        tokio::task::spawn_blocking(move || {
            verify_no_external_resolution_dispatch_block(
                &plans_dir,
                &binding_id,
                &plan_id,
                &approval_id,
                &task_id,
            )
        })
        .await
        .map_err(|_| {
            GuardedDispatchError::pre_effect(
                "io_error",
                "Filesystem I/O error occurred while accessing the coworker workflow store.",
            )
        })?
    })
    .await
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use crate::coworker_workflow::CoworkerBindingLedger;
    use crate::external_resolution::{
        ExternalResolutionProjectRef, ExternalResolutionReceipt, ExternalResolutionScope,
        ExternalResolutionTarget,
    };

    fn receipt(
        request_key: &str,
        target: ExternalResolutionTarget,
        scope: ExternalResolutionScope,
    ) -> ExternalResolutionReceipt {
        ExternalResolutionReceipt {
            schema_version: 1,
            interface_version: crate::external_resolution::EXTERNAL_RESOLUTION_INTERFACE_VERSION
                .to_string(),
            receipt_id: format!("exr_{}", "0".repeat(32)),
            binding_id: "bnd_external_dispatch".to_string(),
            thread_uuid: "0196ed00-0000-7000-8000-0000000000a1".to_string(),
            request_key: request_key.to_string(),
            created_at_ms: 1,
            project: ExternalResolutionProjectRef {
                canonical_repo_root: PathBuf::from("/tmp/project"),
                base_commit: "0".repeat(40),
                base_tree_sha: Some("0".repeat(40)),
            },
            target,
            scope,
            resolved_task_ids: Vec::new(),
            source_ledger_revision: 0,
            recorded_ledger_revision: 1,
            source_job_revision: Some(1),
            source_controller_ledger_revision: 0,
            source_runtime_revision: None,
            work_brief_digest: Some("0".repeat(64)),
            plan_id: None,
            plan_revision: None,
            plan_digest: None,
            approval_id: None,
            approval_subject_digest: None,
            envelope_digest: None,
            explanation: "test receipt".to_string(),
            evidence_refs: Vec::new(),
            request_digest: "0".repeat(64),
            preview_digest: "0".repeat(64),
            receipt_digest: "0".repeat(64),
        }
    }

    fn ledger_with(receipts: Vec<ExternalResolutionReceipt>) -> CoworkerBindingLedger {
        let mut ledger = CoworkerBindingLedger::empty("bnd_external_dispatch").expect("empty");
        ledger.external_resolutions = receipts;
        ledger
    }

    #[test]
    fn task_and_full_job_coverage_is_conservative_and_scope_specific() {
        let empty = ledger_with(Vec::new());
        assert!(external_resolution_blocks_task(
            &empty,
            Some("job_a"),
            Some("plan_a"),
            Some("apr_a"),
            "task-1"
        )
        .is_none());

        let task_scope = ledger_with(vec![receipt(
            "req_partial",
            ExternalResolutionTarget::Job {
                job_id: "job_a".to_string(),
            },
            ExternalResolutionScope::Tasks {
                task_ids: vec!["task-2".to_string()],
            },
        )]);
        assert!(external_resolution_blocks_task(
            &task_scope,
            Some("job_a"),
            Some("plan_a"),
            Some("apr_a"),
            "task-1"
        )
        .is_none());
        let blocked = external_resolution_blocks_task(
            &task_scope,
            Some("job_a"),
            Some("plan_a"),
            Some("apr_a"),
            "task-2",
        )
        .expect("covered task must block");
        assert!(!blocked.full_job);
        assert_eq!(blocked.request_key, "req_partial");

        let full_scope = ledger_with(vec![receipt(
            "req_full",
            ExternalResolutionTarget::Job {
                job_id: "job_a".to_string(),
            },
            ExternalResolutionScope::FullJob,
        )]);
        let blocked = external_resolution_blocks_task(
            &full_scope,
            Some("job_a"),
            Some("plan_a"),
            Some("apr_a"),
            "task-1",
        )
        .expect("full-job coverage must block every task");
        assert!(blocked.full_job);
        assert!(external_resolution_blocks_full_job(
            &full_scope,
            Some("job_a"),
            Some("plan_a"),
            Some("apr_a")
        )
        .is_some());
        assert!(external_resolution_blocks_task(
            &full_scope,
            Some("job_b"),
            Some("plan_b"),
            Some("apr_b"),
            "task-1"
        )
        .is_none());
    }

    #[test]
    fn legacy_plan_coverage_blocks_approved_plan_tasks_regardless_of_local_durability() {
        let ledger = ledger_with(vec![receipt(
            "req_legacy",
            ExternalResolutionTarget::LegacyPlan {
                plan_id: "plan_legacy".to_string(),
                plan_revision: 2,
            },
            ExternalResolutionScope::FullJob,
        )]);
        assert!(external_resolution_blocks_task(
            &ledger,
            None,
            Some("plan_legacy"),
            Some("apr_legacy"),
            "task-1"
        )
        .is_some());
        assert!(external_resolution_blocks_task(
            &ledger,
            None,
            Some("plan_other"),
            Some("apr_legacy"),
            "task-1"
        )
        .is_none());
        // The pure predicate has no witness state: an on-disk receipt is always blocking, so a
        // restarted process cannot mistakenly treat persistence uncertainty as absence.
        assert!(external_resolution_blocks_full_job(
            &ledger,
            None,
            Some("plan_legacy"),
            Some("apr_legacy")
        )
        .is_some());
    }
}
