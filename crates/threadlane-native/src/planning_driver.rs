//! Single-command execution runner for durable [`PlanningCommandStore`] commands (`Milestone 10`).
//!
//! Executes or reconciles at most one planning command per invocation through the real
//! [`CoworkerPlanningService`] while holding a live [`CoworkerJobOwnershipGuard`] across the
//! entire service `.await` boundary and outcome recording.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde::{Deserialize, Serialize};
use threadlane_workflow::{PlanStatus, ScopedWorkflow, WorkflowRole};

use crate::coworker_workflow::{
    verify_grant_registry_dependencies, ArchitectConsultationRequestOutcome,
    CanonicalProjectBaseRef, CoworkerJobLockState, CoworkerJobOwnershipGuard,
    CoworkerOwnershipSummary, CoworkerPlanningService, CoworkerWorkflowStore,
    HumanClarificationRequestOutcome, OrchestrateWorkBriefPlanOutcome,
    OrchestratorPlanningDecision, PersistedPlanningCallArtifact, PlanningAdviceGrant,
    PlanningArtifactOutcomeKind, PlanningGrantStatus, PlanningReservationStatus,
    StructuredOrchestratorOutput, MAX_COWORKER_ID_LEN, MAX_ORCHESTRATOR_ARCHITECT_ROUNDS,
    MAX_PLANNING_ARTIFACT_BYTES,
};
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    constant_time_str_eq, ensure_platform_supported, prepare_private_dir,
    read_private_file_bounded, validate_absolute_no_symlink_components,
    validate_bounded_identifier,
};
use crate::mcp_pair::open_workflow_stores;
use crate::planning_commands::{
    current_wall_clock_ms, EnqueuePlanningCommandRequest, PlanningCommandPayload,
    PlanningCommandRecord, PlanningCommandResultRef, PlanningCommandStatus, PlanningCommandStore,
};
use crate::profile_flow::SelectedRoleChoice;
use crate::role_flow::read_registry_snapshot_readonly;

/// Canonical private (`0700`) subdirectory name under `plans_dir` for durable planning commands
/// and live job-ownership locks.
pub const PLANNING_COMMANDS_DIR_NAME: &str = "coworker-planning-commands";

/// Alias for [`PLANNING_COMMANDS_DIR_NAME`].
pub const COWORKER_PLANNING_COMMANDS_DIR: &str = PLANNING_COMMANDS_DIR_NAME;

/// Canonical private (`0700`) subdirectory name under `plans_dir` for persisted planning call
/// artifacts and snapshots.
pub const PLANNING_ARTIFACTS_DIR_NAME: &str = "coworker-planning-artifacts";

/// Alias for [`PLANNING_ARTIFACTS_DIR_NAME`].
pub const COWORKER_PLANNING_ARTIFACTS_DIR: &str = PLANNING_ARTIFACTS_DIR_NAME;

/// Returns the canonical planning commands directory path under `plans_dir`.
#[must_use]
pub fn planning_commands_dir(plans_dir: &Path) -> PathBuf {
    plans_dir.join(PLANNING_COMMANDS_DIR_NAME)
}

/// Returns the canonical planning artifacts directory path under `plans_dir`.
#[must_use]
pub fn planning_artifacts_dir(plans_dir: &Path) -> PathBuf {
    plans_dir.join(PLANNING_ARTIFACTS_DIR_NAME)
}

/// Bounded, prompt-redacted summary of a [`PlanningCommandRecord`] for CLI and MCP responses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanningCommandSummary {
    /// Deterministic command identifier (`pcm_<32hex>`).
    pub command_id: String,
    /// Idempotent caller request key.
    pub command_key: String,
    /// Frozen internal service request key (`psr_<32hex>`).
    pub service_request_key: String,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Standing [`PlanningAdviceGrant::grant_id`] authorizing this command.
    pub grant_id: String,
    /// Target [`crate::coworker_workflow::CoworkerJobRecord::job_id`] when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    /// Target or published workflow `plan_id` when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
    /// Primary planning role (`Architect` or `Orchestrator`).
    pub role: WorkflowRole,
    /// Resolved headless worker choice verified against the grant profile and policy at enqueue.
    pub selected_choice: SelectedRoleChoice,
    /// Canonical repository and immutable base commit/tree verified at enqueue.
    pub call_project_base: CanonicalProjectBaseRef,
    /// 64-character lowercase hex SHA-256 context digest for the command.
    pub context_digest: String,
    /// 64-character lowercase hex SHA-256 digest of the caller's semantic request body.
    pub request_body_digest: String,
    /// 64-character lowercase hex SHA-256 digest of the immutable command fields and frozen payload.
    pub command_digest: String,
    /// Current command status (`Queued`, `Running`, `Completed`, `KnownFailed`, or `Unknown`).
    pub status: PlanningCommandStatus,
    /// Explicit marker that the command outcome is uncertain and requires artifact/receipt reconciliation.
    pub pending_reconciliation: bool,
    /// Public ownership summary bound when claimed by a live [`CoworkerJobOwnershipGuard`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_summary: Option<CoworkerOwnershipSummary>,
    /// Verified service result reference when `status == Completed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_ref: Option<PlanningCommandResultRef>,
    /// Current persisted status of a verified Orchestrator plan, separate from its frozen result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_plan_status: Option<PlanStatus>,
    /// Explicit non-executable marker for a plan that is no longer Published.
    /// Absence does not grant execution authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<bool>,
    /// Bounded diagnostic message when `KnownFailed` or `Unknown`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
    /// Local wall-clock timestamp (ms since Unix epoch) when the command was enqueued.
    pub enqueued_at_ms: u64,
    /// Local wall-clock timestamp (ms since Unix epoch) when claimed for `Running`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<u64>,
    /// Local wall-clock timestamp (ms since Unix epoch) when last updated.
    pub updated_at_ms: u64,
    /// Local wall-clock timestamp (ms since Unix epoch) when `Completed` or `KnownFailed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<u64>,
}

impl PlanningCommandSummary {
    /// Builds a prompt-redacted [`PlanningCommandSummary`] from a [`PlanningCommandRecord`].
    #[must_use]
    pub fn from_record(record: &PlanningCommandRecord) -> Self {
        let job_id = match &record.payload {
            PlanningCommandPayload::Architect(_) => None,
            PlanningCommandPayload::Orchestrator(req) => Some(req.job_id.clone()),
        };
        let plan_id = match &record.result_ref {
            Some(PlanningCommandResultRef::OrchestratorPlan {
                planning_result, ..
            }) => Some(planning_result.plan_id.clone()),
            _ => match &record.payload {
                PlanningCommandPayload::Orchestrator(req) => req.plan_id.clone(),
                PlanningCommandPayload::Architect(_) => None,
            },
        };
        Self {
            command_id: record.command_id.clone(),
            command_key: record.command_key.clone(),
            service_request_key: record.service_request_key.clone(),
            binding_id: record.binding_id.clone(),
            thread_uuid: record.thread_uuid.clone(),
            grant_id: record.grant_id.clone(),
            job_id,
            plan_id,
            role: record.role,
            selected_choice: record.selected_choice.clone(),
            call_project_base: record.call_project_base.clone(),
            context_digest: record.context_digest.clone(),
            request_body_digest: record.request_body_digest.clone(),
            command_digest: record.command_digest.clone(),
            status: record.status,
            pending_reconciliation: record.pending_reconciliation,
            owner_summary: record.owner_summary.clone(),
            result_ref: record.result_ref.clone(),
            current_plan_status: None,
            executable: None,
            diagnostic: record.diagnostic.clone(),
            enqueued_at_ms: record.enqueued_at_ms,
            started_at_ms: record.started_at_ms,
            updated_at_ms: record.updated_at_ms,
            completed_at_ms: record.completed_at_ms,
        }
    }
}

/// Bounded read-only page of [`PlanningCommandSummary`] rows without raw prompts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanningCommandPage {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Current [` crate::planning_commands::PlanningCommandBindingLedger::revision`] (`0` when uninitialized).
    pub ledger_revision: u64,
    /// Bounded prompt-redacted command summaries.
    pub commands: Vec<PlanningCommandSummary>,
    /// Opaque pagination cursor (`command_id` of the last returned row) when more rows exist.
    pub next_cursor: Option<String>,
}

/// Prompt-redacted summary of [`PlanningCommandExecutionOutcome`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanningCommandExecutionSummary {
    /// Flattened [`PlanningCommandSummary`] fields (`command_id`, `status`, `result_ref`, etc.).
    #[serde(flatten)]
    pub summary: PlanningCommandSummary,
    /// Nested prompt-redacted command summary.
    pub command: PlanningCommandSummary,
    /// `true` when the command was already `Completed` and returned after re-verifying the stored
    /// outcome without invoking a model call.
    pub was_already_completed: bool,
    /// `true` when an interrupted `Unknown` command was reconciled to `Completed` from a complete,
    /// verified existing service outcome on disk without making a new model call.
    pub reconciled_from_existing: bool,
}

impl PlanningCommandExecutionSummary {
    /// Converts a [`PlanningCommandExecutionOutcome`] into a prompt-redacted summary.
    #[must_use]
    pub fn from_outcome(outcome: &PlanningCommandExecutionOutcome) -> Self {
        let summary = PlanningCommandSummary::from_record(&outcome.command);
        Self {
            command: summary.clone(),
            summary,
            was_already_completed: outcome.was_already_completed,
            reconciled_from_existing: outcome.reconciled_from_existing,
        }
    }
}

/// Honest submission state returned by [`start_planning_command_detached`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanningCommandStartStatus {
    /// Detached finite `execute-command` subprocess was spawned; not proof of `Running` or `Completed`.
    SubmissionRequested,
    /// Command already has active `Running` ownership evidence; no duplicate process was launched.
    AlreadyRunning,
    /// Command is already `Completed` with verified artifacts; no duplicate process was launched.
    AlreadyCompleted,
}

/// Honest response returned by `coworker-planning start-command`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanningCommandStartOutcome {
    /// Honest submission state (`submission_requested`, `already_running`, or `already_completed`).
    pub submission_status: PlanningCommandStartStatus,
    /// `true` when a detached `execute-command` subprocess was launched by this call.
    pub submission_requested: bool,
    /// `true` when a detached `execute-command` subprocess was launched by this call.
    pub launched: bool,
    /// Diagnostic OS process ID of the spawned child when launched; never ownership or completion proof.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid_hint: Option<u32>,
    /// Flattened [`PlanningCommandSummary`] fields reflecting the current verified command state.
    #[serde(flatten)]
    pub summary: PlanningCommandSummary,
    /// Nested prompt-redacted command summary.
    pub command: PlanningCommandSummary,
}

/// Outcome of executing or reconciling a single durable [`PlanningCommandRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanningCommandExecutionOutcome {
    /// Verified `Completed` command record in [`PlanningCommandStore`].
    pub command: PlanningCommandRecord,
    /// `true` when the command was already `Completed` and returned after re-verifying the stored
    /// outcome without invoking a model call.
    pub was_already_completed: bool,
    /// `true` when an interrupted `Unknown` command was reconciled to `Completed` from a complete,
    /// verified existing service outcome on disk without making a new model call.
    pub reconciled_from_existing: bool,
    /// Present when this invocation executed an `Orchestrator` command through
    /// [`CoworkerPlanningService::orchestrate_brief_to_plan`].
    pub orchestrator_outcome: Option<OrchestrateWorkBriefPlanOutcome>,
}

/// Bounded library runner that executes one durable planning command against a verified
/// [`PlanningCommandStore`] and [`CoworkerPlanningService`].
#[derive(Debug, Clone)]
pub struct PlanningCommandDriver {
    command_store: PlanningCommandStore,
    service: CoworkerPlanningService,
}

impl PlanningCommandDriver {
    /// Constructs a [`PlanningCommandDriver`] after verifying that `command_store` and `service`
    /// share the exact canonical workflow store, registry directory, and artifacts directory.
    pub fn new(
        command_store: PlanningCommandStore,
        service: CoworkerPlanningService,
    ) -> Result<Self, NativeError> {
        verify_store_and_service_alignment(&command_store, &service)?;
        Ok(Self {
            command_store,
            service,
        })
    }

    /// Borrows the underlying [`PlanningCommandStore`].
    #[must_use]
    pub fn command_store(&self) -> &PlanningCommandStore {
        &self.command_store
    }

    /// Borrows the underlying [`CoworkerPlanningService`].
    #[must_use]
    pub fn service(&self) -> &CoworkerPlanningService {
        &self.service
    }

    /// Executes or reconciles one durable command identified by `(binding_id, thread_uuid, command_id)`.
    pub async fn execute_command(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        command_id: &str,
    ) -> Result<PlanningCommandExecutionOutcome, NativeError> {
        execute_planning_command(
            &self.command_store,
            &self.service,
            binding_id,
            thread_uuid,
            command_id,
        )
        .await
    }

    /// Inspects the authoritative persisted planning command identified by
    /// `(binding_id, thread_uuid, command_id)`, verifies any completed artifact and applicable
    /// published plan identity, and delivers at most one terminal native completion/attention
    /// notice if configured.
    ///
    /// Never calls a model provider, never creates notice directories when unconfigured, and never
    /// mutates the planning command ledger.
    pub async fn notify_terminal_command_if_configured(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        command_id: &str,
    ) -> Result<Option<crate::coworker_notice::PlanningNoticeReport>, NativeError> {
        notify_terminal_planning_command_if_configured(
            &self.command_store,
            &self.service,
            binding_id,
            thread_uuid,
            command_id,
        )
        .await
    }
}

/// Inspects the authoritative persisted planning command in `command_store`, verifies any
/// completed artifact and applicable published plan identity against `service`, and delivers at
/// most one terminal native completion/attention notice when configured under `planning_notice_dir`.
pub async fn notify_terminal_planning_command_if_configured(
    command_store: &PlanningCommandStore,
    service: &CoworkerPlanningService,
    binding_id: &str,
    thread_uuid: &str,
    command_id: &str,
) -> Result<Option<crate::coworker_notice::PlanningNoticeReport>, NativeError> {
    verify_store_and_service_alignment(command_store, service)?;
    let notice_dir =
        crate::coworker_notice::planning_notice_dir(command_store.workflow_store().plans_dir());
    if !inspect_existing_private_subdir_readonly(&notice_dir, "planning_notice_dir")? {
        return Ok(None);
    }

    let Some(cmd) = command_store.get_command(binding_id, thread_uuid, command_id)? else {
        return Ok(None);
    };
    cmd.validate()?;
    if cmd.binding_id != binding_id || cmd.thread_uuid != thread_uuid {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "planning command scope does not match notify_terminal_planning_command_if_configured arguments",
        ));
    }
    let key = cmd.ownership_key()?;
    if key.binding_id != binding_id || key.thread_uuid != thread_uuid || key.job_id != command_id {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "planning command ownership key does not match requested (binding_id, thread_uuid, command_id)",
        ));
    }

    let meta = crate::coworker_notice::PlanningNoticeStore::inspect(
        &notice_dir,
        &key,
        &cmd.command_digest,
    )?;
    if !meta.configured {
        return Ok(None);
    }

    let has_active_owner_lock = matches!(
        command_store.ownership_store().inspect_job_lock(&key)?,
        crate::coworker_workflow::CoworkerJobLockState::HeldByActiveProcess { .. }
    );
    if has_active_owner_lock
        || matches!(
            cmd.status,
            PlanningCommandStatus::Queued | PlanningCommandStatus::Running
        )
    {
        return Ok(None);
    }

    let verified_completed_outcome = if cmd.status == PlanningCommandStatus::Completed {
        let verified_ref = command_store.verify_completed_command(&cmd)?;
        verify_published_plan_if_required(command_store, service, &cmd, &verified_ref)?;
        true
    } else {
        false
    };

    let snapshot = crate::coworker_notice::PlanningNoticeSnapshot {
        status: cmd.status,
        pending_reconciliation: cmd.pending_reconciliation,
        has_active_owner_lock,
        verified_completed_outcome,
    };
    let notice_store = crate::coworker_notice::PlanningNoticeStore::new(&notice_dir)?;
    notice_store
        .notify_if_configured(
            &key,
            &cmd.command_digest,
            Path::new(&cmd.call_project_base.repository_path),
            snapshot,
        )
        .await
}

/// Executes or reconciles a single durable planning command through [`CoworkerPlanningService`]
/// while holding a live [`CoworkerJobOwnershipGuard`] across the entire service `.await` and
/// completion transition.
pub async fn execute_planning_command(
    command_store: &PlanningCommandStore,
    service: &CoworkerPlanningService,
    binding_id: &str,
    thread_uuid: &str,
    command_id: &str,
) -> Result<PlanningCommandExecutionOutcome, NativeError> {
    verify_store_and_service_alignment(command_store, service)?;

    let cmd = command_store
        .get_command(binding_id, thread_uuid, command_id)?
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("planning command `{command_id}` was not found in binding `{binding_id}`"),
            )
        })?;
    cmd.validate()?;
    if cmd.binding_id != binding_id || cmd.thread_uuid != thread_uuid {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "planning command scope does not match execute_planning_command arguments",
        ));
    }

    match cmd.status {
        PlanningCommandStatus::Completed => {
            let verified_ref = command_store.verify_completed_command(&cmd)?;
            verify_published_plan_if_required(command_store, service, &cmd, &verified_ref)?;
            let mut verified_cmd = cmd;
            verified_cmd.result_ref = Some(verified_ref);
            Ok(PlanningCommandExecutionOutcome {
                command: verified_cmd,
                was_already_completed: true,
                reconciled_from_existing: false,
                orchestrator_outcome: None,
            })
        }
        PlanningCommandStatus::KnownFailed => Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "planning command `{command_id}` already terminated in KnownFailed: {}",
                cmd.diagnostic
                    .as_deref()
                    .unwrap_or("no diagnostic recorded")
            ),
        )),
        PlanningCommandStatus::Running => Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            format!("planning command `{command_id}` is currently Running under an active owner"),
        )),
        PlanningCommandStatus::Unknown => try_reconcile_interrupted_unknown(
            command_store,
            service,
            binding_id,
            thread_uuid,
            command_id,
            &cmd,
        ),
        PlanningCommandStatus::Queued => {
            execute_queued_command(
                command_store,
                service,
                binding_id,
                thread_uuid,
                command_id,
                &cmd,
            )
            .await
        }
    }
}

fn verify_store_and_service_alignment(
    command_store: &PlanningCommandStore,
    service: &CoworkerPlanningService,
) -> Result<(), NativeError> {
    if command_store.workflow_store().plans_dir() != service.store().plans_dir() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "PlanningCommandStore and CoworkerPlanningService workflow_store paths do not match",
        ));
    }
    if command_store.registry_dir() != service.registry_dir() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "PlanningCommandStore and CoworkerPlanningService registry_dir paths do not match",
        ));
    }
    let store_artifacts = command_store.artifacts_dir().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::InvalidConfig,
            "PlanningCommandStore must have artifacts_dir configured to execute planning commands",
        )
    })?;
    if store_artifacts != service.artifacts_dir() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "PlanningCommandStore artifacts_dir does not match CoworkerPlanningService artifacts_dir",
        ));
    }
    let _ = command_store.ownership_store().lock_root_dir();
    Ok(())
}

fn open_scoped_workflow_for_binding(
    command_store: &PlanningCommandStore,
    service: &CoworkerPlanningService,
    binding_id: &str,
) -> Result<ScopedWorkflow, NativeError> {
    verify_store_and_service_alignment(command_store, service)?;
    open_scoped_workflow_for_store(command_store, binding_id)
}

fn open_scoped_workflow_for_store(
    command_store: &PlanningCommandStore,
    binding_id: &str,
) -> Result<ScopedWorkflow, NativeError> {
    let (registry, plans) = open_workflow_stores(
        Some(command_store.registry_dir()),
        Some(command_store.workflow_store().plans_dir()),
    )?
    .ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::InvalidConfig,
            "workflow registry and plan stores are required for Orchestrator planning",
        )
    })?;
    let scoped = ScopedWorkflow::new(registry, plans, binding_id.to_string())
        .map_err(|err| NativeError::new(NativeErrorCode::ValidationFailed, err.safe_message()))?;
    if scoped.binding_id() != binding_id {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "ScopedWorkflow binding_id does not match requested binding_id",
        ));
    }
    Ok(scoped)
}

async fn execute_queued_command(
    command_store: &PlanningCommandStore,
    service: &CoworkerPlanningService,
    binding_id: &str,
    thread_uuid: &str,
    command_id: &str,
    queued_cmd: &PlanningCommandRecord,
) -> Result<PlanningCommandExecutionOutcome, NativeError> {
    let key = queued_cmd.ownership_key()?;
    if key.binding_id != binding_id || key.thread_uuid != thread_uuid || key.job_id != command_id {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "command ownership key does not match requested (binding_id, thread_uuid, command_id)",
        ));
    }
    let acquire_now_ms = current_wall_clock_ms()?;
    let guard = command_store
        .ownership_store()
        .try_acquire_job(&key, acquire_now_ms)?
        .into_guard()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                format!("planning command `{command_id}` ownership lock is already held"),
            )
        })?;

    let running_cmd =
        command_store.claim_running(&guard, binding_id, thread_uuid, command_id, None)?;
    running_cmd.validate()?;
    if running_cmd.command_id != command_id
        || running_cmd.binding_id != binding_id
        || running_cmd.thread_uuid != thread_uuid
        || running_cmd.service_request_key != queued_cmd.service_request_key
    {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "claimed Running command scope or service_request_key drifted from Queued record",
        ));
    }
    let recorded_owner = running_cmd.owner_summary.clone().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "claimed Running command is missing owner_summary",
        )
    })?;
    guard.validate_summary_before_effect(&key, &recorded_owner)?;

    let call_now_ms = current_wall_clock_ms()?;
    match &running_cmd.payload {
        PlanningCommandPayload::Architect(stored_req) => {
            let mut req = stored_req.clone();
            req.request_key = running_cmd.service_request_key.clone();
            req.current_time_ms = call_now_ms;

            match service.obtain_architect_advice(&req).await {
                Ok(outcome) => {
                    guard.validate_summary_before_effect(&key, &recorded_owner)?;
                    let report_artifact_id =
                        outcome.record.report_artifact_id.clone().ok_or_else(|| {
                            let err = NativeError::new(
                                NativeErrorCode::ProtocolError,
                                "ArchitectAdviceOutcome is missing report_artifact_id",
                            );
                            let _ = record_post_service_failure(
                                command_store,
                                service,
                                &guard,
                                &running_cmd,
                                &err,
                            );
                            err
                        })?;
                    let result_ref = PlanningCommandResultRef::ArchitectAdvice {
                        advice_id: outcome.record.advice_id.clone(),
                        reservation_id: outcome.reservation_id.clone(),
                        report_artifact_id,
                        context_digest: outcome.record.context_digest.clone(),
                        decision_digest: outcome.record.decision_digest.clone(),
                        reused_existing: outcome.reused_existing,
                        artifact_verified: false,
                    };
                    match command_store.record_completed(
                        &guard,
                        binding_id,
                        thread_uuid,
                        command_id,
                        result_ref,
                        None,
                    ) {
                        Ok(completed) => Ok(PlanningCommandExecutionOutcome {
                            command: completed,
                            was_already_completed: false,
                            reconciled_from_existing: false,
                            orchestrator_outcome: None,
                        }),
                        Err(err) => {
                            let _ = command_store.record_unknown(
                                &guard,
                                binding_id,
                                thread_uuid,
                                command_id,
                                &format!("service completed but record_completed failed: {err}"),
                                None,
                            );
                            Err(err)
                        }
                    }
                }
                Err(service_err) => {
                    let _ = guard.validate_summary_before_effect(&key, &recorded_owner);
                    Err(record_post_service_failure(
                        command_store,
                        service,
                        &guard,
                        &running_cmd,
                        &service_err,
                    ))
                }
            }
        }
        PlanningCommandPayload::Orchestrator(stored_req) => {
            let scoped_workflow =
                match open_scoped_workflow_for_binding(command_store, service, binding_id) {
                    Ok(wf) => wf,
                    Err(err) => {
                        let _ = command_store.record_known_failed(
                            &guard,
                            binding_id,
                            thread_uuid,
                            command_id,
                            &err.to_string(),
                            None,
                        );
                        return Err(err);
                    }
                };
            let mut req = stored_req.clone();
            req.request_key = running_cmd.service_request_key.clone();
            req.current_time_ms = call_now_ms;

            match service
                .orchestrate_brief_to_plan(&scoped_workflow, &req)
                .await
            {
                Ok(outcome) => {
                    guard.validate_summary_before_effect(&key, &recorded_owner)?;
                    let result_ref =
                        match build_orchestrator_result_ref(&stored_req.job_id, &outcome) {
                            Ok(r) => r,
                            Err(err) => {
                                let _ = command_store.record_unknown(
                                    &guard,
                                    binding_id,
                                    thread_uuid,
                                    command_id,
                                    &format!("Orchestrator outcome missing artifact/digest: {err}"),
                                    None,
                                );
                                return Err(err);
                            }
                        };
                    if let Err(err) = verify_published_plan_if_required(
                        command_store,
                        service,
                        &running_cmd,
                        &result_ref,
                    ) {
                        let _ = command_store.record_unknown(
                            &guard,
                            binding_id,
                            thread_uuid,
                            command_id,
                            &format!(
                                "Orchestrator completed but published plan check failed: {err}"
                            ),
                            None,
                        );
                        return Err(err);
                    }
                    match command_store.record_completed(
                        &guard,
                        binding_id,
                        thread_uuid,
                        command_id,
                        result_ref,
                        None,
                    ) {
                        Ok(completed) => Ok(PlanningCommandExecutionOutcome {
                            command: completed,
                            was_already_completed: false,
                            reconciled_from_existing: false,
                            orchestrator_outcome: Some(outcome),
                        }),
                        Err(err) => {
                            let _ = command_store.record_unknown(
                                &guard,
                                binding_id,
                                thread_uuid,
                                command_id,
                                &format!(
                                    "Orchestrator completed but record_completed failed: {err}"
                                ),
                                None,
                            );
                            Err(err)
                        }
                    }
                }
                Err(service_err) => {
                    let _ = guard.validate_summary_before_effect(&key, &recorded_owner);
                    Err(record_post_service_failure(
                        command_store,
                        service,
                        &guard,
                        &running_cmd,
                        &service_err,
                    ))
                }
            }
        }
    }
}

fn build_orchestrator_result_ref(
    job_id: &str,
    outcome: &OrchestrateWorkBriefPlanOutcome,
) -> Result<PlanningCommandResultRef, NativeError> {
    match &outcome.decision {
        OrchestratorPlanningDecision::Plan(plan_box) => {
            Ok(PlanningCommandResultRef::OrchestratorPlan {
                job_id: job_id.to_string(),
                planning_result: Box::new(plan_box.planning_result.clone()),
                artifact_verified: false,
            })
        }
        OrchestratorPlanningDecision::RequiresHumanClarification(clarification) => {
            let report_artifact_id =
                outcome
                    .reservation
                    .report_artifact_id
                    .clone()
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ProtocolError,
                            "completed Orchestrator reservation is missing report_artifact_id",
                        )
                    })?;
            let output_digest = outcome.reservation.output_digest.clone().ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "completed Orchestrator reservation is missing output_digest",
                )
            })?;
            Ok(
                PlanningCommandResultRef::OrchestratorRequiresHumanClarification {
                    job_id: job_id.to_string(),
                    reservation_id: outcome.reservation.reservation_id.clone(),
                    report_artifact_id,
                    output_digest,
                    clarification: clarification.clone(),
                    artifact_verified: false,
                },
            )
        }
        OrchestratorPlanningDecision::RequiresArchitectAdvice(consultation) => {
            let report_artifact_id =
                outcome
                    .reservation
                    .report_artifact_id
                    .clone()
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ProtocolError,
                            "completed Orchestrator reservation is missing report_artifact_id",
                        )
                    })?;
            let output_digest = outcome.reservation.output_digest.clone().ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "completed Orchestrator reservation is missing output_digest",
                )
            })?;
            Ok(
                PlanningCommandResultRef::OrchestratorRequiresArchitectAdvice {
                    job_id: job_id.to_string(),
                    reservation_id: outcome.reservation.reservation_id.clone(),
                    report_artifact_id,
                    output_digest,
                    consultation: consultation.clone(),
                    artifact_verified: false,
                },
            )
        }
    }
}

fn record_post_service_failure(
    command_store: &PlanningCommandStore,
    service: &CoworkerPlanningService,
    guard: &CoworkerJobOwnershipGuard,
    cmd: &PlanningCommandRecord,
    service_err: &NativeError,
) -> NativeError {
    if matches!(
        classify_service_error(command_store, service, cmd),
        Ok(FailureDisposition::KnownFailed)
    ) {
        let _ = command_store.record_known_failed(
            guard,
            &cmd.binding_id,
            &cmd.thread_uuid,
            &cmd.command_id,
            &service_err.to_string(),
            None,
        );
        return service_err.clone();
    }
    let diag = format!(
        "planning command `{}` outcome after service boundary is ambiguous or unverified: {service_err}",
        cmd.command_id
    );
    let _ = command_store.record_unknown(
        guard,
        &cmd.binding_id,
        &cmd.thread_uuid,
        &cmd.command_id,
        &diag,
        None,
    );
    if service_err.error_code() == NativeErrorCode::AmbiguousWakeupPendingReconciliation {
        service_err.clone()
    } else {
        NativeError::new(NativeErrorCode::AmbiguousWakeupPendingReconciliation, diag)
    }
}

enum FailureDisposition {
    KnownFailed,
    Unknown,
}

fn classify_service_error(
    command_store: &PlanningCommandStore,
    service: &CoworkerPlanningService,
    cmd: &PlanningCommandRecord,
) -> Result<FailureDisposition, NativeError> {
    let wf_ledger = command_store
        .workflow_store()
        .read_binding_ledger(&cmd.binding_id)?;
    let Some(grant) = wf_ledger
        .planning_grants
        .iter()
        .find(|g| g.grant_id == cmd.grant_id)
    else {
        return Ok(FailureDisposition::Unknown);
    };
    grant.validate()?;

    let orch_prefix = format!("{}_orch_r", cmd.service_request_key);
    let arch_prefix = format!("{}_arch_r", cmd.service_request_key);
    let related: Vec<_> = grant
        .reservations
        .iter()
        .filter(|r| {
            r.request_key == cmd.service_request_key
                || r.request_key.starts_with(&orch_prefix)
                || r.request_key.starts_with(&arch_prefix)
        })
        .collect();
    if related.is_empty() {
        return Ok(FailureDisposition::KnownFailed);
    }

    for res in &related {
        match res.status {
            PlanningReservationStatus::RefusedBeforeEffect => {}
            PlanningReservationStatus::FailedAfterEffect => {
                let Some(ref art_id) = res.report_artifact_id else {
                    return Ok(FailureDisposition::Unknown);
                };
                if !verify_failed_after_effect_artifact(
                    service,
                    grant,
                    res.reservation_id.as_str(),
                    art_id,
                ) {
                    return Ok(FailureDisposition::Unknown);
                }
            }
            PlanningReservationStatus::ReservedBeforeEffect
            | PlanningReservationStatus::EffectUnknown
            | PlanningReservationStatus::Completed => {
                return Ok(FailureDisposition::Unknown);
            }
        }
    }
    Ok(FailureDisposition::KnownFailed)
}

fn verify_failed_after_effect_artifact(
    service: &CoworkerPlanningService,
    grant: &PlanningAdviceGrant,
    reservation_id: &str,
    artifact_id: &str,
) -> bool {
    let Some(artifact) = read_artifact_for_grant(service, grant, artifact_id) else {
        return false;
    };
    artifact.reservation_id == reservation_id
        && matches!(
            artifact.outcome,
            PlanningArtifactOutcomeKind::FailedAfterEffect { .. }
        )
}

fn read_artifact_for_grant(
    service: &CoworkerPlanningService,
    grant: &PlanningAdviceGrant,
    artifact_id: &str,
) -> Option<PersistedPlanningCallArtifact> {
    let path = service.artifacts_dir().join(format!("{artifact_id}.json"));
    let Ok(Some(bytes)) =
        read_private_file_bounded(&path, MAX_PLANNING_ARTIFACT_BYTES, "planning_call_artifact")
    else {
        return None;
    };
    let Ok(artifact) = serde_json::from_slice::<PersistedPlanningCallArtifact>(&bytes) else {
        return None;
    };
    artifact
        .validate_against_grant(artifact_id, grant)
        .ok()
        .map(|()| artifact)
}

fn try_reconcile_interrupted_unknown(
    command_store: &PlanningCommandStore,
    service: &CoworkerPlanningService,
    binding_id: &str,
    thread_uuid: &str,
    command_id: &str,
    cmd: &PlanningCommandRecord,
) -> Result<PlanningCommandExecutionOutcome, NativeError> {
    let key = cmd.ownership_key()?;
    let now_ms = current_wall_clock_ms()?;
    let guard = command_store
        .ownership_store()
        .try_acquire_job(&key, now_ms)?
        .into_guard()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                format!(
                    "planning command `{command_id}` ownership lock is held during reconciliation"
                ),
            )
        })?;

    // Ownership precedes discovery and every possible recovery mutation.
    let cmd = command_store
        .get_command(binding_id, thread_uuid, command_id)?
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "planning command disappeared during reconciliation",
            )
        })?;
    cmd.validate()?;
    if cmd.status == PlanningCommandStatus::Completed {
        let verified_ref = command_store.verify_completed_command(&cmd)?;
        verify_published_plan_if_required(command_store, service, &cmd, &verified_ref)?;
        let mut command = cmd;
        command.result_ref = Some(verified_ref);
        return Ok(PlanningCommandExecutionOutcome {
            command,
            was_already_completed: true,
            reconciled_from_existing: false,
            orchestrator_outcome: None,
        });
    }
    if cmd.status != PlanningCommandStatus::Unknown || !cmd.pending_reconciliation {
        return Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            "command is no longer Unknown pending reconciliation",
        ));
    }
    let mut candidate_ref = discover_reconcilable_result_ref(command_store, service, &cmd)?;
    if candidate_ref.is_none() {
        verify_store_and_service_alignment(command_store, service)?;
        let workflow = open_scoped_workflow_for_store(command_store, binding_id)?;
        if service.recover_completed_command_plan(&workflow, &cmd)? {
            candidate_ref = discover_reconcilable_result_ref(command_store, service, &cmd)?;
        }
    }
    let candidate_ref = candidate_ref.ok_or_else(|| NativeError::new(
        NativeErrorCode::AmbiguousWakeupPendingReconciliation,
        format!("planning command `{command_id}` has no complete verified service outcome; provider redispatch is forbidden"),
    ))?;

    let reconciled = command_store.reconcile_unknown_completed(
        &guard,
        binding_id,
        thread_uuid,
        command_id,
        candidate_ref,
        None,
    )?;
    Ok(PlanningCommandExecutionOutcome {
        command: reconciled,
        was_already_completed: false,
        reconciled_from_existing: true,
        orchestrator_outcome: None,
    })
}

fn discover_reconcilable_result_ref(
    command_store: &PlanningCommandStore,
    service: &CoworkerPlanningService,
    cmd: &PlanningCommandRecord,
) -> Result<Option<PlanningCommandResultRef>, NativeError> {
    let wf_ledger = command_store
        .workflow_store()
        .read_binding_ledger(&cmd.binding_id)?;
    let Some(grant) = wf_ledger
        .planning_grants
        .iter()
        .find(|g| g.grant_id == cmd.grant_id)
    else {
        return Ok(None);
    };
    if grant.validate().is_err() {
        return Ok(None);
    }

    let orch_prefix = format!("{}_orch_r", cmd.service_request_key);
    let arch_prefix = format!("{}_arch_r", cmd.service_request_key);
    if grant.reservations.iter().any(|r| {
        (r.request_key == cmd.service_request_key
            || r.request_key.starts_with(&orch_prefix)
            || r.request_key.starts_with(&arch_prefix))
            && r.status != PlanningReservationStatus::Completed
    }) {
        return Ok(None);
    }

    let candidate = match &cmd.payload {
        PlanningCommandPayload::Architect(_) => {
            let Some(adv) = wf_ledger.architect_advice.iter().find(|a| {
                a.binding_id == cmd.binding_id
                    && a.thread_uuid == cmd.thread_uuid
                    && a.grant_id == cmd.grant_id
                    && a.request_key == cmd.service_request_key
            }) else {
                return Ok(None);
            };
            let Some(ref report_artifact_id) = adv.report_artifact_id else {
                return Ok(None);
            };
            PlanningCommandResultRef::ArchitectAdvice {
                advice_id: adv.advice_id.clone(),
                reservation_id: adv.reservation_id.clone(),
                report_artifact_id: report_artifact_id.clone(),
                context_digest: adv.context_digest.clone(),
                decision_digest: adv.decision_digest.clone(),
                reused_existing: false,
                artifact_verified: false,
            }
        }
        PlanningCommandPayload::Orchestrator(req) => {
            let Some(res) = grant.reservations.iter().rev().find(|r| {
                r.role == WorkflowRole::Orchestrator
                    && r.status == PlanningReservationStatus::Completed
                    && is_valid_orchestrator_round_key(
                        &r.request_key,
                        &cmd.service_request_key,
                        req.max_architect_rounds,
                    )
            }) else {
                return Ok(None);
            };
            let Some(ref report_artifact_id) = res.report_artifact_id else {
                return Ok(None);
            };
            let Some(ref output_digest) = res.output_digest else {
                return Ok(None);
            };
            let Some(artifact) = read_artifact_for_grant(service, grant, report_artifact_id) else {
                return Ok(None);
            };
            match &artifact.outcome {
                PlanningArtifactOutcomeKind::OrchestratorOutput {
                    structured_output: StructuredOrchestratorOutput::Plan { .. },
                    ..
                } => {
                    let Some(job) = wf_ledger.jobs.iter().find(|j| j.job_id == req.job_id) else {
                        return Ok(None);
                    };
                    let Some(ref pr) = job.planning_result else {
                        return Ok(None);
                    };
                    if pr.grant_id != cmd.grant_id
                        || pr.reservation_id != res.reservation_id
                        || pr.request_key != res.request_key
                        || pr.report_artifact_id != *report_artifact_id
                        || !constant_time_str_eq(&pr.output_digest, output_digest)
                    {
                        return Ok(None);
                    }
                    PlanningCommandResultRef::OrchestratorPlan {
                        job_id: job.job_id.clone(),
                        planning_result: Box::new(pr.clone()),
                        artifact_verified: false,
                    }
                }
                PlanningArtifactOutcomeKind::OrchestratorOutput {
                    structured_output:
                        StructuredOrchestratorOutput::RequiresHumanClarification {
                            summary,
                            questions,
                            referenced_files,
                            ..
                        },
                    ..
                } => PlanningCommandResultRef::OrchestratorRequiresHumanClarification {
                    job_id: req.job_id.clone(),
                    reservation_id: res.reservation_id.clone(),
                    report_artifact_id: report_artifact_id.clone(),
                    output_digest: output_digest.clone(),
                    clarification: HumanClarificationRequestOutcome {
                        summary: summary.clone(),
                        questions: questions.clone(),
                        referenced_files: referenced_files.clone(),
                    },
                    artifact_verified: false,
                },
                PlanningArtifactOutcomeKind::OrchestratorOutput {
                    structured_output:
                        StructuredOrchestratorOutput::RequiresArchitectAdvice {
                            summary, questions, ..
                        },
                    ..
                } => PlanningCommandResultRef::OrchestratorRequiresArchitectAdvice {
                    job_id: req.job_id.clone(),
                    reservation_id: res.reservation_id.clone(),
                    report_artifact_id: report_artifact_id.clone(),
                    output_digest: output_digest.clone(),
                    consultation: ArchitectConsultationRequestOutcome {
                        summary: summary.clone(),
                        questions: questions.clone(),
                    },
                    artifact_verified: false,
                },
                _ => return Ok(None),
            }
        }
    };

    if verify_published_plan_if_required(command_store, service, cmd, &candidate).is_err() {
        return Ok(None);
    }
    match command_store.verify_reconciliation_candidate(cmd, candidate) {
        Ok(verified) => Ok(Some(verified)),
        Err(_) => Ok(None),
    }
}

fn is_valid_orchestrator_round_key(
    candidate_key: &str,
    root_service_key: &str,
    max_architect_rounds: u32,
) -> bool {
    if candidate_key == root_service_key {
        return true;
    }
    let bounded_rounds = max_architect_rounds.min(MAX_ORCHESTRATOR_ARCHITECT_ROUNDS);
    for round in 1..=bounded_rounds {
        let expected = format!("{root_service_key}_orch_r{round}");
        if candidate_key == expected {
            return true;
        }
    }
    false
}

fn verify_published_plan_if_required(
    command_store: &PlanningCommandStore,
    service: &CoworkerPlanningService,
    cmd: &PlanningCommandRecord,
    result_ref: &PlanningCommandResultRef,
) -> Result<(), NativeError> {
    verify_store_and_service_alignment(command_store, service)?;
    verify_published_plan_for_store(command_store, cmd, result_ref)
}

fn verify_published_plan_for_store(
    command_store: &PlanningCommandStore,
    cmd: &PlanningCommandRecord,
    result_ref: &PlanningCommandResultRef,
) -> Result<(), NativeError> {
    let PlanningCommandResultRef::OrchestratorPlan {
        planning_result, ..
    } = result_ref
    else {
        return Ok(());
    };
    let scoped = open_scoped_workflow_for_store(command_store, &cmd.binding_id)?;
    let page = scoped
        .get_plan(
            &planning_result.plan_id,
            Some(planning_result.plan_revision),
            None,
            None,
        )
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "published plan `{}`@{} was not found in PlanStore: {}",
                    planning_result.plan_id,
                    planning_result.plan_revision,
                    err.safe_message()
                ),
            )
        })?;
    if page.header.binding_id != cmd.binding_id
        || page.header.plan_id != planning_result.plan_id
        || page.header.revision != planning_result.plan_revision
        || page.header.status != PlanStatus::Published
        || planning_result.plan_status != PlanStatus::Published
        || page.header.profile_id != planning_result.profile_id
        || page.header.profile_revision != planning_result.registry_revision
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "published plan header in PlanStore does not match OrchestratorPlan PlanningResultRef",
        ));
    }
    let Some(ref stored_digest) = page.header.plan_digest else {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "published plan in PlanStore has no compiled plan_digest",
        ));
    };
    if !constant_time_str_eq(stored_digest, &planning_result.plan_digest) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "published plan digest in PlanStore does not match OrchestratorPlan plan_digest",
        ));
    }
    Ok(())
}

/// Verifies completed history without admitting execution or changing frozen results.
/// Superseded headers receive their digest only from the read-only journal proof.
fn verify_historical_plan_for_store_readonly(
    command_store: &PlanningCommandStore,
    cmd: &PlanningCommandRecord,
    result_ref: &PlanningCommandResultRef,
) -> Result<Option<PlanStatus>, NativeError> {
    let PlanningCommandResultRef::OrchestratorPlan {
        planning_result, ..
    } = result_ref
    else {
        return Ok(None);
    };
    let header = ScopedWorkflow::read_plan_summary_readonly_at(
        command_store.registry_dir(),
        command_store.workflow_store().plans_dir(),
        &cmd.binding_id,
        &planning_result.plan_id,
        planning_result.plan_revision,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "historical plan could not be verified in PlanStore: {}",
                err.safe_message()
            ),
        )
    })?;
    if header.binding_id != cmd.binding_id
        || header.plan_id != planning_result.plan_id
        || header.revision != planning_result.plan_revision
        || !(header.status.is_active_published() || header.status == PlanStatus::Superseded)
        || planning_result.plan_status != PlanStatus::Published
        || header.profile_id != planning_result.profile_id
        || header.profile_revision != planning_result.registry_revision
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "historical plan header in PlanStore does not match OrchestratorPlan PlanningResultRef",
        ));
    }
    let Some(stored_digest) = header.plan_digest.as_deref() else {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "historical plan in PlanStore has no verified plan_digest",
        ));
    };
    if !constant_time_str_eq(stored_digest, &planning_result.plan_digest) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "historical plan digest in PlanStore does not match OrchestratorPlan plan_digest",
        ));
    }
    Ok(Some(header.status))
}

fn verified_command_summary_readonly(
    command_store: &PlanningCommandStore,
    mut cmd: PlanningCommandRecord,
) -> Result<PlanningCommandSummary, NativeError> {
    cmd.validate()?;
    let current_plan_status = if cmd.status == PlanningCommandStatus::Completed {
        let result_ref = cmd.result_ref.as_ref().ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "Completed planning command is missing result_ref",
            )
        })?;
        let status = verify_historical_plan_for_store_readonly(command_store, &cmd, result_ref)?;
        let verified_ref = if status == Some(PlanStatus::Superseded) {
            command_store.verify_completed_command_readonly(&cmd)?
        } else {
            command_store.verify_completed_command(&cmd)?
        };
        cmd.result_ref = Some(verified_ref);
        status
    } else {
        None
    };
    let mut summary = PlanningCommandSummary::from_record(&cmd);
    summary.current_plan_status = current_plan_status;
    summary.executable = current_plan_status
        .filter(|status| *status != PlanStatus::Published)
        .map(|_| false);
    Ok(summary)
}

/// Opens a [`PlanningCommandDriver`] under canonical `plans_dir` and `registry_dir`, creating the
/// private (`0700`) [`PLANNING_COMMANDS_DIR_NAME`] and [`PLANNING_ARTIFACTS_DIR_NAME`]
/// subdirectories if they do not yet exist.
pub fn open_planning_driver_for_plans_dir(
    plans_dir: &Path,
    registry_dir: &Path,
) -> Result<PlanningCommandDriver, NativeError> {
    ensure_platform_supported()?;
    let workflow_store = CoworkerWorkflowStore::new(plans_dir)?;
    let commands_dir = planning_commands_dir(workflow_store.plans_dir());
    let artifacts_dir = planning_artifacts_dir(workflow_store.plans_dir());
    let _ = prepare_private_dir(&commands_dir, "planning_commands_dir")?;
    let _ = prepare_private_dir(&artifacts_dir, "planning_artifacts_dir")?;
    let service =
        CoworkerPlanningService::new(workflow_store.clone(), registry_dir, &artifacts_dir)?;
    let command_store =
        PlanningCommandStore::open_existing(&commands_dir, workflow_store, registry_dir)?
            .with_artifacts_dir(&artifacts_dir)?;
    PlanningCommandDriver::new(command_store, service)
}

/// Enqueues a [`PlanningCommandRecord`] under `plans_dir` and returns its prompt-redacted
/// [`PlanningCommandSummary`].
pub fn enqueue_planning_command_summary(
    plans_dir: &Path,
    registry_dir: &Path,
    request: &EnqueuePlanningCommandRequest,
) -> Result<PlanningCommandSummary, NativeError> {
    ensure_platform_supported()?;
    let workflow_store = CoworkerWorkflowStore::new(plans_dir)?;
    let commands_dir = planning_commands_dir(workflow_store.plans_dir());
    let artifacts_dir = planning_artifacts_dir(workflow_store.plans_dir());
    let _ = prepare_private_dir(&commands_dir, "planning_commands_dir")?;
    let _ = prepare_private_dir(&artifacts_dir, "planning_artifacts_dir")?;
    let command_store =
        PlanningCommandStore::open_existing(&commands_dir, workflow_store, registry_dir)?
            .with_artifacts_dir(&artifacts_dir)?;
    let mut record = command_store.enqueue_command(request)?;
    if record.status == PlanningCommandStatus::Completed {
        let verified_ref = command_store.verify_completed_command(&record)?;
        verify_published_plan_for_store(&command_store, &record, &verified_ref)?;
        record.result_ref = Some(verified_ref);
    }
    Ok(PlanningCommandSummary::from_record(&record))
}

/// Pure read-only lookup of a single planning command summary by `(binding_id, thread_uuid, command_id)`.
///
/// Never creates directories, state files, or lockfiles when [`PLANNING_COMMANDS_DIR_NAME`] or
/// [`PLANNING_ARTIFACTS_DIR_NAME`] is absent. Requires [`PLANNING_ARTIFACTS_DIR_NAME`] and the
/// persisted report artifact on disk when verifying a `Completed` command.
pub fn get_planning_command_summary_readonly(
    plans_dir: &Path,
    registry_dir: &Path,
    binding_id: &str,
    thread_uuid: &str,
    command_id: &str,
) -> Result<Option<PlanningCommandSummary>, NativeError> {
    validate_bounded_identifier(command_id, "command_id", MAX_COWORKER_ID_LEN)?;
    let Some(command_store) = open_planning_command_store_readonly(plans_dir, registry_dir)? else {
        return Ok(None);
    };
    let Some(cmd) = command_store.get_command(binding_id, thread_uuid, command_id)? else {
        return Ok(None);
    };
    cmd.validate()?;
    if cmd.binding_id != binding_id || cmd.thread_uuid != thread_uuid {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "planning command scope does not match requested binding_id and thread_uuid",
        ));
    }
    verified_command_summary_readonly(&command_store, cmd).map(Some)
}

/// Pure read-only bounded list of prompt-redacted [`PlanningCommandSummary`] rows for `(binding_id, thread_uuid)`.
///
/// Never creates directories, state files, or lockfiles when [`PLANNING_COMMANDS_DIR_NAME`] or
/// [`PLANNING_ARTIFACTS_DIR_NAME`] is absent.
pub fn list_planning_command_summaries_readonly(
    plans_dir: &Path,
    registry_dir: &Path,
    binding_id: &str,
    thread_uuid: &str,
    limit: usize,
    cursor: Option<&str>,
) -> Result<PlanningCommandPage, NativeError> {
    let Some(command_store) = open_planning_command_store_readonly(plans_dir, registry_dir)? else {
        return Ok(PlanningCommandPage {
            binding_id: binding_id.to_string(),
            ledger_revision: 0,
            commands: Vec::new(),
            next_cursor: None,
        });
    };
    let ledger = command_store.read_binding_ledger(binding_id)?;
    if let Some(ref pinned) = ledger.thread_uuid {
        if pinned != thread_uuid {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "thread_uuid does not match the thread pinned to this binding's command ledger",
            ));
        }
    }
    let records = command_store.list_commands(binding_id, thread_uuid)?;
    let mut summaries = Vec::with_capacity(records.len());
    for cmd in records {
        if cmd.binding_id != binding_id || cmd.thread_uuid != thread_uuid {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "planning command scope does not match requested binding_id and thread_uuid",
            ));
        }
        summaries.push(verified_command_summary_readonly(&command_store, cmd)?);
    }

    let start = match cursor {
        Some(cursor_id) => {
            let pos = summaries
                .iter()
                .position(|s| s.command_id == cursor_id)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "pagination cursor was not found in this binding's planning command list",
                    )
                })?;
            pos + 1
        }
        None => 0,
    };
    let end = start.saturating_add(limit).min(summaries.len());
    let page_items = summaries[start..end].to_vec();
    let next_cursor = if end < summaries.len() {
        page_items.last().map(|s| s.command_id.clone())
    } else {
        None
    };

    Ok(PlanningCommandPage {
        binding_id: binding_id.to_string(),
        ledger_revision: ledger.revision,
        commands: page_items,
        next_cursor,
    })
}

/// Validates a planning command under `(binding_id, thread_uuid, command_id)` and, when `Queued`,
/// spawns a detached finite one-command subprocess of the canonical `current_exe()` running
/// `coworker-planning execute-command --command-id <ID>` with the exact fixed store and scope arguments.
#[allow(clippy::too_many_arguments)]
pub fn start_planning_command_detached(
    state_dir: &Path,
    registry_dir: &Path,
    plans_dir: &Path,
    authority_dir: &Path,
    sessions_dir: &Path,
    binding_id: &str,
    thread_uuid: &str,
    command_id: &str,
) -> Result<PlanningCommandStartOutcome, NativeError> {
    validate_bounded_identifier(command_id, "command_id", MAX_COWORKER_ID_LEN)?;
    let Some(command_store) = open_planning_command_store_readonly(plans_dir, registry_dir)? else {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("planning command `{command_id}` was not found in binding `{binding_id}`"),
        ));
    };
    let mut cmd = command_store
        .get_command(binding_id, thread_uuid, command_id)?
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("planning command `{command_id}` was not found in binding `{binding_id}`"),
            )
        })?;
    cmd.validate()?;
    if cmd.binding_id != binding_id || cmd.thread_uuid != thread_uuid {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "planning command scope does not match requested binding_id and thread_uuid",
        ));
    }

    match cmd.status {
        PlanningCommandStatus::Completed => {
            let verified_ref = command_store.verify_completed_command(&cmd)?;
            verify_published_plan_for_store(&command_store, &cmd, &verified_ref)?;
            cmd.result_ref = Some(verified_ref);
            let summary = PlanningCommandSummary::from_record(&cmd);
            Ok(PlanningCommandStartOutcome {
                submission_status: PlanningCommandStartStatus::AlreadyCompleted,
                submission_requested: false,
                launched: false,
                pid_hint: None,
                command: summary.clone(),
                summary,
            })
        }
        PlanningCommandStatus::Running => {
            let summary = PlanningCommandSummary::from_record(&cmd);
            Ok(PlanningCommandStartOutcome {
                submission_status: PlanningCommandStartStatus::AlreadyRunning,
                submission_requested: false,
                launched: false,
                pid_hint: None,
                command: summary.clone(),
                summary,
            })
        }
        PlanningCommandStatus::Unknown => Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            format!(
                "planning command `{command_id}` is Unknown pending reconciliation and cannot be freshly launched"
            ),
        )),
        PlanningCommandStatus::KnownFailed => Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "planning command `{command_id}` already terminated in KnownFailed: {}",
                cmd.diagnostic
                    .as_deref()
                    .unwrap_or("no diagnostic recorded")
            ),
        )),
        PlanningCommandStatus::Queued => {
            validate_queued_command_ready_for_launch(&command_store, &cmd)?;
            let artifacts_dir = planning_artifacts_dir(command_store.workflow_store().plans_dir());
            let _ = prepare_private_dir(&artifacts_dir, "planning_artifacts_dir")?;
            let exe = resolve_canonical_current_exe()?;

            let mut child_cmd = Command::new(&exe);
            child_cmd
                .arg("--state-dir")
                .arg(state_dir)
                .arg("--registry-dir")
                .arg(registry_dir)
                .arg("--plans-dir")
                .arg(plans_dir)
                .arg("--authority-dir")
                .arg(authority_dir)
                .arg("--sessions-dir")
                .arg(sessions_dir)
                .arg("--binding")
                .arg(binding_id)
                .arg("--thread")
                .arg(thread_uuid)
                .arg("coworker-planning")
                .arg("execute-command")
                .arg("--command-id")
                .arg(command_id)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());

            #[cfg(unix)]
            {
                use std::os::unix::process::CommandExt;
                child_cmd.process_group(0);
            }

            let child = child_cmd.spawn().map_err(|err| {
                NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to spawn detached coworker-planning execute-command process: {err}"
                    ),
                )
            })?;
            let pid_hint = Some(child.id());
            drop(child);

            let summary = PlanningCommandSummary::from_record(&cmd);
            Ok(PlanningCommandStartOutcome {
                submission_status: PlanningCommandStartStatus::SubmissionRequested,
                submission_requested: true,
                launched: true,
                pid_hint,
                command: summary.clone(),
                summary,
            })
        }
    }
}

fn open_planning_command_store_readonly(
    plans_dir: &Path,
    registry_dir: &Path,
) -> Result<Option<PlanningCommandStore>, NativeError> {
    ensure_platform_supported()?;
    let clean_plans_dir = validate_absolute_no_symlink_components(plans_dir, "plans_dir")?;
    let _ = validate_absolute_no_symlink_components(registry_dir, "registry_dir")?;
    let commands_dir = planning_commands_dir(&clean_plans_dir);
    if !inspect_existing_private_subdir_readonly(&commands_dir, "planning_commands_dir")? {
        return Ok(None);
    }
    let workflow_store = CoworkerWorkflowStore::new(&clean_plans_dir)?;
    let mut store =
        PlanningCommandStore::open_existing(&commands_dir, workflow_store, registry_dir)?;
    let artifacts_dir = planning_artifacts_dir(&clean_plans_dir);
    if inspect_existing_private_subdir_readonly(&artifacts_dir, "planning_artifacts_dir")? {
        store = store.with_artifacts_dir(&artifacts_dir)?;
    }
    Ok(Some(store))
}

fn inspect_existing_private_subdir_readonly(dir: &Path, label: &str) -> Result<bool, NativeError> {
    let clean = validate_absolute_no_symlink_components(dir, label)?;
    let meta = match fs::symlink_metadata(&clean) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(_) => {
            return Err(NativeError::new(
                NativeErrorCode::IoError,
                format!("failed to inspect {label} directory metadata"),
            ));
        }
    };
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!("{label} must be a private non-symlink directory"),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if (meta.permissions().mode() & 0o077) != 0 {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!("{label} directory has insecure permissions (expected 0700)"),
            ));
        }
    }
    Ok(true)
}

fn validate_queued_command_ready_for_launch(
    command_store: &PlanningCommandStore,
    cmd: &PlanningCommandRecord,
) -> Result<(), NativeError> {
    let cmd_ledger = command_store.read_binding_ledger(&cmd.binding_id)?;
    if cmd_ledger
        .commands
        .iter()
        .any(|c| c.status.is_in_flight_or_unknown())
    {
        return Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            "another planning command on this binding is Running or Unknown pending reconciliation",
        ));
    }
    let key = cmd.ownership_key()?;
    if matches!(
        command_store.ownership_store().inspect_job_lock(&key)?,
        CoworkerJobLockState::HeldByActiveProcess { .. }
    ) {
        return Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            format!(
                "planning command `{}` ownership lock is already held by an active process",
                cmd.command_id
            ),
        ));
    }

    let now_ms = current_wall_clock_ms()?;
    let wf_ledger = command_store
        .workflow_store()
        .read_binding_ledger(&cmd.binding_id)?;
    if wf_ledger.binding_id != cmd.binding_id
        || wf_ledger.thread_uuid.as_deref() != Some(cmd.thread_uuid.as_str())
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "binding workflow ledger does not match command binding_id and thread_uuid",
        ));
    }
    if wf_ledger
        .planning_grants
        .iter()
        .any(|g| g.reservations.iter().any(|r| r.status.is_ambiguous()))
    {
        return Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            "a planning call reservation on this binding has an uncertain outcome pending reconciliation",
        ));
    }
    let grant = wf_ledger
        .planning_grants
        .iter()
        .find(|g| g.grant_id == cmd.grant_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("standing grant `{}` was not found in binding", cmd.grant_id),
            )
        })?;
    grant.validate()?;
    if grant.binding_id != cmd.binding_id
        || grant.thread_uuid != cmd.thread_uuid
        || grant.status != PlanningGrantStatus::Active
        || grant.revoked_at_ms.is_some()
        || grant.revoked_reason.is_some()
        || now_ms >= grant.expires_at_ms
        || now_ms < grant.created_at_ms
        || grant.usage.reserved_total_calls >= grant.budget.max_total_calls
        || (cmd.role == WorkflowRole::Architect
            && grant.usage.reserved_architect_calls >= grant.budget.max_architect_calls)
        || (cmd.role == WorkflowRole::Orchestrator
            && grant.usage.reserved_orchestrator_calls >= grant.budget.max_orchestrator_calls)
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "standing planning grant is no longer active or within budget",
        ));
    }
    let live_reg_snapshot = read_registry_snapshot_readonly(command_store.registry_dir())?;
    let _ = verify_grant_registry_dependencies(&live_reg_snapshot, grant)?;
    cmd.call_project_base.verify_live_repository()?;

    if let PlanningCommandPayload::Orchestrator(req) = &cmd.payload {
        let job = wf_ledger
            .jobs
            .iter()
            .find(|j| j.job_id == req.job_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("requested job_id `{}` was not found in binding", req.job_id),
                )
            })?;
        job.validate()?;
        if job.binding_id != cmd.binding_id
            || job.thread_uuid != cmd.thread_uuid
            || wf_ledger
                .jobs
                .iter()
                .any(|j| j.supersedes_job_id.as_deref() == Some(job.job_id.as_str()))
            || job.holds.iter().any(|h| h.active)
            || job.approval_linkage.is_some()
            || !job.work_brief.is_ready_for_execution_planning()
            || job.work_brief.project_base != cmd.call_project_base
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "target CoworkerJobRecord is no longer ready for Orchestrator planning",
            ));
        }
    }
    Ok(())
}

fn resolve_canonical_current_exe() -> Result<PathBuf, NativeError> {
    let raw_exe = std::env::current_exe().map_err(|_| {
        NativeError::new(
            NativeErrorCode::IoError,
            "failed to resolve current executable path",
        )
    })?;
    if !raw_exe.is_absolute() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "current_exe path must be absolute",
        ));
    }
    let canonical = fs::canonicalize(&raw_exe).map_err(|_| {
        NativeError::new(
            NativeErrorCode::SecurityViolation,
            "failed to canonicalize current executable path",
        )
    })?;
    let clean = validate_absolute_no_symlink_components(&canonical, "current_exe")?;
    let meta = fs::symlink_metadata(&clean).map_err(|_| {
        NativeError::new(
            NativeErrorCode::SecurityViolation,
            "failed to inspect canonical current executable metadata",
        )
    })?;
    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "canonical current_exe must be a regular non-symlink file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = meta.permissions().mode();
        if (mode & 0o111) == 0 || (mode & 0o022) != 0 {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "canonical current_exe must be executable and not group- or world-writable",
            ));
        }
    }
    Ok(clean)
}
