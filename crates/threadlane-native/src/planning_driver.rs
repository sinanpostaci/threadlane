//! Single-command execution runner for durable [`PlanningCommandStore`] commands (`Milestone 10`).
//!
//! Executes or reconciles at most one planning command per invocation through the real
//! [`CoworkerPlanningService`] while holding a live [`CoworkerJobOwnershipGuard`] across the
//! entire service `.await` boundary and outcome recording.

use serde::{Deserialize, Serialize};
use threadlane_workflow::{PlanStatus, ScopedWorkflow, WorkflowRole};

use crate::coworker_workflow::{
    ArchitectConsultationRequestOutcome, CoworkerJobOwnershipGuard, CoworkerPlanningService,
    HumanClarificationRequestOutcome, OrchestrateWorkBriefPlanOutcome,
    OrchestratorPlanningDecision, PersistedPlanningCallArtifact, PlanningAdviceGrant,
    PlanningArtifactOutcomeKind, PlanningReservationStatus, StructuredOrchestratorOutput,
    MAX_ORCHESTRATOR_ARCHITECT_ROUNDS, MAX_PLANNING_ARTIFACT_BYTES,
};
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{constant_time_str_eq, read_private_file_bounded};
use crate::mcp_pair::open_workflow_stores;
use crate::planning_commands::{
    current_wall_clock_ms, PlanningCommandPayload, PlanningCommandRecord, PlanningCommandResultRef,
    PlanningCommandStatus, PlanningCommandStore,
};

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
    let Some(candidate_ref) = discover_reconcilable_result_ref(command_store, service, cmd)? else {
        return Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            format!(
                "planning command `{command_id}` is Unknown pending reconciliation and has no complete verified service outcome on disk: {}",
                cmd.diagnostic
                    .as_deref()
                    .unwrap_or("no diagnostic recorded")
            ),
        ));
    };

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
    let mut probe = cmd.clone();
    probe.status = PlanningCommandStatus::Completed;
    probe.pending_reconciliation = false;
    probe.result_ref = Some(candidate);
    match command_store.verify_completed_command(&probe) {
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
    let PlanningCommandResultRef::OrchestratorPlan {
        planning_result, ..
    } = result_ref
    else {
        return Ok(());
    };
    let scoped = open_scoped_workflow_for_binding(command_store, service, &cmd.binding_id)?;
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
