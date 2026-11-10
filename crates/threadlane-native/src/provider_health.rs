//! Scoped advisory metadata bridge. No scheduling, lifecycle changes, or execution authority.
use std::future::Future;

use serde::{Deserialize, Serialize};
use threadlane_workers::provider_health::{
    ProviderDiagnosticAvailability, ProviderHealthObserver, ProviderHealthSnapshot,
};
use threadlane_workflow::WorkflowRole;
use tokio::sync::watch;

use crate::coworker_workflow::{
    deterministic_controller_instance_id, ControllerJobRuntimeState, CoworkerControllerService,
    CoworkerJobOwnershipGuard, CoworkerJobOwnershipKey, CoworkerOwnershipSummary,
    PendingControllerEffectKind, PendingControllerEffectStatus, PlanningCallReservation,
    MAX_COWORKER_ID_LEN, MAX_GRANT_RESERVATIONS,
};
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::validate_bounded_identifier;
use crate::planning_commands::{
    PlanningCommandRecord, PlanningCommandStatus, PlanningCommandStore,
};

/// Open, currently available diagnostic coverage, independent of any current hint.
/// Closed, lost, unsupported or truncated sources remain history, never live attention.
pub(crate) fn has_live_diagnostic_source(snapshot: &ProviderHealthSnapshot) -> bool {
    snapshot.availability == ProviderDiagnosticAvailability::Available
        && snapshot.unavailable_reason.is_none()
}

/// Finite per-job history bound; reaching it refuses optional metadata, never execution.
pub const MAX_ROLE_HEALTH_INVOCATIONS: usize = 1024;

/// Exact invocation correlation, separate from immutable approval and command digests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderHealthInvocation {
    /// One actually reserved planning call, including internal Architect rounds.
    Planning {
        /// Standing grant.
        grant_id: String,
        /// Unique reserved call.
        reservation_id: String,
        /// Actual service call request key.
        request_key: String,
        /// Exact immutable context subject.
        context_digest: String,
        /// Actual invoked role.
        role: WorkflowRole,
    },
    /// One pre-existing controller effect; task None denotes job-level Closure.
    Controller {
        /// Frozen approval.
        approval_id: String,
        /// Immutable controller envelope.
        envelope_digest: String,
        /// Actual reserved effect.
        effect_id: String,
        /// Exact reserved subject.
        subject_digest: String,
        /// Logical task; absent for job Closure.
        task_id: Option<String>,
        /// Actual task attempt; absent for job Closure.
        attempt_no: Option<u32>,
        /// Existing external attempt/derived receipt correlation, when recorded before dispatch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        external_correlation_id: Option<String>,
        /// Actual invoked role.
        role: WorkflowRole,
    },
}

impl ProviderHealthInvocation {
    /// Stable key within the containing scoped ledger.
    pub fn key(&self) -> &str {
        match self {
            Self::Planning { reservation_id, .. } => reservation_id,
            Self::Controller { effect_id, .. } => effect_id,
        }
    }

    fn validate(&self) -> Result<(), NativeError> {
        let (ids, digest, role) = match self {
            Self::Planning {
                grant_id,
                reservation_id,
                request_key,
                context_digest,
                role,
            } => (
                vec![
                    grant_id.as_str(),
                    reservation_id.as_str(),
                    request_key.as_str(),
                ],
                context_digest,
                *role,
            ),
            Self::Controller {
                approval_id,
                effect_id,
                envelope_digest,
                subject_digest,
                task_id,
                attempt_no,
                external_correlation_id,
                role,
            } => {
                if let Some(correlation) = external_correlation_id {
                    validate_bounded_identifier(
                        correlation,
                        "health correlation",
                        MAX_COWORKER_ID_LEN,
                    )?;
                }
                validate_digest(envelope_digest)?;
                if task_id.is_some() != attempt_no.is_some() || *attempt_no == Some(0) {
                    return Err(rejected());
                }
                if let Some(task) = task_id {
                    validate_bounded_identifier(task, "health task", MAX_COWORKER_ID_LEN)?;
                }
                (
                    vec![approval_id.as_str(), effect_id.as_str()],
                    subject_digest,
                    *role,
                )
            }
        };
        for id in ids {
            validate_bounded_identifier(id, "health identity", MAX_COWORKER_ID_LEN)?;
        }
        validate_digest(digest)?;
        match self {
            Self::Planning { .. }
                if matches!(role, WorkflowRole::Architect | WorkflowRole::Orchestrator) =>
            {
                Ok(())
            }
            Self::Controller { task_id, .. }
                if (matches!(role, WorkflowRole::Coder | WorkflowRole::Reviewer)
                    && task_id.is_some())
                    || (role == WorkflowRole::Closure && task_id.is_none()) =>
            {
                Ok(())
            }
            _ => Err(rejected()),
        }
    }
}

fn validate_digest(digest: &str) -> Result<(), NativeError> {
    if digest.len() == 64
        && digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        Ok(())
    } else {
        Err(rejected())
    }
}

pub(crate) fn rejected() -> NativeError {
    NativeError::new(
        NativeErrorCode::SecurityViolation,
        "provider health invocation scope or revision mismatch",
    )
}

/// Bounded history only. Liveness must be projected from the current physical owner and effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedProviderHealth {
    /// Exact binding/thread/command-or-job scope.
    pub ownership_key: CoworkerJobOwnershipKey,
    /// Physically verified owner at capture time.
    pub owner: CoworkerOwnershipSummary,
    /// Logical controller lease epoch, absent for planning.
    pub lease_epoch: Option<u64>,
    /// Immutable invocation identity, never derived from log contents.
    pub invocation: ProviderHealthInvocation,
    /// Cumulative allowlisted unverified diagnostic hint.
    pub snapshot: ProviderHealthSnapshot,
    /// Supervised call returned; says nothing about success, acceptance, or reconciliation.
    pub terminal_observed: bool,
}

impl ScopedProviderHealth {
    /// Validates bounded metadata without granting liveness or authority.
    pub fn validate(&self) -> Result<(), NativeError> {
        let _ = CoworkerJobOwnershipKey::new(
            &self.ownership_key.binding_id,
            &self.ownership_key.thread_uuid,
            &self.ownership_key.job_id,
        )?;
        self.owner.validate_for_key(&self.ownership_key)?;
        self.invocation.validate()?;
        if !self.snapshot.is_valid()
            || serde_json::to_vec(&self.snapshot)
                .map_err(|_| rejected())?
                .len()
                > threadlane_workers::provider_health::MAX_PROVIDER_HEALTH_SNAPSHOT_BYTES
            || self.lease_epoch == Some(0)
            || matches!(self.invocation, ProviderHealthInvocation::Planning { .. })
                != self.lease_epoch.is_none()
        {
            return Err(rejected());
        }
        Ok(())
    }

    /// Exact duplicate updates are idempotent; first/last observations cannot be rewritten.
    pub fn is_valid_successor_of(&self, prior: &Self) -> bool {
        self.validate().is_ok()
            && self.ownership_key == prior.ownership_key
            && self.owner == prior.owner
            && self.lease_epoch == prior.lease_epoch
            && self.invocation == prior.invocation
            && (!prior.terminal_observed || self.terminal_observed)
            && self.snapshot.is_valid_successor_of(&prior.snapshot)
    }
}

/// Read-only presentation of durable history. Unknown outcomes remain in the existing ledgers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderHealthView {
    /// Persisted invocation evidence.
    pub record: ScopedProviderHealth,
    /// True only with available/open coverage and the exact live owner and reserved effect.
    pub live: bool,
}

/// Explicit physical command ownership borrowed across planning service awaits.
#[derive(Clone, Copy)]
pub struct PlanningHealthOwner<'a> {
    store: &'a PlanningCommandStore,
    guard: &'a CoworkerJobOwnershipGuard,
    command: &'a PlanningCommandRecord,
}

impl<'a> PlanningHealthOwner<'a> {
    /// Requires an already claimed exact Running command. This does not authorize execution.
    pub fn new(
        store: &'a PlanningCommandStore,
        guard: &'a CoworkerJobOwnershipGuard,
        command: &'a PlanningCommandRecord,
    ) -> Result<Self, NativeError> {
        let owner = Self {
            store,
            guard,
            command,
        };
        owner.verify()?;
        Ok(owner)
    }

    /// Checks store alignment and the frozen root request before any service reservation.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn verify_service_request(
        &self,
        service: &crate::coworker_workflow::CoworkerPlanningService,
        binding_id: &str,
        thread_uuid: &str,
        grant_id: &str,
        request_key: &str,
        role: WorkflowRole,
    ) -> Result<(), NativeError> {
        self.verify()?;
        if self.store.workflow_store().plans_dir() != service.store().plans_dir()
            || self.store.registry_dir() != service.registry_dir()
            || self.store.artifacts_dir() != Some(service.artifacts_dir())
            || self.command.binding_id != binding_id
            || self.command.thread_uuid != thread_uuid
            || self.command.grant_id != grant_id
            || self.command.service_request_key != request_key
            || self.command.role != role
        {
            return Err(rejected());
        }
        Ok(())
    }

    fn verify(&self) -> Result<(), NativeError> {
        let command = self
            .store
            .get_command(
                &self.command.binding_id,
                &self.command.thread_uuid,
                &self.command.command_id,
            )?
            .ok_or_else(rejected)?;
        if command.status != PlanningCommandStatus::Running
            || command.command_digest != self.command.command_digest
            || command.owner_summary != self.command.owner_summary
        {
            return Err(rejected());
        }
        self.store
            .ownership_store()
            .verify_live_guard(self.guard, &command.ownership_key()?)?;
        self.guard.validate_summary_before_effect(
            &command.ownership_key()?,
            command.owner_summary.as_ref().ok_or_else(rejected)?,
        )?;
        Ok(())
    }

    /// Binds the actual reservation created by the service, never a role/task name alone.
    pub fn invocation(
        &self,
        reservation: &PlanningCallReservation,
    ) -> Result<ScopedHealthSink<'a>, NativeError> {
        self.verify()?;
        let record_identity = ProviderHealthInvocation::Planning {
            grant_id: self.command.grant_id.clone(),
            reservation_id: reservation.reservation_id.clone(),
            request_key: reservation.request_key.clone(),
            context_digest: reservation.context_digest.clone(),
            role: reservation.role,
        };
        self.store
            .verify_provider_health_reservation(self.command, &record_identity)?;
        Ok(ScopedHealthSink::Planning {
            owner: *self,
            invocation: record_identity,
        })
    }
}

/// Borrowed physical owner and immutable invocation identity. No task-local authority is used.
pub enum ScopedHealthSink<'a> {
    /// Existing planning command ledger writer.
    Planning {
        /// Borrowed physical command owner.
        owner: PlanningHealthOwner<'a>,
        /// Actual reserved planning invocation.
        invocation: ProviderHealthInvocation,
    },
    /// Existing controller ledger writer, with a verified logical lease.
    Controller {
        /// Controller owning the existing stores.
        controller: &'a CoworkerControllerService,
        /// Live physical guard held through the supervised await.
        guard: &'a CoworkerJobOwnershipGuard,
        /// Immutable invocation.
        invocation: ProviderHealthInvocation,
        /// Exact logical owner.
        instance_id: String,
        /// Exact lease epoch.
        lease_epoch: u64,
    },
}

/// Guard-backed live eligibility, obtained only by rereading durable invocation metadata.
/// This is separate from terminal planning eligibility, which still rejects an active owner.
pub(crate) struct ProviderHealthNoticeEligibility<'a> {
    pub key: CoworkerJobOwnershipKey,
    pub guard: &'a CoworkerJobOwnershipGuard,
    pub subject_digest: String,
    pub repository_path: String,
    pub fingerprint: String,
    pub job_revision: u64,
    pub planning: bool,
}

impl<'a> ScopedHealthSink<'a> {
    /// Checks current ownership, immutable scope and the DURABLE current advisory, never text.
    pub(crate) fn notice_eligibility(
        &self,
    ) -> Result<Option<ProviderHealthNoticeEligibility<'_>>, NativeError> {
        let (key, guard, subject_digest, repository_path, job_revision, record, planning) =
            match self {
                Self::Planning { owner, invocation } => {
                    owner.verify()?;
                    let command = owner
                        .store
                        .get_command(
                            &owner.command.binding_id,
                            &owner.command.thread_uuid,
                            &owner.command.command_id,
                        )?
                        .ok_or_else(rejected)?;
                    if command.pending_reconciliation {
                        return Ok(None);
                    }
                    owner
                        .store
                        .verify_provider_health_reservation(&command, invocation)?;
                    let Some(record) = command
                        .provider_health
                        .iter()
                        .find(|record| record.invocation == *invocation)
                        .cloned()
                    else {
                        return Ok(None);
                    };
                    (
                        command.ownership_key()?,
                        owner.guard,
                        command.command_digest,
                        command.call_project_base.repository_path,
                        0,
                        record,
                        true,
                    )
                }
                Self::Controller {
                    controller,
                    guard,
                    invocation,
                    instance_id,
                    lease_epoch,
                } => {
                    self.verify_controller_scope()?;
                    let key = guard.key();
                    let state = controller
                        .policy_store()
                        .get_job_runtime_readonly(&key.binding_id, &key.job_id)?
                        .ok_or_else(rejected)?;
                    if state.stop_requested
                        || !state.lease.as_ref().is_some_and(|lease| {
                            lease.active
                                && lease.instance_id == *instance_id
                                && lease.lease_epoch == *lease_epoch
                        })
                    {
                        return Ok(None);
                    }
                    let Some(record) = state.provider_health.get(invocation.key()).cloned() else {
                        return Ok(None);
                    };
                    if record.invocation != *invocation || record.lease_epoch != Some(*lease_epoch)
                    {
                        return Err(rejected());
                    }
                    let ledger = controller
                        .workflow_store()
                        .read_binding_ledger(&key.binding_id)?;
                    // Amendment leaves the old job/envelope intact. Retain its diagnostics and
                    // supervised result, but fence new callbacks as admission already does.
                    if ledger.jobs.iter().any(|other| {
                        other.supersedes_job_id.as_deref() == Some(key.job_id.as_str())
                    }) {
                        return Ok(None);
                    }
                    let job = ledger
                        .jobs
                        .iter()
                        .find(|job| job.job_id == key.job_id)
                        .ok_or_else(rejected)?;
                    (
                        key,
                        *guard,
                        state.envelope.envelope_digest,
                        state.envelope.project_base.repository_path,
                        job.job_revision,
                        record,
                        false,
                    )
                }
            };
        record.validate()?;
        guard.validate_summary_before_effect(&key, &record.owner)?;
        if record.ownership_key != key {
            return Err(rejected());
        }
        if record.terminal_observed
            || !has_live_diagnostic_source(&record.snapshot)
            || record.snapshot.current_failure.is_none()
        {
            return Ok(None);
        }
        let fingerprint = crate::coworker_notice::derive_provider_health_notice_fingerprint(
            &key,
            &subject_digest,
            &record.invocation,
        )?;
        Ok(Some(ProviderHealthNoticeEligibility {
            key,
            guard,
            subject_digest,
            repository_path,
            fingerprint,
            job_revision,
            planning,
        }))
    }

    async fn notify_if_configured(&self) -> Result<(), NativeError> {
        use crate::coworker_notice::{
            coworker_notice_dir, planning_notice_dir, CoworkerNoticeStore, PlanningNoticeStore,
        };
        let (directory, planning) = match self {
            Self::Planning { owner, .. } => (
                planning_notice_dir(owner.store.workflow_store().plans_dir()),
                true,
            ),
            Self::Controller { controller, .. } => (
                coworker_notice_dir(controller.workflow_store().plans_dir()),
                false,
            ),
        };
        // Inspect before opening: optional observation must not enroll or create a notice store.
        match std::fs::symlink_metadata(&directory) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(rejected()),
            Ok(metadata) if !metadata.is_dir() || metadata.file_type().is_symlink() => {
                return Err(rejected())
            }
            Ok(_) => {}
        }
        if planning {
            PlanningNoticeStore::new(directory)?
                .notify_provider_health(self)
                .await?;
        } else {
            CoworkerNoticeStore::new(directory)?
                .notify_provider_health(self)
                .await?;
        }
        Ok(())
    }

    /// Requires an already reserved exact effect. Does not reserve or dispatch anything.
    #[allow(clippy::too_many_arguments)]
    pub fn controller(
        controller: &'a CoworkerControllerService,
        guard: &'a CoworkerJobOwnershipGuard,
        effect_id: &str,
        role: WorkflowRole,
        task_id: Option<&str>,
        attempt_no: Option<u32>,
        subject_digest: &str,
    ) -> Result<Self, NativeError> {
        let key = guard.key();
        controller.verify_physical_ownership(
            guard,
            &key.binding_id,
            &key.thread_uuid,
            &key.job_id,
        )?;
        let state = controller
            .policy_store()
            .get_job_runtime_readonly(&key.binding_id, &key.job_id)?
            .ok_or_else(rejected)?;
        let instance_id =
            deterministic_controller_instance_id(&key.binding_id, &key.job_id, guard.instance_id());
        let lease = state.lease.as_ref().ok_or_else(rejected)?;
        if !lease.active || lease.instance_id != instance_id {
            return Err(rejected());
        }
        let invocation = ProviderHealthInvocation::Controller {
            approval_id: state.envelope.approval_id.clone(),
            envelope_digest: state.envelope.envelope_digest.clone(),
            effect_id: effect_id.to_owned(),
            subject_digest: subject_digest.to_owned(),
            task_id: task_id.map(str::to_owned),
            attempt_no,
            external_correlation_id: state
                .pending_effects
                .get(effect_id)
                .ok_or_else(rejected)?
                .external_correlation_id
                .clone(),
            role,
        };
        verify_controller_invocation(&state, &invocation)?;
        let sink = Self::Controller {
            controller,
            guard,
            invocation,
            instance_id,
            lease_epoch: lease.lease_epoch,
        };
        sink.verify_controller_scope()?;
        Ok(sink)
    }

    fn verify_controller_scope(&self) -> Result<(), NativeError> {
        let Self::Controller {
            controller,
            guard,
            invocation,
            ..
        } = self
        else {
            return Ok(());
        };
        let key = guard.key();
        controller.verify_physical_ownership(
            guard,
            &key.binding_id,
            &key.thread_uuid,
            &key.job_id,
        )?;
        let ledger = controller
            .workflow_store()
            .read_binding_ledger(&key.binding_id)?;
        let job = ledger
            .jobs
            .iter()
            .find(|j| j.job_id == key.job_id)
            .ok_or_else(rejected)?;
        let state = controller
            .policy_store()
            .get_job_runtime_readonly(&key.binding_id, &key.job_id)?
            .ok_or_else(rejected)?;
        if job.binding_id != key.binding_id
            || job.thread_uuid != key.thread_uuid
            || state.envelope.binding_id != key.binding_id
            || state.envelope.thread_uuid != key.thread_uuid
        {
            return Err(rejected());
        }
        state.envelope.verify_matches_job(job)?;
        verify_controller_invocation(&state, invocation)
    }

    fn persist(
        &self,
        snapshot: &ProviderHealthSnapshot,
        terminal: bool,
    ) -> Result<(), NativeError> {
        match self {
            Self::Planning { owner, invocation } => {
                owner.verify()?;
                owner.store.record_provider_health(
                    owner.guard,
                    owner.command,
                    invocation,
                    snapshot,
                    terminal,
                )
            }
            Self::Controller {
                controller,
                guard,
                invocation,
                instance_id,
                lease_epoch,
            } => {
                self.verify_controller_scope()?;
                let key = guard.key();
                let owner = guard.validate_before_effect(&key)?.clone();
                controller.policy_store().mutate_job_runtime(
                    &key.binding_id,
                    &key.job_id,
                    instance_id,
                    *lease_epoch,
                    |state| {
                        // Recheck scope and effect inside the durable transaction.
                        guard.validate_summary_before_effect(&key, &owner)?;
                        verify_controller_invocation(state, invocation)?;
                        let record = ScopedProviderHealth {
                            ownership_key: key.clone(),
                            owner,
                            lease_epoch: Some(*lease_epoch),
                            invocation: invocation.clone(),
                            snapshot: snapshot.clone(),
                            terminal_observed: terminal,
                        };
                        record.validate()?;
                        if let Some(prior) = state.provider_health.get(invocation.key()) {
                            if !record.is_valid_successor_of(prior) {
                                return Err(rejected());
                            }
                        } else if state.provider_health.len() >= MAX_ROLE_HEALTH_INVOCATIONS {
                            return Err(rejected());
                        }
                        state
                            .provider_health
                            .insert(invocation.key().to_owned(), record);
                        Ok(())
                    },
                )
            }
        }
    }
}

pub(crate) fn verify_controller_invocation(
    state: &ControllerJobRuntimeState,
    invocation: &ProviderHealthInvocation,
) -> Result<(), NativeError> {
    invocation.validate()?;
    let ProviderHealthInvocation::Controller {
        approval_id,
        envelope_digest,
        effect_id,
        subject_digest,
        task_id,
        attempt_no,
        external_correlation_id,
        role,
    } = invocation
    else {
        return Err(rejected());
    };
    let kind = match role {
        WorkflowRole::Coder => PendingControllerEffectKind::CoderDispatch,
        WorkflowRole::Reviewer => PendingControllerEffectKind::ReviewerInvocation,
        WorkflowRole::Closure => PendingControllerEffectKind::ClosureInvocation,
        _ => return Err(rejected()),
    };
    let effect = state.pending_effects.get(effect_id).ok_or_else(rejected)?;
    if state.envelope.approval_id != *approval_id
        || state.envelope.envelope_digest != *envelope_digest
        || effect.effect_id != *effect_id
        || effect.job_id != state.job_id
        || effect.subject_digest != *subject_digest
        || effect.task_id != *task_id
        || effect.attempt_no != *attempt_no
        || effect.external_correlation_id != *external_correlation_id
        || effect.kind != kind
        || effect.status != PendingControllerEffectStatus::ReservedBeforeEffect
    {
        return Err(rejected());
    }
    Ok(())
}

/// Observe one supervised invocation without spawning another owner or holding a controller mutex.
/// Persistence failures never replace a proven provider outcome. A concurrent bounded notice
/// consumer runs outside the pipe reader and keeps polling supervision during native delivery.
pub async fn supervise_health<T, E, F, Fut, R>(
    sink: Option<&ScopedHealthSink<'_>>,
    run: F,
    report_snapshot: R,
) -> Result<T, E>
where
    F: FnOnce(ProviderHealthObserver) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    R: Fn(&T) -> Option<&ProviderHealthSnapshot>,
{
    let (notice_tx, mut notice_rx) = watch::channel(false);
    // Keep provider supervision and native delivery out of every caller's inline future.
    // These remain borrowed, jointly polled futures; dropping supervision drops both.
    let supervise = Box::pin(supervise_health_updates(
        run,
        report_snapshot,
        move |snapshot, terminal| {
            sink.map_or(Ok(()), |sink| sink.persist(snapshot, terminal))?;
            if sink.is_some()
                && !terminal
                && has_live_diagnostic_source(snapshot)
                && snapshot.current_failure.is_some()
            {
                notice_tx.send_replace(true);
            }
            Ok(())
        },
    ));
    let notify = Box::pin(async {
        // One attempt per invocation; the store handles durable duplicate/Unknown semantics.
        // Closing supervision drops the sender, including when optional persistence failed.
        while notice_rx.changed().await.is_ok() {
            let available = *notice_rx.borrow_and_update();
            if available {
                if let Some(sink) = sink {
                    let _ = sink.notify_if_configured().await;
                }
                break;
            }
        }
    });
    let (result, ()) = tokio::join!(supervise, notify);
    result
}

async fn supervise_health_updates<T, E, F, Fut, R, P>(
    run: F,
    report_snapshot: R,
    mut persist: P,
) -> Result<T, E>
where
    F: FnOnce(ProviderHealthObserver) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    R: Fn(&T) -> Option<&ProviderHealthSnapshot>,
    P: FnMut(&ProviderHealthSnapshot, bool) -> Result<(), NativeError>,
{
    let (sender, mut receiver) = watch::channel(None);
    // Stack pinning would embed the entire provider future in this select loop.
    let mut future = Box::pin(run(sender));
    let mut last = None;
    let mut channel_open = true;
    let result = loop {
        tokio::select! {
            result = &mut future => break result,
            changed = receiver.changed(), if channel_open => {
                if changed.is_err() { channel_open = false; continue; }
                let snapshot = receiver.borrow_and_update().clone();
                if let Some(snapshot) = snapshot {
                    let _ = persist(&snapshot, false);
                    last = Some(snapshot);
                }
            }
        }
    };
    // Reconcile with the supervisor's terminal report even if the last watch update coalesced.
    // On launch/transport error, close only the advisory display, never the authoritative effect.
    let terminal = result
        .as_ref()
        .ok()
        .and_then(&report_snapshot)
        .cloned()
        .or_else(|| receiver.borrow().clone())
        .or(last);
    if let Some(snapshot) = terminal {
        let _ = persist(&snapshot, true);
    }
    result
}

/// Checks immutable historical linkage without requiring an unresolved effect or a live owner.
pub(crate) fn verify_controller_health_history(
    state: &ControllerJobRuntimeState,
    record: &ScopedProviderHealth,
) -> Result<(), NativeError> {
    record.validate()?;
    let ProviderHealthInvocation::Controller {
        approval_id,
        envelope_digest,
        effect_id,
        subject_digest,
        task_id,
        attempt_no,
        role,
        ..
    } = &record.invocation
    else {
        return Err(rejected());
    };
    let effect = state.pending_effects.get(effect_id).ok_or_else(rejected)?;
    let kind = match role {
        WorkflowRole::Coder => PendingControllerEffectKind::CoderDispatch,
        WorkflowRole::Reviewer => PendingControllerEffectKind::ReviewerInvocation,
        WorkflowRole::Closure => PendingControllerEffectKind::ClosureInvocation,
        _ => return Err(rejected()),
    };
    if record.ownership_key.binding_id != state.envelope.binding_id
        || record.ownership_key.thread_uuid != state.envelope.thread_uuid
        || record.ownership_key.job_id != state.job_id
        || approval_id != &state.envelope.approval_id
        || envelope_digest != &state.envelope.envelope_digest
        || effect.effect_id != *effect_id
        || effect.job_id != state.job_id
        || effect.subject_digest != *subject_digest
        || effect.task_id != *task_id
        || effect.attempt_no != *attempt_no
        || effect.kind != kind
        || task_id
            .as_ref()
            .is_some_and(|task| !state.envelope.task_coder_choices.contains_key(task))
    {
        return Err(rejected());
    }
    Ok(())
}

/// Historical metadata is never live after owner loss, terminal return, or effect reconciliation.
pub fn controller_health_views(
    state: &ControllerJobRuntimeState,
    live_owner: Option<&CoworkerOwnershipSummary>,
) -> Vec<ProviderHealthView> {
    state
        .provider_health
        .values()
        .filter(|record| verify_controller_health_history(state, record).is_ok())
        .map(|record| {
            let live = record.ownership_key.binding_id == state.envelope.binding_id
                && record.ownership_key.thread_uuid == state.envelope.thread_uuid
                && record.ownership_key.job_id == state.job_id
                && !record.terminal_observed
                && has_live_diagnostic_source(&record.snapshot)
                && live_owner == Some(&record.owner)
                && state.lease.as_ref().is_some_and(|lease| {
                    lease.active
                        && Some(lease.lease_epoch) == record.lease_epoch
                        && lease.instance_id
                            == deterministic_controller_instance_id(
                                &record.ownership_key.binding_id,
                                &record.ownership_key.job_id,
                                &record.owner.instance_id,
                            )
                })
                && verify_controller_invocation(state, &record.invocation).is_ok();
            ProviderHealthView {
                record: record.clone(),
                live,
            }
        })
        .collect()
}

/// Bound used by the planning ledger in addition to its existing finite reservation budget.
pub const MAX_PLANNING_HEALTH_INVOCATIONS: usize = MAX_GRANT_RESERVATIONS;

/// Original host Coder evidence, kept separate from derived controller Coder invocations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OriginalWorkerProviderHealth {
    /// Original frozen approval.
    pub approval_id: String,
    /// Original approval subject.
    pub approval_subject_digest: String,
    /// Original approved task, always attempt 1.
    pub task_id: String,
    /// Compiled immutable task subject.
    pub task_digest: String,
    /// Sealed host command.
    pub command_key: String,
    /// Genuine host runtime run.
    pub run_id: String,
    /// Genuine host runtime worker.
    pub worker_id: String,
    /// Actual host dispatcher claim, if recorded.
    pub claim_id: Option<String>,
    /// Host worker revision observed with this snapshot.
    pub worker_revision: u64,
    /// Actual original worker lifecycle; never copied from a derived correction.
    pub worker_status: threadlane_core::WorkerStatus,
    /// Core-owned finite diagnostic DTO.
    pub snapshot: threadlane_core::provider_health::ProviderHealthSnapshot,
}

impl OriginalWorkerProviderHealth {
    /// Extracts only from an independently scoped actual host snapshot.
    pub fn from_snapshot(
        snapshot: &threadlane_host::TaskStateSnapshot,
        approval: &threadlane_authority::ApprovalRecord,
        task: &threadlane_authority::ApprovedTaskRecord,
    ) -> Option<Self> {
        let worker = snapshot.worker.as_ref()?;
        let command_key = snapshot.command_key.as_ref()?;
        if snapshot.approval_id != approval.approval_id
            || snapshot.subject_digest != approval.subject_digest
            || snapshot.binding_id != approval.binding_id
            || snapshot.thread_uuid != approval.thread_uuid
            || snapshot.task_id != task.task_id
            || snapshot.task_digest != task.task_digest
            || snapshot.command_key != task.command_key
            || snapshot.profile_id != task.profile_id
            || snapshot.provider != task.provider
            || snapshot.adapter != task.adapter
            || worker.binding_id != snapshot.session_binding_id
            || snapshot.run_id.as_deref() != Some(worker.run_id.as_str())
            || snapshot.worker_id.as_deref() != Some(worker.worker_id.as_str())
            || Some(&worker.profile_id) != task.profile_id.as_ref()
        {
            return None;
        }
        let health = worker.progress.provider_health.as_ref().or_else(|| {
            worker
                .outcome
                .as_ref()
                .and_then(|outcome| outcome.provider_health.as_ref())
        })?;
        if !health.is_valid() {
            return None;
        }
        Some(Self {
            approval_id: approval.approval_id.clone(),
            approval_subject_digest: approval.subject_digest.clone(),
            task_id: task.task_id.clone(),
            task_digest: task.task_digest.clone(),
            command_key: command_key.clone(),
            run_id: worker.run_id.clone(),
            worker_id: worker.worker_id.clone(),
            claim_id: worker.claim_id.clone(),
            worker_revision: worker.revision,
            worker_status: worker.status,
            snapshot: health.clone(),
        })
    }

    /// Host history may advance only within its sealed original run/worker/claim.
    pub fn is_valid_successor_of(&self, prior: &Self) -> bool {
        self.approval_id == prior.approval_id
            && self.approval_subject_digest == prior.approval_subject_digest
            && self.task_id == prior.task_id
            && self.task_digest == prior.task_digest
            && self.command_key == prior.command_key
            && self.run_id == prior.run_id
            && self.worker_id == prior.worker_id
            && self.claim_id == prior.claim_id
            && self.worker_revision >= prior.worker_revision
            && self.snapshot.is_valid_successor_of(&prior.snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn failed_diagnostic_persistence_does_not_replace_a_proven_terminal_result() {
        let snapshot =
            threadlane_workers::antigravity_diagnostics::AntigravityDiagnosticParser::new()
                .snapshot();
        let expected = snapshot.clone();
        let mut terminal_writes = 0;
        let mut live_writes = 0;
        let result = supervise_health_updates(
            |observer| async move {
                observer.send_replace(Some(snapshot.clone()));
                // Keep the fake provider open until the live consumer has been scheduled.
                for _ in 0..8 {
                    tokio::task::yield_now().await;
                }
                Ok::<_, NativeError>((42, snapshot))
            },
            |result| Some(&result.1),
            |_, terminal| {
                if terminal {
                    terminal_writes += 1;
                } else {
                    live_writes += 1;
                }
                Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "injected optional persistence failure",
                ))
            },
        )
        .await
        .expect("provider result must survive optional persistence failure");
        assert_eq!(result, (42, expected));
        assert!(live_writes > 0);
        assert_eq!(terminal_writes, 1);
    }
}
