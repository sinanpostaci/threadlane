//! Bounded read-only forecast comparison report for one scoped Worker backlog item.
//!
//! Reads existing control-plane SQLite state, frozen authority records, host session
//! manifests/receipts/runtime SQLite state, and optional Forecast A / shadow advice files
//! strictly read-only without opening mutable stores, acquiring or creating lock files,
//! running SQLite migrations, performing live provider/host calls, or echoing raw prompts.

use std::fs::{self, Metadata, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use rusqlite::{
    params, Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior,
};
use serde::{Deserialize, Serialize};
use threadlane::DATABASE_FILENAME;
use threadlane_authority::validation::{
    validate_canonical_thread_uuid, validate_identifier, validate_sha256_hex,
};
use threadlane_authority::{ApprovalRecord, ApprovedTaskRecord, AuthorityStore};
use threadlane_core::{
    BacklogItemKind, BacklogStatus, WorkerBacklogAcceptanceRejectionRecord, WorkerBacklogAuthority,
    WorkerBacklogCompletionRecord, WorkerBacklogDispatchReceipt, WorkerBacklogExecution,
    WorkerBacklogOutcome, WorkerBacklogRecordedOutcome, WorkerExecutionBinding, WorkerStatus,
    SCHEMA_VERSION,
};
use threadlane_host::{
    read_host_manifest_optional, read_session_task_ledger_recorded_history_readonly,
    verify_existing_private_dir, verify_existing_session_dir, RUNTIME_STATE_SUBDIR,
};
pub(crate) use threadlane_native::decision_badge::ForecastComplexity;
use threadlane_native::decision_badge::{
    map_nofollow_open_error as map_artifact_nofollow_open_error,
    read_verified_forecast_artifacts as read_verified_forecast_artifacts_shared,
    validate_existing_private_dir_meta as validate_existing_private_dir_meta_shared,
    verify_same_file_identity as verify_same_file_identity_shared, AdviceLoadState,
    ForecastArtifactError, ForecastRecordFile, ShadowForecastStatus,
};
#[cfg(test)]
use threadlane_native::decision_badge::{
    FORECAST_ADVICE_FILENAME, FORECAST_RECORD_FILENAME, FORECAST_SUBDIR,
};
use threadlane_native::fs_safety::constant_time_str_eq;
use threadlane_native::operator_cli::OperatorCliError;
use threadlane_workflow::Difficulty;

const MAX_CORE_DB_BYTES: u64 = 64 * 1024 * 1024;
const MAX_ITEM_EXECUTIONS: usize = 256;

/// Store paths and caller scope required to build a read-only [`ForecastReport`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct ForecastReportScope<'a> {
    /// Canonical existing private (`0700`) control-plane state directory.
    pub state_dir: &'a Path,
    /// Canonical existing private (`0700`) plans directory holding `ordinary-forecasts`.
    pub plans_dir: &'a Path,
    /// Canonical existing private (`0700`) authority store directory.
    pub authority_dir: &'a Path,
    /// Canonical existing private (`0700`) host sessions directory.
    pub sessions_dir: &'a Path,
    /// Verified control-plane binding identifier.
    pub binding_id: &'a str,
    /// Verified canonical lowercase hyphenated non-nil thread UUID.
    pub thread_uuid: &'a str,
    /// Target durable Worker backlog item identifier.
    pub item_id: &'a str,
}

/// Presence state of the Forecast A `record.json` artifact for the subject digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ForecastRecordStatus {
    /// Scoped, verified Forecast A `record.json` was loaded.
    Present,
    /// No Forecast A `record.json` exists for this approval subject digest.
    Missing,
}

/// Initial Coworker forecast summary derived from frozen approval task difficulty and Forecast A.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct InitialCoworkerForecast {
    /// Whether Forecast A `record.json` was present (`present` or `missing`).
    pub record_status: ForecastRecordStatus,
    /// Frozen initial Coworker difficulty on the approved task (matched against Forecast A when present).
    pub difficulty: Difficulty,
    /// Verified complexity markers from Forecast A, or `None` when Forecast A is absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub complexity: Option<ForecastComplexity>,
}

/// Availability status of optional provider shadow advice (`advice.json`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProviderAdviceStatus {
    /// Valid scope-matched provider advice with a classified difficulty was loaded.
    Available,
    /// Provider shadow advice was explicitly skipped.
    Skipped,
    /// Provider shadow advice is still pending.
    Pending,
    /// Provider shadow advice is unavailable (missing or refused without a classified difficulty).
    Unavailable,
    /// Provider shadow advice outcome is unknown (ambiguous launch/storage error or incomplete output).
    Unknown,
}

/// Minimal provider shadow advice summary (never includes raw prompts or summaries).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ProviderAdviceSummary {
    /// Availability state (`available`, `skipped`, `pending`, `unavailable`, or `unknown`).
    pub status: ProviderAdviceStatus,
    /// Validated provider difficulty when `status == available`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub difficulty: Option<Difficulty>,
    /// Validated finite provider confidence in `[0.0, 1.0]`, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
}

/// Agreement between initial Coworker difficulty and valid provider difficulty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ForecastAgreement {
    /// Initial Coworker difficulty matches valid provider difficulty.
    Agree,
    /// Initial Coworker difficulty differs from valid provider difficulty.
    Disagree,
    /// Agreement is unavailable because valid provider difficulty is absent.
    Unavailable,
}

/// Verified runtime Worker dispatch/result availability for the current execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkerResultStatus {
    /// Current execution reservation has no attached dispatch receipt yet.
    NotDispatched,
    /// Dispatched Worker is still non-terminal (`queued`, `claimed`, `running`, or `cancel_requested`).
    InProgress,
    /// Dispatched Worker reached a terminal or `unknown` runtime state.
    Terminal,
}

/// Verified Worker execution attempt count, timing from verified timestamps, and guarded disposition.
///
/// Never infers actual task difficulty from elapsed time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExecutionOutcomeSummary {
    /// Current durable backlog item status (`todo`, `doing`, `done`, or `blocked`).
    pub item_status: BacklogStatus,
    /// Runtime Worker result availability (`not_dispatched`, `in_progress`, or `terminal`).
    pub worker_result_status: WorkerResultStatus,
    /// Verified runtime Worker status, when dispatched.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_status: Option<WorkerStatus>,
    /// Actual verified Worker dispatch claim attempt count, when dispatched.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_count: Option<u32>,
    /// Verified Worker start timestamp (ms since Unix epoch), when recorded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_started_at_ms: Option<i64>,
    /// Verified Worker completion timestamp (ms since Unix epoch), when recorded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_completed_at_ms: Option<i64>,
    /// Elapsed Worker time in milliseconds computed solely from verified start/completion timestamps.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed_worker_ms: Option<u64>,
    /// Operator-verified technical outcome on the current execution, when recorded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recorded_outcome: Option<WorkerBacklogOutcome>,
    /// `true` only when the item is durably `done` with a matching guarded completion receipt.
    pub guarded_done: bool,
    /// `true` when the current execution carries a guarded Coworker acceptance rejection record.
    pub guarded_acceptance_rejected: bool,
}

/// Overall correction evidence classification scoped to one backlog item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CorrectionEvidenceStatus {
    /// Same-item history proves at least one Coworker acceptance rejection and zero non-rejection retries.
    AcceptanceRejected,
    /// Same-item history proves at least one non-rejection retry and zero same-item acceptance rejections.
    Retried,
    /// Same-item history proves both Coworker acceptance rejection(s) and non-rejection retry attempt(s).
    AcceptanceRejectedAndRetried,
    /// No same-item rejection or retry occurred; correction across separate backlog items cannot be proven.
    Unknown,
}

/// Proof boundary for corrections performed under separate backlog items.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CrossItemCorrectionStatus {
    /// Single-item scoped records cannot prove whether a separate backlog item corrected this delivery.
    Unknown,
}

/// Correction evidence separating Coworker acceptance rejections from technical retries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CorrectionEvidence {
    /// Total durable Worker execution reservations recorded on this backlog item.
    pub item_execution_count: u32,
    /// Count of executions on this item with a guarded Coworker acceptance rejection record.
    pub acceptance_rejection_count: u32,
    /// Count of replacement execution retries on this item following non-acceptance failures.
    pub retry_count: u32,
    /// Count of executions on this item with a recorded technical `failure` outcome.
    pub failed_execution_count: u32,
    /// Correction summary (`unknown` when no same-item rejection/retry exists, since cross-item correction is unproven).
    pub status: CorrectionEvidenceStatus,
    /// Explicit cross-item correction proof status (always `unknown`).
    pub cross_item_status: CrossItemCorrectionStatus,
}

/// Minimal, bounded, path-free, prompt-free read-only forecast comparison report.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ForecastReport {
    /// Verified control-plane binding identifier.
    pub binding_id: String,
    /// Verified canonical lowercase hyphenated non-nil thread UUID.
    pub thread_uuid: String,
    /// Durable Worker backlog item identifier.
    pub item_id: String,
    /// Current durable Worker execution reference on this item.
    pub execution_ref: String,
    /// Frozen approval identifier linked to the current execution.
    pub approval_id: String,
    /// Frozen 64-character lowercase hex SHA-256 approval subject digest.
    pub subject_digest: String,
    /// Frozen approved task identifier.
    pub task_id: String,
    /// Frozen workflow profile identifier.
    pub profile_id: String,
    /// Verified runtime session binding identifier, when dispatched.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_binding_id: Option<String>,
    /// Verified runtime run identifier, when dispatched.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Verified runtime worker identifier, when dispatched.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    /// Initial Coworker forecast (`difficulty` and optional `complexity`).
    pub initial_coworker: InitialCoworkerForecast,
    /// Optional validated provider shadow advice or `skipped`/`pending`/`unavailable`/`unknown`.
    pub provider_advice: ProviderAdviceSummary,
    /// Agreement between initial Coworker difficulty and valid provider difficulty.
    pub agreement: ForecastAgreement,
    /// Actual Worker attempt count, verified elapsed time, and guarded done/rejection state.
    pub execution: ExecutionOutcomeSummary,
    /// Correction evidence separating acceptance rejections from retries (`unknown` across separate items).
    pub correction_evidence: CorrectionEvidence,
}

struct ControlItemState {
    status: BacklogStatus,
    current_execution_ref: Option<String>,
    executions: Vec<WorkerBacklogExecution>,
}

struct VerifiedWorkerRuntime {
    status: WorkerStatus,
    attempt_count: u32,
    started_at_ms: Option<i64>,
    completed_at_ms: Option<i64>,
    elapsed_worker_ms: Option<u64>,
}

struct WorkerRuntimeRow {
    status_raw: String,
    worker_profile_id: String,
    attempt_count_raw: i64,
    execution_binding_json: Option<String>,
    started_at_ms: Option<i64>,
    completed_at_ms: Option<i64>,
}

/// Builds a bounded, strictly read-only [`ForecastReport`] for one scoped Worker backlog item.
pub(crate) fn build_forecast_report(
    scope: ForecastReportScope<'_>,
) -> Result<ForecastReport, OperatorCliError> {
    let binding_id = validate_identifier("binding_id", scope.binding_id).map_err(|_| {
        OperatorCliError::invalid_input("`--binding` failed identifier syntax validation.")
    })?;
    let thread_uuid =
        validate_canonical_thread_uuid("thread_uuid", scope.thread_uuid).map_err(|_| {
            OperatorCliError::invalid_input(
                "`--thread` must be a canonical lowercase hyphenated non-nil UUID.",
            )
        })?;
    let item_id = validate_identifier("item_id", scope.item_id).map_err(|_| {
        OperatorCliError::invalid_input("`--item-id` failed identifier syntax validation.")
    })?;

    let state_db_path = scope.state_dir.join(DATABASE_FILENAME);
    let control = read_control_item_readonly(&state_db_path, &binding_id, &thread_uuid, &item_id)?;
    let current_exec = resolve_current_execution(&control)?;
    let approval = read_verified_approval(
        scope.authority_dir,
        &binding_id,
        &thread_uuid,
        &current_exec.authority,
    )?;
    let approved_task = resolve_approved_task(&approval, &current_exec.authority)?;

    let (session_binding_id, run_id, worker_id, worker_runtime) =
        match current_exec.dispatch_receipt.as_ref() {
            Some(dispatch) => {
                let runtime = read_verified_worker_runtime(
                    scope.sessions_dir,
                    &binding_id,
                    &thread_uuid,
                    &approval,
                    approved_task,
                    dispatch,
                )?;
                (
                    Some(dispatch.session_binding_id.clone()),
                    Some(dispatch.run_id.clone()),
                    Some(dispatch.worker_id.clone()),
                    Some(runtime),
                )
            }
            None => (None, None, None, None),
        };

    let (forecast_record, advice_state) = read_verified_forecast_artifacts(
        scope.plans_dir,
        &binding_id,
        &thread_uuid,
        &approval,
        approved_task,
    )?;

    let initial_coworker = InitialCoworkerForecast {
        record_status: if forecast_record.is_some() {
            ForecastRecordStatus::Present
        } else {
            ForecastRecordStatus::Missing
        },
        difficulty: approved_task.difficulty,
        complexity: forecast_record.as_ref().map(|r| r.complexity),
    };

    let (provider_advice, agreement) = evaluate_provider_advice(
        initial_coworker.difficulty,
        forecast_record.as_ref(),
        &advice_state,
    );

    let (
        worker_result_status,
        worker_status,
        attempt_count,
        worker_started_at_ms,
        worker_completed_at_ms,
        elapsed_worker_ms,
    ) = match worker_runtime {
        None => (
            WorkerResultStatus::NotDispatched,
            None,
            None,
            None,
            None,
            None,
        ),
        Some(rt) => (
            if rt.status.is_terminal() {
                WorkerResultStatus::Terminal
            } else {
                WorkerResultStatus::InProgress
            },
            Some(rt.status),
            Some(rt.attempt_count),
            rt.started_at_ms,
            rt.completed_at_ms,
            rt.elapsed_worker_ms,
        ),
    };

    let execution = ExecutionOutcomeSummary {
        item_status: control.status,
        worker_result_status,
        worker_status,
        attempt_count,
        worker_started_at_ms,
        worker_completed_at_ms,
        elapsed_worker_ms,
        recorded_outcome: current_exec.outcome.as_ref().map(|o| o.outcome),
        guarded_done: control.status == BacklogStatus::Done && current_exec.completion.is_some(),
        guarded_acceptance_rejected: current_exec.acceptance_rejection.is_some(),
    };

    let correction_evidence = compute_correction_evidence(&control.executions);

    Ok(ForecastReport {
        binding_id,
        thread_uuid,
        item_id,
        execution_ref: current_exec.execution_ref.clone(),
        approval_id: approval.approval_id.clone(),
        subject_digest: approval.subject_digest.clone(),
        task_id: approved_task.task_id.clone(),
        profile_id: approval.profile_id,
        session_binding_id,
        run_id,
        worker_id,
        initial_coworker,
        provider_advice,
        agreement,
        execution,
        correction_evidence,
    })
}

fn read_control_item_readonly(
    db_path: &Path,
    binding_id: &str,
    thread_uuid: &str,
    item_id: &str,
) -> Result<ControlItemState, OperatorCliError> {
    with_readonly_sqlite_tx(db_path, |tx| {
        let stored_thread: Option<String> = tx
            .query_row(
                "SELECT thread_uuid FROM bindings WHERE binding_id = ?1",
                params![binding_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| sqlite_read_error())?;
        let Some(stored_thread) = stored_thread else {
            return Err(OperatorCliError::new(
                "binding_not_found",
                "Requested binding was not found in control-plane state.",
            ));
        };
        if stored_thread != thread_uuid {
            return Err(OperatorCliError::new(
                "scope_mismatch",
                "Control-plane binding thread_uuid does not match requested thread scope.",
            ));
        }

        if !readonly_table_exists(tx, "backlog_items")?
            || !readonly_table_exists(tx, "backlog_worker_executions")?
        {
            return Err(OperatorCliError::new(
                "not_found",
                "Backlog tables are not present in control-plane state.",
            ));
        }
        let rejection_table_exists =
            readonly_table_exists(tx, "backlog_worker_acceptance_rejections")?;

        let item_row: Option<(String, String, String, Option<String>)> = tx
            .query_row(
                "SELECT binding_id, kind, status, current_execution_ref
                 FROM backlog_items
                 WHERE item_id = ?1",
                params![item_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(|_| sqlite_read_error())?;

        let Some((stored_binding_id, kind_raw, status_raw, current_execution_ref)) = item_row
        else {
            return Err(OperatorCliError::new(
                "not_found",
                "Requested backlog item was not found.",
            ));
        };
        if stored_binding_id != binding_id {
            return Err(OperatorCliError::new(
                "scope_mismatch",
                "Requested backlog item does not belong to the requested binding and thread scope.",
            ));
        }
        let kind = parse_backlog_kind(&kind_raw).ok_or_else(corrupt_control_state)?;
        if kind != BacklogItemKind::Worker {
            return Err(OperatorCliError::invalid_input(
                "Forecast report is only supported for Worker backlog items.",
            ));
        }
        let status = parse_backlog_status(&status_raw).ok_or_else(corrupt_control_state)?;

        let executions = read_item_executions_tx(tx, binding_id, item_id, rejection_table_exists)?;

        Ok(ControlItemState {
            status,
            current_execution_ref,
            executions,
        })
    })
}

fn read_item_executions_tx(
    tx: &Transaction<'_>,
    binding_id: &str,
    item_id: &str,
    rejection_table_exists: bool,
) -> Result<Vec<WorkerBacklogExecution>, OperatorCliError> {
    let limit = i64::try_from(MAX_ITEM_EXECUTIONS + 1).unwrap_or(i64::MAX);
    let mut stmt = tx
        .prepare(
            "SELECT execution_ref, binding_id, item_id, approval_id,
                    subject_digest, task_id, task_digest,
                    acceptance_condition_digest, delivery_description_digest,
                    reservation_revision, reserved_at_ms, session_binding_id,
                    run_id, worker_id, profile_id, execution_binding_digest,
                    attached_at_ms, outcome, outcome_note, outcome_result_digest,
                    outcome_recorded_at_ms, completion_result_digest,
                    completion_delivery_digest, completion_note,
                    completion_evidence, completed_at_ms
             FROM backlog_worker_executions
             WHERE item_id = ?1
             ORDER BY rowid ASC
             LIMIT ?2",
        )
        .map_err(|_| sqlite_read_error())?;

    let mut rows = stmt
        .query(params![item_id, limit])
        .map_err(|_| sqlite_read_error())?;

    let mut executions = Vec::new();
    while let Some(row) = rows.next().map_err(|_| sqlite_read_error())? {
        if executions.len() >= MAX_ITEM_EXECUTIONS {
            return Err(OperatorCliError::new(
                "corrupt_state",
                "Worker backlog execution history exceeds bounded inspection limit.",
            ));
        }

        let execution_ref: String = row.get(0).map_err(|_| sqlite_read_error())?;
        let stored_binding_id: String = row.get(1).map_err(|_| sqlite_read_error())?;
        let stored_item_id: String = row.get(2).map_err(|_| sqlite_read_error())?;
        if stored_binding_id != binding_id || stored_item_id != item_id {
            return Err(OperatorCliError::new(
                "scope_mismatch",
                "Worker execution row does not belong to the requested binding and item scope.",
            ));
        }

        let approval_id: String = row.get(3).map_err(|_| sqlite_read_error())?;
        let subject_digest: String = row.get(4).map_err(|_| sqlite_read_error())?;
        let task_id: String = row.get(5).map_err(|_| sqlite_read_error())?;
        let task_digest: String = row.get(6).map_err(|_| sqlite_read_error())?;
        let acceptance_condition_digest: String = row.get(7).map_err(|_| sqlite_read_error())?;
        let delivery_description_digest: String = row.get(8).map_err(|_| sqlite_read_error())?;
        let reservation_revision_raw: i64 = row.get(9).map_err(|_| sqlite_read_error())?;
        let reserved_at_ms: i64 = row.get(10).map_err(|_| sqlite_read_error())?;
        let session_binding_id: Option<String> = row.get(11).map_err(|_| sqlite_read_error())?;
        let run_id: Option<String> = row.get(12).map_err(|_| sqlite_read_error())?;
        let worker_id: Option<String> = row.get(13).map_err(|_| sqlite_read_error())?;
        let profile_id: Option<String> = row.get(14).map_err(|_| sqlite_read_error())?;
        let execution_binding_digest: Option<String> =
            row.get(15).map_err(|_| sqlite_read_error())?;
        let attached_at_ms: Option<i64> = row.get(16).map_err(|_| sqlite_read_error())?;
        let outcome_raw: Option<String> = row.get(17).map_err(|_| sqlite_read_error())?;
        let outcome_note: Option<String> = row.get(18).map_err(|_| sqlite_read_error())?;
        let outcome_result_digest: Option<String> = row.get(19).map_err(|_| sqlite_read_error())?;
        let outcome_recorded_at_ms: Option<i64> = row.get(20).map_err(|_| sqlite_read_error())?;
        let completion_result_digest: Option<String> =
            row.get(21).map_err(|_| sqlite_read_error())?;
        let completion_delivery_digest: Option<String> =
            row.get(22).map_err(|_| sqlite_read_error())?;
        let completion_note: Option<String> = row.get(23).map_err(|_| sqlite_read_error())?;
        let completion_evidence: Option<String> = row.get(24).map_err(|_| sqlite_read_error())?;
        let completed_at_ms: Option<i64> = row.get(25).map_err(|_| sqlite_read_error())?;

        let reservation_revision =
            u64::try_from(reservation_revision_raw).map_err(|_| corrupt_control_state())?;

        let dispatch_receipt = match (session_binding_id, run_id, worker_id, profile_id) {
            (None, None, None, None) => {
                if execution_binding_digest.is_some() || attached_at_ms.is_some() {
                    return Err(corrupt_control_state());
                }
                None
            }
            (Some(session_binding_id), Some(run_id), Some(worker_id), Some(profile_id)) => {
                Some(WorkerBacklogDispatchReceipt {
                    session_binding_id,
                    run_id,
                    worker_id,
                    profile_id,
                    execution_binding_digest,
                })
            }
            _ => return Err(corrupt_control_state()),
        };

        let outcome = match outcome_raw {
            None => {
                if outcome_note.is_some()
                    || outcome_result_digest.is_some()
                    || outcome_recorded_at_ms.is_some()
                {
                    return Err(corrupt_control_state());
                }
                None
            }
            Some(raw_outcome) => {
                let kind =
                    parse_worker_backlog_outcome(&raw_outcome).ok_or_else(corrupt_control_state)?;
                let note = outcome_note.ok_or_else(corrupt_control_state)?;
                let recorded_at_ms = outcome_recorded_at_ms.ok_or_else(corrupt_control_state)?;
                let result_digest = if kind == WorkerBacklogOutcome::Unknown {
                    if outcome_result_digest.is_some() {
                        return Err(corrupt_control_state());
                    }
                    None
                } else {
                    Some(outcome_result_digest.ok_or_else(corrupt_control_state)?)
                };
                Some(WorkerBacklogRecordedOutcome {
                    outcome: kind,
                    note,
                    result_digest,
                    recorded_at_ms,
                })
            }
        };

        let completion = match (
            completion_result_digest,
            completion_delivery_digest,
            completion_note,
            completion_evidence,
            completed_at_ms,
        ) {
            (None, None, None, None, None) => None,
            (
                Some(result_digest),
                Some(delivery_digest),
                Some(acceptance_note),
                Some(acceptance_evidence),
                Some(completed_at_ms),
            ) => Some(WorkerBacklogCompletionRecord {
                result_digest,
                delivery_digest,
                acceptance_note,
                acceptance_evidence,
                completed_at_ms,
            }),
            _ => return Err(corrupt_control_state()),
        };

        let acceptance_rejection = if rejection_table_exists {
            read_acceptance_rejection_tx(tx, binding_id, item_id, &execution_ref)?
        } else {
            None
        };

        if completion.is_some() && acceptance_rejection.is_some() {
            return Err(corrupt_control_state());
        }

        executions.push(WorkerBacklogExecution {
            execution_ref,
            binding_id: stored_binding_id,
            item_id: stored_item_id,
            authority: WorkerBacklogAuthority {
                approval_id,
                subject_digest,
                task_id,
                task_digest,
            },
            acceptance_condition_digest,
            delivery_description_digest,
            reservation_revision,
            reserved_at_ms,
            dispatch_receipt,
            attached_at_ms,
            outcome,
            completion,
            acceptance_rejection,
        });
    }

    Ok(executions)
}

fn read_acceptance_rejection_tx(
    tx: &Transaction<'_>,
    binding_id: &str,
    item_id: &str,
    execution_ref: &str,
) -> Result<Option<WorkerBacklogAcceptanceRejectionRecord>, OperatorCliError> {
    let row: Option<(String, String, String, String, String, String, i64)> = tx
        .query_row(
            "SELECT binding_id, item_id, result_digest, delivery_digest,
                    reason, evidence, rejected_at_ms
             FROM backlog_worker_acceptance_rejections
             WHERE execution_ref = ?1",
            params![execution_ref],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()
        .map_err(|_| sqlite_read_error())?;

    let Some((
        stored_binding_id,
        stored_item_id,
        result_digest,
        delivery_digest,
        reason,
        evidence,
        rejected_at_ms,
    )) = row
    else {
        return Ok(None);
    };

    if stored_binding_id != binding_id || stored_item_id != item_id {
        return Err(OperatorCliError::new(
            "scope_mismatch",
            "Acceptance rejection row does not belong to the requested binding and item scope.",
        ));
    }

    Ok(Some(WorkerBacklogAcceptanceRejectionRecord {
        result_digest,
        delivery_digest,
        reason,
        evidence,
        rejected_at_ms,
    }))
}

fn resolve_current_execution(
    control: &ControlItemState,
) -> Result<&WorkerBacklogExecution, OperatorCliError> {
    let Some(current_ref) = control.current_execution_ref.as_deref() else {
        if !control.executions.is_empty() {
            return Err(corrupt_control_state());
        }
        return Err(OperatorCliError::new(
            "execution_not_found",
            "Worker backlog item has no current execution reservation.",
        ));
    };

    let Some(latest) = control.executions.last() else {
        return Err(corrupt_control_state());
    };
    if latest.execution_ref != current_ref {
        return Err(corrupt_control_state());
    }

    match control.status {
        BacklogStatus::Done => {
            let outcome_ok = latest
                .outcome
                .as_ref()
                .is_some_and(|o| o.outcome == WorkerBacklogOutcome::Success);
            if !outcome_ok || latest.completion.is_none() || latest.acceptance_rejection.is_some() {
                return Err(corrupt_control_state());
            }
        }
        BacklogStatus::Todo | BacklogStatus::Doing | BacklogStatus::Blocked => {
            if latest.completion.is_some() {
                return Err(corrupt_control_state());
            }
        }
    }

    validate_sha256_hex("subject_digest", &latest.authority.subject_digest)
        .map_err(|_| corrupt_control_state())?;
    validate_sha256_hex("task_digest", &latest.authority.task_digest)
        .map_err(|_| corrupt_control_state())?;
    validate_identifier("approval_id", &latest.authority.approval_id)
        .map_err(|_| corrupt_control_state())?;
    validate_identifier("task_id", &latest.authority.task_id)
        .map_err(|_| corrupt_control_state())?;

    Ok(latest)
}

fn read_verified_approval(
    authority_dir: &Path,
    binding_id: &str,
    thread_uuid: &str,
    authority_ref: &WorkerBacklogAuthority,
) -> Result<ApprovalRecord, OperatorCliError> {
    let record =
        AuthorityStore::get_for_operator_readonly_at(authority_dir, &authority_ref.approval_id)
            .map_err(|err| super::map_authority_error(&err))?;

    if record.binding_id != binding_id || record.thread_uuid != thread_uuid {
        return Err(OperatorCliError::new(
            "scope_mismatch",
            "Approval record does not belong to the requested binding and thread scope.",
        ));
    }
    if !constant_time_str_eq(&record.subject_digest, &authority_ref.subject_digest) {
        return Err(OperatorCliError::new(
            "subject_mismatch",
            "Approval subject_digest does not match the current execution authority.",
        ));
    }
    validate_identifier("profile_id", &record.profile_id).map_err(|_| {
        OperatorCliError::new(
            "corrupt_store",
            "Approval record workflow profile_id failed validation.",
        )
    })?;

    Ok(record)
}

fn resolve_approved_task<'a>(
    approval: &'a ApprovalRecord,
    authority_ref: &WorkerBacklogAuthority,
) -> Result<&'a ApprovedTaskRecord, OperatorCliError> {
    let mut matches = approval
        .tasks
        .iter()
        .filter(|task| task.task_id == authority_ref.task_id);
    let Some(task) = matches.next() else {
        return Err(OperatorCliError::new(
            "unknown_task",
            "Current execution task was not found in the frozen approval record.",
        ));
    };
    if matches.next().is_some() {
        return Err(OperatorCliError::new(
            "corrupt_store",
            "Approval record contains duplicate matching task entries.",
        ));
    }
    if !constant_time_str_eq(&task.task_digest, &authority_ref.task_digest) {
        return Err(OperatorCliError::new(
            "scope_mismatch",
            "Approved task digest does not match the current execution authority.",
        ));
    }
    Ok(task)
}

fn read_verified_worker_runtime(
    sessions_dir: &Path,
    control_binding_id: &str,
    thread_uuid: &str,
    approval: &ApprovalRecord,
    approved_task: &ApprovedTaskRecord,
    dispatch: &WorkerBacklogDispatchReceipt,
) -> Result<VerifiedWorkerRuntime, OperatorCliError> {
    if dispatch.session_binding_id == control_binding_id {
        return Err(OperatorCliError::new(
            "scope_mismatch",
            "Session binding identifier must be distinct from the control-plane binding.",
        ));
    }
    if approved_task.profile_id.as_deref() != Some(dispatch.profile_id.as_str()) {
        return Err(OperatorCliError::new(
            "scope_mismatch",
            "Dispatch receipt worker profile_id does not match the frozen approved task.",
        ));
    }

    let session_dir = verify_existing_session_dir(sessions_dir, &approval.approval_id)
        .map_err(|err| super::map_host_error(&err))?;

    let manifest = read_host_manifest_optional(&session_dir, approval)
        .map_err(|err| super::map_host_error(&err))?
        .ok_or_else(|| {
            OperatorCliError::new(
                "corrupt_session_state",
                "Host session manifest is missing for a dispatched Worker execution.",
            )
        })?;
    if manifest.control_binding_id != control_binding_id
        || manifest.thread_uuid != thread_uuid
        || !constant_time_str_eq(&manifest.subject_digest, &approval.subject_digest)
        || manifest.session_binding_id.as_deref() != Some(dispatch.session_binding_id.as_str())
    {
        return Err(OperatorCliError::new(
            "scope_mismatch",
            "Host session manifest scope or session binding does not match current execution.",
        ));
    }

    let (_, receipts) = read_session_task_ledger_recorded_history_readonly(
        &session_dir,
        approval,
        &dispatch.session_binding_id,
    )
    .map_err(|err| super::map_host_error(&err))?
    .ok_or_else(|| {
        OperatorCliError::new(
            "corrupt_session_state",
            "Host task receipt ledger is missing for a dispatched Worker execution.",
        )
    })?;

    let receipt = receipts
        .iter()
        .find(|entry| entry.task_id == approved_task.task_id)
        .ok_or_else(|| {
            OperatorCliError::new(
                "corrupt_session_state",
                "Task receipt is missing for the dispatched approved task.",
            )
        })?;
    let receipt_exec_digest = receipt
        .execution_binding
        .as_ref()
        .map(|binding| binding.execution_digest.as_str());
    if !constant_time_str_eq(&receipt.task_digest, &approved_task.task_digest)
        || receipt.profile_id != dispatch.profile_id
        || receipt.run_id != dispatch.run_id
        || receipt.worker_id != dispatch.worker_id
        || receipt_exec_digest != dispatch.execution_binding_digest.as_deref()
    {
        return Err(OperatorCliError::new(
            "scope_mismatch",
            "Host task receipt identity does not match the current Worker execution.",
        ));
    }

    let runtime_dir = verify_existing_private_dir(&session_dir.join(RUNTIME_STATE_SUBDIR))
        .map_err(|err| super::map_host_error(&err))?;
    let session_db_path = runtime_dir.join(DATABASE_FILENAME);

    with_readonly_sqlite_tx(&session_db_path, |tx| {
        let binding_count: i64 = tx
            .query_row("SELECT COUNT(*) FROM bindings", [], |row| row.get(0))
            .map_err(|_| sqlite_read_error())?;
        if binding_count != 1 {
            return Err(OperatorCliError::new(
                "corrupt_session_state",
                "Host session runtime database must contain exactly one session binding.",
            ));
        }

        let (stored_session_binding, stored_thread_uuid): (String, String) = tx
            .query_row(
                "SELECT binding_id, thread_uuid FROM bindings LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|_| sqlite_read_error())?;
        if stored_session_binding != dispatch.session_binding_id
            || stored_thread_uuid != thread_uuid
        {
            return Err(OperatorCliError::new(
                "scope_mismatch",
                "Host session runtime binding does not match session binding or thread scope.",
            ));
        }

        let run_exists: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM runs WHERE run_id = ?1 AND binding_id = ?2",
                params![dispatch.run_id, dispatch.session_binding_id],
                |row| row.get(0),
            )
            .map_err(|_| sqlite_read_error())?;
        if run_exists != 1 {
            return Err(OperatorCliError::new(
                "corrupt_session_state",
                "Host session runtime database is missing the dispatched run record.",
            ));
        }

        let worker_row: Option<WorkerRuntimeRow> = tx
            .query_row(
                "SELECT status, profile_id, attempt_count, execution_binding_json,
                        started_at_ms, completed_at_ms
                 FROM workers
                 WHERE worker_id = ?1 AND run_id = ?2 AND binding_id = ?3",
                params![
                    dispatch.worker_id,
                    dispatch.run_id,
                    dispatch.session_binding_id
                ],
                |row| {
                    Ok(WorkerRuntimeRow {
                        status_raw: row.get(0)?,
                        worker_profile_id: row.get(1)?,
                        attempt_count_raw: row.get(2)?,
                        execution_binding_json: row.get(3)?,
                        started_at_ms: row.get(4)?,
                        completed_at_ms: row.get(5)?,
                    })
                },
            )
            .optional()
            .map_err(|_| sqlite_read_error())?;

        let Some(WorkerRuntimeRow {
            status_raw,
            worker_profile_id,
            attempt_count_raw,
            execution_binding_json,
            started_at_ms,
            completed_at_ms,
        }) = worker_row
        else {
            return Err(OperatorCliError::new(
                "corrupt_session_state",
                "Host session runtime database is missing the dispatched worker record.",
            ));
        };

        if worker_profile_id != dispatch.profile_id {
            return Err(OperatorCliError::new(
                "scope_mismatch",
                "Host worker profile_id does not match the current execution dispatch receipt.",
            ));
        }

        let worker_exec_digest = match execution_binding_json {
            Some(raw_json) => {
                let binding: WorkerExecutionBinding =
                    serde_json::from_str(&raw_json).map_err(|_| {
                        OperatorCliError::new(
                            "corrupt_session_state",
                            "Host worker execution_binding_json failed validation.",
                        )
                    })?;
                Some(binding.execution_digest)
            }
            None => None,
        };
        if worker_exec_digest.as_deref() != dispatch.execution_binding_digest.as_deref() {
            return Err(OperatorCliError::new(
                "scope_mismatch",
                "Host worker execution binding digest does not match the dispatch receipt.",
            ));
        }

        let status = parse_worker_status(&status_raw).ok_or_else(|| {
            OperatorCliError::new(
                "corrupt_session_state",
                "Host worker status failed validation.",
            )
        })?;
        let attempt_count = u32::try_from(attempt_count_raw).map_err(|_| {
            OperatorCliError::new(
                "corrupt_session_state",
                "Host worker attempt_count is out of range.",
            )
        })?;
        let elapsed_worker_ms = compute_verified_elapsed_ms(started_at_ms, completed_at_ms)?;

        Ok(VerifiedWorkerRuntime {
            status,
            attempt_count,
            started_at_ms,
            completed_at_ms,
            elapsed_worker_ms,
        })
    })
}

fn compute_verified_elapsed_ms(
    started_at_ms: Option<i64>,
    completed_at_ms: Option<i64>,
) -> Result<Option<u64>, OperatorCliError> {
    if started_at_ms.is_some_and(|ts| ts < 0) || completed_at_ms.is_some_and(|ts| ts < 0) {
        return Err(OperatorCliError::new(
            "corrupt_session_state",
            "Verified Worker timestamps must be non-negative.",
        ));
    }
    match (started_at_ms, completed_at_ms) {
        (Some(start), Some(end)) => {
            if end < start {
                return Err(OperatorCliError::new(
                    "corrupt_session_state",
                    "Verified Worker completion timestamp precedes start timestamp.",
                ));
            }
            let elapsed = u64::try_from(end - start).map_err(|_| {
                OperatorCliError::new(
                    "corrupt_session_state",
                    "Verified Worker elapsed time delta is out of range.",
                )
            })?;
            Ok(Some(elapsed))
        }
        _ => Ok(None),
    }
}

fn map_forecast_artifact_error(err: ForecastArtifactError) -> OperatorCliError {
    OperatorCliError::new(err.code, err.message)
}

fn read_verified_forecast_artifacts(
    plans_dir: &Path,
    binding_id: &str,
    thread_uuid: &str,
    approval: &ApprovalRecord,
    approved_task: &ApprovedTaskRecord,
) -> Result<(Option<ForecastRecordFile>, AdviceLoadState), OperatorCliError> {
    read_verified_forecast_artifacts_shared(
        plans_dir,
        binding_id,
        thread_uuid,
        approval,
        approved_task,
    )
    .map_err(map_forecast_artifact_error)
}

fn evaluate_provider_advice(
    initial_difficulty: Difficulty,
    forecast_record: Option<&ForecastRecordFile>,
    advice_state: &AdviceLoadState,
) -> (ProviderAdviceSummary, ForecastAgreement) {
    match advice_state {
        AdviceLoadState::Loaded(file) => {
            if let Some(provider_difficulty) = file.advice.difficulty {
                let agreement = if provider_difficulty == initial_difficulty {
                    ForecastAgreement::Agree
                } else {
                    ForecastAgreement::Disagree
                };
                (
                    ProviderAdviceSummary {
                        status: ProviderAdviceStatus::Available,
                        difficulty: Some(provider_difficulty),
                        confidence: file.advice.confidence,
                    },
                    agreement,
                )
            } else {
                (
                    ProviderAdviceSummary {
                        status: ProviderAdviceStatus::Unavailable,
                        difficulty: None,
                        confidence: file.advice.confidence,
                    },
                    ForecastAgreement::Unavailable,
                )
            }
        }
        AdviceLoadState::Pending => (
            ProviderAdviceSummary {
                status: ProviderAdviceStatus::Pending,
                difficulty: None,
                confidence: None,
            },
            ForecastAgreement::Unavailable,
        ),
        AdviceLoadState::Unknown => (
            ProviderAdviceSummary {
                status: ProviderAdviceStatus::Unknown,
                difficulty: None,
                confidence: None,
            },
            ForecastAgreement::Unavailable,
        ),
        AdviceLoadState::Absent => {
            let status = match forecast_record.map(|r| r.status) {
                Some(ShadowForecastStatus::Skipped) => ProviderAdviceStatus::Skipped,
                Some(ShadowForecastStatus::Launched) => ProviderAdviceStatus::Pending,
                Some(ShadowForecastStatus::Unavailable) | None => ProviderAdviceStatus::Unavailable,
                Some(ShadowForecastStatus::Unknown) => ProviderAdviceStatus::Unknown,
            };
            (
                ProviderAdviceSummary {
                    status,
                    difficulty: None,
                    confidence: None,
                },
                ForecastAgreement::Unavailable,
            )
        }
    }
}

fn compute_correction_evidence(executions: &[WorkerBacklogExecution]) -> CorrectionEvidence {
    let item_execution_count = u32::try_from(executions.len()).unwrap_or(u32::MAX);
    let acceptance_rejection_count = u32::try_from(
        executions
            .iter()
            .filter(|exec| exec.acceptance_rejection.is_some())
            .count(),
    )
    .unwrap_or(u32::MAX);
    let failed_execution_count = u32::try_from(
        executions
            .iter()
            .filter(|exec| {
                exec.outcome
                    .as_ref()
                    .is_some_and(|outcome| outcome.outcome == WorkerBacklogOutcome::Failure)
            })
            .count(),
    )
    .unwrap_or(u32::MAX);
    let mut retry_count: u32 = 0;
    for pair in executions.windows(2) {
        if pair[0].acceptance_rejection.is_none() {
            retry_count = retry_count.saturating_add(1);
        }
    }

    let status = match (acceptance_rejection_count > 0, retry_count > 0) {
        (true, true) => CorrectionEvidenceStatus::AcceptanceRejectedAndRetried,
        (true, false) => CorrectionEvidenceStatus::AcceptanceRejected,
        (false, true) => CorrectionEvidenceStatus::Retried,
        (false, false) => CorrectionEvidenceStatus::Unknown,
    };

    CorrectionEvidence {
        item_execution_count,
        acceptance_rejection_count,
        retry_count,
        failed_execution_count,
        status,
        cross_item_status: CrossItemCorrectionStatus::Unknown,
    }
}

fn with_readonly_sqlite_tx<T, F>(db_path: &Path, f: F) -> Result<T, OperatorCliError>
where
    F: FnOnce(&Transaction<'_>) -> Result<T, OperatorCliError>,
{
    let parent = db_path.parent().ok_or_else(|| {
        OperatorCliError::new(
            "security_violation",
            "SQLite database path has no parent directory.",
        )
    })?;
    let parent_meta = validate_existing_private_dir_meta(parent)?;
    validate_readonly_sqlite_file(db_path, &parent_meta, MAX_CORE_DB_BYTES)?;

    let wal_path = sqlite_sidecar_path(db_path, "-wal");
    let shm_path = sqlite_sidecar_path(db_path, "-shm");
    let has_wal = inspect_readonly_sqlite_sidecar(&wal_path, &parent_meta, MAX_CORE_DB_BYTES)?;
    let has_shm = inspect_readonly_sqlite_sidecar(&shm_path, &parent_meta, MAX_CORE_DB_BYTES)?;

    let mut conn = match (has_wal, has_shm) {
        (true, true) => Connection::open_with_flags(
            db_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|_| sqlite_read_error())?,
        (false, false) => {
            let uri = build_sqlite_readonly_immutable_uri(db_path)?;
            Connection::open_with_flags(
                &uri,
                OpenFlags::SQLITE_OPEN_URI
                    | OpenFlags::SQLITE_OPEN_READ_ONLY
                    | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(|_| sqlite_read_error())?
        }
        (true, false) | (false, true) => {
            return Err(OperatorCliError::new(
                "corrupt_state",
                "SQLite database has inconsistent WAL/SHM sidecar state.",
            ));
        }
    };

    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .map_err(|_| sqlite_read_error())?;

    let schema_version: i64 = tx
        .query_row("SELECT version FROM schema_meta WHERE id = 1", [], |row| {
            row.get(0)
        })
        .map_err(|_| sqlite_read_error())?;
    if schema_version != i64::from(SCHEMA_VERSION) {
        return Err(OperatorCliError::new(
            "corrupt_state",
            "SQLite database schema version is incompatible with read-only inspection.",
        ));
    }

    f(&tx)
}

fn readonly_table_exists(tx: &Transaction<'_>, table: &str) -> Result<bool, OperatorCliError> {
    let count: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
            params![table],
            |row| row.get(0),
        )
        .map_err(|_| sqlite_read_error())?;
    Ok(count > 0)
}

fn sqlite_sidecar_path(database_path: &Path, suffix: &str) -> PathBuf {
    let mut os = database_path.as_os_str().to_os_string();
    os.push(suffix);
    PathBuf::from(os)
}

fn inspect_readonly_sqlite_sidecar(
    sidecar_path: &Path,
    parent_meta: &Metadata,
    max_bytes: u64,
) -> Result<bool, OperatorCliError> {
    match fs::symlink_metadata(sidecar_path) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(OperatorCliError::new(
            "io_error",
            "Failed to inspect SQLite sidecar metadata.",
        )),
        Ok(_) => {
            validate_readonly_sqlite_file(sidecar_path, parent_meta, max_bytes)?;
            Ok(true)
        }
    }
}

fn validate_existing_private_dir_meta(dir: &Path) -> Result<Metadata, OperatorCliError> {
    validate_existing_private_dir_meta_shared(dir).map_err(map_forecast_artifact_error)
}

fn validate_readonly_sqlite_file(
    file_path: &Path,
    parent_meta: &Metadata,
    max_bytes: u64,
) -> Result<(), OperatorCliError> {
    let pre_meta = fs::symlink_metadata(file_path).map_err(|err| {
        if err.kind() == io::ErrorKind::NotFound {
            OperatorCliError::new("not_found", "Required SQLite state database was not found.")
        } else {
            OperatorCliError::new("io_error", "Failed to inspect SQLite database metadata.")
        }
    })?;
    verify_readonly_sqlite_metadata(&pre_meta, parent_meta, max_bytes)?;

    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }

    let file = options.open(file_path).map_err(map_nofollow_open_error)?;
    let post_meta = file
        .metadata()
        .map_err(|_| OperatorCliError::new("io_error", "Failed to stat opened SQLite database."))?;
    verify_readonly_sqlite_metadata(&post_meta, parent_meta, max_bytes)?;
    verify_same_file_identity(&pre_meta, &post_meta)?;
    Ok(())
}

fn verify_readonly_sqlite_metadata(
    meta: &Metadata,
    parent_meta: &Metadata,
    max_bytes: u64,
) -> Result<(), OperatorCliError> {
    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
        return Err(OperatorCliError::new(
            "security_violation",
            "SQLite state file must be a non-symlink regular file.",
        ));
    }
    if meta.len() > max_bytes {
        return Err(OperatorCliError::new(
            "output_overflow",
            "SQLite state file exceeds the bounded read size limit.",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if (meta.permissions().mode() & 0o133) != 0 {
            return Err(OperatorCliError::new(
                "security_violation",
                "SQLite state file must not be group/world-writable or executable.",
            ));
        }
        if meta.uid() != parent_meta.uid() {
            return Err(OperatorCliError::new(
                "security_violation",
                "SQLite state file owner does not match parent directory owner.",
            ));
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = parent_meta;
        Err(OperatorCliError::new(
            "unsupported_platform",
            "Trusted local forecast inspection requires Unix private permissions.",
        ))
    }
}

fn verify_same_file_identity(pre: &Metadata, post: &Metadata) -> Result<(), OperatorCliError> {
    verify_same_file_identity_shared(pre, post).map_err(map_forecast_artifact_error)
}

fn map_nofollow_open_error(err: io::Error) -> OperatorCliError {
    map_forecast_artifact_error(map_artifact_nofollow_open_error(err))
}

fn build_sqlite_readonly_immutable_uri(database_path: &Path) -> Result<String, OperatorCliError> {
    if !database_path.is_absolute() {
        return Err(OperatorCliError::new(
            "security_violation",
            "SQLite database path must be absolute.",
        ));
    }

    #[cfg(unix)]
    let raw_bytes: &[u8] = {
        use std::os::unix::ffi::OsStrExt;
        database_path.as_os_str().as_bytes()
    };
    #[cfg(not(unix))]
    let raw_bytes: &[u8] = database_path
        .to_str()
        .ok_or_else(|| {
            OperatorCliError::new(
                "security_violation",
                "SQLite database path must be valid UTF-8.",
            )
        })?
        .as_bytes();

    if raw_bytes.is_empty() || raw_bytes.contains(&0) {
        return Err(OperatorCliError::new(
            "security_violation",
            "SQLite database path must not be empty or contain NUL bytes.",
        ));
    }

    const HEX_UPPER: &[u8; 16] = b"0123456789ABCDEF";
    let mut uri = String::with_capacity(raw_bytes.len() + 32);
    uri.push_str("file://");
    if raw_bytes[0] != b'/' {
        uri.push('/');
    }
    for &byte in raw_bytes {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.' | b'~') {
            uri.push(char::from(byte));
        } else {
            uri.push('%');
            uri.push(char::from(HEX_UPPER[usize::from(byte >> 4)]));
            uri.push(char::from(HEX_UPPER[usize::from(byte & 0x0f)]));
        }
    }
    uri.push_str("?mode=ro&immutable=1");
    Ok(uri)
}

fn parse_backlog_kind(raw: &str) -> Option<BacklogItemKind> {
    match raw {
        "worker" => Some(BacklogItemKind::Worker),
        "coworker" => Some(BacklogItemKind::Coworker),
        _ => None,
    }
}

fn parse_backlog_status(raw: &str) -> Option<BacklogStatus> {
    match raw {
        "todo" => Some(BacklogStatus::Todo),
        "doing" => Some(BacklogStatus::Doing),
        "done" => Some(BacklogStatus::Done),
        "blocked" => Some(BacklogStatus::Blocked),
        _ => None,
    }
}

fn parse_worker_backlog_outcome(raw: &str) -> Option<WorkerBacklogOutcome> {
    match raw {
        "success" => Some(WorkerBacklogOutcome::Success),
        "failure" => Some(WorkerBacklogOutcome::Failure),
        "unknown" => Some(WorkerBacklogOutcome::Unknown),
        _ => None,
    }
}

fn parse_worker_status(raw: &str) -> Option<WorkerStatus> {
    match raw {
        "queued" => Some(WorkerStatus::Queued),
        "claimed" => Some(WorkerStatus::Claimed),
        "running" => Some(WorkerStatus::Running),
        "cancel_requested" => Some(WorkerStatus::CancelRequested),
        "completed" => Some(WorkerStatus::Completed),
        "failed" => Some(WorkerStatus::Failed),
        "cancelled" => Some(WorkerStatus::Cancelled),
        "timed_out" => Some(WorkerStatus::TimedOut),
        "unknown" => Some(WorkerStatus::Unknown),
        _ => None,
    }
}

fn sqlite_read_error() -> OperatorCliError {
    OperatorCliError::new(
        "corrupt_state",
        "Failed to read scoped SQLite state in strict read-only mode.",
    )
}

fn corrupt_control_state() -> OperatorCliError {
    OperatorCliError::new(
        "corrupt_state",
        "Control-plane backlog or execution state failed integrity validation.",
    )
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::Write;
    use std::os::unix::fs::{symlink, DirBuilderExt, PermissionsExt};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use serde_json::json;
    use tempfile::TempDir;
    use threadlane_authority::{OperatorApprovalRequest, APPROVALS_LOCK_FILE_NAME};
    use threadlane_core::{
        Command, CommandOutput, Core, NewBacklogItem, WorkerAcceptanceState,
        WorkerBacklogAcceptanceRejection, WorkerBacklogCompletion, WorkerBacklogOutcomeReport,
        WorkerCompletionRecord, WorkerOutcomeSummary, WorkerSnapshot, WorkerWorktreeUpdate,
    };
    use threadlane_host::{
        prepare_session_dir, record_task_receipt, write_host_manifest, HostPhase,
        HostSessionManifest, TaskRunReceipt, TASK_RECEIPTS_LOCK_FILENAME,
    };
    use threadlane_native::open_workflow_stores;
    use threadlane_workers::{
        resolve_exact_commit_sha, run_sanitized_git, ExecutionBudgets,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    };
    use threadlane_workflow::{
        compile_plan_revision, ModelChoice, ModelDefinition, PlanChange, PlanTask, RegistryChange,
        RoleAssignment, ToolAdapter, ToolDefinition, WorkflowProfile, WorkflowRole,
    };

    use super::*;

    const THREAD_A: &str = "0196ed00-0000-7000-8000-0000000000aa";
    const THREAD_B: &str = "0196ed00-0000-7000-8000-0000000000bb";
    const SUMMARY_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const REQUEST_SUMMARY_SHA: &str =
        "6666666666666666666666666666666666666666666666666666666666666666";
    const CACHE_KEY: &str = "2222222222222222222222222222222222222222222222222222222222222222";
    const RESULT_DIGEST_A: &str =
        "3333333333333333333333333333333333333333333333333333333333333333";
    const DELIVERY_DIGEST_A: &str =
        "4444444444444444444444444444444444444444444444444444444444444444";
    const PROFILE_DIGEST_A: &str =
        "5555555555555555555555555555555555555555555555555555555555555555";

    struct ForecastTestFixture {
        _tmp: TempDir,
        state_dir: PathBuf,
        plans_dir: PathBuf,
        authority_dir: PathBuf,
        sessions_dir: PathBuf,
        repo_dir: PathBuf,
        base_commit: String,
        binding_id: String,
        foreign_binding_id: String,
        registry: Arc<threadlane_workflow::RegistryStore>,
        plans: Arc<threadlane_workflow::PlanStore>,
        authority: AuthorityStore,
        registry_revision: u64,
    }

    fn create_private_dir(path: &Path) {
        match fs::DirBuilder::new().mode(0o700).create(path) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {}
            Err(err) => panic!("create private dir {}: {err}", path.display()),
        }
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
    }

    fn write_private_json(path: &Path, value: &serde_json::Value) {
        if let Some(parent) = path.parent() {
            create_private_dir(parent);
        }
        let mut bytes = serde_json::to_vec_pretty(value).expect("serialize json");
        bytes.push(b'\n');
        fs::write(path, bytes).expect("write json");
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("chmod 0600");
    }

    fn create_fake_executable(path: &Path) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create bin parent");
        }
        let mut file = File::create(path).expect("create fake exe");
        file.write_all(b"#!/bin/sh\nexit 0\n")
            .expect("write fake exe");
        file.sync_all().expect("sync fake exe");
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod 0755");
    }

    impl ForecastTestFixture {
        fn new() -> Self {
            let tmp = TempDir::new().expect("tempdir");
            let root = fs::canonicalize(tmp.path()).expect("canonicalize tempdir");
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("chmod root");

            let state_dir = root.join("state");
            let registry_dir = root.join("registry");
            let plans_dir = root.join("plans");
            let authority_dir = root.join("authority");
            let sessions_dir = root.join("sessions");
            for dir in [&state_dir, &sessions_dir] {
                create_private_dir(dir);
            }

            let repo_dir = root.join("repo");
            fs::create_dir_all(repo_dir.join("src")).expect("create repo src");
            fs::write(repo_dir.join("src/lib.rs"), "pub fn v() -> u32 { 1 }\n")
                .expect("write src/lib.rs");
            run_sanitized_git(
                &repo_dir,
                &["init", "--quiet"],
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )
            .expect("git init");
            run_sanitized_git(
                &repo_dir,
                &["config", "user.email", "test@threadlane.local"],
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )
            .expect("git email");
            run_sanitized_git(
                &repo_dir,
                &["config", "user.name", "Threadlane Test"],
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )
            .expect("git name");
            run_sanitized_git(&repo_dir, &["add", "."], DEFAULT_GIT_COMMAND_TIMEOUT_MS)
                .expect("git add");
            run_sanitized_git(
                &repo_dir,
                &["commit", "-m", "init", "--quiet"],
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )
            .expect("git commit");
            let base_commit =
                resolve_exact_commit_sha(&repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
                    .expect("resolve HEAD");

            let fake_exe = root.join("bin/fake-oc");
            create_fake_executable(&fake_exe);

            let (registry, plans) = open_workflow_stores(Some(&registry_dir), Some(&plans_dir))
                .expect("open workflow stores")
                .expect("stores present");

            let s1 = registry
                .apply(
                    0,
                    RegistryChange::UpsertTool {
                        tool: ToolDefinition {
                            id: "tool-oc".to_string(),
                            label: "OpenCode".to_string(),
                            adapter: ToolAdapter::OpencodeV2,
                            executable_path: Some(fake_exe),
                            enabled: true,
                        },
                    },
                )
                .expect("upsert tool");
            let s2 = registry
                .apply(
                    s1.revision,
                    RegistryChange::UpsertModel {
                        model: ModelDefinition {
                            id: "model-oc".to_string(),
                            tool_id: "tool-oc".to_string(),
                            label: "Worker Model".to_string(),
                            model_arg: "opencode-go/test-worker".to_string(),
                            efforts: vec!["max".to_string()],
                            allow_no_effort: false,
                        },
                    },
                )
                .expect("upsert model");
            let reg_snap = registry
                .apply(
                    s2.revision,
                    RegistryChange::UpsertProfile {
                        profile: WorkflowProfile {
                            decision_advisor: None,
                            id: "profile-main".to_string(),
                            name: "Main Profile".to_string(),
                            assignments: vec![RoleAssignment {
                                role: WorkflowRole::Coder,
                                difficulty: None,
                                choice: ModelChoice {
                                    model_id: "model-oc".to_string(),
                                    effort: Some("max".to_string()),
                                },
                            }],
                        },
                    },
                )
                .expect("upsert profile");

            let authority =
                AuthorityStore::open(&authority_dir, Arc::clone(&registry), Arc::clone(&plans))
                    .expect("open authority");

            let mut core = Core::open(state_dir.join(DATABASE_FILENAME)).expect("open control db");
            let binding_a = core
                .create_binding(THREAD_A, "Primary Chat")
                .expect("create binding A");
            let binding_b = core
                .create_binding(THREAD_B, "Foreign Chat")
                .expect("create binding B");
            drop(core);

            Self {
                _tmp: tmp,
                state_dir,
                plans_dir,
                authority_dir,
                sessions_dir,
                repo_dir,
                base_commit,
                binding_id: binding_a.binding_id,
                foreign_binding_id: binding_b.binding_id,
                registry,
                plans,
                authority,
                registry_revision: reg_snap.revision,
            }
        }

        fn approve_single_task(
            &self,
            binding_id: &str,
            thread_uuid: &str,
            plan_id: &str,
            task_id: &str,
            difficulty: Difficulty,
        ) -> ApprovalRecord {
            let reg_snap = self.registry.snapshot().expect("registry snapshot");
            let draft = self
                .plans
                .change(
                    0,
                    PlanChange::SaveDraft {
                        plan_id: plan_id.to_string(),
                        binding_id: binding_id.to_string(),
                        profile_id: "profile-main".to_string(),
                        profile_revision: reg_snap.revision,
                        summary: format!("Plan {plan_id}"),
                        tasks: vec![PlanTask {
                            id: task_id.to_string(),
                            title: format!("Task {task_id}"),
                            instruction: "Implement scoped unit update cleanly.".to_string(),
                            role: WorkflowRole::Coder,
                            difficulty,
                            depends_on: Vec::new(),
                            allowed_write_paths: vec!["src/lib.rs".to_string()],
                        }],
                    },
                )
                .expect("save draft");
            let published = self
                .plans
                .change(
                    draft.revision,
                    PlanChange::Publish {
                        plan_id: plan_id.to_string(),
                        revision: draft.revision,
                    },
                )
                .expect("publish plan");
            let compiled =
                compile_plan_revision(&published, &reg_snap).expect("compile published plan");

            let summary = self
                .authority
                .approve(OperatorApprovalRequest {
                    binding_id: binding_id.to_string(),
                    thread_uuid: thread_uuid.to_string(),
                    plan_id: published.plan_id,
                    plan_revision: published.revision,
                    expected_registry_revision: reg_snap.revision,
                    expected_plan_digest: compiled.plan_digest,
                    repository_path: self.repo_dir.clone(),
                    base_commit: self.base_commit.clone(),
                    approved_task_ids: vec![task_id.to_string()],
                    approved_write_paths: vec!["src/lib.rs".to_string()],
                    max_parallel: 1,
                    budgets: ExecutionBudgets::default(),
                })
                .expect("approve plan");
            self.authority
                .get_for_operator(&summary.approval_id)
                .expect("load full approval")
        }

        fn create_session_worker(
            &self,
            approval: &ApprovalRecord,
            started_at_ms: Option<i64>,
            completed_at_ms: Option<i64>,
            terminal_status: Option<WorkerStatus>,
        ) -> (String, WorkerSnapshot) {
            let task = &approval.tasks[0];
            let worker_profile_id = task.profile_id.clone().expect("headless worker profile_id");
            let command_key = task
                .command_key
                .clone()
                .expect("headless worker command_key");

            let session_dir =
                prepare_session_dir(&self.sessions_dir, &approval.approval_id).expect("prep dir");
            let runtime_dir = session_dir.join(RUNTIME_STATE_SUBDIR);
            create_private_dir(&runtime_dir);

            let mut session_core =
                Core::open(runtime_dir.join(DATABASE_FILENAME)).expect("open session db");
            let session_binding = session_core
                .create_binding(&approval.thread_uuid, "Session")
                .expect("create session binding");

            let started_out = session_core
                .execute(
                    &session_binding.binding_id,
                    Command::StartWorker {
                        command_key: command_key.clone(),
                        profile_id: worker_profile_id.clone(),
                        profile_digest: PROFILE_DIGEST_A.to_string(),
                        provider: "opencode_v2".to_string(),
                        requested_model: task.base_model.clone(),
                        requested_effort: task.effort.clone().unwrap_or_default(),
                        base_commit: approval.base_commit.clone(),
                        execution_binding: None,
                        instruction: task.instruction.clone(),
                        now_ms: 1_700_000_000_000,
                    },
                )
                .expect("start worker");
            let mut snapshot = match started_out {
                CommandOutput::Worker(worker) => *worker,
                other => panic!("expected Worker output, got {other:?}"),
            };

            if let Some(start_ts) = started_at_ms {
                let claim = session_core
                    .claim_worker(
                        &session_binding.binding_id,
                        &snapshot.run_id,
                        snapshot.revision,
                        PROFILE_DIGEST_A,
                        start_ts - 10,
                    )
                    .expect("claim worker");
                snapshot = session_core
                    .record_worker_running(
                        &claim,
                        WorkerWorktreeUpdate {
                            run_worktree_id: "wt-run-1".to_string(),
                            run_worktree_path: "/tmp/wt-run".to_string(),
                            run_branch_name: "threadlane/run-1".to_string(),
                            task_worktree_id: "wt-task-1".to_string(),
                            task_worktree_path: "/tmp/wt-task".to_string(),
                            task_branch_name: "threadlane/task-1".to_string(),
                            base_commit: approval.base_commit.clone(),
                        },
                        start_ts,
                    )
                    .expect("mark worker running");

                if let (Some(done_ts), Some(status)) = (completed_at_ms, terminal_status) {
                    let duration_ms = u64::try_from(done_ts - start_ts).ok();
                    let acceptance_state = if status == WorkerStatus::Completed {
                        WorkerAcceptanceState::ReadyForReview
                    } else {
                        WorkerAcceptanceState::ExecutionFailed
                    };
                    snapshot = session_core
                        .record_worker_outcome(
                            &claim,
                            WorkerCompletionRecord {
                                status,
                                observed_model: Some(task.base_model.clone()),
                                observed_effort: task.effort.clone(),
                                head_commit: Some(approval.base_commit.clone()),
                                dropped_live_events: 0,
                                outcome: WorkerOutcomeSummary {
                                    evidence_failure: None,
                                    provider_retry_high_water: None,
                                    provider_health: None,
                                    exit_code: Some(if status == WorkerStatus::Completed {
                                        0
                                    } else {
                                        1
                                    }),
                                    signal: None,
                                    duration_ms,
                                    timed_out: status == WorkerStatus::TimedOut,
                                    cancelled: status == WorkerStatus::Cancelled,
                                    cancel_requested_before_completion: false,
                                    missing_model_evidence: false,
                                    missing_effort_evidence: false,
                                    provider_error: status == WorkerStatus::Failed,
                                    incomplete_cleanup: false,
                                    rejected_by_diff_gate: false,
                                    stdout_truncated: false,
                                    stderr_truncated: false,
                                    acceptance_state,
                                    changed_paths_count: 1,
                                    changed_paths_preview: vec!["src/lib.rs".to_string()],
                                    violation_count: 0,
                                    violation_summaries: Vec::new(),
                                    final_summary: Some("done".to_string()),
                                    failure_reason: None,
                                    report_artifact_id: None,
                                },
                            },
                            done_ts,
                        )
                        .expect("record worker outcome");
                }
            }
            drop(session_core);

            let manifest = HostSessionManifest::from_approval_record(
                approval,
                Some(session_binding.binding_id.clone()),
                HostPhase::Stopped,
                None,
                None,
                1_700_000_000_000,
                1_700_000_020_000,
            );
            write_host_manifest(&session_dir, &manifest).expect("write manifest");

            record_task_receipt(
                &session_dir,
                approval,
                &session_binding.binding_id,
                TaskRunReceipt {
                    task_id: task.task_id.clone(),
                    task_digest: task.task_digest.clone(),
                    profile_id: worker_profile_id,
                    command_key,
                    run_id: snapshot.run_id.clone(),
                    worker_id: snapshot.worker_id.clone(),
                    execution_binding: None,
                    recorded_at_ms: 1_700_000_000_050,
                },
            )
            .expect("record task receipt");

            (session_binding.binding_id, snapshot)
        }

        fn ensure_forecast_subject_dir(&self, subject_digest: &str) -> PathBuf {
            let forecasts_root = self.plans_dir.join(FORECAST_SUBDIR);
            create_private_dir(&forecasts_root);
            let subject_dir = forecasts_root.join(subject_digest);
            create_private_dir(&subject_dir);
            subject_dir
        }

        fn write_forecast_record(
            &self,
            approval: &ApprovalRecord,
            component_count: u8,
            coupled: bool,
            uncertain: bool,
            status: &str,
        ) {
            let reason = match status {
                "skipped" => "simple_task",
                "launched" => "shadow_classify_launched",
                "unavailable" => "advisor_not_configured",
                "unknown" => "advice_spawn_failed",
                other => other,
            };
            self.write_forecast_record_with(
                approval,
                component_count,
                coupled,
                uncertain,
                status,
                reason,
                matches!(status, "launched" | "unknown"),
            );
        }

        #[allow(clippy::too_many_arguments)]
        fn write_forecast_record_with(
            &self,
            approval: &ApprovalRecord,
            component_count: u8,
            coupled: bool,
            uncertain: bool,
            status: &str,
            reason: &str,
            include_advisor: bool,
        ) {
            let task = &approval.tasks[0];
            let triggered = uncertain || coupled || component_count > 1;
            let mut obj = json!({
                "schema_version": 1,
                "advisory_only": true,
                "mode": "shadow",
                "binding_id": approval.binding_id,
                "thread_uuid": approval.thread_uuid,
                "plan_id": approval.plan_id,
                "plan_revision": approval.plan_revision,
                "task_id": task.task_id,
                "profile_id": approval.profile_id,
                "profile_revision": approval.registry_revision,
                "base_commit": approval.base_commit,
                "subject_digest": approval.subject_digest,
                "baseline_difficulty": task.difficulty,
                "complexity": {
                    "component_count": component_count,
                    "coupled": coupled,
                    "uncertain": uncertain
                },
                "summary_sha256": SUMMARY_SHA,
                "triggered": triggered,
                "status": status,
                "reason": reason,
                "recorded_at_ms": 1_700_000_000_000u64
            });
            if include_advisor {
                obj["advisor"] = json!({
                    "tool_id": "jev-api",
                    "model_id": "jev-api:jev-latest",
                    "model_arg": "jev-latest",
                    "adapter": "jev_api"
                });
            }
            if status == "launched" {
                obj["request_summary_sha256"] = json!(REQUEST_SUMMARY_SHA);
                obj["request_file"] = json!("request.json");
                obj["advice_file"] = json!("advice.json");
                obj["pid_hint"] = json!(4242u32);
            }
            let path = self
                .ensure_forecast_subject_dir(&approval.subject_digest)
                .join(FORECAST_RECORD_FILENAME);
            write_private_json(&path, &obj);
        }

        fn write_advice_file(
            &self,
            approval: &ApprovalRecord,
            provider_difficulty: Option<Difficulty>,
            confidence: Option<f64>,
        ) {
            let task = &approval.tasks[0];
            let obj = json!({
                "untrusted_data": true,
                "advisory_only": true,
                "mode": "shadow",
                "source": "provider",
                "context_id": approval.thread_uuid,
                "task_id": task.task_id,
                "task_revision": approval.plan_revision,
                "registry_revision": self.registry_revision,
                "profile_id": approval.profile_id,
                "tool_id": "jev-api",
                "model_id": "jev-api:jev-latest",
                "requested_model": "jev-latest",
                "summary_sha256": REQUEST_SUMMARY_SHA,
                "cache_key": CACHE_KEY,
                "advice": {
                    "provider": "jev",
                    "model": "jev-1.14.0",
                    "difficulty": provider_difficulty,
                    "confidence": confidence,
                    "needs_review": provider_difficulty.is_none(),
                    "reason_code": if provider_difficulty.is_some() { "classified" } else { "refusal" },
                    "input_tokens": 120,
                    "output_tokens": 16
                },
                "baseline_difficulty": null,
                "agrees_with_baseline": null,
                "provider_call_performed": true,
                "cache_age_ms": null,
                "elapsed_ms": 340
            });
            let path = self
                .ensure_forecast_subject_dir(&approval.subject_digest)
                .join(FORECAST_ADVICE_FILENAME);
            write_private_json(&path, &obj);
        }

        fn scope<'a>(
            &'a self,
            binding_id: &'a str,
            thread_uuid: &'a str,
            item_id: &'a str,
        ) -> ForecastReportScope<'a> {
            ForecastReportScope {
                state_dir: &self.state_dir,
                plans_dir: &self.plans_dir,
                authority_dir: &self.authority_dir,
                sessions_dir: &self.sessions_dir,
                binding_id,
                thread_uuid,
                item_id,
            }
        }
    }

    #[test]
    fn positive_forecast_report_verifies_all_identities_and_stays_strictly_readonly() {
        let fx = ForecastTestFixture::new();
        let approval = fx.approve_single_task(
            &fx.binding_id,
            THREAD_A,
            "plan-pos",
            "task-pos",
            Difficulty::Medium,
        );
        let task = &approval.tasks[0];
        let (session_binding_id, worker) = fx.create_session_worker(
            &approval,
            Some(1_700_000_001_000),
            Some(1_700_000_008_500),
            Some(WorkerStatus::Completed),
        );
        fx.write_forecast_record(&approval, 3, true, false, "launched");
        fx.write_advice_file(&approval, Some(Difficulty::Medium), Some(0.92));

        let mut core = Core::open(fx.state_dir.join(DATABASE_FILENAME)).expect("open core");
        let item = core
            .create_backlog_item(
                &fx.binding_id,
                "cmd-create-pos",
                NewBacklogItem {
                    title: "Update lib".to_string(),
                    acceptance_condition: "Tests pass".to_string(),
                    delivery_description: "Updated src/lib.rs".to_string(),
                    kind: BacklogItemKind::Worker,
                },
                1_700_000_000_000,
            )
            .expect("create item");
        let exec = core
            .reserve_worker_backlog_execution_operator_attested(
                &fx.binding_id,
                "cmd-res-pos",
                &item.item_id,
                item.revision,
                WorkerBacklogAuthority {
                    approval_id: approval.approval_id.clone(),
                    subject_digest: approval.subject_digest.clone(),
                    task_id: task.task_id.clone(),
                    task_digest: task.task_digest.clone(),
                },
                1_700_000_000_100,
            )
            .expect("reserve");
        let rev = core
            .get_backlog_item(&fx.binding_id, &item.item_id)
            .expect("get item")
            .revision;
        core.attach_worker_backlog_dispatch_receipt_operator_attested(
            &fx.binding_id,
            "cmd-att-pos",
            &item.item_id,
            &exec.execution_ref,
            rev,
            WorkerBacklogDispatchReceipt {
                session_binding_id: session_binding_id.clone(),
                run_id: worker.run_id.clone(),
                worker_id: worker.worker_id.clone(),
                profile_id: task.profile_id.clone().expect("worker profile"),
                execution_binding_digest: None,
            },
            1_700_000_000_200,
        )
        .expect("attach");
        let rev = core
            .get_backlog_item(&fx.binding_id, &item.item_id)
            .expect("get item")
            .revision;
        core.record_worker_backlog_outcome_operator_attested(
            &fx.binding_id,
            "cmd-out-pos",
            &item.item_id,
            &exec.execution_ref,
            rev,
            WorkerBacklogOutcomeReport {
                outcome: WorkerBacklogOutcome::Success,
                note: "verified terminal worker success".to_string(),
                result_digest: Some(RESULT_DIGEST_A.to_string()),
            },
            1_700_000_009_000,
        )
        .expect("outcome");
        let rev = core
            .get_backlog_item(&fx.binding_id, &item.item_id)
            .expect("get item")
            .revision;
        core.complete_worker_backlog_item_operator_attested(
            &fx.binding_id,
            "cmd-done-pos",
            &item.item_id,
            rev,
            WorkerBacklogCompletion {
                execution_ref: exec.execution_ref.clone(),
                result_digest: RESULT_DIGEST_A.to_string(),
                acceptance_note: "verified".to_string(),
                acceptance_evidence: "unit check".to_string(),
                delivery_digest: DELIVERY_DIGEST_A.to_string(),
            },
            1_700_000_010_000,
        )
        .expect("complete");
        drop(core);

        // Remove lock files created during fixture setup to prove strictly read-only execution.
        let authority_lock = fx.authority_dir.join(APPROVALS_LOCK_FILE_NAME);
        let session_dir = fx.sessions_dir.join(&approval.approval_id);
        let receipts_lock = session_dir.join(TASK_RECEIPTS_LOCK_FILENAME);
        let _ = fs::remove_file(&authority_lock);
        let _ = fs::remove_file(&receipts_lock);

        let report =
            build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &item.item_id)).expect("ok");

        assert_eq!(report.binding_id, fx.binding_id);
        assert_eq!(report.thread_uuid, THREAD_A);
        assert_eq!(report.item_id, item.item_id);
        assert_eq!(report.execution_ref, exec.execution_ref);
        assert_eq!(report.approval_id, approval.approval_id);
        assert_eq!(report.subject_digest, approval.subject_digest);
        assert_eq!(report.task_id, "task-pos");
        assert_eq!(report.profile_id, "profile-main");
        assert_eq!(
            report.session_binding_id.as_deref(),
            Some(session_binding_id.as_str())
        );
        assert_eq!(report.run_id.as_deref(), Some(worker.run_id.as_str()));
        assert_eq!(report.worker_id.as_deref(), Some(worker.worker_id.as_str()));
        assert_eq!(
            report.initial_coworker.record_status,
            ForecastRecordStatus::Present
        );
        assert_eq!(report.initial_coworker.difficulty, Difficulty::Medium);
        assert_eq!(
            report.initial_coworker.complexity,
            Some(ForecastComplexity {
                component_count: 3,
                coupled: true,
                uncertain: false,
            })
        );
        assert_eq!(
            report.provider_advice.status,
            ProviderAdviceStatus::Available
        );
        assert_eq!(report.provider_advice.difficulty, Some(Difficulty::Medium));
        assert_eq!(report.provider_advice.confidence, Some(0.92));
        assert_eq!(report.agreement, ForecastAgreement::Agree);
        assert_eq!(report.execution.item_status, BacklogStatus::Done);
        assert_eq!(
            report.execution.worker_result_status,
            WorkerResultStatus::Terminal
        );
        assert_eq!(
            report.execution.worker_status,
            Some(WorkerStatus::Completed)
        );
        assert_eq!(report.execution.attempt_count, Some(1));
        assert_eq!(report.execution.elapsed_worker_ms, Some(7_500));
        assert!(report.execution.guarded_done);
        assert!(!report.execution.guarded_acceptance_rejected);
        assert_eq!(report.correction_evidence.item_execution_count, 1);
        assert_eq!(report.correction_evidence.acceptance_rejection_count, 0);
        assert_eq!(report.correction_evidence.retry_count, 0);
        assert_eq!(report.correction_evidence.failed_execution_count, 0);
        assert_eq!(
            report.correction_evidence.status,
            CorrectionEvidenceStatus::Unknown
        );
        assert_eq!(
            report.correction_evidence.cross_item_status,
            CrossItemCorrectionStatus::Unknown
        );

        // Verify strictly read-only: no lock files or SQLite sidecars created.
        assert!(!authority_lock.exists());
        assert!(!receipts_lock.exists());
        assert!(!fx.state_dir.join("state.db-wal").exists());
        assert!(!session_dir
            .join(RUNTIME_STATE_SUBDIR)
            .join("state.db-wal")
            .exists());

        // Serialized report never leaks raw instructions or filesystem paths.
        let serialized = serde_json::to_string(&report).expect("serialize report");
        assert!(!serialized.contains("Implement scoped unit update"));
        assert!(!serialized.contains(&fx.state_dir.display().to_string()));
    }

    #[test]
    fn foreign_scope_and_symlink_artifacts_fail_closed() {
        let fx = ForecastTestFixture::new();
        let approval = fx.approve_single_task(
            &fx.binding_id,
            THREAD_A,
            "plan-sec",
            "task-sec",
            Difficulty::Easy,
        );
        let task = &approval.tasks[0];
        let (session_binding_id, worker) = fx.create_session_worker(
            &approval,
            Some(1_700_000_001_000),
            Some(1_700_000_002_000),
            Some(WorkerStatus::Completed),
        );
        fx.write_forecast_record(&approval, 2, false, false, "launched");
        fx.write_advice_file(&approval, Some(Difficulty::Hard), Some(0.81));

        let mut core = Core::open(fx.state_dir.join(DATABASE_FILENAME)).expect("open core");
        let item = core
            .create_backlog_item(
                &fx.binding_id,
                "cmd-create-sec",
                NewBacklogItem {
                    title: "Scoped item".to_string(),
                    acceptance_condition: "Pass".to_string(),
                    delivery_description: "File".to_string(),
                    kind: BacklogItemKind::Worker,
                },
                1_700_000_000_000,
            )
            .expect("create item");
        let exec = core
            .reserve_worker_backlog_execution_operator_attested(
                &fx.binding_id,
                "cmd-res-sec",
                &item.item_id,
                item.revision,
                WorkerBacklogAuthority {
                    approval_id: approval.approval_id.clone(),
                    subject_digest: approval.subject_digest.clone(),
                    task_id: task.task_id.clone(),
                    task_digest: task.task_digest.clone(),
                },
                1_700_000_000_100,
            )
            .expect("reserve");
        let rev = core
            .get_backlog_item(&fx.binding_id, &item.item_id)
            .expect("get item")
            .revision;
        core.attach_worker_backlog_dispatch_receipt_operator_attested(
            &fx.binding_id,
            "cmd-att-sec",
            &item.item_id,
            &exec.execution_ref,
            rev,
            WorkerBacklogDispatchReceipt {
                session_binding_id,
                run_id: worker.run_id,
                worker_id: worker.worker_id,
                profile_id: task.profile_id.clone().expect("worker profile"),
                execution_binding_digest: None,
            },
            1_700_000_000_200,
        )
        .expect("attach");
        drop(core);

        // Omitted baseline_difficulty derives disagreement locally against frozen task difficulty.
        let omitted_disagree =
            build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &item.item_id))
                .expect("omitted baseline_difficulty must derive disagreement locally");
        assert_eq!(omitted_disagree.agreement, ForecastAgreement::Disagree);

        // 1. Foreign binding querying binding A's item must fail with scope_mismatch.
        let err = build_forecast_report(fx.scope(&fx.foreign_binding_id, THREAD_B, &item.item_id))
            .expect_err("foreign binding must be rejected");
        assert_eq!(err.code, "scope_mismatch");

        // 2. Wrong thread for binding A must fail with scope_mismatch.
        let err = build_forecast_report(fx.scope(&fx.binding_id, THREAD_B, &item.item_id))
            .expect_err("wrong thread must be rejected");
        assert_eq!(err.code, "scope_mismatch");

        // 3. Tampered foreign context_id, raw summary_sha256, or wrong advisor in advice.json must fail closed.
        let advice_path = fx
            .plans_dir
            .join(FORECAST_SUBDIR)
            .join(&approval.subject_digest)
            .join(FORECAST_ADVICE_FILENAME);
        let original_advice = fs::read(&advice_path).expect("read advice");
        let mut bad_advice: serde_json::Value =
            serde_json::from_slice(&original_advice).expect("parse");
        bad_advice["context_id"] = json!(THREAD_B);
        write_private_json(&advice_path, &bad_advice);
        let err = build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &item.item_id))
            .expect_err("foreign advice context_id must be rejected");
        assert_eq!(err.code, "scope_mismatch");

        // Raw summary_sha256 (instead of request_summary_sha256) must fail closed.
        let mut raw_sha_advice: serde_json::Value =
            serde_json::from_slice(&original_advice).expect("parse");
        raw_sha_advice["summary_sha256"] = json!(SUMMARY_SHA);
        write_private_json(&advice_path, &raw_sha_advice);
        let err = build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &item.item_id))
            .expect_err("advice using raw summary_sha256 instead of request digest must fail");
        assert_eq!(err.code, "scope_mismatch");

        // Mismatched chosen advisor tool/model in advice.json must fail closed.
        let mut wrong_advisor_advice: serde_json::Value =
            serde_json::from_slice(&original_advice).expect("parse");
        wrong_advisor_advice["model_id"] = json!("jev-api:jev-1.14.0");
        write_private_json(&advice_path, &wrong_advisor_advice);
        let err = build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &item.item_id))
            .expect_err("advice with mismatched advisor model_id must be rejected");
        assert_eq!(err.code, "scope_mismatch");

        // Omitted baseline_difficulty with non-null agrees_with_baseline must fail closed.
        let mut stray_agrees_advice: serde_json::Value =
            serde_json::from_slice(&original_advice).expect("parse");
        stray_agrees_advice["agrees_with_baseline"] = json!(false);
        write_private_json(&advice_path, &stray_agrees_advice);
        let err = build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &item.item_id))
            .expect_err("advice with agrees_with_baseline but omitted baseline must be rejected");
        assert_eq!(err.code, "invalid_advice");

        // Invalid supplied legacy baseline_difficulty in advice.json must fail closed.
        let mut wrong_baseline_advice: serde_json::Value =
            serde_json::from_slice(&original_advice).expect("parse");
        wrong_baseline_advice["baseline_difficulty"] = json!("hard");
        wrong_baseline_advice["agrees_with_baseline"] = json!(true);
        write_private_json(&advice_path, &wrong_baseline_advice);
        let err = build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &item.item_id))
            .expect_err("advice with mismatched supplied baseline_difficulty must be rejected");
        assert_eq!(err.code, "scope_mismatch");

        // Valid supplied legacy baseline_difficulty with matching agreement succeeds and derives disagreement locally.
        let mut legacy_baseline_advice: serde_json::Value =
            serde_json::from_slice(&original_advice).expect("parse");
        legacy_baseline_advice["baseline_difficulty"] = json!("easy");
        legacy_baseline_advice["agrees_with_baseline"] = json!(false);
        write_private_json(&advice_path, &legacy_baseline_advice);
        let legacy_report =
            build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &item.item_id))
                .expect("valid legacy supplied baseline_difficulty must be accepted");
        assert_eq!(legacy_report.agreement, ForecastAgreement::Disagree);

        // Legacy supplied baseline_difficulty with contradictory agrees_with_baseline must fail closed.
        legacy_baseline_advice["agrees_with_baseline"] = json!(true);
        write_private_json(&advice_path, &legacy_baseline_advice);
        let err = build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &item.item_id))
            .expect_err("legacy advice with contradictory agrees_with_baseline must be rejected");
        assert_eq!(err.code, "invalid_advice");

        // 4. Symlinked advice.json must fail with security_violation.
        let target_file = fx.plans_dir.join("outside-target.json");
        write_private_json(&target_file, &bad_advice);
        fs::remove_file(&advice_path).expect("remove advice");
        symlink(&target_file, &advice_path).expect("create symlink");
        let err = build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &item.item_id))
            .expect_err("symlink advice.json must be rejected");
        assert_eq!(err.code, "security_violation");

        // 5. Foreign binding_id, mismatched profile_revision/base_commit, or bad advisory_only/mode in record.json must fail closed (even with empty advice.json).
        fs::remove_file(&advice_path).expect("remove symlink advice");
        fs::write(&advice_path, b"").expect("write empty advice");
        fs::set_permissions(&advice_path, fs::Permissions::from_mode(0o600)).expect("chmod empty");
        let record_path = fx
            .plans_dir
            .join(FORECAST_SUBDIR)
            .join(&approval.subject_digest)
            .join(FORECAST_RECORD_FILENAME);
        let original_record = fs::read(&record_path).expect("read record");
        let mut bad_record: serde_json::Value =
            serde_json::from_slice(&original_record).expect("parse record");
        bad_record["binding_id"] = json!(fx.foreign_binding_id);
        write_private_json(&record_path, &bad_record);
        let err = build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &item.item_id))
            .expect_err("foreign record.json binding_id must be rejected even with empty advice");
        assert_eq!(err.code, "scope_mismatch");

        let mut bad_rev_record: serde_json::Value =
            serde_json::from_slice(&original_record).expect("parse record");
        bad_rev_record["profile_revision"] = json!(approval.registry_revision + 1);
        write_private_json(&record_path, &bad_rev_record);
        let err = build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &item.item_id))
            .expect_err("wrong record.json profile_revision must be rejected");
        assert_eq!(err.code, "scope_mismatch");

        let mut bad_commit_record: serde_json::Value =
            serde_json::from_slice(&original_record).expect("parse record");
        bad_commit_record["base_commit"] = json!("0".repeat(40));
        write_private_json(&record_path, &bad_commit_record);
        let err = build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &item.item_id))
            .expect_err("wrong record.json base_commit must be rejected");
        assert_eq!(err.code, "scope_mismatch");

        let mut bad_mode_record: serde_json::Value =
            serde_json::from_slice(&original_record).expect("parse record");
        bad_mode_record["advisory_only"] = json!(false);
        write_private_json(&record_path, &bad_mode_record);
        let err = build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &item.item_id))
            .expect_err("non-advisory record.json must be rejected");
        assert_eq!(err.code, "invalid_forecast");
        fs::remove_file(&advice_path).expect("remove empty advice");

        // 6. Symlinked record.json must also fail with security_violation.
        let record_target = fx.plans_dir.join("record-target.json");
        fs::write(&record_target, original_record).expect("write target");
        fs::set_permissions(&record_target, fs::Permissions::from_mode(0o600)).expect("chmod");
        fs::remove_file(&record_path).expect("remove record");
        symlink(&record_target, &record_path).expect("symlink record");
        let err = build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &item.item_id))
            .expect_err("symlink record.json must be rejected");
        assert_eq!(err.code, "security_violation");
    }

    #[test]
    fn missing_results_and_separate_rejection_vs_retry_evidence() {
        let fx = ForecastTestFixture::new();

        // Part 1: Reserved but unstarted item covering skipped/unavailable/unknown/launched statuses
        // plus empty and partially written advice.json with matching launched record.
        let approval_unstarted = fx.approve_single_task(
            &fx.binding_id,
            THREAD_A,
            "plan-miss",
            "task-miss",
            Difficulty::Easy,
        );
        fx.write_forecast_record(&approval_unstarted, 1, false, false, "skipped");

        let mut core = Core::open(fx.state_dir.join(DATABASE_FILENAME)).expect("open core");
        let unstarted_item = core
            .create_backlog_item(
                &fx.binding_id,
                "cmd-create-miss",
                NewBacklogItem {
                    title: "Unstarted".to_string(),
                    acceptance_condition: "Pass".to_string(),
                    delivery_description: "Output".to_string(),
                    kind: BacklogItemKind::Worker,
                },
                1_700_000_000_000,
            )
            .expect("create unstarted");
        core.reserve_worker_backlog_execution_operator_attested(
            &fx.binding_id,
            "cmd-res-miss",
            &unstarted_item.item_id,
            unstarted_item.revision,
            WorkerBacklogAuthority {
                approval_id: approval_unstarted.approval_id.clone(),
                subject_digest: approval_unstarted.subject_digest.clone(),
                task_id: approval_unstarted.tasks[0].task_id.clone(),
                task_digest: approval_unstarted.tasks[0].task_digest.clone(),
            },
            1_700_000_000_100,
        )
        .expect("reserve unstarted");
        drop(core);

        let missing_report =
            build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &unstarted_item.item_id))
                .expect("unstarted report");
        assert_eq!(
            missing_report.provider_advice.status,
            ProviderAdviceStatus::Skipped
        );
        assert_eq!(missing_report.provider_advice.difficulty, None);
        assert_eq!(missing_report.provider_advice.confidence, None);
        assert_eq!(missing_report.agreement, ForecastAgreement::Unavailable);
        assert_eq!(
            missing_report.execution.worker_result_status,
            WorkerResultStatus::NotDispatched
        );
        assert_eq!(missing_report.execution.attempt_count, None);
        assert_eq!(missing_report.execution.elapsed_worker_ms, None);
        assert!(!missing_report.execution.guarded_done);
        assert!(!missing_report.execution.guarded_acceptance_rejected);

        // Triggered fixed_worker_route skip (with or without advisor metadata) must load cleanly.
        for include_advisor in [true, false] {
            fx.write_forecast_record_with(
                &approval_unstarted,
                2,
                true,
                false,
                "skipped",
                "fixed_worker_route",
                include_advisor,
            );
            let fixed_skip_report =
                build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &unstarted_item.item_id))
                    .expect("fixed_worker_route skipped report must succeed");
            assert_eq!(
                fixed_skip_report.initial_coworker.record_status,
                ForecastRecordStatus::Present
            );
            assert_eq!(
                fixed_skip_report.initial_coworker.complexity,
                Some(ForecastComplexity {
                    component_count: 2,
                    coupled: true,
                    uncertain: false,
                })
            );
            assert_eq!(
                fixed_skip_report.provider_advice.status,
                ProviderAdviceStatus::Skipped
            );
            assert_eq!(fixed_skip_report.agreement, ForecastAgreement::Unavailable);
        }

        // Contradictory skipped records must still fail closed.
        fx.write_forecast_record_with(
            &approval_unstarted,
            2,
            false,
            false,
            "skipped",
            "simple_task",
            false,
        );
        assert_eq!(
            build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &unstarted_item.item_id))
                .expect_err("simple_task skip with triggered=true must fail")
                .code,
            "invalid_forecast"
        );
        fx.write_forecast_record_with(
            &approval_unstarted,
            1,
            false,
            false,
            "skipped",
            "simple_task",
            true,
        );
        assert_eq!(
            build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &unstarted_item.item_id))
                .expect_err("simple_task skip with advisor metadata must fail")
                .code,
            "invalid_forecast"
        );
        fx.write_forecast_record_with(
            &approval_unstarted,
            1,
            false,
            false,
            "skipped",
            "fixed_worker_route",
            true,
        );
        assert_eq!(
            build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &unstarted_item.item_id))
                .expect_err("fixed_worker_route skip with triggered=false must fail")
                .code,
            "invalid_forecast"
        );

        fx.write_forecast_record(&approval_unstarted, 2, false, false, "unavailable");
        let unavailable_report =
            build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &unstarted_item.item_id))
                .expect("unavailable report");
        assert_eq!(
            unavailable_report.provider_advice.status,
            ProviderAdviceStatus::Unavailable
        );
        assert!(!unavailable_report.execution.guarded_done);

        fx.write_forecast_record(&approval_unstarted, 2, false, false, "unknown");
        let unknown_report =
            build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &unstarted_item.item_id))
                .expect("unknown report");
        assert_eq!(
            unknown_report.provider_advice.status,
            ProviderAdviceStatus::Unknown
        );
        assert!(!unknown_report.execution.guarded_done);

        let unstarted_advice_path = fx
            .plans_dir
            .join(FORECAST_SUBDIR)
            .join(&approval_unstarted.subject_digest)
            .join(FORECAST_ADVICE_FILENAME);
        fx.write_forecast_record(&approval_unstarted, 2, false, false, "launched");
        let launched_absent_report =
            build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &unstarted_item.item_id))
                .expect("launched without advice.json must report pending");
        assert_eq!(
            launched_absent_report.provider_advice.status,
            ProviderAdviceStatus::Pending
        );
        assert_eq!(
            launched_absent_report.agreement,
            ForecastAgreement::Unavailable
        );
        assert!(!launched_absent_report.execution.guarded_done);

        fs::write(&unstarted_advice_path, b"").expect("write empty advice.json");
        fs::set_permissions(&unstarted_advice_path, fs::Permissions::from_mode(0o600))
            .expect("chmod empty advice.json");
        let launched_empty_report =
            build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &unstarted_item.item_id))
                .expect("launched with empty advice.json must report pending");
        assert_eq!(
            launched_empty_report.provider_advice.status,
            ProviderAdviceStatus::Pending
        );
        assert_eq!(
            launched_empty_report.agreement,
            ForecastAgreement::Unavailable
        );
        assert!(!launched_empty_report.execution.guarded_done);

        fs::write(
            &unstarted_advice_path,
            b"{\n  \"untrusted_data\": true,\n  \"mode\": \"sha",
        )
        .expect("write partial advice.json");
        fs::set_permissions(&unstarted_advice_path, fs::Permissions::from_mode(0o600))
            .expect("chmod partial advice.json");
        let launched_partial_report =
            build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &unstarted_item.item_id))
                .expect("launched with partially written advice.json must report unknown");
        assert_eq!(
            launched_partial_report.provider_advice.status,
            ProviderAdviceStatus::Unknown
        );
        assert_eq!(
            launched_partial_report.agreement,
            ForecastAgreement::Unavailable
        );
        assert!(!launched_partial_report.execution.guarded_done);

        // Part 2: Multi-attempt item with 1 technical failure retry + 1 acceptance rejection + final Done.
        // Also leaves Forecast A missing on the final approval to verify missing Forecast A handling.
        let apr1 = fx.approve_single_task(
            &fx.binding_id,
            THREAD_A,
            "plan-corr-1",
            "task-corr-1",
            Difficulty::Hard,
        );
        let apr2 = fx.approve_single_task(
            &fx.binding_id,
            THREAD_A,
            "plan-corr-2",
            "task-corr-2",
            Difficulty::Hard,
        );
        let apr3 = fx.approve_single_task(
            &fx.binding_id,
            THREAD_A,
            "plan-corr-3",
            "task-corr-3",
            Difficulty::Hard,
        );
        let (sb1, w1) = fx.create_session_worker(
            &apr1,
            Some(1_700_000_001_000),
            Some(1_700_000_003_000),
            Some(WorkerStatus::Failed),
        );
        let (sb2, w2) = fx.create_session_worker(
            &apr2,
            Some(1_700_000_010_000),
            Some(1_700_000_015_000),
            Some(WorkerStatus::Completed),
        );
        let (sb3, w3) = fx.create_session_worker(
            &apr3,
            Some(1_700_000_020_000),
            Some(1_700_000_024_000),
            Some(WorkerStatus::Completed),
        );

        let mut core = Core::open(fx.state_dir.join(DATABASE_FILENAME)).expect("open core");
        let item = core
            .create_backlog_item(
                &fx.binding_id,
                "cmd-corr-create",
                NewBacklogItem {
                    title: "Retry + rejection item".to_string(),
                    acceptance_condition: "Exact output".to_string(),
                    delivery_description: "Patched file".to_string(),
                    kind: BacklogItemKind::Worker,
                },
                1_700_000_000_000,
            )
            .expect("create item");

        // Attempt 1: technical Failure.
        let e1 = core
            .reserve_worker_backlog_execution_operator_attested(
                &fx.binding_id,
                "cmd-corr-r1",
                &item.item_id,
                item.revision,
                WorkerBacklogAuthority {
                    approval_id: apr1.approval_id.clone(),
                    subject_digest: apr1.subject_digest.clone(),
                    task_id: apr1.tasks[0].task_id.clone(),
                    task_digest: apr1.tasks[0].task_digest.clone(),
                },
                1_700_000_000_500,
            )
            .expect("reserve 1");
        let rev = core
            .get_backlog_item(&fx.binding_id, &item.item_id)
            .expect("item")
            .revision;
        core.attach_worker_backlog_dispatch_receipt_operator_attested(
            &fx.binding_id,
            "cmd-corr-a1",
            &item.item_id,
            &e1.execution_ref,
            rev,
            WorkerBacklogDispatchReceipt {
                session_binding_id: sb1,
                run_id: w1.run_id,
                worker_id: w1.worker_id,
                profile_id: apr1.tasks[0].profile_id.clone().expect("p1"),
                execution_binding_digest: None,
            },
            1_700_000_000_600,
        )
        .expect("attach 1");
        let rev = core
            .get_backlog_item(&fx.binding_id, &item.item_id)
            .expect("item")
            .revision;
        core.record_worker_backlog_outcome_operator_attested(
            &fx.binding_id,
            "cmd-corr-o1",
            &item.item_id,
            &e1.execution_ref,
            rev,
            WorkerBacklogOutcomeReport {
                outcome: WorkerBacklogOutcome::Failure,
                note: "worker failed".to_string(),
                result_digest: Some(RESULT_DIGEST_A.to_string()),
            },
            1_700_000_004_000,
        )
        .expect("fail 1");

        // Attempt 2 (retry after failure): technical Success -> Coworker acceptance rejection.
        let rev = core
            .get_backlog_item(&fx.binding_id, &item.item_id)
            .expect("item")
            .revision;
        let e2 = core
            .reserve_worker_backlog_execution_operator_attested(
                &fx.binding_id,
                "cmd-corr-r2",
                &item.item_id,
                rev,
                WorkerBacklogAuthority {
                    approval_id: apr2.approval_id.clone(),
                    subject_digest: apr2.subject_digest.clone(),
                    task_id: apr2.tasks[0].task_id.clone(),
                    task_digest: apr2.tasks[0].task_digest.clone(),
                },
                1_700_000_009_000,
            )
            .expect("reserve 2");
        let rev = core
            .get_backlog_item(&fx.binding_id, &item.item_id)
            .expect("item")
            .revision;
        core.attach_worker_backlog_dispatch_receipt_operator_attested(
            &fx.binding_id,
            "cmd-corr-a2",
            &item.item_id,
            &e2.execution_ref,
            rev,
            WorkerBacklogDispatchReceipt {
                session_binding_id: sb2,
                run_id: w2.run_id,
                worker_id: w2.worker_id,
                profile_id: apr2.tasks[0].profile_id.clone().expect("p2"),
                execution_binding_digest: None,
            },
            1_700_000_009_100,
        )
        .expect("attach 2");
        let rev = core
            .get_backlog_item(&fx.binding_id, &item.item_id)
            .expect("item")
            .revision;
        core.record_worker_backlog_outcome_operator_attested(
            &fx.binding_id,
            "cmd-corr-o2",
            &item.item_id,
            &e2.execution_ref,
            rev,
            WorkerBacklogOutcomeReport {
                outcome: WorkerBacklogOutcome::Success,
                note: "verified terminal worker success".to_string(),
                result_digest: Some(RESULT_DIGEST_A.to_string()),
            },
            1_700_000_016_000,
        )
        .expect("success 2");
        let rev = core
            .get_backlog_item(&fx.binding_id, &item.item_id)
            .expect("item")
            .revision;
        core.reject_worker_backlog_acceptance_operator_attested(
            &fx.binding_id,
            "cmd-corr-rej2",
            &item.item_id,
            rev,
            WorkerBacklogAcceptanceRejection {
                execution_ref: e2.execution_ref.clone(),
                result_digest: RESULT_DIGEST_A.to_string(),
                delivery_digest: DELIVERY_DIGEST_A.to_string(),
                reason: "semantic mismatch".to_string(),
                evidence: "independent test failed".to_string(),
            },
            1_700_000_017_000,
        )
        .expect("reject 2");

        // Attempt 3 (rework after acceptance rejection): guarded Done.
        let rev = core
            .get_backlog_item(&fx.binding_id, &item.item_id)
            .expect("item")
            .revision;
        let e3 = core
            .reserve_worker_backlog_execution_operator_attested(
                &fx.binding_id,
                "cmd-corr-r3",
                &item.item_id,
                rev,
                WorkerBacklogAuthority {
                    approval_id: apr3.approval_id.clone(),
                    subject_digest: apr3.subject_digest.clone(),
                    task_id: apr3.tasks[0].task_id.clone(),
                    task_digest: apr3.tasks[0].task_digest.clone(),
                },
                1_700_000_019_000,
            )
            .expect("reserve 3");
        let rev = core
            .get_backlog_item(&fx.binding_id, &item.item_id)
            .expect("item")
            .revision;
        core.attach_worker_backlog_dispatch_receipt_operator_attested(
            &fx.binding_id,
            "cmd-corr-a3",
            &item.item_id,
            &e3.execution_ref,
            rev,
            WorkerBacklogDispatchReceipt {
                session_binding_id: sb3,
                run_id: w3.run_id,
                worker_id: w3.worker_id,
                profile_id: apr3.tasks[0].profile_id.clone().expect("p3"),
                execution_binding_digest: None,
            },
            1_700_000_019_100,
        )
        .expect("attach 3");
        let rev = core
            .get_backlog_item(&fx.binding_id, &item.item_id)
            .expect("item")
            .revision;
        core.record_worker_backlog_outcome_operator_attested(
            &fx.binding_id,
            "cmd-corr-o3",
            &item.item_id,
            &e3.execution_ref,
            rev,
            WorkerBacklogOutcomeReport {
                outcome: WorkerBacklogOutcome::Success,
                note: "verified terminal worker success".to_string(),
                result_digest: Some(RESULT_DIGEST_A.to_string()),
            },
            1_700_000_025_000,
        )
        .expect("success 3");
        let rev = core
            .get_backlog_item(&fx.binding_id, &item.item_id)
            .expect("item")
            .revision;
        core.complete_worker_backlog_item_operator_attested(
            &fx.binding_id,
            "cmd-corr-done3",
            &item.item_id,
            rev,
            WorkerBacklogCompletion {
                execution_ref: e3.execution_ref.clone(),
                result_digest: RESULT_DIGEST_A.to_string(),
                acceptance_note: "accepted".to_string(),
                acceptance_evidence: "verified".to_string(),
                delivery_digest: DELIVERY_DIGEST_A.to_string(),
            },
            1_700_000_026_000,
        )
        .expect("complete 3");
        drop(core);

        let corr_report = build_forecast_report(fx.scope(&fx.binding_id, THREAD_A, &item.item_id))
            .expect("corr report");
        assert_eq!(
            corr_report.initial_coworker.record_status,
            ForecastRecordStatus::Missing
        );
        assert_eq!(corr_report.initial_coworker.difficulty, Difficulty::Hard);
        assert_eq!(corr_report.initial_coworker.complexity, None);
        assert_eq!(
            corr_report.provider_advice.status,
            ProviderAdviceStatus::Unavailable
        );
        assert_eq!(corr_report.agreement, ForecastAgreement::Unavailable);
        assert_eq!(corr_report.execution.elapsed_worker_ms, Some(4_000));
        assert!(corr_report.execution.guarded_done);
        assert!(!corr_report.execution.guarded_acceptance_rejected);
        assert_eq!(corr_report.correction_evidence.item_execution_count, 3);
        assert_eq!(
            corr_report.correction_evidence.acceptance_rejection_count,
            1
        );
        assert_eq!(corr_report.correction_evidence.retry_count, 1);
        assert_eq!(corr_report.correction_evidence.failed_execution_count, 1);
        assert_eq!(
            corr_report.correction_evidence.status,
            CorrectionEvidenceStatus::AcceptanceRejectedAndRetried
        );
        assert_eq!(
            corr_report.correction_evidence.cross_item_status,
            CrossItemCorrectionStatus::Unknown
        );
    }
}
