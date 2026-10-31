#![deny(unsafe_code)]
#![deny(missing_docs)]

//! Trusted local operator CLI for approved Threadlane session operations.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use threadlane_authority::validation::{validate_identifier, validate_sha256_hex};
use threadlane_authority::{AuthorityError, AuthorityStore};
use threadlane_host::{ApprovedHostClient, HostError, HostPhase};
use threadlane_native::fs_safety::constant_time_str_eq;
use threadlane_native::operator_cli::{
    emit_error, validate_dirs, verify_binding, write_result, OperatorCliError,
};
use threadlane_native::{
    open_workflow_stores, NativeError, NativeErrorCode, DEFAULT_APPROVED_ACTIVITY_LIMIT,
    DEFAULT_APPROVED_EVENT_BATCH_LIMIT, MAX_APPROVAL_CURSOR_LEN, MAX_APPROVED_ACTIVITY_LIMIT,
    MAX_APPROVED_CANCEL_REASON_LEN, MAX_APPROVED_EVENT_BATCH_LIMIT,
};

#[derive(Debug, Parser)]
#[command(
    name = "threadlane-session",
    version,
    about = "Trusted local operator CLI for Threadlane approved session operations"
)]
struct Cli {
    /// Absolute path to the private (`0700`) Threadlane runtime state directory.
    #[arg(long = "state-dir", global = true, value_name = "ABS")]
    state_dir: Option<PathBuf>,

    /// Absolute path to the private (`0700`) model/role registry directory.
    #[arg(long = "registry-dir", global = true, value_name = "ABS")]
    registry_dir: Option<PathBuf>,

    /// Absolute path to the private (`0700`) scoped plan store directory.
    #[arg(long = "plans-dir", global = true, value_name = "ABS")]
    plans_dir: Option<PathBuf>,

    /// Absolute path to the private (`0700`) operator authority store directory.
    #[arg(long = "authority-dir", global = true, value_name = "ABS")]
    authority_dir: Option<PathBuf>,

    /// Absolute path to the private (`0700`) approved host session store directory.
    #[arg(long = "sessions-dir", global = true, value_name = "ABS")]
    sessions_dir: Option<PathBuf>,

    /// Authoritative Threadlane control binding identifier (`binding_id`).
    #[arg(long = "binding", global = true, value_name = "ID")]
    binding: Option<String>,

    /// Target canonical lowercase hyphenated non-nil native Codex thread UUID.
    #[arg(long = "thread", global = true, value_name = "UUID")]
    thread: Option<String>,

    /// Session subcommand to execute.
    #[command(subcommand)]
    command: SessionCommand,
}

#[derive(Debug, Subcommand)]
enum SessionCommand {
    /// Inspect an approved host session summary after verifying live authenticated readiness.
    Session {
        /// Approval identifier.
        #[arg(long = "approval-id", value_name = "ID")]
        approval_id: String,
    },
    /// Inspect an approved task state snapshot.
    Task {
        /// Approval identifier.
        #[arg(long = "approval-id", value_name = "ID")]
        approval_id: String,

        /// Task identifier within the approved plan.
        #[arg(long = "task-id", value_name = "ID")]
        task_id: String,
    },
    /// Dispatch a ready approved task for execution on the live approved host.
    Dispatch {
        /// Approval identifier.
        #[arg(long = "approval-id", value_name = "ID")]
        approval_id: String,

        /// Task identifier within the approved plan.
        #[arg(long = "task-id", value_name = "ID")]
        task_id: String,

        /// Lowercase 64-character SHA-256 hex digest matching the approval record's `subject_digest`.
        #[arg(long = "confirm-approval", value_name = "SHA256")]
        confirm_approval: String,

        /// Explicit operator approval confirmation flag.
        #[arg(long = "operator-approved")]
        operator_approved: bool,
    },
    /// Cancel a running or queued approved task at an expected task revision.
    Cancel {
        /// Approval identifier.
        #[arg(long = "approval-id", value_name = "ID")]
        approval_id: String,

        /// Task identifier within the approved plan.
        #[arg(long = "task-id", value_name = "ID")]
        task_id: String,

        /// Expected current task revision (`>= 1`).
        #[arg(long = "expected-revision", value_name = "N")]
        expected_revision: u64,

        /// Optional bounded cancellation reason (`<= 512` bytes, no control characters).
        #[arg(long = "reason", value_name = "TEXT")]
        reason: Option<String>,

        /// Lowercase 64-character SHA-256 hex digest matching the approval record's `subject_digest`.
        #[arg(long = "confirm-approval", value_name = "SHA256")]
        confirm_approval: String,

        /// Explicit operator approval confirmation flag.
        #[arg(long = "operator-approved")]
        operator_approved: bool,
    },
    /// Request an authenticated, owner-scoped idle stop of a running approved host session instance.
    Stop {
        /// Approval identifier.
        #[arg(long = "approval-id", value_name = "ID")]
        approval_id: String,

        /// Expected running host instance identifier (`hst_<32hex>`).
        #[arg(long = "expected-instance", value_name = "HST_ID")]
        expected_instance: String,

        /// Lowercase 64-character SHA-256 hex digest matching the approval record's `subject_digest`.
        #[arg(long = "confirm-approval", value_name = "SHA256")]
        confirm_approval: String,

        /// Explicit operator approval confirmation flag.
        #[arg(long = "operator-approved")]
        operator_approved: bool,
    },
    /// List bounded redacted activity events from the live approved host session.
    Activity {
        /// Approval identifier.
        #[arg(long = "approval-id", value_name = "ID")]
        approval_id: String,

        /// Optional task identifier filter.
        #[arg(long = "task-id", value_name = "ID")]
        task_id: Option<String>,

        /// Exclusive lower sequence bound (mutually exclusive with `--cursor`).
        #[arg(long = "after-seq", value_name = "N", conflicts_with = "cursor")]
        after_seq: Option<u64>,

        /// Opaque pagination cursor (mutually exclusive with `--after-seq`).
        #[arg(long = "cursor", value_name = "TEXT", conflicts_with = "after_seq")]
        cursor: Option<String>,

        /// Maximum number of activity entries to return (`1..=20`, default `20`).
        #[arg(long = "limit", value_name = "N")]
        limit: Option<usize>,
    },
    /// Pull a bounded batch of approved host notification events.
    Pull {
        /// Approval identifier.
        #[arg(long = "approval-id", value_name = "ID")]
        approval_id: String,

        /// Optional exclusive lower sequence bound.
        #[arg(long = "after-seq", value_name = "N")]
        after_seq: Option<u64>,

        /// Maximum number of notification events to return (`1..=20`, default `20`).
        #[arg(long = "limit", value_name = "N")]
        limit: Option<usize>,
    },
    /// Acknowledge receipt of a pulled approved host notification batch.
    Ack {
        /// Approval identifier.
        #[arg(long = "approval-id", value_name = "ID")]
        approval_id: String,

        /// Batch identifier returned by `pull`.
        #[arg(long = "batch-id", value_name = "ID")]
        batch_id: String,
    },
}

#[derive(Debug, Clone)]
struct GlobalArgs {
    state_dir: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    binding: String,
    thread: String,
}

impl GlobalArgs {
    fn from_cli(cli: &Cli) -> Result<Self, OperatorCliError> {
        let state_dir = cli.state_dir.clone().ok_or_else(|| {
            OperatorCliError::invalid_input("Missing required `--state-dir` argument.")
        })?;
        let registry_dir = cli.registry_dir.clone().ok_or_else(|| {
            OperatorCliError::invalid_input("Missing required `--registry-dir` argument.")
        })?;
        let plans_dir = cli.plans_dir.clone().ok_or_else(|| {
            OperatorCliError::invalid_input("Missing required `--plans-dir` argument.")
        })?;
        let authority_dir = cli.authority_dir.clone().ok_or_else(|| {
            OperatorCliError::invalid_input("Missing required `--authority-dir` argument.")
        })?;
        let sessions_dir = cli.sessions_dir.clone().ok_or_else(|| {
            OperatorCliError::invalid_input("Missing required `--sessions-dir` argument.")
        })?;
        let binding = cli.binding.clone().ok_or_else(|| {
            OperatorCliError::invalid_input("Missing required `--binding` argument.")
        })?;
        let thread = cli.thread.clone().ok_or_else(|| {
            OperatorCliError::invalid_input("Missing required `--thread` argument.")
        })?;

        Ok(Self {
            state_dir,
            registry_dir,
            plans_dir,
            authority_dir,
            sessions_dir,
            binding,
            thread,
        })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(err) => {
            if matches!(
                err.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            ) {
                let _ = err.print();
                return ExitCode::SUCCESS;
            }
            emit_error(&OperatorCliError::invalid_input(
                "Invalid CLI arguments for `threadlane-session`.",
            ));
            return ExitCode::from(1);
        }
    };

    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            emit_error(&error);
            ExitCode::from(1)
        }
    }
}

async fn run(cli: Cli) -> Result<(), OperatorCliError> {
    let globals = GlobalArgs::from_cli(&cli)?;

    let canonical_dirs = validate_dirs(&[
        ("state_dir", &globals.state_dir),
        ("registry_dir", &globals.registry_dir),
        ("plans_dir", &globals.plans_dir),
        ("authority_dir", &globals.authority_dir),
        ("sessions_dir", &globals.sessions_dir),
    ])?;
    let state_dir = canonical_dirs[0].clone();
    let registry_dir = canonical_dirs[1].clone();
    let plans_dir = canonical_dirs[2].clone();
    let authority_dir = canonical_dirs[3].clone();
    let sessions_dir = canonical_dirs[4].clone();

    verify_binding(&state_dir, &globals.binding, &globals.thread).await?;

    let Some((registry, plans)) = open_workflow_stores(Some(&registry_dir), Some(&plans_dir))
        .map_err(|err| map_native_error(&err))?
    else {
        return Err(OperatorCliError::invalid_input(
            "Workflow registry and plan stores could not be opened.",
        ));
    };
    let authority = Arc::new(
        AuthorityStore::open(&authority_dir, Arc::clone(&registry), Arc::clone(&plans))
            .map_err(|err| map_authority_error(&err))?,
    );
    let client = ApprovedHostClient::new(
        Arc::clone(&authority),
        &sessions_dir,
        globals.binding.clone(),
        globals.thread.clone(),
    )
    .map_err(|err| map_host_error(&err))?;

    match cli.command {
        SessionCommand::Session { approval_id } => {
            verify_operator_approval(
                &authority,
                &approval_id,
                &globals.binding,
                &globals.thread,
                None,
            )?;

            let mut summary = client
                .session_summary(&approval_id)
                .map_err(|err| map_host_error(&err))?;
            if summary.last_error.is_some() {
                summary.last_error = Some(
                    "Approved session recorded an error; inspect host diagnostics.".to_string(),
                );
            }
            if summary.binding_id != globals.binding || summary.thread_uuid != globals.thread {
                return Err(OperatorCliError::new(
                    "scope_mismatch",
                    "Approved session does not belong to the requested binding and thread scope.",
                ));
            }
            if summary.phase == HostPhase::Ready {
                if !summary.gates.supervisor_authenticated_ready
                    || summary.session_binding_id.is_none()
                {
                    return Err(OperatorCliError::new(
                        "host_not_ready",
                        "Approved host session is not ready in the requested scope.",
                    ));
                }

                let live_probe = client
                    .list_activity(&approval_id, None, None, Some(1), None)
                    .await
                    .map_err(|err| match err {
                        HostError::HostNotReady { .. }
                        | HostError::Runtime { .. }
                        | HostError::Io { .. } => OperatorCliError::new(
                            "host_not_ready",
                            "Approved host session is not ready in the requested scope.",
                        ),
                        other => map_host_error(&other),
                    })?;
                if Some(live_probe.binding_id.as_str()) != summary.session_binding_id.as_deref()
                    || live_probe.binding_id == globals.binding
                {
                    return Err(OperatorCliError::new(
                        "corrupt_session_state",
                        "Approved host session binding identity failed validation.",
                    ));
                }
            }

            write_result(&globals.binding, &summary, false)
        }
        SessionCommand::Task {
            approval_id,
            task_id,
        } => {
            verify_operator_approval(
                &authority,
                &approval_id,
                &globals.binding,
                &globals.thread,
                None,
            )?;

            let snapshot = client
                .get_task(&approval_id, &task_id)
                .await
                .map_err(|err| map_host_error(&err))?;
            if snapshot.binding_id != globals.binding || snapshot.thread_uuid != globals.thread {
                return Err(OperatorCliError::new(
                    "scope_mismatch",
                    "Approved task does not belong to the requested binding and thread scope.",
                ));
            }
            write_result(&globals.binding, &snapshot, false)
        }
        SessionCommand::Dispatch {
            approval_id,
            task_id,
            confirm_approval,
            operator_approved,
        } => {
            require_operator_approved(operator_approved)?;
            let checked_confirm = validate_confirm_approval(&confirm_approval)?;
            verify_operator_approval(
                &authority,
                &approval_id,
                &globals.binding,
                &globals.thread,
                Some(&checked_confirm),
            )?;

            verify_binding(&state_dir, &globals.binding, &globals.thread).await?;
            let snapshot = client
                .dispatch_task(&approval_id, &task_id)
                .await
                .map_err(|err| map_host_mutation_error(&err))?;
            if snapshot.binding_id != globals.binding || snapshot.thread_uuid != globals.thread {
                return Err(OperatorCliError::new(
                    "outcome_unknown",
                    "Task dispatch completed, but returned snapshot scope failed validation.",
                ));
            }
            write_result(&globals.binding, &snapshot, true)
        }
        SessionCommand::Cancel {
            approval_id,
            task_id,
            expected_revision,
            reason,
            confirm_approval,
            operator_approved,
        } => {
            require_operator_approved(operator_approved)?;
            if expected_revision == 0 {
                return Err(OperatorCliError::invalid_input(
                    "`--expected-revision` must be >= 1.",
                ));
            }
            let clean_reason = validate_cancel_reason(reason)?;
            let checked_confirm = validate_confirm_approval(&confirm_approval)?;
            verify_operator_approval(
                &authority,
                &approval_id,
                &globals.binding,
                &globals.thread,
                Some(&checked_confirm),
            )?;

            verify_binding(&state_dir, &globals.binding, &globals.thread).await?;
            let snapshot = client
                .cancel_task(&approval_id, &task_id, expected_revision, clean_reason)
                .await
                .map_err(|err| map_host_mutation_error(&err))?;
            if snapshot.binding_id != globals.binding || snapshot.thread_uuid != globals.thread {
                return Err(OperatorCliError::new(
                    "outcome_unknown",
                    "Task cancellation completed, but returned snapshot scope failed validation.",
                ));
            }
            write_result(&globals.binding, &snapshot, true)
        }
        SessionCommand::Stop {
            approval_id,
            expected_instance,
            confirm_approval,
            operator_approved,
        } => {
            require_operator_approved(operator_approved)?;
            let checked_confirm = validate_confirm_approval(&confirm_approval)?;
            verify_operator_approval(
                &authority,
                &approval_id,
                &globals.binding,
                &globals.thread,
                Some(&checked_confirm),
            )?;

            verify_binding(&state_dir, &globals.binding, &globals.thread).await?;
            let mut summary = client
                .stop_idle_session(&approval_id, &expected_instance)
                .await
                .map_err(|err| map_host_mutation_error(&err))?;
            if summary.last_error.is_some() {
                summary.last_error = Some(
                    "Approved session recorded an error; inspect host diagnostics.".to_string(),
                );
            }
            if summary.binding_id != globals.binding
                || summary.thread_uuid != globals.thread
                || summary.instance_id.as_deref() != Some(expected_instance.as_str())
            {
                return Err(OperatorCliError::new(
                    "outcome_unknown",
                    "Session stop completed, but returned summary scope or instance failed validation.",
                ));
            }
            write_result(&globals.binding, &summary, true)
        }
        SessionCommand::Activity {
            approval_id,
            task_id,
            after_seq,
            cursor,
            limit,
        } => {
            let (effective_limit, clean_cursor) = validate_activity_args(after_seq, cursor, limit)?;
            verify_operator_approval(
                &authority,
                &approval_id,
                &globals.binding,
                &globals.thread,
                None,
            )?;

            let page = match task_id {
                Some(task_id) => client
                    .list_task_activity(
                        &approval_id,
                        &task_id,
                        after_seq,
                        clean_cursor,
                        Some(effective_limit),
                        None,
                    )
                    .await
                    .map_err(|err| map_host_error(&err))?,
                None => client
                    .list_activity(
                        &approval_id,
                        after_seq,
                        clean_cursor,
                        Some(effective_limit),
                        None,
                    )
                    .await
                    .map_err(|err| map_host_error(&err))?,
            };
            if page.binding_id.trim().is_empty() || page.binding_id == globals.binding {
                return Err(OperatorCliError::new(
                    "corrupt_session_state",
                    "Approved activity page binding identity failed validation.",
                ));
            }
            write_result(&globals.binding, &page, false)
        }
        SessionCommand::Pull {
            approval_id,
            after_seq,
            limit,
        } => {
            let effective_limit = validate_pull_limit(limit)?;
            verify_operator_approval(
                &authority,
                &approval_id,
                &globals.binding,
                &globals.thread,
                None,
            )?;

            verify_binding(&state_dir, &globals.binding, &globals.thread).await?;
            let mut batch = client
                .pull_events(&approval_id, after_seq, Some(effective_limit))
                .await
                .map_err(|err| map_host_mutation_error(&err))?;
            if batch.binding_id.trim().is_empty() || batch.binding_id == globals.binding {
                return Err(OperatorCliError::new(
                    "outcome_unknown",
                    "Event pull completed, but returned batch binding identity failed validation.",
                ));
            }
            for event in &mut batch.events {
                event.payload.untrusted_data = true;
            }
            write_result(&globals.binding, &batch, true)
        }
        SessionCommand::Ack {
            approval_id,
            batch_id,
        } => {
            validate_identifier("batch_id", &batch_id)
                .map_err(|_| OperatorCliError::invalid_input("Invalid `--batch-id` syntax."))?;
            verify_operator_approval(
                &authority,
                &approval_id,
                &globals.binding,
                &globals.thread,
                None,
            )?;

            verify_binding(&state_dir, &globals.binding, &globals.thread).await?;
            let ack = client
                .ack_events(&approval_id, batch_id)
                .await
                .map_err(|err| map_host_mutation_error(&err))?;
            if ack.binding_id.trim().is_empty() || ack.binding_id == globals.binding {
                return Err(OperatorCliError::new(
                    "outcome_unknown",
                    "Event acknowledgement completed, but returned binding identity failed validation.",
                ));
            }
            write_result(&globals.binding, &ack, true)
        }
    }
}

fn require_operator_approved(operator_approved: bool) -> Result<(), OperatorCliError> {
    if !operator_approved {
        return Err(OperatorCliError::invalid_input(
            "Explicit `--operator-approved` confirmation is required.",
        ));
    }
    Ok(())
}

fn validate_confirm_approval(value: &str) -> Result<String, OperatorCliError> {
    validate_sha256_hex("confirm_approval", value).map_err(|_| {
        OperatorCliError::invalid_input(
            "`--confirm-approval` must be a 64-character lowercase hexadecimal SHA-256 digest.",
        )
    })
}

fn validate_cancel_reason(reason: Option<String>) -> Result<Option<String>, OperatorCliError> {
    let Some(raw_reason) = reason else {
        return Ok(None);
    };
    let trimmed = raw_reason.trim();
    if trimmed.is_empty() {
        return Err(OperatorCliError::invalid_input(
            "`--reason` must not be empty when provided.",
        ));
    }
    if trimmed.len() > MAX_APPROVED_CANCEL_REASON_LEN {
        return Err(OperatorCliError::invalid_input(
            "`--reason` exceeds the 512-byte maximum length.",
        ));
    }
    if trimmed.chars().any(char::is_control) {
        return Err(OperatorCliError::invalid_input(
            "`--reason` must not contain control characters.",
        ));
    }
    Ok(Some(trimmed.to_string()))
}

fn validate_activity_args(
    after_seq: Option<u64>,
    cursor: Option<String>,
    limit: Option<usize>,
) -> Result<(usize, Option<String>), OperatorCliError> {
    if after_seq.is_some() && cursor.is_some() {
        return Err(OperatorCliError::invalid_input(
            "`--after-seq` and `--cursor` cannot be combined.",
        ));
    }
    let effective_limit = limit.unwrap_or(DEFAULT_APPROVED_ACTIVITY_LIMIT);
    if !(1..=MAX_APPROVED_ACTIVITY_LIMIT).contains(&effective_limit) {
        return Err(OperatorCliError::invalid_input(
            "`--limit` must be between 1 and 20.",
        ));
    }
    let clean_cursor = match cursor {
        Some(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty()
                || trimmed != raw
                || trimmed.len() > MAX_APPROVAL_CURSOR_LEN
                || trimmed.chars().any(char::is_control)
            {
                return Err(OperatorCliError::invalid_input("Invalid `--cursor` value."));
            }
            Some(trimmed.to_string())
        }
        None => None,
    };
    Ok((effective_limit, clean_cursor))
}

fn validate_pull_limit(limit: Option<usize>) -> Result<usize, OperatorCliError> {
    let effective_limit = limit.unwrap_or(DEFAULT_APPROVED_EVENT_BATCH_LIMIT);
    if !(1..=MAX_APPROVED_EVENT_BATCH_LIMIT).contains(&effective_limit) {
        return Err(OperatorCliError::invalid_input(
            "`--limit` must be between 1 and 20.",
        ));
    }
    Ok(effective_limit)
}

fn verify_operator_approval(
    authority: &AuthorityStore,
    approval_id: &str,
    expected_binding_id: &str,
    expected_thread_uuid: &str,
    expected_subject_digest: Option<&str>,
) -> Result<(), OperatorCliError> {
    let record = authority
        .get_for_operator(approval_id)
        .map_err(|err| map_authority_error(&err))?;
    if record.binding_id != expected_binding_id || record.thread_uuid != expected_thread_uuid {
        return Err(OperatorCliError::new(
            "scope_mismatch",
            "Approval record does not belong to the requested binding and thread scope.",
        ));
    }
    if let Some(expected_digest) = expected_subject_digest {
        if !constant_time_str_eq(&record.subject_digest, expected_digest) {
            return Err(OperatorCliError::new(
                "subject_mismatch",
                "`--confirm-approval` does not match the approval record's `subject_digest`.",
            ));
        }
    }
    Ok(())
}

fn map_native_error(error: &NativeError) -> OperatorCliError {
    match error.error_code() {
        NativeErrorCode::SecurityViolation => OperatorCliError::new(
            error.code(),
            "Store directory failed private permission or symlink security checks.",
        ),
        NativeErrorCode::UnsupportedPlatform => OperatorCliError::new(
            error.code(),
            "threadlane-session is only supported on Unix platforms.",
        ),
        NativeErrorCode::IoError => OperatorCliError::new(
            error.code(),
            "Filesystem I/O error while opening workflow stores.",
        ),
        _ => OperatorCliError::invalid_input("Workflow store configuration or state is invalid."),
    }
}

fn map_authority_error(error: &AuthorityError) -> OperatorCliError {
    let message = match error {
        AuthorityError::Validation { .. } => "Authority input or identifier failed validation.",
        AuthorityError::PlanNotFound { .. } => "Referenced workflow plan revision was not found.",
        AuthorityError::ApprovalNotFound { .. } => {
            "Requested approval record was not found in the authority store."
        }
        AuthorityError::BindingMismatch { .. } => {
            "Approval or plan binding_id did not match the requested binding."
        }
        AuthorityError::ThreadUuidMismatch { .. } => {
            "Approval thread_uuid did not match the requested native thread UUID."
        }
        AuthorityError::UnpublishedOrSupersededPlan { .. } => {
            "Source workflow plan revision is not an active published revision."
        }
        AuthorityError::StaleRegistryRevision { .. } => {
            "Workflow registry revision no longer matches the approved snapshot."
        }
        AuthorityError::PlanDigestMismatch { .. } => {
            "Compiled workflow plan digest does not match the approved snapshot."
        }
        AuthorityError::UnknownTask { .. } => {
            "Requested task was not found in the frozen approval record."
        }
        AuthorityError::UnsupportedTaskAdapter { .. } => {
            "Selected task uses an unsupported execution adapter."
        }
        AuthorityError::InvalidExecutable { .. } => {
            "Approved worker tool executable could not be validated."
        }
        AuthorityError::InvalidRepository { .. } => {
            "Approved target repository could not be validated."
        }
        AuthorityError::InvalidBaseCommit { .. } => "Approved base commit could not be validated.",
        AuthorityError::InvalidWriteScope { .. } => {
            "Task or operator write scope entry failed validation."
        }
        AuthorityError::UnapprovedTaskWriteScope { .. } => {
            "Selected task write scope is not covered by operator approval."
        }
        AuthorityError::InvalidWorkArea { .. } => "Approved work area directory failed validation.",
        AuthorityError::WorkersConfigInvalid { .. } => {
            "Synthesized worker configuration failed validation."
        }
        AuthorityError::SecurityViolation { .. } => {
            "Authority store path or permission security requirement was violated."
        }
        AuthorityError::CorruptStore { .. } => {
            "Authority store ledger failed integrity verification."
        }
        AuthorityError::Io { .. } => {
            "Filesystem I/O error occurred during authority store operation."
        }
        AuthorityError::Registry { .. } => {
            "Workflow registry store rejected the authority operation."
        }
        AuthorityError::Plan { .. } => "Workflow plan store rejected the authority operation.",
    };
    OperatorCliError::new(error.code(), message)
}

fn map_host_error(error: &HostError) -> OperatorCliError {
    let message = match error {
        HostError::Validation { .. } => "Session, task, or pagination argument failed validation.",
        HostError::ApprovalNotFound { .. } => "Requested approval record was not found.",
        HostError::UnknownTask { .. } => {
            "Requested task was not found in the frozen approval record."
        }
        HostError::ScopeMismatch { .. } => {
            "Approved session resource does not belong to the requested binding and thread scope."
        }
        HostError::DependencyInputsUnverified { .. } => {
            "Dependent task cannot be dispatched because upstream task outputs are unverified."
        }
        HostError::NativeRouteRequired { .. } => {
            "Task targets a manual native route and cannot be dispatched as a worker."
        }
        HostError::TaskNotStarted { .. } => {
            "Requested approved task has not been started in this host session."
        }
        HostError::UnpublishedOrSupersededPlan { .. } => {
            "Source workflow plan revision is not an active published revision."
        }
        HostError::LockBusy { .. } => "Approved host session lock is currently busy.",
        HostError::HostNotReady { .. } => {
            "Approved host session is not running or has not reached authenticated ready state."
        }
        HostError::HostNotIdle { .. } => {
            "Approved host session still has active work or notifications and cannot stop yet."
        }
        HostError::InstanceMismatch { .. } => {
            "Approved host instance changed; refresh the session before retrying."
        }
        HostError::StopOutcomeUncertain { .. } => {
            "Approved host stop outcome could not be confirmed."
        }
        HostError::MutationOutcomeUncertain { .. } => {
            "Session mutation outcome is uncertain after crossing an execution or persistence boundary; inspect session, task, or activity state before retrying."
        }
        HostError::RestartNotAllowed { .. } => {
            "Approved host restart is not allowed in the current session state."
        }
        HostError::StaleRevision { .. } => {
            "Expected worker run revision does not match the current revision."
        }
        HostError::IdempotencyConflict { .. } => {
            "Deterministic command key conflicted with an existing operation."
        }
        HostError::InvalidRunState { .. } => {
            "Operation is not valid for the worker run's current lifecycle state."
        }
        HostError::SecurityViolation { .. } => {
            "Approved host session store path or permission security requirement was violated."
        }
        HostError::CorruptManifest { .. } => {
            "Approved host session manifest failed integrity verification."
        }
        HostError::CorruptReceipts { .. } => {
            "Approved host task receipt ledger failed integrity verification."
        }
        HostError::CorruptSessionState { .. } => {
            "Approved host runtime session violated integrity invariants."
        }
        HostError::Authority { .. } => "Authority store rejected the approved session operation.",
        HostError::Runtime { .. } => "Approved host runtime rejected the operation.",
        HostError::Io { .. } => {
            "Filesystem I/O error occurred while accessing approved host state."
        }
    };
    OperatorCliError::new(error.code(), message)
}

/// Maps [`HostError`] returned after entering a mutating session command (`dispatch`,
/// `cancel`, `stop`, `pull`, or `ack`).
///
/// Mutating session operations can persist an intent/receipt, start or cancel a worker,
/// request an idle host stop, or advance delivered/acknowledged notification state before
/// encountering an I/O, transport/runtime, post-effect scope, or ledger/manifest integrity
/// failure. Ambiguous runtime, I/O, post-effect scope/integrity failures, and uncertain stop
/// outcomes are conservatively reported as `outcome_unknown` so operators inspect current
/// `session`, `task`, or `activity` state instead of blindly retrying, while deterministic
/// pre-effect refusals preserve their original error codes.
fn map_host_mutation_error(error: &HostError) -> OperatorCliError {
    match error {
        HostError::MutationOutcomeUncertain { .. }
        | HostError::StopOutcomeUncertain { .. }
        | HostError::ScopeMismatch { .. }
        | HostError::CorruptManifest { .. }
        | HostError::CorruptReceipts { .. }
        | HostError::CorruptSessionState { .. }
        | HostError::Runtime { .. }
        | HostError::Io { .. } => OperatorCliError::new(
            "outcome_unknown",
            "Session mutation outcome is uncertain after a runtime, I/O, scope, or integrity error; inspect session, task, or activity state before retrying.",
        ),
        HostError::Validation { .. }
        | HostError::ApprovalNotFound { .. }
        | HostError::UnknownTask { .. }
        | HostError::DependencyInputsUnverified { .. }
        | HostError::NativeRouteRequired { .. }
        | HostError::TaskNotStarted { .. }
        | HostError::UnpublishedOrSupersededPlan { .. }
        | HostError::LockBusy { .. }
        | HostError::HostNotReady { .. }
        | HostError::HostNotIdle { .. }
        | HostError::InstanceMismatch { .. }
        | HostError::RestartNotAllowed { .. }
        | HostError::StaleRevision { .. }
        | HostError::IdempotencyConflict { .. }
        | HostError::InvalidRunState { .. }
        | HostError::SecurityViolation { .. }
        | HostError::Authority { .. } => map_host_error(error),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    use tempfile::TempDir;
    use threadlane_authority::OperatorApprovalRequest;
    use threadlane_workers::{
        resolve_exact_commit_sha, run_sanitized_git, ExecutionBudgets,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    };
    use threadlane_workflow::{
        compile_plan_revision, Difficulty, ModelChoice, ModelDefinition, PlanChange, PlanStatus,
        PlanTask, RegistryChange, RoleAssignment, ToolAdapter, ToolDefinition, WorkflowProfile,
        WorkflowRole,
    };

    use super::*;

    const BINDING_A: &str = "bnd_session_a";
    const BINDING_B: &str = "bnd_session_b";
    const THREAD_A: &str = "0196ed00-0000-7000-8000-00000000000a";
    const THREAD_B: &str = "0196ed00-0000-7000-8000-00000000000b";
    const DIGEST_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const DIGEST_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    #[test]
    fn cli_parses_global_flags_before_and_after_subcommand() {
        let before = Cli::try_parse_from([
            "threadlane-session",
            "--state-dir",
            "/tmp/state",
            "--registry-dir",
            "/tmp/registry",
            "--plans-dir",
            "/tmp/plans",
            "--authority-dir",
            "/tmp/authority",
            "--sessions-dir",
            "/tmp/sessions",
            "--binding",
            BINDING_A,
            "--thread",
            THREAD_A,
            "session",
            "--approval-id",
            "apr_1",
        ])
        .expect("parse global flags before subcommand");
        let globals_before = GlobalArgs::from_cli(&before).expect("extract globals");
        assert_eq!(globals_before.binding, BINDING_A);
        assert_eq!(globals_before.thread, THREAD_A);

        let after = Cli::try_parse_from([
            "threadlane-session",
            "dispatch",
            "--approval-id",
            "apr_1",
            "--task-id",
            "task_1",
            "--confirm-approval",
            DIGEST_A,
            "--operator-approved",
            "--state-dir",
            "/tmp/state",
            "--registry-dir",
            "/tmp/registry",
            "--plans-dir",
            "/tmp/plans",
            "--authority-dir",
            "/tmp/authority",
            "--sessions-dir",
            "/tmp/sessions",
            "--binding",
            BINDING_A,
            "--thread",
            THREAD_A,
        ])
        .expect("parse global flags after subcommand");
        let globals_after = GlobalArgs::from_cli(&after).expect("extract globals");
        assert_eq!(globals_after.binding, BINDING_A);
        assert_eq!(globals_after.thread, THREAD_A);

        let stop_cli = Cli::try_parse_from([
            "threadlane-session",
            "stop",
            "--approval-id",
            "apr_1",
            "--expected-instance",
            "hst_0123456789abcdef0123456789abcdef",
            "--confirm-approval",
            DIGEST_A,
            "--operator-approved",
            "--state-dir",
            "/tmp/state",
            "--registry-dir",
            "/tmp/registry",
            "--plans-dir",
            "/tmp/plans",
            "--authority-dir",
            "/tmp/authority",
            "--sessions-dir",
            "/tmp/sessions",
            "--binding",
            BINDING_A,
            "--thread",
            THREAD_A,
        ])
        .expect("parse stop subcommand");
        match &stop_cli.command {
            SessionCommand::Stop {
                approval_id,
                expected_instance,
                confirm_approval,
                operator_approved,
            } => {
                assert_eq!(approval_id, "apr_1");
                assert_eq!(expected_instance, "hst_0123456789abcdef0123456789abcdef");
                assert_eq!(confirm_approval, DIGEST_A);
                assert!(*operator_approved);
            }
            other => panic!("expected SessionCommand::Stop, got {other:?}"),
        }
        assert!(
            Cli::try_parse_from([
                "threadlane-session",
                "stop",
                "--approval-id",
                "apr_1",
                "--confirm-approval",
                DIGEST_A,
                "--operator-approved",
            ])
            .is_err(),
            "stop without --expected-instance must fail clap parsing"
        );

        let missing_binding = Cli::try_parse_from([
            "threadlane-session",
            "--state-dir",
            "/tmp/state",
            "--registry-dir",
            "/tmp/registry",
            "--plans-dir",
            "/tmp/plans",
            "--authority-dir",
            "/tmp/authority",
            "--sessions-dir",
            "/tmp/sessions",
            "--thread",
            THREAD_A,
            "session",
            "--approval-id",
            "apr_1",
        ])
        .expect("clap parse succeeds before GlobalArgs extraction");
        let err = GlobalArgs::from_cli(&missing_binding).expect_err("missing --binding must fail");
        assert_eq!(err.code, "invalid_input");
    }

    #[test]
    fn cli_rejects_after_seq_and_cursor_together() {
        let parse_res = Cli::try_parse_from([
            "threadlane-session",
            "--state-dir",
            "/tmp/state",
            "--registry-dir",
            "/tmp/registry",
            "--plans-dir",
            "/tmp/plans",
            "--authority-dir",
            "/tmp/authority",
            "--sessions-dir",
            "/tmp/sessions",
            "--binding",
            BINDING_A,
            "--thread",
            THREAD_A,
            "activity",
            "--approval-id",
            "apr_1",
            "--after-seq",
            "1",
            "--cursor",
            "1",
        ]);
        assert!(
            parse_res.is_err(),
            "clap must reject mutually exclusive --after-seq and --cursor"
        );

        let helper_err = validate_activity_args(Some(1), Some("1".to_string()), Some(10))
            .expect_err("helper must also reject both after_seq and cursor");
        assert_eq!(helper_err.code, "invalid_input");
    }

    #[test]
    fn validation_helpers_enforce_limits_digests_and_reasons() {
        assert!(require_operator_approved(true).is_ok());
        assert_eq!(
            require_operator_approved(false).unwrap_err().code,
            "invalid_input"
        );

        assert_eq!(validate_confirm_approval(DIGEST_A).unwrap(), DIGEST_A);
        assert_eq!(
            validate_confirm_approval("short").unwrap_err().code,
            "invalid_input"
        );
        assert_eq!(
            validate_confirm_approval(
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            )
            .unwrap_err()
            .code,
            "invalid_input"
        );

        assert_eq!(validate_cancel_reason(None).unwrap(), None);
        assert_eq!(
            validate_cancel_reason(Some("  operator stop  ".to_string())).unwrap(),
            Some("operator stop".to_string())
        );
        assert_eq!(
            validate_cancel_reason(Some("   ".to_string()))
                .unwrap_err()
                .code,
            "invalid_input"
        );
        assert_eq!(
            validate_cancel_reason(Some("bad\nreason".to_string()))
                .unwrap_err()
                .code,
            "invalid_input"
        );
        assert_eq!(
            validate_cancel_reason(Some("x".repeat(MAX_APPROVED_CANCEL_REASON_LEN + 1)))
                .unwrap_err()
                .code,
            "invalid_input"
        );

        assert_eq!(
            validate_activity_args(None, None, None).unwrap(),
            (DEFAULT_APPROVED_ACTIVITY_LIMIT, None)
        );
        assert_eq!(
            validate_activity_args(None, None, Some(0))
                .unwrap_err()
                .code,
            "invalid_input"
        );
        assert_eq!(
            validate_activity_args(None, None, Some(MAX_APPROVED_ACTIVITY_LIMIT + 1))
                .unwrap_err()
                .code,
            "invalid_input"
        );
        assert_eq!(
            validate_activity_args(None, Some(" bad ".to_string()), None)
                .unwrap_err()
                .code,
            "invalid_input"
        );

        assert_eq!(
            validate_pull_limit(None).unwrap(),
            DEFAULT_APPROVED_EVENT_BATCH_LIMIT
        );
        assert_eq!(
            validate_pull_limit(Some(0)).unwrap_err().code,
            "invalid_input"
        );
        assert_eq!(
            validate_pull_limit(Some(MAX_APPROVED_EVENT_BATCH_LIMIT + 1))
                .unwrap_err()
                .code,
            "invalid_input"
        );
    }

    #[test]
    fn verify_operator_approval_enforces_scope_before_digest_check() {
        let tmp = TempDir::new().expect("tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize tempdir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("chmod 0700");

        let repo_dir = root.join("repo");
        fs::create_dir_all(repo_dir.join("src")).expect("create repo src");
        fs::write(repo_dir.join("src/lib.rs"), "pub fn check() -> u32 { 1 }\n")
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
        .expect("git config email");
        run_sanitized_git(
            &repo_dir,
            &["config", "user.name", "Threadlane Test"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git config name");
        run_sanitized_git(&repo_dir, &["add", "."], DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("git add");
        run_sanitized_git(
            &repo_dir,
            &["commit", "-m", "initial commit", "--quiet"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git commit");
        let base_commit =
            resolve_exact_commit_sha(&repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
                .expect("resolve HEAD commit");

        let registry_dir = root.join("registry");
        let plans_dir = root.join("plans");
        let authority_dir = root.join("authority");

        let (registry, plans) = open_workflow_stores(Some(&registry_dir), Some(&plans_dir))
            .expect("open workflow stores")
            .expect("workflow store handles present");

        let s1 = registry
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "tool-codex".to_string(),
                        label: "Native Codex".to_string(),
                        adapter: ToolAdapter::CodexCli,
                        executable_path: None,
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
                        id: "model-codex".to_string(),
                        tool_id: "tool-codex".to_string(),
                        label: "Native Codex Follower".to_string(),
                        model_arg: "gpt-5-codex".to_string(),
                        efforts: vec!["high".to_string()],
                        allow_no_effort: true,
                    },
                },
            )
            .expect("upsert model");
        let registry_snapshot = registry
            .apply(
                s2.revision,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        id: "profile-main".to_string(),
                        name: "Session CLI Test Profile".to_string(),
                        assignments: vec![RoleAssignment {
                            role: WorkflowRole::Follower,
                            difficulty: None,
                            choice: ModelChoice {
                                model_id: "model-codex".to_string(),
                                effort: Some("high".to_string()),
                            },
                        }],
                    },
                },
            )
            .expect("upsert profile");

        let draft = plans
            .change(
                0,
                PlanChange::SaveDraft {
                    plan_id: "plan-session-test".to_string(),
                    binding_id: BINDING_A.to_string(),
                    profile_id: "profile-main".to_string(),
                    profile_revision: registry_snapshot.revision,
                    summary: "Verify scope and digest ordering".to_string(),
                    tasks: vec![PlanTask {
                        id: "task-1".to_string(),
                        title: "Run unit check".to_string(),
                        instruction: "Execute focused check".to_string(),
                        role: WorkflowRole::Follower,
                        difficulty: Difficulty::Easy,
                        depends_on: Vec::new(),
                        allowed_write_paths: vec!["src/lib.rs".to_string()],
                    }],
                },
            )
            .expect("save draft");
        let published = plans
            .change(
                draft.revision,
                PlanChange::Publish {
                    plan_id: "plan-session-test".to_string(),
                    revision: draft.revision,
                },
            )
            .expect("publish plan");
        let compiled =
            compile_plan_revision(&published, &registry_snapshot).expect("compile published plan");

        let authority =
            AuthorityStore::open(&authority_dir, Arc::clone(&registry), Arc::clone(&plans))
                .expect("open authority");
        let approval = authority
            .approve(OperatorApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_A.to_string(),
                plan_id: published.plan_id,
                plan_revision: published.revision,
                expected_registry_revision: registry_snapshot.revision,
                expected_plan_digest: compiled.plan_digest,
                repository_path: repo_dir,
                base_commit,
                approved_task_ids: vec!["task-1".to_string()],
                approved_write_paths: vec!["src/lib.rs".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            })
            .expect("approve plan");

        verify_operator_approval(&authority, &approval.approval_id, BINDING_A, THREAD_A, None)
            .expect("matching scope without digest succeeds");
        verify_operator_approval(
            &authority,
            &approval.approval_id,
            BINDING_A,
            THREAD_A,
            Some(&approval.subject_digest),
        )
        .expect("matching scope and digest succeeds");

        let digest_err = verify_operator_approval(
            &authority,
            &approval.approval_id,
            BINDING_A,
            THREAD_A,
            Some(DIGEST_B),
        )
        .expect_err("wrong digest in matching scope must fail with subject_mismatch");
        assert_eq!(digest_err.code, "subject_mismatch");

        let foreign_binding_err = verify_operator_approval(
            &authority,
            &approval.approval_id,
            BINDING_B,
            THREAD_A,
            Some(DIGEST_B),
        )
        .expect_err("foreign binding must fail with scope_mismatch before digest comparison");
        assert_eq!(foreign_binding_err.code, "scope_mismatch");

        let foreign_thread_err = verify_operator_approval(
            &authority,
            &approval.approval_id,
            BINDING_A,
            THREAD_B,
            Some(DIGEST_B),
        )
        .expect_err("foreign thread must fail with scope_mismatch before digest comparison");
        assert_eq!(foreign_thread_err.code, "scope_mismatch");
    }

    #[test]
    fn error_mappings_remain_static_and_redacted() {
        let host_err = HostError::StaleRevision {
            run_id: "/secret/internal/path details that must not leak".to_string(),
            expected_revision: 1,
            actual_revision: 2,
        };
        let mapped_host = map_host_error(&host_err);
        assert_eq!(mapped_host.code, "stale_revision");
        assert!(!mapped_host.message.contains("/secret/internal/path"));

        let not_idle_err = HostError::HostNotIdle {
            approval_id: "apr_secret".to_string(),
            detail: "/secret/internal/path busy worker details".to_string(),
        };
        let mapped_not_idle = map_host_error(&not_idle_err);
        assert_eq!(mapped_not_idle.code, "host_not_idle");
        assert!(!mapped_not_idle.message.contains("/secret/internal/path"));

        let instance_err = HostError::InstanceMismatch {
            approval_id: "apr_secret".to_string(),
            expected_instance_id: "hst_0123456789abcdef0123456789abcdef".to_string(),
            actual_instance_id: Some("/secret/instance/detail".to_string()),
        };
        let mapped_instance = map_host_error(&instance_err);
        assert_eq!(mapped_instance.code, "host_instance_mismatch");
        assert!(!mapped_instance.message.contains("/secret/instance/detail"));

        let stop_uncertain_err = HostError::StopOutcomeUncertain {
            approval_id: "apr_secret".to_string(),
            detail: "/secret/stop/timeout details".to_string(),
        };
        let mapped_stop_uncertain = map_host_error(&stop_uncertain_err);
        assert_eq!(mapped_stop_uncertain.code, "host_stop_outcome_uncertain");
        assert!(!mapped_stop_uncertain
            .message
            .contains("/secret/stop/timeout"));

        let auth_err = AuthorityError::ApprovalNotFound {
            approval_id: "/secret/authority/path details that must not leak".to_string(),
        };
        let mapped_auth = map_authority_error(&auth_err);
        assert_eq!(mapped_auth.code, "approval_not_found");
        assert!(!mapped_auth.message.contains("/secret/authority/path"));

        let native_err = NativeError::new(
            NativeErrorCode::SecurityViolation,
            "/secret/native/path details that must not leak",
        );
        let mapped_native = map_native_error(&native_err);
        assert_eq!(mapped_native.code, "security_violation");
        assert!(!mapped_native.message.contains("/secret/native/path"));
    }

    #[test]
    fn session_read_vs_mutation_error_contracts_preserve_pre_effect_refusals_and_mark_ambiguous_mutations_outcome_unknown(
    ) {
        let secret = "/secret/provider/or/internal/raw/detail";
        let ambiguous_cases = [
            (
                HostError::MutationOutcomeUncertain {
                    operation: "dispatch_task".to_string(),
                    detail: secret.to_string(),
                },
                "outcome_unknown",
            ),
            (
                HostError::StopOutcomeUncertain {
                    approval_id: "apr_1".to_string(),
                    detail: secret.to_string(),
                },
                "host_stop_outcome_uncertain",
            ),
            (
                HostError::ScopeMismatch {
                    field: "session_binding_id".to_string(),
                    expected: "bnd_expected".to_string(),
                    actual: secret.to_string(),
                    reason: secret.to_string(),
                },
                "scope_mismatch",
            ),
            (
                HostError::CorruptManifest {
                    path: secret.to_string(),
                    reason: secret.to_string(),
                },
                "corrupt_manifest",
            ),
            (
                HostError::CorruptReceipts {
                    path: secret.to_string(),
                    reason: secret.to_string(),
                },
                "corrupt_receipts",
            ),
            (
                HostError::CorruptSessionState {
                    reason: secret.to_string(),
                },
                "corrupt_session_state",
            ),
            (
                HostError::Runtime {
                    code: "transport_error".to_string(),
                    message: secret.to_string(),
                },
                "runtime_error",
            ),
            (
                HostError::Io {
                    operation: "record_task_receipt".to_string(),
                    reason: secret.to_string(),
                },
                "io",
            ),
        ];

        for (err, expected_read_code) in &ambiguous_cases {
            let read_mapped = map_host_error(err);
            assert_eq!(read_mapped.code, *expected_read_code);
            assert!(!read_mapped.message.contains(secret));

            let mutation_mapped = map_host_mutation_error(err);
            assert_eq!(mutation_mapped.code, "outcome_unknown");
            assert!(
                mutation_mapped
                    .message
                    .contains("inspect session, task, or activity state"),
                "mutation outcome_unknown message must instruct inspection before retrying: {}",
                mutation_mapped.message
            );
            assert!(!mutation_mapped.message.contains(secret));
        }

        let refusal_cases = [
            (
                HostError::Validation {
                    field: "task_id".to_string(),
                    reason: secret.to_string(),
                },
                "validation",
            ),
            (
                HostError::ApprovalNotFound {
                    approval_id: secret.to_string(),
                },
                "approval_not_found",
            ),
            (
                HostError::UnknownTask {
                    approval_id: "apr_1".to_string(),
                    task_id: secret.to_string(),
                },
                "unknown_task",
            ),
            (
                HostError::DependencyInputsUnverified {
                    approval_id: "apr_1".to_string(),
                    task_id: "task_2".to_string(),
                    depends_on: vec![secret.to_string()],
                },
                "dependency_inputs_unverified",
            ),
            (
                HostError::NativeRouteRequired {
                    approval_id: "apr_1".to_string(),
                    task_id: "task_native".to_string(),
                    adapter: ToolAdapter::CodexCli,
                },
                "native_route_required",
            ),
            (
                HostError::TaskNotStarted {
                    approval_id: "apr_1".to_string(),
                    task_id: secret.to_string(),
                },
                "not_started",
            ),
            (
                HostError::UnpublishedOrSupersededPlan {
                    plan_id: secret.to_string(),
                    revision: 1,
                    status: PlanStatus::Draft,
                },
                "unpublished_or_superseded_plan",
            ),
            (
                HostError::LockBusy {
                    approval_id: "apr_1".to_string(),
                    session_dir: PathBuf::from(secret),
                },
                "lock_busy",
            ),
            (
                HostError::HostNotReady {
                    approval_id: "apr_1".to_string(),
                    phase: Some(HostPhase::Stopped),
                    reason: secret.to_string(),
                },
                "host_not_ready",
            ),
            (
                HostError::HostNotIdle {
                    approval_id: "apr_1".to_string(),
                    detail: secret.to_string(),
                },
                "host_not_idle",
            ),
            (
                HostError::InstanceMismatch {
                    approval_id: "apr_1".to_string(),
                    expected_instance_id: "hst_0123456789abcdef0123456789abcdef".to_string(),
                    actual_instance_id: Some(secret.to_string()),
                },
                "host_instance_mismatch",
            ),
            (
                HostError::RestartNotAllowed {
                    approval_id: "apr_1".to_string(),
                    phase: Some(HostPhase::Ready),
                    detail: secret.to_string(),
                },
                "host_restart_not_allowed",
            ),
            (
                HostError::StaleRevision {
                    run_id: secret.to_string(),
                    expected_revision: 1,
                    actual_revision: 2,
                },
                "stale_revision",
            ),
            (
                HostError::IdempotencyConflict {
                    binding_id: "bnd_1".to_string(),
                    command_key: secret.to_string(),
                },
                "idempotency_conflict",
            ),
            (
                HostError::InvalidRunState {
                    run_id: "run_1".to_string(),
                    action: "cancel".to_string(),
                    reason: secret.to_string(),
                },
                "invalid_run_state",
            ),
            (
                HostError::SecurityViolation {
                    path: secret.to_string(),
                    reason: secret.to_string(),
                },
                "security_violation",
            ),
            (
                HostError::Authority {
                    code: "plan_digest_mismatch".to_string(),
                    message: secret.to_string(),
                },
                "authority_error",
            ),
        ];

        for (err, expected_code) in &refusal_cases {
            let read_mapped = map_host_error(err);
            let mutation_mapped = map_host_mutation_error(err);
            assert_eq!(read_mapped.code, *expected_code);
            assert_eq!(mutation_mapped.code, *expected_code);
            assert_eq!(read_mapped.message, mutation_mapped.message);
            assert!(!read_mapped.message.contains(secret));
        }
    }

    #[test]
    fn cli_help_and_version_requests_classify_as_display_help_or_version() {
        for help_args in [
            vec!["threadlane-session", "--help"],
            vec!["threadlane-session", "-h"],
            vec!["threadlane-session", "session", "--help"],
            vec!["threadlane-session", "task", "--help"],
            vec!["threadlane-session", "dispatch", "--help"],
            vec!["threadlane-session", "cancel", "--help"],
            vec!["threadlane-session", "stop", "--help"],
            vec!["threadlane-session", "activity", "--help"],
            vec!["threadlane-session", "pull", "--help"],
            vec!["threadlane-session", "ack", "--help"],
        ] {
            let err = Cli::try_parse_from(&help_args)
                .expect_err("help invocation should return clap DisplayHelp");
            assert_eq!(
                err.kind(),
                clap::error::ErrorKind::DisplayHelp,
                "expected DisplayHelp for {help_args:?}"
            );
        }

        for version_args in [
            vec!["threadlane-session", "--version"],
            vec!["threadlane-session", "-V"],
        ] {
            let err = Cli::try_parse_from(&version_args)
                .expect_err("version invocation should return clap DisplayVersion");
            assert_eq!(
                err.kind(),
                clap::error::ErrorKind::DisplayVersion,
                "expected DisplayVersion for {version_args:?}"
            );
        }
    }
}
