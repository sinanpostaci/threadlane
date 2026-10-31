#![deny(unsafe_code)]
#![deny(missing_docs)]

//! Trusted local operator CLI for durable Threadlane role-flow cycles.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use threadlane_authority::validation::{validate_identifier, validate_sha256_hex};
use threadlane_authority::{AuthorityError, AuthorityStore};
use threadlane_host::{ApprovedHostClient, HostError};
use threadlane_native::operator_cli::{
    emit_error, read_request, validate_dirs, verify_binding, write_result, OperatorCliError,
};
use threadlane_native::role_flow::{
    self, CreateRoleFlowCycleRequest, ReconcileRoleHandoffRequest, ReservePackageClosureRequest,
    RoleFlowBindingSnapshot, RoleFlowCycle, RoleFlowStore, RoleHandoffRecord, RoleHandoffRequest,
};
use threadlane_native::{
    open_workflow_stores, ExecutionPolicy, NativeError, NativeErrorCode, PreflightReport,
    PreflightRequest,
};

/// Default number of role-flow cycle summary rows returned by `list`.
const DEFAULT_FLOW_LIST_LIMIT: usize = 20;

/// Maximum number of role-flow cycle summary rows returned by `list`.
const MAX_FLOW_LIST_LIMIT: usize = 20;

/// Maximum byte length accepted for a role-flow cycle pagination cursor.
const MAX_FLOW_CURSOR_LEN: usize = 128;

/// Static error message used when a request body names another binding/thread scope.
const REQUEST_SCOPE_MISMATCH: &str =
    "Role-flow request does not belong to the fixed CLI binding and thread scope.";

/// Static error message used when a mutation returned a record outside the fixed scope.
const MUTATION_SCOPE_MISMATCH: &str =
    "Role-flow mutation completed, but the returned record or reservation scope failed validation.";

/// Static error message used when read-only stored state names another binding/thread scope.
const STORED_SCOPE_MISMATCH: &str =
    "Role-flow stored record does not belong to the requested binding and thread scope.";

#[derive(Debug, Parser)]
#[command(
    name = "threadlane-flow",
    version,
    about = "Trusted local operator CLI for Threadlane role-flow cycles"
)]
struct Cli {
    /// Absolute path to the private (`0700`) Threadlane runtime state directory.
    #[arg(long = "state-dir", global = true, value_name = "ABS")]
    state_dir: Option<PathBuf>,

    /// Absolute path to the private (`0700`) model/role registry directory.
    #[arg(long = "registry-dir", global = true, value_name = "ABS")]
    registry_dir: Option<PathBuf>,

    /// Absolute path to the private (`0700`) scoped plan and role-flow store directory.
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

    /// Role-flow subcommand to execute.
    #[command(subcommand)]
    command: FlowCommand,
}

#[derive(Debug, Subcommand)]
enum FlowCommand {
    /// Evaluate read-only profile/role preflight against the canonical registry snapshot.
    Preflight {
        /// Absolute path to the private (`0600`) JSON [`PreflightRequest`] file.
        #[arg(long = "request", value_name = "ABS")]
        request: PathBuf,

        /// Absolute path to the private (`0600`) JSON [`ExecutionPolicy`] file.
        #[arg(long = "policy", value_name = "ABS")]
        policy: PathBuf,
    },
    /// Create a durable role-flow cycle from an existing frozen operator approval.
    Create {
        /// Absolute path to the private (`0600`) JSON [`CreateRoleFlowCycleRequest`] file.
        #[arg(long = "request", value_name = "ABS")]
        request: PathBuf,
    },
    /// Read one role-flow cycle in the fixed binding scope.
    Get {
        /// Role-flow cycle identifier (`flw_<32hex>`).
        #[arg(long = "cycle-id", value_name = "ID")]
        cycle_id: String,
    },
    /// List bounded role-flow cycle summary rows without prompts or instructions.
    List {
        /// Optional opaque pagination cursor (the last returned `cycle_id`).
        #[arg(long = "cursor", value_name = "TEXT")]
        cursor: Option<String>,

        /// Maximum number of cycle rows to return (`1..=20`, default `20`).
        #[arg(long = "limit", value_name = "N")]
        limit: Option<usize>,
    },
    /// Request a role handoff for one frozen task in an existing cycle.
    Handoff {
        /// Absolute path to the private (`0600`) JSON [`RoleHandoffRequest`] file.
        #[arg(long = "request", value_name = "ABS")]
        request: PathBuf,

        /// Lowercase 64-character SHA-256 digest to force into the request confirmation.
        #[arg(long = "confirm-approval", value_name = "SHA256")]
        confirm_approval: Option<String>,

        /// Explicit operator approval flag to force into the request.
        #[arg(long = "operator-approved")]
        operator_approved: bool,
    },
    /// Reconcile one recorded handoff against the actual approved task receipt.
    Reconcile {
        /// Absolute path to the private (`0600`) JSON [`ReconcileRoleHandoffRequest`] file.
        #[arg(long = "request", value_name = "ABS")]
        request: PathBuf,
    },
    /// Reserve single-use package closure for the cycle's actual approved base candidate.
    ReserveClosure {
        /// Absolute path to the private (`0600`) JSON [`ReservePackageClosureRequest`] file.
        #[arg(long = "request", value_name = "ABS")]
        request: PathBuf,
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

/// Minimal read-only view shared by request bodies and stored records that carry fixed scope.
trait FlowScoped {
    /// Returns the record's control-plane binding identifier.
    fn binding_id(&self) -> &str;

    /// Returns the record's canonical native thread UUID.
    fn thread_uuid(&self) -> &str;
}

impl FlowScoped for CreateRoleFlowCycleRequest {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for RoleHandoffRequest {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for ReconcileRoleHandoffRequest {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for ReservePackageClosureRequest {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for RoleFlowCycle {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for RoleHandoffRecord {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for role_flow::PackageClosureReservation {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

/// Bounded read-only cycle summary row. Never includes task instructions or prompt text.
#[derive(Debug, Clone, serde::Serialize)]
struct RoleFlowCycleSummary {
    cycle_id: String,
    cycle_revision: u64,
    approval_id: String,
    plan_id: String,
    plan_revision: u64,
    profile_id: String,
    registry_revision: u64,
    policy_id: String,
    policy_digest: String,
    task_count: usize,
    handoff_count: usize,
    closure_reservation_count: usize,
}

/// Bounded read-only page of cycle summary rows.
#[derive(Debug, serde::Serialize)]
struct RoleFlowCyclePage {
    binding_id: String,
    ledger_revision: u64,
    cycles: Vec<RoleFlowCycleSummary>,
    next_cursor: Option<String>,
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
                "Invalid CLI arguments for `threadlane-flow`.",
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
    let binding = globals.binding.clone();
    let thread = globals.thread.clone();

    // Every operational path authenticates the live control binding before touching request
    // bodies or store handles. Preflight deliberately remains below this line without opening
    // any workflow, authority, host, or role-flow store.
    verify_binding(&state_dir, &binding, &thread).await?;

    match cli.command {
        FlowCommand::Preflight { request, policy } => {
            let preflight_request: PreflightRequest = read_request(&request)?;
            let execution_policy: ExecutionPolicy = read_request(&policy)?;
            let report: PreflightReport = role_flow::preflight_from_dir_readonly(
                &registry_dir,
                &preflight_request,
                &execution_policy,
            )
            .map_err(|error| map_native_error(&error, false))?;
            write_result(&binding, &report, false)
        }
        FlowCommand::Create { request } => {
            let cycle_request: CreateRoleFlowCycleRequest = read_request(&request)?;
            ensure_scope(
                &cycle_request,
                &binding,
                &thread,
                "scope_mismatch",
                REQUEST_SCOPE_MISMATCH,
            )?;

            let authority = open_authority(&registry_dir, &plans_dir, &authority_dir)?;
            let store = open_flow_store(&plans_dir)?;
            let cycle = store
                .create_cycle(&cycle_request, &authority)
                .map_err(|error| map_native_error(&error, true))?;
            ensure_scope(
                &cycle,
                &binding,
                &thread,
                "outcome_unknown",
                MUTATION_SCOPE_MISMATCH,
            )?;
            write_result(&binding, &cycle, true)
        }
        FlowCommand::Get { cycle_id } => {
            validate_cycle_id(&cycle_id)?;
            let store = open_flow_store(&plans_dir)?;
            let cycle = store
                .get_cycle(&binding, &cycle_id)
                .map_err(|error| map_native_error(&error, false))?;
            let Some(cycle) = cycle else {
                return Err(OperatorCliError::new(
                    "cycle_not_found",
                    "Requested role-flow cycle was not found in this binding scope.",
                ));
            };
            ensure_scope(
                &cycle,
                &binding,
                &thread,
                "scope_mismatch",
                STORED_SCOPE_MISMATCH,
            )?;
            write_result(&binding, &cycle, false)
        }
        FlowCommand::List { cursor, limit } => {
            let (limit, cursor) = validate_list_args(limit, cursor)?;
            let store = open_flow_store(&plans_dir)?;
            let snapshot = store
                .get_binding_snapshot(&binding)
                .map_err(|error| map_native_error(&error, false))?;
            let page = build_cycle_page(snapshot, &binding, &thread, limit, cursor.as_deref())?;
            write_result(&binding, &page, false)
        }
        FlowCommand::Handoff {
            request,
            confirm_approval,
            operator_approved,
        } => {
            let mut handoff_request: RoleHandoffRequest = read_request(&request)?;
            ensure_scope(
                &handoff_request,
                &binding,
                &thread,
                "scope_mismatch",
                REQUEST_SCOPE_MISMATCH,
            )?;
            apply_handoff_confirmations(&mut handoff_request, operator_approved, confirm_approval)?;

            let authority = open_authority(&registry_dir, &plans_dir, &authority_dir)?;
            let client = open_client(Arc::clone(&authority), &sessions_dir, &binding, &thread)?;
            let store = open_flow_store(&plans_dir)?;
            let record = store
                .handoff_role(&handoff_request, &authority, Some(&client))
                .await
                .map_err(|error| map_native_error(&error, true))?;
            ensure_scope(
                &record,
                &binding,
                &thread,
                "outcome_unknown",
                MUTATION_SCOPE_MISMATCH,
            )?;
            write_result(&binding, &record, true)
        }
        FlowCommand::Reconcile { request } => {
            let reconcile_request: ReconcileRoleHandoffRequest = read_request(&request)?;
            ensure_scope(
                &reconcile_request,
                &binding,
                &thread,
                "scope_mismatch",
                REQUEST_SCOPE_MISMATCH,
            )?;

            let authority = open_authority(&registry_dir, &plans_dir, &authority_dir)?;
            let client = open_client(Arc::clone(&authority), &sessions_dir, &binding, &thread)?;
            let store = open_flow_store(&plans_dir)?;
            let record = store
                .reconcile_handoff(&reconcile_request, &client)
                .await
                .map_err(|error| map_native_error(&error, true))?;
            ensure_scope(
                &record,
                &binding,
                &thread,
                "outcome_unknown",
                MUTATION_SCOPE_MISMATCH,
            )?;
            write_result(&binding, &record, true)
        }
        FlowCommand::ReserveClosure { request } => {
            let closure_request: ReservePackageClosureRequest = read_request(&request)?;
            ensure_scope(
                &closure_request,
                &binding,
                &thread,
                "scope_mismatch",
                REQUEST_SCOPE_MISMATCH,
            )?;

            let store = open_flow_store(&plans_dir)?;
            let reservation = store
                .reserve_closure(&closure_request)
                .map_err(|error| map_native_error(&error, true))?;
            ensure_scope(
                &reservation,
                &binding,
                &thread,
                "outcome_unknown",
                MUTATION_SCOPE_MISMATCH,
            )?;
            write_result(&binding, &reservation, true)
        }
    }
}

fn ensure_scope<T: FlowScoped>(
    value: &T,
    expected_binding_id: &str,
    expected_thread_uuid: &str,
    code: &'static str,
    message: &'static str,
) -> Result<(), OperatorCliError> {
    if value.binding_id() != expected_binding_id || value.thread_uuid() != expected_thread_uuid {
        return Err(OperatorCliError::new(code, message));
    }
    Ok(())
}

fn validate_cycle_id(value: &str) -> Result<(), OperatorCliError> {
    validate_identifier("cycle_id", value)
        .map(|_| ())
        .map_err(|_| OperatorCliError::invalid_input("Invalid `--cycle-id` syntax."))
}

fn validate_list_args(
    limit: Option<usize>,
    cursor: Option<String>,
) -> Result<(usize, Option<String>), OperatorCliError> {
    let effective_limit = limit.unwrap_or(DEFAULT_FLOW_LIST_LIMIT);
    if !(1..=MAX_FLOW_LIST_LIMIT).contains(&effective_limit) {
        return Err(OperatorCliError::invalid_input(
            "`--limit` must be between 1 and 20.",
        ));
    }

    let clean_cursor = match cursor {
        Some(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty()
                || trimmed != raw
                || trimmed.len() > MAX_FLOW_CURSOR_LEN
                || trimmed.chars().any(char::is_control)
            {
                return Err(OperatorCliError::invalid_input("Invalid `--cursor` value."));
            }
            validate_identifier("cursor", trimmed)
                .map_err(|_| OperatorCliError::invalid_input("Invalid `--cursor` value."))?;
            Some(trimmed.to_string())
        }
        None => None,
    };

    Ok((effective_limit, clean_cursor))
}

fn validate_confirm_approval(value: &str) -> Result<String, OperatorCliError> {
    validate_sha256_hex("confirm_approval", value).map_err(|_| {
        OperatorCliError::invalid_input(
            "`--confirm-approval` must be a 64-character lowercase hexadecimal SHA-256 digest.",
        )
    })
}

fn apply_handoff_confirmations(
    request: &mut RoleHandoffRequest,
    operator_approved: bool,
    confirm_approval: Option<String>,
) -> Result<(), OperatorCliError> {
    // These two fields are attacker-controlled request data. Always replace, never merge, them
    // with the concrete CLI flags so a private JSON body cannot self-grant approval.
    request.operator_approved = operator_approved;
    request.confirm_approval_digest = match confirm_approval {
        Some(ref raw) => Some(validate_confirm_approval(raw)?),
        None => None,
    };
    Ok(())
}

fn build_cycle_page(
    snapshot: RoleFlowBindingSnapshot,
    expected_binding_id: &str,
    expected_thread_uuid: &str,
    limit: usize,
    cursor: Option<&str>,
) -> Result<RoleFlowCyclePage, OperatorCliError> {
    if snapshot.binding_id != expected_binding_id {
        return Err(OperatorCliError::new(
            "corrupt_flow_state",
            "Role-flow ledger binding identity failed validation.",
        ));
    }
    if snapshot.closures.iter().any(|reservation| {
        reservation.binding_id != expected_binding_id
            || reservation.thread_uuid != expected_thread_uuid
    }) {
        return Err(OperatorCliError::new(
            "corrupt_flow_state",
            "Role-flow ledger contains a closure reservation outside the requested binding and thread scope.",
        ));
    }

    let mut summaries = Vec::with_capacity(snapshot.cycles.len());
    for cycle in snapshot.cycles.iter().rev() {
        if cycle.binding_id != expected_binding_id || cycle.thread_uuid != expected_thread_uuid {
            return Err(OperatorCliError::new(
                "corrupt_flow_state",
                "Role-flow ledger contains a cycle outside the requested binding and thread scope.",
            ));
        }
        let closure_reservation_count = snapshot
            .closures
            .iter()
            .filter(|reservation| reservation.cycle_id == cycle.cycle_id)
            .count();
        summaries.push(RoleFlowCycleSummary {
            cycle_id: cycle.cycle_id.clone(),
            cycle_revision: cycle.cycle_revision,
            approval_id: cycle.approval_id.clone(),
            plan_id: cycle.plan_id.clone(),
            plan_revision: cycle.plan_revision,
            profile_id: cycle.profile_id.clone(),
            registry_revision: cycle.registry_revision,
            policy_id: cycle.policy.policy_id.clone(),
            policy_digest: cycle.policy_digest.clone(),
            task_count: cycle.tasks.len(),
            handoff_count: cycle.handoffs.len(),
            closure_reservation_count,
        });
    }

    let start = match cursor {
        Some(cursor) => {
            let position = summaries
                .iter()
                .position(|summary| summary.cycle_id == cursor)
                .ok_or_else(|| {
                    OperatorCliError::invalid_input(
                        "`--cursor` was not found in this binding's role-flow cycle list.",
                    )
                })?;
            position + 1
        }
        None => 0,
    };
    let end = start.saturating_add(limit).min(summaries.len());
    let cycles: Vec<RoleFlowCycleSummary> = summaries[start..end].to_vec();
    let next_cursor = if end < summaries.len() {
        cycles.last().map(|summary| summary.cycle_id.clone())
    } else {
        None
    };

    Ok(RoleFlowCyclePage {
        binding_id: expected_binding_id.to_string(),
        ledger_revision: snapshot.ledger_revision,
        cycles,
        next_cursor,
    })
}

fn open_flow_store(plans_dir: &Path) -> Result<RoleFlowStore, OperatorCliError> {
    RoleFlowStore::new(plans_dir).map_err(|error| map_native_error(&error, false))
}

fn open_authority(
    registry_dir: &Path,
    plans_dir: &Path,
    authority_dir: &Path,
) -> Result<Arc<AuthorityStore>, OperatorCliError> {
    let Some((registry, plans)) = open_workflow_stores(Some(registry_dir), Some(plans_dir))
        .map_err(|error| map_native_error(&error, false))?
    else {
        return Err(OperatorCliError::invalid_input(
            "Workflow registry and plan stores could not be opened.",
        ));
    };
    let authority = AuthorityStore::open(authority_dir, Arc::clone(&registry), Arc::clone(&plans))
        .map_err(|error| map_authority_error(&error))?;
    Ok(Arc::new(authority))
}

fn open_client(
    authority: Arc<AuthorityStore>,
    sessions_dir: &Path,
    binding_id: &str,
    thread_uuid: &str,
) -> Result<ApprovedHostClient, OperatorCliError> {
    ApprovedHostClient::new(authority, sessions_dir, binding_id, thread_uuid)
        .map_err(|error| map_host_error(&error))
}

fn map_native_error(error: &NativeError, mutation: bool) -> OperatorCliError {
    let code = error.error_code();
    if mutation
        && matches!(
            code,
            NativeErrorCode::AmbiguousTransportFailure
                | NativeErrorCode::TimedOut
                | NativeErrorCode::IoError
        )
    {
        return OperatorCliError::new(
            "outcome_unknown",
            "Role-flow mutation outcome is uncertain after a timeout, I/O, or transport failure; inspect cycle and handoff state before retrying.",
        );
    }

    let message = match code {
        NativeErrorCode::SecurityViolation => {
            "Role-flow store path or private request failed security checks."
        }
        NativeErrorCode::UnsupportedPlatform => {
            "threadlane-flow is only supported on Unix platforms."
        }
        NativeErrorCode::ValidationFailed | NativeErrorCode::InvalidConfig => {
            "Role-flow request or persisted state failed validation."
        }
        NativeErrorCode::IoError => "Filesystem I/O error while accessing role-flow state.",
        NativeErrorCode::OutputOverflow => "Role-flow output exceeded the bounded size limit.",
        NativeErrorCode::UnsupportedCapability | NativeErrorCode::NeedsVerification => {
            "Requested role-flow capability is not supported or still requires verification."
        }
        NativeErrorCode::NotConfigured
        | NativeErrorCode::WorkflowNotConfigured
        | NativeErrorCode::HostNotConfigured => {
            "Required role-flow stores or approved host client are not configured."
        }
        NativeErrorCode::TimedOut => "Role-flow operation timed out before completion.",
        NativeErrorCode::AmbiguousTransportFailure => {
            "Role-flow operation may have crossed an execution boundary; inspect state before retrying."
        }
        NativeErrorCode::TransportUnavailable => "Role-flow host transport is unavailable.",
        NativeErrorCode::ProtocolError
        | NativeErrorCode::RpcError
        | NativeErrorCode::McpToolError => {
            "Role-flow host or provider protocol returned an invalid response."
        }
        NativeErrorCode::ThreadNotFound => "Target native thread was not found.",
        _ => "Role-flow operation failed with a sanitized native error.",
    };
    OperatorCliError::new(code.as_str(), message)
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
    OperatorCliError::new(
        error.code(),
        "Approved host client could not be opened for the role-flow operation.",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use threadlane_native::role_flow::ClosureSubjectSource;

    const BINDING: &str = "bnd_flow_unit_test";
    const THREAD: &str = "0196ed00-0000-7000-8000-0000000000f1";
    const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn handoff_request() -> RoleHandoffRequest {
        RoleHandoffRequest {
            binding_id: BINDING.to_string(),
            thread_uuid: THREAD.to_string(),
            cycle_id: "flw_0123456789abcdef0123456789abcdef".to_string(),
            expected_cycle_revision: 1,
            request_key: "handoff-unit-test".to_string(),
            task_id: "task-1".to_string(),
            current_policy: ExecutionPolicy::deny_all("policy-unit-test")
                .expect("valid deny-all policy"),
            operator_approved: true,
            confirm_approval_digest: Some(DIGEST.to_string()),
            closure_reservation_id: Some("cls_0123456789abcdef0123456789abcdef".to_string()),
        }
    }

    #[test]
    fn cli_parses_global_flags_before_and_after_subcommand() {
        let before = Cli::try_parse_from([
            "threadlane-flow",
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
            BINDING,
            "--thread",
            THREAD,
            "list",
            "--limit",
            "5",
        ])
        .expect("parse global flags before subcommand");
        let globals_before = GlobalArgs::from_cli(&before).expect("extract globals before");
        assert_eq!(globals_before.binding, BINDING);
        assert_eq!(globals_before.thread, THREAD);

        let after = Cli::try_parse_from([
            "threadlane-flow",
            "reserve-closure",
            "--request",
            "/tmp/request.json",
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
            BINDING,
            "--thread",
            THREAD,
        ])
        .expect("parse global flags after subcommand");
        let globals_after = GlobalArgs::from_cli(&after).expect("extract globals after");
        assert_eq!(globals_after.binding, BINDING);
        assert_eq!(globals_after.thread, THREAD);
        assert!(matches!(after.command, FlowCommand::ReserveClosure { .. }));
    }

    #[test]
    fn handoff_flags_replace_json_self_approval_and_preserve_closure() {
        let mut request = handoff_request();
        apply_handoff_confirmations(&mut request, false, None)
            .expect("apply non-approving CLI flags");
        assert!(!request.operator_approved);
        assert!(request.confirm_approval_digest.is_none());
        assert_eq!(
            request.closure_reservation_id.as_deref(),
            Some("cls_0123456789abcdef0123456789abcdef")
        );

        apply_handoff_confirmations(&mut request, true, Some(DIGEST.to_string()))
            .expect("apply explicit CLI approvals");
        assert!(request.operator_approved);
        assert_eq!(request.confirm_approval_digest.as_deref(), Some(DIGEST));

        let err = apply_handoff_confirmations(&mut request, true, Some("not-a-digest".to_string()))
            .expect_err("invalid confirmation digest must fail");
        assert_eq!(err.code, "invalid_input");
    }

    #[test]
    fn list_args_reject_zero_out_of_range_and_hostile_cursor() {
        assert_eq!(
            validate_list_args(Some(0), None)
                .expect_err("zero limit must fail")
                .code,
            "invalid_input"
        );
        assert_eq!(
            validate_list_args(Some(MAX_FLOW_LIST_LIMIT + 1), None)
                .expect_err("oversized limit must fail")
                .code,
            "invalid_input"
        );
        assert_eq!(
            validate_list_args(None, Some("bad\ncursor".to_string()))
                .expect_err("control character cursor must fail")
                .code,
            "invalid_input"
        );
        assert_eq!(
            validate_list_args(None, Some(format!(" {}", "a".repeat(4))))
                .expect_err("padded cursor must fail")
                .code,
            "invalid_input"
        );
        let (limit, cursor) = validate_list_args(None, None).expect("default list args");
        assert_eq!(limit, DEFAULT_FLOW_LIST_LIMIT);
        assert!(cursor.is_none());
    }

    #[test]
    fn closure_source_shape_is_serializable_for_private_requests() {
        let source = ClosureSubjectSource::ApprovedCycleBase {
            subject_digest: DIGEST.to_string(),
            expected_candidate_tree_sha: None,
        };
        let json = serde_json::to_string(&source).expect("serialize closure source");
        assert!(json.contains("approved_cycle_base"));
    }
}
