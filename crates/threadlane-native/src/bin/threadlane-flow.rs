#![deny(unsafe_code)]
#![deny(missing_docs)]

//! Trusted local operator CLI for durable Threadlane role-flow cycles, standing read-only
//! planning grants, and frozen Coworker controller execution envelopes (`coworker-execution
//! preview|record|inspect`).
//!
//! - `coworker-execution preview`: pure validation of [`FreezeControllerExecutionEnvelopeRequest`]
//!   against the live [`CoworkerJobRecord`], [`AuthorityStore`], [`PlanStore`], and [`RegistryStore`]
//!   without mutating controller state, starting workers, or running Git effects.
//! - `coworker-execution record`: requires explicit CLI `--operator-approved`, `--confirm-approval`,
//!   and `--confirm-envelope` flags matching the previewed `approval_subject_digest` and full
//!   `envelope_digest`; never trusts JSON self-attestation or MCP prose, and local operator
//!   attestation is not cryptographic proof of human intent or per-call native chat identity.
//! - `coworker-execution inspect`: read-only lookup of the stored [`ControllerJobRuntimeState`]
//!   without creating files, ACKs, or dispatches. Automatic controller scheduling/driving and
//!   paired MCP wiring are separate downstream owners.
//! - `external-resolution preview|record|inspect`: operator-attested receipt evidence over
//!   `external_resolution_receipts_v1`. Preview/inspect are pure; record requires independent
//!   CLI `--operator-approved`, `--confirm-thread`, and `--confirm-preview` flags, and
//!   `--reconcile-existing` establishes durability only for the original existing bytes.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use threadlane_authority::validation::{
    validate_canonical_thread_uuid, validate_identifier, validate_sha256_hex,
};
use threadlane_authority::{AuthorityError, AuthorityStore};
use threadlane_core::observation::{
    append_journal_record, current_observation_time_ms, current_process_instance,
    generate_observation_id, read_env_parent_request_id, read_env_plan_id, ObservationActor,
    ObservationCallState, ObservationDelivery, ObservationDeliveryStage, ObservationExecutor,
    ObservationJournalRecord, ObservationRecordPhase,
};
use threadlane_host::{ApprovedHostClient, HostError};
use threadlane_native::coworker_admission::{
    AdmissionConfirmation, AdmissionReceipt, AdmissionRequest, CoworkerAdmissionService,
};
use threadlane_native::coworker_driver::{
    CoworkerCodeDriver, CoworkerDriverCancelReport, CoworkerDriverEnsureReport,
    CoworkerDriverExecutionMode, CoworkerDriverRequest, CoworkerDriverRunOptions,
    CoworkerDriverStatusReport, DEFAULT_DRIVER_MAX_STEPS, DEFAULT_DRIVER_POLL_INTERVAL_MS,
    DEFAULT_ENSURE_OWNER_IDLE_LINGER_MS,
};
use threadlane_native::coworker_grants::{
    get_planning_grant_summary, list_planning_grant_summaries,
    prepare_planning_grant_preview_from_dir_readonly, prepare_planning_grant_preview_with_store,
    record_planning_grant_from_registry_dir, revoke_planning_grant_scoped,
    LocalOperatorGrantAttestation, PlanningGrantPreview, PlanningGrantSummary,
    PreparePlanningGrantRequest, RecordPlanningGrantRequest, TrustedGrantScope,
};
use threadlane_native::coworker_workflow::{
    ControllerConfigurationPreview, ControllerJobRuntimeState, ControllerPolicyStore,
    CoworkerJobRecord, CoworkerWorkflowStore, FreezeControllerExecutionEnvelopeRequest,
    FrozenControllerExecutionEnvelope, RevokePlanningGrantRequest,
};
use threadlane_native::external_resolution::{
    ExternalResolutionConfirmation, ExternalResolutionInspection, ExternalResolutionPreview,
    ExternalResolutionReceipt, ExternalResolutionService, ReconcileExternalResolutionRequest,
    RecordExternalResolutionRequest,
};
use threadlane_native::operator_cli::{
    emit_error, read_request, validate_dirs, verify_binding, write_result, OperatorCliError,
};
use threadlane_native::planning_commands::EnqueuePlanningCommandRequest;
use threadlane_native::planning_driver::{
    enqueue_planning_command_summary, get_planning_command_summary_readonly,
    list_planning_command_summaries_readonly, open_planning_driver_for_plans_dir,
    start_planning_command_detached, PlanningCommandExecutionSummary, PlanningCommandSummary,
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

    /// Canonical project root injected by `threadlane-context` for scoped operator commands.
    #[arg(long = "project-root", global = true, value_name = "ABS")]
    project_root: Option<PathBuf>,

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
    /// Prepare a read-only planning/advice grant preview without mutating stores or locks.
    GrantPrepare {
        /// Absolute path to the private (`0600`) JSON [`PreparePlanningGrantRequest`] file.
        #[arg(long = "request", value_name = "ABS")]
        request: PathBuf,
    },
    /// Record an operator-confirmed planning/advice grant from a prepared preview.
    GrantRecord {
        /// Absolute path to the private (`0600`) JSON [`RecordPlanningGrantRequest`] file.
        #[arg(long = "request", value_name = "ABS")]
        request: PathBuf,

        /// Lowercase 64-character SHA-256 preview digest confirmed by the operator.
        #[arg(long = "confirm-preview", value_name = "SHA256")]
        confirm_preview: Option<String>,

        /// Explicit operator approval flag required to record the grant.
        #[arg(long = "operator-approved")]
        operator_approved: bool,
    },
    /// Revoke an existing planning/advice grant in the fixed binding scope.
    GrantRevoke {
        /// Absolute path to the private (`0600`) JSON [`RevokePlanningGrantRequest`] file.
        #[arg(long = "request", value_name = "ABS")]
        request: PathBuf,
    },
    /// List metadata-only planning/advice grant summaries in the fixed binding scope.
    GrantList,
    /// Read one metadata-only planning/advice grant summary in the fixed binding scope.
    GrantGet {
        /// Planning grant identifier (`pgr_<32hex>`).
        #[arg(long = "grant-id", value_name = "ID")]
        grant_id: String,
    },
    /// Preview, record, or inspect a frozen Coworker controller execution envelope (preview | record | inspect).
    #[command(
        subcommand,
        name = "coworker-execution",
        visible_alias = "execution-envelope"
    )]
    CoworkerExecution(CoworkerExecutionCommand),
    /// Admit an exact published job with explicit local consent; never launches execution.
    #[command(subcommand, name = "coworker-admission")]
    CoworkerAdmission(CoworkerAdmissionCommand),
    /// Enqueue, inspect, or execute durable Coworker planning commands (enqueue | get | list | execute-command | start-command).
    #[command(subcommand, name = "coworker-planning")]
    CoworkerPlanning(CoworkerPlanningCommand),
    /// Run, ensure, inspect status, reconcile, or cancel an independent Coworker code controller owner (run-owner | ensure | status | reconcile | cancel).
    #[command(subcommand, name = "code-driver", visible_alias = "coworker-driver")]
    CodeDriver(CoworkerDriverCommand),
    /// Preview, record, inspect, or reconcile an operator-attested external resolution receipt.
    #[command(
        subcommand,
        name = "external-resolution",
        visible_alias = "external_resolution"
    )]
    ExternalResolution(ExternalResolutionCommand),
}

#[derive(Debug, Subcommand)]
enum ExternalResolutionCommand {
    /// Pure read-only preview of an external resolution request without creating journal, lock, or cache state.
    Preview {
        /// Absolute path to the private (`0600`) JSON [`RecordExternalResolutionRequest`] file.
        #[arg(long = "request", value_name = "ABS")]
        request: PathBuf,
    },
    /// Record operator-confirmed receipt evidence; `--reconcile-existing` only syncs original existing bytes.
    Record {
        /// Absolute path to the private (`0600`) JSON [`RecordExternalResolutionRequest`] file.
        #[arg(long = "request", value_name = "ABS")]
        request: PathBuf,

        /// Exact canonical native thread UUID confirmed independently by the operator.
        #[arg(
            long = "confirm-thread",
            visible_alias = "confirm-thread-uuid",
            value_name = "UUID"
        )]
        confirm_thread: Option<String>,

        /// Exact 64-character lowercase SHA-256 preview digest confirmed by the operator.
        #[arg(long = "confirm-preview", value_name = "SHA256")]
        confirm_preview: Option<String>,

        /// Explicit trusted-local operator approval flag required to record or reconcile.
        #[arg(long = "operator-approved")]
        operator_approved: bool,

        /// Reconcile durability for the original existing receipt bytes instead of appending.
        #[arg(long = "reconcile-existing")]
        reconcile_existing: bool,
    },
    /// Pure read-only inspection of stored receipts; optionally focus one original request key.
    Inspect {
        /// Optional original external resolution request key (`<= 128` identifier bytes).
        #[arg(long = "request-key", value_name = "ID")]
        request_key: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum CoworkerDriverCommand {
    /// Run the exclusive physical owner loop for a Coworker job in the fixed binding and thread scope.
    #[command(name = "run-owner")]
    RunOwner {
        /// Optional absolute path to a private (`0600`) JSON [`CoworkerDriverRequest`] file.
        #[arg(long = "request", value_name = "ABS")]
        request: Option<PathBuf>,

        /// Target Coworker job identifier (`job_<32hex>`).
        #[arg(long = "job-id", value_name = "ID")]
        job_id: Option<String>,

        /// Optional maximum controller step iterations (`1..=4096`).
        #[arg(long = "max-steps", value_name = "N")]
        max_steps: Option<u32>,

        /// Optional finite cadence interval in milliseconds (`1..=60000`).
        #[arg(long = "poll-interval-ms", value_name = "MS")]
        poll_interval_ms: Option<u64>,

        /// Optional bounded linger window in milliseconds after reaching a quiescent state (`0..=60000`).
        #[arg(long = "idle-linger-ms", value_name = "MS")]
        idle_linger_ms: Option<u64>,

        /// Optional explicit execution mode (`full_automatic_pipeline` or `phase1_initial_dispatch_only`).
        #[arg(long = "execution-mode", value_name = "MODE")]
        execution_mode: Option<String>,

        /// Explicit diagnostic flag selecting `phase1_initial_dispatch_only` mode.
        #[arg(long = "initial-dispatch-only")]
        initial_dispatch_only: bool,
    },
    /// Ensure a single live verified physical owner for a Coworker job, reusing an active owner or spawning detached `run-owner`.
    #[command(name = "ensure")]
    Ensure {
        /// Optional absolute path to a private (`0600`) JSON [`CoworkerDriverRequest`] file.
        #[arg(long = "request", value_name = "ABS")]
        request: Option<PathBuf>,

        /// Target Coworker job identifier (`job_<32hex>`).
        #[arg(long = "job-id", value_name = "ID")]
        job_id: Option<String>,

        /// Optional maximum controller step iterations (`1..=4096`).
        #[arg(long = "max-steps", value_name = "N")]
        max_steps: Option<u32>,

        /// Optional finite cadence interval in milliseconds (`1..=60000`).
        #[arg(long = "poll-interval-ms", value_name = "MS")]
        poll_interval_ms: Option<u64>,

        /// Optional bounded linger window in milliseconds for a spawned owner (`0..=60000`).
        #[arg(long = "idle-linger-ms", value_name = "MS")]
        idle_linger_ms: Option<u64>,

        /// Optional explicit execution mode (`full_automatic_pipeline` or `phase1_initial_dispatch_only`).
        #[arg(long = "execution-mode", value_name = "MODE")]
        execution_mode: Option<String>,

        /// Explicit diagnostic flag selecting `phase1_initial_dispatch_only` mode.
        #[arg(long = "initial-dispatch-only")]
        initial_dispatch_only: bool,
    },
    /// Inspect verified live owner or persisted status for a Coworker job without mutating state.
    #[command(name = "status")]
    Status {
        /// Optional absolute path to a private (`0600`) JSON [`CoworkerDriverRequest`] file.
        #[arg(long = "request", value_name = "ABS")]
        request: Option<PathBuf>,

        /// Target Coworker job identifier (`job_<32hex>`).
        #[arg(long = "job-id", value_name = "ID")]
        job_id: Option<String>,
    },
    /// Reconcile existing host receipts and step state for a Coworker job without replaying unknown effects.
    #[command(name = "reconcile")]
    Reconcile {
        /// Optional absolute path to a private (`0600`) JSON [`CoworkerDriverRequest`] file.
        #[arg(long = "request", value_name = "ABS")]
        request: Option<PathBuf>,

        /// Target Coworker job identifier (`job_<32hex>`).
        #[arg(long = "job-id", value_name = "ID")]
        job_id: Option<String>,

        /// Optional explicit execution mode (`full_automatic_pipeline` or `phase1_initial_dispatch_only`).
        #[arg(long = "execution-mode", value_name = "MODE")]
        execution_mode: Option<String>,

        /// Explicit diagnostic flag selecting `phase1_initial_dispatch_only` mode.
        #[arg(long = "initial-dispatch-only")]
        initial_dispatch_only: bool,
    },
    /// Record durable `stop_requested` first, then perform narrow `ApprovedHostClient` task cancellation and reconcile.
    #[command(name = "cancel")]
    Cancel {
        /// Optional absolute path to a private (`0600`) JSON [`CoworkerDriverRequest`] file.
        #[arg(long = "request", value_name = "ABS")]
        request: Option<PathBuf>,

        /// Target Coworker job identifier (`job_<32hex>`).
        #[arg(long = "job-id", value_name = "ID")]
        job_id: Option<String>,

        /// Optional cancellation reason recorded in controller runtime state and host task cancellation.
        #[arg(long = "reason", value_name = "TEXT")]
        reason: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum CoworkerPlanningCommand {
    /// Validate and enqueue a durable planning command under an existing active planning grant.
    Enqueue {
        /// Absolute path to the private (`0600`) JSON [`EnqueuePlanningCommandRequest`] file.
        #[arg(long = "request", value_name = "ABS")]
        request: PathBuf,
    },
    /// Read one prompt-redacted planning command summary in the fixed binding and thread scope.
    Get {
        /// Planning command identifier (`pcm_<32hex>`).
        #[arg(long = "command-id", value_name = "ID")]
        command_id: String,
    },
    /// List bounded prompt-redacted planning command summary rows in the fixed binding and thread scope.
    List {
        /// Optional opaque pagination cursor (the last returned `command_id`).
        #[arg(long = "cursor", value_name = "TEXT")]
        cursor: Option<String>,

        /// Maximum number of command rows to return (`1..=20`, default `20`).
        #[arg(long = "limit", value_name = "N")]
        limit: Option<usize>,
    },
    /// Execute or reconcile one durable planning command synchronously in the owned finite driver.
    #[command(name = "execute-command")]
    ExecuteCommand {
        /// Planning command identifier (`pcm_<32hex>`).
        #[arg(long = "command-id", value_name = "ID")]
        command_id: String,
    },
    /// Validate and launch a detached finite `execute-command` subprocess of the canonical binary.
    #[command(name = "start-command")]
    StartCommand {
        /// Planning command identifier (`pcm_<32hex>`).
        #[arg(long = "command-id", value_name = "ID")]
        command_id: String,
    },
}

#[derive(Debug, Subcommand)]
enum CoworkerAdmissionCommand {
    /// Pure normalized consent preview for an existing published, optionally unbound job.
    Preview {
        /// Absolute private (0600) AdmissionRequest JSON file.
        #[arg(long)]
        request: PathBuf,
    },
    /// Persist exact consent only; does not start a host, provider, check or Git operation.
    Record {
        /// Absolute private (0600) AdmissionRequest JSON file.
        #[arg(long)]
        request: PathBuf,
        /// Explicit trusted-local attestation; JSON cannot supply this authorization.
        #[arg(long)]
        operator_approved: bool,
        /// Exact normalized admission preview digest.
        #[arg(long)]
        confirm_preview: Option<String>,
        /// Exact existing authority subject digest.
        #[arg(long)]
        confirm_approval: Option<String>,
        /// Exact full frozen execution envelope digest.
        #[arg(long)]
        confirm_envelope: Option<String>,
    },
    /// Pure historical inspection, including partial/uncertain persistence and original consent.
    Inspect {
        /// Existing job identifier in the fixed context scope.
        #[arg(long)]
        job_id: String,
    },
    /// Explicit persistence recovery using the original request key and immutable subjects.
    Reconcile {
        /// Existing job identifier in the fixed context scope.
        #[arg(long)]
        job_id: String,
        /// Original admission request key; a new key cannot escape a conflict.
        #[arg(long)]
        request_key: String,
    },
}

#[derive(Debug, Subcommand)]
enum CoworkerExecutionCommand {
    /// Preview a read-only frozen controller execution envelope without mutating state.
    Preview {
        /// Absolute path to the private (`0600`) JSON [`FreezeControllerExecutionEnvelopeRequest`] file.
        #[arg(long = "request", value_name = "ABS")]
        request: PathBuf,
    },
    /// Record an operator-attested frozen controller execution envelope for an approved Coworker job.
    Record {
        /// Absolute path to the private (`0600`) JSON [`FreezeControllerExecutionEnvelopeRequest`] file.
        #[arg(long = "request", value_name = "ABS")]
        request: PathBuf,

        /// Lowercase 64-character SHA-256 `approval_subject_digest` confirmed by the operator.
        #[arg(
            long = "confirm-approval",
            visible_alias = "confirm-approval-subject-digest",
            value_name = "SHA256"
        )]
        confirm_approval: Option<String>,

        /// Lowercase 64-character SHA-256 preview `envelope_digest` confirmed by the operator.
        #[arg(
            long = "confirm-envelope",
            visible_alias = "confirm-envelope-digest",
            value_name = "SHA256"
        )]
        confirm_envelope: Option<String>,

        /// Explicit local operator attestation flag required to record the execution envelope.
        #[arg(long = "operator-approved", visible_alias = "operator-attested")]
        operator_approved: bool,
    },
    /// Read the stored controller execution envelope and runtime configuration for a job without mutating state.
    Inspect {
        /// Coworker job identifier (`job_<32hex>`).
        #[arg(long = "job-id", value_name = "ID")]
        job_id: String,
    },
}

#[derive(Debug, Clone)]
struct GlobalArgs {
    state_dir: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    project_root: Option<PathBuf>,
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
            project_root: cli.project_root.clone(),
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

impl FlowScoped for PreparePlanningGrantRequest {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for PlanningGrantPreview {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for PlanningGrantSummary {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for RevokePlanningGrantRequest {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for FreezeControllerExecutionEnvelopeRequest {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for FrozenControllerExecutionEnvelope {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for ControllerConfigurationPreview {
    fn binding_id(&self) -> &str {
        &self.envelope.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.envelope.thread_uuid
    }
}

impl FlowScoped for CoworkerJobRecord {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for RecordExternalResolutionRequest {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for ExternalResolutionPreview {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for ExternalResolutionReceipt {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for ExternalResolutionInspection {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for EnqueuePlanningCommandRequest {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for PlanningCommandSummary {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for CoworkerDriverRequest {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for CoworkerDriverStatusReport {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

impl FlowScoped for CoworkerDriverEnsureReport {
    fn binding_id(&self) -> &str {
        &self.status.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.status.thread_uuid
    }
}

impl FlowScoped for CoworkerDriverCancelReport {
    fn binding_id(&self) -> &str {
        &self.binding_id
    }

    fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }
}

/// Deserialized private JSON body for `grant-record` before CLI flag enforcement.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantRecordInput {
    request_key: String,
    #[serde(default)]
    expected_ledger_revision: Option<u64>,
    preview: PlanningGrantPreview,
    #[serde(default)]
    confirmed_preview_digest: Option<String>,
    #[serde(default)]
    attestation: Option<LocalOperatorGrantAttestation>,
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
    let project_root = match globals.project_root.as_deref() {
        Some(path) => Some(validate_project_root_override(path)?),
        None => None,
    };
    let binding = globals.binding.clone();
    let thread = globals.thread.clone();

    // Every operational path authenticates the live control binding before touching request
    // bodies or store handles. Preflight deliberately remains below this line without opening
    // any workflow, authority, host, or role-flow store.
    verify_binding(&state_dir, &binding, &thread).await?;

    let (op_name, is_mutating, allow_parent_journal) = match &cli.command {
        FlowCommand::Preflight { .. } => ("flow.preflight", false, true),
        FlowCommand::Create { .. } => ("flow.create", true, true),
        FlowCommand::Get { .. } => ("flow.get", false, true),
        FlowCommand::List { .. } => ("flow.list", false, true),
        FlowCommand::Handoff { .. } => ("flow.handoff", true, true),
        FlowCommand::Reconcile { .. } => ("flow.reconcile", true, true),
        FlowCommand::ReserveClosure { .. } => ("flow.reserve-closure", true, true),
        FlowCommand::GrantPrepare { .. } => ("flow.grant-prepare", false, false),
        FlowCommand::GrantRecord { .. } => ("flow.grant-record", true, true),
        FlowCommand::GrantRevoke { .. } => ("flow.grant-revoke", true, true),
        FlowCommand::GrantList => ("flow.grant-list", false, false),
        FlowCommand::GrantGet { .. } => ("flow.grant-get", false, false),
        // Admission has its own durable intent/receipt. Even inherited observation environment
        // must not write before the fixed request scope and consent have been validated.
        FlowCommand::CoworkerAdmission(_) => ("flow.coworker-admission", false, false),
        FlowCommand::CoworkerExecution(sub) => match sub {
            CoworkerExecutionCommand::Preview { .. } => {
                ("flow.coworker-execution.preview", false, false)
            }
            CoworkerExecutionCommand::Record { .. } => {
                ("flow.coworker-execution.record", true, true)
            }
            CoworkerExecutionCommand::Inspect { .. } => {
                ("flow.coworker-execution.inspect", false, false)
            }
        },
        FlowCommand::CoworkerPlanning(sub) => match sub {
            CoworkerPlanningCommand::Enqueue { .. } => {
                ("flow.coworker-planning.enqueue", true, true)
            }
            CoworkerPlanningCommand::Get { .. } => ("flow.coworker-planning.get", false, false),
            CoworkerPlanningCommand::List { .. } => ("flow.coworker-planning.list", false, false),
            CoworkerPlanningCommand::ExecuteCommand { .. } => {
                ("flow.coworker-planning.execute-command", true, true)
            }
            CoworkerPlanningCommand::StartCommand { .. } => {
                ("flow.coworker-planning.start-command", true, true)
            }
        },
        FlowCommand::CodeDriver(sub) => match sub {
            CoworkerDriverCommand::RunOwner { .. } => ("flow.code-driver.run-owner", true, true),
            CoworkerDriverCommand::Ensure { .. } => ("flow.code-driver.ensure", true, true),
            CoworkerDriverCommand::Status { .. } => ("flow.code-driver.status", false, false),
            CoworkerDriverCommand::Reconcile { .. } => ("flow.code-driver.reconcile", true, true),
            CoworkerDriverCommand::Cancel { .. } => ("flow.code-driver.cancel", true, true),
        },
        // External resolution owns its durable receipt ledger. The shared command wrapper must not
        // add an observation journal entry or finish record, even under inherited observation env.
        FlowCommand::ExternalResolution(_) => ("flow.external-resolution", false, false),
    };
    let parent_req = read_env_parent_request_id();
    let should_journal = is_mutating || (allow_parent_journal && parent_req.is_some());
    let (request_id, call_id) = match &parent_req {
        Some(parent) => (parent.clone(), Some(generate_observation_id("obsc"))),
        None => (generate_observation_id("obsreq"), None),
    };
    // Resolve once so attribution and execution use the same typed request.
    let driver_request = match &cli.command {
        FlowCommand::CodeDriver(command) => {
            Some(resolve_driver_command_request(command, &binding, &thread))
        }
        _ => None,
    };
    let driver_correlation = driver_request
        .as_ref()
        .and_then(|request| request.as_ref().ok())
        .and_then(|request| {
            load_driver_observation_correlation(
                &registry_dir,
                &plans_dir,
                &authority_dir,
                &binding,
                &thread,
                &request.job_id,
            )
            .ok()
        });
    let mut resolved_plan_id = if driver_request.is_some() {
        driver_correlation
            .as_ref()
            .map(|value| value.plan_id.clone())
    } else {
        read_env_plan_id()
    };
    let mut verified_job_id = driver_correlation
        .as_ref()
        .map(|value| value.job_id.clone());
    let mut resolved_approval_id = driver_correlation
        .as_ref()
        .map(|value| value.approval_id.clone());
    let project_path = driver_correlation
        .as_ref()
        .map(|value| value.project_path.clone());
    let project_label = project_path.as_deref().and_then(|path| {
        Path::new(path)
            .file_name()
            .and_then(|name| name.to_str())
            .map(str::to_owned)
    });
    let mut resolved_task_id: Option<String> = None;
    let mut resolved_executor: Option<ObservationExecutor> = None;
    let started_at_ms = current_observation_time_ms();
    let actor = ObservationActor::unknown(Some("trusted_local_operator".to_string()));
    if should_journal {
        let _ = append_journal_record(
            &plans_dir,
            &ObservationJournalRecord {
                record_id: generate_observation_id("obsrec"),
                request_id: request_id.clone(),
                parent_request_id: None,
                call_id: call_id.clone(),
                phase: ObservationRecordPhase::Entry,
                at_ms: started_at_ms,
                pid: Some(std::process::id()),
                process_instance: current_process_instance(),
                project_label: project_label.clone(),
                project_path: project_path.clone(),
                binding_id: Some(binding.clone()),
                thread_uuid: Some(thread.clone()),
                thread_label: None,
                job_id: verified_job_id.clone(),
                plan_id: resolved_plan_id.clone(),
                task_id: None,
                approval_id: resolved_approval_id.clone(),
                operation: op_name.to_string(),
                title: op_name.replace('.', " "),
                state: ObservationCallState::Running,
                actor: Some(actor.clone()),
                executor: None,
                usage: None,
                delivery: Vec::new(),
                summary: Some(format!("Executing {op_name}")),
            },
        );
    }

    let outcome: Result<(), OperatorCliError> = async {
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
                verified_job_id = Some(cycle.plan_id.clone());
                resolved_plan_id = Some(cycle.plan_id.clone());
                resolved_approval_id = Some(cycle.approval_id.clone());
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
                verified_job_id = Some(cycle.plan_id.clone());
                resolved_plan_id = Some(cycle.plan_id.clone());
                resolved_approval_id = Some(cycle.approval_id.clone());
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
                apply_handoff_confirmations(
                    &mut handoff_request,
                    operator_approved,
                    confirm_approval,
                )?;

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
                verified_job_id = authority
                    .get_for_operator(&record.approval_id)
                    .ok()
                    .filter(|approval| {
                        approval.binding_id == binding && approval.thread_uuid == thread
                    })
                    .map(|approval| approval.plan_id);
                resolved_plan_id = verified_job_id.clone();
                resolved_approval_id = Some(record.approval_id.clone());
                resolved_task_id = Some(record.task_id.clone());
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
                verified_job_id = authority
                    .get_for_operator(&record.approval_id)
                    .ok()
                    .filter(|approval| {
                        approval.binding_id == binding && approval.thread_uuid == thread
                    })
                    .map(|approval| approval.plan_id);
                resolved_plan_id = verified_job_id.clone();
                resolved_approval_id = Some(record.approval_id.clone());
                resolved_task_id = Some(record.task_id.clone());
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
                verified_job_id = store
                    .get_cycle(&binding, &reservation.cycle_id)
                    .ok()
                    .flatten()
                    .filter(|cycle| {
                        cycle.thread_uuid == thread && cycle.approval_id == reservation.approval_id
                    })
                    .map(|cycle| cycle.plan_id);
                resolved_plan_id = verified_job_id.clone();
                resolved_approval_id = Some(reservation.approval_id.clone());
                write_result(&binding, &reservation, true)
            }
            FlowCommand::GrantPrepare { request } => {
                let prepare_request: PreparePlanningGrantRequest = read_request(&request)?;
                ensure_scope(
                    &prepare_request,
                    &binding,
                    &thread,
                    "scope_mismatch",
                    REQUEST_SCOPE_MISMATCH,
                )?;
                let preview = prepare_grant_preview(&registry_dir, &plans_dir, &prepare_request)?;
                ensure_scope(
                    &preview,
                    &binding,
                    &thread,
                    "scope_mismatch",
                    STORED_SCOPE_MISMATCH,
                )?;
                write_result(&binding, &preview, false)
            }
            FlowCommand::GrantRecord {
                request,
                confirm_preview,
                operator_approved,
            } => {
                let input: GrantRecordInput = read_request(&request)?;
                ensure_scope(
                    &input.preview,
                    &binding,
                    &thread,
                    "scope_mismatch",
                    REQUEST_SCOPE_MISMATCH,
                )?;
                let record_request =
                    apply_grant_record_confirmations(input, operator_approved, confirm_preview)?;
                let summary = record_grant(&plans_dir, &registry_dir, &record_request)?;
                ensure_scope(
                    &summary,
                    &binding,
                    &thread,
                    "outcome_unknown",
                    MUTATION_SCOPE_MISMATCH,
                )?;
                write_result(&binding, &summary, true)
            }
            FlowCommand::GrantRevoke { request } => {
                let revoke_request: RevokePlanningGrantRequest = read_request(&request)?;
                ensure_scope(
                    &revoke_request,
                    &binding,
                    &thread,
                    "scope_mismatch",
                    REQUEST_SCOPE_MISMATCH,
                )?;
                let scope = TrustedGrantScope::new(&binding, &thread)
                    .map_err(|error| map_native_error(&error, false))?;
                let store = open_grant_store(&plans_dir)?;
                let summary = revoke_planning_grant_scoped(&store, &scope, &revoke_request)
                    .map_err(|error| map_native_error(&error, true))?;
                ensure_scope(
                    &summary,
                    &binding,
                    &thread,
                    "outcome_unknown",
                    MUTATION_SCOPE_MISMATCH,
                )?;
                write_result(&binding, &summary, true)
            }
            FlowCommand::GrantList => {
                let scope = TrustedGrantScope::new(&binding, &thread)
                    .map_err(|error| map_native_error(&error, false))?;
                let store = open_grant_store(&plans_dir)?;
                let summaries = list_planning_grant_summaries(&store, &scope)
                    .map_err(|error| map_native_error(&error, false))?;
                for summary in &summaries {
                    ensure_scope(
                        summary,
                        &binding,
                        &thread,
                        "scope_mismatch",
                        STORED_SCOPE_MISMATCH,
                    )?;
                }
                write_result(&binding, &summaries, false)
            }
            FlowCommand::GrantGet { grant_id } => {
                validate_grant_id(&grant_id)?;
                let scope = TrustedGrantScope::new(&binding, &thread)
                    .map_err(|error| map_native_error(&error, false))?;
                let store = open_grant_store(&plans_dir)?;
                let summary = get_planning_grant_summary(&store, &scope, &grant_id)
                    .map_err(|error| map_native_error(&error, false))?;
                let Some(summary) = summary else {
                    return Err(OperatorCliError::new(
                        "grant_not_found",
                        "Requested planning grant was not found in this binding scope.",
                    ));
                };
                ensure_scope(
                    &summary,
                    &binding,
                    &thread,
                    "scope_mismatch",
                    STORED_SCOPE_MISMATCH,
                )?;
                write_result(&binding, &summary, false)
            }
            FlowCommand::CoworkerAdmission(subcommand) => run_coworker_admission(
                subcommand,
                &registry_dir,
                &plans_dir,
                &authority_dir,
                &binding,
                &thread,
            ),
            FlowCommand::CoworkerExecution(subcommand) => run_coworker_execution(
                subcommand,
                &registry_dir,
                &plans_dir,
                &authority_dir,
                &binding,
                &thread,
                &mut verified_job_id,
                &mut resolved_plan_id,
                &mut resolved_approval_id,
            ),
            FlowCommand::CoworkerPlanning(subcommand) => {
                run_coworker_planning(
                    subcommand,
                    &state_dir,
                    &registry_dir,
                    &plans_dir,
                    &authority_dir,
                    &sessions_dir,
                    &binding,
                    &thread,
                    &mut verified_job_id,
                    &mut resolved_plan_id,
                    &mut resolved_executor,
                )
                .await
            }
            FlowCommand::CodeDriver(subcommand) => {
                run_coworker_driver(
                    subcommand,
                    &state_dir,
                    &registry_dir,
                    &plans_dir,
                    &authority_dir,
                    &sessions_dir,
                    &binding,
                    &thread,
                    driver_request.expect("code-driver request was resolved above")?,
                )
                .await
            }
            FlowCommand::ExternalResolution(subcommand) => run_external_resolution(
                subcommand,
                &registry_dir,
                &plans_dir,
                &authority_dir,
                &sessions_dir,
                &binding,
                &thread,
                project_root.as_deref(),
            ),
        }
    }
    .await;

    if should_journal {
        let finished_at_ms = current_observation_time_ms();
        let (final_state, delivery_stage, summary) = match &outcome {
            Ok(()) => (
                ObservationCallState::Completed,
                ObservationDeliveryStage::ResponseReturned,
                format!("Completed {op_name}"),
            ),
            Err(err)
                if err.code == "outcome_unknown"
                    || err.code == "ambiguous_wakeup_pending_reconciliation" =>
            {
                (
                    ObservationCallState::Unknown,
                    ObservationDeliveryStage::Unknown,
                    format!("Outcome unknown for {op_name}: {}", err.code),
                )
            }
            Err(err) => (
                ObservationCallState::Failed,
                ObservationDeliveryStage::Failed,
                format!("Failed {op_name}: {}", err.code),
            ),
        };
        let _ = append_journal_record(
            &plans_dir,
            &ObservationJournalRecord {
                record_id: generate_observation_id("obsrec"),
                request_id,
                parent_request_id: None,
                call_id,
                phase: ObservationRecordPhase::Finish,
                at_ms: finished_at_ms,
                pid: Some(std::process::id()),
                process_instance: current_process_instance(),
                project_label,
                project_path,
                binding_id: Some(binding.clone()),
                thread_uuid: Some(thread),
                thread_label: None,
                job_id: verified_job_id,
                plan_id: resolved_plan_id,
                task_id: resolved_task_id,
                approval_id: resolved_approval_id,
                operation: op_name.to_string(),
                title: op_name.replace('.', " "),
                state: final_state,
                actor: Some(actor),
                executor: resolved_executor,
                usage: None,
                delivery: if parent_req.is_none() {
                    vec![ObservationDelivery {
                        stage: delivery_stage,
                        at_ms: Some(finished_at_ms),
                        summary: summary.clone(),
                        recipient: Some(binding),
                    }]
                } else {
                    Vec::new()
                },
                summary: Some(summary),
            },
        );
    }

    outcome
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

fn validate_project_root_override(path: &Path) -> Result<PathBuf, OperatorCliError> {
    if !path.is_absolute()
        || path.components().any(|part| {
            matches!(
                part,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
    {
        return Err(OperatorCliError::invalid_input(
            "`--project-root` must be a normalized absolute directory path.",
        ));
    }
    let meta = std::fs::symlink_metadata(path).map_err(|_| {
        OperatorCliError::invalid_input("`--project-root` does not name an existing directory.")
    })?;
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return Err(OperatorCliError::new(
            "security_violation",
            "`--project-root` must be a non-symlink directory.",
        ));
    }
    let canonical = std::fs::canonicalize(path).map_err(|_| {
        OperatorCliError::new(
            "security_violation",
            "failed to canonicalize the injected project root.",
        )
    })?;
    if canonical.as_path() != path {
        return Err(OperatorCliError::invalid_input(
            "`--project-root` must already be canonical.",
        ));
    }
    Ok(canonical)
}

struct ExternalResolutionRecordConfirmations {
    thread_uuid: String,
    preview_digest: String,
}

fn apply_external_resolution_record_confirmations(
    request: &RecordExternalResolutionRequest,
    binding_thread_uuid: &str,
    operator_approved: bool,
    confirm_thread: Option<String>,
    confirm_preview: Option<String>,
) -> Result<ExternalResolutionRecordConfirmations, OperatorCliError> {
    // The private request JSON cannot supply or override any of these fields. A record requires
    // independent CLI thread/preview confirmation and explicit local operator attestation.
    if !operator_approved {
        return Err(OperatorCliError::new(
            NativeErrorCode::UnauthorizedOperator.as_str(),
            "External resolution record requires explicit `--operator-approved`.",
        ));
    }
    let raw_thread = confirm_thread.ok_or_else(|| {
        OperatorCliError::invalid_input("Missing required `--confirm-thread` argument.")
    })?;
    let thread_uuid =
        validate_canonical_thread_uuid("confirm_thread", &raw_thread).map_err(|_| {
            OperatorCliError::invalid_input(
                "`--confirm-thread` must be a canonical lowercase non-nil thread UUID.",
            )
        })?;
    if thread_uuid != binding_thread_uuid || thread_uuid != request.thread_uuid {
        return Err(OperatorCliError::new(
            "scope_mismatch",
            "`--confirm-thread` does not match the fixed CLI binding thread and request thread.",
        ));
    }
    let raw_digest = confirm_preview.ok_or_else(|| {
        OperatorCliError::invalid_input("Missing required `--confirm-preview` argument.")
    })?;
    let preview_digest = validate_sha256_hex("confirm_preview", &raw_digest).map_err(|_| {
        OperatorCliError::invalid_input(
            "`--confirm-preview` must be a 64-character lowercase hexadecimal SHA-256 digest.",
        )
    })?;
    Ok(ExternalResolutionRecordConfirmations {
        thread_uuid,
        preview_digest,
    })
}

fn open_external_resolution_service(
    registry_dir: &Path,
    plans_dir: &Path,
    authority_dir: &Path,
    sessions_dir: &Path,
) -> Result<ExternalResolutionService, OperatorCliError> {
    ExternalResolutionService::new(plans_dir, authority_dir, registry_dir, Some(sessions_dir))
        .map_err(|error| map_native_error(&error, false))
}

fn ensure_external_resolution_request_scope(
    request: &RecordExternalResolutionRequest,
    binding: &str,
    thread: &str,
    project_root: Option<&Path>,
) -> Result<(), OperatorCliError> {
    ensure_scope(
        request,
        binding,
        thread,
        "scope_mismatch",
        REQUEST_SCOPE_MISMATCH,
    )?;
    if let Some(expected_root) = project_root {
        if request.canonical_repo_root.as_path() != expected_root {
            return Err(OperatorCliError::new(
                "scope_mismatch",
                "External resolution request canonical_repo_root does not match the router-validated project root.",
            ));
        }
    }
    request
        .validate()
        .map_err(|error| map_native_error(&error, false))
}

fn ensure_external_resolution_preview_scope(
    preview: &ExternalResolutionPreview,
    binding: &str,
    thread: &str,
    project_root: Option<&Path>,
) -> Result<(), OperatorCliError> {
    ensure_scope(
        preview,
        binding,
        thread,
        "scope_mismatch",
        STORED_SCOPE_MISMATCH,
    )?;
    if let Some(expected_root) = project_root {
        if preview.project.canonical_repo_root.as_path() != expected_root {
            return Err(OperatorCliError::new(
                "scope_mismatch",
                "External resolution preview project root does not match the router-validated project root.",
            ));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_external_resolution(
    subcommand: ExternalResolutionCommand,
    registry_dir: &Path,
    plans_dir: &Path,
    authority_dir: &Path,
    sessions_dir: &Path,
    binding: &str,
    thread: &str,
    project_root: Option<&Path>,
) -> Result<(), OperatorCliError> {
    let service =
        open_external_resolution_service(registry_dir, plans_dir, authority_dir, sessions_dir)?;
    match subcommand {
        ExternalResolutionCommand::Preview { request } => {
            let request: RecordExternalResolutionRequest = read_request(&request)?;
            ensure_external_resolution_request_scope(&request, binding, thread, project_root)?;
            let preview = service
                .preview(&request)
                .map_err(|error| map_native_error(&error, false))?;
            ensure_external_resolution_preview_scope(&preview, binding, thread, project_root)?;
            write_result(binding, &preview, false)
        }
        ExternalResolutionCommand::Record {
            request,
            confirm_thread,
            confirm_preview,
            operator_approved,
            reconcile_existing,
        } => {
            let request: RecordExternalResolutionRequest = read_request(&request)?;
            ensure_external_resolution_request_scope(&request, binding, thread, project_root)?;
            let confirmations = apply_external_resolution_record_confirmations(
                &request,
                thread,
                operator_approved,
                confirm_thread,
                confirm_preview,
            )?;

            if reconcile_existing {
                let receipt = service
                    .reconcile(&ReconcileExternalResolutionRequest {
                        trusted_local_attested: true,
                        binding_id: binding.to_string(),
                        thread_uuid: confirmations.thread_uuid,
                        request_key: request.request_key.clone(),
                        expected_preview_digest: confirmations.preview_digest,
                        expected_receipt_digest: None,
                    })
                    .map_err(|error| map_native_error(&error, true))?;
                ensure_scope(
                    &receipt,
                    binding,
                    thread,
                    "outcome_unknown",
                    MUTATION_SCOPE_MISMATCH,
                )?;
                return write_result(binding, &receipt, true);
            }

            let receipt = service
                .record(
                    &request,
                    &ExternalResolutionConfirmation {
                        trusted_local_attested: true,
                        thread_uuid: confirmations.thread_uuid,
                        preview_digest: confirmations.preview_digest,
                    },
                )
                .map_err(|error| map_native_error(&error, true))?;
            ensure_scope(
                &receipt,
                binding,
                thread,
                "outcome_unknown",
                MUTATION_SCOPE_MISMATCH,
            )?;
            write_result(binding, &receipt, true)
        }
        ExternalResolutionCommand::Inspect { request_key } => {
            if let Some(request_key) = request_key {
                validate_bounded_request_key(&request_key)?;
                let entry = service
                    .inspect_by_request_key(binding, thread, &request_key)
                    .map_err(|error| map_native_error(&error, false))?;
                let Some(entry) = entry else {
                    return Err(OperatorCliError::new(
                        "not_configured",
                        "No external resolution receipt exists for this request_key in the fixed scope.",
                    ));
                };
                ensure_scope(
                    &entry.receipt,
                    binding,
                    thread,
                    "scope_mismatch",
                    STORED_SCOPE_MISMATCH,
                )?;
                write_result(binding, &entry, false)
            } else {
                let inspection = service
                    .inspect(binding, thread)
                    .map_err(|error| map_native_error(&error, false))?;
                ensure_scope(
                    &inspection,
                    binding,
                    thread,
                    "scope_mismatch",
                    STORED_SCOPE_MISMATCH,
                )?;
                for entry in &inspection.receipts {
                    ensure_scope(
                        &entry.receipt,
                        binding,
                        thread,
                        "scope_mismatch",
                        STORED_SCOPE_MISMATCH,
                    )?;
                }
                write_result(binding, &inspection, false)
            }
        }
    }
}

fn validate_bounded_request_key(value: &str) -> Result<(), OperatorCliError> {
    if value.trim() != value || value.is_empty() || value.len() > 128 {
        return Err(OperatorCliError::invalid_input(
            "Invalid `--request-key` value.",
        ));
    }
    validate_identifier("request_key", value)
        .map(|_| ())
        .map_err(|_| OperatorCliError::invalid_input("Invalid `--request-key` value."))
}

fn validate_cycle_id(value: &str) -> Result<(), OperatorCliError> {
    validate_identifier("cycle_id", value)
        .map(|_| ())
        .map_err(|_| OperatorCliError::invalid_input("Invalid `--cycle-id` syntax."))
}

fn validate_grant_id(value: &str) -> Result<(), OperatorCliError> {
    validate_identifier("grant_id", value)
        .map(|_| ())
        .map_err(|_| OperatorCliError::invalid_input("Invalid `--grant-id` syntax."))
}

fn validate_confirm_preview(value: &str) -> Result<String, OperatorCliError> {
    validate_sha256_hex("confirm_preview", value).map_err(|_| {
        OperatorCliError::invalid_input(
            "`--confirm-preview` must be a 64-character lowercase hexadecimal SHA-256 digest.",
        )
    })
}

fn apply_grant_record_confirmations(
    input: GrantRecordInput,
    operator_approved: bool,
    confirm_preview: Option<String>,
) -> Result<RecordPlanningGrantRequest, OperatorCliError> {
    // Attestation and digest confirmation are attacker-controlled in the JSON body.
    // Require explicit CLI flags and never honor JSON self-attestation or digest alone.
    let base_attestation = input
        .attestation
        .unwrap_or_else(LocalOperatorGrantAttestation::unattested);
    let attestation = LocalOperatorGrantAttestation {
        operator_attested: operator_approved,
        derived_from_model_or_mcp_prose: base_attestation.derived_from_model_or_mcp_prose,
        native_per_call_identity_verified: base_attestation.native_per_call_identity_verified,
        note: base_attestation.note,
    };
    attestation
        .validate()
        .map_err(|error| map_native_error(&error, false))?;

    let raw_digest = confirm_preview.ok_or_else(|| {
        OperatorCliError::invalid_input("Missing required `--confirm-preview` argument.")
    })?;
    let confirmed_preview_digest = validate_confirm_preview(&raw_digest)?;
    if let Some(ref file_digest) = input.confirmed_preview_digest {
        if !file_digest.is_empty() && file_digest != &confirmed_preview_digest {
            return Err(OperatorCliError::new(
                NativeErrorCode::ValidationFailed.as_str(),
                "Role-flow request or persisted state failed validation.",
            ));
        }
    }

    Ok(RecordPlanningGrantRequest {
        request_key: input.request_key,
        expected_ledger_revision: input.expected_ledger_revision,
        preview: input.preview,
        confirmed_preview_digest,
        attestation,
    })
}

fn prepare_grant_preview(
    registry_dir: &Path,
    plans_dir: &Path,
    req: &PreparePlanningGrantRequest,
) -> Result<PlanningGrantPreview, OperatorCliError> {
    let preview = prepare_planning_grant_preview_from_dir_readonly(registry_dir, req)
        .map_err(|error| map_native_error(&error, false))?;
    let snapshot = role_flow::read_registry_snapshot_readonly(registry_dir)
        .map_err(|error| map_native_error(&error, false))?;
    let store = open_grant_store(plans_dir)?;
    let verified = prepare_planning_grant_preview_with_store(&store, &snapshot, req)
        .map_err(|error| map_native_error(&error, false))?;
    if verified != preview {
        return Err(OperatorCliError::new(
            "validation_failed",
            "Role-flow request or persisted state failed validation.",
        ));
    }
    Ok(verified)
}

fn record_grant(
    plans_dir: &Path,
    registry_dir: &Path,
    req: &RecordPlanningGrantRequest,
) -> Result<PlanningGrantSummary, OperatorCliError> {
    let store = open_grant_store(plans_dir)?;
    record_planning_grant_from_registry_dir(&store, registry_dir, req)
        .map_err(|error| map_native_error(&error, true))
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

fn open_grant_store(plans_dir: &Path) -> Result<CoworkerWorkflowStore, OperatorCliError> {
    CoworkerWorkflowStore::new(plans_dir).map_err(|error| map_native_error(&error, false))
}

fn open_controller_policy_store(
    plans_dir: &Path,
) -> Result<ControllerPolicyStore, OperatorCliError> {
    ControllerPolicyStore::new(plans_dir).map_err(|error| map_native_error(&error, false))
}

fn validate_job_id(value: &str) -> Result<(), OperatorCliError> {
    validate_identifier("job_id", value)
        .map(|_| ())
        .map_err(|_| OperatorCliError::invalid_input("Invalid `--job-id` syntax."))
}

fn validate_confirm_envelope(value: &str) -> Result<String, OperatorCliError> {
    validate_sha256_hex("confirm_envelope", value).map_err(|_| {
        OperatorCliError::invalid_input(
            "`--confirm-envelope` must be a 64-character lowercase hexadecimal SHA-256 digest.",
        )
    })
}

fn apply_execution_record_confirmations(
    request: &mut FreezeControllerExecutionEnvelopeRequest,
    operator_approved: bool,
    confirm_approval: Option<String>,
    confirm_envelope: Option<String>,
) -> Result<(), OperatorCliError> {
    // Attestation and confirmation digests in the JSON body are attacker-controlled.
    // Require explicit CLI flags and reject any disagreement with non-empty JSON fields.
    let checked_approval = match confirm_approval {
        Some(ref raw) => Some(validate_confirm_approval(raw)?),
        None => None,
    };
    let checked_envelope = match confirm_envelope {
        Some(ref raw) => Some(validate_confirm_envelope(raw)?),
        None => None,
    };
    if !operator_approved {
        request.operator_attested = false;
        request.confirm_approval_subject_digest = None;
        request.confirm_envelope_digest = None;
        return Err(OperatorCliError::new(
            NativeErrorCode::UnauthorizedOperator.as_str(),
            "Role-flow or planning-grant operation requires explicit operator authorization.",
        ));
    }
    let Some(checked_approval) = checked_approval else {
        request.operator_attested = false;
        request.confirm_approval_subject_digest = None;
        request.confirm_envelope_digest = None;
        return Err(OperatorCliError::new(
            NativeErrorCode::UnauthorizedOperator.as_str(),
            "Role-flow or planning-grant operation requires explicit operator authorization.",
        ));
    };
    let Some(checked_envelope) = checked_envelope else {
        request.operator_attested = false;
        request.confirm_approval_subject_digest = None;
        request.confirm_envelope_digest = None;
        return Err(OperatorCliError::new(
            NativeErrorCode::UnauthorizedOperator.as_str(),
            "Role-flow or planning-grant operation requires explicit operator authorization.",
        ));
    };
    if let Some(ref file_approval) = request.confirm_approval_subject_digest {
        if !file_approval.is_empty() && file_approval != &checked_approval {
            return Err(OperatorCliError::new(
                NativeErrorCode::UnauthorizedOperator.as_str(),
                "Role-flow or planning-grant operation requires explicit operator authorization.",
            ));
        }
    }
    if let Some(ref file_envelope) = request.confirm_envelope_digest {
        if !file_envelope.is_empty() && file_envelope != &checked_envelope {
            return Err(OperatorCliError::new(
                NativeErrorCode::UnauthorizedOperator.as_str(),
                "Role-flow or planning-grant operation requires explicit operator authorization.",
            ));
        }
    }
    request.operator_attested = true;
    request.confirm_approval_subject_digest = Some(checked_approval);
    request.confirm_envelope_digest = Some(checked_envelope);
    Ok(())
}

fn load_scoped_job(
    store: &CoworkerWorkflowStore,
    expected_binding_id: &str,
    expected_thread_uuid: &str,
    job_id: &str,
) -> Result<CoworkerJobRecord, OperatorCliError> {
    validate_job_id(job_id)?;
    let ledger = store
        .read_binding_ledger(expected_binding_id)
        .map_err(|error| map_native_error(&error, false))?;
    if let Some(ref pinned_thread) = ledger.thread_uuid {
        if pinned_thread != expected_thread_uuid {
            return Err(OperatorCliError::new(
                "scope_mismatch",
                STORED_SCOPE_MISMATCH,
            ));
        }
    }
    let Some(job) = ledger.jobs.into_iter().find(|item| item.job_id == job_id) else {
        return Err(OperatorCliError::new(
            NativeErrorCode::ValidationFailed.as_str(),
            "Role-flow request or persisted state failed validation.",
        ));
    };
    ensure_scope(
        &job,
        expected_binding_id,
        expected_thread_uuid,
        "scope_mismatch",
        STORED_SCOPE_MISMATCH,
    )?;
    Ok(job)
}

fn load_driver_admitted_job(
    store: &CoworkerWorkflowStore,
    expected_binding_id: &str,
    expected_thread_uuid: &str,
    job_id: &str,
) -> Result<CoworkerJobRecord, OperatorCliError> {
    validate_job_id(job_id)?;
    let ledger = store
        .read_binding_ledger(expected_binding_id)
        .map_err(|error| map_native_error(&error, false))?;
    ledger
        .validate(expected_binding_id)
        .map_err(|error| map_native_error(&error, false))?;
    if ledger.thread_uuid.as_deref() != Some(expected_thread_uuid) {
        return Err(OperatorCliError::new(
            NativeErrorCode::SecurityViolation.as_str(),
            STORED_SCOPE_MISMATCH,
        ));
    }
    let Some(job) = ledger.jobs.into_iter().find(|item| item.job_id == job_id) else {
        return Err(OperatorCliError::new(
            NativeErrorCode::ValidationFailed.as_str(),
            "Role-flow request or persisted state failed validation.",
        ));
    };
    ensure_scope(
        &job,
        expected_binding_id,
        expected_thread_uuid,
        NativeErrorCode::SecurityViolation.as_str(),
        STORED_SCOPE_MISMATCH,
    )?;
    Ok(job)
}

#[derive(serde::Serialize)]
struct AdmissionRecordedSummary {
    request_key: String,
    job_id: String,
    job_revision: u64,
    request_digest: String,
    preview_digest: String,
    approval_subject_digest: String,
    envelope_digest: String,
    stage: threadlane_native::coworker_admission::AdmissionStage,
}

fn write_admission_receipt(
    binding: &str,
    receipt: &AdmissionReceipt,
) -> Result<(), OperatorCliError> {
    let p = &receipt.preview;
    // This is the actual persisted linked job revision, not the source request revision.
    let summary = AdmissionRecordedSummary {
        request_key: p.request.request_key.clone(),
        job_id: p.proposed_job.job_id.clone(),
        job_revision: p.proposed_job.job_revision,
        request_digest: threadlane_native::coworker_workflow::sha256_hex(
            &serde_json::to_vec(&("threadlane-native:coworker-admission-request:v1", &p.request))
                .map_err(|_| OperatorCliError::new("outcome_unknown",
                    "Admission persisted but response serialization failed; inspect the original job."))?),
        preview_digest: p.preview_digest.clone(),
        approval_subject_digest: p.approval_subject_digest.clone(),
        envelope_digest: p.envelope.envelope_digest.clone(),
        stage: receipt.stage,
    };
    write_result(binding, &summary, true)
}

fn admission_error(
    service: &CoworkerAdmissionService,
    binding: &str,
    job_id: &str,
    error: &NativeError,
) -> OperatorCliError {
    if error.error_code() == NativeErrorCode::PersistenceUncertain {
        return OperatorCliError::new("persistence_uncertain",
            "Admission persistence is uncertain; inspect then explicitly reconcile the original job and request key.");
    }
    match service.inspect(binding, job_id) {
        Ok(state) if !state.intent_present && !state.linkage_present =>
            map_native_error(error, false),
        Ok(_) => OperatorCliError::new("admission_reconciliation_required",
            "An admission intent exists; inspect and explicitly reconcile its original request key. Do not generate a replacement request."),
        Err(_) => OperatorCliError::new("outcome_unknown",
            "Admission outcome could not be inspected; preserve the original request and inspect before recovery."),
    }
}

fn run_coworker_admission(
    command: CoworkerAdmissionCommand,
    registry_dir: &Path,
    plans_dir: &Path,
    authority_dir: &Path,
    binding: &str,
    thread: &str,
) -> Result<(), OperatorCliError> {
    let service = CoworkerAdmissionService::new(plans_dir, authority_dir, registry_dir)
        .map_err(|e| map_native_error(&e, false))?;
    let workflow =
        CoworkerWorkflowStore::new(plans_dir).map_err(|e| map_native_error(&e, false))?;
    match command {
        CoworkerAdmissionCommand::Preview { request } => {
            let request: AdmissionRequest = read_request(&request)?;
            ensure_admission_scope(&request, binding, thread)?;
            load_scoped_job(&workflow, binding, thread, &request.binding.job_id)?;
            let preview = service
                .preview(&request)
                .map_err(|e| map_native_error(&e, false))?;
            write_result(binding, &preview, false)
        }
        CoworkerAdmissionCommand::Record {
            request,
            operator_approved,
            confirm_preview,
            confirm_approval,
            confirm_envelope,
        } => {
            let mut request: AdmissionRequest = read_request(&request)?;
            ensure_admission_scope(&request, binding, thread)?;
            load_scoped_job(&workflow, binding, thread, &request.binding.job_id)?;
            let confirmed_preview = confirm_preview.filter(|_| operator_approved)
                .ok_or_else(|| OperatorCliError::new("unauthorized_operator",
                    "Admission record requires --operator-approved and all three confirmation flags."))?;
            let confirmed_preview = validate_confirm_preview(&confirmed_preview)?;
            apply_execution_record_confirmations(
                &mut request.execution,
                operator_approved,
                confirm_approval,
                confirm_envelope,
            )?;
            let confirmation = AdmissionConfirmation {
                trusted_local_attested: true,
                preview_digest: confirmed_preview,
                approval_subject_digest: request
                    .execution
                    .confirm_approval_subject_digest
                    .clone()
                    .expect("CLI confirmation validated"),
                envelope_digest: request
                    .execution
                    .confirm_envelope_digest
                    .clone()
                    .expect("CLI confirmation validated"),
            };
            let receipt = service
                .record(&request, &confirmation)
                .map_err(|e| admission_error(&service, binding, &request.binding.job_id, &e))?;
            write_admission_receipt(binding, &receipt)
        }
        CoworkerAdmissionCommand::Inspect { job_id } => {
            load_scoped_job(&workflow, binding, thread, &job_id)?;
            let inspection = service
                .inspect(binding, &job_id)
                .map_err(|e| map_native_error(&e, false))?;
            write_result(binding, &inspection, false)
        }
        CoworkerAdmissionCommand::Reconcile {
            job_id,
            request_key,
        } => {
            load_scoped_job(&workflow, binding, thread, &job_id)?;
            let inspection = service
                .inspect(binding, &job_id)
                .map_err(|e| map_native_error(&e, false))?;
            if inspection
                .receipt
                .as_ref()
                .map(|r| r.preview.request.request_key.as_str())
                != Some(request_key.as_str())
            {
                return Err(OperatorCliError::new(
                    "invalid_input",
                    "Reconciliation requires the original persisted admission request key.",
                ));
            }
            let receipt = service
                .reconcile(binding, &job_id)
                .map_err(|e| admission_error(&service, binding, &job_id, &e))?;
            write_admission_receipt(binding, &receipt)
        }
    }
}

fn ensure_admission_scope(
    request: &AdmissionRequest,
    binding: &str,
    thread: &str,
) -> Result<(), OperatorCliError> {
    if request.binding.binding_id != binding
        || request.binding.thread_uuid != thread
        || request.execution.binding_id != binding
        || request.execution.thread_uuid != thread
        || request.execution.job_id != request.binding.job_id
    {
        return Err(OperatorCliError::new(
            "scope_mismatch",
            REQUEST_SCOPE_MISMATCH,
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_coworker_execution(
    subcommand: CoworkerExecutionCommand,
    registry_dir: &Path,
    plans_dir: &Path,
    authority_dir: &Path,
    binding: &str,
    thread: &str,
    verified_job_id: &mut Option<String>,
    resolved_plan_id: &mut Option<String>,
    resolved_approval_id: &mut Option<String>,
) -> Result<(), OperatorCliError> {
    match subcommand {
        CoworkerExecutionCommand::Preview { request } => {
            let mut execution_request: FreezeControllerExecutionEnvelopeRequest =
                read_request(&request)?;
            ensure_scope(
                &execution_request,
                binding,
                thread,
                "scope_mismatch",
                REQUEST_SCOPE_MISMATCH,
            )?;
            execution_request.operator_attested = false;
            execution_request.confirm_approval_subject_digest = None;
            execution_request.confirm_envelope_digest = None;
            let workflow_store = open_grant_store(plans_dir)?;
            let job = load_scoped_job(&workflow_store, binding, thread, &execution_request.job_id)?;
            let authority = open_authority(registry_dir, plans_dir, authority_dir)?;
            let policy_store = open_controller_policy_store(plans_dir)?;
            let preview: ControllerConfigurationPreview = policy_store
                .preview_execution_envelope(&execution_request, &job, &authority)
                .map_err(|error| map_native_error(&error, false))?;
            ensure_scope(
                &preview,
                binding,
                thread,
                "scope_mismatch",
                STORED_SCOPE_MISMATCH,
            )?;
            write_result(binding, &preview, false)
        }
        CoworkerExecutionCommand::Record {
            request,
            confirm_approval,
            confirm_envelope,
            operator_approved,
        } => {
            let mut execution_request: FreezeControllerExecutionEnvelopeRequest =
                read_request(&request)?;
            ensure_scope(
                &execution_request,
                binding,
                thread,
                "scope_mismatch",
                REQUEST_SCOPE_MISMATCH,
            )?;
            apply_execution_record_confirmations(
                &mut execution_request,
                operator_approved,
                confirm_approval,
                confirm_envelope,
            )?;
            let workflow_store = open_grant_store(plans_dir)?;
            let job = load_scoped_job(&workflow_store, binding, thread, &execution_request.job_id)?;
            let authority = open_authority(registry_dir, plans_dir, authority_dir)?;
            let policy_store = open_controller_policy_store(plans_dir)?;
            let state: ControllerJobRuntimeState = policy_store
                .record_execution_envelope(&execution_request, &job, &authority)
                .map_err(|error| map_native_error(&error, true))?;
            ensure_scope(
                &state.envelope,
                binding,
                thread,
                "outcome_unknown",
                MUTATION_SCOPE_MISMATCH,
            )?;
            *verified_job_id = Some(state.job_id.clone());
            *resolved_plan_id = Some(state.envelope.plan_id.clone());
            *resolved_approval_id = Some(state.envelope.approval_id.clone());
            write_result(binding, &state, true)
        }
        CoworkerExecutionCommand::Inspect { job_id } => {
            validate_job_id(&job_id)?;
            let workflow_store = open_grant_store(plans_dir)?;
            let policy_store = open_controller_policy_store(plans_dir)?;
            let state: Option<ControllerJobRuntimeState> = policy_store
                .get_job_runtime_readonly(binding, &job_id)
                .map_err(|error| map_native_error(&error, false))?;
            let Some(state) = state else {
                return Err(OperatorCliError::new(
                    "not_configured",
                    "No frozen controller execution envelope is recorded for this job in this binding scope.",
                ));
            };
            ensure_scope(
                &state.envelope,
                binding,
                thread,
                "scope_mismatch",
                STORED_SCOPE_MISMATCH,
            )?;
            let job = load_scoped_job(&workflow_store, binding, thread, &job_id)?;
            state
                .envelope
                .verify_matches_job(&job)
                .map_err(|error| map_native_error(&error, false))?;
            write_result(binding, &state, false)
        }
    }
}

fn validate_command_id(value: &str) -> Result<(), OperatorCliError> {
    validate_identifier("command_id", value)
        .map(|_| ())
        .map_err(|_| OperatorCliError::invalid_input("Invalid `--command-id` syntax."))
}

fn record_planning_observation_correlation(
    summary: &PlanningCommandSummary,
    verified_job_id: &mut Option<String>,
    resolved_plan_id: &mut Option<String>,
    resolved_executor: &mut Option<ObservationExecutor>,
) {
    if let Some(ref jid) = summary.job_id {
        *verified_job_id = Some(jid.clone());
    }
    if let Some(ref pid) = summary.plan_id {
        *resolved_plan_id = Some(pid.clone());
    }
    *resolved_executor = Some(ObservationExecutor {
        tool: Some(summary.selected_choice.tool_id.clone()),
        provider: Some(summary.selected_choice.adapter.as_str().to_string()),
        role: Some(summary.role.as_str().to_string()),
        requested_model: Some(summary.selected_choice.model_id.clone()),
        observed_model: None,
        requested_effort: summary.selected_choice.effort.clone(),
        observed_effort: None,
    });
}

#[allow(clippy::too_many_arguments)]
async fn run_coworker_planning(
    subcommand: CoworkerPlanningCommand,
    state_dir: &Path,
    registry_dir: &Path,
    plans_dir: &Path,
    authority_dir: &Path,
    sessions_dir: &Path,
    binding: &str,
    thread: &str,
    verified_job_id: &mut Option<String>,
    resolved_plan_id: &mut Option<String>,
    resolved_executor: &mut Option<ObservationExecutor>,
) -> Result<(), OperatorCliError> {
    match subcommand {
        CoworkerPlanningCommand::Enqueue { request } => {
            let enqueue_request: EnqueuePlanningCommandRequest = read_request(&request)?;
            ensure_scope(
                &enqueue_request,
                binding,
                thread,
                "scope_mismatch",
                REQUEST_SCOPE_MISMATCH,
            )?;
            if enqueue_request.payload.binding_id() != binding
                || enqueue_request.payload.thread_uuid() != thread
            {
                return Err(OperatorCliError::new(
                    "scope_mismatch",
                    REQUEST_SCOPE_MISMATCH,
                ));
            }
            let summary =
                enqueue_planning_command_summary(plans_dir, registry_dir, &enqueue_request)
                    .map_err(|error| map_native_error(&error, true))?;
            ensure_scope(
                &summary,
                binding,
                thread,
                "outcome_unknown",
                MUTATION_SCOPE_MISMATCH,
            )?;
            record_planning_observation_correlation(
                &summary,
                verified_job_id,
                resolved_plan_id,
                resolved_executor,
            );
            write_result(binding, &summary, true)
        }
        CoworkerPlanningCommand::Get { command_id } => {
            validate_command_id(&command_id)?;
            let summary = get_planning_command_summary_readonly(
                plans_dir,
                registry_dir,
                binding,
                thread,
                &command_id,
            )
            .map_err(|error| map_native_error(&error, false))?;
            let Some(summary) = summary else {
                return Err(OperatorCliError::new(
                    "command_not_found",
                    "Requested planning command was not found in this binding scope.",
                ));
            };
            ensure_scope(
                &summary,
                binding,
                thread,
                "scope_mismatch",
                STORED_SCOPE_MISMATCH,
            )?;
            write_result(binding, &summary, false)
        }
        CoworkerPlanningCommand::List { cursor, limit } => {
            let (limit, cursor) = validate_list_args(limit, cursor)?;
            let page = list_planning_command_summaries_readonly(
                plans_dir,
                registry_dir,
                binding,
                thread,
                limit,
                cursor.as_deref(),
            )
            .map_err(|error| map_native_error(&error, false))?;
            for summary in &page.commands {
                ensure_scope(
                    summary,
                    binding,
                    thread,
                    "scope_mismatch",
                    STORED_SCOPE_MISMATCH,
                )?;
            }
            write_result(binding, &page, false)
        }
        CoworkerPlanningCommand::ExecuteCommand { command_id } => {
            validate_command_id(&command_id)?;
            let driver = open_planning_driver_for_plans_dir(plans_dir, registry_dir)
                .map_err(|error| map_native_error(&error, false))?;
            let exec_result = driver.execute_command(binding, thread, &command_id).await;
            let _ = driver
                .notify_terminal_command_if_configured(binding, thread, &command_id)
                .await;
            let outcome = exec_result.map_err(|error| map_native_error(&error, true))?;
            let exec_summary = PlanningCommandExecutionSummary::from_outcome(&outcome);
            ensure_scope(
                &exec_summary.summary,
                binding,
                thread,
                "outcome_unknown",
                MUTATION_SCOPE_MISMATCH,
            )?;
            record_planning_observation_correlation(
                &exec_summary.summary,
                verified_job_id,
                resolved_plan_id,
                resolved_executor,
            );
            write_result(binding, &exec_summary, true)
        }
        CoworkerPlanningCommand::StartCommand { command_id } => {
            validate_command_id(&command_id)?;
            let start_outcome = start_planning_command_detached(
                state_dir,
                registry_dir,
                plans_dir,
                authority_dir,
                sessions_dir,
                binding,
                thread,
                &command_id,
            )
            .map_err(|error| map_native_error(&error, false))?;
            ensure_scope(
                &start_outcome.summary,
                binding,
                thread,
                "outcome_unknown",
                MUTATION_SCOPE_MISMATCH,
            )?;
            record_planning_observation_correlation(
                &start_outcome.summary,
                verified_job_id,
                resolved_plan_id,
                resolved_executor,
            );
            write_result(binding, &start_outcome, true)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn resolve_coworker_driver_request(
    request_path: Option<&Path>,
    cli_job_id: Option<String>,
    cli_reason: Option<String>,
    cli_max_steps: Option<u32>,
    cli_poll_interval_ms: Option<u64>,
    cli_idle_linger_ms: Option<u64>,
    cli_execution_mode: Option<String>,
    cli_initial_dispatch_only: bool,
    binding: &str,
    thread: &str,
) -> Result<CoworkerDriverRequest, OperatorCliError> {
    let mut req = if let Some(path) = request_path {
        let loaded: CoworkerDriverRequest = read_request(path)?;
        ensure_scope(
            &loaded,
            binding,
            thread,
            "scope_mismatch",
            REQUEST_SCOPE_MISMATCH,
        )?;
        if let Some(ref explicit_job_id) = cli_job_id {
            if explicit_job_id != &loaded.job_id {
                return Err(OperatorCliError::invalid_input(
                    "`--job-id` does not match `job_id` in `--request`.",
                ));
            }
        }
        loaded
    } else {
        let Some(job_id) = cli_job_id else {
            return Err(OperatorCliError::invalid_input(
                "Either `--request` or `--job-id` is required for `code-driver` commands.",
            ));
        };
        CoworkerDriverRequest {
            schema_version: threadlane_native::COWORKER_DRIVER_REQUEST_SCHEMA_VERSION,
            binding_id: binding.to_owned(),
            thread_uuid: thread.to_owned(),
            job_id,
            reason: None,
            max_steps: None,
            poll_interval_ms: None,
            idle_linger_ms: None,
            execution_mode: None,
        }
    };

    let parsed_cli_mode = match cli_execution_mode {
        Some(raw) => {
            let parsed = raw.parse::<CoworkerDriverExecutionMode>().map_err(|_| {
                OperatorCliError::invalid_input("Invalid `--execution-mode` value.")
            })?;
            if cli_initial_dispatch_only
                && parsed != CoworkerDriverExecutionMode::Phase1InitialDispatchOnly
            {
                return Err(OperatorCliError::invalid_input(
                    "`--initial-dispatch-only` conflicts with `--execution-mode full_automatic_pipeline`.",
                ));
            }
            Some(parsed)
        }
        None if cli_initial_dispatch_only => {
            Some(CoworkerDriverExecutionMode::Phase1InitialDispatchOnly)
        }
        None => None,
    };

    if let Some(reason) = cli_reason {
        req.reason = Some(reason);
    }
    if let Some(max_steps) = cli_max_steps {
        req.max_steps = Some(max_steps);
    }
    if let Some(poll_interval_ms) = cli_poll_interval_ms {
        req.poll_interval_ms = Some(poll_interval_ms);
    }
    if let Some(idle_linger_ms) = cli_idle_linger_ms {
        req.idle_linger_ms = Some(idle_linger_ms);
    }
    if let Some(mode) = parsed_cli_mode {
        req.execution_mode = Some(mode);
    }

    req.validate_for_scope(binding, thread)
        .map_err(|error| map_native_error(&error, false))?;
    Ok(req)
}

// Observation verifies immutable attribution, not permission to execute. It neither
// acquires controller ownership nor starts/reconciles a host to obtain admission.
struct DriverObservationCorrelation {
    job_id: String,
    plan_id: String,
    approval_id: String,
    project_path: String,
}

fn load_driver_observation_correlation(
    registry_dir: &Path,
    plans_dir: &Path,
    authority_dir: &Path,
    binding: &str,
    thread: &str,
    job_id: &str,
) -> Result<DriverObservationCorrelation, OperatorCliError> {
    let store = open_grant_store(plans_dir)?;
    let job = load_scoped_job(&store, binding, thread, job_id)?;
    let linkage = job.approval_linkage.as_ref().ok_or_else(|| {
        OperatorCliError::new("not_configured", "Job has no frozen approval linkage.")
    })?;
    let authority = open_authority(registry_dir, plans_dir, authority_dir)?;
    let approval = authority
        .get_for_operator(&linkage.approval_id)
        .map_err(|error| map_authority_error(&error))?;
    if approval.binding_id != binding || approval.thread_uuid != thread {
        return Err(OperatorCliError::new(
            "scope_mismatch",
            STORED_SCOPE_MISMATCH,
        ));
    }
    linkage
        .verify_matches_approval(&approval)
        .map_err(|error| map_native_error(&error, false))?;
    Ok(DriverObservationCorrelation {
        job_id: job.job_id,
        plan_id: linkage.plan_id.clone(),
        approval_id: linkage.approval_id.clone(),
        project_path: linkage.project_base.repository_path.clone(),
    })
}

fn resolve_driver_command_request(
    command: &CoworkerDriverCommand,
    binding: &str,
    thread: &str,
) -> Result<CoworkerDriverRequest, OperatorCliError> {
    let (
        request,
        job_id,
        reason,
        max_steps,
        poll_interval_ms,
        idle_linger_ms,
        execution_mode,
        initial_dispatch_only,
    ) = match command {
        CoworkerDriverCommand::RunOwner {
            request,
            job_id,
            max_steps,
            poll_interval_ms,
            idle_linger_ms,
            execution_mode,
            initial_dispatch_only,
        }
        | CoworkerDriverCommand::Ensure {
            request,
            job_id,
            max_steps,
            poll_interval_ms,
            idle_linger_ms,
            execution_mode,
            initial_dispatch_only,
        } => (
            request,
            job_id,
            None,
            *max_steps,
            *poll_interval_ms,
            *idle_linger_ms,
            execution_mode.clone(),
            *initial_dispatch_only,
        ),
        CoworkerDriverCommand::Status { request, job_id } => {
            (request, job_id, None, None, None, None, None, false)
        }
        CoworkerDriverCommand::Reconcile {
            request,
            job_id,
            execution_mode,
            initial_dispatch_only,
        } => (
            request,
            job_id,
            None,
            None,
            None,
            None,
            execution_mode.clone(),
            *initial_dispatch_only,
        ),
        CoworkerDriverCommand::Cancel {
            request,
            job_id,
            reason,
        } => (
            request,
            job_id,
            reason.clone(),
            None,
            None,
            None,
            None,
            false,
        ),
    };
    resolve_coworker_driver_request(
        request.as_deref(),
        job_id.clone(),
        reason,
        max_steps,
        poll_interval_ms,
        idle_linger_ms,
        execution_mode,
        initial_dispatch_only,
        binding,
        thread,
    )
}

#[allow(clippy::too_many_arguments)]
async fn run_coworker_driver(
    subcommand: CoworkerDriverCommand,
    state_dir: &Path,
    registry_dir: &Path,
    plans_dir: &Path,
    authority_dir: &Path,
    sessions_dir: &Path,
    binding: &str,
    thread: &str,
    req: CoworkerDriverRequest,
) -> Result<(), OperatorCliError> {
    // Detached owners are new processes. Establish durability of already completed consent
    // before opening their driver; this gate cannot finish a partial/Uncertain admission.
    if matches!(
        &subcommand,
        CoworkerDriverCommand::RunOwner { .. }
            | CoworkerDriverCommand::Ensure { .. }
            | CoworkerDriverCommand::Reconcile { .. }
    ) {
        let workflow = open_grant_store(plans_dir)?;
        let job = load_driver_admitted_job(&workflow, binding, thread, &req.job_id)?;
        let policy = open_controller_policy_store(plans_dir)?;
        threadlane_native::coworker_admission::verify_complete_admission(&workflow, &policy, &job)
            .map_err(|e| map_native_error(&e, false))?;
    }
    let driver = CoworkerCodeDriver::open_from_dirs(
        state_dir,
        registry_dir,
        plans_dir,
        authority_dir,
        sessions_dir,
    )
    .map_err(|error| map_native_error(&error, false))?;

    match subcommand {
        CoworkerDriverCommand::RunOwner { .. } => {
            let options = CoworkerDriverRunOptions {
                max_steps: req.max_steps.or(DEFAULT_DRIVER_MAX_STEPS),
                poll_interval_ms: req
                    .poll_interval_ms
                    .unwrap_or(DEFAULT_DRIVER_POLL_INTERVAL_MS),
                idle_linger_ms: req.idle_linger_ms,
                execution_mode: req.execution_mode,
            };
            let report = driver
                .run_owner(binding, thread, &req.job_id, options)
                .await
                .map_err(|error| map_native_error(&error, true))?;
            ensure_scope(
                &report,
                binding,
                thread,
                "outcome_unknown",
                MUTATION_SCOPE_MISMATCH,
            )?;
            write_result(binding, &report, true)
        }
        CoworkerDriverCommand::Ensure { .. } => {
            let options = if req.max_steps.is_some()
                || req.poll_interval_ms.is_some()
                || req.idle_linger_ms.is_some()
                || req.execution_mode.is_some()
            {
                Some(CoworkerDriverRunOptions {
                    max_steps: req.max_steps.or(DEFAULT_DRIVER_MAX_STEPS),
                    poll_interval_ms: req
                        .poll_interval_ms
                        .unwrap_or(DEFAULT_DRIVER_POLL_INTERVAL_MS),
                    idle_linger_ms: req.idle_linger_ms.or(DEFAULT_ENSURE_OWNER_IDLE_LINGER_MS),
                    execution_mode: req.execution_mode,
                })
            } else {
                None
            };
            let report = driver
                .ensure_owner(binding, thread, &req.job_id, options)
                .await
                .map_err(|error| map_native_error(&error, true))?;
            ensure_scope(
                &report,
                binding,
                thread,
                "outcome_unknown",
                MUTATION_SCOPE_MISMATCH,
            )?;
            write_result(binding, &report, true)
        }
        CoworkerDriverCommand::Status { .. } => {
            let report = driver
                .status(binding, thread, &req.job_id)
                .await
                .map_err(|error| map_native_error(&error, false))?;
            ensure_scope(
                &report,
                binding,
                thread,
                "scope_mismatch",
                STORED_SCOPE_MISMATCH,
            )?;
            write_result(binding, &report, false)
        }
        CoworkerDriverCommand::Reconcile { .. } => {
            let options = req.execution_mode.map(|mode| CoworkerDriverRunOptions {
                execution_mode: Some(mode),
                ..CoworkerDriverRunOptions::default()
            });
            let report = driver
                .reconcile_with_options(binding, thread, &req.job_id, options)
                .await
                .map_err(|error| map_native_error(&error, true))?;
            ensure_scope(
                &report,
                binding,
                thread,
                "outcome_unknown",
                MUTATION_SCOPE_MISMATCH,
            )?;
            write_result(binding, &report, true)
        }
        CoworkerDriverCommand::Cancel { .. } => {
            let report = driver
                .cancel(binding, thread, &req.job_id, req.reason)
                .await
                .map_err(|error| map_native_error(&error, true))?;
            ensure_scope(
                &report,
                binding,
                thread,
                "outcome_unknown",
                MUTATION_SCOPE_MISMATCH,
            )?;
            write_result(binding, &report, true)
        }
    }
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
        NativeErrorCode::AmbiguousWakeupPendingReconciliation => {
            "Planning command or wakeup outcome is in flight or uncertain pending reconciliation; inspect state before retrying."
        }
        NativeErrorCode::PersistenceUncertain => {
            "External resolution receipt bytes may be visible but durability is unconfirmed; inspect, then use `record --reconcile-existing` with the original request_key and preview_digest."
        }
        NativeErrorCode::TransportUnavailable => "Role-flow host transport is unavailable.",
        NativeErrorCode::ProtocolError
        | NativeErrorCode::RpcError
        | NativeErrorCode::McpToolError => {
            "Role-flow host or provider protocol returned an invalid response."
        }
        NativeErrorCode::ThreadNotFound => "Target native thread was not found.",
        NativeErrorCode::UnauthorizedOperator => {
            "Role-flow or planning-grant operation requires explicit operator authorization."
        }
        NativeErrorCode::RebindRejected => {
            "Role-flow or planning-grant request attempted to rebind or cross scope."
        }
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

    fn external_resolution_request() -> RecordExternalResolutionRequest {
        RecordExternalResolutionRequest {
            binding_id: BINDING.to_string(),
            thread_uuid: THREAD.to_string(),
            canonical_repo_root: PathBuf::from("/tmp/project"),
            target: threadlane_native::external_resolution::ExternalResolutionTarget::LegacyPlan {
                plan_id: "plan_external_unit".to_string(),
                plan_revision: 1,
            },
            scope: threadlane_native::external_resolution::ExternalResolutionScope::FullJob,
            expected_ledger_revision: None,
            expected_job_revision: None,
            expected_runtime_revision: None,
            request_key: "req_external_unit".to_string(),
            created_at_ms: 1,
            explanation: "unit test external resolution".to_string(),
            evidence_refs: Vec::new(),
        }
    }

    #[test]
    fn external_resolution_record_requires_independent_flags_and_exact_thread() {
        let request = external_resolution_request();
        let missing_approval = apply_external_resolution_record_confirmations(
            &request,
            THREAD,
            false,
            Some(THREAD.to_string()),
            Some(DIGEST.to_string()),
        )
        .expect_err("missing operator approval must fail");
        assert_eq!(missing_approval.code, "unauthorized_operator");

        let missing_thread = apply_external_resolution_record_confirmations(
            &request,
            THREAD,
            true,
            None,
            Some(DIGEST.to_string()),
        )
        .expect_err("missing thread confirmation must fail");
        assert_eq!(missing_thread.code, "invalid_input");

        let wrong_thread = apply_external_resolution_record_confirmations(
            &request,
            THREAD,
            true,
            Some("0196ed00-0000-7000-8000-000000000099".to_string()),
            Some(DIGEST.to_string()),
        )
        .expect_err("wrong thread confirmation must fail");
        assert_eq!(wrong_thread.code, "scope_mismatch");

        let missing_preview = apply_external_resolution_record_confirmations(
            &request,
            THREAD,
            true,
            Some(THREAD.to_string()),
            None,
        )
        .expect_err("missing preview confirmation must fail");
        assert_eq!(missing_preview.code, "invalid_input");

        let invalid_preview = apply_external_resolution_record_confirmations(
            &request,
            THREAD,
            true,
            Some(THREAD.to_string()),
            Some("not-a-digest".to_string()),
        )
        .expect_err("invalid preview digest must fail");
        assert_eq!(invalid_preview.code, "invalid_input");

        let confirmed = apply_external_resolution_record_confirmations(
            &request,
            THREAD,
            true,
            Some(THREAD.to_string()),
            Some(DIGEST.to_string()),
        )
        .expect("exact independent confirmations must be accepted");
        assert_eq!(confirmed.thread_uuid, THREAD);
        assert_eq!(confirmed.preview_digest, DIGEST);
    }

    #[test]
    fn external_resolution_request_scope_rejects_project_root_drift() {
        let request = external_resolution_request();
        let err = ensure_external_resolution_request_scope(
            &request,
            BINDING,
            THREAD,
            Some(Path::new("/tmp/other-project")),
        )
        .expect_err("router project-root drift must fail");
        assert_eq!(err.code, "scope_mismatch");

        ensure_external_resolution_request_scope(
            &request,
            BINDING,
            THREAD,
            Some(Path::new("/tmp/project")),
        )
        .expect("matching canonical project root must pass structural validation");
    }

    #[test]
    fn cli_parses_grant_subcommands_with_global_flags() {
        for sub in [
            vec!["grant-prepare", "--request", "/tmp/prepare.json"],
            vec![
                "grant-record",
                "--request",
                "/tmp/record.json",
                "--confirm-preview",
                DIGEST,
                "--operator-approved",
            ],
            vec!["grant-revoke", "--request", "/tmp/revoke.json"],
            vec!["grant-list"],
            vec![
                "grant-get",
                "--grant-id",
                "pgr_0123456789abcdef0123456789abcdef",
            ],
        ] {
            let mut argv = vec![
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
            ];
            argv.extend(sub);
            let parsed = Cli::try_parse_from(argv).expect("parse grant subcommand");
            let globals = GlobalArgs::from_cli(&parsed).expect("extract globals");
            assert_eq!(globals.binding, BINDING);
            assert_eq!(globals.thread, THREAD);
        }
    }

    #[test]
    fn cli_parses_coworker_execution_subcommands_with_global_flags() {
        for sub in [
            vec![
                "coworker-execution",
                "preview",
                "--request",
                "/tmp/preview.json",
            ],
            vec![
                "coworker-execution",
                "record",
                "--request",
                "/tmp/record.json",
                "--confirm-approval",
                DIGEST,
                "--confirm-envelope",
                DIGEST,
                "--operator-approved",
            ],
            vec![
                "coworker-execution",
                "inspect",
                "--job-id",
                "job_0123456789abcdef0123456789abcdef",
            ],
        ] {
            let mut argv = vec![
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
            ];
            argv.extend(sub);
            let parsed = Cli::try_parse_from(argv).expect("parse coworker-execution subcommand");
            let globals = GlobalArgs::from_cli(&parsed).expect("extract globals");
            assert_eq!(globals.binding, BINDING);
            assert_eq!(globals.thread, THREAD);
            assert!(matches!(parsed.command, FlowCommand::CoworkerExecution(_)));
        }
    }

    #[test]
    fn cli_parses_external_resolution_subcommands_and_independent_flags() {
        for sub in [
            vec![
                "external-resolution",
                "preview",
                "--request",
                "/tmp/external-resolution.json",
            ],
            vec![
                "external-resolution",
                "record",
                "--request",
                "/tmp/external-resolution.json",
                "--confirm-thread",
                THREAD,
                "--confirm-preview",
                DIGEST,
                "--operator-approved",
            ],
            vec![
                "external-resolution",
                "record",
                "--request",
                "/tmp/external-resolution.json",
                "--confirm-thread",
                THREAD,
                "--confirm-preview",
                DIGEST,
                "--operator-approved",
                "--reconcile-existing",
            ],
            vec![
                "external_resolution",
                "inspect",
                "--request-key",
                "req_external_unit",
            ],
        ] {
            let mut argv = vec![
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
                "--project-root",
                "/tmp/project",
                "--binding",
                BINDING,
                "--thread",
                THREAD,
            ];
            argv.extend(sub);
            let parsed = Cli::try_parse_from(argv).expect("parse external-resolution subcommand");
            let globals = GlobalArgs::from_cli(&parsed).expect("extract globals");
            assert_eq!(globals.binding, BINDING);
            assert_eq!(globals.thread, THREAD);
            assert_eq!(
                globals.project_root.as_deref(),
                Some(Path::new("/tmp/project"))
            );
            assert!(matches!(parsed.command, FlowCommand::ExternalResolution(_)));
        }
    }

    #[test]
    fn cli_parses_code_driver_subcommands_and_alias_with_global_flags() {
        for sub in [
            vec![
                "code-driver",
                "run-owner",
                "--job-id",
                "job_0123456789abcdef0123456789abcdef",
                "--max-steps",
                "16",
                "--poll-interval-ms",
                "50",
                "--idle-linger-ms",
                "200",
                "--execution-mode",
                "full_automatic_pipeline",
            ],
            vec![
                "code-driver",
                "ensure",
                "--request",
                "/tmp/driver-ensure.json",
                "--initial-dispatch-only",
            ],
            vec![
                "code-driver",
                "status",
                "--job-id",
                "job_0123456789abcdef0123456789abcdef",
            ],
            vec![
                "coworker-driver",
                "reconcile",
                "--job-id",
                "job_0123456789abcdef0123456789abcdef",
                "--execution-mode",
                "phase1_initial_dispatch_only",
            ],
            vec![
                "coworker-driver",
                "cancel",
                "--job-id",
                "job_0123456789abcdef0123456789abcdef",
                "--reason",
                "operator_stop_test",
            ],
        ] {
            let mut argv = vec![
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
            ];
            argv.extend(sub);
            let parsed = Cli::try_parse_from(argv).expect("parse code-driver subcommand");
            let globals = GlobalArgs::from_cli(&parsed).expect("extract globals");
            assert_eq!(globals.binding, BINDING);
            assert_eq!(globals.thread, THREAD);
            assert!(matches!(parsed.command, FlowCommand::CodeDriver(_)));
        }
    }
}

#[cfg(test)]
mod admission_cli_tests {
    use super::*;

    #[test]
    fn admission_global_flags_parse_before_group_between_levels_and_after_operation() {
        for args in [
            vec![
                "threadlane-flow",
                "--binding",
                "binding",
                "coworker-admission",
                "inspect",
                "--job-id",
                "job_a",
            ],
            vec![
                "threadlane-flow",
                "coworker-admission",
                "--binding",
                "binding",
                "inspect",
                "--job-id",
                "job_a",
            ],
            vec![
                "threadlane-flow",
                "coworker-admission",
                "inspect",
                "--job-id",
                "job_a",
                "--binding",
                "binding",
            ],
        ] {
            let parsed = Cli::try_parse_from(args).unwrap();
            assert_eq!(parsed.binding.as_deref(), Some("binding"));
            assert!(matches!(
                parsed.command,
                FlowCommand::CoworkerAdmission(CoworkerAdmissionCommand::Inspect { .. })
            ));
        }
        assert!(Cli::try_parse_from([
            "threadlane-flow",
            "coworker-admission",
            "record",
            "--request",
            "/private/request.json",
            "--operator-approved",
            "--confirm-preview",
            &"a".repeat(64),
            "--confirm-approval",
            &"b".repeat(64),
            "--confirm-envelope",
            &"c".repeat(64)
        ])
        .is_ok());
    }
}
