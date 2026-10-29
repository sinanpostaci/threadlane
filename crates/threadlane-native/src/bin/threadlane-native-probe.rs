//! `threadlane-native-probe`: local operator acceptance CLI for inspecting native Codex
//! thread MCP configuration and verifying two-chat connection-epoch pairing isolation.
//!
//! Uses only documented public `codex app-server proxy` methods (`thread/read` with
//! `includeTurns: false`, `mcpServerStatus/list`, and thread-scoped `mcpServer/tool/call`).
//! Never prints operator tokens, connection nonces, epoch IDs, raw MCP payloads, thread
//! titles, or conversation transcripts.

#![deny(unsafe_code)]
#![deny(missing_docs)]

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use threadlane::SupervisorClient;
use threadlane_native::fs_safety::{
    constant_time_str_eq, sanitize_diagnostic_text, validate_bounded_identifier,
    validate_optional_cwd_path,
};
use threadlane_native::{
    ConnectionEpochState, ConnectionIdentity, NativeClient, NativeConfig, NativeError,
    NativeErrorCode, NativeMcpToolCallResult, NativeThread, NativeVerificationStatus,
    OperatorEnrollRequest, PairingStore, CONNECTION_IDENTITY_TOOL, MCP_SERVER_NAME,
};
use uuid::Uuid;

const MAX_ID_CHARS: usize = 128;
const MAX_THREAD_UUID_CHARS: usize = 64;
const MAX_PROBE_JSON_BYTES: usize = 16 * 1024;
const STATUS_TOOL_NAME: &str = "status";
const BINDING_LABEL_THREAD_A: &str = "native-probe-thread-a";
const BINDING_LABEL_THREAD_B: &str = "native-probe-thread-b";

/// Wrapper that prevents secret values (operator tokens, nonces, epoch IDs) from being printed via `Debug`.
#[derive(Clone, PartialEq, Eq)]
struct RedactedSecret(String);

impl RedactedSecret {
    fn new(raw: String) -> Self {
        Self(raw)
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RedactedSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("\"[REDACTED]\"")
    }
}

/// Validated connection identity with secret nonce and epoch redacted from `Debug` and serialization.
#[derive(Debug, Clone)]
struct ProbedIdentity {
    connection_nonce: RedactedSecret,
    epoch_id: RedactedSecret,
    server_pid: u32,
    state: ConnectionEpochState,
    enrolled_binding_id: Option<String>,
    verification_gate: NativeVerificationStatus,
}

/// Validated binding scope extracted from a read-only `status` MCP tool call.
#[derive(Debug, Clone, PartialEq, Eq)]
struct VerifiedStatusBinding {
    binding_id: String,
    thread_uuid: Option<String>,
}

/// Command-line interface for `threadlane-native-probe`.
#[derive(Debug, Parser)]
#[command(
    name = "threadlane-native-probe",
    version,
    about = "Local operator acceptance probe for native Codex two-chat MCP pairing isolation"
)]
struct ProbeCli {
    /// Probe mode to execute.
    #[command(subcommand)]
    command: ProbeCommand,
}

/// Supported operator acceptance subcommands.
#[derive(Debug, Subcommand)]
enum ProbeCommand {
    /// Inspect read-only metadata, MCP server presence, and unbound/enrolled connection identity for one native thread without enrolling.
    Inspect {
        /// Absolute path to the native `codex` CLI executable.
        #[arg(long = "codex", value_name = "PATH")]
        codex: PathBuf,

        /// Optional operator-selected app-server proxy socket path (`--sock`).
        #[arg(long = "socket", value_name = "PATH")]
        socket: Option<PathBuf>,

        /// Canonical hyphenated non-nil native Codex `thread.id` UUID.
        #[arg(long = "thread", value_name = "UUID")]
        thread: String,

        /// Canonical absolute project root directory expected to match `thread.cwd`.
        #[arg(long = "project-root", value_name = "PATH")]
        project_root: PathBuf,
    },

    /// Enroll and verify two distinct same-project native Codex threads against a private pairing store and running Threadlane supervisor.
    PairCheck {
        /// Absolute path to the native `codex` CLI executable.
        #[arg(long = "codex", value_name = "PATH")]
        codex: PathBuf,

        /// Optional operator-selected app-server proxy socket path (`--sock`).
        #[arg(long = "socket", value_name = "PATH")]
        socket: Option<PathBuf>,

        /// Path to the private (`0700`) temporary native MCP pairing store directory.
        #[arg(long = "pairing-dir", value_name = "DIR")]
        pairing_dir: PathBuf,

        /// Path to the running Threadlane supervisor state directory (`supervisor.json`).
        #[arg(long = "state-dir", value_name = "DIR")]
        state_dir: PathBuf,

        /// Canonical absolute project root directory expected to match both threads' `cwd`.
        #[arg(long = "project-root", value_name = "PATH")]
        project_root: PathBuf,

        /// First canonical hyphenated non-nil native Codex `thread.id` UUID.
        #[arg(long = "thread-a", value_name = "UUID")]
        thread_a: String,

        /// Second canonical hyphenated non-nil native Codex `thread.id` UUID.
        #[arg(long = "thread-b", value_name = "UUID")]
        thread_b: String,

        /// Explicit operator authorization flag required to create bindings and enroll connection epochs.
        #[arg(long = "allow-enroll")]
        allow_enroll: bool,

        /// Optional explicit verification that read-only `status` calls on both threads return their own distinct enrolled `binding_id`s.
        #[arg(long = "verify-status")]
        verify_status: bool,
    },
}

/// Bounded, secret-free JSON report produced by `threadlane-native-probe inspect`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InspectReport {
    mode: &'static str,
    configured: bool,
    thread_id: String,
    thread_id_matched: bool,
    project_id: Option<String>,
    cwd: String,
    cwd_match: bool,
    thread_status: String,
    mcp_server_name: String,
    mcp_present: bool,
    mcp_auth_status: String,
    mcp_runtime_status: Option<String>,
    connection_identity_tool_listed: bool,
    identity_shape_valid: bool,
    server_pid: u32,
    connection_state: ConnectionEpochState,
    enrolled_binding_id: Option<String>,
    enrolled_on_inspect: bool,
    verification_gate: NativeVerificationStatus,
    two_chat_routing_gate: NativeVerificationStatus,
    restart_persistence_gate: NativeVerificationStatus,
    busy_turn_delivery_gate: NativeVerificationStatus,
    overall_live_gate: NativeVerificationStatus,
}

/// Outcome classification for `threadlane-native-probe pair-check`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PairCheckOutcome {
    /// Distinct MCP child connections were enrolled for both threads; per-call status routing was not checked.
    IsolatedEnrollmentObserved,
    /// Distinct MCP child connections were enrolled AND read-only `status` calls confirmed isolated per-thread `binding_id`s.
    IsolatedStatusVerified,
    /// Both threads resolved to a shared MCP child connection; the connection was quarantined and blocked for both threads.
    BlockedSharedConnection,
}

/// Per-call binding routing verification status for `pair-check`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PerCallRoutingStatus {
    /// Per-call binding routing still needs verification (`--verify-status` was not requested).
    NeedsVerification,
    /// Blocked because a single MCP child connection was shared across two threads and quarantined.
    BlockedSharedConnection,
    /// Read-only `status` tool calls from both native threads returned their respective distinct created `binding_id`s.
    StatusBindingVerified,
}

/// Secret-free summary for a single thread in `PairCheckReport`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PairedThreadSummary {
    thread_id: String,
    thread_id_matched: bool,
    project_id: Option<String>,
    cwd: String,
    cwd_match: bool,
    thread_status: String,
    created_binding_id: String,
    binding_label: String,
    identity_shape_valid: bool,
    server_pid: u32,
    connection_state: ConnectionEpochState,
    enrolled_binding_id: Option<String>,
    status_verified_binding_id: Option<String>,
    verification_gate: NativeVerificationStatus,
}

/// Bounded, secret-free JSON report produced by `threadlane-native-probe pair-check`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PairCheckReport {
    mode: &'static str,
    configured: bool,
    outcome: PairCheckOutcome,
    shared_connection_detected: bool,
    quarantined_for_both: bool,
    distinct_connection_nonces: bool,
    distinct_epoch_ids: bool,
    isolated_enrollment_observed: bool,
    status_routing_verified: bool,
    per_call_binding_routing: PerCallRoutingStatus,
    thread_a: PairedThreadSummary,
    thread_b: PairedThreadSummary,
    two_chat_routing_gate: NativeVerificationStatus,
    restart_persistence_gate: NativeVerificationStatus,
    busy_turn_delivery_gate: NativeVerificationStatus,
    overall_live_gate: NativeVerificationStatus,
}

enum ProbeExecutionOutput {
    Inspect(Box<InspectReport>),
    PairCheck(Box<PairCheckReport>),
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = ProbeCli::parse();
    match execute_cli(cli).await {
        Ok(ProbeExecutionOutput::Inspect(report)) => {
            if let Err(err) = print_bounded_json(&*report) {
                print_safe_error(&err, &[]);
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
        Ok(ProbeExecutionOutput::PairCheck(report)) => {
            let blocked = report.outcome == PairCheckOutcome::BlockedSharedConnection;
            if let Err(err) = print_bounded_json(&*report) {
                print_safe_error(&err, &[]);
                return ExitCode::FAILURE;
            }
            if blocked {
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(err) => {
            print_safe_error(&err, &[]);
            ExitCode::FAILURE
        }
    }
}

async fn execute_cli(cli: ProbeCli) -> Result<ProbeExecutionOutput, NativeError> {
    match cli.command {
        ProbeCommand::Inspect {
            codex,
            socket,
            thread,
            project_root,
        } => {
            let report = run_inspect(codex, socket, &thread, &project_root).await?;
            Ok(ProbeExecutionOutput::Inspect(Box::new(report)))
        }
        ProbeCommand::PairCheck {
            codex,
            socket,
            pairing_dir,
            state_dir,
            project_root,
            thread_a,
            thread_b,
            allow_enroll,
            verify_status,
        } => {
            let report = run_pair_check(
                codex,
                socket,
                &pairing_dir,
                &state_dir,
                &project_root,
                &thread_a,
                &thread_b,
                allow_enroll,
                verify_status,
            )
            .await?;
            Ok(ProbeExecutionOutput::PairCheck(Box::new(report)))
        }
    }
}

async fn run_inspect(
    codex: PathBuf,
    socket: Option<PathBuf>,
    thread_raw: &str,
    project_root_raw: &Path,
) -> Result<InspectReport, NativeError> {
    let thread_id = validate_canonical_thread_uuid(thread_raw, "thread")?;
    let expected_root = validate_canonical_project_root(project_root_raw)?;
    let client = NativeClient::new(NativeConfig {
        executable: codex,
        socket,
    })?;

    let thread_meta = verify_thread_metadata_and_cwd(&client, &thread_id, &expected_root).await?;
    let statuses = client
        .mcp_server_status(&thread_meta.id, MCP_SERVER_NAME)
        .await
        .map_err(|e| redact_error(e, &[]))?;

    let Some(mcp_entry) = statuses.iter().find(|s| s.name == MCP_SERVER_NAME) else {
        return Err(NativeError::new(
            NativeErrorCode::NotConfigured,
            "native thread does not have a configured `threadlane` MCP server entry",
        ));
    };

    let identity_listed = mcp_entry
        .tool_names
        .iter()
        .any(|name| name == CONNECTION_IDENTITY_TOOL);

    let probed = query_thread_connection_identity(&client, &thread_meta.id).await?;
    let gate = match probed.state {
        ConnectionEpochState::QuarantinedSharedConnection => {
            NativeVerificationStatus::BlockedSharedConnection
        }
        _ => match probed.verification_gate {
            NativeVerificationStatus::BlockedSharedConnection => {
                NativeVerificationStatus::BlockedSharedConnection
            }
            _ => NativeVerificationStatus::NeedsVerification,
        },
    };

    Ok(InspectReport {
        mode: "inspect",
        configured: true,
        thread_id: thread_meta.id,
        thread_id_matched: true,
        project_id: thread_meta.project_id,
        cwd: thread_meta.cwd,
        cwd_match: true,
        thread_status: thread_meta.status,
        mcp_server_name: MCP_SERVER_NAME.to_string(),
        mcp_present: true,
        mcp_auth_status: mcp_entry.auth_status.clone(),
        mcp_runtime_status: mcp_entry.runtime_status.clone(),
        connection_identity_tool_listed: identity_listed,
        identity_shape_valid: true,
        server_pid: probed.server_pid,
        connection_state: probed.state,
        enrolled_binding_id: probed.enrolled_binding_id,
        enrolled_on_inspect: false,
        verification_gate: gate,
        two_chat_routing_gate: gate,
        restart_persistence_gate: NativeVerificationStatus::NeedsVerification,
        busy_turn_delivery_gate: NativeVerificationStatus::NeedsVerification,
        overall_live_gate: gate,
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_pair_check(
    codex: PathBuf,
    socket: Option<PathBuf>,
    pairing_dir: &Path,
    state_dir: &Path,
    project_root_raw: &Path,
    thread_a_raw: &str,
    thread_b_raw: &str,
    allow_enroll: bool,
    verify_status: bool,
) -> Result<PairCheckReport, NativeError> {
    if !allow_enroll {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "pair-check requires explicit --allow-enroll operator confirmation flag",
        ));
    }

    let thread_a_id = validate_canonical_thread_uuid(thread_a_raw, "thread_a")?;
    let thread_b_id = validate_canonical_thread_uuid(thread_b_raw, "thread_b")?;
    if thread_a_id == thread_b_id {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "thread_a and thread_b must be distinct native thread UUIDs",
        ));
    }

    let expected_root = validate_canonical_project_root(project_root_raw)?;
    if !pairing_dir.is_absolute() || !state_dir.is_absolute() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "pairing-dir and state-dir must be explicit absolute paths",
        ));
    }

    let pairing_store = PairingStore::open(pairing_dir).map_err(|e| redact_error(e, &[]))?;
    if pairing_store.dir() == state_dir {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "pairing-dir and state-dir must be distinct directories",
        ));
    }
    let operator_token = RedactedSecret::new(
        pairing_store
            .operator_token()
            .map_err(|e| redact_error(e, &[]))?,
    );

    let supervisor = SupervisorClient::from_state_dir(state_dir)
        .map_err(|e| map_supervisor_error(e, &[operator_token.as_str()]))?;

    let client = NativeClient::new(NativeConfig {
        executable: codex,
        socket,
    })?;

    // 1. Metadata-only read (`includeTurns: false`) and cwd/project verification for both threads.
    let meta_a = verify_thread_metadata_and_cwd(&client, &thread_a_id, &expected_root).await?;
    let meta_b = verify_thread_metadata_and_cwd(&client, &thread_b_id, &expected_root).await?;
    if meta_a.project_id != meta_b.project_id {
        return Err(NativeError::new(
            NativeErrorCode::ProjectMismatch,
            "thread_a and thread_b reported mismatched projectId metadata",
        ));
    }

    // 2. Query paired connection identity via thread-scoped `mcpServer/tool/call` for each thread.
    let identity_a = query_thread_connection_identity(&client, &meta_a.id).await?;
    let identity_b = query_thread_connection_identity(&client, &meta_b.id).await?;

    let secrets = [
        operator_token.as_str(),
        identity_a.connection_nonce.as_str(),
        identity_a.epoch_id.as_str(),
        identity_b.connection_nonce.as_str(),
        identity_b.epoch_id.as_str(),
    ];

    // 3. Create isolated bindings in the running Threadlane supervisor with safe static labels.
    let binding_a = supervisor
        .create_binding(&meta_a.id, BINDING_LABEL_THREAD_A)
        .await
        .map_err(|e| map_supervisor_error(e, &secrets))?;
    let binding_b = supervisor
        .create_binding(&meta_b.id, BINDING_LABEL_THREAD_B)
        .await
        .map_err(|e| map_supervisor_error(e, &secrets))?;

    // 4. Enroll Thread A first, then compare nonces/epochs and attempt to enroll Thread B.
    let enrolled_a = pairing_store
        .enroll(
            operator_token.as_str(),
            OperatorEnrollRequest {
                connection_nonce: identity_a.connection_nonce.as_str().to_string(),
                epoch_id: identity_a.epoch_id.as_str().to_string(),
                binding_id: binding_a.binding_id.clone(),
                thread_id: meta_a.id.clone(),
                project_id: meta_a.project_id.clone(),
            },
        )
        .map_err(|e| redact_error(e, &secrets))?;

    let same_nonce = constant_time_str_eq(
        identity_a.connection_nonce.as_str(),
        identity_b.connection_nonce.as_str(),
    );
    let same_epoch =
        constant_time_str_eq(identity_a.epoch_id.as_str(), identity_b.epoch_id.as_str());

    let enroll_b_result = pairing_store.enroll(
        operator_token.as_str(),
        OperatorEnrollRequest {
            connection_nonce: identity_b.connection_nonce.as_str().to_string(),
            epoch_id: identity_b.epoch_id.as_str().to_string(),
            binding_id: binding_b.binding_id.clone(),
            thread_id: meta_b.id.clone(),
            project_id: meta_b.project_id.clone(),
        },
    );

    if same_nonce
        || same_epoch
        || matches!(
            &enroll_b_result,
            Err(err) if err.error_code() == NativeErrorCode::SharedConnectionRejected
        )
    {
        // Ensure connection A is quarantined in the store by a verified second-thread enrollment
        // even if only epoch_id matched.
        if !same_nonce {
            let _ = pairing_store.enroll(
                operator_token.as_str(),
                OperatorEnrollRequest {
                    connection_nonce: identity_a.connection_nonce.as_str().to_string(),
                    epoch_id: identity_a.epoch_id.as_str().to_string(),
                    binding_id: binding_b.binding_id.clone(),
                    thread_id: meta_b.id.clone(),
                    project_id: meta_b.project_id.clone(),
                },
            );
            let _ = pairing_store.enroll(
                operator_token.as_str(),
                OperatorEnrollRequest {
                    connection_nonce: identity_b.connection_nonce.as_str().to_string(),
                    epoch_id: identity_b.epoch_id.as_str().to_string(),
                    binding_id: binding_a.binding_id.clone(),
                    thread_id: meta_a.id.clone(),
                    project_id: meta_a.project_id.clone(),
                },
            );
        }

        let inspected_a = pairing_store
            .inspect_epoch(
                identity_a.connection_nonce.as_str(),
                identity_a.epoch_id.as_str(),
            )
            .map_err(|e| redact_error(e, &secrets))?;
        if inspected_a.state != ConnectionEpochState::QuarantinedSharedConnection {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "shared MCP child connection was not quarantined in pairing store",
            ));
        }

        return Ok(build_blocked_shared_report(
            meta_a,
            meta_b,
            binding_a.binding_id,
            binding_b.binding_id,
            identity_a.server_pid,
            identity_b.server_pid,
            !same_nonce,
            !same_epoch,
        ));
    }

    let enrolled_b = enroll_b_result.map_err(|e| redact_error(e, &secrets))?;
    if enrolled_a.binding_id == enrolled_b.binding_id {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "enrolled bindings for thread_a and thread_b must be distinct",
        ));
    }

    if !verify_status {
        return Ok(PairCheckReport {
            mode: "pair_check",
            configured: true,
            outcome: PairCheckOutcome::IsolatedEnrollmentObserved,
            shared_connection_detected: false,
            quarantined_for_both: false,
            distinct_connection_nonces: true,
            distinct_epoch_ids: true,
            isolated_enrollment_observed: true,
            status_routing_verified: false,
            per_call_binding_routing: PerCallRoutingStatus::NeedsVerification,
            thread_a: PairedThreadSummary {
                thread_id: meta_a.id,
                thread_id_matched: true,
                project_id: meta_a.project_id,
                cwd: meta_a.cwd,
                cwd_match: true,
                thread_status: meta_a.status,
                created_binding_id: binding_a.binding_id.clone(),
                binding_label: BINDING_LABEL_THREAD_A.to_string(),
                identity_shape_valid: true,
                server_pid: identity_a.server_pid,
                connection_state: ConnectionEpochState::Enrolled,
                enrolled_binding_id: Some(enrolled_a.binding_id),
                status_verified_binding_id: None,
                verification_gate: NativeVerificationStatus::NeedsVerification,
            },
            thread_b: PairedThreadSummary {
                thread_id: meta_b.id,
                thread_id_matched: true,
                project_id: meta_b.project_id,
                cwd: meta_b.cwd,
                cwd_match: true,
                thread_status: meta_b.status,
                created_binding_id: binding_b.binding_id.clone(),
                binding_label: BINDING_LABEL_THREAD_B.to_string(),
                identity_shape_valid: true,
                server_pid: identity_b.server_pid,
                connection_state: ConnectionEpochState::Enrolled,
                enrolled_binding_id: Some(enrolled_b.binding_id),
                status_verified_binding_id: None,
                verification_gate: NativeVerificationStatus::NeedsVerification,
            },
            two_chat_routing_gate: NativeVerificationStatus::NeedsVerification,
            restart_persistence_gate: NativeVerificationStatus::NeedsVerification,
            busy_turn_delivery_gate: NativeVerificationStatus::NeedsVerification,
            overall_live_gate: NativeVerificationStatus::NeedsVerification,
        });
    }

    // 5. Explicit `--verify-status`: call read-only `status` on both native threads and verify
    //    each returned structured `binding_id` matches its own created binding and differs from the other.
    let status_a = query_thread_status_binding(&client, &meta_a.id, &secrets).await?;
    let status_b = query_thread_status_binding(&client, &meta_b.id, &secrets).await?;

    verify_isolated_status_bindings(
        &meta_a.id,
        &binding_a.binding_id,
        &status_a,
        &meta_b.id,
        &binding_b.binding_id,
        &status_b,
    )?;

    Ok(PairCheckReport {
        mode: "pair_check",
        configured: true,
        outcome: PairCheckOutcome::IsolatedStatusVerified,
        shared_connection_detected: false,
        quarantined_for_both: false,
        distinct_connection_nonces: true,
        distinct_epoch_ids: true,
        isolated_enrollment_observed: true,
        status_routing_verified: true,
        per_call_binding_routing: PerCallRoutingStatus::StatusBindingVerified,
        thread_a: PairedThreadSummary {
            thread_id: meta_a.id,
            thread_id_matched: true,
            project_id: meta_a.project_id,
            cwd: meta_a.cwd,
            cwd_match: true,
            thread_status: meta_a.status,
            created_binding_id: binding_a.binding_id,
            binding_label: BINDING_LABEL_THREAD_A.to_string(),
            identity_shape_valid: true,
            server_pid: identity_a.server_pid,
            connection_state: ConnectionEpochState::Enrolled,
            enrolled_binding_id: Some(enrolled_a.binding_id),
            status_verified_binding_id: Some(status_a.binding_id),
            verification_gate: NativeVerificationStatus::NeedsVerification,
        },
        thread_b: PairedThreadSummary {
            thread_id: meta_b.id,
            thread_id_matched: true,
            project_id: meta_b.project_id,
            cwd: meta_b.cwd,
            cwd_match: true,
            thread_status: meta_b.status,
            created_binding_id: binding_b.binding_id,
            binding_label: BINDING_LABEL_THREAD_B.to_string(),
            identity_shape_valid: true,
            server_pid: identity_b.server_pid,
            connection_state: ConnectionEpochState::Enrolled,
            enrolled_binding_id: Some(enrolled_b.binding_id),
            status_verified_binding_id: Some(status_b.binding_id),
            verification_gate: NativeVerificationStatus::NeedsVerification,
        },
        two_chat_routing_gate: NativeVerificationStatus::NeedsVerification,
        restart_persistence_gate: NativeVerificationStatus::NeedsVerification,
        busy_turn_delivery_gate: NativeVerificationStatus::NeedsVerification,
        overall_live_gate: NativeVerificationStatus::NeedsVerification,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_blocked_shared_report(
    meta_a: NativeThread,
    meta_b: NativeThread,
    binding_a_id: String,
    binding_b_id: String,
    pid_a: u32,
    pid_b: u32,
    distinct_nonces: bool,
    distinct_epochs: bool,
) -> PairCheckReport {
    PairCheckReport {
        mode: "pair_check",
        configured: true,
        outcome: PairCheckOutcome::BlockedSharedConnection,
        shared_connection_detected: true,
        quarantined_for_both: true,
        distinct_connection_nonces: distinct_nonces,
        distinct_epoch_ids: distinct_epochs,
        isolated_enrollment_observed: false,
        status_routing_verified: false,
        per_call_binding_routing: PerCallRoutingStatus::BlockedSharedConnection,
        thread_a: PairedThreadSummary {
            thread_id: meta_a.id,
            thread_id_matched: true,
            project_id: meta_a.project_id,
            cwd: meta_a.cwd,
            cwd_match: true,
            thread_status: meta_a.status,
            created_binding_id: binding_a_id,
            binding_label: BINDING_LABEL_THREAD_A.to_string(),
            identity_shape_valid: true,
            server_pid: pid_a,
            connection_state: ConnectionEpochState::QuarantinedSharedConnection,
            enrolled_binding_id: None,
            status_verified_binding_id: None,
            verification_gate: NativeVerificationStatus::BlockedSharedConnection,
        },
        thread_b: PairedThreadSummary {
            thread_id: meta_b.id,
            thread_id_matched: true,
            project_id: meta_b.project_id,
            cwd: meta_b.cwd,
            cwd_match: true,
            thread_status: meta_b.status,
            created_binding_id: binding_b_id,
            binding_label: BINDING_LABEL_THREAD_B.to_string(),
            identity_shape_valid: true,
            server_pid: pid_b,
            connection_state: ConnectionEpochState::QuarantinedSharedConnection,
            enrolled_binding_id: None,
            status_verified_binding_id: None,
            verification_gate: NativeVerificationStatus::BlockedSharedConnection,
        },
        two_chat_routing_gate: NativeVerificationStatus::BlockedSharedConnection,
        restart_persistence_gate: NativeVerificationStatus::NeedsVerification,
        busy_turn_delivery_gate: NativeVerificationStatus::NeedsVerification,
        overall_live_gate: NativeVerificationStatus::BlockedSharedConnection,
    }
}

fn validate_canonical_thread_uuid(raw: &str, field: &'static str) -> Result<String, NativeError> {
    let clean = validate_bounded_identifier(raw, field, MAX_THREAD_UUID_CHARS)?;
    let parsed = Uuid::parse_str(&clean).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{field} must be a valid canonical hyphenated UUID"),
        )
    })?;
    if parsed.is_nil() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{field} must not be the nil UUID"),
        ));
    }
    let canonical = parsed.hyphenated().to_string();
    if clean != canonical {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{field} must use lowercase canonical hyphenated UUID format"),
        ));
    }
    Ok(canonical)
}

fn validate_canonical_project_root(project_root: &Path) -> Result<PathBuf, NativeError> {
    let Some(clean) = validate_optional_cwd_path(Some(project_root))? else {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "project-root path must not be empty",
        ));
    };
    let meta = fs::symlink_metadata(&clean).map_err(|_| {
        NativeError::new(
            NativeErrorCode::InvalidConfig,
            "project-root directory does not exist",
        )
    })?;
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "project-root must be an existing non-symlink directory",
        ));
    }
    let canonical = fs::canonicalize(&clean).map_err(|_| {
        NativeError::new(
            NativeErrorCode::InvalidConfig,
            "failed to canonicalize project-root directory",
        )
    })?;
    if canonical != clean {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "project-root path is not canonical",
        ));
    }
    Ok(clean)
}

async fn verify_thread_metadata_and_cwd(
    client: &NativeClient,
    expected_thread_id: &str,
    expected_project_root: &Path,
) -> Result<NativeThread, NativeError> {
    let meta = client
        .thread(expected_thread_id)
        .await
        .map_err(|e| redact_error(e, &[]))?;
    if meta.id != expected_thread_id {
        return Err(NativeError::new(
            NativeErrorCode::ThreadNotFound,
            "returned native thread.id did not match requested thread UUID",
        ));
    }
    let thread_cwd_path = Path::new(&meta.cwd);
    if thread_cwd_path != expected_project_root {
        return Err(NativeError::new(
            NativeErrorCode::ProjectMismatch,
            "native thread cwd does not match expected project-root",
        ));
    }
    Ok(meta)
}

async fn query_thread_connection_identity(
    client: &NativeClient,
    thread_id: &str,
) -> Result<ProbedIdentity, NativeError> {
    let result = client
        .call_mcp_tool(
            thread_id,
            MCP_SERVER_NAME,
            CONNECTION_IDENTITY_TOOL,
            json!({}),
        )
        .await
        .map_err(|e| redact_error(e, &[]))?;
    parse_connection_identity_result(&result)
}

fn parse_connection_identity_result(
    result: &NativeMcpToolCallResult,
) -> Result<ProbedIdentity, NativeError> {
    if result.is_error {
        return Err(NativeError::new(
            NativeErrorCode::McpToolError,
            "connection_identity MCP tool returned isError=true",
        ));
    }

    let text_envelope = parse_first_text_content_envelope(&result.content)?;
    let structured_envelope = extract_structured_envelope(result.structured_content.as_ref())?;

    let identity = match (text_envelope, structured_envelope) {
        (Some(text_val), Some(struct_val)) => {
            let from_text = decode_connection_identity_envelope(&text_val)?;
            let from_struct = decode_connection_identity_envelope(&struct_val)?;
            if from_text != from_struct {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "connection_identity content[0].text and structuredContent envelopes did not match",
                ));
            }
            from_text
        }
        (Some(text_val), None) => decode_connection_identity_envelope(&text_val)?,
        (None, Some(struct_val)) => decode_connection_identity_envelope(&struct_val)?,
        (None, None) => {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "connection_identity response contained neither content[0].text nor structuredContent",
            ));
        }
    };

    let clean_nonce = validate_bounded_identifier(
        &identity.connection_nonce,
        "connection_nonce",
        MAX_ID_CHARS,
    )?;
    let clean_epoch = validate_bounded_identifier(&identity.epoch_id, "epoch_id", MAX_ID_CHARS)?;
    if identity.server_pid == 0 {
        return Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            "connection_identity server_pid must be non-zero",
        ));
    }
    let clean_enrolled = match identity.enrolled_binding_id {
        Some(ref bid) => Some(validate_bounded_identifier(
            bid,
            "enrolled_binding_id",
            MAX_ID_CHARS,
        )?),
        None => None,
    };

    Ok(ProbedIdentity {
        connection_nonce: RedactedSecret::new(clean_nonce),
        epoch_id: RedactedSecret::new(clean_epoch),
        server_pid: identity.server_pid,
        state: identity.state,
        enrolled_binding_id: clean_enrolled,
        verification_gate: identity.verification_gate,
    })
}

fn decode_connection_identity_envelope(
    envelope: &Value,
) -> Result<ConnectionIdentity, NativeError> {
    let Some(obj) = envelope.as_object() else {
        return Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            "connection_identity wrapper must be a JSON object",
        ));
    };
    if let Some(err_val) = obj.get("error") {
        if !err_val.is_null() {
            return Err(NativeError::new(
                NativeErrorCode::McpToolError,
                "connection_identity tool returned a structured error envelope",
            ));
        }
    }
    if let Some(tool_val) = obj.get("tool") {
        if tool_val.as_str() != Some(CONNECTION_IDENTITY_TOOL) {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "MCP tool wrapper `tool` field did not match `connection_identity`",
            ));
        }
    }
    let target = obj.get("data").unwrap_or(envelope);
    serde_json::from_value::<ConnectionIdentity>(target.clone()).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "connection_identity payload did not match expected ConnectionIdentity schema",
        )
    })
}

async fn query_thread_status_binding(
    client: &NativeClient,
    thread_id: &str,
    secrets: &[&str],
) -> Result<VerifiedStatusBinding, NativeError> {
    let result = client
        .call_mcp_tool(thread_id, MCP_SERVER_NAME, STATUS_TOOL_NAME, json!({}))
        .await
        .map_err(|e| redact_error(e, secrets))?;
    parse_status_binding_result(&result)
}

fn parse_status_binding_result(
    result: &NativeMcpToolCallResult,
) -> Result<VerifiedStatusBinding, NativeError> {
    if result.is_error {
        return Err(NativeError::new(
            NativeErrorCode::McpToolError,
            "status MCP tool returned isError=true",
        ));
    }

    let text_envelope = parse_first_text_content_envelope(&result.content)?;
    let structured_envelope = extract_structured_envelope(result.structured_content.as_ref())?;

    match (text_envelope, structured_envelope) {
        (Some(text_val), Some(struct_val)) => {
            let from_text = decode_status_binding_envelope(&text_val)?;
            let from_struct = decode_status_binding_envelope(&struct_val)?;
            if from_text != from_struct {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "status content[0].text and structuredContent envelopes did not match",
                ));
            }
            Ok(from_text)
        }
        (Some(text_val), None) => decode_status_binding_envelope(&text_val),
        (None, Some(struct_val)) => decode_status_binding_envelope(&struct_val),
        (None, None) => Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            "status response contained neither content[0].text nor structuredContent",
        )),
    }
}

fn decode_status_binding_envelope(envelope: &Value) -> Result<VerifiedStatusBinding, NativeError> {
    let Some(obj) = envelope.as_object() else {
        return Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            "status tool wrapper must be a JSON object",
        ));
    };
    if let Some(err_val) = obj.get("error") {
        if !err_val.is_null() {
            return Err(NativeError::new(
                NativeErrorCode::McpToolError,
                "status tool returned a structured error envelope",
            ));
        }
    }
    if let Some(tool_val) = obj.get("tool") {
        if tool_val.as_str() != Some(STATUS_TOOL_NAME) {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "MCP tool wrapper `tool` field did not match `status`",
            ));
        }
    }

    let wrapper_binding_id = obj.get("binding_id").and_then(Value::as_str);
    let data_val = obj.get("data").unwrap_or(envelope);
    let nested_binding_obj = data_val.get("binding");
    let nested_binding_id = nested_binding_obj
        .and_then(|b| b.get("binding_id"))
        .and_then(Value::as_str)
        .or_else(|| data_val.get("binding_id").and_then(Value::as_str));
    let nested_thread_uuid = nested_binding_obj
        .and_then(|b| b.get("thread_uuid"))
        .and_then(Value::as_str);

    if let (Some(outer), Some(inner)) = (wrapper_binding_id, nested_binding_id) {
        if outer != inner {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "status wrapper binding_id and data.binding.binding_id did not match",
            ));
        }
    }

    let Some(raw_binding_id) = wrapper_binding_id.or(nested_binding_id) else {
        return Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            "status tool response did not contain a structured binding_id",
        ));
    };

    let binding_id = validate_bounded_identifier(raw_binding_id, "binding_id", MAX_ID_CHARS)?;
    let thread_uuid = match nested_thread_uuid {
        Some(raw_uuid) => Some(validate_canonical_thread_uuid(
            raw_uuid,
            "status.binding.thread_uuid",
        )?),
        None => None,
    };

    Ok(VerifiedStatusBinding {
        binding_id,
        thread_uuid,
    })
}

fn verify_isolated_status_bindings(
    expected_thread_a: &str,
    expected_binding_a: &str,
    status_a: &VerifiedStatusBinding,
    expected_thread_b: &str,
    expected_binding_b: &str,
    status_b: &VerifiedStatusBinding,
) -> Result<(), NativeError> {
    if status_a.binding_id == status_b.binding_id {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "status calls on thread_a and thread_b returned identical binding_id",
        ));
    }
    if status_a.binding_id != expected_binding_a {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "thread_a status binding_id did not match expected created binding for thread_a",
        ));
    }
    if status_b.binding_id != expected_binding_b {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "thread_b status binding_id did not match expected created binding for thread_b",
        ));
    }
    if let Some(ref t_a) = status_a.thread_uuid {
        if t_a != expected_thread_a {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_a status binding.thread_uuid did not match thread_a",
            ));
        }
    }
    if let Some(ref t_b) = status_b.thread_uuid {
        if t_b != expected_thread_b {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_b status binding.thread_uuid did not match thread_b",
            ));
        }
    }
    Ok(())
}

fn parse_first_text_content_envelope(content: &[Value]) -> Result<Option<Value>, NativeError> {
    let Some(first) = content.first() else {
        return Ok(None);
    };
    let Some(text) = first.get("text").and_then(Value::as_str) else {
        return Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            "MCP content[0] block is missing a string `text` field",
        ));
    };
    let parsed: Value = serde_json::from_str(text).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "MCP content[0].text is not valid JSON",
        )
    })?;
    Ok(Some(parsed))
}

fn extract_structured_envelope(structured: Option<&Value>) -> Result<Option<Value>, NativeError> {
    let Some(val) = structured else {
        return Ok(None);
    };
    if val.is_null() {
        return Ok(None);
    }
    if !val.is_object() {
        return Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            "MCP structuredContent must be a JSON object when present",
        ));
    }
    Ok(Some(val.clone()))
}

fn map_supervisor_error(err: threadlane::Error, secrets: &[&str]) -> NativeError {
    let api_err = err.to_api_error();
    let code = match api_err.code {
        threadlane::ErrorCode::ValidationFailed => NativeErrorCode::ValidationFailed,
        threadlane::ErrorCode::Unauthorized => NativeErrorCode::UnauthorizedOperator,
        _ => NativeErrorCode::TransportUnavailable,
    };
    redact_error(NativeError::new(code, api_err.message), secrets)
}

fn redact_error(err: NativeError, secrets: &[&str]) -> NativeError {
    let scrubbed = scrub_secrets_and_tokens(err.message(), secrets);
    NativeError {
        code: err.error_code(),
        message: sanitize_diagnostic_text(&scrubbed),
        rpc_code: err.rpc_code,
    }
}

fn scrub_secrets_and_tokens(raw: &str, secrets: &[&str]) -> String {
    let mut current = raw.to_string();
    for secret in secrets {
        if !secret.is_empty() && current.contains(secret) {
            current = current.replace(secret, "[REDACTED]");
        }
    }
    for prefix in ["nce_", "epc_", "op_"] {
        current = scrub_prefixed_token(&current, prefix);
    }
    current
}

fn scrub_prefixed_token(input: &str, prefix: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(pos) = rest.find(prefix) {
        out.push_str(&rest[..pos]);
        let after_prefix = &rest[pos + prefix.len()..];
        let token_len = after_prefix
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
            .map(char::len_utf8)
            .sum::<usize>();
        if token_len > 0 {
            out.push_str("[REDACTED]");
            rest = &after_prefix[token_len..];
        } else {
            out.push_str(prefix);
            rest = after_prefix;
        }
    }
    out.push_str(rest);
    out
}

fn print_bounded_json<T: Serialize>(value: &T) -> Result<(), NativeError> {
    let encoded = serde_json::to_string_pretty(value).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to serialize probe report JSON",
        )
    })?;
    if encoded.len() > MAX_PROBE_JSON_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            "serialized probe report exceeded bounded output limit",
        ));
    }
    println!("{encoded}");
    Ok(())
}

fn print_safe_error(err: &NativeError, secrets: &[&str]) {
    let safe_err = redact_error(err.clone(), secrets);
    if let Ok(serialized) = serde_json::to_string_pretty(&safe_err) {
        eprintln!("{serialized}");
    } else {
        eprintln!(
            "{{\"code\":\"{}\",\"message\":\"native probe error\"}}",
            safe_err.code()
        );
    }
}
