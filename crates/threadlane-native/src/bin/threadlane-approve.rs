#![deny(unsafe_code)]
#![deny(missing_docs)]

//! Explicit local operator approval CLI (`threadlane-approve`) for native Codex workflows.
//!
//! Prepares a read-only digest-bound [`threadlane_authority::OperatorApprovalPreview`] (`prepare`) and records an
//! operator-confirmed [`ApprovalSummary`] (`record`) via [`AuthorityStore::approve_confirmed`].
//!
//! `prepare-private` is the opt-in stdin route: it accepts one bounded strict
//! [`threadlane_authority::OperatorApprovalIntent`] JSON document on stdin (never argv), runs the
//! same authoritative prepare and live/fixed-scope validation as `prepare`, then persists the
//! unchanged typed preview plus newline as `preview.json` (`0600`) in a new unique private
//! (`0700`) temp directory and returns a bounded `{preview_path, preview}` receipt. It writes no
//! approval ledger entry and dispatches no work.
//! `prepare-private-exact` uses the same flow but requires all five budget fields on stdin.
//!
//! `prepare-task` is the scoped single-task fast path: it accepts one bounded strict JSON request
//! on stdin (never argv), creates and publishes exactly one ordinary worker task through
//! `ScopedWorkflow`, and then runs the same authoritative private preview flow. It creates only
//! the draft/publication and private preview; it never records authority, reserves execution, or
//! starts a worker.
//! `prepare-task-exact` uses the same flow but requires all five budget fields on stdin.
//!
//! `record` accepts exactly one private input: the unchanged normalized
//! [`threadlane_authority::OperatorApprovalRequest`] (`--request`) or the complete typed
//! [`threadlane_authority::OperatorApprovalPreview`] returned by `prepare` (`--preview`). The preview
//! route recompiles its unchanged normalized request and requires the fresh authoritative preview
//! to match the whole typed preview before the same persistence checks run; it grants no new
//! authority.
//!
//! ## Security and Authority Boundaries
//!
//! - **Not a transcript listener or classifier**: Never reads conversation turns, never infers
//!   approval from `"yes"` text heuristics, and never accepts authority from MCP or worker output.
//! - **Explicit operator attestation**: `--confirm-subject`, `--confirm-thread`, and
//!   `--operator-approved` are explicit local operator attestations of the scope presented to the
//!   human in native Codex; they do not cryptographically authenticate human identity.
//! - **Live target binding confirmation**: Verifies `(binding_id, thread_uuid)` against the live
//!   authenticated local [`SupervisorClient`] before validation and again immediately before
//!   returning `prepare` or persisting `record`.

use std::ffi::OsStr;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use threadlane::{ErrorCode as RuntimeErrorCode, SupervisorClient};
use threadlane_authority::validation::{
    validate_canonical_thread_uuid, validate_identifier, validate_sha256_hex,
};
use threadlane_authority::{
    ApprovalSummary, AuthorityError, AuthorityStore, OperatorApprovalIntent,
    OperatorApprovalPreview, OperatorApprovalRequest,
};
use threadlane_native::fs_safety::{
    ensure_platform_supported, read_private_file_bounded, validate_absolute_no_symlink_components,
};
use threadlane_native::{open_workflow_stores, NativeError, NativeErrorCode};
use threadlane_workers::{ExecutionBudgets, ProgressWatchdogPolicy};

/// Scoped single-task fast path implementation.
#[path = "threadlane-approve/ordinary_task.rs"]
mod ordinary_task;

/// Maximum byte size (`1 MiB`) permitted for private JSON request files, bounded stdin JSON, and
/// bounded stdout JSON responses (including the trailing newline).
const MAX_IO_BYTES: usize = 1_048_576;

/// Error code emitted when a mutating approval operation completed before its bounded stdout
/// response could be serialized and flushed.
const OUTCOME_UNKNOWN_CODE: &str = "outcome_unknown";

#[derive(Debug, Parser)]
#[command(
    name = "threadlane-approve",
    version,
    about = "Trusted local operator CLI for preparing and recording workflow scope approvals"
)]
struct Cli {
    /// Normalized absolute path to the private running supervisor state directory (`supervisor.json`).
    #[arg(long = "state-dir", value_name = "ABS", global = true)]
    state_dir: Option<PathBuf>,
    /// Normalized absolute path to the private global workflow registry store directory (`registry.json`).
    #[arg(long = "registry-dir", value_name = "ABS", global = true)]
    registry_dir: Option<PathBuf>,
    /// Normalized absolute path to the private workflow plan store directory (`plans.json`).
    #[arg(long = "plans-dir", value_name = "ABS", global = true)]
    plans_dir: Option<PathBuf>,
    /// Normalized absolute path to the private operator approval authority store directory (`approvals.json`).
    #[arg(long = "authority-dir", value_name = "ABS", global = true)]
    authority_dir: Option<PathBuf>,
    /// Optional fixed binding_id scope enforced against the decoded approval request.
    #[arg(long = "binding", value_name = "BINDING_ID", global = true)]
    binding: Option<String>,
    /// Optional fixed canonical lowercase hyphenated non-nil thread_uuid scope enforced against the decoded approval request.
    #[arg(long = "thread", value_name = "THREAD_UUID", global = true)]
    thread: Option<String>,
    /// Optional fixed canonical repository project root enforced against the decoded approval request `repository_path`.
    #[arg(long = "project-root", value_name = "ABS", global = true)]
    project_root: Option<PathBuf>,
    #[command(subcommand)]
    command: ApproveCommand,
}

#[derive(Debug, Subcommand)]
#[command(rename_all = "kebab-case")]
enum ApproveCommand {
    /// Prepare and validate a read-only `OperatorApprovalPreview` from a private `OperatorApprovalIntent` JSON file.
    Prepare {
        /// Normalized absolute path to the private JSON request file (`OperatorApprovalIntent`).
        #[arg(long = "request", value_name = "ABS_PRIVATE_JSON")]
        request: PathBuf,
    },
    /// Prepare a validated preview from one bounded strict `OperatorApprovalIntent` JSON document on stdin.
    ///
    /// Runs the same authoritative prepare and live/fixed-scope validation as `prepare`, then
    /// writes the unchanged typed preview plus newline to a new `0600` `preview.json` in a unique
    /// private `0700` temp directory and returns `{preview_path, preview}`. This route accepts no
    /// output path argument and writes no authority ledger entry.
    PreparePrivate,
    /// Prepare a private preview, requiring all five execution budget values on stdin.
    PreparePrivateExact,
    /// Create and publish one ordinary single-task plan, then write its private operator preview.
    ///
    /// Requires the complete fixed `--binding`, `--thread`, and `--project-root` scope forwarded
    /// by `threadlane-context`, accepts one bounded strict JSON request on stdin (never argv),
    /// and creates only the draft/publication and private preview. It never records approval
    /// authority, reserves execution, or starts a worker.
    PrepareTask,
    /// Publish one ordinary task and preview it, requiring all five execution budget values.
    PrepareTaskExact,
    /// Validate and persist a confirmed operator approval into `approvals.json`.
    ///
    /// Requires exactly one input: `--request` (unchanged normalized `OperatorApprovalRequest`)
    /// or `--preview` (the complete typed `OperatorApprovalPreview` returned by `prepare` or
    /// `prepare-private`).
    #[command(group(
        clap::ArgGroup::new("record_input")
            .required(true)
            .multiple(false)
            .args(["request", "preview"])
    ))]
    Record {
        /// Normalized absolute path to the private JSON request file (`OperatorApprovalRequest`).
        #[arg(long = "request", value_name = "ABS_PRIVATE_JSON")]
        request: Option<PathBuf>,
        /// Normalized absolute path to the private JSON preview file (`OperatorApprovalPreview`).
        #[arg(long = "preview", value_name = "ABS_PRIVATE_JSON")]
        preview: Option<PathBuf>,
        /// Explicit 64-character lowercase hex SHA-256 subject digest confirmation.
        #[arg(long = "confirm-subject", value_name = "SHA256")]
        confirm_subject: String,
        /// Explicit canonical lowercase hyphenated non-nil native Codex thread UUID confirmation.
        #[arg(long = "confirm-thread", value_name = "THREAD_UUID")]
        confirm_thread: String,
        /// Explicit operator attestation that the human approved the exact presented scope.
        #[arg(long = "operator-approved")]
        operator_approved: bool,
    },
}

/// Bounded, non-authoritative context for reconciling the original plan after a later failure.
///
/// The presence of this context means the named phase was confirmed by an actual mutation
/// receipt. It never grants authority, never implies an unconfirmed publication succeeded, and
/// never authorizes an automatic retry.
#[derive(Debug, Clone, Serialize)]
struct RecoveryContext {
    plan_id: String,
    revision: u64,
    phase: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct CliError {
    code: &'static str,
    message: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    recovery: Option<RecoveryContext>,
}

impl CliError {
    const fn new(code: &'static str, message: &'static str) -> Self {
        Self {
            code,
            message,
            recovery: None,
        }
    }

    fn with_recovery(mut self, recovery: Option<RecoveryContext>) -> Self {
        self.recovery = recovery;
        self
    }

    const fn invalid_input(message: &'static str) -> Self {
        Self::new("invalid_input", message)
    }

    const fn security_violation(message: &'static str) -> Self {
        Self::new("security_violation", message)
    }

    const fn binding_mismatch(message: &'static str) -> Self {
        Self::new("binding_mismatch", message)
    }

    const fn thread_uuid_mismatch(message: &'static str) -> Self {
        Self::new("thread_uuid_mismatch", message)
    }

    const fn outcome_unknown(message: &'static str) -> Self {
        Self::new(OUTCOME_UNKNOWN_CODE, message)
    }

    fn from_native(err: &NativeError) -> Self {
        match err.error_code() {
            NativeErrorCode::SecurityViolation => Self::security_violation(
                "store directory failed private permission or symlink security checks",
            ),
            NativeErrorCode::UnsupportedPlatform => Self::new(
                "unsupported_platform",
                "threadlane-approve is only supported on Unix platforms",
            ),
            NativeErrorCode::IoError => {
                Self::new("io", "filesystem I/O error while opening workflow stores")
            }
            _ => Self::invalid_input("workflow store configuration or state is invalid"),
        }
    }

    fn from_supervisor(err: &threadlane::Error) -> Self {
        match err {
            threadlane::Error::SecurityViolation(_) => Self::security_violation(
                "runtime state directory or descriptor failed security checks",
            ),
            threadlane::Error::UnsupportedPlatform(_) => Self::new(
                "unsupported_platform",
                "threadlane-approve is only supported on Unix platforms",
            ),
            _ => {
                let api_err = err.to_api_error();
                match api_err.code {
                    RuntimeErrorCode::BindingNotFound => Self::new(
                        "binding_not_found",
                        "requested binding_id was not found in the live supervisor runtime",
                    ),
                    RuntimeErrorCode::Unauthorized => Self::new(
                        "unauthorized_operator",
                        "supervisor rejected local operator authentication",
                    ),
                    RuntimeErrorCode::ValidationFailed => Self::new(
                        "validation",
                        "binding_id failed supervisor identifier validation",
                    ),
                    RuntimeErrorCode::SecurityViolation => Self::security_violation(
                        "runtime state directory or descriptor failed security checks",
                    ),
                    _ => Self::new(
                        "transport_unavailable",
                        "live supervisor runtime is offline or unreachable",
                    ),
                }
            }
        }
    }

    fn from_authority(err: &AuthorityError) -> Self {
        match err {
            AuthorityError::Validation { field, .. } => {
                let message = match field.as_str() {
                    "binding_id" => "binding_id failed identifier validation",
                    "thread_uuid" | "confirm_thread" => {
                        "thread_uuid must be a canonical lowercase hyphenated non-nil UUID"
                    }
                    "plan_id" => "plan_id failed identifier validation",
                    "plan_revision" => "plan_revision failed revision bounds validation",
                    "expected_registry_revision" => {
                        "expected_registry_revision failed revision bounds validation"
                    }
                    "expected_plan_digest" => {
                        "expected_plan_digest must be a 64-character lowercase hex SHA-256 digest"
                    }
                    "expected_subject_digest" => {
                        "confirmed subject digest is invalid or did not match the compiled approval subject"
                    }
                    "max_parallel" => "max_parallel is outside the permitted concurrency range",
                    "budgets" => "execution budgets failed bounds validation",
                    "approved_task_ids" | "approved_task_ids entry" => {
                        "approved_task_ids failed validation (empty, duplicate, or missing required transitive predecessors)"
                    }
                    "approved_write_paths" => {
                        "approved_write_paths failed count or entry validation"
                    }
                    "approvals" => {
                        "authority store approval capacity or serialization limit exceeded"
                    }
                    _ => "approval request field failed validation",
                };
                Self::new(err.code(), message)
            }
            AuthorityError::PlanNotFound { .. } => Self::new(
                err.code(),
                "referenced workflow plan revision was not found in the plan store",
            ),
            AuthorityError::ApprovalNotFound { .. } => Self::new(
                err.code(),
                "referenced approval record was not found in the authority store",
            ),
            AuthorityError::BindingMismatch { .. } => Self::new(
                err.code(),
                "request binding_id does not match the target plan revision binding_id",
            ),
            AuthorityError::ThreadUuidMismatch { .. } => Self::new(
                err.code(),
                "thread_uuid does not match the confirmed or approved native thread UUID",
            ),
            AuthorityError::UnpublishedOrSupersededPlan { .. } => Self::new(
                err.code(),
                "target plan revision is draft or superseded and is not an active published revision",
            ),
            AuthorityError::StaleRegistryRevision { .. } => Self::new(
                err.code(),
                "expected_registry_revision does not match the plan profile_revision and current registry revision",
            ),
            AuthorityError::PlanDigestMismatch { .. } => Self::new(
                err.code(),
                "compiled plan digest does not match expected_plan_digest",
            ),
            AuthorityError::UnknownTask { .. } => Self::new(
                err.code(),
                "approved_task_ids contains a task_id that does not exist in the compiled plan revision",
            ),
            AuthorityError::UnsupportedTaskAdapter { .. } => Self::new(
                err.code(),
                "selected task uses an unsupported execution tool adapter",
            ),
            AuthorityError::InvalidExecutable { .. } => Self::new(
                err.code(),
                "configured worker tool executable path could not be resolved or validated",
            ),
            AuthorityError::InvalidRepository { .. } => Self::new(
                err.code(),
                "repository_path is not an existing canonical non-overlapping Git repository",
            ),
            AuthorityError::InvalidBaseCommit { .. } => Self::new(
                err.code(),
                "base_commit is not an exact 40-character hex commit existing in repository_path",
            ),
            AuthorityError::InvalidWriteScope { .. } => Self::new(
                err.code(),
                "write scope entry failed syntax, symlink, protected-path, or base-commit validation",
            ),
            AuthorityError::UnapprovedTaskWriteScope { .. } => Self::new(
                err.code(),
                "selected task write scope is not contained within operator approved_write_paths",
            ),
            AuthorityError::InvalidWorkArea { .. } => {
                Self::new(err.code(), "work_area_root failed validation")
            }
            AuthorityError::WorkersConfigInvalid { .. } => Self::new(
                err.code(),
                "synthesized worker profile configuration failed validation",
            ),
            AuthorityError::SecurityViolation { .. } => Self::new(
                err.code(),
                "path, symlink, or private permission security check failed",
            ),
            AuthorityError::CorruptStore { .. } => Self::new(
                err.code(),
                "authority store ledger is corrupt or failed integrity verification",
            ),
            AuthorityError::Io { .. } => Self::new(
                err.code(),
                "filesystem I/O error during authority store operation",
            ),
            AuthorityError::Registry { .. } => {
                Self::new(err.code(), "workflow registry store operation failed")
            }
            AuthorityError::Plan { .. } => Self::new(
                err.code(),
                "workflow plan store or compilation operation failed",
            ),
        }
    }
}

#[derive(Debug, Serialize)]
struct PrivatePreviewReceipt {
    preview_path: PathBuf,
    preview: OperatorApprovalPreview,
}

/// Explicit stdin budgets. Unlike the persisted type, these five limits cannot default.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExplicitBudgets {
    timeout_ms: u64,
    max_line_bytes: usize,
    max_stdout_bytes: usize,
    max_stderr_bytes: usize,
    max_events: usize,
    #[serde(default)]
    progress_watchdog: Option<ProgressWatchdogPolicy>,
}

impl From<ExplicitBudgets> for ExecutionBudgets {
    fn from(budgets: ExplicitBudgets) -> Self {
        Self {
            timeout_ms: budgets.timeout_ms,
            max_line_bytes: budgets.max_line_bytes,
            max_stdout_bytes: budgets.max_stdout_bytes,
            max_stderr_bytes: budgets.max_stderr_bytes,
            max_events: budgets.max_events,
            progress_watchdog: budgets.progress_watchdog,
        }
    }
}

/// Stdin intent with explicit budgets, converted to the existing authority input unchanged.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExplicitApprovalIntent {
    binding_id: String,
    thread_uuid: String,
    plan_id: String,
    plan_revision: u64,
    expected_registry_revision: u64,
    repository_path: PathBuf,
    base_commit: String,
    approved_task_ids: Vec<String>,
    approved_write_paths: Vec<String>,
    max_parallel: usize,
    budgets: ExplicitBudgets,
}

impl From<ExplicitApprovalIntent> for OperatorApprovalIntent {
    fn from(intent: ExplicitApprovalIntent) -> Self {
        Self {
            binding_id: intent.binding_id,
            thread_uuid: intent.thread_uuid,
            plan_id: intent.plan_id,
            plan_revision: intent.plan_revision,
            expected_registry_revision: intent.expected_registry_revision,
            repository_path: intent.repository_path,
            base_commit: intent.base_commit,
            approved_task_ids: intent.approved_task_ids,
            approved_write_paths: intent.approved_write_paths,
            max_parallel: intent.max_parallel,
            budgets: intent.budgets.into(),
        }
    }
}

/// Reads one bounded strict JSON DTO from stdin without accepting any argv JSON.
fn load_stdin_json<T: DeserializeOwned>() -> Result<T, CliError> {
    let mut buf = Vec::new();
    io::stdin()
        .lock()
        .take((MAX_IO_BYTES as u64) + 1)
        .read_to_end(&mut buf)
        .map_err(|_| CliError::invalid_input("stdin request could not be read"))?;
    if buf.len() > MAX_IO_BYTES {
        return Err(CliError::invalid_input(
            "stdin request exceeds maximum size of 1 MiB",
        ));
    }

    serde_json::from_slice(&buf)
        .map_err(|_| CliError::invalid_input("stdin JSON is malformed or violates the schema"))
}

/// Runs the single authoritative prepare flow shared by `prepare` and `prepare-private`.
async fn prepare_operator_preview(
    dirs: &ValidatedStoreDirs,
    fixed_scope: Option<&FixedApprovalScope>,
    intent: OperatorApprovalIntent,
) -> Result<OperatorApprovalPreview, CliError> {
    let binding_id = validate_identifier("binding_id", &intent.binding_id)
        .map_err(|e| CliError::from_authority(&e))?;
    let thread_uuid = validate_canonical_thread_uuid("thread_uuid", &intent.thread_uuid)
        .map_err(|e| CliError::from_authority(&e))?;
    if let Some(fixed) = fixed_scope {
        enforce_fixed_scope_on_request(
            fixed,
            &binding_id,
            &thread_uuid,
            &intent.repository_path,
            None,
        )?;
    }
    validate_repo_non_overlap_with_state_dir(&intent.repository_path, &dirs.state_dir)?;

    let supervisor = SupervisorClient::from_state_dir(&dirs.state_dir)
        .map_err(|e| CliError::from_supervisor(&e))?;
    verify_live_binding(&supervisor, &binding_id, &thread_uuid).await?;

    let authority = open_validated_authority_store(dirs)?;
    let preview = authority
        .prepare_for_operator(intent)
        .map_err(|e| CliError::from_authority(&e))?;

    if let Some(fixed) = fixed_scope {
        enforce_fixed_scope_on_request(
            fixed,
            &preview.request.binding_id,
            &preview.request.thread_uuid,
            &preview.request.repository_path,
            None,
        )?;
    }

    verify_live_binding(
        &supervisor,
        &preview.request.binding_id,
        &preview.request.thread_uuid,
    )
    .await?;

    Ok(preview)
}

/// Writes the unchanged typed preview plus newline into a new private temp directory.
///
/// The returned [`tempfile::TempDir`] stays owned by the caller until the bounded receipt has
/// been written, so any failure removes the directory and any success keeps it.
#[cfg(unix)]
fn write_private_preview_file(
    preview: &OperatorApprovalPreview,
) -> Result<(PathBuf, tempfile::TempDir), CliError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

    let mut bytes = serde_json::to_vec(preview)
        .map_err(|_| CliError::new("json", "failed to serialize prepared approval preview"))?;
    bytes.push(b'\n');
    if bytes.len() > MAX_IO_BYTES {
        return Err(CliError::invalid_input(
            "private approval preview exceeds maximum size of 1 MiB",
        ));
    }

    let directory = tempfile::Builder::new()
        .prefix("threadlane-approval-preview-")
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()
        .map_err(|_| CliError::new("io", "failed to create private preview directory"))?;
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
        .map_err(|_| CliError::new("io", "failed to secure private preview directory"))?;

    let canonical_dir = fs::canonicalize(directory.path())
        .map_err(|_| CliError::new("io", "failed to canonicalize private preview directory"))?;
    let dir_meta = fs::symlink_metadata(&canonical_dir)
        .map_err(|_| CliError::new("io", "failed to inspect private preview directory"))?;
    if !canonical_dir.is_absolute()
        || dir_meta.file_type().is_symlink()
        || !dir_meta.file_type().is_dir()
        || (dir_meta.mode() & 0o777) != 0o700
    {
        return Err(CliError::security_violation(
            "private preview directory failed canonical path or 0700 permission checks",
        ));
    }

    let preview_path = canonical_dir.join("preview.json");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&preview_path)
        .map_err(|_| CliError::new("io", "failed to create private approval preview file"))?;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|_| CliError::new("io", "failed to secure private approval preview file"))?;
    file.write_all(&bytes)
        .map_err(|_| CliError::new("io", "failed to write private approval preview file"))?;
    file.sync_all()
        .map_err(|_| CliError::new("io", "failed to sync private approval preview file"))?;
    drop(file);

    let readback =
        read_private_file_bounded(&preview_path, MAX_IO_BYTES, "private approval preview file")
            .map_err(|err| match err.error_code() {
                NativeErrorCode::SecurityViolation => CliError::security_violation(
                    "private approval preview file failed path or permission checks",
                ),
                _ => CliError::new("io", "failed to read back private approval preview file"),
            })?
            .ok_or_else(|| {
                CliError::new("io", "failed to read back private approval preview file")
            })?;
    if readback != bytes {
        return Err(CliError::security_violation(
            "private approval preview file failed readback checks",
        ));
    }

    Ok((preview_path, directory))
}

#[cfg(not(unix))]
fn write_private_preview_file(
    _preview: &OperatorApprovalPreview,
) -> Result<(PathBuf, tempfile::TempDir), CliError> {
    Err(CliError::new(
        "unsupported_platform",
        "threadlane-approve is only supported on Unix platforms",
    ))
}

#[tokio::main]
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
            emit_error(&CliError::invalid_input(
                "invalid or missing command-line arguments",
            ));
            return ExitCode::from(1);
        }
    };

    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            emit_error(&err);
            ExitCode::from(1)
        }
    }
}

async fn run(cli: Cli) -> Result<(), CliError> {
    ensure_platform_supported().map_err(|e| CliError::from_native(&e))?;

    let (state_dir, registry_dir, plans_dir, authority_dir) = resolve_store_dir_args(&cli)?;
    let raw_binding = cli.binding.as_deref();
    let raw_thread = cli.thread.as_deref();
    let raw_project_root = cli.project_root.as_deref();

    match cli.command {
        ApproveCommand::Prepare { request } => {
            let dirs =
                validate_four_store_dirs(&state_dir, &registry_dir, &plans_dir, &authority_dir)?;
            let fixed_scope =
                resolve_fixed_scope(raw_binding, raw_thread, raw_project_root, &dirs)?;
            let intent: OperatorApprovalIntent = load_request_json(&request)?;
            let preview = prepare_operator_preview(&dirs, fixed_scope.as_ref(), intent).await?;

            write_stdout_json(&preview, false)
        }
        ApproveCommand::PreparePrivate | ApproveCommand::PreparePrivateExact => {
            let dirs =
                validate_four_store_dirs(&state_dir, &registry_dir, &plans_dir, &authority_dir)?;
            let fixed_scope =
                resolve_fixed_scope(raw_binding, raw_thread, raw_project_root, &dirs)?;
            let intent = if matches!(cli.command, ApproveCommand::PreparePrivateExact) {
                let explicit: ExplicitApprovalIntent = load_stdin_json()?;
                explicit.into()
            } else {
                load_stdin_json()?
            };
            let preview = prepare_operator_preview(&dirs, fixed_scope.as_ref(), intent).await?;
            let (preview_path, directory) = write_private_preview_file(&preview)?;

            let receipt = PrivatePreviewReceipt {
                preview_path,
                preview,
            };

            // The receipt is serialized before stdout so an oversized or failed response leaves
            // no private preview directory behind; `keep` commits it only after a successful
            // bounded write.
            write_stdout_json(&receipt, false)?;
            let _kept_path = directory.keep();
            Ok(())
        }
        ApproveCommand::PrepareTask | ApproveCommand::PrepareTaskExact => {
            let dirs =
                validate_four_store_dirs(&state_dir, &registry_dir, &plans_dir, &authority_dir)?;
            let fixed_scope =
                resolve_complete_fixed_scope(raw_binding, raw_thread, raw_project_root, &dirs)?;
            ordinary_task::run_prepare_task(
                &dirs,
                &fixed_scope,
                matches!(cli.command, ApproveCommand::PrepareTaskExact),
            )
            .await
        }
        ApproveCommand::Record {
            request,
            preview,
            confirm_subject,
            confirm_thread,
            operator_approved,
        } => {
            if !operator_approved {
                return Err(CliError::invalid_input(
                    "record requires explicit --operator-approved attestation flag",
                ));
            }
            let checked_subject = validate_sha256_hex("expected_subject_digest", &confirm_subject)
                .map_err(|e| CliError::from_authority(&e))?;
            let checked_confirm_thread =
                validate_canonical_thread_uuid("confirm_thread", &confirm_thread)
                    .map_err(|e| CliError::from_authority(&e))?;

            let dirs =
                validate_four_store_dirs(&state_dir, &registry_dir, &plans_dir, &authority_dir)?;
            let fixed_scope =
                resolve_fixed_scope(raw_binding, raw_thread, raw_project_root, &dirs)?;
            if let Some(ref fixed) = fixed_scope {
                if checked_confirm_thread != fixed.thread_uuid {
                    return Err(CliError::thread_uuid_mismatch(
                        "confirmation --confirm-thread does not match fixed context --thread scope",
                    ));
                }
            }
            let (req, supplied_preview): (
                OperatorApprovalRequest,
                Option<OperatorApprovalPreview>,
            ) = match (request, preview) {
                (Some(request), None) => (load_request_json(&request)?, None),
                (None, Some(preview)) => {
                    let supplied: OperatorApprovalPreview = load_request_json(&preview)?;
                    (supplied.request.clone(), Some(supplied))
                }
                _ => {
                    return Err(CliError::invalid_input(
                        "record requires exactly one of --request or --preview",
                    ));
                }
            };

            let binding_id = validate_identifier("binding_id", &req.binding_id)
                .map_err(|e| CliError::from_authority(&e))?;
            let thread_uuid = validate_canonical_thread_uuid("thread_uuid", &req.thread_uuid)
                .map_err(|e| CliError::from_authority(&e))?;
            if checked_confirm_thread != thread_uuid {
                return Err(CliError::thread_uuid_mismatch(
                    "confirmation --confirm-thread does not match request thread_uuid",
                ));
            }
            if let Some(ref fixed) = fixed_scope {
                enforce_fixed_scope_on_request(
                    fixed,
                    &binding_id,
                    &thread_uuid,
                    &req.repository_path,
                    Some(&checked_confirm_thread),
                )?;
            }
            validate_repo_non_overlap_with_state_dir(&req.repository_path, &dirs.state_dir)?;

            let supervisor = SupervisorClient::from_state_dir(&dirs.state_dir)
                .map_err(|e| CliError::from_supervisor(&e))?;
            verify_live_binding(&supervisor, &binding_id, &thread_uuid).await?;

            let authority = open_validated_authority_store(&dirs)?;
            let fresh_preview = authority
                .preview_for_operator(req.clone())
                .map_err(|e| CliError::from_authority(&e))?;
            if supplied_preview.is_some_and(|supplied| supplied != fresh_preview) {
                return Err(CliError::new(
                    "validation",
                    "private approval preview does not match the fresh authoritative preview; re-run prepare",
                ));
            }
            if fresh_preview.subject_digest != checked_subject {
                return Err(CliError::new(
                    "validation",
                    "confirmed subject digest is invalid or did not match the compiled approval subject",
                ));
            }
            if let Some(ref fixed) = fixed_scope {
                enforce_fixed_scope_on_request(
                    fixed,
                    &fresh_preview.request.binding_id,
                    &fresh_preview.request.thread_uuid,
                    &fresh_preview.request.repository_path,
                    Some(&checked_confirm_thread),
                )?;
            }

            verify_live_binding(&supervisor, &binding_id, &thread_uuid).await?;

            let summary: ApprovalSummary = authority
                .approve_confirmed(req, &checked_subject)
                .map_err(|e| CliError::from_authority(&e))?;

            write_stdout_json(&summary, true)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FixedApprovalScope {
    binding_id: String,
    thread_uuid: String,
    project_root: Option<PathBuf>,
}

fn resolve_store_dir_args(cli: &Cli) -> Result<(PathBuf, PathBuf, PathBuf, PathBuf), CliError> {
    let (Some(state_dir), Some(registry_dir), Some(plans_dir), Some(authority_dir)) = (
        cli.state_dir.clone(),
        cli.registry_dir.clone(),
        cli.plans_dir.clone(),
        cli.authority_dir.clone(),
    ) else {
        return Err(CliError::invalid_input(
            "invalid or missing command-line arguments",
        ));
    };
    Ok((state_dir, registry_dir, plans_dir, authority_dir))
}

fn resolve_fixed_scope(
    binding: Option<&str>,
    thread: Option<&str>,
    project_root: Option<&Path>,
    dirs: &ValidatedStoreDirs,
) -> Result<Option<FixedApprovalScope>, CliError> {
    if binding.is_none() && thread.is_none() && project_root.is_none() {
        return Ok(None);
    }

    let (Some(raw_binding), Some(raw_thread)) = (binding, thread) else {
        return Err(CliError::invalid_input(
            "fixed-scope flags require both --binding and --thread",
        ));
    };

    let binding_id =
        validate_identifier("binding_id", raw_binding).map_err(|e| CliError::from_authority(&e))?;
    let thread_uuid = validate_canonical_thread_uuid("thread_uuid", raw_thread)
        .map_err(|e| CliError::from_authority(&e))?;
    let clean_project_root = match project_root {
        Some(path) => Some(validate_existing_canonical_project_root(path, dirs)?),
        None => None,
    };

    Ok(Some(FixedApprovalScope {
        binding_id,
        thread_uuid,
        project_root: clean_project_root,
    }))
}

/// Resolves the complete fixed context scope required by the `prepare-task` fast path.
fn resolve_complete_fixed_scope(
    binding: Option<&str>,
    thread: Option<&str>,
    project_root: Option<&Path>,
    dirs: &ValidatedStoreDirs,
) -> Result<FixedApprovalScope, CliError> {
    let fixed = resolve_fixed_scope(binding, thread, project_root, dirs)?.ok_or_else(|| {
        CliError::invalid_input(
            "prepare-task requires --binding, --thread, and --project-root scope flags",
        )
    })?;
    if fixed.project_root.is_none() {
        return Err(CliError::invalid_input(
            "prepare-task requires --project-root together with --binding and --thread",
        ));
    }
    Ok(fixed)
}

#[cfg(unix)]
fn validate_existing_canonical_project_root(
    path: &Path,
    dirs: &ValidatedStoreDirs,
) -> Result<PathBuf, CliError> {
    use std::os::unix::ffi::OsStrExt;

    let raw_bytes = path.as_os_str().as_bytes();
    if raw_bytes.is_empty() || !path.is_absolute() || raw_bytes.ends_with(b"/") {
        return Err(CliError::invalid_input(
            "project-root path must be a normalized absolute directory path",
        ));
    }

    let _ = validate_absolute_no_symlink_components(path, "project root")
        .map_err(|e| CliError::from_native(&e))?;

    let mut normal_parts: Vec<&OsStr> = Vec::new();
    let mut saw_root = false;
    for component in path.components() {
        match component {
            Component::RootDir if !saw_root && normal_parts.is_empty() => {
                saw_root = true;
            }
            Component::Normal(part) if saw_root && !part.is_empty() => {
                normal_parts.push(part);
            }
            Component::CurDir | Component::ParentDir => {
                return Err(CliError::security_violation(
                    "project-root path must not contain relative or traversal components",
                ));
            }
            _ => {
                return Err(CliError::invalid_input(
                    "project-root path must be a normalized absolute directory path",
                ));
            }
        }
    }

    if !saw_root || normal_parts.is_empty() {
        return Err(CliError::invalid_input(
            "project-root path must be a normalized absolute directory path",
        ));
    }

    let mut reconstructed = PathBuf::from("/");
    for part in &normal_parts {
        reconstructed.push(part);
    }
    if reconstructed.as_os_str() != path.as_os_str() {
        return Err(CliError::invalid_input(
            "project-root path must be normalized without redundant separators or `.` segments",
        ));
    }

    let mut current = PathBuf::from("/");
    for part in &normal_parts {
        current.push(part);
        match fs::symlink_metadata(&current) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(CliError::security_violation(
                        "project-root path must not contain symlinks",
                    ));
                }
                if !meta.file_type().is_dir() {
                    return Err(CliError::security_violation(
                        "project-root path component is not a directory",
                    ));
                }
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Err(CliError::invalid_input(
                    "project-root directory does not exist",
                ));
            }
            Err(_) => {
                return Err(CliError::new(
                    "io",
                    "failed to inspect project-root directory metadata",
                ));
            }
        }
    }

    let canonical = fs::canonicalize(&reconstructed)
        .map_err(|_| CliError::security_violation("failed to canonicalize project-root path"))?;
    if canonical != reconstructed {
        return Err(CliError::security_violation(
            "project-root path is not canonical",
        ));
    }

    for store_dir in [
        &dirs.state_dir,
        &dirs.registry_dir,
        &dirs.plans_dir,
        &dirs.authority_dir,
    ] {
        if &canonical == store_dir
            || canonical.starts_with(store_dir)
            || store_dir.starts_with(&canonical)
        {
            return Err(CliError::security_violation(
                "project-root must not match or overlap any private store directory",
            ));
        }
    }

    Ok(canonical)
}

#[cfg(not(unix))]
fn validate_existing_canonical_project_root(
    _path: &Path,
    _dirs: &ValidatedStoreDirs,
) -> Result<PathBuf, CliError> {
    Err(CliError::new(
        "unsupported_platform",
        "threadlane-approve is only supported on Unix platforms",
    ))
}

fn enforce_fixed_scope_on_request(
    fixed: &FixedApprovalScope,
    request_binding_id: &str,
    request_thread_uuid: &str,
    request_repository_path: &Path,
    confirm_thread_uuid: Option<&str>,
) -> Result<(), CliError> {
    if request_binding_id != fixed.binding_id {
        return Err(CliError::binding_mismatch(
            "request binding_id does not match fixed context --binding scope",
        ));
    }
    if request_thread_uuid != fixed.thread_uuid {
        return Err(CliError::thread_uuid_mismatch(
            "request thread_uuid does not match fixed context --thread scope",
        ));
    }
    if let Some(confirm_thread) = confirm_thread_uuid {
        if confirm_thread != fixed.thread_uuid {
            return Err(CliError::thread_uuid_mismatch(
                "confirmation --confirm-thread does not match fixed context --thread scope",
            ));
        }
    }
    if let Some(ref project_root) = fixed.project_root {
        if request_repository_path != project_root.as_path() {
            return Err(CliError::new(
                "invalid_repository",
                "request repository_path does not match fixed context --project-root scope",
            ));
        }
    }
    Ok(())
}

struct ValidatedStoreDirs {
    state_dir: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
}

fn validate_four_store_dirs(
    state_dir: &Path,
    registry_dir: &Path,
    plans_dir: &Path,
    authority_dir: &Path,
) -> Result<ValidatedStoreDirs, CliError> {
    let clean_state = validate_normalized_store_dir(state_dir, false)?;
    let clean_registry = validate_normalized_store_dir(registry_dir, true)?;
    let clean_plans = validate_normalized_store_dir(plans_dir, true)?;
    let clean_authority = validate_normalized_store_dir(authority_dir, false)?;

    let all = [
        &clean_state,
        &clean_registry,
        &clean_plans,
        &clean_authority,
    ];
    for i in 0..all.len() {
        for j in (i + 1)..all.len() {
            if all[i] == all[j] || all[i].starts_with(all[j]) || all[j].starts_with(all[i]) {
                return Err(CliError::security_violation(
                    "state, registry, plans, and authority directories must be pairwise distinct and non-overlapping",
                ));
            }
        }
    }

    Ok(ValidatedStoreDirs {
        state_dir: clean_state,
        registry_dir: clean_registry,
        plans_dir: clean_plans,
        authority_dir: clean_authority,
    })
}

#[cfg(unix)]
fn validate_normalized_store_dir(path: &Path, must_exist: bool) -> Result<PathBuf, CliError> {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;

    let raw_bytes = path.as_os_str().as_bytes();
    if raw_bytes.is_empty() || !path.is_absolute() || raw_bytes.ends_with(b"/") {
        return Err(CliError::invalid_input(
            "store directory path must be a normalized absolute directory path",
        ));
    }

    let _ = validate_absolute_no_symlink_components(path, "store directory")
        .map_err(|e| CliError::from_native(&e))?;

    let mut normal_parts: Vec<&OsStr> = Vec::new();
    let mut saw_root = false;
    for component in path.components() {
        match component {
            Component::RootDir if !saw_root && normal_parts.is_empty() => {
                saw_root = true;
            }
            Component::Normal(part) if saw_root && !part.is_empty() => {
                normal_parts.push(part);
            }
            Component::CurDir | Component::ParentDir => {
                return Err(CliError::security_violation(
                    "store directory path must not contain relative or traversal components",
                ));
            }
            _ => {
                return Err(CliError::invalid_input(
                    "store directory path must be a normalized absolute directory path",
                ));
            }
        }
    }

    if !saw_root || normal_parts.is_empty() {
        return Err(CliError::invalid_input(
            "store directory path must be a normalized absolute directory path",
        ));
    }

    let mut reconstructed = PathBuf::from("/");
    for part in &normal_parts {
        reconstructed.push(part);
    }
    if reconstructed.as_os_str() != path.as_os_str() {
        return Err(CliError::invalid_input(
            "store directory path must be normalized without redundant separators or `.` segments",
        ));
    }

    let mut current = PathBuf::from("/");
    let root_meta = fs::symlink_metadata(&current)
        .map_err(|_| CliError::invalid_input("failed to inspect root directory metadata"))?;
    if root_meta.file_type().is_symlink() || !root_meta.file_type().is_dir() {
        return Err(CliError::security_violation(
            "store directory ancestor must be a non-symlink directory",
        ));
    }

    let mut deepest_existing = current.clone();
    let mut leaf_exists = true;

    for (idx, part) in normal_parts.iter().enumerate() {
        current.push(part);
        let is_leaf = idx + 1 == normal_parts.len();
        match fs::symlink_metadata(&current) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(CliError::security_violation(
                        "store directory path must not contain symlinks",
                    ));
                }
                if !meta.file_type().is_dir() {
                    return Err(CliError::security_violation(
                        "store directory path component is not a directory",
                    ));
                }
                deepest_existing.clone_from(&current);
                if is_leaf && (meta.mode() & 0o077) != 0 {
                    return Err(CliError::security_violation(
                        "store directory must be private with 0700 permissions",
                    ));
                }
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                if must_exist {
                    return Err(CliError::invalid_input(
                        "required store directory does not exist",
                    ));
                }
                leaf_exists = false;
                break;
            }
            Err(_) => {
                return Err(CliError::new(
                    "io",
                    "failed to inspect store directory metadata",
                ));
            }
        }
    }

    let canonical_existing = fs::canonicalize(&deepest_existing)
        .map_err(|_| CliError::security_violation("failed to canonicalize store directory path"))?;
    if canonical_existing != deepest_existing {
        return Err(CliError::security_violation(
            "store directory path is not canonical",
        ));
    }

    if leaf_exists && canonical_existing != reconstructed {
        return Err(CliError::security_violation(
            "store directory path is not canonical",
        ));
    }

    Ok(reconstructed)
}

#[cfg(not(unix))]
fn validate_normalized_store_dir(_path: &Path, _must_exist: bool) -> Result<PathBuf, CliError> {
    Err(CliError::new(
        "unsupported_platform",
        "threadlane-approve is only supported on Unix platforms",
    ))
}

fn validate_repo_non_overlap_with_state_dir(
    repository_path: &Path,
    state_dir: &Path,
) -> Result<(), CliError> {
    if repository_path == state_dir
        || repository_path.starts_with(state_dir)
        || state_dir.starts_with(repository_path)
    {
        return Err(CliError::new(
            "invalid_repository",
            "repository_path must not overlap the runtime state directory",
        ));
    }
    Ok(())
}

fn open_validated_authority_store(dirs: &ValidatedStoreDirs) -> Result<AuthorityStore, CliError> {
    let Some((registry, plans)) =
        open_workflow_stores(Some(&dirs.registry_dir), Some(&dirs.plans_dir))
            .map_err(|e| CliError::from_native(&e))?
    else {
        return Err(CliError::invalid_input(
            "workflow registry and plan stores could not be opened",
        ));
    };

    let authority = AuthorityStore::open(
        &dirs.authority_dir,
        Arc::clone(&registry),
        Arc::clone(&plans),
    )
    .map_err(|e| CliError::from_authority(&e))?;

    for dir in [
        dirs.state_dir.as_path(),
        registry.config_dir(),
        plans.dir(),
        authority.dir(),
    ] {
        let canonical = fs::canonicalize(dir).map_err(|_| {
            CliError::security_violation("failed to canonicalize opened store directory")
        })?;
        if canonical != dir {
            return Err(CliError::security_violation(
                "opened store directory path is not canonical",
            ));
        }
    }

    Ok(authority)
}

async fn verify_live_binding(
    supervisor: &SupervisorClient,
    expected_binding_id: &str,
    expected_thread_uuid: &str,
) -> Result<(), CliError> {
    let live_binding = supervisor
        .get_binding(expected_binding_id)
        .await
        .map_err(|e| CliError::from_supervisor(&e))?;

    if live_binding.binding_id != expected_binding_id {
        return Err(CliError::binding_mismatch(
            "live runtime binding_id did not match requested binding_id",
        ));
    }

    let live_thread_uuid = validate_canonical_thread_uuid("thread_uuid", &live_binding.thread_uuid)
        .map_err(|e| CliError::from_authority(&e))?;
    if live_thread_uuid != expected_thread_uuid {
        return Err(CliError::thread_uuid_mismatch(
            "live runtime binding thread_uuid did not match requested thread_uuid",
        ));
    }

    Ok(())
}

#[cfg(unix)]
fn load_request_json<T: DeserializeOwned>(path: &Path) -> Result<T, CliError> {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    let raw_bytes = path.as_os_str().as_bytes();
    if raw_bytes.is_empty() || !path.is_absolute() || raw_bytes.ends_with(b"/") {
        return Err(CliError::invalid_input(
            "request path must be a normalized absolute file path",
        ));
    }

    let _ = validate_absolute_no_symlink_components(path, "request file")
        .map_err(|e| CliError::from_native(&e))?;

    let mut normal_parts: Vec<&OsStr> = Vec::new();
    let mut saw_root = false;
    for component in path.components() {
        match component {
            Component::RootDir if !saw_root && normal_parts.is_empty() => {
                saw_root = true;
            }
            Component::Normal(part) if saw_root && !part.is_empty() => {
                normal_parts.push(part);
            }
            Component::CurDir | Component::ParentDir => {
                return Err(CliError::security_violation(
                    "request path must not contain relative or traversal components",
                ));
            }
            _ => {
                return Err(CliError::invalid_input(
                    "request path must be a normalized absolute file path",
                ));
            }
        }
    }

    if !saw_root || normal_parts.is_empty() {
        return Err(CliError::invalid_input(
            "request path must be a normalized absolute file path",
        ));
    }

    let mut reconstructed = PathBuf::from("/");
    for part in &normal_parts {
        reconstructed.push(part);
    }
    if reconstructed.as_os_str() != path.as_os_str() {
        return Err(CliError::invalid_input(
            "request path must be normalized without redundant separators or `.` segments",
        ));
    }

    let mut current = PathBuf::from("/");
    let root_meta = fs::symlink_metadata(&current)
        .map_err(|_| CliError::invalid_input("invalid root directory metadata"))?;
    if root_meta.file_type().is_symlink() || !root_meta.file_type().is_dir() {
        return Err(CliError::security_violation(
            "request path ancestor must be a non-symlink directory",
        ));
    }

    let mut leaf_dev = 0u64;
    let mut leaf_ino = 0u64;

    for (idx, part) in normal_parts.iter().enumerate() {
        current.push(part);
        let meta = fs::symlink_metadata(&current)
            .map_err(|_| CliError::invalid_input("request file path does not exist"))?;
        if meta.file_type().is_symlink() {
            return Err(CliError::security_violation(
                "request path must not contain symlinks",
            ));
        }

        let is_leaf = idx + 1 == normal_parts.len();
        if !is_leaf {
            if !meta.file_type().is_dir() {
                return Err(CliError::invalid_input(
                    "request path ancestor must be a directory",
                ));
            }
        } else {
            if !meta.file_type().is_file() {
                return Err(CliError::invalid_input(
                    "request path must refer to a regular file",
                ));
            }
            if (meta.mode() & 0o077) != 0 {
                return Err(CliError::security_violation(
                    "request file must be private with no group or other permissions",
                ));
            }
            if meta.len() > MAX_IO_BYTES as u64 {
                return Err(CliError::invalid_input(
                    "request file exceeds maximum size of 1 MiB",
                ));
            }
            leaf_dev = meta.dev();
            leaf_ino = meta.ino();
        }
    }

    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| CliError::invalid_input("request file could not be opened"))?;

    let fstat_meta = file
        .metadata()
        .map_err(|_| CliError::invalid_input("invalid request file metadata"))?;
    if !fstat_meta.file_type().is_file() {
        return Err(CliError::invalid_input(
            "request path must refer to a regular file",
        ));
    }
    if (fstat_meta.mode() & 0o077) != 0 {
        return Err(CliError::security_violation(
            "request file must be private with no group or other permissions",
        ));
    }
    if fstat_meta.dev() != leaf_dev || fstat_meta.ino() != leaf_ino {
        return Err(CliError::security_violation(
            "request file identity changed during open",
        ));
    }
    if fstat_meta.len() > MAX_IO_BYTES as u64 {
        return Err(CliError::invalid_input(
            "request file exceeds maximum size of 1 MiB",
        ));
    }

    let initial_len = fstat_meta.len() as usize;
    let mut buf = Vec::with_capacity(initial_len.min(MAX_IO_BYTES) + 1);
    (&mut file)
        .take((MAX_IO_BYTES as u64) + 1)
        .read_to_end(&mut buf)
        .map_err(|_| CliError::invalid_input("request file could not be read"))?;

    if buf.len() > MAX_IO_BYTES {
        return Err(CliError::invalid_input(
            "request file exceeds maximum size of 1 MiB",
        ));
    }
    if buf.len() != initial_len {
        return Err(CliError::security_violation(
            "request file size changed during read",
        ));
    }

    let post_meta = file
        .metadata()
        .map_err(|_| CliError::invalid_input("invalid request file metadata"))?;
    if !post_meta.file_type().is_file()
        || (post_meta.mode() & 0o077) != 0
        || post_meta.len() != (buf.len() as u64)
    {
        return Err(CliError::security_violation(
            "request file state changed during read",
        ));
    }

    serde_json::from_slice(&buf)
        .map_err(|_| CliError::invalid_input("request file contains invalid JSON"))
}

#[cfg(not(unix))]
fn load_request_json<T: DeserializeOwned>(_path: &Path) -> Result<T, CliError> {
    Err(CliError::new(
        "unsupported_platform",
        "threadlane-approve is only supported on Unix platforms",
    ))
}

struct BoundedVecWriter {
    buf: Vec<u8>,
    max_bytes: usize,
}

impl BoundedVecWriter {
    fn new(max_bytes: usize) -> Self {
        Self {
            buf: Vec::new(),
            max_bytes,
        }
    }
}

impl Write for BoundedVecWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if data.len() > self.max_bytes.saturating_sub(self.buf.len()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "output exceeded byte limit",
            ));
        }
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn write_stdout_json<T: Serialize>(value: &T, is_mutation: bool) -> Result<(), CliError> {
    let mut writer = BoundedVecWriter::new(MAX_IO_BYTES);
    if serde_json::to_writer(&mut writer, value).is_err() || writer.write_all(b"\n").is_err() {
        return Err(output_error(is_mutation, "json"));
    }

    let mut stdout = io::stdout().lock();
    if stdout.write_all(&writer.buf).is_err() || stdout.flush().is_err() {
        return Err(output_error(is_mutation, "io"));
    }
    Ok(())
}

const fn output_error(is_mutation: bool, read_only_code: &'static str) -> CliError {
    if is_mutation {
        CliError::outcome_unknown(
            "approval record persisted, but writing bounded stdout result failed",
        )
    } else {
        CliError::new(read_only_code, "failed to write bounded command output")
    }
}

fn emit_error(err: &CliError) {
    let mut stderr = io::stderr().lock();
    if let Ok(mut encoded) = serde_json::to_vec(err) {
        encoded.push(b'\n');
        let _ = stderr.write_all(&encoded);
    } else {
        let _ = writeln!(
            stderr,
            "{{\"code\":\"{}\",\"message\":\"operational failure\"}}",
            err.code
        );
    }
    let _ = stderr.flush();
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn explicit_intent_matches_authority_wire_shape_and_values() {
        let value = serde_json::json!({
            "binding_id": "bnd_1", "thread_uuid": "550e8400-e29b-41d4-a716-446655440000",
            "plan_id": "plan_1", "plan_revision": 2, "expected_registry_revision": 3,
            "repository_path": "/tmp/repo", "base_commit": "0123456789abcdef0123456789abcdef01234567",
            "approved_task_ids": ["task-a", "task-b"], "approved_write_paths": ["a", "b"],
            "max_parallel": 2,
            "budgets": {"timeout_ms": 90000, "max_line_bytes": 16384,
                "max_stdout_bytes": 2097152, "max_stderr_bytes": 100000, "max_events": 321,
                "progress_watchdog": {"soft_timeout_ms": 60000, "extension_ms": 10000,
                    "recent_advancement_ms": 20000}}
        });
        let exact: ExplicitApprovalIntent = serde_json::from_value(value.clone()).unwrap();
        let original: OperatorApprovalIntent = serde_json::from_value(value.clone()).unwrap();
        let converted: OperatorApprovalIntent = exact.into();
        assert_eq!(converted, original);
        assert_eq!(serde_json::to_value(converted).unwrap(), value);
    }

    #[test]
    fn cli_parses_legacy_subcommand_flags_and_context_routed_global_flags() {
        let legacy = Cli::try_parse_from([
            "threadlane-approve",
            "prepare",
            "--state-dir",
            "/abs/state",
            "--registry-dir",
            "/abs/registry",
            "--plans-dir",
            "/abs/plans",
            "--authority-dir",
            "/abs/authority",
            "--request",
            "/abs/req.json",
        ])
        .expect("parse legacy prepare flags");
        assert_eq!(
            resolve_store_dir_args(&legacy).expect("resolve store dirs"),
            (
                PathBuf::from("/abs/state"),
                PathBuf::from("/abs/registry"),
                PathBuf::from("/abs/plans"),
                PathBuf::from("/abs/authority"),
            )
        );
        assert!(legacy.binding.is_none());
        assert!(legacy.thread.is_none());
        assert!(legacy.project_root.is_none());

        let routed = Cli::try_parse_from([
            "threadlane-approve",
            "--state-dir",
            "/abs/state",
            "--registry-dir",
            "/abs/registry",
            "--plans-dir",
            "/abs/plans",
            "--authority-dir",
            "/abs/authority",
            "--binding",
            "bnd_test_1",
            "--thread",
            "550e8400-e29b-41d4-a716-446655440000",
            "--project-root",
            "/abs/repo",
            "record",
            "--request",
            "/abs/req.json",
            "--confirm-subject",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--confirm-thread",
            "550e8400-e29b-41d4-a716-446655440000",
            "--operator-approved",
        ])
        .expect("parse context-routed record flags");
        assert_eq!(routed.binding.as_deref(), Some("bnd_test_1"));
        assert_eq!(
            routed.thread.as_deref(),
            Some("550e8400-e29b-41d4-a716-446655440000")
        );
        assert_eq!(routed.project_root.as_deref(), Some(Path::new("/abs/repo")));

        let preview_routed = Cli::try_parse_from([
            "threadlane-approve",
            "--state-dir",
            "/abs/state",
            "--registry-dir",
            "/abs/registry",
            "--plans-dir",
            "/abs/plans",
            "--authority-dir",
            "/abs/authority",
            "record",
            "--preview",
            "/abs/preview.json",
            "--confirm-subject",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--confirm-thread",
            "550e8400-e29b-41d4-a716-446655440000",
            "--operator-approved",
        ])
        .expect("parse record with a typed preview input");
        match preview_routed.command {
            ApproveCommand::Record {
                request: None,
                preview: Some(path),
                ..
            } => assert_eq!(path, PathBuf::from("/abs/preview.json")),
            other => panic!("unexpected record input: {other:?}"),
        }

        let ambiguous = Cli::try_parse_from([
            "threadlane-approve",
            "record",
            "--request",
            "/abs/req.json",
            "--preview",
            "/abs/preview.json",
            "--confirm-subject",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--confirm-thread",
            "550e8400-e29b-41d4-a716-446655440000",
            "--operator-approved",
        ]);
        assert!(ambiguous.is_err(), "both record inputs must be rejected");

        let missing_input = Cli::try_parse_from([
            "threadlane-approve",
            "record",
            "--confirm-subject",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--confirm-thread",
            "550e8400-e29b-41d4-a716-446655440000",
            "--operator-approved",
        ]);
        assert!(
            missing_input.is_err(),
            "record without exactly one private input must be rejected"
        );
    }

    #[test]
    fn prepare_private_parses_without_request_and_rejects_argv_inputs() {
        let cli = Cli::try_parse_from([
            "threadlane-approve",
            "--state-dir",
            "/abs/state",
            "--registry-dir",
            "/abs/registry",
            "--plans-dir",
            "/abs/plans",
            "--authority-dir",
            "/abs/authority",
            "prepare-private",
        ])
        .expect("parse prepare-private");
        assert!(matches!(cli.command, ApproveCommand::PreparePrivate));
        assert!(cli.binding.is_none());
        assert!(cli.thread.is_none());
        assert!(cli.project_root.is_none());

        let argv_json = Cli::try_parse_from([
            "threadlane-approve",
            "prepare-private",
            "--request",
            "/abs/req.json",
        ]);
        assert!(
            argv_json.is_err(),
            "prepare-private must reject argv JSON paths"
        );

        let positional_json = Cli::try_parse_from([
            "threadlane-approve",
            "prepare-private",
            "{\"binding_id\":\"bnd_test_1\"}",
        ]);
        assert!(
            positional_json.is_err(),
            "prepare-private must reject positional JSON on argv"
        );

        let legacy_without_request = Cli::try_parse_from([
            "threadlane-approve",
            "--state-dir",
            "/abs/state",
            "--registry-dir",
            "/abs/registry",
            "--plans-dir",
            "/abs/plans",
            "--authority-dir",
            "/abs/authority",
            "prepare",
        ]);
        assert!(
            legacy_without_request.is_err(),
            "prepare must keep requiring --request"
        );
    }

    #[test]
    fn prepare_task_parses_as_stdin_only_subcommand_and_rejects_argv_json() {
        let cli = Cli::try_parse_from([
            "threadlane-approve",
            "--state-dir",
            "/abs/state",
            "--registry-dir",
            "/abs/registry",
            "--plans-dir",
            "/abs/plans",
            "--authority-dir",
            "/abs/authority",
            "--binding",
            "bnd_test_1",
            "--thread",
            "550e8400-e29b-41d4-a716-446655440000",
            "--project-root",
            "/abs/repo",
            "prepare-task",
        ])
        .expect("parse prepare-task with fixed context scope");
        assert!(matches!(cli.command, ApproveCommand::PrepareTask));

        let argv_json = Cli::try_parse_from([
            "threadlane-approve",
            "prepare-task",
            "--request",
            "/abs/req.json",
        ]);
        assert!(
            argv_json.is_err(),
            "prepare-task must reject argv request paths"
        );

        let positional_json = Cli::try_parse_from(["threadlane-approve", "prepare-task", "{}"]);
        assert!(
            positional_json.is_err(),
            "prepare-task must reject positional JSON on argv"
        );
    }

    #[test]
    fn fixed_scope_enforces_binding_thread_confirm_thread_and_project_root() {
        let fixed = FixedApprovalScope {
            binding_id: "bnd_chat_a".to_string(),
            thread_uuid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            project_root: Some(PathBuf::from("/abs/repo_a")),
        };

        enforce_fixed_scope_on_request(
            &fixed,
            "bnd_chat_a",
            "550e8400-e29b-41d4-a716-446655440000",
            Path::new("/abs/repo_a"),
            Some("550e8400-e29b-41d4-a716-446655440000"),
        )
        .expect("matching request scope succeeds");

        let wrong_binding = enforce_fixed_scope_on_request(
            &fixed,
            "bnd_chat_b",
            "550e8400-e29b-41d4-a716-446655440000",
            Path::new("/abs/repo_a"),
            None,
        )
        .expect_err("wrong binding must fail");
        assert_eq!(wrong_binding.code, "binding_mismatch");

        let wrong_thread = enforce_fixed_scope_on_request(
            &fixed,
            "bnd_chat_a",
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
            Path::new("/abs/repo_a"),
            None,
        )
        .expect_err("wrong thread must fail");
        assert_eq!(wrong_thread.code, "thread_uuid_mismatch");

        let wrong_confirm = enforce_fixed_scope_on_request(
            &fixed,
            "bnd_chat_a",
            "550e8400-e29b-41d4-a716-446655440000",
            Path::new("/abs/repo_a"),
            Some("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
        )
        .expect_err("wrong confirm thread must fail");
        assert_eq!(wrong_confirm.code, "thread_uuid_mismatch");

        let wrong_repo = enforce_fixed_scope_on_request(
            &fixed,
            "bnd_chat_a",
            "550e8400-e29b-41d4-a716-446655440000",
            Path::new("/abs/repo_b"),
            None,
        )
        .expect_err("wrong repository_path must fail");
        assert_eq!(wrong_repo.code, "invalid_repository");
    }
}
