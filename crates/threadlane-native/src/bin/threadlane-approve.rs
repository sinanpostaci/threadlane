#![deny(unsafe_code)]
#![deny(missing_docs)]

//! Explicit local operator approval CLI (`threadlane-approve`) for native Codex workflows.
//!
//! Prepares a read-only digest-bound [`threadlane_authority::OperatorApprovalPreview`] (`prepare`) and records an
//! operator-confirmed [`ApprovalSummary`] (`record`) via [`AuthorityStore::approve_confirmed`].
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
use serde::{de::DeserializeOwned, Serialize};
use threadlane::{ErrorCode as RuntimeErrorCode, SupervisorClient};
use threadlane_authority::validation::{
    validate_canonical_thread_uuid, validate_identifier, validate_sha256_hex,
};
use threadlane_authority::{
    ApprovalSummary, AuthorityError, AuthorityStore, OperatorApprovalIntent,
    OperatorApprovalRequest,
};
use threadlane_native::fs_safety::{
    ensure_platform_supported, validate_absolute_no_symlink_components,
};
use threadlane_native::{open_workflow_stores, NativeError, NativeErrorCode};

/// Maximum byte size (`1 MiB`) permitted for private JSON request files and bounded stdout JSON
/// responses (including the trailing newline).
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
    #[command(subcommand)]
    command: ApproveCommand,
}

#[derive(Debug, Subcommand)]
#[command(rename_all = "kebab-case")]
enum ApproveCommand {
    /// Prepare and validate a read-only `OperatorApprovalPreview` from a private `OperatorApprovalIntent` JSON file.
    Prepare {
        /// Normalized absolute path to the private running supervisor state directory (`supervisor.json`).
        #[arg(long = "state-dir", value_name = "ABS")]
        state_dir: PathBuf,
        /// Normalized absolute path to the private global workflow registry store directory (`registry.json`).
        #[arg(long = "registry-dir", value_name = "ABS")]
        registry_dir: PathBuf,
        /// Normalized absolute path to the private workflow plan store directory (`plans.json`).
        #[arg(long = "plans-dir", value_name = "ABS")]
        plans_dir: PathBuf,
        /// Normalized absolute path to the private operator approval authority store directory (`approvals.json`).
        #[arg(long = "authority-dir", value_name = "ABS")]
        authority_dir: PathBuf,
        /// Normalized absolute path to the private JSON request file (`OperatorApprovalIntent`).
        #[arg(long = "request", value_name = "ABS_PRIVATE_JSON")]
        request: PathBuf,
    },
    /// Validate and persist a confirmed `OperatorApprovalRequest` into `approvals.json`.
    Record {
        /// Normalized absolute path to the private running supervisor state directory (`supervisor.json`).
        #[arg(long = "state-dir", value_name = "ABS")]
        state_dir: PathBuf,
        /// Normalized absolute path to the private global workflow registry store directory (`registry.json`).
        #[arg(long = "registry-dir", value_name = "ABS")]
        registry_dir: PathBuf,
        /// Normalized absolute path to the private workflow plan store directory (`plans.json`).
        #[arg(long = "plans-dir", value_name = "ABS")]
        plans_dir: PathBuf,
        /// Normalized absolute path to the private operator approval authority store directory (`approvals.json`).
        #[arg(long = "authority-dir", value_name = "ABS")]
        authority_dir: PathBuf,
        /// Normalized absolute path to the private JSON request file (`OperatorApprovalRequest`).
        #[arg(long = "request", value_name = "ABS_PRIVATE_JSON")]
        request: PathBuf,
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

#[derive(Debug, Clone, Serialize)]
struct CliError {
    code: &'static str,
    message: &'static str,
}

impl CliError {
    const fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
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

    match cli.command {
        ApproveCommand::Prepare {
            state_dir,
            registry_dir,
            plans_dir,
            authority_dir,
            request,
        } => {
            let dirs =
                validate_four_store_dirs(&state_dir, &registry_dir, &plans_dir, &authority_dir)?;
            let intent: OperatorApprovalIntent = load_request_json(&request)?;

            let binding_id = validate_identifier("binding_id", &intent.binding_id)
                .map_err(|e| CliError::from_authority(&e))?;
            let thread_uuid = validate_canonical_thread_uuid("thread_uuid", &intent.thread_uuid)
                .map_err(|e| CliError::from_authority(&e))?;
            validate_repo_non_overlap_with_state_dir(&intent.repository_path, &dirs.state_dir)?;

            let supervisor = SupervisorClient::from_state_dir(&dirs.state_dir)
                .map_err(|e| CliError::from_supervisor(&e))?;
            verify_live_binding(&supervisor, &binding_id, &thread_uuid).await?;

            let authority = open_validated_authority_store(&dirs)?;
            let preview = authority
                .prepare_for_operator(intent)
                .map_err(|e| CliError::from_authority(&e))?;

            verify_live_binding(
                &supervisor,
                &preview.request.binding_id,
                &preview.request.thread_uuid,
            )
            .await?;

            write_stdout_json(&preview, false)
        }
        ApproveCommand::Record {
            state_dir,
            registry_dir,
            plans_dir,
            authority_dir,
            request,
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
            let req: OperatorApprovalRequest = load_request_json(&request)?;

            let binding_id = validate_identifier("binding_id", &req.binding_id)
                .map_err(|e| CliError::from_authority(&e))?;
            let thread_uuid = validate_canonical_thread_uuid("thread_uuid", &req.thread_uuid)
                .map_err(|e| CliError::from_authority(&e))?;
            if checked_confirm_thread != thread_uuid {
                return Err(CliError::thread_uuid_mismatch(
                    "confirmation --confirm-thread does not match request thread_uuid",
                ));
            }
            validate_repo_non_overlap_with_state_dir(&req.repository_path, &dirs.state_dir)?;

            let supervisor = SupervisorClient::from_state_dir(&dirs.state_dir)
                .map_err(|e| CliError::from_supervisor(&e))?;
            verify_live_binding(&supervisor, &binding_id, &thread_uuid).await?;

            let authority = open_validated_authority_store(&dirs)?;
            let preview = authority
                .preview_for_operator(req.clone())
                .map_err(|e| CliError::from_authority(&e))?;
            if preview.subject_digest != checked_subject {
                return Err(CliError::new(
                    "validation",
                    "confirmed subject digest is invalid or did not match the compiled approval subject",
                ));
            }

            verify_live_binding(&supervisor, &binding_id, &thread_uuid).await?;

            let summary: ApprovalSummary = authority
                .approve_confirmed(req, &checked_subject)
                .map_err(|e| CliError::from_authority(&e))?;

            write_stdout_json(&summary, true)
        }
    }
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
