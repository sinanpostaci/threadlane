#![deny(unsafe_code)]
#![deny(missing_docs)]

//! Explicit local operator workflow CLI (`threadlane-operator`) for Milestone 5A.
//!
//! Exposes task commit, read-only review subject inspection, digest-confirmed review
//! acceptance, read-only integration proposal, and digest/target-confirmed integration
//! execution to a trusted local operator without inventing receipts or weakening
//! `threadlane-operations` invariants.

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use serde::{de::DeserializeOwned, Serialize};
use threadlane_operations::{
    accept_review_subject, commit_task_worktree_changes, execute_integration,
    inspect_review_subject, propose_integration, CommitTaskChangesRequest,
    ExecuteIntegrationRequest, InspectReviewSubjectRequest, IntegrationProposalRequest,
    OperationsError, OperationsErrorCode, ReviewAcceptanceRequest,
};
use threadlane_workers::{sanitize_and_redact, CleanupRefusalReason};

/// Maximum byte size (1 MiB) permitted for request files and serialized stdout responses.
const MAX_IO_BYTES: usize = 1_048_576;

/// Maximum byte length for sanitized operational error messages on stderr.
const MAX_ERROR_MESSAGE_BYTES: usize = 512;

/// Error code emitted when a mutating operation completed (or reached an uncertain output
/// boundary) before its bounded result could be serialized and flushed to stdout.
const OUTCOME_UNKNOWN_CODE: &str = "outcome_unknown";

#[derive(Debug, Parser)]
#[command(
    name = "threadlane-operator",
    version,
    about = "Trusted local operator CLI for Threadlane task review and integration workflows"
)]
struct Cli {
    #[command(subcommand)]
    command: OperatorCommand,
}

#[derive(Debug, Subcommand)]
#[command(rename_all = "kebab-case")]
enum OperatorCommand {
    /// Stage and commit validated in-scope changes inside an owned task worktree.
    CommitTask {
        /// Explicit absolute path to a private JSON request file (`CommitTaskChangesRequest`).
        #[arg(long = "request", value_name = "ABS_JSON")]
        request: PathBuf,
        /// Explicit confirmation matching `request.task_worktree_id`.
        #[arg(long = "confirm-task", value_name = "WORKTREE_ID")]
        confirm_task: String,
    },
    /// Read-only inspection of an owned task worktree to produce a digest-bound `ReviewSubject`.
    InspectReview {
        /// Explicit absolute path to a private JSON request file (`InspectReviewSubjectRequest`).
        #[arg(long = "request", value_name = "ABS_JSON")]
        request: PathBuf,
    },
    /// Accept a validated `ReviewSubject` and persist a frozen `ReviewReceipt`.
    AcceptReview {
        /// Explicit absolute path to a private JSON request file (`ReviewAcceptanceRequest`).
        #[arg(long = "request", value_name = "ABS_JSON")]
        request: PathBuf,
        /// Explicit confirmation matching `request.expected_subject_digest` and `request.subject.subject_digest`.
        #[arg(long = "confirm-subject", value_name = "SHA256")]
        confirm_subject: String,
    },
    /// Compute a read-only `IntegrationProposal` without mutating any worktree or ref.
    ProposeIntegration {
        /// Explicit absolute path to a private JSON request file (`IntegrationProposalRequest`).
        #[arg(long = "request", value_name = "ABS_JSON")]
        request: PathBuf,
    },
    /// Integrate an approved `ReviewSubject` into an owned integration worktree.
    Integrate {
        /// Explicit absolute path to a private JSON request file (`ExecuteIntegrationRequest`).
        #[arg(long = "request", value_name = "ABS_JSON")]
        request: PathBuf,
        /// Explicit confirmation matching `request.subject.subject_digest`.
        #[arg(long = "confirm-subject", value_name = "SHA256")]
        confirm_subject: String,
        /// Explicit confirmation matching `request.expected_integration_head_commit`.
        #[arg(long = "confirm-target", value_name = "SHA40")]
        confirm_target: String,
    },
}

#[derive(Debug, Clone, Serialize)]
struct CliError {
    code: &'static str,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cleanup_refusal_reason: Option<CleanupRefusalReason>,
}

impl CliError {
    fn invalid_input(message: &'static str) -> Self {
        Self {
            code: OperationsErrorCode::InvalidInput.as_str(),
            message: message.to_string(),
            cleanup_refusal_reason: None,
        }
    }

    fn subject_mismatch(message: &'static str) -> Self {
        Self {
            code: OperationsErrorCode::SubjectMismatch.as_str(),
            message: message.to_string(),
            cleanup_refusal_reason: None,
        }
    }

    fn outcome_unknown(message: &'static str) -> Self {
        Self {
            code: OUTCOME_UNKNOWN_CODE,
            message: message.to_string(),
            cleanup_refusal_reason: None,
        }
    }

    fn from_op(err: OperationsError) -> Self {
        let api_err = err.to_api_error();
        let sanitized = sanitize_and_redact(&api_err.message, MAX_ERROR_MESSAGE_BYTES);
        let message = if sanitized.trim().is_empty() {
            "operation failed".to_string()
        } else {
            sanitized
        };
        Self {
            code: api_err.code.as_str(),
            message,
            cleanup_refusal_reason: api_err.cleanup_refusal_reason,
        }
    }
}

fn invalid_input(message: &'static str) -> CliError {
    CliError::invalid_input(message)
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            emit_error(&err);
            ExitCode::from(1)
        }
    }
}

fn run(cli: Cli) -> Result<(), CliError> {
    ensure_supported_platform()?;

    match cli.command {
        OperatorCommand::CommitTask {
            request,
            confirm_task,
        } => {
            let req: CommitTaskChangesRequest = load_request_json(&request)?;
            if confirm_task.is_empty() || confirm_task != req.task_worktree_id {
                return Err(CliError::subject_mismatch(
                    "confirmation does not match request task_worktree_id",
                ));
            }
            let report = commit_task_worktree_changes(&req).map_err(CliError::from_op)?;
            write_stdout_json(&report, true)
        }
        OperatorCommand::InspectReview { request } => {
            let req: InspectReviewSubjectRequest = load_request_json(&request)?;
            let subject = inspect_review_subject(&req).map_err(CliError::from_op)?;
            write_stdout_json(&subject, false)
        }
        OperatorCommand::AcceptReview {
            request,
            confirm_subject,
        } => {
            let req: ReviewAcceptanceRequest = load_request_json(&request)?;
            if confirm_subject.is_empty()
                || confirm_subject != req.expected_subject_digest
                || confirm_subject != req.subject.subject_digest
            {
                return Err(CliError::subject_mismatch(
                    "confirmation does not match request review subject digest",
                ));
            }
            let receipt = accept_review_subject(&req).map_err(CliError::from_op)?;
            write_stdout_json(&receipt, true)
        }
        OperatorCommand::ProposeIntegration { request } => {
            let req: IntegrationProposalRequest = load_request_json(&request)?;
            let proposal = propose_integration(&req).map_err(CliError::from_op)?;
            write_stdout_json(&proposal, false)
        }
        OperatorCommand::Integrate {
            request,
            confirm_subject,
            confirm_target,
        } => {
            let req: ExecuteIntegrationRequest = load_request_json(&request)?;
            if confirm_subject.is_empty()
                || confirm_subject != req.subject.subject_digest
                || confirm_target.is_empty()
                || confirm_target != req.expected_integration_head_commit
            {
                return Err(CliError::subject_mismatch(
                    "confirmation does not match request subject digest or target head",
                ));
            }
            let receipt = execute_integration(&req).map_err(CliError::from_op)?;
            write_stdout_json(&receipt, true)
        }
    }
}

#[cfg(unix)]
fn ensure_supported_platform() -> Result<(), CliError> {
    Ok(())
}

#[cfg(not(unix))]
fn ensure_supported_platform() -> Result<(), CliError> {
    Err(CliError {
        code: OperationsErrorCode::UnsupportedPlatform.as_str(),
        message: "threadlane-operator is only supported on Unix platforms".to_string(),
        cleanup_refusal_reason: None,
    })
}

#[cfg(unix)]
fn load_request_json<T: DeserializeOwned>(path: &Path) -> Result<T, CliError> {
    use std::ffi::OsStr;
    use std::fs::{self, OpenOptions};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    use std::path::Component;

    let raw_bytes = path.as_os_str().as_bytes();
    if raw_bytes.is_empty() || !path.is_absolute() || raw_bytes.ends_with(b"/") {
        return Err(invalid_input(
            "request path must be a normalized absolute file path",
        ));
    }

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
            _ => {
                return Err(invalid_input(
                    "request path must not contain relative or traversal components",
                ));
            }
        }
    }

    if !saw_root || normal_parts.is_empty() {
        return Err(invalid_input(
            "request path must be a normalized absolute file path",
        ));
    }

    let mut current = PathBuf::from("/");
    let root_meta = fs::symlink_metadata(&current).map_err(|_| invalid_input("invalid root"))?;
    if root_meta.file_type().is_symlink() || !root_meta.file_type().is_dir() {
        return Err(invalid_input(
            "request path ancestor must be a non-symlink directory",
        ));
    }

    let mut leaf_dev = 0u64;
    let mut leaf_ino = 0u64;

    for (idx, part) in normal_parts.iter().enumerate() {
        current.push(part);
        let meta = fs::symlink_metadata(&current).map_err(|_| invalid_input("invalid path"))?;
        if meta.file_type().is_symlink() {
            return Err(invalid_input("request path must not contain symlinks"));
        }

        let is_leaf = idx + 1 == normal_parts.len();
        if !is_leaf {
            if !meta.file_type().is_dir() {
                return Err(invalid_input("request path ancestor must be a directory"));
            }
        } else {
            if !meta.file_type().is_file() {
                return Err(invalid_input("request path must refer to a regular file"));
            }
            if (meta.mode() & 0o077) != 0 {
                return Err(invalid_input(
                    "request file must be private with no group or other permissions",
                ));
            }
            if meta.len() > MAX_IO_BYTES as u64 {
                return Err(invalid_input("request file exceeds maximum size of 1 MiB"));
            }
            leaf_dev = meta.dev();
            leaf_ino = meta.ino();
        }
    }

    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| invalid_input("request file could not be opened"))?;

    let fstat_meta = file
        .metadata()
        .map_err(|_| invalid_input("invalid request metadata"))?;
    if !fstat_meta.file_type().is_file() {
        return Err(invalid_input("request path must refer to a regular file"));
    }
    if (fstat_meta.mode() & 0o077) != 0 {
        return Err(invalid_input(
            "request file must be private with no group or other permissions",
        ));
    }
    if fstat_meta.dev() != leaf_dev || fstat_meta.ino() != leaf_ino {
        return Err(invalid_input("request file identity changed during open"));
    }
    if fstat_meta.len() > MAX_IO_BYTES as u64 {
        return Err(invalid_input("request file exceeds maximum size of 1 MiB"));
    }

    let initial_len = fstat_meta.len() as usize;
    let mut buf = Vec::with_capacity(initial_len.min(MAX_IO_BYTES) + 1);
    (&mut file)
        .take((MAX_IO_BYTES as u64) + 1)
        .read_to_end(&mut buf)
        .map_err(|_| invalid_input("request file could not be read"))?;

    if buf.len() > MAX_IO_BYTES {
        return Err(invalid_input("request file exceeds maximum size of 1 MiB"));
    }
    if buf.len() != initial_len {
        return Err(invalid_input("request file size changed during read"));
    }

    let post_meta = file
        .metadata()
        .map_err(|_| invalid_input("invalid request metadata"))?;
    if !post_meta.file_type().is_file()
        || (post_meta.mode() & 0o077) != 0
        || post_meta.len() != (buf.len() as u64)
    {
        return Err(invalid_input("request file state changed during read"));
    }

    serde_json::from_slice(&buf).map_err(|_| invalid_input("request file contains invalid JSON"))
}

#[cfg(not(unix))]
fn load_request_json<T: DeserializeOwned>(_path: &Path) -> Result<T, CliError> {
    ensure_supported_platform()?;
    unreachable!()
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
        return Err(output_error(is_mutation, OperationsErrorCode::Json));
    }

    let mut stdout = io::stdout().lock();
    if stdout.write_all(&writer.buf).is_err() || stdout.flush().is_err() {
        return Err(output_error(is_mutation, OperationsErrorCode::Io));
    }
    Ok(())
}

fn output_error(is_mutation: bool, read_only_code: OperationsErrorCode) -> CliError {
    if is_mutation {
        CliError::outcome_unknown("mutation completed, but writing bounded stdout result failed")
    } else {
        CliError {
            code: read_only_code.as_str(),
            message: "failed to write bounded command output".to_string(),
            cleanup_refusal_reason: None,
        }
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
