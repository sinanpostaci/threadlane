use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane::{ValidatedWorkerProfile, WorkersConfig};
use threadlane_authority::{
    build_approved_worker_profile_config, compute_recorded_history_worker_profile_digest,
    ApprovalRecord, ApprovedTaskRecord,
};
use threadlane_core::WorkerExecutionBinding;
use uuid::Uuid;

use crate::error::HostError;
use crate::types::{
    HostExecutionGates, HostLaunchIntent, HostPhase, HostSessionManifest, TaskDispatchIntent,
    TaskRunReceipt, HOST_LAUNCH_INTENT_SCHEMA_VERSION, HOST_LOCK_FILENAME, HOST_MANIFEST_FILENAME,
    HOST_MANIFEST_SCHEMA_VERSION, LAUNCH_INTENTS_SUBDIR, MAX_HOST_MANIFEST_BYTES,
    MAX_LAUNCH_INTENT_BYTES, MAX_TASK_RECEIPTS_BYTES, RUNTIME_STATE_SUBDIR, TASK_RECEIPTS_FILENAME,
    TASK_RECEIPTS_LOCK_FILENAME, TASK_RECEIPTS_SCHEMA_VERSION, WORK_AREA_SUBDIR,
};

const APPROVAL_ID_PREFIX: &str = "apr_";
const APPROVAL_ID_HEX_LEN: usize = 32;
const HOST_INSTANCE_ID_PREFIX: &str = "hst_";
const HOST_INSTANCE_ID_HEX_LEN: usize = 32;
const MAX_IDENTIFIER_LEN: usize = 128;
const HOST_MANIFEST_TMP_PREFIX: &str = ".host-manifest.json.tmp.";
const TASK_RECEIPTS_TMP_PREFIX: &str = ".task-receipts.json.tmp.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecognizedAtomicTemp {
    committed_name: &'static str,
    max_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TaskReceiptsFile {
    pub schema_version: u32,
    pub approval_id: String,
    pub subject_digest: String,
    pub session_binding_id: String,
    #[serde(default)]
    pub pending_intents: Vec<TaskDispatchIntent>,
    pub receipts: Vec<TaskRunReceipt>,
}

/// Exclusive OS-level file lock (`<session_dir>/host.lock`) held for the entire lifetime
/// of a running `threadlane-host` session.
#[derive(Debug)]
pub struct HostSessionLock {
    lock_path: PathBuf,
    file: File,
}

impl HostSessionLock {
    /// Acquires the exclusive non-blocking process lock for `approval_id` inside `session_dir`.
    ///
    /// Fails closed with [`HostError::LockBusy`] if another active host process already holds the lock.
    pub fn acquire(session_dir: &Path, approval_id: &str) -> Result<Self, HostError> {
        validate_approval_id(approval_id)?;
        let lock_path = session_dir.join(HOST_LOCK_FILENAME);
        let file = open_private_lock_file(&lock_path)?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Self { lock_path, file }),
            Err(_) => Err(HostError::LockBusy {
                approval_id: approval_id.to_string(),
                session_dir: session_dir.to_path_buf(),
            }),
        }
    }

    /// Returns the path to the underlying `host.lock` file.
    pub fn path(&self) -> &Path {
        &self.lock_path
    }
}

impl Drop for HostSessionLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// Probes whether `<session_dir>/host.lock` is currently held by an active process.
///
/// Opens a separate file descriptor and attempts a non-blocking exclusive lock, immediately
/// unlocking if acquired.
pub fn is_host_lock_held(session_dir: &Path) -> Result<bool, HostError> {
    let lock_path = session_dir.join(HOST_LOCK_FILENAME);
    let checked = ensure_no_symlink_components(&lock_path)?;
    let meta = match fs::symlink_metadata(&checked) {
        Ok(m) => m,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(HostError::Io {
                operation: "symlink_metadata(host.lock)".to_string(),
                reason: err.to_string(),
            })
        }
    };
    validate_private_regular_metadata(&checked, &meta)?;

    let file = open_private_lock_file(&checked)?;
    match file.try_lock_exclusive() {
        Ok(()) => {
            let _ = file.unlock();
            Ok(false)
        }
        Err(_) => Ok(true),
    }
}

/// Returns the current wall-clock timestamp in milliseconds since Unix epoch.
pub(crate) fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Validates that `approval_id` has the canonical `apr_<32 lowercase hex>` format.
pub fn validate_approval_id(approval_id: &str) -> Result<(), HostError> {
    if !approval_id.starts_with(APPROVAL_ID_PREFIX)
        || approval_id.len() != APPROVAL_ID_PREFIX.len() + APPROVAL_ID_HEX_LEN
    {
        return Err(HostError::validation(
            "approval_id",
            "must match `apr_<32 lowercase hex characters>`",
        ));
    }
    let hex_part = &approval_id[APPROVAL_ID_PREFIX.len()..];
    if !hex_part
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(HostError::validation(
            "approval_id",
            "must contain only lowercase hexadecimal characters after `apr_`",
        ));
    }
    Ok(())
}

/// Generates a fresh public per-start host instance identifier (`hst_<32 lowercase hex>`).
pub(crate) fn generate_host_instance_id() -> String {
    format!("{HOST_INSTANCE_ID_PREFIX}{}", Uuid::new_v4().simple())
}

/// Validates that `value` has the canonical `hst_<32 lowercase hex>` format.
pub fn validate_host_instance_id(field: &'static str, value: &str) -> Result<(), HostError> {
    if !value.starts_with(HOST_INSTANCE_ID_PREFIX)
        || value.len() != HOST_INSTANCE_ID_PREFIX.len() + HOST_INSTANCE_ID_HEX_LEN
    {
        return Err(HostError::validation(
            field,
            "must match `hst_<32 lowercase hex characters>`",
        ));
    }
    let hex_part = &value[HOST_INSTANCE_ID_PREFIX.len()..];
    if !hex_part
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(HostError::validation(
            field,
            "must contain only lowercase hexadecimal characters after `hst_`",
        ));
    }
    Ok(())
}

/// Validates a bounded safe identifier (`[a-zA-Z0-9_-]{1,128}`).
pub fn validate_identifier(field: &'static str, value: &str) -> Result<(), HostError> {
    if value.is_empty() {
        return Err(HostError::validation(field, "must not be empty"));
    }
    if value.len() > MAX_IDENTIFIER_LEN {
        return Err(HostError::validation(
            field,
            format!("exceeds maximum length of {MAX_IDENTIFIER_LEN} characters"),
        ));
    }
    if !value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(HostError::validation(
            field,
            "must contain only ASCII alphanumeric characters, `_`, or `-`",
        ));
    }
    Ok(())
}

/// Validates a canonical lowercase hyphenated non-nil UUID string.
pub fn validate_canonical_thread_uuid(field: &'static str, value: &str) -> Result<(), HostError> {
    let parsed = Uuid::parse_str(value)
        .map_err(|_| HostError::validation(field, "must be a valid hyphenated UUID"))?;
    if parsed.is_nil() {
        return Err(HostError::validation(field, "must not be the nil UUID"));
    }
    if parsed.hyphenated().to_string() != value {
        return Err(HostError::validation(
            field,
            "must be formatted as canonical lowercase hyphenated UUID",
        ));
    }
    Ok(())
}

/// Validates a 64-character lowercase hexadecimal SHA-256 digest.
pub fn validate_hex_digest(field: &'static str, value: &str) -> Result<(), HostError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(HostError::validation(
            field,
            "must be a 64-character lowercase hex SHA-256 digest",
        ));
    }
    Ok(())
}

/// Derives a deterministic, bounded command key for revision-checked worker cancellation.
pub fn derive_cancel_command_key(
    subject_digest: &str,
    task_id: &str,
    run_id: &str,
    expected_revision: u64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"threadlane-host-cancel-v1\0");
    hasher.update(subject_digest.as_bytes());
    hasher.update(b"\0");
    hasher.update(task_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(run_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(expected_revision.to_string().as_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(32);
    for byte in &digest[..16] {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    format!("cancel_{hex}")
}

/// Derives a deterministic, bounded command key for acknowledging a delivered event batch.
///
/// Keyed by domain separator (`threadlane-host-ack-v1`), the approval's `subject_digest`,
/// the runtime `session_binding_id`, and `batch_id`.
pub fn derive_ack_command_key(
    subject_digest: &str,
    session_binding_id: &str,
    batch_id: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"threadlane-host-ack-v1\0");
    hasher.update(subject_digest.as_bytes());
    hasher.update(b"\0");
    hasher.update(session_binding_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(batch_id.as_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(32);
    for byte in &digest[..16] {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    format!("ack_{hex}")
}

/// Ensures no existing component of `path` is a symlink and rejects parent-directory traversal.
pub fn ensure_no_symlink_components(path: &Path) -> Result<PathBuf, HostError> {
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|err| HostError::Io {
                operation: "current_dir".to_string(),
                reason: err.to_string(),
            })?
            .join(path)
    };

    let mut acc = PathBuf::new();
    for component in abs.components() {
        match component {
            Component::Prefix(prefix) => acc.push(prefix.as_os_str()),
            Component::RootDir => acc.push(Component::RootDir.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(HostError::SecurityViolation {
                    path: path.display().to_string(),
                    reason: "parent directory traversal (`..`) is not permitted".to_string(),
                });
            }
            Component::Normal(part) => {
                acc.push(part);
                match fs::symlink_metadata(&acc) {
                    Ok(meta) => {
                        if meta.file_type().is_symlink() {
                            return Err(HostError::SecurityViolation {
                                path: acc.display().to_string(),
                                reason: "symlink path component is not permitted".to_string(),
                            });
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => {
                        return Err(HostError::Io {
                            operation: "symlink_metadata".to_string(),
                            reason: err.to_string(),
                        });
                    }
                }
            }
        }
    }
    Ok(abs)
}

/// Ensures `path` exists as a private `0700` directory with no symlink components and returns
/// its canonical path.
pub fn ensure_private_dir(path: &Path) -> Result<PathBuf, HostError> {
    let checked = ensure_no_symlink_components(path)?;
    match fs::symlink_metadata(&checked) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(HostError::SecurityViolation {
                    path: checked.display().to_string(),
                    reason: "directory must not be a symlink".to_string(),
                });
            }
            if !meta.file_type().is_dir() {
                return Err(HostError::SecurityViolation {
                    path: checked.display().to_string(),
                    reason: "expected a directory".to_string(),
                });
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = meta.permissions().mode() & 0o777;
                if (mode & 0o077) != 0 {
                    return Err(HostError::SecurityViolation {
                        path: checked.display().to_string(),
                        reason: format!(
                            "directory permissions {mode:04o} are too permissive; group/other bits must be 0 (expected 0700)"
                        ),
                    });
                }
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            builder.recursive(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            builder
                .create(&checked)
                .map_err(|create_err| HostError::Io {
                    operation: "create_private_dir".to_string(),
                    reason: create_err.to_string(),
                })?;
        }
        Err(err) => {
            return Err(HostError::Io {
                operation: "symlink_metadata".to_string(),
                reason: err.to_string(),
            });
        }
    }

    let rechecked = ensure_no_symlink_components(&checked)?;
    let meta = fs::symlink_metadata(&rechecked).map_err(|err| HostError::Io {
        operation: "symlink_metadata".to_string(),
        reason: err.to_string(),
    })?;
    if !meta.file_type().is_dir() {
        return Err(HostError::SecurityViolation {
            path: rechecked.display().to_string(),
            reason: "expected a directory".to_string(),
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = meta.permissions().mode() & 0o777;
        if (mode & 0o077) != 0 {
            return Err(HostError::SecurityViolation {
                path: rechecked.display().to_string(),
                reason: format!(
                    "directory permissions {mode:04o} are too permissive; group/other bits must be 0 (expected 0700)"
                ),
            });
        }
    }

    fs::canonicalize(&rechecked).map_err(|err| HostError::Io {
        operation: "canonicalize".to_string(),
        reason: err.to_string(),
    })
}

/// Verifies that `path` already exists as a private `0700` directory with no symlink components.
pub fn verify_existing_private_dir(path: &Path) -> Result<PathBuf, HostError> {
    let checked = ensure_no_symlink_components(path)?;
    let meta = match fs::symlink_metadata(&checked) {
        Ok(m) => m,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(HostError::SecurityViolation {
                path: checked.display().to_string(),
                reason: "expected existing private directory".to_string(),
            });
        }
        Err(err) => {
            return Err(HostError::Io {
                operation: "symlink_metadata".to_string(),
                reason: err.to_string(),
            });
        }
    };
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return Err(HostError::SecurityViolation {
            path: checked.display().to_string(),
            reason: "path must be a real directory, not a symlink or regular file".to_string(),
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = meta.permissions().mode() & 0o777;
        if (mode & 0o077) != 0 {
            return Err(HostError::SecurityViolation {
                path: checked.display().to_string(),
                reason: format!(
                    "directory permissions {mode:04o} are too permissive; group/other bits must be 0 (expected 0700)"
                ),
            });
        }
    }
    fs::canonicalize(&checked).map_err(|err| HostError::Io {
        operation: "canonicalize".to_string(),
        reason: err.to_string(),
    })
}

/// Returns `true` if `a` is an ancestor of `b`, `b` is an ancestor of `a`, or `a == b`.
pub fn paths_overlap(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

/// Validates the operator-selected `sessions_dir` root:
/// - Ensures `sessions_dir` is a private `0700` directory without symlink components.
/// - Rejects any path overlap with `other_dirs` (registry, plans, authority, or repository roots).
/// - Rejects any foreign or non-private entry directly inside `sessions_dir`.
///
/// Startup and legacy client construction may create a missing private root. Strictly read-only
/// callers must use [`validate_existing_sessions_root`] instead.
pub fn validate_sessions_root(
    sessions_dir: &Path,
    other_dirs: &[(&str, &Path)],
) -> Result<PathBuf, HostError> {
    let canonical_sessions = ensure_private_dir(sessions_dir)?;
    validate_sessions_root_contents(&canonical_sessions, other_dirs)?;
    Ok(canonical_sessions)
}

/// Validates an already existing operator-selected `sessions_dir` root without creating it,
/// changing its permissions, or acquiring/creating any lock file.
///
/// Shares the exact overlap and entry checks used by [`validate_sessions_root`].
pub fn validate_existing_sessions_root(
    sessions_dir: &Path,
    other_dirs: &[(&str, &Path)],
) -> Result<PathBuf, HostError> {
    let canonical_sessions = verify_existing_private_dir(sessions_dir)?;
    validate_sessions_root_contents(&canonical_sessions, other_dirs)?;
    Ok(canonical_sessions)
}

fn validate_sessions_root_contents(
    canonical_sessions: &Path,
    other_dirs: &[(&str, &Path)],
) -> Result<(), HostError> {
    for (label, other) in other_dirs {
        let other_resolved = if other.exists() {
            fs::canonicalize(other).unwrap_or_else(|_| other.to_path_buf())
        } else {
            ensure_no_symlink_components(other).unwrap_or_else(|_| other.to_path_buf())
        };
        if paths_overlap(canonical_sessions, &other_resolved) {
            return Err(HostError::SecurityViolation {
                path: canonical_sessions.display().to_string(),
                reason: format!(
                    "sessions_dir must not equal or overlap `{label}` (`{}`)",
                    other_resolved.display()
                ),
            });
        }
    }

    let entries = fs::read_dir(canonical_sessions).map_err(|err| HostError::Io {
        operation: "read_dir(sessions_dir)".to_string(),
        reason: err.to_string(),
    })?;
    for entry in entries {
        let entry = entry.map_err(|err| HostError::Io {
            operation: "read_dir_entry(sessions_dir)".to_string(),
            reason: err.to_string(),
        })?;
        let entry_path = entry.path();
        let file_name = entry.file_name();
        let Some(name_str) = file_name.to_str() else {
            return Err(HostError::SecurityViolation {
                path: entry_path.display().to_string(),
                reason: "non-UTF-8 entry in sessions_dir is not permitted".to_string(),
            });
        };
        if name_str == LAUNCH_INTENTS_SUBDIR {
            let meta = fs::symlink_metadata(&entry_path).map_err(|err| HostError::Io {
                operation: "symlink_metadata(launch_intents_dir)".to_string(),
                reason: err.to_string(),
            })?;
            if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
                return Err(HostError::SecurityViolation {
                    path: entry_path.display().to_string(),
                    reason: ".launch-intents inside sessions_dir must be a private directory"
                        .to_string(),
                });
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = meta.permissions().mode() & 0o777;
                if (mode & 0o077) != 0 {
                    return Err(HostError::SecurityViolation {
                        path: entry_path.display().to_string(),
                        reason: format!(
                            ".launch-intents directory permissions {mode:04o} are too permissive (expected 0700)"
                        ),
                    });
                }
            }
            inspect_launch_intents_dir(&entry_path)?;
            continue;
        }
        if validate_approval_id(name_str).is_err() {
            return Err(HostError::SecurityViolation {
                path: entry_path.display().to_string(),
                reason: format!("foreign entry `{name_str}` in sessions_dir is not permitted"),
            });
        }
        let meta = fs::symlink_metadata(&entry_path).map_err(|err| HostError::Io {
            operation: "symlink_metadata(session_entry)".to_string(),
            reason: err.to_string(),
        })?;
        if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
            return Err(HostError::SecurityViolation {
                path: entry_path.display().to_string(),
                reason: "session entry inside sessions_dir must be a private directory".to_string(),
            });
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = meta.permissions().mode() & 0o777;
            if (mode & 0o077) != 0 {
                return Err(HostError::SecurityViolation {
                    path: entry_path.display().to_string(),
                    reason: format!(
                        "session directory permissions {mode:04o} are too permissive (expected 0700)"
                    ),
                });
            }
        }
    }

    Ok(())
}

/// Validates and prepares `<sessions_root>/<approval_id>` for host startup.
///
/// Rejects symlinks, non-private permissions, and foreign files or pre-populated foreign directories.
pub fn prepare_session_dir(sessions_root: &Path, approval_id: &str) -> Result<PathBuf, HostError> {
    validate_approval_id(approval_id)?;
    let session_dir = sessions_root.join(approval_id);
    let checked = ensure_no_symlink_components(&session_dir)?;

    if fs::symlink_metadata(&checked).is_ok() {
        let canonical = verify_existing_private_dir(&checked)?;
        inspect_session_dir_entries(&canonical)?;
        Ok(canonical)
    } else {
        let canonical = ensure_private_dir(&checked)?;
        inspect_session_dir_entries(&canonical)?;
        Ok(canonical)
    }
}

/// Verifies an existing `<sessions_root>/<approval_id>` directory for client access without
/// creating missing directories.
pub fn verify_existing_session_dir(
    sessions_root: &Path,
    approval_id: &str,
) -> Result<PathBuf, HostError> {
    validate_approval_id(approval_id)?;
    let session_dir = sessions_root.join(approval_id);
    let checked = ensure_no_symlink_components(&session_dir)?;

    match fs::symlink_metadata(&checked) {
        Ok(_) => {
            let canonical = verify_existing_private_dir(&checked)?;
            inspect_session_dir_entries(&canonical)?;
            Ok(canonical)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Err(HostError::HostNotReady {
            approval_id: approval_id.to_string(),
            phase: None,
            reason: "host session directory does not exist".to_string(),
        }),
        Err(err) => Err(HostError::Io {
            operation: "symlink_metadata(session_dir)".to_string(),
            reason: err.to_string(),
        }),
    }
}

/// Verifies under a held [`HostSessionLock`] that `session_dir` contains no prior host manifest,
/// task receipts, runtime state, work area, or transient temp files before a `--start-fresh-only`
/// launch writes `Bootstrapping`.
///
/// An empty session directory containing only the zero-byte `host.lock` file is permitted. All
/// existing files are preserved untouched on error.
pub fn ensure_fresh_session_dir_locked(
    session_dir: &Path,
    record: &ApprovalRecord,
) -> Result<(), HostError> {
    inspect_session_dir_entries(session_dir)?;

    if let Some(existing_manifest) = read_host_manifest_optional(session_dir, record)? {
        return Err(HostError::RestartNotAllowed {
            approval_id: record.approval_id.clone(),
            phase: Some(existing_manifest.phase),
            detail: format!(
                "fresh-only host start refused existing host session in phase `{}`",
                existing_manifest.phase
            ),
        });
    }

    let entries = fs::read_dir(session_dir).map_err(|err| HostError::Io {
        operation: "read_dir(session_dir)".to_string(),
        reason: err.to_string(),
    })?;

    let mut prior_entries = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|err| HostError::Io {
            operation: "read_dir_entry(session_dir)".to_string(),
            reason: err.to_string(),
        })?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            return Err(HostError::SecurityViolation {
                path: entry.path().display().to_string(),
                reason: "non-UTF-8 entry in session directory is not permitted".to_string(),
            });
        };
        if name == HOST_LOCK_FILENAME {
            let meta = fs::symlink_metadata(entry.path()).map_err(|err| HostError::Io {
                operation: "symlink_metadata(host.lock)".to_string(),
                reason: err.to_string(),
            })?;
            validate_private_regular_metadata(&entry.path(), &meta)?;
            if meta.len() != 0 {
                return Err(HostError::SecurityViolation {
                    path: entry.path().display().to_string(),
                    reason: format!(
                        "host.lock must be an empty 0-byte lock file (found {} bytes)",
                        meta.len()
                    ),
                });
            }
            continue;
        }
        prior_entries.push(name.to_string());
    }

    if !prior_entries.is_empty() {
        prior_entries.sort();
        return Err(HostError::RestartNotAllowed {
            approval_id: record.approval_id.clone(),
            phase: None,
            detail: format!(
                "fresh-only host start refused pre-existing session state ({})",
                prior_entries.join(", ")
            ),
        });
    }

    Ok(())
}

fn is_valid_lowercase_uuid_v4_simple(suffix: &str) -> bool {
    if suffix.len() != 32
        || !suffix
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return false;
    }
    let Ok(parsed) = Uuid::parse_str(suffix) else {
        return false;
    };
    parsed.get_version_num() == 4 && (parsed.as_bytes()[8] & 0xc0) == 0x80
}

fn recognized_atomic_temp(name: &str) -> Option<RecognizedAtomicTemp> {
    if let Some(suffix) = name.strip_prefix(HOST_MANIFEST_TMP_PREFIX) {
        if is_valid_lowercase_uuid_v4_simple(suffix) {
            return Some(RecognizedAtomicTemp {
                committed_name: HOST_MANIFEST_FILENAME,
                max_bytes: MAX_HOST_MANIFEST_BYTES,
            });
        }
    } else if let Some(suffix) = name.strip_prefix(TASK_RECEIPTS_TMP_PREFIX) {
        if is_valid_lowercase_uuid_v4_simple(suffix) {
            return Some(RecognizedAtomicTemp {
                committed_name: TASK_RECEIPTS_FILENAME,
                max_bytes: MAX_TASK_RECEIPTS_BYTES,
            });
        }
    }
    None
}

fn inspect_session_dir_entries(session_dir: &Path) -> Result<(), HostError> {
    let entries = fs::read_dir(session_dir).map_err(|err| HostError::Io {
        operation: "read_dir(session_dir)".to_string(),
        reason: err.to_string(),
    })?;

    let mut entry_names = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|err| HostError::Io {
            operation: "read_dir_entry(session_dir)".to_string(),
            reason: err.to_string(),
        })?;
        entry_names.push(entry.file_name());
    }

    inspect_session_dir_snapshot(session_dir, &entry_names)
}

/// Validates a snapshot of directory entry names observed inside `<sessions_root>/<approval_id>`.
///
/// Recognizes only the committed session artifacts and active/remnant atomic writer temporary
/// files for `host-manifest.json` and `task-receipts.json` (`.<target>.tmp.<32-char lowercase UUIDv4>`),
/// enforcing private `0600` regular file metadata and byte budgets while requiring at least one
/// committed ownership anchor (`host.lock` or `host-manifest.json`) whenever the directory is
/// non-empty.
pub(crate) fn inspect_session_dir_snapshot<I, S>(
    session_dir: &Path,
    entry_names: I,
) -> Result<(), HostError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut saw_any_entry = false;
    let mut has_ownership_anchor = false;

    for raw_name in entry_names {
        saw_any_entry = true;
        let file_name = raw_name.as_ref();
        let entry_path = session_dir.join(file_name);
        let Some(name) = file_name.to_str() else {
            return Err(HostError::SecurityViolation {
                path: entry_path.display().to_string(),
                reason: "non-UTF-8 entry in session directory is not permitted".to_string(),
            });
        };

        if let Some(temp_spec) = recognized_atomic_temp(name) {
            match fs::symlink_metadata(&entry_path) {
                Ok(meta) => {
                    if meta.file_type().is_symlink() {
                        return Err(HostError::SecurityViolation {
                            path: entry_path.display().to_string(),
                            reason: "symlink inside session directory is not permitted".to_string(),
                        });
                    }
                    validate_private_regular_metadata(&entry_path, &meta)?;
                    if meta.len() > temp_spec.max_bytes as u64 {
                        return Err(HostError::SecurityViolation {
                            path: entry_path.display().to_string(),
                            reason: format!(
                                "transient atomic file `{name}` size ({} bytes) exceeds maximum {} bytes",
                                meta.len(),
                                temp_spec.max_bytes
                            ),
                        });
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    let committed_path = session_dir.join(temp_spec.committed_name);
                    match fs::symlink_metadata(&committed_path) {
                        Ok(committed_meta) => {
                            if committed_meta.file_type().is_symlink() {
                                return Err(HostError::SecurityViolation {
                                    path: committed_path.display().to_string(),
                                    reason: "symlink inside session directory is not permitted"
                                        .to_string(),
                                });
                            }
                            validate_private_regular_metadata(&committed_path, &committed_meta)?;
                            if committed_meta.len() > temp_spec.max_bytes as u64 {
                                return Err(HostError::SecurityViolation {
                                    path: committed_path.display().to_string(),
                                    reason: format!(
                                        "committed atomic target `{}` size ({} bytes) exceeds maximum {} bytes",
                                        temp_spec.committed_name,
                                        committed_meta.len(),
                                        temp_spec.max_bytes
                                    ),
                                });
                            }
                            if temp_spec.committed_name == HOST_MANIFEST_FILENAME {
                                has_ownership_anchor = true;
                            }
                        }
                        Err(committed_err)
                            if committed_err.kind() == std::io::ErrorKind::NotFound => {}
                        Err(committed_err) => {
                            return Err(HostError::Io {
                                operation: "symlink_metadata(session_entry)".to_string(),
                                reason: committed_err.to_string(),
                            });
                        }
                    }
                }
                Err(err) => {
                    return Err(HostError::Io {
                        operation: "symlink_metadata(session_entry)".to_string(),
                        reason: err.to_string(),
                    });
                }
            }
            continue;
        }

        let meta = match fs::symlink_metadata(&entry_path) {
            Ok(meta) => meta,
            Err(err) => {
                if !matches!(
                    name,
                    HOST_LOCK_FILENAME
                        | HOST_MANIFEST_FILENAME
                        | TASK_RECEIPTS_FILENAME
                        | TASK_RECEIPTS_LOCK_FILENAME
                        | RUNTIME_STATE_SUBDIR
                        | WORK_AREA_SUBDIR
                ) {
                    return Err(HostError::SecurityViolation {
                        path: entry_path.display().to_string(),
                        reason: format!(
                            "foreign or unexpected entry `{name}` in session directory"
                        ),
                    });
                }
                return Err(HostError::Io {
                    operation: "symlink_metadata(session_entry)".to_string(),
                    reason: err.to_string(),
                });
            }
        };
        if meta.file_type().is_symlink() {
            return Err(HostError::SecurityViolation {
                path: entry_path.display().to_string(),
                reason: "symlink inside session directory is not permitted".to_string(),
            });
        }

        match name {
            HOST_LOCK_FILENAME | HOST_MANIFEST_FILENAME => {
                validate_private_regular_metadata(&entry_path, &meta)?;
                has_ownership_anchor = true;
            }
            TASK_RECEIPTS_FILENAME | TASK_RECEIPTS_LOCK_FILENAME => {
                validate_private_regular_metadata(&entry_path, &meta)?;
            }
            RUNTIME_STATE_SUBDIR | WORK_AREA_SUBDIR => {
                if !meta.file_type().is_dir() {
                    return Err(HostError::SecurityViolation {
                        path: entry_path.display().to_string(),
                        reason: format!("expected `{name}` to be a private directory"),
                    });
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mode = meta.permissions().mode() & 0o777;
                    if (mode & 0o077) != 0 {
                        return Err(HostError::SecurityViolation {
                            path: entry_path.display().to_string(),
                            reason: format!(
                                "subdirectory `{name}` permissions {mode:04o} are too permissive (expected 0700)"
                            ),
                        });
                    }
                }
            }
            _ => {
                return Err(HostError::SecurityViolation {
                    path: entry_path.display().to_string(),
                    reason: format!("foreign or unexpected entry `{name}` in session directory"),
                });
            }
        }
    }

    if saw_any_entry && !has_ownership_anchor {
        return Err(HostError::SecurityViolation {
            path: session_dir.display().to_string(),
            reason:
                "pre-existing non-empty session directory lacks host.lock and host-manifest.json"
                    .to_string(),
        });
    }

    Ok(())
}

fn validate_private_regular_metadata(path: &Path, meta: &fs::Metadata) -> Result<(), HostError> {
    if meta.file_type().is_symlink() {
        return Err(HostError::SecurityViolation {
            path: path.display().to_string(),
            reason: "symlink file is not permitted".to_string(),
        });
    }
    if !meta.file_type().is_file() {
        return Err(HostError::SecurityViolation {
            path: path.display().to_string(),
            reason: "expected a regular file".to_string(),
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = meta.permissions().mode() & 0o777;
        if (mode & 0o177) != 0 {
            return Err(HostError::SecurityViolation {
                path: path.display().to_string(),
                reason: format!(
                    "file permissions {mode:04o} are not private regular 0600 (executable/group/other bits must be 0)"
                ),
            });
        }
    }
    Ok(())
}

fn open_private_lock_file(lock_path: &Path) -> Result<File, HostError> {
    let checked = ensure_no_symlink_components(lock_path)?;
    if let Ok(meta) = fs::symlink_metadata(&checked) {
        validate_private_regular_metadata(&checked, &meta)?;
    }

    let mut opts = OpenOptions::new();
    opts.read(true).write(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
        opts.custom_flags(libc_o_nofollow());
    }
    let file = opts.open(&checked).map_err(|err| HostError::Io {
        operation: format!("open_lock({})", checked.display()),
        reason: err.to_string(),
    })?;
    let meta = file.metadata().map_err(|err| HostError::Io {
        operation: "fstat(lock)".to_string(),
        reason: err.to_string(),
    })?;
    validate_private_regular_metadata(&checked, &meta)?;
    Ok(file)
}

fn open_existing_private_lock_file(lock_path: &Path) -> Result<Option<File>, HostError> {
    let checked = ensure_no_symlink_components(lock_path)?;
    match fs::symlink_metadata(&checked) {
        Ok(meta) => validate_private_regular_metadata(&checked, &meta)?,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(HostError::Io {
                operation: format!("symlink_metadata({})", checked.display()),
                reason: err.to_string(),
            })
        }
    }

    let mut opts = OpenOptions::new();
    opts.read(true).write(true).create(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.custom_flags(libc_o_nofollow());
    }
    let file = match opts.open(&checked) {
        Ok(f) => f,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(HostError::Io {
                operation: format!("open_existing_lock({})", checked.display()),
                reason: err.to_string(),
            })
        }
    };
    let meta = file.metadata().map_err(|err| HostError::Io {
        operation: "fstat(lock)".to_string(),
        reason: err.to_string(),
    })?;
    validate_private_regular_metadata(&checked, &meta)?;
    Ok(Some(file))
}

#[cfg(unix)]
const fn libc_o_nofollow() -> i32 {
    0o400000
}

fn atomic_write_private_file(
    target_path: &Path,
    bytes: &[u8],
    max_bytes: usize,
) -> Result<(), HostError> {
    if bytes.len() > max_bytes {
        return Err(HostError::Validation {
            field: "file_size".to_string(),
            reason: format!(
                "serialized payload of {} bytes exceeds limit of {max_bytes} bytes",
                bytes.len()
            ),
        });
    }

    let checked_target = ensure_no_symlink_components(target_path)?;
    if let Ok(meta) = fs::symlink_metadata(&checked_target) {
        validate_private_regular_metadata(&checked_target, &meta)?;
    }

    let parent = checked_target
        .parent()
        .ok_or_else(|| HostError::SecurityViolation {
            path: checked_target.display().to_string(),
            reason: "target file must have a parent directory".to_string(),
        })?;
    verify_existing_private_dir(parent)?;

    let file_name = checked_target
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("record.json");
    let tmp_name = format!(".{file_name}.tmp.{}", Uuid::new_v4().simple());
    let tmp_path = parent.join(tmp_name);

    let write_res = (|| -> Result<(), HostError> {
        let mut opts = OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
            opts.custom_flags(libc_o_nofollow());
        }
        let mut file = opts.open(&tmp_path).map_err(|err| HostError::Io {
            operation: "create_temp_file".to_string(),
            reason: err.to_string(),
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(fs::Permissions::from_mode(0o600))
                .map_err(|err| HostError::Io {
                    operation: "chmod_temp_file".to_string(),
                    reason: err.to_string(),
                })?;
        }
        file.write_all(bytes).map_err(|err| HostError::Io {
            operation: "write_temp_file".to_string(),
            reason: err.to_string(),
        })?;
        file.sync_all().map_err(|err| HostError::Io {
            operation: "fsync_temp_file".to_string(),
            reason: err.to_string(),
        })?;
        drop(file);

        fs::rename(&tmp_path, &checked_target).map_err(|err| HostError::Io {
            operation: "rename_temp_file".to_string(),
            reason: err.to_string(),
        })?;

        let dir_file = File::open(parent).map_err(|err| HostError::Io {
            operation: "open_parent_dir_for_fsync".to_string(),
            reason: err.to_string(),
        })?;
        dir_file.sync_all().map_err(|err| HostError::Io {
            operation: "fsync_parent_dir".to_string(),
            reason: err.to_string(),
        })?;
        Ok(())
    })();

    if write_res.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    write_res
}

/// Validates and writes `<session_dir>/host-manifest.json` atomically (`0600`, `<= 64 KiB`).
pub fn write_host_manifest(
    session_dir: &Path,
    manifest: &HostSessionManifest,
) -> Result<(), HostError> {
    validate_host_manifest_structure(manifest)?;
    let mut bytes =
        serde_json::to_vec_pretty(manifest).map_err(|err| HostError::CorruptManifest {
            path: session_dir
                .join(HOST_MANIFEST_FILENAME)
                .display()
                .to_string(),
            reason: format!("failed to serialize host manifest: {err}"),
        })?;
    bytes.push(b'\n');
    let manifest_path = session_dir.join(HOST_MANIFEST_FILENAME);
    if bytes.len() > MAX_HOST_MANIFEST_BYTES {
        return Err(HostError::CorruptManifest {
            path: manifest_path.display().to_string(),
            reason: format!(
                "serialized host manifest ({} bytes) exceeds maximum {} bytes",
                bytes.len(),
                MAX_HOST_MANIFEST_BYTES
            ),
        });
    }
    atomic_write_private_file(&manifest_path, &bytes, MAX_HOST_MANIFEST_BYTES)
}

/// Reads and verifies `<session_dir>/host-manifest.json` against `record` if it exists.
///
/// Corrupt or tampered manifests are preserved on disk and return [`HostError::CorruptManifest`].
pub fn read_host_manifest_optional(
    session_dir: &Path,
    record: &ApprovalRecord,
) -> Result<Option<HostSessionManifest>, HostError> {
    let manifest_path = session_dir.join(HOST_MANIFEST_FILENAME);
    let checked = ensure_no_symlink_components(&manifest_path)?;
    match fs::symlink_metadata(&checked) {
        Ok(_) => read_and_verify_host_manifest(&checked, record).map(Some),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(HostError::Io {
            operation: "symlink_metadata(host-manifest.json)".to_string(),
            reason: err.to_string(),
        }),
    }
}

/// Reads and verifies `<session_dir>/host-manifest.json` against `record`, failing if missing.
pub fn read_and_verify_host_manifest(
    manifest_path: &Path,
    record: &ApprovalRecord,
) -> Result<HostSessionManifest, HostError> {
    let checked = ensure_no_symlink_components(manifest_path)?;
    let meta = fs::symlink_metadata(&checked).map_err(|err| HostError::Io {
        operation: "symlink_metadata(host-manifest.json)".to_string(),
        reason: err.to_string(),
    })?;
    validate_private_regular_metadata(&checked, &meta)?;
    if meta.len() > MAX_HOST_MANIFEST_BYTES as u64 {
        return Err(HostError::CorruptManifest {
            path: checked.display().to_string(),
            reason: format!(
                "manifest file size ({} bytes) exceeds maximum {} bytes",
                meta.len(),
                MAX_HOST_MANIFEST_BYTES
            ),
        });
    }

    let file = File::open(&checked).map_err(|err| HostError::Io {
        operation: "open(host-manifest.json)".to_string(),
        reason: err.to_string(),
    })?;
    let mut bytes = Vec::with_capacity(meta.len() as usize);
    file.take(MAX_HOST_MANIFEST_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|err| HostError::Io {
            operation: "read(host-manifest.json)".to_string(),
            reason: err.to_string(),
        })?;
    if bytes.len() > MAX_HOST_MANIFEST_BYTES {
        return Err(HostError::CorruptManifest {
            path: checked.display().to_string(),
            reason: format!(
                "manifest content exceeds maximum {} bytes",
                MAX_HOST_MANIFEST_BYTES
            ),
        });
    }

    let manifest: HostSessionManifest =
        serde_json::from_slice(&bytes).map_err(|err| HostError::CorruptManifest {
            path: checked.display().to_string(),
            reason: format!("invalid host-manifest.json JSON: {err}"),
        })?;

    verify_manifest_against_approval(&manifest, record, &checked)?;
    Ok(manifest)
}

fn validate_host_manifest_structure(manifest: &HostSessionManifest) -> Result<(), HostError> {
    if manifest.schema_version != HOST_MANIFEST_SCHEMA_VERSION {
        return Err(HostError::validation(
            "schema_version",
            format!(
                "expected schema_version {}, got {}",
                HOST_MANIFEST_SCHEMA_VERSION, manifest.schema_version
            ),
        ));
    }
    validate_approval_id(&manifest.approval_id)?;
    validate_hex_digest("subject_digest", &manifest.subject_digest)?;
    validate_identifier("control_binding_id", &manifest.control_binding_id)?;
    validate_canonical_thread_uuid("thread_uuid", &manifest.thread_uuid)?;
    validate_identifier("plan_id", &manifest.plan_id)?;
    if manifest.plan_revision == 0 {
        return Err(HostError::validation("plan_revision", "must be >= 1"));
    }
    if let Some(ref sb) = manifest.session_binding_id {
        validate_identifier("session_binding_id", sb)?;
    }
    if let Some(ref inst) = manifest.instance_id {
        validate_host_instance_id("instance_id", inst)?;
    }
    if matches!(manifest.phase, HostPhase::Ready | HostPhase::Stopping)
        && manifest.session_binding_id.is_none()
    {
        return Err(HostError::validation(
            "session_binding_id",
            "Ready or Stopping manifest must record session_binding_id",
        ));
    }
    let ready = manifest.phase == HostPhase::Ready;
    let expected_gates = HostExecutionGates::for_ready_state(ready);
    let legacy_gates = HostExecutionGates::legacy_for_ready_state(ready);
    if manifest.gates != expected_gates && manifest.gates != legacy_gates {
        return Err(HostError::validation(
            "gates",
            "manifest gates do not match phase invariants",
        ));
    }
    manifest.notifications.validate()?;
    Ok(())
}

fn verify_manifest_against_approval(
    manifest: &HostSessionManifest,
    record: &ApprovalRecord,
    manifest_path: &Path,
) -> Result<(), HostError> {
    let corrupt = |reason: String| HostError::CorruptManifest {
        path: manifest_path.display().to_string(),
        reason,
    };

    if manifest.schema_version != HOST_MANIFEST_SCHEMA_VERSION {
        return Err(corrupt(format!(
            "unsupported schema_version {} (expected {})",
            manifest.schema_version, HOST_MANIFEST_SCHEMA_VERSION
        )));
    }
    if manifest.approval_id != record.approval_id {
        return Err(corrupt(format!(
            "manifest approval_id `{}` does not match expected `{}`",
            manifest.approval_id, record.approval_id
        )));
    }
    if manifest.subject_digest != record.subject_digest {
        return Err(corrupt(format!(
            "manifest subject_digest `{}` does not match approval `{}`",
            manifest.subject_digest, record.subject_digest
        )));
    }
    if manifest.policy_version != record.policy_version {
        return Err(corrupt(format!(
            "manifest policy_version {} does not match approval {}",
            manifest.policy_version, record.policy_version
        )));
    }
    if manifest.control_binding_id != record.binding_id {
        return Err(corrupt(format!(
            "manifest control_binding_id `{}` does not match approval `{}`",
            manifest.control_binding_id, record.binding_id
        )));
    }
    if manifest.thread_uuid != record.thread_uuid {
        return Err(corrupt(format!(
            "manifest thread_uuid `{}` does not match approval `{}`",
            manifest.thread_uuid, record.thread_uuid
        )));
    }
    if manifest.plan_id != record.plan_id || manifest.plan_revision != record.plan_revision {
        return Err(corrupt(format!(
            "manifest plan `{}` rev {} does not match approval `{}` rev {}",
            manifest.plan_id, manifest.plan_revision, record.plan_id, record.plan_revision
        )));
    }
    if let Some(ref sb) = manifest.session_binding_id {
        if validate_identifier("session_binding_id", sb).is_err() {
            return Err(corrupt(format!(
                "manifest session_binding_id `{sb}` is invalid"
            )));
        }
    }
    if let Some(ref inst) = manifest.instance_id {
        if validate_host_instance_id("instance_id", inst).is_err() {
            return Err(corrupt(format!("manifest instance_id `{inst}` is invalid")));
        }
    }
    if matches!(manifest.phase, HostPhase::Ready | HostPhase::Stopping)
        && manifest.session_binding_id.is_none()
    {
        return Err(corrupt(format!(
            "manifest is in `{}` phase without `session_binding_id`",
            manifest.phase
        )));
    }

    let expected_reference = HostSessionManifest::from_approval_record(
        record,
        manifest.session_binding_id.clone(),
        manifest.phase,
        manifest.pid,
        manifest.last_error.clone(),
        manifest.created_at_ms,
        manifest.updated_at_ms,
    );
    if manifest.worker_profile_count != expected_reference.worker_profile_count
        || manifest.native_route_count != expected_reference.native_route_count
        || manifest.root_worker_task_ids != expected_reference.root_worker_task_ids
        || manifest.dependent_task_ids != expected_reference.dependent_task_ids
        || manifest.native_route_task_ids != expected_reference.native_route_task_ids
    {
        return Err(corrupt(
            "manifest task classification counts or task ID lists do not match approval record"
                .to_string(),
        ));
    }
    let ready = manifest.phase == HostPhase::Ready;
    let legacy_gates = HostExecutionGates::legacy_for_ready_state(ready);
    if manifest.gates != expected_reference.gates && manifest.gates != legacy_gates {
        return Err(corrupt(
            "manifest execution gates do not match required phase invariants".to_string(),
        ));
    }
    manifest.notifications.validate().map_err(|err| {
        corrupt(format!(
            "manifest notifications summary is inconsistent: {err}"
        ))
    })?;
    Ok(())
}

/// Reads the persisted [`TaskDispatchIntent`] for `task_id` from `<session_dir>/task-receipts.json`, if present.
///
/// Validates the entire receipts file against `record` and `session_binding_id` under `task-receipts.lock`.
/// Corrupt or tampered receipt files are preserved untouched and return [`HostError::CorruptReceipts`].
pub fn read_task_dispatch_intent(
    session_dir: &Path,
    record: &ApprovalRecord,
    session_binding_id: &str,
    task_id: &str,
) -> Result<Option<TaskDispatchIntent>, HostError> {
    let (intent, _) = read_task_ledger_entry(session_dir, record, session_binding_id, task_id)?;
    Ok(intent)
}

/// Reads the sealed [`TaskRunReceipt`] for `task_id` from `<session_dir>/task-receipts.json`, if present.
///
/// Validates the entire receipts file against `record` and `session_binding_id` under `task-receipts.lock`.
/// Corrupt or tampered receipt files are preserved untouched and return [`HostError::CorruptReceipts`].
pub fn read_task_receipt(
    session_dir: &Path,
    record: &ApprovalRecord,
    session_binding_id: &str,
    task_id: &str,
) -> Result<Option<TaskRunReceipt>, HostError> {
    let (_, receipt) = read_task_ledger_entry(session_dir, record, session_binding_id, task_id)?;
    Ok(receipt)
}

/// Persisted pending dispatch intents and sealed task run receipts.
pub type ReadonlyTaskLedger = (Vec<TaskDispatchIntent>, Vec<TaskRunReceipt>);

/// Internal profile-validation policy for reading a session task ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskLedgerReadPolicy {
    StrictLive,
    RecordedHistoryReadonly,
}

/// Reads the persisted [`TaskDispatchIntent`] and [`TaskRunReceipt`] lists from `<session_dir>/task-receipts.json`
/// in strictly read-only mode without opening or creating any lock file.
pub fn read_session_task_ledger_readonly(
    session_dir: &Path,
    record: &ApprovalRecord,
    session_binding_id: &str,
) -> Result<Option<ReadonlyTaskLedger>, HostError> {
    read_task_receipts_locked(session_dir, record, session_binding_id)
        .map(|opt| opt.map(|ledger| (ledger.pending_intents, ledger.receipts)))
}

/// Reads the persisted [`TaskDispatchIntent`] and [`TaskRunReceipt`] lists from `<session_dir>/task-receipts.json`
/// in read-only recorded-history mode without opening or creating any lock file or requiring
/// the historical provider executable to remain on disk.
///
/// Never use this historical reader for runtime admission or live execution authority.
pub fn read_session_task_ledger_recorded_history_readonly(
    session_dir: &Path,
    record: &ApprovalRecord,
    session_binding_id: &str,
) -> Result<Option<ReadonlyTaskLedger>, HostError> {
    read_task_receipts_locked_with_policy(
        session_dir,
        record,
        session_binding_id,
        TaskLedgerReadPolicy::RecordedHistoryReadonly,
    )
    .map(|opt| opt.map(|ledger| (ledger.pending_intents, ledger.receipts)))
}

/// Verifies `<session_dir>/task-receipts.json` (if present) against `record` and `session_binding_id`
/// without creating a missing lock file on disk.
pub(crate) fn verify_existing_task_receipts(
    session_dir: &Path,
    record: &ApprovalRecord,
    session_binding_id: &str,
) -> Result<Option<TaskReceiptsFile>, HostError> {
    let receipts_path = session_dir.join(TASK_RECEIPTS_FILENAME);
    let checked = ensure_no_symlink_components(&receipts_path)?;
    match fs::symlink_metadata(&checked) {
        Ok(meta) => validate_private_regular_metadata(&checked, &meta)?,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let lock_path = session_dir.join(TASK_RECEIPTS_LOCK_FILENAME);
            let checked_lock = ensure_no_symlink_components(&lock_path)?;
            if let Ok(lock_meta) = fs::symlink_metadata(&checked_lock) {
                validate_private_regular_metadata(&checked_lock, &lock_meta)?;
            }
            return Ok(None);
        }
        Err(err) => {
            return Err(HostError::Io {
                operation: "symlink_metadata(task-receipts.json)".to_string(),
                reason: err.to_string(),
            });
        }
    }

    if let Some(lock_file) =
        open_existing_private_lock_file(&session_dir.join(TASK_RECEIPTS_LOCK_FILENAME))?
    {
        lock_file.lock_exclusive().map_err(|err| HostError::Io {
            operation: "lock_exclusive(task-receipts.lock)".to_string(),
            reason: err.to_string(),
        })?;
        let res = read_task_receipts_locked(session_dir, record, session_binding_id);
        let _ = lock_file.unlock();
        res
    } else {
        read_task_receipts_locked(session_dir, record, session_binding_id)
    }
}

pub(crate) fn read_task_ledger_entry(
    session_dir: &Path,
    record: &ApprovalRecord,
    session_binding_id: &str,
    task_id: &str,
) -> Result<(Option<TaskDispatchIntent>, Option<TaskRunReceipt>), HostError> {
    let receipts_path = session_dir.join(TASK_RECEIPTS_FILENAME);
    let checked_receipts = ensure_no_symlink_components(&receipts_path)?;
    let lock_path = session_dir.join(TASK_RECEIPTS_LOCK_FILENAME);
    let checked_lock = ensure_no_symlink_components(&lock_path)?;
    match fs::symlink_metadata(&checked_receipts) {
        Ok(meta) => validate_private_regular_metadata(&checked_receipts, &meta)?,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if let Ok(lock_meta) = fs::symlink_metadata(&checked_lock) {
                validate_private_regular_metadata(&checked_lock, &lock_meta)?;
            }
            return Ok((None, None));
        }
        Err(err) => {
            return Err(HostError::Io {
                operation: "symlink_metadata(task-receipts.json)".to_string(),
                reason: err.to_string(),
            });
        }
    }

    let lock_guard = open_existing_private_lock_file(&checked_lock)?;
    if let Some(ref lock_file) = lock_guard {
        lock_file.lock_exclusive().map_err(|err| HostError::Io {
            operation: "lock_exclusive(task-receipts.lock)".to_string(),
            reason: err.to_string(),
        })?;
    }
    let res =
        read_task_receipts_locked(session_dir, record, session_binding_id).map(|opt| match opt {
            Some(ledger) => {
                let intent = ledger
                    .pending_intents
                    .into_iter()
                    .find(|entry| entry.task_id == task_id);
                let receipt = ledger
                    .receipts
                    .into_iter()
                    .find(|entry| entry.task_id == task_id);
                (intent, receipt)
            }
            None => (None, None),
        });
    if let Some(lock_file) = lock_guard {
        let _ = lock_file.unlock();
    }
    res
}

/// Read-only preflight check for whether an existing Ready host session has a validated
/// prepared dependency input seal or existing ledger entry for `task_id`.
pub fn has_prepared_dependency_input_or_ledger_entry(
    sessions_root: &Path,
    record: &ApprovalRecord,
    task_id: &str,
) -> Result<bool, HostError> {
    let candidate = sessions_root.join(&record.approval_id);
    match fs::symlink_metadata(&candidate) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(HostError::Io {
                operation: "symlink_metadata(session_dir)".to_string(),
                reason: err.to_string(),
            });
        }
        Ok(_) => {}
    }

    let session_dir = verify_existing_session_dir(sessions_root, &record.approval_id)?;
    let Some(manifest) = read_host_manifest_optional(&session_dir, record)? else {
        return Ok(false);
    };
    if manifest.phase != HostPhase::Ready
        || !manifest.gates.supervisor_authenticated_ready
        || manifest.gates.is_legacy_root_only()
        || !is_host_lock_held(&session_dir)?
    {
        return Ok(false);
    }
    let Some(ref session_binding_id) = manifest.session_binding_id else {
        return Ok(false);
    };

    let (intent, receipt) =
        read_task_ledger_entry(&session_dir, record, session_binding_id, task_id)?;
    if intent.is_some() || receipt.is_some() {
        return Ok(true);
    }

    if verify_all_transitive_predecessors_in_approval(record, task_id).is_err() {
        return Ok(false);
    }
    let Some(task) = record.tasks.iter().find(|t| t.task_id == task_id) else {
        return Ok(false);
    };
    let work_area_dir = session_dir.join(WORK_AREA_SUBDIR);
    let Ok(profile) =
        build_validated_task_profile(record, session_binding_id, &work_area_dir, task)
    else {
        return Ok(false);
    };
    let Some(ref policy) = profile.dependency_input else {
        return Ok(false);
    };

    match threadlane_operations::inspect_dependency_input_seal_readonly(
        policy,
        &record.repository_path,
        &work_area_dir,
        &record.base_commit,
        &task.instruction,
        &policy.command_key,
    ) {
        Ok(Some(seal)) => {
            let expected_preds: BTreeSet<&str> =
                task.depends_on.iter().map(String::as_str).collect();
            let actual_preds: BTreeSet<&str> = seal
                .predecessors
                .iter()
                .map(|p| p.task_id.as_str())
                .collect();
            Ok(expected_preds == actual_preds && seal.predecessors.len() == task.depends_on.len())
        }
        Ok(None) => Ok(false),
        Err(threadlane_operations::OperationsError::SecurityViolation(reason)) => {
            Err(HostError::SecurityViolation {
                path: work_area_dir.display().to_string(),
                reason,
            })
        }
        Err(_) => Ok(false),
    }
}

/// Atomically persists a validated [`TaskDispatchIntent`] inside `<session_dir>/task-receipts.json`
/// before invoking the runtime `start_worker` effect.
///
/// If an intent for `intent.task_id` already exists with identical frozen fields, returns it
/// idempotently; if an existing intent or receipt conflicts, fails closed with
/// [`HostError::CorruptReceipts`].
pub fn record_task_dispatch_intent(
    session_dir: &Path,
    record: &ApprovalRecord,
    session_binding_id: &str,
    intent: TaskDispatchIntent,
) -> Result<TaskDispatchIntent, HostError> {
    let receipts_path = session_dir.join(TASK_RECEIPTS_FILENAME);
    if validate_identifier("session_binding_id", session_binding_id).is_err() {
        return Err(HostError::CorruptReceipts {
            path: receipts_path.display().to_string(),
            reason: format!("invalid session_binding_id `{session_binding_id}`"),
        });
    }
    validate_single_intent(&intent, record, session_binding_id, &receipts_path)?;

    let lock_file = open_private_lock_file(&session_dir.join(TASK_RECEIPTS_LOCK_FILENAME))?;
    lock_file.lock_exclusive().map_err(|err| HostError::Io {
        operation: "lock_exclusive(task-receipts.lock)".to_string(),
        reason: err.to_string(),
    })?;

    let res = (|| -> Result<TaskDispatchIntent, HostError> {
        let mut ledger = match read_task_receipts_locked(session_dir, record, session_binding_id)? {
            Some(existing) => existing,
            None => TaskReceiptsFile {
                schema_version: TASK_RECEIPTS_SCHEMA_VERSION,
                approval_id: record.approval_id.clone(),
                subject_digest: record.subject_digest.clone(),
                session_binding_id: session_binding_id.to_string(),
                pending_intents: Vec::new(),
                receipts: Vec::new(),
            },
        };

        if let Some(existing_receipt) = ledger.receipts.iter().find(|r| r.task_id == intent.task_id)
        {
            if existing_receipt.task_digest != intent.task_digest
                || existing_receipt.profile_id != intent.profile_id
                || existing_receipt.command_key != intent.command_key
                || existing_receipt.execution_binding != intent.execution_binding
            {
                return Err(HostError::CorruptReceipts {
                    path: receipts_path.display().to_string(),
                    reason: format!(
                        "conflicting task receipt for task `{}` when recording dispatch intent",
                        intent.task_id
                    ),
                });
            }
        }

        if let Some(existing) = ledger
            .pending_intents
            .iter()
            .find(|i| i.task_id == intent.task_id)
        {
            if existing.task_digest != intent.task_digest
                || existing.profile_id != intent.profile_id
                || existing.command_key != intent.command_key
                || existing.execution_binding != intent.execution_binding
            {
                return Err(HostError::CorruptReceipts {
                    path: receipts_path.display().to_string(),
                    reason: format!("conflicting dispatch intent for task `{}`", intent.task_id),
                });
            }
            return Ok(existing.clone());
        }

        ledger.pending_intents.push(intent.clone());
        ledger
            .pending_intents
            .sort_by(|left, right| left.task_id.cmp(&right.task_id));

        write_task_receipts_locked(&receipts_path, &ledger)?;
        Ok(intent)
    })();

    let _ = lock_file.unlock();
    res
}

/// Atomically persists a sealed [`TaskRunReceipt`] inside `<session_dir>/task-receipts.json`.
///
/// If a receipt for `receipt.task_id` already exists with identical mapping fields, returns it
/// idempotently; if an existing receipt conflicts, fails closed with [`HostError::CorruptReceipts`].
/// Retains the corresponding validated [`TaskDispatchIntent`] in `pending_intents`.
pub fn record_task_receipt(
    session_dir: &Path,
    record: &ApprovalRecord,
    session_binding_id: &str,
    receipt: TaskRunReceipt,
) -> Result<TaskRunReceipt, HostError> {
    let receipts_path = session_dir.join(TASK_RECEIPTS_FILENAME);
    if validate_identifier("session_binding_id", session_binding_id).is_err() {
        return Err(HostError::CorruptReceipts {
            path: receipts_path.display().to_string(),
            reason: format!("invalid session_binding_id `{session_binding_id}`"),
        });
    }
    validate_single_receipt(&receipt, record, session_binding_id, &receipts_path)?;

    let lock_file = open_private_lock_file(&session_dir.join(TASK_RECEIPTS_LOCK_FILENAME))?;
    lock_file.lock_exclusive().map_err(|err| HostError::Io {
        operation: "lock_exclusive(task-receipts.lock)".to_string(),
        reason: err.to_string(),
    })?;

    let res = (|| -> Result<TaskRunReceipt, HostError> {
        let mut ledger = match read_task_receipts_locked(session_dir, record, session_binding_id)? {
            Some(existing) => existing,
            None => TaskReceiptsFile {
                schema_version: TASK_RECEIPTS_SCHEMA_VERSION,
                approval_id: record.approval_id.clone(),
                subject_digest: record.subject_digest.clone(),
                session_binding_id: session_binding_id.to_string(),
                pending_intents: Vec::new(),
                receipts: Vec::new(),
            },
        };

        let mut added_intent = false;
        if let Some(existing_intent) = ledger
            .pending_intents
            .iter()
            .find(|i| i.task_id == receipt.task_id)
        {
            if existing_intent.task_digest != receipt.task_digest
                || existing_intent.profile_id != receipt.profile_id
                || existing_intent.command_key != receipt.command_key
                || existing_intent.execution_binding != receipt.execution_binding
            {
                return Err(HostError::CorruptReceipts {
                    path: receipts_path.display().to_string(),
                    reason: format!(
                        "conflicting dispatch intent for task `{}` when sealing receipt",
                        receipt.task_id
                    ),
                });
            }
        } else {
            ledger.pending_intents.push(TaskDispatchIntent {
                task_id: receipt.task_id.clone(),
                task_digest: receipt.task_digest.clone(),
                profile_id: receipt.profile_id.clone(),
                command_key: receipt.command_key.clone(),
                execution_binding: receipt.execution_binding.clone(),
                recorded_at_ms: receipt.recorded_at_ms,
            });
            ledger
                .pending_intents
                .sort_by(|left, right| left.task_id.cmp(&right.task_id));
            added_intent = true;
        }

        if let Some(existing) = ledger
            .receipts
            .iter()
            .find(|r| r.task_id == receipt.task_id)
        {
            if existing.task_digest != receipt.task_digest
                || existing.profile_id != receipt.profile_id
                || existing.command_key != receipt.command_key
                || existing.run_id != receipt.run_id
                || existing.worker_id != receipt.worker_id
                || existing.execution_binding != receipt.execution_binding
            {
                return Err(HostError::CorruptReceipts {
                    path: receipts_path.display().to_string(),
                    reason: format!(
                        "conflicting task receipt for task `{}`: existing run_id `{}` vs new `{}`",
                        receipt.task_id, existing.run_id, receipt.run_id
                    ),
                });
            }
            let existing = existing.clone();
            if added_intent {
                write_task_receipts_locked(&receipts_path, &ledger)?;
            }
            return Ok(existing);
        }

        ledger.receipts.push(receipt.clone());
        ledger
            .receipts
            .sort_by(|left, right| left.task_id.cmp(&right.task_id));

        write_task_receipts_locked(&receipts_path, &ledger)?;
        Ok(receipt)
    })();

    let _ = lock_file.unlock();
    res
}

fn write_task_receipts_locked(
    receipts_path: &Path,
    ledger: &TaskReceiptsFile,
) -> Result<(), HostError> {
    let mut bytes =
        serde_json::to_vec_pretty(ledger).map_err(|err| HostError::CorruptReceipts {
            path: receipts_path.display().to_string(),
            reason: format!("failed to serialize task-receipts.json: {err}"),
        })?;
    bytes.push(b'\n');
    if bytes.len() > MAX_TASK_RECEIPTS_BYTES {
        return Err(HostError::CorruptReceipts {
            path: receipts_path.display().to_string(),
            reason: format!(
                "serialized task-receipts.json ({} bytes) exceeds maximum {} bytes",
                bytes.len(),
                MAX_TASK_RECEIPTS_BYTES
            ),
        });
    }

    atomic_write_private_file(receipts_path, &bytes, MAX_TASK_RECEIPTS_BYTES)
}

fn read_task_receipts_locked(
    session_dir: &Path,
    record: &ApprovalRecord,
    session_binding_id: &str,
) -> Result<Option<TaskReceiptsFile>, HostError> {
    read_task_receipts_locked_with_policy(
        session_dir,
        record,
        session_binding_id,
        TaskLedgerReadPolicy::StrictLive,
    )
}

fn read_task_receipts_locked_with_policy(
    session_dir: &Path,
    record: &ApprovalRecord,
    session_binding_id: &str,
    policy: TaskLedgerReadPolicy,
) -> Result<Option<TaskReceiptsFile>, HostError> {
    let receipts_path = session_dir.join(TASK_RECEIPTS_FILENAME);
    let checked = ensure_no_symlink_components(&receipts_path)?;
    let meta = match fs::symlink_metadata(&checked) {
        Ok(m) => m,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(HostError::Io {
                operation: "symlink_metadata(task-receipts.json)".to_string(),
                reason: err.to_string(),
            })
        }
    };
    validate_private_regular_metadata(&checked, &meta)?;
    if meta.len() > MAX_TASK_RECEIPTS_BYTES as u64 {
        return Err(HostError::CorruptReceipts {
            path: checked.display().to_string(),
            reason: format!(
                "task-receipts.json size ({} bytes) exceeds maximum {} bytes",
                meta.len(),
                MAX_TASK_RECEIPTS_BYTES
            ),
        });
    }

    let file = File::open(&checked).map_err(|err| HostError::Io {
        operation: "open(task-receipts.json)".to_string(),
        reason: err.to_string(),
    })?;
    let mut bytes = Vec::with_capacity(meta.len() as usize);
    file.take(MAX_TASK_RECEIPTS_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|err| HostError::Io {
            operation: "read(task-receipts.json)".to_string(),
            reason: err.to_string(),
        })?;
    if bytes.len() > MAX_TASK_RECEIPTS_BYTES {
        return Err(HostError::CorruptReceipts {
            path: checked.display().to_string(),
            reason: format!(
                "task-receipts.json content exceeds maximum {} bytes",
                MAX_TASK_RECEIPTS_BYTES
            ),
        });
    }

    let ledger: TaskReceiptsFile =
        serde_json::from_slice(&bytes).map_err(|err| HostError::CorruptReceipts {
            path: checked.display().to_string(),
            reason: format!("invalid task-receipts.json JSON: {err}"),
        })?;

    if policy == TaskLedgerReadPolicy::StrictLive {
        verify_task_receipts_ledger(&ledger, record, session_binding_id, &checked)?;
    } else {
        verify_task_receipts_ledger_with_policy(
            &ledger,
            record,
            session_binding_id,
            &checked,
            policy,
        )?;
    }
    Ok(Some(ledger))
}

fn verify_task_receipts_ledger(
    ledger: &TaskReceiptsFile,
    record: &ApprovalRecord,
    session_binding_id: &str,
    receipts_path: &Path,
) -> Result<(), HostError> {
    verify_task_receipts_ledger_with_policy(
        ledger,
        record,
        session_binding_id,
        receipts_path,
        TaskLedgerReadPolicy::StrictLive,
    )
}

fn verify_task_receipts_ledger_with_policy(
    ledger: &TaskReceiptsFile,
    record: &ApprovalRecord,
    session_binding_id: &str,
    receipts_path: &Path,
    policy: TaskLedgerReadPolicy,
) -> Result<(), HostError> {
    let corrupt = |reason: String| HostError::CorruptReceipts {
        path: receipts_path.display().to_string(),
        reason,
    };

    if ledger.schema_version != TASK_RECEIPTS_SCHEMA_VERSION {
        return Err(corrupt(format!(
            "unsupported schema_version {} (expected {})",
            ledger.schema_version, TASK_RECEIPTS_SCHEMA_VERSION
        )));
    }
    if ledger.approval_id != record.approval_id {
        return Err(corrupt(format!(
            "receipts approval_id `{}` does not match expected `{}`",
            ledger.approval_id, record.approval_id
        )));
    }
    if ledger.subject_digest != record.subject_digest {
        return Err(corrupt(format!(
            "receipts subject_digest `{}` does not match expected `{}`",
            ledger.subject_digest, record.subject_digest
        )));
    }
    if validate_identifier("session_binding_id", &ledger.session_binding_id).is_err()
        || ledger.session_binding_id != session_binding_id
    {
        return Err(corrupt(format!(
            "receipts session_binding_id `{}` does not match expected `{}`",
            ledger.session_binding_id, session_binding_id
        )));
    }

    let mut seen_intent_tasks = BTreeSet::new();
    for intent in &ledger.pending_intents {
        if !seen_intent_tasks.insert(intent.task_id.clone()) {
            return Err(corrupt(format!(
                "duplicate dispatch intent entry for task `{}`",
                intent.task_id
            )));
        }
        if policy == TaskLedgerReadPolicy::StrictLive {
            validate_single_intent(intent, record, session_binding_id, receipts_path)?;
        } else {
            validate_single_intent_with_policy(
                intent,
                record,
                session_binding_id,
                receipts_path,
                policy,
            )?;
        }
    }

    let mut seen_tasks = BTreeSet::new();
    let mut seen_runs = BTreeSet::new();
    let mut seen_workers = BTreeSet::new();
    for receipt in &ledger.receipts {
        if !seen_tasks.insert(receipt.task_id.clone()) {
            return Err(corrupt(format!(
                "duplicate receipt entry for task `{}`",
                receipt.task_id
            )));
        }
        if !seen_runs.insert(receipt.run_id.clone()) {
            return Err(corrupt(format!(
                "duplicate run_id `{}` across receipt entries",
                receipt.run_id
            )));
        }
        if !seen_workers.insert(receipt.worker_id.clone()) {
            return Err(corrupt(format!(
                "duplicate worker_id `{}` across receipt entries",
                receipt.worker_id
            )));
        }
        if policy == TaskLedgerReadPolicy::StrictLive {
            validate_single_receipt(receipt, record, session_binding_id, receipts_path)?;
        } else {
            validate_single_receipt_with_policy(
                receipt,
                record,
                session_binding_id,
                receipts_path,
                policy,
            )?;
        }
        if let Some(intent) = ledger
            .pending_intents
            .iter()
            .find(|i| i.task_id == receipt.task_id)
        {
            if intent.task_digest != receipt.task_digest
                || intent.profile_id != receipt.profile_id
                || intent.command_key != receipt.command_key
                || intent.execution_binding != receipt.execution_binding
            {
                return Err(corrupt(format!(
                    "dispatch intent and receipt for task `{}` do not match",
                    receipt.task_id
                )));
            }
        }
    }

    Ok(())
}

/// Verifies that every direct and transitive predecessor of `task_id` is included in `record.tasks`
/// and is a headless worker task (`provider.is_some()` and `adapter != ToolAdapter::CodexCli`),
/// returning the sorted list of all transitive predecessor task IDs.
pub(crate) fn verify_all_transitive_predecessors_in_approval(
    record: &ApprovalRecord,
    task_id: &str,
) -> Result<Vec<String>, String> {
    let tasks_by_id: BTreeMap<&str, &ApprovedTaskRecord> = record
        .tasks
        .iter()
        .map(|t| (t.task_id.as_str(), t))
        .collect();
    let Some(root_task) = tasks_by_id.get(task_id) else {
        return Err(format!("unknown task `{task_id}`"));
    };

    let mut visited = BTreeSet::new();
    let mut visiting = BTreeSet::new();
    fn visit<'a>(
        current_id: &'a str,
        tasks_by_id: &BTreeMap<&'a str, &'a ApprovedTaskRecord>,
        visiting: &mut BTreeSet<&'a str>,
        visited: &mut BTreeSet<String>,
    ) -> Result<(), String> {
        if !visiting.insert(current_id) {
            return Err(format!(
                "dependency cycle detected involving task `{current_id}`"
            ));
        }
        let Some(task) = tasks_by_id.get(current_id) else {
            return Err(format!(
                "predecessor task `{current_id}` is missing from frozen approval record"
            ));
        };
        for dep_id in &task.depends_on {
            let Some(dep_task) = tasks_by_id.get(dep_id.as_str()) else {
                return Err(format!(
                    "task `{current_id}` depends on task `{dep_id}` which is missing from frozen approval record"
                ));
            };
            if !dep_task.is_frozen_headless_worker() {
                return Err(format!(
                    "predecessor task `{dep_id}` uses manual native route `{}` and cannot produce worker dependency receipts",
                    dep_task.adapter
                ));
            }
            if !visited.contains(dep_id.as_str()) {
                visit(dep_id.as_str(), tasks_by_id, visiting, visited)?;
                visited.insert(dep_id.clone());
            }
        }
        visiting.remove(current_id);
        Ok(())
    }

    visit(
        root_task.task_id.as_str(),
        &tasks_by_id,
        &mut visiting,
        &mut visited,
    )?;
    Ok(visited.into_iter().collect())
}

pub(crate) fn build_validated_task_profile(
    record: &ApprovalRecord,
    session_binding_id: &str,
    work_area_root: &Path,
    task: &ApprovedTaskRecord,
) -> Result<ValidatedWorkerProfile, String> {
    let profile_cfg = build_approved_worker_profile_config(
        record,
        task,
        session_binding_id,
        work_area_root,
        work_area_root,
    )
    .map_err(|err| err.to_string())?;
    let profile_id = profile_cfg.profile_id.clone();

    let cfg = WorkersConfig {
        max_parallel: 1,
        profiles: vec![profile_cfg],
    };
    let mut validated = cfg.validate().map_err(|err| err.to_string())?;
    validated
        .profiles
        .remove(&profile_id)
        .ok_or_else(|| format!("validated profile `{profile_id}` not found"))
}

fn build_recorded_history_task_profile_digest(
    record: &ApprovalRecord,
    session_binding_id: &str,
    work_area_root: &Path,
    task: &ApprovedTaskRecord,
) -> Result<String, String> {
    compute_recorded_history_worker_profile_digest(
        record,
        task,
        session_binding_id,
        work_area_root,
        work_area_root,
    )
    .map_err(|err| err.to_string())
}

fn validate_task_execution_binding(
    task: &ApprovedTaskRecord,
    execution_binding: &Option<WorkerExecutionBinding>,
    record: &ApprovalRecord,
    session_binding_id: &str,
    receipts_path: &Path,
) -> Result<(), HostError> {
    validate_task_execution_binding_with_policy(
        task,
        execution_binding,
        record,
        session_binding_id,
        receipts_path,
        TaskLedgerReadPolicy::StrictLive,
    )
}

fn validate_task_execution_binding_with_policy(
    task: &ApprovedTaskRecord,
    execution_binding: &Option<WorkerExecutionBinding>,
    record: &ApprovalRecord,
    session_binding_id: &str,
    receipts_path: &Path,
    policy: TaskLedgerReadPolicy,
) -> Result<(), HostError> {
    let corrupt = |reason: String| HostError::CorruptReceipts {
        path: receipts_path.display().to_string(),
        reason,
    };

    if task.depends_on.is_empty() {
        if execution_binding.is_some() {
            return Err(corrupt(format!(
                "root task `{}` must not carry an execution_binding",
                task.task_id
            )));
        }
        return Ok(());
    }

    verify_all_transitive_predecessors_in_approval(record, &task.task_id).map_err(|reason| {
        corrupt(format!(
            "dependent task `{}` fails approval predecessor closure check: {reason}",
            task.task_id
        ))
    })?;

    let Some(ref binding) = execution_binding else {
        return Err(corrupt(format!(
            "dependent task `{}` is missing required execution_binding",
            task.task_id
        )));
    };

    let session_dir = receipts_path
        .parent()
        .ok_or_else(|| corrupt("task-receipts.json path has no session_dir parent".to_string()))?;
    let work_area_root = session_dir.join(WORK_AREA_SUBDIR);
    let expected_profile_digest = match policy {
        TaskLedgerReadPolicy::StrictLive => {
            build_validated_task_profile(record, session_binding_id, &work_area_root, task)
                .map(|profile| profile.profile_digest)
        }
        TaskLedgerReadPolicy::RecordedHistoryReadonly => {
            build_recorded_history_task_profile_digest(
                record,
                session_binding_id,
                &work_area_root,
                task,
            )
        }
    }
    .map_err(|reason| {
        corrupt(format!(
            "failed to validate frozen profile for dependent task `{}`: {reason}",
            task.task_id
        ))
    })?;

    let expected_binding = WorkerExecutionBinding::new(
        &expected_profile_digest,
        &task.instruction,
        &binding.input_digest,
        &binding.seal_file_digest,
        &binding.execution_base_commit,
    )
    .map_err(|err| {
        corrupt(format!(
            "dependent task `{}` has structurally invalid execution_binding: {err}",
            task.task_id
        ))
    })?;

    if &expected_binding != binding {
        return Err(corrupt(format!(
            "dependent task `{}` execution_binding digest `{}` does not match recomputed `{}`",
            task.task_id, binding.execution_digest, expected_binding.execution_digest
        )));
    }

    binding
        .verify_against(
            &expected_profile_digest,
            &task.instruction,
            &binding.execution_base_commit,
        )
        .map_err(|err| {
            corrupt(format!(
                "dependent task `{}` execution_binding does not match frozen profile or instruction digest: {err}",
                task.task_id
            ))
        })?;

    Ok(())
}

fn validate_single_intent(
    intent: &TaskDispatchIntent,
    record: &ApprovalRecord,
    session_binding_id: &str,
    receipts_path: &Path,
) -> Result<(), HostError> {
    validate_single_intent_with_policy(
        intent,
        record,
        session_binding_id,
        receipts_path,
        TaskLedgerReadPolicy::StrictLive,
    )
}

fn validate_single_intent_with_policy(
    intent: &TaskDispatchIntent,
    record: &ApprovalRecord,
    session_binding_id: &str,
    receipts_path: &Path,
    policy: TaskLedgerReadPolicy,
) -> Result<(), HostError> {
    let corrupt = |reason: String| HostError::CorruptReceipts {
        path: receipts_path.display().to_string(),
        reason,
    };

    let Some(task) = record.tasks.iter().find(|t| t.task_id == intent.task_id) else {
        return Err(corrupt(format!(
            "dispatch intent references unknown task_id `{}`",
            intent.task_id
        )));
    };
    if !task.is_frozen_headless_worker() {
        return Err(corrupt(format!(
            "dispatch intent references manual native route task `{}`",
            intent.task_id
        )));
    }
    let (Some(expected_profile), Some(expected_command_key)) =
        (&task.profile_id, &task.command_key)
    else {
        return Err(corrupt(format!(
            "dispatch intent references manual native route task `{}`",
            intent.task_id
        )));
    };

    if validate_hex_digest("task_digest", &intent.task_digest).is_err()
        || intent.task_digest != task.task_digest
    {
        return Err(corrupt(format!(
            "dispatch intent task_digest `{}` for task `{}` does not match frozen `{}`",
            intent.task_digest, intent.task_id, task.task_digest
        )));
    }
    if intent.profile_id != *expected_profile {
        return Err(corrupt(format!(
            "dispatch intent profile_id `{}` for task `{}` does not match frozen `{expected_profile}`",
            intent.profile_id, intent.task_id
        )));
    }
    if intent.command_key != *expected_command_key {
        return Err(corrupt(format!(
            "dispatch intent command_key `{}` for task `{}` does not match frozen `{expected_command_key}`",
            intent.command_key, intent.task_id
        )));
    }
    if policy == TaskLedgerReadPolicy::StrictLive {
        validate_task_execution_binding(
            task,
            &intent.execution_binding,
            record,
            session_binding_id,
            receipts_path,
        )?;
    } else {
        validate_task_execution_binding_with_policy(
            task,
            &intent.execution_binding,
            record,
            session_binding_id,
            receipts_path,
            policy,
        )?;
    }

    Ok(())
}

fn validate_single_receipt(
    receipt: &TaskRunReceipt,
    record: &ApprovalRecord,
    session_binding_id: &str,
    receipts_path: &Path,
) -> Result<(), HostError> {
    validate_single_receipt_with_policy(
        receipt,
        record,
        session_binding_id,
        receipts_path,
        TaskLedgerReadPolicy::StrictLive,
    )
}

fn validate_single_receipt_with_policy(
    receipt: &TaskRunReceipt,
    record: &ApprovalRecord,
    session_binding_id: &str,
    receipts_path: &Path,
    policy: TaskLedgerReadPolicy,
) -> Result<(), HostError> {
    let corrupt = |reason: String| HostError::CorruptReceipts {
        path: receipts_path.display().to_string(),
        reason,
    };

    let Some(task) = record.tasks.iter().find(|t| t.task_id == receipt.task_id) else {
        return Err(corrupt(format!(
            "receipt references unknown task_id `{}`",
            receipt.task_id
        )));
    };
    if !task.is_frozen_headless_worker() {
        return Err(corrupt(format!(
            "receipt references manual native route task `{}`",
            receipt.task_id
        )));
    }
    let (Some(expected_profile), Some(expected_command_key)) =
        (&task.profile_id, &task.command_key)
    else {
        return Err(corrupt(format!(
            "receipt references manual native route task `{}`",
            receipt.task_id
        )));
    };

    if validate_hex_digest("task_digest", &receipt.task_digest).is_err()
        || receipt.task_digest != task.task_digest
    {
        return Err(corrupt(format!(
            "receipt task_digest `{}` for task `{}` does not match frozen `{}`",
            receipt.task_digest, receipt.task_id, task.task_digest
        )));
    }
    if receipt.profile_id != *expected_profile {
        return Err(corrupt(format!(
            "receipt profile_id `{}` for task `{}` does not match frozen `{expected_profile}`",
            receipt.profile_id, receipt.task_id
        )));
    }
    if receipt.command_key != *expected_command_key {
        return Err(corrupt(format!(
            "receipt command_key `{}` for task `{}` does not match frozen `{expected_command_key}`",
            receipt.command_key, receipt.task_id
        )));
    }
    if validate_identifier("run_id", &receipt.run_id).is_err()
        || !receipt.run_id.starts_with("run_")
    {
        return Err(corrupt(format!(
            "receipt for task `{}` has invalid run_id `{}`",
            receipt.task_id, receipt.run_id
        )));
    }
    if validate_identifier("worker_id", &receipt.worker_id).is_err()
        || !receipt.worker_id.starts_with("wrk_")
    {
        return Err(corrupt(format!(
            "receipt for task `{}` has invalid worker_id `{}`",
            receipt.task_id, receipt.worker_id
        )));
    }
    if policy == TaskLedgerReadPolicy::StrictLive {
        validate_task_execution_binding(
            task,
            &receipt.execution_binding,
            record,
            session_binding_id,
            receipts_path,
        )?;
    } else {
        validate_task_execution_binding_with_policy(
            task,
            &receipt.execution_binding,
            record,
            session_binding_id,
            receipts_path,
            policy,
        )?;
    }

    Ok(())
}

/// Exclusive cross-process file lock (`<sessions_root>/.launch-intents/<approval_id>.lock`)
/// serializing durable initial host launch intent reads/writes and pre-manifest process spawns.
#[derive(Debug)]
pub struct HostLaunchIntentLock {
    lock_path: PathBuf,
    file: File,
}

impl HostLaunchIntentLock {
    /// Acquires an exclusive lock on `<sessions_root>/.launch-intents/<approval_id>.lock`.
    pub fn acquire_blocking(sessions_root: &Path, approval_id: &str) -> Result<Self, HostError> {
        validate_approval_id(approval_id)?;
        let intents_dir = ensure_private_dir(&sessions_root.join(LAUNCH_INTENTS_SUBDIR))?;
        inspect_launch_intents_dir(&intents_dir)?;
        let lock_path = intents_dir.join(format!("{approval_id}.lock"));
        let file = open_private_lock_file(&lock_path)?;
        file.lock_exclusive().map_err(|err| HostError::Io {
            operation: "lock_exclusive(launch_intent.lock)".to_string(),
            reason: err.to_string(),
        })?;
        Ok(Self { lock_path, file })
    }

    /// Attempts to non-blockingly acquire an exclusive lock on
    /// `<sessions_root>/.launch-intents/<approval_id>.lock`.
    ///
    /// Returns `Ok(None)` when another process or handle currently holds the exclusive lock.
    pub fn try_acquire(sessions_root: &Path, approval_id: &str) -> Result<Option<Self>, HostError> {
        validate_approval_id(approval_id)?;
        let intents_dir = ensure_private_dir(&sessions_root.join(LAUNCH_INTENTS_SUBDIR))?;
        inspect_launch_intents_dir(&intents_dir)?;
        let lock_path = intents_dir.join(format!("{approval_id}.lock"));
        let file = open_private_lock_file(&lock_path)?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Some(Self { lock_path, file })),
            Err(_) => Ok(None),
        }
    }

    /// Returns the path to the underlying `<approval_id>.lock` file.
    pub fn path(&self) -> &Path {
        &self.lock_path
    }
}

impl Drop for HostLaunchIntentLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn inspect_launch_intents_dir(intents_dir: &Path) -> Result<(), HostError> {
    let entries = fs::read_dir(intents_dir).map_err(|err| HostError::Io {
        operation: "read_dir(launch_intents_dir)".to_string(),
        reason: err.to_string(),
    })?;

    for entry in entries {
        let entry = entry.map_err(|err| HostError::Io {
            operation: "read_dir_entry(launch_intents_dir)".to_string(),
            reason: err.to_string(),
        })?;
        let entry_path = entry.path();
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            return Err(HostError::SecurityViolation {
                path: entry_path.display().to_string(),
                reason: "non-UTF-8 entry in .launch-intents is not permitted".to_string(),
            });
        };

        if let Some(rest) = name.strip_prefix('.') {
            if let Some((target_json, suffix)) = rest.split_once(".tmp.") {
                if let Some(approval_id) = target_json.strip_suffix(".json") {
                    if validate_approval_id(approval_id).is_ok()
                        && is_valid_lowercase_uuid_v4_simple(suffix)
                    {
                        match fs::symlink_metadata(&entry_path) {
                            Ok(meta) => {
                                validate_private_regular_metadata(&entry_path, &meta)?;
                                if meta.len() > MAX_LAUNCH_INTENT_BYTES as u64 {
                                    return Err(HostError::SecurityViolation {
                                        path: entry_path.display().to_string(),
                                        reason: format!(
                                            "transient launch intent temp file `{name}` size ({} bytes) exceeds maximum {} bytes",
                                            meta.len(),
                                            MAX_LAUNCH_INTENT_BYTES
                                        ),
                                    });
                                }
                            }
                            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                                let committed_path = intents_dir.join(target_json);
                                if let Ok(committed_meta) = fs::symlink_metadata(&committed_path) {
                                    validate_private_regular_metadata(
                                        &committed_path,
                                        &committed_meta,
                                    )?;
                                    if committed_meta.len() > MAX_LAUNCH_INTENT_BYTES as u64 {
                                        return Err(HostError::SecurityViolation {
                                            path: committed_path.display().to_string(),
                                            reason: format!(
                                                "committed launch intent `{target_json}` size ({} bytes) exceeds maximum {} bytes",
                                                committed_meta.len(),
                                                MAX_LAUNCH_INTENT_BYTES
                                            ),
                                        });
                                    }
                                }
                            }
                            Err(err) => {
                                return Err(HostError::Io {
                                    operation: "symlink_metadata(launch_intent_tmp)".to_string(),
                                    reason: err.to_string(),
                                });
                            }
                        }
                        continue;
                    }
                }
            }
        }

        let meta = fs::symlink_metadata(&entry_path).map_err(|err| HostError::Io {
            operation: "symlink_metadata(launch_intent_entry)".to_string(),
            reason: err.to_string(),
        })?;
        validate_private_regular_metadata(&entry_path, &meta)?;

        if let Some(approval_id) = name.strip_suffix(".json") {
            if validate_approval_id(approval_id).is_err() {
                return Err(HostError::SecurityViolation {
                    path: entry_path.display().to_string(),
                    reason: format!(
                        "foreign or invalid launch intent file `{name}` in .launch-intents"
                    ),
                });
            }
            if meta.len() > MAX_LAUNCH_INTENT_BYTES as u64 {
                return Err(HostError::SecurityViolation {
                    path: entry_path.display().to_string(),
                    reason: format!(
                        "launch intent file `{name}` size ({} bytes) exceeds maximum {} bytes",
                        meta.len(),
                        MAX_LAUNCH_INTENT_BYTES
                    ),
                });
            }
        } else if let Some(approval_id) = name.strip_suffix(".lock") {
            if validate_approval_id(approval_id).is_err() {
                return Err(HostError::SecurityViolation {
                    path: entry_path.display().to_string(),
                    reason: format!(
                        "foreign or invalid launch intent lock `{name}` in .launch-intents"
                    ),
                });
            }
            if meta.len() != 0 {
                return Err(HostError::SecurityViolation {
                    path: entry_path.display().to_string(),
                    reason: format!(
                        "launch intent lock `{name}` must be a 0-byte lock file (found {} bytes)",
                        meta.len()
                    ),
                });
            }
        } else {
            return Err(HostError::SecurityViolation {
                path: entry_path.display().to_string(),
                reason: format!("foreign or unexpected entry `{name}` in .launch-intents"),
            });
        }
    }

    Ok(())
}

fn verify_host_launch_intent(
    intent: &HostLaunchIntent,
    record: &ApprovalRecord,
    intent_path: &Path,
) -> Result<(), HostError> {
    let corrupt = |reason: String| HostError::CorruptSessionState {
        reason: format!("{} ({})", reason, intent_path.display()),
    };

    if intent.schema_version != HOST_LAUNCH_INTENT_SCHEMA_VERSION {
        return Err(corrupt(format!(
            "unsupported launch intent schema_version {} (expected {})",
            intent.schema_version, HOST_LAUNCH_INTENT_SCHEMA_VERSION
        )));
    }
    if validate_approval_id(&intent.approval_id).is_err()
        || intent.approval_id != record.approval_id
    {
        return Err(corrupt(format!(
            "launch intent approval_id `{}` does not match expected `{}`",
            intent.approval_id, record.approval_id
        )));
    }
    if validate_hex_digest("subject_digest", &intent.subject_digest).is_err()
        || intent.subject_digest != record.subject_digest
    {
        return Err(corrupt(format!(
            "launch intent subject_digest `{}` does not match frozen `{}`",
            intent.subject_digest, record.subject_digest
        )));
    }
    if intent.policy_version != record.policy_version {
        return Err(corrupt(format!(
            "launch intent policy_version {} does not match frozen {}",
            intent.policy_version, record.policy_version
        )));
    }
    if validate_identifier("control_binding_id", &intent.control_binding_id).is_err()
        || intent.control_binding_id != record.binding_id
    {
        return Err(corrupt(format!(
            "launch intent control_binding_id `{}` does not match frozen `{}`",
            intent.control_binding_id, record.binding_id
        )));
    }
    if validate_canonical_thread_uuid("thread_uuid", &intent.thread_uuid).is_err()
        || intent.thread_uuid != record.thread_uuid
    {
        return Err(corrupt(format!(
            "launch intent thread_uuid `{}` does not match frozen `{}`",
            intent.thread_uuid, record.thread_uuid
        )));
    }
    if validate_identifier("plan_id", &intent.plan_id).is_err() || intent.plan_id != record.plan_id
    {
        return Err(corrupt(format!(
            "launch intent plan_id `{}` does not match frozen `{}`",
            intent.plan_id, record.plan_id
        )));
    }
    if intent.plan_revision == 0 || intent.plan_revision != record.plan_revision {
        return Err(corrupt(format!(
            "launch intent plan_revision {} does not match frozen {}",
            intent.plan_revision, record.plan_revision
        )));
    }
    if intent.executable_path.as_os_str().is_empty() || !intent.executable_path.is_absolute() {
        return Err(corrupt(
            "launch intent executable_path must be a non-empty absolute path".to_string(),
        ));
    }
    for comp in intent.executable_path.components() {
        if matches!(comp, Component::CurDir | Component::ParentDir) {
            return Err(corrupt(
                "launch intent executable_path must not contain `.` or `..` components".to_string(),
            ));
        }
    }
    if intent.spawn_pid == Some(0) {
        return Err(corrupt("launch intent spawn_pid must not be 0".to_string()));
    }
    if intent.created_at_ms <= 0 || intent.updated_at_ms < intent.created_at_ms {
        return Err(corrupt("launch intent timestamps are invalid".to_string()));
    }
    if let Some(ref err_msg) = intent.last_error {
        if err_msg.is_empty() || err_msg.len() > 512 || err_msg.chars().any(char::is_control) {
            return Err(corrupt(
                "launch intent last_error must be non-empty, <= 512 bytes, and free of control characters"
                    .to_string(),
            ));
        }
    }

    Ok(())
}

/// Reads and verifies `<sessions_root>/.launch-intents/<approval_id>.json` against `record`,
/// returning `Ok(None)` if no launch intent exists yet.
///
/// Corrupt or mismatched files are preserved untouched on disk and fail closed with
/// [`HostError::CorruptSessionState`].
pub fn read_host_launch_intent_optional(
    sessions_root: &Path,
    record: &ApprovalRecord,
) -> Result<Option<HostLaunchIntent>, HostError> {
    validate_approval_id(&record.approval_id)?;
    let intents_dir_candidate = sessions_root.join(LAUNCH_INTENTS_SUBDIR);
    let checked_dir = ensure_no_symlink_components(&intents_dir_candidate)?;
    match fs::symlink_metadata(&checked_dir) {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(HostError::Io {
                operation: "symlink_metadata(launch_intents_dir)".to_string(),
                reason: err.to_string(),
            });
        }
    }

    let intents_dir = verify_existing_private_dir(&checked_dir)?;
    inspect_launch_intents_dir(&intents_dir)?;

    let intent_path = intents_dir.join(format!("{}.json", record.approval_id));
    let checked_file = ensure_no_symlink_components(&intent_path)?;
    let meta = match fs::symlink_metadata(&checked_file) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(HostError::Io {
                operation: "symlink_metadata(launch_intent.json)".to_string(),
                reason: err.to_string(),
            });
        }
    };
    validate_private_regular_metadata(&checked_file, &meta)?;
    if meta.len() > MAX_LAUNCH_INTENT_BYTES as u64 {
        return Err(HostError::CorruptSessionState {
            reason: format!(
                "launch intent file size ({} bytes) exceeds maximum {} bytes",
                meta.len(),
                MAX_LAUNCH_INTENT_BYTES
            ),
        });
    }

    let file = File::open(&checked_file).map_err(|err| HostError::Io {
        operation: "open(launch_intent.json)".to_string(),
        reason: err.to_string(),
    })?;
    let mut bytes = Vec::with_capacity(meta.len() as usize);
    file.take(MAX_LAUNCH_INTENT_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|err| HostError::Io {
            operation: "read(launch_intent.json)".to_string(),
            reason: err.to_string(),
        })?;
    if bytes.len() > MAX_LAUNCH_INTENT_BYTES {
        return Err(HostError::CorruptSessionState {
            reason: format!(
                "launch intent content exceeds maximum {} bytes",
                MAX_LAUNCH_INTENT_BYTES
            ),
        });
    }

    let intent: HostLaunchIntent =
        serde_json::from_slice(&bytes).map_err(|err| HostError::CorruptSessionState {
            reason: format!("invalid launch intent JSON: {err}"),
        })?;
    verify_host_launch_intent(&intent, record, &checked_file)?;
    Ok(Some(intent))
}

/// Validates and atomically writes `<sessions_root>/.launch-intents/<approval_id>.json` (`0600`,
/// `<= 16 KiB`).
pub fn write_host_launch_intent(
    sessions_root: &Path,
    record: &ApprovalRecord,
    intent: &HostLaunchIntent,
) -> Result<(), HostError> {
    validate_approval_id(&record.approval_id)?;
    let intents_dir = ensure_private_dir(&sessions_root.join(LAUNCH_INTENTS_SUBDIR))?;
    inspect_launch_intents_dir(&intents_dir)?;
    let intent_path = intents_dir.join(format!("{}.json", record.approval_id));
    verify_host_launch_intent(intent, record, &intent_path)?;

    let mut bytes =
        serde_json::to_vec_pretty(intent).map_err(|err| HostError::CorruptSessionState {
            reason: format!("failed to serialize launch intent: {err}"),
        })?;
    bytes.push(b'\n');
    if bytes.len() > MAX_LAUNCH_INTENT_BYTES {
        return Err(HostError::CorruptSessionState {
            reason: format!(
                "serialized launch intent ({} bytes) exceeds maximum {} bytes",
                bytes.len(),
                MAX_LAUNCH_INTENT_BYTES
            ),
        });
    }
    atomic_write_private_file(&intent_path, &bytes, MAX_LAUNCH_INTENT_BYTES)
}
