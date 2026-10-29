use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_authority::ApprovalRecord;
use uuid::Uuid;

use crate::error::HostError;
use crate::types::{
    HostExecutionGates, HostPhase, HostSessionManifest, TaskRunReceipt, HOST_LOCK_FILENAME,
    HOST_MANIFEST_FILENAME, HOST_MANIFEST_SCHEMA_VERSION, MAX_HOST_MANIFEST_BYTES,
    MAX_TASK_RECEIPTS_BYTES, RUNTIME_STATE_SUBDIR, TASK_RECEIPTS_FILENAME,
    TASK_RECEIPTS_LOCK_FILENAME, TASK_RECEIPTS_SCHEMA_VERSION, WORK_AREA_SUBDIR,
};

const APPROVAL_ID_PREFIX: &str = "apr_";
const APPROVAL_ID_HEX_LEN: usize = 32;
const MAX_IDENTIFIER_LEN: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TaskReceiptsFile {
    pub schema_version: u32,
    pub approval_id: String,
    pub subject_digest: String,
    pub session_binding_id: String,
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
            builder.create(&checked).map_err(|create_err| HostError::Io {
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
pub fn validate_sessions_root(
    sessions_dir: &Path,
    other_dirs: &[(&str, &Path)],
) -> Result<PathBuf, HostError> {
    let canonical_sessions = ensure_private_dir(sessions_dir)?;

    for (label, other) in other_dirs {
        let other_resolved = if other.exists() {
            fs::canonicalize(other).unwrap_or_else(|_| other.to_path_buf())
        } else {
            ensure_no_symlink_components(other).unwrap_or_else(|_| other.to_path_buf())
        };
        if paths_overlap(&canonical_sessions, &other_resolved) {
            return Err(HostError::SecurityViolation {
                path: canonical_sessions.display().to_string(),
                reason: format!(
                    "sessions_dir must not equal or overlap `{label}` (`{}`)",
                    other_resolved.display()
                ),
            });
        }
    }

    let entries = fs::read_dir(&canonical_sessions).map_err(|err| HostError::Io {
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

    Ok(canonical_sessions)
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

fn inspect_session_dir_entries(session_dir: &Path) -> Result<(), HostError> {
    let entries = fs::read_dir(session_dir).map_err(|err| HostError::Io {
        operation: "read_dir(session_dir)".to_string(),
        reason: err.to_string(),
    })?;

    let mut seen_names = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|err| HostError::Io {
            operation: "read_dir_entry(session_dir)".to_string(),
            reason: err.to_string(),
        })?;
        let entry_path = entry.path();
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            return Err(HostError::SecurityViolation {
                path: entry_path.display().to_string(),
                reason: "non-UTF-8 entry in session directory is not permitted".to_string(),
            });
        };
        let meta = fs::symlink_metadata(&entry_path).map_err(|err| HostError::Io {
            operation: "symlink_metadata(session_entry)".to_string(),
            reason: err.to_string(),
        })?;
        if meta.file_type().is_symlink() {
            return Err(HostError::SecurityViolation {
                path: entry_path.display().to_string(),
                reason: "symlink inside session directory is not permitted".to_string(),
            });
        }

        match name {
            HOST_LOCK_FILENAME
            | HOST_MANIFEST_FILENAME
            | TASK_RECEIPTS_FILENAME
            | TASK_RECEIPTS_LOCK_FILENAME => {
                validate_private_regular_metadata(&entry_path, &meta)?;
                seen_names.push(name.to_string());
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
                seen_names.push(name.to_string());
            }
            _ => {
                return Err(HostError::SecurityViolation {
                    path: entry_path.display().to_string(),
                    reason: format!("foreign or unexpected entry `{name}` in session directory"),
                });
            }
        }
    }

    if !seen_names.is_empty()
        && !seen_names
            .iter()
            .any(|n| n == HOST_LOCK_FILENAME || n == HOST_MANIFEST_FILENAME)
    {
        return Err(HostError::SecurityViolation {
            path: session_dir.display().to_string(),
            reason: "pre-existing non-empty session directory lacks host.lock and host-manifest.json".to_string(),
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
    let mut bytes = serde_json::to_vec_pretty(manifest).map_err(|err| HostError::CorruptManifest {
        path: session_dir.join(HOST_MANIFEST_FILENAME).display().to_string(),
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
    if manifest.phase == HostPhase::Ready && manifest.session_binding_id.is_none() {
        return Err(HostError::validation(
            "session_binding_id",
            "Ready manifest must record session_binding_id",
        ));
    }
    let expected_gates = HostExecutionGates::for_ready_state(manifest.phase == HostPhase::Ready);
    if manifest.gates != expected_gates {
        return Err(HostError::validation(
            "gates",
            "manifest gates do not match phase invariants",
        ));
    }
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
    if manifest.phase == HostPhase::Ready && manifest.session_binding_id.is_none() {
        return Err(corrupt(
            "manifest is in `ready` phase without `session_binding_id`".to_string(),
        ));
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
    if manifest.gates != expected_reference.gates {
        return Err(corrupt(
            "manifest execution gates do not match required phase invariants".to_string(),
        ));
    }
    Ok(())
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
    let lock_file = open_private_lock_file(&session_dir.join(TASK_RECEIPTS_LOCK_FILENAME))?;
    lock_file.lock_exclusive().map_err(|err| HostError::Io {
        operation: "lock_exclusive(task-receipts.lock)".to_string(),
        reason: err.to_string(),
    })?;
    let res = read_task_receipts_locked(session_dir, record, session_binding_id).map(|opt| {
        opt.and_then(|ledger| {
            ledger
                .receipts
                .into_iter()
                .find(|entry| entry.task_id == task_id)
        })
    });
    let _ = lock_file.unlock();
    res
}

/// Atomically persists a sealed [`TaskRunReceipt`] inside `<session_dir>/task-receipts.json`.
///
/// If a receipt for `receipt.task_id` already exists with identical mapping fields, returns it
/// idempotently; if an existing receipt conflicts, fails closed with [`HostError::CorruptReceipts`].
pub fn record_task_receipt(
    session_dir: &Path,
    record: &ApprovalRecord,
    session_binding_id: &str,
    receipt: TaskRunReceipt,
) -> Result<TaskRunReceipt, HostError> {
    validate_single_receipt(&receipt, record, &session_dir.join(TASK_RECEIPTS_FILENAME))?;

    let lock_file = open_private_lock_file(&session_dir.join(TASK_RECEIPTS_LOCK_FILENAME))?;
    lock_file.lock_exclusive().map_err(|err| HostError::Io {
        operation: "lock_exclusive(task-receipts.lock)".to_string(),
        reason: err.to_string(),
    })?;

    let res = (|| -> Result<TaskRunReceipt, HostError> {
        let receipts_path = session_dir.join(TASK_RECEIPTS_FILENAME);
        let mut ledger =
            match read_task_receipts_locked(session_dir, record, session_binding_id)? {
                Some(existing) => existing,
                None => TaskReceiptsFile {
                    schema_version: TASK_RECEIPTS_SCHEMA_VERSION,
                    approval_id: record.approval_id.clone(),
                    subject_digest: record.subject_digest.clone(),
                    session_binding_id: session_binding_id.to_string(),
                    receipts: Vec::new(),
                },
            };

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
            {
                return Err(HostError::CorruptReceipts {
                    path: receipts_path.display().to_string(),
                    reason: format!(
                        "conflicting task receipt for task `{}`: existing run_id `{}` vs new `{}`",
                        receipt.task_id, existing.run_id, receipt.run_id
                    ),
                });
            }
            return Ok(existing.clone());
        }

        ledger.receipts.push(receipt.clone());
        ledger
            .receipts
            .sort_by(|left, right| left.task_id.cmp(&right.task_id));

        let mut bytes =
            serde_json::to_vec_pretty(&ledger).map_err(|err| HostError::CorruptReceipts {
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

        atomic_write_private_file(&receipts_path, &bytes, MAX_TASK_RECEIPTS_BYTES)?;
        Ok(receipt)
    })();

    let _ = lock_file.unlock();
    res
}

fn read_task_receipts_locked(
    session_dir: &Path,
    record: &ApprovalRecord,
    session_binding_id: &str,
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

    verify_task_receipts_ledger(&ledger, record, session_binding_id, &checked)?;
    Ok(Some(ledger))
}

fn verify_task_receipts_ledger(
    ledger: &TaskReceiptsFile,
    record: &ApprovalRecord,
    session_binding_id: &str,
    receipts_path: &Path,
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
    if ledger.session_binding_id != session_binding_id {
        return Err(corrupt(format!(
            "receipts session_binding_id `{}` does not match expected `{}`",
            ledger.session_binding_id, session_binding_id
        )));
    }

    let mut seen_tasks = BTreeSet::new();
    let mut seen_runs = BTreeSet::new();
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
        validate_single_receipt(receipt, record, receipts_path)?;
    }

    Ok(())
}

fn validate_single_receipt(
    receipt: &TaskRunReceipt,
    record: &ApprovalRecord,
    receipts_path: &Path,
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
    if !task.depends_on.is_empty() {
        return Err(corrupt(format!(
            "receipt references dependent task `{}` which cannot be started",
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

    if validate_hex_digest("task_digest", &receipt.task_digest).is_err() {
        return Err(corrupt(format!(
            "receipt for task `{}` has invalid task_digest `{}`",
            receipt.task_id, receipt.task_digest
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

    Ok(())
}
