use std::fs::{self, File, OpenOptions};
use std::hint::black_box;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use fs2::FileExt;
use uuid::Uuid;

use crate::error::{NativeError, NativeErrorCode};

/// Maximum bounded size for private state files (`1 MiB`).
pub const MAX_PRIVATE_STATE_FILE_BYTES: usize = 1024 * 1024;

/// Maximum characters retained in a sanitized diagnostic error message.
pub const MAX_DIAGNOSTIC_CHARS: usize = 240;

/// Ensures the current operating system supports Unix private permissions (`0700`/`0600`) and process groups.
pub fn ensure_platform_supported() -> Result<(), NativeError> {
    #[cfg(unix)]
    {
        Ok(())
    }
    #[cfg(not(unix))]
    {
        Err(NativeError::new(
            NativeErrorCode::UnsupportedPlatform,
            "threadlane-native requires Unix process-group supervision and 0700/0600 permission enforcement",
        ))
    }
}

/// Validates that `path` is absolute and that no existing component is a symlink or `..`.
pub fn validate_absolute_no_symlink_components(
    path: &Path,
    label: &str,
) -> Result<PathBuf, NativeError> {
    ensure_platform_supported()?;
    if path.as_os_str().is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            format!("{label} path must not be empty"),
        ));
    }
    if !path.is_absolute() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            format!("{label} path must be an absolute path"),
        ));
    }

    let mut current = PathBuf::new();
    let mut components = path.components().peekable();
    while let Some(comp) = components.next() {
        match comp {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(comp.as_os_str()),
            Component::CurDir => {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    format!("{label} path must not contain `.` components"),
                ));
            }
            Component::ParentDir => {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    format!("{label} path must not contain `..` traversal components"),
                ));
            }
            Component::Normal(part) => {
                current.push(part);
                let is_last = components.peek().is_none();
                match fs::symlink_metadata(&current) {
                    Ok(meta) => {
                        if meta.file_type().is_symlink() {
                            return Err(NativeError::new(
                                NativeErrorCode::SecurityViolation,
                                format!("{label} path traverses a symlink component"),
                            ));
                        }
                        if !is_last && !meta.file_type().is_dir() {
                            return Err(NativeError::new(
                                NativeErrorCode::InvalidConfig,
                                format!("{label} parent component is not a directory"),
                            ));
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => {
                        return Err(NativeError::new(
                            NativeErrorCode::IoError,
                            format!(
                                "failed to inspect {label} path metadata: {}",
                                sanitize_io_error(&err)
                            ),
                        ));
                    }
                }
            }
        }
    }

    Ok(current)
}

/// Validates the configured native Codex executable path.
///
/// Requires an explicit, clean absolute input path (no `.` or `..` segments), canonicalizes
/// symlink chains once (supporting NixOS `/etc/profiles/...` and `/run/current-system/sw/bin`
/// or macOS PATH symlinks), verifies the resolved target is a regular executable file, and
/// returns the canonical target path to launch.
pub fn validate_executable_path(executable: &Path) -> Result<PathBuf, NativeError> {
    ensure_platform_supported()?;
    if executable.as_os_str().is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::NotConfigured,
            "native Codex executable path is not configured",
        ));
    }
    if !executable.is_absolute() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "native Codex executable path must be an absolute path",
        ));
    }
    for comp in executable.components() {
        match comp {
            Component::CurDir => {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "native Codex executable path must not contain `.` components",
                ));
            }
            Component::ParentDir => {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "native Codex executable path must not contain `..` traversal components",
                ));
            }
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {}
        }
    }

    let canonical = match fs::canonicalize(executable) {
        Ok(path) => path,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(NativeError::new(
                NativeErrorCode::NotConfigured,
                "configured native Codex executable does not exist",
            ));
        }
        Err(err) => {
            return Err(NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "cannot canonicalize native Codex executable: {}",
                    sanitize_io_error(&err)
                ),
            ));
        }
    };

    let meta = match fs::symlink_metadata(&canonical) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(NativeError::new(
                NativeErrorCode::NotConfigured,
                "configured native Codex executable does not exist",
            ));
        }
        Err(err) => {
            return Err(NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "cannot stat native Codex executable: {}",
                    sanitize_io_error(&err)
                ),
            ));
        }
    };
    if meta.file_type().is_symlink() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "resolved native Codex executable target must not be a symlink",
        ));
    }
    if !meta.file_type().is_file() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "native Codex executable path is not a regular file",
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if meta.permissions().mode() & 0o111 == 0 {
            return Err(NativeError::new(
                NativeErrorCode::InvalidConfig,
                "native Codex executable file is not marked executable",
            ));
        }
    }

    Ok(canonical)
}

/// Validates an optional operator-selected app-server proxy socket path.
pub fn validate_optional_socket_path(
    socket: Option<&Path>,
) -> Result<Option<PathBuf>, NativeError> {
    let Some(sock) = socket else {
        return Ok(None);
    };
    if sock.as_os_str().is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "native app-server socket path must not be empty when specified",
        ));
    }
    let clean = validate_absolute_no_symlink_components(sock, "native app-server socket")?;
    if let Ok(meta) = fs::symlink_metadata(&clean) {
        if meta.file_type().is_symlink() {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "native app-server socket path must not be a symlink",
            ));
        }
        if meta.file_type().is_dir() {
            return Err(NativeError::new(
                NativeErrorCode::InvalidConfig,
                "native app-server socket path must not be a directory",
            ));
        }
    }
    Ok(Some(clean))
}

/// Validates an optional canonical working directory filter.
pub fn validate_optional_cwd_path(cwd: Option<&Path>) -> Result<Option<PathBuf>, NativeError> {
    let Some(dir) = cwd else {
        return Ok(None);
    };
    let clean = validate_absolute_no_symlink_components(dir, "working directory filter")?;
    if let Ok(meta) = fs::symlink_metadata(&clean) {
        if meta.file_type().is_symlink() {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "working directory filter must not be a symlink",
            ));
        }
        if !meta.file_type().is_dir() {
            return Err(NativeError::new(
                NativeErrorCode::InvalidConfig,
                "working directory filter is not a directory",
            ));
        }
    }
    Ok(Some(clean))
}

/// Prepares and validates a private `0700` directory, rejecting symlinks and group/world permissions.
pub fn prepare_private_dir(dir: &Path, label: &str) -> Result<PathBuf, NativeError> {
    ensure_platform_supported()?;
    let resolved = if dir.is_absolute() {
        dir.to_path_buf()
    } else {
        let cwd = std::env::current_dir().map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to resolve current directory: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        cwd.join(dir)
    };
    let clean = validate_absolute_no_symlink_components(&resolved, label)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

        let mut current = PathBuf::new();
        for comp in clean.components() {
            match comp {
                Component::Prefix(p) => current.push(p.as_os_str()),
                Component::RootDir => current.push(comp.as_os_str()),
                Component::CurDir | Component::ParentDir => {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        format!("{label} directory must not contain relative traversal components"),
                    ));
                }
                Component::Normal(part) => {
                    current.push(part);
                    match fs::symlink_metadata(&current) {
                        Ok(meta) => {
                            if meta.file_type().is_symlink() {
                                return Err(NativeError::new(
                                    NativeErrorCode::SecurityViolation,
                                    format!("{label} directory path traverses a symlink"),
                                ));
                            }
                            if !meta.file_type().is_dir() {
                                return Err(NativeError::new(
                                    NativeErrorCode::SecurityViolation,
                                    format!("{label} path component is not a directory"),
                                ));
                            }
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                            let mut builder = fs::DirBuilder::new();
                            builder.mode(0o700);
                            builder.create(&current).map_err(|create_err| {
                                NativeError::new(
                                    NativeErrorCode::IoError,
                                    format!(
                                        "failed to create {label} directory: {}",
                                        sanitize_io_error(&create_err)
                                    ),
                                )
                            })?;
                        }
                        Err(err) => {
                            return Err(NativeError::new(
                                NativeErrorCode::IoError,
                                format!(
                                    "failed to stat {label} directory: {}",
                                    sanitize_io_error(&err)
                                ),
                            ));
                        }
                    }
                }
            }
        }

        let meta = fs::symlink_metadata(&clean).map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to stat {label} directory: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!("{label} must be a non-symlink directory"),
            ));
        }
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            let is_non_empty = fs::read_dir(&clean)
                .map_err(|err| {
                    NativeError::new(
                        NativeErrorCode::IoError,
                        format!(
                            "failed to read {label} directory: {}",
                            sanitize_io_error(&err)
                        ),
                    )
                })?
                .next()
                .is_some();
            if is_non_empty {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    format!(
                        "pre-existing {label} directory has insecure permissions {:04o} (expected 0700)",
                        mode
                    ),
                ));
            }
            fs::set_permissions(&clean, fs::Permissions::from_mode(0o700)).map_err(|err| {
                NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to set {label} directory permissions: {}",
                        sanitize_io_error(&err)
                    ),
                )
            })?;
        }
    }

    Ok(clean)
}

/// Validates that a file path inside a private directory is not a symlink and (if present) has `0600` permissions.
pub fn validate_private_file_path(path: &Path, label: &str) -> Result<(), NativeError> {
    let _ = validate_absolute_no_symlink_components(path, label)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        match fs::symlink_metadata(path) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        format!("{label} file must not be a symlink"),
                    ));
                }
                if !meta.file_type().is_file() {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        format!("{label} path must be a regular file"),
                    ));
                }
                let mode = meta.permissions().mode() & 0o777;
                if mode & 0o077 != 0 {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        format!(
                            "{label} file has insecure permissions {:04o} (expected 0600)",
                            mode
                        ),
                    ));
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(NativeError::new(
                    NativeErrorCode::IoError,
                    format!("failed to stat {label} file: {}", sanitize_io_error(&err)),
                ));
            }
        }
    }

    Ok(())
}

/// Exclusive cross-process file lock guard (`0600` lock file).
#[derive(Debug)]
pub struct CrossProcessLockGuard {
    file: File,
}

impl Drop for CrossProcessLockGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// Acquires an exclusive cross-process `fs2` lock on `<dir>/<lock_filename>`.
pub fn acquire_cross_process_lock(
    dir: &Path,
    lock_filename: &str,
    label: &str,
) -> Result<CrossProcessLockGuard, NativeError> {
    ensure_platform_supported()?;
    let lock_path = dir.join(lock_filename);
    validate_private_file_path(&lock_path, label)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&lock_path)
            .map_err(|err| {
                NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to open {label} lock file: {}",
                        sanitize_io_error(&err)
                    ),
                )
            })?;
        let meta = file.metadata().map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to stat {label} lock file: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        if !meta.file_type().is_file() {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!("{label} lock file is not a regular file"),
            ));
        }
        let sym_meta = fs::symlink_metadata(&lock_path).map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to symlink-stat {label} lock file: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        if sym_meta.file_type().is_symlink() {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!("{label} lock file must not be a symlink"),
            ));
        }
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|err| {
                NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to set {label} lock permissions: {}",
                        sanitize_io_error(&err)
                    ),
                )
            })?;
        file.lock_exclusive().map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!("failed to lock {label}: {}", sanitize_io_error(&err)),
            )
        })?;

        Ok(CrossProcessLockGuard { file })
    }

    #[cfg(not(unix))]
    {
        let _ = (lock_path, label);
        ensure_platform_supported()?;
        unreachable!()
    }
}

/// Reads a bounded private (`0600`) regular file, returning `Ok(None)` if it does not exist.
pub fn read_private_file_bounded(
    path: &Path,
    max_bytes: usize,
    label: &str,
) -> Result<Option<Vec<u8>>, NativeError> {
    validate_private_file_path(path, label)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let sym_meta = match fs::symlink_metadata(path) {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(NativeError::new(
                    NativeErrorCode::IoError,
                    format!("failed to stat {label}: {}", sanitize_io_error(&err)),
                ));
            }
        };
        if sym_meta.file_type().is_symlink() || !sym_meta.file_type().is_file() {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!("{label} must be a non-symlink regular file"),
            ));
        }
        if (sym_meta.permissions().mode() & 0o077) != 0 {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!("{label} has insecure group/world permissions"),
            ));
        }
        if sym_meta.len() > max_bytes as u64 {
            return Err(NativeError::new(
                NativeErrorCode::OutputOverflow,
                format!("{label} exceeds maximum size of {max_bytes} bytes"),
            ));
        }

        let file = File::open(path).map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!("failed to open {label}: {}", sanitize_io_error(&err)),
            )
        })?;
        let open_meta = file.metadata().map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!("failed to stat opened {label}: {}", sanitize_io_error(&err)),
            )
        })?;
        if !open_meta.file_type().is_file()
            || open_meta.dev() != sym_meta.dev()
            || open_meta.ino() != sym_meta.ino()
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!("{label} changed during open"),
            ));
        }
        if (open_meta.permissions().mode() & 0o077) != 0 {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!("{label} has insecure group/world permissions"),
            ));
        }

        let mut buf = Vec::with_capacity(open_meta.len().min(max_bytes as u64) as usize);
        file.take((max_bytes as u64).saturating_add(1))
            .read_to_end(&mut buf)
            .map_err(|err| {
                NativeError::new(
                    NativeErrorCode::IoError,
                    format!("failed to read {label}: {}", sanitize_io_error(&err)),
                )
            })?;
        if buf.len() > max_bytes {
            return Err(NativeError::new(
                NativeErrorCode::OutputOverflow,
                format!("{label} exceeds maximum size of {max_bytes} bytes"),
            ));
        }

        Ok(Some(buf))
    }

    #[cfg(not(unix))]
    {
        let _ = (path, max_bytes, label);
        ensure_platform_supported()?;
        unreachable!()
    }
}

/// Atomically writes `bytes` to `<dir>/<filename>` with `0600` permissions and `fsync`.
pub fn write_private_file_atomic(
    dir: &Path,
    filename: &str,
    bytes: &[u8],
    label: &str,
) -> Result<PathBuf, NativeError> {
    ensure_platform_supported()?;
    if bytes.len() > MAX_PRIVATE_STATE_FILE_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            format!("{label} payload exceeds maximum size of {MAX_PRIVATE_STATE_FILE_BYTES} bytes"),
        ));
    }
    let target_path = dir.join(filename);
    validate_private_file_path(&target_path, label)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let temp_prefix = format!(".{filename}.tmp.");
        let mut temp = tempfile::Builder::new()
            .prefix(&temp_prefix)
            .permissions(fs::Permissions::from_mode(0o600))
            .tempfile_in(dir)
            .map_err(|err| {
                NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to create temporary {label} file: {}",
                        sanitize_io_error(&err)
                    ),
                )
            })?;

        temp.write_all(bytes).map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to write temporary {label} file: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        temp.as_file().sync_all().map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to sync temporary {label} file: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        temp.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|err| {
                NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to set permissions on {label} file: {}",
                        sanitize_io_error(&err)
                    ),
                )
            })?;

        temp.persist(&target_path).map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to persist {label} file: {}",
                    sanitize_io_error(&err.error)
                ),
            )
        })?;
        fs::set_permissions(&target_path, fs::Permissions::from_mode(0o600)).map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to enforce 0600 on {label} file: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        if let Ok(dir_file) = File::open(dir) {
            let _ = dir_file.sync_all();
        }
    }

    Ok(target_path)
}

/// Generates a prefixed CSPRNG 64-hex-character identifier from two random v4 UUIDs.
pub fn generate_prefixed_random_id(prefix: &str) -> String {
    let a = Uuid::new_v4().simple().to_string();
    let b = Uuid::new_v4().simple().to_string();
    format!("{prefix}_{a}{b}")
}

/// Non-short-circuiting byte equality check for operator tokens and nonces.
pub fn constant_time_str_eq(lhs: &str, rhs: &str) -> bool {
    let a = black_box(lhs.as_bytes());
    let b = black_box(rhs.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&x, &y) in a.iter().zip(b.iter()) {
        diff = black_box(diff | (x ^ y));
    }
    black_box(diff) == 0
}

/// Validates a non-empty bounded identifier (such as `thread_id`, `binding_id`, `project_id`, or `turn_id`).
pub fn validate_bounded_identifier(
    raw: &str,
    field: &'static str,
    max_len: usize,
) -> Result<String, NativeError> {
    if raw.is_empty()
        || raw.len() > max_len
        || raw.trim() != raw
        || raw
            .chars()
            .any(|c| c.is_control() || matches!(c, '/' | '\\' | '?' | '#' | '%'))
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{field} is empty, exceeds maximum length, or contains invalid characters"),
        ));
    }
    Ok(raw.to_string())
}

/// Sanitizes diagnostic text so it never contains control characters or unbounded output.
pub fn sanitize_diagnostic_text(raw: &str) -> String {
    let mut out = String::new();
    for ch in raw.chars() {
        if out.chars().count() >= MAX_DIAGNOSTIC_CHARS {
            break;
        }
        if ch.is_control() {
            if !out.ends_with(' ') {
                out.push(' ');
            }
        } else {
            out.push(ch);
        }
    }
    let trimmed = out.trim();
    if trimmed.is_empty() {
        "native operation error".to_string()
    } else {
        trimmed.to_string()
    }
}

fn sanitize_io_error(err: &std::io::Error) -> String {
    sanitize_diagnostic_text(&err.kind().to_string())
}
