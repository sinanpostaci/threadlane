use std::fmt;
#[cfg(target_os = "linux")]
use std::fs::{self, File, OpenOptions};
#[cfg(target_os = "linux")]
use std::io::Read;
use std::path::PathBuf;
#[cfg(target_os = "linux")]
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};

#[cfg(target_os = "linux")]
use crate::descriptor::validate_no_symlink_components;
use crate::error::Error;

/// Required directory name prefix for launcher-created native session directories inside `descriptor_root`.
pub const SESSION_DIR_PREFIX: &str = "tl-codex-";

/// Required filename for the per-session endpoint descriptor inside a `tl-codex-*` directory.
pub const ENDPOINT_DESCRIPTOR_FILENAME: &str = "descriptor.json";

/// Supported schema version for launcher `descriptor.json` files.
pub const ENDPOINT_DESCRIPTOR_SCHEMA_VERSION: u32 = 1;

/// Required transport identifier in launcher `descriptor.json` files.
pub const ENDPOINT_TRANSPORT_UNIX_WEBSOCKET: &str = "unix_websocket";

/// Maximum permitted byte size (`16 KiB`) for a single launcher `descriptor.json` file.
pub const MAX_ENDPOINT_DESCRIPTOR_BYTES: u64 = 16_384;

/// Maximum directory entries enumerated in `descriptor_root` before failing closed.
pub const MAX_DESCRIPTOR_ROOT_ENTRIES: usize = 1_024;

/// Maximum `tl-codex-*` candidate directories inspected in `descriptor_root` before failing closed.
pub const MAX_ENDPOINT_CANDIDATES: usize = 64;

#[cfg(target_os = "linux")]
const MAX_CUMULATIVE_DESCRIPTOR_BYTES: u64 =
    (MAX_ENDPOINT_CANDIDATES as u64) * MAX_ENDPOINT_DESCRIPTOR_BYTES;

#[cfg(target_os = "linux")]
const MAX_PROC_FILE_BYTES: u64 = 16_384;

/// Categorized native endpoint resolution and validation failure with sanitized, path-free reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndpointError {
    /// Transient unavailability (no live matching endpoint is ready); eligible for bounded pre-effect retry.
    Unavailable(&'static str),
    /// Terminal security, configuration, permission, descriptor, process-identity, or ambiguity refusal.
    Refused(&'static str),
}

impl EndpointError {
    /// Returns the sanitized, path-free static error reason.
    pub(crate) const fn reason(&self) -> &'static str {
        match self {
            Self::Unavailable(reason) | Self::Refused(reason) => reason,
        }
    }
}

impl fmt::Display for EndpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.reason())
    }
}

impl std::error::Error for EndpointError {}

/// Explicit operator-selected configuration for automatic Linux desktop native endpoint
/// discovery and process-identity verification across backend restarts.
///
/// This configuration is an operator-selected local scope only and is not a per-call
/// native MCP authentication proof.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeReconnectConfig {
    /// Absolute normalized canonical path to the existing same-UID `0700` directory containing `tl-codex-*` session directories.
    pub descriptor_root: PathBuf,
    /// Absolute normalized canonical path to the existing regular executable backend binary (`real_codex`).
    pub backend_executable: PathBuf,
    /// Absolute normalized canonical path to the existing regular executable desktop application binary.
    pub application_executable: PathBuf,
}

impl fmt::Debug for NativeReconnectConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeReconnectConfig")
            .field("descriptor_root", &"[REDACTED]")
            .field("backend_executable", &"[REDACTED]")
            .field("application_executable", &"[REDACTED]")
            .finish()
    }
}

impl NativeReconnectConfig {
    /// Validates static private `descriptor_root` directory policy and canonical executable policy
    /// for `backend_executable` and `application_executable`.
    ///
    /// Automatic native endpoint reconnect is supported only on Linux; other platforms fail closed.
    pub fn validate(&self) -> Result<(), Error> {
        #[cfg(target_os = "linux")]
        {
            self.validate_with_euid(current_euid())
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(Error::UnsupportedPlatform(
                "native endpoint auto-reconnect is only supported on Linux".to_string(),
            ))
        }
    }

    /// Validates this configuration against an explicit expected effective UID.
    pub(crate) fn validate_with_euid(&self, expected_uid: u32) -> Result<(), Error> {
        #[cfg(target_os = "linux")]
        {
            validate_private_canonical_dir(
                &self.descriptor_root,
                expected_uid,
                "native reconnect descriptor_root",
            )?;
            let _ = validate_canonical_executable(
                &self.backend_executable,
                "native reconnect backend_executable",
            )?;
            let _ = validate_canonical_executable(
                &self.application_executable,
                "native reconnect application_executable",
            )?;
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = expected_uid;
            Err(Error::UnsupportedPlatform(
                "native endpoint auto-reconnect is only supported on Linux".to_string(),
            ))
        }
    }

    /// Discovers and verifies the unique live matching native endpoint under `descriptor_root`.
    ///
    /// Performs synchronous, bounded, side-effect-free inspection of immediate `tl-codex-*`
    /// session directories, `descriptor.json` files, `/proc` ancestry and executable identities,
    /// and private Unix socket ownership and permissions.
    pub(crate) fn resolve(&self) -> Result<ResolvedNativeEndpoint, EndpointError> {
        #[cfg(target_os = "linux")]
        {
            self.resolve_with_context(Path::new("/proc"), current_euid())
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(EndpointError::Refused(
                "native endpoint auto-reconnect is only supported on Linux",
            ))
        }
    }

    #[cfg(target_os = "linux")]
    fn resolve_with_context(
        &self,
        proc_root: &Path,
        expected_uid: u32,
    ) -> Result<ResolvedNativeEndpoint, EndpointError> {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let (root_dev, root_ino) = validate_private_canonical_dir(
            &self.descriptor_root,
            expected_uid,
            "native reconnect descriptor_root",
        )
        .map_err(|_| {
            EndpointError::Refused(
                "native reconnect descriptor_root violated private directory policy",
            )
        })?;

        let (backend_exe_dev, backend_exe_ino) = validate_canonical_executable(
            &self.backend_executable,
            "native reconnect backend_executable",
        )
        .map_err(|_| {
            EndpointError::Refused(
                "native reconnect backend_executable violated canonical executable policy",
            )
        })?;

        let (app_exe_dev, app_exe_ino) = validate_canonical_executable(
            &self.application_executable,
            "native reconnect application_executable",
        )
        .map_err(|_| {
            EndpointError::Refused(
                "native reconnect application_executable violated canonical executable policy",
            )
        })?;

        let read_dir = fs::read_dir(&self.descriptor_root).map_err(|_| {
            EndpointError::Refused("failed to enumerate native reconnect descriptor_root")
        })?;

        let after_root_meta = fs::symlink_metadata(&self.descriptor_root).map_err(|_| {
            EndpointError::Refused("failed to revalidate native reconnect descriptor_root")
        })?;
        if after_root_meta.file_type().is_symlink()
            || !after_root_meta.file_type().is_dir()
            || after_root_meta.dev() != root_dev
            || after_root_meta.ino() != root_ino
            || after_root_meta.uid() != expected_uid
            || (after_root_meta.permissions().mode() & 0o777) != 0o700
        {
            return Err(EndpointError::Refused(
                "native reconnect descriptor_root changed during enumeration",
            ));
        }

        let mut entries_seen = 0usize;
        let mut candidates_seen = 0usize;
        let mut cumulative_bytes = 0u64;
        let mut live_matches: Vec<ResolvedNativeEndpoint> = Vec::new();

        for entry_res in read_dir {
            entries_seen = entries_seen.saturating_add(1);
            if entries_seen > MAX_DESCRIPTOR_ROOT_ENTRIES {
                return Err(EndpointError::Refused(
                    "native reconnect descriptor_root exceeded maximum entry count",
                ));
            }

            let entry = entry_res.map_err(|_| {
                EndpointError::Refused(
                    "failed to read directory entry in native reconnect descriptor_root",
                )
            })?;

            let file_name = entry.file_name();
            let Some(name_str) = file_name.to_str() else {
                continue;
            };
            if !name_str.starts_with(SESSION_DIR_PREFIX) {
                continue;
            }

            candidates_seen = candidates_seen.saturating_add(1);
            if candidates_seen > MAX_ENDPOINT_CANDIDATES {
                return Err(EndpointError::Refused(
                    "native reconnect descriptor_root exceeded maximum candidate count",
                ));
            }

            let suffix = &name_str[SESSION_DIR_PREFIX.len()..];
            if suffix.is_empty()
                || suffix.contains('/')
                || suffix.contains("..")
                || suffix.chars().any(char::is_control)
            {
                return Err(EndpointError::Refused(
                    "native reconnect candidate session directory has invalid name",
                ));
            }

            let session_dir = self.descriptor_root.join(&file_name);
            let Some((session_dir_dev, session_dir_ino)) =
                inspect_candidate_session_dir(&session_dir, expected_uid)?
            else {
                // Candidate directory vanished concurrently during cleanup.
                continue;
            };

            let descriptor_path = session_dir.join(ENDPOINT_DESCRIPTOR_FILENAME);
            let Some(descriptor) = read_and_parse_candidate_descriptor(
                &descriptor_path,
                &session_dir,
                expected_uid,
                &mut cumulative_bytes,
            )?
            else {
                // Descriptor not written yet or already unlinked during session teardown.
                continue;
            };

            let Some((backend_snapshot, wrapper_snapshot, app_snapshot)) =
                verify_candidate_process_chain(
                    proc_root,
                    &descriptor,
                    &self.backend_executable,
                    (backend_exe_dev, backend_exe_ino),
                    &self.application_executable,
                    (app_exe_dev, app_exe_ino),
                    expected_uid,
                )?
            else {
                // Provably dead/stale candidate process chain; ignore and continue.
                continue;
            };

            let Some((resolved_socket_path, socket_dev, socket_ino)) =
                validate_candidate_socket(&session_dir, &descriptor.socket_path, expected_uid)?
            else {
                // Socket not present or already unlinked; candidate is not ready/live.
                continue;
            };

            // Re-verify session directory identity after reading descriptor and socket.
            let post_dir_meta = match fs::symlink_metadata(&session_dir) {
                Ok(meta) => meta,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(_) => {
                    return Err(EndpointError::Refused(
                        "failed to revalidate native reconnect session directory",
                    ));
                }
            };
            if post_dir_meta.file_type().is_symlink()
                || !post_dir_meta.file_type().is_dir()
                || post_dir_meta.dev() != session_dir_dev
                || post_dir_meta.ino() != session_dir_ino
                || post_dir_meta.uid() != expected_uid
                || (post_dir_meta.permissions().mode() & 0o777) != 0o700
            {
                return Err(EndpointError::Refused(
                    "native reconnect session directory identity changed during resolution",
                ));
            }

            live_matches.push(ResolvedNativeEndpoint {
                socket_path: resolved_socket_path,
                proc_root: proc_root.to_path_buf(),
                session_dir,
                session_dir_dev,
                session_dir_ino,
                advertised_socket_path: descriptor.socket_path,
                socket_dev,
                socket_ino,
                expected_uid,
                backend: backend_snapshot,
                wrapper: wrapper_snapshot,
                application: app_snapshot,
            });
        }

        if live_matches.len() > 1 {
            return Err(EndpointError::Refused(
                "multiple live matching native endpoints found",
            ));
        }

        live_matches.pop().ok_or(EndpointError::Unavailable(
            "no live matching native endpoint found",
        ))
    }
}

#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
struct ProcessSnapshot {
    pid: u32,
    ppid: u32,
    start_time_ticks: u64,
    uid: u32,
    exe_path: PathBuf,
    exe_dev: u64,
    exe_ino: u64,
}

impl fmt::Debug for ProcessSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessSnapshot")
            .field("pid", &self.pid)
            .field("ppid", &self.ppid)
            .field("start_time_ticks", &self.start_time_ticks)
            .field("uid", &self.uid)
            .field("exe_path", &"[REDACTED]")
            .finish()
    }
}

/// Verified live native endpoint resolution and process-ancestry snapshots.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ResolvedNativeEndpoint {
    /// Validated canonical physical Unix domain socket path for WebSocket connection.
    pub(crate) socket_path: PathBuf,
    proc_root: PathBuf,
    session_dir: PathBuf,
    session_dir_dev: u64,
    session_dir_ino: u64,
    advertised_socket_path: PathBuf,
    socket_dev: u64,
    socket_ino: u64,
    expected_uid: u32,
    backend: ProcessSnapshot,
    wrapper: ProcessSnapshot,
    application: ProcessSnapshot,
}

impl fmt::Debug for ResolvedNativeEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedNativeEndpoint")
            .field("socket_path", &"[REDACTED]")
            .field("expected_uid", &self.expected_uid)
            .field("backend", &self.backend)
            .field("wrapper", &self.wrapper)
            .field("application", &self.application)
            .finish()
    }
}

impl ResolvedNativeEndpoint {
    /// Revalidates the captured backend, wrapper, and application process snapshots and socket
    /// identity before an effect-bearing operation.
    pub(crate) fn validate_processes(&self) -> Result<(), EndpointError> {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};

            revalidate_process_snapshot(&self.proc_root, &self.backend)?;
            revalidate_process_snapshot(&self.proc_root, &self.wrapper)?;
            revalidate_process_snapshot(&self.proc_root, &self.application)?;

            let dir_meta = match fs::symlink_metadata(&self.session_dir) {
                Ok(meta) => meta,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Err(EndpointError::Unavailable(
                        "native endpoint session directory no longer exists",
                    ));
                }
                Err(_) => {
                    return Err(EndpointError::Refused(
                        "failed to inspect native endpoint session directory",
                    ));
                }
            };
            if dir_meta.file_type().is_symlink()
                || !dir_meta.file_type().is_dir()
                || dir_meta.dev() != self.session_dir_dev
                || dir_meta.ino() != self.session_dir_ino
                || dir_meta.uid() != self.expected_uid
                || (dir_meta.permissions().mode() & 0o777) != 0o700
            {
                return Err(EndpointError::Refused(
                    "native endpoint session directory identity or permissions changed",
                ));
            }

            let Some((resolved_socket_path, socket_dev, socket_ino)) = validate_candidate_socket(
                &self.session_dir,
                &self.advertised_socket_path,
                self.expected_uid,
            )?
            else {
                return Err(EndpointError::Unavailable(
                    "native endpoint Unix socket no longer exists",
                ));
            };

            if resolved_socket_path != self.socket_path
                || socket_dev != self.socket_dev
                || socket_ino != self.socket_ino
            {
                return Err(EndpointError::Refused(
                    "native endpoint Unix socket identity changed",
                ));
            }

            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(EndpointError::Refused(
                "native endpoint auto-reconnect is only supported on Linux",
            ))
        }
    }

    /// Validates that the connected Unix domain socket's kernel peer credentials (`SO_PEERCRED`)
    /// match the captured backend UID and PID snapshot.
    #[cfg(unix)]
    pub(crate) fn validate_peer(
        &self,
        stream: &tokio::net::UnixStream,
    ) -> Result<(), EndpointError> {
        #[cfg(target_os = "linux")]
        {
            let cred = stream.peer_cred().map_err(|_| {
                EndpointError::Refused("failed to read native Unix socket peer credentials")
            })?;

            if cred.uid() != self.expected_uid || cred.uid() != self.backend.uid {
                return Err(EndpointError::Refused(
                    "native Unix socket peer UID does not match expected backend UID",
                ));
            }

            let Some(peer_pid) = cred.pid() else {
                return Err(EndpointError::Refused(
                    "native Unix socket peer credentials did not report a PID",
                ));
            };
            let Ok(peer_pid_u32) = u32::try_from(peer_pid) else {
                return Err(EndpointError::Refused(
                    "native Unix socket peer credentials reported an invalid PID",
                ));
            };
            if peer_pid_u32 == 0 || peer_pid_u32 != self.backend.pid {
                return Err(EndpointError::Refused(
                    "native Unix socket peer PID does not match expected backend PID",
                ));
            }

            revalidate_process_snapshot(&self.proc_root, &self.backend)?;
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = stream;
            Err(EndpointError::Refused(
                "native endpoint auto-reconnect is only supported on Linux",
            ))
        }
    }
}

#[cfg(target_os = "linux")]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LauncherDescriptor {
    schema_version: u32,
    wrapper_pid: u32,
    backend_pid: u32,
    socket_path: PathBuf,
    real_codex: PathBuf,
    transport: String,
}

#[cfg(target_os = "linux")]
fn current_euid() -> u32 {
    rustix::process::geteuid().as_raw()
}

#[cfg(target_os = "linux")]
fn validate_normalized_absolute_path_syntax(path: &Path, label: &str) -> Result<(), Error> {
    if path.as_os_str().is_empty() {
        return Err(Error::InvalidConfig(format!("{label} must not be empty")));
    }
    if !path.is_absolute() {
        return Err(Error::InvalidConfig(format!(
            "{label} must be an absolute path"
        )));
    }

    let mut reconstructed = PathBuf::new();
    let mut saw_normal = false;
    for comp in path.components() {
        match comp {
            Component::Prefix(prefix) => reconstructed.push(prefix.as_os_str()),
            Component::RootDir => reconstructed.push(comp.as_os_str()),
            Component::CurDir | Component::ParentDir => {
                return Err(Error::SecurityViolation(format!(
                    "{label} must not contain `.` or `..` traversal components"
                )));
            }
            Component::Normal(part) => {
                if part.is_empty() {
                    return Err(Error::InvalidConfig(format!(
                        "{label} contains an empty path component"
                    )));
                }
                reconstructed.push(part);
                saw_normal = true;
            }
        }
    }

    if !saw_normal {
        return Err(Error::InvalidConfig(format!(
            "{label} must not be the root directory"
        )));
    }

    if reconstructed.as_os_str() != path.as_os_str() {
        return Err(Error::InvalidConfig(format!(
            "{label} must be normalized without redundant or trailing separators"
        )));
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn validate_normalized_absolute_path(path: &Path, label: &str) -> Result<(), Error> {
    validate_normalized_absolute_path_syntax(path, label)?;
    let _ = validate_no_symlink_components(path, label)?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn validate_private_canonical_dir(
    dir: &Path,
    expected_uid: u32,
    label: &str,
) -> Result<(u64, u64), Error> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    validate_normalized_absolute_path(dir, label)?;

    let meta = fs::symlink_metadata(dir).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            Error::InvalidConfig(format!("{label} directory does not exist"))
        } else {
            Error::Io(err)
        }
    })?;
    if meta.file_type().is_symlink() {
        return Err(Error::SecurityViolation(format!(
            "{label} must not be a symlink"
        )));
    }
    if !meta.file_type().is_dir() {
        return Err(Error::SecurityViolation(format!(
            "{label} must be a directory"
        )));
    }
    if meta.uid() != expected_uid {
        return Err(Error::SecurityViolation(format!(
            "{label} is not owned by the current effective UID"
        )));
    }
    let mode = meta.permissions().mode() & 0o777;
    if mode != 0o700 {
        return Err(Error::SecurityViolation(format!(
            "{label} has permissions {:04o} (expected 0700)",
            mode
        )));
    }

    let canonical = fs::canonicalize(dir)
        .map_err(|_| Error::InvalidConfig(format!("failed to canonicalize {label}")))?;
    if canonical != dir {
        return Err(Error::SecurityViolation(format!(
            "{label} must equal its canonical path"
        )));
    }

    Ok((meta.dev(), meta.ino()))
}

#[cfg(target_os = "linux")]
fn validate_canonical_executable(exe: &Path, label: &str) -> Result<(u64, u64), Error> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    validate_normalized_absolute_path(exe, label)?;

    let meta = fs::symlink_metadata(exe).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            Error::InvalidConfig(format!("{label} does not exist"))
        } else {
            Error::Io(err)
        }
    })?;
    if meta.file_type().is_symlink() {
        return Err(Error::SecurityViolation(format!(
            "{label} must not be a symlink"
        )));
    }
    if !meta.file_type().is_file() {
        return Err(Error::InvalidConfig(format!(
            "{label} must be a regular file"
        )));
    }
    let mode = meta.permissions().mode() & 0o777;
    if mode & 0o111 == 0 {
        return Err(Error::InvalidConfig(format!(
            "{label} is not an executable file"
        )));
    }

    let canonical = fs::canonicalize(exe)
        .map_err(|_| Error::InvalidConfig(format!("failed to canonicalize {label}")))?;
    if canonical != exe {
        return Err(Error::SecurityViolation(format!(
            "{label} must equal its canonical path"
        )));
    }

    Ok((meta.dev(), meta.ino()))
}

#[cfg(target_os = "linux")]
fn inspect_candidate_session_dir(
    session_dir: &Path,
    expected_uid: u32,
) -> Result<Option<(u64, u64)>, EndpointError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let meta = match fs::symlink_metadata(session_dir) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(EndpointError::Refused(
                "failed to inspect native reconnect candidate session directory",
            ));
        }
    };

    if meta.file_type().is_symlink() {
        return Err(EndpointError::Refused(
            "native reconnect candidate session directory must not be a symlink",
        ));
    }
    if !meta.file_type().is_dir() {
        return Err(EndpointError::Refused(
            "native reconnect candidate session entry must be a directory",
        ));
    }
    if meta.uid() != expected_uid {
        return Err(EndpointError::Refused(
            "native reconnect candidate session directory is not owned by the current effective UID",
        ));
    }
    let mode = meta.permissions().mode() & 0o777;
    if mode != 0o700 {
        return Err(EndpointError::Refused(
            "native reconnect candidate session directory must have 0700 permissions",
        ));
    }

    let canonical = match fs::canonicalize(session_dir) {
        Ok(canonical) => canonical,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(EndpointError::Refused(
                "failed to canonicalize native reconnect candidate session directory",
            ));
        }
    };
    if canonical != session_dir {
        return Err(EndpointError::Refused(
            "native reconnect candidate session directory must equal its canonical path",
        ));
    }

    Ok(Some((meta.dev(), meta.ino())))
}

#[cfg(target_os = "linux")]
fn open_nofollow_regular_file(
    path: &Path,
    expected_uid: u32,
    expected_mode: Option<u32>,
    max_bytes: u64,
) -> Result<Option<Vec<u8>>, EndpointError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

    let sym_meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(EndpointError::Refused(
                "failed to inspect private file metadata",
            ));
        }
    };

    if sym_meta.file_type().is_symlink() {
        return Err(EndpointError::Refused(
            "private file must not be a symbolic link",
        ));
    }
    if !sym_meta.file_type().is_file() {
        return Err(EndpointError::Refused(
            "private file path must be a regular file",
        ));
    }
    if sym_meta.uid() != expected_uid {
        return Err(EndpointError::Refused(
            "private file is not owned by the current effective UID",
        ));
    }
    if let Some(req_mode) = expected_mode {
        if (sym_meta.permissions().mode() & 0o777) != req_mode {
            return Err(EndpointError::Refused(
                "private file has unexpected permission bits",
            ));
        }
    }
    if sym_meta.len() > max_bytes {
        return Err(EndpointError::Refused(
            "private file exceeds maximum permitted byte size",
        ));
    }

    let file = match OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open(path)
    {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(EndpointError::Refused(
                "failed to open private file with O_NOFOLLOW",
            ));
        }
    };

    let open_meta = file
        .metadata()
        .map_err(|_| EndpointError::Refused("failed to read opened private file metadata"))?;
    if !open_meta.file_type().is_file()
        || open_meta.dev() != sym_meta.dev()
        || open_meta.ino() != sym_meta.ino()
        || open_meta.uid() != expected_uid
        || open_meta.len() != sym_meta.len()
        || open_meta.len() > max_bytes
    {
        return Err(EndpointError::Refused(
            "private file identity or size changed during open",
        ));
    }
    if let Some(req_mode) = expected_mode {
        if (open_meta.permissions().mode() & 0o777) != req_mode {
            return Err(EndpointError::Refused(
                "private file permissions changed during open",
            ));
        }
    }

    let mut buf = Vec::with_capacity(open_meta.len() as usize);
    (&file)
        .take(max_bytes + 1)
        .read_to_end(&mut buf)
        .map_err(|_| EndpointError::Refused("failed to read private file contents"))?;
    if (buf.len() as u64) > max_bytes || (buf.len() as u64) != open_meta.len() {
        return Err(EndpointError::Refused(
            "private file byte length changed during read",
        ));
    }

    let after_meta = file.metadata().map_err(|_| {
        EndpointError::Refused("failed to revalidate private file metadata after read")
    })?;
    if !after_meta.file_type().is_file()
        || after_meta.dev() != open_meta.dev()
        || after_meta.ino() != open_meta.ino()
        || after_meta.uid() != expected_uid
        || after_meta.len() != open_meta.len()
    {
        return Err(EndpointError::Refused(
            "private file identity or size changed after read",
        ));
    }
    if let Some(req_mode) = expected_mode {
        if (after_meta.permissions().mode() & 0o777) != req_mode {
            return Err(EndpointError::Refused(
                "private file permissions changed after read",
            ));
        }
    }

    Ok(Some(buf))
}

#[cfg(target_os = "linux")]
fn read_and_parse_candidate_descriptor(
    descriptor_path: &Path,
    session_dir: &Path,
    expected_uid: u32,
    cumulative_bytes: &mut u64,
) -> Result<Option<LauncherDescriptor>, EndpointError> {
    let Some(bytes) = open_nofollow_regular_file(
        descriptor_path,
        expected_uid,
        Some(0o600),
        MAX_ENDPOINT_DESCRIPTOR_BYTES,
    )?
    else {
        return Ok(None);
    };

    if bytes.is_empty() {
        // The launcher creates the private file before flushing its small JSON payload.
        return Ok(None);
    }

    *cumulative_bytes = cumulative_bytes.saturating_add(bytes.len() as u64);
    if *cumulative_bytes > MAX_CUMULATIVE_DESCRIPTOR_BYTES {
        return Err(EndpointError::Refused(
            "native reconnect cumulative descriptor byte budget exceeded",
        ));
    }

    let descriptor: LauncherDescriptor = serde_json::from_slice(&bytes)
        .map_err(|_| EndpointError::Refused("native reconnect descriptor JSON is malformed"))?;

    if descriptor.schema_version != ENDPOINT_DESCRIPTOR_SCHEMA_VERSION {
        return Err(EndpointError::Refused(
            "unsupported native reconnect descriptor schema_version",
        ));
    }
    if descriptor.transport != ENDPOINT_TRANSPORT_UNIX_WEBSOCKET {
        return Err(EndpointError::Refused(
            "unsupported native reconnect descriptor transport",
        ));
    }
    if descriptor.wrapper_pid == 0
        || descriptor.backend_pid == 0
        || descriptor.wrapper_pid == descriptor.backend_pid
        || i32::try_from(descriptor.wrapper_pid).is_err()
        || i32::try_from(descriptor.backend_pid).is_err()
    {
        return Err(EndpointError::Refused(
            "native reconnect descriptor contains invalid process IDs",
        ));
    }

    validate_normalized_absolute_path_syntax(
        &descriptor.socket_path,
        "native reconnect descriptor socket_path",
    )
    .map_err(|_| {
        EndpointError::Refused(
            "native reconnect descriptor socket_path is not a normalized absolute path",
        )
    })?;

    if descriptor.socket_path.parent() != Some(session_dir) {
        return Err(EndpointError::Refused(
            "native reconnect descriptor socket_path parent must equal its session directory",
        ));
    }

    validate_normalized_absolute_path_syntax(
        &descriptor.real_codex,
        "native reconnect descriptor real_codex",
    )
    .map_err(|_| {
        EndpointError::Refused(
            "native reconnect descriptor real_codex is not a normalized absolute path",
        )
    })?;

    Ok(Some(descriptor))
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcInspectError {
    DeadOrZombie,
    Refused(&'static str),
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ParsedProcStat {
    pid: u32,
    state: char,
    ppid: u32,
    start_time_ticks: u64,
}

#[cfg(target_os = "linux")]
fn parse_proc_stat_content(
    content: &str,
    expected_pid: u32,
) -> Result<ParsedProcStat, ProcInspectError> {
    let Some(open_paren) = content.find('(') else {
        return Err(ProcInspectError::Refused(
            "malformed /proc/pid/stat: missing opening parenthesis",
        ));
    };
    let Some(close_paren) = content.rfind(')') else {
        return Err(ProcInspectError::Refused(
            "malformed /proc/pid/stat: missing closing parenthesis",
        ));
    };
    if close_paren <= open_paren {
        return Err(ProcInspectError::Refused(
            "malformed /proc/pid/stat: invalid command delimiters",
        ));
    }

    let pid_str = content[..open_paren].trim();
    let parsed_pid: u32 = pid_str
        .parse()
        .map_err(|_| ProcInspectError::Refused("malformed /proc/pid/stat: invalid PID field"))?;
    if parsed_pid != expected_pid {
        return Err(ProcInspectError::Refused(
            "/proc/pid/stat PID did not match expected process ID",
        ));
    }

    let rest = &content[close_paren + 1..];
    let fields: Vec<&str> = rest.split_ascii_whitespace().collect();
    if fields.len() < 20 {
        return Err(ProcInspectError::Refused(
            "malformed /proc/pid/stat: truncated field list",
        ));
    }

    let mut state_chars = fields[0].chars();
    let Some(state) = state_chars.next() else {
        return Err(ProcInspectError::Refused(
            "malformed /proc/pid/stat: empty state field",
        ));
    };
    if state_chars.next().is_some() {
        return Err(ProcInspectError::Refused(
            "malformed /proc/pid/stat: multi-character state field",
        ));
    }
    if matches!(state, 'Z' | 'X' | 'x') {
        return Err(ProcInspectError::DeadOrZombie);
    }

    let ppid: u32 = fields[1]
        .parse()
        .map_err(|_| ProcInspectError::Refused("malformed /proc/pid/stat: invalid PPID field"))?;

    let start_time_ticks: u64 = fields[19].parse().map_err(|_| {
        ProcInspectError::Refused("malformed /proc/pid/stat: invalid starttime field")
    })?;
    if start_time_ticks == 0 {
        return Err(ProcInspectError::Refused(
            "malformed /proc/pid/stat: zero starttime field",
        ));
    }

    Ok(ParsedProcStat {
        pid: parsed_pid,
        state,
        ppid,
        start_time_ticks,
    })
}

#[cfg(target_os = "linux")]
fn parse_proc_status_uids(content: &str) -> Result<(u32, u32, u32, u32), ProcInspectError> {
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("Uid:") {
            let parts: Vec<&str> = rest.split_ascii_whitespace().collect();
            if parts.len() < 4 {
                return Err(ProcInspectError::Refused(
                    "malformed /proc/pid/status: truncated Uid line",
                ));
            }
            let real_uid: u32 = parts[0].parse().map_err(|_| {
                ProcInspectError::Refused("malformed /proc/pid/status: invalid real UID")
            })?;
            let effective_uid: u32 = parts[1].parse().map_err(|_| {
                ProcInspectError::Refused("malformed /proc/pid/status: invalid effective UID")
            })?;
            let saved_uid: u32 = parts[2].parse().map_err(|_| {
                ProcInspectError::Refused("malformed /proc/pid/status: invalid saved UID")
            })?;
            let fs_uid: u32 = parts[3].parse().map_err(|_| {
                ProcInspectError::Refused("malformed /proc/pid/status: invalid filesystem UID")
            })?;
            return Ok((real_uid, effective_uid, saved_uid, fs_uid));
        }
    }

    Err(ProcInspectError::Refused(
        "malformed /proc/pid/status: missing Uid line",
    ))
}

#[cfg(target_os = "linux")]
fn is_io_not_found_or_esrch(err: &std::io::Error) -> bool {
    err.kind() == std::io::ErrorKind::NotFound || err.raw_os_error() == Some(3)
}

#[cfg(target_os = "linux")]
fn read_bounded_proc_text(path: &Path) -> Result<String, ProcInspectError> {
    let file = File::open(path).map_err(|err| {
        if is_io_not_found_or_esrch(&err) {
            ProcInspectError::DeadOrZombie
        } else {
            ProcInspectError::Refused("failed to open /proc process metadata file")
        }
    })?;

    let mut buf = Vec::new();
    file.take(MAX_PROC_FILE_BYTES + 1)
        .read_to_end(&mut buf)
        .map_err(|err| {
            if is_io_not_found_or_esrch(&err) {
                ProcInspectError::DeadOrZombie
            } else {
                ProcInspectError::Refused("failed to read /proc process metadata file")
            }
        })?;

    if (buf.len() as u64) > MAX_PROC_FILE_BYTES {
        return Err(ProcInspectError::Refused(
            "/proc process metadata file exceeded maximum size",
        ));
    }

    String::from_utf8(buf).map_err(|_| {
        ProcInspectError::Refused("/proc process metadata file contained invalid UTF-8")
    })
}

#[cfg(target_os = "linux")]
fn read_process_snapshot(
    proc_root: &Path,
    pid: u32,
    expected_uid: u32,
) -> Result<ProcessSnapshot, ProcInspectError> {
    use std::os::unix::fs::MetadataExt;

    if pid == 0 || i32::try_from(pid).is_err() {
        return Err(ProcInspectError::Refused("invalid process ID"));
    }

    let pid_dir = proc_root.join(pid.to_string());
    let dir_meta = fs::symlink_metadata(&pid_dir).map_err(|err| {
        if is_io_not_found_or_esrch(&err) {
            ProcInspectError::DeadOrZombie
        } else {
            ProcInspectError::Refused("failed to inspect /proc process directory")
        }
    })?;
    if dir_meta.file_type().is_symlink() || !dir_meta.file_type().is_dir() {
        return Err(ProcInspectError::Refused(
            "/proc process entry is not a real directory",
        ));
    }

    let stat_path = pid_dir.join("stat");
    let stat_text = read_bounded_proc_text(&stat_path)?;
    let parsed_stat = parse_proc_stat_content(&stat_text, pid)?;

    if dir_meta.uid() != expected_uid {
        return Err(ProcInspectError::Refused(
            "process directory UID does not match current effective UID",
        ));
    }

    let status_path = pid_dir.join("status");
    let status_text = read_bounded_proc_text(&status_path)?;
    let (real_uid, effective_uid, saved_uid, fs_uid) = parse_proc_status_uids(&status_text)?;
    if real_uid != expected_uid
        || effective_uid != expected_uid
        || saved_uid != expected_uid
        || fs_uid != expected_uid
    {
        return Err(ProcInspectError::Refused(
            "process credentials do not match current effective UID",
        ));
    }

    let exe_link = pid_dir.join("exe");
    let exe_path = match fs::read_link(&exe_link) {
        Ok(path) => path,
        Err(err) if is_io_not_found_or_esrch(&err) => {
            // Re-check stat: if the process exited or became a zombie between reading stat and exe,
            // classify as DeadOrZombie rather than a security refusal.
            match read_bounded_proc_text(&stat_path)
                .and_then(|txt| parse_proc_stat_content(&txt, pid))
            {
                Err(ProcInspectError::DeadOrZombie) => return Err(ProcInspectError::DeadOrZombie),
                _ => {
                    return Err(ProcInspectError::Refused(
                        "process executable symlink is missing",
                    ));
                }
            }
        }
        Err(_) => {
            return Err(ProcInspectError::Refused(
                "failed to read process executable symlink",
            ));
        }
    };

    let exe_meta = match fs::metadata(&exe_link) {
        Ok(meta) => meta,
        Err(err) if is_io_not_found_or_esrch(&err) => {
            match read_bounded_proc_text(&stat_path)
                .and_then(|txt| parse_proc_stat_content(&txt, pid))
            {
                Err(ProcInspectError::DeadOrZombie) => return Err(ProcInspectError::DeadOrZombie),
                _ => {
                    return Err(ProcInspectError::Refused(
                        "process executable target does not exist",
                    ));
                }
            }
        }
        Err(_) => {
            return Err(ProcInspectError::Refused(
                "failed to inspect process executable target metadata",
            ));
        }
    };
    if !exe_meta.file_type().is_file() {
        return Err(ProcInspectError::Refused(
            "process executable target is not a regular file",
        ));
    }

    // Re-read stat after capturing exe to ensure process did not change or exit mid-snapshot.
    let post_stat_text = read_bounded_proc_text(&stat_path)?;
    let post_stat = parse_proc_stat_content(&post_stat_text, pid)?;
    if post_stat.ppid != parsed_stat.ppid
        || post_stat.start_time_ticks != parsed_stat.start_time_ticks
    {
        return Err(ProcInspectError::Refused(
            "process identity changed while capturing /proc snapshot",
        ));
    }

    Ok(ProcessSnapshot {
        pid,
        ppid: parsed_stat.ppid,
        start_time_ticks: parsed_stat.start_time_ticks,
        uid: expected_uid,
        exe_path,
        exe_dev: exe_meta.dev(),
        exe_ino: exe_meta.ino(),
    })
}

#[cfg(target_os = "linux")]
fn revalidate_process_snapshot(
    proc_root: &Path,
    expected: &ProcessSnapshot,
) -> Result<(), EndpointError> {
    let current = match read_process_snapshot(proc_root, expected.pid, expected.uid) {
        Ok(snapshot) => snapshot,
        Err(ProcInspectError::DeadOrZombie) => {
            return Err(EndpointError::Unavailable(
                "native endpoint process exited before operation completed",
            ));
        }
        Err(ProcInspectError::Refused(reason)) => {
            return Err(EndpointError::Refused(reason));
        }
    };

    if current != *expected {
        return Err(EndpointError::Refused(
            "native endpoint process snapshot identity changed",
        ));
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn is_pid_alive_non_zombie(proc_root: &Path, pid: u32) -> Result<bool, EndpointError> {
    if pid == 0 || i32::try_from(pid).is_err() {
        return Err(EndpointError::Refused("invalid process ID in descriptor"));
    }
    let stat_path = proc_root.join(pid.to_string()).join("stat");
    match read_bounded_proc_text(&stat_path).and_then(|text| parse_proc_stat_content(&text, pid)) {
        Ok(_) => Ok(true),
        Err(ProcInspectError::DeadOrZombie) => Ok(false),
        Err(ProcInspectError::Refused(reason)) => Err(EndpointError::Refused(reason)),
    }
}

#[cfg(target_os = "linux")]
fn verify_candidate_process_chain(
    proc_root: &Path,
    descriptor: &LauncherDescriptor,
    expected_backend_exe: &Path,
    expected_backend_id: (u64, u64),
    expected_app_exe: &Path,
    expected_app_id: (u64, u64),
    expected_uid: u32,
) -> Result<Option<(ProcessSnapshot, ProcessSnapshot, ProcessSnapshot)>, EndpointError> {
    // First check whether the advertised backend PID is alive and non-zombie.
    // Stale descriptors left behind by exited/crashed backend processes are ignored.
    if !is_pid_alive_non_zombie(proc_root, descriptor.backend_pid)? {
        return Ok(None);
    }

    let backend_snapshot =
        match read_process_snapshot(proc_root, descriptor.backend_pid, expected_uid) {
            Ok(snapshot) => snapshot,
            Err(ProcInspectError::DeadOrZombie) => return Ok(None),
            Err(ProcInspectError::Refused(reason)) => return Err(EndpointError::Refused(reason)),
        };

    if descriptor.real_codex != expected_backend_exe {
        let foreign_identity =
            validate_canonical_executable(&descriptor.real_codex, "unselected native backend")
                .map_err(|_| {
                    EndpointError::Refused("unselected native backend violated executable policy")
                })?;
        if backend_snapshot.exe_path != descriptor.real_codex
            || (backend_snapshot.exe_dev, backend_snapshot.exe_ino) != foreign_identity
        {
            return Err(EndpointError::Refused(
                "unselected native descriptor disagrees with its live backend executable",
            ));
        }
        // A private descriptor belonging to another real installation is not a candidate.
        return Ok(None);
    }

    if backend_snapshot.exe_path != expected_backend_exe
        || (backend_snapshot.exe_dev, backend_snapshot.exe_ino) != expected_backend_id
    {
        return Err(EndpointError::Refused(
            "native reconnect backend process executable does not match configured backend_executable",
        ));
    }

    if backend_snapshot.ppid <= 1 && !is_pid_alive_non_zombie(proc_root, descriptor.wrapper_pid)? {
        // Backend was orphaned and reparented to init/PID 1 after its wrapper exited.
        return Ok(None);
    }

    if backend_snapshot.ppid != descriptor.wrapper_pid {
        return Err(EndpointError::Refused(
            "native reconnect backend parent PID does not match descriptor wrapper_pid",
        ));
    }

    let wrapper_snapshot =
        match read_process_snapshot(proc_root, descriptor.wrapper_pid, expected_uid) {
            Ok(snapshot) => snapshot,
            Err(ProcInspectError::DeadOrZombie) => return Ok(None),
            Err(ProcInspectError::Refused(reason)) => return Err(EndpointError::Refused(reason)),
        };

    let app_pid = wrapper_snapshot.ppid;
    if app_pid <= 1 {
        // Wrapper was orphaned and reparented to init/PID 1 when the desktop app exited.
        return Ok(None);
    }
    if app_pid == descriptor.wrapper_pid || app_pid == descriptor.backend_pid {
        return Err(EndpointError::Refused(
            "native reconnect wrapper parent PID forms an invalid cycle",
        ));
    }

    let app_snapshot = match read_process_snapshot(proc_root, app_pid, expected_uid) {
        Ok(snapshot) => snapshot,
        Err(ProcInspectError::DeadOrZombie) => return Ok(None),
        Err(ProcInspectError::Refused(reason)) => return Err(EndpointError::Refused(reason)),
    };

    if app_snapshot.exe_path != expected_app_exe
        || (app_snapshot.exe_dev, app_snapshot.exe_ino) != expected_app_id
    {
        return Err(EndpointError::Refused(
            "native reconnect application process executable does not match configured application_executable",
        ));
    }

    // Re-check wrapper and backend snapshots so a parent exit during chain traversal cannot slip through.
    revalidate_process_snapshot(proc_root, &wrapper_snapshot)?;
    revalidate_process_snapshot(proc_root, &backend_snapshot)?;

    Ok(Some((backend_snapshot, wrapper_snapshot, app_snapshot)))
}

#[cfg(target_os = "linux")]
fn validate_candidate_socket(
    session_dir: &Path,
    advertised_socket_path: &Path,
    expected_uid: u32,
) -> Result<Option<(PathBuf, u64, u64)>, EndpointError> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};

    if advertised_socket_path.parent() != Some(session_dir) {
        return Err(EndpointError::Refused(
            "advertised native socket parent directory must equal its session directory",
        ));
    }

    let adv_meta = match fs::symlink_metadata(advertised_socket_path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(EndpointError::Refused(
                "failed to inspect advertised native socket",
            ));
        }
    };

    if adv_meta.uid() != expected_uid {
        return Err(EndpointError::Refused(
            "advertised native socket is not owned by the current effective UID",
        ));
    }

    let (resolved_socket_path, target_meta) = if adv_meta.file_type().is_symlink() {
        let canonical_target = match fs::canonicalize(advertised_socket_path) {
            Ok(target) => target,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => {
                return Err(EndpointError::Refused(
                    "advertised native socket symlink target could not be resolved",
                ));
            }
        };
        validate_normalized_absolute_path(&canonical_target, "resolved native socket").map_err(
            |_| {
                EndpointError::Refused(
                    "resolved native socket path violated canonical no-symlink policy",
                )
            },
        )?;
        let Some(target_parent) = canonical_target.parent() else {
            return Err(EndpointError::Refused(
                "resolved native socket path must have a direct parent directory",
            ));
        };
        let _ = validate_private_canonical_dir(
            target_parent,
            expected_uid,
            "resolved native socket parent directory",
        )
        .map_err(|_| {
            EndpointError::Refused(
                "resolved native socket parent directory violated private 0700 directory policy",
            )
        })?;

        let target_meta = fs::symlink_metadata(&canonical_target).map_err(|_| {
            EndpointError::Refused("failed to inspect resolved native socket target")
        })?;
        (canonical_target, target_meta)
    } else if adv_meta.file_type().is_socket() {
        (advertised_socket_path.to_path_buf(), adv_meta)
    } else {
        return Err(EndpointError::Refused(
            "advertised native socket path is neither a Unix socket nor a symlink",
        ));
    };

    if target_meta.file_type().is_symlink() || !target_meta.file_type().is_socket() {
        return Err(EndpointError::Refused(
            "resolved native socket target is not a Unix domain socket",
        ));
    }
    if target_meta.uid() != expected_uid {
        return Err(EndpointError::Refused(
            "resolved native socket target is not owned by the current effective UID",
        ));
    }
    if (target_meta.permissions().mode() & 0o077) != 0 {
        return Err(EndpointError::Refused(
            "resolved native socket target has insecure group or world permissions",
        ));
    }

    Ok(Some((
        resolved_socket_path,
        target_meta.dev(),
        target_meta.ino(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_sensitive_paths_and_rejects_unknown_json_fields() {
        let cfg = NativeReconnectConfig {
            descriptor_root: PathBuf::from("/tmp/secret-runtime-dir"),
            backend_executable: PathBuf::from("/tmp/secret-backend-bin/codex"),
            application_executable: PathBuf::from("/tmp/secret-app-bin/chatgpt"),
        };

        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("secret-runtime-dir"));
        assert!(!dbg.contains("secret-backend-bin"));
        assert!(!dbg.contains("secret-app-bin"));
        assert!(dbg.contains("[REDACTED]"));

        let encoded = serde_json::to_string(&cfg).expect("serialize reconnect config");
        let decoded: NativeReconnectConfig =
            serde_json::from_str(&encoded).expect("deserialize reconnect config");
        assert_eq!(decoded, cfg);

        let unknown = serde_json::from_str::<NativeReconnectConfig>(
            r#"{
                "descriptor_root": "/tmp/a",
                "backend_executable": "/tmp/b",
                "application_executable": "/tmp/c",
                "extra": true
            }"#,
        );
        assert!(unknown.is_err());
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn non_linux_fails_closed() {
        let cfg = NativeReconnectConfig {
            descriptor_root: PathBuf::from("/tmp/runtime"),
            backend_executable: PathBuf::from("/tmp/codex"),
            application_executable: PathBuf::from("/tmp/app"),
        };
        assert!(matches!(cfg.validate(), Err(Error::UnsupportedPlatform(_))));
        assert!(matches!(cfg.resolve(), Err(EndpointError::Refused(_))));
    }

    #[cfg(target_os = "linux")]
    mod linux_tests {
        use super::*;
        use serde_json::json;
        use std::os::unix::fs::{symlink, PermissionsExt};
        use std::os::unix::net::UnixListener;

        struct SyntheticFixture {
            _temp: tempfile::TempDir,
            root: PathBuf,
            descriptor_root: PathBuf,
            proc_root: PathBuf,
            backend_exe: PathBuf,
            wrapper_exe: PathBuf,
            app_exe: PathBuf,
            other_exe: PathBuf,
            config: NativeReconnectConfig,
            uid: u32,
        }

        impl SyntheticFixture {
            fn new() -> Self {
                let temp = tempfile::tempdir().expect("tempdir");
                let root = fs::canonicalize(temp.path()).expect("canonical tempdir");
                let uid = current_euid();

                let descriptor_root = root.join("runtime");
                fs::create_dir(&descriptor_root).expect("create descriptor_root");
                fs::set_permissions(&descriptor_root, fs::Permissions::from_mode(0o700))
                    .expect("chmod 0700 descriptor_root");

                let proc_root = root.join("proc");
                fs::create_dir(&proc_root).expect("create proc_root");

                let bin_dir = root.join("bin");
                fs::create_dir(&bin_dir).expect("create bin_dir");

                let backend_exe = bin_dir.join("codex-backend");
                let wrapper_exe = bin_dir.join("python-wrapper");
                let app_exe = bin_dir.join("codex-desktop-app");
                let other_exe = bin_dir.join("other-codex-backend");

                for exe in [&backend_exe, &wrapper_exe, &app_exe, &other_exe] {
                    fs::write(exe, b"#!/bin/sh\nexit 0\n").expect("write synthetic exe");
                    fs::set_permissions(exe, fs::Permissions::from_mode(0o755))
                        .expect("chmod 0755 exe");
                }

                let config = NativeReconnectConfig {
                    descriptor_root: descriptor_root.clone(),
                    backend_executable: backend_exe.clone(),
                    application_executable: app_exe.clone(),
                };

                Self {
                    _temp: temp,
                    root,
                    descriptor_root,
                    proc_root,
                    backend_exe,
                    wrapper_exe,
                    app_exe,
                    other_exe,
                    config,
                    uid,
                }
            }

            #[allow(
                clippy::too_many_arguments,
                reason = "Synthetic stat/status fixtures specify independent identity fields"
            )]
            fn write_proc_entry(
                &self,
                pid: u32,
                comm: &str,
                state: char,
                ppid: u32,
                start_time_ticks: u64,
                uid: u32,
                exe_target: Option<&Path>,
            ) {
                let pid_dir = self.proc_root.join(pid.to_string());
                let _ = fs::remove_dir_all(&pid_dir);
                fs::create_dir_all(&pid_dir).expect("create proc pid dir");

                // Field 1: pid, Field 2: (comm), Field 3: state, Field 4: ppid,
                // Fields 5..=21: 17 zeros, Field 22: starttime, Field 23: vsize.
                let zeros = vec!["0"; 17].join(" ");
                let stat_line =
                    format!("{pid} ({comm}) {state} {ppid} {zeros} {start_time_ticks} 0\n");
                fs::write(pid_dir.join("stat"), stat_line).expect("write proc stat");

                let status_text = format!(
                    "Name:\tsynthetic\nState:\t{state}\nPid:\t{pid}\nPPid:\t{ppid}\nUid:\t{uid}\t{uid}\t{uid}\t{uid}\n"
                );
                fs::write(pid_dir.join("status"), status_text).expect("write proc status");

                if let Some(target) = exe_target {
                    symlink(target, pid_dir.join("exe")).expect("symlink proc exe");
                }
            }

            fn write_valid_proc_chain(
                &self,
                app_pid: u32,
                wrapper_pid: u32,
                backend_pid: u32,
                start_time_ticks: u64,
            ) {
                self.write_proc_entry(
                    app_pid,
                    "Codex Desktop (App)",
                    'S',
                    1,
                    start_time_ticks,
                    self.uid,
                    Some(&self.app_exe),
                );
                self.write_proc_entry(
                    wrapper_pid,
                    "python3 (wrapper)",
                    'S',
                    app_pid,
                    start_time_ticks + 10,
                    self.uid,
                    Some(&self.wrapper_exe),
                );
                self.write_proc_entry(
                    backend_pid,
                    "codex-threadlane-original",
                    'S',
                    wrapper_pid,
                    start_time_ticks + 20,
                    self.uid,
                    Some(&self.backend_exe),
                );
            }

            fn create_session_candidate(
                &self,
                suffix: &str,
                wrapper_pid: u32,
                backend_pid: u32,
                real_codex: &Path,
            ) -> (PathBuf, UnixListener) {
                let session_dir = self
                    .descriptor_root
                    .join(format!("{SESSION_DIR_PREFIX}{suffix}"));
                fs::create_dir(&session_dir).expect("create session_dir");
                fs::set_permissions(&session_dir, fs::Permissions::from_mode(0o700))
                    .expect("chmod 0700 session_dir");

                let sock_path = session_dir.join("s.sock");
                let listener = UnixListener::bind(&sock_path).expect("bind session socket");
                fs::set_permissions(&sock_path, fs::Permissions::from_mode(0o600))
                    .expect("chmod 0600 socket");

                let descriptor_path = session_dir.join(ENDPOINT_DESCRIPTOR_FILENAME);
                let payload = json!({
                    "schema_version": 1,
                    "wrapper_pid": wrapper_pid,
                    "backend_pid": backend_pid,
                    "socket_path": sock_path,
                    "real_codex": real_codex,
                    "transport": "unix_websocket",
                });
                fs::write(
                    &descriptor_path,
                    serde_json::to_vec_pretty(&payload).expect("serialize descriptor"),
                )
                .expect("write descriptor");
                fs::set_permissions(&descriptor_path, fs::Permissions::from_mode(0o600))
                    .expect("chmod 0600 descriptor");

                (session_dir, listener)
            }
        }

        #[test]
        fn validate_enforces_private_dir_and_canonical_executable_policies() {
            let fix = SyntheticFixture::new();
            fix.config.validate().expect("valid baseline config");

            let wrong_uid = fix.uid.wrapping_add(1);
            assert!(fix.config.validate_with_euid(wrong_uid).is_err());

            // Insecure descriptor_root permissions (0755).
            fs::set_permissions(&fix.descriptor_root, fs::Permissions::from_mode(0o755))
                .expect("chmod 0755");
            assert!(fix.config.validate().is_err());
            fs::set_permissions(&fix.descriptor_root, fs::Permissions::from_mode(0o700))
                .expect("restore 0700");

            // Non-executable backend_executable (0644).
            fs::set_permissions(&fix.backend_exe, fs::Permissions::from_mode(0o644))
                .expect("chmod 0644");
            assert!(fix.config.validate().is_err());
            fs::set_permissions(&fix.backend_exe, fs::Permissions::from_mode(0o755))
                .expect("restore 0755");

            // Symlink executable path must be rejected.
            let symlink_exe = fix.root.join("symlink-codex");
            symlink(&fix.backend_exe, &symlink_exe).expect("create symlink exe");
            let mut bad_cfg = fix.config.clone();
            bad_cfg.backend_executable = symlink_exe;
            assert!(bad_cfg.validate().is_err());

            // Relative path or `..` traversal must be rejected.
            let mut rel_cfg = fix.config.clone();
            rel_cfg.descriptor_root = PathBuf::from("relative/runtime");
            assert!(rel_cfg.validate().is_err());

            let mut dotdot_cfg = fix.config.clone();
            dotdot_cfg.descriptor_root = fix.descriptor_root.join("..").join("runtime");
            assert!(dotdot_cfg.validate().is_err());
        }

        #[test]
        fn resolve_rejects_malformed_oversized_and_insecure_candidates() {
            let fix = SyntheticFixture::new();
            fix.write_valid_proc_chain(100, 200, 300, 1_000);

            // 1. Insecure session directory mode (0755) fails closed.
            let (session_dir, _listener) =
                fix.create_session_candidate("insecure-dir", 200, 300, &fix.backend_exe);
            fs::set_permissions(&session_dir, fs::Permissions::from_mode(0o755))
                .expect("chmod 0755 session_dir");
            assert!(matches!(
                fix.config.resolve_with_context(&fix.proc_root, fix.uid),
                Err(EndpointError::Refused(_))
            ));
            fs::set_permissions(&session_dir, fs::Permissions::from_mode(0o700))
                .expect("restore 0700 session_dir");

            // 2. Insecure descriptor.json mode (0644) fails closed.
            let descriptor_path = session_dir.join(ENDPOINT_DESCRIPTOR_FILENAME);
            fs::set_permissions(&descriptor_path, fs::Permissions::from_mode(0o644))
                .expect("chmod 0644 descriptor");
            assert!(matches!(
                fix.config.resolve_with_context(&fix.proc_root, fix.uid),
                Err(EndpointError::Refused(_))
            ));

            // 3. Oversized descriptor.json (> 16 KiB) fails closed.
            let oversized = vec![b' '; (MAX_ENDPOINT_DESCRIPTOR_BYTES as usize) + 1];
            fs::write(&descriptor_path, &oversized).expect("write oversized descriptor");
            fs::set_permissions(&descriptor_path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 descriptor");
            assert!(matches!(
                fix.config.resolve_with_context(&fix.proc_root, fix.uid),
                Err(EndpointError::Refused(_))
            ));

            // 4. Malformed JSON / unknown field fails closed.
            fs::write(
                &descriptor_path,
                br#"{"schema_version": 1, "unknown": true}"#,
            )
            .expect("write malformed descriptor");
            fs::set_permissions(&descriptor_path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 descriptor");
            assert!(matches!(
                fix.config.resolve_with_context(&fix.proc_root, fix.uid),
                Err(EndpointError::Refused(_))
            ));

            // 5. Symlink descriptor.json fails closed.
            fs::remove_file(&descriptor_path).expect("remove descriptor");
            let outside_desc = fix.root.join("outside-descriptor.json");
            fs::write(&outside_desc, b"{}").expect("write outside descriptor");
            fs::set_permissions(&outside_desc, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 outside");
            symlink(&outside_desc, &descriptor_path).expect("symlink descriptor");
            assert!(matches!(
                fix.config.resolve_with_context(&fix.proc_root, fix.uid),
                Err(EndpointError::Refused(_))
            ));
        }

        #[test]
        fn resolve_ignores_stale_dead_or_zombie_candidates_and_resolves_live_candidate() {
            let fix = SyntheticFixture::new();

            // Empty descriptor_root -> Unavailable.
            assert!(matches!(
                fix.config.resolve_with_context(&fix.proc_root, fix.uid),
                Err(EndpointError::Unavailable(_))
            ));

            // Candidate 1: dead non-existent PIDs (4040 / 4041).
            let (_stale_dir_1, _stale_listener_1) =
                fix.create_session_candidate("dead-pids", 4040, 4041, &fix.backend_exe);

            // Candidate 2: zombie backend process ('Z').
            fix.write_valid_proc_chain(500, 501, 502, 2_000);
            fix.write_proc_entry(
                502,
                "codex-zombie",
                'Z',
                501,
                2_020,
                fix.uid,
                Some(&fix.backend_exe),
            );
            let (_stale_dir_2, _stale_listener_2) =
                fix.create_session_candidate("zombie-backend", 501, 502, &fix.backend_exe);

            // Only dead/zombie candidates exist -> Unavailable.
            assert!(matches!(
                fix.config.resolve_with_context(&fix.proc_root, fix.uid),
                Err(EndpointError::Unavailable(_))
            ));

            // Candidate 3: live valid process chain (600 -> 601 -> 602).
            fix.write_valid_proc_chain(600, 601, 602, 3_000);
            let (live_dir, _live_listener) =
                fix.create_session_candidate("live-current", 601, 602, &fix.backend_exe);

            let resolved = fix
                .config
                .resolve_with_context(&fix.proc_root, fix.uid)
                .expect("should ignore stale candidates and resolve live candidate");
            assert_eq!(resolved.socket_path, live_dir.join("s.sock"));
            resolved
                .validate_processes()
                .expect("live snapshots remain valid");
        }

        #[test]
        fn resolve_rejects_ambiguous_multiple_live_matching_candidates() {
            let fix = SyntheticFixture::new();
            fix.write_valid_proc_chain(100, 200, 300, 1_000);
            fix.write_valid_proc_chain(100, 201, 301, 1_100);

            let (_dir_a, _listener_a) =
                fix.create_session_candidate("live-a", 200, 300, &fix.backend_exe);
            let (_dir_b, _listener_b) =
                fix.create_session_candidate("live-b", 201, 301, &fix.backend_exe);

            assert!(matches!(
                fix.config.resolve_with_context(&fix.proc_root, fix.uid),
                Err(EndpointError::Refused(_))
            ));
        }

        #[test]
        fn resolve_rejects_tampered_installation_claim_and_broken_process_ancestry() {
            let fix = SyntheticFixture::new();
            fix.write_valid_proc_chain(100, 200, 300, 1_000);

            // 1. Descriptor real_codex points to a different installation.
            let (session_dir, _listener) =
                fix.create_session_candidate("wrong-install", 200, 300, &fix.other_exe);
            assert!(matches!(
                fix.config.resolve_with_context(&fix.proc_root, fix.uid),
                Err(EndpointError::Refused(_))
            ));
            fs::remove_dir_all(&session_dir).expect("cleanup session_dir");

            // 2. Descriptor real_codex matches config, but /proc/<backend_pid>/exe is a different binary.
            let (session_dir, _listener) =
                fix.create_session_candidate("wrong-proc-exe", 200, 300, &fix.backend_exe);
            fix.write_proc_entry(
                300,
                "codex-other",
                'S',
                200,
                1_020,
                fix.uid,
                Some(&fix.other_exe),
            );
            assert!(matches!(
                fix.config.resolve_with_context(&fix.proc_root, fix.uid),
                Err(EndpointError::Refused(_))
            ));

            // 3. Restore backend exe, but break backend PPID != wrapper_pid.
            fix.write_proc_entry(
                300,
                "codex-backend",
                'S',
                999,
                1_020,
                fix.uid,
                Some(&fix.backend_exe),
            );
            assert!(matches!(
                fix.config.resolve_with_context(&fix.proc_root, fix.uid),
                Err(EndpointError::Refused(_))
            ));

            // 4. Restore backend PPID, but set application exe to wrong executable.
            fix.write_proc_entry(
                300,
                "codex-backend",
                'S',
                200,
                1_020,
                fix.uid,
                Some(&fix.backend_exe),
            );
            fix.write_proc_entry(
                100,
                "wrong-app",
                'S',
                1,
                1_000,
                fix.uid,
                Some(&fix.other_exe),
            );
            assert!(matches!(
                fix.config.resolve_with_context(&fix.proc_root, fix.uid),
                Err(EndpointError::Refused(_))
            ));
            let _ = session_dir;
        }

        #[test]
        fn unrelated_live_installation_is_ignored_without_blocking_the_selected_one() {
            let fix = SyntheticFixture::new();
            fix.write_valid_proc_chain(100, 200, 300, 1_000);
            fix.write_proc_entry(
                300,
                "other-installation",
                'S',
                200,
                1_020,
                fix.uid,
                Some(&fix.other_exe),
            );
            let (_foreign, _foreign_listener) =
                fix.create_session_candidate("foreign", 200, 300, &fix.other_exe);
            assert!(matches!(
                fix.config.resolve_with_context(&fix.proc_root, fix.uid),
                Err(EndpointError::Unavailable(_))
            ));

            fix.write_valid_proc_chain(400, 500, 600, 2_000);
            let (selected, _selected_listener) =
                fix.create_session_candidate("selected", 500, 600, &fix.backend_exe);
            let resolved = fix
                .config
                .resolve_with_context(&fix.proc_root, fix.uid)
                .unwrap();
            assert_eq!(resolved.socket_path, selected.join("s.sock"));
            assert_eq!(resolved.backend.pid, 600);
        }

        #[test]
        fn resolve_supports_symlinked_socket_in_private_dir_and_revalidates_snapshots() {
            let fix = SyntheticFixture::new();
            fix.write_valid_proc_chain(100, 200, 300, 5_000);

            let phys_dir = fix.root.join("physical-sock-dir");
            fs::create_dir(&phys_dir).expect("create phys_dir");
            fs::set_permissions(&phys_dir, fs::Permissions::from_mode(0o700))
                .expect("chmod 0700 phys_dir");
            let phys_sock = phys_dir.join("codex-ipc.sock");
            let _listener = UnixListener::bind(&phys_sock).expect("bind phys_sock");
            fs::set_permissions(&phys_sock, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 phys_sock");

            let session_dir = fix.descriptor_root.join("tl-codex-symlink-sock");
            fs::create_dir(&session_dir).expect("create session_dir");
            fs::set_permissions(&session_dir, fs::Permissions::from_mode(0o700))
                .expect("chmod 0700 session_dir");

            let adv_sock = session_dir.join("s.sock");
            symlink(&phys_sock, &adv_sock).expect("symlink s.sock -> phys_sock");

            let descriptor_path = session_dir.join(ENDPOINT_DESCRIPTOR_FILENAME);
            let payload = json!({
                "schema_version": 1,
                "wrapper_pid": 200,
                "backend_pid": 300,
                "socket_path": adv_sock,
                "real_codex": fix.backend_exe,
                "transport": "unix_websocket",
            });
            fs::write(
                &descriptor_path,
                serde_json::to_vec_pretty(&payload).expect("serialize"),
            )
            .expect("write descriptor");
            fs::set_permissions(&descriptor_path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 descriptor");

            let resolved = fix
                .config
                .resolve_with_context(&fix.proc_root, fix.uid)
                .expect("resolve symlinked socket");
            assert_eq!(resolved.socket_path, phys_sock);
            resolved.validate_processes().expect("processes valid");

            // Simulated PID reuse (starttime changed) must be refused on revalidation.
            fix.write_proc_entry(
                300,
                "codex-reused-pid",
                'S',
                200,
                9_999,
                fix.uid,
                Some(&fix.backend_exe),
            );
            assert!(matches!(
                resolved.validate_processes(),
                Err(EndpointError::Refused(_))
            ));

            // Simulated process exit before turn/start must report Unavailable.
            fs::remove_dir_all(fix.proc_root.join("300")).expect("remove proc 300");
            assert!(matches!(
                resolved.validate_processes(),
                Err(EndpointError::Unavailable(_))
            ));
        }

        #[test]
        fn publication_transitions_wait_without_accepting_malformed_descriptors() {
            let fix = SyntheticFixture::new();
            fix.write_valid_proc_chain(100, 200, 300, 5_000);
            let (session_dir, listener) =
                fix.create_session_candidate("publishing", 200, 300, &fix.backend_exe);
            let descriptor = session_dir.join(ENDPOINT_DESCRIPTOR_FILENAME);
            let complete = fs::read(&descriptor).unwrap();

            fs::write(&descriptor, b"").unwrap();
            assert!(matches!(
                fix.config.resolve_with_context(&fix.proc_root, fix.uid),
                Err(EndpointError::Unavailable(_))
            ));
            fs::write(&descriptor, b"{").unwrap();
            assert!(matches!(
                fix.config.resolve_with_context(&fix.proc_root, fix.uid),
                Err(EndpointError::Refused(_))
            ));
            fs::write(&descriptor, &complete).unwrap();
            let resolved = fix
                .config
                .resolve_with_context(&fix.proc_root, fix.uid)
                .unwrap();

            drop(listener);
            fs::remove_file(&resolved.socket_path).unwrap();
            let physical_dir = fix.root.join("pending-physical");
            fs::create_dir(&physical_dir).unwrap();
            fs::set_permissions(&physical_dir, fs::Permissions::from_mode(0o700)).unwrap();
            let pending_socket = physical_dir.join("pending.sock");
            symlink(&pending_socket, &resolved.socket_path).unwrap();
            assert!(matches!(
                fix.config.resolve_with_context(&fix.proc_root, fix.uid),
                Err(EndpointError::Unavailable(_))
            ));
            let _new_listener = UnixListener::bind(&pending_socket).unwrap();
            fs::set_permissions(&pending_socket, fs::Permissions::from_mode(0o600)).unwrap();
            assert_eq!(
                fix.config
                    .resolve_with_context(&fix.proc_root, fix.uid)
                    .unwrap()
                    .socket_path,
                pending_socket
            );
        }

        #[test]
        fn real_proc_and_unix_stream_peer_cred_validation() {
            let fix = SyntheticFixture::new();
            let self_pid = std::process::id();

            let self_snap = read_process_snapshot(Path::new("/proc"), self_pid, fix.uid)
                .expect("read current process snapshot from real /proc");
            let parent_snap = read_process_snapshot(Path::new("/proc"), self_snap.ppid, fix.uid)
                .expect("read parent process snapshot from real /proc");

            // Populate synthetic /proc for app -> wrapper -> backend(self_pid) using real self_pid snapshot
            // and also test validate_peer against a real tokio::net::UnixStream.
            let app_pid = if parent_snap.ppid > 1 {
                parent_snap.ppid
            } else {
                90_001
            };
            fix.write_proc_entry(
                app_pid,
                "synthetic-app",
                'S',
                1,
                100,
                fix.uid,
                Some(&fix.app_exe),
            );
            fix.write_proc_entry(
                self_snap.ppid,
                "synthetic-wrapper",
                'S',
                app_pid,
                200,
                fix.uid,
                Some(&fix.wrapper_exe),
            );
            fix.write_proc_entry(
                self_pid,
                "synthetic-backend",
                'S',
                self_snap.ppid,
                self_snap.start_time_ticks,
                fix.uid,
                Some(&fix.backend_exe),
            );

            let (_session_dir, _listener) = fix.create_session_candidate(
                "peer-cred",
                self_snap.ppid,
                self_pid,
                &fix.backend_exe,
            );

            let resolved = fix
                .config
                .resolve_with_context(&fix.proc_root, fix.uid)
                .expect("resolve candidate for self_pid");

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build current-thread tokio runtime");

            rt.block_on(async {
                let stream = tokio::net::UnixStream::connect(&resolved.socket_path)
                    .await
                    .expect("connect to local unix listener");
                resolved
                    .validate_peer(&stream)
                    .expect("peer_cred UID and PID match self_pid");

                let mut wrong_pid_endpoint = resolved.clone();
                wrong_pid_endpoint.backend.pid = self_pid.saturating_add(1);
                assert!(matches!(
                    wrong_pid_endpoint.validate_peer(&stream),
                    Err(EndpointError::Refused(_))
                ));

                let mut wrong_uid_endpoint = resolved.clone();
                wrong_uid_endpoint.backend.uid = fix.uid.wrapping_add(1);
                assert!(matches!(
                    wrong_uid_endpoint.validate_peer(&stream),
                    Err(EndpointError::Refused(_))
                ));
            });

            if parent_snap.ppid > 1 {
                if let Ok(grandparent_snap) =
                    read_process_snapshot(Path::new("/proc"), parent_snap.ppid, fix.uid)
                {
                    let real_proc_root = fix.root.join("real-proc-runtime");
                    fs::create_dir(&real_proc_root).expect("create real_proc_root");
                    fs::set_permissions(&real_proc_root, fs::Permissions::from_mode(0o700))
                        .expect("chmod 0700 real_proc_root");

                    let real_session_dir = real_proc_root.join("tl-codex-real-proc");
                    fs::create_dir(&real_session_dir).expect("create real_session_dir");
                    fs::set_permissions(&real_session_dir, fs::Permissions::from_mode(0o700))
                        .expect("chmod 0700 real_session_dir");

                    let real_sock = real_session_dir.join("s.sock");
                    let _real_listener = UnixListener::bind(&real_sock).expect("bind real_sock");
                    fs::set_permissions(&real_sock, fs::Permissions::from_mode(0o600))
                        .expect("chmod 0600 real_sock");

                    let real_desc = real_session_dir.join(ENDPOINT_DESCRIPTOR_FILENAME);
                    let payload = json!({
                        "schema_version": 1,
                        "wrapper_pid": self_snap.ppid,
                        "backend_pid": self_pid,
                        "socket_path": real_sock,
                        "real_codex": self_snap.exe_path,
                        "transport": "unix_websocket",
                    });
                    fs::write(
                        &real_desc,
                        serde_json::to_vec_pretty(&payload).expect("serialize real_desc"),
                    )
                    .expect("write real_desc");
                    fs::set_permissions(&real_desc, fs::Permissions::from_mode(0o600))
                        .expect("chmod 0600 real_desc");

                    let real_cfg = NativeReconnectConfig {
                        descriptor_root: real_proc_root,
                        backend_executable: self_snap.exe_path,
                        application_executable: grandparent_snap.exe_path,
                    };
                    real_cfg.validate().expect("validate real_cfg");
                    let real_resolved = real_cfg.resolve().expect("resolve against real /proc");
                    assert_eq!(real_resolved.socket_path, real_sock);
                    real_resolved
                        .validate_processes()
                        .expect("validate_processes against real /proc");
                }
            }
        }
    }
}
