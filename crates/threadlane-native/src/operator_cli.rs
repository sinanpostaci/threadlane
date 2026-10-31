use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};

use serde::{de::DeserializeOwned, Serialize};
use threadlane::{ErrorCode as RuntimeErrorCode, SupervisorClient};
use threadlane_authority::validation::{validate_canonical_thread_uuid, validate_identifier};

use crate::fs_safety::ensure_platform_supported;

/// Maximum byte size (`1 MiB`) permitted for private JSON request files and bounded stdout JSON
/// responses (including the trailing newline).
const MAX_IO_BYTES: usize = 1_048_576;

/// Maximum byte size (`4 KiB`) permitted for a static stderr JSON error line.
const MAX_ERROR_LINE_BYTES: usize = 4_096;

/// Route identifier emitted in the trusted local operator result envelope.
const TRUSTED_LOCAL_OPERATOR_ROUTE: &str = "trusted_local_operator";

/// Error code emitted when an operation that may already have effects succeeded before its bounded
/// stdout response could be serialized and flushed.
const OUTCOME_UNKNOWN_CODE: &str = "outcome_unknown";

/// Static, sanitized error emitted by trusted local operator CLI binaries.
///
/// Carries a machine-readable `snake_case` `code` and a static user-safe `message` that never
/// exposes raw request bodies, secrets, filesystem paths, or raw library/provider diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OperatorCliError {
    /// Stable machine-readable `snake_case` error code.
    pub code: &'static str,
    /// Static, sanitized English description of the error.
    pub message: &'static str,
}

impl OperatorCliError {
    /// Creates a new [`OperatorCliError`] with a static `code` and static `message`.
    pub const fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }

    /// Creates an `invalid_input` [`OperatorCliError`] with a static `message`.
    pub const fn invalid_input(message: &'static str) -> Self {
        Self::new("invalid_input", message)
    }
}

impl fmt::Display for OperatorCliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for OperatorCliError {}

#[derive(Debug, Serialize)]
struct OperatorResultEnvelope<'a, T: ?Sized> {
    untrusted_data: bool,
    route: &'static str,
    native_per_call_identity_verified: bool,
    binding_id: &'a str,
    data: &'a T,
}

/// Validates a slice of `(label, path)` store directory entries.
///
/// Every entry must be a normalized absolute path referring to an existing, canonical,
/// non-symlink private directory (`0700` owner-only permissions on Unix), and all entries must be
/// pairwise distinct and non-overlapping. Never creates missing directories or modifies
/// permissions. Labels are used only for internal input validation; all returned errors are
/// static and sanitized.
pub fn validate_dirs(entries: &[(&str, &Path)]) -> Result<Vec<PathBuf>, OperatorCliError> {
    ensure_platform_supported().map_err(|_| {
        OperatorCliError::new(
            "unsupported_platform",
            "trusted local operator CLI is only supported on Unix platforms",
        )
    })?;

    if entries.is_empty() {
        return Err(OperatorCliError::invalid_input(
            "at least one store directory entry is required",
        ));
    }

    let mut validated = Vec::with_capacity(entries.len());
    for (idx, (label, path)) in entries.iter().enumerate() {
        if label.trim().is_empty() {
            return Err(OperatorCliError::invalid_input(
                "store directory entry label must not be empty",
            ));
        }
        for (prev_label, _) in &entries[..idx] {
            if prev_label == label {
                return Err(OperatorCliError::invalid_input(
                    "store directory entry labels must be pairwise distinct",
                ));
            }
        }
        let clean = validate_single_existing_private_dir(path)?;
        validated.push(clean);
    }

    for i in 0..validated.len() {
        for j in (i + 1)..validated.len() {
            if validated[i] == validated[j]
                || validated[i].starts_with(&validated[j])
                || validated[j].starts_with(&validated[i])
            {
                return Err(OperatorCliError::new(
                    "security_violation",
                    "store directories must be pairwise distinct and non-overlapping",
                ));
            }
        }
    }

    Ok(validated)
}

#[cfg(unix)]
fn validate_single_existing_private_dir(path: &Path) -> Result<PathBuf, OperatorCliError> {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;

    let raw_bytes = path.as_os_str().as_bytes();
    if raw_bytes.is_empty() || !path.is_absolute() || raw_bytes.ends_with(b"/") {
        return Err(OperatorCliError::invalid_input(
            "store directory path must be a normalized absolute directory path",
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
            Component::CurDir | Component::ParentDir => {
                return Err(OperatorCliError::new(
                    "security_violation",
                    "store directory path must not contain relative or traversal components",
                ));
            }
            _ => {
                return Err(OperatorCliError::invalid_input(
                    "store directory path must be a normalized absolute directory path",
                ));
            }
        }
    }

    if !saw_root || normal_parts.is_empty() {
        return Err(OperatorCliError::invalid_input(
            "store directory path must be a normalized absolute directory path",
        ));
    }

    let mut reconstructed = PathBuf::from("/");
    for part in &normal_parts {
        reconstructed.push(part);
    }
    if reconstructed.as_os_str() != path.as_os_str() {
        return Err(OperatorCliError::invalid_input(
            "store directory path must be normalized without redundant separators or `.` segments",
        ));
    }

    let mut current = PathBuf::from("/");
    let root_meta = fs::symlink_metadata(&current).map_err(|_| {
        OperatorCliError::invalid_input("failed to inspect root directory metadata")
    })?;
    if root_meta.file_type().is_symlink() || !root_meta.file_type().is_dir() {
        return Err(OperatorCliError::new(
            "security_violation",
            "store directory ancestor must be a non-symlink directory",
        ));
    }

    for (idx, part) in normal_parts.iter().enumerate() {
        current.push(part);
        let is_leaf = idx + 1 == normal_parts.len();
        match fs::symlink_metadata(&current) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(OperatorCliError::new(
                        "security_violation",
                        "store directory path must not contain symlinks",
                    ));
                }
                if !meta.file_type().is_dir() {
                    return Err(OperatorCliError::new(
                        "security_violation",
                        "store directory path component is not a directory",
                    ));
                }
                if is_leaf && (meta.mode() & 0o777) != 0o700 {
                    return Err(OperatorCliError::new(
                        "security_violation",
                        "store directory must be private with owner-only 0700 permissions",
                    ));
                }
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Err(OperatorCliError::invalid_input(
                    "required store directory does not exist",
                ));
            }
            Err(_) => {
                return Err(OperatorCliError::new(
                    "io_error",
                    "failed to inspect store directory metadata",
                ));
            }
        }
    }

    let canonical = fs::canonicalize(&reconstructed).map_err(|_| {
        OperatorCliError::new(
            "security_violation",
            "failed to canonicalize store directory path",
        )
    })?;
    if canonical != reconstructed {
        return Err(OperatorCliError::new(
            "security_violation",
            "store directory path is not canonical",
        ));
    }

    Ok(canonical)
}

#[cfg(not(unix))]
fn validate_single_existing_private_dir(_path: &Path) -> Result<PathBuf, OperatorCliError> {
    Err(OperatorCliError::new(
        "unsupported_platform",
        "trusted local operator CLI is only supported on Unix platforms",
    ))
}

/// Validates `binding_id` syntax and canonical lowercase hyphenated non-nil `thread_uuid` format,
/// then verifies against the live authenticated local [`SupervisorClient`] at `state_dir` that
/// the binding exists and owns the exact `thread_uuid`.
///
/// This check confirms live local runtime binding state; it does **not** prove caller or human
/// per-call conversation identity.
pub async fn verify_binding(
    state_dir: &Path,
    binding_id: &str,
    thread_uuid: &str,
) -> Result<(), OperatorCliError> {
    let dirs = validate_dirs(&[("state_dir", state_dir)])?;
    let clean_state_dir = &dirs[0];

    let checked_binding_id = validate_identifier("binding_id", binding_id).map_err(|_| {
        OperatorCliError::invalid_input("binding_id failed identifier syntax validation")
    })?;
    let checked_thread_uuid =
        validate_canonical_thread_uuid("thread_uuid", thread_uuid).map_err(|_| {
            OperatorCliError::invalid_input(
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
            )
        })?;

    let supervisor = SupervisorClient::from_state_dir(clean_state_dir)
        .map_err(|err| map_supervisor_error(&err))?;
    let live_binding = supervisor
        .get_binding(&checked_binding_id)
        .await
        .map_err(|err| map_supervisor_error(&err))?;

    if live_binding.binding_id != checked_binding_id {
        return Err(OperatorCliError::new(
            "binding_mismatch",
            "live runtime binding_id did not match requested binding_id",
        ));
    }

    let live_thread_uuid = validate_canonical_thread_uuid("thread_uuid", &live_binding.thread_uuid)
        .map_err(|_| {
            OperatorCliError::new(
                "thread_uuid_mismatch",
                "live runtime binding thread_uuid is not a canonical non-nil UUID",
            )
        })?;
    if live_thread_uuid != checked_thread_uuid {
        return Err(OperatorCliError::new(
            "thread_uuid_mismatch",
            "live runtime binding thread_uuid did not match requested thread_uuid",
        ));
    }

    Ok(())
}

fn map_supervisor_error(err: &threadlane::Error) -> OperatorCliError {
    match err {
        threadlane::Error::SecurityViolation(_) => OperatorCliError::new(
            "security_violation",
            "runtime state directory or descriptor failed security checks",
        ),
        threadlane::Error::UnsupportedPlatform(_) => OperatorCliError::new(
            "unsupported_platform",
            "trusted local operator CLI is only supported on Unix platforms",
        ),
        _ => {
            let api_err = err.to_api_error();
            match api_err.code {
                RuntimeErrorCode::BindingNotFound => OperatorCliError::new(
                    "binding_not_found",
                    "requested binding_id was not found in the live supervisor runtime",
                ),
                RuntimeErrorCode::Unauthorized => OperatorCliError::new(
                    "unauthorized_operator",
                    "supervisor rejected local operator authentication",
                ),
                RuntimeErrorCode::ValidationFailed => OperatorCliError::invalid_input(
                    "binding_id failed supervisor identifier validation",
                ),
                RuntimeErrorCode::SecurityViolation => OperatorCliError::new(
                    "security_violation",
                    "runtime state directory or descriptor failed security checks",
                ),
                _ => OperatorCliError::new(
                    "transport_unavailable",
                    "live supervisor runtime is offline or unreachable",
                ),
            }
        }
    }
}

/// Reads and deserializes a typed JSON request `T` from a normalized, absolute, non-symlink,
/// private regular file (`<= 1 MiB`).
///
/// Opens with `O_NOFOLLOW | O_NONBLOCK` on Unix, rechecks file identity (`dev`/`ino`), permissions,
/// and byte length before and after the bounded read, and never echoes raw input on error.
#[cfg(unix)]
pub fn read_request<T: DeserializeOwned>(path: &Path) -> Result<T, OperatorCliError> {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    let raw_bytes = path.as_os_str().as_bytes();
    if raw_bytes.is_empty() || !path.is_absolute() || raw_bytes.ends_with(b"/") {
        return Err(OperatorCliError::invalid_input(
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
            Component::CurDir | Component::ParentDir => {
                return Err(OperatorCliError::new(
                    "security_violation",
                    "request path must not contain relative or traversal components",
                ));
            }
            _ => {
                return Err(OperatorCliError::invalid_input(
                    "request path must be a normalized absolute file path",
                ));
            }
        }
    }

    if !saw_root || normal_parts.is_empty() {
        return Err(OperatorCliError::invalid_input(
            "request path must be a normalized absolute file path",
        ));
    }

    let mut reconstructed = PathBuf::from("/");
    for part in &normal_parts {
        reconstructed.push(part);
    }
    if reconstructed.as_os_str() != path.as_os_str() {
        return Err(OperatorCliError::invalid_input(
            "request path must be normalized without redundant separators or `.` segments",
        ));
    }

    let mut current = PathBuf::from("/");
    let root_meta = fs::symlink_metadata(&current)
        .map_err(|_| OperatorCliError::invalid_input("invalid root directory metadata"))?;
    if root_meta.file_type().is_symlink() || !root_meta.file_type().is_dir() {
        return Err(OperatorCliError::new(
            "security_violation",
            "request path ancestor must be a non-symlink directory",
        ));
    }

    let mut leaf_dev = 0u64;
    let mut leaf_ino = 0u64;

    for (idx, part) in normal_parts.iter().enumerate() {
        current.push(part);
        let meta = fs::symlink_metadata(&current)
            .map_err(|_| OperatorCliError::invalid_input("request file path does not exist"))?;
        if meta.file_type().is_symlink() {
            return Err(OperatorCliError::new(
                "security_violation",
                "request path must not contain symlinks",
            ));
        }

        let is_leaf = idx + 1 == normal_parts.len();
        if !is_leaf {
            if !meta.file_type().is_dir() {
                return Err(OperatorCliError::invalid_input(
                    "request path ancestor must be a directory",
                ));
            }
        } else {
            if !meta.file_type().is_file() {
                return Err(OperatorCliError::invalid_input(
                    "request path must refer to a regular file",
                ));
            }
            if (meta.mode() & 0o077) != 0 || (meta.mode() & 0o400) == 0 {
                return Err(OperatorCliError::new(
                    "security_violation",
                    "request file must be private with owner-only permissions",
                ));
            }
            if meta.len() > MAX_IO_BYTES as u64 {
                return Err(OperatorCliError::invalid_input(
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
        .open(&reconstructed)
        .map_err(|_| OperatorCliError::invalid_input("request file could not be opened"))?;

    let fstat_meta = file
        .metadata()
        .map_err(|_| OperatorCliError::invalid_input("invalid request file metadata"))?;
    if !fstat_meta.file_type().is_file() {
        return Err(OperatorCliError::invalid_input(
            "request path must refer to a regular file",
        ));
    }
    if (fstat_meta.mode() & 0o077) != 0 || (fstat_meta.mode() & 0o400) == 0 {
        return Err(OperatorCliError::new(
            "security_violation",
            "request file must be private with owner-only permissions",
        ));
    }
    if fstat_meta.dev() != leaf_dev || fstat_meta.ino() != leaf_ino {
        return Err(OperatorCliError::new(
            "security_violation",
            "request file identity changed during open",
        ));
    }
    if fstat_meta.len() > MAX_IO_BYTES as u64 {
        return Err(OperatorCliError::invalid_input(
            "request file exceeds maximum size of 1 MiB",
        ));
    }

    let initial_len = fstat_meta.len() as usize;
    let mut buf = Vec::with_capacity(initial_len.min(MAX_IO_BYTES) + 1);
    (&mut file)
        .take((MAX_IO_BYTES as u64) + 1)
        .read_to_end(&mut buf)
        .map_err(|_| OperatorCliError::invalid_input("request file could not be read"))?;

    if buf.len() > MAX_IO_BYTES {
        return Err(OperatorCliError::invalid_input(
            "request file exceeds maximum size of 1 MiB",
        ));
    }
    if buf.len() != initial_len {
        return Err(OperatorCliError::new(
            "security_violation",
            "request file size changed during read",
        ));
    }

    let post_meta = file
        .metadata()
        .map_err(|_| OperatorCliError::invalid_input("invalid request file metadata"))?;
    if !post_meta.file_type().is_file()
        || (post_meta.mode() & 0o077) != 0
        || (post_meta.mode() & 0o400) == 0
        || post_meta.dev() != leaf_dev
        || post_meta.ino() != leaf_ino
        || post_meta.len() != (buf.len() as u64)
    {
        return Err(OperatorCliError::new(
            "security_violation",
            "request file state changed during read",
        ));
    }

    serde_json::from_slice(&buf)
        .map_err(|_| OperatorCliError::invalid_input("request file contains invalid JSON"))
}

/// Reads and deserializes a typed JSON request `T` from `path` (unsupported on non-Unix platforms).
#[cfg(not(unix))]
pub fn read_request<T: DeserializeOwned>(_path: &Path) -> Result<T, OperatorCliError> {
    Err(OperatorCliError::new(
        "unsupported_platform",
        "trusted local operator CLI is only supported on Unix platforms",
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

/// Serializes and flushes the bounded (`<= 1 MiB` including trailing newline) trusted local
/// operator JSON envelope to standard output.
///
/// Emits `{"untrusted_data":true,"route":"trusted_local_operator","native_per_call_identity_verified":false,"binding_id":...,"data":...}`.
/// If `may_have_effects` is `true`, any serialization, size, write, or flush failure returns
/// `outcome_unknown` so callers never blindly replay effects.
pub fn write_result<T: Serialize>(
    binding_id: &str,
    value: &T,
    may_have_effects: bool,
) -> Result<(), OperatorCliError> {
    let mut stdout = io::stdout().lock();
    write_result_to(&mut stdout, binding_id, value, may_have_effects)
}

fn write_result_to<W: Write, T: Serialize>(
    out: &mut W,
    binding_id: &str,
    value: &T,
    may_have_effects: bool,
) -> Result<(), OperatorCliError> {
    if validate_identifier("binding_id", binding_id).is_err() {
        return Err(output_failure(
            may_have_effects,
            "invalid_input",
            "binding_id failed identifier syntax validation",
        ));
    }

    let envelope = OperatorResultEnvelope {
        untrusted_data: true,
        route: TRUSTED_LOCAL_OPERATOR_ROUTE,
        native_per_call_identity_verified: false,
        binding_id,
        data: value,
    };

    let mut writer = BoundedVecWriter::new(MAX_IO_BYTES);
    if serde_json::to_writer(&mut writer, &envelope).is_err() || writer.write_all(b"\n").is_err() {
        return Err(output_failure(
            may_have_effects,
            "output_overflow",
            "failed to encode bounded command output within 1 MiB",
        ));
    }

    if out.write_all(&writer.buf).is_err() || out.flush().is_err() {
        return Err(output_failure(
            may_have_effects,
            "io_error",
            "failed to write and flush bounded command output",
        ));
    }

    Ok(())
}

const fn output_failure(
    may_have_effects: bool,
    read_only_code: &'static str,
    read_only_message: &'static str,
) -> OperatorCliError {
    if may_have_effects {
        OperatorCliError::new(
            OUTCOME_UNKNOWN_CODE,
            "operation may already have taken effect, but writing bounded stdout result failed",
        )
    } else {
        OperatorCliError::new(read_only_code, read_only_message)
    }
}

/// Emits a static, bounded JSON error envelope (`{"code":...,"message":...}`) to standard error.
pub fn emit_error(error: &OperatorCliError) {
    let mut stderr = io::stderr().lock();
    emit_error_to(&mut stderr, error);
}

fn emit_error_to<W: Write>(err_out: &mut W, error: &OperatorCliError) {
    let mut writer = BoundedVecWriter::new(MAX_ERROR_LINE_BYTES);
    if serde_json::to_writer(&mut writer, error).is_ok() && writer.write_all(b"\n").is_ok() {
        let _ = err_out.write_all(&writer.buf);
    } else {
        let _ = err_out
            .write_all(b"{\"code\":\"internal_error\",\"message\":\"operator command failed\"}\n");
    }
    let _ = err_out.flush();
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::path::Path;
    use std::time::Duration;

    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use tempfile::TempDir;
    use threadlane::{Supervisor, SupervisorClient, SupervisorConfig};

    use super::*;

    const VALID_THREAD_UUID: &str = "0196ed00-0000-7000-8000-000000000001";
    const OTHER_THREAD_UUID: &str = "0196ed00-0000-7000-8000-000000000002";

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct SampleRequest {
        command_key: String,
        count: u64,
    }

    #[test]
    fn validate_dirs_enforces_existing_canonical_private_non_overlapping_dirs() {
        let tmp = TempDir::new().expect("tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize tempdir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("chmod root 0700");

        let dir_a = root.join("store-a");
        let dir_b = root.join("store-b");
        fs::create_dir(&dir_a).expect("create dir_a");
        fs::create_dir(&dir_b).expect("create dir_b");
        fs::set_permissions(&dir_a, fs::Permissions::from_mode(0o700)).expect("chmod dir_a 0700");
        fs::set_permissions(&dir_b, fs::Permissions::from_mode(0o700)).expect("chmod dir_b 0700");

        let validated =
            validate_dirs(&[("dir_a", &dir_a), ("dir_b", &dir_b)]).expect("valid directories");
        assert_eq!(validated, vec![dir_a.clone(), dir_b.clone()]);

        // Missing directory must be rejected without creating it.
        let missing = root.join("does-not-exist");
        let missing_err =
            validate_dirs(&[("missing", &missing)]).expect_err("missing dir must fail");
        assert_eq!(missing_err.code, "invalid_input");
        assert!(
            !missing.exists(),
            "validate_dirs must not create missing dir"
        );

        // Non-private directory must be rejected without fixing permissions.
        let open_dir = root.join("open-dir");
        fs::create_dir(&open_dir).expect("create open_dir");
        fs::set_permissions(&open_dir, fs::Permissions::from_mode(0o755))
            .expect("chmod open_dir 0755");
        let open_err =
            validate_dirs(&[("open_dir", &open_dir)]).expect_err("0755 directory must fail");
        assert_eq!(open_err.code, "security_violation");
        let mode_after = fs::symlink_metadata(&open_dir)
            .expect("stat open_dir")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            mode_after, 0o755,
            "validate_dirs must not silently fix directory permissions"
        );

        // Symlink directory must be rejected.
        let link_dir = root.join("link-dir");
        symlink(&dir_a, &link_dir).expect("create symlink dir");
        let link_err =
            validate_dirs(&[("link", &link_dir)]).expect_err("symlink directory must fail");
        assert_eq!(link_err.code, "security_violation");

        // Overlapping / identical directories must be rejected.
        let dup_err = validate_dirs(&[("a", &dir_a), ("b", &dir_a)])
            .expect_err("duplicate directories must fail");
        assert_eq!(dup_err.code, "security_violation");

        let nested = dir_a.join("nested");
        fs::create_dir(&nested).expect("create nested");
        fs::set_permissions(&nested, fs::Permissions::from_mode(0o700)).expect("chmod nested 0700");
        let overlap_err = validate_dirs(&[("a", &dir_a), ("nested", &nested)])
            .expect_err("overlapping directories must fail");
        assert_eq!(overlap_err.code, "security_violation");

        // Trailing slash or unnormalized `.` segment must be rejected.
        let trailing = PathBuf::from(format!("{}/", dir_a.display()));
        assert_eq!(
            validate_dirs(&[("trailing", &trailing)])
                .expect_err("trailing slash must fail")
                .code,
            "invalid_input"
        );
        let dot_path = PathBuf::from(format!("{}/./store-a", root.display()));
        assert_eq!(
            validate_dirs(&[("dot", &dot_path)])
                .expect_err("dot segment must fail")
                .code,
            "invalid_input"
        );
    }

    #[tokio::test]
    async fn verify_binding_checks_syntax_canonical_uuid_and_live_supervisor() {
        let tmp = TempDir::new().expect("tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize tempdir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("chmod root 0700");
        let state_dir = root.join("runtime-state");

        let sup = Supervisor::start(SupervisorConfig {
            state_dir: state_dir.clone(),
            tick_interval: Duration::from_millis(50),
            codex: None,
            workers: None,
        })
        .await
        .expect("start supervisor");

        let client = SupervisorClient::from_state_dir(&state_dir).expect("supervisor client");
        let binding = client
            .create_binding(VALID_THREAD_UUID, "Operator Workflow Chat")
            .await
            .expect("create binding");

        verify_binding(&state_dir, &binding.binding_id, VALID_THREAD_UUID)
            .await
            .expect("live binding verification succeeds");

        // Nil UUID and non-canonical uppercase UUID must fail with invalid_input.
        let nil_err = verify_binding(
            &state_dir,
            &binding.binding_id,
            "00000000-0000-0000-0000-000000000000",
        )
        .await
        .expect_err("nil UUID must fail");
        assert_eq!(nil_err.code, "invalid_input");

        let upper_err = verify_binding(
            &state_dir,
            &binding.binding_id,
            "0196ED00-0000-7000-8000-000000000001",
        )
        .await
        .expect_err("uppercase UUID must fail");
        assert_eq!(upper_err.code, "invalid_input");

        // Mismatched thread UUID must fail with thread_uuid_mismatch.
        let mismatch_err = verify_binding(&state_dir, &binding.binding_id, OTHER_THREAD_UUID)
            .await
            .expect_err("wrong thread UUID must fail");
        assert_eq!(mismatch_err.code, "thread_uuid_mismatch");

        // Unknown binding ID must fail with binding_not_found.
        let missing_err = verify_binding(&state_dir, "bnd_does_not_exist", VALID_THREAD_UUID)
            .await
            .expect_err("unknown binding must fail");
        assert_eq!(missing_err.code, "binding_not_found");

        // Shut down supervisor; subsequent verification must fail closed.
        sup.shutdown().await;
        let offline_err = verify_binding(&state_dir, &binding.binding_id, VALID_THREAD_UUID)
            .await
            .expect_err("offline supervisor must fail");
        assert_eq!(offline_err.code, "transport_unavailable");
    }

    #[test]
    fn read_request_enforces_private_regular_bounded_json_and_redacts_input() {
        let tmp = TempDir::new().expect("tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize tempdir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("chmod root 0700");

        let valid_path = root.join("valid.json");
        fs::write(&valid_path, br#"{"command_key":"cmd-1","count":42}"#).expect("write valid");
        fs::set_permissions(&valid_path, fs::Permissions::from_mode(0o600))
            .expect("chmod valid 0600");

        let parsed: SampleRequest = read_request(&valid_path).expect("read valid request");
        assert_eq!(
            parsed,
            SampleRequest {
                command_key: "cmd-1".to_string(),
                count: 42,
            }
        );

        // Unknown fields must be rejected without leaking sentinel values.
        let unknown_path = root.join("unknown.json");
        fs::write(
            &unknown_path,
            br#"{"command_key":"cmd-1","count":1,"secret":"TOP_SECRET_SENTINEL_VALUE"}"#,
        )
        .expect("write unknown");
        fs::set_permissions(&unknown_path, fs::Permissions::from_mode(0o600))
            .expect("chmod unknown 0600");
        let unknown_err =
            read_request::<SampleRequest>(&unknown_path).expect_err("unknown field must fail");
        assert_eq!(unknown_err.code, "invalid_input");
        assert!(!unknown_err.message.contains("TOP_SECRET_SENTINEL_VALUE"));

        // Non-private file (0644) must be rejected.
        let open_file = root.join("open.json");
        fs::write(&open_file, br#"{"command_key":"cmd-1","count":1}"#).expect("write open");
        fs::set_permissions(&open_file, fs::Permissions::from_mode(0o644))
            .expect("chmod open 0644");
        let open_err =
            read_request::<SampleRequest>(&open_file).expect_err("0644 request file must fail");
        assert_eq!(open_err.code, "security_violation");

        // Symlink request file must be rejected.
        let link_file = root.join("link.json");
        symlink(&valid_path, &link_file).expect("symlink request file");
        let link_err =
            read_request::<SampleRequest>(&link_file).expect_err("symlink request file must fail");
        assert_eq!(link_err.code, "security_violation");

        // Oversize (> 1 MiB) request file must be rejected.
        let oversize_path = root.join("oversize.json");
        fs::write(&oversize_path, vec![b' '; MAX_IO_BYTES + 1]).expect("write oversize");
        fs::set_permissions(&oversize_path, fs::Permissions::from_mode(0o600))
            .expect("chmod oversize 0600");
        let oversize_err =
            read_request::<SampleRequest>(&oversize_path).expect_err("oversize file must fail");
        assert_eq!(oversize_err.code, "invalid_input");
    }

    #[test]
    fn write_result_and_emit_error_enforce_envelope_bounds_and_outcome_unknown() {
        let payload = SampleRequest {
            command_key: "cmd-out".to_string(),
            count: 7,
        };
        let mut buf = Vec::new();
        write_result_to(&mut buf, "bnd_1", &payload, false).expect("write_result_to succeeds");
        assert!(buf.ends_with(b"\n"));
        assert!(buf.len() <= MAX_IO_BYTES);

        let decoded: Value = serde_json::from_slice(&buf).expect("parse envelope JSON");
        assert_eq!(decoded["untrusted_data"], Value::Bool(true));
        assert_eq!(
            decoded["route"],
            Value::String("trusted_local_operator".to_string())
        );
        assert_eq!(
            decoded["native_per_call_identity_verified"],
            Value::Bool(false)
        );
        assert_eq!(decoded["binding_id"], Value::String("bnd_1".to_string()));
        assert_eq!(decoded["data"]["command_key"], "cmd-out");
        assert_eq!(decoded["data"]["count"], 7);

        // Oversize (> 1 MiB) output: read-only returns output_overflow; mutating returns outcome_unknown.
        let huge = "x".repeat(MAX_IO_BYTES);
        let mut read_only_buf = Vec::new();
        let ro_err = write_result_to(&mut read_only_buf, "bnd_1", &huge, false)
            .expect_err("oversize read-only output must fail");
        assert_eq!(ro_err.code, "output_overflow");

        let mut mut_buf = Vec::new();
        let mut_err = write_result_to(&mut mut_buf, "bnd_1", &huge, true)
            .expect_err("oversize mutating output must return outcome_unknown");
        assert_eq!(mut_err.code, "outcome_unknown");

        // /dev/full write failure: read-only returns io_error; mutating returns outcome_unknown.
        let dev_full = Path::new("/dev/full");
        if dev_full.exists() {
            let mut full_ro = OpenOptions::new()
                .write(true)
                .open(dev_full)
                .expect("open /dev/full");
            let full_ro_err = write_result_to(&mut full_ro, "bnd_1", &payload, false)
                .expect_err("/dev/full read-only write must fail");
            assert_eq!(full_ro_err.code, "io_error");

            let mut full_mut = OpenOptions::new()
                .write(true)
                .open(dev_full)
                .expect("open /dev/full");
            let full_mut_err = write_result_to(&mut full_mut, "bnd_1", &payload, true)
                .expect_err("/dev/full mutating write must report outcome_unknown");
            assert_eq!(full_mut_err.code, "outcome_unknown");
        }

        let mut err_buf = Vec::new();
        emit_error_to(
            &mut err_buf,
            &OperatorCliError::invalid_input("invalid or missing command-line arguments"),
        );
        let err_json: Value = serde_json::from_slice(&err_buf).expect("parse stderr error JSON");
        assert_eq!(err_json["code"], "invalid_input");
        assert_eq!(
            err_json["message"],
            "invalid or missing command-line arguments"
        );
    }
}
