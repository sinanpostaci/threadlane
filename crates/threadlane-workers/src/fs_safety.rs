use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::WorkerError;

/// Ensures that the current operating system supports verified Unix process-group supervision
/// and `0700`/`0600` private ownership manifests.
pub fn ensure_platform_supported() -> Result<(), WorkerError> {
    #[cfg(unix)]
    {
        Ok(())
    }
    #[cfg(not(unix))]
    {
        Err(WorkerError::UnsupportedPlatform(
            "Unix process-group lifecycle supervision and 0700/0600 private ownership manifests are only implemented on Unix (verified on Linux/NixOS; macOS and Windows are not yet verified)"
                .to_string(),
        ))
    }
}

/// Validates that `path` is an absolute path with no `.` or `..` components and that every
/// existing component along the path is not a symlink.
pub fn validate_absolute_no_symlink_components(
    path: &Path,
    label: &str,
) -> Result<PathBuf, WorkerError> {
    ensure_platform_supported()?;

    if path.as_os_str().is_empty() {
        return Err(WorkerError::InvalidSpec(format!(
            "{label} path must not be empty"
        )));
    }
    if !path.is_absolute() {
        return Err(WorkerError::InvalidSpec(format!(
            "{label} path `{}` must be an absolute path",
            path.display()
        )));
    }

    let mut current = PathBuf::new();
    let mut components = path.components().peekable();
    while let Some(comp) = components.next() {
        match comp {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(comp.as_os_str()),
            Component::CurDir => {
                return Err(WorkerError::SecurityViolation(format!(
                    "{label} path `{}` must not contain `.` components",
                    path.display()
                )));
            }
            Component::ParentDir => {
                return Err(WorkerError::SecurityViolation(format!(
                    "{label} path `{}` must not contain parent-directory (`..`) traversal components",
                    path.display()
                )));
            }
            Component::Normal(part) => {
                current.push(part);
                let is_last = components.peek().is_none();
                match fs::symlink_metadata(&current) {
                    Ok(meta) => {
                        if meta.file_type().is_symlink() {
                            return Err(WorkerError::SecurityViolation(format!(
                                "{label} path `{}` traverses symlink component `{}`",
                                path.display(),
                                current.display()
                            )));
                        }
                        if !is_last && !meta.file_type().is_dir() {
                            return Err(WorkerError::SecurityViolation(format!(
                                "{label} parent component `{}` is not a directory",
                                current.display()
                            )));
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        // Remaining components do not exist yet; all existing ancestors are verified non-symlinks.
                    }
                    Err(err) => return Err(WorkerError::Io(err)),
                }
            }
        }
    }

    Ok(current)
}

/// Validates that `executable` is an existing, absolute, non-symlink regular file with
/// execute permissions on Unix.
pub fn validate_executable_file(executable: &Path) -> Result<PathBuf, WorkerError> {
    let checked = validate_absolute_no_symlink_components(executable, "worker executable")?;

    let meta = fs::symlink_metadata(&checked).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            WorkerError::InvalidSpec(format!(
                "worker executable `{}` does not exist",
                executable.display()
            ))
        } else {
            WorkerError::Io(err)
        }
    })?;

    if meta.file_type().is_symlink() {
        return Err(WorkerError::SecurityViolation(format!(
            "worker executable `{}` must not be a symlink",
            executable.display()
        )));
    }
    if !meta.file_type().is_file() {
        return Err(WorkerError::InvalidSpec(format!(
            "worker executable `{}` must be a regular file",
            executable.display()
        )));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o111 == 0 {
            return Err(WorkerError::InvalidSpec(format!(
                "worker executable `{}` is not executable (mode {:04o})",
                executable.display(),
                mode
            )));
        }
    }

    let canonical = fs::canonicalize(&checked)?;
    if canonical != checked {
        return Err(WorkerError::SecurityViolation(format!(
            "worker executable `{}` resolved to unexpected canonical path `{}`",
            checked.display(),
            canonical.display()
        )));
    }

    Ok(canonical)
}

/// Validates that `dir` is an existing, absolute, non-symlink directory and returns its canonical path.
pub fn validate_existing_canonical_dir(dir: &Path, label: &str) -> Result<PathBuf, WorkerError> {
    let checked = validate_absolute_no_symlink_components(dir, label)?;
    let meta = fs::symlink_metadata(&checked).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            WorkerError::InvalidSpec(format!("{label} `{}` does not exist", dir.display()))
        } else {
            WorkerError::Io(err)
        }
    })?;
    if meta.file_type().is_symlink() {
        return Err(WorkerError::SecurityViolation(format!(
            "{label} `{}` must not be a symlink",
            dir.display()
        )));
    }
    if !meta.file_type().is_dir() {
        return Err(WorkerError::InvalidSpec(format!(
            "{label} `{}` must be a directory",
            dir.display()
        )));
    }
    let canonical = fs::canonicalize(&checked)?;
    if canonical != checked {
        return Err(WorkerError::SecurityViolation(format!(
            "{label} `{}` resolved to unexpected canonical path `{}`",
            checked.display(),
            canonical.display()
        )));
    }
    Ok(canonical)
}

/// Creates a new private directory (`0700` on Unix) or validates an existing private (`0700`)
/// directory without following symlinks and without `chmod`ing pre-existing directories.
pub fn ensure_private_dir(path: &Path, label: &str) -> Result<PathBuf, WorkerError> {
    let checked = validate_absolute_no_symlink_components(path, label)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

        let mut current = PathBuf::new();
        for comp in checked.components() {
            match comp {
                Component::Prefix(prefix) => current.push(prefix.as_os_str()),
                Component::RootDir => current.push(comp.as_os_str()),
                Component::CurDir | Component::ParentDir => {
                    return Err(WorkerError::SecurityViolation(format!(
                        "{label} path `{}` must not contain relative traversal components",
                        path.display()
                    )));
                }
                Component::Normal(part) => {
                    current.push(part);
                    match fs::symlink_metadata(&current) {
                        Ok(meta) => {
                            if meta.file_type().is_symlink() {
                                return Err(WorkerError::SecurityViolation(format!(
                                    "{label} path `{}` traverses symlink `{}`",
                                    path.display(),
                                    current.display()
                                )));
                            }
                            if !meta.file_type().is_dir() {
                                return Err(WorkerError::SecurityViolation(format!(
                                    "{label} component `{}` is not a directory",
                                    current.display()
                                )));
                            }
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                            let mut builder = fs::DirBuilder::new();
                            builder.mode(0o700);
                            builder.create(&current)?;
                            fs::set_permissions(&current, fs::Permissions::from_mode(0o700))?;
                        }
                        Err(err) => return Err(WorkerError::Io(err)),
                    }
                }
            }
        }

        let meta = fs::symlink_metadata(&checked)?;
        if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
            return Err(WorkerError::SecurityViolation(format!(
                "{label} `{}` must be a non-symlink directory",
                checked.display()
            )));
        }
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(WorkerError::SecurityViolation(format!(
                "{label} `{}` has non-private permissions {:04o} (expected 0700); refusing to chmod existing directory",
                checked.display(),
                mode
            )));
        }
    }

    Ok(checked)
}

/// Validates that a pre-existing private state/manifest file (if present) is not a symlink,
/// is a regular file, and has `0600` permissions on Unix.
pub fn validate_preexisting_private_file(path: &Path, label: &str) -> Result<(), WorkerError> {
    let _ = validate_absolute_no_symlink_components(path, label)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        match fs::symlink_metadata(path) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(WorkerError::SecurityViolation(format!(
                        "{label} `{}` must not be a symlink",
                        path.display()
                    )));
                }
                if !meta.file_type().is_file() {
                    return Err(WorkerError::SecurityViolation(format!(
                        "{label} `{}` must be a regular file",
                        path.display()
                    )));
                }
                let mode = meta.permissions().mode() & 0o777;
                if mode & 0o077 != 0 {
                    return Err(WorkerError::SecurityViolation(format!(
                        "{label} `{}` has insecure permissions {:04o} (expected 0600)",
                        path.display(),
                        mode
                    )));
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(WorkerError::Io(err)),
        }
    }

    Ok(())
}

/// Opens or creates a private lock file (`0600` on Unix) after verifying it is not a symlink.
pub fn open_private_lock_file(path: &Path, label: &str) -> Result<File, WorkerError> {
    validate_preexisting_private_file(path, label)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path)?;

        let sym_meta = fs::symlink_metadata(path)?;
        if sym_meta.file_type().is_symlink() || !sym_meta.file_type().is_file() {
            return Err(WorkerError::SecurityViolation(format!(
                "{label} `{}` must be a regular non-symlink file",
                path.display()
            )));
        }
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        Ok(file)
    }

    #[cfg(not(unix))]
    {
        let _ = (path, label);
        ensure_platform_supported()?;
        unreachable!()
    }
}

/// Atomically writes a JSON document with `0600` permissions inside `dir`.
pub fn write_private_json_atomic<T: Serialize>(
    dir: &Path,
    filename: &str,
    value: &T,
    label: &str,
) -> Result<PathBuf, WorkerError> {
    let checked_dir = ensure_private_dir(dir, label)?;
    let target_path = checked_dir.join(filename);
    validate_preexisting_private_file(&target_path, label)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut temp = tempfile::Builder::new()
            .prefix(&format!(".{filename}.tmp."))
            .permissions(fs::Permissions::from_mode(0o600))
            .tempfile_in(&checked_dir)?;

        let encoded = serde_json::to_vec_pretty(value)?;
        temp.write_all(&encoded)?;
        temp.as_file().sync_all()?;
        temp.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))?;

        temp.persist(&target_path)
            .map_err(|err| WorkerError::Io(err.error))?;
        fs::set_permissions(&target_path, fs::Permissions::from_mode(0o600))?;
    }

    Ok(target_path)
}

/// Reads and deserializes a private (`0600`) JSON file with a hard byte bound.
pub fn read_private_json<T: DeserializeOwned>(
    path: &Path,
    max_bytes: u64,
    label: &str,
) -> Result<T, WorkerError> {
    validate_preexisting_private_file(path, label)?;

    let meta = fs::symlink_metadata(path)?;
    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
        return Err(WorkerError::SecurityViolation(format!(
            "{label} `{}` must be a regular non-symlink file",
            path.display()
        )));
    }
    if meta.len() > max_bytes {
        return Err(WorkerError::SecurityViolation(format!(
            "{label} `{}` exceeds maximum size of {max_bytes} bytes",
            path.display()
        )));
    }

    let mut file = File::open(path)?;
    let mut bytes = Vec::with_capacity(usize::try_from(meta.len()).unwrap_or(0));
    file.read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > max_bytes {
        return Err(WorkerError::SecurityViolation(format!(
            "{label} `{}` exceeded maximum size of {max_bytes} bytes during read",
            path.display()
        )));
    }

    let parsed = serde_json::from_slice(&bytes)?;
    Ok(parsed)
}

/// Creates a supervisor-only, private, nonblocking Linux FIFO with fixed kernel capacity.
///
/// No user-supplied path reaches this helper. O_NOFOLLOW, held-fd metadata, and per-round
/// continuity checks remain required at the caller; content semantics stay unverified.
#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
pub(crate) fn create_private_diagnostic_fifo(path: &Path) -> std::io::Result<File> {
    use std::ffi::CString;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};
    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("invalid diagnostic sink"))?;
    // SAFETY: c_path is a live NUL-terminated path; mkfifo creates but never follows a target.
    if unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // RDWR is Linux-qualified: the supervisor keeps an anchor so early startup/dup2 does not
    // cause false EOF or SIGPIPE. This fd is never inherited by the child.
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let metadata = file.metadata()?;
    // SAFETY: geteuid takes no arguments and has no side effects.
    let owner = unsafe { libc::geteuid() };
    if !metadata.file_type().is_fifo()
        || metadata.uid() != owner
        || metadata.mode() & 0o7777 != 0o600
        || metadata.nlink() != 1
    {
        return Err(std::io::Error::other("invalid diagnostic sink ownership"));
    }
    // SAFETY: file is an open FIFO; F_SETPIPE_SZ changes only its bounded kernel capacity.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETPIPE_SZ, 4096) } != 4096 {
        return Err(std::io::Error::other("unavailable bounded diagnostic sink"));
    }
    Ok(file)
}

/// Sanitizes untrusted text for diagnostic summaries by replacing control characters,
/// redacting common credential/token patterns, and truncating to `max_bytes` on a UTF-8 boundary.
pub fn sanitize_and_redact(raw: &str, max_bytes: usize) -> String {
    if max_bytes == 0 {
        return String::new();
    }

    let cleaned: String = raw
        .chars()
        .map(|ch| {
            if ch == '\n' || ch == '\r' || ch == '\t' {
                ' '
            } else if ch.is_control() {
                '?'
            } else {
                ch
            }
        })
        .collect();

    let redacted = redact_tokens(&cleaned);
    truncate_utf8(&redacted, max_bytes)
}

fn redact_tokens(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut prev_was_auth_keyword = false;

    for (idx, raw_word) in input.split_whitespace().enumerate() {
        if idx > 0 {
            out.push(' ');
        }

        if prev_was_auth_keyword {
            out.push_str("[REDACTED]");
            prev_was_auth_keyword = false;
            continue;
        }

        let lower = raw_word.to_ascii_lowercase();
        let trimmed_lower = lower.trim_matches(is_wrapper_punct);

        if matches!(
            trimmed_lower,
            "bearer"
                | "authorization:"
                | "token:"
                | "api_key:"
                | "apikey:"
                | "secret:"
                | "password:"
        ) {
            out.push_str(raw_word);
            prev_was_auth_keyword = true;
            continue;
        }

        if let Some(eq_idx) = raw_word.find(['=', ':']) {
            let key_part = &lower[..eq_idx];
            let clean_key =
                key_part.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '-');
            if is_sensitive_key(clean_key) && eq_idx + 1 < raw_word.len() {
                let sep = &raw_word[eq_idx..=eq_idx];
                let prefix_key = &raw_word[..eq_idx];
                out.push_str(prefix_key);
                out.push_str(sep);
                out.push_str("[REDACTED]");
                continue;
            }
        }

        let token_candidate = raw_word.trim_matches(is_wrapper_punct);
        if looks_like_secret_token(token_candidate) {
            out.push_str("[REDACTED]");
        } else {
            out.push_str(raw_word);
        }
    }

    out
}

fn is_wrapper_punct(c: char) -> bool {
    matches!(c, '"' | '\'' | ',' | ';' | '(' | ')' | '[' | ']')
}

fn is_sensitive_key(key: &str) -> bool {
    matches!(
        key,
        "api_key"
            | "apikey"
            | "api-key"
            | "token"
            | "access_token"
            | "refresh_token"
            | "auth_token"
            | "secret"
            | "client_secret"
            | "password"
            | "passwd"
            | "authorization"
            | "openai_api_key"
            | "anthropic_api_key"
            | "gemini_api_key"
            | "google_api_key"
    )
}

fn looks_like_secret_token(token: &str) -> bool {
    if token.len() < 10 {
        return false;
    }
    token.starts_with("sk-")
        || token.starts_with("ghp_")
        || token.starts_with("gho_")
        || token.starts_with("github_pat_")
        || token.starts_with("AIza")
        || token.starts_with("xoxb-")
        || token.starts_with("xoxp-")
        || token.starts_with("ya29.")
}

/// Truncates `text` to at most `max_bytes` UTF-8 bytes, appending `...[truncated]` when shortened.
pub fn truncate_utf8(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    const SUFFIX: &str = "...[truncated]";
    if max_bytes <= SUFFIX.len() {
        let mut end = max_bytes;
        while end > 0 && !text.is_char_boundary(end) {
            end -= 1;
        }
        return text[..end].to_string();
    }
    let target = max_bytes - SUFFIX.len();
    let mut end = target;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{SUFFIX}", &text[..end])
}

/// Descriptor-relative cleanup for an owned private FIFO or its empty directory.
/// No symlink is followed and no recursive removal is performed. Questionable entries stay.
#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
pub(crate) fn remove_owned_diagnostic_entry(
    parent: &File,
    name: &std::ffi::OsStr,
    held: &File,
    directory: bool,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes == b"." || bytes == b".." || bytes.contains(&b'/') {
        return Err(std::io::Error::other("invalid diagnostic entry name"));
    }
    let name = std::ffi::CString::new(bytes)
        .map_err(|_| std::io::Error::other("invalid diagnostic entry name"))?;
    let expected = held.metadata()?;
    let mut named = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: both descriptors remain held and name is a single NUL-terminated component.
    if unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            named.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: fstatat initialized the stat buffer on success.
    let named = unsafe { named.assume_init() };
    let kind = if directory {
        libc::S_IFDIR
    } else {
        libc::S_IFIFO
    };
    let mode = if directory { 0o700 } else { 0o600 };
    if named.st_dev != expected.dev()
        || named.st_ino != expected.ino()
        || named.st_uid != expected.uid()
        || named.st_mode & libc::S_IFMT != kind
        || named.st_mode & 0o7777 != mode
        || (!directory && named.st_nlink != 1)
    {
        return Err(std::io::Error::other("diagnostic entry identity changed"));
    }
    // SAFETY: unlinkat is relative to the held directory and never follows a final symlink.
    // Identity checks are conservative, not atomic protection against hostile same-user writes.
    let flags = if directory { libc::AT_REMOVEDIR } else { 0 };
    if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), flags) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(all(test, target_os = "linux"))]
mod diagnostic_cleanup_tests {
    use super::*;
    use std::os::unix::fs::{symlink, OpenOptionsExt, PermissionsExt};

    #[test]
    fn descriptor_relative_cleanup_survives_parent_replacement_and_preserves_foreign_leaf() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("parent");
        fs::create_dir(&parent).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
        let parent_fd = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&parent)
            .unwrap();
        let fifo = create_private_diagnostic_fifo(&parent.join("capture")).unwrap();
        let moved = root.path().join("moved");
        fs::rename(&parent, &moved).unwrap();
        let replacement = root.path().join("replacement");
        fs::create_dir(&replacement).unwrap();
        fs::write(replacement.join("capture"), b"foreign").unwrap();
        symlink(&replacement, &parent).unwrap();

        // Simulates a parent change after the caller's continuity check.
        remove_owned_diagnostic_entry(&parent_fd, std::ffi::OsStr::new("capture"), &fifo, false)
            .unwrap();
        assert!(!moved.join("capture").exists());
        assert_eq!(fs::read(parent.join("capture")).unwrap(), b"foreign");

        let fifo2 = create_private_diagnostic_fifo(&moved.join("capture")).unwrap();
        fs::remove_file(moved.join("capture")).unwrap();
        symlink(replacement.join("capture"), moved.join("capture")).unwrap();
        assert!(remove_owned_diagnostic_entry(
            &parent_fd,
            std::ffi::OsStr::new("capture"),
            &fifo2,
            false,
        )
        .is_err());
        assert!(moved
            .join("capture")
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(fs::read(replacement.join("capture")).unwrap(), b"foreign");
    }
}
