use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::hint::black_box;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::Error;

/// Supported runtime descriptor format version.
pub const DESCRIPTOR_VERSION: u32 = 1;

/// Required loopback bind host for local supervisor IPC.
pub const LOOPBACK_HOST: &str = "127.0.0.1";

/// Filename for the process-lifetime state directory lock.
pub const LOCK_FILENAME: &str = "supervisor.lock";

/// Filename for the SQLite state database owned by the supervisor.
pub const DATABASE_FILENAME: &str = "state.db";

/// Filename for the user-private runtime descriptor.
pub const DESCRIPTOR_FILENAME: &str = "supervisor.json";

/// Maximum permitted byte size for `supervisor.json`.
pub const MAX_DESCRIPTOR_BYTES: u64 = 4_096;

/// Exact hex character length of the bearer token (64 hex chars encoding two v4 UUIDs / 244 random bits).
pub const TOKEN_HEX_LEN: usize = 64;

/// Redacted 64-hex-character bearer token (244 bits of CSPRNG entropy from two v4 UUIDs)
/// used to authenticate local IPC requests.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretToken(String);

impl SecretToken {
    /// Generates a fresh 64-hex-character bearer token carrying 244 bits of CSPRNG entropy
    /// from two random v4 UUIDs.
    pub fn generate() -> Self {
        let first = Uuid::new_v4().simple().to_string();
        let second = Uuid::new_v4().simple().to_string();
        Self(format!("{first}{second}"))
    }

    /// Wraps a raw token string.
    pub fn new(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }

    /// Returns the raw token string for authenticated header construction.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Validates that the token is a 64-character hexadecimal secret.
    pub fn validate_format(&self) -> Result<(), Error> {
        if self.0.len() != TOKEN_HEX_LEN || !self.0.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(Error::SecurityViolation(
                "descriptor bearer token must be a 64-character hex secret".to_string(),
            ));
        }
        Ok(())
    }

    /// Performs a non-short-circuiting byte-wise equality check against a candidate bearer token.
    ///
    /// Uses `std::hint::black_box` on the XOR accumulator to avoid early prefix returns;
    /// this is a best-effort non-short-circuiting comparison in standard Rust rather than
    /// a hardware-verified constant-time primitive.
    pub fn constant_time_eq(&self, candidate: &str) -> bool {
        let a = black_box(self.0.as_bytes());
        let b = black_box(candidate.as_bytes());
        if a.len() != b.len() {
            return false;
        }
        let mut diff = 0u8;
        for (&lhs, &rhs) in a.iter().zip(b.iter()) {
            diff = black_box(diff | (lhs ^ rhs));
        }
        black_box(diff) == 0
    }
}

impl fmt::Debug for SecretToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretToken([REDACTED])")
    }
}

impl fmt::Display for SecretToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

/// User-private runtime descriptor written atomically by the active supervisor.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeDescriptor {
    /// Descriptor schema version (`1`).
    pub version: u32,
    /// Loopback IPv4 address (`127.0.0.1`).
    pub host: String,
    /// Bound TCP port on `127.0.0.1`.
    pub port: u16,
    /// Process ID of the owning supervisor.
    pub pid: u32,
    /// Fresh 64-hex-character bearer secret required on all HTTP requests.
    pub token: SecretToken,
}

impl fmt::Debug for RuntimeDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeDescriptor")
            .field("version", &self.version)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("pid", &self.pid)
            .field("token", &self.token)
            .finish()
    }
}

impl RuntimeDescriptor {
    /// Validates descriptor version, loopback host, non-zero port/pid, and bearer token format.
    pub fn validate(&self) -> Result<(), Error> {
        if self.version != DESCRIPTOR_VERSION {
            return Err(Error::SecurityViolation(format!(
                "unsupported runtime descriptor version {} (expected {})",
                self.version, DESCRIPTOR_VERSION
            )));
        }
        if self.host != LOOPBACK_HOST {
            return Err(Error::SecurityViolation(format!(
                "descriptor host `{}` is not loopback `{}`",
                self.host, LOOPBACK_HOST
            )));
        }
        if self.port == 0 {
            return Err(Error::SecurityViolation(
                "descriptor port must be non-zero".to_string(),
            ));
        }
        if self.pid == 0 {
            return Err(Error::SecurityViolation(
                "descriptor pid must be non-zero".to_string(),
            ));
        }
        self.token.validate_format()?;
        Ok(())
    }

    /// Returns the base `http://127.0.0.1:<port>` URL after validating loopback invariants.
    pub fn base_url(&self) -> Result<String, Error> {
        self.validate()?;
        Ok(format!("http://{}:{}", self.host, self.port))
    }
}

/// Exclusive process-lifetime lock on a Threadlane state directory.
#[derive(Debug)]
pub struct StateLock {
    lock_file: File,
    lock_path: PathBuf,
}

impl StateLock {
    /// Acquires the exclusive non-blocking process-lifetime lock on `<state_dir>/supervisor.lock`.
    pub fn acquire(state_dir: &Path) -> Result<Self, Error> {
        ensure_platform_supported()?;
        prepare_state_dir(state_dir)?;

        let lock_path = state_dir.join(LOCK_FILENAME);
        validate_preexisting_state_file(&lock_path, "lock")?;

        let lock_file = open_private_file(&lock_path, "lock")?;
        lock_file
            .try_lock_exclusive()
            .map_err(|_| Error::LockBusy {
                state_dir: state_dir.to_path_buf(),
            })?;

        Ok(Self {
            lock_file,
            lock_path,
        })
    }

    /// Returns the path of the held lock file.
    pub fn path(&self) -> &Path {
        &self.lock_path
    }
}

impl Drop for StateLock {
    fn drop(&mut self) {
        let _ = self.lock_file.unlock();
    }
}

/// Ensures that the current platform supports private Unix permission enforcement.
pub fn ensure_platform_supported() -> Result<(), Error> {
    #[cfg(unix)]
    {
        Ok(())
    }
    #[cfg(not(unix))]
    {
        Err(Error::UnsupportedPlatform(
            "secure runtime descriptor and state directory permission enforcement (0700/0600) is currently implemented for Unix only"
                .to_string(),
        ))
    }
}

/// Validates that `path` and every existing ancestor component contain no symlinks or `..` segments.
///
/// Relative paths are resolved against the process current working directory before walking components.
pub fn validate_no_symlink_components(path: &Path, label: &str) -> Result<PathBuf, Error> {
    ensure_platform_supported()?;
    if path.as_os_str().is_empty() {
        return Err(Error::InvalidConfig(format!(
            "{label} path must not be empty"
        )));
    }

    for comp in path.components() {
        if matches!(comp, Component::ParentDir) {
            return Err(Error::SecurityViolation(format!(
                "{label} path `{}` must not contain parent-directory (`..`) traversal components",
                path.display()
            )));
        }
    }

    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        let cwd = std::env::current_dir()?;
        cwd.join(path)
    };

    let mut current = PathBuf::new();
    let mut components = resolved.components().peekable();
    while let Some(comp) = components.next() {
        match comp {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(comp.as_os_str()),
            Component::CurDir => continue,
            Component::ParentDir => {
                return Err(Error::SecurityViolation(format!(
                    "{label} path `{}` must not contain `..` components",
                    path.display()
                )));
            }
            Component::Normal(part) => {
                current.push(part);
                let is_last = components.peek().is_none();
                match fs::symlink_metadata(&current) {
                    Ok(meta) => {
                        if meta.file_type().is_symlink() {
                            return Err(Error::SecurityViolation(format!(
                                "{label} path `{}` traverses symlink component `{}`",
                                path.display(),
                                current.display()
                            )));
                        }
                        if !is_last && !meta.file_type().is_dir() {
                            return Err(Error::SecurityViolation(format!(
                                "{label} parent component `{}` is not a directory",
                                current.display()
                            )));
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        // Remaining components do not exist yet; none of the existing ancestors are symlinks.
                    }
                    Err(err) => return Err(Error::Io(err)),
                }
            }
        }
    }

    Ok(resolved)
}

/// Validates static path safety and existing ancestor components for a lazy client `state_dir`
/// without requiring the directory or `supervisor.json` to exist yet.
pub fn validate_lazy_state_dir(state_dir: &Path) -> Result<PathBuf, Error> {
    let resolved = validate_no_symlink_components(state_dir, "state directory")?;
    if let Ok(meta) = fs::symlink_metadata(&resolved) {
        if meta.file_type().is_symlink() {
            return Err(Error::SecurityViolation(format!(
                "state directory `{}` must not be a symlink",
                state_dir.display()
            )));
        }
        if !meta.file_type().is_dir() {
            return Err(Error::SecurityViolation(format!(
                "state directory `{}` is not a directory",
                state_dir.display()
            )));
        }
    }
    Ok(resolved)
}

/// Prepares and validates the state directory (`0700` on Unix, rejecting symlinks in any
/// component and refusing to modify pre-existing non-empty directories that contain no Threadlane
/// state entries).
pub fn prepare_state_dir(state_dir: &Path) -> Result<(), Error> {
    let resolved = validate_no_symlink_components(state_dir, "state directory")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

        // Walk and create any missing directory components one level at a time without following symlinks.
        let mut current = PathBuf::new();
        for comp in resolved.components() {
            match comp {
                Component::Prefix(p) => current.push(p.as_os_str()),
                Component::RootDir => current.push(comp.as_os_str()),
                Component::CurDir => continue,
                Component::ParentDir => {
                    return Err(Error::SecurityViolation(format!(
                        "state directory `{}` must not contain `..`",
                        state_dir.display()
                    )));
                }
                Component::Normal(part) => {
                    current.push(part);
                    match fs::symlink_metadata(&current) {
                        Ok(meta) => {
                            if meta.file_type().is_symlink() {
                                return Err(Error::SecurityViolation(format!(
                                    "state directory path `{}` traverses symlink `{}`",
                                    state_dir.display(),
                                    current.display()
                                )));
                            }
                            if !meta.file_type().is_dir() {
                                return Err(Error::SecurityViolation(format!(
                                    "state directory component `{}` is not a directory",
                                    current.display()
                                )));
                            }
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                            let mut builder = fs::DirBuilder::new();
                            builder.mode(0o700);
                            builder.create(&current)?;
                        }
                        Err(err) => return Err(Error::Io(err)),
                    }
                }
            }
        }

        let meta = fs::symlink_metadata(&resolved)?;
        if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
            return Err(Error::SecurityViolation(format!(
                "state directory `{}` must be a non-symlink directory",
                state_dir.display()
            )));
        }
        let is_non_empty = fs::read_dir(&resolved)?.next().transpose()?.is_some();
        if is_non_empty && !has_any_state_entry(&resolved) {
            return Err(Error::SecurityViolation(format!(
                "state directory `{}` is a pre-existing non-empty directory with no Threadlane runtime state entries",
                state_dir.display()
            )));
        }
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 && has_any_state_entry(&resolved) {
            return Err(Error::SecurityViolation(format!(
                "pre-existing state directory `{}` has insecure permissions {:04o} (expected 0700)",
                state_dir.display(),
                mode
            )));
        }
        fs::set_permissions(&resolved, fs::Permissions::from_mode(0o700))?;
    }

    Ok(())
}

fn has_any_state_entry(state_dir: &Path) -> bool {
    [
        LOCK_FILENAME,
        DATABASE_FILENAME,
        DESCRIPTOR_FILENAME,
        "state.db-wal",
        "state.db-shm",
    ]
    .iter()
    .any(|name| fs::symlink_metadata(state_dir.join(name)).is_ok())
}

/// Validates that a pre-existing state file (if present) is not a symlink, is a regular file,
/// and does not grant group or world permissions.
pub fn validate_preexisting_state_file(path: &Path, label: &str) -> Result<(), Error> {
    let _ = validate_no_symlink_components(path, label)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        match fs::symlink_metadata(path) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(Error::SecurityViolation(format!(
                        "{label} path `{}` must not be a symlink",
                        path.display()
                    )));
                }
                if !meta.file_type().is_file() {
                    return Err(Error::SecurityViolation(format!(
                        "{label} path `{}` must be a regular file",
                        path.display()
                    )));
                }
                let mode = meta.permissions().mode() & 0o777;
                if mode & 0o077 != 0 {
                    return Err(Error::SecurityViolation(format!(
                        "pre-existing {label} file `{}` has insecure permissions {:04o} (expected 0600)",
                        path.display(),
                        mode
                    )));
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(Error::Io(err)),
        }
    }

    Ok(())
}

/// Prepares the SQLite database path inside `state_dir`, rejecting symlinks or insecure pre-existing
/// database/WAL/SHM files and ensuring `state.db` is created with `0600` permissions.
pub fn prepare_database_path(state_dir: &Path) -> Result<PathBuf, Error> {
    prepare_state_dir(state_dir)?;
    let db_path = state_dir.join(DATABASE_FILENAME);
    let wal_path = state_dir.join(format!("{DATABASE_FILENAME}-wal"));
    let shm_path = state_dir.join(format!("{DATABASE_FILENAME}-shm"));

    validate_preexisting_state_file(&db_path, "database")?;
    validate_preexisting_state_file(&wal_path, "database WAL")?;
    validate_preexisting_state_file(&shm_path, "database SHM")?;

    let _ = open_private_file(&db_path, "database")?;
    Ok(db_path)
}

/// Enforces `0600` permissions on the SQLite database and any WAL/SHM sidecar files.
pub fn enforce_database_permissions(state_dir: &Path) -> Result<(), Error> {
    ensure_platform_supported()?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        for name in [DATABASE_FILENAME, "state.db-wal", "state.db-shm"] {
            let path = state_dir.join(name);
            if let Ok(meta) = fs::symlink_metadata(&path) {
                if meta.file_type().is_symlink() || !meta.file_type().is_file() {
                    return Err(Error::SecurityViolation(format!(
                        "database file `{}` is not a regular non-symlink file",
                        path.display()
                    )));
                }
                fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
            }
        }
    }

    Ok(())
}

fn open_private_file(path: &Path, label: &str) -> Result<File, Error> {
    validate_preexisting_state_file(path, label)?;

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

        let meta = file.metadata()?;
        if !meta.file_type().is_file() {
            return Err(Error::SecurityViolation(format!(
                "{label} file `{}` is not a regular file",
                path.display()
            )));
        }
        let sym_meta = fs::symlink_metadata(path)?;
        if sym_meta.file_type().is_symlink() {
            return Err(Error::SecurityViolation(format!(
                "{label} file `{}` must not be a symlink",
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

/// Atomically writes `supervisor.json` with `0600` permissions inside `state_dir`.
pub fn write_descriptor_atomic(
    state_dir: &Path,
    descriptor: &RuntimeDescriptor,
) -> Result<PathBuf, Error> {
    descriptor.validate()?;
    prepare_state_dir(state_dir)?;

    let descriptor_path = state_dir.join(DESCRIPTOR_FILENAME);
    validate_preexisting_state_file(&descriptor_path, "descriptor")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut temp = tempfile::Builder::new()
            .prefix(".supervisor.json.tmp.")
            .permissions(fs::Permissions::from_mode(0o600))
            .tempfile_in(state_dir)?;

        let encoded = serde_json::to_vec_pretty(descriptor)?;
        temp.write_all(&encoded)?;
        temp.as_file().sync_all()?;
        temp.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))?;

        temp.persist(&descriptor_path)
            .map_err(|err| Error::Io(err.error))?;
        fs::set_permissions(&descriptor_path, fs::Permissions::from_mode(0o600))?;
    }

    Ok(descriptor_path)
}

/// Removes `supervisor.json` only if it matches `expected` (verifying `pid`, `port`, and token)
/// and is a non-symlink regular file. Must be called while still holding [`StateLock`].
pub fn remove_owned_descriptor(state_dir: &Path, expected: &RuntimeDescriptor) {
    let descriptor_path = state_dir.join(DESCRIPTOR_FILENAME);
    let Ok(meta) = fs::symlink_metadata(&descriptor_path) else {
        return;
    };
    if !meta.file_type().is_file() || meta.file_type().is_symlink() {
        return;
    }
    if let Ok(bytes) = fs::read(&descriptor_path) {
        if let Ok(on_disk) = serde_json::from_slice::<RuntimeDescriptor>(&bytes) {
            if on_disk.pid == expected.pid
                && on_disk.port == expected.port
                && on_disk.token.constant_time_eq(expected.token.as_str())
            {
                let _ = fs::remove_file(&descriptor_path);
            }
        }
    }
}

/// Reads and validates `<state_dir>/supervisor.json` before a client sends any bearer token.
///
/// Verifies:
/// 1. `state_dir` and all its ancestor components contain no symlinks, and `state_dir` has `0700` permissions.
/// 2. `supervisor.json` is a non-symlink regular file with `0600` permissions and bounded size.
/// 3. Descriptor JSON has `version == 1`, `host == "127.0.0.1"`, `port > 0`, `pid > 0`, and a 64-hex token.
pub fn read_and_validate_descriptor(state_dir: &Path) -> Result<RuntimeDescriptor, Error> {
    let resolved_dir = validate_no_symlink_components(state_dir, "state directory")?;
    let descriptor_path = resolved_dir.join(DESCRIPTOR_FILENAME);

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let dir_meta = fs::symlink_metadata(&resolved_dir).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                Error::Transport(format!(
                    "state directory `{}` does not exist (is supervisor running?)",
                    state_dir.display()
                ))
            } else {
                Error::Io(err)
            }
        })?;
        if dir_meta.file_type().is_symlink() {
            return Err(Error::SecurityViolation(format!(
                "state directory `{}` must not be a symlink",
                state_dir.display()
            )));
        }
        if !dir_meta.file_type().is_dir() {
            return Err(Error::SecurityViolation(format!(
                "state directory `{}` is not a directory",
                state_dir.display()
            )));
        }
        let dir_mode = dir_meta.permissions().mode() & 0o777;
        if dir_mode & 0o077 != 0 {
            return Err(Error::SecurityViolation(format!(
                "state directory `{}` has insecure permissions {:04o} (expected 0700)",
                state_dir.display(),
                dir_mode
            )));
        }

        let sym_meta = fs::symlink_metadata(&descriptor_path).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                Error::Transport(format!(
                    "runtime descriptor `{}` not found (is `threadlane serve` running?)",
                    descriptor_path.display()
                ))
            } else {
                Error::Io(err)
            }
        })?;
        if sym_meta.file_type().is_symlink() {
            return Err(Error::SecurityViolation(format!(
                "runtime descriptor `{}` must not be a symlink",
                descriptor_path.display()
            )));
        }
        if !sym_meta.file_type().is_file() {
            return Err(Error::SecurityViolation(format!(
                "runtime descriptor `{}` must be a regular file",
                descriptor_path.display()
            )));
        }
        let file_mode = sym_meta.permissions().mode() & 0o777;
        if file_mode & 0o077 != 0 {
            return Err(Error::SecurityViolation(format!(
                "runtime descriptor `{}` has insecure permissions {:04o} (expected 0600)",
                descriptor_path.display(),
                file_mode
            )));
        }
        if sym_meta.len() > MAX_DESCRIPTOR_BYTES {
            return Err(Error::SecurityViolation(format!(
                "runtime descriptor `{}` exceeds maximum size of {} bytes",
                descriptor_path.display(),
                MAX_DESCRIPTOR_BYTES
            )));
        }

        let file = File::open(&descriptor_path)?;
        let open_meta = file.metadata()?;
        if !open_meta.file_type().is_file()
            || open_meta.dev() != sym_meta.dev()
            || open_meta.ino() != sym_meta.ino()
        {
            return Err(Error::SecurityViolation(format!(
                "runtime descriptor `{}` changed during open",
                descriptor_path.display()
            )));
        }
        let open_mode = open_meta.permissions().mode() & 0o777;
        if open_mode & 0o077 != 0 {
            return Err(Error::SecurityViolation(format!(
                "runtime descriptor `{}` has insecure permissions {:04o} (expected 0600)",
                descriptor_path.display(),
                open_mode
            )));
        }

        let mut buf = Vec::with_capacity(open_meta.len() as usize);
        file.take(MAX_DESCRIPTOR_BYTES + 1).read_to_end(&mut buf)?;
        if buf.len() as u64 > MAX_DESCRIPTOR_BYTES {
            return Err(Error::SecurityViolation(format!(
                "runtime descriptor `{}` exceeds maximum size of {} bytes",
                descriptor_path.display(),
                MAX_DESCRIPTOR_BYTES
            )));
        }

        let descriptor: RuntimeDescriptor = serde_json::from_slice(&buf).map_err(|err| {
            Error::SecurityViolation(format!("invalid runtime descriptor JSON: {err}"))
        })?;
        descriptor.validate()?;
        Ok(descriptor)
    }

    #[cfg(not(unix))]
    {
        let _ = descriptor_path;
        ensure_platform_supported()?;
        unreachable!()
    }
}
