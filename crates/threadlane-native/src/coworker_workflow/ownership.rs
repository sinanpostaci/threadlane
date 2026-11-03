//! Private per-job OS file-lock ownership, CSPRNG instance/challenge identity, and pre-effect
//! inode/scope validation for the Threadlane Coworker runtime.
//!
//! ## Core Invariants
//!
//! - **Real kernel file-lock ownership**: A deterministic or copied `instance_id`, stored PID, or
//!   lease timestamp is never treated as process ownership. [`CoworkerJobOwnershipGuard`] holds an
//!   open file descriptor with an exclusive OS kernel lock (`flock` `LOCK_EX | LOCK_NB`) for the
//!   entire driver/step/effect lifetime, is `!Clone`, and cannot be deserialized or reconstructed
//!   from metadata.
//! - **Non-blocking acquisition**: [`CoworkerJobOwnershipStore::try_acquire_job`] and
//!   [`CoworkerJobOwnershipStore::try_acquire_job_option`] never block a Tokio executor; lock
//!   contention returns [`CoworkerOwnershipAcquireOutcome::Busy`] / `Ok(None)` immediately.
//! - **Fresh CSPRNG identity per acquisition**: Each successful lock acquisition mints a new CSPRNG
//!   `instance_id` (`cwo_<64hex>`) and private `challenge_secret` (`cwc_<64hex>`). Only the SHA-256
//!   `challenge_digest` appears in [`CoworkerOwnershipSummary`] or `Debug` output.
//! - **In-place lockfile lifecycle**: Lock root directories must be canonical, existing, private
//!   (`0700`), non-symlink directories owned by the current effective UID. Lockfiles are opened
//!   with `O_NOFOLLOW | O_CLOEXEC` (`0600`, same EUID, regular file, `nlink == 1`) and are never
//!   unlinked or recreated on drop or recovery; kernel lock release on guard drop or process exit
//!   permits reacquisition with a new CSPRNG identity.
//! - **No execution authority or exactly-once inference claim**: Holding
//!   [`CoworkerJobOwnershipGuard`] grants no code, model, grant, or approval authority
//!   ([`OWNERSHIP_GRANTS_NO_EXECUTION_AUTHORITY`]). Full unknown-effect receipt reconciliation
//!   still belongs to the controller and remains mandatory before restarting inference
//!   ([`OWNERSHIP_REQUIRES_RECEIPT_RECONCILIATION_BEFORE_INFERENCE`]).

use std::fmt::{self, Write as _};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_authority::validation::validate_canonical_thread_uuid;
use threadlane_workers::validate_sha256_hex_digest;

use crate::coworker_workflow::contracts::{sha256_hex, MAX_COWORKER_ID_LEN};
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    constant_time_str_eq, ensure_platform_supported, generate_prefixed_random_id,
    sanitize_io_error, validate_absolute_no_symlink_components, validate_bounded_identifier,
    validate_private_file_path,
};

/// Current wire and on-disk schema version for per-job ownership lockfile metadata.
pub const COWORKER_JOB_OWNERSHIP_SCHEMA_VERSION: u32 = 1;

/// Maximum bounded byte size for a per-job ownership lockfile summary (`16 KiB`).
pub const MAX_COWORKER_OWNERSHIP_METADATA_BYTES: usize = 16 * 1024;

/// Invariant marker: per-job ownership never grants code, model, grant, or approval authority.
pub const OWNERSHIP_GRANTS_NO_EXECUTION_AUTHORITY: bool = true;

/// Invariant marker: unknown-effect receipt reconciliation remains mandatory before restarting inference.
pub const OWNERSHIP_REQUIRES_RECEIPT_RECONCILIATION_BEFORE_INFERENCE: bool = true;

/// Prefix for CSPRNG per-acquisition instance identifiers (`cwo_<64hex>`).
pub const COWORKER_OWNERSHIP_INSTANCE_PREFIX: &str = "cwo";

/// Prefix for CSPRNG per-acquisition secret challenge tokens (`cwc_<64hex>`).
pub const COWORKER_OWNERSHIP_CHALLENGE_PREFIX: &str = "cwc";

/// Device and inode identity of a validated private directory or lockfile on Unix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerLockFileIdentity {
    /// Filesystem device identifier (`st_dev`).
    pub dev: u64,
    /// Filesystem inode number (`st_ino`).
    pub ino: u64,
}

/// Scoped identifier key for per-job runtime ownership (`binding_id`, `thread_uuid`, `job_id`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerJobOwnershipKey {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Scoped Coworker workflow job identifier.
    pub job_id: String,
}

impl CoworkerJobOwnershipKey {
    /// Constructs and validates a [`CoworkerJobOwnershipKey`].
    pub fn new(
        binding_id: impl Into<String>,
        thread_uuid: impl Into<String>,
        job_id: impl Into<String>,
    ) -> Result<Self, NativeError> {
        let key = Self {
            binding_id: binding_id.into(),
            thread_uuid: thread_uuid.into(),
            job_id: job_id.into(),
        };
        key.validate()?;
        Ok(key)
    }

    /// Validates `binding_id`, `thread_uuid`, and `job_id`.
    pub fn validate(&self) -> Result<(), NativeError> {
        validate_bounded_identifier(&self.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", &self.thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
            )
        })?;
        validate_bounded_identifier(&self.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        Ok(())
    }

    /// Computes the deterministic 32-hex collision-resistant scope tag for this key.
    pub fn tag(&self) -> Result<String, NativeError> {
        self.validate()?;
        let mut hasher = Sha256::new();
        hasher.update(b"threadlane-native:coworker-job-ownership:v1\0");
        for part in [&self.binding_id, &self.thread_uuid, &self.job_id] {
            hasher.update((part.len() as u64).to_le_bytes());
            hasher.update(part.as_bytes());
        }
        let digest = hasher.finalize();
        let mut out = String::with_capacity(32);
        for b in &digest[..16] {
            let _ = write!(&mut out, "{b:02x}");
        }
        Ok(out)
    }

    /// Returns the deterministic private lockfile name (`coworker-job-ownership-<tag>.lock`).
    pub fn lock_filename(&self) -> Result<String, NativeError> {
        let tag = self.tag()?;
        Ok(format!("coworker-job-ownership-{tag}.lock"))
    }
}

/// Public, secret-free diagnostic summary recorded inside a per-job ownership lockfile.
///
/// Presenting or deserializing a [`CoworkerOwnershipSummary`] never grants job ownership or
/// reconstructs a [`CoworkerJobOwnershipGuard`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerOwnershipSummary {
    /// Wire and storage schema version (`1`).
    pub schema_version: u32,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Scoped Coworker workflow job identifier.
    pub job_id: String,
    /// Fresh CSPRNG instance identifier (`cwo_<64hex>`) minted upon kernel lock acquisition.
    pub instance_id: String,
    /// Lowercase 64-hex SHA-256 digest of the private CSPRNG challenge secret.
    pub challenge_digest: String,
    /// Caller-supplied acquisition timestamp (ms since Unix epoch).
    pub acquired_at_ms: u64,
    /// Diagnostic OS process ID hint recorded at acquisition (never trusted for ownership or signaled).
    pub pid_hint: u32,
    /// Device and inode identity of the locked file at acquisition.
    pub lock_file_identity: CoworkerLockFileIdentity,
    /// Explicit marker that ownership grants no execution, model, or approval authority (`false`).
    pub grants_execution_authority: bool,
}

impl CoworkerOwnershipSummary {
    /// Returns the [`CoworkerJobOwnershipKey`] for this summary.
    pub fn key(&self) -> CoworkerJobOwnershipKey {
        CoworkerJobOwnershipKey {
            binding_id: self.binding_id.clone(),
            thread_uuid: self.thread_uuid.clone(),
            job_id: self.job_id.clone(),
        }
    }

    /// Validates structural fields, schema version, CSPRNG `instance_id` format, and `challenge_digest`.
    pub fn validate(&self) -> Result<(), NativeError> {
        if self.schema_version != COWORKER_JOB_OWNERSHIP_SCHEMA_VERSION {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                format!(
                    "unsupported coworker job ownership schema_version {} (expected {COWORKER_JOB_OWNERSHIP_SCHEMA_VERSION})",
                    self.schema_version
                ),
            ));
        }
        if self.grants_execution_authority {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "CoworkerOwnershipSummary.grants_execution_authority must be false",
            ));
        }
        self.key().validate()?;
        validate_prefixed_csprng_token(
            &self.instance_id,
            COWORKER_OWNERSHIP_INSTANCE_PREFIX,
            "instance_id",
        )?;
        validate_sha256_hex_digest(&self.challenge_digest, "challenge_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "challenge_digest must be a 64-character lowercase hex SHA-256 digest",
            )
        })?;
        Ok(())
    }

    /// Validates this summary and verifies that its scope matches `expected_key`.
    pub fn validate_for_key(
        &self,
        expected_key: &CoworkerJobOwnershipKey,
    ) -> Result<(), NativeError> {
        expected_key.validate()?;
        self.validate()?;
        if self.binding_id != expected_key.binding_id
            || self.thread_uuid != expected_key.thread_uuid
            || self.job_id != expected_key.job_id
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "CoworkerOwnershipSummary scope does not match expected (binding_id, thread_uuid, job_id)",
            ));
        }
        Ok(())
    }
}

/// Read-only inspection state of a per-job lockfile on disk.
///
/// Returned by [`CoworkerJobOwnershipStore::inspect_job_lock`] without creating directories or
/// files, modifying lockfile contents, acknowledging events, or dispatching work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoworkerJobLockState {
    /// No lockfile exists on disk for the requested job key.
    Absent,
    /// Lockfile exists and an exclusive OS file lock is currently held by an active process.
    HeldByActiveProcess {
        /// Public summary recorded in the active lockfile, if valid.
        recorded_summary: Option<CoworkerOwnershipSummary>,
        /// Device and inode identity of the inspected lockfile.
        lock_file_identity: CoworkerLockFileIdentity,
    },
    /// Lockfile exists on disk from a prior run, but no process currently holds the OS lock.
    ///
    /// Stale metadata in this file grants no ownership and does not block reacquisition.
    UnlockedStaleFile {
        /// Public summary left from the previous run, if valid.
        recorded_summary: Option<CoworkerOwnershipSummary>,
        /// Device and inode identity of the inspected lockfile.
        lock_file_identity: CoworkerLockFileIdentity,
    },
}

/// In-memory driver claim issued by an active [`CoworkerJobOwnershipGuard`].
///
/// Redacts the private `challenge_secret` in [`fmt::Debug`], is not serializable or
/// deserializable, and still requires a live [`CoworkerJobOwnershipGuard`] to validate before
/// any effect.
#[derive(Clone, PartialEq, Eq)]
pub struct CoworkerDriverOwnershipClaim {
    key: CoworkerJobOwnershipKey,
    instance_id: String,
    challenge_digest: String,
    challenge_secret: String,
    lock_file_identity: CoworkerLockFileIdentity,
}

impl CoworkerDriverOwnershipClaim {
    /// Returns the scoped [`CoworkerJobOwnershipKey`] for this claim.
    pub fn key(&self) -> &CoworkerJobOwnershipKey {
        &self.key
    }

    /// Returns the CSPRNG `instance_id` bound to this claim.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Returns the SHA-256 `challenge_digest` bound to this claim.
    pub fn challenge_digest(&self) -> &str {
        &self.challenge_digest
    }

    /// Returns the locked file's `(dev, ino)` identity bound to this claim.
    pub const fn lock_file_identity(&self) -> CoworkerLockFileIdentity {
        self.lock_file_identity
    }
}

impl fmt::Debug for CoworkerDriverOwnershipClaim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoworkerDriverOwnershipClaim")
            .field("key", &self.key)
            .field("instance_id", &self.instance_id)
            .field("challenge_digest", &self.challenge_digest)
            .field("challenge_secret", &"[REDACTED]")
            .field("lock_file_identity", &self.lock_file_identity)
            .finish()
    }
}

/// Exclusive per-job OS file-lock ownership guard held for the full driver/step/effect lifetime.
///
/// This guard is intentionally `!Clone` and implements neither `Serialize` nor `Deserialize` so it
/// can never be reconstructed from copied metadata or a stored `instance_id`.
pub struct CoworkerJobOwnershipGuard {
    file: File,
    lock_root_dir: PathBuf,
    lock_root_identity: CoworkerLockFileIdentity,
    lock_path: PathBuf,
    owner_euid: u32,
    summary: CoworkerOwnershipSummary,
    challenge_secret: String,
}

impl fmt::Debug for CoworkerJobOwnershipGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoworkerJobOwnershipGuard")
            .field("lock_root_dir", &self.lock_root_dir)
            .field("lock_root_identity", &self.lock_root_identity)
            .field("lock_path", &self.lock_path)
            .field("owner_euid", &self.owner_euid)
            .field("summary", &self.summary)
            .field("challenge_secret", &"[REDACTED]")
            .finish()
    }
}

impl Drop for CoworkerJobOwnershipGuard {
    fn drop(&mut self) {
        // Release the exclusive OS file lock without unlinking or recreating the lockfile.
        let _ = self.file.unlock();
    }
}

/// Non-blocking outcome of attempting to acquire per-job OS file-lock ownership.
#[derive(Debug)]
pub enum CoworkerOwnershipAcquireOutcome {
    /// Exclusive OS file lock was acquired with a freshly generated CSPRNG identity and challenge.
    Acquired(CoworkerJobOwnershipGuard),
    /// The job lock is currently held by another active owner handle; returned immediately without blocking.
    Busy {
        /// Public summary recorded in the active lockfile, if readable and valid.
        observed_holder: Option<CoworkerOwnershipSummary>,
    },
}

impl CoworkerOwnershipAcquireOutcome {
    /// Converts this outcome into `Some(guard)` when `Acquired` or `None` when `Busy`.
    pub fn into_guard(self) -> Option<CoworkerJobOwnershipGuard> {
        match self {
            Self::Acquired(guard) => Some(guard),
            Self::Busy { .. } => None,
        }
    }

    /// Returns `true` when exclusive ownership was acquired.
    pub const fn is_acquired(&self) -> bool {
        matches!(self, Self::Acquired(_))
    }

    /// Returns `true` when the job lock is currently held by another active owner.
    pub const fn is_busy(&self) -> bool {
        matches!(self, Self::Busy { .. })
    }
}

impl CoworkerJobOwnershipGuard {
    /// Returns the scoped [`CoworkerJobOwnershipKey`] held by this guard.
    pub fn key(&self) -> CoworkerJobOwnershipKey {
        self.summary.key()
    }

    /// Returns the control-plane `binding_id` held by this guard.
    pub fn binding_id(&self) -> &str {
        &self.summary.binding_id
    }

    /// Returns the canonical native Codex `thread_uuid` held by this guard.
    pub fn thread_uuid(&self) -> &str {
        &self.summary.thread_uuid
    }

    /// Returns the `job_id` held by this guard.
    pub fn job_id(&self) -> &str {
        &self.summary.job_id
    }

    /// Returns the CSPRNG `instance_id` (`cwo_<64hex>`) minted when this lock was acquired.
    pub fn instance_id(&self) -> &str {
        &self.summary.instance_id
    }

    /// Returns the SHA-256 `challenge_digest` of the private challenge secret.
    pub fn challenge_digest(&self) -> &str {
        &self.summary.challenge_digest
    }

    /// Returns the canonical `0700` lock root directory path.
    pub fn lock_root_dir(&self) -> &Path {
        &self.lock_root_dir
    }

    /// Returns the `(dev, ino)` identity of the lock root directory at acquisition.
    pub const fn lock_root_identity(&self) -> CoworkerLockFileIdentity {
        self.lock_root_identity
    }

    /// Returns the canonical `0600` lockfile path held by this guard.
    pub fn lock_path(&self) -> &Path {
        &self.lock_path
    }

    /// Returns the `(dev, ino)` identity of the locked file descriptor.
    pub const fn lock_file_identity(&self) -> CoworkerLockFileIdentity {
        self.summary.lock_file_identity
    }

    /// Returns the public secret-free [`CoworkerOwnershipSummary`] for this guard.
    pub fn summary(&self) -> &CoworkerOwnershipSummary {
        &self.summary
    }

    /// Issues an in-memory [`CoworkerDriverOwnershipClaim`] bound to this guard's active CSPRNG
    /// `instance_id` and private `challenge_secret`.
    pub fn issue_driver_claim(&self) -> CoworkerDriverOwnershipClaim {
        CoworkerDriverOwnershipClaim {
            key: self.summary.key(),
            instance_id: self.summary.instance_id.clone(),
            challenge_digest: self.summary.challenge_digest.clone(),
            challenge_secret: self.challenge_secret.clone(),
            lock_file_identity: self.summary.lock_file_identity,
        }
    }

    /// Verifies a candidate challenge secret in constant time without exposing the secret in diagnostics.
    pub fn verify_challenge(&self, candidate_secret: &str) -> Result<(), NativeError> {
        if !constant_time_str_eq(&self.challenge_secret, candidate_secret) {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "coworker job ownership challenge secret verification failed",
            ));
        }
        Ok(())
    }

    /// Validates scope, current effective UID, canonical `0700` lock root directory `(dev, ino)`,
    /// current `0600` non-symlink single-hardlink lockfile `(dev, ino)` against the held file
    /// descriptor, and on-disk summary continuity before executing a driver step or effect.
    pub fn validate_before_effect(
        &self,
        expected_key: &CoworkerJobOwnershipKey,
    ) -> Result<&CoworkerOwnershipSummary, NativeError> {
        self.validate_before_effect_with_euid(expected_key, current_effective_uid())
    }

    /// Validates `validate_before_effect` plus a caller-presented `expected_instance_id` and
    /// optional `candidate_challenge` token in constant time.
    pub fn validate_claim_before_effect(
        &self,
        expected_key: &CoworkerJobOwnershipKey,
        expected_instance_id: &str,
        candidate_challenge: Option<&str>,
    ) -> Result<&CoworkerOwnershipSummary, NativeError> {
        let summary = self.validate_before_effect(expected_key)?;
        if !constant_time_str_eq(&summary.instance_id, expected_instance_id) {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "presented instance_id does not match active lock-bound ownership identity",
            ));
        }
        if let Some(challenge) = candidate_challenge {
            self.verify_challenge(challenge)?;
        }
        Ok(summary)
    }

    /// Validates `validate_before_effect` plus an in-memory [`CoworkerDriverOwnershipClaim`].
    pub fn validate_driver_claim(
        &self,
        expected_key: &CoworkerJobOwnershipKey,
        claim: &CoworkerDriverOwnershipClaim,
    ) -> Result<&CoworkerOwnershipSummary, NativeError> {
        if &claim.key != expected_key {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "driver ownership claim key does not match expected job scope",
            ));
        }
        if claim.lock_file_identity != self.summary.lock_file_identity
            || !constant_time_str_eq(&claim.challenge_digest, &self.summary.challenge_digest)
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "driver ownership claim does not match active lockfile identity or challenge digest",
            ));
        }
        self.validate_claim_before_effect(
            expected_key,
            &claim.instance_id,
            Some(&claim.challenge_secret),
        )
    }

    /// Validates `validate_before_effect` and verifies that `claimed_summary` matches the active
    /// guard's `CoworkerOwnershipSummary` (rejecting stale or copied summaries from prior runs).
    pub fn validate_summary_before_effect(
        &self,
        expected_key: &CoworkerJobOwnershipKey,
        claimed_summary: &CoworkerOwnershipSummary,
    ) -> Result<&CoworkerOwnershipSummary, NativeError> {
        claimed_summary.validate_for_key(expected_key)?;
        let active = self.validate_before_effect(expected_key)?;
        if !constant_time_str_eq(&active.instance_id, &claimed_summary.instance_id)
            || !constant_time_str_eq(&active.challenge_digest, &claimed_summary.challenge_digest)
            || active.lock_file_identity != claimed_summary.lock_file_identity
            || active != claimed_summary
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "presented ownership summary is stale or does not match active lock-bound guard",
            ));
        }
        Ok(active)
    }

    fn validate_before_effect_with_euid(
        &self,
        expected_key: &CoworkerJobOwnershipKey,
        expected_euid: u32,
    ) -> Result<&CoworkerOwnershipSummary, NativeError> {
        ensure_platform_supported()?;
        self.summary.validate_for_key(expected_key)?;
        if expected_euid != self.owner_euid {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "effective UID changed since job ownership lock acquisition",
            ));
        }

        let (canonical_root, current_root_id) =
            validate_existing_private_lock_root(&self.lock_root_dir, expected_euid)?;
        if canonical_root != self.lock_root_dir || current_root_id != self.lock_root_identity {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "job ownership lock root directory path or inode changed since acquisition",
            ));
        }

        let expected_lock_path = canonical_root.join(expected_key.lock_filename()?);
        if expected_lock_path != self.lock_path {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "job ownership lockfile path does not match canonical lock root and key",
            ));
        }

        let path_id = verify_held_fd_matches_lock_path(
            &self.file,
            &self.lock_path,
            expected_euid,
            self.summary.lock_file_identity,
        )?;
        if path_id != self.summary.lock_file_identity {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "job ownership lockfile inode drifted from held file descriptor",
            ));
        }

        let disk_summary = read_summary_from_path_readonly(&self.lock_path, expected_euid)?
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "active job ownership lockfile summary is missing or unreadable on disk",
                )
            })?;
        disk_summary.validate_for_key(expected_key)?;
        if !constant_time_str_eq(&disk_summary.instance_id, &self.summary.instance_id)
            || !constant_time_str_eq(
                &disk_summary.challenge_digest,
                &self.summary.challenge_digest,
            )
            || disk_summary != self.summary
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "on-disk job ownership lockfile summary drifted from active guard identity",
            ));
        }

        Ok(&self.summary)
    }
}

/// Private per-job OS file-lock ownership store rooted in a canonical `0700` directory.
#[derive(Debug, Clone)]
pub struct CoworkerJobOwnershipStore {
    lock_root_dir: PathBuf,
    lock_root_identity: CoworkerLockFileIdentity,
    owner_euid: u32,
}

impl CoworkerJobOwnershipStore {
    /// Opens a [`CoworkerJobOwnershipStore`] over an existing, canonical, private (`0700`),
    /// non-symlink directory owned by the current effective UID.
    ///
    /// Never creates directories or lockfiles.
    pub fn new(lock_root_dir: impl AsRef<Path>) -> Result<Self, NativeError> {
        Self::new_with_euid(lock_root_dir.as_ref(), current_effective_uid())
    }

    fn new_with_euid(lock_root_dir: &Path, expected_euid: u32) -> Result<Self, NativeError> {
        ensure_platform_supported()?;
        let (canonical_root, root_id) =
            validate_existing_private_lock_root(lock_root_dir, expected_euid)?;
        Ok(Self {
            lock_root_dir: canonical_root,
            lock_root_identity: root_id,
            owner_euid: expected_euid,
        })
    }

    /// Returns the canonical `0700` lock root directory path.
    pub fn lock_root_dir(&self) -> &Path {
        &self.lock_root_dir
    }

    /// Returns the `(dev, ino)` identity of the lock root directory captured when the store was opened.
    pub const fn lock_root_identity(&self) -> CoworkerLockFileIdentity {
        self.lock_root_identity
    }

    /// Returns the deterministic lockfile path (`<lock_root_dir>/coworker-job-ownership-<tag>.lock`)
    /// for `key` without touching the filesystem.
    pub fn lock_path_for_job(&self, key: &CoworkerJobOwnershipKey) -> Result<PathBuf, NativeError> {
        let filename = key.lock_filename()?;
        Ok(self.lock_root_dir.join(filename))
    }

    /// Performs a read-only inspection of `key`'s lockfile state on disk without creating files,
    /// modifying lockfile contents, acknowledging events, or dispatching work.
    pub fn inspect_job_lock(
        &self,
        key: &CoworkerJobOwnershipKey,
    ) -> Result<CoworkerJobLockState, NativeError> {
        self.ensure_root_unchanged()?;
        let lock_path = self.lock_path_for_job(key)?;
        inspect_lockfile_readonly(&lock_path, key, self.owner_euid)
    }

    /// Attempts to acquire exclusive OS file-lock ownership for `key` without blocking.
    ///
    /// On success, mints a fresh CSPRNG `instance_id` (`cwo_<64hex>`) and `challenge_secret`
    /// (`cwc_<64hex>`), writes the public [`CoworkerOwnershipSummary`] in-place to the locked
    /// file descriptor, and returns [`CoworkerOwnershipAcquireOutcome::Acquired`]. If another
    /// handle currently holds the kernel lock, returns [`CoworkerOwnershipAcquireOutcome::Busy`]
    /// immediately.
    pub fn try_acquire_job(
        &self,
        key: &CoworkerJobOwnershipKey,
        acquired_at_ms: u64,
    ) -> Result<CoworkerOwnershipAcquireOutcome, NativeError> {
        self.ensure_root_unchanged()?;
        let lock_path = self.lock_path_for_job(key)?;
        try_acquire_lockfile(
            &self.lock_root_dir,
            self.lock_root_identity,
            &lock_path,
            key,
            self.owner_euid,
            acquired_at_ms,
        )
    }

    /// Convenience non-blocking acquisition helper returning `Ok(Some(guard))` when acquired or
    /// `Ok(None)` when the job lock is currently busy.
    pub fn try_acquire_job_option(
        &self,
        key: &CoworkerJobOwnershipKey,
        acquired_at_ms: u64,
    ) -> Result<Option<CoworkerJobOwnershipGuard>, NativeError> {
        Ok(self.try_acquire_job(key, acquired_at_ms)?.into_guard())
    }

    /// Attempts non-blocking lock acquisition when a caller presents a copied or persisted
    /// [`CoworkerOwnershipSummary`] from another process or prior run.
    ///
    /// Never trusts `copied_summary.instance_id`, `copied_summary.pid_hint`, or
    /// `copied_summary.acquired_at_ms` to bypass the kernel lock: if the lock is held, returns
    /// [`CoworkerOwnershipAcquireOutcome::Busy`]; if the lock is free, acquires it and mints a
    /// brand-new CSPRNG `instance_id` distinct from `copied_summary.instance_id`.
    pub fn try_acquire_job_with_copied_summary(
        &self,
        key: &CoworkerJobOwnershipKey,
        copied_summary: &CoworkerOwnershipSummary,
        acquired_at_ms: u64,
    ) -> Result<CoworkerOwnershipAcquireOutcome, NativeError> {
        copied_summary.validate_for_key(key)?;
        self.try_acquire_job(key, acquired_at_ms)
    }

    /// Verifies that `guard` belongs to this store's canonical lock root directory and passes full
    /// pre-effect path, inode, permission, and summary validation for `expected_key`.
    pub fn verify_live_guard<'a>(
        &self,
        guard: &'a CoworkerJobOwnershipGuard,
        expected_key: &CoworkerJobOwnershipKey,
    ) -> Result<&'a CoworkerOwnershipSummary, NativeError> {
        self.ensure_root_unchanged()?;
        if guard.lock_root_dir != self.lock_root_dir
            || guard.lock_root_identity != self.lock_root_identity
            || guard.owner_euid != self.owner_euid
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "job ownership guard does not belong to this store's lock root directory",
            ));
        }
        guard.validate_before_effect_with_euid(expected_key, self.owner_euid)
    }

    /// Verifies `verify_live_guard` and validates an in-memory [`CoworkerDriverOwnershipClaim`].
    pub fn verify_live_claim<'a>(
        &self,
        guard: &'a CoworkerJobOwnershipGuard,
        expected_key: &CoworkerJobOwnershipKey,
        claim: &CoworkerDriverOwnershipClaim,
    ) -> Result<&'a CoworkerOwnershipSummary, NativeError> {
        let _ = self.verify_live_guard(guard, expected_key)?;
        guard.validate_driver_claim(expected_key, claim)
    }

    fn ensure_root_unchanged(&self) -> Result<(), NativeError> {
        if current_effective_uid() != self.owner_euid {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "current process effective UID does not match store owner EUID",
            ));
        }
        let (canonical_root, current_id) =
            validate_existing_private_lock_root(&self.lock_root_dir, self.owner_euid)?;
        if canonical_root != self.lock_root_dir || current_id != self.lock_root_identity {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "job ownership lock root directory path or inode drifted after store open",
            ));
        }
        Ok(())
    }
}

fn current_effective_uid() -> u32 {
    #[cfg(unix)]
    {
        rustix::process::geteuid().as_raw()
    }
    #[cfg(not(unix))]
    {
        0
    }
}

fn validate_prefixed_csprng_token(
    raw: &str,
    expected_prefix: &str,
    field: &'static str,
) -> Result<(), NativeError> {
    let Some(hex_part) = raw
        .strip_prefix(expected_prefix)
        .and_then(|rest| rest.strip_prefix('_'))
    else {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{field} must have prefix `{expected_prefix}_`"),
        ));
    };
    let _ = validate_sha256_hex_digest(hex_part, field).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{field} must contain 64 lowercase hexadecimal characters after `{expected_prefix}_`"),
        )
    })?;
    Ok(())
}

fn validate_existing_private_lock_root(
    dir: &Path,
    expected_euid: u32,
) -> Result<(PathBuf, CoworkerLockFileIdentity), NativeError> {
    ensure_platform_supported()?;
    let clean = validate_absolute_no_symlink_components(dir, "job_ownership_lock_root")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

        let lstat = fs::symlink_metadata(&clean).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                NativeError::new(
                    NativeErrorCode::NotConfigured,
                    "required job ownership lock root directory does not exist",
                )
            } else {
                NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to stat job ownership lock root directory: {}",
                        sanitize_io_error(&err)
                    ),
                )
            }
        })?;
        if lstat.file_type().is_symlink() || !lstat.file_type().is_dir() {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "job ownership lock root must be an existing non-symlink directory",
            ));
        }
        if lstat.uid() != expected_euid {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "job ownership lock root directory UID does not match current effective UID",
            ));
        }
        if (lstat.permissions().mode() & 0o7777) != 0o700 {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "job ownership lock root directory must have exact 0700 private permissions",
            ));
        }

        let dir_handle = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&clean)
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "failed to open job ownership lock root directory with O_NOFOLLOW | O_CLOEXEC",
                )
            })?;
        let fstat = dir_handle.metadata().map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to fstat job ownership lock root directory: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        if !fstat.is_dir()
            || fstat.dev() != lstat.dev()
            || fstat.ino() != lstat.ino()
            || fstat.uid() != expected_euid
            || (fstat.permissions().mode() & 0o7777) != 0o700
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "job ownership lock root directory failed fd/path identity or permission check",
            ));
        }

        let canonical = fs::canonicalize(&clean).map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to canonicalize job ownership lock root directory: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        if canonical != clean {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "job ownership lock root directory must be canonical without symlink components",
            ));
        }

        Ok((
            clean,
            CoworkerLockFileIdentity {
                dev: fstat.dev(),
                ino: fstat.ino(),
            },
        ))
    }

    #[cfg(not(unix))]
    {
        let _ = (clean, expected_euid);
        ensure_platform_supported()?;
        unreachable!()
    }
}

#[cfg(unix)]
fn validate_lockfile_metadata_pair(
    lstat: &fs::Metadata,
    fstat: &fs::Metadata,
    expected_euid: u32,
) -> Result<CoworkerLockFileIdentity, NativeError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    if lstat.file_type().is_symlink()
        || !lstat.file_type().is_file()
        || !fstat.file_type().is_file()
    {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "job ownership lockfile must be a regular non-symlink file",
        ));
    }
    if lstat.dev() != fstat.dev() || lstat.ino() != fstat.ino() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "job ownership lockfile fd/path (dev, ino) identity mismatch",
        ));
    }
    if lstat.uid() != expected_euid || fstat.uid() != expected_euid {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "job ownership lockfile UID does not match current effective UID",
        ));
    }
    if (lstat.permissions().mode() & 0o7777) != 0o600
        || (fstat.permissions().mode() & 0o7777) != 0o600
    {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "job ownership lockfile must have exact 0600 private permissions",
        ));
    }
    if lstat.nlink() != 1 || fstat.nlink() != 1 {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "job ownership lockfile must not have unexpected hardlinks (nlink != 1)",
        ));
    }
    Ok(CoworkerLockFileIdentity {
        dev: fstat.dev(),
        ino: fstat.ino(),
    })
}

fn verify_held_fd_matches_lock_path(
    file: &File,
    lock_path: &Path,
    expected_euid: u32,
    expected_identity: CoworkerLockFileIdentity,
) -> Result<CoworkerLockFileIdentity, NativeError> {
    validate_private_file_path(lock_path, "job_ownership_lockfile")?;

    #[cfg(unix)]
    {
        let lstat = fs::symlink_metadata(lock_path).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "active job ownership lockfile was unlinked from lock root directory",
                )
            } else {
                NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to stat job ownership lockfile: {}",
                        sanitize_io_error(&err)
                    ),
                )
            }
        })?;
        let fstat = file.metadata().map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to fstat held job ownership lockfile: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        let current_id = validate_lockfile_metadata_pair(&lstat, &fstat, expected_euid)?;
        if current_id != expected_identity {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "job ownership lockfile (dev, ino) changed from held guard identity",
            ));
        }
        Ok(current_id)
    }

    #[cfg(not(unix))]
    {
        let _ = (file, lock_path, expected_euid, expected_identity);
        ensure_platform_supported()?;
        unreachable!()
    }
}

#[cfg(unix)]
fn is_lock_contended_error(err: &std::io::Error) -> bool {
    if err.kind() == std::io::ErrorKind::WouldBlock
        || err.raw_os_error() == fs2::lock_contended_error().raw_os_error()
    {
        return true;
    }
    matches!(
        err.raw_os_error(),
        Some(code)
            if code == libc::EWOULDBLOCK || code == libc::EAGAIN || code == libc::EACCES
    )
}

#[cfg(unix)]
fn read_summary_from_open_file(
    file: &File,
    expected_key: Option<&CoworkerJobOwnershipKey>,
) -> Result<Option<CoworkerOwnershipSummary>, NativeError> {
    let fstat = file.metadata().map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to stat open job ownership lockfile: {}",
                sanitize_io_error(&err)
            ),
        )
    })?;
    if fstat.len() == 0 {
        return Ok(None);
    }
    if fstat.len() > MAX_COWORKER_OWNERSHIP_METADATA_BYTES as u64 {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            "job ownership lockfile metadata exceeds maximum bounded byte size",
        ));
    }
    let mut reader = file;
    let _ = reader.seek(SeekFrom::Start(0));
    let mut buf = Vec::with_capacity(fstat.len() as usize);
    reader
        .take((MAX_COWORKER_OWNERSHIP_METADATA_BYTES as u64).saturating_add(1))
        .read_to_end(&mut buf)
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to read job ownership lockfile metadata: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
    if buf.is_empty() {
        return Ok(None);
    }
    if buf.len() > MAX_COWORKER_OWNERSHIP_METADATA_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            "job ownership lockfile metadata exceeds maximum bounded byte size",
        ));
    }
    let Ok(summary) = serde_json::from_slice::<CoworkerOwnershipSummary>(&buf) else {
        return Ok(None);
    };
    if let Some(key) = expected_key {
        if summary.validate_for_key(key).is_err() {
            return Ok(None);
        }
    } else if summary.validate().is_err() {
        return Ok(None);
    }
    Ok(Some(summary))
}

fn read_summary_from_path_readonly(
    lock_path: &Path,
    expected_euid: u32,
) -> Result<Option<CoworkerOwnershipSummary>, NativeError> {
    validate_private_file_path(lock_path, "job_ownership_lockfile")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        let lstat = match fs::symlink_metadata(lock_path) {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to stat job ownership lockfile: {}",
                        sanitize_io_error(&err)
                    ),
                ));
            }
        };
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(lock_path)
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "failed to open job ownership lockfile safely with O_NOFOLLOW | O_CLOEXEC",
                )
            })?;
        let fstat = file.metadata().map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to fstat opened job ownership lockfile: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        let _ = validate_lockfile_metadata_pair(&lstat, &fstat, expected_euid)?;
        read_summary_from_open_file(&file, None)
    }

    #[cfg(not(unix))]
    {
        let _ = (lock_path, expected_euid);
        ensure_platform_supported()?;
        unreachable!()
    }
}

fn inspect_lockfile_readonly(
    lock_path: &Path,
    expected_key: &CoworkerJobOwnershipKey,
    expected_euid: u32,
) -> Result<CoworkerJobLockState, NativeError> {
    expected_key.validate()?;
    validate_private_file_path(lock_path, "job_ownership_lockfile")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        let lstat = match fs::symlink_metadata(lock_path) {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(CoworkerJobLockState::Absent);
            }
            Err(err) => {
                return Err(NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to stat job ownership lockfile: {}",
                        sanitize_io_error(&err)
                    ),
                ));
            }
        };
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(lock_path)
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "failed to open job ownership lockfile safely with O_NOFOLLOW | O_CLOEXEC",
                )
            })?;
        let fstat = file.metadata().map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to fstat opened job ownership lockfile: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        let lock_file_identity = validate_lockfile_metadata_pair(&lstat, &fstat, expected_euid)?;
        let recorded_summary = read_summary_from_open_file(&file, Some(expected_key))?;

        match file.try_lock_exclusive() {
            Ok(()) => {
                let _ = file.unlock();
                Ok(CoworkerJobLockState::UnlockedStaleFile {
                    recorded_summary,
                    lock_file_identity,
                })
            }
            Err(err) if is_lock_contended_error(&err) => {
                Ok(CoworkerJobLockState::HeldByActiveProcess {
                    recorded_summary,
                    lock_file_identity,
                })
            }
            Err(err) => Err(NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to probe job ownership lockfile state: {}",
                    sanitize_io_error(&err)
                ),
            )),
        }
    }

    #[cfg(not(unix))]
    {
        let _ = (lock_path, expected_key, expected_euid);
        ensure_platform_supported()?;
        unreachable!()
    }
}

fn try_acquire_lockfile(
    lock_root_dir: &Path,
    lock_root_identity: CoworkerLockFileIdentity,
    lock_path: &Path,
    key: &CoworkerJobOwnershipKey,
    expected_euid: u32,
    acquired_at_ms: u64,
) -> Result<CoworkerOwnershipAcquireOutcome, NativeError> {
    key.validate()?;
    validate_private_file_path(lock_path, "job_ownership_lockfile")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

        let pre_lstat = match fs::symlink_metadata(lock_path) {
            Ok(meta) => {
                if meta.file_type().is_symlink() || !meta.file_type().is_file() {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "job ownership lockfile must be a regular non-symlink file",
                    ));
                }
                if meta.uid() != expected_euid {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "job ownership lockfile UID does not match current effective UID",
                    ));
                }
                if (meta.permissions().mode() & 0o7777) != 0o600 {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "job ownership lockfile must have exact 0600 private permissions",
                    ));
                }
                if meta.nlink() != 1 {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "job ownership lockfile must not have unexpected hardlinks (nlink != 1)",
                    ));
                }
                Some(meta)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => {
                return Err(NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to stat job ownership lockfile: {}",
                        sanitize_io_error(&err)
                    ),
                ));
            }
        };

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(lock_path)
            .map_err(|err| {
                if err.raw_os_error() == Some(libc::ELOOP)
                    || err.kind() == std::io::ErrorKind::PermissionDenied
                {
                    NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "failed to open job ownership lockfile with O_NOFOLLOW | O_CLOEXEC",
                    )
                } else {
                    NativeError::new(
                        NativeErrorCode::IoError,
                        format!(
                            "failed to open job ownership lockfile: {}",
                            sanitize_io_error(&err)
                        ),
                    )
                }
            })?;

        let fstat_before = file.metadata().map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to fstat opened job ownership lockfile: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        let post_lstat_before = fs::symlink_metadata(lock_path).map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to stat opened job ownership lockfile path: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        if let Some(ref pre) = pre_lstat {
            if pre.dev() != fstat_before.dev() || pre.ino() != fstat_before.ino() {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "job ownership lockfile inode changed during open",
                ));
            }
        }
        let _ = validate_lockfile_metadata_pair(&post_lstat_before, &fstat_before, expected_euid)?;

        match file.try_lock_exclusive() {
            Ok(()) => {}
            Err(err) if is_lock_contended_error(&err) => {
                let observed_holder = read_summary_from_open_file(&file, Some(key)).ok().flatten();
                return Ok(CoworkerOwnershipAcquireOutcome::Busy { observed_holder });
            }
            Err(err) => {
                return Err(NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to acquire exclusive job ownership lock: {}",
                        sanitize_io_error(&err)
                    ),
                ));
            }
        }

        // Re-verify fd and path identity after acquiring the kernel lock.
        let fstat_locked = file.metadata().map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to fstat locked job ownership lockfile: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        let post_lstat_locked = fs::symlink_metadata(lock_path).map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to stat locked job ownership lockfile path: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        let lock_file_identity =
            validate_lockfile_metadata_pair(&post_lstat_locked, &fstat_locked, expected_euid)?;
        if fstat_locked.dev() != fstat_before.dev() || fstat_locked.ino() != fstat_before.ino() {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "job ownership lockfile inode drifted before lock acquisition",
            ));
        }

        let instance_id = generate_prefixed_random_id(COWORKER_OWNERSHIP_INSTANCE_PREFIX);
        let challenge_secret = generate_prefixed_random_id(COWORKER_OWNERSHIP_CHALLENGE_PREFIX);
        let challenge_digest = sha256_hex(challenge_secret.as_bytes());
        let summary = CoworkerOwnershipSummary {
            schema_version: COWORKER_JOB_OWNERSHIP_SCHEMA_VERSION,
            binding_id: key.binding_id.clone(),
            thread_uuid: key.thread_uuid.clone(),
            job_id: key.job_id.clone(),
            instance_id,
            challenge_digest,
            acquired_at_ms,
            pid_hint: std::process::id(),
            lock_file_identity,
            grants_execution_authority: false,
        };
        summary.validate_for_key(key)?;
        write_summary_in_place(&file, &summary)?;

        Ok(CoworkerOwnershipAcquireOutcome::Acquired(
            CoworkerJobOwnershipGuard {
                file,
                lock_root_dir: lock_root_dir.to_path_buf(),
                lock_root_identity,
                lock_path: lock_path.to_path_buf(),
                owner_euid: expected_euid,
                summary,
                challenge_secret,
            },
        ))
    }

    #[cfg(not(unix))]
    {
        let _ = (
            lock_root_dir,
            lock_root_identity,
            lock_path,
            key,
            expected_euid,
            acquired_at_ms,
        );
        ensure_platform_supported()?;
        unreachable!()
    }
}

#[cfg(unix)]
fn write_summary_in_place(
    file: &File,
    summary: &CoworkerOwnershipSummary,
) -> Result<(), NativeError> {
    let bytes = serde_json::to_vec_pretty(summary).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to serialize coworker job ownership summary JSON",
        )
    })?;
    if bytes.len() > MAX_COWORKER_OWNERSHIP_METADATA_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            "coworker job ownership summary exceeds maximum metadata byte budget",
        ));
    }
    file.set_len(0).map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to truncate locked job ownership lockfile in-place: {}",
                sanitize_io_error(&err)
            ),
        )
    })?;
    let mut writer = file;
    writer.seek(SeekFrom::Start(0)).map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to seek locked job ownership lockfile: {}",
                sanitize_io_error(&err)
            ),
        )
    })?;
    writer.write_all(&bytes).map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to write locked job ownership summary: {}",
                sanitize_io_error(&err)
            ),
        )
    })?;
    file.sync_all().map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to sync locked job ownership summary: {}",
                sanitize_io_error(&err)
            ),
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    #[test]
    fn euid_mismatch_is_rejected_on_lock_root_and_before_effect() {
        let tmp = TempDir::new().expect("tempdir");
        let root = fs::canonicalize(tmp.path())
            .expect("canonicalize")
            .join("locks");
        fs::create_dir(&root).expect("create locks dir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("chmod 0700");

        let real_euid = current_effective_uid();
        let wrong_euid = real_euid.wrapping_add(1);

        let err = CoworkerJobOwnershipStore::new_with_euid(&root, wrong_euid)
            .expect_err("wrong EUID must be rejected when opening store");
        assert_eq!(err.error_code(), NativeErrorCode::SecurityViolation);

        let store = CoworkerJobOwnershipStore::new(&root).expect("open store");
        let key = CoworkerJobOwnershipKey::new(
            "bnd_alpha",
            "550e8400-e29b-41d4-a716-446655440000",
            "job_alpha",
        )
        .expect("valid key");
        let guard = store
            .try_acquire_job_option(&key, 1_700_000_000_000)
            .expect("acquire")
            .expect("acquired guard");

        let drift_err = guard
            .validate_before_effect_with_euid(&key, wrong_euid)
            .expect_err("EUID drift before effect must be rejected");
        assert_eq!(drift_err.error_code(), NativeErrorCode::SecurityViolation);
    }
}
