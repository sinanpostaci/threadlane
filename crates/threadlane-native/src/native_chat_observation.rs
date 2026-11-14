//! Continuous `connect_current_chat`-gated native Codex chat observation sampler and bounded
//! private `WorkspaceNativeSample` sidecar store.
//!
//! ## Authority and Sampling Invariants
//!
//! - **Strict `connect_current_chat` gate**: A background sampler is started only after
//!   `connect_current_chat` independently verifies the managed Linux desktop parent connection,
//!   loaded thread candidate, canonical repository `cwd`, control-plane binding, and pairing-store
//!   enrollment. Caller hints (`_meta.threadId`), `connection_identity`, or manual operator
//!   enrollment alone never start a sampler.
//! - **Read-only metadata over verified parent WebSocket**: Polls `thread/read` with
//!   `includeTurns: false` on the existing verified managed parent Unix WebSocket without
//!   requiring a new MCP call or worker result, never spawning a second `codex app-server`, never
//!   reading transcripts, never installing an OS service, and never mutating global environment
//!   state.
//! - **Truthful activity and selection**: Maps only backend-proven thread statuses and
//!   `activeFlags` to `working`, `idle`, `waiting_for_approval`, and `waiting_for_input`; missing,
//!   unrecognized, or conflicting flags degrade to `unknown`. Whenever availability is
//!   `disconnected`, `stale`, or `unavailable`, activity is `unknown` and configured model/effort
//!   selection is cleared (`unavailable`). Configured selection is always classified as
//!   `verified_configuration` (`counts_as_runtime_telemetry == false`).
//! - **Verified display title only**: `thread_title` is populated only from a successful
//!   exact-thread/project/enrollment/parent-verified read. Failed, timed-out, disconnected,
//!   scope-mismatched, and otherwise unverified samples never carry one.
//! - **Canonical source identity**: Derives `source_id` (`src_<64hex>`) from canonical validated
//!   `(plans_dir, authority_dir, sessions_dir)` paths using the exact
//!   `threadlane-observation-source-v1\0` SHA-256 framing used by desktop observation source
//!   discovery, falling back to `"default"` only when neither workflow nor host stores are
//!   configured on a minimal runtime server.
//! - **Bounded private persistence**: Stores atomic `0600` JSON sidecars under
//!   `<state_dir>/native-chat-samples/` (`0700`) outside any source repository, enforcing
//!   `reconcile_workspace_native_sample`, `<= 64 KiB` per sidecar, and `<= 128` samples per
//!   `source_id`.

use std::collections::BTreeSet;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sha2::{Digest, Sha256};
use threadlane::NativeReconnectConfig;
use threadlane_core::observation::{
    bound_workspace_native_samples_per_source, current_observation_time_ms,
    parse_workspace_native_sample_sidecar, reconcile_workspace_native_sample,
    sanitize_observation_label, serialize_workspace_native_sample_sidecar,
    ObservationProcessInstance, WorkspaceActivityState, WorkspaceAvailabilityState,
    WorkspaceCapabilityState, WorkspaceChatIdentity, WorkspaceConfiguredSelection,
    WorkspaceConnectionState, WorkspaceContractError, WorkspaceEvaluatedNativeSample,
    WorkspaceFreshnessState, WorkspaceNativeCapabilities, WorkspaceNativeSample,
    WorkspaceSampleConnection, WorkspaceSampleReconcileDecision, WorkspaceSamplingPolicy,
    MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE, MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES,
    WORKSPACE_SAMPLE_STALE_THRESHOLD_MS,
};
use tokio::sync::{watch, Mutex as AsyncMutex};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::auto_pair::{validate_refreshed_thread_metadata, AutomaticPairing};
use crate::client::{NativeClient, NativeThreadMetadata, NativeThreadStatusDetail};
use crate::context_export::OperatorContextServerConfig;
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    acquire_cross_process_lock, ensure_platform_supported, generate_prefixed_random_id,
    prepare_private_dir, read_private_file_bounded, sanitize_diagnostic_text, sanitize_io_error,
    validate_absolute_no_symlink_components, write_private_file_atomic,
};
use crate::operator_cli::{validate_dirs, OperatorCliError};
use crate::pairing::PairingStore;

/// Subdirectory name under the private runtime `state_dir` holding native chat sample sidecars.
pub const NATIVE_CHAT_SAMPLES_DIR_NAME: &str = "native-chat-samples";

/// Cross-process lock filename inside `<state_dir>/native-chat-samples/`.
pub const NATIVE_CHAT_SAMPLES_LOCK_FILENAME: &str = ".native-chat-samples.lock";

/// Filename prefix for deterministic per-chat sample sidecars (`sample-<sha256>.json`).
pub const NATIVE_CHAT_SAMPLE_FILE_PREFIX: &str = "sample-";

/// Verified route identifier attached to managed Linux desktop native chat samples.
pub const NATIVE_CHAT_SAMPLE_ROUTE: &str = "managed_linux_desktop";

/// Maximum directory entries scanned when reading `<state_dir>/native-chat-samples/`.
const MAX_SAMPLE_DIR_SCAN_ENTRIES: usize = 1024;

/// Returns the shared `<state_dir>/native-chat-samples` directory path.
#[must_use]
pub fn native_chat_samples_dir(state_dir: &Path) -> PathBuf {
    state_dir.join(NATIVE_CHAT_SAMPLES_DIR_NAME)
}

/// Computes the deterministic collision-free sidecar filename (`sample-<64hex>.json`) for a
/// validated [`WorkspaceChatIdentity`].
pub fn native_chat_sample_filename(
    identity: &WorkspaceChatIdentity,
) -> Result<String, NativeError> {
    let canonical_key = identity.canonical_key().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "workspace chat identity failed validation for native sample filename",
        )
    })?;
    let digest = Sha256::digest(canonical_key.as_bytes());
    Ok(format!(
        "{NATIVE_CHAT_SAMPLE_FILE_PREFIX}{}.json",
        encode_hex_lower(&digest)
    ))
}

/// Computes the deterministic sidecar path (`<state_dir>/native-chat-samples/sample-<64hex>.json`)
/// for a validated [`WorkspaceChatIdentity`].
pub fn native_chat_sample_path(
    state_dir: &Path,
    identity: &WorkspaceChatIdentity,
) -> Result<PathBuf, NativeError> {
    Ok(native_chat_samples_dir(state_dir).join(native_chat_sample_filename(identity)?))
}

/// Generates a fresh CSPRNG `producer_epoch` token (`pepc_<64hex>`).
#[must_use]
pub fn generate_sample_producer_epoch() -> String {
    generate_prefixed_random_id("pepc")
}

/// Computes the canonical `src_<64hex>` observation source identifier from validated private
/// `(plans_dir, authority_dir, sessions_dir)` store paths using the exact
/// `threadlane-observation-source-v1\0` SHA-256 algorithm shared with desktop source discovery.
pub fn compute_observation_source_id(
    plans_dir: &Path,
    authority_dir: &Path,
    sessions_dir: &Path,
) -> Result<String, NativeError> {
    let validated = validate_dirs(&[
        ("plans_dir", plans_dir),
        ("authority_dir", authority_dir),
        ("sessions_dir", sessions_dir),
    ])
    .map_err(|err| map_dir_error("observation_stores", err))?;

    Ok(compute_stable_source_id_from_canonical(
        &validated[0],
        &validated[1],
        &validated[2],
    ))
}

#[cfg(unix)]
fn compute_stable_source_id_from_canonical(
    plans_dir: &Path,
    authority_dir: &Path,
    sessions_dir: &Path,
) -> String {
    use std::os::unix::ffi::OsStrExt;

    let mut hasher = Sha256::new();
    hasher.update(b"threadlane-observation-source-v1\0");
    for (label, path) in [
        ("plans_dir", plans_dir),
        ("authority_dir", authority_dir),
        ("sessions_dir", sessions_dir),
    ] {
        let raw = path.as_os_str().as_bytes();
        hasher.update(label.as_bytes());
        hasher.update(b"\0");
        hasher.update((raw.len() as u64).to_be_bytes());
        hasher.update(raw);
        hasher.update(b"\0");
    }
    let digest = hasher.finalize();
    format!("src_{}", encode_hex_lower(&digest))
}

#[cfg(not(unix))]
fn compute_stable_source_id_from_canonical(
    plans_dir: &Path,
    authority_dir: &Path,
    sessions_dir: &Path,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"threadlane-observation-source-v1\0");
    for (label, path) in [
        ("plans_dir", plans_dir),
        ("authority_dir", authority_dir),
        ("sessions_dir", sessions_dir),
    ] {
        let raw = path.to_string_lossy();
        let bytes = raw.as_bytes();
        hasher.update(label.as_bytes());
        hasher.update(b"\0");
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
        hasher.update(b"\0");
    }
    let digest = hasher.finalize();
    format!("src_{}", encode_hex_lower(&digest))
}

/// Reads the live OS [`ObservationProcessInstance`] (`pid`, kernel `boot_id`, and Linux start
/// ticks) for `pid` from `/proc`, returning `None` if `pid == 0`, missing, or zombie/dead.
#[must_use]
pub fn read_backend_process_instance(pid: u32) -> Option<ObservationProcessInstance> {
    if pid == 0 {
        return None;
    }
    let proc_root = Path::new("/proc");
    if !proc_root.is_dir() {
        return None;
    }
    let boot_id = fs::read_to_string(proc_root.join("sys/kernel/random/boot_id"))
        .ok()
        .map(|s| s.trim().to_string())?;
    if boot_id.is_empty() || boot_id.len() > 128 || !boot_id.bytes().all(|b| b.is_ascii_graphic()) {
        return None;
    }

    let stat = fs::read_to_string(proc_root.join(pid.to_string()).join("stat")).ok()?;
    let rparen = stat.rfind(')')?;
    let after = stat.get(rparen + 1..)?.trim_start();
    let mut fields = after.split_whitespace();
    let state = fields.next()?;
    if state.starts_with('Z') || state.starts_with('X') || state.starts_with('x') {
        return None;
    }
    // Field 3 is `state` (consumed above). Field 22 is `starttime`, 18 fields after `state`.
    let start_ticks = fields.nth(18)?.parse::<u64>().ok()?;

    Some(ObservationProcessInstance {
        pid,
        boot_id,
        start_ticks,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum KnownActiveFlag {
    WaitingForApproval,
    WaitingForInput,
}

/// Maps a verified [`NativeThreadStatusDetail`] from an available thread into proven
/// `(`[`WorkspaceActivityState`]`, `[`WorkspaceCapabilityState`]`)`.
///
/// Only backend-proven statuses and `activeFlags` produce `working`, `idle`,
/// `waiting_for_approval`, or `waiting_for_input`. Plain `"active"` strings without structured
/// `activeFlags`, missing `activeFlags` arrays, malformed flag entries, unrecognized flag tokens,
/// or conflicting waiting flags degrade to [`WorkspaceActivityState::Unknown`].
#[must_use]
pub fn prove_native_thread_activity(
    detail: &NativeThreadStatusDetail,
) -> (WorkspaceActivityState, WorkspaceCapabilityState) {
    match detail.status.as_str() {
        "idle" => {
            if detail.has_invalid_active_flags || !detail.active_flags.is_empty() {
                (
                    WorkspaceActivityState::Unknown,
                    WorkspaceCapabilityState::Unknown,
                )
            } else {
                let cap = if detail.tagged {
                    WorkspaceCapabilityState::Supported
                } else {
                    WorkspaceCapabilityState::Unknown
                };
                (WorkspaceActivityState::Idle, cap)
            }
        }
        "active" => {
            if !detail.tagged || !detail.active_flags_present || detail.has_invalid_active_flags {
                return (
                    WorkspaceActivityState::Unknown,
                    WorkspaceCapabilityState::Unknown,
                );
            }
            let mut parsed_flags = BTreeSet::new();
            for raw_flag in &detail.active_flags {
                match raw_flag.as_str() {
                    "waitingOnApproval" | "waiting_on_approval" => {
                        parsed_flags.insert(KnownActiveFlag::WaitingForApproval);
                    }
                    "waitingOnUserInput" | "waiting_on_user_input" => {
                        parsed_flags.insert(KnownActiveFlag::WaitingForInput);
                    }
                    _ => {
                        return (
                            WorkspaceActivityState::Unknown,
                            WorkspaceCapabilityState::Unknown,
                        );
                    }
                }
            }
            match parsed_flags.len() {
                0 => (
                    WorkspaceActivityState::Working,
                    WorkspaceCapabilityState::Supported,
                ),
                1 => {
                    let activity = match parsed_flags.iter().next().copied() {
                        Some(KnownActiveFlag::WaitingForApproval) => {
                            WorkspaceActivityState::WaitingForApproval
                        }
                        Some(KnownActiveFlag::WaitingForInput) => {
                            WorkspaceActivityState::WaitingForInput
                        }
                        None => WorkspaceActivityState::Unknown,
                    };
                    (activity, WorkspaceCapabilityState::Supported)
                }
                _ => (
                    WorkspaceActivityState::Unknown,
                    WorkspaceCapabilityState::Unknown,
                ),
            }
        }
        _ => (
            WorkspaceActivityState::Unknown,
            WorkspaceCapabilityState::Unknown,
        ),
    }
}

/// Outcome of reconciling and persisting a [`WorkspaceNativeSample`] sidecar on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceSampleWriteOutcome {
    /// Reconciliation decision against any prior persisted sample for the same chat identity.
    pub decision: WorkspaceSampleReconcileDecision,
    /// Canonical path to the `0600` sample sidecar file under `<state_dir>/native-chat-samples`.
    pub path: PathBuf,
    /// Count of older sample files pruned for the same `source_id` to maintain the `128` cap.
    pub pruned_for_source: usize,
}

/// Read-only projection of a persisted [`WorkspaceNativeSample`] paired with its live evaluation
/// at a caller-supplied reference timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeChatSampleReadEntry {
    /// Canonical path of the `0600` sidecar file from which this sample was read.
    pub path: PathBuf,
    /// Persisted sample payload with original `observed_at_ms` and `last_success_at_ms` preserved.
    pub sample: WorkspaceNativeSample,
    /// Live evaluation at `reference_now_ms` against `stale_threshold_ms` (`10,000 ms` default).
    pub evaluated: WorkspaceEvaluatedNativeSample,
}

/// Bounded read-only snapshot of native chat samples loaded from `<state_dir>/native-chat-samples`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeChatSamplesReadReport {
    /// Canonical `<state_dir>/native-chat-samples` directory path.
    pub samples_dir: PathBuf,
    /// Retained samples bounded to [`MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE`] (`128`) per `source_id`.
    pub entries: Vec<NativeChatSampleReadEntry>,
    /// Count of samples omitted due to the per-source `128` cap or failed validation.
    pub omitted_count: usize,
    /// Sanitized non-fatal warnings encountered while scanning sample sidecars.
    pub warnings: Vec<String>,
}

/// Reconciles and atomically persists `sample` under `<state_dir>/native-chat-samples/` (`0700`
/// directory, `0600` file), enforcing repository disjointness, [`reconcile_workspace_native_sample`],
/// [`MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES`] (`64 KiB`), and
/// [`MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE`] (`128` samples per `source_id`) by completing
/// validated stale-sample pruning and directory sync before installing any new retained sidecar.
pub fn write_workspace_native_sample_sidecar(
    state_dir: &Path,
    sample: &WorkspaceNativeSample,
) -> Result<WorkspaceSampleWriteOutcome, NativeError> {
    write_workspace_native_sample_sidecar_with(
        state_dir,
        sample,
        |stale_path| fs::remove_file(stale_path),
        |dir_path| File::open(dir_path),
        |dir_file| dir_file.sync_all(),
    )
}

fn write_workspace_native_sample_sidecar_with<RemoveStale, OpenDir, SyncDir>(
    state_dir: &Path,
    sample: &WorkspaceNativeSample,
    mut remove_stale_file: RemoveStale,
    open_samples_dir: OpenDir,
    sync_samples_dir: SyncDir,
) -> Result<WorkspaceSampleWriteOutcome, NativeError>
where
    RemoveStale: FnMut(&Path) -> std::io::Result<()>,
    OpenDir: FnOnce(&Path) -> std::io::Result<File>,
    SyncDir: FnOnce(&File) -> std::io::Result<()>,
{
    ensure_platform_supported()?;
    sample.validate().map_err(map_contract_error)?;
    let normalized = sample.normalized();
    normalized.validate().map_err(map_contract_error)?;

    let validated_state = validate_dirs(&[("state_dir", state_dir)])
        .map_err(|err| map_dir_error("state_dir", err))?;
    let clean_state_dir = validated_state[0].clone();

    let project_path = Path::new(&normalized.chat_identity.canonical_project_scope);
    if paths_overlap(&clean_state_dir, project_path) {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "native chat sample state_dir must not equal or overlap canonical_project_scope",
        ));
    }

    let serialized =
        serialize_workspace_native_sample_sidecar(&normalized).map_err(map_contract_error)?;
    let filename = native_chat_sample_filename(&normalized.chat_identity)?;

    let samples_dir = ensure_owned_samples_dir(&clean_state_dir)?;
    let _lock = acquire_cross_process_lock(
        &samples_dir,
        NATIVE_CHAT_SAMPLES_LOCK_FILENAME,
        "native_chat_samples_lock",
    )?;

    let target_path = samples_dir.join(&filename);
    let existing = match read_private_file_bounded(
        &target_path,
        MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES,
        "native_chat_sample",
    )? {
        Some(bytes) => {
            let parsed = parse_workspace_native_sample_sidecar(&bytes).map_err(|_| {
                NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "existing native chat sample sidecar failed schema verification; refusing to overwrite",
                )
            })?;
            if parsed.chat_identity != normalized.chat_identity {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "existing native chat sample sidecar identity did not match target filename",
                ));
            }
            Some(parsed)
        }
        None => None,
    };

    let decision = reconcile_workspace_native_sample(existing.as_ref(), &normalized);
    if !decision.is_accepted() {
        return Ok(WorkspaceSampleWriteOutcome {
            decision,
            path: target_path,
            pruned_for_source: 0,
        });
    }

    let (incoming_retained, files_to_prune) =
        plan_per_source_retention(&samples_dir, &clean_state_dir, &filename, &normalized)?;

    if !incoming_retained {
        let on_disk_to_prune: Vec<String> = files_to_prune
            .into_iter()
            .filter(|stale_file| stale_file.as_str() != filename || existing.is_some())
            .collect();
        let removed_on_disk = prune_stale_sample_files_and_sync(
            &samples_dir,
            &on_disk_to_prune,
            false,
            &mut remove_stale_file,
            open_samples_dir,
            sync_samples_dir,
        )?;
        let pruned_for_source = removed_on_disk.saturating_add(usize::from(existing.is_none()));
        return Ok(WorkspaceSampleWriteOutcome {
            decision,
            path: target_path,
            pruned_for_source,
        });
    }

    let on_disk_to_prune: Vec<String> = files_to_prune
        .into_iter()
        .filter(|stale_file| stale_file.as_str() != filename)
        .collect();

    if existing.is_none() {
        let pruned_for_source = prune_stale_sample_files_and_sync(
            &samples_dir,
            &on_disk_to_prune,
            false,
            &mut remove_stale_file,
            open_samples_dir,
            sync_samples_dir,
        )?;
        let write_res =
            write_private_file_atomic(&samples_dir, &filename, &serialized, "native_chat_sample");
        let written_path = match write_res {
            Ok(path) => path,
            Err(err) => {
                if !on_disk_to_prune.is_empty() && err.error_code() == NativeErrorCode::IoError {
                    let msg = format!(
                        "pruned stale native_chat_sample sidecars, but failed to persist incoming sample: {}",
                        err.message()
                    );
                    return Err(NativeError::persistence_uncertain(msg));
                }
                return Err(err);
            }
        };
        return Ok(WorkspaceSampleWriteOutcome {
            decision,
            path: written_path,
            pruned_for_source,
        });
    }

    let written_path =
        write_private_file_atomic(&samples_dir, &filename, &serialized, "native_chat_sample")?;
    let pruned_for_source = prune_stale_sample_files_and_sync(
        &samples_dir,
        &on_disk_to_prune,
        true,
        &mut remove_stale_file,
        open_samples_dir,
        sync_samples_dir,
    )?;

    Ok(WorkspaceSampleWriteOutcome {
        decision,
        path: written_path,
        pruned_for_source,
    })
}

fn prune_stale_sample_files_and_sync<RemoveStale, OpenDir, SyncDir>(
    samples_dir: &Path,
    stale_files: &[String],
    sample_already_persisted: bool,
    remove_stale_file: &mut RemoveStale,
    open_samples_dir: OpenDir,
    sync_samples_dir: SyncDir,
) -> Result<usize, NativeError>
where
    RemoveStale: FnMut(&Path) -> std::io::Result<()>,
    OpenDir: FnOnce(&Path) -> std::io::Result<File>,
    SyncDir: FnOnce(&File) -> std::io::Result<()>,
{
    if stale_files.is_empty() {
        return Ok(0);
    }

    let mut pruned_for_source = 0usize;
    let mut mutated_dir = false;

    for stale_file in stale_files {
        if !is_expected_sample_filename_shape(stale_file) {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "refusing to prune unexpected filename in native chat samples directory",
            ));
        }
        let stale_path = samples_dir.join(stale_file);
        let _ = validate_absolute_no_symlink_components(&stale_path, "stale_native_chat_sample")?;
        let meta = match fs::symlink_metadata(&stale_path) {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                mutated_dir = true;
                continue;
            }
            Err(err) => {
                let reason = sanitize_io_error(&err);
                let err_out = if sample_already_persisted || mutated_dir {
                    NativeError::persistence_uncertain(format!(
                        "persisted native_chat_sample state may be visible, but failed to stat stale sample sidecar during bounded retention cleanup: {reason}"
                    ))
                } else {
                    NativeError::new(
                        NativeErrorCode::IoError,
                        format!(
                            "failed to stat stale native_chat_sample sidecar during bounded retention cleanup: {reason}"
                        ),
                    )
                };
                return Err(err_out);
            }
        };

        if meta.file_type().is_symlink() || !meta.file_type().is_file() {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "stale native chat sample sidecar changed to a symlink or non-regular file before retention cleanup",
            ));
        }

        match remove_stale_file(&stale_path) {
            Ok(()) => {
                mutated_dir = true;
                pruned_for_source = pruned_for_source.saturating_add(1);
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                mutated_dir = true;
            }
            Err(err) => {
                let reason = sanitize_io_error(&err);
                let err_out = if sample_already_persisted || mutated_dir {
                    NativeError::persistence_uncertain(format!(
                        "persisted native_chat_sample state may be visible, but failed to remove stale sample sidecar during bounded retention cleanup: {reason}"
                    ))
                } else {
                    NativeError::new(
                        NativeErrorCode::IoError,
                        format!(
                            "failed to remove stale native_chat_sample sidecar during bounded retention cleanup: {reason}"
                        ),
                    )
                };
                return Err(err_out);
            }
        }
    }

    let dir_file = open_samples_dir(samples_dir).map_err(|err| {
        NativeError::persistence_uncertain(format!(
            "persisted native_chat_sample retention cleanup may be visible, but failed to open samples directory to confirm durability: {}",
            sanitize_io_error(&err)
        ))
    })?;
    sync_samples_dir(&dir_file).map_err(|err| {
        NativeError::persistence_uncertain(format!(
            "persisted native_chat_sample retention cleanup may be visible, but failed to sync samples directory to confirm durability: {}",
            sanitize_io_error(&err)
        ))
    })?;

    Ok(pruned_for_source)
}

/// Reads and evaluates a single [`WorkspaceNativeSample`] sidecar in strictly read-only mode
/// without creating directories, lockfiles, or mutating sample timestamps.
pub fn read_workspace_native_sample_readonly(
    state_dir: &Path,
    identity: &WorkspaceChatIdentity,
    reference_now_ms: i64,
) -> Result<Option<NativeChatSampleReadEntry>, NativeError> {
    ensure_platform_supported()?;
    identity.validate().map_err(map_contract_error)?;

    let validated_state = validate_dirs(&[("state_dir", state_dir)])
        .map_err(|err| map_dir_error("state_dir", err))?;
    let clean_state_dir = validated_state[0].clone();

    let project_path = Path::new(&identity.canonical_project_scope);
    if paths_overlap(&clean_state_dir, project_path) {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "native chat sample state_dir must not equal or overlap canonical_project_scope",
        ));
    }

    let expected_samples_dir = native_chat_samples_dir(&clean_state_dir);
    let Some(clean_samples_dir) = validate_optional_existing_private_dir(&expected_samples_dir)?
    else {
        return Ok(None);
    };

    let filename = native_chat_sample_filename(identity)?;
    let sample_path = clean_samples_dir.join(&filename);
    let Some(bytes) = read_existing_private_sample_readonly(&sample_path)? else {
        return Ok(None);
    };

    let sample = parse_workspace_native_sample_sidecar(&bytes).map_err(map_contract_error)?;
    sample.validate().map_err(map_contract_error)?;
    if &sample.chat_identity != identity {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "native chat sample sidecar identity did not match requested chat identity",
        ));
    }

    let evaluated = sample.evaluate_at(reference_now_ms, WORKSPACE_SAMPLE_STALE_THRESHOLD_MS);
    Ok(Some(NativeChatSampleReadEntry {
        path: sample_path,
        sample,
        evaluated,
    }))
}

/// Reads and evaluates all valid [`WorkspaceNativeSample`] sidecars under
/// `<state_dir>/native-chat-samples` in strictly read-only mode, optionally filtered by
/// `source_id_filter`, bounded to [`MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE`] (`128`) per source.
pub fn read_workspace_native_samples_readonly(
    state_dir: &Path,
    source_id_filter: Option<&str>,
    reference_now_ms: i64,
) -> Result<NativeChatSamplesReadReport, NativeError> {
    ensure_platform_supported()?;
    let validated_state = validate_dirs(&[("state_dir", state_dir)])
        .map_err(|err| map_dir_error("state_dir", err))?;
    let clean_state_dir = validated_state[0].clone();
    let expected_samples_dir = native_chat_samples_dir(&clean_state_dir);

    let Some(clean_samples_dir) = validate_optional_existing_private_dir(&expected_samples_dir)?
    else {
        return Ok(NativeChatSamplesReadReport {
            samples_dir: expected_samples_dir,
            entries: Vec::new(),
            omitted_count: 0,
            warnings: Vec::new(),
        });
    };

    let read_dir = fs::read_dir(&clean_samples_dir).map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to enumerate native chat samples directory: {}",
                sanitize_diagnostic_text(&err.kind().to_string())
            ),
        )
    })?;

    let mut warnings = Vec::new();
    let mut raw_samples = Vec::new();
    let mut path_by_key = std::collections::BTreeMap::new();

    for (scanned, entry_res) in read_dir.enumerate() {
        if scanned >= MAX_SAMPLE_DIR_SCAN_ENTRIES {
            warnings.push(format!(
                "native chat samples scan capped at {MAX_SAMPLE_DIR_SCAN_ENTRIES} entries"
            ));
            break;
        }

        let entry = match entry_res {
            Ok(e) => e,
            Err(err) => {
                warnings.push(format!(
                    "skipped unreadable directory entry in native-chat-samples ({})",
                    sanitize_diagnostic_text(&err.kind().to_string())
                ));
                continue;
            }
        };
        let file_name_os = entry.file_name();
        let Some(file_name) = file_name_os.to_str() else {
            warnings.push("skipped non-UTF8 filename in native-chat-samples".to_string());
            continue;
        };
        if file_name == NATIVE_CHAT_SAMPLES_LOCK_FILENAME || file_name.starts_with('.') {
            continue;
        }
        if !is_expected_sample_filename_shape(file_name) {
            warnings.push("skipped unexpected file in native-chat-samples".to_string());
            continue;
        }

        let path = clean_samples_dir.join(file_name);
        let bytes = match read_existing_private_sample_readonly(&path) {
            Ok(Some(b)) => b,
            Ok(None) => continue,
            Err(err) => {
                warnings.push(format!(
                    "skipped invalid native chat sample sidecar ({})",
                    err.code()
                ));
                continue;
            }
        };

        let sample = match parse_workspace_native_sample_sidecar(&bytes) {
            Ok(s) => s,
            Err(_) => {
                warnings.push("skipped malformed native chat sample sidecar JSON".to_string());
                continue;
            }
        };
        if sample.validate().is_err() {
            warnings.push(
                "skipped native chat sample failing identity or epoch validation".to_string(),
            );
            continue;
        }
        let Ok(expected_name) = native_chat_sample_filename(&sample.chat_identity) else {
            warnings.push("skipped native chat sample with invalid canonical key".to_string());
            continue;
        };
        if expected_name != file_name {
            warnings.push(
                "skipped native chat sample whose filename did not match chat_identity".to_string(),
            );
            continue;
        }
        if paths_overlap(
            &clean_state_dir,
            Path::new(&sample.chat_identity.canonical_project_scope),
        ) {
            warnings.push("skipped native chat sample overlapping runtime state_dir".to_string());
            continue;
        }
        if let Some(wanted_source) = source_id_filter {
            if sample.chat_identity.source_id != wanted_source {
                continue;
            }
        }
        if let Some(key) = sample.chat_identity.canonical_key() {
            path_by_key.insert(key, path);
            raw_samples.push(sample);
        }
    }

    let (bounded, omitted_count) = bound_workspace_native_samples_per_source(raw_samples);
    let mut entries = Vec::with_capacity(bounded.len());
    for sample in bounded {
        let Some(key) = sample.chat_identity.canonical_key() else {
            continue;
        };
        let path = path_by_key
            .get(&key)
            .cloned()
            .unwrap_or_else(|| clean_samples_dir.clone());
        let evaluated = sample.evaluate_at(reference_now_ms, WORKSPACE_SAMPLE_STALE_THRESHOLD_MS);
        entries.push(NativeChatSampleReadEntry {
            path,
            sample,
            evaluated,
        });
    }

    Ok(NativeChatSamplesReadReport {
        samples_dir: clean_samples_dir,
        entries,
        omitted_count,
        warnings,
    })
}

type ReconnectResolverFn =
    dyn Fn() -> Result<(NativeReconnectConfig, u32), NativeError> + Send + Sync;
type ProcessInstanceReaderFn = dyn Fn(u32) -> Option<ObservationProcessInstance> + Send + Sync;

/// Explicit managed-parent connection and sampling configuration for [`crate::PairedMcpServer`].
///
/// Production servers omit this and discover the Linux managed desktop parent automatically via
/// [`NativeReconnectConfig::discover_for_mcp_parent`], `std::env::current_dir()`,
/// [`read_backend_process_instance`], and [`WorkspaceSamplingPolicy::default`]. Explicit
/// configuration allows deterministic behavioral testing against isolated private Unix WebSocket
/// fixtures without mutating global process working directories or user runtime sockets.
#[derive(Clone)]
pub struct ManagedParentConnectionConfig {
    reconnect_resolver: Arc<ReconnectResolverFn>,
    project_cwd: PathBuf,
    process_instance_reader: Arc<ProcessInstanceReaderFn>,
    sampling_policy: WorkspaceSamplingPolicy,
}

impl fmt::Debug for ManagedParentConnectionConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedParentConnectionConfig")
            .field("project_cwd", &"[REDACTED]")
            .field("sampling_policy", &self.sampling_policy)
            .finish()
    }
}

impl ManagedParentConnectionConfig {
    /// Creates a [`ManagedParentConnectionConfig`] for a fixed `(reconnect, backend_pid, project_cwd)`
    /// using [`read_backend_process_instance`] and [`WorkspaceSamplingPolicy::default`].
    pub fn new(
        reconnect: NativeReconnectConfig,
        backend_pid: u32,
        project_cwd: impl Into<PathBuf>,
    ) -> Self {
        Self {
            reconnect_resolver: Arc::new(move || Ok((reconnect.clone(), backend_pid))),
            project_cwd: project_cwd.into(),
            process_instance_reader: Arc::new(read_backend_process_instance),
            sampling_policy: WorkspaceSamplingPolicy::default(),
        }
    }

    /// Creates a [`ManagedParentConnectionConfig`] with a dynamic reconnect resolver closure.
    pub fn with_reconnect_resolver<F>(resolver: F, project_cwd: impl Into<PathBuf>) -> Self
    where
        F: Fn() -> Result<(NativeReconnectConfig, u32), NativeError> + Send + Sync + 'static,
    {
        Self {
            reconnect_resolver: Arc::new(resolver),
            project_cwd: project_cwd.into(),
            process_instance_reader: Arc::new(read_backend_process_instance),
            sampling_policy: WorkspaceSamplingPolicy::default(),
        }
    }

    /// Overrides the process-instance reader (for example to test PID reuse deterministically).
    #[must_use]
    pub fn with_process_instance_reader<F>(mut self, reader: F) -> Self
    where
        F: Fn(u32) -> Option<ObservationProcessInstance> + Send + Sync + 'static,
    {
        self.process_instance_reader = Arc::new(reader);
        self
    }

    /// Overrides the [`WorkspaceSamplingPolicy`] (normalized to non-zero contract bounds).
    #[must_use]
    pub fn with_sampling_policy(mut self, policy: WorkspaceSamplingPolicy) -> Self {
        self.sampling_policy = policy.normalized();
        self
    }

    pub(crate) fn discover_reconnect(&self) -> Result<(NativeReconnectConfig, u32), NativeError> {
        (self.reconnect_resolver)()
    }

    pub(crate) fn project_cwd(&self) -> &Path {
        &self.project_cwd
    }

    pub(crate) fn read_process_instance(&self, pid: u32) -> Option<ObservationProcessInstance> {
        (self.process_instance_reader)(pid)
    }

    /// Returns the normalized [`WorkspaceSamplingPolicy`].
    #[must_use]
    pub fn sampling_policy(&self) -> &WorkspaceSamplingPolicy {
        &self.sampling_policy
    }
}

#[derive(Clone)]
pub(crate) struct ManagedParentEnvironment {
    custom: Option<ManagedParentConnectionConfig>,
}

impl fmt::Debug for ManagedParentEnvironment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedParentEnvironment")
            .field("custom", &self.custom.is_some())
            .finish()
    }
}

impl ManagedParentEnvironment {
    pub(crate) fn from_optional(custom: Option<ManagedParentConnectionConfig>) -> Self {
        Self { custom }
    }

    pub(crate) fn discover_reconnect(
        &self,
        missing_code: NativeErrorCode,
        missing_message: &'static str,
    ) -> Result<(NativeReconnectConfig, u32), NativeError> {
        match &self.custom {
            Some(cfg) => cfg.discover_reconnect(),
            None => NativeReconnectConfig::discover_for_mcp_parent()
                .map_err(|_| NativeError::new(missing_code, missing_message)),
        }
    }

    pub(crate) fn resolve_project_cwd(&self) -> Result<PathBuf, NativeError> {
        match &self.custom {
            Some(cfg) => Ok(cfg.project_cwd().to_path_buf()),
            None => std::env::current_dir().map_err(|_| {
                NativeError::new(
                    NativeErrorCode::InvalidConfig,
                    "Cannot determine the MCP project directory",
                )
            }),
        }
    }

    pub(crate) fn read_process_instance(&self, pid: u32) -> Option<ObservationProcessInstance> {
        match &self.custom {
            Some(cfg) => cfg.read_process_instance(pid),
            None => read_backend_process_instance(pid),
        }
    }

    pub(crate) fn sampling_policy(&self) -> WorkspaceSamplingPolicy {
        match &self.custom {
            Some(cfg) => cfg.sampling_policy().normalized(),
            None => WorkspaceSamplingPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SamplerRegistrationKey {
    connection_nonce: String,
    epoch_id: String,
    server_pid: u32,
    chat_identity: WorkspaceChatIdentity,
    expected_project_id: Option<String>,
    backend_process_instance: ObservationProcessInstance,
}

#[derive(Debug)]
struct SamplerProducerState {
    sequence: u64,
    last_observed_at_ms: Option<i64>,
    last_success_at_ms: Option<i64>,
    invalidated: bool,
}

impl SamplerProducerState {
    fn next_timestamp_ms(&mut self) -> i64 {
        let now = current_observation_time_ms().max(1);
        let monotonic = match self.last_observed_at_ms {
            Some(prev) if now <= prev => prev.saturating_add(1),
            _ => now,
        };
        self.last_observed_at_ms = Some(monotonic);
        monotonic
    }
}

struct TaskRunningGuard {
    flag: Arc<AtomicBool>,
}

impl Drop for TaskRunningGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

struct ActiveSamplerRegistration {
    key: SamplerRegistrationKey,
    producer_epoch: String,
    producer_state: Arc<AsyncMutex<SamplerProducerState>>,
    running_flag: Arc<AtomicBool>,
    shutdown_tx: watch::Sender<bool>,
    join_handle: JoinHandle<()>,
}

impl ActiveSamplerRegistration {
    fn stop(&self) {
        let _ = self.shutdown_tx.send(true);
        self.join_handle.abort();
        self.running_flag.store(false, Ordering::Release);
    }
}

/// Lifecycle owner for the single background native chat sampler tied to a [`crate::PairedMcpServer`].
///
/// Dropping the last clone of `Arc<NativeChatSamplerOwner>` stops any active background sampler
/// immediately.
pub(crate) struct NativeChatSamplerOwner {
    active: Mutex<Option<ActiveSamplerRegistration>>,
}

impl Default for NativeChatSamplerOwner {
    fn default() -> Self {
        Self {
            active: Mutex::new(None),
        }
    }
}

impl fmt::Debug for NativeChatSamplerOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let is_running = self.has_active_sampler();
        f.debug_struct("NativeChatSamplerOwner")
            .field("active", &is_running)
            .finish()
    }
}

impl Drop for NativeChatSamplerOwner {
    fn drop(&mut self) {
        self.stop_active_sampler();
    }
}

pub(crate) struct SamplerStartRequest<'a> {
    pub connection_nonce: &'a str,
    pub epoch_id: &'a str,
    pub server_pid: u32,
    pub binding_id: &'a str,
    pub thread_uuid: &'a str,
    pub verified_cwd: &'a Path,
    pub verified_project_id: Option<&'a str>,
    pub initial_metadata: &'a NativeThreadMetadata,
    pub reconnect: NativeReconnectConfig,
    pub backend_pid: u32,
    pub parent_env: ManagedParentEnvironment,
    pub pairing_store: PairingStore,
    pub automatic_pairing: AutomaticPairing,
    pub context_config: &'a OperatorContextServerConfig<'a>,
}

impl NativeChatSamplerOwner {
    pub(crate) fn has_active_sampler(&self) -> bool {
        let guard = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.as_ref().is_some_and(|reg| {
            reg.running_flag.load(Ordering::Acquire) && !reg.join_handle.is_finished()
        })
    }

    pub(crate) fn active_producer_epoch(&self) -> Option<String> {
        let guard = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard
            .as_ref()
            .filter(|reg| {
                reg.running_flag.load(Ordering::Acquire) && !reg.join_handle.is_finished()
            })
            .map(|reg| reg.producer_epoch.clone())
    }

    pub(crate) fn stop_active_sampler(&self) {
        let previous = {
            let mut guard = self
                .active
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.take()
        };
        if let Some(reg) = previous {
            reg.stop();
        }
    }

    pub(crate) async fn ensure_started_after_verified_connect(
        &self,
        req: SamplerStartRequest<'_>,
    ) -> Result<(), NativeError> {
        let (clean_state_dir, source_id, clean_project_root) =
            match resolve_native_sample_store_scope(req.context_config, req.verified_cwd) {
                Ok(scope) => scope,
                Err(err) if err.error_code() == NativeErrorCode::InvalidConfig => {
                    // Partial workflow-only store setup without host stores does not start an unscoped sampler.
                    self.stop_active_sampler();
                    return Ok(());
                }
                Err(err) => {
                    self.stop_active_sampler();
                    return Err(err);
                }
            };

        let project_scope_str = clean_project_root.to_str().ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::InvalidConfig,
                "verified project directory must be valid UTF-8 for native chat observation",
            )
        })?;
        let chat_identity = WorkspaceChatIdentity::new(
            &source_id,
            req.binding_id,
            req.thread_uuid,
            project_scope_str,
        )
        .map_err(map_contract_error)?;

        let Some(backend_instance) = req.parent_env.read_process_instance(req.backend_pid) else {
            self.stop_active_sampler();
            return Ok(());
        };
        if backend_instance.pid != req.backend_pid {
            self.stop_active_sampler();
            return Ok(());
        }

        let desired_key = SamplerRegistrationKey {
            connection_nonce: req.connection_nonce.to_string(),
            epoch_id: req.epoch_id.to_string(),
            server_pid: req.server_pid,
            chat_identity: chat_identity.clone(),
            expected_project_id: req.verified_project_id.map(str::to_string),
            backend_process_instance: backend_instance.clone(),
        };

        // Check whether an identical verified sampler is already running; if so, reuse it without
        // spawning a second background task or rotating `producer_epoch`.
        let existing_handle = {
            let mut guard = self
                .active
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(existing) = guard.as_ref() {
                let still_running = existing.running_flag.load(Ordering::Acquire)
                    && !existing.join_handle.is_finished();
                if still_running && existing.key == desired_key {
                    Some((
                        existing.producer_epoch.clone(),
                        Arc::clone(&existing.producer_state),
                    ))
                } else {
                    if let Some(old) = guard.take() {
                        old.stop();
                    }
                    None
                }
            } else {
                None
            }
        };

        let policy = req.parent_env.sampling_policy();
        if let Some((existing_epoch, existing_state)) = existing_handle {
            let mut state_guard = existing_state.lock().await;
            if !state_guard.invalidated {
                let sample = build_sample_from_verified_metadata(
                    &mut state_guard,
                    &chat_identity,
                    req.verified_project_id,
                    &existing_epoch,
                    &backend_instance,
                    &policy,
                    req.initial_metadata,
                );
                let state_dir_for_write = clean_state_dir.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    write_workspace_native_sample_sidecar(&state_dir_for_write, &sample)
                })
                .await;
                return Ok(());
            }
            // Existing state was invalidated concurrently; clear and start a fresh producer epoch.
            self.stop_active_sampler();
        }

        let producer_epoch = generate_sample_producer_epoch();
        let mut initial_state = SamplerProducerState {
            sequence: 0,
            last_observed_at_ms: None,
            last_success_at_ms: None,
            invalidated: false,
        };
        let initial_sample = build_sample_from_verified_metadata(
            &mut initial_state,
            &chat_identity,
            req.verified_project_id,
            &producer_epoch,
            &backend_instance,
            &policy,
            req.initial_metadata,
        );
        let state_dir_initial = clean_state_dir.clone();
        tokio::task::spawn_blocking(move || {
            write_workspace_native_sample_sidecar(&state_dir_initial, &initial_sample)
        })
        .await
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::IoError,
                "native chat initial sample persistence task failed",
            )
        })??;

        let producer_state = Arc::new(AsyncMutex::new(initial_state));
        let running_flag = Arc::new(AtomicBool::new(true));
        let running_guard = TaskRunningGuard {
            flag: Arc::clone(&running_flag),
        };
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let loop_ctx = BackgroundSamplerContext {
            key: desired_key.clone(),
            producer_epoch: producer_epoch.clone(),
            producer_state: Arc::clone(&producer_state),
            state_dir: clean_state_dir,
            reconnect: req.reconnect,
            backend_pid: req.backend_pid,
            parent_env: req.parent_env,
            policy,
            pairing_store: req.pairing_store,
            automatic_pairing: req.automatic_pairing,
        };

        let join_handle = tokio::spawn(async move {
            run_background_sampler_loop(loop_ctx, shutdown_rx, running_guard).await;
        });

        let mut guard = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(replaced) = guard.replace(ActiveSamplerRegistration {
            key: desired_key,
            producer_epoch,
            producer_state,
            running_flag,
            shutdown_tx,
            join_handle,
        }) {
            replaced.stop();
        }

        Ok(())
    }
}

struct BackgroundSamplerContext {
    key: SamplerRegistrationKey,
    producer_epoch: String,
    producer_state: Arc<AsyncMutex<SamplerProducerState>>,
    state_dir: PathBuf,
    reconnect: NativeReconnectConfig,
    backend_pid: u32,
    parent_env: ManagedParentEnvironment,
    policy: WorkspaceSamplingPolicy,
    pairing_store: PairingStore,
    automatic_pairing: AutomaticPairing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TickDisposition {
    Continue,
    TerminateProducer,
}

async fn run_background_sampler_loop(
    ctx: BackgroundSamplerContext,
    mut shutdown_rx: watch::Receiver<bool>,
    _running_guard: TaskRunningGuard,
) {
    let interval_duration = Duration::from_millis(ctx.policy.interval_ms.max(1));
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + interval_duration,
        interval_duration,
    );
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            _ = ticker.tick() => {
                if *shutdown_rx.borrow() {
                    break;
                }
                if execute_single_sampler_tick(&ctx).await == TickDisposition::TerminateProducer {
                    break;
                }
            }
        }
    }
}

async fn execute_single_sampler_tick(ctx: &BackgroundSamplerContext) -> TickDisposition {
    let mut state_guard = ctx.producer_state.lock().await;
    if state_guard.invalidated {
        return TickDisposition::TerminateProducer;
    }

    if let Err(err) = verify_sampler_enrollment(ctx) {
        state_guard.invalidated = true;
        ctx.automatic_pairing
            .invalidate_sampler_observation_context(
                &ctx.key.connection_nonce,
                &ctx.key.epoch_id,
                ctx.key.server_pid,
            );
        let sample = build_terminal_invalidation_sample(
            &mut state_guard,
            ctx,
            WorkspaceAvailabilityState::Unavailable,
            WorkspaceConnectionState::Unavailable,
            sanitize_diagnostic_text(err.message()),
        );
        persist_sample_best_effort(&ctx.state_dir, sample).await;
        return TickDisposition::TerminateProducer;
    }

    if let Some(disposition) = check_parent_process_continuity(&mut state_guard, ctx).await {
        return disposition;
    }

    let timeout_duration = Duration::from_millis(ctx.policy.metadata_timeout_ms.max(1));
    let rpc_outcome = tokio::time::timeout(timeout_duration, async {
        let client = NativeClient::new_managed_websocket(ctx.reconnect.clone())?
            .with_expected_backend_pid(ctx.backend_pid)
            .with_timeout(timeout_duration)?;
        client
            .thread_metadata(&ctx.key.chat_identity.thread_uuid)
            .await
    })
    .await;

    // Re-check enrollment and backend process continuity immediately after the RPC returns so
    // quarantine, epoch restart, or PID reuse during an in-flight read invalidates immediately.
    if let Err(err) = verify_sampler_enrollment(ctx) {
        state_guard.invalidated = true;
        ctx.automatic_pairing
            .invalidate_sampler_observation_context(
                &ctx.key.connection_nonce,
                &ctx.key.epoch_id,
                ctx.key.server_pid,
            );
        let sample = build_terminal_invalidation_sample(
            &mut state_guard,
            ctx,
            WorkspaceAvailabilityState::Unavailable,
            WorkspaceConnectionState::Unavailable,
            sanitize_diagnostic_text(err.message()),
        );
        persist_sample_best_effort(&ctx.state_dir, sample).await;
        return TickDisposition::TerminateProducer;
    }

    if let Some(disposition) = check_parent_process_continuity(&mut state_guard, ctx).await {
        return disposition;
    }

    match rpc_outcome {
        Ok(Ok(metadata)) => {
            let sample = build_sample_from_verified_metadata(
                &mut state_guard,
                &ctx.key.chat_identity,
                ctx.key.expected_project_id.as_deref(),
                &ctx.producer_epoch,
                &ctx.key.backend_process_instance,
                &ctx.policy,
                &metadata,
            );
            if sample.availability == WorkspaceAvailabilityState::Available {
                ctx.automatic_pairing.apply_sampler_thread_metadata(
                    &ctx.key.connection_nonce,
                    &ctx.key.epoch_id,
                    ctx.key.server_pid,
                    &ctx.key.chat_identity.binding_id,
                    &ctx.key.chat_identity.thread_uuid,
                    metadata.thread.title.as_deref(),
                    metadata.thread.model.as_deref(),
                    metadata.thread.effort.as_deref(),
                );
            } else {
                ctx.automatic_pairing.clear_sampler_selected_model(
                    &ctx.key.connection_nonce,
                    &ctx.key.epoch_id,
                    ctx.key.server_pid,
                    &ctx.key.chat_identity.binding_id,
                    &ctx.key.chat_identity.thread_uuid,
                );
            }
            persist_sample_best_effort(&ctx.state_dir, sample).await;
            TickDisposition::Continue
        }
        Err(_elapsed) => {
            ctx.automatic_pairing.clear_sampler_selected_model(
                &ctx.key.connection_nonce,
                &ctx.key.epoch_id,
                ctx.key.server_pid,
                &ctx.key.chat_identity.binding_id,
                &ctx.key.chat_identity.thread_uuid,
            );
            let sample = build_timeout_sample(&mut state_guard, ctx);
            persist_sample_best_effort(&ctx.state_dir, sample).await;
            TickDisposition::Continue
        }
        Ok(Err(err)) if err.error_code() == NativeErrorCode::TimedOut => {
            ctx.automatic_pairing.clear_sampler_selected_model(
                &ctx.key.connection_nonce,
                &ctx.key.epoch_id,
                ctx.key.server_pid,
                &ctx.key.chat_identity.binding_id,
                &ctx.key.chat_identity.thread_uuid,
            );
            let sample = build_timeout_sample(&mut state_guard, ctx);
            persist_sample_best_effort(&ctx.state_dir, sample).await;
            TickDisposition::Continue
        }
        Ok(Err(err)) if err.error_code() == NativeErrorCode::TransportUnavailable => {
            ctx.automatic_pairing.clear_sampler_selected_model(
                &ctx.key.connection_nonce,
                &ctx.key.epoch_id,
                ctx.key.server_pid,
                &ctx.key.chat_identity.binding_id,
                &ctx.key.chat_identity.thread_uuid,
            );
            let now_ms = state_guard.next_timestamp_ms();
            let freshness = compute_failure_freshness(
                now_ms,
                state_guard.last_success_at_ms,
                ctx.policy.stale_threshold_ms,
            );
            state_guard.sequence = state_guard.sequence.saturating_add(1);
            let sample = WorkspaceNativeSample {
                chat_identity: ctx.key.chat_identity.clone(),
                producer_epoch: ctx.producer_epoch.clone(),
                backend_process_instance: Some(ctx.key.backend_process_instance.clone()),
                sequence: state_guard.sequence,
                observed_at_ms: Some(now_ms),
                last_success_at_ms: state_guard.last_success_at_ms,
                thread_title: None,
                availability: WorkspaceAvailabilityState::Disconnected,
                activity: WorkspaceActivityState::Unknown,
                connection: WorkspaceSampleConnection {
                    state: WorkspaceConnectionState::Disconnected,
                    freshness,
                    route: Some(NATIVE_CHAT_SAMPLE_ROUTE.to_string()),
                    diagnostic: Some("managed Codex parent connection unavailable".to_string()),
                },
                capabilities: unknown_capabilities(),
                configured_selection: WorkspaceConfiguredSelection::unavailable(),
            };
            persist_sample_best_effort(&ctx.state_dir, sample).await;
            TickDisposition::Continue
        }
        Ok(Err(err)) => {
            let fatal_security = err.error_code() == NativeErrorCode::SecurityViolation;
            if fatal_security {
                state_guard.invalidated = true;
                ctx.automatic_pairing
                    .invalidate_sampler_observation_context(
                        &ctx.key.connection_nonce,
                        &ctx.key.epoch_id,
                        ctx.key.server_pid,
                    );
            } else {
                ctx.automatic_pairing.clear_sampler_selected_model(
                    &ctx.key.connection_nonce,
                    &ctx.key.epoch_id,
                    ctx.key.server_pid,
                    &ctx.key.chat_identity.binding_id,
                    &ctx.key.chat_identity.thread_uuid,
                );
            }
            let now_ms = state_guard.next_timestamp_ms();
            let freshness = compute_failure_freshness(
                now_ms,
                state_guard.last_success_at_ms,
                ctx.policy.stale_threshold_ms,
            );
            let availability = if freshness == WorkspaceFreshnessState::Stale && !fatal_security {
                WorkspaceAvailabilityState::Stale
            } else {
                WorkspaceAvailabilityState::Unavailable
            };
            state_guard.sequence = state_guard.sequence.saturating_add(1);
            let sample = WorkspaceNativeSample {
                chat_identity: ctx.key.chat_identity.clone(),
                producer_epoch: ctx.producer_epoch.clone(),
                backend_process_instance: Some(ctx.key.backend_process_instance.clone()),
                sequence: state_guard.sequence,
                observed_at_ms: Some(now_ms),
                last_success_at_ms: state_guard.last_success_at_ms,
                thread_title: None,
                availability,
                activity: WorkspaceActivityState::Unknown,
                connection: WorkspaceSampleConnection {
                    state: WorkspaceConnectionState::Unavailable,
                    freshness,
                    route: Some(NATIVE_CHAT_SAMPLE_ROUTE.to_string()),
                    diagnostic: Some(sanitize_diagnostic_text(err.message())),
                },
                capabilities: unknown_capabilities(),
                configured_selection: WorkspaceConfiguredSelection::unavailable(),
            };
            persist_sample_best_effort(&ctx.state_dir, sample).await;
            if fatal_security {
                TickDisposition::TerminateProducer
            } else {
                TickDisposition::Continue
            }
        }
    }
}

fn verify_sampler_enrollment(ctx: &BackgroundSamplerContext) -> Result<(), NativeError> {
    let epoch = ctx
        .pairing_store
        .inspect_epoch(&ctx.key.connection_nonce, &ctx.key.epoch_id)?;
    if epoch.server_pid != ctx.key.server_pid || epoch.server_pid != std::process::id() {
        return Err(NativeError::new(
            NativeErrorCode::ExpiredEpoch,
            "paired MCP server process changed",
        ));
    }
    let enrolled = ctx
        .pairing_store
        .verify_enrolled_binding(&ctx.key.connection_nonce, &ctx.key.epoch_id)?;
    if enrolled.binding_id != ctx.key.chat_identity.binding_id
        || enrolled.thread_id != ctx.key.chat_identity.thread_uuid
        || enrolled.project_id != ctx.key.expected_project_id
    {
        return Err(NativeError::new(
            NativeErrorCode::RebindRejected,
            "enrolled binding no longer matches verified chat identity",
        ));
    }
    Ok(())
}

async fn check_parent_process_continuity(
    state_guard: &mut SamplerProducerState,
    ctx: &BackgroundSamplerContext,
) -> Option<TickDisposition> {
    let discovered = ctx.parent_env.discover_reconnect(
        NativeErrorCode::TransportUnavailable,
        "managed Codex parent endpoint disconnected",
    );
    let Ok((current_reconnect, current_pid)) = discovered else {
        state_guard.invalidated = true;
        ctx.automatic_pairing
            .invalidate_sampler_observation_context(
                &ctx.key.connection_nonce,
                &ctx.key.epoch_id,
                ctx.key.server_pid,
            );
        let sample = build_terminal_invalidation_sample(
            state_guard,
            ctx,
            WorkspaceAvailabilityState::Disconnected,
            WorkspaceConnectionState::Disconnected,
            "managed Codex parent endpoint disconnected".to_string(),
        );
        persist_sample_best_effort(&ctx.state_dir, sample).await;
        return Some(TickDisposition::TerminateProducer);
    };

    if current_reconnect != ctx.reconnect || current_pid != ctx.backend_pid {
        state_guard.invalidated = true;
        ctx.automatic_pairing
            .invalidate_sampler_observation_context(
                &ctx.key.connection_nonce,
                &ctx.key.epoch_id,
                ctx.key.server_pid,
            );
        let sample = build_terminal_invalidation_sample(
            state_guard,
            ctx,
            WorkspaceAvailabilityState::Unavailable,
            WorkspaceConnectionState::Unavailable,
            "managed Codex parent endpoint changed".to_string(),
        );
        persist_sample_best_effort(&ctx.state_dir, sample).await;
        return Some(TickDisposition::TerminateProducer);
    }

    match ctx.parent_env.read_process_instance(ctx.backend_pid) {
        None => {
            state_guard.invalidated = true;
            ctx.automatic_pairing
                .invalidate_sampler_observation_context(
                    &ctx.key.connection_nonce,
                    &ctx.key.epoch_id,
                    ctx.key.server_pid,
                );
            let sample = build_terminal_invalidation_sample(
                state_guard,
                ctx,
                WorkspaceAvailabilityState::Disconnected,
                WorkspaceConnectionState::Disconnected,
                "managed Codex parent process exited".to_string(),
            );
            persist_sample_best_effort(&ctx.state_dir, sample).await;
            Some(TickDisposition::TerminateProducer)
        }
        Some(actual_instance) if actual_instance != ctx.key.backend_process_instance => {
            state_guard.invalidated = true;
            ctx.automatic_pairing
                .invalidate_sampler_observation_context(
                    &ctx.key.connection_nonce,
                    &ctx.key.epoch_id,
                    ctx.key.server_pid,
                );
            let sample = build_terminal_invalidation_sample(
                state_guard,
                ctx,
                WorkspaceAvailabilityState::Unavailable,
                WorkspaceConnectionState::Unavailable,
                "managed Codex parent PID was reused by a different process instance".to_string(),
            );
            persist_sample_best_effort(&ctx.state_dir, sample).await;
            Some(TickDisposition::TerminateProducer)
        }
        Some(_) => None,
    }
}

async fn persist_sample_best_effort(state_dir: &Path, sample: WorkspaceNativeSample) {
    let dir = state_dir.to_path_buf();
    let _ =
        tokio::task::spawn_blocking(move || write_workspace_native_sample_sidecar(&dir, &sample))
            .await;
}

fn build_sample_from_verified_metadata(
    state: &mut SamplerProducerState,
    chat_identity: &WorkspaceChatIdentity,
    expected_project_id: Option<&str>,
    producer_epoch: &str,
    backend_process_instance: &ObservationProcessInstance,
    policy: &WorkspaceSamplingPolicy,
    metadata: &NativeThreadMetadata,
) -> WorkspaceNativeSample {
    let now_ms = state.next_timestamp_ms();
    state.sequence = state.sequence.saturating_add(1);

    let scope_matches = validate_refreshed_thread_metadata(
        &metadata.thread,
        &chat_identity.thread_uuid,
        Path::new(&chat_identity.canonical_project_scope),
        expected_project_id,
    );

    if !scope_matches {
        let freshness =
            compute_failure_freshness(now_ms, state.last_success_at_ms, policy.stale_threshold_ms);
        return WorkspaceNativeSample {
            chat_identity: chat_identity.clone(),
            producer_epoch: producer_epoch.to_string(),
            backend_process_instance: Some(backend_process_instance.clone()),
            sequence: state.sequence,
            observed_at_ms: Some(now_ms),
            last_success_at_ms: state.last_success_at_ms,
            thread_title: None,
            availability: WorkspaceAvailabilityState::Unavailable,
            activity: WorkspaceActivityState::Unknown,
            connection: WorkspaceSampleConnection {
                state: WorkspaceConnectionState::Unavailable,
                freshness,
                route: Some(NATIVE_CHAT_SAMPLE_ROUTE.to_string()),
                diagnostic: Some(
                    "native thread metadata did not match verified chat scope".to_string(),
                ),
            },
            capabilities: unknown_capabilities(),
            configured_selection: WorkspaceConfiguredSelection::unavailable(),
        };
    }

    match metadata.status_detail.status.as_str() {
        "idle" | "active" => {
            state.last_success_at_ms = Some(now_ms);
            let (activity, activity_detail_cap) =
                prove_native_thread_activity(&metadata.status_detail);
            let configured_selection = WorkspaceConfiguredSelection::verified(
                metadata.thread.model.as_deref(),
                metadata.thread.effort.as_deref(),
            );
            let selection_cap = if configured_selection.selected_model.is_some()
                || configured_selection.selected_effort.is_some()
            {
                WorkspaceCapabilityState::Supported
            } else {
                WorkspaceCapabilityState::Unknown
            };
            // Populated only from this independently verified exact-thread read. Failed,
            // disconnected, timeout, and scope-mismatch samples above never carry a title.
            let thread_title = metadata
                .thread
                .title
                .as_deref()
                .map(|raw| sanitize_observation_label(raw, ""))
                .filter(|clean| !clean.is_empty());
            WorkspaceNativeSample {
                chat_identity: chat_identity.clone(),
                producer_epoch: producer_epoch.to_string(),
                backend_process_instance: Some(backend_process_instance.clone()),
                sequence: state.sequence,
                observed_at_ms: Some(now_ms),
                last_success_at_ms: Some(now_ms),
                thread_title,
                availability: WorkspaceAvailabilityState::Available,
                activity,
                connection: WorkspaceSampleConnection {
                    state: WorkspaceConnectionState::Connected,
                    freshness: WorkspaceFreshnessState::Fresh,
                    route: Some(NATIVE_CHAT_SAMPLE_ROUTE.to_string()),
                    diagnostic: None,
                },
                capabilities: WorkspaceNativeCapabilities {
                    exact_thread_read: WorkspaceCapabilityState::Supported,
                    activity_detail: activity_detail_cap,
                    configured_selection: selection_cap,
                    wakeup_delivery: WorkspaceCapabilityState::Unknown,
                    per_call_identity: WorkspaceCapabilityState::Unsupported,
                },
                configured_selection,
            }
        }
        other => {
            let freshness = compute_failure_freshness(
                now_ms,
                state.last_success_at_ms,
                policy.stale_threshold_ms,
            );
            WorkspaceNativeSample {
                chat_identity: chat_identity.clone(),
                producer_epoch: producer_epoch.to_string(),
                backend_process_instance: Some(backend_process_instance.clone()),
                sequence: state.sequence,
                observed_at_ms: Some(now_ms),
                last_success_at_ms: state.last_success_at_ms,
                thread_title: None,
                availability: WorkspaceAvailabilityState::Unavailable,
                activity: WorkspaceActivityState::Unknown,
                connection: WorkspaceSampleConnection {
                    state: WorkspaceConnectionState::Unavailable,
                    freshness,
                    route: Some(NATIVE_CHAT_SAMPLE_ROUTE.to_string()),
                    diagnostic: Some(format!("native thread status is {other}")),
                },
                capabilities: WorkspaceNativeCapabilities {
                    exact_thread_read: WorkspaceCapabilityState::Supported,
                    activity_detail: WorkspaceCapabilityState::Unknown,
                    configured_selection: WorkspaceCapabilityState::Unknown,
                    wakeup_delivery: WorkspaceCapabilityState::Unknown,
                    per_call_identity: WorkspaceCapabilityState::Unsupported,
                },
                configured_selection: WorkspaceConfiguredSelection::unavailable(),
            }
        }
    }
}

fn build_timeout_sample(
    state: &mut SamplerProducerState,
    ctx: &BackgroundSamplerContext,
) -> WorkspaceNativeSample {
    let now_ms = state.next_timestamp_ms();
    let freshness = compute_failure_freshness(
        now_ms,
        state.last_success_at_ms,
        ctx.policy.stale_threshold_ms,
    );
    let availability = if freshness == WorkspaceFreshnessState::Stale {
        WorkspaceAvailabilityState::Stale
    } else {
        WorkspaceAvailabilityState::Unavailable
    };
    state.sequence = state.sequence.saturating_add(1);
    WorkspaceNativeSample {
        chat_identity: ctx.key.chat_identity.clone(),
        producer_epoch: ctx.producer_epoch.clone(),
        backend_process_instance: Some(ctx.key.backend_process_instance.clone()),
        sequence: state.sequence,
        observed_at_ms: Some(now_ms),
        last_success_at_ms: state.last_success_at_ms,
        thread_title: None,
        availability,
        activity: WorkspaceActivityState::Unknown,
        connection: WorkspaceSampleConnection {
            state: WorkspaceConnectionState::Degraded,
            freshness,
            route: Some(NATIVE_CHAT_SAMPLE_ROUTE.to_string()),
            diagnostic: Some("native thread metadata read timed out".to_string()),
        },
        capabilities: unknown_capabilities(),
        configured_selection: WorkspaceConfiguredSelection::unavailable(),
    }
}

fn build_terminal_invalidation_sample(
    state: &mut SamplerProducerState,
    ctx: &BackgroundSamplerContext,
    availability: WorkspaceAvailabilityState,
    connection_state: WorkspaceConnectionState,
    diagnostic: String,
) -> WorkspaceNativeSample {
    let now_ms = state.next_timestamp_ms();
    let freshness = compute_failure_freshness(
        now_ms,
        state.last_success_at_ms,
        ctx.policy.stale_threshold_ms,
    );
    state.sequence = state.sequence.saturating_add(1);
    WorkspaceNativeSample {
        chat_identity: ctx.key.chat_identity.clone(),
        producer_epoch: ctx.producer_epoch.clone(),
        backend_process_instance: Some(ctx.key.backend_process_instance.clone()),
        sequence: state.sequence,
        observed_at_ms: Some(now_ms),
        last_success_at_ms: state.last_success_at_ms,
        thread_title: None,
        availability,
        activity: WorkspaceActivityState::Unknown,
        connection: WorkspaceSampleConnection {
            state: connection_state,
            freshness,
            route: Some(NATIVE_CHAT_SAMPLE_ROUTE.to_string()),
            diagnostic: Some(diagnostic),
        },
        capabilities: unknown_capabilities(),
        configured_selection: WorkspaceConfiguredSelection::unavailable(),
    }
}

fn compute_failure_freshness(
    observed_at_ms: i64,
    last_success_at_ms: Option<i64>,
    stale_threshold_ms: u64,
) -> WorkspaceFreshnessState {
    let threshold = if stale_threshold_ms == 0 {
        WORKSPACE_SAMPLE_STALE_THRESHOLD_MS
    } else {
        stale_threshold_ms
    };
    let Some(anchor_ms) = last_success_at_ms else {
        return WorkspaceFreshnessState::NeverSucceeded;
    };
    if observed_at_ms < anchor_ms {
        return WorkspaceFreshnessState::Unknown;
    }
    let age_ms = u64::try_from(observed_at_ms.saturating_sub(anchor_ms)).unwrap_or(u64::MAX);
    if age_ms > threshold {
        WorkspaceFreshnessState::Stale
    } else {
        WorkspaceFreshnessState::Fresh
    }
}

const fn unknown_capabilities() -> WorkspaceNativeCapabilities {
    WorkspaceNativeCapabilities {
        exact_thread_read: WorkspaceCapabilityState::Unknown,
        activity_detail: WorkspaceCapabilityState::Unknown,
        configured_selection: WorkspaceCapabilityState::Unknown,
        wakeup_delivery: WorkspaceCapabilityState::Unknown,
        per_call_identity: WorkspaceCapabilityState::Unsupported,
    }
}

pub(crate) fn resolve_native_sample_store_scope(
    context_config: &OperatorContextServerConfig<'_>,
    verified_project_root: &Path,
) -> Result<(PathBuf, String, PathBuf), NativeError> {
    ensure_platform_supported()?;
    let validated_pairing = validate_dirs(&[("pairing_dir", context_config.pairing_dir)])
        .map_err(|err| map_dir_error("pairing_dir", err))?;
    let clean_pairing_dir = validated_pairing[0].clone();

    let clean_project_root =
        validate_existing_canonical_non_symlink_dir(verified_project_root, "project_root")?;
    if paths_overlap(&clean_project_root, &clean_pairing_dir) {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "verified project_root must not equal or overlap pairing_dir",
        ));
    }

    match (context_config.workflow_stores, context_config.host_stores) {
        (Some((registry_dir, plans_dir)), Some((authority_dir, sessions_dir))) => {
            let validated = validate_dirs(&[
                ("state_dir", context_config.state_dir),
                ("registry_dir", registry_dir),
                ("plans_dir", plans_dir),
                ("authority_dir", authority_dir),
                ("sessions_dir", sessions_dir),
            ])
            .map_err(|err| map_dir_error("stores", err))?;

            for store_dir in &validated {
                if paths_overlap(&clean_pairing_dir, store_dir) {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "pairing_dir must not equal or overlap private operator stores",
                    ));
                }
                if paths_overlap(&clean_project_root, store_dir) {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "verified project_root must not equal or overlap private operator stores",
                    ));
                }
            }

            let source_id = compute_stable_source_id_from_canonical(
                &validated[2],
                &validated[3],
                &validated[4],
            );
            Ok((validated[0].clone(), source_id, clean_project_root))
        }
        (None, None) => {
            let validated = validate_dirs(&[("state_dir", context_config.state_dir)])
                .map_err(|err| map_dir_error("state_dir", err))?;
            let clean_state_dir = validated[0].clone();
            if paths_overlap(&clean_pairing_dir, &clean_state_dir) {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "pairing_dir must not equal or overlap state_dir",
                ));
            }
            if paths_overlap(&clean_project_root, &clean_state_dir) {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "verified project_root must not equal or overlap state_dir",
                ));
            }
            Ok((clean_state_dir, "default".to_string(), clean_project_root))
        }
        _ => Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "native chat observation requires either complete workflow+host store configuration or default state_dir scope",
        )),
    }
}

fn ensure_owned_samples_dir(clean_state_dir: &Path) -> Result<PathBuf, NativeError> {
    let dir = native_chat_samples_dir(clean_state_dir);
    let prepared = prepare_private_dir(&dir, "native_chat_samples_dir")?;
    let validated = validate_dirs(&[("native_chat_samples_dir", &prepared)])
        .map_err(|err| map_dir_error("native_chat_samples_dir", err))?;
    Ok(validated[0].clone())
}

fn validate_optional_existing_private_dir(dir: &Path) -> Result<Option<PathBuf>, NativeError> {
    match fs::symlink_metadata(dir) {
        Ok(meta) => {
            if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "native chat samples path must be a non-symlink private directory",
                ));
            }
            let validated = validate_dirs(&[("native_chat_samples_dir", dir)])
                .map_err(|err| map_dir_error("native_chat_samples_dir", err))?;
            Ok(Some(validated[0].clone()))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to stat native chat samples directory: {}",
                sanitize_diagnostic_text(&err.kind().to_string())
            ),
        )),
    }
}

#[cfg(unix)]
fn read_existing_private_sample_readonly(path: &Path) -> Result<Option<Vec<u8>>, NativeError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

    let _ = validate_absolute_no_symlink_components(path, "native_chat_sample")?;
    let pre_meta = match fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to stat native chat sample: {}",
                    sanitize_diagnostic_text(&err.kind().to_string())
                ),
            ));
        }
    };
    if pre_meta.file_type().is_symlink() || !pre_meta.file_type().is_file() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "native chat sample sidecar must be a non-symlink regular file",
        ));
    }
    let expected_uid = rustix::process::getuid().as_raw();
    if pre_meta.uid() != expected_uid {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "native chat sample sidecar owner does not match current user",
        ));
    }
    if (pre_meta.permissions().mode() & 0o177) != 0 {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "native chat sample sidecar must have private 0600 permissions",
        ));
    }
    if pre_meta.len() > MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES as u64 {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            "native chat sample sidecar exceeds 64 KiB limit",
        ));
    }

    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
    {
        Ok(f) => f,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!(
                    "failed to open native chat sample sidecar safely: {}",
                    sanitize_diagnostic_text(&err.kind().to_string())
                ),
            ));
        }
    };

    let post_meta = file.metadata().map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to stat opened native chat sample sidecar: {}",
                sanitize_diagnostic_text(&err.kind().to_string())
            ),
        )
    })?;
    if !post_meta.file_type().is_file()
        || post_meta.dev() != pre_meta.dev()
        || post_meta.ino() != pre_meta.ino()
        || post_meta.uid() != expected_uid
        || (post_meta.permissions().mode() & 0o177) != 0
    {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "native chat sample sidecar changed or violated 0600 permissions during open",
        ));
    }
    if post_meta.len() > MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES as u64 {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            "native chat sample sidecar exceeds 64 KiB limit",
        ));
    }

    let cap = MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES;
    let mut buf = Vec::with_capacity((post_meta.len() as usize).min(cap));
    file.take((cap as u64).saturating_add(1))
        .read_to_end(&mut buf)
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to read native chat sample sidecar: {}",
                    sanitize_diagnostic_text(&err.kind().to_string())
                ),
            )
        })?;
    if buf.len() > cap {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            "native chat sample sidecar exceeds 64 KiB limit",
        ));
    }
    Ok(Some(buf))
}

#[cfg(not(unix))]
fn read_existing_private_sample_readonly(_path: &Path) -> Result<Option<Vec<u8>>, NativeError> {
    ensure_platform_supported()?;
    unreachable!()
}

fn plan_per_source_retention(
    samples_dir: &Path,
    clean_state_dir: &Path,
    incoming_filename: &str,
    incoming: &WorkspaceNativeSample,
) -> Result<(bool, Vec<String>), NativeError> {
    let Some(incoming_key) = incoming.chat_identity.canonical_key() else {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "incoming native chat sample identity failed canonical key construction",
        ));
    };

    let read_dir = fs::read_dir(samples_dir).map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to enumerate native chat samples for retention: {}",
                sanitize_diagnostic_text(&err.kind().to_string())
            ),
        )
    })?;

    let mut candidates: Vec<(String, String, WorkspaceNativeSample)> = Vec::new();
    candidates.push((
        incoming_filename.to_string(),
        incoming_key,
        incoming.clone(),
    ));

    for (scanned, entry_res) in read_dir.enumerate() {
        if scanned >= MAX_SAMPLE_DIR_SCAN_ENTRIES {
            return Err(NativeError::new(
                NativeErrorCode::OutputOverflow,
                format!(
                    "native chat samples directory exceeded maximum scan bound of {MAX_SAMPLE_DIR_SCAN_ENTRIES} entries during retention check"
                ),
            ));
        }

        let entry = entry_res.map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to read native chat samples directory entry for retention: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        let fname_os = entry.file_name();
        let Some(fname) = fname_os.to_str() else {
            continue;
        };
        if fname == incoming_filename
            || fname == NATIVE_CHAT_SAMPLES_LOCK_FILENAME
            || fname.starts_with('.')
            || !is_expected_sample_filename_shape(fname)
        {
            continue;
        }
        let path = samples_dir.join(fname);
        let Ok(Some(bytes)) = read_existing_private_sample_readonly(&path) else {
            continue;
        };
        let Ok(parsed) = parse_workspace_native_sample_sidecar(&bytes) else {
            continue;
        };
        if parsed.validate().is_err() {
            continue;
        }
        if parsed.chat_identity.source_id != incoming.chat_identity.source_id {
            continue;
        }
        if native_chat_sample_filename(&parsed.chat_identity)
            .ok()
            .as_deref()
            != Some(fname)
        {
            continue;
        }
        if paths_overlap(
            clean_state_dir,
            Path::new(&parsed.chat_identity.canonical_project_scope),
        ) {
            continue;
        }
        if let Some(key) = parsed.chat_identity.canonical_key() {
            candidates.push((fname.to_string(), key, parsed));
        }
    }

    if candidates.len() <= MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE {
        return Ok((true, Vec::new()));
    }

    candidates.sort_by(|(_, left_key, left), (_, right_key, right)| {
        right
            .observed_at_ms
            .unwrap_or(i64::MIN)
            .cmp(&left.observed_at_ms.unwrap_or(i64::MIN))
            .then_with(|| right.sequence.cmp(&left.sequence))
            .then_with(|| left_key.cmp(right_key))
    });

    let mut incoming_retained = false;
    for (fname, _, _) in candidates
        .iter()
        .take(MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE)
    {
        if fname == incoming_filename {
            incoming_retained = true;
            break;
        }
    }

    let to_prune = candidates
        .into_iter()
        .skip(MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE)
        .map(|(fname, _, _)| fname)
        .collect();

    Ok((incoming_retained, to_prune))
}

fn is_expected_sample_filename_shape(file_name: &str) -> bool {
    let Some(rest) = file_name.strip_prefix(NATIVE_CHAT_SAMPLE_FILE_PREFIX) else {
        return false;
    };
    let Some(hex) = rest.strip_suffix(".json") else {
        return false;
    };
    hex.len() == 64
        && hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn validate_existing_canonical_non_symlink_dir(
    path: &Path,
    label: &str,
) -> Result<PathBuf, NativeError> {
    let clean = validate_absolute_no_symlink_components(path, label)?;
    for comp in clean.components() {
        if matches!(comp, Component::CurDir | Component::ParentDir) {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!("{label} must not contain relative path traversal components"),
            ));
        }
    }
    let meta = fs::symlink_metadata(&clean).map_err(|err| {
        let code = if err.kind() == std::io::ErrorKind::NotFound {
            NativeErrorCode::InvalidConfig
        } else {
            NativeErrorCode::IoError
        };
        NativeError::new(
            code,
            format!(
                "failed to stat {label}: {}",
                sanitize_diagnostic_text(&err.kind().to_string())
            ),
        )
    })?;
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!("{label} must be an existing non-symlink directory"),
        ));
    }
    let canonical = fs::canonicalize(&clean).map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to canonicalize {label}: {}",
                sanitize_diagnostic_text(&err.kind().to_string())
            ),
        )
    })?;
    if canonical != clean {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!("{label} must already be canonical without symlink indirection"),
        ));
    }
    Ok(canonical)
}

fn paths_overlap(lhs: &Path, rhs: &Path) -> bool {
    lhs == rhs || lhs.starts_with(rhs) || rhs.starts_with(lhs)
}

fn map_dir_error(label: &str, err: OperatorCliError) -> NativeError {
    let code = match err.code {
        "security_violation" => NativeErrorCode::SecurityViolation,
        "unsupported_platform" => NativeErrorCode::UnsupportedPlatform,
        _ => NativeErrorCode::InvalidConfig,
    };
    NativeError::new(code, format!("{label} validation failed: {}", err.message))
}

fn map_contract_error(err: WorkspaceContractError) -> NativeError {
    match err {
        WorkspaceContractError::SidecarTooLarge { .. } => NativeError::new(
            NativeErrorCode::OutputOverflow,
            sanitize_diagnostic_text(&err.to_string()),
        ),
        WorkspaceContractError::InvalidJson(_) => NativeError::new(
            NativeErrorCode::ProtocolError,
            sanitize_diagnostic_text(&err.to_string()),
        ),
        WorkspaceContractError::InvalidChatIdentity { .. }
        | WorkspaceContractError::InvalidNativeSample(_)
        | WorkspaceContractError::ForbiddenMessageSource(_) => NativeError::new(
            NativeErrorCode::ValidationFailed,
            sanitize_diagnostic_text(&err.to_string()),
        ),
    }
}

fn encode_hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::fs::DirBuilder;
    use std::io::{self, Write};
    use std::os::unix::fs::{symlink, DirBuilderExt, OpenOptionsExt};

    const UNIT_SOURCE_ID: &str =
        "src_2222222222222222222222222222222222222222222222222222222222222222";
    const OTHER_SOURCE_ID: &str =
        "src_3333333333333333333333333333333333333333333333333333333333333333";

    fn build_source_sample(
        source_id: &str,
        repo_str: &str,
        idx: u32,
        epoch: &str,
        sequence: u64,
        observed_at_ms: i64,
    ) -> WorkspaceNativeSample {
        let thread_uuid = format!("0196ed00-0000-7000-8000-{:012x}", idx + 1);
        let binding_id = format!("bnd_unit_{idx}");
        let chat_identity =
            WorkspaceChatIdentity::new(source_id, &binding_id, &thread_uuid, repo_str)
                .expect("valid unit chat identity");
        WorkspaceNativeSample {
            chat_identity,
            producer_epoch: epoch.to_string(),
            backend_process_instance: Some(ObservationProcessInstance {
                pid: 4242,
                boot_id: "boot-unit-1".to_string(),
                start_ticks: 9001,
            }),
            sequence,
            observed_at_ms: Some(observed_at_ms),
            last_success_at_ms: Some(observed_at_ms),
            thread_title: None,
            availability: WorkspaceAvailabilityState::Available,
            activity: WorkspaceActivityState::Idle,
            connection: WorkspaceSampleConnection {
                state: WorkspaceConnectionState::Connected,
                freshness: WorkspaceFreshnessState::Fresh,
                route: Some(NATIVE_CHAT_SAMPLE_ROUTE.to_string()),
                diagnostic: None,
            },
            capabilities: WorkspaceNativeCapabilities::default(),
            configured_selection: WorkspaceConfiguredSelection::verified(
                Some("gpt-5.4"),
                Some("high"),
            ),
        }
    }

    fn build_unit_sample(
        repo_str: &str,
        idx: u32,
        epoch: &str,
        sequence: u64,
        observed_at_ms: i64,
    ) -> WorkspaceNativeSample {
        build_source_sample(
            UNIT_SOURCE_ID,
            repo_str,
            idx,
            epoch,
            sequence,
            observed_at_ms,
        )
    }

    fn sample_sidecar_path(state_dir: &Path, sample: &WorkspaceNativeSample) -> PathBuf {
        let res = native_chat_sample_path(state_dir, &sample.chat_identity);
        res.expect("sample path")
    }

    fn count_sample_sidecars_on_disk(samples_dir: &Path) -> usize {
        let entries = fs::read_dir(samples_dir).expect("read samples_dir");
        let mut count = 0usize;
        for entry_res in entries {
            let entry = entry_res.expect("dir entry");
            let file_name = entry.file_name();
            if let Some(name) = file_name.to_str() {
                if is_expected_sample_filename_shape(name) {
                    count += 1;
                }
            }
        }
        count
    }

    fn write_private_test_file(path: &Path, bytes: &[u8]) {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .expect("create private test file");
        file.write_all(bytes).expect("write private test file");
    }

    struct PopulatedUnitStore {
        _temp: tempfile::TempDir,
        state_dir: PathBuf,
        samples_dir: PathBuf,
        repo_dir: PathBuf,
        unrelated_path: PathBuf,
        other_sample_path: PathBuf,
        oldest_0: WorkspaceNativeSample,
        oldest_1: WorkspaceNativeSample,
    }

    impl PopulatedUnitStore {
        fn new() -> Self {
            let temp = tempfile::tempdir().expect("tempdir");
            let root = fs::canonicalize(temp.path()).expect("canonical root");
            let state_dir = root.join("private-state");
            DirBuilder::new()
                .mode(0o700)
                .create(&state_dir)
                .expect("create state_dir");
            let repo_dir = root.join("project-repo");
            fs::create_dir(&repo_dir).expect("create repo_dir");
            let repo_str = repo_dir.to_str().expect("utf8 repo_dir");

            let mut oldest_0 = None;
            let mut oldest_1 = None;
            for idx in 0..MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE as u32 {
                let epoch = generate_sample_producer_epoch();
                let at_ms = 1_000 + i64::from(idx) * 10;
                let sample = build_unit_sample(repo_str, idx, &epoch, 1, at_ms);
                if idx == 0 {
                    oldest_0 = Some(sample.clone());
                } else if idx == 1 {
                    oldest_1 = Some(sample.clone());
                }
                let res = write_workspace_native_sample_sidecar(&state_dir, &sample);
                res.expect("seed bounded sample");
            }

            let other_epoch = generate_sample_producer_epoch();
            let other_sample =
                build_source_sample(OTHER_SOURCE_ID, repo_str, 0, &other_epoch, 1, 500);
            let other_res = write_workspace_native_sample_sidecar(&state_dir, &other_sample);
            let other_out = other_res.expect("seed other-source sample");

            let samples_dir = native_chat_samples_dir(&state_dir);
            let unrelated_path = samples_dir.join("unrelated-transcript.jsonl");
            write_private_test_file(&unrelated_path, b"keep-unrelated-target");

            Self {
                _temp: temp,
                state_dir,
                samples_dir,
                repo_dir,
                unrelated_path,
                other_sample_path: other_out.path,
                oldest_0: oldest_0.expect("seeded idx 0"),
                oldest_1: oldest_1.expect("seeded idx 1"),
            }
        }

        fn assert_unit_count(&self, expected: usize) {
            let disk_count = count_sample_sidecars_on_disk(&self.samples_dir);
            assert_eq!(disk_count, expected + 1);
            let filter = Some(UNIT_SOURCE_ID);
            let res = read_workspace_native_samples_readonly(&self.state_dir, filter, 9_000);
            let report = res.expect("readonly unit report");
            assert_eq!(report.entries.len(), expected);
            assert_eq!(report.omitted_count, 0);
        }

        fn assert_unrelated_preserved(&self) {
            let bytes = fs::read(&self.unrelated_path).expect("read unrelated file");
            assert_eq!(bytes, b"keep-unrelated-target");
            assert!(self.other_sample_path.exists());
            let filter = Some(OTHER_SOURCE_ID);
            let res = read_workspace_native_samples_readonly(&self.state_dir, filter, 9_000);
            let other_report = res.expect("other-source report");
            assert_eq!(other_report.entries.len(), 1);
        }
    }

    #[test]
    fn retention_cleanup_remove_and_sync_failures_never_claim_bounded_persistence() {
        let store = PopulatedUnitStore::new();
        let repo_str = store.repo_dir.to_str().expect("utf8 repo_dir");
        let oldest_0_path = sample_sidecar_path(&store.state_dir, &store.oldest_0);
        let oldest_1_path = sample_sidecar_path(&store.state_dir, &store.oldest_1);
        store.assert_unit_count(MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE);
        store.assert_unrelated_preserved();

        // 1. Failing required stale unlink before installing a new incoming sample returns IoError,
        //    leaves the incoming sidecar absent, and keeps the 128-sample bound intact.
        let epoch_128 = generate_sample_producer_epoch();
        let sample_128 = build_unit_sample(repo_str, 128, &epoch_128, 1, 3_000);
        let sample_128_path = sample_sidecar_path(&store.state_dir, &sample_128);
        let remove_err = write_workspace_native_sample_sidecar_with(
            &store.state_dir,
            &sample_128,
            |_stale_path| Err(io::Error::other("simulated stale unlink failure")),
            |dir_path| File::open(dir_path),
            |dir_file| dir_file.sync_all(),
        )
        .expect_err("stale unlink failure must abort before installing new sample");
        assert_eq!(remove_err.error_code(), NativeErrorCode::IoError);
        assert!(!sample_128_path.exists());
        assert!(oldest_0_path.exists());
        store.assert_unit_count(MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE);
        store.assert_unrelated_preserved();

        // 2. Directory open failure after unlinking oldest sample returns PersistenceUncertain
        //    without installing the new incoming sidecar or growing sample count.
        let open_err = write_workspace_native_sample_sidecar_with(
            &store.state_dir,
            &sample_128,
            |stale_path| fs::remove_file(stale_path),
            |_dir_path| {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "simulated dir open failure",
                ))
            },
            |dir_file| dir_file.sync_all(),
        )
        .expect_err("directory open failure after unlink must fail");
        assert_eq!(open_err.error_code(), NativeErrorCode::PersistenceUncertain);
        assert!(!sample_128_path.exists());
        assert!(!oldest_0_path.exists());
        store.assert_unit_count(MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE - 1);
        store.assert_unrelated_preserved();

        let refill_128 = write_workspace_native_sample_sidecar(&store.state_dir, &sample_128)
            .expect("refill store back to 128 samples");
        assert_eq!(refill_128.pruned_for_source, 0);
        assert!(sample_128_path.exists());
        store.assert_unit_count(MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE);

        // 3. Directory sync failure after unlinking oldest sample returns PersistenceUncertain
        //    without installing the new incoming sidecar or growing sample count.
        let epoch_129 = generate_sample_producer_epoch();
        let sample_129 = build_unit_sample(repo_str, 129, &epoch_129, 1, 3_020);
        let sample_129_path = sample_sidecar_path(&store.state_dir, &sample_129);
        let sync_err = write_workspace_native_sample_sidecar_with(
            &store.state_dir,
            &sample_129,
            |stale_path| fs::remove_file(stale_path),
            |dir_path| File::open(dir_path),
            |_dir_file| Err(io::Error::other("simulated dir sync failure")),
        )
        .expect_err("directory sync failure after unlink must fail");
        assert_eq!(sync_err.error_code(), NativeErrorCode::PersistenceUncertain);
        assert!(!sample_129_path.exists());
        assert!(!oldest_1_path.exists());
        store.assert_unit_count(MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE - 1);
        store.assert_unrelated_preserved();

        let refill_129 = write_workspace_native_sample_sidecar(&store.state_dir, &sample_129)
            .expect("refill store back to 128 samples");
        assert_eq!(refill_129.pruned_for_source, 0);
        store.assert_unit_count(MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE);

        // 4. Actual partial deletion (first stale unlink succeeds, second fails) reports
        //    PersistenceUncertain while still refusing to install the new incoming sidecar.
        let reseed_res = serialize_workspace_native_sample_sidecar(&store.oldest_0);
        let reseed_bytes = reseed_res.expect("serialize oldest_0");
        write_private_test_file(&oldest_0_path, &reseed_bytes);
        assert_eq!(
            count_sample_sidecars_on_disk(&store.samples_dir),
            MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE + 2
        );

        let epoch_130 = generate_sample_producer_epoch();
        let sample_130 = build_unit_sample(repo_str, 130, &epoch_130, 1, 3_030);
        let sample_130_path = sample_sidecar_path(&store.state_dir, &sample_130);
        let mut unlinked = 0usize;
        let partial_err = write_workspace_native_sample_sidecar_with(
            &store.state_dir,
            &sample_130,
            |stale_path| {
                if unlinked == 0 {
                    unlinked += 1;
                    fs::remove_file(stale_path)
                } else {
                    Err(io::Error::other("simulated second stale unlink failure"))
                }
            },
            |dir_path| File::open(dir_path),
            |dir_file| dir_file.sync_all(),
        )
        .expect_err("partial stale deletion must report PersistenceUncertain");
        assert_eq!(
            partial_err.error_code(),
            NativeErrorCode::PersistenceUncertain
        );
        assert_eq!(unlinked, 1);
        assert!(!sample_130_path.exists());
        store.assert_unit_count(MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE);

        // 5. Healthy prune, existing-sample update, and older-incoming drop preserve semantics.
        let clean_outcome = write_workspace_native_sample_sidecar(&store.state_dir, &sample_130)
            .expect("healthy prune");
        assert_eq!(clean_outcome.pruned_for_source, 1);
        assert!(sample_130_path.exists());

        let sample_130_v2 = build_unit_sample(repo_str, 130, &epoch_130, 2, 3_040);
        let update_res = write_workspace_native_sample_sidecar(&store.state_dir, &sample_130_v2);
        let update_outcome = update_res.expect("replace existing retained sample");
        assert_eq!(
            update_outcome.decision,
            WorkspaceSampleReconcileDecision::AcceptedNextSequence
        );
        assert_eq!(update_outcome.pruned_for_source, 0);

        let old_epoch = generate_sample_producer_epoch();
        let old_incoming = build_unit_sample(repo_str, 500, &old_epoch, 1, 900);
        let old_incoming_path = sample_sidecar_path(&store.state_dir, &old_incoming);
        let old_outcome = write_workspace_native_sample_sidecar(&store.state_dir, &old_incoming)
            .expect("drop older incoming sample cleanly");
        assert_eq!(old_outcome.pruned_for_source, 1);
        assert!(!old_incoming_path.exists());
        store.assert_unit_count(MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE);
        store.assert_unrelated_preserved();
    }

    #[test]
    fn repeated_distinct_new_inputs_under_prune_failure_preserve_bound_until_recovery() {
        let store = PopulatedUnitStore::new();
        let repo_str = store.repo_dir.to_str().expect("utf8 repo_dir");
        let oldest_0_path = sample_sidecar_path(&store.state_dir, &store.oldest_0);

        let mut rejected_paths = Vec::new();
        for offset in 0..8u32 {
            let idx = 200 + offset;
            let epoch = generate_sample_producer_epoch();
            let at_ms = 5_000 + i64::from(offset) * 10;
            let incoming = build_unit_sample(repo_str, idx, &epoch, 1, at_ms);
            let incoming_path = sample_sidecar_path(&store.state_dir, &incoming);
            let err = write_workspace_native_sample_sidecar_with(
                &store.state_dir,
                &incoming,
                |_stale_path| {
                    Err(io::Error::other(
                        "simulated persistent stale unlink failure",
                    ))
                },
                |dir_path| File::open(dir_path),
                |dir_file| dir_file.sync_all(),
            )
            .expect_err("repeated distinct chat under prune failure must fail closed");
            assert_eq!(err.error_code(), NativeErrorCode::IoError);
            assert!(!incoming_path.exists());
            store.assert_unit_count(MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE);
            rejected_paths.push(incoming_path);
        }

        assert!(oldest_0_path.exists());
        for path in &rejected_paths {
            assert!(!path.exists());
        }
        store.assert_unrelated_preserved();

        for offset in 0..3u32 {
            let idx = 300 + offset;
            let epoch = generate_sample_producer_epoch();
            let at_ms = 6_000 + i64::from(offset) * 10;
            let incoming = build_unit_sample(repo_str, idx, &epoch, 1, at_ms);
            let incoming_path = sample_sidecar_path(&store.state_dir, &incoming);
            let write_res = write_workspace_native_sample_sidecar(&store.state_dir, &incoming);
            let outcome = write_res.expect("healthy recovery write");
            assert_eq!(outcome.pruned_for_source, 1);
            assert!(incoming_path.exists());
            store.assert_unit_count(MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE);
        }

        assert!(!oldest_0_path.exists());
        for path in &rejected_paths {
            assert!(!path.exists());
        }
        store.assert_unrelated_preserved();
    }

    #[test]
    fn prune_helper_rejects_symlink_and_directory_entries_without_deleting_targets() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = fs::canonicalize(temp.path()).expect("canonical root");
        let samples_dir = root.join("samples");
        DirBuilder::new()
            .mode(0o700)
            .create(&samples_dir)
            .expect("create samples_dir");

        let outside_file = root.join("outside-transcript.jsonl");
        fs::write(&outside_file, b"preserve-me").expect("write outside file");

        let symlink_name =
            "sample-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json"
                .to_string();
        symlink(&outside_file, samples_dir.join(&symlink_name)).expect("create symlink");

        let mut remove_called = false;
        for already_persisted in [false, true] {
            let sym_err = prune_stale_sample_files_and_sync(
                &samples_dir,
                std::slice::from_ref(&symlink_name),
                already_persisted,
                &mut |_p| {
                    remove_called = true;
                    Ok(())
                },
                |dir_path| File::open(dir_path),
                |dir_file| dir_file.sync_all(),
            )
            .expect_err("symlink at stale path must fail closed");
            assert_eq!(sym_err.error_code(), NativeErrorCode::SecurityViolation);
            assert!(!remove_called);
        }
        assert_eq!(
            fs::read(&outside_file).expect("outside file preserved"),
            b"preserve-me"
        );

        let dir_name =
            "sample-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json"
                .to_string();
        DirBuilder::new()
            .mode(0o700)
            .create(samples_dir.join(&dir_name))
            .expect("create directory with sample shape");

        for already_persisted in [false, true] {
            let dir_err = prune_stale_sample_files_and_sync(
                &samples_dir,
                std::slice::from_ref(&dir_name),
                already_persisted,
                &mut |_p| {
                    remove_called = true;
                    Ok(())
                },
                |dir_path| File::open(dir_path),
                |dir_file| dir_file.sync_all(),
            )
            .expect_err("directory at stale path must fail closed");
            assert_eq!(dir_err.error_code(), NativeErrorCode::SecurityViolation);
            assert!(!remove_called);
        }
        assert!(samples_dir.join(&dir_name).is_dir());
    }
}
