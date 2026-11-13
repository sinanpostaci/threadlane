#![cfg(unix)]

//! Explicit live acceptance capture for the public authorized worker-presence route.
//!
//! The parent real-provider acceptance harness runs this ignored test only while a real
//! authorized worker is busy. `THREADLANE_REAL_PRESENCE_REQUEST` must point to an
//! owner-private canonical `0600` JSON request. The test validates the supplied scope,
//! calls the production read-only route, checks the returned DTO contract, and writes the
//! snapshot to one new private output. It never starts a host or mutates runtime state.

use std::fs::{self, Metadata, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use threadlane::presence::{
    WorkerPresenceAvailability, WorkerPresenceFreshness, WORKER_PRESENCE_SCHEMA_VERSION,
};
use threadlane::WorkerStatus;
use threadlane_host::ApprovedHostClient;

const REQUEST_ENV: &str = "THREADLANE_REAL_PRESENCE_REQUEST";
const MAX_REQUEST_BYTES: u64 = 64 * 1024;
const MAX_OUTPUT_BYTES: usize = 64 * 1024;
const PRESENCE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CaptureRequest {
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    control_binding_id: String,
    native_thread_uuid: String,
    approval_id: String,
    task_id: String,
    expected_run_id: String,
    expected_worker_id: String,
    output_path: PathBuf,
}

#[tokio::test]
#[ignore = "explicit real provider acceptance only"]
async fn capture_real_authorized_worker_presence() {
    let request_path = std::env::var_os(REQUEST_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("{REQUEST_ENV} must point to the capture request"));
    let request = read_capture_request(&request_path);

    let authority_dir = canonical_private_directory(&request.authority_dir, "authority_dir");
    let sessions_dir = canonical_private_directory(&request.sessions_dir, "sessions_dir");
    let output_path = validate_output_path(&request.output_path, &authority_dir, &sessions_dir);

    let presence = tokio::time::timeout(PRESENCE_TIMEOUT, async {
        // A dispatch receipt can precede the first runner-owned process sample.
        // Wait only for read-only evidence of this same invocation, never redispatch.
        loop {
            let sample = ApprovedHostClient::get_task_presence_readonly_at(
                &authority_dir,
                &sessions_dir,
                &request.control_binding_id,
                &request.native_thread_uuid,
                &request.approval_id,
                &request.task_id,
            )
            .await
            .expect("authorized presence capture failed");
            if let Some(sample) = sample {
                break sample;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("presence capture exceeded the 5s timeout");
    let now_ms = unix_time_ms();

    assert_eq!(presence.schema_version, WORKER_PRESENCE_SCHEMA_VERSION);
    assert_eq!(presence.run_id, request.expected_run_id);
    assert_eq!(presence.worker_id, request.expected_worker_id);
    assert!(presence.live, "captured worker must be actually live");
    assert_eq!(presence.availability, WorkerPresenceAvailability::Live);
    assert_eq!(presence.freshness, WorkerPresenceFreshness::Fresh);
    assert_eq!(presence.worker_status, WorkerStatus::Running);
    assert!(presence.unavailable_reason.is_none());

    let owner = presence
        .owner
        .as_ref()
        .expect("captured presence must include its host-control owner");
    assert_eq!(owner.approval_id, request.approval_id);
    assert_eq!(owner.control_binding_id, request.control_binding_id);
    assert_eq!(owner.thread_uuid, request.native_thread_uuid);
    // The snapshot's `binding_id` is the host session binding; the request's
    // `control_binding_id` is owned exactly by the host-control owner tuple above.
    assert_eq!(presence.binding_id, owner.session_binding_id);

    let process = presence
        .process
        .as_ref()
        .expect("captured presence must include its runner-owned process snapshot");
    assert!(
        process.is_valid(),
        "captured process must be structurally valid"
    );
    assert!(process.is_live(), "captured process must be actually live");

    let sampled_at_ms = presence
        .sampled_at_ms
        .expect("captured presence must include its original sampled_at_ms");
    let expires_at_ms = presence
        .expires_at_ms
        .expect("captured presence must include its original expires_at_ms");
    let ttl_ms = presence.max_sample_age_ms;
    assert!(sampled_at_ms > 0, "sampled_at_ms must be nonzero");
    assert!(expires_at_ms > 0, "expires_at_ms must be nonzero");
    assert!(ttl_ms > 0, "max_sample_age_ms must be nonzero");
    assert!(
        sampled_at_ms <= now_ms,
        "sampled_at_ms must not be in the future"
    );
    assert!(
        now_ms < expires_at_ms,
        "captured presence sample must not be expired"
    );
    let ttl_i64 = i64::try_from(ttl_ms).expect("presence TTL must fit in i64");
    assert!(
        expires_at_ms <= sampled_at_ms.saturating_add(ttl_i64),
        "expires_at_ms must not exceed sampled_at_ms + TTL"
    );
    let sample_age_ms = presence
        .sample_age_ms
        .expect("captured presence must include its sample age");
    assert!(
        sample_age_ms <= ttl_ms,
        "reported sample age must stay within TTL"
    );
    let current_age_ms =
        u64::try_from(now_ms - sampled_at_ms).expect("current sample age must be nonnegative");
    assert!(
        current_age_ms <= ttl_ms,
        "current sample age must stay within TTL"
    );

    let serialized = serde_json::to_vec(&presence).expect("serialize captured presence snapshot");
    assert!(
        serialized.len() <= MAX_OUTPUT_BYTES,
        "captured presence snapshot must be at most 64 KiB"
    );

    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&output_path)
        .expect("create one fresh private presence output");
    output
        .write_all(&serialized)
        .expect("write captured presence output");
    output.sync_all().expect("sync captured presence output");
    let metadata = output.metadata().expect("inspect fresh presence output");
    assert!(
        metadata.file_type().is_file(),
        "presence output must remain a regular file"
    );
    assert_private_owner(&metadata, 0o600, "presence output");
    assert_eq!(
        metadata.len(),
        u64::try_from(serialized.len()).expect("serialized presence length fits in u64"),
        "presence output must contain the complete serialized snapshot"
    );

    println!("real authorized worker presence capture passed: live fresh running owner-scoped");
}

fn read_capture_request(path: &Path) -> CaptureRequest {
    assert!(path.is_absolute(), "capture request path must be absolute");
    let metadata = fs::symlink_metadata(path)
        .unwrap_or_else(|err| panic!("capture request must be an existing private file: {err}"));
    assert!(
        !metadata.file_type().is_symlink(),
        "capture request path must not be a symlink"
    );
    assert!(
        metadata.file_type().is_file(),
        "capture request path must be a regular file"
    );
    assert_private_owner(&metadata, 0o600, "capture request");
    assert!(
        metadata.len() <= MAX_REQUEST_BYTES,
        "capture request must be at most 64 KiB"
    );
    let canonical =
        fs::canonicalize(path).unwrap_or_else(|err| panic!("canonicalize capture request: {err}"));
    assert_eq!(
        canonical.as_path(),
        path,
        "capture request path must already be canonical"
    );

    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&canonical)
        .unwrap_or_else(|err| panic!("open capture request without following links: {err}"));
    let opened = file
        .metadata()
        .unwrap_or_else(|err| panic!("inspect opened capture request: {err}"));
    assert!(
        opened.file_type().is_file(),
        "capture request must remain a regular file"
    );
    assert_private_owner(&opened, 0o600, "capture request");
    assert!(
        opened.len() <= MAX_REQUEST_BYTES,
        "capture request must be at most 64 KiB"
    );

    let mut bytes = Vec::new();
    let mut bounded = file.take(MAX_REQUEST_BYTES + 1);
    bounded
        .read_to_end(&mut bytes)
        .unwrap_or_else(|err| panic!("read bounded capture request: {err}"));
    assert!(
        bytes.len() <= MAX_REQUEST_BYTES as usize,
        "capture request must be at most 64 KiB"
    );
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|err| panic!("capture request must be valid strict JSON: {err}"))
}

fn canonical_private_directory(path: &Path, label: &str) -> PathBuf {
    assert!(path.is_absolute(), "{label} must be an absolute path");
    let metadata = fs::symlink_metadata(path)
        .unwrap_or_else(|err| panic!("{label} must be an existing private directory: {err}"));
    assert!(
        !metadata.file_type().is_symlink(),
        "{label} must not be a symlink"
    );
    assert!(metadata.file_type().is_dir(), "{label} must be a directory");
    assert_private_owner(&metadata, 0o700, label);
    let canonical =
        fs::canonicalize(path).unwrap_or_else(|err| panic!("canonicalize {label}: {err}"));
    assert_eq!(
        canonical.as_path(),
        path,
        "{label} must already be canonical"
    );
    canonical
}

fn validate_output_path(output_path: &Path, authority_dir: &Path, sessions_dir: &Path) -> PathBuf {
    assert!(output_path.is_absolute(), "output_path must be absolute");
    let file_name = output_path
        .file_name()
        .expect("output_path must name a file");
    let parent = output_path
        .parent()
        .expect("output_path must have a parent");
    let canonical_parent = canonical_private_directory(parent, "output parent");
    let exact_output = canonical_parent.join(file_name);
    assert_eq!(
        output_path,
        exact_output.as_path(),
        "output_path must be a direct child of its canonical parent"
    );
    match fs::symlink_metadata(output_path) {
        Ok(_) => panic!("output_path must not already exist"),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => panic!("inspect output_path before creation: {err}"),
    }
    for (label, store) in [
        ("authority_dir", authority_dir),
        ("sessions_dir", sessions_dir),
    ] {
        assert!(
            !output_path.starts_with(store) && !store.starts_with(output_path),
            "output_path must not overlap {label}"
        );
    }
    exact_output
}

fn assert_private_owner(metadata: &Metadata, expected_mode: u32, label: &str) {
    assert_eq!(
        metadata.uid(),
        unsafe { libc::geteuid() },
        "{label} must be owned by the effective user"
    );
    assert_eq!(
        metadata.permissions().mode() & 0o777,
        expected_mode,
        "{label} must have mode {expected_mode:04o}"
    );
}

fn unix_time_ms() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch");
    i64::try_from(elapsed.as_millis()).expect("Unix time fits in i64")
}
