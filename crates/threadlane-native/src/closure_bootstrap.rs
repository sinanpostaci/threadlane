//! Verified ordinary closure bootstrap for the paired native route.

use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::context_export::OperatorContextBootstrap;
use crate::fs_safety::{
    read_private_file_bounded, validate_absolute_no_symlink_components,
    MAX_PRIVATE_STATE_FILE_BYTES,
};
use crate::{NativeError, NativeErrorCode};

const MAX_CLI_BYTES: u64 = 512 * 1024 * 1024;
const MAX_SOURCE_BYTES: usize = 192 * 1024;
const MAX_BOOTSTRAP_BYTES: usize = 16 * 1024;
pub(crate) const BOOTSTRAP_SOURCE: &str =
    include_str!("../../../skills/threadlane/references/ordinary-bootstrap.js");

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ClosureOrigin {
    pub schema_version: u32,
    pub binding_id: String,
    pub thread_id: String,
    pub project_root: PathBuf,
    pub cli_package: PathBuf,
    pub context_path: PathBuf,
    pub context_router_executable: PathBuf,
    pub context_sha256: String,
    pub cli_sha256: String,
    pub guide_version: String,
    pub guide_sha256: String,
    pub template_version: String,
    pub source_sha256: String,
    pub bootstrap_sha256: String,
}

fn rejected(message: &'static str) -> NativeError {
    NativeError::new(NativeErrorCode::SecurityViolation, message)
}

fn hash_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileStamp {
    device: u64,
    inode: u64,
    length: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    modified: i64,
    modified_ns: i64,
    changed: i64,
    changed_ns: i64,
}

#[cfg(unix)]
fn file_stamp(meta: &fs::Metadata) -> Option<FileStamp> {
    use std::os::unix::fs::MetadataExt;
    if !meta.file_type().is_file() || meta.file_type().is_symlink() {
        return None;
    }
    Some(FileStamp {
        device: meta.dev(),
        inode: meta.ino(),
        length: meta.len(),
        mode: meta.mode(),
        uid: meta.uid(),
        gid: meta.gid(),
        modified: meta.mtime(),
        modified_ns: meta.mtime_nsec(),
        changed: meta.ctime(),
        changed_ns: meta.ctime_nsec(),
    })
}

#[cfg(not(unix))]
type FileStamp = ();

#[cfg(not(unix))]
fn file_stamp(_meta: &fs::Metadata) -> Option<FileStamp> {
    None
}

#[derive(Debug, Clone)]
struct CachedCliHash {
    path: PathBuf,
    stamp: FileStamp,
    digest: String,
    file: Arc<File>,
}

#[derive(Debug, Default)]
struct CacheState {
    generation: u128,
    entry: Option<CachedCliHash>,
}

#[derive(Debug, Default)]
pub(crate) struct CliHashCache {
    state: Mutex<CacheState>,
    #[cfg(test)]
    reads: std::sync::atomic::AtomicUsize,
}

impl CliHashCache {
    pub(crate) fn clear(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.generation = state.generation.saturating_add(1);
            state.entry = None;
        }
    }
}

fn executable_hash(path: &Path, cache: &CliHashCache) -> Result<String, NativeError> {
    let checked = validate_absolute_no_symlink_components(path, "closure CLI")?;
    let metadata =
        fs::symlink_metadata(&checked).map_err(|_| rejected("closure CLI is unavailable"))?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() > MAX_CLI_BYTES
        || metadata.len() == 0
    {
        return Err(rejected("closure CLI is not a bounded regular executable"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o111 == 0 {
            return Err(rejected("closure CLI is not executable"));
        }
    }
    if fs::canonicalize(&checked).ok().as_deref() != Some(checked.as_path()) {
        return Err(rejected("closure CLI path is not canonical"));
    }
    let mut file = File::open(&checked).map_err(|_| rejected("closure CLI cannot be opened"))?;
    let opened = file
        .metadata()
        .map_err(|_| rejected("closure CLI cannot be inspected"))?;
    if !same_file(&metadata, &opened) {
        return Err(rejected("closure CLI changed during open"));
    }
    let stamp =
        file_stamp(&opened).ok_or_else(|| rejected("closure CLI metadata is unsupported"))?;
    let (cached, generation) = {
        let mut state = cache
            .state
            .lock()
            .map_err(|_| rejected("closure CLI cache is unavailable"))?;
        let value = match state.entry.as_ref() {
            Some(previous) if previous.path == checked && previous.stamp == stamp => {
                Some(previous.clone())
            }
            _ => {
                state.entry = None;
                None
            }
        };
        (value, state.generation)
    };
    if let Some(cached) = cached {
        let after = file
            .metadata()
            .map_err(|_| rejected("closure CLI cannot be inspected"))?;
        let pinned = cached
            .file
            .metadata()
            .map_err(|_| rejected("pinned closure CLI cannot be inspected"))?;
        let path_after =
            fs::symlink_metadata(&checked).map_err(|_| rejected("closure CLI disappeared"))?;
        if !same_file(&opened, &after)
            || !same_file(&opened, &pinned)
            || !same_file(&opened, &path_after)
        {
            cache.clear();
            return Err(rejected("closure CLI changed during cached validation"));
        }
        return Ok(cached.digest);
    }
    #[cfg(test)]
    cache
        .reads
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut bounded = (&mut file).take(MAX_CLI_BYTES + 1);
    let mut hasher = Sha256::new();
    let count = io::copy(&mut bounded, &mut hasher)
        .map_err(|_| rejected("closure CLI cannot be hashed"))?;
    let after = file
        .metadata()
        .map_err(|_| rejected("closure CLI cannot be inspected"))?;
    let path_after =
        fs::symlink_metadata(&checked).map_err(|_| rejected("closure CLI disappeared"))?;
    if count != metadata.len()
        || count > MAX_CLI_BYTES
        || !same_file(&opened, &after)
        || !same_file(&opened, &path_after)
    {
        return Err(rejected("closure CLI changed while hashing"));
    }
    let digest = format!("{:x}", hasher.finalize());
    let mut state = cache
        .state
        .lock()
        .map_err(|_| rejected("closure CLI cache is unavailable"))?;
    if state.generation == generation && generation != u128::MAX {
        state.entry = Some(CachedCliHash {
            path: checked,
            stamp,
            digest: digest.clone(),
            file: Arc::new(file),
        });
    }
    Ok(digest)
}

#[cfg(unix)]
fn same_file(a: &fs::Metadata, b: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    b.file_type().is_file()
        && !b.file_type().is_symlink()
        && a.dev() == b.dev()
        && a.ino() == b.ino()
        && a.len() == b.len()
        && a.mode() == b.mode()
        && a.uid() == b.uid()
        && a.gid() == b.gid()
        && a.mtime() == b.mtime()
        && a.mtime_nsec() == b.mtime_nsec()
        && a.ctime() == b.ctime()
        && a.ctime_nsec() == b.ctime_nsec()
}

#[cfg(not(unix))]
fn same_file(_a: &fs::Metadata, _b: &fs::Metadata) -> bool {
    false
}

pub(crate) fn origin_for_connection(
    binding_id: &str,
    thread_id: &str,
    context: &OperatorContextBootstrap,
    cache: &CliHashCache,
) -> Result<ClosureOrigin, NativeError> {
    if !context.available
        || context.status != "ready"
        || context.route != "trusted_local_operator"
        || context.native_per_call_identity_verified
        || context.human_approval_verified
        || context.schema_version != Some(1)
        || binding_id.is_empty()
        || thread_id.is_empty()
    {
        return Err(rejected("verified operator context is unavailable"));
    }
    let project_root = context
        .project_root
        .clone()
        .ok_or_else(|| rejected("project root is unavailable"))?;
    let cli_package = context
        .cli_package
        .clone()
        .ok_or_else(|| rejected("CLI package is unavailable"))?;
    let context_path = context
        .context_path
        .clone()
        .ok_or_else(|| rejected("context path is unavailable"))?;
    let router = context
        .context_router_executable
        .clone()
        .ok_or_else(|| rejected("context router is unavailable"))?;
    let context_sha256 = context
        .content_sha256
        .clone()
        .ok_or_else(|| rejected("context digest is unavailable"))?;
    if router != cli_package.join("bin/threadlane-context")
        || validate_absolute_no_symlink_components(&project_root, "project root")? != project_root
        || validate_absolute_no_symlink_components(&cli_package, "CLI package")? != cli_package
        || fs::canonicalize(&project_root).ok().as_deref() != Some(project_root.as_path())
        || fs::canonicalize(&cli_package).ok().as_deref() != Some(cli_package.as_path())
    {
        return Err(rejected("project or package identity changed"));
    }
    let context_bytes = read_private_file_bounded(
        &context_path,
        MAX_PRIVATE_STATE_FILE_BYTES,
        "operator context",
    )?
    .ok_or_else(|| rejected("operator context disappeared"))?;
    if hash_bytes(&context_bytes) != context_sha256 {
        return Err(rejected("operator context digest changed"));
    }
    let cli_sha256 = executable_hash(&cli_package.join("bin/threadlane"), cache)?;
    Ok(ClosureOrigin {
        schema_version: 1,
        binding_id: binding_id.to_owned(),
        thread_id: thread_id.to_owned(),
        project_root,
        cli_package,
        context_path,
        context_router_executable: router,
        context_sha256,
        cli_sha256,
        guide_version: threadlane::OPERATING_GUIDE_VERSION.to_owned(),
        guide_sha256: hash_bytes(threadlane::OPERATING_GUIDE_MARKDOWN.as_bytes()),
        template_version: threadlane::CLOSURE_TEMPLATE_VERSION.to_owned(),
        source_sha256: threadlane::closure_template_source_sha256(),
        bootstrap_sha256: hash_bytes(BOOTSTRAP_SOURCE.as_bytes()),
    })
}

pub(crate) fn verify_source(source: &str) -> Result<String, NativeError> {
    if source.len() > MAX_SOURCE_BYTES
        || source.as_bytes() != threadlane::CLOSURE_TEMPLATE_JAVASCRIPT.as_bytes()
    {
        return Err(rejected(
            "closure source does not match the compiled package",
        ));
    }
    Ok(hash_bytes(source.as_bytes()))
}

pub(crate) fn verify_bootstrap(source: &str) -> Result<String, NativeError> {
    if source.len() > MAX_BOOTSTRAP_BYTES || source.as_bytes() != BOOTSTRAP_SOURCE.as_bytes() {
        return Err(rejected(
            "closure bootstrap does not match the compiled package",
        ));
    }
    Ok(hash_bytes(source.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn fixture() -> (tempfile::TempDir, OperatorContextBootstrap) {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let project = root.path().join("project");
        let package = root.path().join("package");
        let bin = package.join("bin");
        fs::create_dir(&project).unwrap();
        fs::create_dir_all(&bin).unwrap();
        let cli = bin.join("threadlane");
        let router = bin.join("threadlane-context");
        fs::write(&cli, b"cli bytes").unwrap();
        fs::write(&router, b"router bytes").unwrap();
        fs::set_permissions(&cli, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&router, fs::Permissions::from_mode(0o700)).unwrap();
        let path = root.path().join("context.json");
        fs::write(&path, b"private context").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let context = OperatorContextBootstrap {
            available: true,
            status: "ready".into(),
            code: None,
            message: None,
            schema_version: Some(1),
            route: "trusted_local_operator".into(),
            native_per_call_identity_verified: false,
            human_approval_verified: false,
            context_path: Some(path),
            context_router_executable: Some(router),
            project_root: Some(project),
            cli_package: Some(package),
            content_sha256: Some(hash_bytes(b"private context")),
            reused_existing: Some(false),
        };
        (root, context)
    }

    #[cfg(unix)]
    #[test]
    fn connection_receipt_fails_closed_on_package_or_private_context_drift() {
        let (_root, context) = fixture();
        let origin = origin_for_connection(
            "bnd_original",
            "thread_original",
            &context,
            &CliHashCache::default(),
        )
        .unwrap();
        assert_eq!(
            origin.source_sha256,
            threadlane::closure_template_source_sha256()
        );
        assert_eq!(origin.cli_sha256, hash_bytes(b"cli bytes"));
        assert_ne!(
            origin,
            origin_for_connection(
                "bnd_other",
                "thread_original",
                &context,
                &CliHashCache::default()
            )
            .unwrap()
        );
        let cli = context.cli_package.as_ref().unwrap().join("bin/threadlane");
        fs::write(&cli, b"changed cli").unwrap();
        assert_ne!(
            origin,
            origin_for_connection(
                "bnd_original",
                "thread_original",
                &context,
                &CliHashCache::default()
            )
            .unwrap()
        );
        fs::write(context.context_path.as_ref().unwrap(), b"changed context").unwrap();
        assert!(origin_for_connection(
            "bnd_original",
            "thread_original",
            &context,
            &CliHashCache::default()
        )
        .is_err());
    }

    #[cfg(unix)]
    #[test]
    fn connection_receipt_rejects_symlinked_or_non_executable_cli() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let (_root, context) = fixture();
        let cli = context.cli_package.as_ref().unwrap().join("bin/threadlane");
        fs::set_permissions(&cli, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(
            origin_for_connection("bnd", "thread", &context, &CliHashCache::default()).is_err()
        );
        fs::remove_file(&cli).unwrap();
        symlink(context.context_router_executable.as_ref().unwrap(), &cli).unwrap();
        assert!(
            origin_for_connection("bnd", "thread", &context, &CliHashCache::default()).is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn cached_cli_identity_revalidates_opened_file_and_detects_equal_size_changes() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        use std::sync::atomic::Ordering;
        let (_root, context) = fixture();
        let cache = CliHashCache::default();
        let cli = context.cli_package.as_ref().unwrap().join("bin/threadlane");
        let first = executable_hash(&cli, &cache).unwrap();
        assert_eq!(first, executable_hash(&cli, &cache).unwrap());
        assert_eq!(
            cache.reads.load(Ordering::Relaxed),
            1,
            "unchanged descriptor does not rehash"
        );
        std::fs::write(&cli, b"cli byteX").unwrap();
        let changed = executable_hash(&cli, &cache).unwrap();
        assert_ne!(changed, first);
        assert_eq!(
            cache.reads.load(Ordering::Relaxed),
            2,
            "same-size write rehashes"
        );
        let old_descriptor = {
            let state = cache.state.lock().unwrap();
            Arc::downgrade(&state.entry.as_ref().unwrap().file)
        };
        let replacement = cli.with_extension("replacement");
        std::fs::write(&replacement, b"cli byteX").unwrap();
        std::fs::set_permissions(&replacement, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::rename(&replacement, &cli).unwrap();
        assert!(
            old_descriptor.upgrade().is_some(),
            "old inode remains pinned until revalidation"
        );
        assert_eq!(changed, executable_hash(&cli, &cache).unwrap());
        assert!(
            old_descriptor.upgrade().is_none(),
            "revalidation releases the old pinned descriptor"
        );
        assert_eq!(
            cache.reads.load(Ordering::Relaxed),
            3,
            "atomic replacement rehashes"
        );
        cache.clear();
        assert_eq!(changed, executable_hash(&cli, &cache).unwrap());
        assert_eq!(
            cache.reads.load(Ordering::Relaxed),
            4,
            "connection reset discards the cache"
        );
        std::fs::remove_file(&cli).unwrap();
        symlink(context.context_router_executable.as_ref().unwrap(), &cli).unwrap();
        assert!(executable_hash(&cli, &cache).is_err());
    }

    #[test]
    fn source_verification_checks_the_actual_bytes_before_attesting() {
        let source = threadlane::CLOSURE_TEMPLATE_JAVASCRIPT;
        let proof = verify_source(source).expect("the compiled source is trusted");
        assert_eq!(proof, threadlane::closure_template_source_sha256());
        let mut changed = source.to_owned();
        changed.push_str("\nthrow new Error('must not execute')");
        assert!(verify_source(&changed).is_err());
        assert!(verify_source("").is_err());
    }

    #[test]
    #[ignore]
    fn measure_local_cli_hash() {
        let path = std::env::var_os("THREADLANE_HASH_PROBE").expect("local CLI path");
        let cache = CliHashCache::default();
        for _ in 0..3 {
            let start = std::time::Instant::now();
            let digest = executable_hash(Path::new(&path), &cache).expect("hash CLI");
            eprintln!(
                "cli_sha256={digest} elapsed_ms={}",
                start.elapsed().as_millis()
            );
        }
    }

    #[test]
    fn actual_source_fits_one_bounded_native_request() {
        let input = serde_json::json!({"javascript": threadlane::CLOSURE_TEMPLATE_JAVASCRIPT});
        assert!(serde_json::to_vec(&input).unwrap().len() < 256 * 1024);
    }
}
