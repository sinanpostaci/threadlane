use super::*;

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::io::Write;
use std::os::unix::fs::{symlink, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static TEST_TREE_COUNTER: AtomicU64 = AtomicU64::new(0);

const LOCK_WORKER_FLAG_ENV: &str = "THREADLANE_OBSERVATION_SOURCES_LOCK_WORKER";
const LOCK_WORKER_APP_DATA_ENV: &str = "THREADLANE_OBSERVATION_SOURCES_LOCK_WORKER_APP_DATA";
const LOCK_WORKER_CONTEXT_ENV: &str = "THREADLANE_OBSERVATION_SOURCES_LOCK_WORKER_CONTEXT";
const LOCK_WORKER_READY_ENV: &str = "THREADLANE_OBSERVATION_SOURCES_LOCK_WORKER_READY";
const LOCK_WORKER_FILTER: &str = "linked_context_lock_worker";
const LOCK_WORKER_TIMEOUT: Duration = Duration::from_secs(10);
const LOCK_BLOCK_CONFIRMATION: Duration = Duration::from_millis(500);

struct TestTree {
    root: PathBuf,
}

impl TestTree {
    fn new(label: &str) -> Self {
        let base = fs::canonicalize(env::temp_dir()).unwrap_or_else(|_| env::temp_dir());
        loop {
            let seq = TEST_TREE_COUNTER.fetch_add(1, Ordering::Relaxed);
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0);
            let root = base.join(format!(
                "threadlane-observation-sources-{label}-{}-{seq}-{nanos}",
                std::process::id()
            ));
            match fs::create_dir(&root) {
                Ok(()) => {
                    set_mode(&root, 0o700);
                    return Self { root };
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(err) => panic!("create private test tree {}: {err}", root.display()),
            }
        }
    }

    fn path(&self) -> &Path {
        &self.root
    }

    fn join(&self, relative: &str) -> PathBuf {
        self.root.join(relative)
    }

    fn dir(&self, relative: &str) -> PathBuf {
        let mut current = self.root.clone();
        for segment in relative.split('/') {
            current.push(segment);
            match fs::create_dir(&current) {
                Ok(()) => set_mode(&current, 0o700),
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    set_mode(&current, 0o700)
                }
                Err(err) => panic!("create test directory {}: {err}", current.display()),
            }
        }
        current
    }

    fn write_file(&self, relative: &str, bytes: &[u8], mode: u32) -> PathBuf {
        let path = self.join(relative);
        let parent = path.parent().expect("test file parent");
        assert!(parent.is_dir(), "parent {} must exist", parent.display());
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(mode)
            .open(&path)
            .unwrap_or_else(|err| panic!("create {}: {err}", path.display()));
        file.write_all(bytes)
            .unwrap_or_else(|err| panic!("write {}: {err}", path.display()));
        file.sync_all()
            .unwrap_or_else(|err| panic!("sync {}: {err}", path.display()));
        drop(file);
        set_mode(&path, mode);
        path
    }

    fn write_json(&self, relative: &str, value: &serde_json::Value) -> (PathBuf, Vec<u8>) {
        let bytes = serde_json::to_vec_pretty(value).expect("serialize test context");
        let path = self.write_file(relative, &bytes, 0o600);
        (path, bytes)
    }
}

impl Drop for TestTree {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn set_mode(path: &Path, mode: u32) {
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .unwrap_or_else(|err| panic!("chmod {mode:04o} {}: {err}", path.display()));
}

fn path_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

struct Stores {
    state: PathBuf,
    registry: PathBuf,
    plans: PathBuf,
    authority: PathBuf,
    sessions: PathBuf,
}

impl Stores {
    fn new(tree: &TestTree, prefix: &str) -> Self {
        Self {
            state: tree.dir(&format!("{prefix}/state")),
            registry: tree.dir(&format!("{prefix}/registry")),
            plans: tree.dir(&format!("{prefix}/plans")),
            authority: tree.dir(&format!("{prefix}/authority")),
            sessions: tree.dir(&format!("{prefix}/sessions")),
        }
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "state_dir": path_text(&self.state),
            "registry_dir": path_text(&self.registry),
            "plans_dir": path_text(&self.plans),
            "authority_dir": path_text(&self.authority),
            "sessions_dir": path_text(&self.sessions),
        })
    }
}

fn context_value(
    project_root: &Path,
    binding_id: &str,
    thread_uuid: &str,
    stores: &Stores,
) -> serde_json::Value {
    serde_json::json!({
        "schema_version": 1,
        "project_root": path_text(project_root),
        "binding_id": binding_id,
        "thread_uuid": thread_uuid,
        "route": "trusted_local_operator",
        "native_per_call_identity_verified": false,
        "stores": stores.json(),
    })
}

fn config(app_data_dir: &Path, stores: &Stores) -> ObservationSourceConfig {
    ObservationSourceConfig {
        runtime_state_dir: None,
        registry_dir: stores.registry.clone(),
        plans_dir: stores.plans.clone(),
        authority_dir: stores.authority.clone(),
        sessions_dir: stores.sessions.clone(),
        app_data_dir: app_data_dir.to_path_buf(),
    }
}

fn persisted_registry(app_data_dir: &Path) -> serde_json::Value {
    let path = app_data_dir
        .join(OBSERVATIONS_DIR_NAME)
        .join(LINKED_CONTEXTS_FILE_NAME);
    let bytes = fs::read(&path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    serde_json::from_slice(&bytes).unwrap_or_else(|err| panic!("parse {}: {err}", path.display()))
}

fn tree_entries(root: &Path) -> BTreeSet<PathBuf> {
    fn walk(dir: &Path, entries: &mut BTreeSet<PathBuf>) {
        for entry in fs::read_dir(dir).unwrap_or_else(|err| panic!("read {}: {err}", dir.display()))
        {
            let entry = entry.expect("read directory entry");
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).expect("read directory entry metadata");
            entries.insert(path.clone());
            if metadata.is_dir() && !metadata.file_type().is_symlink() {
                walk(&path, entries);
            }
        }
    }

    let mut entries = BTreeSet::new();
    walk(root, &mut entries);
    entries
}

fn write_private_ready_marker(path: &Path) {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap_or_else(|err| panic!("create ready marker {}: {err}", path.display()));
    file.write_all(b"ready\n")
        .unwrap_or_else(|err| panic!("write ready marker {}: {err}", path.display()));
    file.sync_all()
        .unwrap_or_else(|err| panic!("sync ready marker {}: {err}", path.display()));
    drop(file);
    set_mode(path, 0o600);
}

fn wait_for_path(path: &Path, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if path.exists() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn kill_and_reap_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn wait_for_child_exit(child: &mut Child, timeout: Duration) -> std::process::ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return status,
            Ok(None) => {}
            Err(err) => panic!("poll lock worker: {err}"),
        }
        if Instant::now() >= deadline {
            kill_and_reap_child(child);
            panic!("lock worker did not exit within {timeout:?}");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(unix)]
struct LockWorkerChild {
    name: &'static str,
    child: Child,
    exited: bool,
}

#[cfg(unix)]
impl LockWorkerChild {
    fn spawn(name: &'static str, app_data: &Path, context: &Path, ready: &Path) -> Self {
        let child = Command::new(env::current_exe().expect("resolve current test binary"))
            .arg("--test-threads=1")
            .arg(LOCK_WORKER_FILTER)
            .env(LOCK_WORKER_FLAG_ENV, "1")
            .env(LOCK_WORKER_APP_DATA_ENV, app_data)
            .env(LOCK_WORKER_CONTEXT_ENV, context)
            .env(LOCK_WORKER_READY_ENV, ready)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap_or_else(|err| panic!("spawn {name}: {err}"));
        Self {
            name,
            child,
            exited: false,
        }
    }

    fn assert_still_pending(&mut self) {
        match self.child.try_wait() {
            Ok(Some(status)) => {
                self.exited = true;
                panic!(
                    "{} exited with {status} while the registry lock was held",
                    self.name
                );
            }
            Ok(None) => {}
            Err(err) => panic!("poll {}: {err}", self.name),
        }
    }

    fn wait_success(&mut self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) => {
                    self.exited = true;
                    assert!(status.success(), "{} failed with {status}", self.name);
                    return;
                }
                Ok(None) => {}
                Err(err) => panic!("poll {}: {err}", self.name),
            }
            if Instant::now() >= deadline {
                panic!("{} did not exit within {timeout:?}", self.name);
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

#[cfg(unix)]
impl Drop for LockWorkerChild {
    fn drop(&mut self) {
        if !self.exited {
            kill_and_reap_child(&mut self.child);
        }
    }
}

#[test]
fn inventory_missing_stores_and_app_data_creates_nothing() {
    let tree = TestTree::new("missing");
    let missing = tree.join("missing");
    let registry = ObservationSourceRegistry::new(ObservationSourceConfig {
        runtime_state_dir: None,
        registry_dir: missing.join("registry"),
        plans_dir: missing.join("plans"),
        authority_dir: missing.join("authority"),
        sessions_dir: missing.join("sessions"),
        app_data_dir: missing.join("app-data"),
    });

    let inventory = registry.inventory();

    assert_eq!(inventory.sources.len(), 1);
    assert!(inventory.specs.is_empty());
    assert_eq!(
        inventory.sources[0].availability,
        ObservationSourceAvailability::Unavailable
    );
    assert!(!inventory.warnings.is_empty());
    assert!(tree_entries(tree.path()).is_empty());
}

#[test]
fn read_only_inventory_with_existing_app_data_creates_no_observations_or_lock() {
    let tree = TestTree::new("inventory-readonly");
    let app_data = tree.dir("app-data");
    let missing = tree.join("missing-default");
    let registry = ObservationSourceRegistry::new(ObservationSourceConfig {
        runtime_state_dir: None,
        registry_dir: missing.join("registry"),
        plans_dir: missing.join("plans"),
        authority_dir: missing.join("authority"),
        sessions_dir: missing.join("sessions"),
        app_data_dir: app_data.clone(),
    });

    let inventory = registry.inventory();

    assert_eq!(inventory.sources.len(), 1);
    assert!(inventory.specs.is_empty());
    assert!(!inventory.warnings.is_empty());
    let observations = app_data.join(OBSERVATIONS_DIR_NAME);
    assert!(!observations.exists());
    assert!(!observations.join(LINKED_CONTEXTS_LOCK_FILE_NAME).exists());
    assert!(!observations.join(LINKED_CONTEXTS_FILE_NAME).exists());
    assert!(tree_entries(&app_data).is_empty());
}

#[test]
fn valid_linked_context_is_available_inventory_and_persisted() {
    let tree = TestTree::new("link");
    let stores = Stores::new(&tree, "stores");
    let app_data = tree.dir("app-data");
    let project = tree.dir("project");
    tree.dir("contexts");
    let (context, _) = tree.write_json(
        "contexts/linked.json",
        &context_value(
            &project,
            "binding-linked",
            "11111111-1111-4111-8111-111111111111",
            &stores,
        ),
    );
    let missing = tree.join("missing-default");
    let registry = ObservationSourceRegistry::new(ObservationSourceConfig {
        runtime_state_dir: None,
        registry_dir: missing.join("registry"),
        plans_dir: missing.join("plans"),
        authority_dir: missing.join("authority"),
        sessions_dir: missing.join("sessions"),
        app_data_dir: app_data.clone(),
    });

    let source = registry.link_context(&context).expect("link valid context");

    assert!(source.id.starts_with("src_"));
    assert_eq!(source.label, LINKED_SOURCE_LABEL);
    assert_eq!(
        source.project_path.as_deref(),
        Some(path_text(&project).as_str())
    );
    assert_eq!(
        source.availability,
        ObservationSourceAvailability::Available
    );

    let inventory = registry.inventory();
    assert_eq!(inventory.specs.len(), 1);
    let spec = &inventory.specs[0];
    assert_eq!(spec.source.id, source.id);
    assert_eq!(
        spec.source.availability,
        ObservationSourceAvailability::Available
    );
    assert_eq!(spec.project_root_hint.as_deref(), Some(project.as_path()));
    assert_eq!(spec.binding_id_hint.as_deref(), Some("binding-linked"));
    assert_eq!(
        spec.thread_uuid_hint.as_deref(),
        Some("11111111-1111-4111-8111-111111111111")
    );
    assert_eq!(spec.runtime_state_dirs, vec![stores.state.clone()]);
    assert!(inventory
        .sources
        .iter()
        .any(|candidate| candidate.id == source.id));

    let persisted = persisted_registry(&app_data);
    assert_eq!(persisted["schema_version"].as_u64(), Some(1));
    let links = persisted["links"].as_array().expect("links array");
    assert_eq!(links.len(), 1);
    assert_eq!(links[0]["source_id"].as_str(), Some(source.id.as_str()));
    assert_eq!(
        links[0]["context_path"].as_str(),
        Some(path_text(&context).as_str())
    );

    let observations = app_data.join(OBSERVATIONS_DIR_NAME);
    assert_eq!(
        fs::metadata(&observations).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(observations.join(LINKED_CONTEXTS_FILE_NAME))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let lock_path = observations.join(LINKED_CONTEXTS_LOCK_FILE_NAME);
    assert!(lock_path.is_file());
    assert_eq!(
        fs::metadata(&lock_path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[test]
fn linked_context_matching_default_stores_deduplicates_and_relink_is_idempotent() {
    let tree = TestTree::new("dedup");
    let stores = Stores::new(&tree, "stores");
    let app_data = tree.dir("app-data");
    let project = tree.dir("project");
    tree.dir("contexts");
    let (context, _) = tree.write_json(
        "contexts/shared.json",
        &context_value(
            &project,
            "binding-shared",
            "22222222-2222-4222-8222-222222222222",
            &stores,
        ),
    );
    let registry = ObservationSourceRegistry::new(config(&app_data, &stores));

    let before = registry.inventory();
    assert_eq!(before.sources.len(), 1);
    assert_eq!(
        before.sources[0].availability,
        ObservationSourceAvailability::Available
    );
    assert!(before.sources[0].project_path.is_none());

    let first = registry.link_context(&context).expect("first link");
    let after_first = registry.inventory();
    assert_eq!(after_first.sources.len(), 1);
    assert_eq!(after_first.specs.len(), 1);
    assert_eq!(after_first.sources[0].id, first.id);
    assert_eq!(after_first.specs[0].source.id, first.id);
    assert_eq!(
        after_first.specs[0].project_root_hint.as_deref(),
        Some(project.as_path())
    );

    let second = registry.link_context(&context).expect("duplicate link");
    let after_second = registry.inventory();
    assert_eq!(second.id, first.id);
    assert_eq!(after_second.sources.len(), 1);

    let persisted = persisted_registry(&app_data);
    let links = persisted["links"].as_array().expect("links array");
    assert_eq!(links.len(), 1);
    assert_eq!(
        links
            .iter()
            .filter(|entry| {
                entry["context_path"].as_str() == Some(path_text(&context).as_str())
            })
            .count(),
        1
    );
}

#[test]
fn missing_linked_context_is_unavailable_while_healthy_source_stays_available() {
    let tree = TestTree::new("recovery");
    let stores_a = Stores::new(&tree, "a");
    let stores_b = Stores::new(&tree, "b");
    let app_data = tree.dir("app-data");
    let project_a = tree.dir("project-a");
    let project_b = tree.dir("project-b");
    tree.dir("contexts");
    let (context_a, _) = tree.write_json(
        "contexts/a.json",
        &context_value(
            &project_a,
            "binding-a",
            "33333333-3333-4333-8333-333333333333",
            &stores_a,
        ),
    );
    let (context_b, _) = tree.write_json(
        "contexts/b.json",
        &context_value(
            &project_b,
            "binding-b",
            "44444444-4444-4444-8444-444444444444",
            &stores_b,
        ),
    );
    let missing = tree.join("missing-default");
    let registry = ObservationSourceRegistry::new(ObservationSourceConfig {
        runtime_state_dir: None,
        registry_dir: missing.join("registry"),
        plans_dir: missing.join("plans"),
        authority_dir: missing.join("authority"),
        sessions_dir: missing.join("sessions"),
        app_data_dir: app_data,
    });

    let source_a = registry.link_context(&context_a).expect("link source a");
    let source_b = registry.link_context(&context_b).expect("link source b");
    assert_ne!(source_a.id, source_b.id);

    let healthy = registry.inventory();
    assert_eq!(
        healthy
            .sources
            .iter()
            .find(|source| source.id == source_a.id)
            .expect("source a present")
            .availability,
        ObservationSourceAvailability::Available
    );
    assert_eq!(
        healthy
            .sources
            .iter()
            .find(|source| source.id == source_b.id)
            .expect("source b present")
            .availability,
        ObservationSourceAvailability::Available
    );
    assert_eq!(healthy.specs.len(), 2);

    fs::remove_file(&context_a).expect("remove linked context a");
    let recovered = registry.inventory();
    let missing_source = recovered
        .sources
        .iter()
        .find(|source| source.id == source_a.id)
        .expect("missing source present");
    assert_eq!(
        missing_source.availability,
        ObservationSourceAvailability::Unavailable
    );
    assert_eq!(missing_source.label, LINKED_SOURCE_LABEL);
    let healthy_source = recovered
        .sources
        .iter()
        .find(|source| source.id == source_b.id)
        .expect("healthy source present");
    assert_eq!(
        healthy_source.availability,
        ObservationSourceAvailability::Available
    );
    assert_eq!(recovered.specs.len(), 1);
    assert_eq!(recovered.specs[0].source.id, source_b.id);
}

#[test]
fn rejects_world_readable_context_and_context_symlink_without_registry() {
    let tree = TestTree::new("reject");
    let stores = Stores::new(&tree, "stores");
    let app_data = tree.dir("app-data");
    let project = tree.dir("project");
    tree.dir("contexts");
    let value = context_value(
        &project,
        "binding-reject",
        "55555555-5555-4555-8555-555555555555",
        &stores,
    );
    let (context, _) = tree.write_json("contexts/world-readable.json", &value);
    set_mode(&context, 0o644);
    let registry = ObservationSourceRegistry::new(config(&app_data, &stores));

    assert_eq!(
        registry.link_context(&context).unwrap_err(),
        ObservationSourceError::InaccessibleContextFile
    );
    assert!(!app_data.join(OBSERVATIONS_DIR_NAME).exists());

    let (target, _) = tree.write_json("contexts/target.json", &value);
    let link = tree.join("contexts/link.json");
    symlink(&target, &link).expect("create context symlink");
    assert_eq!(
        registry.link_context(&link).unwrap_err(),
        ObservationSourceError::InvalidContextPath
    );
    assert!(!app_data.join(OBSERVATIONS_DIR_NAME).exists());
    assert!(!app_data
        .join(OBSERVATIONS_DIR_NAME)
        .join(LINKED_CONTEXTS_FILE_NAME)
        .exists());
}

#[test]
fn ignores_cli_package_metadata_without_executing_fixture_or_mutating_context() {
    let tree = TestTree::new("cli-package");
    let stores = Stores::new(&tree, "stores");
    let app_data = tree.dir("app-data");
    let project = tree.dir("project");
    let tools = tree.dir("tools");
    let marker = tools.join("marker");
    let script = tools.join("fixture-cli.sh");
    let script_body = format!("#!/bin/sh\n: > '{}'\n", marker.display());
    tree.write_file("tools/fixture-cli.sh", script_body.as_bytes(), 0o755);
    tree.dir("contexts");

    let mut value = context_value(
        &project,
        "binding-cli",
        "66666666-6666-4666-8666-666666666666",
        &stores,
    );
    value["cli_package"] = serde_json::json!({
        "name": "fixture-cli",
        "executable": path_text(&script),
    });
    let (context, _) = tree.write_json("contexts/cli.json", &value);
    let original_bytes = fs::read(&context).expect("capture context bytes");
    let registry = ObservationSourceRegistry::new(config(&app_data, &stores));

    let source = registry
        .link_context(&context)
        .expect("link context with cli_package metadata");

    assert_eq!(
        source.availability,
        ObservationSourceAvailability::Available
    );
    assert!(!marker.exists(), "cli_package fixture was executed");
    assert_eq!(
        fs::read(&context).expect("read context after link"),
        original_bytes
    );
    assert!(app_data
        .join(OBSERVATIONS_DIR_NAME)
        .join(LINKED_CONTEXTS_FILE_NAME)
        .is_file());
}

#[test]
fn ambiguous_linked_hints_aggregate_to_none_for_single_source() {
    let tree = TestTree::new("ambiguous");
    let stores = Stores::new(&tree, "stores");
    let app_data = tree.dir("app-data");
    let project_a = tree.dir("project-a");
    let project_b = tree.dir("project-b");
    tree.dir("contexts");
    let (context_a, _) = tree.write_json(
        "contexts/a.json",
        &context_value(
            &project_a,
            "binding-alpha",
            "77777777-7777-4777-8777-777777777777",
            &stores,
        ),
    );
    let (context_b, _) = tree.write_json(
        "contexts/b.json",
        &context_value(
            &project_b,
            "binding-beta",
            "88888888-8888-4888-8888-888888888888",
            &stores,
        ),
    );
    let registry = ObservationSourceRegistry::new(config(&app_data, &stores));

    let first = registry
        .link_context(&context_a)
        .expect("link first ambiguous context");
    let second = registry
        .link_context(&context_b)
        .expect("link second ambiguous context");
    assert_eq!(first.id, second.id);

    let inventory = registry.inventory();
    assert_eq!(inventory.sources.len(), 1);
    assert_eq!(inventory.specs.len(), 1);
    let spec = &inventory.specs[0];
    assert_eq!(spec.source.id, first.id);
    assert!(spec.project_root_hint.is_none());
    assert!(spec.binding_id_hint.is_none());
    assert!(spec.thread_uuid_hint.is_none());
    assert!(spec.source.project_path.is_none());
    assert_eq!(
        persisted_registry(&app_data)["links"]
            .as_array()
            .expect("links array")
            .len(),
        2
    );
}

#[test]
fn duplicate_context_json_keys_are_rejected_without_registry() {
    let tree = TestTree::new("duplicate-keys");
    let stores = Stores::new(&tree, "stores");
    let app_data = tree.dir("app-data");
    let project = tree.dir("project");
    tree.dir("contexts");
    let value = context_value(
        &project,
        "binding-duplicate",
        "99999999-9999-4999-8999-999999999999",
        &stores,
    );
    let serialized = serde_json::to_string(&value).expect("serialize context");
    let duplicate = format!("{{\"schema_version\":1,{}", &serialized[1..]);
    let context = tree.write_file("contexts/duplicate.json", duplicate.as_bytes(), 0o600);
    let registry = ObservationSourceRegistry::new(config(&app_data, &stores));

    assert_eq!(
        registry.link_context(&context).unwrap_err(),
        ObservationSourceError::InvalidContextFormat
    );
    assert!(!app_data.join(OBSERVATIONS_DIR_NAME).exists());
    assert!(!app_data
        .join(OBSERVATIONS_DIR_NAME)
        .join(LINKED_CONTEXTS_FILE_NAME)
        .exists());
}

#[test]
fn cross_process_registry_lock_serializes_writers() {
    let tree = TestTree::new("lock-cross-process");
    let stores = Stores::new(&tree, "stores");
    let app_data = tree.dir("app-data");
    let project = tree.dir("project");
    tree.dir("contexts");
    let (context, _) = tree.write_json(
        "contexts/locked.json",
        &context_value(
            &project,
            "binding-lock",
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            &stores,
        ),
    );
    let observations = app_data.join(OBSERVATIONS_DIR_NAME);
    ensure_observations_dir(&observations).expect("create observations directory");
    let held_lock =
        acquire_linked_registry_lock(&observations).expect("acquire parent registry lock");
    let ready = tree.join("worker-ready");

    let mut child = Command::new(env::current_exe().expect("resolve current test binary"))
        .arg("--test-threads=1")
        .arg(LOCK_WORKER_FILTER)
        .env(LOCK_WORKER_FLAG_ENV, "1")
        .env(LOCK_WORKER_APP_DATA_ENV, &app_data)
        .env(LOCK_WORKER_CONTEXT_ENV, &context)
        .env(LOCK_WORKER_READY_ENV, &ready)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn lock worker");

    if !wait_for_path(&ready, LOCK_WORKER_TIMEOUT) {
        kill_and_reap_child(&mut child);
        panic!("lock worker did not reach link_context before timeout");
    }
    thread::sleep(LOCK_BLOCK_CONFIRMATION);
    assert!(
        child.try_wait().expect("poll lock worker").is_none(),
        "lock worker completed while the registry lock was held"
    );

    drop(held_lock);

    let status = wait_for_child_exit(&mut child, LOCK_WORKER_TIMEOUT);
    assert!(status.success(), "lock worker failed with {status}");

    let persisted = persisted_registry(&app_data);
    let links = persisted["links"].as_array().expect("links array");
    assert_eq!(links.len(), 1);
    assert_eq!(
        links[0]["context_path"].as_str(),
        Some(path_text(&context).as_str())
    );
    assert!(observations.join(LINKED_CONTEXTS_LOCK_FILE_NAME).is_file());
}

#[cfg(unix)]
#[test]
fn cross_process_registry_lock_preserves_two_distinct_linked_contexts() {
    let tree = TestTree::new("lock-cross-process-two");
    let stores_a = Stores::new(&tree, "stores-a");
    let stores_b = Stores::new(&tree, "stores-b");
    let app_data = tree.dir("app-data");
    let project_a = tree.dir("project-a");
    let project_b = tree.dir("project-b");
    tree.dir("contexts");
    let (context_a, _) = tree.write_json(
        "contexts/a.json",
        &context_value(
            &project_a,
            "binding-cross-a",
            "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            &stores_a,
        ),
    );
    let (context_b, _) = tree.write_json(
        "contexts/b.json",
        &context_value(
            &project_b,
            "binding-cross-b",
            "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
            &stores_b,
        ),
    );
    assert_ne!(context_a, context_b);

    let observations = app_data.join(OBSERVATIONS_DIR_NAME);
    ensure_observations_dir(&observations).expect("create observations directory");
    let held_lock =
        acquire_linked_registry_lock(&observations).expect("acquire parent registry lock");
    let ready_a = tree.join("worker-ready-a");
    let ready_b = tree.join("worker-ready-b");

    let mut worker_a = LockWorkerChild::spawn("worker a", &app_data, &context_a, &ready_a);
    let mut worker_b = LockWorkerChild::spawn("worker b", &app_data, &context_b, &ready_b);

    let ready_deadline = Instant::now() + LOCK_WORKER_TIMEOUT;
    let remaining = ready_deadline.saturating_duration_since(Instant::now());
    assert!(
        wait_for_path(&ready_a, remaining),
        "worker a did not reach link_context before timeout"
    );
    let remaining = ready_deadline.saturating_duration_since(Instant::now());
    assert!(
        wait_for_path(&ready_b, remaining),
        "worker b did not reach link_context before timeout"
    );

    worker_a.assert_still_pending();
    worker_b.assert_still_pending();

    drop(held_lock);

    worker_a.wait_success(LOCK_WORKER_TIMEOUT);
    worker_b.wait_success(LOCK_WORKER_TIMEOUT);

    let persisted = persisted_registry(&app_data);
    let links = persisted["links"].as_array().expect("links array");
    assert_eq!(links.len(), 2);

    let expected_contexts = BTreeSet::from([path_text(&context_a), path_text(&context_b)]);
    let persisted_contexts: BTreeSet<String> = links
        .iter()
        .map(|link| {
            link["context_path"]
                .as_str()
                .expect("persisted context path")
                .to_owned()
        })
        .collect();
    assert_eq!(persisted_contexts, expected_contexts);

    let source_ids: BTreeSet<&str> = links
        .iter()
        .map(|link| link["source_id"].as_str().expect("persisted source id"))
        .collect();
    assert_eq!(source_ids.len(), 2, "linked source ids must be distinct");
}

#[test]
fn linked_context_lock_worker() {
    if env::var_os(LOCK_WORKER_FLAG_ENV).is_none() {
        return;
    }

    let app_data = PathBuf::from(
        env::var_os(LOCK_WORKER_APP_DATA_ENV).expect("lock worker app data directory"),
    );
    let context =
        PathBuf::from(env::var_os(LOCK_WORKER_CONTEXT_ENV).expect("lock worker context path"));
    let ready =
        PathBuf::from(env::var_os(LOCK_WORKER_READY_ENV).expect("lock worker ready marker path"));
    let missing = app_data.join("missing-default");
    let registry = ObservationSourceRegistry::new(ObservationSourceConfig {
        runtime_state_dir: None,
        registry_dir: missing.join("registry"),
        plans_dir: missing.join("plans"),
        authority_dir: missing.join("authority"),
        sessions_dir: missing.join("sessions"),
        app_data_dir: app_data,
    });

    write_private_ready_marker(&ready);
    let source = registry
        .link_context(&context)
        .expect("worker link context should succeed after lock release");
    assert_eq!(
        source.availability,
        ObservationSourceAvailability::Available
    );
}
