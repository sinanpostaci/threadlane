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

    let expected_source_id =
        compute_observation_source_id(&stores.plans, &stores.authority, &stores.sessions)
            .expect("canonical source id");
    assert_eq!(source.id, expected_source_id);
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
    assert_eq!(
        source_a.id,
        compute_observation_source_id(&stores_a.plans, &stores_a.authority, &stores_a.sessions)
            .expect("canonical source id a")
    );
    assert_eq!(
        source_b.id,
        compute_observation_source_id(&stores_b.plans, &stores_b.authority, &stores_b.sessions)
            .expect("canonical source id b")
    );
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

#[cfg(unix)]
mod coworker_projection_tests {
    use super::*;
    use crate::observation::ObservationService;
    use threadlane_core::observation::{
        append_journal_record, ObservationCallState, ObservationJobState, ObservationJournalRecord,
        ObservationProcessInstance, ObservationRecordPhase, ObservationUsage,
    };
    use threadlane_native::coworker_observation::{
        begin_coworker_call_observation, CoworkerCallFinishSpec, CoworkerCallObservationSpec,
        CoworkerObservationExecutorSpec, COWORKER_OBS_OP_PLANNING_ORCHESTRATOR,
        COWORKER_OBS_OP_REVIEW_TASK,
    };
    use threadlane_native::coworker_workflow::{
        AcceptedPredecessorReceiptRef, CandidateCheckEvidence, CanonicalProjectBaseRef,
        CombinedCandidateIdentity, CombinedCandidateStatus, CoworkerBindingLedger,
        CoworkerJobPhase, CoworkerJobRecord, CoworkerTaskRecord, CoworkerTaskStage,
        CoworkerWorkflowStore, FrozenApprovalLinkage, ReviewVerdictOutcome,
        StructuredReviewVerdict, TaskCandidateRef, WorkBrief, WorkBriefKind, WorkflowHoldReason,
        WorkflowHoldRecord, COWORKER_WORKFLOW_SCHEMA_VERSION,
    };
    use threadlane_native::profile_flow::{RoleExecutionRoute, SelectedRoleChoice};
    use threadlane_plans::{
        Difficulty, PlanRevision, PlanStatus, PlanTask, ToolAdapter, WorkflowRole,
        PLAN_STORE_SCHEMA_VERSION,
    };

    fn sample_choice(_role: WorkflowRole) -> SelectedRoleChoice {
        SelectedRoleChoice {
            matched_difficulty: Some(Difficulty::Medium),
            tool_id: "tool-opencode".to_string(),
            adapter: ToolAdapter::OpencodeV2,
            route: RoleExecutionRoute::HeadlessWorker,
            model_id: "model-flash".to_string(),
            model_arg: "opencode-go/deepseek-v4.1-flash".to_string(),
            effort: Some("max".to_string()),
        }
    }

    fn sample_project_base(project_dir: &Path) -> CanonicalProjectBaseRef {
        CanonicalProjectBaseRef {
            repository_path: path_text(project_dir),
            base_commit: "1111111111111111111111111111111111111111".to_string(),
            base_tree_sha: "2222222222222222222222222222222222222222".to_string(),
        }
    }

    fn make_job_record(
        job_id: &str,
        request_key: &str,
        binding_id: &str,
        thread_uuid: &str,
        project_dir: &Path,
        summary: &str,
        phase: CoworkerJobPhase,
    ) -> CoworkerJobRecord {
        let work_brief = WorkBrief {
            brief_id: format!("brf_{job_id}"),
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            project_base: sample_project_base(project_dir),
            kind: WorkBriefKind::NewJob,
            summary: summary.to_string(),
            confirmed_requirements: vec!["Preserve exact scope and truthful telemetry.".to_string()],
            proposed_additions: Vec::new(),
            assumptions: Vec::new(),
            open_questions: Vec::new(),
            accepted_architect_decisions: Vec::new(),
        };
        let work_brief_digest = work_brief.digest().expect("brief digest");
        CoworkerJobRecord {
            job_id: job_id.to_string(),
            job_revision: 1,
            create_request_key: request_key.to_string(),
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            phase,
            work_brief,
            work_brief_digest,
            supersedes_job_id: None,
            planning_result: None,
            approval_linkage: None,
            tasks: Vec::new(),
            holds: Vec::new(),
            outbox: Vec::new(),
            combined_candidates: Vec::new(),
        }
    }

    fn make_approved_task(task_id: &str, stage: CoworkerTaskStage) -> CoworkerTaskRecord {
        let cand_id = format!("cnd_{task_id}");
        let mut accepted_candidate_id = None;
        let mut candidates = Vec::new();
        if matches!(
            stage,
            CoworkerTaskStage::Accepted | CoworkerTaskStage::Integrated
        ) {
            accepted_candidate_id = Some(cand_id.clone());
            candidates.push(TaskCandidateRef {
                candidate_id: cand_id.clone(),
                task_id: task_id.to_string(),
                attempt_no: 1,
                base_commit: "1111111111111111111111111111111111111111".to_string(),
                head_commit: "3333333333333333333333333333333333333333".to_string(),
                tree_sha: "4444444444444444444444444444444444444444".to_string(),
                diff_digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
                write_paths: vec!["src/lib.rs".to_string()],
                check_evidence_digests: vec![
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
                ],
                review_subject_digest: Some(
                    "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_string(),
                ),
                review_verdict: Some(StructuredReviewVerdict {
                    verdict_id: format!("vrd_{task_id}"),
                    task_id: task_id.to_string(),
                    attempt_no: 1,
                    candidate_id: cand_id,
                    base_commit: "1111111111111111111111111111111111111111".to_string(),
                    task_head_commit: "3333333333333333333333333333333333333333".to_string(),
                    task_tree_sha: "4444444444444444444444444444444444444444".to_string(),
                    diff_digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                    review_subject_digest:
                        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                            .to_string(),
                    policy_digest:
                        "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                            .to_string(),
                    outcome: ReviewVerdictOutcome::Accept,
                    findings: Vec::new(),
                    required_in_scope_fixes: Vec::new(),
                    conflicting_paths_or_interfaces: Vec::new(),
                    prose_summary: Some("Accepted.".to_string()),
                    prose_may_advance_acceptance: false,
                }),
                review_receipt_id: Some(format!("rev_{task_id}")),
                integration_receipt_id: (stage == CoworkerTaskStage::Integrated)
                    .then(|| format!("int_{task_id}")),
            });
        }

        CoworkerTaskRecord {
            task_id: task_id.to_string(),
            task_digest: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                .to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Medium,
            selected_choice: sample_choice(WorkflowRole::Coder),
            depends_on: Vec::new(),
            write_paths: vec!["src/lib.rs".to_string()],
            owned_interfaces: Vec::new(),
            consumed_interfaces: Vec::new(),
            stage,
            stage_before_hold: None,
            current_attempt_no: 1,
            active_candidate_id: accepted_candidate_id.clone(),
            accepted_candidate_id,
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
            attempts: Vec::new(),
            candidates,
            active_hold_ids: Vec::new(),
            unresolved_outbox_id: None,
        }
    }

    fn attach_approval_linkage(job: &mut CoworkerJobRecord, plan_id: &str, approval_id: &str) {
        job.approval_linkage = Some(FrozenApprovalLinkage {
            approval_id: approval_id.to_string(),
            approval_subject_digest:
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_string(),
            plan_id: plan_id.to_string(),
            plan_revision: 1,
            plan_digest: "9999999999999999999999999999999999999999999999999999999999999999"
                .to_string(),
            project_base: job.work_brief.project_base.clone(),
            profile_id: "prof-test".to_string(),
            registry_revision: 1,
            policy_id: "pol-test".to_string(),
            policy_digest: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                .to_string(),
            approved_task_ids: vec!["task-1".to_string()],
            approved_write_paths: vec!["src/lib.rs".to_string()],
            controller_envelope: None,
        });
    }

    fn write_binding_ledger(plans_dir: &Path, ledger: &CoworkerBindingLedger) -> PathBuf {
        ledger
            .validate(&ledger.binding_id)
            .expect("test CoworkerBindingLedger must pass strict validation");
        let store = CoworkerWorkflowStore::new(plans_dir).expect("open CoworkerWorkflowStore");
        let path = store
            .ledger_path_for_binding(&ledger.binding_id)
            .expect("resolve ledger path");
        let bytes = serde_json::to_vec_pretty(ledger).expect("serialize ledger");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&path)
            .expect("open ledger file");
        file.write_all(&bytes).expect("write ledger file");
        file.sync_all().expect("sync ledger file");
        drop(file);
        set_mode(&path, 0o600);
        path
    }

    #[test]
    fn coworker_workflow_projects_stages_dedupes_verified_plan_and_requires_closure_for_completion()
    {
        let tree = TestTree::new("coworker-stages");
        let stores = Stores::new(&tree, "stores");
        let app_data = tree.dir("app-data");
        let project = tree.dir("project-alpha");
        let binding_id = "bind-coworker-alpha";
        let thread_uuid = "12121212-1212-4212-8212-121212121212";

        // Write two legacy plans in `plans.json` with the exact same summary title:
        // - `plan-linked` is bound to `job_awaiting` and must be deduplicated into `job_awaiting`.
        // - `plan-unrelated-same-title` has the same title but a distinct `plan_id` and must NOT be deduplicated.
        let plan_linked = PlanRevision {
            plan_id: "plan-linked".to_string(),
            revision: 1,
            binding_id: binding_id.to_string(),
            profile_id: "prof-test".to_string(),
            profile_revision: 1,
            status: PlanStatus::Published,
            summary: "Shared Title For Dedupe Verification".to_string(),
            tasks: vec![PlanTask {
                id: "task-1".to_string(),
                title: "Implement feature".to_string(),
                instruction: "Implement within scope.".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Medium,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            }],
            created_at_ms: 1_700_000_001_000,
        };
        let plan_unrelated = PlanRevision {
            plan_id: "plan-unrelated-same-title".to_string(),
            ..plan_linked.clone()
        };
        let plans_envelope = serde_json::json!({
            "schema_version": PLAN_STORE_SCHEMA_VERSION,
            "revisions": [plan_linked, plan_unrelated],
        });
        let plans_bytes = serde_json::to_vec_pretty(&plans_envelope).expect("serialize plans");
        tree.write_file("stores/plans/plans.json", &plans_bytes, 0o600);

        let job_planning = make_job_record(
            "job_planning_01",
            "req_planning_01",
            binding_id,
            thread_uuid,
            &project,
            "Drafting technical plan",
            CoworkerJobPhase::Planning,
        );

        let mut job_awaiting = make_job_record(
            "job_awaiting_02",
            "req_awaiting_02",
            binding_id,
            thread_uuid,
            &project,
            "Shared Title For Dedupe Verification",
            CoworkerJobPhase::AwaitingApproval,
        );
        // Bind `plan-linked` on `job_awaiting` via approval_linkage (or planning_result);
        // here we test awaiting approval first, then executing/reviewing/held/completed on job_exec.
        let mut job_running = make_job_record(
            "job_running_03",
            "req_running_03",
            binding_id,
            thread_uuid,
            &project,
            "Active execution job",
            CoworkerJobPhase::Executing,
        );
        attach_approval_linkage(&mut job_running, "plan-linked", "apr_running_03");
        job_running.tasks = vec![make_approved_task("task-1", CoworkerTaskStage::Running)];

        let mut job_reviewing = make_job_record(
            "job_reviewing_04",
            "req_reviewing_04",
            binding_id,
            thread_uuid,
            &project,
            "Under review job",
            CoworkerJobPhase::Executing,
        );
        attach_approval_linkage(&mut job_reviewing, "plan-review", "apr_reviewing_04");
        job_reviewing.tasks = vec![make_approved_task(
            "task-1",
            CoworkerTaskStage::ReadyForReview,
        )];

        let mut job_needs_user = make_job_record(
            "job_held_05",
            "req_held_05",
            binding_id,
            thread_uuid,
            &project,
            "Held awaiting user clarification",
            CoworkerJobPhase::Held,
        );
        attach_approval_linkage(&mut job_needs_user, "plan-held", "apr_held_05");
        job_needs_user.tasks = vec![make_approved_task("task-1", CoworkerTaskStage::Held)];
        job_needs_user.holds.push(WorkflowHoldRecord {
            hold_id: "hld_05".to_string(),
            request_key: "req_hld_05".to_string(),
            reason: WorkflowHoldReason::ConversationClarificationPending,
            detail: "Awaiting user clarification.".to_string(),
            directly_affected_task_ids: vec!["task-1".to_string()],
            affected_write_paths: vec!["src/lib.rs".to_string()],
            affected_interfaces: Vec::new(),
            holds_entire_job: true,
            active: true,
            created_at_job_revision: 1,
            resolved_at_job_revision: None,
            resolution_note: None,
        });

        // A job where all tasks are `Integrated` but `phase` is still `Executing` (no Closure proof)
        // must NOT project as `Completed`.
        let mut job_integrated_not_closed = make_job_record(
            "job_integrated_no_closure_06",
            "req_integrated_06",
            binding_id,
            thread_uuid,
            &project,
            "Integrated tasks awaiting closure",
            CoworkerJobPhase::Executing,
        );
        attach_approval_linkage(
            &mut job_integrated_not_closed,
            "plan-integrated",
            "apr_integrated_06",
        );
        job_integrated_not_closed.tasks =
            vec![make_approved_task("task-1", CoworkerTaskStage::Integrated)];

        // A job in `CoworkerJobPhase::Completed` WITH a verified `ClosureAccepted` combined candidate.
        let mut job_completed = make_job_record(
            "job_completed_07",
            "req_completed_07",
            binding_id,
            thread_uuid,
            &project,
            "Completed with closure acceptance",
            CoworkerJobPhase::Completed,
        );
        attach_approval_linkage(&mut job_completed, "plan-completed", "apr_completed_07");
        job_completed.tasks = vec![make_approved_task("task-1", CoworkerTaskStage::Integrated)];
        let mut combined = CombinedCandidateIdentity {
            combined_candidate_id: "cmb_completed_07".to_string(),
            request_key: "req_cmb_07".to_string(),
            job_id: job_completed.job_id.clone(),
            job_revision: 1,
            project_base: job_completed.work_brief.project_base.clone(),
            final_commit_sha: Some("5555555555555555555555555555555555555555".to_string()),
            final_tree_sha: "6666666666666666666666666666666666666666".to_string(),
            plan_id: "plan-completed".to_string(),
            plan_revision: 1,
            plan_digest: "9999999999999999999999999999999999999999999999999999999999999999"
                .to_string(),
            approval_id: "apr_completed_07".to_string(),
            approval_subject_digest:
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_string(),
            scope_policy_revision: 1,
            policy_id: "pol-test".to_string(),
            policy_digest: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                .to_string(),
            accepted_predecessor_receipts: vec![AcceptedPredecessorReceiptRef {
                task_id: "task-1".to_string(),
                candidate_id: "cnd_task-1".to_string(),
                head_commit: "3333333333333333333333333333333333333333".to_string(),
                tree_sha: "4444444444444444444444444444444444444444".to_string(),
                review_subject_digest:
                    "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_string(),
                review_receipt_id: "rev_task-1".to_string(),
                integration_receipt_id: "int_task-1".to_string(),
            }],
            check_evidence: vec![CandidateCheckEvidence {
                check_id: "cargo_test".to_string(),
                command_summary: "cargo test".to_string(),
                passed: true,
                exit_code: 0,
                evidence_digest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_string(),
            }],
            candidate_digest: String::new(),
            closure_reservation_id: Some("cls_completed_07".to_string()),
            status: CombinedCandidateStatus::ClosureAccepted,
        };
        combined.candidate_digest = combined.compute_candidate_digest().expect("cmb digest");
        job_completed.combined_candidates.push(combined);

        // Give `job_awaiting` no approval_linkage so it projects `AwaitingApproval` -> `NeedsUser` + `pending_approval`.
        job_awaiting.phase = CoworkerJobPhase::AwaitingApproval;

        let ledger = CoworkerBindingLedger {
            schema_version: COWORKER_WORKFLOW_SCHEMA_VERSION,
            binding_id: binding_id.to_string(),
            thread_uuid: Some(thread_uuid.to_string()),
            ledger_revision: 1,
            planning_grants: Vec::new(),
            architect_advice: Vec::new(),
            jobs: vec![
                job_planning,
                job_awaiting,
                job_running,
                job_reviewing,
                job_needs_user,
                job_integrated_not_closed,
                job_completed,
            ],
            admission_markers: std::collections::BTreeMap::new(),
            external_resolutions: Vec::new(),
        };
        write_binding_ledger(&stores.plans, &ledger);

        let service = ObservationService::new(config(&app_data, &stores));
        let snapshot = service.snapshot(None).expect("snapshot");

        let by_id: std::collections::BTreeMap<&str, &threadlane_core::observation::ObservationJob> =
            snapshot.jobs.iter().map(|j| (j.id.as_str(), j)).collect();

        assert_eq!(
            by_id["job_planning_01"].state,
            ObservationJobState::Planning
        );
        assert_eq!(by_id["job_planning_01"].approval_status, "draft");

        assert_eq!(
            by_id["job_awaiting_02"].state,
            ObservationJobState::NeedsUser
        );
        assert_eq!(by_id["job_awaiting_02"].approval_status, "pending_approval");

        assert_eq!(by_id["job_running_03"].state, ObservationJobState::Running);
        assert_eq!(by_id["job_running_03"].execution_status, "running");

        assert_eq!(
            by_id["job_reviewing_04"].state,
            ObservationJobState::Reviewing
        );
        assert_eq!(
            by_id["job_reviewing_04"].execution_status,
            "ready_for_review"
        );

        assert_eq!(by_id["job_held_05"].state, ObservationJobState::NeedsUser);
        assert_eq!(by_id["job_held_05"].execution_status, "held");

        // All tasks integrated without Closure proof must stay Reviewing, never Completed.
        assert_eq!(
            by_id["job_integrated_no_closure_06"].state,
            ObservationJobState::Reviewing
        );
        assert_ne!(
            by_id["job_integrated_no_closure_06"].state,
            ObservationJobState::Completed
        );

        // Plausible ledger receipt IDs without store-backed ControllerJobRuntimeState,
        // FinalCandidateClosureStore verdict, and task receipts must fail closed to Unknown.
        assert_eq!(
            by_id["job_completed_07"].state,
            ObservationJobState::Unknown
        );
        assert_eq!(by_id["job_completed_07"].approval_status, "unknown");
        assert_eq!(by_id["job_completed_07"].execution_status, "unknown");
        assert_eq!(
            by_id["job_completed_07"].review_state.as_deref(),
            Some("unknown")
        );

        // Same-title unrelated legacy plan is preserved (`plan-unrelated-same-title`).
        assert!(by_id.contains_key("plan-unrelated-same-title"));
    }

    #[test]
    fn coworker_workflow_requests_bind_verified_scope_and_degrade_dead_process_or_forged_hints() {
        let tree = TestTree::new("coworker-requests");
        let stores = Stores::new(&tree, "stores");
        let app_data = tree.dir("app-data");
        let project = tree.dir("project-beta");
        let binding_id = "bind-coworker-beta";
        let thread_uuid = "34343434-3434-4434-8434-343434343434";

        let mut job_active = make_job_record(
            "job_active_beta",
            "req_active_beta",
            binding_id,
            thread_uuid,
            &project,
            "Active beta job",
            CoworkerJobPhase::Executing,
        );
        attach_approval_linkage(&mut job_active, "plan-beta", "apr_beta");
        job_active.tasks = vec![make_approved_task("task-1", CoworkerTaskStage::UnderReview)];

        let ledger = CoworkerBindingLedger {
            schema_version: COWORKER_WORKFLOW_SCHEMA_VERSION,
            binding_id: binding_id.to_string(),
            thread_uuid: Some(thread_uuid.to_string()),
            ledger_revision: 1,
            planning_grants: Vec::new(),
            architect_advice: Vec::new(),
            jobs: vec![job_active],
            admission_markers: std::collections::BTreeMap::new(),
            external_resolutions: Vec::new(),
        };
        write_binding_ledger(&stores.plans, &ledger);

        // Also write a corrupt misnamed coworker ledger file to verify safe warning + skip.
        tree.write_file(
            "stores/plans/coworker-workflow-invalid-tag.json",
            b"{not json",
            0o600,
        );

        // 1. Record a real finished Reviewer call via `begin_coworker_call_observation` + `finish`.
        let handle = begin_coworker_call_observation(CoworkerCallObservationSpec {
            journal_dir: stores.plans.clone(),
            operation: COWORKER_OBS_OP_REVIEW_TASK.to_string(),
            title: "Task Reviewer: task-1".to_string(),
            call_key: "rev_call_01".to_string(),
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            repository_path: project.clone(),
            job_id: Some("job_active_beta".to_string()),
            plan_id: Some("plan-beta".to_string()),
            task_id: Some("task-1".to_string()),
            approval_id: Some("apr_beta".to_string()),
            attempt_no: Some(1),
            run_id: Some("run_rev_01".to_string()),
            worker_id: Some("wrk_rev_01".to_string()),
            candidate_id: Some("cnd_task-1".to_string()),
            executor: CoworkerObservationExecutorSpec::from_selected_role_choice(
                WorkflowRole::Reviewer,
                &sample_choice(WorkflowRole::Reviewer),
            ),
            verified_context: None,
            entry_summary: Some("stage=task_review".to_string()),
        });
        let rev_req_id = handle.request_id().to_string();
        let rev_call_id = handle.call_id().to_string();
        handle.finish(CoworkerCallFinishSpec {
            state: Some(ObservationCallState::Completed),
            observed_model: Some("opencode-go/deepseek-v4.1-flash".to_string()),
            observed_effort: Some("max".to_string()),
            // All-None usage must normalize to `None` (never fabricated 0).
            usage: Some(ObservationUsage {
                input_tokens: None,
                output_tokens: None,
                cache_read_tokens: None,
                total_tokens: None,
                provenance: "unreported".to_string(),
            }),
            receipt_id: Some("rev_task-1".to_string()),
            artifact_id: Some("trr_01".to_string()),
            diagnostic: Some("outcome=accept".to_string()),
            ..Default::default()
        });

        // 2. Append a forged cross-binding `coworker.*` record claiming `job_active_beta`.
        let forged_record = ObservationJournalRecord {
            record_id: "obsrec_forged_01".to_string(),
            request_id: "obsreq_forged_01".to_string(),
            parent_request_id: None,
            call_id: Some("obsc_forged_01".to_string()),
            phase: ObservationRecordPhase::Finish,
            at_ms: 1_700_000_010_000,
            pid: Some(12345),
            process_instance: None,
            project_label: Some("project-beta".to_string()),
            project_path: Some(path_text(&project)),
            binding_id: Some("bind-wrong-foreign".to_string()),
            thread_uuid: Some(thread_uuid.to_string()),
            thread_label: None,
            job_id: Some("job_active_beta".to_string()),
            plan_id: Some("plan-beta".to_string()),
            task_id: Some("task-1".to_string()),
            approval_id: Some("apr_beta".to_string()),
            operation: COWORKER_OBS_OP_PLANNING_ORCHESTRATOR.to_string(),
            title: "Forged call".to_string(),
            state: ObservationCallState::Completed,
            actor: None,
            executor: None,
            usage: None,
            delivery: Vec::new(),
            summary: Some("forged".to_string()),
        };
        append_journal_record(&stores.plans, &forged_record).expect("append forged record");

        // 3. Append an unfinished `Entry` record with a dead/reused `ObservationProcessInstance`.
        let crashed_entry = ObservationJournalRecord {
            record_id: "obsrec_crashed_01".to_string(),
            request_id: "obsreq_crashed_01".to_string(),
            parent_request_id: None,
            call_id: Some("obsc_crashed_01".to_string()),
            phase: ObservationRecordPhase::Entry,
            at_ms: 1_700_000_020_000,
            pid: Some(std::process::id()),
            process_instance: Some(ObservationProcessInstance {
                pid: std::process::id(),
                boot_id: "dead-boot-id-mismatch".to_string(),
                start_ticks: u64::MAX,
            }),
            project_label: Some("project-beta".to_string()),
            project_path: Some(path_text(&project)),
            binding_id: Some(binding_id.to_string()),
            thread_uuid: Some(thread_uuid.to_string()),
            thread_label: None,
            job_id: Some("job_active_beta".to_string()),
            plan_id: Some("plan-beta".to_string()),
            task_id: Some("task-1".to_string()),
            approval_id: Some("apr_beta".to_string()),
            operation: COWORKER_OBS_OP_REVIEW_TASK.to_string(),
            title: "Crashed Reviewer call".to_string(),
            state: ObservationCallState::Running,
            actor: None,
            executor: None,
            usage: None,
            delivery: Vec::new(),
            summary: Some("crashed".to_string()),
        };
        append_journal_record(&stores.plans, &crashed_entry).expect("append crashed entry");

        let service = ObservationService::new(config(&app_data, &stores));
        let snapshot = service.snapshot(None).expect("snapshot");

        let rev_req = snapshot
            .requests
            .iter()
            .find(|r| r.id == rev_req_id)
            .expect("verified reviewer request");
        assert_eq!(rev_req.job_id.as_deref(), Some("job_active_beta"));
        assert_eq!(rev_req.state, ObservationCallState::Completed);
        assert!(rev_req.usage.is_none(), "unreported usage must stay None");
        assert_eq!(rev_req.children.len(), 1);
        assert_eq!(rev_req.children[0].id, rev_call_id);
        let exec = rev_req.executor.as_ref().expect("executor");
        assert_eq!(exec.role.as_deref(), Some("reviewer"));
        assert_eq!(
            exec.requested_model.as_deref(),
            Some("opencode-go/deepseek-v4.1-flash")
        );
        assert_eq!(
            exec.observed_model.as_deref(),
            Some("opencode-go/deepseek-v4.1-flash")
        );

        let forged_req = snapshot
            .requests
            .iter()
            .find(|r| r.id == "obsreq_forged_01")
            .expect("forged request");
        assert_eq!(
            forged_req.job_id, None,
            "cross-binding forged hint must not attach to job"
        );

        let crashed_req = snapshot
            .requests
            .iter()
            .find(|r| r.id == "obsreq_crashed_01")
            .expect("crashed request");
        assert_eq!(
            crashed_req.state,
            ObservationCallState::Unknown,
            "unfinished Entry with dead/reused process_instance must degrade to Unknown"
        );
        assert_eq!(crashed_req.children[0].state, ObservationCallState::Unknown);

        let projected_job = snapshot
            .jobs
            .iter()
            .find(|j| j.id == "job_active_beta")
            .expect("projected job");
        assert_eq!(
            projected_job.state,
            ObservationJobState::Unknown,
            "job with dead in-flight call must degrade to Unknown instead of staying Running/Reviewing"
        );
        assert!(snapshot.warnings.iter().any(|w| w.contains(
            "Skipped unreadable or invalid coworker workflow ledger in observation source."
        )));
        assert!(snapshot.warnings.iter().any(|w| w
            .contains("Ignored unverified job, plan, task, or approval hint on journal record.")));
    }

    #[test]
    fn coworker_workflow_snapshot_is_strictly_readonly_and_stable_across_repeated_polls() {
        let tree = TestTree::new("coworker-readonly");
        let stores = Stores::new(&tree, "stores");
        let app_data = tree.dir("app-data");
        let project = tree.dir("project-gamma");
        let binding_id = "bind-coworker-gamma";
        let thread_uuid = "56565656-5656-4656-8656-565656565656";

        let job = make_job_record(
            "job_gamma_01",
            "req_gamma_01",
            binding_id,
            thread_uuid,
            &project,
            "Readonly stability test",
            CoworkerJobPhase::Planning,
        );
        let ledger = CoworkerBindingLedger {
            schema_version: COWORKER_WORKFLOW_SCHEMA_VERSION,
            binding_id: binding_id.to_string(),
            thread_uuid: Some(thread_uuid.to_string()),
            ledger_revision: 1,
            planning_grants: Vec::new(),
            architect_advice: Vec::new(),
            jobs: vec![job],
            admission_markers: std::collections::BTreeMap::new(),
            external_resolutions: Vec::new(),
        };
        write_binding_ledger(&stores.plans, &ledger);

        let before_entries = tree_entries(tree.path());
        let service = ObservationService::new(config(&app_data, &stores));
        let snap1 = service.snapshot(None).expect("snapshot 1");
        let snap2 = service.snapshot(None).expect("snapshot 2");
        let after_entries = tree_entries(tree.path());

        assert_eq!(
            before_entries, after_entries,
            "snapshot must not create files, locks, or directories"
        );
        assert_eq!(snap1.jobs, snap2.jobs);
        assert_eq!(snap1.requests, snap2.requests);
    }
}
