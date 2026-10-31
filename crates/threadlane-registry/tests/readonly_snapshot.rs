use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::PathBuf;

use tempfile::TempDir;
use threadlane_registry::{
    RegistryChange, RegistryStore, ToolAdapter, ToolDefinition, REGISTRY_FILE_NAME,
    REGISTRY_LOCK_FILE_NAME,
};

fn sample_tool(id: &str, adapter: ToolAdapter) -> ToolDefinition {
    ToolDefinition {
        id: id.to_string(),
        label: format!("Tool {id}"),
        adapter,
        executable_path: if adapter.is_cli_adapter() {
            Some(PathBuf::from(format!("/usr/local/bin/{id}")))
        } else {
            None
        },
        enabled: true,
    }
}

#[test]
fn readonly_snapshot_never_creates_lock_or_missing_directory() {
    let temp = TempDir::new().expect("tempdir");
    let missing_dir = temp.path().join("missing-registry");

    let missing_err = RegistryStore::read_snapshot_readonly_at(&missing_dir)
        .expect_err("missing registry directory must be rejected");
    assert_eq!(missing_err.code(), "io_error");
    assert!(
        !missing_dir.exists(),
        "read-only snapshot must not create a missing configuration directory"
    );

    let config_dir = temp.path().join("registry");
    let store = RegistryStore::open(&config_dir).expect("open registry store");
    let written = store
        .apply(
            0,
            RegistryChange::UpsertTool {
                tool: sample_tool("codex", ToolAdapter::CodexCli),
            },
        )
        .expect("apply tool");
    let registry_bytes = fs::read(store.registry_path()).expect("read registry bytes");
    assert!(store.lock_path().exists());

    fs::remove_file(store.lock_path()).expect("remove registry.lock");
    assert!(!store.lock_path().exists());

    let instance_snapshot = store
        .snapshot_readonly()
        .expect("instance readonly snapshot");
    assert_eq!(instance_snapshot, written);
    assert!(
        !store.lock_path().exists(),
        "instance readonly snapshot must not recreate registry.lock"
    );

    let static_snapshot =
        RegistryStore::read_snapshot_readonly_at(&config_dir).expect("static readonly snapshot");
    assert_eq!(static_snapshot, written);
    assert!(
        !store.lock_path().exists(),
        "static readonly snapshot must not recreate registry.lock"
    );
    assert_eq!(
        fs::read(store.registry_path()).expect("read registry bytes after"),
        registry_bytes
    );
}

#[test]
fn readonly_snapshot_rejects_corrupt_and_symlink_paths() {
    let temp = TempDir::new().expect("tempdir");
    let config_dir = temp.path().join("registry");
    fs::create_dir(&config_dir).expect("create registry dir");
    fs::set_permissions(&config_dir, fs::Permissions::from_mode(0o700))
        .expect("set registry dir mode");
    let registry_path = config_dir.join(REGISTRY_FILE_NAME);
    let lock_path = config_dir.join(REGISTRY_LOCK_FILE_NAME);

    let corrupt_bytes = b"{\"schema_version\":1,\"revision\":0,corrupt!";
    fs::write(&registry_path, corrupt_bytes).expect("write corrupt registry");
    let corrupt_err = RegistryStore::read_snapshot_readonly_at(&config_dir)
        .expect_err("corrupt registry must fail readonly read");
    assert_eq!(corrupt_err.code(), "corrupt_store");
    assert!(!lock_path.exists());
    assert_eq!(
        fs::read(&registry_path).expect("read corrupt registry back"),
        corrupt_bytes
    );

    fs::remove_file(&registry_path).expect("remove corrupt registry");
    let victim_file = temp.path().join("victim.json");
    fs::write(&victim_file, b"keep-me-safe").expect("write victim file");
    symlink(&victim_file, &registry_path).expect("symlink registry.json");

    let symlink_err = RegistryStore::read_snapshot_readonly_at(&config_dir)
        .expect_err("symlink registry.json must fail readonly read");
    assert_eq!(symlink_err.code(), "security_violation");
    assert!(!lock_path.exists());
    assert_eq!(
        fs::read(&victim_file).expect("read victim"),
        b"keep-me-safe"
    );

    fs::remove_file(&registry_path).expect("remove registry symlink");
    let symlink_dir = temp.path().join("registry-link");
    symlink(&config_dir, &symlink_dir).expect("symlink registry dir");
    let dir_err = RegistryStore::read_snapshot_readonly_at(&symlink_dir)
        .expect_err("symlink configuration directory must fail readonly read");
    assert_eq!(dir_err.code(), "security_violation");
    assert!(!lock_path.exists());
}
