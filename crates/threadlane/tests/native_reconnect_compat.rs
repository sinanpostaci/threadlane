use serde_json::json;
use threadlane::NativeToolOutputConfig;

#[test]
fn legacy_wire_shape_and_frozen_policy_digest_remain_unchanged() {
    let legacy = json!({
        "schema_version": 1,
        "socket_path": "/tmp/legacy-native.sock",
        "thread_id": "01900000-0000-7000-8000-000000000001",
        "project_root": "/tmp/legacy-project",
        "model": "gpt-6-luna",
        "effort": "max"
    });
    let config: NativeToolOutputConfig = serde_json::from_value(legacy.clone()).unwrap();

    assert_eq!(serde_json::to_value(&config).unwrap(), legacy);
    assert_eq!(
        config.digest(),
        "6dc84e6979a8f33533824fd6b381df8160a9236b27f392d564ad24e0d330f9af"
    );
}

#[test]
fn every_reconnect_selector_path_is_bound_to_the_policy_digest() {
    let value = json!({
        "schema_version": 1,
        "socket_path": "/tmp/legacy-native.sock",
        "thread_id": "01900000-0000-7000-8000-000000000001",
        "project_root": "/tmp/legacy-project",
        "model": "gpt-6-luna",
        "effort": "max",
        "reconnect": {
            "descriptor_root": "/private/runtime",
            "backend_executable": "/private/bin/backend",
            "application_executable": "/private/bin/application"
        }
    });
    let original: NativeToolOutputConfig = serde_json::from_value(value.clone()).unwrap();
    for field in [
        "descriptor_root",
        "backend_executable",
        "application_executable",
    ] {
        let mut changed = value.clone();
        changed["reconnect"][field] = json!("/different/selection");
        let changed: NativeToolOutputConfig = serde_json::from_value(changed).unwrap();
        assert_ne!(
            original.digest(),
            changed.digest(),
            "unbound field: {field}"
        );
    }
}
