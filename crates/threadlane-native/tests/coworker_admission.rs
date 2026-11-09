//! Read-only constructor and bounded wire contracts require no initialized runtime.
use std::fs;

use tempfile::TempDir;
use threadlane_native::coworker_admission::{
    AdmissionConfirmation, AdmissionRequest, CoworkerAdmissionService,
};

#[test]
fn uninitialized_admission_does_not_create_any_store_or_lock() {
    let tmp = TempDir::new().unwrap();
    let plans = tmp.path().join("plans");
    let authority = tmp.path().join("authority");
    let registry = tmp.path().join("registry");
    assert!(CoworkerAdmissionService::new(&plans, &authority, &registry).is_err());
    assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 0);
}

#[test]
fn admission_wire_types_reject_unknown_authority_fields() {
    let confirmation = serde_json::json!({
        "trusted_local_attested": true,
        "preview_digest": "a".repeat(64),
        "approval_subject_digest": "b".repeat(64),
        "envelope_digest": "c".repeat(64),
        "allow_dispatch": true
    });
    assert!(serde_json::from_value::<AdmissionConfirmation>(confirmation).is_err());
    assert!(serde_json::from_value::<AdmissionRequest>(
        serde_json::json!({"allow_dispatch": true})
    )
    .is_err());
}
