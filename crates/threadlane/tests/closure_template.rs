#![forbid(unsafe_code)]

use std::process::Command;

use sha2::{Digest, Sha256};
use threadlane::closure_template::{
    closure_template_source_sha256, render_closure_template, render_closure_template_json,
    ClosureTemplate, CLOSURE_TEMPLATE_FACTORY_NAME, CLOSURE_TEMPLATE_JAVASCRIPT,
    CLOSURE_TEMPLATE_SOURCE_PATH, CLOSURE_TEMPLATE_TOOL, CLOSURE_TEMPLATE_VERSION,
};

fn packaged_source() -> &'static str {
    include_str!("../../../skills/threadlane/references/ordinary-closure.js")
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let mut hex = String::with_capacity(64);
    for byte in hasher.finalize() {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

#[test]
fn closure_template_embeds_exact_packaged_source_and_hash() {
    let template = render_closure_template();
    assert_eq!(template.schema_version, 1);
    assert_eq!(template.tool, CLOSURE_TEMPLATE_TOOL);
    assert_eq!(template.version, CLOSURE_TEMPLATE_VERSION);
    assert_eq!(template.source_path, CLOSURE_TEMPLATE_SOURCE_PATH);
    assert_eq!(template.factory_name, CLOSURE_TEMPLATE_FACTORY_NAME);
    assert_eq!(template.javascript, CLOSURE_TEMPLATE_JAVASCRIPT);
    assert_eq!(template.javascript, packaged_source());
    assert_eq!(template.source_sha256, closure_template_source_sha256());
    assert_eq!(
        template.source_sha256,
        sha256_hex(packaged_source().as_bytes())
    );
    assert!(packaged_source().contains(CLOSURE_TEMPLATE_VERSION));
    assert!(packaged_source().contains(CLOSURE_TEMPLATE_FACTORY_NAME));
    for required_correction in [
        "sequence_continuity",
        "duplicate_event_id",
        "transport_error",
        "conflicting_envelope",
        "physical_inspection_running",
        "physical_inspection_exit_nonzero",
        "physical_inspection_truncated",
        "ORDINARY_CLOSURE_MAX_BATCH_BYTES = 49152",
        "batch_too_large",
        "immutable_execution_mismatch",
        "utf8ByteLength",
        "event step_seq",
        "acceptance_condition_digest",
        "delivery_description_digest",
        "reserved_at_ms",
    ] {
        assert!(
            packaged_source().contains(required_correction),
            "packaged template is missing correction marker `{required_correction}`"
        );
    }
    assert_eq!(template.source_sha256.len(), 64);
    assert!(template
        .source_sha256
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
}

#[test]
fn closure_template_json_is_compact_and_round_trips() {
    let serialized = render_closure_template_json().expect("serialize template");
    assert!(!serialized.contains('\n'));
    let decoded: ClosureTemplate = serde_json::from_str(&serialized).expect("parse template");
    assert_eq!(decoded, render_closure_template());
    assert_eq!(decoded.javascript, packaged_source());
}

#[test]
fn closure_template_cli_is_offline_and_prints_only_json() {
    let output = Command::new(env!("CARGO_BIN_EXE_threadlane"))
        .arg("closure-template")
        .output()
        .expect("run threadlane closure-template");
    assert!(
        output.status.success(),
        "closure-template must succeed offline"
    );
    assert!(
        output.stderr.is_empty(),
        "closure-template must not write diagnostics to stderr"
    );

    let stdout = String::from_utf8(output.stdout).expect("UTF-8 stdout");
    assert_eq!(
        stdout.trim_end().lines().count(),
        1,
        "closure-template must print exactly one compact JSON line"
    );
    let decoded: ClosureTemplate =
        serde_json::from_str(stdout.trim_end()).expect("stdout is one JSON document");
    assert_eq!(decoded, render_closure_template());
    assert_eq!(decoded.javascript, packaged_source());
    assert!(
        !stdout.contains("01: "),
        "stdout must contain only the JSON document, not tool prefixes"
    );
}
