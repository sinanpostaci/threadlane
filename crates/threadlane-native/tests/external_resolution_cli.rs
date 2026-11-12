//! CLI surface tests for the trusted-local `external-resolution` flow family.
//!
//! Store-backed behavior is covered by the qualified service tests and the shared
//! dispatch-predicate tests. These tests ensure the operator-facing family and its
//! independent confirmation flags remain discoverable and cannot be replaced by a
//! JSON-only or MCP self-approval path.

use std::process::Command;

fn flow_help(args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_threadlane-flow"))
        .args(args)
        .output()
        .expect("run threadlane-flow help");
    assert!(
        output.status.success(),
        "threadlane-flow help failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("help output is UTF-8")
}

#[test]
fn external_resolution_help_lists_preview_record_and_inspect() {
    let help = flow_help(&["external-resolution", "--help"]);
    for expected in ["preview", "record", "inspect"] {
        assert!(
            help.contains(expected),
            "external-resolution help must list `{expected}`:\n{help}"
        );
    }
}

#[test]
fn external_resolution_record_help_requires_independent_confirmations() {
    let help = flow_help(&["external-resolution", "record", "--help"]);
    for expected in [
        "--operator-approved",
        "--confirm-thread",
        "--confirm-preview",
        "--reconcile-existing",
    ] {
        assert!(
            help.contains(expected),
            "external-resolution record help must list `{expected}`:\n{help}"
        );
    }
}
