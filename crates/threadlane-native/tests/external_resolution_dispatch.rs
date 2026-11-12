//! Behavioral tests for the shared external-resolution dispatch predicate.
//!
//! These tests exercise the public read-only predicate used by direct native dispatch,
//! controller/outbox boundaries, and execution/driver gates without minting receipts or
//! changing the qualified `ExternalResolutionService` evaluator.

use std::path::PathBuf;

use threadlane_native::coworker_workflow::CoworkerBindingLedger;
use threadlane_native::dispatch_guard::{
    external_resolution_blocks_full_job, external_resolution_blocks_task,
};
use threadlane_native::external_resolution::{
    ExternalResolutionProjectRef, ExternalResolutionReceipt, ExternalResolutionScope,
    ExternalResolutionTarget, EXTERNAL_RESOLUTION_INTERFACE_VERSION,
};

const BINDING: &str = "bnd_external_dispatch_integration";
const THREAD: &str = "0196ed00-0000-7000-8000-0000000000a1";

fn receipt(
    request_key: &str,
    target: ExternalResolutionTarget,
    scope: ExternalResolutionScope,
) -> ExternalResolutionReceipt {
    ExternalResolutionReceipt {
        schema_version: 1,
        interface_version: EXTERNAL_RESOLUTION_INTERFACE_VERSION.to_string(),
        receipt_id: format!("exr_{}", "a".repeat(32)),
        binding_id: BINDING.to_string(),
        thread_uuid: THREAD.to_string(),
        request_key: request_key.to_string(),
        created_at_ms: 1,
        project: ExternalResolutionProjectRef {
            canonical_repo_root: PathBuf::from("/tmp/project"),
            base_commit: "a".repeat(40),
            base_tree_sha: Some("b".repeat(40)),
        },
        target,
        scope,
        resolved_task_ids: Vec::new(),
        source_ledger_revision: 0,
        recorded_ledger_revision: 1,
        source_job_revision: Some(1),
        source_controller_ledger_revision: 0,
        source_runtime_revision: None,
        work_brief_digest: Some("c".repeat(64)),
        plan_id: None,
        plan_revision: None,
        plan_digest: None,
        approval_id: None,
        approval_subject_digest: None,
        envelope_digest: None,
        explanation: "integration dispatch predicate fixture".to_string(),
        evidence_refs: Vec::new(),
        request_digest: "d".repeat(64),
        preview_digest: "e".repeat(64),
        receipt_digest: "f".repeat(64),
    }
}

fn ledger() -> CoworkerBindingLedger {
    CoworkerBindingLedger::empty(BINDING).expect("empty binding ledger")
}

#[test]
fn task_scope_blocks_only_the_covered_task_and_full_job_closes_advancement() {
    let mut partial = ledger();
    partial.external_resolutions = vec![receipt(
        "req_partial",
        ExternalResolutionTarget::Job {
            job_id: "job_a".to_string(),
        },
        ExternalResolutionScope::Tasks {
            task_ids: vec!["task-2".to_string()],
        },
    )];
    assert!(external_resolution_blocks_task(
        &partial,
        Some("job_a"),
        Some("plan_a"),
        Some("apr_a"),
        "task-1"
    )
    .is_none());
    let block = external_resolution_blocks_task(
        &partial,
        Some("job_a"),
        Some("plan_a"),
        Some("apr_a"),
        "task-2",
    )
    .expect("covered task must block");
    assert!(!block.full_job);
    assert!(external_resolution_blocks_full_job(
        &partial,
        Some("job_a"),
        Some("plan_a"),
        Some("apr_a")
    )
    .is_none());

    let mut full = ledger();
    full.external_resolutions = vec![receipt(
        "req_full",
        ExternalResolutionTarget::Job {
            job_id: "job_a".to_string(),
        },
        ExternalResolutionScope::FullJob,
    )];
    for task_id in ["task-1", "task-2"] {
        assert!(external_resolution_blocks_task(
            &full,
            Some("job_a"),
            Some("plan_a"),
            Some("apr_a"),
            task_id
        )
        .expect("full-job receipt must block every task")
        .full_job);
    }
    assert!(external_resolution_blocks_full_job(
        &full,
        Some("job_a"),
        Some("plan_a"),
        Some("apr_a")
    )
    .is_some());
    assert!(external_resolution_blocks_task(
        &full,
        Some("job_other"),
        Some("plan_other"),
        Some("apr_other"),
        "task-1"
    )
    .is_none());
}

#[test]
fn legacy_plan_receipts_block_the_bound_plan_even_after_restart_without_a_witness() {
    let mut ledger = ledger();
    ledger.external_resolutions = vec![receipt(
        "req_legacy",
        ExternalResolutionTarget::LegacyPlan {
            plan_id: "plan_legacy".to_string(),
            plan_revision: 1,
        },
        ExternalResolutionScope::FullJob,
    )];
    assert!(external_resolution_blocks_task(
        &ledger,
        None,
        Some("plan_legacy"),
        Some("apr_legacy"),
        "task-1"
    )
    .is_some());
    assert!(external_resolution_blocks_task(
        &ledger,
        None,
        Some("plan_other"),
        Some("apr_legacy"),
        "task-1"
    )
    .is_none());
}
