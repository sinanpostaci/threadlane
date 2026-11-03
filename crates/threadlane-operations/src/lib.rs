#![deny(unsafe_code)]
#![deny(missing_docs)]

//! `threadlane-operations`: review subject binding, explicit operator acceptance/integration/push/cleanup
//! workflows, and auxiliary Git/tmux/CodeGraph tool capabilities for Threadlane Milestone 4.
//!
//! ## Key Invariants
//!
//! - **Digest-bound review subjects**: [`ReviewSubject`] binds the canonical repository,
//!   `base_commit`, `task_head_commit`, `task_tree_sha`, `diff_digest`, `allowed_write_paths`,
//!   [`ReviewPolicyBinding`], and [`TestEvidenceSummary`] into a single SHA-256 `subject_digest`.
//!   Any modification to `task_head_commit`, `base_commit`, `allowed_write_paths`, `policy`, or
//!   `test_evidence` invalidates any previously issued [`ReviewReceipt`].
//! - **Explicit operator entry points only**: Acceptance ([`accept_review_subject`]), integration
//!   ([`execute_integration`]), integrated worktree cleanup ([`cleanup_integrated_task_worktree`] /
//!   [`cleanup_integrated_worktree`]), push ([`execute_push`]), and remote branch deletion
//!   ([`execute_remote_branch_delete`]) are never triggered automatically by worker exit `0` or
//!   model prose.
//! - **Read-only plans vs. verified execution**: [`propose_integration`], [`propose_push`],
//!   [`propose_remote_branch_delete`], and [`propose_codegraph_index_plan`] are strictly read-only.
//!   Execution methods require matching approved receipts and current `HEAD` commits.
//! - **Owned integration checkout and artifact preservation**: Integration executes exclusively
//!   inside the owned `WorktreeKind::RunIntegration` checkout while preserving the primary source
//!   checkout and any dirty or uncertain worktree artifacts.
//! - **Sanitized `GIT_*` environment**: All Git subprocesses strip inherited `GIT_*` routing/config
//!   variables and set controlled `GIT_TERMINAL_PROMPT=0`, `GIT_CONFIG_NOSYSTEM=1`,
//!   `GIT_CONFIG_SYSTEM=/dev/null`, `GIT_CONFIG_GLOBAL=/dev/null`, and `GIT_OPTIONAL_LOCKS=0`
//!   overrides.
//! - **Auxiliary tool boundaries**: [`ToolCapabilities`] reports `git`, `tmux`, and `codegraph`
//!   availability without making `tmux` or `codegraph` mandatory OS dependencies. [`TmuxAttachmentDescriptor`]
//!   is strictly read-only (`-r`) and never replaces the Unix process-group supervisor.
//!   [`CodeGraphWorktreeStatus`] enforces that the CodeGraph `project_path` is an actual owned
//!   worktree checkout, tracks explicit index freshness (`missing`, `fresh`, `stale`, `unverified`),
//!   and never conflates the `codegraph_explore` MCP server with a standalone CLI binary.

/// Read-only final combined candidate inspection/freeze on owned `RunIntegration` checkouts and
/// cross-process locked compare-and-swap closure-attempt ledger.
pub mod candidate;
/// Auxiliary `git`, `tmux`, and `codegraph` capability reporting, read-only `tmux` attachment
/// descriptors, and owned-worktree CodeGraph project/freshness checks.
pub mod capabilities;
/// Predecessor proof verification, deterministic Git tree/commit composition, compare-and-swap
/// snapshot ref updates, durable preparation intent journaling, and write-once dependency input seals.
pub mod dependency;
/// Structured error codes, error types, and API error envelopes.
pub mod error;
/// Read-only integration/push/remote-delete proposals, owned integration checkout execution,
/// frozen integration receipts, and proof-verified integrated worktree cleanup.
pub mod integration;
/// Review subject construction, SHA-256 digest binding, policy/test evidence gates, and frozen review receipts.
pub mod review;

pub use candidate::{
    inspect_final_candidate_readonly, verify_final_candidate_subject_readonly,
    verify_legacy_base_closure_insufficient, ClosureAttemptRecord, ClosureAttemptState,
    ClosureExecutionEvidence, ClosureReservationOutcome, ClosureTerminalFailure, ClosureVerdict,
    ClosureVerdictKind, FinalCandidateClosureLedger, FinalCandidateClosureStore,
    FinalCandidatePolicyBinding, FinalCandidateSubject, FinalCandidateTaskInput,
    InspectFinalCandidateRequest, MarkClosureSubmittedRequest, MarkClosureUnknownRequest,
    ReconcileClosureAttemptRequest, RecordClosureFailureRequest, RecordClosureVerdictRequest,
    ReserveClosureAttemptRequest, VerifiedCandidateTaskEvidence, CLOSURES_SUBDIR,
    CLOSURE_LEDGER_FILENAME, CLOSURE_LEDGER_LOCK_FILENAME, FINAL_CANDIDATE_CLOSURE_SCHEMA_VERSION,
    FINAL_CANDIDATE_SUBJECT_SCHEMA_VERSION, MAX_CLOSURE_ATTEMPTS, MAX_CLOSURE_LEDGER_BYTES,
};
pub use capabilities::{
    build_tmux_readonly_descriptor, inspect_codegraph_worktree, inspect_tool_capabilities_passive,
    probe_tool_capabilities_explicit, propose_codegraph_index_plan, record_codegraph_index_receipt,
    AuxiliaryToolKind, CodeGraphIndexFreshness, CodeGraphIndexProposal, CodeGraphIndexReceipt,
    CodeGraphSurfaceDistinction, CodeGraphWorktreeStatus, TmuxAttachmentDescriptor,
    TmuxDescriptorRequest, ToolCapabilities, ToolCapabilityReport, ToolPathsConfig,
    ToolSupportStatus, CODEGRAPH_INDEX_RECEIPT_SCHEMA_VERSION, MAX_TOOL_PROBE_OUTPUT_BYTES,
};
pub use dependency::{
    dependency_input_intent_path, dependency_input_snapshot_ref,
    inspect_dependency_input_seal_readonly, prepare_and_seal_dependency_input,
    verify_dependency_predecessor_proof, verify_dependency_predecessor_receipts,
    DependencyPredecessorCandidate, DependencyPreparationIntent, DEPENDENCIES_SUBDIR,
    DEPENDENCY_INPUT_REF_PREFIX, MAX_DEPENDENCY_CLOSURE_TASKS, MAX_DEPENDENCY_PREDECESSORS,
};
pub use error::{OperationsApiError, OperationsError, OperationsErrorCode};
pub use integration::{
    cleanup_integrated_task_worktree, execute_integration, execute_push,
    execute_remote_branch_delete, load_integration_receipt, propose_integration, propose_push,
    propose_remote_branch_delete, ExecuteIntegrationRequest, ExecutePushRequest,
    ExecuteRemoteDeleteRequest, IntegrationProposal, IntegrationProposalRequest,
    IntegrationReceipt, IntegrationStrategy, PushProposal, PushProposalRequest, PushReceipt,
    RemoteDeleteProposal, RemoteDeleteProposalRequest, RemoteDeleteReceipt,
    VerifiedIntegratedCleanupRequest, INTEGRATION_RECEIPT_SCHEMA_VERSION,
    PUSH_RECEIPT_SCHEMA_VERSION, REMOTE_DELETE_RECEIPT_SCHEMA_VERSION,
};
pub use review::{
    accept_review_subject, commit_task_worktree_changes, inspect_review_subject,
    inspect_review_subject_readonly, load_review_receipt, validate_worker_status_for_review,
    verify_review_receipt, verify_review_receipt_readonly, CommitTaskChangesRequest,
    InspectReviewSubjectRequest, ReviewAcceptanceRequest, ReviewPolicyBinding, ReviewReceipt,
    ReviewSubject, TaskCommitReport, TestEvidenceSummary, MAX_RECEIPT_BYTES, MAX_REVIEW_NOTE_BYTES,
    RECEIPTS_SUBDIR, REVIEW_RECEIPT_SCHEMA_VERSION, REVIEW_SUBJECT_SCHEMA_VERSION,
};
pub use threadlane_workers::{
    cleanup_integrated_worktree, IntegratedCleanupReport, IntegratedCleanupRequest,
};
