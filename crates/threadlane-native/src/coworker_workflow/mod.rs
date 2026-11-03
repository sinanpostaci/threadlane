//! Shared contracts and private per-binding ledger store for the end-to-end Threadlane Coworker
//! workflow.
//!
//! ## Scope and Layered Ownership
//!
//! This module provides the foundational data contracts ([`contracts`]) and cross-process locked
//! compare-and-swap persistence ([`store`]) for the Coworker workflow:
//! - [`contracts`]: [`WorkBrief`], [`PlanningAdviceGrant`], [`ArchitectContextDependencies`],
//!   [`ArchitectAdviceRecord`], [`FrozenApprovalLinkage`], [`FrozenControllerCapabilityEnvelope`],
//!   [`CoworkerTaskStage`], [`StructuredReviewVerdict`], [`OutboxDispatchRecord`],
//!   [`WorkflowHoldRecord`], [`TaskIndependenceProof`], [`CombinedCandidateIdentity`], and
//!   [`CoworkerJobRecord`].
//! - [`store`]: [`CoworkerBindingLedger`], [`CoworkerWorkflowStore`], and transactional helpers
//!   for grants, reservations, architect advice reuse, job creation/amendments, immutable
//!   approval binding, outbox reservations/reconciliation, structured review verdicts, holds, and
//!   combined candidate assembly.
//!
//! Subsequent independent writers add runtime services on top of this foundation (specified in
//! `docs/COWORKER-WORKFLOW-CONTRACT.md`):
//! - `planning.rs`: async read-only orchestrator and architect execution over
//!   [`PlanningAdviceGrant`] reservations and [`ScopedWorkflow`] draft/publish orchestration.
//! - `controller.rs`: automatic routine task/review/in-scope-fix/dependency scheduling over
//!   [`CoworkerWorkflowStore`], [`ApprovedHostClient`], and [`ConversationChangeStore`], governed
//!   by an explicit [`FrozenControllerCapabilityEnvelope`].
//! - `candidate.rs`: verified combined-candidate assembly, check evidence verification, and
//!   single-closure reservation binding over [`RoleFlowStore`].

/// Verified combined-candidate assembly, check evidence verification, and single-closure
/// reservation/execution over [`threadlane_operations`] and [`CoworkerWorkflowStore`].
pub mod candidate;
/// Bounded frozen verification check execution over owned `Task` and `RunIntegration` worktrees
/// and verified [`CandidateCheckEvidence`] promotion.
pub mod checks;
/// Serializable data contracts, validation rules, deterministic digests, and independence proofs.
pub mod contracts;
/// Automatic Coworker workflow controller (Phase 1: admission, physical ownership, original
/// `Coder` host task reconciliation, dependency-seal gating, and bounded parallel dispatch).
pub mod controller;
/// Typed controller execution configuration/envelope, frozen check command specifications,
/// per-task and job-wide correction/attempt/role budgets, declared interface-independence
/// completeness receipts, derived correction attempt receipts, and per-job driver lease state.
pub mod controller_policy;
/// Final multi-worker job Git aggregation, candidate check execution, candidate assembly, and Closure stage.
pub mod job_completion;
/// Private per-job OS file-lock ownership, CSPRNG instance/challenge identity, and pre-effect validation.
pub mod ownership;
/// Asynchronous read-only Coworker planning and architectural advice service.
pub mod planning;
/// Strict task `Reviewer` JSON response parser and identity verification.
pub mod review;
/// Bounded supervised read-only task `Reviewer` execution primitive over owned task worktrees.
pub mod review_execution;
/// Private per-binding Coworker workflow ledger and compare-and-swap transaction store.
pub mod store;
/// Per-task in-scope correction preparation and durable reservation stage (`CorrectInScopeRequired -> DispatchReserved`).
pub mod task_correction;
/// Per-task derived `Coder` correction execution stage (`DispatchReserved -> Running -> ReadyForReview`).
pub mod task_correction_execution;
/// Per-task original `RunIntegration` controller stage over accepted task candidates and owned
/// `RunIntegration` worktrees.
pub mod task_integration;
/// Supervised read-only task `Reviewer` controller stage: verified candidate -> durable verdict
/// and real [`threadlane_operations::ReviewReceipt`] on `Accept`.
pub mod task_review_stage;
/// Completed coder task worktree commit, frozen verification check execution, and
/// [`threadlane_operations::ReviewSubject`] preparation before `Reviewer` dispatch.
pub mod task_verification;

pub use checks::*;
pub use contracts::*;
pub use controller::*;
pub use controller_policy::*;
pub use job_completion::*;
pub use ownership::*;
pub use planning::*;
pub use review::*;
pub use review_execution::*;
pub use store::*;
pub use task_correction::*;
pub use task_correction_execution::*;
pub use task_integration::*;
pub use task_review_stage::*;
pub use task_verification::*;
