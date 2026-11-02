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

/// Serializable data contracts, validation rules, deterministic digests, and independence proofs.
pub mod contracts;
/// Private per-binding Coworker workflow ledger and compare-and-swap transaction store.
pub mod store;

pub use contracts::*;
pub use store::*;
