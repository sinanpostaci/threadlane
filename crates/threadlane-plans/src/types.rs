use std::fmt;

use serde::{Deserialize, Serialize};
use threadlane_registry::{Difficulty, WorkflowRole};

/// Lifecycle state of a persisted [`PlanRevision`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanStatus {
    /// Unapproved editable draft revision; not eligible for runtime execution.
    Draft,
    /// Explicitly published revision frozen for delivery to the runtime.
    Published,
    /// Published revision acknowledged as received by the runtime.
    Received,
    /// Received revision applied to the active runtime task set.
    Applied,
    /// Prior revision superseded by a newer revision of the same plan.
    Superseded,
}

impl PlanStatus {
    /// Returns the canonical `snake_case` wire representation of the status.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Published => "published",
            Self::Received => "received",
            Self::Applied => "applied",
            Self::Superseded => "superseded",
        }
    }

    /// Returns `true` if this status represents a revision that has crossed the explicit
    /// publish boundary (`published`, `received`, `applied`, or `superseded`).
    pub const fn is_immutable_published(self) -> bool {
        !matches!(self, Self::Draft)
    }

    /// Returns `true` if this revision is an active published revision eligible for compilation
    /// (`published`, `received`, or `applied`).
    pub const fn is_active_published(self) -> bool {
        matches!(self, Self::Published | Self::Received | Self::Applied)
    }
}

impl fmt::Display for PlanStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A single bounded task node within a [`PlanRevision`] DAG.
///
/// Prose inside `title` or `instruction` is treated purely as untrusted task data and never
/// grants new scope, selects unapproved models, or implies operator authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanTask {
    /// Unique task identifier within the plan revision.
    pub id: String,
    /// Short human-readable task title.
    pub title: String,
    /// Untrusted task instruction prompt.
    pub instruction: String,
    /// Workflow role used to resolve the target model from the workflow profile.
    pub role: WorkflowRole,
    /// Task difficulty used to resolve the target model from the workflow profile.
    pub difficulty: Difficulty,
    /// Upstream task identifiers within the same plan that must complete before this task.
    pub depends_on: Vec<String>,
    /// Explicit repository-relative file or directory paths this task is permitted to modify.
    pub allowed_write_paths: Vec<String>,
}

/// Immutable or draft versioned plan entry recorded in the durable [`crate::PlanStore`] ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanRevision {
    /// Stable logical plan identifier across revisions.
    pub plan_id: String,
    /// Chat binding identifier that owns this plan.
    pub binding_id: String,
    /// 1-based monotonic revision number for `plan_id` (`<= 9_007_199_254_740_991`).
    pub revision: u64,
    /// Workflow profile identifier used when compiling task role/difficulty assignments.
    pub profile_id: String,
    /// Pinned registry revision (`RegistrySnapshot::revision`) for `profile_id`.
    pub profile_revision: u64,
    /// Human-readable plan summary.
    pub summary: String,
    /// Ordered list of tasks forming a validated DAG.
    pub tasks: Vec<PlanTask>,
    /// Current lifecycle status of this plan revision.
    pub status: PlanStatus,
    /// Creation timestamp in milliseconds since the Unix epoch.
    pub created_at_ms: i64,
}

impl PlanRevision {
    /// Returns a [`DeliveryReceipt`] summarizing the current lifecycle state of this revision.
    pub fn delivery_receipt(&self) -> DeliveryReceipt {
        DeliveryReceipt {
            plan_id: self.plan_id.clone(),
            revision: self.revision,
            state: self.status,
        }
    }
}

/// Compact receipt recording the delivery/application state of a specific [`PlanRevision`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryReceipt {
    /// Logical plan identifier.
    pub plan_id: String,
    /// Plan revision number (`<= 9_007_199_254_740_991`).
    pub revision: u64,
    /// Observed lifecycle state of `(plan_id, revision)`.
    pub state: PlanStatus,
}

/// Internally tagged mutation applied to [`crate::PlanStore`] via [`crate::PlanStore::change`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum PlanChange {
    /// Creates a new draft revision (`current_revision + 1`) for `plan_id`.
    ///
    /// Requires `expected_revision` to match the current latest revision of `plan_id`
    /// (`0` when creating the first draft of a new plan).
    SaveDraft {
        /// Logical plan identifier.
        plan_id: String,
        /// Chat binding identifier owning the plan.
        binding_id: String,
        /// Workflow profile identifier referenced by this plan revision.
        profile_id: String,
        /// Pinned registry revision number (`<= 9_007_199_254_740_991`).
        profile_revision: u64,
        /// Human-readable summary of the plan revision.
        summary: String,
        /// Non-empty validated task DAG for the plan revision.
        tasks: Vec<PlanTask>,
    },
    /// Explicitly publishes the current draft revision (`Draft -> Published`), freezing its
    /// contents and superseding any earlier revisions of `plan_id`.
    Publish {
        /// Logical plan identifier.
        plan_id: String,
        /// Target revision number (`must equal expected_revision and current_revision`).
        revision: u64,
    },
    /// Records runtime receipt of the current published revision (`Published -> Received`).
    Receive {
        /// Logical plan identifier.
        plan_id: String,
        /// Target revision number (`must equal expected_revision and current_revision`).
        revision: u64,
    },
    /// Records runtime application of the current received revision (`Received -> Applied`).
    Apply {
        /// Logical plan identifier.
        plan_id: String,
        /// Target revision number (`must equal expected_revision and current_revision`).
        revision: u64,
    },
}

impl PlanChange {
    /// Returns the `plan_id` targeted by this change.
    pub fn plan_id(&self) -> &str {
        match self {
            Self::SaveDraft { plan_id, .. }
            | Self::Publish { plan_id, .. }
            | Self::Receive { plan_id, .. }
            | Self::Apply { plan_id, .. } => plan_id,
        }
    }

    /// Returns the canonical `snake_case` operation tag name.
    pub const fn op_name(&self) -> &'static str {
        match self {
            Self::SaveDraft { .. } => "save_draft",
            Self::Publish { .. } => "publish",
            Self::Receive { .. } => "receive",
            Self::Apply { .. } => "apply",
        }
    }
}

/// Lifecycle state for cancelling and replacing an affected task across plan revisions.
///
/// An affected task in [`ReplacementState::Unknown`] never admits a replacement until its prior
/// execution is explicitly proven stopped ([`ReplacementState::Stopped`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplacementState {
    /// Task is affected by a plan update and awaits cancellation or stop verification.
    Pending,
    /// Cancellation has been requested on the prior task execution, which is not yet proven stopped.
    CancelRequested,
    /// Prior task execution is proven stopped (or never started); replacement admission is now eligible.
    Stopped,
    /// Replacement task from the new plan revision has been admitted after proven stop.
    Admitted,
    /// Stop outcome of the prior task execution is unknown; blocks replacement admission.
    Unknown,
}

impl ReplacementState {
    /// Returns the canonical `snake_case` wire representation of the replacement state.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::CancelRequested => "cancel_requested",
            Self::Stopped => "stopped",
            Self::Admitted => "admitted",
            Self::Unknown => "unknown",
        }
    }

    /// Returns `true` if a replacement task may be admitted from this state.
    ///
    /// Only [`ReplacementState::Stopped`] permits transitioning to [`ReplacementState::Admitted`].
    pub const fn can_admit_replacement(self) -> bool {
        matches!(self, Self::Stopped)
    }

    /// Returns `true` if this state blocks replacement admission (`pending`, `cancel_requested`, or `unknown`).
    pub const fn blocks_new_admission(self) -> bool {
        matches!(self, Self::Pending | Self::CancelRequested | Self::Unknown)
    }
}

impl fmt::Display for ReplacementState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
