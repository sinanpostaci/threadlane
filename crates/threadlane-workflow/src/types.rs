use std::fmt;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_plans::{
    validate_js_safe_revision, CompiledPlan, CompiledTask, DeliveryReceipt, PlanRevision,
    PlanStatus, PlanTask,
};
use threadlane_registry::{
    DecisionAdvisorMode, Difficulty, RegistryError, RegistrySnapshot, ScopedProfileDependencySpec,
    ToolAdapter, WorkflowRole,
};

use crate::error::WorkflowError;

/// Schema version for [`ScopedProfileDependencyProof`].
pub const SCOPED_PROFILE_DEPENDENCY_PROOF_SCHEMA_VERSION: u32 = 1;

/// Default page size limit for [`crate::ScopedWorkflow::catalog`].
pub const DEFAULT_CATALOG_LIMIT: usize = 20;

/// Maximum page size limit for [`crate::ScopedWorkflow::catalog`].
pub const MAX_CATALOG_LIMIT: usize = 20;

/// Default page size limit for [`crate::ScopedWorkflow::list_plans`].
pub const DEFAULT_PLAN_LIST_LIMIT: usize = 20;

/// Maximum page size limit for [`crate::ScopedWorkflow::list_plans`].
pub const MAX_PLAN_LIST_LIMIT: usize = 20;

/// Default task page size limit for [`crate::ScopedWorkflow::get_plan`].
pub const DEFAULT_PLAN_TASK_LIMIT: usize = 10;

/// Maximum task page size limit for [`crate::ScopedWorkflow::get_plan`].
pub const MAX_PLAN_TASK_LIMIT: usize = 20;

/// Maximum byte length permitted for an opaque pagination cursor.
pub const MAX_CURSOR_LEN: usize = 256;

/// Maximum serialized MCP response envelope size (256 KiB = 262,144 bytes).
pub const MAX_MCP_RESPONSE_BYTES: usize = 256 * 1024;

/// Public execution route classification for a resolved workflow task.
///
/// Maps [`ToolAdapter::Antigravity`] and [`ToolAdapter::OpencodeV2`] to [`Self::HeadlessWorker`],
/// [`ToolAdapter::CodexCli`] to [`Self::NativeFollower`], and all API or auxiliary tools to
/// [`Self::Unsupported`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskExecutionRoute {
    /// Isolated headless CLI worker execution (`antigravity` or `opencode_v2`).
    HeadlessWorker,
    /// Native conversational follower route (`codex_cli`).
    NativeFollower,
    /// Non-worker API or auxiliary tool route (`jev_api`, `openai_decisions_api`, `git`, `tmux`, `codegraph`).
    Unsupported,
}

/// Type alias for [`TaskExecutionRoute`].
pub type TaskRoute = TaskExecutionRoute;

impl TaskExecutionRoute {
    /// Derives the public execution route for a [`ToolAdapter`].
    pub const fn from_adapter(adapter: ToolAdapter) -> Self {
        match adapter {
            ToolAdapter::Antigravity | ToolAdapter::OpencodeV2 => Self::HeadlessWorker,
            ToolAdapter::CodexCli => Self::NativeFollower,
            ToolAdapter::JevApi
            | ToolAdapter::OpenaiDecisionsApi
            | ToolAdapter::Git
            | ToolAdapter::Tmux
            | ToolAdapter::Codegraph => Self::Unsupported,
        }
    }

    /// Returns the canonical `snake_case` wire identifier for this route.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HeadlessWorker => "headless_worker",
            Self::NativeFollower => "native_follower",
            Self::Unsupported => "unsupported",
        }
    }

    /// Returns `true` if this route is a supported workflow task execution route
    /// (`headless_worker` or `native_follower`).
    pub const fn is_supported(self) -> bool {
        !matches!(self, Self::Unsupported)
    }
}

impl fmt::Display for TaskExecutionRoute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Public, credential-free and executable-path-free view of a role/difficulty assignment
/// within a [`CatalogProfile`].
///
/// Assignments referencing disabled tools are marked with `tool_enabled: false` and
/// `available: false` without ever substituting an unapproved model or tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "RawCatalogRoleAssignment")]
pub struct CatalogRoleAssignment {
    /// Target workflow role.
    pub role: WorkflowRole,
    /// Derived human-readable display label for `role` ([`WorkflowRole::display_label`], e.g. `"Worker"` for [`WorkflowRole::Coder`]).
    ///
    /// Non-authoritative display metadata built from the canonical `role`; never changes
    /// executable model assignment, count, availability, authority, or registry snapshots.
    pub role_label: String,
    /// Optional target difficulty tier (`None` for the explicit role-level default).
    pub difficulty: Option<Difficulty>,
    /// Identifier of the configured model in the registry.
    pub model_id: String,
    /// Human-readable display label of the configured model.
    pub model_label: String,
    /// Exact provider/CLI model argument string.
    pub model_arg: String,
    /// Selected reasoning/effort level, if any.
    pub effort: Option<String>,
    /// Identifier of the tool owning `model_id`.
    pub tool_id: String,
    /// Human-readable display label of the owning tool.
    pub tool_label: String,
    /// Adapter kind of the owning tool.
    pub adapter: ToolAdapter,
    /// Public execution route derived from `adapter`.
    pub route: TaskExecutionRoute,
    /// Whether the owning tool is currently enabled in the registry.
    pub tool_enabled: bool,
    /// Whether this assignment is currently available (`tool_enabled` is `true`).
    pub available: bool,
}

#[derive(Deserialize)]
struct RawCatalogRoleAssignment {
    role: WorkflowRole,
    #[serde(default)]
    #[allow(dead_code)]
    role_label: Option<String>,
    difficulty: Option<Difficulty>,
    model_id: String,
    model_label: String,
    model_arg: String,
    effort: Option<String>,
    tool_id: String,
    tool_label: String,
    adapter: ToolAdapter,
    route: TaskExecutionRoute,
    tool_enabled: bool,
    available: bool,
}

impl From<RawCatalogRoleAssignment> for CatalogRoleAssignment {
    fn from(raw: RawCatalogRoleAssignment) -> Self {
        Self {
            role: raw.role,
            role_label: raw.role.display_label().to_string(),
            difficulty: raw.difficulty,
            model_id: raw.model_id,
            model_label: raw.model_label,
            model_arg: raw.model_arg,
            effort: raw.effort,
            tool_id: raw.tool_id,
            tool_label: raw.tool_label,
            adapter: raw.adapter,
            route: raw.route,
            tool_enabled: raw.tool_enabled,
            available: raw.available,
        }
    }
}

/// Type alias for [`CatalogRoleAssignment`].
pub type CatalogAssignment = CatalogRoleAssignment;

/// Public, credential-free and executable-path-free view of a profile's optional read-only
/// decision advisor configuration.
///
/// This is static registry metadata resolved from the same pinned snapshot as the profile's
/// role assignments. It is not proof of credentials, entitlement, provider availability or
/// model health, and it grants no execution authority. Catalog reads never probe a provider,
/// request a secret/status check, or dispatch advice work. `adapter` identifies the configured
/// provider boundary only; OpenAI Decisions entries remain future configuration metadata and
/// do not imply live advice support.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogDecisionAdvisor {
    /// Configured advisory mode.
    pub mode: DecisionAdvisorMode,
    /// Identifier of the configured advisor model in the registry.
    pub model_id: String,
    /// Human-readable display label of the configured advisor model.
    pub model_label: String,
    /// Exact provider/CLI model argument configured for the advisor model.
    pub model_arg: String,
    /// Identifier of the tool owning `model_id`.
    pub tool_id: String,
    /// Human-readable display label of the owning tool.
    pub tool_label: String,
    /// Adapter kind of the owning tool (configuration provenance only).
    pub adapter: ToolAdapter,
}

/// Public summary of a workflow profile in a [`CatalogPage`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogProfile {
    /// Stable unique workflow profile identifier.
    pub profile_id: String,
    /// Human-readable profile name.
    pub name: String,
    /// Pinned registry revision from which this profile was read.
    pub profile_revision: u64,
    /// Whether this profile is the registry's current default workflow profile.
    pub is_default: bool,
    /// Whether all active execution role assignments on this profile are available (`!assignments.is_empty()` and all `available`).
    pub available: bool,
    /// Resolved active execution role/difficulty assignments configured on this profile ([`WorkflowRole::is_execution_role`]).
    pub assignments: Vec<CatalogRoleAssignment>,
    /// Optional read-only decision advisor configuration resolved from the same registry snapshot.
    ///
    /// `None` when no advisor is configured, including legacy profiles without the key. This
    /// metadata is not a seventh worker role, does not affect `available`, and is never
    /// credential, entitlement or availability proof or execution authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_advisor: Option<CatalogDecisionAdvisor>,
}

impl CatalogProfile {
    /// Returns the workflow profile identifier.
    pub fn id(&self) -> &str {
        &self.profile_id
    }
}

/// Bounded, cursor-paginated page of configured workflow profiles from the global registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogPage {
    /// Registry revision pinned by this page and its `next_cursor`.
    pub registry_revision: u64,
    /// Optional active default workflow profile identifier in the registry.
    pub default_profile_id: Option<String>,
    /// Paginated workflow profile summaries (at most [`MAX_CATALOG_LIMIT`] items).
    pub profiles: Vec<CatalogProfile>,
    /// Opaque cursor for fetching the next page at the same `registry_revision`, if more profiles remain.
    pub next_cursor: Option<String>,
    /// `true` when additional profiles remain after this page (`next_cursor.is_some()`).
    pub has_more: bool,
}

/// Compact public header and summary for a single [`PlanRevision`].
///
/// Omits task instructions and CLI executable paths. Includes `plan_digest` when the revision
/// compiles cleanly against the current [`threadlane_registry::RegistrySnapshot`] at `profile_revision`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanSummary {
    /// Logical plan identifier.
    pub plan_id: String,
    /// Owning chat binding identifier.
    pub binding_id: String,
    /// 1-based plan revision number.
    pub revision: u64,
    /// Referenced workflow profile identifier.
    pub profile_id: String,
    /// Pinned registry revision (`RegistrySnapshot::revision`) for `profile_id`.
    pub profile_revision: u64,
    /// Lifecycle status of this plan revision (`draft`, `published`, `received`, `applied`, `superseded`).
    pub status: PlanStatus,
    /// Human-readable plan summary.
    pub summary: String,
    /// Creation timestamp in milliseconds since the Unix epoch.
    pub created_at_ms: i64,
    /// Total number of tasks in this plan revision.
    pub task_count: usize,
    /// 64-character lowercase hex SHA-256 compiled plan digest when compilation against the current
    /// registry snapshot at `profile_revision` is valid; `None` otherwise.
    pub plan_digest: Option<String>,
}

impl PlanSummary {
    /// Constructs a [`PlanSummary`] from a [`PlanRevision`] and an optional precomputed `plan_digest`.
    pub fn from_revision(revision: &PlanRevision, plan_digest: Option<String>) -> Self {
        Self {
            plan_id: revision.plan_id.clone(),
            binding_id: revision.binding_id.clone(),
            revision: revision.revision,
            profile_id: revision.profile_id.clone(),
            profile_revision: revision.profile_revision,
            status: revision.status,
            summary: revision.summary.clone(),
            created_at_ms: revision.created_at_ms,
            task_count: revision.tasks.len(),
            plan_digest,
        }
    }
}

/// Type alias for [`PlanSummary`] when used as the header of [`PlanPage`] or [`PublishedPlanView`].
pub type PlanHeader = PlanSummary;

/// Bounded, cursor-paginated page of [`PlanSummary`] records belonging to the scoped binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanSummaryPage {
    /// Scoped binding identifier.
    pub binding_id: String,
    /// Paginated plan summaries for this binding (at most [`MAX_PLAN_LIST_LIMIT`] items).
    pub plans: Vec<PlanSummary>,
    /// Opaque cursor for fetching the next page, if more plans remain.
    pub next_cursor: Option<String>,
    /// `true` when additional plans remain after this page (`next_cursor.is_some()`).
    pub has_more: bool,
}

/// Bounded, cursor-paginated page of full [`PlanTask`] records for a single [`PlanRevision`]
/// belonging to the scoped binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanPage {
    /// Plan revision header metadata, task count, and optional compiled digest.
    pub header: PlanHeader,
    /// Paginated slice of full [`PlanTask`] items (instructions are never truncated).
    pub tasks: Vec<PlanTask>,
    /// Opaque cursor for fetching the next slice of tasks for this exact plan revision, if more tasks remain.
    pub next_task_cursor: Option<String>,
    /// `true` when additional tasks remain after this page (`next_task_cursor.is_some()`).
    pub has_more: bool,
}

impl PlanPage {
    /// Returns the opaque task pagination cursor for the next page, if any.
    pub fn next_cursor(&self) -> Option<&str> {
        self.next_task_cursor.as_deref()
    }
}

/// Scoped request to create or update a draft plan revision in [`crate::ScopedWorkflow::save_draft`].
///
/// Does not accept a caller-supplied `binding_id`; the service always injects its own trusted
/// `binding_id`. When `plan_id` is `None`, a deterministic plan identifier is derived from
/// `(binding_id, command_key)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SavePlanDraft {
    /// Caller-supplied idempotency key scoped to the binding.
    pub command_key: String,
    /// Optional target plan identifier (`None` derives a deterministic ID from `(binding_id, command_key)`).
    #[serde(default)]
    pub plan_id: Option<String>,
    /// Expected current revision of the plan (`0` when creating a new plan).
    pub expected_revision: u64,
    /// Workflow profile identifier used to resolve task roles and difficulties.
    pub profile_id: String,
    /// Pinned registry revision (`RegistrySnapshot::revision`) that must match the current registry state.
    pub profile_revision: u64,
    /// Human-readable plan summary.
    pub summary: String,
    /// Non-empty validated task DAG for the draft revision.
    pub tasks: Vec<PlanTask>,
}

/// Public, credential-free and executable-path-free route descriptor for a compiled task in a
/// [`PublishedPlanView`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicTaskRoute {
    /// Unique task identifier within the plan.
    pub task_id: String,
    /// Assigned workflow role.
    pub role: WorkflowRole,
    /// Assigned task difficulty tier.
    pub difficulty: Difficulty,
    /// Resolved model identifier from the workflow profile.
    pub model_id: String,
    /// Exact provider/CLI model argument string from the model definition.
    pub model_arg: String,
    /// Resolved reasoning/effort level from the workflow profile, if any.
    pub effort: Option<String>,
    /// Identifier of the enabled tool owning `model_id`.
    pub tool_id: String,
    /// Adapter kind of the owning tool.
    pub adapter: ToolAdapter,
    /// Public execution route classification for this adapter.
    pub route: TaskExecutionRoute,
    /// Validated upstream dependency task identifiers within the plan.
    pub depends_on: Vec<String>,
    /// Approved repository-relative write paths for this task.
    pub allowed_write_paths: Vec<String>,
    /// 64-character lowercase hex SHA-256 digest of the complete compiled task specification.
    pub task_digest: String,
}

impl PublicTaskRoute {
    /// Converts a [`CompiledTask`] into a [`PublicTaskRoute`], stripping `executable_path` and `instruction`.
    pub fn from_compiled_task(task: &CompiledTask) -> Self {
        Self {
            task_id: task.id.clone(),
            role: task.role,
            difficulty: task.difficulty,
            model_id: task.choice.model_id.clone(),
            model_arg: task.model_arg.clone(),
            effort: task.choice.effort.clone(),
            tool_id: task.tool_id.clone(),
            adapter: task.adapter,
            route: TaskExecutionRoute::from_adapter(task.adapter),
            depends_on: task.depends_on.clone(),
            allowed_write_paths: task.allowed_write_paths.clone(),
            task_digest: task.task_digest.clone(),
        }
    }
}

/// Public view returned by [`crate::ScopedWorkflow::publish`].
///
/// Contains the published plan header, truthful [`DeliveryReceipt`], and bounded [`PublicTaskRoute`]
/// entries without exposing CLI executable paths or auto-advancing delivery receipts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedPlanView {
    /// Plan revision header with `plan_digest`.
    pub header: PlanHeader,
    /// Truthful delivery receipt (`published`, `received`, or `applied`).
    pub receipt: DeliveryReceipt,
    /// Resolved public task routes in plan declaration order.
    pub tasks: Vec<PublicTaskRoute>,
}

impl PublishedPlanView {
    /// Constructs a [`PublishedPlanView`] from a [`PlanRevision`] and its [`CompiledPlan`].
    pub fn from_compiled(revision: &PlanRevision, compiled: &CompiledPlan) -> Self {
        let header = PlanSummary::from_revision(revision, Some(compiled.plan_digest.clone()));
        let receipt = revision.delivery_receipt();
        let tasks = compiled
            .tasks
            .iter()
            .map(PublicTaskRoute::from_compiled_task)
            .collect();
        Self {
            header,
            receipt,
            tasks,
        }
    }
}

/// Durable, integrity-bound proof of a selected profile's authority-relevant registry
/// dependencies at a provenance `RegistrySnapshot::revision`.
///
/// Binds the canonical [`ScopedProfileDependencySpec`] and `provenance_revision` under a
/// 64-character lowercase hex SHA-256 `dependency_digest` so persisted planning grants and
/// previews can verify both self-integrity and semantic equivalence against later
/// [`RegistrySnapshot`] revisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedProfileDependencyProof {
    /// Schema version (`1`).
    pub schema_version: u32,
    /// Original `RegistrySnapshot::revision` when the authority was previewed/recorded.
    pub provenance_revision: u64,
    /// Canonical dependency specification for the selected workflow profile.
    pub spec: ScopedProfileDependencySpec,
    /// 64-character lowercase hex SHA-256 digest over `(schema_version, provenance_revision, spec)`.
    pub dependency_digest: String,
}

impl ScopedProfileDependencyProof {
    /// Computes the canonical 64-character lowercase hex SHA-256 digest over
    /// `(schema_version, provenance_revision, spec)`.
    pub fn compute_digest(
        schema_version: u32,
        provenance_revision: u64,
        spec: &ScopedProfileDependencySpec,
    ) -> Result<String, WorkflowError> {
        let payload = serde_json::to_vec(&(
            "threadlane-workflow:scoped_profile_dependency_proof:v1",
            schema_version,
            provenance_revision,
            spec,
        ))
        .map_err(|_| {
            WorkflowError::Serialization(
                "Failed to serialize scoped profile dependency proof payload.",
            )
        })?;
        let digest = Sha256::digest(&payload);
        let mut hex = String::with_capacity(64);
        for byte in digest {
            let _ = write!(&mut hex, "{byte:02x}");
        }
        Ok(hex)
    }

    /// Constructs and validates a [`ScopedProfileDependencyProof`] for `profile_id` from `snapshot`.
    pub fn from_snapshot(
        snapshot: &RegistrySnapshot,
        profile_id: &str,
    ) -> Result<Self, WorkflowError> {
        if snapshot.revision == 0 {
            return Err(WorkflowError::InvalidInput(
                "Scoped profile dependency provenance revision must be at least 1.",
            ));
        }
        validate_js_safe_revision("provenance_revision", snapshot.revision)?;
        let spec = ScopedProfileDependencySpec::from_snapshot(snapshot, profile_id)?;
        let dependency_digest = Self::compute_digest(
            SCOPED_PROFILE_DEPENDENCY_PROOF_SCHEMA_VERSION,
            snapshot.revision,
            &spec,
        )?;
        let proof = Self {
            schema_version: SCOPED_PROFILE_DEPENDENCY_PROOF_SCHEMA_VERSION,
            provenance_revision: snapshot.revision,
            spec,
            dependency_digest,
        };
        proof.validate()?;
        Ok(proof)
    }

    /// Validates the schema version, provenance revision, canonical dependency specification,
    /// and SHA-256 `dependency_digest` integrity of this proof.
    pub fn validate(&self) -> Result<(), WorkflowError> {
        if self.schema_version != SCOPED_PROFILE_DEPENDENCY_PROOF_SCHEMA_VERSION {
            return Err(WorkflowError::InvalidInput(
                "Unsupported scoped profile dependency proof schema version.",
            ));
        }
        if self.provenance_revision == 0 {
            return Err(WorkflowError::InvalidInput(
                "Scoped profile dependency provenance revision must be at least 1.",
            ));
        }
        validate_js_safe_revision("provenance_revision", self.provenance_revision)?;
        self.spec.validate()?;
        if self.dependency_digest.len() != 64
            || !self
                .dependency_digest
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(WorkflowError::InvalidInput(
                "Scoped profile dependency digest must be 64 lowercase hex characters.",
            ));
        }
        let expected_digest =
            Self::compute_digest(self.schema_version, self.provenance_revision, &self.spec)?;
        if self.dependency_digest != expected_digest {
            return Err(WorkflowError::InvalidInput(
                "Scoped profile dependency proof digest mismatch.",
            ));
        }
        Ok(())
    }

    /// Validates `self` and verifies that `live_snapshot` preserves identical authority-relevant
    /// dependencies for `self.spec.profile_id` at `live_snapshot.revision >= self.provenance_revision`.
    ///
    /// On success, returns a minimal [`RegistrySnapshot`] pinned to `self.provenance_revision`
    /// containing only the verified profile and its referenced models and tools.
    pub fn verify_against_live_snapshot(
        &self,
        live_snapshot: &RegistrySnapshot,
    ) -> Result<RegistrySnapshot, WorkflowError> {
        self.validate()?;
        if live_snapshot.revision < self.provenance_revision {
            return Err(WorkflowError::StaleRevision(
                "Live registry revision is older than scoped profile dependency provenance revision.",
            ));
        }
        let live_spec = match ScopedProfileDependencySpec::from_snapshot(
            live_snapshot,
            &self.spec.profile_id,
        ) {
            Ok(spec) => spec,
            Err(RegistryError::NotFound(_)) => {
                return Err(WorkflowError::StaleRevision(
                    "Selected workflow profile was deleted after authority was granted.",
                ));
            }
            Err(err) => return Err(WorkflowError::from(err)),
        };
        if live_spec != self.spec {
            return Err(WorkflowError::StaleRevision(
                "Selected workflow profile dependencies drifted from standing authority.",
            ));
        }
        let live_digest =
            Self::compute_digest(self.schema_version, self.provenance_revision, &live_spec)?;
        if live_digest != self.dependency_digest {
            return Err(WorkflowError::StaleRevision(
                "Selected workflow profile dependency digest drifted from standing authority.",
            ));
        }
        Ok(self
            .spec
            .to_scoped_registry_snapshot(self.provenance_revision))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_role_assignment_derives_role_label_and_defaults_legacy_catalog_reads() {
        let legacy_json = serde_json::json!({
            "role": "coder",
            "difficulty": "hard",
            "model_id": "argon-high",
            "model_label": "Model argon-high",
            "model_arg": "gemini-4-argon-high",
            "effort": "high",
            "tool_id": "agy",
            "tool_label": "Tool agy",
            "adapter": "antigravity",
            "route": "headless_worker",
            "tool_enabled": true,
            "available": true
        });
        let decoded: CatalogRoleAssignment =
            serde_json::from_value(legacy_json).expect("decode legacy CatalogRoleAssignment");
        assert_eq!(decoded.role, WorkflowRole::Coder);
        assert_eq!(decoded.role_label, "Worker");

        let serialized = serde_json::to_value(&decoded).expect("serialize CatalogRoleAssignment");
        assert_eq!(serialized["role"], "coder");
        assert_eq!(serialized["role_label"], "Worker");

        let round_trip: CatalogRoleAssignment =
            serde_json::from_value(serialized).expect("round-trip CatalogRoleAssignment");
        assert_eq!(round_trip, decoded);

        let worker_role_json = serde_json::json!({
            "role": "worker",
            "difficulty": "easy",
            "model_id": "deepseek-flash",
            "model_label": "Model deepseek-flash",
            "model_arg": "opencode-go/deepseek-v4.1-flash",
            "effort": "max",
            "tool_id": "opencode",
            "tool_label": "Tool opencode",
            "adapter": "opencode_v2",
            "route": "headless_worker",
            "tool_enabled": true,
            "available": true
        });
        assert!(
            serde_json::from_value::<CatalogRoleAssignment>(worker_role_json).is_err(),
            "CatalogRoleAssignment.role uses canonical WorkflowRole serde and must not accept non-canonical wire role strings"
        );
    }
}
