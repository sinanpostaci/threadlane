use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::RegistryError;

/// Current on-disk and wire schema version for [`RegistrySnapshot`].
pub const REGISTRY_SCHEMA_VERSION: u32 = 1;

/// Maximum JS-safe integer (`2^53 - 1`) permitted for [`RegistrySnapshot::revision`].
pub const MAX_JS_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// Maximum number of [`ToolDefinition`] entries permitted in a [`RegistrySnapshot`].
pub const MAX_TOOLS: usize = 64;

/// Maximum number of [`ModelDefinition`] entries permitted in a [`RegistrySnapshot`].
pub const MAX_MODELS: usize = 256;

/// Maximum number of [`WorkflowProfile`] entries permitted in a [`RegistrySnapshot`].
pub const MAX_PROFILES: usize = 64;

/// Maximum number of [`RoleAssignment`] entries permitted in a single [`WorkflowProfile`].
pub const MAX_ASSIGNMENTS_PER_PROFILE: usize = 30;

/// Maximum number of effort strings permitted on a single [`ModelDefinition`].
pub const MAX_EFFORTS_PER_MODEL: usize = 32;

/// Maximum character length for tool, model, and profile identifiers.
pub const MAX_ID_LEN: usize = 128;

/// Maximum character length for human-readable tool, model, and profile labels/names.
pub const MAX_LABEL_LEN: usize = 256;

/// Maximum character length for a provider [`ModelDefinition::model_arg`] string.
pub const MAX_MODEL_ARG_LEN: usize = 256;

/// Maximum character length for a single reasoning/effort identifier.
pub const MAX_EFFORT_LEN: usize = 64;

/// Maximum UTF-8 byte length for an optional [`ToolDefinition::executable_path`].
pub const MAX_EXECUTABLE_PATH_LEN: usize = 4_096;

/// Maximum byte size permitted when reading or writing `registry.json` (`1 MiB`).
pub const MAX_REGISTRY_FILE_BYTES: u64 = 1_048_576;

/// Filename for the persisted registry snapshot inside the registry directory.
pub const REGISTRY_FILE_NAME: &str = "registry.json";

/// Filename for the stable cross-process lock file inside the registry directory.
pub const REGISTRY_LOCK_FILE_NAME: &str = "registry.lock";

/// External CLI or API adapter kind associated with a [`ToolDefinition`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolAdapter {
    /// Native Codex CLI (`codex`).
    #[serde(rename = "codex_cli")]
    CodexCli,
    /// OpenCode v2 CLI (`opencode`).
    #[serde(rename = "opencode_v2", alias = "open_code_v2")]
    OpencodeV2,
    /// Google Antigravity CLI (`agy` / `antigravity`).
    #[serde(rename = "antigravity")]
    Antigravity,
    /// Jev System One HTTPS API (`https://api.typesafe.ai/v1/...`).
    #[serde(rename = "jev_api")]
    JevApi,
    /// OpenAI Decisions HTTPS API (`https://api.openai.com/v1/decisions`).
    #[serde(rename = "openai_decisions_api", alias = "open_ai_decisions_api")]
    OpenaiDecisionsApi,
    /// Local Git CLI (`git`).
    #[serde(rename = "git")]
    Git,
    /// Local terminal multiplexer CLI (`tmux`).
    #[serde(rename = "tmux")]
    Tmux,
    /// CodeGraph CLI or index tool (`codegraph`).
    #[serde(rename = "codegraph", alias = "code_graph")]
    Codegraph,
}

#[allow(non_upper_case_globals)]
impl ToolAdapter {
    /// Compatibility alias for [`Self::OpencodeV2`].
    pub const OpenCodeV2: Self = Self::OpencodeV2;

    /// Compatibility alias for [`Self::OpenaiDecisionsApi`].
    pub const OpenAiDecisionsApi: Self = Self::OpenaiDecisionsApi;

    /// Compatibility alias for [`Self::OpenaiDecisionsApi`].
    pub const OpenAIDecisionsApi: Self = Self::OpenaiDecisionsApi;

    /// Compatibility alias for [`Self::Codegraph`].
    pub const CodeGraph: Self = Self::Codegraph;

    /// All supported [`ToolAdapter`] variants in canonical order.
    pub const ALL: [Self; 8] = [
        Self::CodexCli,
        Self::OpencodeV2,
        Self::Antigravity,
        Self::JevApi,
        Self::OpenaiDecisionsApi,
        Self::Git,
        Self::Tmux,
        Self::Codegraph,
    ];

    /// Returns the canonical `snake_case` wire identifier for this adapter.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CodexCli => "codex_cli",
            Self::OpencodeV2 => "opencode_v2",
            Self::Antigravity => "antigravity",
            Self::JevApi => "jev_api",
            Self::OpenaiDecisionsApi => "openai_decisions_api",
            Self::Git => "git",
            Self::Tmux => "tmux",
            Self::Codegraph => "codegraph",
        }
    }

    /// Returns `true` if this adapter is an HTTPS API integration rather than a local CLI binary.
    pub const fn is_api_adapter(self) -> bool {
        matches!(self, Self::JevApi | Self::OpenaiDecisionsApi)
    }

    /// Returns `true` if this adapter invokes a local CLI executable.
    pub const fn is_cli_adapter(self) -> bool {
        !self.is_api_adapter()
    }
}

impl fmt::Display for ToolAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ToolAdapter {
    type Err = RegistryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "codex_cli" => Ok(Self::CodexCli),
            "opencode_v2" | "open_code_v2" => Ok(Self::OpencodeV2),
            "antigravity" => Ok(Self::Antigravity),
            "jev_api" => Ok(Self::JevApi),
            "openai_decisions_api" | "open_ai_decisions_api" => Ok(Self::OpenaiDecisionsApi),
            "git" => Ok(Self::Git),
            "tmux" => Ok(Self::Tmux),
            "codegraph" | "code_graph" => Ok(Self::Codegraph),
            _ => Err(RegistryError::InvalidInput("Unknown tool adapter variant.")),
        }
    }
}

/// Configured external CLI tool or API provider entry in the global registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolDefinition {
    /// Stable unique identifier for this tool entry.
    pub id: String,
    /// Human-readable display label.
    pub label: String,
    /// Adapter type determining how the tool is probed or invoked.
    pub adapter: ToolAdapter,
    /// Optional filesystem path to the executable binary (`null` when unset or API-based).
    pub executable_path: Option<PathBuf>,
    /// Whether this tool is currently enabled for selection and probing.
    pub enabled: bool,
}

/// Registered model definition associated with a specific [`ToolDefinition`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelDefinition {
    /// Stable unique identifier for this model entry in the registry.
    pub id: String,
    /// Identifier of the owning [`ToolDefinition`].
    pub tool_id: String,
    /// Human-readable display label.
    pub label: String,
    /// Exact provider/CLI model argument passed to the underlying tool.
    pub model_arg: String,
    /// Supported reasoning or effort levels for this model in display order.
    pub efforts: Vec<String>,
    /// Whether this model may be selected with `effort: None` (`null`).
    pub allow_no_effort: bool,
}

impl ModelDefinition {
    /// Returns `true` if `effort` is valid for this model definition.
    pub fn supports_effort(&self, effort: Option<&str>) -> bool {
        match effort {
            Some(level) => self.efforts.iter().any(|candidate| candidate == level),
            None => self.allow_no_effort,
        }
    }
}

/// Concrete model and optional effort selection assigned to a workflow role.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelChoice {
    /// Identifier of the selected [`ModelDefinition`].
    pub model_id: String,
    /// Selected reasoning/effort level, or `None` (`null`) when no effort is used.
    pub effort: Option<String>,
}

/// Canonical Threadlane workflow roles, including the five configurable execution roles and
/// the legacy [`Self::Follower`] variant retained for historical snapshot/plan compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowRole {
    /// Legacy conversational follower role retained for backward-compatible deserialization.
    ///
    /// Active native Coworker chat model selection is controlled by the human in Codex and is
    /// not configured through profile execution role assignments.
    #[serde(rename = "follower")]
    Follower,
    /// Substantive workflow orchestrator role.
    #[serde(rename = "orchestrator")]
    Orchestrator,
    /// System and task architect role.
    #[serde(rename = "architect")]
    Architect,
    /// Task execution worker role (user-facing label `Worker`; canonical wire and storage identifier `"coder"`).
    #[serde(rename = "coder")]
    Coder,
    /// Task verification and code reviewer role.
    #[serde(rename = "reviewer")]
    Reviewer,
    /// Package-closing reviewer role.
    #[serde(rename = "closure")]
    Closure,
}

impl WorkflowRole {
    /// All six [`WorkflowRole`] variants in canonical workflow order, including legacy [`Self::Follower`].
    pub const ALL: [Self; 6] = [
        Self::Follower,
        Self::Orchestrator,
        Self::Architect,
        Self::Coder,
        Self::Reviewer,
        Self::Closure,
    ];

    /// The five configurable execution roles in Threadlane profiles (`Orchestrator`, `Architect`, `Coder` / user-facing `Worker`, `Reviewer`, `Closure`).
    ///
    /// Native Coworker chat model selection is controlled by the human in Codex and is outside
    /// profile execution role assignments.
    pub const EXECUTION_ROLES: [Self; 5] = [
        Self::Orchestrator,
        Self::Architect,
        Self::Coder,
        Self::Reviewer,
        Self::Closure,
    ];

    /// Returns `true` when `self` is one of the five configurable execution roles.
    pub const fn is_execution_role(self) -> bool {
        !matches!(self, Self::Follower)
    }

    /// Returns `true` when `self` is the legacy [`Self::Follower`] variant retained for historical deserialization.
    pub const fn is_legacy_follower(self) -> bool {
        matches!(self, Self::Follower)
    }

    /// Returns the canonical `snake_case` identifier for this role (`"coder"` for [`Self::Coder`]).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Follower => "follower",
            Self::Orchestrator => "orchestrator",
            Self::Architect => "architect",
            Self::Coder => "coder",
            Self::Reviewer => "reviewer",
            Self::Closure => "closure",
        }
    }

    /// Returns the human-readable user-facing display label for this role (`"Worker"` for [`Self::Coder`]).
    pub const fn display_label(self) -> &'static str {
        match self {
            Self::Follower => "Follower",
            Self::Orchestrator => "Orchestrator",
            Self::Architect => "Architect",
            Self::Coder => "Worker",
            Self::Reviewer => "Reviewer",
            Self::Closure => "Closure",
        }
    }
}

impl fmt::Display for WorkflowRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for WorkflowRole {
    type Err = RegistryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "follower" => Ok(Self::Follower),
            "orchestrator" => Ok(Self::Orchestrator),
            "architect" => Ok(Self::Architect),
            "coder" | "worker" => Ok(Self::Coder),
            "reviewer" => Ok(Self::Reviewer),
            "closure" => Ok(Self::Closure),
            _ => Err(RegistryError::InvalidInput(
                "Unknown workflow role variant.",
            )),
        }
    }
}

/// Task difficulty tier used to specialize [`WorkflowRole`] assignments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Difficulty {
    /// Routine, low-complexity work.
    #[serde(rename = "easy")]
    Easy,
    /// Standard moderate-complexity work.
    #[serde(rename = "medium")]
    Medium,
    /// Complex implementation or reasoning work.
    #[serde(rename = "hard")]
    Hard,
    /// Highest-complexity architectural or closure work.
    #[serde(rename = "extreme")]
    Extreme,
}

impl Difficulty {
    /// All four [`Difficulty`] tiers in ascending order.
    pub const ALL: [Self; 4] = [Self::Easy, Self::Medium, Self::Hard, Self::Extreme];

    /// Returns the canonical `snake_case` identifier for this difficulty tier.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Easy => "easy",
            Self::Medium => "medium",
            Self::Hard => "hard",
            Self::Extreme => "extreme",
        }
    }
}

impl fmt::Display for Difficulty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Difficulty {
    type Err = RegistryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "easy" => Ok(Self::Easy),
            "medium" => Ok(Self::Medium),
            "hard" => Ok(Self::Hard),
            "extreme" => Ok(Self::Extreme),
            _ => Err(RegistryError::InvalidInput("Unknown difficulty variant.")),
        }
    }
}

/// Mapping from a [`WorkflowRole`] and optional [`Difficulty`] tier to a [`ModelChoice`].
///
/// When `difficulty` is `None` (`null` in JSON), the assignment acts as the explicit
/// role-level default fallback when no exact difficulty match is configured for that role.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleAssignment {
    /// Target workflow role.
    pub role: WorkflowRole,
    /// Specific difficulty tier, or `None` (`null`) for the explicit role default.
    pub difficulty: Option<Difficulty>,
    /// Selected model and optional effort.
    pub choice: ModelChoice,
}

/// Operating mode for a workflow profile's [`DecisionAdvisor`].
///
/// Only [`Self::Shadow`] is supported; advisor evaluations are observational and never
/// automatically modify plans, models, permissions, or approvals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionAdvisorMode {
    /// Shadow-only advisory mode.
    #[serde(rename = "shadow")]
    Shadow,
}

impl DecisionAdvisorMode {
    /// All supported [`DecisionAdvisorMode`] variants in canonical order.
    pub const ALL: [Self; 1] = [Self::Shadow];

    /// Returns the canonical `snake_case` wire identifier for this mode.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Shadow => "shadow",
        }
    }
}

impl fmt::Display for DecisionAdvisorMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for DecisionAdvisorMode {
    type Err = RegistryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "shadow" => Ok(Self::Shadow),
            _ => Err(RegistryError::InvalidInput(
                "Unknown decision advisor mode variant.",
            )),
        }
    }
}

/// Optional decision advisor model selection for a [`WorkflowProfile`], separate from the
/// six worker [`WorkflowRole`] assignments.
///
/// Selecting a decision advisor in the registry is static configuration only; it does not
/// perform live API inference, verify credentials, or authorize provider execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionAdvisor {
    /// Identifier of the selected [`ModelDefinition`].
    pub model_id: String,
    /// Operating mode for the decision advisor.
    pub mode: DecisionAdvisorMode,
}

/// Named collection of [`RoleAssignment`] rules and optional [`DecisionAdvisor`] configuration.
///
/// Partial profiles (profiles that omit one or more roles or difficulties) can be saved;
/// unassigned roles/difficulties produce an explicit error at resolution time rather than
/// substituting an unapproved model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowProfile {
    /// Stable unique profile identifier.
    pub id: String,
    /// Human-readable profile name.
    pub name: String,
    /// Role and difficulty assignments configured for this profile.
    pub assignments: Vec<RoleAssignment>,
    /// Optional decision advisor configuration (`None` when disabled/unset).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_advisor: Option<DecisionAdvisor>,
}

impl WorkflowProfile {
    /// Iterates over role assignments that belong to the five configurable execution roles,
    /// excluding any legacy [`WorkflowRole::Follower`] entries.
    pub fn execution_assignments(&self) -> impl Iterator<Item = &RoleAssignment> {
        self.assignments
            .iter()
            .filter(|assignment| assignment.role.is_execution_role())
    }

    /// Returns `true` when every role in [`WorkflowRole::EXECUTION_ROLES`] has an explicit
    /// default assignment (`difficulty: None`).
    pub fn has_complete_execution_defaults(&self) -> bool {
        WorkflowRole::EXECUTION_ROLES.iter().all(|&role| {
            self.assignments
                .iter()
                .any(|assignment| assignment.role == role && assignment.difficulty.is_none())
        })
    }
}

/// Complete versioned state of the global tool, model, and workflow profile registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistrySnapshot {
    /// Wire and storage schema version (`1`).
    pub schema_version: u32,
    /// Monotonically increasing compare-and-swap revision (`0` when newly initialized/empty).
    pub revision: u64,
    /// Registered tools.
    pub tools: Vec<ToolDefinition>,
    /// Registered models referencing entries in `tools`.
    pub models: Vec<ModelDefinition>,
    /// Saved workflow profiles referencing entries in `models`.
    pub profiles: Vec<WorkflowProfile>,
    /// Optional default workflow profile identifier (`null` when unset).
    pub default_profile_id: Option<String>,
}

impl RegistrySnapshot {
    /// Creates an empty revision-0 snapshot with no implicit tools, models, or profiles.
    pub const fn empty() -> Self {
        Self {
            schema_version: REGISTRY_SCHEMA_VERSION,
            revision: 0,
            tools: Vec::new(),
            models: Vec::new(),
            profiles: Vec::new(),
            default_profile_id: None,
        }
    }
}

impl Default for RegistrySnapshot {
    fn default() -> Self {
        Self::empty()
    }
}

/// Atomic mutation operation applied to a [`RegistrySnapshot`] via [`crate::RegistryStore::apply`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum RegistryChange {
    /// Inserts a new [`ToolDefinition`] or replaces an existing tool with the same `id`.
    #[serde(rename = "upsert_tool")]
    UpsertTool {
        /// Tool definition to insert or update.
        tool: ToolDefinition,
    },
    /// Deletes an existing [`ToolDefinition`] if no models reference `tool_id`.
    #[serde(rename = "delete_tool")]
    DeleteTool {
        /// Identifier of the tool to delete.
        tool_id: String,
    },
    /// Inserts a new [`ModelDefinition`] or replaces an existing model with the same `id`.
    #[serde(rename = "upsert_model")]
    UpsertModel {
        /// Model definition to insert or update.
        model: ModelDefinition,
    },
    /// Inserts or updates a batch of [`ModelDefinition`] entries atomically in a single revision.
    #[serde(rename = "upsert_models")]
    UpsertModels {
        /// Batch of model definitions to insert or update.
        models: Vec<ModelDefinition>,
    },
    /// Deletes an existing [`ModelDefinition`] if no workflow profile role assignments or
    /// decision advisors reference `model_id`.
    #[serde(rename = "delete_model")]
    DeleteModel {
        /// Identifier of the model to delete.
        model_id: String,
    },
    /// Inserts a new [`WorkflowProfile`] or replaces an existing profile with the same `id`.
    #[serde(rename = "upsert_profile")]
    UpsertProfile {
        /// Workflow profile to insert or update.
        profile: WorkflowProfile,
    },
    /// Deletes an existing [`WorkflowProfile`] by `profile_id`.
    #[serde(rename = "delete_profile")]
    DeleteProfile {
        /// Identifier of the workflow profile to delete.
        profile_id: String,
    },
    /// Sets or clears (`None`) the active default workflow profile.
    #[serde(rename = "set_default_profile")]
    SetDefaultProfile {
        /// Profile identifier to mark as default, or `None` (`null`) to clear.
        #[serde(default)]
        profile_id: Option<String>,
    },
}

/// Authority-relevant tool properties bound into a [`ScopedProfileDependencySpec`].
///
/// Captures the exact execution identity of a referenced tool (`id`, `adapter`,
/// `executable_path`, and `enabled`) while omitting display-only labels so unrelated
/// cosmetic label edits do not alter execution semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedToolDependency {
    /// Unique tool identifier (`[a-zA-Z0-9_-]{1,64}`).
    pub id: String,
    /// Execution adapter type.
    pub adapter: ToolAdapter,
    /// Optional absolute or configured CLI executable path.
    pub executable_path: Option<PathBuf>,
    /// Whether this tool is currently enabled for workflow execution.
    pub enabled: bool,
}

/// Authority-relevant model properties bound into a [`ScopedProfileDependencySpec`].
///
/// Captures the exact execution identity of a referenced model (`id`, `tool_id`,
/// `model_arg`, `efforts`, and `allow_no_effort`) while omitting display-only labels.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedModelDependency {
    /// Unique model identifier (`[a-zA-Z0-9_-]{1,64}`).
    pub id: String,
    /// Referenced [`ToolDefinition::id`].
    pub tool_id: String,
    /// Provider-facing model argument string passed to the adapter.
    pub model_arg: String,
    /// Supported reasoning/execution effort levels for this model.
    pub efforts: Vec<String>,
    /// Whether the model can be invoked without an explicit effort level (`effort: None`).
    pub allow_no_effort: bool,
}

/// Canonical, self-contained snapshot of all authority-relevant registry dependencies for a
/// single selected [`WorkflowProfile`].
///
/// Includes the selected `profile_id`, its canonical execution role assignments, every model
/// referenced by those assignments, and every tool referenced by those models. Unrelated profiles,
/// unreferenced models/tools, `default_profile_id`, and cosmetic display labels (`Profile::name`,
/// `ModelDefinition::label`, `ToolDefinition::label`) are excluded so unrelated registry edits
/// cannot invalidate an unchanged profile's standing planning authority, while any semantic change
/// to the selected profile's assignments, models, efforts, or tools is detected deterministically.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedProfileDependencySpec {
    /// Selected workflow profile identifier.
    pub profile_id: String,
    /// Canonical execution role assignments sorted by `(role, difficulty)`.
    pub assignments: Vec<RoleAssignment>,
    /// Referenced model definitions sorted by `id`.
    pub models: Vec<ScopedModelDependency>,
    /// Referenced tool definitions sorted by `id`.
    pub tools: Vec<ScopedToolDependency>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn workflow_role_display_label_from_str_alias_and_strict_serde_contract() {
        assert_eq!(WorkflowRole::Follower.as_str(), "follower");
        assert_eq!(WorkflowRole::Orchestrator.as_str(), "orchestrator");
        assert_eq!(WorkflowRole::Architect.as_str(), "architect");
        assert_eq!(WorkflowRole::Coder.as_str(), "coder");
        assert_eq!(WorkflowRole::Reviewer.as_str(), "reviewer");
        assert_eq!(WorkflowRole::Closure.as_str(), "closure");

        assert_eq!(WorkflowRole::Coder.to_string(), "coder");

        assert_eq!(WorkflowRole::Follower.display_label(), "Follower");
        assert_eq!(WorkflowRole::Orchestrator.display_label(), "Orchestrator");
        assert_eq!(WorkflowRole::Architect.display_label(), "Architect");
        assert_eq!(WorkflowRole::Coder.display_label(), "Worker");
        assert_eq!(WorkflowRole::Reviewer.display_label(), "Reviewer");
        assert_eq!(WorkflowRole::Closure.display_label(), "Closure");

        assert_eq!("coder".parse::<WorkflowRole>(), Ok(WorkflowRole::Coder));
        assert_eq!("worker".parse::<WorkflowRole>(), Ok(WorkflowRole::Coder));
        for role in WorkflowRole::ALL {
            assert_eq!(role.as_str().parse::<WorkflowRole>(), Ok(role));
        }
        for unknown in ["Worker", "CODER", "builder", "executor", "", "coder "] {
            assert_eq!(
                unknown.parse::<WorkflowRole>(),
                Err(RegistryError::InvalidInput(
                    "Unknown workflow role variant."
                )),
                "unknown role `{unknown}` must be rejected by FromStr"
            );
        }

        // Canonical serde stays "coder" and rejects "worker" so map keys can never collide.
        assert_eq!(
            serde_json::to_string(&WorkflowRole::Coder).expect("serialize Coder"),
            "\"coder\""
        );
        assert_eq!(
            serde_json::from_str::<WorkflowRole>("\"coder\"").expect("deserialize coder"),
            WorkflowRole::Coder
        );
        for rejected in ["\"worker\"", "\"Worker\"", "\"builder\"", "\"unknown\""] {
            assert!(
                serde_json::from_str::<WorkflowRole>(rejected).is_err(),
                "core WorkflowRole serde must reject {rejected}"
            );
        }

        let valid_map: BTreeMap<WorkflowRole, &str> =
            serde_json::from_str(r#"{"coder":"argon-high","reviewer":"astra-high"}"#)
                .expect("deserialize canonical role map");
        assert_eq!(valid_map.get(&WorkflowRole::Coder), Some(&"argon-high"));
        assert!(
            serde_json::from_str::<BTreeMap<WorkflowRole, String>>(r#"{"worker":"argon-high"}"#)
                .is_err(),
            "BTreeMap<WorkflowRole, _> must reject worker key"
        );
        assert!(
            serde_json::from_str::<BTreeMap<WorkflowRole, String>>(
                r#"{"coder":"argon-high","worker":"override-attempt"}"#
            )
            .is_err(),
            "simultaneous coder and worker keys in a role map must fail closed"
        );
    }
}
