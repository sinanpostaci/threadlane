use std::collections::BTreeSet;
use std::fmt::{self, Write as _};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_workers::{resolve_model_target, WorkerProvider};
use threadlane_workflow::{
    Difficulty, ModelDefinition, RegistryError, RegistrySnapshot, RegistryStore, RoleAssignment,
    ToolAdapter, ToolDefinition, WorkflowRole,
};

pub use threadlane_workflow::resolve_choice;

use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::validate_bounded_identifier;

/// Current wire and storage schema version for [`ExecutionPolicy`].
pub const EXECUTION_POLICY_SCHEMA_VERSION: u32 = 1;

/// Maximum number of allow rules permitted in a single [`ExecutionPolicy`].
pub const MAX_POLICY_ALLOW_RULES: usize = 64;

/// Maximum number of role/difficulty entries permitted in a single [`PreflightRequest`].
pub const MAX_PREFLIGHT_ROLES: usize = 30;

const MAX_REGISTRY_ID_LEN: usize = 128;
const MAX_REGISTRY_LABEL_LEN: usize = 256;
const MAX_REGISTRY_MODEL_ARG_LEN: usize = 256;
const MAX_REGISTRY_EFFORT_LEN: usize = 64;
const MAX_REGISTRY_EFFORTS_PER_MODEL: usize = 32;
const MAX_REGISTRY_TOOLS: usize = 64;
const MAX_REGISTRY_MODELS: usize = 256;
const MAX_REGISTRY_PROFILES: usize = 64;
const MAX_JS_SAFE_REVISION: u64 = 9_007_199_254_740_991;
const REGISTRY_SCHEMA_VERSION: u32 = 1;

/// Explicit exact-choice allow rule within an [`ExecutionPolicy`].
///
/// Permits a specific `(tool_id, model_id)` pair with an explicit list of allowed reasoning
/// `efforts`, an explicit `allow_no_effort` flag, and optional workflow `roles` restrictions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyAllowRule {
    /// Exact registry tool identifier permitted by this rule.
    pub tool_id: String,
    /// Exact registry model identifier permitted by this rule.
    pub model_id: String,
    /// Allowed reasoning/effort levels for `(tool_id, model_id)`.
    #[serde(default, alias = "allowed_efforts")]
    pub efforts: Vec<String>,
    /// Whether `effort: None` (`null`) is explicitly permitted for `(tool_id, model_id)`.
    #[serde(default)]
    pub allow_no_effort: bool,
    /// Optional workflow role restriction. When empty, this rule applies to all [`WorkflowRole`] variants.
    #[serde(default)]
    pub roles: Vec<WorkflowRole>,
}

impl PolicyAllowRule {
    /// Returns `true` if this rule applies to `role` (`roles` is empty or contains `role`).
    pub fn allows_role(&self, role: WorkflowRole) -> bool {
        self.roles.is_empty() || self.roles.contains(&role)
    }

    /// Returns `true` if this rule permits `effort`.
    pub fn allows_effort(&self, effort: Option<&str>) -> bool {
        match effort {
            Some(level) => self.efforts.iter().any(|allowed| allowed == level),
            None => self.allow_no_effort,
        }
    }
}

/// Generic deny-by-default operator execution policy for profile/role preflight and admission.
///
/// Contains a schema version, a stable `policy_id`, and a single explicit exact-choice
/// `allow_rules` list. An empty `allow_rules` list is valid and denies all role selections.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPolicy {
    /// Wire and storage schema version (`1`).
    pub schema_version: u32,
    /// Stable operator-assigned policy identifier.
    pub policy_id: String,
    /// Explicit exact-choice allow rules. Empty rules deny all selections.
    #[serde(default)]
    pub allow_rules: Vec<PolicyAllowRule>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
struct CanonicalAllowRule<'a> {
    tool_id: &'a str,
    model_id: &'a str,
    roles: Vec<WorkflowRole>,
    allow_no_effort: bool,
    efforts: Vec<&'a str>,
}

impl ExecutionPolicy {
    /// Constructs a validated deny-all [`ExecutionPolicy`] with empty `allow_rules`.
    pub fn deny_all(policy_id: impl Into<String>) -> Result<Self, NativeError> {
        let policy = Self {
            schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
            policy_id: policy_id.into(),
            allow_rules: Vec::new(),
        };
        policy.validate()?;
        Ok(policy)
    }

    /// Validates the policy schema version, identifier bounds, rule bounds, and uniqueness invariants.
    pub fn validate(&self) -> Result<(), NativeError> {
        let _ = self.canonical_rules()?;
        Ok(())
    }

    /// Validates the policy and returns a deterministic 64-character lowercase hex SHA-256 digest.
    ///
    /// Canonicalizes rule, role, and effort ordering so semantically identical policies always
    /// produce the exact same digest.
    pub fn digest(&self) -> Result<String, NativeError> {
        let canonical_rules = self.canonical_rules()?;
        let seed = (
            "threadlane-native:execution_policy:v1",
            self.schema_version,
            self.policy_id.as_str(),
            &canonical_rules,
        );
        let bytes = serde_json::to_vec(&seed).map_err(|_| {
            NativeError::new(
                NativeErrorCode::InvalidConfig,
                "failed to serialize execution policy for digest calculation",
            )
        })?;
        Ok(sha256_hex(&bytes))
    }

    /// Validates the policy and returns `true` if `(role, tool_id, model_id, effort)` is permitted.
    pub fn allows(
        &self,
        role: WorkflowRole,
        tool_id: &str,
        model_id: &str,
        effort: Option<&str>,
    ) -> Result<bool, NativeError> {
        self.validate()?;
        Ok(self
            .find_matching_rule(role, tool_id, model_id)
            .is_some_and(|rule| rule.allows_effort(effort)))
    }

    fn find_matching_rule(
        &self,
        role: WorkflowRole,
        tool_id: &str,
        model_id: &str,
    ) -> Option<&PolicyAllowRule> {
        self.allow_rules.iter().find(|rule| {
            rule.tool_id == tool_id && rule.model_id == model_id && rule.allows_role(role)
        })
    }

    fn canonical_rules(&self) -> Result<Vec<CanonicalAllowRule<'_>>, NativeError> {
        if self.schema_version != EXECUTION_POLICY_SCHEMA_VERSION {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "unsupported execution policy schema_version {} (expected {EXECUTION_POLICY_SCHEMA_VERSION})",
                    self.schema_version
                ),
            ));
        }
        validate_bounded_identifier(&self.policy_id, "policy_id", MAX_REGISTRY_ID_LEN)?;

        if self.allow_rules.len() > MAX_POLICY_ALLOW_RULES {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "execution policy allow_rules exceeds maximum of {MAX_POLICY_ALLOW_RULES} rules"
                ),
            ));
        }

        let mut covered_targets = BTreeSet::new();
        let mut canonical = Vec::with_capacity(self.allow_rules.len());

        for rule in &self.allow_rules {
            validate_registry_id(&rule.tool_id, "tool_id")?;
            validate_registry_id(&rule.model_id, "model_id")?;

            if rule.efforts.len() > MAX_REGISTRY_EFFORTS_PER_MODEL {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "policy allow rule efforts exceeds maximum of {MAX_REGISTRY_EFFORTS_PER_MODEL} entries"
                    ),
                ));
            }

            let mut effort_set = BTreeSet::new();
            for effort in &rule.efforts {
                validate_bounded_identifier(effort, "effort", MAX_REGISTRY_EFFORT_LEN)?;
                if !effort_set.insert(effort.as_str()) {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "policy allow rule contains duplicate effort entries",
                    ));
                }
            }

            if effort_set.is_empty() && !rule.allow_no_effort {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "policy allow rule must permit at least one effort or set allow_no_effort to true",
                ));
            }

            if rule.roles.len() > WorkflowRole::ALL.len() {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "policy allow rule roles exceeds workflow role count",
                ));
            }

            let mut role_set = BTreeSet::new();
            for &role in &rule.roles {
                if !role_set.insert(role) {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "policy allow rule contains duplicate workflow roles",
                    ));
                }
            }

            let effective_roles: &[WorkflowRole] = if rule.roles.is_empty() {
                &WorkflowRole::ALL
            } else {
                &rule.roles
            };
            for &role in effective_roles {
                if !covered_targets.insert((rule.tool_id.as_str(), rule.model_id.as_str(), role)) {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "execution policy contains duplicate or overlapping allow rules for the same tool, model, and role",
                    ));
                }
            }

            canonical.push(CanonicalAllowRule {
                tool_id: rule.tool_id.as_str(),
                model_id: rule.model_id.as_str(),
                roles: role_set.into_iter().collect(),
                allow_no_effort: rule.allow_no_effort,
                efforts: effort_set.into_iter().collect(),
            });
        }

        canonical.sort();
        Ok(canonical)
    }
}

/// Execution route classification for a resolved profile role assignment.
///
/// [`ToolAdapter::CodexCli`] maps to [`Self::NativeHandoff`] (never a headless worker).
/// [`ToolAdapter::Antigravity`] and [`ToolAdapter::OpencodeV2`] map to [`Self::HeadlessWorker`].
/// API and auxiliary adapters map to [`Self::Unsupported`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleExecutionRoute {
    /// Explicit native Codex conversational handoff route (`codex_cli`). Never a headless worker.
    NativeHandoff,
    /// Isolated headless CLI worker route (`antigravity` or `opencode_v2`).
    HeadlessWorker,
    /// Currently unsupported API or auxiliary tool route (`jev_api`, `openai_decisions_api`, `git`, `tmux`, `codegraph`).
    Unsupported,
}

impl RoleExecutionRoute {
    /// Derives the execution route for a [`ToolAdapter`].
    pub const fn from_adapter(adapter: ToolAdapter) -> Self {
        match adapter {
            ToolAdapter::CodexCli => Self::NativeHandoff,
            ToolAdapter::Antigravity | ToolAdapter::OpencodeV2 => Self::HeadlessWorker,
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
            Self::NativeHandoff => "native_handoff",
            Self::HeadlessWorker => "headless_worker",
            Self::Unsupported => "unsupported",
        }
    }

    /// Returns `true` if this route is a supported workflow execution route (`native_handoff` or `headless_worker`).
    pub const fn is_supported(self) -> bool {
        !matches!(self, Self::Unsupported)
    }
}

impl fmt::Display for RoleExecutionRoute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Concrete machine-readable block reason reported when a requested role cannot be admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreflightBlockReason {
    /// Caller requested the default profile (`profile_id: None`), but `default_profile_id` is unset in the registry.
    MissingDefaultProfile,
    /// Selected workflow profile identifier does not exist in the registry snapshot.
    MissingProfile,
    /// Selected profile has no assignment for the requested `(role, difficulty)` or explicit `(role, None)` default.
    MissingAssignment,
    /// Assigned `model_id` does not exist in the registry snapshot.
    MissingModel,
    /// Owning `tool_id` for the assigned model does not exist in the registry snapshot.
    MissingTool,
    /// Owning tool is disabled (`enabled: false`) in the registry snapshot.
    ToolDisabled,
    /// Assigned effort (or `None`) is not supported by the resolved [`ModelDefinition`].
    UnsupportedEffort,
    /// Owning tool uses an API or auxiliary adapter that is not a supported execution route.
    UnsupportedRoute,
    /// Resolved `(tool_id, model_id, effort, role)` choice is denied by the [`ExecutionPolicy`].
    PolicyBlocked,
}

impl PreflightBlockReason {
    /// Returns the canonical `snake_case` wire identifier for this block reason.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MissingDefaultProfile => "missing_default_profile",
            Self::MissingProfile => "missing_profile",
            Self::MissingAssignment => "missing_assignment",
            Self::MissingModel => "missing_model",
            Self::MissingTool => "missing_tool",
            Self::ToolDisabled => "tool_disabled",
            Self::UnsupportedEffort => "unsupported_effort",
            Self::UnsupportedRoute => "unsupported_route",
            Self::PolicyBlocked => "policy_blocked",
        }
    }
}

impl fmt::Display for PreflightBlockReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Single role and optional difficulty requested in a [`PreflightRequest`].
///
/// Never accepts caller-supplied model, effort, tool, or executable overrides.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolePreflightRequest {
    /// Target workflow role to resolve.
    pub role: WorkflowRole,
    /// Optional target difficulty tier (`None` resolves the explicit role default directly).
    #[serde(default)]
    pub difficulty: Option<Difficulty>,
}

impl RolePreflightRequest {
    /// Constructs a new [`RolePreflightRequest`].
    pub const fn new(role: WorkflowRole, difficulty: Option<Difficulty>) -> Self {
        Self { role, difficulty }
    }
}

/// Pure, read-only preflight request over a workflow profile selection and requested roles.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreflightRequest {
    /// Explicit workflow profile identifier, or `None` to select the registry's actual `default_profile_id`.
    #[serde(default)]
    pub profile_id: Option<String>,
    /// Non-empty, duplicate-free list of `(role, difficulty)` combinations to resolve and check.
    pub roles: Vec<RolePreflightRequest>,
}

/// Exact resolved tool, model, effort, and execution route for a permitted role in [`RolePreflightOutcome`].
///
/// `model_arg` and `effort` come exclusively from the resolved registry definition and profile
/// assignment, never from caller overrides.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectedRoleChoice {
    /// Resolved registry tool identifier.
    pub tool_id: String,
    /// Resolved registry tool adapter.
    pub adapter: ToolAdapter,
    /// Derived execution route (`native_handoff` or `headless_worker`).
    pub route: RoleExecutionRoute,
    /// Resolved registry model identifier.
    pub model_id: String,
    /// Exact provider/CLI model argument from the registry [`ModelDefinition`].
    pub model_arg: String,
    /// Resolved reasoning/effort level from the profile assignment, if any.
    pub effort: Option<String>,
    /// Difficulty tier on the matched [`RoleAssignment`] (`None` when resolved via explicit role default).
    pub matched_difficulty: Option<Difficulty>,
}

/// Per-role ready or blocked outcome returned in [`PreflightReport`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolePreflightOutcome {
    /// Requested workflow role.
    pub role: WorkflowRole,
    /// Requested difficulty tier (`None` when no difficulty override was requested).
    pub requested_difficulty: Option<Difficulty>,
    /// `true` when the role resolved cleanly and passed tool, effort, route, and policy checks.
    pub ready: bool,
    /// Concrete block reason when `ready` is `false`; `None` when `ready` is `true`.
    pub block_reason: Option<PreflightBlockReason>,
    /// Safe human-readable diagnostic summary describing the resolution or block reason.
    pub detail: String,
    /// Exact selected model, effort, and route when `ready` is `true`; `None` when blocked.
    pub selected: Option<SelectedRoleChoice>,
}

/// Complete pure preflight report for a [`PreflightRequest`] and [`ExecutionPolicy`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreflightReport {
    /// Registry snapshot revision inspected by this preflight check.
    pub registry_revision: u64,
    /// Explicit or default workflow profile identifier selected (`None` when `default_profile_id` is unset).
    pub profile_id: Option<String>,
    /// `true` when profile selection used the registry's `default_profile_id` (`request.profile_id` was `None`).
    pub used_default_profile: bool,
    /// Validated execution policy identifier.
    pub policy_id: String,
    /// Deterministic 64-character lowercase hex SHA-256 digest of the validated [`ExecutionPolicy`].
    pub policy_digest: String,
    /// `true` if and only if every requested entry in `roles` has `ready: true`.
    pub ready: bool,
    /// Per-role preflight outcomes in the exact order requested.
    pub roles: Vec<RolePreflightOutcome>,
}

/// Evaluates a pure, read-only profile and role preflight check against an existing [`RegistryStore`].
///
/// Reads a point-in-time [`RegistrySnapshot`] via [`RegistryStore::snapshot_readonly`] and
/// delegates to [`preflight`] without acquiring or creating `registry.lock` and without mutating
/// the registry or creating any workflow cycle, host, or worktree.
pub fn preflight_from_store(
    registry: &RegistryStore,
    request: &PreflightRequest,
    policy: &ExecutionPolicy,
) -> Result<PreflightReport, NativeError> {
    let snapshot = registry.snapshot_readonly().map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to read registry snapshot for preflight: {err}"),
        )
    })?;
    preflight(&snapshot, request, policy)
}

/// Evaluates a pure, side-effect-free profile and role preflight check over a [`RegistrySnapshot`],
/// [`PreflightRequest`], and [`ExecutionPolicy`].
///
/// Selects either an explicit `request.profile_id` or `snapshot.default_profile_id` (never inferring
/// default selection from the profile name or ID `"default"`), resolves only the requested
/// `(role, difficulty)` combinations, and reports concrete per-role readiness or block reasons.
pub fn preflight(
    snapshot: &RegistrySnapshot,
    request: &PreflightRequest,
    policy: &ExecutionPolicy,
) -> Result<PreflightReport, NativeError> {
    let policy_digest = policy.digest()?;
    validate_snapshot_header(snapshot)?;
    validate_preflight_request(request)?;

    let used_default_profile = request.profile_id.is_none();
    let Some(target_profile_id) = request
        .profile_id
        .as_deref()
        .or(snapshot.default_profile_id.as_deref())
    else {
        let roles = request
            .roles
            .iter()
            .map(|req| {
                blocked_outcome(
                    req,
                    PreflightBlockReason::MissingDefaultProfile,
                    "registry snapshot has no default_profile_id configured".to_string(),
                )
            })
            .collect();
        return Ok(PreflightReport {
            registry_revision: snapshot.revision,
            profile_id: None,
            used_default_profile: true,
            policy_id: policy.policy_id.clone(),
            policy_digest,
            ready: false,
            roles,
        });
    };

    let Some(profile) = snapshot
        .profiles
        .iter()
        .find(|candidate| candidate.id == target_profile_id)
    else {
        let roles = request
            .roles
            .iter()
            .map(|req| {
                blocked_outcome(
                    req,
                    PreflightBlockReason::MissingProfile,
                    format!(
                        "workflow profile `{target_profile_id}` was not found in registry snapshot"
                    ),
                )
            })
            .collect();
        return Ok(PreflightReport {
            registry_revision: snapshot.revision,
            profile_id: Some(target_profile_id.to_string()),
            used_default_profile,
            policy_id: policy.policy_id.clone(),
            policy_digest,
            ready: false,
            roles,
        });
    };

    validate_profile_structure(profile)?;

    let mut outcomes = Vec::with_capacity(request.roles.len());
    for role_req in &request.roles {
        outcomes.push(evaluate_role_request(
            snapshot,
            &profile.id,
            &profile.assignments,
            role_req,
            policy,
        )?);
    }

    let ready = outcomes.iter().all(|outcome| outcome.ready);
    Ok(PreflightReport {
        registry_revision: snapshot.revision,
        profile_id: Some(profile.id.clone()),
        used_default_profile,
        policy_id: policy.policy_id.clone(),
        policy_digest,
        ready,
        roles: outcomes,
    })
}

fn evaluate_role_request(
    snapshot: &RegistrySnapshot,
    profile_id: &str,
    assignments: &[RoleAssignment],
    role_req: &RolePreflightRequest,
    policy: &ExecutionPolicy,
) -> Result<RolePreflightOutcome, NativeError> {
    let Some(assignment) = find_role_assignment(assignments, role_req.role, role_req.difficulty)
    else {
        let diff_desc = match role_req.difficulty {
            Some(diff) => format!("difficulty `{diff}` or role default"),
            None => "explicit role default".to_string(),
        };
        return Ok(blocked_outcome(
            role_req,
            PreflightBlockReason::MissingAssignment,
            format!(
                "profile `{profile_id}` has no assignment for role `{}` ({diff_desc})",
                role_req.role
            ),
        ));
    };

    validate_registry_id(&assignment.choice.model_id, "assignment.choice.model_id")?;
    if let Some(ref effort) = assignment.choice.effort {
        validate_bounded_identifier(effort, "assignment.choice.effort", MAX_REGISTRY_EFFORT_LEN)?;
    }

    let Some(model) = snapshot
        .models
        .iter()
        .find(|candidate| candidate.id == assignment.choice.model_id)
    else {
        return Ok(blocked_outcome(
            role_req,
            PreflightBlockReason::MissingModel,
            format!(
                "assigned model `{}` was not found in registry snapshot",
                assignment.choice.model_id
            ),
        ));
    };
    validate_model_definition_structure(model)?;

    let Some(tool) = snapshot
        .tools
        .iter()
        .find(|candidate| candidate.id == model.tool_id)
    else {
        return Ok(blocked_outcome(
            role_req,
            PreflightBlockReason::MissingTool,
            format!(
                "assigned model `{}` references missing tool `{}`",
                model.id, model.tool_id
            ),
        ));
    };
    validate_tool_definition_structure(tool)?;

    if !tool.enabled {
        return Ok(blocked_outcome(
            role_req,
            PreflightBlockReason::ToolDisabled,
            format!(
                "tool `{}` ({}) is disabled in registry snapshot",
                tool.id, tool.adapter
            ),
        ));
    }

    let resolved_choice =
        match resolve_choice(snapshot, profile_id, role_req.role, role_req.difficulty) {
            Ok(choice) => choice,
            Err(RegistryError::UnsupportedEffort(_)) => {
                let eff_desc = assignment.choice.effort.as_deref().unwrap_or("none");
                return Ok(blocked_outcome(
                    role_req,
                    PreflightBlockReason::UnsupportedEffort,
                    format!(
                        "model `{}` does not support configured effort `{eff_desc}`",
                        model.id
                    ),
                ));
            }
            Err(err) => {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("registry resolution failed: {err}"),
                ));
            }
        };

    let route = RoleExecutionRoute::from_adapter(tool.adapter);
    if !route.is_supported() {
        return Ok(blocked_outcome(
            role_req,
            PreflightBlockReason::UnsupportedRoute,
            format!(
                "tool `{}` uses adapter `{}` which is not a supported execution route",
                tool.id, tool.adapter
            ),
        ));
    }

    let worker_provider = match tool.adapter {
        ToolAdapter::Antigravity => Some(WorkerProvider::Antigravity),
        ToolAdapter::OpencodeV2 => Some(WorkerProvider::OpenCodeV2),
        _ => None,
    };

    // Registry `supports_effort` metadata is necessary but not sufficient for an executable
    // worker route. Use the canonical pure worker target resolver so the same model, effort, and
    // OpenCode inline-variant rules that gate actual execution also gate admission. Native Codex
    // handoff remains an explicit route and does not resolve a worker model target here.
    let effective_effort = if let Some(provider) = worker_provider {
        match resolve_model_target(
            provider,
            &model.model_arg,
            resolved_choice.effort.as_deref(),
        ) {
            Ok(target) => target.effort,
            Err(err) => {
                return Ok(blocked_outcome(
                    role_req,
                    PreflightBlockReason::UnsupportedEffort,
                    format!(
                        "model `{}` has an invalid {} worker target `{}`: {err}",
                        model.id, tool.adapter, model.model_arg
                    ),
                ));
            }
        }
    } else {
        resolved_choice.effort.clone()
    };

    let Some(rule) = policy.find_matching_rule(role_req.role, &tool.id, &model.id) else {
        let reason = if policy.allow_rules.is_empty() {
            format!(
                "execution policy `{}` has no allow rules (deny-by-default)",
                policy.policy_id
            )
        } else {
            format!(
                "execution policy `{}` does not allow tool `{}` with model `{}` for role `{}`",
                policy.policy_id, tool.id, model.id, role_req.role
            )
        };
        return Ok(blocked_outcome(
            role_req,
            PreflightBlockReason::PolicyBlocked,
            reason,
        ));
    };

    if !rule.allows_effort(effective_effort.as_deref()) {
        let eff_desc = effective_effort.as_deref().unwrap_or("none");
        return Ok(blocked_outcome(
            role_req,
            PreflightBlockReason::PolicyBlocked,
            format!(
                "execution policy `{}` does not allow effort `{eff_desc}` on model `{}` for role `{}`",
                policy.policy_id, model.id, role_req.role
            ),
        ));
    }

    Ok(RolePreflightOutcome {
        role: role_req.role,
        requested_difficulty: role_req.difficulty,
        ready: true,
        block_reason: None,
        detail: format!(
            "role `{}` ready with model `{}` ({}) via tool `{}` ({route})",
            role_req.role, model.id, model.model_arg, tool.id
        ),
        selected: Some(SelectedRoleChoice {
            tool_id: tool.id.clone(),
            adapter: tool.adapter,
            route,
            model_id: model.id.clone(),
            model_arg: model.model_arg.clone(),
            effort: effective_effort,
            matched_difficulty: assignment.difficulty,
        }),
    })
}

fn find_role_assignment(
    assignments: &[RoleAssignment],
    role: WorkflowRole,
    difficulty: Option<Difficulty>,
) -> Option<&RoleAssignment> {
    match difficulty {
        Some(target_difficulty) => assignments
            .iter()
            .find(|entry| entry.role == role && entry.difficulty == Some(target_difficulty))
            .or_else(|| {
                assignments
                    .iter()
                    .find(|entry| entry.role == role && entry.difficulty.is_none())
            }),
        None => assignments
            .iter()
            .find(|entry| entry.role == role && entry.difficulty.is_none()),
    }
}

fn blocked_outcome(
    role_req: &RolePreflightRequest,
    reason: PreflightBlockReason,
    detail: String,
) -> RolePreflightOutcome {
    RolePreflightOutcome {
        role: role_req.role,
        requested_difficulty: role_req.difficulty,
        ready: false,
        block_reason: Some(reason),
        detail,
        selected: None,
    }
}

fn validate_preflight_request(request: &PreflightRequest) -> Result<(), NativeError> {
    if let Some(ref explicit_profile_id) = request.profile_id {
        validate_registry_id(explicit_profile_id, "profile_id")?;
    }

    if request.roles.is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "preflight request must specify at least one role",
        ));
    }

    if request.roles.len() > MAX_PREFLIGHT_ROLES {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("preflight request roles exceeds maximum of {MAX_PREFLIGHT_ROLES} entries"),
        ));
    }

    let mut seen = BTreeSet::new();
    for entry in &request.roles {
        if !seen.insert((entry.role, entry.difficulty)) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "preflight request contains duplicate (role, difficulty) entries",
            ));
        }
    }

    Ok(())
}

fn validate_snapshot_header(snapshot: &RegistrySnapshot) -> Result<(), NativeError> {
    if snapshot.schema_version != REGISTRY_SCHEMA_VERSION {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "unsupported registry snapshot schema_version {} (expected {REGISTRY_SCHEMA_VERSION})",
                snapshot.schema_version
            ),
        ));
    }
    if snapshot.revision > MAX_JS_SAFE_REVISION {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "registry snapshot revision exceeds maximum JS-safe integer",
        ));
    }
    if snapshot.tools.len() > MAX_REGISTRY_TOOLS
        || snapshot.models.len() > MAX_REGISTRY_MODELS
        || snapshot.profiles.len() > MAX_REGISTRY_PROFILES
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "registry snapshot collection count exceeds bounded capacity",
        ));
    }
    if let Some(ref default_id) = snapshot.default_profile_id {
        validate_registry_id(default_id, "default_profile_id")?;
    }
    Ok(())
}

fn validate_profile_structure(
    profile: &threadlane_workflow::WorkflowProfile,
) -> Result<(), NativeError> {
    validate_registry_id(&profile.id, "profile.id")?;
    validate_registry_text_native(&profile.name, "profile.name", MAX_REGISTRY_LABEL_LEN)?;
    if profile.assignments.len() > MAX_PREFLIGHT_ROLES {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "workflow profile assignment count exceeds maximum (30)",
        ));
    }
    validate_unique_assignments(&profile.assignments).map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("invalid workflow profile assignments: {err}"),
        )
    })?;
    Ok(())
}

fn validate_model_definition_structure(model: &ModelDefinition) -> Result<(), NativeError> {
    validate_registry_id(&model.id, "model.id")?;
    validate_registry_id(&model.tool_id, "model.tool_id")?;
    validate_registry_text_native(&model.label, "model.label", MAX_REGISTRY_LABEL_LEN)?;
    validate_registry_text_native(
        &model.model_arg,
        "model.model_arg",
        MAX_REGISTRY_MODEL_ARG_LEN,
    )?;
    if model.efforts.len() > MAX_REGISTRY_EFFORTS_PER_MODEL {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "model definition efforts exceeds maximum length",
        ));
    }
    let mut seen = BTreeSet::new();
    for effort in &model.efforts {
        validate_bounded_identifier(effort, "model.effort", MAX_REGISTRY_EFFORT_LEN)?;
        if !seen.insert(effort.as_str()) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "model definition contains duplicate effort levels",
            ));
        }
    }
    Ok(())
}

fn validate_tool_definition_structure(tool: &ToolDefinition) -> Result<(), NativeError> {
    validate_registry_id(&tool.id, "tool.id")?;
    validate_registry_text_native(&tool.label, "tool.label", MAX_REGISTRY_LABEL_LEN)?;
    Ok(())
}

fn validate_unique_assignments(assignments: &[RoleAssignment]) -> Result<(), RegistryError> {
    let mut seen = BTreeSet::new();
    for assignment in assignments {
        if !seen.insert((assignment.role, assignment.difficulty)) {
            return Err(RegistryError::DuplicateAssignment(
                "Workflow profile contains duplicate role and difficulty assignments.",
            ));
        }
    }
    Ok(())
}

fn validate_registry_text(value: &str, max_chars: usize) -> Result<(), RegistryError> {
    if value.is_empty() || value.trim() != value {
        return Err(RegistryError::InvalidInput(
            "Identifier or text field must be non-empty and trimmed.",
        ));
    }
    let char_count = value.chars().count();
    if char_count == 0 || char_count > max_chars {
        return Err(RegistryError::InvalidInput(
            "Identifier or text field exceeds maximum permitted length.",
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(RegistryError::InvalidInput(
            "Identifier or text field must not contain control characters.",
        ));
    }
    Ok(())
}

fn validate_registry_text_native(
    value: &str,
    field: &'static str,
    max_chars: usize,
) -> Result<(), NativeError> {
    validate_registry_text(value, max_chars).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{field} is empty, exceeds maximum length, or contains invalid characters"),
        )
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for b in digest {
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

fn validate_registry_id(value: &str, field: &str) -> Result<(), NativeError> {
    threadlane_workflow::validate_registry_identifier(value).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{field} failed canonical registry identifier validation"),
        )
    })
}
