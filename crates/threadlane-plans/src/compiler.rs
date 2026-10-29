use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_registry::{
    resolve_choice, Difficulty, ModelChoice, RegistrySnapshot, ToolAdapter, WorkflowRole,
};

use crate::error::PlanError;
use crate::types::{PlanRevision, PlanStatus};
use crate::validation::validate_plan_revision;

/// Immutable compiled task specification produced by resolving a [` crate::PlanTask`] against a
/// pinned [`RegistrySnapshot`].
///
/// Contains the frozen [`ModelChoice`], tool metadata, validated dependencies, and repository-relative
/// write allowlist. Compilation is completely side-effect-free and never registers dynamic profiles
/// or spawns processes in the runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledTask {
    /// Unique task identifier within the plan.
    pub id: String,
    /// Human-readable task title.
    pub title: String,
    /// Frozen task instruction prompt.
    pub instruction: String,
    /// Workflow role assigned to the task.
    pub role: WorkflowRole,
    /// Task difficulty assigned to the task.
    pub difficulty: Difficulty,
    /// Immutable resolved model and optional effort from the workflow profile.
    pub choice: ModelChoice,
    /// Identifier of the enabled tool owning `choice.model_id`.
    pub tool_id: String,
    /// Tool adapter kind from the registry tool definition.
    pub adapter: ToolAdapter,
    /// Optional configured executable path from the registry tool definition.
    pub executable_path: Option<PathBuf>,
    /// Exact CLI/API model argument string from the registry model definition.
    pub model_arg: String,
    /// Validated upstream dependency task identifiers within the plan.
    pub depends_on: Vec<String>,
    /// Approved repository-relative write paths for this task.
    pub allowed_write_paths: Vec<String>,
    /// 64-character lowercase hex SHA-256 digest of `instruction`.
    pub instruction_digest: String,
    /// 64-character lowercase hex SHA-256 digest of the resolved choice, tool, dependencies, and write scope.
    pub scope_and_choice_digest: String,
    /// 64-character lowercase hex SHA-256 digest of the complete compiled task specification.
    pub task_digest: String,
}

/// Immutable compiled runtime snapshot for a published [`PlanRevision`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledPlan {
    /// Logical plan identifier.
    pub plan_id: String,
    /// Chat binding identifier owning the plan.
    pub binding_id: String,
    /// Published plan revision number.
    pub plan_revision: u64,
    /// Workflow profile identifier used for resolution.
    pub profile_id: String,
    /// Registry revision pinned by the plan revision.
    pub profile_revision: u64,
    /// Lifecycle status of the plan revision at compilation time.
    pub status: PlanStatus,
    /// Deterministic topological execution order of task identifiers.
    pub topological_order: Vec<String>,
    /// Immutable compiled tasks in plan declaration order.
    pub tasks: Vec<CompiledTask>,
    /// 64-character lowercase hex SHA-256 digest of the compiled plan snapshot.
    pub plan_digest: String,
}

impl CompiledPlan {
    /// Looks up a compiled task by `task_id`.
    pub fn task(&self, task_id: &str) -> Option<&CompiledTask> {
        self.tasks.iter().find(|task| task.id == task_id)
    }
}

/// Pure compiler that resolves a published [`PlanRevision`] (`published`, `received`, or `applied`)
/// against a [`RegistrySnapshot`] at the exact pinned `profile_revision`.
///
/// Rejects `draft` and `superseded` plan revisions, mismatched registry revisions, missing role/difficulty
/// assignments, disabled tools, unknown models, or unsupported efforts. Never performs fallback model
/// substitution or side effects.
pub fn compile_plan_revision(
    plan: &PlanRevision,
    registry: &RegistrySnapshot,
) -> Result<CompiledPlan, PlanError> {
    if !plan.status.is_active_published() {
        return Err(PlanError::UnpublishedPlan {
            plan_id: plan.plan_id.clone(),
            revision: plan.revision,
            status: plan.status,
        });
    }
    compile_internal(plan, registry)
}

/// Pure dry-run compiler that validates whether a `draft` (or active published) [`PlanRevision`]
/// resolves cleanly against `registry` at `plan.profile_revision`.
///
/// Still rejects `superseded` revisions.
pub fn preview_draft_compilation(
    plan: &PlanRevision,
    registry: &RegistrySnapshot,
) -> Result<CompiledPlan, PlanError> {
    if plan.status == PlanStatus::Superseded {
        return Err(PlanError::UnpublishedPlan {
            plan_id: plan.plan_id.clone(),
            revision: plan.revision,
            status: plan.status,
        });
    }
    compile_internal(plan, registry)
}

fn compile_internal(
    plan: &PlanRevision,
    registry: &RegistrySnapshot,
) -> Result<CompiledPlan, PlanError> {
    let topological_order = validate_plan_revision(plan)?;

    if registry.revision != plan.profile_revision {
        return Err(PlanError::ProfileRevisionMismatch {
            plan_id: plan.plan_id.clone(),
            plan_revision: plan.revision,
            profile_id: plan.profile_id.clone(),
            expected_profile_revision: plan.profile_revision,
            actual_registry_revision: registry.revision,
        });
    }

    let mut compiled_tasks = Vec::with_capacity(plan.tasks.len());
    let mut task_digests = Vec::with_capacity(plan.tasks.len());

    for task in &plan.tasks {
        let choice = resolve_choice(registry, &plan.profile_id, task.role, Some(task.difficulty))
            .map_err(|err| {
            PlanError::from_registry_error(&plan.plan_id, &task.id, &plan.profile_id, &err)
        })?;

        let Some(model_def) = registry
            .models
            .iter()
            .find(|model| model.id == choice.model_id)
        else {
            return Err(PlanError::RegistryResolution {
                plan_id: plan.plan_id.clone(),
                task_id: task.id.clone(),
                profile_id: plan.profile_id.clone(),
                registry_code: "unknown_model".to_string(),
                reason: format!(
                    "resolved model `{}` does not exist in registry snapshot",
                    choice.model_id
                ),
            });
        };

        match &choice.effort {
            Some(effort) => {
                if !model_def.efforts.iter().any(|allowed| allowed == effort) {
                    return Err(PlanError::RegistryResolution {
                        plan_id: plan.plan_id.clone(),
                        task_id: task.id.clone(),
                        profile_id: plan.profile_id.clone(),
                        registry_code: "unsupported_effort".to_string(),
                        reason: format!(
                            "effort `{effort}` is not supported by model `{}`",
                            model_def.id
                        ),
                    });
                }
            }
            None => {
                if !model_def.allow_no_effort {
                    return Err(PlanError::RegistryResolution {
                        plan_id: plan.plan_id.clone(),
                        task_id: task.id.clone(),
                        profile_id: plan.profile_id.clone(),
                        registry_code: "missing_effort".to_string(),
                        reason: format!(
                            "model `{}` requires an explicit effort level",
                            model_def.id
                        ),
                    });
                }
            }
        }

        let Some(tool_def) = registry
            .tools
            .iter()
            .find(|tool| tool.id == model_def.tool_id)
        else {
            return Err(PlanError::RegistryResolution {
                plan_id: plan.plan_id.clone(),
                task_id: task.id.clone(),
                profile_id: plan.profile_id.clone(),
                registry_code: "unknown_tool".to_string(),
                reason: format!(
                    "tool `{}` for model `{}` does not exist in registry snapshot",
                    model_def.tool_id, model_def.id
                ),
            });
        };

        if !tool_def.enabled {
            return Err(PlanError::RegistryResolution {
                plan_id: plan.plan_id.clone(),
                task_id: task.id.clone(),
                profile_id: plan.profile_id.clone(),
                registry_code: "tool_disabled".to_string(),
                reason: format!("tool `{}` is disabled in registry snapshot", tool_def.id),
            });
        }

        let instruction_digest = sha256_hex(task.instruction.as_bytes());
        let scope_and_choice_payload = serde_json::to_vec(&(
            task.role,
            task.difficulty,
            &choice.model_id,
            &choice.effort,
            &tool_def.id,
            &tool_def.adapter,
            &tool_def.executable_path,
            &model_def.model_arg,
            &task.depends_on,
            &task.allowed_write_paths,
        ))?;
        let scope_and_choice_digest = sha256_hex(&scope_and_choice_payload);

        let full_task_payload = serde_json::to_vec(&(
            &task.id,
            &task.title,
            &instruction_digest,
            &scope_and_choice_digest,
        ))?;
        let task_digest = sha256_hex(&full_task_payload);
        task_digests.push((task.id.clone(), task_digest.clone()));

        compiled_tasks.push(CompiledTask {
            id: task.id.clone(),
            title: task.title.clone(),
            instruction: task.instruction.clone(),
            role: task.role,
            difficulty: task.difficulty,
            choice,
            tool_id: tool_def.id.clone(),
            adapter: tool_def.adapter,
            executable_path: tool_def.executable_path.clone(),
            model_arg: model_def.model_arg.clone(),
            depends_on: task.depends_on.clone(),
            allowed_write_paths: task.allowed_write_paths.clone(),
            instruction_digest,
            scope_and_choice_digest,
            task_digest,
        });
    }

    let plan_digest_payload = serde_json::to_vec(&(
        &plan.plan_id,
        &plan.binding_id,
        plan.revision,
        &plan.profile_id,
        plan.profile_revision,
        &topological_order,
        &task_digests,
    ))?;
    let plan_digest = sha256_hex(&plan_digest_payload);

    Ok(CompiledPlan {
        plan_id: plan.plan_id.clone(),
        binding_id: plan.binding_id.clone(),
        plan_revision: plan.revision,
        profile_id: plan.profile_id.clone(),
        profile_revision: plan.profile_revision,
        status: plan.status,
        topological_order,
        tasks: compiled_tasks,
        plan_digest,
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}
