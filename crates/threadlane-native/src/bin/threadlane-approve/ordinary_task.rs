//! Scoped single-task fast path for `threadlane-approve prepare-task`.
//!
//! The route accepts one bounded strict request on stdin, validates the fixed context scope and
//! every statically-checkable task/repository/budget invariant before the first mutation, creates
//! and publishes exactly one ordinary worker task through [`ScopedWorkflow`], and then reuses the
//! authoritative private preview flow. It never records authority, reserves execution, starts a
//! worker, or silently recovers an unknown effect.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Deserialize;
use threadlane::SupervisorClient;
use threadlane_authority::validation::{
    validate_canonical_thread_uuid, validate_executable_worker_instruction, validate_identifier,
    validate_repository_and_base_commit, validate_required_changed_paths_at_base_commit,
    validate_revision, validate_worker_task_static, validate_write_scope_entry_at_base_commit,
};
use threadlane_authority::{
    resolve_and_validate_worker_executable, OperatorApprovalIntent, OperatorApprovalIntentV2,
    AUTHORITY_REQUEST_V2_SCHEMA_VERSION,
};
use threadlane_native::open_workflow_stores;
use threadlane_workers::ExecutionBudgets;
use threadlane_workflow::{
    deterministic_plan_id, preview_draft_compilation, Difficulty, PlanError, PlanRevision,
    PlanStatus, PlanSummary, PlanTask, PublishedPlanView, RegistryError, SavePlanDraft,
    ScopedWorkflow, WorkflowError, WorkflowRole,
};

use super::{
    enforce_fixed_scope_on_request, load_stdin_json, optional_record_request,
    prepare_operator_preview, prepare_operator_preview_v2, verify_live_binding,
    write_private_preview_file, write_stdout_json, CliError, ExplicitBudgets, FixedApprovalScope,
    PrivatePreviewReceipt, RecoveryContext, ValidatedStoreDirs,
};

const SAVE_DRAFT_OUTCOME_UNKNOWN: &str = "workflow draft outcome is uncertain after an I/O, serialization, or response error; reconcile the original plan before retrying";
const PUBLISH_OUTCOME_UNKNOWN: &str = "workflow publication outcome is uncertain after an I/O, serialization, or response error; reconcile the original plan before retrying";
const RECEIPT_OUTCOME_UNKNOWN: &str = "the workflow plan or private preview may have been created, but writing the bounded stdout receipt failed";
const DRAFT_SAVED_PHASE: &str = "draft_saved";
const PUBLISHED_PHASE: &str = "published";

/// Strict stdin request accepted by the legacy `prepare-task` single-task fast path.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrepareTaskRequest {
    binding_id: String,
    thread_uuid: String,
    repository_path: PathBuf,
    base_commit: String,
    profile_id: String,
    profile_revision: u64,
    command_key: String,
    summary: String,
    task: PrepareTaskTask,
    budgets: ExecutionBudgets,
}

/// Strict stdin request accepted by `prepare-task-exact`, requiring all five explicit budgets and
/// supporting opt-in exact-file `task.required_changed_paths`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrepareTaskExactRequest {
    binding_id: String,
    thread_uuid: String,
    repository_path: PathBuf,
    base_commit: String,
    profile_id: String,
    profile_revision: u64,
    command_key: String,
    summary: String,
    task: PrepareTaskExactTask,
    budgets: ExplicitBudgets,
}

impl PrepareTaskExactRequest {
    fn into_parts(self) -> (PrepareTaskRequest, Option<Vec<String>>) {
        let PrepareTaskExactTask {
            id,
            title,
            instruction,
            role,
            difficulty,
            depends_on,
            allowed_write_paths,
            required_changed_paths,
        } = self.task;
        (
            PrepareTaskRequest {
                binding_id: self.binding_id,
                thread_uuid: self.thread_uuid,
                repository_path: self.repository_path,
                base_commit: self.base_commit,
                profile_id: self.profile_id,
                profile_revision: self.profile_revision,
                command_key: self.command_key,
                summary: self.summary,
                task: PrepareTaskTask {
                    id,
                    title,
                    instruction,
                    role,
                    difficulty,
                    depends_on,
                    allowed_write_paths,
                },
                budgets: self.budgets.into(),
            },
            required_changed_paths,
        )
    }
}

/// Strict single-task object accepted before conversion to the canonical [`PlanTask`].
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrepareTaskTask {
    id: String,
    title: String,
    instruction: String,
    role: PrepareTaskRole,
    difficulty: Difficulty,
    depends_on: Vec<String>,
    allowed_write_paths: Vec<String>,
}

/// Strict single-task object accepted by `prepare-task-exact`, extending [`PrepareTaskTask`] with
/// opt-in exact-file `required_changed_paths`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrepareTaskExactTask {
    id: String,
    title: String,
    instruction: String,
    role: PrepareTaskRole,
    difficulty: Difficulty,
    depends_on: Vec<String>,
    allowed_write_paths: Vec<String>,
    #[serde(default)]
    required_changed_paths: Option<Vec<String>>,
}

/// Narrow role input accepted only on the `prepare-task` stdin boundary.
///
/// The user-facing `"worker"` spelling and the canonical persisted `"coder"` identifier both
/// select [`WorkflowRole::Coder`]; every other role fails strict deserialization before mutation.
#[derive(Debug, Clone, Copy, Deserialize)]
enum PrepareTaskRole {
    #[serde(rename = "coder", alias = "worker")]
    Coder,
}

impl From<PrepareTaskRole> for WorkflowRole {
    fn from(role: PrepareTaskRole) -> Self {
        match role {
            PrepareTaskRole::Coder => Self::Coder,
        }
    }
}

impl From<PrepareTaskTask> for PlanTask {
    fn from(task: PrepareTaskTask) -> Self {
        Self {
            id: task.id,
            title: task.title,
            instruction: task.instruction,
            role: task.role.into(),
            difficulty: task.difficulty,
            depends_on: task.depends_on,
            allowed_write_paths: task.allowed_write_paths,
        }
    }
}

/// Builds bounded recovery context from an actual original mutation receipt.
fn recovery_context(summary: &PlanSummary, phase: &'static str) -> RecoveryContext {
    RecoveryContext {
        plan_id: summary.plan_id.clone(),
        revision: summary.revision,
        phase,
    }
}

/// Repository and task scope proven before the first draft mutation.
struct ValidatedRequestScope {
    binding_id: String,
    thread_uuid: String,
    canonical_repo: PathBuf,
    base_commit: String,
}

/// Creates and publishes one ordinary single-task plan, then writes its private preview receipt.
pub(super) async fn run_prepare_task(
    dirs: &ValidatedStoreDirs,
    fixed_scope: &FixedApprovalScope,
    exact_budgets: bool,
) -> Result<(), CliError> {
    let (request, required_changed_paths): (PrepareTaskRequest, Option<Vec<String>>) =
        if exact_budgets {
            let explicit: PrepareTaskExactRequest = load_stdin_json()?;
            explicit.into_parts()
        } else {
            (load_stdin_json()?, None)
        };
    let task = PlanTask::from(request.task.clone());
    let scope = prevalidate_request(dirs, fixed_scope, &request, &task)?;

    verify_live_binding_at(dirs, &scope.binding_id, &scope.thread_uuid).await?;

    let Some((registry, plans)) =
        open_workflow_stores(Some(&dirs.registry_dir), Some(&dirs.plans_dir))
            .map_err(|err| CliError::from_native(&err))?
    else {
        return Err(CliError::invalid_input(
            "workflow registry and plan stores could not be opened",
        ));
    };
    let workflow = ScopedWorkflow::new(
        Arc::clone(&registry),
        Arc::clone(&plans),
        scope.binding_id.clone(),
    )
    .map_err(|err| workflow_error(&err))?;

    let (validated_task_paths, validated_required_paths) = prevalidate_compiled_task(
        &workflow,
        &request,
        &task,
        required_changed_paths.as_deref(),
        &scope.canonical_repo,
        &scope.base_commit,
        dirs,
    )?;

    verify_live_binding_at(dirs, &scope.binding_id, &scope.thread_uuid).await?;

    let draft = SavePlanDraft {
        command_key: request.command_key.clone(),
        plan_id: None,
        expected_revision: 0,
        profile_id: request.profile_id.clone(),
        profile_revision: request.profile_revision,
        summary: request.summary.clone(),
        tasks: vec![task.clone()],
    };
    let saved = workflow
        .save_draft(draft)
        .map_err(|err| workflow_mutation_error(&err, SAVE_DRAFT_OUTCOME_UNKNOWN))?;

    let mut recovery = Some(recovery_context(&saved, DRAFT_SAVED_PHASE));
    finish_prepare_task(
        dirs,
        fixed_scope,
        &request,
        &task,
        &scope,
        &workflow,
        &validated_task_paths,
        validated_required_paths.as_deref(),
        &saved,
        &mut recovery,
    )
    .await
    .map_err(|err| err.with_recovery(recovery))
}

/// Completes publication, live revalidation, private preview, and the bounded receipt after the
/// original draft receipt exists, updating the bounded recovery context after each confirmed phase.
#[allow(clippy::too_many_arguments)]
async fn finish_prepare_task(
    dirs: &ValidatedStoreDirs,
    fixed_scope: &FixedApprovalScope,
    request: &PrepareTaskRequest,
    task: &PlanTask,
    scope: &ValidatedRequestScope,
    workflow: &ScopedWorkflow,
    validated_task_paths: &[String],
    validated_required_paths: Option<&[String]>,
    saved: &PlanSummary,
    recovery: &mut Option<RecoveryContext>,
) -> Result<(), CliError> {
    verify_persisted_draft(workflow, saved, request, task)?;

    verify_live_binding_at(dirs, &scope.binding_id, &scope.thread_uuid).await?;
    ensure_current_registry_revision(workflow, request.profile_revision)?;

    let published = workflow
        .publish(&saved.plan_id, saved.revision)
        .map_err(|err| workflow_mutation_error(&err, PUBLISH_OUTCOME_UNKNOWN))?;
    let published_header =
        validate_published_view(&published, saved, request, task, validated_task_paths)?;
    *recovery = Some(recovery_context(&published_header, PUBLISHED_PHASE));

    verify_live_binding_at(dirs, &scope.binding_id, &scope.thread_uuid).await?;

    let intent = OperatorApprovalIntent {
        binding_id: scope.binding_id.clone(),
        thread_uuid: scope.thread_uuid.clone(),
        plan_id: published_header.plan_id.clone(),
        plan_revision: published_header.revision,
        expected_registry_revision: published_header.profile_revision,
        repository_path: scope.canonical_repo.clone(),
        base_commit: scope.base_commit.clone(),
        approved_task_ids: vec![task.id.clone()],
        approved_write_paths: validated_task_paths.to_vec(),
        max_parallel: 1,
        budgets: request.budgets.clone(),
    };

    let preview = match validated_required_paths {
        Some(required_paths) => {
            let mut task_required_changed_paths = BTreeMap::new();
            task_required_changed_paths.insert(task.id.clone(), required_paths.to_vec());
            prepare_operator_preview_v2(
                dirs,
                Some(fixed_scope),
                OperatorApprovalIntentV2 {
                    schema_version: AUTHORITY_REQUEST_V2_SCHEMA_VERSION,
                    intent,
                    task_required_changed_paths,
                },
            )
            .await?
        }
        None => prepare_operator_preview(dirs, Some(fixed_scope), intent).await?,
    };
    validate_preview(
        &preview,
        request,
        task,
        &scope.canonical_repo,
        &scope.base_commit,
        validated_task_paths,
        validated_required_paths,
        &published_header,
    )?;

    let record_request = optional_record_request(&preview)?;
    let (preview_path, directory) = write_private_preview_file(&preview)?;
    let receipt = PrivatePreviewReceipt {
        preview_path,
        preview,
        record_request,
    };

    // The receipt is serialized before stdout so an oversized or failed response leaves no
    // private preview directory behind. A mutation already happened, so an output failure is
    // reported as `outcome_unknown` and never retried silently.
    write_stdout_json(&receipt, false)
        .map_err(|_| CliError::outcome_unknown(RECEIPT_OUTCOME_UNKNOWN))?;
    let _kept_path = directory.keep();
    Ok(())
}

/// Validates fixed scope, identifiers, repository/base commit, task shape, and budgets before any
/// store mutation.
fn prevalidate_request(
    dirs: &ValidatedStoreDirs,
    fixed_scope: &FixedApprovalScope,
    request: &PrepareTaskRequest,
    task: &PlanTask,
) -> Result<ValidatedRequestScope, CliError> {
    let binding_id = validate_identifier("binding_id", &request.binding_id)
        .map_err(|err| CliError::from_authority(&err))?;
    let thread_uuid = validate_canonical_thread_uuid("thread_uuid", &request.thread_uuid)
        .map_err(|err| CliError::from_authority(&err))?;
    enforce_fixed_scope_on_request(
        fixed_scope,
        &binding_id,
        &thread_uuid,
        &request.repository_path,
        None,
    )?;
    if fixed_scope.project_root.is_none() {
        return Err(CliError::invalid_input(
            "prepare-task requires --project-root together with --binding and --thread",
        ));
    }

    if task.role != WorkflowRole::Coder {
        return Err(CliError::invalid_input(
            "task role must be coder (the ordinary worker role)",
        ));
    }
    if !task.depends_on.is_empty() {
        return Err(CliError::invalid_input(
            "prepare-task accepts exactly one task with no dependencies",
        ));
    }

    let _ = validate_identifier("command_key", &request.command_key)
        .map_err(|err| CliError::from_authority(&err))?;
    let _ = validate_identifier("profile_id", &request.profile_id)
        .map_err(|err| CliError::from_authority(&err))?;
    let _ = validate_revision("profile_revision", request.profile_revision)
        .map_err(|err| CliError::from_authority(&err))?;
    if request.profile_revision == 0 {
        return Err(CliError::invalid_input(
            "profile_revision must identify an existing registry revision",
        ));
    }
    request
        .budgets
        .validate()
        .map_err(|_| CliError::new("validation", "execution budgets failed bounds validation"))?;

    let (canonical_repo, base_commit) =
        validate_repository_and_base_commit(&request.repository_path, &request.base_commit)
            .map_err(|err| CliError::from_authority(&err))?;
    if Some(&canonical_repo) != fixed_scope.project_root.as_ref() {
        return Err(CliError::new(
            "invalid_repository",
            "request repository_path does not match fixed context --project-root scope",
        ));
    }
    validate_repository_outside_store_dirs(&canonical_repo, dirs)?;

    Ok(ValidatedRequestScope {
        binding_id,
        thread_uuid,
        canonical_repo,
        base_commit,
    })
}

/// Validates the single task compiles against the pinned registry revision and its write scopes,
/// optional `required_changed_paths`, and worker contract are executable at the base commit.
#[allow(clippy::too_many_arguments)]
fn prevalidate_compiled_task(
    workflow: &ScopedWorkflow,
    request: &PrepareTaskRequest,
    task: &PlanTask,
    required_changed_paths: Option<&[String]>,
    canonical_repo: &Path,
    base_commit: &str,
    dirs: &ValidatedStoreDirs,
) -> Result<(Vec<String>, Option<Vec<String>>), CliError> {
    let snapshot = workflow
        .registry()
        .snapshot()
        .map_err(|err| registry_error(&err))?;
    if snapshot.revision != request.profile_revision {
        return Err(stale_registry_error());
    }
    if !snapshot
        .profiles
        .iter()
        .any(|profile| profile.id.as_str() == request.profile_id.as_str())
    {
        return Err(CliError::new(
            "registry_resolution_failed",
            "referenced workflow profile was not found in the live registry",
        ));
    }

    // The deterministic ID is used only for prevalidation; the persisted ID and revision always
    // come from the actual `save_draft` and `publish` receipts.
    let plan_id = deterministic_plan_id(&request.binding_id, &request.command_key)
        .map_err(|err| workflow_error(&err))?;
    let candidate = PlanRevision {
        plan_id,
        binding_id: request.binding_id.clone(),
        revision: 1,
        profile_id: request.profile_id.clone(),
        profile_revision: request.profile_revision,
        summary: request.summary.clone(),
        tasks: vec![task.clone()],
        status: PlanStatus::Draft,
        created_at_ms: 0,
    };
    let compiled =
        preview_draft_compilation(&candidate, &snapshot).map_err(|err| plan_error(&err))?;
    if compiled.tasks.len() != 1 || compiled.tasks[0].id != task.id {
        return Err(CliError::new(
            "corrupt_store",
            "compiled draft did not contain exactly the requested single task",
        ));
    }
    let compiled_task = &compiled.tasks[0];

    let mut validated_task_paths = Vec::with_capacity(compiled_task.allowed_write_paths.len());
    for path in &compiled_task.allowed_write_paths {
        validated_task_paths.push(
            validate_write_scope_entry_at_base_commit(
                path,
                canonical_repo,
                base_commit,
                "task allowed_write_paths entry",
            )
            .map_err(|err| CliError::from_authority(&err))?,
        );
    }
    let validated_required_paths = match required_changed_paths {
        Some(raw_required) => Some(
            validate_required_changed_paths_at_base_commit(
                &compiled_task.id,
                raw_required,
                &validated_task_paths,
                canonical_repo,
                base_commit,
            )
            .map_err(|err| CliError::from_authority(&err))?,
        ),
        None => None,
    };
    validate_executable_worker_instruction(&compiled_task.instruction)
        .map_err(|err| CliError::from_authority(&err))?;

    let (provider, executable) = resolve_and_validate_worker_executable(
        &compiled_task.tool_id,
        &compiled_task.id,
        compiled_task.adapter,
        compiled_task.executable_path.as_deref(),
    )
    .map_err(|err| CliError::from_authority(&err))?;
    validate_worker_task_static(
        provider,
        &executable,
        &compiled_task.model_arg,
        compiled_task.choice.effort.as_deref(),
        canonical_repo,
        base_commit,
        &validated_task_paths,
        &compiled_task.instruction,
        &request.budgets,
        &dirs.authority_dir,
    )
    .map_err(|err| CliError::from_authority(&err))?;

    Ok((validated_task_paths, validated_required_paths))
}

/// Verifies the persisted original draft for this command key exactly matches the request before
/// publication, including idempotent replays that find the revision already published.
fn verify_persisted_draft(
    workflow: &ScopedWorkflow,
    saved: &PlanSummary,
    request: &PrepareTaskRequest,
    task: &PlanTask,
) -> Result<(), CliError> {
    if saved.binding_id != request.binding_id
        || saved.profile_id != request.profile_id
        || saved.profile_revision != request.profile_revision
        || saved.summary != request.summary
        || saved.task_count != 1
        || saved.revision == 0
    {
        return Err(original_mismatch_error());
    }

    let revisions = workflow
        .plans()
        .plan_revisions(&saved.plan_id)
        .map_err(|err| plan_error(&err))?;
    let target = revisions
        .iter()
        .find(|revision| revision.revision == saved.revision)
        .ok_or_else(original_mismatch_error)?;
    if target.binding_id != request.binding_id
        || target.profile_id != request.profile_id
        || target.profile_revision != request.profile_revision
        || target.summary != request.summary
        || target.tasks != vec![task.clone()]
    {
        return Err(original_mismatch_error());
    }
    if !matches!(
        target.status,
        PlanStatus::Draft | PlanStatus::Published | PlanStatus::Received | PlanStatus::Applied
    ) {
        return Err(CliError::new(
            "stale_revision",
            "the original plan revision is superseded; inspect the original plan without redoing the draft",
        ));
    }
    Ok(())
}

/// Verifies the publication is exactly the original saved revision and still describes the one
/// requested task.
fn validate_published_view(
    published: &PublishedPlanView,
    saved: &PlanSummary,
    request: &PrepareTaskRequest,
    task: &PlanTask,
    validated_task_paths: &[String],
) -> Result<PlanSummary, CliError> {
    let header = &published.header;
    if header.plan_id != saved.plan_id
        || header.revision != saved.revision
        || header.binding_id != request.binding_id
        || header.profile_id != request.profile_id
        || header.profile_revision != request.profile_revision
        || header.summary != request.summary
        || header.task_count != 1
        || !header.status.is_active_published()
        || published.receipt.plan_id != header.plan_id
        || published.receipt.revision != header.revision
        || published.receipt.state != header.status
        || published.tasks.len() != 1
    {
        return Err(original_mismatch_error());
    }

    let public_task = &published.tasks[0];
    if public_task.task_id != task.id
        || public_task.role != WorkflowRole::Coder
        || !public_task.depends_on.is_empty()
        || public_task.allowed_write_paths.as_slice() != validated_task_paths
    {
        return Err(original_mismatch_error());
    }

    Ok(header.clone())
}

/// Verifies the authoritative preview still matches the exactly requested single-task scope before
/// it is written to disk.
#[allow(clippy::too_many_arguments)]
fn validate_preview(
    preview: &threadlane_authority::OperatorApprovalPreview,
    request: &PrepareTaskRequest,
    task: &PlanTask,
    canonical_repo: &Path,
    base_commit: &str,
    validated_task_paths: &[String],
    validated_required_paths: Option<&[String]>,
    published_header: &PlanSummary,
) -> Result<(), CliError> {
    let preview_request = &preview.request;
    if preview_request.binding_id != request.binding_id
        || preview_request.thread_uuid != request.thread_uuid
        || preview_request.plan_id != published_header.plan_id
        || preview_request.plan_revision != published_header.revision
        || preview_request.expected_registry_revision != request.profile_revision
        || preview_request.repository_path.as_path() != canonical_repo
        || preview_request.base_commit.as_str() != base_commit
        || published_header.plan_digest.as_deref()
            != Some(preview_request.expected_plan_digest.as_str())
        || preview_request.approved_task_ids != vec![task.id.clone()]
        || preview_request.approved_write_paths.as_slice() != validated_task_paths
        || preview_request.max_parallel != 1
        || preview_request.budgets != request.budgets
        || preview.tasks.len() != 1
    {
        return Err(original_mismatch_error());
    }

    let approved_task = &preview.tasks[0];
    if approved_task.task_id != task.id
        || approved_task.instruction != task.instruction
        || approved_task.role != WorkflowRole::Coder
        || !approved_task.depends_on.is_empty()
        || approved_task.allowed_write_paths.as_slice() != validated_task_paths
        || approved_task.required_changed_paths.as_deref() != validated_required_paths
    {
        return Err(original_mismatch_error());
    }
    Ok(())
}

/// Rechecks the live registry revision immediately before publication.
fn ensure_current_registry_revision(
    workflow: &ScopedWorkflow,
    expected_revision: u64,
) -> Result<(), CliError> {
    let snapshot = workflow
        .registry()
        .snapshot()
        .map_err(|err| registry_error(&err))?;
    if snapshot.revision != expected_revision {
        return Err(stale_registry_error());
    }
    Ok(())
}

/// Rejects a repository path that matches or overlaps any private store directory.
fn validate_repository_outside_store_dirs(
    canonical_repo: &Path,
    dirs: &ValidatedStoreDirs,
) -> Result<(), CliError> {
    for store_dir in [
        &dirs.state_dir,
        &dirs.registry_dir,
        &dirs.plans_dir,
        &dirs.authority_dir,
    ] {
        if canonical_repo == store_dir.as_path()
            || canonical_repo.starts_with(store_dir)
            || store_dir.starts_with(canonical_repo)
        {
            return Err(CliError::new(
                "invalid_repository",
                "repository_path must not overlap any private store directory",
            ));
        }
    }
    Ok(())
}

/// Verifies the fixed binding and thread against the live authenticated supervisor.
async fn verify_live_binding_at(
    dirs: &ValidatedStoreDirs,
    binding_id: &str,
    thread_uuid: &str,
) -> Result<(), CliError> {
    let supervisor = SupervisorClient::from_state_dir(&dirs.state_dir)
        .map_err(|err| CliError::from_supervisor(&err))?;
    verify_live_binding(&supervisor, binding_id, thread_uuid).await
}

/// Maps a read-only workflow error to its bounded static CLI error.
fn workflow_error(err: &WorkflowError) -> CliError {
    CliError::new(err.code(), err.safe_message())
}

/// Maps a workflow error from a mutating call, conservatively reporting ambiguous I/O outcomes.
fn workflow_mutation_error(err: &WorkflowError, phase_message: &'static str) -> CliError {
    match err {
        WorkflowError::Io(_)
        | WorkflowError::Serialization(_)
        | WorkflowError::ResponseTooLarge(_) => CliError::outcome_unknown(phase_message),
        WorkflowError::IdempotencyConflict(_) => CliError::new(
            "idempotency_conflict",
            "command_key already refers to a different original draft or publication; inspect the original plan instead of reusing the key",
        ),
        WorkflowError::StaleRevision(_) => CliError::new(
            "stale_revision",
            "the original plan revision is stale or superseded; inspect the original plan without redoing the draft",
        ),
        _ => workflow_error(err),
    }
}

fn registry_error(err: &RegistryError) -> CliError {
    workflow_error(&WorkflowError::from_registry_error(err))
}

fn plan_error(err: &PlanError) -> CliError {
    workflow_error(&WorkflowError::from_plan_error(err))
}

fn stale_registry_error() -> CliError {
    CliError::new(
        "stale_registry_revision",
        "profile_revision does not match the current registry revision; refresh the catalog or inspect the original plan instead of redoing the draft",
    )
}

fn original_mismatch_error() -> CliError {
    CliError::new(
        "corrupt_store",
        "the original draft or publication for this command_key does not exactly match the requested single-task scope",
    )
}
