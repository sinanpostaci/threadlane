use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use threadlane::{WorkerProfileConfig, WorkerSpecTemplate, WorkersConfig};
use threadlane_core::Binding;
use threadlane_plans::{compile_plan_revision, PlanStore};
use threadlane_registry::{RegistryStore, ToolAdapter};
use threadlane_workers::{
    ensure_platform_supported, ensure_private_dir, open_private_lock_file,
    validate_absolute_no_symlink_components, validate_preexisting_private_file,
    WorkerFilePermissions, WorkerProvider,
};
use uuid::Uuid;

use crate::error::AuthorityError;
use crate::types::{
    ApprovalRecord, ApprovalSummary, ApprovedTaskDispatch, ApprovedTaskRecord, MaterializedPlan,
    NativeRoute, OperatorApprovalRequest, APPROVALS_FILE_NAME, APPROVALS_LOCK_FILE_NAME,
    AUTHORITY_POLICY_VERSION, AUTHORITY_SCHEMA_VERSION, MAX_APPROVALS_IN_STORE,
    MAX_APPROVAL_MAX_PARALLEL, MAX_APPROVED_TASKS, MAX_APPROVED_WRITE_PATHS,
    MAX_AUTHORITY_STORE_BYTES, MAX_EXECUTABLE_PROFILES_PER_APPROVAL, MIN_APPROVAL_MAX_PARALLEL,
};
use crate::validation::{
    compute_authority_core_digest, compute_record_subject_digest, derive_approval_id,
    derive_command_key, derive_profile_id, is_task_scope_contained_in_operator_allowlist,
    resolve_and_validate_worker_executable, sha256_hex, validate_canonical_thread_uuid,
    validate_identifier, validate_repository_and_base_commit, validate_revision,
    validate_sha256_hex, validate_work_area_for_materialization, validate_worker_task_static,
    validate_write_scope_entry, verify_approval_record_integrity,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedAuthorityLedger {
    schema_version: u32,
    approvals: Vec<ApprovalRecord>,
}

impl Default for PersistedAuthorityLedger {
    fn default() -> Self {
        Self {
            schema_version: AUTHORITY_SCHEMA_VERSION,
            approvals: Vec::new(),
        }
    }
}

struct ExclusiveLockGuard {
    file: File,
}

impl Drop for ExclusiveLockGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// Persistent, cross-process locked store for operator-only workflow [`ApprovalRecord`] artifacts
/// and deterministic [`MaterializedPlan`] worker configuration synthesis.
///
/// The authority store resides in its own private (`0700`/`0600`) directory separate from the
/// global [`RegistryStore`] and [`PlanStore`] directories.
#[derive(Debug)]
pub struct AuthorityStore {
    dir: PathBuf,
    approvals_path: PathBuf,
    lock_path: PathBuf,
    registry: Arc<RegistryStore>,
    plans: Arc<PlanStore>,
    in_process_lock: Mutex<()>,
}

impl AuthorityStore {
    /// Opens or initializes a private [`AuthorityStore`] in `dir` backed by the operator-provided
    /// [`RegistryStore`] and [`PlanStore`].
    ///
    /// Enforces private `0700` directory and `0600` file permissions, refuses symlink components,
    /// verifies that `dir` is separate from the registry and plan store directories, acquires a
    /// stable `fs2` cross-process lock (`authority.lock`), and validates any pre-existing ledger
    /// file (`approvals.json`) without overwriting corrupt state.
    pub fn open(
        dir: impl AsRef<Path>,
        registry: Arc<RegistryStore>,
        plans: Arc<PlanStore>,
    ) -> Result<Self, AuthorityError> {
        ensure_platform_supported().map_err(|err| AuthorityError::SecurityViolation {
            path: dir.as_ref().display().to_string(),
            reason: err.to_string(),
        })?;

        let raw_dir = dir.as_ref();
        let resolved_input = if raw_dir.is_absolute() {
            raw_dir.to_path_buf()
        } else {
            std::env::current_dir()?.join(raw_dir)
        };

        let dir_path =
            ensure_private_dir(&resolved_input, "authority store directory").map_err(|err| {
                AuthorityError::SecurityViolation {
                    path: resolved_input.display().to_string(),
                    reason: err.to_string(),
                }
            })?;

        if dir_path.starts_with(registry.config_dir())
            || registry.config_dir().starts_with(&dir_path)
        {
            return Err(AuthorityError::SecurityViolation {
                path: dir_path.display().to_string(),
                reason: format!(
                    "authority store directory `{}` must be separate from registry store directory `{}`",
                    dir_path.display(),
                    registry.config_dir().display()
                ),
            });
        }

        if dir_path.starts_with(plans.dir()) || plans.dir().starts_with(&dir_path) {
            return Err(AuthorityError::SecurityViolation {
                path: dir_path.display().to_string(),
                reason: format!(
                    "authority store directory `{}` must be separate from plan store directory `{}`",
                    dir_path.display(),
                    plans.dir().display()
                ),
            });
        }

        let approvals_path = dir_path.join(APPROVALS_FILE_NAME);
        let lock_path = dir_path.join(APPROVALS_LOCK_FILE_NAME);

        let store = Self {
            dir: dir_path,
            approvals_path,
            lock_path,
            registry,
            plans,
            in_process_lock: Mutex::new(()),
        };

        {
            let _in_proc = store
                .in_process_lock
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let _file_lock = store.acquire_exclusive_lock()?;
            let _ = store.read_ledger_locked()?;
        }

        Ok(store)
    }

    /// Returns the canonical private directory backing this [`AuthorityStore`].
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Returns the shared [`RegistryStore`] handle backing this authority store.
    ///
    /// This is a trusted Rust-only handle for sharing the exact operator-configured store and
    /// preserving `Arc` identity. It is never serialized or exposed through MCP responses.
    pub fn registry(&self) -> &Arc<RegistryStore> {
        &self.registry
    }

    /// Returns the shared [`PlanStore`] handle backing this authority store.
    ///
    /// This is a trusted Rust-only handle for sharing the exact operator-configured store and
    /// preserving `Arc` identity. It is never serialized or exposed through MCP responses.
    pub fn plans(&self) -> &Arc<PlanStore> {
        &self.plans
    }

    /// Returns the path to `approvals.json` inside [`Self::dir`].
    pub fn approvals_path(&self) -> &Path {
        &self.approvals_path
    }

    /// Returns the path to `authority.lock` inside [`Self::dir`].
    pub fn lock_path(&self) -> &Path {
        &self.lock_path
    }

    /// Validates an operator approval request against the pinned plan revision, global registry,
    /// Git repository, base commit, CLI executables, and write scopes, persisting an immutable
    /// frozen [`ApprovalRecord`] and returning its bounded [`ApprovalSummary`].
    ///
    /// ## Security & Authority Invariants
    ///
    /// - **Operator-only**: Only the trusted operator CLI or Tauri control panel may call this
    ///   method; MCP tools must never call or expose `approve`.
    /// - **UI confirmation boundary**: The operator UI must explicitly display and confirm the
    ///   request parameters with the user; this method validates and records the artifact and does
    ///   not by itself prove human action.
    /// - **No execution or status side effects**: `approved != started` and `approved != accepted`.
    ///   This method never mutates `PlanStatus::Received` or `PlanStatus::Applied`, never spawns
    ///   workers, never performs Git merge/push, and never derives approval from worker text.
    /// - **Idempotent determinism**: Repeating an identical valid [`OperatorApprovalRequest`]
    ///   returns the existing immutable [`ApprovalSummary`] with its original creation timestamp.
    pub fn approve(
        &self,
        request: OperatorApprovalRequest,
    ) -> Result<ApprovalSummary, AuthorityError> {
        let now_ms = current_unix_epoch_ms()?;
        self.approve_at(request, now_ms)
    }

    /// Validates and persists an [`OperatorApprovalRequest`] using an explicit `created_at_ms`
    /// timestamp when creating a new record (preserving the original timestamp on idempotent
    /// repeats).
    pub fn approve_at(
        &self,
        request: OperatorApprovalRequest,
        created_at_ms: i64,
    ) -> Result<ApprovalSummary, AuthorityError> {
        let _in_proc = self
            .in_process_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _file_lock = self.acquire_exclusive_lock()?;
        let mut ledger = self.read_ledger_locked()?;

        let record = self.build_validated_approval_record(request, created_at_ms)?;

        if let Some(existing) = ledger.approvals.iter().find(|entry| {
            entry.approval_id == record.approval_id || entry.subject_digest == record.subject_digest
        }) {
            if existing.approval_id != record.approval_id
                || existing.subject_digest != record.subject_digest
            {
                return Err(AuthorityError::CorruptStore {
                    path: self.approvals_path.display().to_string(),
                    reason: format!(
                        "conflicting approval_id `{}` / subject_digest `{}` in authority store",
                        existing.approval_id, existing.subject_digest
                    ),
                });
            }
            return Ok(existing.to_summary());
        }

        if ledger.approvals.len() >= MAX_APPROVALS_IN_STORE {
            return Err(AuthorityError::validation(
                "approvals",
                format!("authority store cannot exceed {MAX_APPROVALS_IN_STORE} approvals"),
            ));
        }

        let summary = record.to_summary();
        ledger.approvals.push(record);
        self.write_ledger_locked(&ledger)?;

        Ok(summary)
    }

    /// Loads the complete private [`ApprovalRecord`] for `approval_id` (operator-only).
    ///
    /// Rereads and verifies the on-disk ledger under the cross-process lock before returning the
    /// record.
    pub fn get_for_operator(&self, approval_id: &str) -> Result<ApprovalRecord, AuthorityError> {
        validate_identifier("approval_id", approval_id)?;
        let _in_proc = self
            .in_process_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _file_lock = self.acquire_exclusive_lock()?;
        let ledger = self.read_ledger_locked()?;

        ledger
            .approvals
            .into_iter()
            .find(|record| record.approval_id == approval_id)
            .ok_or_else(|| AuthorityError::ApprovalNotFound {
                approval_id: approval_id.to_string(),
            })
    }

    /// Lists bounded [`ApprovalSummary`] records for `binding_id` without exposing executable
    /// paths, credentials, or task instructions.
    pub fn list_for_binding(
        &self,
        binding_id: &str,
    ) -> Result<Vec<ApprovalSummary>, AuthorityError> {
        validate_identifier("binding_id", binding_id)?;
        let _in_proc = self
            .in_process_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _file_lock = self.acquire_exclusive_lock()?;
        let ledger = self.read_ledger_locked()?;

        Ok(ledger
            .approvals
            .iter()
            .filter(|record| record.binding_id == binding_id)
            .map(ApprovalRecord::to_summary)
            .collect())
    }

    /// Materializes a persisted approval by `approval_id` for a specific runtime `session_binding`
    /// and private `work_area_root`.
    ///
    /// ## Enforcement & Synthesis Rules
    ///
    /// - Loads the trusted [`ApprovalRecord`] from disk by `approval_id` (never from caller data).
    /// - Enforces `session_binding.thread_uuid == record.thread_uuid`. The runtime session's
    ///   software-generated `session_binding.binding_id` may differ from the paired chat
    ///   `record.binding_id` and becomes the **only** entry in each generated profile's
    ///   `allowed_binding_ids`.
    /// - Rejects materialization if the source plan revision has been `Superseded` in
    ///   [`PlanStore`], while a newer unpublished `Draft` revision alone leaves the approved
    ///   published revision valid.
    /// - Validates `work_area_root` side-effect-free (canonical/non-symlink, private `0700` if
    ///   existing or inside an operator-owned ancestor directory if new, and non-overlapping with
    ///   `repository_path` or `.git`). Never calls `WorktreeManager::new`.
    /// - Synthesizes `WorkersConfig` for selected `antigravity` and `opencode_v2` tasks only
    ///   (`workers: None` for native-only approvals) and validates it via side-effect-free
    ///   [`WorkersConfig::validate`].
    pub fn materialize(
        &self,
        approval_id: &str,
        session_binding: &Binding,
        work_area_root: &Path,
    ) -> Result<MaterializedPlan, AuthorityError> {
        validate_identifier("approval_id", approval_id)?;
        validate_identifier("session_binding.binding_id", &session_binding.binding_id)?;
        let session_thread_uuid = validate_canonical_thread_uuid(
            "session_binding.thread_uuid",
            &session_binding.thread_uuid,
        )?;

        let record = self.get_for_operator(approval_id)?;

        if session_thread_uuid != record.thread_uuid {
            return Err(AuthorityError::ThreadUuidMismatch {
                approval_id: record.approval_id,
                expected_thread_uuid: record.thread_uuid,
                actual_thread_uuid: session_thread_uuid,
            });
        }

        let source_plan = self
            .plans
            .get_revision(&record.plan_id, record.plan_revision)?
            .ok_or_else(|| AuthorityError::PlanNotFound {
                plan_id: record.plan_id.clone(),
                revision: record.plan_revision,
            })?;

        if source_plan.binding_id != record.binding_id {
            return Err(AuthorityError::BindingMismatch {
                plan_id: record.plan_id.clone(),
                expected_binding_id: source_plan.binding_id,
                actual_binding_id: record.binding_id,
            });
        }

        if !source_plan.status.is_active_published() {
            return Err(AuthorityError::UnpublishedOrSupersededPlan {
                plan_id: record.plan_id.clone(),
                revision: record.plan_revision,
                status: source_plan.status,
            });
        }

        let checked_work_area = validate_work_area_for_materialization(
            work_area_root,
            &record.repository_path,
            &self.dir,
            self.registry.config_dir(),
            self.plans.dir(),
        )?;

        let allowed_binding_ids = BTreeSet::from([session_binding.binding_id.clone()]);
        let mut profiles = Vec::new();
        let mut task_dispatch = Vec::new();
        let mut native_routes = Vec::new();

        for task in &record.tasks {
            if let Some(provider) = task.provider {
                let executable = task.canonical_executable.clone().ok_or_else(|| {
                    AuthorityError::CorruptStore {
                        path: self.approvals_path.display().to_string(),
                        reason: format!(
                            "worker task `{}` is missing canonical_executable",
                            task.task_id
                        ),
                    }
                })?;
                let profile_id =
                    task.profile_id
                        .clone()
                        .ok_or_else(|| AuthorityError::CorruptStore {
                            path: self.approvals_path.display().to_string(),
                            reason: format!("worker task `{}` is missing profile_id", task.task_id),
                        })?;
                let command_key =
                    task.command_key
                        .clone()
                        .ok_or_else(|| AuthorityError::CorruptStore {
                            path: self.approvals_path.display().to_string(),
                            reason: format!(
                                "worker task `{}` is missing command_key",
                                task.task_id
                            ),
                        })?;

                let standalone = provider == WorkerProvider::OpenCodeV2;
                let allowed_models = BTreeSet::from([task.base_model.clone()]);
                let allowed_write_paths: BTreeSet<String> =
                    task.allowed_write_paths.iter().cloned().collect();

                let spec_template = WorkerSpecTemplate {
                    provider,
                    executable,
                    model: task.base_model.clone(),
                    effort: task.effort.clone(),
                    allowed_models,
                    standalone,
                    session_id: None,
                    repository_path: record.repository_path.clone(),
                    base_commit: record.base_commit.clone(),
                    work_area_root: checked_work_area.clone(),
                    allowed_write_paths,
                    file_permissions: WorkerFilePermissions::TaskFiles,
                    budgets: record.budgets.clone(),
                };

                // Validate full WorkerSpec (including TaskFiles permission rules for both AGY and OC).
                let full_spec = spec_template.to_worker_spec(task.instruction.clone());
                full_spec
                    .validate()
                    .map_err(|err| AuthorityError::WorkersConfigInvalid {
                        reason: format!("profile `{profile_id}` spec validation failed: {err}"),
                    })?;

                profiles.push(WorkerProfileConfig {
                    profile_id: profile_id.clone(),
                    allowed_binding_ids: allowed_binding_ids.clone(),
                    spec: spec_template,
                });

                task_dispatch.push(ApprovedTaskDispatch {
                    task_id: task.task_id.clone(),
                    task_digest: task.task_digest.clone(),
                    profile_id,
                    command_key,
                    instruction: task.instruction.clone(),
                });
            } else {
                native_routes.push(NativeRoute {
                    task_id: task.task_id.clone(),
                    task_digest: task.task_digest.clone(),
                    title: task.title.clone(),
                    instruction: task.instruction.clone(),
                    role: task.role,
                    difficulty: task.difficulty,
                    tool_id: task.tool_id.clone(),
                    adapter: task.adapter,
                    model_id: task.model_id.clone(),
                    model_arg: task.model_arg.clone(),
                    effort: task.effort.clone(),
                    depends_on: task.depends_on.clone(),
                    allowed_write_paths: task.allowed_write_paths.clone(),
                    thread_uuid: record.thread_uuid.clone(),
                    session_binding_id: session_binding.binding_id.clone(),
                });
            }
        }

        let workers = if profiles.is_empty() {
            None
        } else {
            let workers_config = WorkersConfig {
                max_parallel: record.max_parallel,
                profiles,
            };
            validate_workers_config_side_effect_free(&workers_config)?;
            Some(workers_config)
        };

        Ok(MaterializedPlan {
            approval_id: record.approval_id,
            subject_digest: record.subject_digest,
            workers,
            task_dispatch,
            native_routes,
        })
    }

    fn build_validated_approval_record(
        &self,
        request: OperatorApprovalRequest,
        created_at_ms: i64,
    ) -> Result<ApprovalRecord, AuthorityError> {
        let binding_id = validate_identifier("binding_id", &request.binding_id)?;
        let thread_uuid = validate_canonical_thread_uuid("thread_uuid", &request.thread_uuid)?;
        let plan_id = validate_identifier("plan_id", &request.plan_id)?;
        let plan_revision = validate_revision("plan_revision", request.plan_revision)?;
        let expected_registry_revision = validate_revision(
            "expected_registry_revision",
            request.expected_registry_revision,
        )?;
        let expected_plan_digest =
            validate_sha256_hex("expected_plan_digest", &request.expected_plan_digest)?;

        if !(MIN_APPROVAL_MAX_PARALLEL..=MAX_APPROVAL_MAX_PARALLEL).contains(&request.max_parallel)
        {
            return Err(AuthorityError::validation(
                "max_parallel",
                format!(
                    "max_parallel must be between {MIN_APPROVAL_MAX_PARALLEL} and {MAX_APPROVAL_MAX_PARALLEL} (got {})",
                    request.max_parallel
                ),
            ));
        }

        request
            .budgets
            .validate()
            .map_err(|err| AuthorityError::validation("budgets", err.to_string()))?;

        if request.approved_task_ids.is_empty()
            || request.approved_task_ids.len() > MAX_APPROVED_TASKS
        {
            return Err(AuthorityError::validation(
                "approved_task_ids",
                format!(
                    "approved_task_ids must contain between 1 and {MAX_APPROVED_TASKS} task IDs (got {})",
                    request.approved_task_ids.len()
                ),
            ));
        }

        let mut selected_task_set = BTreeSet::new();
        for task_id in &request.approved_task_ids {
            validate_identifier("approved_task_ids entry", task_id)?;
            if !selected_task_set.insert(task_id.clone()) {
                return Err(AuthorityError::validation(
                    "approved_task_ids",
                    format!("duplicate task_id `{task_id}` in approved_task_ids"),
                ));
            }
        }

        if request.approved_write_paths.is_empty()
            || request.approved_write_paths.len() > MAX_APPROVED_WRITE_PATHS
        {
            return Err(AuthorityError::validation(
                "approved_write_paths",
                format!(
                    "approved_write_paths must contain between 1 and {MAX_APPROVED_WRITE_PATHS} entries (got {})",
                    request.approved_write_paths.len()
                ),
            ));
        }

        let plan = self
            .plans
            .get_revision(&plan_id, plan_revision)?
            .ok_or_else(|| AuthorityError::PlanNotFound {
                plan_id: plan_id.clone(),
                revision: plan_revision,
            })?;

        if plan.binding_id != binding_id {
            return Err(AuthorityError::BindingMismatch {
                plan_id: plan_id.clone(),
                expected_binding_id: plan.binding_id,
                actual_binding_id: binding_id,
            });
        }

        if !plan.status.is_active_published() {
            return Err(AuthorityError::UnpublishedOrSupersededPlan {
                plan_id: plan_id.clone(),
                revision: plan_revision,
                status: plan.status,
            });
        }

        let registry_snapshot =
            self.registry
                .snapshot()
                .map_err(|err| AuthorityError::Registry {
                    reason: err.to_string(),
                })?;

        if expected_registry_revision != plan.profile_revision
            || registry_snapshot.revision != expected_registry_revision
        {
            return Err(AuthorityError::StaleRegistryRevision {
                plan_id: plan_id.clone(),
                plan_revision,
                expected_registry_revision,
                plan_profile_revision: plan.profile_revision,
                actual_registry_revision: registry_snapshot.revision,
            });
        }

        let compiled_plan = compile_plan_revision(&plan, &registry_snapshot)?;
        if compiled_plan.plan_digest != expected_plan_digest {
            return Err(AuthorityError::PlanDigestMismatch {
                plan_id: plan_id.clone(),
                plan_revision,
                expected_plan_digest,
                actual_plan_digest: compiled_plan.plan_digest,
            });
        }

        let (canonical_repo, normalized_base_commit) =
            validate_repository_and_base_commit(&request.repository_path, &request.base_commit)?;

        for (label, store_dir) in [
            ("authority store", self.dir.as_path()),
            ("registry store", self.registry.config_dir()),
            ("plan store", self.plans.dir()),
        ] {
            if canonical_repo.starts_with(store_dir) || store_dir.starts_with(&canonical_repo) {
                return Err(AuthorityError::InvalidRepository {
                    path: canonical_repo.display().to_string(),
                    reason: format!(
                        "repository_path `{}` must not overlap {label} directory `{}`",
                        canonical_repo.display(),
                        store_dir.display()
                    ),
                });
            }
        }

        let mut validated_operator_write_paths =
            Vec::with_capacity(request.approved_write_paths.len());
        let mut seen_op_paths = BTreeSet::new();
        for op_path in &request.approved_write_paths {
            let validated =
                validate_write_scope_entry(op_path, &canonical_repo, "approved_write_paths entry")?;
            if !seen_op_paths.insert(validated.clone()) {
                return Err(AuthorityError::InvalidWriteScope {
                    path: op_path.clone(),
                    reason: format!("duplicate entry `{op_path}` in approved_write_paths"),
                });
            }
            validated_operator_write_paths.push(validated);
        }

        // Verify every requested task ID exists in the compiled plan.
        for requested_id in &request.approved_task_ids {
            if compiled_plan.task(requested_id).is_none() {
                return Err(AuthorityError::UnknownTask {
                    plan_id: plan_id.clone(),
                    plan_revision,
                    task_id: requested_id.clone(),
                });
            }
        }

        let mut frozen_tasks = Vec::with_capacity(selected_task_set.len());
        let mut canonical_approved_task_ids = Vec::with_capacity(selected_task_set.len());
        let mut executable_profile_count = 0usize;

        for compiled_task in &compiled_plan.tasks {
            if !selected_task_set.contains(&compiled_task.id) {
                continue;
            }

            if compiled_task.allowed_write_paths.is_empty() {
                return Err(AuthorityError::InvalidWriteScope {
                    path: String::new(),
                    reason: format!(
                        "selected task `{}` must have at least one allowed_write_paths entry",
                        compiled_task.id
                    ),
                });
            }

            let mut validated_task_write_paths =
                Vec::with_capacity(compiled_task.allowed_write_paths.len());
            for task_path in &compiled_task.allowed_write_paths {
                let checked_task_path = validate_write_scope_entry(
                    task_path,
                    &canonical_repo,
                    "task allowed_write_paths entry",
                )?;
                if !is_task_scope_contained_in_operator_allowlist(
                    &checked_task_path,
                    &validated_operator_write_paths,
                ) {
                    return Err(AuthorityError::UnapprovedTaskWriteScope {
                        task_id: compiled_task.id.clone(),
                        task_path: checked_task_path,
                        reason: "task write scope is not covered componentwise by operator approved_write_paths (file-exact or trailing-`/` directory prefix)".to_string(),
                    });
                }
                validated_task_write_paths.push(checked_task_path);
            }

            let (
                provider,
                canonical_executable,
                base_model,
                effort,
                cli_model_arg,
                standalone,
                file_permissions,
            ) = match compiled_task.adapter {
                ToolAdapter::Antigravity | ToolAdapter::OpencodeV2 => {
                    executable_profile_count += 1;
                    if executable_profile_count > MAX_EXECUTABLE_PROFILES_PER_APPROVAL {
                        return Err(AuthorityError::validation(
                            "approved_task_ids",
                            format!(
                                "selected executable worker profile count ({executable_profile_count}) exceeds maximum of {MAX_EXECUTABLE_PROFILES_PER_APPROVAL}"
                            ),
                        ));
                    }

                    let (resolved_provider, canonical_exe) =
                        resolve_and_validate_worker_executable(
                            &compiled_task.tool_id,
                            &compiled_task.id,
                            compiled_task.adapter,
                            compiled_task.executable_path.as_deref(),
                        )?;

                    let (resolved_base_model, resolved_effort, resolved_cli_arg) =
                        validate_worker_task_static(
                            resolved_provider,
                            &canonical_exe,
                            &compiled_task.model_arg,
                            compiled_task.choice.effort.as_deref(),
                            &canonical_repo,
                            &normalized_base_commit,
                            &validated_task_write_paths,
                            &compiled_task.instruction,
                            &request.budgets,
                            &self.dir,
                        )?;

                    (
                        Some(resolved_provider),
                        Some(canonical_exe),
                        resolved_base_model,
                        resolved_effort,
                        Some(resolved_cli_arg),
                        Some(resolved_provider == WorkerProvider::OpenCodeV2),
                        Some(WorkerFilePermissions::TaskFiles),
                    )
                }
                ToolAdapter::CodexCli => (
                    None,
                    None,
                    compiled_task.model_arg.clone(),
                    compiled_task.choice.effort.clone(),
                    None,
                    None,
                    None,
                ),
                other => {
                    return Err(AuthorityError::UnsupportedTaskAdapter {
                        plan_id: plan_id.clone(),
                        task_id: compiled_task.id.clone(),
                        adapter: other,
                        reason: format!(
                            "adapter `{other}` is unsupported for headless worker execution; only `antigravity`, `opencode_v2`, and native follower `codex_cli` are supported"
                        ),
                    });
                }
            };

            let instruction_digest = sha256_hex(compiled_task.instruction.as_bytes());
            canonical_approved_task_ids.push(compiled_task.id.clone());
            frozen_tasks.push(ApprovedTaskRecord {
                task_id: compiled_task.id.clone(),
                title: compiled_task.title.clone(),
                instruction: compiled_task.instruction.clone(),
                instruction_digest,
                role: compiled_task.role,
                difficulty: compiled_task.difficulty,
                tool_id: compiled_task.tool_id.clone(),
                adapter: compiled_task.adapter,
                provider,
                canonical_executable,
                model_id: compiled_task.choice.model_id.clone(),
                model_arg: compiled_task.model_arg.clone(),
                base_model,
                effort,
                cli_model_arg,
                standalone,
                file_permissions,
                depends_on: compiled_task.depends_on.clone(),
                allowed_write_paths: validated_task_write_paths,
                task_digest: compiled_task.task_digest.clone(),
                profile_id: None,
                command_key: None,
            });
        }

        let mut record = ApprovalRecord {
            approval_id: String::new(),
            subject_digest: String::new(),
            policy_version: AUTHORITY_POLICY_VERSION,
            binding_id,
            thread_uuid,
            plan_id,
            plan_revision,
            profile_id: compiled_plan.profile_id,
            registry_revision: expected_registry_revision,
            plan_digest: compiled_plan.plan_digest,
            repository_path: canonical_repo,
            base_commit: normalized_base_commit,
            approved_task_ids: canonical_approved_task_ids,
            approved_write_paths: validated_operator_write_paths,
            max_parallel: request.max_parallel,
            budgets: request.budgets,
            tasks: frozen_tasks,
            created_at_ms,
        };

        let core_digest = compute_authority_core_digest(&record)?;
        for task in &mut record.tasks {
            if task.provider.is_some() {
                task.profile_id = Some(derive_profile_id(
                    &core_digest,
                    &task.task_id,
                    &task.task_digest,
                ));
                task.command_key = Some(derive_command_key(
                    &core_digest,
                    &task.task_id,
                    &task.task_digest,
                ));
            }
        }

        let subject_digest = compute_record_subject_digest(&record)?;
        let approval_id = derive_approval_id(&subject_digest);
        record.subject_digest = subject_digest;
        record.approval_id = approval_id;

        verify_approval_record_integrity(&record, &self.approvals_path)?;
        Ok(record)
    }

    fn acquire_exclusive_lock(&self) -> Result<ExclusiveLockGuard, AuthorityError> {
        let lock_file = open_private_lock_file(&self.lock_path, "authority store lock file")
            .map_err(|err| AuthorityError::SecurityViolation {
                path: self.lock_path.display().to_string(),
                reason: err.to_string(),
            })?;
        lock_file
            .lock_exclusive()
            .map_err(|err| AuthorityError::Io {
                operation: "lock_exclusive".to_string(),
                reason: err.to_string(),
            })?;
        Ok(ExclusiveLockGuard { file: lock_file })
    }

    fn read_ledger_locked(&self) -> Result<PersistedAuthorityLedger, AuthorityError> {
        let _ = validate_absolute_no_symlink_components(
            &self.approvals_path,
            "authority store ledger file",
        )
        .map_err(|err| AuthorityError::SecurityViolation {
            path: self.approvals_path.display().to_string(),
            reason: err.to_string(),
        })?;

        let symlink_meta = match fs::symlink_metadata(&self.approvals_path) {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(PersistedAuthorityLedger::default());
            }
            Err(err) => {
                return Err(AuthorityError::Io {
                    operation: "symlink_metadata".to_string(),
                    reason: err.to_string(),
                });
            }
        };

        validate_preexisting_private_file(&self.approvals_path, "authority store ledger file")
            .map_err(|err| AuthorityError::SecurityViolation {
                path: self.approvals_path.display().to_string(),
                reason: err.to_string(),
            })?;

        if symlink_meta.len() > MAX_AUTHORITY_STORE_BYTES as u64 {
            return Err(AuthorityError::CorruptStore {
                path: self.approvals_path.display().to_string(),
                reason: format!(
                    "authority ledger file size ({} bytes) exceeds maximum of {MAX_AUTHORITY_STORE_BYTES} bytes",
                    symlink_meta.len()
                ),
            });
        }

        let file = File::open(&self.approvals_path).map_err(|err| AuthorityError::Io {
            operation: "open_ledger".to_string(),
            reason: err.to_string(),
        })?;
        let open_meta = file.metadata().map_err(|err| AuthorityError::Io {
            operation: "ledger_metadata".to_string(),
            reason: err.to_string(),
        })?;
        if !open_meta.file_type().is_file() {
            return Err(AuthorityError::SecurityViolation {
                path: self.approvals_path.display().to_string(),
                reason: "authority ledger is not a regular file".to_string(),
            });
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            if symlink_meta.dev() != open_meta.dev() || symlink_meta.ino() != open_meta.ino() {
                return Err(AuthorityError::SecurityViolation {
                    path: self.approvals_path.display().to_string(),
                    reason: "authority ledger inode changed during open".to_string(),
                });
            }
            let mode = open_meta.permissions().mode() & 0o777;
            if mode & 0o077 != 0 {
                return Err(AuthorityError::SecurityViolation {
                    path: self.approvals_path.display().to_string(),
                    reason: format!(
                        "authority ledger has non-private permissions {:04o} (expected 0600)",
                        mode
                    ),
                });
            }
        }

        let mut bytes = Vec::with_capacity(
            usize::try_from(open_meta.len())
                .unwrap_or(1024)
                .min(MAX_AUTHORITY_STORE_BYTES + 1),
        );
        file.take((MAX_AUTHORITY_STORE_BYTES as u64) + 1)
            .read_to_end(&mut bytes)
            .map_err(|err| AuthorityError::Io {
                operation: "read_ledger".to_string(),
                reason: err.to_string(),
            })?;

        if bytes.len() > MAX_AUTHORITY_STORE_BYTES {
            return Err(AuthorityError::CorruptStore {
                path: self.approvals_path.display().to_string(),
                reason: format!(
                    "authority ledger exceeds maximum size of {MAX_AUTHORITY_STORE_BYTES} bytes"
                ),
            });
        }

        let ledger: PersistedAuthorityLedger =
            serde_json::from_slice(&bytes).map_err(|err| AuthorityError::CorruptStore {
                path: self.approvals_path.display().to_string(),
                reason: format!("invalid JSON in authority ledger: {err}"),
            })?;

        if ledger.schema_version != AUTHORITY_SCHEMA_VERSION {
            return Err(AuthorityError::CorruptStore {
                path: self.approvals_path.display().to_string(),
                reason: format!(
                    "unsupported authority schema_version {} (expected {AUTHORITY_SCHEMA_VERSION})",
                    ledger.schema_version
                ),
            });
        }

        if ledger.approvals.len() > MAX_APPROVALS_IN_STORE {
            return Err(AuthorityError::CorruptStore {
                path: self.approvals_path.display().to_string(),
                reason: format!(
                    "authority ledger contains {} approvals, exceeding maximum of {MAX_APPROVALS_IN_STORE}",
                    ledger.approvals.len()
                ),
            });
        }

        let mut seen_ids = BTreeSet::new();
        let mut seen_digests = BTreeSet::new();
        for record in &ledger.approvals {
            verify_approval_record_integrity(record, &self.approvals_path)?;
            if !seen_ids.insert(record.approval_id.clone()) {
                return Err(AuthorityError::CorruptStore {
                    path: self.approvals_path.display().to_string(),
                    reason: format!("duplicate approval_id `{}` in ledger", record.approval_id),
                });
            }
            if !seen_digests.insert(record.subject_digest.clone()) {
                return Err(AuthorityError::CorruptStore {
                    path: self.approvals_path.display().to_string(),
                    reason: format!(
                        "duplicate subject_digest `{}` in ledger",
                        record.subject_digest
                    ),
                });
            }
        }

        Ok(ledger)
    }

    fn write_ledger_locked(&self, ledger: &PersistedAuthorityLedger) -> Result<(), AuthorityError> {
        validate_preexisting_private_file(&self.approvals_path, "authority store ledger file")
            .map_err(|err| AuthorityError::SecurityViolation {
                path: self.approvals_path.display().to_string(),
                reason: err.to_string(),
            })?;

        let serialized = serde_json::to_vec_pretty(ledger).map_err(|err| {
            AuthorityError::validation("approvals", format!("failed to serialize ledger: {err}"))
        })?;
        if serialized.len() > MAX_AUTHORITY_STORE_BYTES {
            return Err(AuthorityError::validation(
                "approvals",
                format!(
                    "serialized authority ledger ({} bytes) exceeds maximum of {MAX_AUTHORITY_STORE_BYTES} bytes",
                    serialized.len()
                ),
            ));
        }

        let temp_name = format!(
            ".{APPROVALS_FILE_NAME}.tmp.{}.{}",
            std::process::id(),
            Uuid::new_v4()
        );
        let temp_path = self.dir.join(temp_name);
        let _ = validate_absolute_no_symlink_components(&temp_path, "temporary authority ledger")
            .map_err(|err| AuthorityError::SecurityViolation {
            path: temp_path.display().to_string(),
            reason: err.to_string(),
        })?;

        let write_result = (|| -> Result<(), AuthorityError> {
            #[cfg(unix)]
            let mut temp_file = {
                use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
                let f = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(&temp_path)?;
                f.set_permissions(fs::Permissions::from_mode(0o600))?;
                f
            };

            #[cfg(not(unix))]
            let mut temp_file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)?;

            temp_file.write_all(&serialized)?;
            temp_file.sync_all()?;
            drop(temp_file);

            fs::rename(&temp_path, &self.approvals_path)?;
            let dir_file = File::open(&self.dir)?;
            dir_file.sync_all()?;
            Ok(())
        })();

        if write_result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }

        write_result
    }
}

fn validate_workers_config_side_effect_free(
    workers_config: &WorkersConfig,
) -> Result<(), AuthorityError> {
    workers_config
        .validate()
        .map(|_| ())
        .map_err(|err| AuthorityError::WorkersConfigInvalid {
            reason: err.to_string(),
        })
}

fn current_unix_epoch_ms() -> Result<i64, AuthorityError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| AuthorityError::Io {
            operation: "system_clock".to_string(),
            reason: err.to_string(),
        })?;
    i64::try_from(duration.as_millis()).map_err(|err| AuthorityError::Io {
        operation: "system_clock_overflow".to_string(),
        reason: err.to_string(),
    })
}
