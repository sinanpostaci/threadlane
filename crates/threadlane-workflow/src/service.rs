use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Serialize;
use sha2::{Digest, Sha256};
use threadlane::ThreadlaneMcpServer;
use threadlane_plans::{
    compile_plan_revision, preview_draft_compilation, validate_identifier,
    validate_js_safe_revision, validate_plan_tasks, validate_summary, PlanChange, PlanError,
    PlanRevision, PlanStatus, PlanStore, PLAN_STORE_SCHEMA_VERSION,
};
use threadlane_registry::{
    RegistrySnapshot, RegistryStore, WorkflowProfile, REGISTRY_SCHEMA_VERSION,
};

use crate::cursor::{
    decode_and_validate_cursor, encode_cursor, short_tag, CursorResource, CursorSpec,
};
use crate::error::WorkflowError;
use crate::journal::{
    compute_payload_digest, compute_tasks_digest, journal_paths_for_binding, read_journal_locked,
    with_journal_lock, write_journal_locked, CommandJournalEntry, CommandJournalState,
    MAX_COMMAND_JOURNAL_ENTRIES,
};
use crate::types::{
    CatalogPage, CatalogProfile, CatalogRoleAssignment, PlanPage, PlanSummary, PlanSummaryPage,
    PublishedPlanView, SavePlanDraft, TaskExecutionRoute, DEFAULT_CATALOG_LIMIT,
    DEFAULT_PLAN_LIST_LIMIT, DEFAULT_PLAN_TASK_LIMIT, MAX_CATALOG_LIMIT, MAX_MCP_RESPONSE_BYTES,
    MAX_PLAN_LIST_LIMIT, MAX_PLAN_TASK_LIMIT,
};

/// Derives a deterministic, binding-isolated plan identifier (`plan_<32-hex>`) from
/// `(binding_id, command_key)`.
///
/// The same `command_key` under two different bindings always produces two distinct plan
/// identifiers.
pub fn deterministic_plan_id(binding_id: &str, command_key: &str) -> Result<String, WorkflowError> {
    validate_identifier("binding_id", binding_id)?;
    validate_identifier("command_key", command_key)?;

    let seed = ("threadlane-workflow:plan_id:v1", binding_id, command_key);
    let Ok(payload) = serde_json::to_vec(&seed) else {
        return Err(WorkflowError::Serialization(
            "Failed to encode deterministic plan ID seed.",
        ));
    };
    let digest = Sha256::digest(&payload);
    let mut hex = String::with_capacity(32);
    for byte in &digest[..16] {
        let _ = write!(&mut hex, "{byte:02x}");
    }
    Ok(format!("plan_{hex}"))
}

/// Measures the exact serialized byte length of the `rmcp` [`rmcp::model::CallToolResult`]
/// envelope produced by [`ThreadlaneMcpServer::build_structured_tool_result`] for `(binding_id, tool_name, data)`.
pub fn measure_mcp_response_bytes<T: Serialize>(
    binding_id: &str,
    tool_name: &str,
    data: &T,
) -> Result<usize, WorkflowError> {
    let envelope = ThreadlaneMcpServer::build_structured_tool_result(binding_id, tool_name, data);
    let Ok(bytes) = serde_json::to_vec(&envelope) else {
        return Err(WorkflowError::Serialization(
            "Failed to serialize MCP response envelope.",
        ));
    };
    Ok(bytes.len())
}

/// Verifies that the serialized [`ThreadlaneMcpServer::build_structured_tool_result`] envelope
/// for `(binding_id, tool_name, data)` does not exceed [`MAX_MCP_RESPONSE_BYTES`] (256 KiB).
pub fn ensure_mcp_response_budget<T: Serialize>(
    binding_id: &str,
    tool_name: &str,
    data: &T,
) -> Result<usize, WorkflowError> {
    let len = measure_mcp_response_bytes(binding_id, tool_name, data)?;
    if len > MAX_MCP_RESPONSE_BYTES {
        return Err(WorkflowError::ResponseTooLarge(
            "Serialized MCP response exceeds the 256 KiB wire budget.",
        ));
    }
    Ok(len)
}

/// Synchronous, binding-scoped workflow profile catalog and versioned plan service.
///
/// Constructed by the trusted operator with shared [`RegistryStore`] and [`PlanStore`] handles
/// and a fixed `binding_id`. Callers cannot override filesystem paths or binding identity.
#[derive(Debug, Clone)]
pub struct ScopedWorkflow {
    registry: Arc<RegistryStore>,
    plans: Arc<PlanStore>,
    binding_id: String,
    journal_path: PathBuf,
    journal_lock_path: PathBuf,
}

impl ScopedWorkflow {
    /// Creates a new [`ScopedWorkflow`] scoped to `binding_id`.
    pub fn new(
        registry: Arc<RegistryStore>,
        plans: Arc<PlanStore>,
        binding_id: String,
    ) -> Result<Self, WorkflowError> {
        let validated_binding = validate_identifier("binding_id", &binding_id)?;
        let (journal_path, journal_lock_path) =
            journal_paths_for_binding(plans.dir(), &validated_binding)?;
        Ok(Self {
            registry,
            plans,
            binding_id: validated_binding,
            journal_path,
            journal_lock_path,
        })
    }

    /// Returns the trusted chat binding identifier owning this [`ScopedWorkflow`].
    pub fn binding_id(&self) -> &str {
        &self.binding_id
    }

    /// Returns a reference to the shared global [`RegistryStore`].
    pub fn registry(&self) -> &Arc<RegistryStore> {
        &self.registry
    }

    /// Returns a reference to the underlying [`PlanStore`].
    pub fn plans(&self) -> &Arc<PlanStore> {
        &self.plans
    }

    #[cfg(test)]
    pub(crate) fn command_journal_path(&self) -> &Path {
        &self.journal_path
    }

    /// Lists a bounded page of configured workflow profiles from the global registry.
    ///
    /// - Page size defaults to [`DEFAULT_CATALOG_LIMIT`] (`20`) and cannot exceed [`MAX_CATALOG_LIMIT`] (`20`).
    /// - Summaries omit CLI executable paths and credentials.
    /// - Assignments referencing disabled tools are marked `tool_enabled: false` and `available: false`
    ///   without substituting another model or tool.
    /// - Opaque cursors pin the binding scope and `registry_revision`; if the registry changes between
    ///   pages, the cursor is rejected as stale (`stale_revision`).
    pub fn catalog(
        &self,
        cursor: Option<String>,
        limit: Option<usize>,
    ) -> Result<CatalogPage, WorkflowError> {
        let requested_limit = validate_page_limit(limit, DEFAULT_CATALOG_LIMIT, MAX_CATALOG_LIMIT)?;
        let snapshot = self.registry.snapshot()?;

        let all_profiles: Vec<CatalogProfile> = snapshot
            .profiles
            .iter()
            .map(|profile| build_catalog_profile(profile, &snapshot))
            .collect::<Result<Vec<_>, _>>()?;

        let profile_ids: Vec<&str> = all_profiles.iter().map(|p| p.profile_id.as_str()).collect();
        let filter_tag = short_tag("catalog_filter", "")?;
        let state_tag = short_tag(
            "catalog_state",
            &(snapshot.schema_version, snapshot.revision, &profile_ids),
        )?;
        let cursor_spec = CursorSpec {
            resource: CursorResource::Catalog,
            binding_id: &self.binding_id,
            filter_tag,
            store_schema_version: REGISTRY_SCHEMA_VERSION,
            revision_watermark: snapshot.revision,
            state_tag,
        };

        let start_idx = match cursor.as_deref() {
            Some(raw) => decode_and_validate_cursor(raw, &cursor_spec, &profile_ids)?,
            None => 0,
        };

        if start_idx >= all_profiles.len() {
            let empty_page = CatalogPage {
                registry_revision: snapshot.revision,
                default_profile_id: snapshot.default_profile_id.clone(),
                profiles: Vec::new(),
                next_cursor: None,
                has_more: false,
            };
            ensure_mcp_response_budget(&self.binding_id, "catalog", &empty_page)?;
            return Ok(empty_page);
        }

        let max_take = requested_limit.min(all_profiles.len() - start_idx);
        for take_count in (1..=max_take).rev() {
            let end_idx = start_idx + take_count;
            let next_cursor = if end_idx < all_profiles.len() {
                Some(encode_cursor(
                    &cursor_spec,
                    end_idx,
                    &all_profiles[end_idx - 1].profile_id,
                )?)
            } else {
                None
            };
            let has_more = next_cursor.is_some();
            let candidate = CatalogPage {
                registry_revision: snapshot.revision,
                default_profile_id: snapshot.default_profile_id.clone(),
                profiles: all_profiles[start_idx..end_idx].to_vec(),
                next_cursor,
                has_more,
            };
            if measure_mcp_response_bytes(&self.binding_id, "catalog", &candidate)?
                <= MAX_MCP_RESPONSE_BYTES
            {
                return Ok(candidate);
            }
        }

        Err(WorkflowError::ResponseTooLarge(
            "Single catalog profile entry exceeds the 256 KiB MCP wire budget.",
        ))
    }

    /// Lists a bounded page of [`PlanSummary`] records belonging exclusively to this binding.
    ///
    /// - Page size defaults to [`DEFAULT_PLAN_LIST_LIMIT`] (`20`) and cannot exceed [`MAX_PLAN_LIST_LIMIT`] (`20`).
    /// - Summaries omit task instructions and CLI executable paths.
    /// - Read-only: never acknowledges or advances delivery receipts.
    pub fn list_plans(
        &self,
        cursor: Option<String>,
        limit: Option<usize>,
    ) -> Result<PlanSummaryPage, WorkflowError> {
        let requested_limit =
            validate_page_limit(limit, DEFAULT_PLAN_LIST_LIMIT, MAX_PLAN_LIST_LIMIT)?;
        let registry_snapshot = self.registry.snapshot()?;
        let all_revisions = self.plans.snapshot()?;

        // Group latest revision per plan_id for own binding only (in deterministic ascending plan_id order).
        let mut latest_by_plan: BTreeMap<String, PlanRevision> = BTreeMap::new();
        for rev in all_revisions {
            if rev.binding_id == self.binding_id {
                latest_by_plan.insert(rev.plan_id.clone(), rev);
            }
        }

        let own_revisions: Vec<PlanRevision> = latest_by_plan.into_values().collect();
        let plan_ids: Vec<&str> = own_revisions.iter().map(|r| r.plan_id.as_str()).collect();
        let watermark_entries: Vec<(&str, u64, PlanStatus)> = own_revisions
            .iter()
            .map(|r| (r.plan_id.as_str(), r.revision, r.status))
            .collect();
        let revision_watermark: u64 = own_revisions
            .iter()
            .fold(0u64, |acc, r| acc.saturating_add(r.revision));

        let filter_tag = short_tag("list_plans_filter", "")?;
        let state_tag = short_tag(
            "list_plans_state",
            &(PLAN_STORE_SCHEMA_VERSION, &watermark_entries),
        )?;
        let cursor_spec = CursorSpec {
            resource: CursorResource::ListPlans,
            binding_id: &self.binding_id,
            filter_tag,
            store_schema_version: PLAN_STORE_SCHEMA_VERSION,
            revision_watermark,
            state_tag,
        };

        let start_idx = match cursor.as_deref() {
            Some(raw) => decode_and_validate_cursor(raw, &cursor_spec, &plan_ids)?,
            None => 0,
        };

        let summaries: Vec<PlanSummary> = own_revisions
            .iter()
            .map(|rev| build_plan_summary(rev, &registry_snapshot))
            .collect();

        if start_idx >= summaries.len() {
            let empty_page = PlanSummaryPage {
                binding_id: self.binding_id.clone(),
                plans: Vec::new(),
                next_cursor: None,
                has_more: false,
            };
            ensure_mcp_response_budget(&self.binding_id, "list_plans", &empty_page)?;
            return Ok(empty_page);
        }

        let max_take = requested_limit.min(summaries.len() - start_idx);
        for take_count in (1..=max_take).rev() {
            let end_idx = start_idx + take_count;
            let next_cursor = if end_idx < summaries.len() {
                Some(encode_cursor(
                    &cursor_spec,
                    end_idx,
                    &summaries[end_idx - 1].plan_id,
                )?)
            } else {
                None
            };
            let has_more = next_cursor.is_some();
            let candidate = PlanSummaryPage {
                binding_id: self.binding_id.clone(),
                plans: summaries[start_idx..end_idx].to_vec(),
                next_cursor,
                has_more,
            };
            if measure_mcp_response_bytes(&self.binding_id, "list_plans", &candidate)?
                <= MAX_MCP_RESPONSE_BYTES
            {
                return Ok(candidate);
            }
        }

        Err(WorkflowError::ResponseTooLarge(
            "Single plan summary exceeds the 256 KiB MCP wire budget.",
        ))
    }

    /// Fetches a specific [`PlanRevision`] (or the latest revision when `revision` is `None`)
    /// belonging to this binding, with cursor-paginated full [`PlanTask`] records.
    ///
    /// - Task page size defaults to [`DEFAULT_PLAN_TASK_LIMIT`] (`10`) and cannot exceed
    ///   [`MAX_PLAN_TASK_LIMIT`] (`20`).
    /// - Task instructions are returned in full and are never truncated.
    /// - Automatically bounds each page so the serialized MCP envelope stays `<= 256 KiB`;
    ///   if a single task exceeds the 256 KiB MCP envelope, fails with `response_too_large`.
    /// - Returns the identical static `not_found` error when `plan_id` does not exist, belongs to
    ///   another binding, or the requested `revision` does not exist.
    /// - Read-only: never acknowledges or advances delivery receipts.
    pub fn get_plan(
        &self,
        plan_id: &str,
        revision: Option<u64>,
        task_cursor: Option<String>,
        task_limit: Option<usize>,
    ) -> Result<PlanPage, WorkflowError> {
        validate_identifier("plan_id", plan_id)?;
        let requested_limit =
            validate_page_limit(task_limit, DEFAULT_PLAN_TASK_LIMIT, MAX_PLAN_TASK_LIMIT)?;
        if let Some(rev_num) = revision {
            if rev_num == 0 {
                return Err(WorkflowError::InvalidInput(
                    "Requested plan revision must be at least 1.",
                ));
            }
            validate_js_safe_revision("revision", rev_num)?;
        }

        let revisions = self.plans.plan_revisions(plan_id)?;
        if revisions.is_empty() || revisions[0].binding_id != self.binding_id {
            return Err(WorkflowError::plan_not_found());
        }

        let target_rev = match revision {
            Some(rev_num) => revisions
                .iter()
                .find(|r| r.revision == rev_num)
                .ok_or_else(WorkflowError::plan_not_found)?,
            None => revisions.last().ok_or_else(WorkflowError::plan_not_found)?,
        };

        let task_ids: Vec<&str> = target_rev.tasks.iter().map(|t| t.id.as_str()).collect();
        let filter_tag = short_tag("plan_tasks_filter", &(&target_rev.plan_id, revision))?;
        let state_tag = short_tag(
            "plan_tasks_state",
            &(
                PLAN_STORE_SCHEMA_VERSION,
                &target_rev.plan_id,
                target_rev.revision,
                target_rev.status,
                &task_ids,
            ),
        )?;
        let cursor_spec = CursorSpec {
            resource: CursorResource::PlanTasks,
            binding_id: &self.binding_id,
            filter_tag,
            store_schema_version: PLAN_STORE_SCHEMA_VERSION,
            revision_watermark: target_rev.revision,
            state_tag,
        };

        let start_idx = match task_cursor.as_deref() {
            Some(raw) => decode_and_validate_cursor(raw, &cursor_spec, &task_ids)?,
            None => 0,
        };

        let registry_snapshot = self.registry.snapshot()?;
        let header = build_plan_summary(target_rev, &registry_snapshot);

        if start_idx >= target_rev.tasks.len() {
            let empty_page = PlanPage {
                header,
                tasks: Vec::new(),
                next_task_cursor: None,
                has_more: false,
            };
            ensure_mcp_response_budget(&self.binding_id, "get_plan", &empty_page)?;
            return Ok(empty_page);
        }

        let max_take = requested_limit.min(target_rev.tasks.len() - start_idx);
        for take_count in (1..=max_take).rev() {
            let end_idx = start_idx + take_count;
            let next_task_cursor = if end_idx < target_rev.tasks.len() {
                Some(encode_cursor(
                    &cursor_spec,
                    end_idx,
                    &target_rev.tasks[end_idx - 1].id,
                )?)
            } else {
                None
            };
            let has_more = next_task_cursor.is_some();
            let candidate = PlanPage {
                header: header.clone(),
                tasks: target_rev.tasks[start_idx..end_idx].to_vec(),
                next_task_cursor,
                has_more,
            };
            if measure_mcp_response_bytes(&self.binding_id, "get_plan", &candidate)?
                <= MAX_MCP_RESPONSE_BYTES
            {
                return Ok(candidate);
            }
        }

        Err(WorkflowError::ResponseTooLarge(
            "Single plan task exceeds the 256 KiB MCP wire budget.",
        ))
    }

    /// Creates or updates a draft plan revision scoped to this binding.
    ///
    /// - When `request.plan_id` is `None`, generates a deterministic plan ID from
    ///   `(binding_id, command_key)`.
    /// - Enforces that any existing `plan_id` already belongs to this binding, and that an
    ///   unknown `plan_id` may only be created when `expected_revision == 0`; any cross-binding
    ///   collision or non-zero lookup on an unknown/other-binding plan returns the identical
    ///   static `not_found` error.
    /// - Validates the task DAG, write scopes, and role/difficulty resolution against the current
    ///   registry snapshot via [`preview_draft_compilation`] before mutating [`PlanStore`].
    /// - Persists a durable per-binding `command_key` intent journal entry (`Pending` before
    ///   `PlanStore::change`, reconciled to `Completed` or `Failed` afterwards) under
    ///   `PlanStore::dir()` protected by a cross-process `fs2` lock, supporting idempotent retries
    ///   of the original saved revision across reopens and intervening edits while rejecting any
    ///   changed payload, target, or revision reuse with `idempotency_conflict`.
    pub fn save_draft(&self, request: SavePlanDraft) -> Result<PlanSummary, WorkflowError> {
        validate_identifier("command_key", &request.command_key)?;
        validate_js_safe_revision("expected_revision", request.expected_revision)?;
        validate_identifier("profile_id", &request.profile_id)?;
        validate_js_safe_revision("profile_revision", request.profile_revision)?;
        validate_summary(&request.summary)?;

        let explicit_plan_id = match request.plan_id.as_deref() {
            Some(explicit_id) => Some(validate_identifier("plan_id", explicit_id)?),
            None => None,
        };
        let target_plan_id = match &explicit_plan_id {
            Some(explicit_id) => explicit_id.clone(),
            None => deterministic_plan_id(&self.binding_id, &request.command_key)?,
        };

        validate_plan_tasks(&target_plan_id, &request.tasks)?;
        let tasks_digest = compute_tasks_digest(&request.tasks)?;
        let payload_digest = compute_payload_digest(
            &target_plan_id,
            &request.profile_id,
            request.profile_revision,
            &request.summary,
            &request.tasks,
        )?;

        with_journal_lock(self.plans.dir(), &self.journal_lock_path, |clean_dir| {
            self.save_draft_locked(
                clean_dir,
                request,
                explicit_plan_id,
                target_plan_id,
                tasks_digest,
                payload_digest,
            )
        })
    }

    fn save_draft_locked(
        &self,
        clean_dir: &Path,
        request: SavePlanDraft,
        explicit_plan_id: Option<String>,
        target_plan_id: String,
        tasks_digest: String,
        payload_digest: String,
    ) -> Result<PlanSummary, WorkflowError> {
        let mut journal = read_journal_locked(&self.journal_path, &self.binding_id)?;

        // Scope guard before any revision or compilation checks so cross-binding probes always
        // receive the canonical static `not_found` error.
        let existing_revisions = self.plans.plan_revisions(&target_plan_id)?;
        if let Some(first) = existing_revisions.first() {
            if first.binding_id != self.binding_id {
                return Err(WorkflowError::plan_not_found());
            }
        }
        let reserved_for_target = journal
            .find(&request.command_key)
            .is_some_and(|entry| entry.resolved_plan_id == target_plan_id);
        if existing_revisions.is_empty() && !reserved_for_target && request.expected_revision != 0 {
            return Err(WorkflowError::plan_not_found());
        }

        // Check durable per-binding command-key journal.
        if let Some(existing_entry) = journal.find(&request.command_key).cloned() {
            if !existing_entry.matches_intent(
                &target_plan_id,
                request.expected_revision,
                &request.profile_id,
                request.profile_revision,
                &request.summary,
                &tasks_digest,
                &payload_digest,
            ) {
                return Err(WorkflowError::IdempotencyConflict(
                    "Command key was already used with a different draft payload or target.",
                ));
            }

            if let Some(persisted_rev) = existing_revisions
                .iter()
                .find(|r| r.revision == existing_entry.target_revision)
            {
                let payload_matches_store = persisted_rev.binding_id == self.binding_id
                    && persisted_rev.profile_id == request.profile_id
                    && persisted_rev.profile_revision == request.profile_revision
                    && persisted_rev.summary == request.summary
                    && persisted_rev.tasks == request.tasks;

                match existing_entry.state {
                    CommandJournalState::Completed {
                        created_revision,
                        created_at_ms,
                        plan_digest,
                    } => {
                        if !payload_matches_store || persisted_rev.revision != created_revision {
                            return Err(WorkflowError::CorruptStore(
                                "Command journal completed record contradicts persisted plan store.",
                            ));
                        }
                        let resolved_digest = match plan_digest.or(existing_entry.plan_digest) {
                            Some(d) => Some(d),
                            None => {
                                let registry_snapshot = self.registry.snapshot()?;
                                let mut draft_view = persisted_rev.clone();
                                draft_view.status = PlanStatus::Draft;
                                preview_draft_compilation(&draft_view, &registry_snapshot)
                                    .ok()
                                    .map(|c| c.plan_digest)
                            }
                        };
                        let summary = PlanSummary {
                            plan_id: persisted_rev.plan_id.clone(),
                            binding_id: persisted_rev.binding_id.clone(),
                            revision: persisted_rev.revision,
                            profile_id: persisted_rev.profile_id.clone(),
                            profile_revision: persisted_rev.profile_revision,
                            status: PlanStatus::Draft,
                            summary: persisted_rev.summary.clone(),
                            created_at_ms,
                            task_count: persisted_rev.tasks.len(),
                            plan_digest: resolved_digest,
                        };
                        ensure_mcp_response_budget(&self.binding_id, "save_draft", &summary)?;
                        return Ok(summary);
                    }
                    CommandJournalState::Pending => {
                        if payload_matches_store {
                            let resolved_digest = match existing_entry.plan_digest {
                                Some(d) => Some(d),
                                None => {
                                    let registry_snapshot = self.registry.snapshot()?;
                                    let mut draft_view = persisted_rev.clone();
                                    draft_view.status = PlanStatus::Draft;
                                    preview_draft_compilation(&draft_view, &registry_snapshot)
                                        .ok()
                                        .map(|c| c.plan_digest)
                                }
                            };
                            if let Some(slot) = journal.find_mut(&request.command_key) {
                                slot.state = CommandJournalState::Completed {
                                    created_revision: persisted_rev.revision,
                                    created_at_ms: persisted_rev.created_at_ms,
                                    plan_digest: resolved_digest.clone(),
                                };
                            }
                            write_journal_locked(clean_dir, &self.journal_path, &journal)?;
                            let summary = PlanSummary {
                                plan_id: persisted_rev.plan_id.clone(),
                                binding_id: persisted_rev.binding_id.clone(),
                                revision: persisted_rev.revision,
                                profile_id: persisted_rev.profile_id.clone(),
                                profile_revision: persisted_rev.profile_revision,
                                status: PlanStatus::Draft,
                                summary: persisted_rev.summary.clone(),
                                created_at_ms: persisted_rev.created_at_ms,
                                task_count: persisted_rev.tasks.len(),
                                plan_digest: resolved_digest,
                            };
                            ensure_mcp_response_budget(&self.binding_id, "save_draft", &summary)?;
                            return Ok(summary);
                        }

                        // A competing changed revision already claimed `target_revision` before our
                        // pending command mutated `PlanStore`. Mark this command key `Failed` so the
                        // key stays reserved, and never overwrite or fabricate success.
                        if let Some(slot) = journal.find_mut(&request.command_key) {
                            slot.state = CommandJournalState::Failed;
                        }
                        write_journal_locked(clean_dir, &self.journal_path, &journal)?;
                        return Err(WorkflowError::StaleRevision(
                            "Plan revision is stale; reload the latest plan revision.",
                        ));
                    }
                    CommandJournalState::Failed => {
                        return Err(WorkflowError::StaleRevision(
                            "Plan revision is stale; reload the latest plan revision.",
                        ));
                    }
                }
            } else {
                // `target_revision` does not exist in `PlanStore` yet.
                match existing_entry.state {
                    CommandJournalState::Completed { .. } => {
                        return Err(WorkflowError::CorruptStore(
                            "Command journal references a missing persisted plan revision.",
                        ));
                    }
                    CommandJournalState::Failed => {
                        return Err(WorkflowError::StaleRevision(
                            "Previous draft command with this command key failed; use a new command key.",
                        ));
                    }
                    CommandJournalState::Pending => {
                        if request.expected_revision != existing_entry.expected_revision {
                            return Err(WorkflowError::IdempotencyConflict(
                                "Command key was already used with a different draft payload or target.",
                            ));
                        }
                        // Fall through to complete the pending mutation if `expected_revision` is still current.
                    }
                }
            }
        }

        let actual_revision = existing_revisions.last().map(|r| r.revision).unwrap_or(0);
        if request.expected_revision != actual_revision {
            if let Some(slot) = journal.find_mut(&request.command_key) {
                slot.state = CommandJournalState::Failed;
                let _ = write_journal_locked(clean_dir, &self.journal_path, &journal);
            }
            return Err(WorkflowError::StaleRevision(
                "Plan revision is stale; reload the latest plan revision.",
            ));
        }

        let Some(next_revision) = actual_revision.checked_add(1) else {
            return Err(WorkflowError::InvalidInput(
                "Plan revision counter overflow.",
            ));
        };
        validate_js_safe_revision("revision", next_revision)?;

        let registry_snapshot = self.registry.snapshot()?;
        let preview_revision = PlanRevision {
            plan_id: target_plan_id.clone(),
            binding_id: self.binding_id.clone(),
            revision: next_revision,
            profile_id: request.profile_id.clone(),
            profile_revision: request.profile_revision,
            summary: request.summary.clone(),
            tasks: request.tasks.clone(),
            status: PlanStatus::Draft,
            created_at_ms: 0,
        };
        let compiled = match preview_draft_compilation(&preview_revision, &registry_snapshot) {
            Ok(c) => c,
            Err(err) => {
                if let Some(slot) = journal.find_mut(&request.command_key) {
                    slot.state = CommandJournalState::Failed;
                    let _ = write_journal_locked(clean_dir, &self.journal_path, &journal);
                }
                return Err(WorkflowError::from_plan_error(&err));
            }
        };

        let preview_summary =
            PlanSummary::from_revision(&preview_revision, Some(compiled.plan_digest.clone()));
        if let Err(err) =
            ensure_mcp_response_budget(&self.binding_id, "save_draft", &preview_summary)
        {
            if let Some(slot) = journal.find_mut(&request.command_key) {
                slot.state = CommandJournalState::Failed;
                let _ = write_journal_locked(clean_dir, &self.journal_path, &journal);
            }
            return Err(err);
        }

        // Persist `Pending` command intent to the durable per-binding journal BEFORE mutating `PlanStore`.
        if let Some(slot) = journal.find_mut(&request.command_key) {
            slot.plan_digest = Some(compiled.plan_digest.clone());
            write_journal_locked(clean_dir, &self.journal_path, &journal)?;
        } else {
            if journal.entries.len() >= MAX_COMMAND_JOURNAL_ENTRIES {
                return Err(WorkflowError::InvalidInput(
                    "Workflow command journal entry limit exceeded.",
                ));
            }
            journal.entries.push(CommandJournalEntry {
                command_key: request.command_key.clone(),
                explicit_plan_id,
                resolved_plan_id: target_plan_id.clone(),
                expected_revision: request.expected_revision,
                target_revision: next_revision,
                profile_id: request.profile_id.clone(),
                profile_revision: request.profile_revision,
                summary: request.summary.clone(),
                tasks_digest,
                payload_digest,
                plan_digest: Some(compiled.plan_digest.clone()),
                state: CommandJournalState::Pending,
            });
            write_journal_locked(clean_dir, &self.journal_path, &journal)?;
        }

        let saved = match self.plans.change(
            request.expected_revision,
            PlanChange::SaveDraft {
                plan_id: target_plan_id.clone(),
                binding_id: self.binding_id.clone(),
                profile_id: request.profile_id,
                profile_revision: request.profile_revision,
                summary: request.summary,
                tasks: request.tasks,
            },
        ) {
            Ok(rev) => rev,
            Err(err) => {
                // The mutation may already have persisted (for example, a parent-directory fsync
                // failure after the atomic rename). Keep the durable `Pending` intent intact instead
                // of journaling `Failed`; a same-key retry reconciles the exact target revision and
                // payload without duplicating or overwriting a competing revision.
                return match err {
                    PlanError::BindingMismatch { .. } | PlanError::PlanNotFound { .. } => {
                        Err(WorkflowError::plan_not_found())
                    }
                    PlanError::StaleRevision { .. } => {
                        if let Ok(revs) = self.plans.plan_revisions(&target_plan_id) {
                            if revs.is_empty() || revs[0].binding_id != self.binding_id {
                                return Err(WorkflowError::plan_not_found());
                            }
                        }
                        Err(WorkflowError::StaleRevision(
                            "Plan revision is stale; reload the latest plan revision.",
                        ))
                    }
                    other => Err(WorkflowError::from_plan_error(&other)),
                };
            }
        };

        if let Some(slot) = journal.find_mut(&request.command_key) {
            slot.state = CommandJournalState::Completed {
                created_revision: saved.revision,
                created_at_ms: saved.created_at_ms,
                plan_digest: Some(compiled.plan_digest.clone()),
            };
        }
        write_journal_locked(clean_dir, &self.journal_path, &journal)?;

        let summary = PlanSummary::from_revision(&saved, Some(compiled.plan_digest));
        ensure_mcp_response_budget(&self.binding_id, "save_draft", &summary)?;
        Ok(summary)
    }

    /// Explicitly publishes `(plan_id, revision)` (`PlanChange::Publish` only) and returns its
    /// [`PublishedPlanView`].
    ///
    /// - Enforces binding ownership; cross-binding or missing plans fail with the identical static
    ///   `not_found` error.
    /// - Rejects `Superseded` or non-current revisions with `stale_revision`.
    /// - Compiles against the exact pinned `profile_revision` in the global [`RegistryStore`] and
    ///   verifies the 256 KiB MCP envelope budget **before** mutating [`PlanStore`], preventing
    ///   partial mutations when a view exceeds the wire budget.
    /// - Repeated exact `publish` calls on a current revision already in `Published`, `Received`,
    ///   or `Applied` return its current truthful state and [`threadlane_plans::DeliveryReceipt`]
    ///   without regressing `Received` or `Applied` back to `Published`.
    /// - Never advances a plan to `Received` or `Applied`, never grants operator approval, and
    ///   never spawns workers or invokes models.
    pub fn publish(
        &self,
        plan_id: &str,
        revision: u64,
    ) -> Result<PublishedPlanView, WorkflowError> {
        validate_identifier("plan_id", plan_id)?;
        if revision == 0 {
            return Err(WorkflowError::InvalidInput(
                "Published plan revision must be at least 1.",
            ));
        }
        validate_js_safe_revision("revision", revision)?;

        let revisions = self.plans.plan_revisions(plan_id)?;
        if revisions.is_empty() || revisions[0].binding_id != self.binding_id {
            return Err(WorkflowError::plan_not_found());
        }

        let latest = revisions.last().ok_or_else(WorkflowError::plan_not_found)?;
        if let Some(target) = revisions.iter().find(|r| r.revision == revision) {
            if target.status == PlanStatus::Superseded || target.revision != latest.revision {
                return Err(WorkflowError::StaleRevision(
                    "Target plan revision is superseded or stale.",
                ));
            }
        } else {
            return Err(WorkflowError::StaleRevision(
                "Target plan revision does not match the current revision.",
            ));
        }

        let registry_snapshot = self.registry.snapshot()?;

        // If the current revision is already Published, Received, or Applied, return its truthful
        // current state and receipt without regressing status.
        if latest.status.is_active_published() {
            let compiled = compile_plan_revision(latest, &registry_snapshot)?;
            let view = PublishedPlanView::from_compiled(latest, &compiled);
            ensure_mcp_response_budget(&self.binding_id, "publish", &view)?;
            return Ok(view);
        }

        // Preview compilation and verify the 256 KiB MCP wire budget BEFORE persisting `Publish`.
        let mut candidate_published = latest.clone();
        candidate_published.status = PlanStatus::Published;
        let compiled_preview = compile_plan_revision(&candidate_published, &registry_snapshot)?;
        let candidate_view =
            PublishedPlanView::from_compiled(&candidate_published, &compiled_preview);
        ensure_mcp_response_budget(&self.binding_id, "publish", &candidate_view)?;

        match self.plans.change(
            revision,
            PlanChange::Publish {
                plan_id: plan_id.to_string(),
                revision,
            },
        ) {
            Ok(published_rev) => {
                let compiled = compile_plan_revision(&published_rev, &registry_snapshot)?;
                let view = PublishedPlanView::from_compiled(&published_rev, &compiled);
                ensure_mcp_response_budget(&self.binding_id, "publish", &view)?;
                Ok(view)
            }
            Err(PlanError::ImmutablePublishedRevision { .. }) => {
                // Handle concurrent transition to Received/Applied/Superseded.
                let refreshed = self
                    .plans
                    .get_revision(plan_id, revision)?
                    .ok_or_else(WorkflowError::plan_not_found)?;
                if refreshed.binding_id != self.binding_id {
                    return Err(WorkflowError::plan_not_found());
                }
                if refreshed.status == PlanStatus::Superseded {
                    return Err(WorkflowError::StaleRevision(
                        "Target plan revision is superseded or stale.",
                    ));
                }
                let compiled = compile_plan_revision(&refreshed, &registry_snapshot)?;
                let view = PublishedPlanView::from_compiled(&refreshed, &compiled);
                ensure_mcp_response_budget(&self.binding_id, "publish", &view)?;
                Ok(view)
            }
            Err(PlanError::BindingMismatch { .. } | PlanError::PlanNotFound { .. }) => {
                Err(WorkflowError::plan_not_found())
            }
            Err(PlanError::StaleRevision { .. }) => {
                if let Ok(revs) = self.plans.plan_revisions(plan_id) {
                    if revs.is_empty() || revs[0].binding_id != self.binding_id {
                        return Err(WorkflowError::plan_not_found());
                    }
                }
                Err(WorkflowError::StaleRevision(
                    "Target plan revision is superseded or stale.",
                ))
            }
            Err(other) => Err(WorkflowError::from_plan_error(&other)),
        }
    }
}

fn validate_page_limit(
    limit: Option<usize>,
    default_limit: usize,
    max_limit: usize,
) -> Result<usize, WorkflowError> {
    let resolved = limit.unwrap_or(default_limit);
    if resolved == 0 || resolved > max_limit {
        return Err(WorkflowError::InvalidInput(
            "Requested page limit is outside the allowed range.",
        ));
    }
    Ok(resolved)
}

fn build_catalog_profile(
    profile: &WorkflowProfile,
    snapshot: &RegistrySnapshot,
) -> Result<CatalogProfile, WorkflowError> {
    let mut assignments = Vec::with_capacity(profile.assignments.len());
    for assignment in &profile.assignments {
        let model = snapshot
            .models
            .iter()
            .find(|m| m.id == assignment.choice.model_id)
            .ok_or(WorkflowError::CorruptStore(
                "Registry profile references a missing model definition.",
            ))?;
        let tool = snapshot
            .tools
            .iter()
            .find(|t| t.id == model.tool_id)
            .ok_or(WorkflowError::CorruptStore(
                "Registry model references a missing tool definition.",
            ))?;
        let route = TaskExecutionRoute::from_adapter(tool.adapter);
        let tool_enabled = tool.enabled;
        let available = tool_enabled && route.is_supported();
        assignments.push(CatalogRoleAssignment {
            role: assignment.role,
            difficulty: assignment.difficulty,
            model_id: model.id.clone(),
            model_label: model.label.clone(),
            model_arg: model.model_arg.clone(),
            effort: assignment.choice.effort.clone(),
            tool_id: tool.id.clone(),
            tool_label: tool.label.clone(),
            adapter: tool.adapter,
            route,
            tool_enabled,
            available,
        });
    }

    let is_default = snapshot.default_profile_id.as_deref() == Some(profile.id.as_str());
    let available = !assignments.is_empty() && assignments.iter().all(|a| a.available);

    Ok(CatalogProfile {
        profile_id: profile.id.clone(),
        name: profile.name.clone(),
        profile_revision: snapshot.revision,
        is_default,
        available,
        assignments,
    })
}

fn build_plan_summary(
    revision: &PlanRevision,
    registry_snapshot: &RegistrySnapshot,
) -> PlanSummary {
    let plan_digest = preview_draft_compilation(revision, registry_snapshot)
        .ok()
        .map(|compiled| compiled.plan_digest);
    PlanSummary::from_revision(revision, plan_digest)
}
