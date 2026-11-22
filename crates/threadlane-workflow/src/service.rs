use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Serialize;
use sha2::{Digest, Sha256};
use threadlane::ThreadlaneMcpServer;
use threadlane_plans::{
    compile_plan_revision, preview_draft_compilation, validate_identifier,
    validate_js_safe_revision, validate_plan_revision, validate_plan_tasks, validate_summary,
    PlanChange, PlanError, PlanRevision, PlanStatus, PlanStore, PlanTask,
    PLAN_STORE_SCHEMA_VERSION,
};
use threadlane_registry::{
    RegistrySnapshot, RegistryStore, WorkflowProfile, REGISTRY_SCHEMA_VERSION,
};

use crate::cursor::{
    decode_and_validate_cursor, encode_cursor, short_tag, CursorResource, CursorSpec,
};
use crate::error::WorkflowError;
use crate::journal::{
    compute_payload_digest_with_proof, compute_tasks_digest, journal_paths_for_binding,
    read_journal_locked, read_journal_readonly, with_journal_lock, write_journal_locked,
    CommandJournalEntry, CommandJournalState, PersistedCommandJournal, MAX_COMMAND_JOURNAL_ENTRIES,
};
use crate::types::{
    CatalogDecisionAdvisor, CatalogPage, CatalogProfile, CatalogRoleAssignment, PlanPage,
    PlanSummary, PlanSummaryPage, PublishedPlanView, SavePlanDraft, ScopedProfileDependencyProof,
    TaskExecutionRoute, DEFAULT_CATALOG_LIMIT, DEFAULT_PLAN_LIST_LIMIT, DEFAULT_PLAN_TASK_LIMIT,
    MAX_CATALOG_LIMIT, MAX_MCP_RESPONSE_BYTES, MAX_PLAN_LIST_LIMIT, MAX_PLAN_TASK_LIMIT,
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

/// Verified immutable metadata for a completed `save_draft` command journal receipt and its
/// corresponding persisted [`PlanRevision`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedSaveDraftReceipt {
    /// Original `save_draft` command key that produced this receipt.
    pub command_key: String,
    /// Optional explicit `plan_id` supplied to `save_draft` (`None` when deterministically derived).
    pub explicit_plan_id: Option<String>,
    /// Resolved `plan_id` persisted in [`PlanStore`].
    pub plan_id: String,
    /// Expected prior plan revision recorded in the journal receipt (`revision - 1`).
    pub expected_revision: u64,
    /// Plan revision number created by the completed `save_draft` command (`expected_revision + 1`).
    pub revision: u64,
    /// Workflow profile identifier bound to the persisted plan revision.
    pub profile_id: String,
    /// Registry revision bound to the persisted plan revision.
    pub profile_revision: u64,
    /// Verified plan summary recorded identically in the journal receipt and [`PlanRevision`].
    pub summary: String,
    /// Verified ordered task list persisted in [`PlanRevision`].
    pub tasks: Vec<PlanTask>,
    /// Verified 64-character lowercase hex SHA-256 digest of `tasks`.
    pub tasks_digest: String,
    /// Verified 64-character lowercase hex SHA-256 digest of the canonical `save_draft` payload.
    pub payload_digest: String,
    /// Verified creation timestamp (milliseconds since Unix epoch) recorded identically in the
    /// journal receipt and [`PlanRevision`].
    pub created_at_ms: i64,
    /// Optional verified scoped profile dependency proof bound to the `save_draft` receipt.
    pub scoped_profile_dependency_proof: Option<ScopedProfileDependencyProof>,
    /// Lifecycle status currently recorded for `(plan_id, revision)` in [`PlanStore`].
    pub status: PlanStatus,
    /// Verified 64-character lowercase hex SHA-256 plan digest for `(plan_id, revision)`.
    pub plan_digest: String,
    /// Total number of persisted [`PlanRevision`] entries for `plan_id` in this binding.
    pub plan_revision_count: usize,
    /// Highest persisted revision number for `plan_id` in this binding.
    pub latest_plan_revision: u64,
    /// Whether any persisted [`PlanRevision`] for `plan_id` in this binding has crossed the
    /// publish boundary (`published`, `received`, `applied`, or `superseded`).
    pub plan_has_published_or_superseded_revision: bool,
    /// Whether any other non-failed (`pending` or `completed`) command journal entry in this
    /// binding references `plan_id`.
    pub has_other_active_journal_entry_for_plan: bool,
}

impl VerifiedSaveDraftReceipt {
    /// Returns `true` if and only if this receipt proves a sole unpublished first draft
    /// (`expected_revision == 0`, `revision == 1`, `status == PlanStatus::Draft`) with no
    /// additional persisted revisions, no published or superseded revision for the same
    /// `plan_id`, and no other active (`pending` or `completed`) journal entry for `plan_id`.
    #[must_use]
    pub const fn is_sole_unpublished_first_draft(&self) -> bool {
        self.expected_revision == 0
            && self.revision == 1
            && matches!(self.status, PlanStatus::Draft)
            && self.plan_revision_count == 1
            && self.latest_plan_revision == 1
            && !self.plan_has_published_or_superseded_revision
            && !self.has_other_active_journal_entry_for_plan
    }
}

/// Read-only indexed snapshot of a single binding's workflow command journal, persisted plan
/// revisions, and registry snapshot for bounded `save_draft` receipt verification.
///
/// Constructed once via [`ScopedWorkflow::read_save_draft_receipts_readonly_at`] without opening
/// writer locks or creating directories or lockfiles, then queried repeatedly in memory by
/// `command_key`.
#[derive(Debug)]
pub struct ReadonlySaveDraftReceipts {
    binding_id: String,
    registry: RegistrySnapshot,
    journal: Option<PersistedCommandJournal>,
    journal_entries_by_key: BTreeMap<String, usize>,
    journal_entries_by_plan_rev: BTreeMap<(String, u64), Vec<usize>>,
    journal_entries_by_plan: BTreeMap<String, Vec<usize>>,
    revisions_by_key: BTreeMap<(String, u64), PlanRevision>,
    revisions_by_plan: BTreeMap<String, Vec<u64>>,
}

impl ReadonlySaveDraftReceipts {
    /// Verifies a completed `save_draft` journal receipt for `command_key` against the preloaded
    /// binding-scoped command journal, plan revisions, and registry snapshot.
    ///
    /// - Returns `Ok(None)` when `command_key` is absent from the binding's command journal.
    /// - Supports both explicit `plan_id` receipts (`explicit_plan_id == Some(plan_id)`) and
    ///   implicit deterministic `plan_id` receipts
    ///   (`resolved_plan_id == deterministic_plan_id(binding_id, command_key)`).
    /// - Returns `Err` when a matching journal entry is not `Completed`, lacks or disagrees with
    ///   its persisted binding-scoped [`PlanRevision`], conflicts with another journal entry for
    ///   the same `(plan_id, revision)`, or fails tasks/payload/compiled `plan_digest` checks.
    pub fn verify_completed_save_draft(
        &self,
        command_key: &str,
    ) -> Result<Option<VerifiedSaveDraftReceipt>, WorkflowError> {
        validate_identifier("command_key", command_key)?;
        let Some(journal) = self.journal.as_ref() else {
            return Ok(None);
        };
        let Some(&entry_idx) = self.journal_entries_by_key.get(command_key) else {
            return Ok(None);
        };
        let entry = journal
            .entries
            .get(entry_idx)
            .ok_or(WorkflowError::CorruptStore(
                "Indexed workflow command journal entry is missing.",
            ))?;
        if entry.command_key != command_key {
            return Err(WorkflowError::CorruptStore(
                "Workflow command journal entry key mismatch.",
            ));
        }

        validate_journal_entry_plan_id(&self.binding_id, command_key, entry)?;
        let (created_revision, created_at_ms, completed_digest) =
            extract_completed_journal_receipt_fields(entry)?;

        let plan_rev_key = (entry.resolved_plan_id.clone(), created_revision);
        let rev = self
            .revisions_by_key
            .get(&plan_rev_key)
            .ok_or(WorkflowError::CorruptStore(
                "Completed save_draft receipt references a missing or cross-binding plan revision.",
            ))?;
        validate_plan_revision(rev).map_err(|_| {
            WorkflowError::CorruptStore(
                "Persisted plan revision referenced by save_draft receipt is invalid.",
            )
        })?;
        if rev.binding_id != self.binding_id
            || rev.plan_id != entry.resolved_plan_id
            || rev.revision != created_revision
            || rev.created_at_ms != created_at_ms
            || rev.profile_id != entry.profile_id
            || rev.profile_revision != entry.profile_revision
            || rev.summary != entry.summary
            || rev.profile_revision == 0
            || rev.profile_revision > self.registry.revision
        {
            return Err(WorkflowError::CorruptStore(
                "Persisted plan revision metadata does not match completed save_draft receipt.",
            ));
        }

        let expected_tasks_digest = compute_tasks_digest(&rev.tasks)?;
        if expected_tasks_digest != entry.tasks_digest {
            return Err(WorkflowError::CorruptStore(
                "Persisted plan revision tasks_digest does not match completed save_draft receipt.",
            ));
        }
        let expected_payload_digest = compute_payload_digest_with_proof(
            &rev.plan_id,
            &rev.profile_id,
            rev.profile_revision,
            &rev.summary,
            &rev.tasks,
            entry.scoped_profile_dependency_proof.as_ref(),
        )?;
        if expected_payload_digest != entry.payload_digest {
            return Err(WorkflowError::CorruptStore(
                "Persisted plan revision payload_digest does not match completed save_draft receipt.",
            ));
        }

        verify_unique_plan_revision_journal_entry(
            journal,
            &self.journal_entries_by_plan_rev,
            &plan_rev_key,
            entry_idx,
            rev,
            &expected_tasks_digest,
        )?;
        verify_receipt_compiled_digest(&self.registry, entry, rev, completed_digest)?;

        let plan_revisions =
            self.revisions_by_plan
                .get(&rev.plan_id)
                .ok_or(WorkflowError::CorruptStore(
                    "Indexed plan revision is missing from plan revision summary index.",
                ))?;
        let plan_revision_count = plan_revisions.len();
        let mut latest_plan_revision = 0u64;
        let mut plan_has_published_or_superseded_revision = false;
        for &rev_num in plan_revisions {
            latest_plan_revision = latest_plan_revision.max(rev_num);
            let candidate = self
                .revisions_by_key
                .get(&(rev.plan_id.clone(), rev_num))
                .ok_or(WorkflowError::CorruptStore(
                    "Indexed plan revision entry is missing from plan revision map.",
                ))?;
            if candidate.status.is_immutable_published() {
                plan_has_published_or_superseded_revision = true;
            }
        }

        let mut has_other_active_journal_entry_for_plan = false;
        if let Some(plan_journal_indices) = self.journal_entries_by_plan.get(&rev.plan_id) {
            for &other_idx in plan_journal_indices {
                if other_idx == entry_idx {
                    continue;
                }
                let other = journal
                    .entries
                    .get(other_idx)
                    .ok_or(WorkflowError::CorruptStore(
                        "Indexed workflow command journal entry is missing.",
                    ))?;
                if !matches!(other.state, CommandJournalState::Failed) {
                    has_other_active_journal_entry_for_plan = true;
                    break;
                }
            }
        }

        Ok(Some(VerifiedSaveDraftReceipt {
            command_key: entry.command_key.clone(),
            explicit_plan_id: entry.explicit_plan_id.clone(),
            plan_id: rev.plan_id.clone(),
            expected_revision: entry.expected_revision,
            revision: rev.revision,
            profile_id: rev.profile_id.clone(),
            profile_revision: rev.profile_revision,
            summary: rev.summary.clone(),
            tasks: rev.tasks.clone(),
            tasks_digest: entry.tasks_digest.clone(),
            payload_digest: entry.payload_digest.clone(),
            created_at_ms: rev.created_at_ms,
            scoped_profile_dependency_proof: entry.scoped_profile_dependency_proof.clone(),
            status: rev.status,
            plan_digest: completed_digest.to_string(),
            plan_revision_count,
            latest_plan_revision,
            plan_has_published_or_superseded_revision,
            has_other_active_journal_entry_for_plan,
        }))
    }
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

    /// Reads a scoped plan header and its verified digest without opening stores or writer locks.
    ///
    /// Existing private directories and the same bounded ledger/registry parsers are
    /// required. Superseded or registry-stale publication digests require the frozen
    /// command journal proof. This historical read never admits execution.
    pub fn read_plan_summary_readonly_at(
        registry_dir: &Path,
        plans_dir: &Path,
        binding_id: &str,
        plan_id: &str,
        revision: u64,
    ) -> Result<PlanSummary, WorkflowError> {
        validate_identifier("binding_id", binding_id)?;
        validate_identifier("plan_id", plan_id)?;
        validate_js_safe_revision("revision", revision)?;
        if revision == 0 {
            return Err(WorkflowError::InvalidInput(
                "Requested plan revision must be at least 1.",
            ));
        }
        let registry = RegistryStore::read_snapshot_readonly_at(registry_dir)?;
        let revisions = PlanStore::read_snapshot_readonly_at(plans_dir)?;
        let target = revisions
            .iter()
            .find(|candidate| {
                candidate.binding_id == binding_id
                    && candidate.plan_id == plan_id
                    && candidate.revision == revision
            })
            .ok_or_else(WorkflowError::plan_not_found)?;
        let (journal_path, _) = journal_paths_for_binding(plans_dir, binding_id)?;
        Ok(build_plan_summary_readonly(
            target,
            &registry,
            binding_id,
            plans_dir,
            &journal_path,
            &mut None,
        ))
    }

    /// Reads and indexes a binding's workflow command journal, persisted plan revisions, and
    /// registry snapshot once without opening stores or acquiring writer locks.
    ///
    /// Existing private store directories and the same bounded ledger/journal/registry parsers are
    /// required. Callers can query the returned [`ReadonlySaveDraftReceipts`] repeatedly in memory
    /// by `command_key` to verify completed `save_draft` receipts (including explicit `plan_id`
    /// revisions) without rescanning the journal from disk.
    pub fn read_save_draft_receipts_readonly_at(
        registry_dir: &Path,
        plans_dir: &Path,
        binding_id: &str,
    ) -> Result<ReadonlySaveDraftReceipts, WorkflowError> {
        let validated_binding = validate_identifier("binding_id", binding_id)?;
        let registry = RegistryStore::read_snapshot_readonly_at(registry_dir)?;
        let revisions = PlanStore::read_snapshot_readonly_at(plans_dir)?;
        let (journal_path, _) = journal_paths_for_binding(plans_dir, &validated_binding)?;
        let journal = read_journal_readonly(plans_dir, &journal_path, &validated_binding)?;

        let mut journal_entries_by_key = BTreeMap::new();
        let mut journal_entries_by_plan_rev: BTreeMap<(String, u64), Vec<usize>> = BTreeMap::new();
        let mut journal_entries_by_plan: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        if let Some(ref persisted) = journal {
            for (idx, entry) in persisted.entries.iter().enumerate() {
                journal_entries_by_key.insert(entry.command_key.clone(), idx);
                index_journal_entry_plan_revisions(&mut journal_entries_by_plan_rev, idx, entry);
                index_journal_entry_plans(&mut journal_entries_by_plan, idx, entry);
            }
        }

        let mut revisions_by_key = BTreeMap::new();
        let mut revisions_by_plan: BTreeMap<String, Vec<u64>> = BTreeMap::new();
        for rev in revisions {
            if rev.binding_id == validated_binding {
                let plan_id = rev.plan_id.clone();
                let revision = rev.revision;
                if revisions_by_key
                    .insert((plan_id.clone(), revision), rev)
                    .is_some()
                {
                    return Err(WorkflowError::CorruptStore(
                        "Duplicate plan revision found in binding snapshot.",
                    ));
                }
                revisions_by_plan.entry(plan_id).or_default().push(revision);
            }
        }

        Ok(ReadonlySaveDraftReceipts {
            binding_id: validated_binding,
            registry,
            journal,
            journal_entries_by_key,
            journal_entries_by_plan_rev,
            journal_entries_by_plan,
            revisions_by_key,
            revisions_by_plan,
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
    /// - Exposes only the five active execution role assignments ([`WorkflowRole::is_execution_role`]),
    ///   omitting legacy [`WorkflowRole::Follower`] assignments from catalog entries and active profile
    ///   availability calculations while preserving stored historical profile snapshots and full model/tool
    ///   corruption validation.
    /// - Assignments referencing disabled tools are marked `tool_enabled: false` and `available: false`
    ///   without substituting another model or tool.
    /// - Optional `decision_advisor` metadata is resolved from the same `registry_revision` snapshot;
    ///   it never probes a provider, changes assignment counts or `available`, or grants execution
    ///   authority.
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
    /// - Active published revisions whose pinned `profile_revision` predates the current registry
    ///   revision resolve `plan_digest` read-only from the binding's verified `Completed` command
    ///   journal receipt without opening lock files or recompiling against changed role choices.
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

        if start_idx >= own_revisions.len() {
            let empty_page = PlanSummaryPage {
                binding_id: self.binding_id.clone(),
                plans: Vec::new(),
                next_cursor: None,
                has_more: false,
            };
            ensure_mcp_response_budget(&self.binding_id, "list_plans", &empty_page)?;
            return Ok(empty_page);
        }

        let max_take = requested_limit.min(own_revisions.len() - start_idx);
        let page_summaries = self.build_plan_summaries(
            &own_revisions[start_idx..start_idx + max_take],
            &registry_snapshot,
        );
        for take_count in (1..=max_take).rev() {
            let end_idx = start_idx + take_count;
            let next_cursor = if end_idx < own_revisions.len() {
                Some(encode_cursor(
                    &cursor_spec,
                    end_idx,
                    &page_summaries[take_count - 1].plan_id,
                )?)
            } else {
                None
            };
            let has_more = next_cursor.is_some();
            let candidate = PlanSummaryPage {
                binding_id: self.binding_id.clone(),
                plans: page_summaries[..take_count].to_vec(),
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
    /// - Active published revisions whose pinned `profile_revision` predates the current registry
    ///   revision resolve `header.plan_digest` read-only from the binding's verified `Completed`
    ///   command journal receipt without opening lock files or recompiling against changed role choices.
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
        let mut cached_journal = None;
        let header = self.build_plan_summary_with_journal_cache(
            target_rev,
            &registry_snapshot,
            &mut cached_journal,
        );

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
        self.save_draft_with_scoped_proof(request, None)
    }

    /// Creates or updates a draft plan revision scoped to this binding, optionally binding a
    /// verified [`ScopedProfileDependencyProof`] into the durable command journal so unrelated
    /// global registry edits do not invalidate compilation or publication while the selected
    /// profile dependencies remain unchanged.
    pub fn save_draft_with_scoped_proof(
        &self,
        request: SavePlanDraft,
        scoped_profile_dependency_proof: Option<ScopedProfileDependencyProof>,
    ) -> Result<PlanSummary, WorkflowError> {
        validate_identifier("command_key", &request.command_key)?;
        validate_js_safe_revision("expected_revision", request.expected_revision)?;
        validate_identifier("profile_id", &request.profile_id)?;
        validate_js_safe_revision("profile_revision", request.profile_revision)?;
        validate_summary(&request.summary)?;

        if let Some(ref proof) = scoped_profile_dependency_proof {
            proof.validate()?;
            if proof.spec.profile_id != request.profile_id {
                return Err(WorkflowError::InvalidInput(
                    "Scoped profile dependency proof profile_id does not match draft request.",
                ));
            }
            if proof.provenance_revision != request.profile_revision {
                return Err(WorkflowError::InvalidInput(
                    "Scoped profile dependency proof provenance_revision does not match draft request.",
                ));
            }
        }

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
        let payload_digest = compute_payload_digest_with_proof(
            &target_plan_id,
            &request.profile_id,
            request.profile_revision,
            &request.summary,
            &request.tasks,
            scoped_profile_dependency_proof.as_ref(),
        )?;

        with_journal_lock(self.plans.dir(), &self.journal_lock_path, |clean_dir| {
            self.save_draft_locked(
                clean_dir,
                request,
                explicit_plan_id,
                target_plan_id,
                tasks_digest,
                payload_digest,
                scoped_profile_dependency_proof,
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn save_draft_locked(
        &self,
        clean_dir: &Path,
        request: SavePlanDraft,
        explicit_plan_id: Option<String>,
        target_plan_id: String,
        tasks_digest: String,
        payload_digest: String,
        scoped_profile_dependency_proof: Option<ScopedProfileDependencyProof>,
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
                scoped_profile_dependency_proof.as_ref(),
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
                                resolve_compilation_snapshot(
                                    &registry_snapshot,
                                    scoped_profile_dependency_proof.as_ref(),
                                )
                                .ok()
                                .and_then(|snap| preview_draft_compilation(&draft_view, &snap).ok())
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
                                    resolve_compilation_snapshot(
                                        &registry_snapshot,
                                        scoped_profile_dependency_proof.as_ref(),
                                    )
                                    .ok()
                                    .and_then(|snap| {
                                        preview_draft_compilation(&draft_view, &snap).ok()
                                    })
                                    .map(|c| c.plan_digest)
                                }
                            };
                            if let Some(slot) = journal.find_mut(&request.command_key) {
                                slot.plan_digest = resolved_digest.clone();
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
        let compilation_snapshot = match resolve_compilation_snapshot(
            &registry_snapshot,
            scoped_profile_dependency_proof.as_ref(),
        ) {
            Ok(snap) => snap,
            Err(err) => {
                if let Some(slot) = journal.find_mut(&request.command_key) {
                    slot.state = CommandJournalState::Failed;
                    let _ = write_journal_locked(clean_dir, &self.journal_path, &journal);
                }
                return Err(err);
            }
        };
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
        let compiled = match preview_draft_compilation(&preview_revision, &compilation_snapshot) {
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
            slot.scoped_profile_dependency_proof = scoped_profile_dependency_proof;
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
                scoped_profile_dependency_proof,
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
    /// - Compiles against the exact pinned `profile_revision` in the global [`RegistryStore`] (or
    ///   against the journal's verified [`ScopedProfileDependencyProof`] when present and current
    ///   selected dependencies still match) and verifies the 256 KiB MCP envelope budget **before**
    ///   mutating [`PlanStore`], preventing partial mutations when a view exceeds the wire budget.
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
        let journal_opt =
            read_journal_readonly(self.plans.dir(), &self.journal_path, &self.binding_id)?;
        let verified_proof = journal_opt.as_ref().and_then(|journal| {
            journal.verified_scoped_profile_dependency_proof(&self.binding_id, latest)
        });
        let compilation_snapshot =
            resolve_compilation_snapshot(&registry_snapshot, verified_proof)?;

        // If the current revision is already Published, Received, or Applied, return its truthful
        // current state and receipt without regressing status.
        if latest.status.is_active_published() {
            let compiled = compile_plan_revision(latest, &compilation_snapshot)?;
            let view = PublishedPlanView::from_compiled(latest, &compiled);
            ensure_mcp_response_budget(&self.binding_id, "publish", &view)?;
            return Ok(view);
        }

        // Preview compilation and verify the 256 KiB MCP wire budget BEFORE persisting `Publish`.
        let mut candidate_published = latest.clone();
        candidate_published.status = PlanStatus::Published;
        let compiled_preview = compile_plan_revision(&candidate_published, &compilation_snapshot)?;
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
                let compiled = compile_plan_revision(&published_rev, &compilation_snapshot)?;
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
                let compiled = compile_plan_revision(&refreshed, &compilation_snapshot)?;
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

    fn build_plan_summaries(
        &self,
        revisions: &[PlanRevision],
        registry_snapshot: &RegistrySnapshot,
    ) -> Vec<PlanSummary> {
        let mut cached_journal = None;
        revisions
            .iter()
            .map(|rev| {
                self.build_plan_summary_with_journal_cache(
                    rev,
                    registry_snapshot,
                    &mut cached_journal,
                )
            })
            .collect()
    }

    fn build_plan_summary_with_journal_cache(
        &self,
        revision: &PlanRevision,
        registry_snapshot: &RegistrySnapshot,
        cached_journal: &mut Option<Option<PersistedCommandJournal>>,
    ) -> PlanSummary {
        build_plan_summary_readonly(
            revision,
            registry_snapshot,
            &self.binding_id,
            self.plans.dir(),
            &self.journal_path,
            cached_journal,
        )
    }
}

fn resolve_compilation_snapshot(
    live_snapshot: &RegistrySnapshot,
    scoped_profile_dependency_proof: Option<&ScopedProfileDependencyProof>,
) -> Result<RegistrySnapshot, WorkflowError> {
    match scoped_profile_dependency_proof {
        Some(proof) => proof.verify_against_live_snapshot(live_snapshot),
        None => Ok(live_snapshot.clone()),
    }
}

fn build_plan_summary_readonly(
    revision: &PlanRevision,
    registry_snapshot: &RegistrySnapshot,
    binding_id: &str,
    plans_dir: &Path,
    journal_path: &Path,
    cached_journal: &mut Option<Option<PersistedCommandJournal>>,
) -> PlanSummary {
    // Superseded revisions must never be recompiled or admitted for execution.
    // Their frozen journal proof is required even without registry drift.
    let historical_proof_required = revision.status == PlanStatus::Superseded
        || (revision.status.is_active_published()
            && revision.profile_revision > 0
            && revision.profile_revision < registry_snapshot.revision);
    let plan_digest = if historical_proof_required {
        if revision.binding_id == binding_id
            && revision.profile_revision > 0
            && revision.profile_revision <= registry_snapshot.revision
        {
            let journal_opt = cached_journal.get_or_insert_with(|| {
                read_journal_readonly(plans_dir, journal_path, binding_id)
                    .ok()
                    .flatten()
            });
            journal_opt
                .as_ref()
                .and_then(|journal| journal.verified_historical_plan_digest(binding_id, revision))
        } else {
            None
        }
    } else if let Ok(compiled) = preview_draft_compilation(revision, registry_snapshot) {
        Some(compiled.plan_digest)
    } else if revision.binding_id == binding_id
        && revision.profile_revision > 0
        && revision.profile_revision <= registry_snapshot.revision
    {
        let journal_opt = cached_journal.get_or_insert_with(|| {
            read_journal_readonly(plans_dir, journal_path, binding_id)
                .ok()
                .flatten()
        });
        journal_opt
            .as_ref()
            .and_then(|journal| {
                journal.verified_scoped_profile_dependency_proof(binding_id, revision)
            })
            .and_then(|proof| proof.verify_against_live_snapshot(registry_snapshot).ok())
            .and_then(|scoped_snapshot| {
                preview_draft_compilation(revision, &scoped_snapshot)
                    .ok()
                    .map(|compiled| compiled.plan_digest)
            })
    } else {
        None
    };
    PlanSummary::from_revision(revision, plan_digest)
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
        if !assignment.role.is_execution_role() {
            continue;
        }
        let route = TaskExecutionRoute::from_adapter(tool.adapter);
        let tool_enabled = tool.enabled;
        let available = tool_enabled && route.is_supported();
        assignments.push(CatalogRoleAssignment {
            role: assignment.role,
            role_label: assignment.role.display_label().to_string(),
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

    // Advisor references are resolved from this same snapshot with no default inference or
    // substitution. Registry writes already validate the configured model and tool; a missing
    // reference can only indicate a corrupt/tampered snapshot and uses the same static,
    // sanitized error convention as role assignments.
    let decision_advisor = match profile.decision_advisor.as_ref() {
        Some(advisor) => {
            let model = snapshot
                .models
                .iter()
                .find(|model| model.id == advisor.model_id)
                .ok_or(WorkflowError::CorruptStore(
                    "Registry profile references a missing model definition.",
                ))?;
            let tool = snapshot
                .tools
                .iter()
                .find(|tool| tool.id == model.tool_id)
                .ok_or(WorkflowError::CorruptStore(
                    "Registry model references a missing tool definition.",
                ))?;
            Some(CatalogDecisionAdvisor {
                mode: advisor.mode,
                model_id: model.id.clone(),
                model_label: model.label.clone(),
                model_arg: model.model_arg.clone(),
                tool_id: tool.id.clone(),
                tool_label: tool.label.clone(),
                adapter: tool.adapter,
            })
        }
        None => None,
    };

    Ok(CatalogProfile {
        profile_id: profile.id.clone(),
        name: profile.name.clone(),
        profile_revision: snapshot.revision,
        is_default,
        available,
        assignments,
        decision_advisor,
    })
}

fn index_journal_entry_plan_revisions(
    index: &mut BTreeMap<(String, u64), Vec<usize>>,
    entry_idx: usize,
    entry: &CommandJournalEntry,
) {
    let mut plan_ids = Vec::with_capacity(2);
    plan_ids.push(entry.resolved_plan_id.as_str());
    if let Some(explicit) = entry.explicit_plan_id.as_deref() {
        if explicit != entry.resolved_plan_id {
            plan_ids.push(explicit);
        }
    }
    let mut rev_nums = Vec::with_capacity(2);
    rev_nums.push(entry.target_revision);
    if let CommandJournalState::Completed {
        created_revision, ..
    } = entry.state
    {
        if created_revision != entry.target_revision {
            rev_nums.push(created_revision);
        }
    }
    for plan_id in plan_ids {
        for &rev_num in &rev_nums {
            index
                .entry((plan_id.to_string(), rev_num))
                .or_default()
                .push(entry_idx);
        }
    }
}

fn index_journal_entry_plans(
    index: &mut BTreeMap<String, Vec<usize>>,
    entry_idx: usize,
    entry: &CommandJournalEntry,
) {
    index
        .entry(entry.resolved_plan_id.clone())
        .or_default()
        .push(entry_idx);
    if let Some(explicit) = entry
        .explicit_plan_id
        .as_deref()
        .filter(|id| *id != entry.resolved_plan_id.as_str())
    {
        index
            .entry(explicit.to_string())
            .or_default()
            .push(entry_idx);
    }
}

fn validate_journal_entry_plan_id(
    binding_id: &str,
    command_key: &str,
    entry: &CommandJournalEntry,
) -> Result<(), WorkflowError> {
    match entry.explicit_plan_id.as_deref() {
        Some(explicit) => {
            validate_identifier("plan_id", explicit)?;
            if explicit != entry.resolved_plan_id {
                return Err(WorkflowError::CorruptStore(
                    "Completed save_draft receipt explicit_plan_id does not match resolved_plan_id.",
                ));
            }
        }
        None => {
            let expected_plan_id = deterministic_plan_id(binding_id, command_key)?;
            if entry.resolved_plan_id != expected_plan_id {
                return Err(WorkflowError::CorruptStore(
                    "Completed save_draft receipt resolved_plan_id does not match deterministic plan ID.",
                ));
            }
        }
    }
    Ok(())
}

fn extract_completed_journal_receipt_fields(
    entry: &CommandJournalEntry,
) -> Result<(u64, i64, &str), WorkflowError> {
    let (created_revision, created_at_ms, completed_digest) = match &entry.state {
        CommandJournalState::Completed {
            created_revision,
            created_at_ms,
            plan_digest,
        } => {
            let (Some(entry_digest), Some(state_digest)) =
                (entry.plan_digest.as_deref(), plan_digest.as_deref())
            else {
                return Err(WorkflowError::CorruptStore(
                    "Completed save_draft receipt is missing plan_digest.",
                ));
            };
            if entry_digest != state_digest || !is_lowercase_hex_64(state_digest) {
                return Err(WorkflowError::CorruptStore(
                    "Completed save_draft receipt has inconsistent or malformed plan_digest.",
                ));
            }
            (*created_revision, *created_at_ms, state_digest)
        }
        CommandJournalState::Pending | CommandJournalState::Failed => {
            return Err(WorkflowError::InvalidStateTransition(
                "Workflow command journal entry for save_draft key is not completed.",
            ));
        }
    };

    if created_revision == 0
        || created_revision != entry.target_revision
        || entry.expected_revision.checked_add(1) != Some(created_revision)
        || created_at_ms < 0
    {
        return Err(WorkflowError::CorruptStore(
            "Completed save_draft receipt has invalid revision or timestamp metadata.",
        ));
    }
    Ok((created_revision, created_at_ms, completed_digest))
}

fn verify_unique_plan_revision_journal_entry(
    journal: &PersistedCommandJournal,
    index: &BTreeMap<(String, u64), Vec<usize>>,
    plan_rev_key: &(String, u64),
    entry_idx: usize,
    rev: &PlanRevision,
    expected_tasks_digest: &str,
) -> Result<(), WorkflowError> {
    let Some(touching_indices) = index.get(plan_rev_key) else {
        return Err(WorkflowError::CorruptStore(
            "Indexed workflow command journal entry is missing from plan revision index.",
        ));
    };
    let mut saw_this_completed = false;
    for &other_idx in touching_indices {
        let other = journal
            .entries
            .get(other_idx)
            .ok_or(WorkflowError::CorruptStore(
                "Indexed workflow command journal entry is missing.",
            ))?;
        if other.resolved_plan_id != rev.plan_id
            || other
                .explicit_plan_id
                .as_deref()
                .is_some_and(|id| id != rev.plan_id)
        {
            return Err(WorkflowError::CorruptStore(
                "Conflicting workflow command journal entry targets the same plan revision.",
            ));
        }
        match &other.state {
            CommandJournalState::Pending => {
                return Err(WorkflowError::CorruptStore(
                    "Pending workflow command journal entry conflicts with completed save_draft receipt.",
                ));
            }
            CommandJournalState::Failed => {
                let other_payload_digest = compute_payload_digest_with_proof(
                    &rev.plan_id,
                    &rev.profile_id,
                    rev.profile_revision,
                    &rev.summary,
                    &rev.tasks,
                    other.scoped_profile_dependency_proof.as_ref(),
                )?;
                if other.target_revision == rev.revision
                    && other.profile_id == rev.profile_id
                    && other.profile_revision == rev.profile_revision
                    && other.summary == rev.summary
                    && other.tasks_digest == expected_tasks_digest
                    && other.payload_digest == other_payload_digest
                {
                    return Err(WorkflowError::CorruptStore(
                        "Failed workflow command journal entry conflicts with completed save_draft receipt.",
                    ));
                }
            }
            CommandJournalState::Completed { .. } => {
                if other_idx != entry_idx || saw_this_completed {
                    return Err(WorkflowError::CorruptStore(
                        "Duplicate completed workflow command journal entries target the same plan revision.",
                    ));
                }
                saw_this_completed = true;
            }
        }
    }
    if !saw_this_completed {
        return Err(WorkflowError::CorruptStore(
            "Completed save_draft receipt was not uniquely indexed for its plan revision.",
        ));
    }
    Ok(())
}

fn verify_receipt_compiled_digest(
    registry: &RegistrySnapshot,
    entry: &CommandJournalEntry,
    rev: &PlanRevision,
    completed_digest: &str,
) -> Result<(), WorkflowError> {
    if let Some(ref proof) = entry.scoped_profile_dependency_proof {
        proof.validate().map_err(|_| {
            WorkflowError::CorruptStore(
                "Completed save_draft receipt contains an invalid scoped profile dependency proof.",
            )
        })?;
        if proof.provenance_revision != rev.profile_revision
            || proof.spec.profile_id != rev.profile_id
        {
            return Err(WorkflowError::CorruptStore(
                "Completed save_draft receipt scoped profile dependency proof does not match plan revision.",
            ));
        }
        let proof_snapshot = proof
            .spec
            .to_scoped_registry_snapshot(proof.provenance_revision);
        let mut draft_view = rev.clone();
        draft_view.status = PlanStatus::Draft;
        let compiled = preview_draft_compilation(&draft_view, &proof_snapshot).map_err(|_| {
            WorkflowError::CorruptStore(
                "Completed save_draft receipt failed scoped compilation digest verification.",
            )
        })?;
        if compiled.plan_digest != completed_digest {
            return Err(WorkflowError::CorruptStore(
                "Completed save_draft receipt plan_digest does not match compiled plan.",
            ));
        }
    } else if rev.profile_revision == registry.revision {
        let mut draft_view = rev.clone();
        draft_view.status = PlanStatus::Draft;
        let compiled = preview_draft_compilation(&draft_view, registry).map_err(|_| {
            WorkflowError::CorruptStore(
                "Completed save_draft receipt failed registry compilation digest verification.",
            )
        })?;
        if compiled.plan_digest != completed_digest {
            return Err(WorkflowError::CorruptStore(
                "Completed save_draft receipt plan_digest does not match compiled plan.",
            ));
        }
    } else if !(rev.status.is_active_published() || rev.status == PlanStatus::Superseded) {
        return Err(WorkflowError::CorruptStore(
            "Historical draft receipt cannot be verified without scoped proof after registry advancement.",
        ));
    }
    Ok(())
}

fn is_lowercase_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[cfg(all(test, unix))]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    use tempfile::TempDir;
    use threadlane_registry::{
        Difficulty, ModelChoice, ModelDefinition, RegistryChange, RoleAssignment, ToolAdapter,
        ToolDefinition, WorkflowRole,
    };

    use super::*;

    struct ReceiptTestFixture {
        _tmp: TempDir,
        reg_dir: PathBuf,
        plans_dir: PathBuf,
        registry: Arc<RegistryStore>,
        plans: Arc<PlanStore>,
    }

    fn setup_test_workflow_dirs() -> ReceiptTestFixture {
        let tmp = TempDir::new().expect("create tempdir");
        let reg_dir = tmp.path().join("registry-store");
        let plans_dir = tmp.path().join("plans-store");
        let registry = Arc::new(RegistryStore::open(&reg_dir).expect("open registry"));
        let plans = Arc::new(PlanStore::open(&plans_dir).expect("open plans"));

        let s1 = registry
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "tool-agy".to_string(),
                        label: "Antigravity".to_string(),
                        adapter: ToolAdapter::Antigravity,
                        executable_path: Some(PathBuf::from("/usr/bin/agy")),
                        enabled: true,
                    },
                },
            )
            .expect("upsert tool");
        let s2 = registry
            .apply(
                s1.revision,
                RegistryChange::UpsertModel {
                    model: ModelDefinition {
                        id: "model-argon".to_string(),
                        tool_id: "tool-agy".to_string(),
                        label: "Argon".to_string(),
                        model_arg: "gemini-4-argon-high".to_string(),
                        efforts: vec!["high".to_string()],
                        allow_no_effort: false,
                    },
                },
            )
            .expect("upsert model");
        registry
            .apply(
                s2.revision,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        id: "profile-main".to_string(),
                        name: "Main profile".to_string(),
                        assignments: vec![RoleAssignment {
                            role: WorkflowRole::Coder,
                            difficulty: Some(Difficulty::Hard),
                            choice: ModelChoice {
                                model_id: "model-argon".to_string(),
                                effort: Some("high".to_string()),
                            },
                        }],
                        decision_advisor: None,
                    },
                },
            )
            .expect("upsert profile");

        ReceiptTestFixture {
            _tmp: tmp,
            reg_dir,
            plans_dir,
            registry,
            plans,
        }
    }

    fn sample_plan_task(id: &str) -> PlanTask {
        PlanTask {
            id: id.to_string(),
            title: format!("Task {id}"),
            instruction: format!("Implement {id} cleanly."),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/lib.rs".to_string()],
        }
    }

    #[test]
    fn readonly_save_draft_receipts_verify_implicit_explicit_superseded_and_reject_tampering() {
        let fixture = setup_test_workflow_dirs();
        let reg_dir = fixture.reg_dir;
        let plans_dir = fixture.plans_dir;
        let workflow = ScopedWorkflow::new(
            Arc::clone(&fixture.registry),
            Arc::clone(&fixture.plans),
            "binding-a".to_string(),
        )
        .expect("workflow binding-a");

        // 1. Implicit plan_id rev 1 later superseded by rev 2 on the same plan_id.
        let imp_r1 = workflow
            .save_draft(SavePlanDraft {
                command_key: "cmd_imp_r1".to_string(),
                plan_id: None,
                expected_revision: 0,
                profile_id: "profile-main".to_string(),
                profile_revision: 3,
                summary: "Implicit plan rev 1".to_string(),
                tasks: vec![sample_plan_task("task_imp_1")],
            })
            .expect("save implicit rev 1");
        let imp_r1_pub = workflow
            .publish(&imp_r1.plan_id, imp_r1.revision)
            .expect("publish implicit rev 1");
        let imp_r2 = workflow
            .save_draft(SavePlanDraft {
                command_key: "cmd_imp_r2".to_string(),
                plan_id: Some(imp_r1.plan_id.clone()),
                expected_revision: 1,
                profile_id: "profile-main".to_string(),
                profile_revision: 3,
                summary: "Implicit plan rev 2".to_string(),
                tasks: vec![sample_plan_task("task_imp_2")],
            })
            .expect("save implicit rev 2");
        let imp_r2_pub = workflow
            .publish(&imp_r2.plan_id, imp_r2.revision)
            .expect("publish implicit rev 2");

        // 2. Explicit plan_id rev 1.
        let exp_r1 = workflow
            .save_draft(SavePlanDraft {
                command_key: "cmd_exp_r1".to_string(),
                plan_id: Some("plan_explicit_u".to_string()),
                expected_revision: 0,
                profile_id: "profile-main".to_string(),
                profile_revision: 3,
                summary: "Explicit plan rev 1".to_string(),
                tasks: vec![sample_plan_task("task_exp_1")],
            })
            .expect("save explicit rev 1");
        let exp_r1_pub = workflow
            .publish(&exp_r1.plan_id, exp_r1.revision)
            .expect("publish explicit rev 1");

        // Remove lockfiles to verify read-only index creation touches no locks.
        for lock_path in [
            reg_dir.join("registry.lock"),
            plans_dir.join("plans.lock"),
            workflow.journal_lock_path.clone(),
        ] {
            if lock_path.exists() {
                fs::remove_file(&lock_path).expect("remove lockfile before readonly read");
            }
        }

        let receipts =
            ScopedWorkflow::read_save_draft_receipts_readonly_at(&reg_dir, &plans_dir, "binding-a")
                .expect("read binding-a save_draft receipts");
        assert!(!reg_dir.join("registry.lock").exists());
        assert!(!plans_dir.join("plans.lock").exists());
        assert!(!workflow.journal_lock_path.exists());

        assert_eq!(
            receipts
                .verify_completed_save_draft("cmd_absent")
                .expect("absent key returns None"),
            None
        );

        let verified_imp_r1 = receipts
            .verify_completed_save_draft("cmd_imp_r1")
            .expect("verify superseded implicit rev 1")
            .expect("imp r1 receipt present");
        assert_eq!(verified_imp_r1.explicit_plan_id, None);
        assert_eq!(verified_imp_r1.plan_id, imp_r1.plan_id);
        assert_eq!(verified_imp_r1.revision, 1);
        assert_eq!(verified_imp_r1.status, PlanStatus::Superseded);
        assert_eq!(
            Some(verified_imp_r1.plan_digest),
            imp_r1_pub.header.plan_digest
        );

        let verified_imp_r2 = receipts
            .verify_completed_save_draft("cmd_imp_r2")
            .expect("verify active implicit rev 2")
            .expect("imp r2 receipt present");
        assert_eq!(
            verified_imp_r2.explicit_plan_id,
            Some(imp_r1.plan_id.clone())
        );
        assert_eq!(verified_imp_r2.revision, 2);
        assert_eq!(verified_imp_r2.status, PlanStatus::Published);
        assert_eq!(
            Some(verified_imp_r2.plan_digest),
            imp_r2_pub.header.plan_digest
        );

        let verified_exp_r1 = receipts
            .verify_completed_save_draft("cmd_exp_r1")
            .expect("verify explicit rev 1")
            .expect("exp r1 receipt present");
        assert_eq!(
            verified_exp_r1.explicit_plan_id.as_deref(),
            Some("plan_explicit_u")
        );
        assert_eq!(verified_exp_r1.plan_id, "plan_explicit_u");
        assert_eq!(verified_exp_r1.revision, 1);
        assert_eq!(verified_exp_r1.status, PlanStatus::Published);
        assert_eq!(
            Some(verified_exp_r1.plan_digest),
            exp_r1_pub.header.plan_digest
        );

        // Cross-binding isolation: binding-b cannot resolve binding-a's keys.
        let other_binding =
            ScopedWorkflow::read_save_draft_receipts_readonly_at(&reg_dir, &plans_dir, "binding-b")
                .expect("read binding-b save_draft receipts");
        assert_eq!(
            other_binding
                .verify_completed_save_draft("cmd_imp_r1")
                .expect("cross-binding lookup returns None"),
            None
        );

        // Tamper checks against `workflow-commands-*.json` and `plans.json`.
        let journal_path = workflow.command_journal_path().to_path_buf();
        let original_journal = fs::read(&journal_path).expect("read original journal");
        let plans_path = plans_dir.join("plans.json");
        let original_plans = fs::read(&plans_path).expect("read original plans.json");

        let write_private = |path: &Path, bytes: &[u8]| {
            fs::write(path, bytes).expect("write tampered file");
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("chmod 0600");
        };

        // Tampered `plan_digest` on Superseded rev 1 must fail compiled digest verification.
        let mut bad_digest: PersistedCommandJournal =
            serde_json::from_slice(&original_journal).expect("parse journal");
        let slot = bad_digest.find_mut("cmd_imp_r1").expect("cmd_imp_r1");
        let forged_digest = "0".repeat(64);
        slot.plan_digest = Some(forged_digest.clone());
        if let CommandJournalState::Completed {
            ref mut plan_digest,
            ..
        } = slot.state
        {
            *plan_digest = Some(forged_digest);
        }
        write_private(
            &journal_path,
            &serde_json::to_vec_pretty(&bad_digest).expect("serialize bad_digest"),
        );
        let tampered_index =
            ScopedWorkflow::read_save_draft_receipts_readonly_at(&reg_dir, &plans_dir, "binding-a")
                .expect("load tampered digest index");
        assert!(matches!(
            tampered_index.verify_completed_save_draft("cmd_imp_r1"),
            Err(WorkflowError::CorruptStore(_))
        ));

        // Tampered `resolved_plan_id` when `explicit_plan_id` is None must fail.
        let mut bad_resolved: PersistedCommandJournal =
            serde_json::from_slice(&original_journal).expect("parse journal");
        bad_resolved
            .find_mut("cmd_imp_r1")
            .expect("cmd_imp_r1")
            .resolved_plan_id = "plan_explicit_u".to_string();
        write_private(
            &journal_path,
            &serde_json::to_vec_pretty(&bad_resolved).expect("serialize bad_resolved"),
        );
        let bad_resolved_index =
            ScopedWorkflow::read_save_draft_receipts_readonly_at(&reg_dir, &plans_dir, "binding-a")
                .expect("load bad_resolved index");
        assert!(matches!(
            bad_resolved_index.verify_completed_save_draft("cmd_imp_r1"),
            Err(WorkflowError::CorruptStore(_))
        ));

        // Non-completed `Pending` and `Failed` states must refuse.
        for state in [CommandJournalState::Pending, CommandJournalState::Failed] {
            let mut non_completed: PersistedCommandJournal =
                serde_json::from_slice(&original_journal).expect("parse journal");
            non_completed
                .find_mut("cmd_exp_r1")
                .expect("cmd_exp_r1")
                .state = state;
            write_private(
                &journal_path,
                &serde_json::to_vec_pretty(&non_completed).expect("serialize non_completed"),
            );
            let idx = ScopedWorkflow::read_save_draft_receipts_readonly_at(
                &reg_dir,
                &plans_dir,
                "binding-a",
            )
            .expect("load non_completed index");
            assert!(matches!(
                idx.verify_completed_save_draft("cmd_exp_r1"),
                Err(WorkflowError::InvalidStateTransition(_))
            ));
        }

        // Restore journal and remove `plan_explicit_u` from `plans.json` -> missing revision fails.
        write_private(&journal_path, &original_journal);
        let mut plans_val: serde_json::Value =
            serde_json::from_slice(&original_plans).expect("parse plans.json");
        plans_val["revisions"]
            .as_array_mut()
            .expect("revisions array")
            .retain(|rev| rev["plan_id"] != "plan_explicit_u");
        write_private(
            &plans_path,
            &serde_json::to_vec_pretty(&plans_val).expect("serialize trimmed plans.json"),
        );
        let missing_plan_index =
            ScopedWorkflow::read_save_draft_receipts_readonly_at(&reg_dir, &plans_dir, "binding-a")
                .expect("load missing_plan index");
        assert!(matches!(
            missing_plan_index.verify_completed_save_draft("cmd_exp_r1"),
            Err(WorkflowError::CorruptStore(_))
        ));
    }

    #[test]
    fn readonly_save_draft_receipts_verify_unpublished_first_draft_all_fields_and_contradictions() {
        let fixture = setup_test_workflow_dirs();
        let reg_dir = fixture.reg_dir;
        let plans_dir = fixture.plans_dir;
        let workflow = ScopedWorkflow::new(
            Arc::clone(&fixture.registry),
            Arc::clone(&fixture.plans),
            "binding-draft".to_string(),
        )
        .expect("workflow binding-draft");

        let reg_snapshot = fixture.registry.snapshot().expect("registry snapshot");
        let scoped_proof =
            ScopedProfileDependencyProof::from_snapshot(&reg_snapshot, "profile-main")
                .expect("build scoped profile dependency proof");

        // 1. Implicit unpublished first draft (no scoped proof).
        let imp_tasks = vec![sample_plan_task("task_draft_imp_1")];
        let imp_summary = "Implicit unpublished first draft".to_string();
        let imp_draft = workflow
            .save_draft(SavePlanDraft {
                command_key: "cmd_draft_imp_1".to_string(),
                plan_id: None,
                expected_revision: 0,
                profile_id: "profile-main".to_string(),
                profile_revision: 3,
                summary: imp_summary.clone(),
                tasks: imp_tasks.clone(),
            })
            .expect("save implicit unpublished draft");

        // 2. Explicit unpublished first draft with scoped profile dependency proof.
        let exp_tasks = vec![sample_plan_task("task_draft_exp_1")];
        let exp_summary = "Explicit unpublished first draft with scoped proof".to_string();
        let exp_draft = workflow
            .save_draft_with_scoped_proof(
                SavePlanDraft {
                    command_key: "cmd_draft_exp_1".to_string(),
                    plan_id: Some("plan_draft_scoped".to_string()),
                    expected_revision: 0,
                    profile_id: "profile-main".to_string(),
                    profile_revision: 3,
                    summary: exp_summary.clone(),
                    tasks: exp_tasks.clone(),
                },
                Some(scoped_proof.clone()),
            )
            .expect("save explicit unpublished draft with proof");

        // 3. Published rev 1 + unpublished rev 2 draft (NOT a sole unpublished first draft).
        let multi_r1 = workflow
            .save_draft(SavePlanDraft {
                command_key: "cmd_multi_r1".to_string(),
                plan_id: Some("plan_multi_rev".to_string()),
                expected_revision: 0,
                profile_id: "profile-main".to_string(),
                profile_revision: 3,
                summary: "Multi rev 1 published".to_string(),
                tasks: vec![sample_plan_task("task_multi_1")],
            })
            .expect("save multi rev 1");
        workflow
            .publish(&multi_r1.plan_id, multi_r1.revision)
            .expect("publish multi rev 1");
        let _multi_r2 = workflow
            .save_draft(SavePlanDraft {
                command_key: "cmd_multi_r2".to_string(),
                plan_id: Some("plan_multi_rev".to_string()),
                expected_revision: 1,
                profile_id: "profile-main".to_string(),
                profile_revision: 3,
                summary: "Multi rev 2 draft".to_string(),
                tasks: vec![sample_plan_task("task_multi_2")],
            })
            .expect("save multi rev 2 draft");

        // 4. Two-revision published plan used to test 7e-style rev 1 Draft + rev 2 Published.
        let e7_r1 = workflow
            .save_draft(SavePlanDraft {
                command_key: "cmd_7e_r1".to_string(),
                plan_id: Some("plan_7e_case".to_string()),
                expected_revision: 0,
                profile_id: "profile-main".to_string(),
                profile_revision: 3,
                summary: "7e rev 1".to_string(),
                tasks: vec![sample_plan_task("task_7e_1")],
            })
            .expect("save 7e rev 1");
        workflow
            .publish(&e7_r1.plan_id, e7_r1.revision)
            .expect("publish 7e rev 1");
        let e7_r2 = workflow
            .save_draft(SavePlanDraft {
                command_key: "cmd_7e_r2".to_string(),
                plan_id: Some("plan_7e_case".to_string()),
                expected_revision: 1,
                profile_id: "profile-main".to_string(),
                profile_revision: 3,
                summary: "7e rev 2".to_string(),
                tasks: vec![sample_plan_task("task_7e_2")],
            })
            .expect("save 7e rev 2");
        workflow
            .publish(&e7_r2.plan_id, e7_r2.revision)
            .expect("publish 7e rev 2");

        // Remove lockfiles to verify read-only indexing remains strictly lock-free.
        for lock_path in [
            reg_dir.join("registry.lock"),
            plans_dir.join("plans.lock"),
            workflow.journal_lock_path.clone(),
        ] {
            if lock_path.exists() {
                fs::remove_file(&lock_path).expect("remove lockfile");
            }
        }

        let receipts = ScopedWorkflow::read_save_draft_receipts_readonly_at(
            &reg_dir,
            &plans_dir,
            "binding-draft",
        )
        .expect("read readonly save_draft receipts");
        assert!(!reg_dir.join("registry.lock").exists());
        assert!(!plans_dir.join("plans.lock").exists());
        assert!(!workflow.journal_lock_path.exists());

        // Verify EVERY field on the implicit unpublished first draft.
        let verified_imp = receipts
            .verify_completed_save_draft("cmd_draft_imp_1")
            .expect("verify implicit draft")
            .expect("implicit draft receipt present");
        let expected_imp_plan_id =
            deterministic_plan_id("binding-draft", "cmd_draft_imp_1").expect("deterministic id");
        let expected_imp_tasks_digest =
            compute_tasks_digest(&imp_tasks).expect("implicit tasks digest");
        let expected_imp_payload_digest = compute_payload_digest_with_proof(
            &expected_imp_plan_id,
            "profile-main",
            3,
            &imp_summary,
            &imp_tasks,
            None,
        )
        .expect("implicit payload digest");
        assert_eq!(verified_imp.command_key, "cmd_draft_imp_1");
        assert_eq!(verified_imp.explicit_plan_id, None);
        assert_eq!(verified_imp.plan_id, expected_imp_plan_id);
        assert_eq!(verified_imp.plan_id, imp_draft.plan_id);
        assert_eq!(verified_imp.expected_revision, 0);
        assert_eq!(verified_imp.revision, 1);
        assert_eq!(verified_imp.profile_id, "profile-main");
        assert_eq!(verified_imp.profile_revision, 3);
        assert_eq!(verified_imp.summary, imp_summary);
        assert_eq!(verified_imp.tasks, imp_tasks);
        assert_eq!(verified_imp.tasks_digest, expected_imp_tasks_digest);
        assert_eq!(verified_imp.payload_digest, expected_imp_payload_digest);
        assert!(verified_imp.created_at_ms >= 0);
        assert_eq!(verified_imp.scoped_profile_dependency_proof, None);
        assert_eq!(verified_imp.status, PlanStatus::Draft);
        assert_eq!(
            Some(verified_imp.plan_digest.clone()),
            imp_draft.plan_digest
        );
        assert_eq!(verified_imp.plan_revision_count, 1);
        assert_eq!(verified_imp.latest_plan_revision, 1);
        assert!(!verified_imp.plan_has_published_or_superseded_revision);
        assert!(!verified_imp.has_other_active_journal_entry_for_plan);
        assert!(verified_imp.is_sole_unpublished_first_draft());

        // Verify EVERY field on the explicit unpublished first draft with scoped proof.
        let verified_exp = receipts
            .verify_completed_save_draft("cmd_draft_exp_1")
            .expect("verify explicit draft")
            .expect("explicit draft receipt present");
        let expected_exp_tasks_digest =
            compute_tasks_digest(&exp_tasks).expect("explicit tasks digest");
        let expected_exp_payload_digest = compute_payload_digest_with_proof(
            "plan_draft_scoped",
            "profile-main",
            3,
            &exp_summary,
            &exp_tasks,
            Some(&scoped_proof),
        )
        .expect("explicit payload digest");
        assert_eq!(verified_exp.command_key, "cmd_draft_exp_1");
        assert_eq!(
            verified_exp.explicit_plan_id.as_deref(),
            Some("plan_draft_scoped")
        );
        assert_eq!(verified_exp.plan_id, "plan_draft_scoped");
        assert_eq!(verified_exp.expected_revision, 0);
        assert_eq!(verified_exp.revision, 1);
        assert_eq!(verified_exp.summary, exp_summary);
        assert_eq!(verified_exp.tasks, exp_tasks);
        assert_eq!(verified_exp.tasks_digest, expected_exp_tasks_digest);
        assert_eq!(verified_exp.payload_digest, expected_exp_payload_digest);
        assert_eq!(
            verified_exp.scoped_profile_dependency_proof,
            Some(scoped_proof.clone())
        );
        assert_eq!(verified_exp.status, PlanStatus::Draft);
        assert_eq!(
            Some(verified_exp.plan_digest.clone()),
            exp_draft.plan_digest
        );
        assert!(verified_exp.is_sole_unpublished_first_draft());

        // Rev 2 Draft alongside Published rev 1 is NOT a sole unpublished first draft.
        let verified_multi_r2 = receipts
            .verify_completed_save_draft("cmd_multi_r2")
            .expect("verify multi rev 2 draft")
            .expect("multi rev 2 receipt present");
        assert_eq!(verified_multi_r2.expected_revision, 1);
        assert_eq!(verified_multi_r2.revision, 2);
        assert_eq!(verified_multi_r2.status, PlanStatus::Draft);
        assert_eq!(verified_multi_r2.plan_revision_count, 2);
        assert_eq!(verified_multi_r2.latest_plan_revision, 2);
        assert!(verified_multi_r2.plan_has_published_or_superseded_revision);
        assert!(verified_multi_r2.has_other_active_journal_entry_for_plan);
        assert!(!verified_multi_r2.is_sole_unpublished_first_draft());

        // Snapshot disk state for tamper and contradiction checks.
        let journal_path = workflow.command_journal_path().to_path_buf();
        let original_journal = fs::read(&journal_path).expect("read original journal");
        let plans_path = plans_dir.join("plans.json");
        let original_plans = fs::read(&plans_path).expect("read original plans.json");
        let write_private = |path: &Path, bytes: &[u8]| {
            fs::write(path, bytes).expect("write private test file");
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("chmod 0600");
        };

        // 7e-style contradiction: rev 1 mutated back to Draft while rev 2 remains Published.
        // Returns verified metadata with `is_sole_unpublished_first_draft() == false` so callers
        // fail closed with validation error instead of silently accepting Draft.
        {
            let mut plans_val: serde_json::Value =
                serde_json::from_slice(&original_plans).expect("parse plans");
            let rev1 = plans_val["revisions"]
                .as_array_mut()
                .expect("revisions")
                .iter_mut()
                .find(|r| r["plan_id"] == "plan_7e_case" && r["revision"] == 1)
                .expect("7e rev 1");
            rev1["status"] = serde_json::json!("draft");
            write_private(
                &plans_path,
                &serde_json::to_vec_pretty(&plans_val).expect("serialize 7e plans"),
            );
            let idx = ScopedWorkflow::read_save_draft_receipts_readonly_at(
                &reg_dir,
                &plans_dir,
                "binding-draft",
            )
            .expect("load 7e index");
            let r = idx
                .verify_completed_save_draft("cmd_7e_r1")
                .expect("7e receipt metadata returned")
                .expect("7e rev 1 receipt present");
            assert_eq!(r.status, PlanStatus::Draft);
            assert_eq!(r.plan_revision_count, 2);
            assert_eq!(r.latest_plan_revision, 2);
            assert!(r.plan_has_published_or_superseded_revision);
            assert!(r.has_other_active_journal_entry_for_plan);
            assert!(!r.is_sole_unpublished_first_draft());
            write_private(&plans_path, &original_plans);
        }

        // Extra Pending journal entry for the same plan_id disqualifies sole first draft.
        {
            let mut journal_data: PersistedCommandJournal =
                serde_json::from_slice(&original_journal).expect("parse journal");
            let mut pending_r2 = journal_data
                .find("cmd_draft_imp_1")
                .expect("cmd_draft_imp_1")
                .clone();
            pending_r2.command_key = "cmd_draft_imp_pending_r2".to_string();
            pending_r2.explicit_plan_id = Some(imp_draft.plan_id.clone());
            pending_r2.expected_revision = 1;
            pending_r2.target_revision = 2;
            pending_r2.plan_digest = None;
            pending_r2.state = CommandJournalState::Pending;
            journal_data.entries.push(pending_r2);
            write_private(
                &journal_path,
                &serde_json::to_vec_pretty(&journal_data).expect("serialize journal"),
            );
            let idx = ScopedWorkflow::read_save_draft_receipts_readonly_at(
                &reg_dir,
                &plans_dir,
                "binding-draft",
            )
            .expect("load pending-sibling index");
            let r = idx
                .verify_completed_save_draft("cmd_draft_imp_1")
                .expect("verify rev 1 with pending rev 2")
                .expect("rev 1 receipt present");
            assert!(r.has_other_active_journal_entry_for_plan);
            assert!(!r.is_sole_unpublished_first_draft());
            write_private(&journal_path, &original_journal);
        }

        // Tamper checks on Draft receipt fields (summary, tasks_digest, payload_digest, created_at_ms).
        {
            let mut bad_tasks: PersistedCommandJournal =
                serde_json::from_slice(&original_journal).expect("parse journal");
            bad_tasks
                .find_mut("cmd_draft_imp_1")
                .expect("cmd_draft_imp_1")
                .tasks_digest = "a".repeat(64);
            write_private(
                &journal_path,
                &serde_json::to_vec_pretty(&bad_tasks).expect("serialize bad tasks_digest"),
            );
            let idx = ScopedWorkflow::read_save_draft_receipts_readonly_at(
                &reg_dir,
                &plans_dir,
                "binding-draft",
            )
            .expect("load bad tasks_digest index");
            assert!(matches!(
                idx.verify_completed_save_draft("cmd_draft_imp_1"),
                Err(WorkflowError::CorruptStore(_))
            ));

            let mut bad_payload: PersistedCommandJournal =
                serde_json::from_slice(&original_journal).expect("parse journal");
            bad_payload
                .find_mut("cmd_draft_imp_1")
                .expect("cmd_draft_imp_1")
                .payload_digest = "b".repeat(64);
            write_private(
                &journal_path,
                &serde_json::to_vec_pretty(&bad_payload).expect("serialize bad payload_digest"),
            );
            let idx = ScopedWorkflow::read_save_draft_receipts_readonly_at(
                &reg_dir,
                &plans_dir,
                "binding-draft",
            )
            .expect("load bad payload_digest index");
            assert!(matches!(
                idx.verify_completed_save_draft("cmd_draft_imp_1"),
                Err(WorkflowError::CorruptStore(_))
            ));

            let mut bad_ts: PersistedCommandJournal =
                serde_json::from_slice(&original_journal).expect("parse journal");
            if let CommandJournalState::Completed {
                ref mut created_at_ms,
                ..
            } = bad_ts
                .find_mut("cmd_draft_imp_1")
                .expect("cmd_draft_imp_1")
                .state
            {
                *created_at_ms += 1;
            }
            write_private(
                &journal_path,
                &serde_json::to_vec_pretty(&bad_ts).expect("serialize bad created_at_ms"),
            );
            let idx = ScopedWorkflow::read_save_draft_receipts_readonly_at(
                &reg_dir,
                &plans_dir,
                "binding-draft",
            )
            .expect("load bad created_at_ms index");
            assert!(matches!(
                idx.verify_completed_save_draft("cmd_draft_imp_1"),
                Err(WorkflowError::CorruptStore(_))
            ));
            write_private(&journal_path, &original_journal);

            let mut bad_summary_plans: serde_json::Value =
                serde_json::from_slice(&original_plans).expect("parse plans");
            let rev = bad_summary_plans["revisions"]
                .as_array_mut()
                .expect("revisions")
                .iter_mut()
                .find(|r| r["plan_id"] == imp_draft.plan_id && r["revision"] == 1)
                .expect("imp draft rev 1");
            rev["summary"] = serde_json::json!("Tampered draft summary");
            write_private(
                &plans_path,
                &serde_json::to_vec_pretty(&bad_summary_plans).expect("serialize tampered summary"),
            );
            let idx = ScopedWorkflow::read_save_draft_receipts_readonly_at(
                &reg_dir,
                &plans_dir,
                "binding-draft",
            )
            .expect("load tampered summary index");
            assert!(matches!(
                idx.verify_completed_save_draft("cmd_draft_imp_1"),
                Err(WorkflowError::CorruptStore(_))
            ));
            write_private(&plans_path, &original_plans);
        }

        // Advance registry revision: scoped-proof draft remains verifiable; unproved draft refuses.
        fixture
            .registry
            .apply(
                3,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "tool-other".to_string(),
                        label: "Unrelated tool".to_string(),
                        adapter: ToolAdapter::OpenCodeV2,
                        executable_path: Some(PathBuf::from("/usr/bin/opencode")),
                        enabled: true,
                    },
                },
            )
            .expect("advance registry to revision 4");
        let after_adv = ScopedWorkflow::read_save_draft_receipts_readonly_at(
            &reg_dir,
            &plans_dir,
            "binding-draft",
        )
        .expect("read receipts after registry advancement");
        let exp_after_adv = after_adv
            .verify_completed_save_draft("cmd_draft_exp_1")
            .expect("scoped-proof draft remains valid after registry advancement")
            .expect("explicit draft present");
        assert!(exp_after_adv.is_sole_unpublished_first_draft());
        assert_eq!(
            exp_after_adv.scoped_profile_dependency_proof,
            Some(scoped_proof)
        );
        assert!(matches!(
            after_adv.verify_completed_save_draft("cmd_draft_imp_1"),
            Err(WorkflowError::CorruptStore(_))
        ));
    }
}
