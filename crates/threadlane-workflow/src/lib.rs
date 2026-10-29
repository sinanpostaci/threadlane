#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! `threadlane-workflow`: synchronous, binding-scoped workflow profile catalog and versioned
//! plan service for Threadlane Milestone 4.
//!
//! ## Core Invariants
//!
//! - **Trusted binding scope**: [`ScopedWorkflow::new`] binds a shared global [`RegistryStore`],
//!   a [`PlanStore`], and a single validated `binding_id`. Callers never supply filesystem paths
//!   or binding identifiers in requests.
//! - **Cross-binding isolation**: `list_plans` and `get_plan` only expose plans owned by the
//!   configured `binding_id`. Any attempt to read, create, update, or publish a plan owned by
//!   another binding (or an unknown plan with `expected_revision > 0`) fails with the identical
//!   static [`WorkflowError::plan_not_found`] (`not_found`), never leaking other bindings' IDs
//!   or revision counters.
//! - **Public view redaction**: [`CatalogPage`], [`PlanSummaryPage`], [`PlanPage`], [`PlanSummary`],
//!   and [`PublishedPlanView`] serialize in `snake_case` and never expose CLI `executable_path`
//!   values or credentials.
//! - **Validated compilation & explicit publish boundary**: `save_draft` validates task DAGs,
//!   write scopes, and role/difficulty resolution against the pinned `profile_revision` via
//!   [`preview_draft_compilation`] before mutating [`PlanStore`]. `publish` transitions a draft
//!   exclusively via [`PlanChange::Publish`] after verifying both frozen registry compilation and
//!   the 256 KiB MCP wire budget. Neither `list_plans`, `get_plan`, nor `publish` ever advances
//!   delivery receipts to `received`/`applied` or spawns workers/models.
//! - **Exact 256 KiB MCP envelope budget**: Every public response is verified against
//!   [`ThreadlaneMcpServer::build_structured_tool_result`] (`<= 256 KiB`). `get_plan` paginates
//!   full, untruncated [`PlanTask`] instructions across pages and fails with `response_too_large`
//!   if a single item exceeds the 256 KiB envelope.

mod cursor;
/// Redacted workflow error codes and static user-safe error types.
pub mod error;
mod journal;
/// Synchronous binding-scoped workflow catalog and plan service (`ScopedWorkflow`).
pub mod service;
/// Public `snake_case` DTOs for catalog pages, plan summaries, task pages, draft requests, and published routes.
pub mod types;

pub use error::WorkflowError;
pub use service::{
    deterministic_plan_id, ensure_mcp_response_budget, measure_mcp_response_bytes, ScopedWorkflow,
};
pub use threadlane_plans::{
    compile_plan_revision, preview_draft_compilation, DeliveryReceipt, PlanChange, PlanError,
    PlanRevision, PlanStatus, PlanStore, PlanTask,
};
pub use threadlane_registry::{
    Difficulty, ModelChoice, ModelDefinition, RegistryChange, RegistryError, RegistrySnapshot,
    RegistryStore, RoleAssignment, ToolAdapter, ToolDefinition, WorkflowProfile, WorkflowRole,
};
pub use types::{
    CatalogAssignment, CatalogPage, CatalogProfile, CatalogRoleAssignment, PlanHeader, PlanPage,
    PlanSummary, PlanSummaryPage, PublicTaskRoute, PublishedPlanView, SavePlanDraft,
    TaskExecutionRoute, TaskRoute, DEFAULT_CATALOG_LIMIT, DEFAULT_PLAN_LIST_LIMIT,
    DEFAULT_PLAN_TASK_LIMIT, MAX_CATALOG_LIMIT, MAX_CURSOR_LEN, MAX_MCP_RESPONSE_BYTES,
    MAX_PLAN_LIST_LIMIT, MAX_PLAN_TASK_LIMIT,
};

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Arc, Barrier};
    use std::thread;

    use tempfile::TempDir;
    use threadlane::ThreadlaneMcpServer;

    use crate::journal::{
        compute_payload_digest, compute_tasks_digest, read_journal_locked, with_journal_lock,
        write_journal_locked, CommandJournalEntry, CommandJournalState, PersistedCommandJournal,
        MAX_COMMAND_JOURNAL_ENTRIES,
    };

    use super::*;

    fn sample_tool(id: &str, adapter: ToolAdapter, enabled: bool) -> ToolDefinition {
        ToolDefinition {
            id: id.to_string(),
            label: format!("Tool {id}"),
            adapter,
            executable_path: if adapter.is_cli_adapter() {
                Some(PathBuf::from(format!("/opt/secret-cli-paths/{id}-bin")))
            } else {
                None
            },
            enabled,
        }
    }

    fn sample_model(
        id: &str,
        tool_id: &str,
        model_arg: &str,
        efforts: &[&str],
        allow_no_effort: bool,
    ) -> ModelDefinition {
        ModelDefinition {
            id: id.to_string(),
            tool_id: tool_id.to_string(),
            label: format!("Model {id}"),
            model_arg: model_arg.to_string(),
            efforts: efforts.iter().map(|s| (*s).to_string()).collect(),
            allow_no_effort,
        }
    }

    fn sample_task(
        id: &str,
        role: WorkflowRole,
        difficulty: Difficulty,
        depends_on: &[&str],
        write_paths: &[&str],
        instruction: &str,
    ) -> PlanTask {
        PlanTask {
            id: id.to_string(),
            title: format!("Title for {id}"),
            instruction: instruction.to_string(),
            role,
            difficulty,
            depends_on: depends_on.iter().map(|s| (*s).to_string()).collect(),
            allowed_write_paths: write_paths.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    fn setup_stores() -> (TempDir, Arc<RegistryStore>, Arc<PlanStore>, u64) {
        let tmp = TempDir::new().expect("tempdir");
        let reg_dir = tmp.path().join("global-registry");
        let plans_dir = tmp.path().join("plans-store");

        let registry = Arc::new(RegistryStore::open(&reg_dir).expect("open registry"));
        let plans = Arc::new(PlanStore::open(&plans_dir).expect("open plans"));

        let s1 = registry
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: sample_tool("agy", ToolAdapter::Antigravity, true),
                },
            )
            .expect("upsert agy");
        let s2 = registry
            .apply(
                s1.revision,
                RegistryChange::UpsertTool {
                    tool: sample_tool("opencode", ToolAdapter::OpencodeV2, true),
                },
            )
            .expect("upsert opencode");
        let s3 = registry
            .apply(
                s2.revision,
                RegistryChange::UpsertTool {
                    tool: sample_tool("codex", ToolAdapter::CodexCli, true),
                },
            )
            .expect("upsert codex");
        let s4 = registry
            .apply(
                s3.revision,
                RegistryChange::UpsertTool {
                    tool: sample_tool("git", ToolAdapter::Git, true),
                },
            )
            .expect("upsert git");
        let s5 = registry
            .apply(
                s4.revision,
                RegistryChange::UpsertModels {
                    models: vec![
                        sample_model(
                            "argon-high",
                            "agy",
                            "gemini-4-argon-high",
                            &["medium", "high"],
                            false,
                        ),
                        sample_model(
                            "deepseek-flash",
                            "opencode",
                            "opencode-go/deepseek-v4.1-flash",
                            &["max"],
                            false,
                        ),
                        sample_model("luna-max", "codex", "gpt-6-luna", &["high", "max"], false),
                        sample_model("git-helper", "git", "git-cli", &[], true),
                    ],
                },
            )
            .expect("upsert models");

        let profile = WorkflowProfile {
            id: "default-profile".to_string(),
            name: "Default Workflow Profile".to_string(),
            assignments: vec![
                RoleAssignment {
                    role: WorkflowRole::Follower,
                    difficulty: None,
                    choice: ModelChoice {
                        model_id: "luna-max".to_string(),
                        effort: Some("max".to_string()),
                    },
                },
                RoleAssignment {
                    role: WorkflowRole::Coder,
                    difficulty: Some(Difficulty::Easy),
                    choice: ModelChoice {
                        model_id: "deepseek-flash".to_string(),
                        effort: Some("max".to_string()),
                    },
                },
                RoleAssignment {
                    role: WorkflowRole::Coder,
                    difficulty: Some(Difficulty::Hard),
                    choice: ModelChoice {
                        model_id: "argon-high".to_string(),
                        effort: Some("high".to_string()),
                    },
                },
                RoleAssignment {
                    role: WorkflowRole::Reviewer,
                    difficulty: None,
                    choice: ModelChoice {
                        model_id: "argon-high".to_string(),
                        effort: Some("high".to_string()),
                    },
                },
                RoleAssignment {
                    role: WorkflowRole::Closure,
                    difficulty: Some(Difficulty::Extreme),
                    choice: ModelChoice {
                        model_id: "git-helper".to_string(),
                        effort: None,
                    },
                },
            ],
        };

        let s6 = registry
            .apply(s5.revision, RegistryChange::UpsertProfile { profile })
            .expect("upsert profile");
        let s7 = registry
            .apply(
                s6.revision,
                RegistryChange::SetDefaultProfile {
                    profile_id: Some("default-profile".to_string()),
                },
            )
            .expect("set default profile");

        (tmp, registry, plans, s7.revision)
    }

    #[test]
    fn catalog_pagination_disabled_tools_and_no_executable_or_secret_exposure() {
        let (_tmp, registry, plans, rev) = setup_stores();
        let wf = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-1".into(),
        )
        .expect("create workflow");

        // Add 22 additional profiles (total 23) to verify default 20 and max 20 pagination.
        let mut current_rev = rev;
        for idx in 1..=22 {
            let p = WorkflowProfile {
                id: format!("extra-profile-{idx:02}"),
                name: format!("Extra Profile {idx}"),
                assignments: vec![RoleAssignment {
                    role: WorkflowRole::Coder,
                    difficulty: Some(Difficulty::Hard),
                    choice: ModelChoice {
                        model_id: "argon-high".to_string(),
                        effort: Some("high".to_string()),
                    },
                }],
            };
            current_rev = registry
                .apply(current_rev, RegistryChange::UpsertProfile { profile: p })
                .expect("upsert extra profile")
                .revision;
        }

        // Limit > 20 and limit == 0 must be rejected.
        assert_eq!(
            wf.catalog(None, Some(0)).expect_err("limit 0").code(),
            "invalid_input"
        );
        assert_eq!(
            wf.catalog(None, Some(21)).expect_err("limit 21").code(),
            "invalid_input"
        );

        // Default limit is 20.
        let page1 = wf.catalog(None, None).expect("catalog page 1");
        assert_eq!(page1.registry_revision, current_rev);
        assert_eq!(page1.default_profile_id.as_deref(), Some("default-profile"));
        assert_eq!(page1.profiles.len(), 20);
        assert!(page1.has_more);
        assert!(page1.next_cursor.is_some());
        assert!(page1.profiles[0].is_default);
        assert_eq!(page1.profiles[0].profile_revision, current_rev);

        let page2 = wf
            .catalog(page1.next_cursor.clone(), None)
            .expect("catalog page 2");
        assert_eq!(page2.profiles.len(), 3);
        assert!(!page2.has_more);
        assert!(page2.next_cursor.is_none());

        // Verify no CLI executable paths or secret tokens appear in serialized catalog output.
        let envelope =
            ThreadlaneMcpServer::build_structured_tool_result("binding-1", "catalog", &page1);
        let serialized = serde_json::to_string(&envelope).expect("serialize catalog envelope");
        assert!(serialized.len() <= MAX_MCP_RESPONSE_BYTES);
        assert!(!serialized.contains("/opt/secret-cli-paths"));
        assert!(!serialized.contains("executable_path"));

        // Disable `opencode` tool and verify `deepseek-flash` assignment is clearly marked
        // unavailable without substituting `argon-high` or another tool.
        let disabled_snap = registry
            .apply(
                current_rev,
                RegistryChange::UpsertTool {
                    tool: sample_tool("opencode", ToolAdapter::OpencodeV2, false),
                },
            )
            .expect("disable opencode");

        // Old cursor from `current_rev` must now be rejected as stale!
        let stale_cursor_err = wf
            .catalog(page1.next_cursor, None)
            .expect_err("cursor after registry mutation must fail");
        assert_eq!(stale_cursor_err.code(), "stale_revision");

        let refreshed = wf.catalog(None, Some(5)).expect("refreshed catalog");
        assert_eq!(refreshed.registry_revision, disabled_snap.revision);
        let def_prof = &refreshed.profiles[0];
        assert_eq!(def_prof.profile_id, "default-profile");
        assert!(!def_prof.available);

        let easy_coder = def_prof
            .assignments
            .iter()
            .find(|a| a.role == WorkflowRole::Coder && a.difficulty == Some(Difficulty::Easy))
            .expect("easy coder assignment present");
        assert_eq!(easy_coder.model_id, "deepseek-flash");
        assert_eq!(easy_coder.model_arg, "opencode-go/deepseek-v4.1-flash");
        assert_eq!(easy_coder.tool_id, "opencode");
        assert!(!easy_coder.tool_enabled);
        assert!(!easy_coder.available);
    }

    #[test]
    fn two_bindings_collision_isolation_and_identical_not_found() {
        let (_tmp, registry, plans, reg_rev) = setup_stores();
        let wf_a = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-alpha".into(),
        )
        .expect("wf_a");
        let wf_b = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-beta".into(),
        )
        .expect("wf_b");

        let task = sample_task(
            "t1",
            WorkflowRole::Coder,
            Difficulty::Hard,
            &[],
            &["crates/threadlane-workflow/src/lib.rs"],
            "Implement isolated workflow logic",
        );

        // Invalid binding_id in constructor must be rejected.
        for bad_binding in ["", " bad ", "../traversal", "-leading-dash"] {
            let bad_err = ScopedWorkflow::new(
                Arc::clone(&registry),
                Arc::clone(&plans),
                bad_binding.to_string(),
            )
            .expect_err("invalid binding_id must fail");
            assert_eq!(bad_err.code(), "invalid_input");
        }

        // 1. Both bindings pass `plan_id: None` with the exact same `command_key`.
        // Deterministic IDs must not collide across bindings.
        let sum_a = wf_a
            .save_draft(SavePlanDraft {
                command_key: "same-key".to_string(),
                plan_id: None,
                expected_revision: 0,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: "Alpha plan".to_string(),
                tasks: vec![task.clone()],
            })
            .expect("wf_a save_draft");
        let sum_b = wf_b
            .save_draft(SavePlanDraft {
                command_key: "same-key".to_string(),
                plan_id: None,
                expected_revision: 0,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: "Beta plan".to_string(),
                tasks: vec![task.clone()],
            })
            .expect("wf_b save_draft");
        assert_ne!(sum_a.plan_id, sum_b.plan_id);
        assert_eq!(sum_a.binding_id, "binding-alpha");
        assert_eq!(sum_b.binding_id, "binding-beta");

        // 2. Explicit `plan_id` created by `binding-alpha`.
        let explicit_a = wf_a
            .save_draft(SavePlanDraft {
                command_key: "create-shared-id".to_string(),
                plan_id: Some("plan-owned-by-alpha".to_string()),
                expected_revision: 0,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: "Alpha owned plan".to_string(),
                tasks: vec![task.clone()],
            })
            .expect("wf_a create explicit plan");
        assert_eq!(explicit_a.revision, 1);

        // Baseline `not_found` error when `binding-beta` accesses a completely nonexistent plan.
        let missing_get_err = wf_b
            .get_plan("plan-does-not-exist", None, None, None)
            .expect_err("missing get_plan");
        assert_eq!(missing_get_err.code(), "not_found");

        let missing_save_nonzero_err = wf_b
            .save_draft(SavePlanDraft {
                command_key: "probe-missing".to_string(),
                plan_id: Some("plan-does-not-exist".to_string()),
                expected_revision: 1,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: "Probe".to_string(),
                tasks: vec![task.clone()],
            })
            .expect_err("missing save_draft with expected_revision 1");
        assert_eq!(missing_save_nonzero_err, missing_get_err);

        // 3. `binding-beta` attempts `get_plan`, `save_draft` (expected 0 and expected 1), and `publish`
        // on `plan-owned-by-alpha`. All must return the exact same `missing_get_err` (`not_found`)
        // without leaking `"binding-alpha"` or revision counters.
        let cross_get_latest = wf_b
            .get_plan("plan-owned-by-alpha", None, None, None)
            .expect_err("cross-binding get_plan latest");
        let cross_get_rev1 = wf_b
            .get_plan("plan-owned-by-alpha", Some(1), None, None)
            .expect_err("cross-binding get_plan rev 1");
        let cross_save_rev0 = wf_b
            .save_draft(SavePlanDraft {
                command_key: "collide-0".to_string(),
                plan_id: Some("plan-owned-by-alpha".to_string()),
                expected_revision: 0,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: "Attempt hijack at rev 0".to_string(),
                tasks: vec![task.clone()],
            })
            .expect_err("cross-binding save_draft expected 0");
        let cross_save_rev1 = wf_b
            .save_draft(SavePlanDraft {
                command_key: "collide-1".to_string(),
                plan_id: Some("plan-owned-by-alpha".to_string()),
                expected_revision: 1,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: "Attempt hijack at rev 1".to_string(),
                tasks: vec![task],
            })
            .expect_err("cross-binding save_draft expected 1");
        let cross_publish = wf_b
            .publish("plan-owned-by-alpha", 1)
            .expect_err("cross-binding publish");

        for err in [
            &cross_get_latest,
            &cross_get_rev1,
            &cross_save_rev0,
            &cross_save_rev1,
            &cross_publish,
        ] {
            assert_eq!(*err, missing_get_err);
            assert_eq!(err.code(), "not_found");
            assert!(!err.to_string().contains("binding-alpha"));
            assert!(!err.to_string().contains("plan-owned-by-alpha"));
        }

        // 4. `list_plans` on each binding only lists that binding's own plans.
        let list_a = wf_a.list_plans(None, None).expect("list_a");
        assert_eq!(list_a.plans.len(), 2);
        assert!(list_a.plans.iter().all(|p| p.binding_id == "binding-alpha"));

        let list_b = wf_b.list_plans(None, None).expect("list_b");
        assert_eq!(list_b.plans.len(), 1);
        assert_eq!(list_b.plans[0].plan_id, sum_b.plan_id);
        assert_eq!(list_b.plans[0].binding_id, "binding-beta");
    }

    #[test]
    fn stale_cas_idempotent_retry_and_reopen_persistence() {
        let (tmp, registry, plans, reg_rev) = setup_stores();
        let reg_dir = registry.config_dir().to_path_buf();
        let plans_dir = plans.dir().to_path_buf();

        let wf = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-1".into(),
        )
        .expect("wf");

        let t1 = sample_task(
            "t1",
            WorkflowRole::Coder,
            Difficulty::Easy,
            &[],
            &["crates/threadlane-workflow/src/service.rs"],
            "Initial easy coding slice",
        );

        let draft_req = SavePlanDraft {
            command_key: "cmd-draft-v1".to_string(),
            plan_id: None,
            expected_revision: 0,
            profile_id: "default-profile".to_string(),
            profile_revision: reg_rev,
            summary: "Draft v1 summary".to_string(),
            tasks: vec![t1.clone()],
        };

        let created = wf.save_draft(draft_req.clone()).expect("create draft v1");
        assert_eq!(created.revision, 1);
        assert_eq!(created.status, PlanStatus::Draft);
        assert!(created.plan_digest.is_some());

        // Exact retry with `expected_revision: 0` returns existing rev 1 without creating rev 2.
        let retry_same_expected = wf
            .save_draft(draft_req.clone())
            .expect("idempotent retry expected 0");
        assert_eq!(retry_same_expected, created);
        assert_eq!(
            plans.plan_revisions(&created.plan_id).expect("revs").len(),
            1
        );

        // Retry with `expected_revision: 1` (the created revision) and exact same payload also returns rev 1.
        let mut retry_new_rev_req = draft_req.clone();
        retry_new_rev_req.expected_revision = 1;
        let retry_new_expected = wf
            .save_draft(retry_new_rev_req)
            .expect("idempotent retry expected 1");
        assert_eq!(retry_new_expected, created);
        assert_eq!(
            plans.plan_revisions(&created.plan_id).expect("revs").len(),
            1
        );

        // Reusing the same `command_key` with a different summary must fail with `idempotency_conflict`.
        let mut conflicting_key_req = draft_req.clone();
        conflicting_key_req.summary = "Mutated summary with same command_key".to_string();
        let conflict_err = wf
            .save_draft(conflicting_key_req)
            .expect_err("same command_key with different payload must fail");
        assert_eq!(conflict_err.code(), "idempotency_conflict");

        // Using a different `command_key` at stale `expected_revision: 0` on the same plan_id fails with `stale_revision`.
        let stale_err = wf
            .save_draft(SavePlanDraft {
                command_key: "cmd-different-key".to_string(),
                plan_id: Some(created.plan_id.clone()),
                expected_revision: 0,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: "Different summary at stale rev 0".to_string(),
                tasks: vec![t1.clone()],
            })
            .expect_err("stale CAS expected 0");
        assert_eq!(stale_err.code(), "stale_revision");

        // Reopen stores and workflow from disk; verify idempotent retry still works without creating rev 2.
        drop(wf);
        drop(plans);
        drop(registry);

        let reopened_reg = Arc::new(RegistryStore::open(&reg_dir).expect("reopen reg"));
        let reopened_plans = Arc::new(PlanStore::open(&plans_dir).expect("reopen plans"));
        let reopened_wf = ScopedWorkflow::new(
            Arc::clone(&reopened_reg),
            Arc::clone(&reopened_plans),
            "binding-1".into(),
        )
        .expect("reopened wf");

        let retry_after_reopen = reopened_wf
            .save_draft(draft_req.clone())
            .expect("idempotent retry after reopen");
        assert_eq!(retry_after_reopen, created);
        assert_eq!(
            reopened_plans
                .plan_revisions(&created.plan_id)
                .expect("revs")
                .len(),
            1
        );

        // Reusing the deterministic `command_key` with `plan_id: None` and a different payload after reopen
        // must also fail with `idempotency_conflict`.
        let mut post_reopen_conflict = draft_req;
        post_reopen_conflict.summary = "Different summary after reopen".to_string();
        let post_reopen_err = reopened_wf
            .save_draft(post_reopen_conflict)
            .expect_err("conflict after reopen");
        assert_eq!(post_reopen_err.code(), "idempotency_conflict");

        drop(tmp);
    }

    #[test]
    fn reopen_changed_payload_target_revision_reuse_and_intervening_edit_retry() {
        let (_tmp, registry, plans, reg_rev) = setup_stores();
        let reg_dir = registry.config_dir().to_path_buf();
        let plans_dir = plans.dir().to_path_buf();

        let t1 = sample_task(
            "t1",
            WorkflowRole::Coder,
            Difficulty::Easy,
            &[],
            &["crates/threadlane-workflow/src/service.rs"],
            "Original v1 instruction",
        );
        let t2 = sample_task(
            "t1",
            WorkflowRole::Coder,
            Difficulty::Hard,
            &[],
            &["crates/threadlane-workflow/src/service.rs"],
            "Modified task instruction",
        );

        let initial_req = SavePlanDraft {
            command_key: "cmd-durable-v1".to_string(),
            plan_id: Some("plan-durable".to_string()),
            expected_revision: 0,
            profile_id: "default-profile".to_string(),
            profile_revision: reg_rev,
            summary: "Original v1 summary".to_string(),
            tasks: vec![t1.clone()],
        };

        let wf1 = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-1".into(),
        )
        .expect("wf1");
        let rev1_summary = wf1.save_draft(initial_req.clone()).expect("save v1");
        assert_eq!(rev1_summary.revision, 1);
        assert_eq!(rev1_summary.summary, "Original v1 summary");
        drop(wf1);

        // Reopen a brand-new `ScopedWorkflow` instance (simulating per-call native MCP instantiation).
        // Reusing `cmd-durable-v1` with `expected_revision = 1` (the current revision!) and a CHANGED
        // summary, changed tasks, changed profile, or changed target `plan_id` MUST fail with
        // `idempotency_conflict` rather than being accepted as revision 2!
        let wf2 = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-1".into(),
        )
        .expect("wf2");

        let changed_summary_at_current_rev = SavePlanDraft {
            expected_revision: 1,
            summary: "Sneaky v2 summary reusing cmd-durable-v1".to_string(),
            ..initial_req.clone()
        };
        assert_eq!(
            wf2.save_draft(changed_summary_at_current_rev)
                .expect_err("changed summary at current expected_revision must conflict")
                .code(),
            "idempotency_conflict"
        );

        let changed_tasks_at_current_rev = SavePlanDraft {
            expected_revision: 1,
            tasks: vec![t2.clone()],
            ..initial_req.clone()
        };
        assert_eq!(
            wf2.save_draft(changed_tasks_at_current_rev)
                .expect_err("changed tasks at current expected_revision must conflict")
                .code(),
            "idempotency_conflict"
        );

        let changed_profile_rev_at_current_rev = SavePlanDraft {
            expected_revision: 1,
            profile_revision: reg_rev + 1,
            ..initial_req.clone()
        };
        assert_eq!(
            wf2.save_draft(changed_profile_rev_at_current_rev)
                .expect_err("changed profile_revision at current expected_revision must conflict")
                .code(),
            "idempotency_conflict"
        );

        let changed_target_plan = SavePlanDraft {
            plan_id: Some("plan-other-target".to_string()),
            expected_revision: 0,
            ..initial_req.clone()
        };
        assert_eq!(
            wf2.save_draft(changed_target_plan)
                .expect_err("changed target plan_id with same command_key must conflict")
                .code(),
            "idempotency_conflict"
        );

        // Verify `PlanStore` still only has revision 1.
        assert_eq!(
            plans
                .plan_revisions("plan-durable")
                .expect("revisions")
                .len(),
            1
        );

        // Now save a legitimate intervening revision 2 using a new command key `cmd-durable-v2`.
        let rev2_summary = wf2
            .save_draft(SavePlanDraft {
                command_key: "cmd-durable-v2".to_string(),
                plan_id: Some("plan-durable".to_string()),
                expected_revision: 1,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: "Intervening v2 summary".to_string(),
                tasks: vec![t2],
            })
            .expect("save legitimate v2");
        assert_eq!(rev2_summary.revision, 2);
        assert_eq!(rev2_summary.summary, "Intervening v2 summary");
        drop(wf2);

        // Reopen stores and create `wf3` from disk after intervening revision 2.
        let reopened_reg = Arc::new(RegistryStore::open(&reg_dir).expect("reopen reg"));
        let reopened_plans = Arc::new(PlanStore::open(&plans_dir).expect("reopen plans"));
        let wf3 = ScopedWorkflow::new(
            Arc::clone(&reopened_reg),
            Arc::clone(&reopened_plans),
            "binding-1".into(),
        )
        .expect("wf3");

        // Reusing `cmd-durable-v1` at `expected_revision = 2` (even with `cmd-durable-v1`'s original payload
        // or with a changed payload) after intervening revision 2 MUST fail with `idempotency_conflict`.
        let reuse_at_rev2 = SavePlanDraft {
            expected_revision: 2,
            ..initial_req.clone()
        };
        assert_eq!(
            wf3.save_draft(reuse_at_rev2)
                .expect_err("reusing cmd-durable-v1 at expected_revision 2 must conflict")
                .code(),
            "idempotency_conflict"
        );

        let changed_after_intervening = SavePlanDraft {
            expected_revision: 0,
            summary: "Changed summary after intervening v2".to_string(),
            ..initial_req.clone()
        };
        assert_eq!(
            wf3.save_draft(changed_after_intervening)
                .expect_err("changed payload reuse after intervening v2 must conflict")
                .code(),
            "idempotency_conflict"
        );

        // Exact retry of `cmd-durable-v1` (with `expected_revision = 0` or `expected_revision = 1`)
        // across reopen after intervening revision 2 MUST return the ORIGINAL saved revision 1
        // `PlanSummary` (`revision == 1`, `summary == "Original v1 summary"`), NOT revision 2,
        // and must NOT create revision 3!
        let exact_retry_exp0 = wf3
            .save_draft(initial_req.clone())
            .expect("exact retry of cmd-durable-v1 (expected 0) after intervening v2");
        assert_eq!(exact_retry_exp0, rev1_summary);
        assert_eq!(exact_retry_exp0.revision, 1);
        assert_eq!(exact_retry_exp0.summary, "Original v1 summary");

        let exact_retry_exp1 = wf3
            .save_draft(SavePlanDraft {
                expected_revision: 1,
                ..initial_req
            })
            .expect("exact retry of cmd-durable-v1 (expected 1) after intervening v2");
        assert_eq!(exact_retry_exp1, rev1_summary);

        assert_eq!(
            reopened_plans
                .plan_revisions("plan-durable")
                .expect("revisions")
                .len(),
            2
        );
    }

    #[test]
    fn independent_instances_concurrent_duplicate_and_conflict() {
        let (_tmp, registry, plans, reg_rev) = setup_stores();

        let task = sample_task(
            "t1",
            WorkflowRole::Coder,
            Difficulty::Easy,
            &[],
            &["crates/threadlane-workflow/src/lib.rs"],
            "Concurrent task instruction",
        );

        // 1. Two independent `ScopedWorkflow` instances concurrently submit the exact same `SavePlanDraft`.
        let wf_same_1 = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-1".into(),
        )
        .expect("wf_same_1");
        let wf_same_2 = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-1".into(),
        )
        .expect("wf_same_2");

        let same_req = SavePlanDraft {
            command_key: "cmd-concurrent-same".to_string(),
            plan_id: None,
            expected_revision: 0,
            profile_id: "default-profile".to_string(),
            profile_revision: reg_rev,
            summary: "Identical concurrent draft".to_string(),
            tasks: vec![task.clone()],
        };

        let barrier_same = Arc::new(Barrier::new(2));
        let b1 = Arc::clone(&barrier_same);
        let req1 = same_req.clone();
        let h1 = thread::spawn(move || {
            b1.wait();
            wf_same_1.save_draft(req1)
        });

        let b2 = Arc::clone(&barrier_same);
        let req2 = same_req;
        let h2 = thread::spawn(move || {
            b2.wait();
            wf_same_2.save_draft(req2)
        });

        let res1 = h1.join().expect("join h1").expect("h1 save_draft");
        let res2 = h2.join().expect("join h2").expect("h2 save_draft");
        assert_eq!(res1, res2);
        assert_eq!(res1.revision, 1);
        assert_eq!(
            plans
                .plan_revisions(&res1.plan_id)
                .expect("revisions")
                .len(),
            1
        );

        // 2. Two independent `ScopedWorkflow` instances concurrently submit the same `command_key`
        // with different payloads at `expected_revision = 1`. Exactly one must succeed (creating rev 2)
        // and the other must fail with `idempotency_conflict`.
        let wf_diff_1 = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-1".into(),
        )
        .expect("wf_diff_1");
        let wf_diff_2 = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-1".into(),
        )
        .expect("wf_diff_2");

        let diff_req_a = SavePlanDraft {
            command_key: "cmd-concurrent-conflict".to_string(),
            plan_id: Some(res1.plan_id.clone()),
            expected_revision: 1,
            profile_id: "default-profile".to_string(),
            profile_revision: reg_rev,
            summary: "Concurrent candidate A".to_string(),
            tasks: vec![task.clone()],
        };
        let diff_req_b = SavePlanDraft {
            summary: "Concurrent candidate B".to_string(),
            ..diff_req_a.clone()
        };

        let barrier_diff = Arc::new(Barrier::new(2));
        let bd1 = Arc::clone(&barrier_diff);
        let hd1 = thread::spawn(move || {
            bd1.wait();
            wf_diff_1.save_draft(diff_req_a)
        });
        let bd2 = Arc::clone(&barrier_diff);
        let hd2 = thread::spawn(move || {
            bd2.wait();
            wf_diff_2.save_draft(diff_req_b)
        });

        let out1 = hd1.join().expect("join hd1");
        let out2 = hd2.join().expect("join hd2");

        match (out1, out2) {
            (Ok(ok_sum), Err(err)) | (Err(err), Ok(ok_sum)) => {
                assert_eq!(ok_sum.revision, 2);
                assert_eq!(err.code(), "idempotency_conflict");
            }
            other => panic!("expected one Ok and one idempotency_conflict Err, got: {other:?}"),
        }

        assert_eq!(
            plans
                .plan_revisions(&res1.plan_id)
                .expect("revisions")
                .len(),
            2
        );
    }

    #[test]
    fn crash_pending_journal_recovery_and_competing_revision_protection() {
        let (_tmp, registry, plans, reg_rev) = setup_stores();
        let wf = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-1".into(),
        )
        .expect("wf");

        let task_a = sample_task(
            "t1",
            WorkflowRole::Coder,
            Difficulty::Easy,
            &[],
            &["crates/threadlane-workflow/src/service.rs"],
            "Task A instruction",
        );
        let task_b = sample_task(
            "t1",
            WorkflowRole::Coder,
            Difficulty::Hard,
            &[],
            &["crates/threadlane-workflow/src/service.rs"],
            "Competing Task B instruction",
        );

        // --- Case 1: Crash AFTER persisting `Pending` intent, BEFORE `PlanStore::change` wrote rev 1 ---
        let plan_before = "plan-crash-before-store";
        let summary_before = "Pending before PlanStore mutation";
        let tasks_before = vec![task_a.clone()];
        let tasks_digest_before = compute_tasks_digest(&tasks_before).expect("tasks digest");
        let payload_digest_before = compute_payload_digest(
            plan_before,
            "default-profile",
            reg_rev,
            summary_before,
            &tasks_before,
        )
        .expect("payload digest");

        let mut journal = PersistedCommandJournal::new("binding-1");
        journal.entries.push(CommandJournalEntry {
            command_key: "cmd-crash-before".to_string(),
            explicit_plan_id: Some(plan_before.to_string()),
            resolved_plan_id: plan_before.to_string(),
            expected_revision: 0,
            target_revision: 1,
            profile_id: "default-profile".to_string(),
            profile_revision: reg_rev,
            summary: summary_before.to_string(),
            tasks_digest: tasks_digest_before,
            payload_digest: payload_digest_before,
            plan_digest: None,
            state: CommandJournalState::Pending,
        });
        with_journal_lock(
            plans.dir(),
            &plans.dir().join("test-seed.lock"),
            |clean_dir| write_journal_locked(clean_dir, wf.command_journal_path(), &journal),
        )
        .expect("seed pending journal");
        drop(wf);

        // Reopen workflow and verify changed payload reuse against `Pending` entry conflicts.
        let wf_reopened = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-1".into(),
        )
        .expect("wf_reopened");
        assert_eq!(
            wf_reopened
                .save_draft(SavePlanDraft {
                    command_key: "cmd-crash-before".to_string(),
                    plan_id: Some(plan_before.to_string()),
                    expected_revision: 0,
                    profile_id: "default-profile".to_string(),
                    profile_revision: reg_rev,
                    summary: "Mutated summary".to_string(),
                    tasks: tasks_before.clone(),
                })
                .expect_err("changed payload on Pending entry must conflict")
                .code(),
            "idempotency_conflict"
        );
        assert!(plans.plan_revisions(plan_before).expect("revs").is_empty());

        // Exact retry of `cmd-crash-before` completes the mutation in `PlanStore` and marks `Completed`.
        let recovered_before = wf_reopened
            .save_draft(SavePlanDraft {
                command_key: "cmd-crash-before".to_string(),
                plan_id: Some(plan_before.to_string()),
                expected_revision: 0,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: summary_before.to_string(),
                tasks: tasks_before.clone(),
            })
            .expect("recover pending command before store");
        assert_eq!(recovered_before.revision, 1);
        assert_eq!(plans.plan_revisions(plan_before).expect("revs").len(), 1);

        // --- Case 2: Crash AFTER `PlanStore::change` wrote rev 1, BEFORE journal updated to `Completed` ---
        let plan_after = "plan-crash-after-store";
        let summary_after = "Pending after PlanStore mutation";
        plans
            .change(
                0,
                PlanChange::SaveDraft {
                    plan_id: plan_after.to_string(),
                    binding_id: "binding-1".to_string(),
                    profile_id: "default-profile".to_string(),
                    profile_revision: reg_rev,
                    summary: summary_after.to_string(),
                    tasks: tasks_before.clone(),
                },
            )
            .expect("seed PlanStore rev 1");

        let mut journal_after =
            read_journal_locked(wf_reopened.command_journal_path(), "binding-1")
                .expect("read journal");
        journal_after.entries.push(CommandJournalEntry {
            command_key: "cmd-crash-after".to_string(),
            explicit_plan_id: Some(plan_after.to_string()),
            resolved_plan_id: plan_after.to_string(),
            expected_revision: 0,
            target_revision: 1,
            profile_id: "default-profile".to_string(),
            profile_revision: reg_rev,
            summary: summary_after.to_string(),
            tasks_digest: compute_tasks_digest(&tasks_before).expect("tasks digest"),
            payload_digest: compute_payload_digest(
                plan_after,
                "default-profile",
                reg_rev,
                summary_after,
                &tasks_before,
            )
            .expect("payload digest"),
            plan_digest: None,
            state: CommandJournalState::Pending,
        });
        with_journal_lock(
            plans.dir(),
            &plans.dir().join("test-seed.lock"),
            |clean_dir| {
                write_journal_locked(
                    clean_dir,
                    wf_reopened.command_journal_path(),
                    &journal_after,
                )
            },
        )
        .expect("write pending-after journal");

        let wf_reopened_2 = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-1".into(),
        )
        .expect("wf_reopened_2");
        let reconciled_after = wf_reopened_2
            .save_draft(SavePlanDraft {
                command_key: "cmd-crash-after".to_string(),
                plan_id: Some(plan_after.to_string()),
                expected_revision: 0,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: summary_after.to_string(),
                tasks: tasks_before.clone(),
            })
            .expect("reconcile pending command after store");
        assert_eq!(reconciled_after.revision, 1);
        assert_eq!(reconciled_after.summary, summary_after);
        assert_eq!(plans.plan_revisions(plan_after).expect("revs").len(), 1);

        // --- Case 3: Crash after `Pending` persisted, and a competing revision won at `target_revision` ---
        let plan_competing = "plan-crash-competing";
        let loser_summary = "Loser pending summary";
        let winner_summary = "Winner competing summary";

        // Winner saves rev 1 in `PlanStore`.
        wf_reopened_2
            .save_draft(SavePlanDraft {
                command_key: "cmd-competing-winner".to_string(),
                plan_id: Some(plan_competing.to_string()),
                expected_revision: 0,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: winner_summary.to_string(),
                tasks: vec![task_b],
            })
            .expect("winner saves rev 1");

        // Seed a `Pending` entry for `cmd-competing-loser` that also targeted rev 1 on `plan-crash-competing`.
        let mut journal_comp =
            read_journal_locked(wf_reopened_2.command_journal_path(), "binding-1")
                .expect("read journal");
        journal_comp.entries.push(CommandJournalEntry {
            command_key: "cmd-competing-loser".to_string(),
            explicit_plan_id: Some(plan_competing.to_string()),
            resolved_plan_id: plan_competing.to_string(),
            expected_revision: 0,
            target_revision: 1,
            profile_id: "default-profile".to_string(),
            profile_revision: reg_rev,
            summary: loser_summary.to_string(),
            tasks_digest: compute_tasks_digest(&tasks_before).expect("tasks digest"),
            payload_digest: compute_payload_digest(
                plan_competing,
                "default-profile",
                reg_rev,
                loser_summary,
                &tasks_before,
            )
            .expect("payload digest"),
            plan_digest: None,
            state: CommandJournalState::Pending,
        });
        with_journal_lock(
            plans.dir(),
            &plans.dir().join("test-seed.lock"),
            |clean_dir| {
                write_journal_locked(
                    clean_dir,
                    wf_reopened_2.command_journal_path(),
                    &journal_comp,
                )
            },
        )
        .expect("write competing pending journal");

        let wf_reopened_3 = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-1".into(),
        )
        .expect("wf_reopened_3");

        // Retrying `cmd-competing-loser` must NOT overwrite rev 1 or fabricate success; it fails with `stale_revision`.
        let loser_err = wf_reopened_3
            .save_draft(SavePlanDraft {
                command_key: "cmd-competing-loser".to_string(),
                plan_id: Some(plan_competing.to_string()),
                expected_revision: 0,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: loser_summary.to_string(),
                tasks: tasks_before.clone(),
            })
            .expect_err("loser pending retry must fail against competing revision");
        assert_eq!(loser_err.code(), "stale_revision");

        // Verify winner's rev 1 in `PlanStore` was NOT overwritten and no rev 2 was created.
        let comp_revs = plans.plan_revisions(plan_competing).expect("comp_revs");
        assert_eq!(comp_revs.len(), 1);
        assert_eq!(comp_revs[0].summary, winner_summary);

        // And `cmd-competing-loser` remains reserved in the journal, so reusing it with a changed payload
        // or at `expected_revision = 1` with a new summary fails with `idempotency_conflict`.
        let reuse_loser_err = wf_reopened_3
            .save_draft(SavePlanDraft {
                command_key: "cmd-competing-loser".to_string(),
                plan_id: Some(plan_competing.to_string()),
                expected_revision: 1,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: "Reuse failed loser key for rev 2".to_string(),
                tasks: tasks_before,
            })
            .expect_err("reusing reserved failed key with changed payload must conflict");
        assert_eq!(reuse_loser_err.code(), "idempotency_conflict");
    }

    #[test]
    fn dag_write_scope_and_registry_version_validated_before_mutation() {
        let (_tmp, registry, plans, reg_rev) = setup_stores();
        let wf = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-1".into(),
        )
        .expect("wf");

        // 1. Duplicate task IDs.
        let dup_err = wf
            .save_draft(SavePlanDraft {
                command_key: "k-dup".to_string(),
                plan_id: None,
                expected_revision: 0,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: "Dup tasks".to_string(),
                tasks: vec![
                    sample_task(
                        "dup",
                        WorkflowRole::Coder,
                        Difficulty::Easy,
                        &[],
                        &["src/a.rs"],
                        "A",
                    ),
                    sample_task(
                        "dup",
                        WorkflowRole::Coder,
                        Difficulty::Hard,
                        &[],
                        &["src/b.rs"],
                        "B",
                    ),
                ],
            })
            .expect_err("duplicate task id");
        assert_eq!(dup_err.code(), "duplicate_task_id");

        // 2. Dependency cycle.
        let cycle_err = wf
            .save_draft(SavePlanDraft {
                command_key: "k-cycle".to_string(),
                plan_id: None,
                expected_revision: 0,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: "Cycle tasks".to_string(),
                tasks: vec![
                    sample_task(
                        "a",
                        WorkflowRole::Coder,
                        Difficulty::Easy,
                        &["b"],
                        &["src/a.rs"],
                        "A",
                    ),
                    sample_task(
                        "b",
                        WorkflowRole::Coder,
                        Difficulty::Hard,
                        &["a"],
                        &["src/b.rs"],
                        "B",
                    ),
                ],
            })
            .expect_err("cycle");
        assert_eq!(cycle_err.code(), "dependency_cycle");

        // 3. Protected write scope (`AGENTS.md`).
        let scope_err = wf
            .save_draft(SavePlanDraft {
                command_key: "k-scope".to_string(),
                plan_id: None,
                expected_revision: 0,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: "Bad write scope".to_string(),
                tasks: vec![sample_task(
                    "t1",
                    WorkflowRole::Coder,
                    Difficulty::Easy,
                    &[],
                    &["AGENTS.md"],
                    "Do not touch AGENTS.md",
                )],
            })
            .expect_err("protected write path");
        assert_eq!(scope_err.code(), "invalid_write_scope");

        // 4. Stale / mismatched `profile_revision`.
        let rev_mismatch_err = wf
            .save_draft(SavePlanDraft {
                command_key: "k-rev".to_string(),
                plan_id: None,
                expected_revision: 0,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev + 99,
                summary: "Wrong registry revision".to_string(),
                tasks: vec![sample_task(
                    "t1",
                    WorkflowRole::Coder,
                    Difficulty::Easy,
                    &[],
                    &["src/lib.rs"],
                    "Valid task",
                )],
            })
            .expect_err("profile_revision mismatch");
        assert_eq!(rev_mismatch_err.code(), "profile_revision_mismatch");
        assert!(rev_mismatch_err.is_stale());

        // 5. Unassigned role (`Architect` is not assigned in `default-profile`).
        let unassigned_err = wf
            .save_draft(SavePlanDraft {
                command_key: "k-unassigned".to_string(),
                plan_id: None,
                expected_revision: 0,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: "Unassigned architect".to_string(),
                tasks: vec![sample_task(
                    "t1",
                    WorkflowRole::Architect,
                    Difficulty::Hard,
                    &[],
                    &["src/lib.rs"],
                    "Design architecture",
                )],
            })
            .expect_err("unassigned role");
        assert_eq!(unassigned_err.code(), "registry_resolution_failed");

        // PlanStore must still be completely empty after all rejected `save_draft` calls.
        assert!(plans.snapshot().expect("snapshot").is_empty());

        // 6. Save a valid draft at `reg_rev`, then advance the registry and verify `publish` rejects
        // the now-stale `profile_revision` without mutating the draft to `Published`.
        let valid_draft = wf
            .save_draft(SavePlanDraft {
                command_key: "k-valid".to_string(),
                plan_id: Some("plan-reg-pin".to_string()),
                expected_revision: 0,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: "Pinned to reg_rev".to_string(),
                tasks: vec![sample_task(
                    "t1",
                    WorkflowRole::Coder,
                    Difficulty::Hard,
                    &[],
                    &["src/lib.rs"],
                    "Hard coding task",
                )],
            })
            .expect("save valid draft");
        assert!(valid_draft.plan_digest.is_some());

        // Advance registry by clearing default profile.
        registry
            .apply(
                reg_rev,
                RegistryChange::SetDefaultProfile { profile_id: None },
            )
            .expect("advance registry");

        let pub_err = wf
            .publish("plan-reg-pin", 1)
            .expect_err("publish with stale registry revision must fail");
        assert_eq!(pub_err.code(), "profile_revision_mismatch");
        assert_eq!(
            plans
                .latest_revision("plan-reg-pin")
                .expect("latest")
                .expect("exists")
                .status,
            PlanStatus::Draft
        );

        // `get_plan` and `list_plans` still succeed when registry advances, reporting `plan_digest: None`.
        let page = wf
            .get_plan("plan-reg-pin", None, None, None)
            .expect("get_plan after registry advance");
        assert_eq!(page.header.status, PlanStatus::Draft);
        assert_eq!(page.header.plan_digest, None);
    }

    #[test]
    fn publish_routes_receipts_no_auto_ack_or_approval_and_old_published_new_draft_coexist() {
        let (_tmp, registry, plans, reg_rev) = setup_stores();
        let wf = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-1".into(),
        )
        .expect("wf");

        let tasks_v1 = vec![
            sample_task(
                "task-follower",
                WorkflowRole::Follower,
                Difficulty::Medium,
                &[],
                &["docs/notes.md"],
                "Coordinate with user in native chat",
            ),
            sample_task(
                "task-easy",
                WorkflowRole::Coder,
                Difficulty::Easy,
                &["task-follower"],
                &["crates/threadlane-workflow/src/types.rs"],
                "Implement DTOs",
            ),
            sample_task(
                "task-hard",
                WorkflowRole::Coder,
                Difficulty::Hard,
                &["task-easy"],
                &["crates/threadlane-workflow/src/service.rs"],
                "Implement service",
            ),
            sample_task(
                "task-aux",
                WorkflowRole::Closure,
                Difficulty::Extreme,
                &["task-hard"],
                &["crates/threadlane-workflow/src/lib.rs"],
                "Aux git task",
            ),
        ];

        let draft_v1 = wf
            .save_draft(SavePlanDraft {
                command_key: "cmd-v1".to_string(),
                plan_id: Some("plan-lifecycle".to_string()),
                expected_revision: 0,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: "Version 1 summary".to_string(),
                tasks: tasks_v1.clone(),
            })
            .expect("save v1");
        assert_eq!(draft_v1.status, PlanStatus::Draft);

        let published_v1 = wf.publish("plan-lifecycle", 1).expect("publish v1");
        assert_eq!(published_v1.header.status, PlanStatus::Published);
        assert_eq!(published_v1.header.plan_digest, draft_v1.plan_digest);
        assert_eq!(
            published_v1.receipt,
            DeliveryReceipt {
                plan_id: "plan-lifecycle".to_string(),
                revision: 1,
                state: PlanStatus::Published,
            }
        );
        assert_eq!(published_v1.tasks.len(), 4);
        assert_eq!(
            published_v1.tasks[0].route,
            TaskExecutionRoute::NativeFollower
        );
        assert_eq!(published_v1.tasks[0].adapter, ToolAdapter::CodexCli);
        assert_eq!(
            published_v1.tasks[1].route,
            TaskExecutionRoute::HeadlessWorker
        );
        assert_eq!(published_v1.tasks[1].adapter, ToolAdapter::OpencodeV2);
        assert_eq!(
            published_v1.tasks[2].route,
            TaskExecutionRoute::HeadlessWorker
        );
        assert_eq!(published_v1.tasks[2].adapter, ToolAdapter::Antigravity);
        assert_eq!(published_v1.tasks[3].route, TaskExecutionRoute::Unsupported);
        assert_eq!(published_v1.tasks[3].adapter, ToolAdapter::Git);

        // Verify no CLI executable paths or raw task instructions leak in PublishedPlanView JSON.
        let pub_json = serde_json::to_string(&published_v1).expect("serialize published view");
        assert!(!pub_json.contains("/opt/secret-cli-paths"));
        assert!(!pub_json.contains("executable_path"));
        assert!(!pub_json.contains("Coordinate with user in native chat"));

        // `list_plans` and `get_plan` must NOT auto-acknowledge or advance `Published` to `Received`/`Applied`.
        let _ = wf.list_plans(None, None).expect("list_plans");
        let _ = wf
            .get_plan("plan-lifecycle", Some(1), None, None)
            .expect("get_plan");
        assert_eq!(
            plans
                .delivery_receipt("plan-lifecycle", 1)
                .expect("receipt")
                .expect("exists")
                .state,
            PlanStatus::Published
        );

        // Repeated exact publish on Published returns identical view.
        let repeat_pub = wf.publish("plan-lifecycle", 1).expect("repeat publish");
        assert_eq!(repeat_pub, published_v1);

        // Simulate runtime advancing receipt to `Received` and then `Applied`; repeated `publish(1)`
        // must return the current truthful state and never regress back to `Published`.
        plans
            .change(
                1,
                PlanChange::Receive {
                    plan_id: "plan-lifecycle".to_string(),
                    revision: 1,
                },
            )
            .expect("runtime receive");
        let view_when_received = wf
            .publish("plan-lifecycle", 1)
            .expect("publish when received");
        assert_eq!(view_when_received.header.status, PlanStatus::Received);
        assert_eq!(view_when_received.receipt.state, PlanStatus::Received);

        plans
            .change(
                1,
                PlanChange::Apply {
                    plan_id: "plan-lifecycle".to_string(),
                    revision: 1,
                },
            )
            .expect("runtime apply");
        let view_when_applied = wf
            .publish("plan-lifecycle", 1)
            .expect("publish when applied");
        assert_eq!(view_when_applied.header.status, PlanStatus::Applied);
        assert_eq!(view_when_applied.receipt.state, PlanStatus::Applied);

        // Save a new Draft v2 (`expected_revision: 1`). Old published/applied v1 and new Draft v2 must coexist!
        let tasks_v2 = vec![sample_task(
            "task-hard",
            WorkflowRole::Reviewer,
            Difficulty::Hard,
            &[],
            &["crates/threadlane-workflow/src/lib.rs"],
            "Updated review instruction for v2",
        )];
        let draft_v2 = wf
            .save_draft(SavePlanDraft {
                command_key: "cmd-v2".to_string(),
                plan_id: Some("plan-lifecycle".to_string()),
                expected_revision: 1,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: "Version 2 draft summary".to_string(),
                tasks: tasks_v2.clone(),
            })
            .expect("save draft v2");
        assert_eq!(draft_v2.revision, 2);
        assert_eq!(draft_v2.status, PlanStatus::Draft);

        // Revision 1 is still `Applied` with its original 4 tasks, while revision 2 is `Draft` with 1 task.
        let rev1_page = wf
            .get_plan("plan-lifecycle", Some(1), None, None)
            .expect("get rev 1 while v2 is draft");
        assert_eq!(rev1_page.header.revision, 1);
        assert_eq!(rev1_page.header.status, PlanStatus::Applied);
        assert_eq!(rev1_page.header.task_count, 4);
        assert_eq!(rev1_page.tasks, tasks_v1);

        let rev2_page = wf
            .get_plan("plan-lifecycle", None, None, None)
            .expect("get latest (rev 2)");
        assert_eq!(rev2_page.header.revision, 2);
        assert_eq!(rev2_page.header.status, PlanStatus::Draft);
        assert_eq!(rev2_page.header.task_count, 1);
        assert_eq!(rev2_page.tasks, tasks_v2);

        // Publishing revision 1 when revision 2 is current must fail with `stale_revision`.
        let stale_pub_err = wf
            .publish("plan-lifecycle", 1)
            .expect_err("publish old rev 1 when rev 2 exists");
        assert_eq!(stale_pub_err.code(), "stale_revision");

        // Publishing revision 2 transitions revision 2 to `Published` and marks revision 1 `Superseded`.
        let pub_v2 = wf.publish("plan-lifecycle", 2).expect("publish rev 2");
        assert_eq!(pub_v2.header.revision, 2);
        assert_eq!(pub_v2.header.status, PlanStatus::Published);
        assert_eq!(pub_v2.receipt.state, PlanStatus::Published);

        let rev1_after_v2_pub = wf
            .get_plan("plan-lifecycle", Some(1), None, None)
            .expect("get superseded rev 1");
        assert_eq!(rev1_after_v2_pub.header.status, PlanStatus::Superseded);
        assert_eq!(rev1_after_v2_pub.header.plan_digest, None);
        assert_eq!(rev1_after_v2_pub.tasks, tasks_v1);

        let superseded_pub_err = wf
            .publish("plan-lifecycle", 1)
            .expect_err("publish superseded rev 1");
        assert_eq!(superseded_pub_err.code(), "stale_revision");
    }

    #[test]
    fn opaque_cursors_validate_binding_filter_resource_and_staleness() {
        let (_tmp, registry, plans, reg_rev) = setup_stores();
        let wf_a = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-a".into(),
        )
        .expect("wf_a");
        let wf_b = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-b".into(),
        )
        .expect("wf_b");

        let tasks = vec![
            sample_task(
                "t1",
                WorkflowRole::Coder,
                Difficulty::Easy,
                &[],
                &["src/a.rs"],
                "Task 1",
            ),
            sample_task(
                "t2",
                WorkflowRole::Coder,
                Difficulty::Hard,
                &["t1"],
                &["src/b.rs"],
                "Task 2",
            ),
            sample_task(
                "t3",
                WorkflowRole::Reviewer,
                Difficulty::Hard,
                &["t2"],
                &["src/c.rs"],
                "Task 3",
            ),
        ];

        for idx in 1..=3 {
            wf_a.save_draft(SavePlanDraft {
                command_key: format!("cmd-a-{idx}"),
                plan_id: Some(format!("plan-a-{idx}")),
                expected_revision: 0,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: format!("Plan A {idx}"),
                tasks: tasks.clone(),
            })
            .expect("save plan a");
        }

        let list_page1 = wf_a
            .list_plans(None, Some(2))
            .expect("list_plans page 1 (limit 2)");
        assert_eq!(list_page1.plans.len(), 2);
        let list_cursor = list_page1.next_cursor.clone().expect("list next_cursor");

        let task_page1 = wf_a
            .get_plan("plan-a-1", Some(1), None, Some(1))
            .expect("get_plan page 1 (limit 1)");
        assert_eq!(task_page1.tasks.len(), 1);
        let task_cursor = task_page1
            .next_task_cursor
            .clone()
            .expect("task next_cursor");

        // 1. Malformed and overlong cursors are rejected with `invalid_cursor`.
        for bad_cursor in [
            String::new(),
            "   ".to_string(),
            "not-a-valid-cursor".to_string(),
            "a".repeat(MAX_CURSOR_LEN + 1),
        ] {
            assert_eq!(
                wf_a.list_plans(Some(bad_cursor.clone()), None)
                    .unwrap_err()
                    .code(),
                "invalid_cursor"
            );
            assert_eq!(
                wf_a.get_plan("plan-a-1", Some(1), Some(bad_cursor), None)
                    .unwrap_err()
                    .code(),
                "invalid_cursor"
            );
        }

        // 2. Cross-resource cursor misuse is rejected with `invalid_cursor`.
        assert_eq!(
            wf_a.catalog(Some(list_cursor.clone()), None)
                .unwrap_err()
                .code(),
            "invalid_cursor"
        );
        assert_eq!(
            wf_a.get_plan("plan-a-1", Some(1), Some(list_cursor.clone()), None)
                .unwrap_err()
                .code(),
            "invalid_cursor"
        );
        assert_eq!(
            wf_a.list_plans(Some(task_cursor.clone()), None)
                .unwrap_err()
                .code(),
            "invalid_cursor"
        );

        // 3. Cross-binding cursor misuse is rejected (`invalid_cursor`).
        assert_eq!(
            wf_b.list_plans(Some(list_cursor.clone()), None)
                .unwrap_err()
                .code(),
            "invalid_cursor"
        );
        assert_eq!(
            wf_b.catalog(Some(list_cursor.clone()), None)
                .unwrap_err()
                .code(),
            "invalid_cursor"
        );

        // 4. Wrong filter on `get_plan` (different `plan_id` or different `revision` filter) is rejected.
        assert_eq!(
            wf_a.get_plan("plan-a-2", Some(1), Some(task_cursor.clone()), Some(1))
                .unwrap_err()
                .code(),
            "invalid_cursor"
        );
        assert_eq!(
            wf_a.get_plan("plan-a-1", None, Some(task_cursor.clone()), Some(1))
                .unwrap_err()
                .code(),
            "invalid_cursor"
        );

        // 5. Creating a plan in `binding-b` does NOT invalidate `binding-a`'s `list_plans` cursor.
        wf_b.save_draft(SavePlanDraft {
            command_key: "cmd-b-1".to_string(),
            plan_id: Some("plan-b-1".to_string()),
            expected_revision: 0,
            profile_id: "default-profile".to_string(),
            profile_revision: reg_rev,
            summary: "Plan B 1".to_string(),
            tasks: tasks.clone(),
        })
        .expect("save plan b");

        let list_page2 = wf_a
            .list_plans(Some(list_cursor.clone()), Some(2))
            .expect("list_plans page 2 unaffected by binding-b");
        assert_eq!(list_page2.plans.len(), 1);
        assert_eq!(list_page2.plans[0].plan_id, "plan-a-3");

        // 6. Publishing `plan-a-1` changes its status in `binding-a`, which invalidates both
        // `list_cursor` and `task_cursor` as `stale_revision`.
        wf_a.publish("plan-a-1", 1).expect("publish plan-a-1");

        assert_eq!(
            wf_a.list_plans(Some(list_cursor), Some(2))
                .unwrap_err()
                .code(),
            "stale_revision"
        );
        assert_eq!(
            wf_a.get_plan("plan-a-1", Some(1), Some(task_cursor), Some(1))
                .unwrap_err()
                .code(),
            "stale_revision"
        );
    }

    #[test]
    fn mcp_wire_budget_256kib_full_text_pagination_and_oversized_rejection() {
        let (_tmp, registry, plans, reg_rev) = setup_stores();
        let wf = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-1".into(),
        )
        .expect("wf");

        // 1. Create a plan with 5 tasks, each carrying a 45 KiB full instruction (~225 KiB raw,
        // which is ~450 KiB in the double-encoded `CallToolResult` envelope).
        // `get_plan` with default limit 10 must automatically paginate without truncating any instruction,
        // keeping every page `<= 256 KiB`.
        let large_tasks: Vec<PlanTask> = (1..=5)
            .map(|idx| PlanTask {
                id: format!("big-task-{idx}"),
                title: format!("Title for big-task-{idx}"),
                instruction: format!("Task-{idx}-header:{}", "x".repeat(45_000)),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: if idx == 1 {
                    Vec::new()
                } else {
                    vec![format!("big-task-{}", idx - 1)]
                },
                allowed_write_paths: vec![format!("crates/threadlane-workflow/src/part_{idx}.rs")],
            })
            .collect();

        wf.save_draft(SavePlanDraft {
            command_key: "cmd-big-plan".to_string(),
            plan_id: Some("plan-big-paged".to_string()),
            expected_revision: 0,
            profile_id: "default-profile".to_string(),
            profile_revision: reg_rev,
            summary: "Plan with multi-page 45 KiB tasks".to_string(),
            tasks: large_tasks.clone(),
        })
        .expect("save big plan");

        let mut collected_tasks = Vec::new();
        let mut cursor = None;
        let mut page_count = 0usize;

        loop {
            let page = wf
                .get_plan("plan-big-paged", None, cursor.clone(), Some(10))
                .expect("get_plan page within 256 KiB budget");
            page_count += 1;

            let envelope =
                ThreadlaneMcpServer::build_structured_tool_result("binding-1", "get_plan", &page);
            let encoded = serde_json::to_vec(&envelope).expect("serialize MCP envelope");
            assert!(
                encoded.len() <= MAX_MCP_RESPONSE_BYTES,
                "page {page_count} MCP envelope ({} bytes) exceeded {MAX_MCP_RESPONSE_BYTES}",
                encoded.len()
            );

            collected_tasks.extend(page.tasks);
            if let Some(next) = page.next_task_cursor {
                assert!(page.has_more);
                cursor = Some(next);
            } else {
                assert!(!page.has_more);
                break;
            }
        }

        assert!(
            page_count >= 2,
            "5 x 45 KiB tasks must span multiple MCP-bounded pages"
        );
        assert_eq!(collected_tasks, large_tasks);

        // 2. A single task whose 64 KiB instruction contains 50,000 quote characters (`"`) expands past
        // 256 KiB in the double-encoded `CallToolResult` envelope and must fail `get_plan` with `response_too_large`.
        let quote_heavy_instruction = "\"".repeat(50_000);
        wf.save_draft(SavePlanDraft {
            command_key: "cmd-oversized-single-task".to_string(),
            plan_id: Some("plan-oversized-item".to_string()),
            expected_revision: 0,
            profile_id: "default-profile".to_string(),
            profile_revision: reg_rev,
            summary: "Single item envelope > 256 KiB".to_string(),
            tasks: vec![sample_task(
                "huge-item",
                WorkflowRole::Coder,
                Difficulty::Easy,
                &[],
                &["src/lib.rs"],
                &quote_heavy_instruction,
            )],
        })
        .expect("save_draft succeeds because summary omits instructions");

        let too_large_get = wf
            .get_plan("plan-oversized-item", None, None, Some(1))
            .expect_err("single item exceeding 256 KiB MCP envelope must fail");
        assert_eq!(too_large_get.code(), "response_too_large");

        // 3. A draft whose `PublishedPlanView` routes exceed the 256 KiB MCP envelope must fail `publish`
        // with `response_too_large` BEFORE mutating `PlanStore` (plan remains `Draft`).
        let many_route_tasks: Vec<PlanTask> = (1..=60)
            .map(|idx| {
                let paths: Vec<String> = (1..=8)
                    .map(|p_idx| {
                        format!(
                            "crates/threadlane-workflow/src/deep_module_{idx:02}/sub_{p_idx:02}/{}",
                            "segment_".repeat(45)
                        )
                    })
                    .collect();
                PlanTask {
                    id: format!("route-task-{idx:02}"),
                    title: format!("Route Task {idx}"),
                    instruction: "Short instruction".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: Vec::new(),
                    allowed_write_paths: paths,
                }
            })
            .collect();

        wf.save_draft(SavePlanDraft {
            command_key: "cmd-oversized-routes".to_string(),
            plan_id: Some("plan-oversized-publish".to_string()),
            expected_revision: 0,
            profile_id: "default-profile".to_string(),
            profile_revision: reg_rev,
            summary: "Plan with oversized route view".to_string(),
            tasks: many_route_tasks,
        })
        .expect("save draft with large route paths");

        let pub_too_large = wf
            .publish("plan-oversized-publish", 1)
            .expect_err("publish exceeding 256 KiB MCP envelope must fail before mutation");
        assert_eq!(pub_too_large.code(), "response_too_large");

        // Verify `plan-oversized-publish` was NOT mutated to `Published` in `PlanStore`.
        let unmutated = plans
            .latest_revision("plan-oversized-publish")
            .expect("latest")
            .expect("exists");
        assert_eq!(unmutated.status, PlanStatus::Draft);
    }

    #[test]
    fn corrupt_stores_and_command_journal_bounds_are_rejected_and_preserved_untouched() {
        let (_tmp, registry, plans, reg_rev) = setup_stores();
        let wf = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-1".into(),
        )
        .expect("wf");

        let sample = sample_task(
            "t1",
            WorkflowRole::Coder,
            Difficulty::Easy,
            &[],
            &["src/lib.rs"],
            "Sample task",
        );

        // 1. Populate the command journal to `MAX_COMMAND_JOURNAL_ENTRIES` and verify the `(MAX + 1)`th
        // new `command_key` is rejected with `invalid_input` without mutating `PlanStore`.
        let mut full_journal = PersistedCommandJournal::new("binding-1");
        let td = compute_tasks_digest(std::slice::from_ref(&sample)).expect("td");
        for idx in 0..MAX_COMMAND_JOURNAL_ENTRIES {
            let plan_id = format!("plan-cap-{idx}");
            let pd = compute_payload_digest(
                &plan_id,
                "default-profile",
                reg_rev,
                "Cap summary",
                std::slice::from_ref(&sample),
            )
            .expect("pd");
            full_journal.entries.push(CommandJournalEntry {
                command_key: format!("cmd-cap-{idx}"),
                explicit_plan_id: Some(plan_id.clone()),
                resolved_plan_id: plan_id,
                expected_revision: 0,
                target_revision: 1,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: "Cap summary".to_string(),
                tasks_digest: td.clone(),
                payload_digest: pd,
                plan_digest: None,
                state: CommandJournalState::Failed,
            });
        }
        with_journal_lock(
            plans.dir(),
            &plans.dir().join("test-cap.lock"),
            |clean_dir| write_journal_locked(clean_dir, wf.command_journal_path(), &full_journal),
        )
        .expect("write full journal");

        let cap_err = wf
            .save_draft(SavePlanDraft {
                command_key: "cmd-over-capacity".to_string(),
                plan_id: Some("plan-over-capacity".to_string()),
                expected_revision: 0,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: "Over capacity".to_string(),
                tasks: vec![sample.clone()],
            })
            .expect_err("exceeding MAX_COMMAND_JOURNAL_ENTRIES must fail");
        assert_eq!(cap_err.code(), "invalid_input");
        assert!(plans.snapshot().expect("snapshot").is_empty());

        // 2. Corrupt the per-binding command journal file and verify `save_draft` fails closed with
        // `corrupt_store` while preserving the corrupt journal bytes untouched.
        let corrupt_journal_bytes = b"{\"schema_version\":1,\"binding_id\":\"binding-1\",corrupt!";
        let journal_file = wf.command_journal_path().to_path_buf();
        fs::write(&journal_file, corrupt_journal_bytes).expect("write corrupt command journal");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&journal_file, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 journal");
        }

        let wf_reopened = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            "binding-1".into(),
        )
        .expect("wf_reopened");
        let journal_err = wf_reopened
            .save_draft(SavePlanDraft {
                command_key: "cmd-on-corrupt-journal".to_string(),
                plan_id: None,
                expected_revision: 0,
                profile_id: "default-profile".to_string(),
                profile_revision: reg_rev,
                summary: "Should fail on corrupt journal".to_string(),
                tasks: vec![sample],
            })
            .expect_err("corrupt command journal must fail closed");
        assert_eq!(journal_err.code(), "corrupt_store");
        assert_eq!(
            fs::read(&journal_file).expect("read corrupt command journal"),
            corrupt_journal_bytes
        );
        assert!(plans.snapshot().expect("snapshot").is_empty());

        // 3. Corrupt `plans.json` and `registry.json` and verify both are preserved untouched.
        let corrupt_plans_bytes = b"{\"schema_version\":1,\"revisions\":[corrupt!";
        let plans_file = plans.dir().join("plans.json");
        fs::write(&plans_file, corrupt_plans_bytes).expect("write corrupt plans.json");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&plans_file, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 plans.json");
        }

        let err = wf
            .list_plans(None, None)
            .expect_err("corrupt plans.json must fail");
        assert_eq!(err.code(), "corrupt_store");
        assert_eq!(
            fs::read(&plans_file).expect("read corrupt plans.json"),
            corrupt_plans_bytes
        );

        let corrupt_reg_bytes = b"{\"schema_version\":1,corrupt!";
        fs::write(registry.registry_path(), corrupt_reg_bytes)
            .expect("write corrupt registry.json");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(registry.registry_path(), fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 registry.json");
        }

        let reg_err = wf
            .catalog(None, None)
            .expect_err("corrupt registry.json must fail");
        assert_eq!(reg_err.code(), "corrupt_store");
        assert_eq!(
            fs::read(registry.registry_path()).expect("read corrupt registry.json"),
            corrupt_reg_bytes
        );
    }
}
