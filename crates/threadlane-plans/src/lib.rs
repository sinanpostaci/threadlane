#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! `threadlane-plans`: durable versioned plan ledger, pure registry resolution compiler,
//! and cancellation/proven-stop/replacement reconciliation for Threadlane Milestone 4.
//!
//! ## Core Invariants
//!
//! - **Explicit publish boundary**: Prose in task titles or instructions is untrusted data and
//!   never grants scope, selects models, or authorizes execution. Only explicitly published
//!   revisions (`published`, `received`, `applied`) can be compiled for runtime execution.
//! - **Compare-and-swap & idempotent transitions**: `expected_revision` checks the current
//!   revision of `plan_id` (`0` for a new plan). `save_draft` increments the revision number;
//!   `publish`, `receive`, and `apply` transition the current revision in place with atomic CAS
//!   and idempotent deduplication of exact repeated transitions.
//! - **Immutable published revisions**: Published revisions never mutate their task, summary,
//!   profile, or binding payload in place and cannot revert to `draft`. Publishing a newer
//!   revision marks prior revisions `superseded` while preserving their historical snapshot.
//! - **Validated task DAG and write scopes**: Every plan revision enforces unique task IDs,
//!   valid dependency references, cycle-free topological order, and clean repository-relative
//!   non-protected `allowed_write_paths`.
//! - **Pure compiler & replacement reconciliation**: Role/difficulty resolution against a pinned
//!   [`RegistrySnapshot`] and task replacement tracking are pure functions with zero process or
//!   runtime side effects. In-flight live steering is never claimed (`live_steer_delivered: false`),
//!   prior artifacts are preserved (`preserve_old_artifacts: true`), and `unknown` stop outcomes
//!   always block replacement admission until proven stopped.

/// Pure runtime snapshot compiler resolving workflow roles and difficulties against a pinned registry snapshot.
pub mod compiler;
/// Structured domain, validation, store, compiler, and reconciliation error types.
pub mod error;
/// Pure task diff, cancellation, proven-stop, and replacement admission reconciliation.
pub mod reconcile;
/// Persistent cross-process locked versioned plan ledger (`PlanStore`).
pub mod store;
/// Serializable plan, task, status, receipt, change, and replacement state types.
pub mod types;
/// Bounds constants, identifier/scope validators, and DAG cycle detection.
pub mod validation;

pub use compiler::{compile_plan_revision, preview_draft_compilation, CompiledPlan, CompiledTask};
pub use error::PlanError;
pub use reconcile::{
    admit_task_replacement, reconcile_compiled_plans, reconcile_plan_revisions,
    record_task_proven_stop, record_task_unknown_stop, request_task_cancel,
    PlanReconciliationReport, PriorTaskStopStatus, TaskChangeKind, TaskReplacement,
};
pub use store::PlanStore;
pub use threadlane_registry::{
    resolve_choice, Difficulty, ModelChoice, ModelDefinition, RegistryError, RegistrySnapshot,
    RoleAssignment, ToolAdapter, ToolDefinition, WorkflowProfile, WorkflowRole,
};
pub use types::{
    DeliveryReceipt, PlanChange, PlanRevision, PlanStatus, PlanTask, ReplacementState,
};
pub use validation::{
    is_protected_control_path, validate_allowed_write_path, validate_identifier,
    validate_js_safe_revision, validate_plan_revision, validate_plan_tasks, validate_summary,
    validate_task_instruction, validate_task_title, MAX_DEPENDENCIES_PER_TASK, MAX_ID_LEN,
    MAX_JS_SAFE_REVISION, MAX_PLANS_IN_STORE, MAX_PLAN_STORE_BYTES, MAX_PLAN_SUMMARY_BYTES,
    MAX_REVISIONS_PER_PLAN, MAX_TASKS_PER_PLAN, MAX_TASK_INSTRUCTION_BYTES, MAX_TASK_TITLE_BYTES,
    MAX_WRITE_PATHS_PER_TASK, MAX_WRITE_PATH_BYTES, PLAN_STORE_SCHEMA_VERSION,
};

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;

    use tempfile::TempDir;

    use super::*;

    fn sample_task(
        id: &str,
        role: WorkflowRole,
        difficulty: Difficulty,
        depends_on: Vec<&str>,
        write_paths: Vec<&str>,
    ) -> PlanTask {
        PlanTask {
            id: id.to_string(),
            title: format!("Task {id}"),
            instruction: format!("Implement bounded slice for {id}"),
            role,
            difficulty,
            depends_on: depends_on.into_iter().map(str::to_string).collect(),
            allowed_write_paths: write_paths.into_iter().map(str::to_string).collect(),
        }
    }

    fn sample_registry_snapshot(revision: u64) -> RegistrySnapshot {
        let agy_adapter: ToolAdapter =
            serde_json::from_str("\"antigravity\"").expect("deserialize antigravity adapter");
        let opencode_adapter: ToolAdapter =
            serde_json::from_str("\"opencode_v2\"").expect("deserialize opencode_v2 adapter");

        RegistrySnapshot {
            schema_version: 1,
            revision,
            tools: vec![
                ToolDefinition {
                    id: "tool-agy".to_string(),
                    label: "Antigravity".to_string(),
                    adapter: agy_adapter,
                    executable_path: Some(PathBuf::from("/usr/bin/agy")),
                    enabled: true,
                },
                ToolDefinition {
                    id: "tool-opencode".to_string(),
                    label: "OpenCode v2".to_string(),
                    adapter: opencode_adapter,
                    executable_path: Some(PathBuf::from("/usr/bin/opencode")),
                    enabled: true,
                },
            ],
            models: vec![
                ModelDefinition {
                    id: "model-argon".to_string(),
                    tool_id: "tool-agy".to_string(),
                    label: "Gemini 4 Argon High".to_string(),
                    model_arg: "gemini-4-argon-high".to_string(),
                    efforts: vec!["medium".to_string(), "high".to_string()],
                    allow_no_effort: false,
                },
                ModelDefinition {
                    id: "model-deepseek".to_string(),
                    tool_id: "tool-opencode".to_string(),
                    label: "DeepSeek v4.1 Flash".to_string(),
                    model_arg: "opencode-go/deepseek-v4.1-flash".to_string(),
                    efforts: vec!["max".to_string()],
                    allow_no_effort: false,
                },
            ],
            profiles: vec![WorkflowProfile {
                id: "profile-default".to_string(),
                name: "Default M4 Profile".to_string(),
                assignments: vec![
                    RoleAssignment {
                        role: WorkflowRole::Coder,
                        difficulty: Some(Difficulty::Easy),
                        choice: ModelChoice {
                            model_id: "model-deepseek".to_string(),
                            effort: Some("max".to_string()),
                        },
                    },
                    RoleAssignment {
                        role: WorkflowRole::Coder,
                        difficulty: Some(Difficulty::Hard),
                        choice: ModelChoice {
                            model_id: "model-argon".to_string(),
                            effort: Some("high".to_string()),
                        },
                    },
                    RoleAssignment {
                        role: WorkflowRole::Reviewer,
                        difficulty: None,
                        choice: ModelChoice {
                            model_id: "model-argon".to_string(),
                            effort: Some("high".to_string()),
                        },
                    },
                ],
            }],
            default_profile_id: Some("profile-default".to_string()),
        }
    }

    #[test]
    fn store_reopen_lifecycle_cas_and_idempotent_dedup_transitions() {
        let tmp = TempDir::new().expect("tempdir");
        let store_dir = tmp.path().join("plans-store");

        let store = PlanStore::open(&store_dir).expect("open store");
        assert!(store.snapshot().expect("initial snapshot").is_empty());

        let tasks = vec![
            sample_task(
                "task-1",
                WorkflowRole::Coder,
                Difficulty::Easy,
                vec![],
                vec!["crates/threadlane-plans/src/types.rs"],
            ),
            sample_task(
                "task-2",
                WorkflowRole::Reviewer,
                Difficulty::Hard,
                vec!["task-1"],
                vec!["crates/threadlane-plans/src/lib.rs"],
            ),
        ];

        let draft = store
            .change_at(
                0,
                PlanChange::SaveDraft {
                    plan_id: "plan-alpha".to_string(),
                    binding_id: "binding-1".to_string(),
                    profile_id: "profile-default".to_string(),
                    profile_revision: 3,
                    summary: "Initial M4 plan".to_string(),
                    tasks: tasks.clone(),
                },
                1_700_000_000_000,
            )
            .expect("save draft rev 1");
        assert_eq!(draft.revision, 1);
        assert_eq!(draft.status, PlanStatus::Draft);
        assert_eq!(draft.created_at_ms, 1_700_000_000_000);

        // Publish revision 1 and repeat exact same transition idempotently.
        let published = store
            .change(
                1,
                PlanChange::Publish {
                    plan_id: "plan-alpha".to_string(),
                    revision: 1,
                },
            )
            .expect("publish rev 1");
        assert_eq!(published.status, PlanStatus::Published);
        assert_eq!(published.created_at_ms, 1_700_000_000_000);

        let published_repeat = store
            .change(
                1,
                PlanChange::Publish {
                    plan_id: "plan-alpha".to_string(),
                    revision: 1,
                },
            )
            .expect("idempotent repeat publish rev 1");
        assert_eq!(published_repeat, published);

        // Receive revision 1 and repeat idempotently.
        let received = store
            .change(
                1,
                PlanChange::Receive {
                    plan_id: "plan-alpha".to_string(),
                    revision: 1,
                },
            )
            .expect("receive rev 1");
        assert_eq!(received.status, PlanStatus::Received);

        let received_repeat = store
            .change(
                1,
                PlanChange::Receive {
                    plan_id: "plan-alpha".to_string(),
                    revision: 1,
                },
            )
            .expect("idempotent repeat receive rev 1");
        assert_eq!(received_repeat, received);

        // Apply revision 1 and repeat idempotently.
        let applied = store
            .change(
                1,
                PlanChange::Apply {
                    plan_id: "plan-alpha".to_string(),
                    revision: 1,
                },
            )
            .expect("apply rev 1");
        assert_eq!(applied.status, PlanStatus::Applied);

        let applied_repeat = store
            .change(
                1,
                PlanChange::Apply {
                    plan_id: "plan-alpha".to_string(),
                    revision: 1,
                },
            )
            .expect("idempotent repeat apply rev 1");
        assert_eq!(applied_repeat, applied);

        drop(store);

        // Reopen from disk and verify durability and delivery receipts.
        let reopened = PlanStore::open(&store_dir).expect("reopen store");
        let snap = reopened.snapshot().expect("snapshot after reopen");
        assert_eq!(snap, vec![applied.clone()]);
        assert_eq!(
            reopened
                .delivery_receipt("plan-alpha", 1)
                .expect("receipt lookup"),
            Some(DeliveryReceipt {
                plan_id: "plan-alpha".to_string(),
                revision: 1,
                state: PlanStatus::Applied,
            })
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let dir_mode = fs::symlink_metadata(&store_dir)
                .expect("dir meta")
                .permissions()
                .mode()
                & 0o777;
            let file_mode = fs::symlink_metadata(store_dir.join("plans.json"))
                .expect("file meta")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(dir_mode, 0o700);
            assert_eq!(file_mode, 0o600);
        }
    }

    #[test]
    fn stale_revisions_and_out_of_order_transitions_are_rejected() {
        let tmp = TempDir::new().expect("tempdir");
        let store = PlanStore::open(tmp.path().join("plans")).expect("open store");

        let task = sample_task(
            "t1",
            WorkflowRole::Coder,
            Difficulty::Easy,
            vec![],
            vec!["src/lib.rs"],
        );

        // New plan requires expected_revision == 0.
        let err = store
            .change(
                1,
                PlanChange::SaveDraft {
                    plan_id: "plan-1".to_string(),
                    binding_id: "binding-1".to_string(),
                    profile_id: "profile-default".to_string(),
                    profile_revision: 1,
                    summary: "First draft".to_string(),
                    tasks: vec![task.clone()],
                },
            )
            .expect_err("expected stale revision on new plan with expected_revision 1");
        assert_eq!(err.code(), "stale_revision");
        assert!(matches!(
            err,
            PlanError::StaleRevision {
                expected_revision: 1,
                actual_revision: 0,
                ..
            }
        ));

        let _rev1 = store
            .change(
                0,
                PlanChange::SaveDraft {
                    plan_id: "plan-1".to_string(),
                    binding_id: "binding-1".to_string(),
                    profile_id: "profile-default".to_string(),
                    profile_revision: 1,
                    summary: "First draft".to_string(),
                    tasks: vec![task.clone()],
                },
            )
            .expect("create rev 1");

        // Stale expected_revision 0 when actual is 1.
        let stale_draft = store
            .change(
                0,
                PlanChange::SaveDraft {
                    plan_id: "plan-1".to_string(),
                    binding_id: "binding-1".to_string(),
                    profile_id: "profile-default".to_string(),
                    profile_revision: 1,
                    summary: "Conflicting draft".to_string(),
                    tasks: vec![task],
                },
            )
            .expect_err("stale save_draft at expected_revision 0");
        assert!(matches!(
            stale_draft,
            PlanError::StaleRevision {
                expected_revision: 0,
                actual_revision: 1,
                ..
            }
        ));

        // Out-of-order: Receive on Draft (skipping Publish).
        let out_of_order_receive = store
            .change(
                1,
                PlanChange::Receive {
                    plan_id: "plan-1".to_string(),
                    revision: 1,
                },
            )
            .expect_err("receive before publish must fail");
        assert_eq!(out_of_order_receive.code(), "invalid_state_transition");

        // Out-of-order: Apply on Draft (skipping Publish and Receive).
        let out_of_order_apply_draft = store
            .change(
                1,
                PlanChange::Apply {
                    plan_id: "plan-1".to_string(),
                    revision: 1,
                },
            )
            .expect_err("apply on draft must fail");
        assert_eq!(out_of_order_apply_draft.code(), "invalid_state_transition");

        // Publish rev 1, then try Apply before Receive.
        store
            .change(
                1,
                PlanChange::Publish {
                    plan_id: "plan-1".to_string(),
                    revision: 1,
                },
            )
            .expect("publish rev 1");

        let out_of_order_apply_pub = store
            .change(
                1,
                PlanChange::Apply {
                    plan_id: "plan-1".to_string(),
                    revision: 1,
                },
            )
            .expect_err("apply on published before receive must fail");
        assert_eq!(out_of_order_apply_pub.code(), "invalid_state_transition");
    }

    #[test]
    fn published_immutability_and_supersede_semantics() {
        let tmp = TempDir::new().expect("tempdir");
        let store = PlanStore::open(tmp.path().join("plans")).expect("open store");

        let task_v1 = sample_task(
            "task-a",
            WorkflowRole::Coder,
            Difficulty::Easy,
            vec![],
            vec!["crates/threadlane-plans/src/store.rs"],
        );
        let mut task_v2 = task_v1.clone();
        task_v2.instruction = "Updated instruction for rev 2".to_string();

        // Draft 1 -> Draft 2 without publishing: Draft 1 immediately becomes Superseded.
        let d1 = store
            .change_at(
                0,
                PlanChange::SaveDraft {
                    plan_id: "plan-draft-supersede".to_string(),
                    binding_id: "binding-1".to_string(),
                    profile_id: "profile-default".to_string(),
                    profile_revision: 1,
                    summary: "Draft 1".to_string(),
                    tasks: vec![task_v1.clone()],
                },
                1000,
            )
            .expect("save draft 1");
        assert_eq!(d1.status, PlanStatus::Draft);

        let d2 = store
            .change_at(
                1,
                PlanChange::SaveDraft {
                    plan_id: "plan-draft-supersede".to_string(),
                    binding_id: "binding-1".to_string(),
                    profile_id: "profile-default".to_string(),
                    profile_revision: 1,
                    summary: "Draft 2".to_string(),
                    tasks: vec![task_v2.clone()],
                },
                2000,
            )
            .expect("save draft 2");
        assert_eq!(d2.revision, 2);
        assert_eq!(d2.status, PlanStatus::Draft);
        assert_eq!(
            store
                .get_revision("plan-draft-supersede", 1)
                .expect("get rev 1")
                .expect("rev 1 exists")
                .status,
            PlanStatus::Superseded
        );

        // Published revision immutability:
        let _ = store
            .change_at(
                0,
                PlanChange::SaveDraft {
                    plan_id: "plan-immutable".to_string(),
                    binding_id: "binding-1".to_string(),
                    profile_id: "profile-default".to_string(),
                    profile_revision: 1,
                    summary: "Published v1 summary".to_string(),
                    tasks: vec![task_v1.clone()],
                },
                5000,
            )
            .expect("create rev 1");
        let pub_v1 = store
            .change(
                1,
                PlanChange::Publish {
                    plan_id: "plan-immutable".to_string(),
                    revision: 1,
                },
            )
            .expect("publish rev 1");

        // Saving draft v2 does not alter published rev 1 in place; rev 1 stays Published
        // with its exact original payload while v2 is an unapproved Draft.
        let draft_v2 = store
            .change_at(
                1,
                PlanChange::SaveDraft {
                    plan_id: "plan-immutable".to_string(),
                    binding_id: "binding-1".to_string(),
                    profile_id: "profile-default".to_string(),
                    profile_revision: 2,
                    summary: "Draft v2 summary".to_string(),
                    tasks: vec![task_v2.clone()],
                },
                6000,
            )
            .expect("save draft v2");
        assert_eq!(draft_v2.revision, 2);
        assert_eq!(draft_v2.status, PlanStatus::Draft);

        let rev1_while_v2_draft = store
            .get_revision("plan-immutable", 1)
            .expect("get rev 1")
            .expect("rev 1 exists");
        assert_eq!(rev1_while_v2_draft, pub_v1);

        // Publishing rev 2 marks rev 1 as Superseded while keeping all of rev 1's tasks,
        // summary, profile_id, profile_revision, and created_at_ms completely immutable.
        let pub_v2 = store
            .change(
                2,
                PlanChange::Publish {
                    plan_id: "plan-immutable".to_string(),
                    revision: 2,
                },
            )
            .expect("publish rev 2");
        assert_eq!(pub_v2.status, PlanStatus::Published);

        let rev1_after_v2_pub = store
            .get_revision("plan-immutable", 1)
            .expect("get rev 1")
            .expect("rev 1 exists");
        assert_eq!(rev1_after_v2_pub.status, PlanStatus::Superseded);
        assert_eq!(rev1_after_v2_pub.summary, pub_v1.summary);
        assert_eq!(rev1_after_v2_pub.tasks, pub_v1.tasks);
        assert_eq!(rev1_after_v2_pub.profile_id, pub_v1.profile_id);
        assert_eq!(rev1_after_v2_pub.profile_revision, pub_v1.profile_revision);
        assert_eq!(rev1_after_v2_pub.created_at_ms, pub_v1.created_at_ms);

        // Once rev 2 advances to Received, trying to re-Publish rev 2 is rejected as immutable.
        store
            .change(
                2,
                PlanChange::Receive {
                    plan_id: "plan-immutable".to_string(),
                    revision: 2,
                },
            )
            .expect("receive rev 2");
        let revert_err = store
            .change(
                2,
                PlanChange::Publish {
                    plan_id: "plan-immutable".to_string(),
                    revision: 2,
                },
            )
            .expect_err("cannot revert received revision back to published");
        assert_eq!(revert_err.code(), "immutable_published_revision");
    }

    #[test]
    fn dag_cycles_duplicate_ids_unknown_refs_and_write_scope_bounds_rejected() {
        // Duplicate task IDs.
        let dup_tasks = vec![
            sample_task(
                "dup",
                WorkflowRole::Coder,
                Difficulty::Easy,
                vec![],
                vec!["src/a.rs"],
            ),
            sample_task(
                "dup",
                WorkflowRole::Coder,
                Difficulty::Easy,
                vec![],
                vec!["src/b.rs"],
            ),
        ];
        let dup_err = validate_plan_tasks("plan-x", &dup_tasks).expect_err("duplicate task id");
        assert_eq!(dup_err.code(), "duplicate_task_id");

        // Unknown dependency reference.
        let unknown_dep_tasks = vec![sample_task(
            "t1",
            WorkflowRole::Coder,
            Difficulty::Easy,
            vec!["nonexistent"],
            vec!["src/a.rs"],
        )];
        let dep_err =
            validate_plan_tasks("plan-x", &unknown_dep_tasks).expect_err("unknown dependency");
        assert_eq!(dep_err.code(), "unknown_dependency");

        // Self-cycle (t1 -> t1).
        let self_cycle_tasks = vec![sample_task(
            "t1",
            WorkflowRole::Coder,
            Difficulty::Easy,
            vec!["t1"],
            vec!["src/a.rs"],
        )];
        let self_cycle_err =
            validate_plan_tasks("plan-x", &self_cycle_tasks).expect_err("self cycle");
        assert_eq!(self_cycle_err.code(), "dependency_cycle");

        // 3-node cycle (t1 -> t2 -> t3 -> t1).
        let cycle_3_tasks = vec![
            sample_task(
                "t1",
                WorkflowRole::Coder,
                Difficulty::Easy,
                vec!["t3"],
                vec!["src/a.rs"],
            ),
            sample_task(
                "t2",
                WorkflowRole::Coder,
                Difficulty::Easy,
                vec!["t1"],
                vec!["src/b.rs"],
            ),
            sample_task(
                "t3",
                WorkflowRole::Coder,
                Difficulty::Easy,
                vec!["t2"],
                vec!["src/c.rs"],
            ),
        ];
        let cycle_3_err = validate_plan_tasks("plan-x", &cycle_3_tasks).expect_err("3-node cycle");
        assert_eq!(cycle_3_err.code(), "dependency_cycle");

        // Disallowed write scopes: absolute paths, parent traversal, wildcards, and protected control paths.
        for bad_path in [
            "/etc/nixos/configuration.nix",
            "../outside/file.rs",
            "./src/lib.rs",
            "src//lib.rs",
            "src/dir/",
            "src/*.rs",
            ".git/config",
            "AGENTS.md",
            ".codex/config.toml",
            ".env.production",
            ".github/workflows/ci.yml",
            ".cargo/config.toml",
        ] {
            let bad_scope_tasks = vec![sample_task(
                "t1",
                WorkflowRole::Coder,
                Difficulty::Easy,
                vec![],
                vec![bad_path],
            )];
            let scope_err = validate_plan_tasks("plan-x", &bad_scope_tasks).unwrap_err();
            assert_eq!(scope_err.code(), "invalid_write_scope", "path `{bad_path}`");
        }

        // Valid diamond DAG produces deterministic topological order.
        let diamond_tasks = vec![
            sample_task(
                "root",
                WorkflowRole::Architect,
                Difficulty::Hard,
                vec![],
                vec!["docs/ROADMAP.md"],
            ),
            sample_task(
                "left",
                WorkflowRole::Coder,
                Difficulty::Easy,
                vec!["root"],
                vec!["crates/threadlane-plans/src/types.rs"],
            ),
            sample_task(
                "right",
                WorkflowRole::Coder,
                Difficulty::Hard,
                vec!["root"],
                vec!["crates/threadlane-plans/src/store.rs"],
            ),
            sample_task(
                "join",
                WorkflowRole::Reviewer,
                Difficulty::Hard,
                vec!["left", "right"],
                vec!["crates/threadlane-plans/src/lib.rs"],
            ),
        ];
        let order = validate_plan_tasks("plan-diamond", &diamond_tasks).expect("valid diamond DAG");
        assert_eq!(order, vec!["root", "left", "right", "join"]);
    }

    #[test]
    fn pure_registry_compiler_resolves_choices_and_rejects_unpublished_or_mismatched_profiles() {
        let registry = sample_registry_snapshot(7);
        let tasks = vec![
            sample_task(
                "coder-easy",
                WorkflowRole::Coder,
                Difficulty::Easy,
                vec![],
                vec!["crates/threadlane-plans/src/types.rs"],
            ),
            sample_task(
                "coder-hard",
                WorkflowRole::Coder,
                Difficulty::Hard,
                vec!["coder-easy"],
                vec!["crates/threadlane-plans/src/store.rs"],
            ),
            sample_task(
                "reviewer-any",
                WorkflowRole::Reviewer,
                Difficulty::Medium,
                vec!["coder-hard"],
                vec!["crates/threadlane-plans/src/lib.rs"],
            ),
        ];

        let mut plan = PlanRevision {
            plan_id: "plan-compile".to_string(),
            binding_id: "binding-1".to_string(),
            revision: 1,
            profile_id: "profile-default".to_string(),
            profile_revision: 7,
            summary: "Compiler test plan".to_string(),
            tasks,
            status: PlanStatus::Draft,
            created_at_ms: 10_000,
        };

        // Draft plans cannot be compiled as approved runtime plans.
        let unpub_err = compile_plan_revision(&plan, &registry)
            .expect_err("draft plan must not compile for runtime execution");
        assert_eq!(unpub_err.code(), "unpublished_plan");

        // Publish the plan and compile against matching registry snapshot.
        plan.status = PlanStatus::Published;
        let compiled =
            compile_plan_revision(&plan, &registry).expect("compile published plan revision");
        assert_eq!(
            compiled.topological_order,
            vec!["coder-easy", "coder-hard", "reviewer-any"]
        );
        assert_eq!(compiled.tasks.len(), 3);
        assert_eq!(compiled.tasks[0].choice.model_id, "model-deepseek");
        assert_eq!(compiled.tasks[0].choice.effort.as_deref(), Some("max"));
        assert_eq!(
            compiled.tasks[0].model_arg,
            "opencode-go/deepseek-v4.1-flash"
        );
        assert_eq!(compiled.tasks[1].choice.model_id, "model-argon");
        assert_eq!(compiled.tasks[1].choice.effort.as_deref(), Some("high"));
        // Reviewer falls back to explicit role default (difficulty: None), never another role.
        assert_eq!(compiled.tasks[2].choice.model_id, "model-argon");
        assert_eq!(compiled.tasks[2].choice.effort.as_deref(), Some("high"));

        // Mismatched registry revision is rejected.
        let stale_registry = sample_registry_snapshot(8);
        let mismatch_err = compile_plan_revision(&plan, &stale_registry)
            .expect_err("mismatched profile revision must fail");
        assert_eq!(mismatch_err.code(), "profile_revision_mismatch");

        // Missing role assignment (e.g. Architect not assigned in profile) is rejected without fallback.
        let mut missing_role_plan = plan.clone();
        missing_role_plan.tasks.push(sample_task(
            "arch-task",
            WorkflowRole::Architect,
            Difficulty::Hard,
            vec![],
            vec!["crates/threadlane-plans/src/compiler.rs"],
        ));
        let role_err = compile_plan_revision(&missing_role_plan, &registry)
            .expect_err("unassigned role must fail resolution");
        assert_eq!(role_err.code(), "registry_resolution_failed");

        // Changing tool executable_path with the same model and write scope invalidates
        // scope_and_choice_digest, task_digest, and plan_digest without creating execution authority.
        let mut reconfigured_registry = registry.clone();
        reconfigured_registry.tools[0].executable_path =
            Some(PathBuf::from("/opt/antigravity/bin/agy"));
        let compiled_reconfigured = compile_plan_revision(&plan, &reconfigured_registry)
            .expect("compile plan with updated tool executable_path");
        assert_eq!(
            compiled_reconfigured.tasks[0].scope_and_choice_digest,
            compiled.tasks[0].scope_and_choice_digest,
            "unaffected tool task digest remains stable"
        );
        assert_ne!(
            compiled_reconfigured.tasks[1].scope_and_choice_digest,
            compiled.tasks[1].scope_and_choice_digest,
            "changed executable_path must invalidate scope_and_choice_digest"
        );
        assert_ne!(
            compiled_reconfigured.tasks[1].task_digest, compiled.tasks[1].task_digest,
            "changed executable_path must invalidate task_digest"
        );
        assert_ne!(
            compiled_reconfigured.plan_digest, compiled.plan_digest,
            "changed executable_path must invalidate plan_digest"
        );
    }

    #[test]
    fn replacement_reconciliation_enforces_proven_stop_and_blocks_unknown_stop() {
        let registry = sample_registry_snapshot(4);

        let rev1 = PlanRevision {
            plan_id: "plan-reconcile".to_string(),
            binding_id: "binding-1".to_string(),
            revision: 1,
            profile_id: "profile-default".to_string(),
            profile_revision: 4,
            summary: "Rev 1".to_string(),
            tasks: vec![
                sample_task(
                    "keep",
                    WorkflowRole::Coder,
                    Difficulty::Easy,
                    vec![],
                    vec!["crates/threadlane-plans/src/types.rs"],
                ),
                sample_task(
                    "edit",
                    WorkflowRole::Coder,
                    Difficulty::Easy,
                    vec!["keep"],
                    vec!["crates/threadlane-plans/src/store.rs"],
                ),
                sample_task(
                    "downstream",
                    WorkflowRole::Reviewer,
                    Difficulty::Hard,
                    vec!["edit"],
                    vec!["crates/threadlane-plans/src/lib.rs"],
                ),
                sample_task(
                    "retire",
                    WorkflowRole::Coder,
                    Difficulty::Easy,
                    vec![],
                    vec!["crates/threadlane-plans/src/error.rs"],
                ),
            ],
            status: PlanStatus::Applied,
            created_at_ms: 1_000,
        };

        let mut modified_edit = rev1.tasks[1].clone();
        modified_edit.instruction = "Revised instruction requiring replacement".to_string();
        modified_edit.difficulty = Difficulty::Hard;

        let rev2 = PlanRevision {
            plan_id: "plan-reconcile".to_string(),
            binding_id: "binding-1".to_string(),
            revision: 2,
            profile_id: "profile-default".to_string(),
            profile_revision: 4,
            summary: "Rev 2".to_string(),
            tasks: vec![
                rev1.tasks[0].clone(),
                modified_edit,
                rev1.tasks[2].clone(),
                sample_task(
                    "added",
                    WorkflowRole::Coder,
                    Difficulty::Easy,
                    vec!["edit"],
                    vec!["crates/threadlane-plans/src/reconcile.rs"],
                ),
            ],
            status: PlanStatus::Published,
            created_at_ms: 2_000,
        };

        let compiled_v1 = compile_plan_revision(&rev1, &registry).expect("compile v1");
        let compiled_v2 = compile_plan_revision(&rev2, &registry).expect("compile v2");

        let mut observed = BTreeMap::new();
        observed.insert("keep".to_string(), PriorTaskStopStatus::Running);
        observed.insert("edit".to_string(), PriorTaskStopStatus::Running);
        observed.insert("downstream".to_string(), PriorTaskStopStatus::NotStarted);
        observed.insert("retire".to_string(), PriorTaskStopStatus::CancelRequested);

        let report = reconcile_compiled_plans(&compiled_v1, &compiled_v2, &observed)
            .expect("reconcile plans");

        assert!(!report.live_steer_delivered);
        assert_eq!(report.unchanged_task_ids, vec!["keep"]);
        assert_eq!(
            report.affected_task_ids,
            vec!["edit", "downstream", "added", "retire"]
        );

        let edit_entry = report.task_replacement("edit").expect("edit entry");
        assert_eq!(edit_entry.change_kind, TaskChangeKind::Modified);
        assert_eq!(edit_entry.state, ReplacementState::Pending);
        assert!(!edit_entry.admission_allowed);
        assert!(edit_entry.preserve_old_artifacts);
        assert!(!edit_entry.live_steer_delivered);

        let downstream_entry = report
            .task_replacement("downstream")
            .expect("downstream entry");
        assert_eq!(
            downstream_entry.change_kind,
            TaskChangeKind::DependencyUpdated
        );
        assert_eq!(downstream_entry.state, ReplacementState::Stopped);
        // Blocked because upstream `edit` is not yet Admitted!
        assert!(!downstream_entry.admission_allowed);
        assert!(downstream_entry.preserve_old_artifacts);

        // Cannot admit `edit` while Pending.
        let pending_err =
            admit_task_replacement(&report, "edit").expect_err("cannot admit from pending");
        assert_eq!(pending_err.code(), "replacement_not_stopped");

        // Request cancel on `edit` (Pending -> CancelRequested) and repeat idempotently.
        let report = request_task_cancel(&report, "edit").expect("request cancel");
        let report_repeat =
            request_task_cancel(&report, "edit").expect("idempotent request cancel");
        assert_eq!(report_repeat, report);
        assert_eq!(
            report.task_replacement("edit").unwrap().state,
            ReplacementState::CancelRequested
        );

        // Transition `edit` to `Unknown` (e.g. lost supervision).
        let report = record_task_unknown_stop(&report, "edit").expect("record unknown stop");
        assert_eq!(
            report.task_replacement("edit").unwrap().state,
            ReplacementState::Unknown
        );
        assert!(!report.task_replacement("edit").unwrap().admission_allowed);

        // Unknown NEVER admits replacement — for `edit` itself OR for downstream tasks!
        let unknown_err =
            admit_task_replacement(&report, "edit").expect_err("unknown blocks admission");
        assert_eq!(unknown_err.code(), "unknown_stop_blocks_admission");

        let downstream_unknown_err = admit_task_replacement(&report, "downstream")
            .expect_err("upstream unknown blocks downstream admission");
        assert_eq!(
            downstream_unknown_err.code(),
            "unknown_stop_blocks_admission"
        );

        // Once `edit` is proven stopped (Unknown -> Stopped), admission becomes allowed for `edit`.
        let report = record_task_proven_stop(&report, "edit").expect("record proven stop");
        assert_eq!(
            report.task_replacement("edit").unwrap().state,
            ReplacementState::Stopped
        );
        assert!(report.task_replacement("edit").unwrap().admission_allowed);

        // Admit `edit` (Stopped -> Admitted); now `downstream` and `added` become eligible for admission.
        let report = admit_task_replacement(&report, "edit").expect("admit edit replacement");
        assert_eq!(
            report.task_replacement("edit").unwrap().state,
            ReplacementState::Admitted
        );
        assert!(
            report
                .task_replacement("downstream")
                .unwrap()
                .admission_allowed
        );
        assert!(report.task_replacement("added").unwrap().admission_allowed);

        let report =
            admit_task_replacement(&report, "downstream").expect("admit downstream replacement");
        assert_eq!(
            report.task_replacement("downstream").unwrap().state,
            ReplacementState::Admitted
        );

        // Removed task `retire` can be proven stopped, preserves old artifacts, and never admits replacement.
        let report = record_task_proven_stop(&report, "retire").expect("retire proven stopped");
        let retire_entry = report.task_replacement("retire").unwrap();
        assert_eq!(retire_entry.state, ReplacementState::Stopped);
        assert!(retire_entry.preserve_old_artifacts);
        assert!(!retire_entry.admission_allowed);
        let retire_admit_err = admit_task_replacement(&report, "retire")
            .expect_err("removed task cannot admit replacement");
        assert_eq!(retire_admit_err.code(), "invalid_replacement_transition");
    }

    #[test]
    fn corrupt_store_is_preserved_and_rejected_on_open() {
        let tmp = TempDir::new().expect("tempdir");
        let store_dir = tmp.path().join("corrupt-plans");
        let _store = PlanStore::open(&store_dir).expect("initialize store dir");

        let ledger_path = store_dir.join("plans.json");
        let corrupt_bytes = b"{not-valid-json";
        #[cfg(unix)]
        {
            use std::io::Write as _;
            use std::os::unix::fs::OpenOptionsExt;
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&ledger_path)
                .expect("write corrupt ledger");
            file.write_all(corrupt_bytes).expect("write bytes");
        }

        let err = PlanStore::open(&store_dir).expect_err("corrupt ledger must fail open");
        assert_eq!(err.code(), "corrupt_store");
        assert_eq!(
            fs::read(&ledger_path).expect("corrupt file preserved"),
            corrupt_bytes
        );
    }
}
