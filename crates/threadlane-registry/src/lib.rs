#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! `threadlane-registry`: persistent global tool, model, and workflow profile registry
//! for Threadlane Milestone 4.
//!
//! This crate is independent of Tauri, runtime SQLite state, and external provider CLIs.
//! It provides:
//! - Canonical `snake_case` wire types ([`ToolAdapter`], [`ToolDefinition`], [`ModelDefinition`],
//!   [`ModelChoice`], [`WorkflowRole`], [`Difficulty`], [`RoleAssignment`],
//!   [`DecisionAdvisorMode`], [`DecisionAdvisor`], [`WorkflowProfile`], [`RegistrySnapshot`], and
//!   [`RegistryChange`]).
//! - [`RegistryStore`]: a private (`0700`/`0600`), cross-process locked (`fs2`), atomic
//!   compare-and-swap JSON store that starts empty at revision `0` with no implicit models.
//! - [`resolve_choice`]: deterministic role and difficulty resolution (`exact difficulty`
//!   then `explicit role default`) with no silent model substitution.

mod error;
mod store;
mod types;

pub use error::RegistryError;
pub use store::{resolve_choice, validate_registry_identifier, RegistryStore};
pub use types::{
    DecisionAdvisor, DecisionAdvisorMode, Difficulty, ModelChoice, ModelDefinition, RegistryChange,
    RegistrySnapshot, RoleAssignment, ScopedModelDependency, ScopedProfileDependencySpec,
    ScopedToolDependency, ToolAdapter, ToolDefinition, WorkflowProfile, WorkflowRole,
    MAX_ASSIGNMENTS_PER_PROFILE, MAX_EFFORTS_PER_MODEL, MAX_EFFORT_LEN, MAX_EXECUTABLE_PATH_LEN,
    MAX_ID_LEN, MAX_JS_SAFE_INTEGER, MAX_LABEL_LEN, MAX_MODELS, MAX_MODEL_ARG_LEN, MAX_PROFILES,
    MAX_REGISTRY_FILE_BYTES, MAX_TOOLS, REGISTRY_FILE_NAME, REGISTRY_LOCK_FILE_NAME,
    REGISTRY_SCHEMA_VERSION,
};

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::path::PathBuf;

    use super::*;

    fn sample_tool(id: &str, adapter: ToolAdapter) -> ToolDefinition {
        ToolDefinition {
            id: id.to_string(),
            label: format!("Tool {id}"),
            adapter,
            executable_path: if adapter.is_cli_adapter() {
                Some(PathBuf::from(format!("/usr/local/bin/{id}")))
            } else {
                None
            },
            enabled: true,
        }
    }

    fn sample_model(
        id: &str,
        tool_id: &str,
        efforts: &[&str],
        allow_no_effort: bool,
    ) -> ModelDefinition {
        ModelDefinition {
            id: id.to_string(),
            tool_id: tool_id.to_string(),
            label: format!("Model {id}"),
            model_arg: id.to_string(),
            efforts: efforts.iter().map(|s| (*s).to_string()).collect(),
            allow_no_effort,
        }
    }

    #[test]
    fn empty_store_starts_at_revision_zero_with_no_implicit_models() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config_dir = temp.path().join("registry");

        let store = RegistryStore::open(&config_dir).expect("open registry store");
        assert!(!store.registry_path().exists());
        assert!(store.lock_path().exists());

        let dir_mode = fs::symlink_metadata(&config_dir)
            .expect("dir metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dir_mode, 0o700);

        let lock_mode = fs::symlink_metadata(store.lock_path())
            .expect("lock metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(lock_mode, 0o600);

        let snap = store.snapshot().expect("initial snapshot");
        assert_eq!(snap.schema_version, REGISTRY_SCHEMA_VERSION);
        assert_eq!(snap.revision, 0);
        assert!(snap.tools.is_empty());
        assert!(snap.models.is_empty());
        assert!(snap.profiles.is_empty());
        assert_eq!(snap.default_profile_id, None);

        let json = serde_json::to_value(&snap).expect("serialize snapshot");
        assert_eq!(json["default_profile_id"], serde_json::Value::Null);
    }

    #[test]
    fn serde_wire_format_and_option_null_serialization() {
        let expected_adapters = [
            (ToolAdapter::CodexCli, "codex_cli"),
            (ToolAdapter::OpencodeV2, "opencode_v2"),
            (ToolAdapter::Antigravity, "antigravity"),
            (ToolAdapter::JevApi, "jev_api"),
            (ToolAdapter::OpenaiDecisionsApi, "openai_decisions_api"),
            (ToolAdapter::Git, "git"),
            (ToolAdapter::Tmux, "tmux"),
            (ToolAdapter::Codegraph, "codegraph"),
        ];
        for (variant, wire) in expected_adapters {
            let encoded = serde_json::to_string(&variant).expect("serialize adapter");
            assert_eq!(encoded, format!("\"{wire}\""));
            let decoded: ToolAdapter = serde_json::from_str(&encoded).expect("deserialize adapter");
            assert_eq!(decoded, variant);
        }

        assert_eq!(ToolAdapter::OpenCodeV2, ToolAdapter::OpencodeV2);
        assert_eq!(
            ToolAdapter::OpenAiDecisionsApi,
            ToolAdapter::OpenaiDecisionsApi
        );
        assert_eq!(ToolAdapter::CodeGraph, ToolAdapter::Codegraph);

        let expected_roles = [
            (WorkflowRole::Follower, "follower"),
            (WorkflowRole::Orchestrator, "orchestrator"),
            (WorkflowRole::Architect, "architect"),
            (WorkflowRole::Coder, "coder"),
            (WorkflowRole::Reviewer, "reviewer"),
            (WorkflowRole::Closure, "closure"),
        ];
        for (role, wire) in expected_roles {
            let encoded = serde_json::to_string(&role).expect("serialize role");
            assert_eq!(encoded, format!("\"{wire}\""));
            let decoded: WorkflowRole = serde_json::from_str(&encoded).expect("deserialize role");
            assert_eq!(decoded, role);
        }

        let expected_difficulties = [
            (Difficulty::Easy, "easy"),
            (Difficulty::Medium, "medium"),
            (Difficulty::Hard, "hard"),
            (Difficulty::Extreme, "extreme"),
        ];
        for (diff, wire) in expected_difficulties {
            let encoded = serde_json::to_string(&diff).expect("serialize difficulty");
            assert_eq!(encoded, format!("\"{wire}\""));
            let decoded: Difficulty =
                serde_json::from_str(&encoded).expect("deserialize difficulty");
            assert_eq!(decoded, diff);
        }

        let api_tool = ToolDefinition {
            id: "jev".to_string(),
            label: "Jev API".to_string(),
            adapter: ToolAdapter::JevApi,
            executable_path: None,
            enabled: true,
        };
        let tool_val = serde_json::to_value(&api_tool).expect("serialize tool");
        assert!(tool_val.get("executable_path").is_some());
        assert_eq!(tool_val["executable_path"], serde_json::Value::Null);

        let assignment = RoleAssignment {
            role: WorkflowRole::Coder,
            difficulty: None,
            choice: ModelChoice {
                model_id: "m1".to_string(),
                effort: None,
            },
        };
        let assign_val = serde_json::to_value(&assignment).expect("serialize assignment");
        assert_eq!(assign_val["difficulty"], serde_json::Value::Null);
        assert_eq!(assign_val["choice"]["effort"], serde_json::Value::Null);

        let change = RegistryChange::SetDefaultProfile { profile_id: None };
        let change_val = serde_json::to_value(&change).expect("serialize change");
        assert_eq!(change_val["op"], "set_default_profile");
        assert_eq!(change_val["profile_id"], serde_json::Value::Null);
    }

    #[test]
    fn crud_lifecycle_default_profile_and_reopen_persistence() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config_dir = temp.path().join("registry");
        let store = RegistryStore::open(&config_dir).expect("open store");

        let s1 = store
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: sample_tool("agy", ToolAdapter::Antigravity),
                },
            )
            .expect("upsert agy tool");
        assert_eq!(s1.revision, 1);
        assert_eq!(s1.tools.len(), 1);

        let file_mode = fs::symlink_metadata(store.registry_path())
            .expect("registry.json metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(file_mode, 0o600);

        let s2 = store
            .apply(
                1,
                RegistryChange::UpsertTool {
                    tool: sample_tool("opencode", ToolAdapter::OpencodeV2),
                },
            )
            .expect("upsert opencode tool");
        assert_eq!(s2.revision, 2);
        assert_eq!(s2.tools.len(), 2);

        let s3 = store
            .apply(
                2,
                RegistryChange::UpsertModels {
                    models: vec![
                        sample_model("argon", "agy", &["medium", "high"], false),
                        sample_model("deepseek-flash", "opencode", &["max"], true),
                    ],
                },
            )
            .expect("upsert models batch");
        assert_eq!(s3.revision, 3);
        assert_eq!(s3.models.len(), 2);

        let profile = WorkflowProfile {
            id: "default-workflow".to_string(),
            name: "Default Workflow".to_string(),
            assignments: vec![
                RoleAssignment {
                    role: WorkflowRole::Coder,
                    difficulty: None,
                    choice: ModelChoice {
                        model_id: "deepseek-flash".to_string(),
                        effort: None,
                    },
                },
                RoleAssignment {
                    role: WorkflowRole::Coder,
                    difficulty: Some(Difficulty::Hard),
                    choice: ModelChoice {
                        model_id: "argon".to_string(),
                        effort: Some("high".to_string()),
                    },
                },
            ],
            decision_advisor: None,
        };
        let s4 = store
            .apply(3, RegistryChange::UpsertProfile { profile })
            .expect("upsert profile");
        assert_eq!(s4.revision, 4);
        assert_eq!(s4.profiles.len(), 1);

        let s5 = store
            .apply(
                4,
                RegistryChange::SetDefaultProfile {
                    profile_id: Some("default-workflow".to_string()),
                },
            )
            .expect("set default profile");
        assert_eq!(s5.revision, 5);
        assert_eq!(s5.default_profile_id.as_deref(), Some("default-workflow"));

        // Reopen a second store instance on the same directory and verify exact persistence.
        let reopened = RegistryStore::open(&config_dir).expect("reopen store");
        let loaded = reopened.snapshot().expect("snapshot after reopen");
        assert_eq!(loaded, s5);

        // Delete the profile; default_profile_id must be cleared automatically.
        let s6 = reopened
            .apply(
                5,
                RegistryChange::DeleteProfile {
                    profile_id: "default-workflow".to_string(),
                },
            )
            .expect("delete profile");
        assert_eq!(s6.revision, 6);
        assert!(s6.profiles.is_empty());
        assert_eq!(s6.default_profile_id, None);

        // Delete models and tools in order.
        let s7 = reopened
            .apply(
                6,
                RegistryChange::DeleteModel {
                    model_id: "argon".to_string(),
                },
            )
            .expect("delete argon");
        let s8 = reopened
            .apply(
                s7.revision,
                RegistryChange::DeleteModel {
                    model_id: "deepseek-flash".to_string(),
                },
            )
            .expect("delete deepseek");
        let s9 = reopened
            .apply(
                s8.revision,
                RegistryChange::DeleteTool {
                    tool_id: "agy".to_string(),
                },
            )
            .expect("delete agy");
        let s10 = reopened
            .apply(
                s9.revision,
                RegistryChange::DeleteTool {
                    tool_id: "opencode".to_string(),
                },
            )
            .expect("delete opencode");
        assert_eq!(s10.revision, 10);
        assert!(s10.tools.is_empty());
        assert!(s10.models.is_empty());
    }

    #[test]
    fn stale_revision_cas_and_cross_instance_reread() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config_dir = temp.path().join("registry");

        let store_a = RegistryStore::open(&config_dir).expect("open store A");
        let store_b = RegistryStore::open(&config_dir).expect("open store B");

        store_a
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: sample_tool("codex", ToolAdapter::CodexCli),
                },
            )
            .expect("store A apply rev 0 -> 1");

        let err = store_b
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: sample_tool("git", ToolAdapter::Git),
                },
            )
            .expect_err("store B stale revision 0 must fail");
        assert_eq!(
            err,
            RegistryError::StaleRevision {
                expected: 0,
                actual: 1
            }
        );
        assert_eq!(err.code(), "stale_revision");

        let latest = store_b.snapshot().expect("store B reread snapshot");
        assert_eq!(latest.revision, 1);
        assert_eq!(latest.tools.len(), 1);
        assert_eq!(latest.tools[0].id, "codex");

        let updated = store_b
            .apply(
                latest.revision,
                RegistryChange::UpsertTool {
                    tool: sample_tool("git", ToolAdapter::Git),
                },
            )
            .expect("store B apply at latest revision");
        assert_eq!(updated.revision, 2);
        assert_eq!(updated.tools.len(), 2);

        let overflow_err = store_b
            .apply(
                MAX_JS_SAFE_INTEGER + 1,
                RegistryChange::SetDefaultProfile { profile_id: None },
            )
            .expect_err("revision above JS safe integer must be rejected");
        assert_eq!(overflow_err.code(), "invalid_input");
    }

    #[test]
    fn reference_integrity_effort_validation_and_referenced_deletions_rejected() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = RegistryStore::open(temp.path().join("registry")).expect("open store");

        // Model referencing missing tool must fail.
        let err = store
            .apply(
                0,
                RegistryChange::UpsertModel {
                    model: sample_model("argon", "missing-tool", &["high"], false),
                },
            )
            .expect_err("missing tool reference");
        assert_eq!(err.code(), "invalid_reference");

        let s1 = store
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: sample_tool("agy", ToolAdapter::Antigravity),
                },
            )
            .expect("upsert tool");
        let s2 = store
            .apply(
                s1.revision,
                RegistryChange::UpsertModel {
                    model: sample_model("argon", "agy", &["medium", "high"], false),
                },
            )
            .expect("upsert model");

        // Unsupported effort must fail.
        let bad_effort_profile = WorkflowProfile {
            id: "p1".to_string(),
            name: "Profile 1".to_string(),
            assignments: vec![RoleAssignment {
                role: WorkflowRole::Coder,
                difficulty: Some(Difficulty::Hard),
                choice: ModelChoice {
                    model_id: "argon".to_string(),
                    effort: Some("extreme".to_string()),
                },
            }],
            decision_advisor: None,
        };
        let err = store
            .apply(
                s2.revision,
                RegistryChange::UpsertProfile {
                    profile: bad_effort_profile,
                },
            )
            .expect_err("unsupported effort");
        assert_eq!(err.code(), "unsupported_effort");

        // Missing effort when allow_no_effort is false must fail.
        let missing_effort_profile = WorkflowProfile {
            id: "p1".to_string(),
            name: "Profile 1".to_string(),
            assignments: vec![RoleAssignment {
                role: WorkflowRole::Coder,
                difficulty: None,
                choice: ModelChoice {
                    model_id: "argon".to_string(),
                    effort: None,
                },
            }],
            decision_advisor: None,
        };
        let err = store
            .apply(
                s2.revision,
                RegistryChange::UpsertProfile {
                    profile: missing_effort_profile,
                },
            )
            .expect_err("none effort disallowed");
        assert_eq!(err.code(), "unsupported_effort");

        // Duplicate (role, difficulty) in the same profile must fail.
        let duplicate_profile = WorkflowProfile {
            id: "p1".to_string(),
            name: "Profile 1".to_string(),
            assignments: vec![
                RoleAssignment {
                    role: WorkflowRole::Coder,
                    difficulty: Some(Difficulty::Hard),
                    choice: ModelChoice {
                        model_id: "argon".to_string(),
                        effort: Some("medium".to_string()),
                    },
                },
                RoleAssignment {
                    role: WorkflowRole::Coder,
                    difficulty: Some(Difficulty::Hard),
                    choice: ModelChoice {
                        model_id: "argon".to_string(),
                        effort: Some("high".to_string()),
                    },
                },
            ],
            decision_advisor: None,
        };
        let err = store
            .apply(
                s2.revision,
                RegistryChange::UpsertProfile {
                    profile: duplicate_profile,
                },
            )
            .expect_err("duplicate role/difficulty");
        assert_eq!(err.code(), "duplicate_assignment");

        // Save valid partial profile.
        let valid_profile = WorkflowProfile {
            id: "p1".to_string(),
            name: "Profile 1".to_string(),
            assignments: vec![RoleAssignment {
                role: WorkflowRole::Coder,
                difficulty: Some(Difficulty::Hard),
                choice: ModelChoice {
                    model_id: "argon".to_string(),
                    effort: Some("high".to_string()),
                },
            }],
            decision_advisor: None,
        };
        let s3 = store
            .apply(
                s2.revision,
                RegistryChange::UpsertProfile {
                    profile: valid_profile,
                },
            )
            .expect("save valid partial profile");

        // Deleting referenced tool or referenced model must be rejected.
        let err = store
            .apply(
                s3.revision,
                RegistryChange::DeleteTool {
                    tool_id: "agy".to_string(),
                },
            )
            .expect_err("referenced tool deletion must fail");
        assert_eq!(err.code(), "reference_conflict");

        let err = store
            .apply(
                s3.revision,
                RegistryChange::DeleteModel {
                    model_id: "argon".to_string(),
                },
            )
            .expect_err("referenced model deletion must fail");
        assert_eq!(err.code(), "reference_conflict");

        // Updating a model to drop an effort used by an existing profile must also be rejected.
        let err = store
            .apply(
                s3.revision,
                RegistryChange::UpsertModel {
                    model: sample_model("argon", "agy", &["medium"], false),
                },
            )
            .expect_err("removing in-use effort from model must fail");
        assert_eq!(err.code(), "unsupported_effort");

        // Setting nonexistent default profile must fail.
        let err = store
            .apply(
                s3.revision,
                RegistryChange::SetDefaultProfile {
                    profile_id: Some("does-not-exist".to_string()),
                },
            )
            .expect_err("nonexistent default profile");
        assert_eq!(err.code(), "invalid_reference");

        // Revision must remain unchanged at s3.revision after all rejected mutations.
        assert_eq!(store.snapshot().expect("snapshot").revision, s3.revision);
    }

    #[test]
    fn resolve_choice_uses_exact_difficulty_then_role_default_without_substitution() {
        let snap = RegistrySnapshot {
            schema_version: REGISTRY_SCHEMA_VERSION,
            revision: 1,
            tools: vec![
                sample_tool("agy", ToolAdapter::Antigravity),
                sample_tool("opencode", ToolAdapter::OpencodeV2),
            ],
            models: vec![
                sample_model("argon", "agy", &["medium", "high"], false),
                sample_model("deepseek-flash", "opencode", &["max"], true),
            ],
            profiles: vec![WorkflowProfile {
                id: "partial".to_string(),
                name: "Partial Profile".to_string(),
                assignments: vec![
                    RoleAssignment {
                        role: WorkflowRole::Coder,
                        difficulty: None,
                        choice: ModelChoice {
                            model_id: "deepseek-flash".to_string(),
                            effort: Some("max".to_string()),
                        },
                    },
                    RoleAssignment {
                        role: WorkflowRole::Coder,
                        difficulty: Some(Difficulty::Hard),
                        choice: ModelChoice {
                            model_id: "argon".to_string(),
                            effort: Some("high".to_string()),
                        },
                    },
                    RoleAssignment {
                        role: WorkflowRole::Reviewer,
                        difficulty: Some(Difficulty::Hard),
                        choice: ModelChoice {
                            model_id: "argon".to_string(),
                            effort: Some("high".to_string()),
                        },
                    },
                ],
                decision_advisor: None,
            }],
            default_profile_id: None,
        };

        // Exact difficulty match takes precedence over role default.
        let hard_coder = resolve_choice(
            &snap,
            "partial",
            WorkflowRole::Coder,
            Some(Difficulty::Hard),
        )
        .expect("hard coder");
        assert_eq!(
            hard_coder,
            ModelChoice {
                model_id: "argon".to_string(),
                effort: Some("high".to_string()),
            }
        );

        // Unconfigured difficulty falls back to explicit role default (difficulty: None).
        let easy_coder = resolve_choice(
            &snap,
            "partial",
            WorkflowRole::Coder,
            Some(Difficulty::Easy),
        )
        .expect("easy coder falls back to coder default");
        assert_eq!(
            easy_coder,
            ModelChoice {
                model_id: "deepseek-flash".to_string(),
                effort: Some("max".to_string()),
            }
        );

        // Requesting difficulty: None resolves the explicit role default.
        let default_coder =
            resolve_choice(&snap, "partial", WorkflowRole::Coder, None).expect("default coder");
        assert_eq!(default_coder, easy_coder);

        // Reviewer has Hard configured, but NO role default. Easy or None must fail explicitly.
        let hard_reviewer = resolve_choice(
            &snap,
            "partial",
            WorkflowRole::Reviewer,
            Some(Difficulty::Hard),
        )
        .expect("hard reviewer");
        assert_eq!(hard_reviewer.model_id, "argon");

        let easy_reviewer_err = resolve_choice(
            &snap,
            "partial",
            WorkflowRole::Reviewer,
            Some(Difficulty::Easy),
        )
        .expect_err("easy reviewer must not substitute hard reviewer");
        assert_eq!(easy_reviewer_err.code(), "missing_assignment");

        let none_reviewer_err = resolve_choice(&snap, "partial", WorkflowRole::Reviewer, None)
            .expect_err("none reviewer must not substitute hard reviewer");
        assert_eq!(none_reviewer_err.code(), "missing_assignment");

        // Unassigned role must fail explicitly.
        let architect_err = resolve_choice(
            &snap,
            "partial",
            WorkflowRole::Architect,
            Some(Difficulty::Hard),
        )
        .expect_err("unassigned role must fail");
        assert_eq!(architect_err.code(), "missing_assignment");

        // Unknown profile must fail with not_found.
        let unknown_profile_err =
            resolve_choice(&snap, "missing", WorkflowRole::Coder, None).expect_err("missing");
        assert_eq!(unknown_profile_err.code(), "not_found");
    }

    #[test]
    fn corrupt_snapshot_is_rejected_and_preserved_untouched() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config_dir = temp.path().join("registry");
        let store = RegistryStore::open(&config_dir).expect("open store");

        let corrupt_bytes = b"{\"schema_version\":1,\"revision\":0,corrupt!";
        fs::write(store.registry_path(), corrupt_bytes).expect("write corrupt file");

        let snap_err = store
            .snapshot()
            .expect_err("corrupt snapshot must fail read");
        assert_eq!(snap_err.code(), "corrupt_store");

        let apply_err = store
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: sample_tool("codex", ToolAdapter::CodexCli),
                },
            )
            .expect_err("apply on corrupt snapshot must fail without overwriting");
        assert_eq!(apply_err.code(), "corrupt_store");

        let preserved = fs::read(store.registry_path()).expect("read corrupt file back");
        assert_eq!(preserved, corrupt_bytes);
    }

    #[test]
    fn symlink_refusal_for_config_dir_snapshot_and_lock() {
        let temp = tempfile::tempdir().expect("tempdir");
        let real_dir = temp.path().join("real");
        fs::create_dir(&real_dir).expect("create real dir");
        let symlink_dir = temp.path().join("symlink-dir");
        symlink(&real_dir, &symlink_dir).expect("create dir symlink");

        let err = RegistryStore::open(&symlink_dir).expect_err("symlink config_dir must fail");
        assert_eq!(err.code(), "security_violation");

        let victim_file = temp.path().join("victim.json");
        fs::write(&victim_file, b"keep-me-safe").expect("write victim file");

        let store_dir = temp.path().join("store");
        let store = RegistryStore::open(&store_dir).expect("open valid store");
        symlink(&victim_file, store.registry_path()).expect("symlink registry.json");

        let snap_err = store
            .snapshot()
            .expect_err("symlink registry.json must be refused on snapshot");
        assert_eq!(snap_err.code(), "security_violation");

        let apply_err = store
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: sample_tool("codex", ToolAdapter::CodexCli),
                },
            )
            .expect_err("symlink registry.json must be refused on apply");
        assert_eq!(apply_err.code(), "security_violation");

        let reopen_err = RegistryStore::open(&store_dir)
            .expect_err("symlink registry.json must be refused on open");
        assert_eq!(reopen_err.code(), "security_violation");

        assert_eq!(
            fs::read(&victim_file).expect("read victim"),
            b"keep-me-safe"
        );

        // Replace registry.lock with a symlink and verify refusal.
        fs::remove_file(store.registry_path()).expect("remove symlink registry.json");
        fs::remove_file(store.lock_path()).expect("remove registry.lock");
        symlink(&victim_file, store.lock_path()).expect("symlink registry.lock");

        let lock_err = store
            .snapshot()
            .expect_err("symlink registry.lock must be refused");
        assert_eq!(lock_err.code(), "security_violation");
        assert_eq!(
            fs::read(&victim_file).expect("read victim"),
            b"keep-me-safe"
        );
    }

    #[test]
    fn collection_caps_and_string_bounds_enforced() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = RegistryStore::open(temp.path().join("registry")).expect("open store");

        let untrimmed_err = store
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: sample_tool(" bad-id ", ToolAdapter::Git),
                },
            )
            .expect_err("untrimmed id must fail");
        assert_eq!(untrimmed_err.code(), "invalid_input");

        let overlong_id = "a".repeat(MAX_ID_LEN + 1);
        let overlong_err = store
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: sample_tool(&overlong_id, ToolAdapter::Git),
                },
            )
            .expect_err("overlong id must fail");
        assert_eq!(overlong_err.code(), "invalid_input");

        let mut rev = 0;
        for idx in 0..MAX_TOOLS {
            let snap = store
                .apply(
                    rev,
                    RegistryChange::UpsertTool {
                        tool: sample_tool(&format!("tool-{idx}"), ToolAdapter::Git),
                    },
                )
                .expect("upsert within cap");
            rev = snap.revision;
        }

        let cap_err = store
            .apply(
                rev,
                RegistryChange::UpsertTool {
                    tool: sample_tool("tool-overflow", ToolAdapter::Git),
                },
            )
            .expect_err("65th tool must exceed cap");
        assert_eq!(cap_err.code(), "capacity_exceeded");
    }

    #[test]
    fn decision_advisor_wire_format_legacy_roundtrip_and_null_omission() {
        assert_eq!(DecisionAdvisorMode::ALL, [DecisionAdvisorMode::Shadow]);
        assert_eq!(DecisionAdvisorMode::Shadow.as_str(), "shadow");
        assert_eq!(DecisionAdvisorMode::Shadow.to_string(), "shadow");
        assert_eq!(
            "shadow".parse::<DecisionAdvisorMode>(),
            Ok(DecisionAdvisorMode::Shadow)
        );
        for unsupported in ["automatic", "assist", "active", "enforce", ""] {
            let err = unsupported
                .parse::<DecisionAdvisorMode>()
                .expect_err("unsupported mode must fail FromStr");
            assert_eq!(err.code(), "invalid_input");
            let mode_json = format!("\"{unsupported}\"");
            assert!(
                serde_json::from_str::<DecisionAdvisorMode>(&mode_json).is_err(),
                "serde must reject unsupported mode {unsupported}"
            );
        }

        let encoded_mode =
            serde_json::to_string(&DecisionAdvisorMode::Shadow).expect("serialize shadow mode");
        assert_eq!(encoded_mode, "\"shadow\"");
        let decoded_mode: DecisionAdvisorMode =
            serde_json::from_str(&encoded_mode).expect("deserialize shadow mode");
        assert_eq!(decoded_mode, DecisionAdvisorMode::Shadow);

        // Legacy JSON without `decision_advisor` must deserialize to `None` and re-serialize
        // with `decision_advisor` omitted (byte-shape compatible with old profiles).
        let legacy_profile_json = r#"{"id":"legacy","name":"Legacy Profile","assignments":[{"role":"coder","difficulty":"hard","choice":{"model_id":"argon","effort":"high"}}]}"#;
        let decoded_legacy: WorkflowProfile =
            serde_json::from_str(legacy_profile_json).expect("deserialize legacy profile");
        assert_eq!(decoded_legacy.decision_advisor, None);
        let reencoded_legacy =
            serde_json::to_string(&decoded_legacy).expect("reserialize legacy profile");
        assert_eq!(reencoded_legacy, legacy_profile_json);
        let legacy_val = serde_json::to_value(&decoded_legacy).expect("to_value legacy");
        assert!(legacy_val.get("decision_advisor").is_none());

        // Explicit `"decision_advisor": null` must deserialize to `None` and be omitted on
        // serialization.
        let explicit_null_json = r#"{"id":"legacy","name":"Legacy Profile","assignments":[{"role":"coder","difficulty":"hard","choice":{"model_id":"argon","effort":"high"}}],"decision_advisor":null}"#;
        let decoded_null: WorkflowProfile =
            serde_json::from_str(explicit_null_json).expect("deserialize explicit null advisor");
        assert_eq!(decoded_null, decoded_legacy);
        assert_eq!(decoded_null.decision_advisor, None);
        let reencoded_null =
            serde_json::to_string(&decoded_null).expect("reserialize explicit null advisor");
        assert_eq!(reencoded_null, legacy_profile_json);

        // Configured `Some(DecisionAdvisor)` roundtrips cleanly in shadow mode.
        let with_advisor = WorkflowProfile {
            id: "with-jev".to_string(),
            name: "With Jev".to_string(),
            assignments: Vec::new(),
            decision_advisor: Some(DecisionAdvisor {
                model_id: "jev-system-one".to_string(),
                mode: DecisionAdvisorMode::Shadow,
            }),
        };
        let advisor_val = serde_json::to_value(&with_advisor).expect("serialize with_advisor");
        assert_eq!(
            advisor_val["decision_advisor"],
            serde_json::json!({
                "model_id": "jev-system-one",
                "mode": "shadow"
            })
        );
        let roundtrip: WorkflowProfile =
            serde_json::from_value(advisor_val).expect("deserialize with_advisor");
        assert_eq!(roundtrip, with_advisor);

        // Unknown and duplicate fields on `DecisionAdvisor` and `WorkflowProfile` must be rejected.
        for bad_json in [
            r#"{"model_id":"jev-system-one","mode":"shadow","effort":null}"#,
            r#"{"model_id":"jev-system-one","mode":"shadow","effort":"high"}"#,
            r#"{"model_id":"jev-system-one","mode":"shadow","extra":true}"#,
            r#"{"model_id":"jev-system-one","mode":"automatic"}"#,
            r#"{"model_id":"jev-system-one","mode":"assist"}"#,
            r#"{"model_id":"a","model_id":"b","mode":"shadow"}"#,
            r#"{"model_id":"a","mode":"shadow","mode":"shadow"}"#,
        ] {
            assert!(
                serde_json::from_str::<DecisionAdvisor>(bad_json).is_err(),
                "DecisionAdvisor must reject malformed JSON: {bad_json}"
            );
        }

        for bad_profile_json in [
            r#"{"id":"p1","name":"P1","assignments":[],"decision_advisor":null,"decision_advisor":null}"#,
            r#"{"id":"p1","name":"P1","assignments":[],"unknown_field":123}"#,
            r#"{"id":"p1","name":"P1","assignments":[],"decision_advisor":{"model_id":"m1","mode":"assist"}}"#,
            r#"{"id":"p1","name":"P1","assignments":[],"decision_advisor":{"model_id":"m1","mode":"shadow","effort":"low"}}"#,
        ] {
            assert!(
                serde_json::from_str::<WorkflowProfile>(bad_profile_json).is_err(),
                "WorkflowProfile must reject malformed JSON: {bad_profile_json}"
            );
        }

        // On-disk legacy snapshot without `decision_advisor` loads and re-saves without adding the key.
        let temp = tempfile::tempdir().expect("tempdir");
        let config_dir = temp.path().join("registry");
        let store = RegistryStore::open(&config_dir).expect("open store");
        let legacy_snapshot_json = r#"{
  "schema_version": 1,
  "revision": 1,
  "tools": [
    {
      "id": "agy",
      "label": "Tool agy",
      "adapter": "antigravity",
      "executable_path": "/usr/local/bin/agy",
      "enabled": true
    }
  ],
  "models": [
    {
      "id": "argon",
      "tool_id": "agy",
      "label": "Model argon",
      "model_arg": "argon",
      "efforts": [
        "high"
      ],
      "allow_no_effort": false
    }
  ],
  "profiles": [
    {
      "id": "legacy",
      "name": "Legacy Profile",
      "assignments": [
        {
          "role": "coder",
          "difficulty": "hard",
          "choice": {
            "model_id": "argon",
            "effort": "high"
          }
        }
      ]
    }
  ],
  "default_profile_id": "legacy"
}
"#;
        fs::write(store.registry_path(), legacy_snapshot_json).expect("write legacy registry.json");
        let loaded = store.snapshot().expect("load legacy snapshot");
        assert_eq!(loaded.profiles.len(), 1);
        assert_eq!(loaded.profiles[0].decision_advisor, None);
        assert_eq!(
            store.snapshot_readonly().expect("readonly snapshot"),
            loaded
        );
        let readonly_at =
            RegistryStore::read_snapshot_readonly_at(&config_dir).expect("readonly_at snapshot");
        assert_eq!(readonly_at, loaded);

        let updated = store
            .apply(
                1,
                RegistryChange::UpsertProfile {
                    profile: loaded.profiles[0].clone(),
                },
            )
            .expect("resave legacy profile with None advisor");
        assert_eq!(updated.revision, 2);
        let saved_text =
            fs::read_to_string(store.registry_path()).expect("read saved registry.json");
        assert!(
            !saved_text.contains("decision_advisor"),
            "None decision_advisor must stay omitted on disk"
        );
    }

    #[test]
    fn decision_advisor_validation_valid_jev_and_openai_and_invalid_selections_refused() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = RegistryStore::open(temp.path().join("registry")).expect("open store");

        let mut disabled_jev = sample_tool("jev-disabled", ToolAdapter::JevApi);
        disabled_jev.enabled = false;
        let mut disabled_openai = sample_tool("openai-disabled", ToolAdapter::OpenaiDecisionsApi);
        disabled_openai.enabled = false;

        let mut rev = 0;
        for tool in [
            sample_tool("jev", ToolAdapter::JevApi),
            sample_tool("openai-decisions", ToolAdapter::OpenaiDecisionsApi),
            disabled_jev,
            disabled_openai,
            sample_tool("codex", ToolAdapter::CodexCli),
            sample_tool("agy", ToolAdapter::Antigravity),
            sample_tool("opencode", ToolAdapter::OpencodeV2),
            sample_tool("git", ToolAdapter::Git),
        ] {
            rev = store
                .apply(rev, RegistryChange::UpsertTool { tool })
                .expect("upsert tool")
                .revision;
        }

        let s_models = store
            .apply(
                rev,
                RegistryChange::UpsertModels {
                    models: vec![
                        sample_model("jev-system-one", "jev", &[], true),
                        sample_model("jev-optional-effort", "jev", &["low"], true),
                        sample_model("jev-effort-required", "jev", &["high"], false),
                        sample_model("openai-decision-v1", "openai-decisions", &[], true),
                        sample_model(
                            "openai-effort-required",
                            "openai-decisions",
                            &["high"],
                            false,
                        ),
                        sample_model("jev-disabled-model", "jev-disabled", &[], true),
                        sample_model("openai-disabled-model", "openai-disabled", &[], true),
                        sample_model("codex-luna", "codex", &["max"], true),
                        sample_model("agy-argon", "agy", &["high"], true),
                        sample_model("opencode-deepseek", "opencode", &["max"], true),
                        sample_model("git-dummy", "git", &[], true),
                    ],
                },
            )
            .expect("upsert models");
        rev = s_models.revision;

        // Valid Jev advisor on a partial profile (empty assignments) succeeds.
        let jev_profile = WorkflowProfile {
            id: "jev-profile".to_string(),
            name: "Jev Profile".to_string(),
            assignments: Vec::new(),
            decision_advisor: Some(DecisionAdvisor {
                model_id: "jev-system-one".to_string(),
                mode: DecisionAdvisorMode::Shadow,
            }),
        };
        let s_jev = store
            .apply(
                rev,
                RegistryChange::UpsertProfile {
                    profile: jev_profile.clone(),
                },
            )
            .expect("upsert valid Jev advisor profile");
        assert_eq!(s_jev.profiles[0], jev_profile);
        rev = s_jev.revision;

        // Valid Jev advisor with optional efforts but allow_no_effort: true also succeeds.
        let s_jev_opt = store
            .apply(
                rev,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        decision_advisor: Some(DecisionAdvisor {
                            model_id: "jev-optional-effort".to_string(),
                            mode: DecisionAdvisorMode::Shadow,
                        }),
                        ..jev_profile.clone()
                    },
                },
            )
            .expect("upsert Jev advisor with allow_no_effort: true and optional efforts");
        rev = s_jev_opt.revision;

        // Valid OpenAI Decisions advisor succeeds as documented extension configuration.
        let openai_profile = WorkflowProfile {
            id: "openai-profile".to_string(),
            name: "OpenAI Decisions Profile".to_string(),
            assignments: vec![RoleAssignment {
                role: WorkflowRole::Coder,
                difficulty: Some(Difficulty::Hard),
                choice: ModelChoice {
                    model_id: "agy-argon".to_string(),
                    effort: Some("high".to_string()),
                },
            }],
            decision_advisor: Some(DecisionAdvisor {
                model_id: "openai-decision-v1".to_string(),
                mode: DecisionAdvisorMode::Shadow,
            }),
        };
        let s_openai = store
            .apply(
                rev,
                RegistryChange::UpsertProfile {
                    profile: openai_profile.clone(),
                },
            )
            .expect("upsert valid OpenAI Decisions advisor profile");
        assert_eq!(s_openai.profiles.len(), 2);
        rev = s_openai.revision;

        // Empty or untrimmed advisor model_id must fail with invalid_input.
        for bad_id in ["", " jev-system-one ", "jev\n"] {
            let err = store
                .apply(
                    rev,
                    RegistryChange::UpsertProfile {
                        profile: WorkflowProfile {
                            id: "bad-advisor".to_string(),
                            name: "Bad Advisor".to_string(),
                            assignments: Vec::new(),
                            decision_advisor: Some(DecisionAdvisor {
                                model_id: bad_id.to_string(),
                                mode: DecisionAdvisorMode::Shadow,
                            }),
                        },
                    },
                )
                .expect_err("malformed advisor model_id must fail");
            assert_eq!(err.code(), "invalid_input");
        }

        // Missing model, CLI/wrong-tool models, and disabled-tool models must fail with invalid_reference.
        for rejected_model_id in [
            "missing-model",
            "codex-luna",
            "agy-argon",
            "opencode-deepseek",
            "git-dummy",
            "jev-disabled-model",
            "openai-disabled-model",
        ] {
            let err = store
                .apply(
                    rev,
                    RegistryChange::UpsertProfile {
                        profile: WorkflowProfile {
                            id: "bad-advisor".to_string(),
                            name: "Bad Advisor".to_string(),
                            assignments: Vec::new(),
                            decision_advisor: Some(DecisionAdvisor {
                                model_id: rejected_model_id.to_string(),
                                mode: DecisionAdvisorMode::Shadow,
                            }),
                        },
                    },
                )
                .expect_err("invalid advisor reference must be refused");
            assert_eq!(
                err.code(),
                "invalid_reference",
                "expected invalid_reference for {rejected_model_id}"
            );
        }

        // Effort-required models (allow_no_effort: false) must fail with unsupported_effort.
        for effort_required_id in ["jev-effort-required", "openai-effort-required"] {
            let err = store
                .apply(
                    rev,
                    RegistryChange::UpsertProfile {
                        profile: WorkflowProfile {
                            id: "bad-advisor".to_string(),
                            name: "Bad Advisor".to_string(),
                            assignments: Vec::new(),
                            decision_advisor: Some(DecisionAdvisor {
                                model_id: effort_required_id.to_string(),
                                mode: DecisionAdvisorMode::Shadow,
                            }),
                        },
                    },
                )
                .expect_err("effort-required advisor model must be refused");
            assert_eq!(
                err.code(),
                "unsupported_effort",
                "expected unsupported_effort for {effort_required_id}"
            );
        }

        // Revision and stored profiles must remain unchanged after all rejected mutations.
        let snap = store.snapshot().expect("snapshot after refusals");
        assert_eq!(snap.revision, rev);
        assert_eq!(snap.profiles.len(), 2);
    }

    #[test]
    fn decision_advisor_reference_protection_cas_and_readonly_snapshot() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config_dir = temp.path().join("registry");
        let store = RegistryStore::open(&config_dir).expect("open store");

        let mut disabled_jev = sample_tool("jev-disabled", ToolAdapter::JevApi);
        disabled_jev.enabled = false;

        let s1 = store
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: sample_tool("jev", ToolAdapter::JevApi),
                },
            )
            .expect("upsert jev");
        let s2 = store
            .apply(
                s1.revision,
                RegistryChange::UpsertTool {
                    tool: sample_tool("codex", ToolAdapter::CodexCli),
                },
            )
            .expect("upsert codex");
        let s3 = store
            .apply(
                s2.revision,
                RegistryChange::UpsertTool { tool: disabled_jev },
            )
            .expect("upsert jev-disabled");
        let s4 = store
            .apply(
                s3.revision,
                RegistryChange::UpsertModel {
                    model: sample_model("jev-system-one", "jev", &[], true),
                },
            )
            .expect("upsert jev model");

        let profile = WorkflowProfile {
            id: "p1".to_string(),
            name: "Advisor Profile".to_string(),
            assignments: Vec::new(),
            decision_advisor: Some(DecisionAdvisor {
                model_id: "jev-system-one".to_string(),
                mode: DecisionAdvisorMode::Shadow,
            }),
        };
        let s5 = store
            .apply(
                s4.revision,
                RegistryChange::UpsertProfile {
                    profile: profile.clone(),
                },
            )
            .expect("upsert profile with advisor");

        // Stale CAS revision when updating advisor profile must be rejected.
        let stale_err = store
            .apply(
                s4.revision,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        decision_advisor: None,
                        ..profile.clone()
                    },
                },
            )
            .expect_err("stale revision must fail");
        assert_eq!(
            stale_err,
            RegistryError::StaleRevision {
                expected: s4.revision,
                actual: s5.revision,
            }
        );
        assert_eq!(stale_err.code(), "stale_revision");

        // Deleting model referenced only by decision_advisor must fail with reference_conflict.
        let del_model_err = store
            .apply(
                s5.revision,
                RegistryChange::DeleteModel {
                    model_id: "jev-system-one".to_string(),
                },
            )
            .expect_err("deleting advisor model must conflict");
        assert_eq!(del_model_err.code(), "reference_conflict");

        // Deleting tool owning that model must also fail with reference_conflict.
        let del_tool_err = store
            .apply(
                s5.revision,
                RegistryChange::DeleteTool {
                    tool_id: "jev".to_string(),
                },
            )
            .expect_err("deleting advisor tool must conflict");
        assert_eq!(del_tool_err.code(), "reference_conflict");

        // Disabling the referenced decision API tool or changing its adapter to a CLI adapter
        // must be refused without silently clearing or substituting the profile's advisor.
        let mut disabled_tool_edit = sample_tool("jev", ToolAdapter::JevApi);
        disabled_tool_edit.enabled = false;
        let disable_err = store
            .apply(
                s5.revision,
                RegistryChange::UpsertTool {
                    tool: disabled_tool_edit,
                },
            )
            .expect_err("disabling tool referenced by decision_advisor must fail");
        assert_eq!(disable_err.code(), "invalid_reference");

        let adapter_err = store
            .apply(
                s5.revision,
                RegistryChange::UpsertTool {
                    tool: sample_tool("jev", ToolAdapter::CodexCli),
                },
            )
            .expect_err("changing referenced advisor tool adapter to CLI must fail");
        assert_eq!(adapter_err.code(), "invalid_reference");

        // Editing the referenced model to require effort or move to a CLI/disabled tool must fail.
        let effort_err = store
            .apply(
                s5.revision,
                RegistryChange::UpsertModel {
                    model: sample_model("jev-system-one", "jev", &["high"], false),
                },
            )
            .expect_err("making referenced advisor model require effort must fail");
        assert_eq!(effort_err.code(), "unsupported_effort");

        let batch_effort_err = store
            .apply(
                s5.revision,
                RegistryChange::UpsertModels {
                    models: vec![sample_model("jev-system-one", "jev", &["high"], false)],
                },
            )
            .expect_err("batch making referenced advisor model require effort must fail");
        assert_eq!(batch_effort_err.code(), "unsupported_effort");

        let move_cli_err = store
            .apply(
                s5.revision,
                RegistryChange::UpsertModel {
                    model: sample_model("jev-system-one", "codex", &[], true),
                },
            )
            .expect_err("moving referenced advisor model to CLI tool must fail");
        assert_eq!(move_cli_err.code(), "invalid_reference");

        let move_disabled_err = store
            .apply(
                s5.revision,
                RegistryChange::UpsertModel {
                    model: sample_model("jev-system-one", "jev-disabled", &[], true),
                },
            )
            .expect_err("moving referenced advisor model to disabled tool must fail");
        assert_eq!(move_disabled_err.code(), "invalid_reference");

        // Benign edits to the enabled tool and referenced model (retaining enabled API adapter
        // and allow_no_effort: true) must succeed and preserve the profile's decision_advisor.
        let mut renamed_tool = sample_tool("jev", ToolAdapter::JevApi);
        renamed_tool.label = "Jev System One API".to_string();
        let s6 = store
            .apply(
                s5.revision,
                RegistryChange::UpsertTool { tool: renamed_tool },
            )
            .expect("rename enabled advisor tool");

        let mut updated_model = sample_model("jev-system-one", "jev", &["low"], true);
        updated_model.label = "Jev System One v2".to_string();
        updated_model.model_arg = "jev-system-one-v2".to_string();
        let s7 = store
            .apply(
                s6.revision,
                RegistryChange::UpsertModel {
                    model: updated_model.clone(),
                },
            )
            .expect("update enabled advisor model while preserving invariants");
        assert_eq!(
            s7.profiles[0].decision_advisor,
            Some(DecisionAdvisor {
                model_id: "jev-system-one".to_string(),
                mode: DecisionAdvisorMode::Shadow,
            })
        );

        // Read-only snapshot routes must work without creating or touching `registry.lock`
        // and no extra credential files are written.
        fs::remove_file(store.lock_path()).expect("remove registry.lock");
        assert!(!store.lock_path().exists());
        let ro_snap = store.snapshot_readonly().expect("snapshot_readonly");
        assert_eq!(ro_snap, s7);
        assert!(!store.lock_path().exists());
        let ro_at_snap = RegistryStore::read_snapshot_readonly_at(&config_dir)
            .expect("read_snapshot_readonly_at");
        assert_eq!(ro_at_snap, s7);
        assert!(!store.lock_path().exists());

        let entries: Vec<_> = fs::read_dir(&config_dir)
            .expect("read_dir")
            .map(|entry| entry.expect("dir entry").file_name())
            .collect();
        assert_eq!(entries, vec![std::ffi::OsString::from(REGISTRY_FILE_NAME)]);

        // Clearing `decision_advisor` to `None` permits deleting the advisor model and tool.
        let s8 = store
            .apply(
                s7.revision,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        decision_advisor: None,
                        ..profile
                    },
                },
            )
            .expect("clear decision_advisor");
        assert_eq!(s8.profiles[0].decision_advisor, None);
        let saved_json = fs::read_to_string(store.registry_path()).expect("read registry.json");
        assert!(!saved_json.contains("decision_advisor"));

        let s9 = store
            .apply(
                s8.revision,
                RegistryChange::DeleteModel {
                    model_id: "jev-system-one".to_string(),
                },
            )
            .expect("delete advisor model after clearing profile advisor");
        assert!(s9.models.is_empty());

        let s10 = store
            .apply(
                s9.revision,
                RegistryChange::DeleteTool {
                    tool_id: "jev".to_string(),
                },
            )
            .expect("delete jev tool after deleting model");
        assert!(!s10.tools.iter().any(|tool| tool.id == "jev"));

        // Corrupt on-disk snapshot with invalid advisor reference must fail closed as corrupt_store.
        let mut tampered_value = serde_json::to_value(&s10).expect("serialize s10");
        tampered_value["profiles"][0]["decision_advisor"] = serde_json::json!({
            "model_id": "missing-advisor-model",
            "mode": "shadow"
        });
        fs::write(
            store.registry_path(),
            serde_json::to_vec_pretty(&tampered_value).expect("encode tampered"),
        )
        .expect("write tampered registry.json");

        assert_eq!(
            store.snapshot().expect_err("tampered snapshot").code(),
            "corrupt_store"
        );
        assert_eq!(
            store
                .snapshot_readonly()
                .expect_err("tampered snapshot_readonly")
                .code(),
            "corrupt_store"
        );
        assert_eq!(
            RegistryStore::read_snapshot_readonly_at(&config_dir)
                .expect_err("tampered read_snapshot_readonly_at")
                .code(),
            "corrupt_store"
        );
    }

    #[test]
    fn execution_roles_exclude_legacy_follower_while_preserving_legacy_snapshot_decode() {
        assert_eq!(
            WorkflowRole::EXECUTION_ROLES,
            [
                WorkflowRole::Orchestrator,
                WorkflowRole::Architect,
                WorkflowRole::Coder,
                WorkflowRole::Reviewer,
                WorkflowRole::Closure,
            ]
        );
        assert_eq!(WorkflowRole::EXECUTION_ROLES.len(), 5);
        assert_eq!(WorkflowRole::ALL.len(), 6);
        assert!(!WorkflowRole::Follower.is_execution_role());
        assert!(WorkflowRole::Follower.is_legacy_follower());
        for role in WorkflowRole::EXECUTION_ROLES {
            assert!(role.is_execution_role());
            assert!(!role.is_legacy_follower());
        }

        let temp = tempfile::tempdir().expect("tempdir");
        let store = RegistryStore::open(temp.path().join("registry")).expect("open store");
        let s1 = store
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: sample_tool("agy", ToolAdapter::Antigravity),
                },
            )
            .expect("upsert tool");
        let s2 = store
            .apply(
                s1.revision,
                RegistryChange::UpsertModel {
                    model: sample_model("argon", "agy", &["high"], false),
                },
            )
            .expect("upsert model");

        // 1. Five-role execution profile without Follower is complete and saves cleanly.
        let five_role_assignments: Vec<RoleAssignment> = WorkflowRole::EXECUTION_ROLES
            .into_iter()
            .map(|role| RoleAssignment {
                role,
                difficulty: None,
                choice: ModelChoice {
                    model_id: "argon".to_string(),
                    effort: Some("high".to_string()),
                },
            })
            .collect();
        let five_role_profile = WorkflowProfile {
            id: "five-role-default".to_string(),
            name: "Five Role Default".to_string(),
            assignments: five_role_assignments,
            decision_advisor: None,
        };
        assert!(five_role_profile.has_complete_execution_defaults());
        assert_eq!(five_role_profile.execution_assignments().count(), 5);

        let s3 = store
            .apply(
                s2.revision,
                RegistryChange::UpsertProfile {
                    profile: five_role_profile.clone(),
                },
            )
            .expect("save five-role profile");
        assert_eq!(s3.profiles[0], five_role_profile);
        let serialized_s3 = serde_json::to_string(&s3).expect("serialize s3");
        assert!(!serialized_s3.contains("\"follower\""));

        // 2. Legacy snapshot containing a `follower` role assignment still decodes and validates.
        let mut legacy_assignments = five_role_profile.assignments.clone();
        legacy_assignments.insert(
            0,
            RoleAssignment {
                role: WorkflowRole::Follower,
                difficulty: None,
                choice: ModelChoice {
                    model_id: "argon".to_string(),
                    effort: Some("high".to_string()),
                },
            },
        );
        let legacy_profile = WorkflowProfile {
            id: "legacy-six-role".to_string(),
            name: "Legacy Six Role".to_string(),
            assignments: legacy_assignments,
            decision_advisor: None,
        };
        assert!(legacy_profile.has_complete_execution_defaults());
        assert_eq!(legacy_profile.assignments.len(), 6);
        assert_eq!(legacy_profile.execution_assignments().count(), 5);

        let s4 = store
            .apply(
                s3.revision,
                RegistryChange::UpsertProfile {
                    profile: legacy_profile.clone(),
                },
            )
            .expect("save legacy profile with follower");
        let reloaded = store
            .snapshot()
            .expect("reload snapshot with legacy follower");
        assert_eq!(reloaded, s4);
    }
}
