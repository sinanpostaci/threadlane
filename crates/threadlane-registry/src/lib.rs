#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! `threadlane-registry`: persistent global tool, model, and workflow profile registry
//! for Threadlane Milestone 4.
//!
//! This crate is independent of Tauri, runtime SQLite state, and external provider CLIs.
//! It provides:
//! - Canonical `snake_case` wire types ([`ToolAdapter`], [`ToolDefinition`], [`ModelDefinition`],
//!   [`ModelChoice`], [`WorkflowRole`], [`Difficulty`], [`RoleAssignment`], [`WorkflowProfile`],
//!   [`RegistrySnapshot`], and [`RegistryChange`]).
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
    Difficulty, ModelChoice, ModelDefinition, RegistryChange, RegistrySnapshot, RoleAssignment,
    ToolAdapter, ToolDefinition, WorkflowProfile, WorkflowRole, MAX_ASSIGNMENTS_PER_PROFILE,
    MAX_EFFORTS_PER_MODEL, MAX_EFFORT_LEN, MAX_EXECUTABLE_PATH_LEN, MAX_ID_LEN,
    MAX_JS_SAFE_INTEGER, MAX_LABEL_LEN, MAX_MODELS, MAX_MODEL_ARG_LEN, MAX_PROFILES,
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
}
