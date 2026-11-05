use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};

use tempfile::TempDir;
use tokio::sync::watch;

use threadlane_workers::{
    build_antigravity_read_only_settings, build_antigravity_read_only_settings_json,
    build_antigravity_task_files_settings_json, build_opencode_config_content,
    evaluate_antigravity_settings, resolve_model_target, run_advisory_worker,
    run_advisory_worker_with_cancel, validate_advisory_provider_and_mode,
    validate_advisory_read_only_workdir, AdvisoryWorkerSpec, AntigravitySessionSettings,
    CleanupRefusalReason, ExecutionBudgets, OpenCodeConfigContent, OpenCodeModelObject,
    OpenCodeRuleEffect, WorkerApiError, WorkerError, WorkerErrorCode, WorkerFilePermissions,
    WorkerProvider, WorkerRunStatus, WorkerSpec, AGY_DENIED_EXECUTION_AND_DELEGATION_TOOLS,
    AGY_PROTECTED_READ_DENY_PATTERNS, AGY_READ_PERMISSION_TOOL, AGY_WRITE_PERMISSION_TOOLS,
    OPENCODE_CONFIG_ENV_VAR, OPENCODE_PINNED_AGENTS,
};

fn write_executable_script(dir: &Path, name: &str, script_body: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, script_body).expect("write executable script");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755))
        .expect("chmod 0755 executable script");
    path
}

#[test]
fn worker_file_permissions_serde_and_legacy_defaults_remain_compatible() {
    assert_eq!(
        WorkerFilePermissions::default(),
        WorkerFilePermissions::Inherit
    );
    assert_eq!(WorkerFilePermissions::Inherit.as_str(), "inherit");
    assert_eq!(WorkerFilePermissions::TaskFiles.as_str(), "task_files");
    assert_eq!(WorkerFilePermissions::ReadOnly.as_str(), "read_only");

    assert!(!WorkerFilePermissions::Inherit.is_read_only());
    assert!(!WorkerFilePermissions::TaskFiles.is_read_only());
    assert!(WorkerFilePermissions::ReadOnly.is_read_only());

    assert!(!WorkerFilePermissions::Inherit.enforces_preventive_read_only());
    assert!(!WorkerFilePermissions::TaskFiles.enforces_preventive_read_only());
    assert!(WorkerFilePermissions::ReadOnly.enforces_preventive_read_only());

    assert!(!WorkerFilePermissions::Inherit.requires_workdir_argv());
    assert!(WorkerFilePermissions::TaskFiles.requires_workdir_argv());
    assert!(WorkerFilePermissions::ReadOnly.requires_workdir_argv());

    for (mode, expected_json) in [
        (WorkerFilePermissions::Inherit, "\"inherit\""),
        (WorkerFilePermissions::TaskFiles, "\"task_files\""),
        (WorkerFilePermissions::ReadOnly, "\"read_only\""),
    ] {
        let serialized = serde_json::to_string(&mode).expect("serialize WorkerFilePermissions");
        assert_eq!(serialized, expected_json);
        let roundtrip: WorkerFilePermissions =
            serde_json::from_str(&serialized).expect("deserialize WorkerFilePermissions");
        assert_eq!(roundtrip, mode);
    }
}

#[test]
fn unsupported_advisory_providers_and_non_read_only_modes_fail_closed() {
    assert_eq!(
        validate_advisory_provider_and_mode("antigravity", WorkerFilePermissions::ReadOnly)
            .expect("antigravity read_only supported"),
        WorkerProvider::Antigravity
    );
    assert_eq!(
        validate_advisory_provider_and_mode("opencode_v2", WorkerFilePermissions::ReadOnly)
            .expect("opencode_v2 read_only supported"),
        WorkerProvider::OpenCodeV2
    );
    assert_eq!(
        validate_advisory_provider_and_mode("codex_cli", WorkerFilePermissions::ReadOnly)
            .expect("codex_cli read_only supported"),
        WorkerProvider::CodexCli
    );

    for unsupported_provider in [
        "jev_api",
        "openai_decisions_api",
        "git",
        "tmux",
        "codegraph",
        "unknown_provider",
        "",
    ] {
        let err = validate_advisory_provider_and_mode(
            unsupported_provider,
            WorkerFilePermissions::ReadOnly,
        )
        .expect_err("unsupported provider must fail closed");
        assert_eq!(err.code(), WorkerErrorCode::UnsupportedCapability);
        assert!(matches!(err, WorkerError::UnsupportedCapability { .. }));
    }

    for provider in [
        WorkerProvider::Antigravity,
        WorkerProvider::OpenCodeV2,
        WorkerProvider::CodexCli,
    ] {
        for non_advisory_mode in [
            WorkerFilePermissions::Inherit,
            WorkerFilePermissions::TaskFiles,
        ] {
            let err = non_advisory_mode
                .ensure_advisory_read_only_capability(provider)
                .expect_err("non-read_only mode must fail closed for advisory worker");
            assert_eq!(err.code(), WorkerErrorCode::UnsupportedCapability);

            let mode_err =
                validate_advisory_provider_and_mode(provider.as_str(), non_advisory_mode)
                    .expect_err(
                        "validate_advisory_provider_and_mode must reject non-read_only mode",
                    );
            assert_eq!(mode_err.code(), WorkerErrorCode::UnsupportedCapability);
            let api_err = mode_err.to_api_error();
            assert_eq!(api_err.code, WorkerErrorCode::UnsupportedCapability);
            assert_eq!(api_err.cleanup_refusal_reason, None);
            assert_eq!(api_err.message, mode_err.to_string());
        }
    }

    // Verify stable machine error code mapping, display/serde, and legacy `to_api_error()` compatibility
    let unsupported_err = WorkerError::UnsupportedCapability {
        provider: "codex_cli".to_string(),
        capability: "read_only_advisory_worker".to_string(),
        reason: "preventive read_only policy is unavailable".to_string(),
    };
    assert_eq!(
        unsupported_err.code(),
        WorkerErrorCode::UnsupportedCapability
    );
    assert_eq!(unsupported_err.code().as_str(), "unsupported_capability");
    assert_eq!(unsupported_err.code().to_string(), "unsupported_capability");
    assert_eq!(
        serde_json::to_string(&unsupported_err.code()).expect("serialize error code"),
        "\"unsupported_capability\""
    );
    let roundtrip_code: WorkerErrorCode =
        serde_json::from_str("\"unsupported_capability\"").expect("deserialize error code");
    assert_eq!(roundtrip_code, WorkerErrorCode::UnsupportedCapability);
    assert_eq!(
        unsupported_err.to_string(),
        "unsupported capability `read_only_advisory_worker` for provider `codex_cli`: preventive read_only policy is unavailable"
    );
    assert_eq!(
        unsupported_err.to_api_error(),
        WorkerApiError {
            code: WorkerErrorCode::UnsupportedCapability,
            message: unsupported_err.to_string(),
            cleanup_refusal_reason: None,
        }
    );

    let cleanup_err = WorkerError::CleanupRefused {
        worktree_id: "wt-1".to_string(),
        reason: CleanupRefusalReason::DirtyWorktree,
        detail: "uncommitted changes".to_string(),
    };
    assert_eq!(cleanup_err.code(), WorkerErrorCode::CleanupRefused);
    assert_eq!(cleanup_err.code().as_str(), "cleanup_refused");
    assert_eq!(
        cleanup_err.to_api_error(),
        WorkerApiError {
            code: WorkerErrorCode::CleanupRefused,
            message: cleanup_err.to_string(),
            cleanup_refusal_reason: Some(CleanupRefusalReason::DirtyWorktree),
        }
    );
}

#[test]
fn read_only_requires_zero_write_scopes_while_write_modes_require_non_empty_scopes() {
    let empty_scopes = BTreeSet::new();
    let non_empty_scopes = BTreeSet::from(["src/lib.rs".to_string()]);

    for provider in [
        WorkerProvider::Antigravity,
        WorkerProvider::OpenCodeV2,
        WorkerProvider::CodexCli,
    ] {
        WorkerFilePermissions::ReadOnly
            .validate_for_provider_and_paths(provider, &empty_scopes)
            .expect("read_only must accept zero write scopes");

        let non_empty_err = WorkerFilePermissions::ReadOnly
            .validate_for_provider_and_paths(provider, &non_empty_scopes)
            .expect_err("read_only must reject non-empty write scopes");
        assert_eq!(non_empty_err.code(), WorkerErrorCode::InvalidSpec);
        assert!(non_empty_err
            .to_string()
            .contains("allowed_write_paths must be empty when `file_permissions` is `read_only`"));
    }

    for provider in [WorkerProvider::Antigravity, WorkerProvider::OpenCodeV2] {
        for write_mode in [
            WorkerFilePermissions::Inherit,
            WorkerFilePermissions::TaskFiles,
        ] {
            let empty_err = write_mode
                .validate_for_provider_and_paths(provider, &empty_scopes)
                .expect_err("write modes must still reject empty allowed_write_paths");
            assert_eq!(empty_err.code(), WorkerErrorCode::InvalidSpec);
            assert!(empty_err.to_string().contains(
                "allowed_write_paths must contain at least one repository-relative path"
            ));

            write_mode
                .validate_for_provider_and_paths(provider, &non_empty_scopes)
                .expect("write modes must accept valid repository-relative path");
        }
    }
}

#[test]
fn antigravity_read_only_settings_enforce_scoped_read_precedence_and_default_denial() {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("advisory-repo");
    fs::create_dir_all(&workdir).expect("create workdir");
    let canonical_workdir = fs::canonicalize(&workdir).expect("canonical workdir");
    let clean_workdir = canonical_workdir
        .to_str()
        .expect("utf8")
        .trim_end_matches('/');

    let settings = build_antigravity_read_only_settings(&canonical_workdir)
        .expect("build antigravity read_only settings");
    let settings_json = build_antigravity_read_only_settings_json(&canonical_workdir)
        .expect("build antigravity read_only settings json");
    let parsed_from_json: AntigravitySessionSettings =
        serde_json::from_str(&settings_json).expect("parse settings json");
    assert_eq!(parsed_from_json, settings);

    // Exactly one allow rule: read_file(<canonical_workdir>/**)
    assert_eq!(
        settings.permissions.allow,
        vec![format!("{AGY_READ_PERMISSION_TOOL}({clean_workdir}/**)")]
    );
    assert!(!settings.permissions.deny.is_empty());

    // Allowed reads inside canonical non-root cwd
    for allowed_rel in [
        "src/lib.rs",
        "crates/threadlane-workers/src/permissions.rs",
        "docs/ROADMAP.md",
        "AGENTS.md",
        "Cargo.toml",
    ] {
        let target = format!("{clean_workdir}/{allowed_rel}");
        assert_eq!(
            evaluate_antigravity_settings(&settings, "read_file", Some(&target)),
            OpenCodeRuleEffect::Allow,
            "expected read_file to be allowed on `{target}`"
        );
    }

    // Protected secret and control paths inside cwd must be denied (deny overrides allow)
    for protected_rel in [
        ".env",
        ".env.local",
        "nested/.env",
        "nested/.env.production",
        ".git",
        ".git/config",
        ".git/hooks/pre-commit",
        "sub/.git/HEAD",
        ".opencode",
        ".opencode/config.json",
        "sub/.opencode/state.json",
        ".codex",
        ".codex/config.toml",
        "sub/.codex/auth.json",
        ".gemini",
        ".gemini/settings.json",
        "sub/.gemini/oauth.json",
        ".agents",
        ".agents/flow.toml",
        "sub/.agents/role.toml",
        ".claude",
        ".claude/settings.json",
        "sub/.claude/CLAUDE.md",
    ] {
        let target = format!("{clean_workdir}/{protected_rel}");
        assert_eq!(
            evaluate_antigravity_settings(&settings, "read_file", Some(&target)),
            OpenCodeRuleEffect::Deny,
            "expected read_file to be denied on protected path `{target}`"
        );
    }

    // Reads outside cwd, on root, relative paths, or with traversal must be denied
    for external_or_bad_target in [
        "/etc/passwd",
        "/tmp/outside-file.txt",
        clean_workdir,
        "src/lib.rs",
        "./src/lib.rs",
        &format!("{clean_workdir}/../outside.txt"),
        &format!("{clean_workdir}/src/../../outside.txt"),
        "",
    ] {
        assert_eq!(
            evaluate_antigravity_settings(&settings, "read_file", Some(external_or_bad_target)),
            OpenCodeRuleEffect::Deny,
            "expected read_file to be denied on `{external_or_bad_target}`"
        );
    }
    assert_eq!(
        evaluate_antigravity_settings(&settings, "read_file", None),
        OpenCodeRuleEffect::Deny
    );

    // All write/edit tools and `notebook_edit` must be denied both globally and inside cwd
    let sample_inside = format!("{clean_workdir}/src/lib.rs");
    let sample_notebook = format!("{clean_workdir}/notebooks/analysis.ipynb");
    for write_tool in AGY_WRITE_PERMISSION_TOOLS {
        let global_rule = format!("{write_tool}(*)");
        let workdir_rule = format!("{write_tool}({clean_workdir}/**)");
        assert!(settings.permissions.deny.contains(&global_rule));
        assert!(settings.permissions.deny.contains(&workdir_rule));
        assert_eq!(
            evaluate_antigravity_settings(&settings, write_tool, Some(&sample_inside)),
            OpenCodeRuleEffect::Deny,
            "expected write tool `{write_tool}` to be denied inside cwd"
        );
        assert_eq!(
            evaluate_antigravity_settings(&settings, write_tool, Some("/tmp/other.rs")),
            OpenCodeRuleEffect::Deny,
            "expected write tool `{write_tool}` to be denied outside cwd"
        );
    }
    let notebook_global_rule = "notebook_edit(*)".to_string();
    let notebook_workdir_rule = format!("notebook_edit({clean_workdir}/**)");
    assert!(settings.permissions.deny.contains(&notebook_global_rule));
    assert!(settings.permissions.deny.contains(&notebook_workdir_rule));
    assert_eq!(
        evaluate_antigravity_settings(&settings, "notebook_edit", Some(&sample_notebook)),
        OpenCodeRuleEffect::Deny
    );
    assert_eq!(
        evaluate_antigravity_settings(&settings, "notebook_edit", Some("/tmp/other.ipynb")),
        OpenCodeRuleEffect::Deny
    );
    assert_eq!(
        evaluate_antigravity_settings(&settings, "notebook_edit", None),
        OpenCodeRuleEffect::Deny
    );

    // Explicit inventory check for real AGY 1.4.12 init effectful, notebook, browser/network,
    // delegation, workflow, knowledge, marketplace, and account tools:
    for required_denied_tool in [
        "notebook_edit",
        "notebook_execution",
        "delete_knowledge",
        "browser_subagent",
        "execute_browser_javascript",
        "run_workflow",
        "send_command_input",
        "open_browser_url",
        "read_url_content",
        "generate_image",
        "browser_click_element",
        "browser_drag_pixel_to_pixel",
        "browser_input",
        "browser_mouse_down",
        "browser_mouse_up",
        "browser_move_mouse",
        "browser_press_key",
        "browser_refresh_page",
        "browser_resize_window",
        "browser_scroll",
        "browser_scroll_dom",
        "browser_select_option",
        "click_browser_pixel",
        "manage_inbox",
        "search_marketplace",
        "list_plugin_accounts",
    ] {
        let expected_rule = format!("{required_denied_tool}(*)");
        assert!(
            AGY_DENIED_EXECUTION_AND_DELEGATION_TOOLS.contains(&required_denied_tool),
            "expected `{required_denied_tool}` in AGY_DENIED_EXECUTION_AND_DELEGATION_TOOLS"
        );
        assert!(
            settings.permissions.deny.contains(&expected_rule),
            "expected `{expected_rule}` in generated read_only deny rules"
        );
    }

    // All shell, notebook, subagent, MCP, web/browser, interactive, and unknown tools are denied
    // in the local generated-settings fixture evaluator (note: this tests the generated fixture
    // structure, not live external `agy` binary enforcement).
    for &denied_tool in AGY_DENIED_EXECUTION_AND_DELEGATION_TOOLS {
        assert_eq!(
            evaluate_antigravity_settings(&settings, denied_tool, Some("cargo test")),
            OpenCodeRuleEffect::Deny,
            "expected `{denied_tool}` to be denied with argument"
        );
        assert_eq!(
            evaluate_antigravity_settings(&settings, denied_tool, None),
            OpenCodeRuleEffect::Deny,
            "expected `{denied_tool}` to be denied without argument"
        );
    }
    assert_eq!(
        evaluate_antigravity_settings(&settings, "unlisted_future_tool", Some(&sample_inside)),
        OpenCodeRuleEffect::Deny
    );

    // Local fixture precedence check on mutated settings structure:
    let mut mutated = settings.clone();
    mutated
        .permissions
        .allow
        .push(format!("edit_file({clean_workdir}/**)"));
    mutated
        .permissions
        .allow
        .push(format!("notebook_edit({clean_workdir}/**)"));
    mutated.permissions.allow.push("run_command(*)".to_string());
    mutated
        .permissions
        .allow
        .push("execute_browser_javascript(*)".to_string());
    assert_eq!(
        mutated.evaluate("edit_file", Some(&sample_inside)),
        OpenCodeRuleEffect::Deny
    );
    assert_eq!(
        mutated.evaluate("notebook_edit", Some(&sample_notebook)),
        OpenCodeRuleEffect::Deny
    );
    assert_eq!(
        mutated.evaluate("run_command", Some("ls -la")),
        OpenCodeRuleEffect::Deny
    );
    assert_eq!(
        mutated.evaluate("execute_browser_javascript", Some("document.cookie")),
        OpenCodeRuleEffect::Deny
    );
    assert_eq!(AGY_PROTECTED_READ_DENY_PATTERNS.len(), 28);
}

#[test]
fn opencode_read_only_config_pins_all_agent_models_and_enforces_default_deny_precedence() {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("opencode-advisory");
    fs::create_dir_all(&workdir).expect("create workdir");

    let resolved = resolve_model_target(
        WorkerProvider::OpenCodeV2,
        "opencode-go/deepseek-v4.1-flash#max",
        None,
    )
    .expect("resolve opencode model");

    let config = build_opencode_config_content(
        &resolved,
        WorkerFilePermissions::ReadOnly,
        &workdir,
        &BTreeSet::new(),
    )
    .expect("build opencode read_only config");

    let expected_model_obj = OpenCodeModelObject {
        provider: "opencode-go".to_string(),
        model: "deepseek-v4.1-flash".to_string(),
        variant: Some("max".to_string()),
    };
    assert_eq!(config.update, "disable");
    assert_eq!(config.default_agent, "build");
    assert_eq!(config.model, expected_model_obj);

    // Every pinned agent (build, title, summary, compaction) must be present and pinned to the
    // exact requested model + variant with no small-model fallback.
    assert_eq!(config.agents.len(), OPENCODE_PINNED_AGENTS.len());
    for agent_name in OPENCODE_PINNED_AGENTS {
        let agent_cfg = config
            .agents
            .get(agent_name)
            .unwrap_or_else(|| panic!("missing pinned agent `{agent_name}`"));
        assert_eq!(agent_cfg.model, expected_model_obj);
        assert_eq!(agent_cfg.permissions, config.permissions);
    }

    // Policy evaluation: only `opencode-go` provider allowed; MCP and effectful/delegation tools denied.
    assert_eq!(
        config.evaluate_policy("provider.use", "opencode-go"),
        OpenCodeRuleEffect::Allow
    );
    for other_provider in ["anthropic", "openai", "google", "openrouter", "*"] {
        assert_eq!(
            config.evaluate_policy("provider.use", other_provider),
            OpenCodeRuleEffect::Deny,
            "expected provider `{other_provider}` to be denied"
        );
    }
    assert_eq!(
        config.evaluate_policy("mcp.use", "*"),
        OpenCodeRuleEffect::Deny
    );
    assert_eq!(
        config.evaluate_policy("mcp.use", "codegraph"),
        OpenCodeRuleEffect::Deny
    );
    for denied_tool in [
        "subagent",
        "task",
        "agent",
        "edit",
        "write",
        "patch",
        "apply_patch",
        "multiedit",
        "shell",
        "bash",
        "execute",
        "web",
        "webfetch",
        "websearch",
        "todowrite",
        "question",
    ] {
        assert_eq!(
            config.evaluate_policy("tool.use", denied_tool),
            OpenCodeRuleEffect::Deny,
            "expected tool.use `{denied_tool}` policy to be denied"
        );
    }

    // Permission evaluation across global and all pinned agents:
    for agent_opt in [
        None,
        Some("build"),
        Some("title"),
        Some("summary"),
        Some("compaction"),
    ] {
        // Internal read/glob/grep allowed on normal repository paths
        for read_action in ["read", "glob", "grep"] {
            for rel_path in [
                "src/lib.rs",
                "./src/lib.rs",
                "docs/ROADMAP.md",
                "AGENTS.md",
                "Cargo.toml",
            ] {
                assert_eq!(
                    config.evaluate_permission(agent_opt, read_action, rel_path),
                    OpenCodeRuleEffect::Allow,
                    "expected `{read_action}` on `{rel_path}` to be allowed for {agent_opt:?}"
                );
            }

            // Protected control and secret paths denied by trailing rules
            for protected_path in [
                ".env",
                ".env.local",
                "nested/.env",
                "nested/.env.production",
                ".git",
                ".git/config",
                "sub/.git/HEAD",
                ".opencode",
                ".opencode/config.json",
                ".codex",
                ".codex/config.toml",
                ".gemini",
                ".gemini/settings.json",
                ".agents",
                ".agents/flow.toml",
                ".claude",
                ".claude/settings.json",
            ] {
                assert_eq!(
                    config.evaluate_permission(agent_opt, read_action, protected_path),
                    OpenCodeRuleEffect::Deny,
                    "expected `{read_action}` on protected `{protected_path}` to be denied"
                );
            }

            // External and traversal paths denied via `external_directory` deny
            for external_path in ["/etc/passwd", "../outside.txt", "src/../../outside.txt", ""] {
                assert_eq!(
                    config.evaluate_permission(agent_opt, read_action, external_path),
                    OpenCodeRuleEffect::Deny,
                    "expected `{read_action}` on external/traversal `{external_path}` to be denied"
                );
            }
        }

        // All edit/write actions denied everywhere
        for write_action in ["edit", "write", "patch", "apply_patch", "multiedit"] {
            for target in [
                "src/lib.rs",
                "docs/ROADMAP.md",
                "AGENTS.md",
                ".env",
                "/tmp/out.txt",
                "*",
            ] {
                assert_eq!(
                    config.evaluate_permission(agent_opt, write_action, target),
                    OpenCodeRuleEffect::Deny,
                    "expected `{write_action}` on `{target}` to be denied in read_only mode"
                );
            }
        }

        // All shell, subagent, web, MCP, interactive, and unknown actions denied
        for non_read_action in [
            "shell",
            "bash",
            "execute",
            "subagent",
            "task",
            "agent",
            "web",
            "webfetch",
            "websearch",
            "mcp",
            "todowrite",
            "question",
            "external_directory",
            "unknown_custom_action",
        ] {
            assert_eq!(
                config.evaluate_permission(agent_opt, non_read_action, "src/lib.rs"),
                OpenCodeRuleEffect::Deny,
                "expected `{non_read_action}` to be denied in read_only mode"
            );
        }
    }
}

#[test]
fn path_symlink_root_metacharacter_and_protected_control_workdir_refusal() {
    let temp = TempDir::new().expect("tempdir");
    let real_dir = temp.path().join("real-workdir");
    fs::create_dir_all(&real_dir).expect("create real_dir");

    // 1. Filesystem root `/` must be refused
    let root_path = Path::new("/");
    assert!(validate_advisory_read_only_workdir(root_path).is_err());
    assert!(build_antigravity_read_only_settings(root_path).is_err());

    let resolved_oc = resolve_model_target(
        WorkerProvider::OpenCodeV2,
        "opencode-go/deepseek-v4.1-flash#max",
        None,
    )
    .expect("resolve model");
    assert!(build_opencode_config_content(
        &resolved_oc,
        WorkerFilePermissions::ReadOnly,
        root_path,
        &BTreeSet::new()
    )
    .is_err());

    // 2. Relative path must be refused
    let rel_path = Path::new("relative/workdir");
    assert!(validate_advisory_read_only_workdir(rel_path).is_err());
    assert!(build_antigravity_read_only_settings(rel_path).is_err());

    // 3. Symlinked working directory must be refused
    let symlink_dir = temp.path().join("symlink-workdir");
    symlink(&real_dir, &symlink_dir).expect("create symlink workdir");
    let sym_err = validate_advisory_read_only_workdir(&symlink_dir)
        .expect_err("symlink workdir must be refused");
    assert_eq!(sym_err.code(), WorkerErrorCode::SecurityViolation);
    assert!(build_antigravity_read_only_settings(&symlink_dir).is_err());
    assert!(build_opencode_config_content(
        &resolved_oc,
        WorkerFilePermissions::ReadOnly,
        &symlink_dir,
        &BTreeSet::new()
    )
    .is_err());

    // 4. Glob/rule metacharacters in working directory must be refused
    for bad_dir_name in ["dir*star", "dir?question", "dir[bracket]", "dir(paren)"] {
        let bad_dir = temp.path().join(bad_dir_name);
        fs::create_dir_all(&bad_dir).expect("create metachar dir");
        let meta_err = validate_advisory_read_only_workdir(&bad_dir)
            .expect_err("glob/rule metacharacter workdir must be refused");
        assert_eq!(meta_err.code(), WorkerErrorCode::InvalidSpec);
        assert!(build_antigravity_read_only_settings(&bad_dir).is_err());
        assert!(build_opencode_config_content(
            &resolved_oc,
            WorkerFilePermissions::ReadOnly,
            &bad_dir,
            &BTreeSet::new()
        )
        .is_err());
    }

    // 5. Protected control directories as working_directory (or ancestor) must be refused
    for protected_sub in [
        ".git",
        ".git/hooks",
        ".codex",
        ".gemini",
        ".opencode",
        ".agents",
        ".claude",
        ".github",
        ".cargo",
        ".env",
        ".env.local",
    ] {
        let protected_dir = real_dir.join(protected_sub);
        fs::create_dir_all(&protected_dir).expect("create protected dir");
        let prot_err = validate_advisory_read_only_workdir(&protected_dir)
            .expect_err("protected control directory must be refused as advisory cwd");
        assert_eq!(prot_err.code(), WorkerErrorCode::InvalidSpec);
        assert!(build_antigravity_read_only_settings(&protected_dir).is_err());
        assert!(build_opencode_config_content(
            &resolved_oc,
            WorkerFilePermissions::ReadOnly,
            &protected_dir,
            &BTreeSet::new()
        )
        .is_err());
    }
}

#[test]
fn legacy_inherit_and_task_files_argv_and_settings_remain_unchanged() {
    let temp = TempDir::new().expect("tempdir");
    let repo_dir = temp.path().join("repo");
    let work_area = temp.path().join("work-area");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&repo_dir).expect("create repo");
    fs::create_dir_all(&work_area).expect("create work_area");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let fake_agy = write_executable_script(&bin_dir, "fake-agy", "#!/usr/bin/env sh\nexit 0\n");
    let fake_oc = write_executable_script(&bin_dir, "fake-opencode", "#!/usr/bin/env sh\nexit 0\n");

    // 1. Legacy Antigravity `Inherit` argv
    let agy_inherit_spec = WorkerSpec {
        provider: WorkerProvider::Antigravity,
        executable: fake_agy.clone(),
        model: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        allowed_models: BTreeSet::new(),
        session_id: None,
        standalone: false,
        repository_path: repo_dir.clone(),
        base_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
        work_area_root: work_area.clone(),
        allowed_write_paths: BTreeSet::from(["src/lib.rs".to_string()]),
        prompt: "implement task".to_string(),
        file_permissions: WorkerFilePermissions::Inherit,
        budgets: ExecutionBudgets::default(),
    };
    let agy_inherit_argv = agy_inherit_spec
        .build_argv()
        .expect("build agy inherit argv");
    assert_eq!(
        agy_inherit_argv,
        vec![
            "--model",
            "gemini-4-argon-high",
            "--effort",
            "high",
            "--mode",
            "accept-edits",
            "--output-format",
            "stream-json",
            "--print-timeout",
            "0",
            "--print",
            "implement task",
        ]
    );
    assert!(!agy_inherit_argv
        .iter()
        .any(|arg| arg.contains("dangerously-skip-permissions") || arg == "--auto"));

    // 2. Legacy Antigravity `TaskFiles` settings JSON omits `deny` so byte representation is unchanged
    let task_files_json = build_antigravity_task_files_settings_json(
        &repo_dir,
        &BTreeSet::from(["src/lib.rs".to_string()]),
    )
    .expect("build task_files settings json");
    assert!(!task_files_json.contains("\"deny\""));
    let parsed_val: serde_json::Value = serde_json::from_str(&task_files_json).expect("valid json");
    assert!(parsed_val["permissions"]["allow"].is_array());
    assert!(parsed_val["permissions"].get("deny").is_none());

    // 3. Legacy OpenCode V2 `Inherit` argv and child env
    let oc_inherit_spec = WorkerSpec {
        provider: WorkerProvider::OpenCodeV2,
        executable: fake_oc,
        model: "opencode-go/deepseek-v4.1-flash#max".to_string(),
        effort: None,
        allowed_models: BTreeSet::new(),
        session_id: Some("sess-123".to_string()),
        standalone: true,
        repository_path: repo_dir.clone(),
        base_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
        work_area_root: work_area,
        allowed_write_paths: BTreeSet::from(["src/lib.rs".to_string()]),
        prompt: "implement task".to_string(),
        file_permissions: WorkerFilePermissions::Inherit,
        budgets: ExecutionBudgets::default(),
    };
    let oc_inherit_argv = oc_inherit_spec
        .build_argv()
        .expect("build opencode inherit argv");
    assert_eq!(
        oc_inherit_argv,
        vec![
            "--standalone",
            "run",
            "--model",
            "opencode-go/deepseek-v4.1-flash#max",
            "--format",
            "json",
            "--session",
            "sess-123",
            "--",
            "implement task",
        ]
    );
    let oc_inherit_cfg = oc_inherit_spec
        .build_opencode_config_content_for_workdir(&repo_dir)
        .expect("build opencode inherit config");
    assert!(oc_inherit_cfg.permissions.is_none());

    // 4. `WorkerSpec` default deserialization preserves `Inherit` and rejects zero-write or `ReadOnly`
    let worker_spec_without_permissions_json = serde_json::json!({
        "provider": "antigravity",
        "executable": fake_agy,
        "model": "gemini-4-argon-high",
        "effort": "high",
        "repository_path": repo_dir,
        "base_commit": "0123456789abcdef0123456789abcdef01234567",
        "work_area_root": agy_inherit_spec.work_area_root,
        "allowed_write_paths": ["src/lib.rs"],
        "prompt": "implement task"
    });
    let deserialized_worker_spec: WorkerSpec =
        serde_json::from_value(worker_spec_without_permissions_json)
            .expect("deserialize WorkerSpec without file_permissions");
    assert_eq!(
        deserialized_worker_spec.file_permissions,
        WorkerFilePermissions::Inherit
    );

    let mut empty_writes_worker_spec = agy_inherit_spec.clone();
    empty_writes_worker_spec.allowed_write_paths.clear();
    let empty_inherit_err = empty_writes_worker_spec
        .validate()
        .expect_err("WorkerSpec must reject empty allowed_write_paths in Inherit mode");
    assert_eq!(empty_inherit_err.code(), WorkerErrorCode::InvalidSpec);
    assert!(empty_inherit_err
        .to_string()
        .contains("allowed_write_paths must contain at least one repository-relative path"));

    empty_writes_worker_spec.file_permissions = WorkerFilePermissions::ReadOnly;
    let empty_readonly_on_worker_spec_err = empty_writes_worker_spec
        .validate()
        .expect_err("WorkerSpec must reject empty allowed_write_paths even if ReadOnly is set");
    assert_eq!(
        empty_readonly_on_worker_spec_err.code(),
        WorkerErrorCode::InvalidSpec
    );
    assert!(empty_readonly_on_worker_spec_err
        .to_string()
        .contains("allowed_write_paths must contain at least one repository-relative path"));

    let mut non_empty_readonly_worker_spec = agy_inherit_spec;
    non_empty_readonly_worker_spec.file_permissions = WorkerFilePermissions::ReadOnly;
    let readonly_worker_spec_err = non_empty_readonly_worker_spec
        .validate()
        .expect_err("WorkerSpec must reject file_permissions: read_only");
    assert_eq!(
        readonly_worker_spec_err.code(),
        WorkerErrorCode::InvalidSpec
    );
    assert!(readonly_worker_spec_err
        .to_string()
        .contains("`file_permissions: read_only` requires `AdvisoryWorkerSpec`"));

    // 5. `AdvisoryWorkerSpec` default deserialization defaults to `ReadOnly` and zero write scopes
    let advisory_spec_without_permissions_json = serde_json::json!({
        "provider": "antigravity",
        "executable": fake_agy,
        "model": "gemini-4-argon-high",
        "effort": "high",
        "working_directory": repo_dir,
        "prompt": "read-only review"
    });
    let deserialized_advisory_spec: AdvisoryWorkerSpec =
        serde_json::from_value(advisory_spec_without_permissions_json)
            .expect("deserialize AdvisoryWorkerSpec without file_permissions");
    assert_eq!(
        deserialized_advisory_spec.file_permissions,
        WorkerFilePermissions::ReadOnly
    );
    assert!(deserialized_advisory_spec.allowed_write_paths.is_empty());
    let advisory_validation = deserialized_advisory_spec
        .validate()
        .expect("default deserialized AdvisoryWorkerSpec must validate");
    let expected_settings_json =
        build_antigravity_read_only_settings_json(&advisory_validation.canonical_working_directory)
            .expect("build expected read_only settings json");
    assert_eq!(
        advisory_validation.constructed_argv,
        vec![
            "--model".to_string(),
            "gemini-4-argon-high".to_string(),
            "--effort".to_string(),
            "high".to_string(),
            "--mode".to_string(),
            "plan".to_string(),
            "--output-format".to_string(),
            "stream-json".to_string(),
            "--print-timeout".to_string(),
            "0".to_string(),
            "--settings".to_string(),
            expected_settings_json,
            "--print".to_string(),
            "read-only review".to_string(),
        ]
    );
    assert!(
        !advisory_validation.constructed_argv.iter().any(|arg| {
            arg == "accept-edits"
                || arg == "auto"
                || arg == "--auto"
                || arg.contains("dangerously-skip")
        }),
        "advisory argv must never use accept-edits, auto, or dangerously-skip flags"
    );
}

#[test]
fn antigravity_advisory_argv_uses_plan_mode_while_legacy_worker_spec_preserves_accept_edits() {
    let temp = TempDir::new().expect("tempdir");
    let repo_dir = temp.path().join("repo");
    let work_area = temp.path().join("work-area");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&repo_dir).expect("create repo");
    fs::create_dir_all(&work_area).expect("create work_area");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let fake_agy = write_executable_script(&bin_dir, "fake-agy", "#!/usr/bin/env sh\nexit 0\n");

    let advisory_spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::Antigravity,
        executable: fake_agy.clone(),
        model: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        allowed_models: BTreeSet::new(),
        session_id: None,
        standalone: false,
        working_directory: repo_dir.clone(),
        allowed_write_paths: BTreeSet::new(),
        prompt: "inspect architecture only".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    };
    let advisory_argv = advisory_spec.build_argv().expect("build advisory argv");
    let advisory_workdir_argv = advisory_spec
        .build_argv_for_workdir(&repo_dir)
        .expect("build advisory argv for workdir");
    assert_eq!(advisory_argv, advisory_workdir_argv);

    let mode_idx = advisory_argv
        .iter()
        .position(|arg| arg == "--mode")
        .expect("`--mode` must be present in Antigravity advisory argv");
    assert_eq!(
        advisory_argv.get(mode_idx + 1).map(String::as_str),
        Some("plan")
    );
    assert!(
        !advisory_argv.iter().any(|arg| {
            arg == "accept-edits"
                || arg == "auto"
                || arg == "--auto"
                || arg.contains("dangerously-skip")
        }),
        "advisory argv must never contain accept-edits, auto, or dangerously-skip"
    );

    // Ordinary WorkerSpec (both Inherit and TaskFiles static preview) keeps legacy accept-edits byte-for-byte
    for mode in [
        WorkerFilePermissions::Inherit,
        WorkerFilePermissions::TaskFiles,
    ] {
        let write_spec = WorkerSpec {
            provider: WorkerProvider::Antigravity,
            executable: fake_agy.clone(),
            model: "gemini-4-argon-high".to_string(),
            effort: Some("high".to_string()),
            allowed_models: BTreeSet::new(),
            session_id: None,
            standalone: false,
            repository_path: repo_dir.clone(),
            base_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
            work_area_root: work_area.clone(),
            allowed_write_paths: BTreeSet::from(["src/lib.rs".to_string()]),
            prompt: "implement task".to_string(),
            file_permissions: mode,
            budgets: ExecutionBudgets::default(),
        };
        let validation = write_spec.validate().expect("validate WorkerSpec");
        assert_eq!(
            validation.constructed_argv,
            vec![
                "--model",
                "gemini-4-argon-high",
                "--effort",
                "high",
                "--mode",
                "accept-edits",
                "--output-format",
                "stream-json",
                "--print-timeout",
                "0",
                "--print",
                "implement task",
            ]
        );
    }
}

#[tokio::test]
async fn supervised_advisory_worker_antigravity_distinguishes_provider_private_logs_and_reaps_pipe_descendants(
) {
    let temp = TempDir::new().expect("tempdir");
    let advisory_cwd = temp.path().join("advisory-cwd");
    let provider_private_dir = temp.path().join("provider-private-state");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&advisory_cwd).expect("create advisory_cwd");
    fs::create_dir_all(&provider_private_dir).expect("create provider_private_dir");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let tracked_source = advisory_cwd.join("README.md");
    fs::write(&tracked_source, "original content\n").expect("write tracked file");
    let private_log_path = provider_private_dir.join("agy-session.log");

    // Fake Antigravity binary that:
    // 1. Verifies `--settings` is passed in argv and contains `"deny"`;
    // 2. Writes intentional provider-private state outside `advisory_cwd`;
    // 3. Emits diagnostic output on stderr;
    // 4. Spawns a background descendant holding stdout/stderr pipes (`sleep 30 &`);
    // 5. Emits valid Antigravity NDJSON stream events for `gemini-4-argon-high` with `effort: high`.
    let script = format!(
        r#"#!/usr/bin/env sh
set -eu
echo "provider telemetry initialized" >> "{}"
echo "advisory stderr diagnostic note" >&2
sleep 30 &
printf '%s\n' '{{"type":"system","subtype":"init","session_id":"agy-adv-1","model":"gemini-4-argon-high","effort":"high"}}'
printf '%s\n' '{{"type":"assistant","message":{{"model":"gemini-4-argon-high","content":[{{"type":"text","text":"Advisory plan ready."}}]}}}}'
printf '%s\n' '{{"type":"result","subtype":"success","session_id":"agy-adv-1","model":"gemini-4-argon-high","effort":"high","result":"Advisory plan ready."}}'
exit 0
"#,
        private_log_path.display()
    );
    let fake_agy = write_executable_script(&bin_dir, "fake-agy-advisory", &script);

    let spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::Antigravity,
        executable: fake_agy,
        model: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        allowed_models: BTreeSet::from(["gemini-4-argon-high".to_string()]),
        session_id: None,
        standalone: false,
        working_directory: advisory_cwd.clone(),
        allowed_write_paths: BTreeSet::new(),
        prompt: "Produce a read-only architecture plan.".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    };

    let report = run_advisory_worker(&spec)
        .await
        .expect("run_advisory_worker should succeed");

    assert_eq!(report.status, WorkerRunStatus::Succeeded);
    assert_eq!(report.exit_code, Some(0));
    assert!(report.descendants_terminated_after_exit);
    assert!(!report.cleanup_incomplete);
    assert!(report
        .redacted_stderr_summary
        .as_deref()
        .is_some_and(|s| s.contains("advisory stderr diagnostic note")));
    assert!(private_log_path.exists());
    assert_eq!(
        fs::read_to_string(&tracked_source).expect("read tracked source"),
        "original content\n"
    );

    // Verify `--mode plan` and `--settings` were injected into `constructed_argv` with scoped read_only rules
    let mode_idx = report
        .constructed_argv
        .iter()
        .position(|arg| arg == "--mode")
        .expect("`--mode` flag must be present in Antigravity read_only argv");
    assert_eq!(
        report
            .constructed_argv
            .get(mode_idx + 1)
            .map(String::as_str),
        Some("plan")
    );
    assert!(!report.constructed_argv.iter().any(|arg| {
        arg == "accept-edits"
            || arg == "auto"
            || arg == "--auto"
            || arg.contains("dangerously-skip")
    }));
    let settings_idx = report
        .constructed_argv
        .iter()
        .position(|arg| arg == "--settings")
        .expect("`--settings` flag must be present in Antigravity read_only argv");
    let settings_arg = &report.constructed_argv[settings_idx + 1];
    let parsed_settings: AntigravitySessionSettings =
        serde_json::from_str(settings_arg).expect("parse `--settings` json");
    assert_eq!(parsed_settings.permissions.allow.len(), 1);
    assert!(!parsed_settings.permissions.deny.is_empty());
}

#[tokio::test]
async fn supervised_advisory_worker_opencode_passes_read_only_config_env_and_preserves_cancellation_and_evidence_gates(
) {
    let temp = TempDir::new().expect("tempdir");
    let advisory_cwd = temp.path().join("opencode-cwd");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&advisory_cwd).expect("create advisory_cwd");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    // 1. OpenCode V2 advisory run with explicit session_id that verifies OPENCODE_CONFIG_CONTENT
    //    is present in the child environment and emits stream evidence lacking effort ->
    //    must report `MissingEffortEvidence` (bounded unknown/incomplete evidence gate preserved).
    let env_dump_path = temp.path().join("captured_opencode_config.json");
    let oc_script = format!(
        r#"#!/usr/bin/env sh
set -eu
printf '%s' "${{{OPENCODE_CONFIG_ENV_VAR}}}" > "{}"
printf '%s\n' '{{"type":"step_start","sessionID":"ses_adv_1","part":{{"type":"step-start","model":"opencode-go/deepseek-v4.1-flash"}}}}'
printf '%s\n' '{{"type":"step_finish","sessionID":"ses_adv_1","part":{{"type":"step-finish","reason":"stop","model":"opencode-go/deepseek-v4.1-flash"}}}}'
exit 0
"#,
        env_dump_path.display()
    );
    let fake_oc = write_executable_script(&bin_dir, "fake-opencode-advisory", &oc_script);

    let oc_spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::OpenCodeV2,
        executable: fake_oc,
        model: "opencode-go/deepseek-v4.1-flash#max".to_string(),
        effort: None,
        allowed_models: BTreeSet::from(["opencode-go/deepseek-v4.1-flash#max".to_string()]),
        session_id: Some("ses_adv_1".to_string()),
        standalone: true,
        working_directory: advisory_cwd.clone(),
        allowed_write_paths: BTreeSet::new(),
        prompt: "Inspect repository read-only.".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    };

    let oc_report = run_advisory_worker(&oc_spec)
        .await
        .expect("run opencode advisory worker");
    assert_eq!(oc_report.status, WorkerRunStatus::MissingEffortEvidence);

    let captured_cfg_raw =
        fs::read_to_string(&env_dump_path).expect("read captured OPENCODE_CONFIG_CONTENT");
    let captured_cfg: OpenCodeConfigContent =
        serde_json::from_str(&captured_cfg_raw).expect("parse captured OPENCODE_CONFIG_CONTENT");
    assert_eq!(
        captured_cfg.evaluate_permission(Some("build"), "read", "src/lib.rs"),
        OpenCodeRuleEffect::Allow
    );
    assert_eq!(
        captured_cfg.evaluate_permission(Some("build"), "edit", "src/lib.rs"),
        OpenCodeRuleEffect::Deny
    );
    assert_eq!(
        captured_cfg.evaluate_permission(Some("build"), "shell", "*"),
        OpenCodeRuleEffect::Deny
    );

    // 2. Cancellation behavior preserved for advisory workers
    let slow_agy = write_executable_script(
        &bin_dir,
        "slow-agy-advisory",
        "#!/usr/bin/env sh\nsleep 30\n",
    );
    let cancel_spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::Antigravity,
        executable: slow_agy,
        model: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        allowed_models: BTreeSet::new(),
        session_id: None,
        standalone: false,
        working_directory: advisory_cwd,
        allowed_write_paths: BTreeSet::new(),
        prompt: "Cancel me.".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    };
    let (cancel_tx, mut cancel_rx) = watch::channel(false);
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let _ = cancel_tx.send(true);
    });
    let cancel_report = run_advisory_worker_with_cancel(&cancel_spec, &mut cancel_rx)
        .await
        .expect("run_advisory_worker_with_cancel");
    assert_eq!(cancel_report.status, WorkerRunStatus::Cancelled);
    assert!(!cancel_report.cleanup_incomplete);
}
