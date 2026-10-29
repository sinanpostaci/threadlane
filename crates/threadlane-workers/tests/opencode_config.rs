#![cfg(unix)]
#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;
use tempfile::TempDir;
use threadlane_workers::permissions::{
    build_opencode_config_content, build_opencode_config_content_json, evaluate_opencode_rules,
    OpenCodeAgentConfig, OpenCodeConfigContent, OpenCodeModelObject, OpenCodePermissionRule,
    OpenCodeRuleEffect, MAX_OPENCODE_CONFIG_BYTES, MAX_TASK_FILES_WRITE_PATHS,
    OPENCODE_CONFIG_ENV_VAR, OPENCODE_DEFAULT_AGENT, OPENCODE_PINNED_AGENTS,
};
use threadlane_workers::{
    execute_worker_task, is_path_allowed, run_worker, ExecutionBudgets, FieldEvidence,
    ResolvedModelTarget, WorkAcceptanceState, WorkerError, WorkerFilePermissions, WorkerProvider,
    WorkerRunStatus, WorkerSpec,
};
use tokio::sync::watch;

struct OpenCodeTestEnv {
    _temp_dir: TempDir,
    root_dir: PathBuf,
    repo_dir: PathBuf,
    work_area: PathBuf,
    bin_dir: PathBuf,
    base_commit: String,
}

impl OpenCodeTestEnv {
    fn new() -> Self {
        let temp_dir = TempDir::new().expect("create temp dir");
        let root_dir = fs::canonicalize(temp_dir.path()).expect("canonicalize temp dir");
        let repo_dir = root_dir.join("source-repo");
        let work_area = root_dir.join("work-area");
        let bin_dir = root_dir.join("bin");

        fs::create_dir_all(&repo_dir).expect("create repo_dir");
        fs::create_dir_all(&bin_dir).expect("create bin_dir");
        fs::set_permissions(&bin_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 bin_dir");

        git_cmd(&repo_dir, &["init", "-b", "main"]);
        git_cmd(
            &repo_dir,
            &["config", "--local", "user.name", "Threadlane Test"],
        );
        git_cmd(
            &repo_dir,
            &["config", "--local", "user.email", "test@threadlane.local"],
        );

        let workers_src = repo_dir.join("crates/threadlane-workers/src");
        let core_src = repo_dir.join("crates/threadlane-core/src");
        let docs_dir = repo_dir.join("docs");
        fs::create_dir_all(&workers_src).expect("create workers_src");
        fs::create_dir_all(&core_src).expect("create core_src");
        fs::create_dir_all(&docs_dir).expect("create docs_dir");

        fs::write(
            workers_src.join("lib.rs"),
            "pub fn baseline() -> u32 { 1 }\n",
        )
        .expect("write workers lib.rs");
        fs::write(core_src.join("lib.rs"), "pub fn core_only() {}\n").expect("write core lib.rs");
        fs::write(docs_dir.join("WORKERS.md"), "# Workers\n").expect("write docs/WORKERS.md");

        git_cmd(&repo_dir, &["add", "."]);
        git_cmd(&repo_dir, &["commit", "-m", "Initial commit"]);

        let base_commit = git_stdout(&repo_dir, &["rev-parse", "--verify", "HEAD^{commit}"])
            .trim()
            .to_string();
        assert_eq!(base_commit.len(), 40);

        Self {
            _temp_dir: temp_dir,
            root_dir,
            repo_dir,
            work_area,
            bin_dir,
            base_commit,
        }
    }

    fn write_fake_executable(&self, name: &str, script_body: &str) -> PathBuf {
        let path = self.bin_dir.join(name);
        let full_script = format!("#!/bin/sh\nset -eu\n{script_body}\n");
        fs::write(&path, full_script).expect("write fake executable");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 fake executable");
        path
    }

    fn default_scoped_paths(&self) -> BTreeSet<String> {
        BTreeSet::from([
            "crates/threadlane-workers/".to_string(),
            "docs/WORKERS.md".to_string(),
        ])
    }

    fn opencode_spec(
        &self,
        executable: PathBuf,
        prompt: &str,
        file_permissions: WorkerFilePermissions,
    ) -> WorkerSpec {
        WorkerSpec {
            provider: WorkerProvider::OpenCodeV2,
            executable,
            model: "opencode-go/deepseek-v4.1-flash".to_string(),
            effort: Some("max".to_string()),
            allowed_models: BTreeSet::from([
                "opencode-go/deepseek-v4.1-flash".to_string(),
                "opencode-go/deepseek-v4.1-flash#max".to_string(),
            ]),
            session_id: Some("sess-oc-220".to_string()),
            standalone: true,
            repository_path: self.repo_dir.clone(),
            base_commit: self.base_commit.clone(),
            work_area_root: self.work_area.clone(),
            allowed_write_paths: self.default_scoped_paths(),
            prompt: prompt.to_string(),
            file_permissions,
            budgets: ExecutionBudgets::default(),
        }
    }

    fn antigravity_spec(
        &self,
        executable: PathBuf,
        prompt: &str,
        file_permissions: WorkerFilePermissions,
    ) -> WorkerSpec {
        WorkerSpec {
            provider: WorkerProvider::Antigravity,
            executable,
            model: "gemini-4-argon-high".to_string(),
            effort: Some("high".to_string()),
            allowed_models: BTreeSet::from(["gemini-4-argon-high".to_string()]),
            session_id: None,
            standalone: false,
            repository_path: self.repo_dir.clone(),
            base_commit: self.base_commit.clone(),
            work_area_root: self.work_area.clone(),
            allowed_write_paths: self.default_scoped_paths(),
            prompt: prompt.to_string(),
            file_permissions,
            budgets: ExecutionBudgets::default(),
        }
    }
}

fn git_cmd(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .expect("spawn git");
    assert!(
        out.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
}

fn git_stdout(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .expect("spawn git");
    assert!(
        out.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).to_string()
}

#[test]
fn pins_primary_and_auxiliary_models_and_provider_policies_under_inherit_and_task_files() {
    let env = OpenCodeTestEnv::new();
    let bin = env.write_fake_executable("noop-oc", "exit 0");
    let workdir = env.root_dir.join("canonical-oc-workdir");
    fs::create_dir_all(workdir.join("crates/threadlane-workers/src")).expect("create workdir sub");

    let expected_model = OpenCodeModelObject {
        provider: "opencode-go".to_string(),
        model: "deepseek-v4.1-flash".to_string(),
        variant: Some("max".to_string()),
    };

    for mode in [
        WorkerFilePermissions::Inherit,
        WorkerFilePermissions::TaskFiles,
    ] {
        let spec = env.opencode_spec(bin.clone(), "Verify model and policy pins", mode);
        let config = spec
            .build_opencode_config_content_for_workdir(&workdir)
            .expect("build_opencode_config_content_for_workdir");
        let raw_json = spec
            .build_opencode_config_for_workdir(&workdir)
            .expect("build_opencode_config_for_workdir");
        let parsed: Value = serde_json::from_str(&raw_json).expect("parse OPENCODE_CONFIG_CONTENT");
        let root = parsed.as_object().expect("root object");
        assert_eq!(parsed["update"], "disable");
        assert_eq!(parsed["model"]["providerID"], "opencode-go");
        assert!(parsed["model"].get("provider").is_none());

        // Must never emit V1 schema keys (`permission`, `agent`, `bash`, `task`).
        for forbidden_v1_key in ["permission", "agent", "bash", "task"] {
            assert!(
                !root.contains_key(forbidden_v1_key),
                "OpenCode V2 config must never emit V1 key `{forbidden_v1_key}`"
            );
        }

        assert_eq!(config.default_agent, OPENCODE_DEFAULT_AGENT);
        assert_eq!(config.model, expected_model);
        assert_eq!(config.agents.len(), OPENCODE_PINNED_AGENTS.len());
        for agent_name in OPENCODE_PINNED_AGENTS {
            let agent_cfg = config
                .agents
                .get(agent_name)
                .unwrap_or_else(|| panic!("missing pinned agent `{agent_name}`"));
            assert_eq!(
                agent_cfg.model, expected_model,
                "agent `{agent_name}` must be pinned to the exact primary provider/model/variant"
            );
            assert_eq!(
                parsed["agents"][agent_name]["model"]["providerID"],
                "opencode-go"
            );
            assert_eq!(agent_cfg.permissions, config.permissions);
        }

        // Provider and MCP policies: deny *, then allow selected provider; deny all MCP.
        assert_eq!(
            &config.policies[..3],
            &vec![
                OpenCodePermissionRule {
                    action: "provider.use".to_string(),
                    resource: "*".to_string(),
                    effect: OpenCodeRuleEffect::Deny,
                },
                OpenCodePermissionRule {
                    action: "provider.use".to_string(),
                    resource: "opencode-go".to_string(),
                    effect: OpenCodeRuleEffect::Allow,
                },
                OpenCodePermissionRule {
                    action: "mcp.use".to_string(),
                    resource: "*".to_string(),
                    effect: OpenCodeRuleEffect::Deny,
                },
            ]
        );
        for nested_tool in ["subagent", "task", "agent"] {
            assert_eq!(
                config.evaluate_policy("tool.use", nested_tool),
                OpenCodeRuleEffect::Deny
            );
        }
        assert_eq!(
            config.evaluate_policy("provider.use", "opencode-go"),
            OpenCodeRuleEffect::Allow
        );
        assert_eq!(
            config.evaluate_policy("provider.use", "openai"),
            OpenCodeRuleEffect::Deny
        );
        assert_eq!(
            config.evaluate_policy("provider.use", "anthropic"),
            OpenCodeRuleEffect::Deny
        );
        assert_eq!(
            config.evaluate_policy("provider.use", "google"),
            OpenCodeRuleEffect::Deny
        );
        assert_eq!(
            config.evaluate_policy("mcp.use", "filesystem"),
            OpenCodeRuleEffect::Deny
        );

        // Authoritative CLI argv check: exact `--model`, no `--auto` or skip-permissions.
        let argv = spec
            .build_argv_for_workdir(&workdir)
            .expect("build_argv_for_workdir");
        assert_eq!(
            argv,
            vec![
                "--standalone",
                "run",
                "--model",
                "opencode-go/deepseek-v4.1-flash#max",
                "--format",
                "json",
                "--session",
                "sess-oc-220",
                "--",
                "Verify model and policy pins",
            ]
        );
        assert!(!argv
            .iter()
            .any(|arg| arg == "--auto" || arg.contains("skip-permissions") || arg == "--settings"));

        match mode {
            WorkerFilePermissions::Inherit => {
                assert!(
                    config.permissions.is_none(),
                    "Inherit mode must not inject file permission rules"
                );
                assert!(
                    !root.contains_key("permissions"),
                    "Inherit JSON must omit `permissions` key"
                );
            }
            WorkerFilePermissions::TaskFiles => {
                assert!(
                    config.permissions.is_some(),
                    "TaskFiles mode must populate V2 `permissions` rules"
                );
                assert!(root.get("permissions").and_then(Value::as_array).is_some());
            }
        }
    }

    // Verify model without variant omits `variant` field cleanly.
    let mut no_variant_spec =
        env.opencode_spec(bin, "No variant model", WorkerFilePermissions::Inherit);
    no_variant_spec.effort = None;
    let no_variant_json = no_variant_spec
        .build_opencode_config_for_workdir(&workdir)
        .expect("build config without variant");
    let no_variant_val: Value = serde_json::from_str(&no_variant_json).expect("parse json");
    assert!(no_variant_val["model"].get("variant").is_none());
    for agent_name in OPENCODE_PINNED_AGENTS {
        assert!(no_variant_val["agents"][agent_name]["model"]
            .get("variant")
            .is_none());
    }
}

#[test]
fn v2_relative_permission_schema_enforces_exact_and_directory_scopes_and_denies_metadata() {
    let env = OpenCodeTestEnv::new();
    let bin = env.write_fake_executable("schema-oc", "exit 0");
    let workdir = env.root_dir.join("v2-schema-workdir");
    fs::create_dir_all(workdir.join("crates/threadlane-workers/src")).expect("create workers src");
    fs::create_dir_all(workdir.join("crates/threadlane-core/src")).expect("create core src");
    fs::create_dir_all(workdir.join("docs")).expect("create docs");
    fs::write(workdir.join("docs/WORKERS.md"), "# Workers\n").expect("write WORKERS.md");

    // Deliberately include:
    // - `"crates/threadlane-workers/"` (explicit directory scope with trailing slash -> exact + `/**`)
    // - `"crates/threadlane-core"` (existing directory WITHOUT trailing slash -> exact only, NO `/**`)
    // - `"docs/WORKERS.md"` (exact file path without trailing slash -> exact only, NO `/**`)
    let mut spec = env.opencode_spec(
        bin,
        "Test V2 relative permission schema",
        WorkerFilePermissions::TaskFiles,
    );
    spec.allowed_write_paths = BTreeSet::from([
        "crates/threadlane-workers/".to_string(),
        "crates/threadlane-core".to_string(),
        "docs/WORKERS.md".to_string(),
    ]);

    let config = spec
        .build_opencode_config_content_for_workdir(&workdir)
        .expect("build V2 TaskFiles config");
    let rules = config
        .permissions
        .as_ref()
        .expect("TaskFiles permissions rules");

    // 1. First rule is baseline `{ action: "*", resource: "*", effect: "deny" }`.
    assert_eq!(
        rules.first(),
        Some(&OpenCodePermissionRule {
            action: "*".to_string(),
            resource: "*".to_string(),
            effect: OpenCodeRuleEffect::Deny,
        })
    );

    // 2. Internal resources are strictly location-relative (never contain workdir or repo_dir path).
    let workdir_str = workdir.to_str().expect("utf8 workdir");
    let repo_str = env.repo_dir.to_str().expect("utf8 repo");
    for rule in rules {
        assert!(
            !rule.resource.contains(workdir_str) && !rule.resource.contains(repo_str),
            "V2 permission rule resource `{}` must be location-relative, not absolute",
            rule.resource
        );
        assert!(
            !rule.resource.starts_with('/'),
            "V2 permission rule resource `{}` must not start with `/`",
            rule.resource
        );
        assert!(
            !matches!(rule.action.as_str(), "bash" | "task"),
            "V2 permission rule must use `shell`/`subagent` actions, not V1 `{}`",
            rule.action
        );
    }

    // 3. Verify exact vs explicit trailing-slash descendant `edit` allow rules.
    let edit_allows: Vec<&str> = rules
        .iter()
        .filter(|r| r.action == "edit" && r.effect == OpenCodeRuleEffect::Allow)
        .map(|r| r.resource.as_str())
        .collect();
    assert_eq!(
        edit_allows,
        vec![
            "crates/threadlane-core",
            "crates/threadlane-workers",
            "crates/threadlane-workers/**",
            "docs/WORKERS.md",
        ],
        "descendant `/**` rule must be granted ONLY for explicit trailing-slash directory scopes"
    );

    // 4. `write`, `edit`, and `patch` all map to `edit` action and respect exact vs trailing-slash scopes.
    for edit_action in ["edit", "write", "patch", "apply_patch", "multiedit"] {
        // Allowed under explicit trailing-slash directory scope `crates/threadlane-workers/`:
        assert_eq!(
            evaluate_opencode_rules(rules, edit_action, "crates/threadlane-workers"),
            OpenCodeRuleEffect::Allow
        );
        assert_eq!(
            evaluate_opencode_rules(rules, edit_action, "crates/threadlane-workers/src/lib.rs"),
            OpenCodeRuleEffect::Allow
        );
        assert_eq!(
            evaluate_opencode_rules(
                rules,
                edit_action,
                "crates/threadlane-workers/tests/nested/mod.rs"
            ),
            OpenCodeRuleEffect::Allow
        );

        // Allowed on exact path `docs/WORKERS.md`, denied on hypothetical descendants:
        assert_eq!(
            evaluate_opencode_rules(rules, edit_action, "docs/WORKERS.md"),
            OpenCodeRuleEffect::Allow
        );
        assert_eq!(
            evaluate_opencode_rules(rules, edit_action, "docs/WORKERS.md/sub"),
            OpenCodeRuleEffect::Deny
        );
        assert_eq!(
            evaluate_opencode_rules(rules, edit_action, "docs/ROADMAP.md"),
            OpenCodeRuleEffect::Deny
        );

        // `crates/threadlane-core` had NO trailing slash -> exact match allowed, descendants denied!
        assert_eq!(
            evaluate_opencode_rules(rules, edit_action, "crates/threadlane-core"),
            OpenCodeRuleEffect::Allow
        );
        assert_eq!(
            evaluate_opencode_rules(rules, edit_action, "crates/threadlane-core/src/lib.rs"),
            OpenCodeRuleEffect::Deny
        );

        // Unrelated scopes denied:
        assert_eq!(
            evaluate_opencode_rules(rules, edit_action, "Cargo.toml"),
            OpenCodeRuleEffect::Deny
        );
        assert_eq!(
            evaluate_opencode_rules(rules, edit_action, "crates/threadlane/src/main.rs"),
            OpenCodeRuleEffect::Deny
        );

        // Protected control paths and metadata are denied even inside an allowed trailing-slash directory!
        for protected_target in [
            "AGENTS.md",
            "crates/threadlane-workers/AGENTS.md",
            "crates/threadlane-workers/CLAUDE.md",
            "crates/threadlane-workers/.gitignore",
            "crates/threadlane-workers/.gitattributes",
            "crates/threadlane-workers/.gitmodules",
            "crates/threadlane-workers/opencode.json",
            "crates/threadlane-workers/opencode.jsonc",
            "crates/threadlane-workers/.env",
            "crates/threadlane-workers/.env.local",
            "crates/threadlane-workers/.git/config",
            "crates/threadlane-workers/.opencode/config.json",
            "crates/threadlane-workers/.codex/config.toml",
            "crates/threadlane-workers/.gemini/settings.json",
            "crates/threadlane-workers/.agents/skill.md",
            "crates/threadlane-workers/.claude/settings.json",
            ".git/HEAD",
            ".github/workflows/ci.yml",
            ".cargo/config.toml",
            "opencode.json",
        ] {
            assert_eq!(
                evaluate_opencode_rules(rules, edit_action, protected_target),
                OpenCodeRuleEffect::Deny,
                "`{edit_action}` on protected path `{protected_target}` must be denied"
            );
        }
    }

    // 5. Internal `read`, `glob`, `grep` are allowed on normal repository files, but denied on
    // `.env` and protected agent/git control directories, and denied on external/traversal paths.
    for inspect_action in ["read", "glob", "grep"] {
        for allowed_read in [
            "crates/threadlane-workers/src/lib.rs",
            "crates/threadlane-core/src/lib.rs",
            "docs/ROADMAP.md",
            "AGENTS.md",
            "Cargo.toml",
        ] {
            assert_eq!(
                evaluate_opencode_rules(rules, inspect_action, allowed_read),
                OpenCodeRuleEffect::Allow,
                "`{inspect_action}` on `{allowed_read}` should be allowed"
            );
        }

        for denied_read in [
            ".env",
            ".env.local",
            ".env.production",
            "crates/threadlane-workers/.env",
            "crates/threadlane-workers/.env.test",
            ".git",
            ".git/config",
            "crates/threadlane-workers/.git/HEAD",
            ".opencode",
            ".opencode/config.json",
            ".codex/auth.json",
            ".gemini/settings.json",
            ".agents/config.json",
            ".claude/settings.json",
            "/etc/passwd",
            "../outside-repo/secret.txt",
            "crates/../../outside.txt",
        ] {
            assert_eq!(
                evaluate_opencode_rules(rules, inspect_action, denied_read),
                OpenCodeRuleEffect::Deny,
                "`{inspect_action}` on `{denied_read}` must be denied"
            );
        }
    }

    // 6. Non-file and external actions (`shell`, `subagent`, `execute`, `web`, `external_directory`) are denied.
    for denied_action in [
        "shell",
        "bash",
        "subagent",
        "task",
        "execute",
        "web",
        "webfetch",
        "websearch",
        "external_directory",
    ] {
        assert_eq!(
            evaluate_opencode_rules(rules, denied_action, "*"),
            OpenCodeRuleEffect::Deny,
            "action `{denied_action}` must be denied by baseline"
        );
        assert_eq!(
            evaluate_opencode_rules(rules, denied_action, "crates/threadlane-workers/src/lib.rs"),
            OpenCodeRuleEffect::Deny,
            "action `{denied_action}` must be denied on internal paths"
        );
    }

    // 7. Agent-specific rules append after global rules and last matching rule wins.
    let mut custom_config: OpenCodeConfigContent = config;
    custom_config.agents.insert(
        "build".to_string(),
        OpenCodeAgentConfig {
            model: custom_config.model.clone(),
            permissions: Some(vec![OpenCodePermissionRule {
                action: "edit".to_string(),
                resource: "crates/threadlane-workers/src/locked.rs".to_string(),
                effect: OpenCodeRuleEffect::Deny,
            }]),
        },
    );
    assert_eq!(
        custom_config.evaluate_permission(
            Some("build"),
            "edit",
            "crates/threadlane-workers/src/lib.rs"
        ),
        OpenCodeRuleEffect::Allow
    );
    assert_eq!(
        custom_config.evaluate_permission(
            Some("build"),
            "edit",
            "crates/threadlane-workers/src/locked.rs"
        ),
        OpenCodeRuleEffect::Deny
    );
}

#[tokio::test]
async fn rejects_protected_controls_metacharacters_symlinks_and_bounds_before_spawn() {
    let env = OpenCodeTestEnv::new();
    let marker_file = env.root_dir.join("oc_must_not_spawn.marker");
    let spy_bin = env.write_fake_executable(
        "spy-oc",
        &format!(
            "printf 'spawned\\n' > '{}'\nexit 0\n",
            marker_file.display()
        ),
    );
    let clean_workdir = env.root_dir.join("clean-oc-workdir");
    fs::create_dir_all(clean_workdir.join("crates/threadlane-workers/src"))
        .expect("create clean_workdir");
    fs::write(
        clean_workdir.join("crates/threadlane-workers/src/lib.rs"),
        "pub fn x() {}\n",
    )
    .expect("write lib.rs");

    // 1. Root, traversal, and protected control paths must be rejected in allowed_write_paths.
    let forbidden_paths = [
        "/",
        ".",
        "./",
        "..",
        "../",
        "/crates/threadlane-workers",
        "crates//threadlane-workers",
        "crates/threadlane-workers//",
        ".git",
        ".git/",
        ".git/config",
        ".gitignore",
        ".gitattributes",
        ".gitmodules",
        "AGENTS.md",
        "agents.md",
        "crates/threadlane-workers/AGENTS.md",
        "CLAUDE.md",
        "opencode.json",
        "opencode.jsonc",
        "crates/threadlane-workers/opencode.json",
        ".opencode",
        ".opencode/",
        ".opencode/config.json",
        ".codex",
        ".gemini",
        ".agents",
        ".claude",
        ".env",
        ".env.local",
        "crates/threadlane-workers/.env",
        ".github",
        ".github/workflows/ci.yml",
        ".cargo/config",
        ".cargo/config.toml",
    ];

    for bad_path in forbidden_paths {
        for mode in [
            WorkerFilePermissions::Inherit,
            WorkerFilePermissions::TaskFiles,
        ] {
            let mut bad_spec = env.opencode_spec(spy_bin.clone(), "Reject control path", mode);
            bad_spec.allowed_write_paths = BTreeSet::from([bad_path.to_string()]);
            assert!(
                bad_spec.validate().is_err(),
                "forbidden path `{bad_path}` must fail validate() in mode {mode:?}"
            );
            assert!(
                bad_spec
                    .build_opencode_config_for_workdir(&clean_workdir)
                    .is_err(),
                "forbidden path `{bad_path}` must fail build_opencode_config_for_workdir()"
            );
            assert!(
                run_worker(&bad_spec, &clean_workdir).await.is_err(),
                "forbidden path `{bad_path}` must fail run_worker() before spawn"
            );
            assert!(!marker_file.exists(), "spy_bin must never spawn");
        }
    }

    // 2. Trailing slash on an existing regular file must be rejected.
    let mut file_trailing_slash_spec = env.opencode_spec(
        spy_bin.clone(),
        "Reject trailing slash on file",
        WorkerFilePermissions::TaskFiles,
    );
    file_trailing_slash_spec.allowed_write_paths =
        BTreeSet::from(["crates/threadlane-workers/src/lib.rs/".to_string()]);
    assert!(matches!(
        file_trailing_slash_spec.build_opencode_config_for_workdir(&clean_workdir),
        Err(WorkerError::InvalidSpec(ref msg)) if msg.contains("non-directory")
    ));
    assert!(run_worker(&file_trailing_slash_spec, &clean_workdir)
        .await
        .is_err());
    assert!(!marker_file.exists());

    // 3. Unsafe glob/rule metacharacters in allowed_write_paths or working_directory must be rejected.
    for unsafe_entry in [
        "crates/threadlane-*",
        "crates/threadlane-?",
        "crates/[threadlane]",
        "crates/{threadlane}",
        "crates/(threadlane)",
        "crates/thread\\lane",
        "crates/thread\nlane",
        "crates/thread\tlane",
    ] {
        let mut bad_spec = env.opencode_spec(
            spy_bin.clone(),
            "Reject metacharacters",
            WorkerFilePermissions::TaskFiles,
        );
        bad_spec.allowed_write_paths = BTreeSet::from([unsafe_entry.to_string()]);
        assert!(bad_spec.validate().is_err());
        assert!(run_worker(&bad_spec, &clean_workdir).await.is_err());
        assert!(!marker_file.exists());
    }

    let valid_task_spec = env.opencode_spec(
        spy_bin.clone(),
        "Reject unsafe workdir",
        WorkerFilePermissions::TaskFiles,
    );
    for bad_dir_name in [
        "oc_workdir*glob",
        "oc_workdir?question",
        "oc_workdir[bracket]",
        "oc_workdir{brace}",
        "oc_workdir(paren)",
        "oc_workdir\\backslash",
    ] {
        let bad_workdir = env.root_dir.join(bad_dir_name);
        fs::create_dir_all(&bad_workdir).expect("create bad_workdir");
        assert!(valid_task_spec
            .build_opencode_config_for_workdir(&bad_workdir)
            .is_err());
        assert!(valid_task_spec
            .build_argv_for_workdir(&bad_workdir)
            .is_err());
        assert!(run_worker(&valid_task_spec, &bad_workdir).await.is_err());
        assert!(!marker_file.exists());
    }

    // 4. Symlink target inside working_directory must be rejected before spawn.
    let symlink_workdir = env.root_dir.join("oc-symlink-workdir");
    fs::create_dir_all(symlink_workdir.join("crates")).expect("create crates");
    symlink(
        &env.repo_dir,
        symlink_workdir.join("crates/threadlane-workers"),
    )
    .expect("create symlink");
    assert!(matches!(
        valid_task_spec.build_opencode_config_for_workdir(&symlink_workdir),
        Err(WorkerError::SecurityViolation(_))
    ));
    assert!(run_worker(&valid_task_spec, &symlink_workdir)
        .await
        .is_err());
    assert!(!marker_file.exists());

    // 5. Root directory `/` must be rejected.
    let resolved_model = ResolvedModelTarget {
        base_model: "opencode-go/deepseek-v4.1-flash".to_string(),
        effort: Some("max".to_string()),
        cli_model_arg: "opencode-go/deepseek-v4.1-flash#max".to_string(),
    };
    assert!(build_opencode_config_content(
        &resolved_model,
        WorkerFilePermissions::TaskFiles,
        Path::new("/"),
        &env.default_scoped_paths(),
    )
    .is_err());

    // 6. Enforce MAX_TASK_FILES_WRITE_PATHS (128) and MAX_OPENCODE_CONFIG_BYTES (65_536 / 64 KiB).
    let over_128_paths: BTreeSet<String> = (0..(MAX_TASK_FILES_WRITE_PATHS + 1))
        .map(|i| format!("crates/threadlane-workers/src/mod_{i:03}.rs"))
        .collect();
    let mut over_128_spec = env.opencode_spec(
        spy_bin.clone(),
        "Reject >128 scopes",
        WorkerFilePermissions::TaskFiles,
    );
    over_128_spec.allowed_write_paths = over_128_paths.clone();
    assert!(matches!(
        over_128_spec.validate(),
        Err(WorkerError::InvalidSpec(ref msg)) if msg.contains("128")
    ));
    assert!(run_worker(&over_128_spec, &clean_workdir).await.is_err());
    assert!(!marker_file.exists());

    // 128 long trailing-slash directory entries whose serialized JSON exceeds 65,536 bytes.
    let seg = "d".repeat(120);
    let over_64k_paths: BTreeSet<String> = (0..120)
        .map(|i| format!("crates/threadlane-workers/{seg}/{seg}/dir_{i:03}/"))
        .collect();
    let mut over_64k_spec = env.opencode_spec(
        spy_bin,
        "Reject >64KiB OPENCODE_CONFIG_CONTENT",
        WorkerFilePermissions::TaskFiles,
    );
    over_64k_spec.allowed_write_paths = over_64k_paths.clone();
    over_64k_spec
        .validate()
        .expect("120 valid entries pass static count check");
    assert!(matches!(
        build_opencode_config_content_json(
            &resolved_model,
            WorkerFilePermissions::TaskFiles,
            &clean_workdir,
            &over_64k_paths,
        ),
        Err(WorkerError::InvalidSpec(ref msg)) if msg.contains(&MAX_OPENCODE_CONFIG_BYTES.to_string())
    ));
    assert!(matches!(
        over_64k_spec.build_opencode_config_for_workdir(&clean_workdir),
        Err(WorkerError::InvalidSpec(ref msg)) if msg.contains("65536")
    ));
    assert!(matches!(
        run_worker(&over_64k_spec, &clean_workdir).await,
        Err(WorkerError::InvalidSpec(ref msg)) if msg.contains("65536")
    ));
    assert!(!marker_file.exists());
}

#[tokio::test]
async fn spawned_fake_opencode_receives_generated_env_overriding_hostile_ambient_while_preserving_gates_and_agy(
) {
    let env = OpenCodeTestEnv::new();

    // Set a hostile ambient OPENCODE_CONFIG_CONTENT in the parent process to verify that:
    // 1. The OpenCode child receives the Threadlane-generated V2 config (overriding the hostile value).
    // 2. The parent process environment variable remains untouched.
    // 3. Antigravity workers do not receive Threadlane OpenCode config overrides.
    let hostile_ambient =
        r#"{"default_agent":"rogue","model":{"provider":"evil","model":"unapproved"}}"#;
    std::env::set_var(OPENCODE_CONFIG_ENV_VAR, hostile_ambient);

    let fake_oc = env.write_fake_executable(
        "fake-opencode-v2",
        r#"
printf '%s' "${OPENCODE_CONFIG_CONTENT:-}" > crates/threadlane-workers/received_opencode_env.json
for arg in "$@"; do
    printf '%s\n' "$arg" >> crates/threadlane-workers/received_opencode_argv.txt
done
printf 'pub fn updated_by_opencode_v2() -> u32 { 7 }\n' >> crates/threadlane-workers/src/lib.rs
printf '{"type":"step_start","sessionID":"sess-oc-220","model":"opencode-go/deepseek-v4.1-flash"}\n'
printf '{"type":"text","sessionID":"sess-oc-220","text":"Updated lib.rs"}\n'
printf '{"type":"step_finish","sessionID":"sess-oc-220","model":"opencode-go/deepseek-v4.1-flash","reason":"stop"}\n'
"#,
    );

    let oc_spec = env.opencode_spec(
        fake_oc,
        "Update crates/threadlane-workers/src/lib.rs",
        WorkerFilePermissions::TaskFiles,
    );
    assert!(is_path_allowed(
        "crates/threadlane-workers/src/lib.rs",
        &oc_spec.allowed_write_paths
    ));

    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let task_report = execute_worker_task(&oc_spec, "run-oc-v2", "task-1", None, &mut cancel_rx)
        .await
        .expect("execute_worker_task for OpenCode V2 TaskFiles");

    // Parent ambient environment variable must remain completely untouched.
    assert_eq!(
        std::env::var(OPENCODE_CONFIG_ENV_VAR).as_deref(),
        Ok(hostile_ambient),
        "parent process ambient OPENCODE_CONFIG_CONTENT must not be mutated"
    );

    let checkout_path = &task_report.manifest.checkout_path;
    let received_env = fs::read_to_string(
        checkout_path.join("crates/threadlane-workers/received_opencode_env.json"),
    )
    .expect("read received_opencode_env.json");
    assert_ne!(received_env, hostile_ambient);

    let expected_env = oc_spec
        .build_opencode_config_for_workdir(checkout_path)
        .expect("expected OpenCode config JSON");
    assert_eq!(received_env, expected_env);

    let parsed_child_cfg: OpenCodeConfigContent =
        serde_json::from_str(&received_env).expect("deserialize child OpenCodeConfigContent");
    assert_eq!(parsed_child_cfg.default_agent, "build");
    assert_eq!(parsed_child_cfg.model.provider, "opencode-go");
    assert_eq!(parsed_child_cfg.model.model, "deepseek-v4.1-flash");
    assert_eq!(parsed_child_cfg.model.variant.as_deref(), Some("max"));
    assert!(parsed_child_cfg.permissions.is_some());

    // Honest evidence check: OpenCode 2.2.0 JSON stream does not echo variant/effort, so status
    // must remain MissingEffortEvidence / FinishedNeedsEvidenceAndReview.
    assert_eq!(
        task_report.worker_report.evidence.effort_evidence,
        FieldEvidence::Missing {
            requested: "max".to_string()
        }
    );
    assert_eq!(
        task_report.worker_report.status,
        WorkerRunStatus::MissingEffortEvidence
    );
    assert!(task_report.diff_gate.passed);
    assert_eq!(
        task_report.overall_acceptance_state,
        WorkAcceptanceState::FinishedNeedsEvidenceAndReview
    );

    // Post-run Git diff gate still rejects unauthorized writes outside allowed_write_paths.
    let rogue_oc = env.write_fake_executable(
        "rogue-opencode-v2",
        r#"
printf '// unauthorized edit\n' >> crates/threadlane-core/src/lib.rs
printf '{"type":"step_start","sessionID":"sess-oc-220","model":"opencode-go/deepseek-v4.1-flash","variant":"max"}\n'
printf '{"type":"step_finish","sessionID":"sess-oc-220","model":"opencode-go/deepseek-v4.1-flash","variant":"max","reason":"stop"}\n'
"#,
    );
    let rogue_spec = env.opencode_spec(
        rogue_oc,
        "Attempt unauthorized write",
        WorkerFilePermissions::TaskFiles,
    );
    let rogue_report =
        execute_worker_task(&rogue_spec, "run-oc-v2", "task-rogue", None, &mut cancel_rx)
            .await
            .expect("execute rogue OpenCode worker task");
    assert!(!rogue_report.diff_gate.passed);
    assert_eq!(
        rogue_report.overall_acceptance_state,
        WorkAcceptanceState::RejectedByDiffGate
    );

    // Existing Antigravity behavior is unchanged and does not inject OPENCODE_CONFIG_CONTENT.
    let fake_agy = env.write_fake_executable(
        "fake-agy",
        r#"
printf '%s' "${OPENCODE_CONFIG_CONTENT:-}" > crates/threadlane-workers/agy_seen_opencode_env.txt
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-argon-high","effort":"high","result":"ok"}\n'
"#,
    );
    let agy_spec = env.antigravity_spec(
        fake_agy,
        "Verify AGY unchanged",
        WorkerFilePermissions::TaskFiles,
    );
    assert!(agy_spec
        .build_child_env_for_workdir(&env.repo_dir)
        .expect("AGY build_child_env_for_workdir")
        .is_empty());

    let agy_report = execute_worker_task(&agy_spec, "run-agy", "task-agy", None, &mut cancel_rx)
        .await
        .expect("execute AGY worker task");
    assert_eq!(agy_report.worker_report.status, WorkerRunStatus::Succeeded);
    let agy_seen_env = fs::read_to_string(
        agy_report
            .manifest
            .checkout_path
            .join("crates/threadlane-workers/agy_seen_opencode_env.txt"),
    )
    .expect("read agy_seen_opencode_env.txt");
    assert_eq!(agy_seen_env, hostile_ambient);

    std::env::remove_var(OPENCODE_CONFIG_ENV_VAR);
}
