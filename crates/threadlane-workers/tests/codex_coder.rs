use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::os::unix::fs::{symlink, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use tempfile::TempDir;
use tokio::sync::watch;

use threadlane_workers::{
    build_codex_task_files_permission_profile_config,
    build_codex_task_files_permission_profile_config_for_scopes,
    evaluate_codex_task_files_permission_profile_config, execute_worker_task,
    extract_pinned_codex_task_files_write_scopes, is_pinned_codex_read_only_argv,
    is_pinned_codex_task_files_argv, is_process_alive,
    parse_and_validate_codex_task_files_permission_profile_config, resolve_model_target,
    run_worker, run_worker_with_cancel, validate_codex_task_files_scope_str,
    validate_codex_task_files_workdir, AdvisoryWorkerSpec, CodexTurnLifecycleEvidence,
    DiffViolationKind, ExecutionBudgets, FieldEvidence, NdjsonStreamParser, OpenCodeRuleEffect,
    StreamTokenUsage, WorkAcceptanceState, WorkerError, WorkerErrorCode, WorkerFilePermissions,
    WorkerProvider, WorkerRunStatus, WorkerSpec, WriteScopeKind, CODEX_ADVISORY_MAX_WORKDIR_DEPTH,
    CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG, CODEX_ADVISORY_PERMISSION_PROFILE_NAME,
    CODEX_TASK_FILES_PERMISSION_PROFILE_NAME, CODEX_TASK_FILES_PROTECTED_DENY_PATTERNS,
};

fn write_executable_script(dir: &Path, name: &str, script_body: &str) -> PathBuf {
    let staging_path = dir.join(format!(".{name}.tmp"));
    let path = dir.join(name);
    {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o755)
            .open(&staging_path)
            .expect("open staging executable script");
        file.write_all(script_body.as_bytes())
            .expect("write executable script");
        file.sync_all().expect("sync executable script");
    }
    fs::rename(&staging_path, &path).expect("rename executable script into place");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755))
        .expect("chmod 0755 executable script");
    path
}

fn init_git_repo(repo_dir: &Path) -> String {
    fs::create_dir_all(repo_dir.join("src/nested")).expect("create src/nested");
    fs::create_dir_all(repo_dir.join("docs")).expect("create docs");
    fs::write(
        repo_dir.join("src/lib.rs"),
        "pub fn greeting() -> &'static str {\n    \"before\"\n}\n",
    )
    .expect("write initial src/lib.rs");
    fs::write(
        repo_dir.join("src/nested/mod.rs"),
        "pub const VERSION: u32 = 1;\n",
    )
    .expect("write initial src/nested/mod.rs");
    fs::write(repo_dir.join("docs/README.md"), "# Test Repo\n").expect("write docs/README.md");

    let run_git = |args: &[&str]| {
        let out = Command::new("git")
            .args(args)
            .current_dir(repo_dir)
            .env("GIT_AUTHOR_NAME", "Threadlane Test")
            .env("GIT_AUTHOR_EMAIL", "test@example.invalid")
            .env("GIT_COMMITTER_NAME", "Threadlane Test")
            .env("GIT_COMMITTER_EMAIL", "test@example.invalid")
            .output()
            .expect("spawn git");
        assert!(
            out.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout)
            .expect("utf8 git stdout")
            .trim()
            .to_string()
    };

    run_git(&["init", "-b", "main"]);
    run_git(&["add", "."]);
    run_git(&["commit", "-m", "Initial commit"]);
    run_git(&["rev-parse", "HEAD"])
}

fn sample_codex_coder_spec(
    executable: PathBuf,
    repository_path: PathBuf,
    base_commit: String,
    work_area_root: PathBuf,
    target: (&str, &str),
    allowed_write_paths: BTreeSet<String>,
    prompt: &str,
) -> WorkerSpec {
    WorkerSpec {
        provider: WorkerProvider::CodexCli,
        executable,
        model: target.0.to_string(),
        effort: Some(target.1.to_string()),
        allowed_models: BTreeSet::from([target.0.to_string()]),
        session_id: None,
        standalone: false,
        repository_path,
        base_commit,
        work_area_root,
        allowed_write_paths,
        prompt: prompt.to_string(),
        file_permissions: WorkerFilePermissions::TaskFiles,
        budgets: ExecutionBudgets::default(),
    }
}

#[test]
fn codex_task_files_launch_pins_model_effort_ignore_rules_scopes_and_preserves_advisory_argv() {
    let temp = TempDir::new().expect("tempdir");
    let repo_dir = temp.path().join("repo");
    let work_area = temp.path().join("work-area");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&work_area).expect("create work_area");
    fs::set_permissions(&work_area, fs::Permissions::from_mode(0o700)).expect("private work area");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");
    let base_commit = init_git_repo(&repo_dir);
    let fake_codex = write_executable_script(&bin_dir, "fake-codex", "#!/usr/bin/env sh\nexit 0\n");

    // 1. Validate both primary target (gpt-6-luna / max) and generalized Codex models/efforts.
    for (model, effort) in [
        ("gpt-6-luna", "max"),
        ("gpt-6-luna", "high"),
        ("gpt-6.1-sol", "xhigh"),
    ] {
        let allowed_write_paths =
            BTreeSet::from(["src/lib.rs".to_string(), "src/nested/".to_string()]);
        let spec = sample_codex_coder_spec(
            fake_codex.clone(),
            repo_dir.clone(),
            base_commit.clone(),
            work_area.clone(),
            (model, effort),
            allowed_write_paths,
            "Implement approved task changes in src/lib.rs.",
        );

        let report = spec
            .validate()
            .expect("validate Codex TaskFiles WorkerSpec");
        assert_eq!(report.provider, WorkerProvider::CodexCli);
        assert_eq!(report.file_permissions, WorkerFilePermissions::TaskFiles);
        assert!(report.workdir_argv_resolution_required);
        assert!(report
            .argv_resolution_note
            .as_deref()
            .is_some_and(|note| note.contains(CODEX_TASK_FILES_PERMISSION_PROFILE_NAME)));
        assert_eq!(report.resolved_model.base_model, model);
        assert_eq!(report.resolved_model.cli_model_arg, model);
        assert_eq!(report.resolved_model.effort.as_deref(), Some(effort));

        // Static build_argv() must fail closed because TaskFiles requires workdir/base_commit resolution.
        let static_err = spec
            .build_argv()
            .expect_err("build_argv without workdir must fail for Codex TaskFiles");
        assert_eq!(static_err.code(), WorkerErrorCode::InvalidSpec);

        // Static preview argv must NOT pass the launch-pinned verifier because it lacks the
        // workdir-resolved `permissions.threadlane-task-files={...}` inline config.
        assert!(!is_pinned_codex_task_files_argv(
            &report.constructed_argv,
            &report.resolved_model
        ));
        assert!(!is_pinned_codex_read_only_argv(
            &report.constructed_argv,
            &report.resolved_model
        ));

        // Workdir-resolved argv must include exact flags (including --ignore-rules),
        // model/effort, named profile, and disabled features.
        let resolved_argv = spec
            .build_argv_for_workdir(&repo_dir)
            .expect("build_argv_for_workdir for Codex TaskFiles");
        let child_env = spec
            .build_child_env_for_workdir(&repo_dir)
            .expect("build_child_env_for_workdir for Codex TaskFiles");
        assert!(child_env.is_empty());

        assert!(is_pinned_codex_task_files_argv(
            &resolved_argv,
            &report.resolved_model
        ));
        assert!(!is_pinned_codex_read_only_argv(
            &resolved_argv,
            &report.resolved_model
        ));

        let extracted_scopes =
            extract_pinned_codex_task_files_write_scopes(&resolved_argv, &report.resolved_model)
                .expect("extract write scopes from pinned Codex TaskFiles argv");
        assert_eq!(
            extracted_scopes,
            BTreeMap::from([
                ("src/lib.rs".to_string(), WriteScopeKind::ExactFile),
                ("src/nested".to_string(), WriteScopeKind::DirectorySubtree),
            ])
        );

        assert_eq!(resolved_argv[0], "--no-daemon");
        assert_eq!(resolved_argv[1], "exec");
        assert_eq!(resolved_argv[2], "--json");
        assert_eq!(resolved_argv[3], "--ephemeral");
        assert_eq!(resolved_argv[4], "--ignore-user-config");
        assert_eq!(resolved_argv[5], "--ignore-rules");
        assert_eq!(resolved_argv[6], "--model");
        assert_eq!(resolved_argv[7], model);
        assert_eq!(resolved_argv[8], "-c");
        assert_eq!(
            resolved_argv[9],
            format!("model_reasoning_effort=\"{effort}\"")
        );
        assert_eq!(resolved_argv[10], "-c");
        assert_eq!(resolved_argv[11], "approval_policy=\"never\"");
        assert_eq!(resolved_argv[12], "-c");
        assert_eq!(resolved_argv[13], "model_provider=\"openai\"");
        assert_eq!(resolved_argv[14], "-c");
        assert_eq!(
            resolved_argv[15],
            format!("default_permissions=\"{CODEX_TASK_FILES_PERMISSION_PROFILE_NAME}\"")
        );
        assert_eq!(resolved_argv[16], "-c");
        assert!(resolved_argv[17].starts_with("permissions.threadlane-task-files={"));
        assert_eq!(resolved_argv[18], "-c");
        assert_eq!(resolved_argv[19], "web_search=\"disabled\"");

        // Omitting --ignore-rules must fail the strict mutable argv verifier
        let mut without_ignore_rules = resolved_argv.clone();
        without_ignore_rules.remove(5);
        assert!(!is_pinned_codex_task_files_argv(
            &without_ignore_rules,
            &report.resolved_model
        ));
        for widened in [
            resolved_argv[17].replace("\".\"=\"read\"", "\".\"=\"write\""),
            resolved_argv[17].replace(
                "\"src/lib.rs\"=\"write\"",
                "\"src/lib.rs\"=\"write\",\".\"=\"write\"",
            ),
            resolved_argv[17].replace(
                "\"src/lib.rs\"=\"write\"",
                "\"src/bad\\\"quote.rs\"=\"write\"",
            ),
        ] {
            let mut tampered = resolved_argv.clone();
            tampered[17] = widened;
            assert!(extract_pinned_codex_task_files_write_scopes(
                &tampered,
                &report.resolved_model
            )
            .is_none());
        }

        assert!(
            !resolved_argv.iter().any(|arg| {
                arg == "--sandbox"
                    || arg == "workspace-write"
                    || arg == "danger-full-access"
                    || arg.contains("bypass")
                    || arg.contains("full-auto")
                    || arg.contains("yolo")
            }),
            "Codex TaskFiles argv must never include legacy --sandbox or blanket write/bypass flags"
        );
    }

    // 2. Verify read-only AdvisoryWorkerSpec argv remains 100% unchanged and mutually exclusive.
    let advisory_spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::CodexCli,
        executable: fake_codex,
        model: "gpt-6.1-sol".to_string(),
        effort: Some("xhigh".to_string()),
        allowed_models: BTreeSet::from(["gpt-6.1-sol".to_string()]),
        session_id: None,
        standalone: false,
        working_directory: repo_dir,
        allowed_write_paths: BTreeSet::new(),
        prompt: "Read-only advisory check.".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    };
    let adv_report = advisory_spec.validate().expect("validate advisory spec");
    assert!(is_pinned_codex_read_only_argv(
        &adv_report.constructed_argv,
        &adv_report.resolved_model
    ));
    assert!(!is_pinned_codex_task_files_argv(
        &adv_report.constructed_argv,
        &adv_report.resolved_model
    ));
    assert_eq!(
        adv_report.constructed_argv[14],
        format!("default_permissions=\"{CODEX_ADVISORY_PERMISSION_PROFILE_NAME}\"")
    );
    assert_eq!(
        adv_report.constructed_argv[16],
        CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG
    );
}

#[test]
fn codex_task_files_permission_profile_evaluates_bounded_writes_and_rejects_tampering_and_toml_injection(
) {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("checkout");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(workdir.join("src/sub")).expect("create src/sub");

    let scopes = BTreeMap::from([
        ("src/lib.rs".to_string(), WriteScopeKind::ExactFile),
        ("src/sub".to_string(), WriteScopeKind::DirectorySubtree),
    ]);
    let config = build_codex_task_files_permission_profile_config_for_scopes(&workdir, &scopes)
        .expect("build Codex TaskFiles permission profile config");

    let parsed = parse_and_validate_codex_task_files_permission_profile_config(&config)
        .expect("parse and validate generated Codex TaskFiles profile");
    assert_eq!(parsed, scopes);

    // Also test syntactic builder helper
    let syntactic_config = build_codex_task_files_permission_profile_config(
        &workdir,
        &BTreeSet::from(["src/lib.rs".to_string(), "src/sub/".to_string()]),
    )
    .expect("build syntactic Codex TaskFiles permission profile config");
    assert_eq!(syntactic_config, config);

    // Direct tests for double-quote filename, comma filename, equals filename, and TOML injection payloads:
    for injection_scope in [
        "src/bad\"quote.rs",
        "src/bad,comma.rs",
        "src/bad=equals.rs",
        "src/bad'single.rs",
        "src/bad:colon.rs",
        "src/bad;semi.rs",
        "src/bad#hash.rs",
        "src/bad space.rs",
        "src/ok.rs\"=\"write\",\".\"=\"write",
        "src/ok.rs\",\".\"=\"all",
    ] {
        assert_eq!(
            validate_codex_task_files_scope_str(injection_scope, "allowed_write_paths entry")
                .expect_err("TOML delimiter or injection scope must fail closed")
                .code(),
            WorkerErrorCode::InvalidSpec,
            "scope={injection_scope}"
        );
        assert!(
            build_codex_task_files_permission_profile_config(
                &workdir,
                &BTreeSet::from([injection_scope.to_string()])
            )
            .is_err(),
            "syntactic builder must reject {injection_scope}"
        );
        assert!(
            build_codex_task_files_permission_profile_config_for_scopes(
                &workdir,
                &BTreeMap::from([(injection_scope.to_string(), WriteScopeKind::ExactFile)])
            )
            .is_err(),
            "scopes builder must reject {injection_scope}"
        );
    }

    // Evaluate read and write access across workspace, exact file, directory subtree, siblings, and protected paths.
    assert_eq!(
        evaluate_codex_task_files_permission_profile_config(&config, "read", "README.md")
            .expect("eval read README.md"),
        OpenCodeRuleEffect::Allow
    );
    assert_eq!(
        evaluate_codex_task_files_permission_profile_config(&config, "read", "/nix/store/pkg/bin")
            .expect("eval read /nix/store"),
        OpenCodeRuleEffect::Allow
    );
    assert_eq!(
        evaluate_codex_task_files_permission_profile_config(&config, "write", "src/lib.rs")
            .expect("eval write exact approved file"),
        OpenCodeRuleEffect::Allow
    );
    assert_eq!(
        evaluate_codex_task_files_permission_profile_config(
            &config,
            "write",
            "src/sub/nested/module.rs"
        )
        .expect("eval write inside approved directory subtree"),
        OpenCodeRuleEffect::Allow
    );

    // Sibling/unapproved files and descendant of ExactFile must be denied for write.
    for unapproved in [
        "README.md",
        "src/other.rs",
        "src/lib.rs/child.rs",
        "../outside.rs",
        "/tmp/outside.rs",
    ] {
        assert_eq!(
            evaluate_codex_task_files_permission_profile_config(&config, "write", unapproved)
                .expect("eval unapproved write"),
            OpenCodeRuleEffect::Deny,
            "unapproved write path `{unapproved}` must be denied"
        );
    }

    // Protected paths must be denied for both read and write, even inside an approved directory subtree!
    for protected in [
        ".git/config",
        ".codex/config.toml",
        ".agents/skills/run.sh",
        ".aws/credentials",
        ".env",
        ".env.local",
        "codex.toml",
        ".codex.toml",
        "AGENTS.md",
        "src/.aws/credentials",
        "src/.cargo/config.toml",
        "CLAUDE.md",
        "opencode.json",
        "opencode.jsonc",
        ".gemini/settings.json",
        ".opencode/config.json",
        ".claude/settings.json",
        ".github/workflows/ci.yml",
        ".cargo/config.toml",
        "src/sub/.env.production",
        "src/sub/AGENTS.md",
        "src/sub/.codex/hooks.toml",
    ] {
        assert!(
            build_codex_task_files_permission_profile_config_for_scopes(
                &workdir,
                &BTreeMap::from([(protected.to_string(), WriteScopeKind::ExactFile)])
            )
            .is_err(),
            "protected scope {protected} must fail before rendering"
        );
        assert_eq!(
            evaluate_codex_task_files_permission_profile_config(&config, "write", protected)
                .expect("eval protected write"),
            OpenCodeRuleEffect::Deny,
            "protected path `{protected}` must be denied for write"
        );
        assert_eq!(
            evaluate_codex_task_files_permission_profile_config(&config, "read", protected)
                .expect("eval protected read"),
            OpenCodeRuleEffect::Deny,
            "protected path `{protected}` must be denied for read"
        );
    }

    let too_deep_scope = format!(
        "{}file.rs",
        "nested/".repeat(CODEX_ADVISORY_MAX_WORKDIR_DEPTH)
    );
    assert!(
        build_codex_task_files_permission_profile_config_for_scopes(
            &workdir,
            &BTreeMap::from([(too_deep_scope, WriteScopeKind::ExactFile)])
        )
        .is_err(),
        "nonexistent deep scopes must fail before launch"
    );

    // Tampered configs and injected extra rules must fail parse_and_validate_codex_task_files_permission_profile_config.
    let tampered_cases = [
        config.replace("\".\"=\"read\"", "\".\"=\"write\""),
        config.replace(
            "\"src/lib.rs\"=\"write\"",
            "\"src/lib.rs\"=\"write\",\".\"=\"write\"",
        ),
        config.replace(
            "\"src/lib.rs\"=\"write\"",
            "\"src/bad\\\"quote.rs\"=\"write\"",
        ),
        config.replace("\"src/lib.rs\"=\"write\"", "\"src/bad,comma.rs\"=\"write\""),
        config.replace("\"enabled\"=false", "\"enabled\"=true"),
        config.replace("\"glob_scan_max_depth\"=64", "\"glob_scan_max_depth\"=128"),
        config.replace("\"**/.git\"=\"deny\",", ""),
        config.replace("\"/nix/store\"=\"read\"", "\"/nix/store\"=\"write\""),
        CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG.to_string(),
    ];
    for tampered in tampered_cases {
        assert!(
            parse_and_validate_codex_task_files_permission_profile_config(&tampered).is_err(),
            "tampered Codex TaskFiles profile config must be rejected: {tampered}"
        );
    }
    assert!(!CODEX_TASK_FILES_PROTECTED_DENY_PATTERNS.is_empty());
}

#[test]
fn codex_task_files_fails_closed_on_missing_effort_inherit_protected_paths_symlinks_and_workdir_hazards(
) {
    let temp = TempDir::new().expect("tempdir");
    let repo_dir = temp.path().join("repo");
    let work_area = temp.path().join("work-area");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&work_area).expect("create work_area");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");
    let base_commit = init_git_repo(&repo_dir);
    let fake_codex = write_executable_script(&bin_dir, "fake-codex", "#!/usr/bin/env sh\nexit 0\n");

    let base_spec = sample_codex_coder_spec(
        fake_codex,
        repo_dir,
        base_commit,
        work_area,
        ("gpt-6-luna", "max"),
        BTreeSet::from(["src/lib.rs".to_string()]),
        "Update src/lib.rs.",
    );

    // 1. Missing effort fails closed
    let mut no_effort = base_spec.clone();
    no_effort.effort = None;
    assert_eq!(
        no_effort.validate().unwrap_err().code(),
        WorkerErrorCode::InvalidSpec
    );

    // 2. Inline #variant or provider/model slash fails closed for CodexCli
    for bad_model in ["gpt-6-luna#max", "openai/gpt-6-luna", "gpt-6-luna --yolo"] {
        let mut bad_model_spec = base_spec.clone();
        bad_model_spec.model = bad_model.to_string();
        bad_model_spec.allowed_models = BTreeSet::from([bad_model.to_string()]);
        assert_eq!(
            bad_model_spec.validate().unwrap_err().code(),
            WorkerErrorCode::InvalidSpec
        );
    }

    // 3. Inherit mode fails closed with UnsupportedCapability; ReadOnly fails on WorkerSpec with InvalidSpec
    let mut inherit_spec = base_spec.clone();
    inherit_spec.file_permissions = WorkerFilePermissions::Inherit;
    let inherit_err = inherit_spec
        .validate()
        .expect_err("CodexCli must refuse Inherit on WorkerSpec");
    assert_eq!(inherit_err.code(), WorkerErrorCode::UnsupportedCapability);
    assert!(matches!(
        inherit_err,
        WorkerError::UnsupportedCapability { .. }
    ));

    let mut readonly_spec = base_spec.clone();
    readonly_spec.file_permissions = WorkerFilePermissions::ReadOnly;
    assert_eq!(
        readonly_spec.validate().unwrap_err().code(),
        WorkerErrorCode::InvalidSpec
    );

    // 4. session_id and standalone fail closed on CodexCli WorkerSpec
    let mut with_session = base_spec.clone();
    with_session.session_id = Some("ses_123".to_string());
    assert_eq!(
        with_session.validate().unwrap_err().code(),
        WorkerErrorCode::InvalidSpec
    );
    let mut with_standalone = base_spec.clone();
    with_standalone.standalone = true;
    assert_eq!(
        with_standalone.validate().unwrap_err().code(),
        WorkerErrorCode::InvalidSpec
    );

    // 5. Invalid, traversal, glob-metacharacter, TOML delimiter/injection, or protected write scopes fail closed
    for bad_scope in [
        "",
        ".",
        "./src/lib.rs",
        "../src/lib.rs",
        "src/../lib.rs",
        "/etc/passwd",
        "src/*.rs",
        "src/[a-z].rs",
        "src/bad\"quote.rs",
        "src/bad,comma.rs",
        "src/bad=equals.rs",
        "src/ok.rs\"=\"write\",\".\"=\"write",
        ".git/config",
        ".codex/config.toml",
        ".agents/skill.md",
        ".env",
        ".env.local",
        "AGENTS.md",
        "CLAUDE.md",
        "codex.toml",
        ".github/workflows/ci.yml",
        ".cargo/config.toml",
    ] {
        let mut bad_scope_spec = base_spec.clone();
        bad_scope_spec.allowed_write_paths = BTreeSet::from([bad_scope.to_string()]);
        assert!(
            bad_scope_spec.validate().is_err(),
            "bad write scope `{bad_scope}` must be rejected"
        );
    }

    // 6. Symlink inside working directory or project config marker fails workdir validation
    let hazard_dir = temp.path().join("hazard-workdir");
    fs::create_dir_all(hazard_dir.join(".git")).expect("create .git");
    fs::create_dir_all(hazard_dir.join("src")).expect("create src");
    let symlink_path = hazard_dir.join("src/escape_link");
    symlink("/tmp", &symlink_path).expect("create symlink");
    let symlink_err = validate_codex_task_files_workdir(&hazard_dir)
        .expect_err("symlink inside workdir must be rejected");
    assert_eq!(symlink_err.code(), WorkerErrorCode::InvalidSpec);
    fs::remove_file(&symlink_path).expect("remove symlink");

    // Project config marker `.codex` inside workdir must be rejected before spawn
    fs::create_dir_all(hazard_dir.join(".codex")).expect("create .codex dir");
    let marker_err = validate_codex_task_files_workdir(&hazard_dir)
        .expect_err(".codex directory inside workdir must be rejected");
    assert_eq!(marker_err.code(), WorkerErrorCode::InvalidSpec);
    fs::remove_dir_all(hazard_dir.join(".codex")).expect("remove .codex");

    // Excessive directory depth fails closed at the actual permission scanner safety bound.
    let mut deep = hazard_dir.clone();
    for idx in 0..=CODEX_ADVISORY_MAX_WORKDIR_DEPTH {
        deep = deep.join(format!("d{idx}"));
    }
    fs::create_dir_all(&deep).expect("create deep dir");
    let depth_err = validate_codex_task_files_workdir(&hazard_dir)
        .expect_err("excessive workdir depth must be rejected");
    assert_eq!(depth_err.code(), WorkerErrorCode::InvalidSpec);
}

#[test]
fn codex_stream_parser_tracks_bounded_lifecycle_without_retained_events_and_rejects_malformed_ordering(
) {
    let resolved = resolve_model_target(WorkerProvider::CodexCli, "gpt-6-luna", Some("max"))
        .expect("resolve gpt-6-luna/max");

    // 1. Valid turn with 300 item.completed events under max_events=8 (dropping 294 events from
    //    retained_events) still validates in O(1) state via CodexTurnLifecycleEvidence.
    let mut parser =
        NdjsonStreamParser::new_for_provider(WorkerProvider::CodexCli, resolved.clone(), None, 8);
    parser.ingest_line(b"{\"type\":\"thread.started\",\"thread_id\":\"thread-bounded-1\"}");
    parser.ingest_line(b"{\"type\":\"turn.started\"}");
    for idx in 1..=300 {
        let line = format!(
            "{{\"type\":\"item.completed\",\"item\":{{\"id\":\"item_{idx}\",\"type\":\"agent_message\",\"text\":\"Step {idx}\"}}}}"
        );
        parser.ingest_line(line.as_bytes());
    }
    parser.ingest_line(
        b"{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":1000,\"output_tokens\":200}}",
    );
    let summary = parser.finalize();
    assert_eq!(summary.retained_events.len(), 8);
    assert_eq!(summary.dropped_events_count, 295);
    assert!(summary.saw_init_event);
    assert!(summary.saw_result_event);
    assert!(summary.has_valid_codex_turn_lifecycle());
    assert_eq!(
        summary.codex_turn_lifecycle,
        Some(CodexTurnLifecycleEvidence {
            thread_started_count: 1,
            turn_started_count: 1,
            turn_completed_count: 1,
            item_events_count: 300,
            trailing_events_after_completion_count: 0,
            violation: None,
        })
    );

    // 2. Rejected ordering / framing / truncation / duplicate-terminal / post-terminal shapes
    let rejected_streams: &[(&str, &[&str])] = &[
        (
            "turn_completed_before_thread_started",
            &[
                "{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}",
                "{\"type\":\"thread.started\",\"thread_id\":\"thread-order-1\"}",
                "{\"type\":\"turn.started\"}",
            ],
        ),
        (
            "turn_completed_without_turn_started",
            &[
                "{\"type\":\"thread.started\",\"thread_id\":\"thread-no-turn-start\"}",
                "{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}",
            ],
        ),
        (
            "turn_started_before_thread_started",
            &[
                "{\"type\":\"turn.started\"}",
                "{\"type\":\"thread.started\",\"thread_id\":\"thread-late\"}",
                "{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}",
            ],
        ),
        (
            "duplicate_turn_started",
            &[
                "{\"type\":\"thread.started\",\"thread_id\":\"thread-dup-turn\"}",
                "{\"type\":\"turn.started\"}",
                "{\"type\":\"turn.started\"}",
                "{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}",
            ],
        ),
        (
            "duplicate_turn_completed",
            &[
                "{\"type\":\"thread.started\",\"thread_id\":\"thread-dup-complete\"}",
                "{\"type\":\"turn.started\"}",
                "{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}",
                "{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}",
            ],
        ),
        (
            "trailing_event_after_turn_completed",
            &[
                "{\"type\":\"thread.started\",\"thread_id\":\"thread-trailing\"}",
                "{\"type\":\"turn.started\"}",
                "{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}",
                "{\"type\":\"item.completed\",\"item\":{\"id\":\"item_late\",\"type\":\"agent_message\",\"text\":\"late\"}}",
            ],
        ),
        (
            "item_before_turn_started",
            &[
                "{\"type\":\"thread.started\",\"thread_id\":\"thread-early-item\"}",
                "{\"type\":\"item.completed\",\"item\":{\"id\":\"item_early\",\"type\":\"agent_message\",\"text\":\"early\"}}",
                "{\"type\":\"turn.started\"}",
                "{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}",
            ],
        ),
        (
            "truncated_turn_completed",
            &[
                "{\"type\":\"thread.started\",\"thread_id\":\"thread-trunc\"}",
                "{\"type\":\"turn.started\"}",
                "{\"type\":\"turn.completed\",\"stop_reason\":\"max_tokens\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}",
            ],
        ),
        (
            "partial_item_completed",
            &[
                "{\"type\":\"thread.started\",\"thread_id\":\"thread-partial\"}",
                "{\"type\":\"turn.started\"}",
                "{\"type\":\"item.completed\",\"item\":{\"id\":\"item_1\",\"type\":\"agent_message\",\"partial\":true,\"text\":\"half\"}}",
                "{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}",
            ],
        ),
    ];

    for (case_name, lines) in rejected_streams {
        let mut p = NdjsonStreamParser::new_for_provider(
            WorkerProvider::CodexCli,
            resolved.clone(),
            None,
            64,
        );
        for line in *lines {
            p.ingest_line(line.as_bytes());
        }
        let s = p.finalize();
        assert!(
            !s.has_valid_codex_turn_lifecycle(),
            "case `{case_name}` must fail codex_turn_lifecycle validation"
        );
        assert!(
            !s.saw_result_event,
            "case `{case_name}` must not set saw_result_event"
        );
        assert!(
            s.observed_usage.is_none(),
            "case `{case_name}` must not retain usage from an invalid turn"
        );
        assert!(
            s.codex_turn_lifecycle
                .as_ref()
                .and_then(|lc| lc.violation.as_ref())
                .is_some(),
            "case `{case_name}` must record a lifecycle violation"
        );
    }
}

#[tokio::test]
async fn fake_codex_coder_executes_in_isolated_worktree_and_reaches_ready_for_review_without_invented_observed_metadata(
) {
    let temp = TempDir::new().expect("tempdir");
    let repo_dir = temp.path().join("repo");
    let work_area = temp.path().join("work-area");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&work_area).expect("create work_area");
    fs::set_permissions(&work_area, fs::Permissions::from_mode(0o700)).expect("private work area");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");
    let base_commit = init_git_repo(&repo_dir);

    let fake_codex_script = r#"#!/usr/bin/env sh
set -eu
if [ "$1" != "--no-daemon" ] || [ "$2" != "exec" ] || [ "$3" != "--json" ] || [ "$4" != "--ephemeral" ] || [ "$5" != "--ignore-user-config" ] || [ "$6" != "--ignore-rules" ]; then
    echo "unexpected leading flags: $*" >&2
    exit 2
fi
if [ "$7" != "--model" ] || [ "$8" != "gpt-6-luna" ]; then
    echo "unexpected model args: $7 $8" >&2
    exit 3
fi
cat <<'EOF' > src/lib.rs
pub fn greeting() -> &'static str {
    "updated by fake codex luna coder"
}
EOF
printf '%s\n' '{"type":"thread.started","thread_id":"019cfa00-1111-7222-8333-444455556666"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item_1","type":"agent_message","text":"Updated src/lib.rs within approved TaskFiles scope."}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":1420,"cached_input_tokens":512,"output_tokens":285}}'
"#;
    let fake_codex = write_executable_script(&bin_dir, "fake-codex-ok", fake_codex_script);

    let spec = sample_codex_coder_spec(
        fake_codex,
        repo_dir,
        base_commit,
        work_area,
        ("gpt-6-luna", "max"),
        BTreeSet::from(["src/lib.rs".to_string()]),
        "Update greeting() in src/lib.rs.",
    );

    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let task_report =
        execute_worker_task(&spec, "run-luna-01", "task-luna-01", None, &mut cancel_rx)
            .await
            .expect("execute_worker_task for Codex TaskFiles");

    assert_eq!(
        task_report.diff_gate.acceptance_state,
        WorkAcceptanceState::DiffGatePassedPendingReview
    );
    assert!(task_report.diff_gate.passed);
    assert!(task_report.diff_gate.violations.is_empty());
    assert_eq!(task_report.diff_gate.changed_paths.len(), 1);
    assert_eq!(task_report.diff_gate.changed_paths[0].path, "src/lib.rs");

    let worker_report = &task_report.worker_report;
    assert_eq!(worker_report.provider, WorkerProvider::CodexCli);
    assert_eq!(worker_report.status, WorkerRunStatus::Succeeded);
    assert_eq!(worker_report.exit_code, Some(0));
    assert!(worker_report.has_pinned_codex_task_files_launch_config());
    assert!(!worker_report.has_pinned_codex_read_only_launch_config());
    assert!(worker_report.has_pinned_codex_launch_config());
    assert_eq!(
        worker_report.codex_launch_pin_evidence_mode(),
        Some("codex_task_files_launch_pin")
    );
    assert!(worker_report.has_satisfied_model_evidence());
    assert!(worker_report.has_satisfied_effort_evidence());
    assert!(worker_report.has_satisfied_session_evidence());
    assert!(worker_report.has_satisfied_result_evidence());
    assert!(worker_report.has_complete_natural_completion_evidence());
    assert!(worker_report.evidence.has_valid_codex_turn_lifecycle());
    let mut cancelled_after_result = worker_report.clone();
    cancelled_after_result.status = WorkerRunStatus::Cancelled;
    assert!(!cancelled_after_result.has_complete_natural_completion_evidence());

    // Crucial honesty invariant: observed model and effort remain None when absent from stream;
    // requested values are NEVER copied into observed fields.
    assert_eq!(worker_report.requested_model.base_model, "gpt-6-luna");
    assert_eq!(worker_report.requested_model.effort.as_deref(), Some("max"));
    assert_eq!(worker_report.effective_observed_model(), None);
    assert_eq!(worker_report.effective_observed_effort(), None);
    assert!(matches!(
        worker_report.evidence.model_evidence,
        FieldEvidence::Missing { .. }
    ));
    assert!(matches!(
        worker_report.evidence.effort_evidence,
        FieldEvidence::Missing { .. }
    ));
    assert_eq!(
        worker_report.evidence.observed_session_id.as_deref(),
        Some("019cfa00-1111-7222-8333-444455556666")
    );
    assert_eq!(
        worker_report.evidence.observed_usage,
        Some(StreamTokenUsage {
            input_tokens: 1420,
            cached_input_tokens: Some(512),
            cache_write_input_tokens: None,
            output_tokens: 285,
            reasoning_output_tokens: None,
        })
    );
}

#[tokio::test]
async fn fake_codex_coder_rejects_forbidden_writes_malformed_ordering_and_cleans_up_on_cancel() {
    let temp = TempDir::new().expect("tempdir");
    let repo_dir = temp.path().join("repo");
    let work_area = temp.path().join("work-area");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&work_area).expect("create work_area");
    fs::set_permissions(&work_area, fs::Permissions::from_mode(0o700)).expect("private work area");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");
    let base_commit = init_git_repo(&repo_dir);

    let mode_file = temp.path().join("codex_mode.txt");
    let child_pid_file = temp.path().join("codex_child_pid.txt");
    let script = format!(
        r#"#!/usr/bin/env sh
set -eu
MODE="$(cat "{mode_file}")"
case "$MODE" in
    forbidden_write)
        printf 'unapproved modification\n' >> docs/README.md
        printf '%s\n' '{{"type":"thread.started","thread_id":"019cfa00-1111-7222-8333-444455556666"}}'
        printf '%s\n' '{{"type":"turn.started"}}'
        printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":10,"output_tokens":5}}}}'
        exit 0
        ;;
    missing_terminal)
        printf '%s\n' '{{"type":"thread.started","thread_id":"019cfa00-1111-7222-8333-444455556666"}}'
        printf '%s\n' '{{"type":"turn.started"}}'
        exit 0
        ;;
    missing_turn_started)
        printf '%s\n' '{{"type":"thread.started","thread_id":"019cfa00-1111-7222-8333-444455556666"}}'
        printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":10,"output_tokens":5}}}}'
        exit 0
        ;;
    turn_completed_before_thread)
        printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":10,"output_tokens":5}}}}'
        printf '%s\n' '{{"type":"thread.started","thread_id":"019cfa00-1111-7222-8333-444455556666"}}'
        printf '%s\n' '{{"type":"turn.started"}}'
        exit 0
        ;;
    duplicate_turn_completed)
        printf '%s\n' '{{"type":"thread.started","thread_id":"019cfa00-1111-7222-8333-444455556666"}}'
        printf '%s\n' '{{"type":"turn.started"}}'
        printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":10,"output_tokens":5}}}}'
        printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":10,"output_tokens":5}}}}'
        exit 0
        ;;
    event_after_terminal)
        printf '%s\n' '{{"type":"thread.started","thread_id":"019cfa00-1111-7222-8333-444455556666"}}'
        printf '%s\n' '{{"type":"turn.started"}}'
        printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":10,"output_tokens":5}}}}'
        printf '%s\n' '{{"type":"item.completed","item":{{"id":"item_late","type":"agent_message","text":"after terminal"}}}}'
        exit 0
        ;;
    truncated_terminal)
        printf '%s\n' '{{"type":"thread.started","thread_id":"019cfa00-1111-7222-8333-444455556666"}}'
        printf '%s\n' '{{"type":"turn.started"}}'
        printf '%s\n' '{{"type":"turn.completed","stop_reason":"max_tokens","usage":{{"input_tokens":10,"output_tokens":5}}}}'
        exit 0
        ;;
    duplicate_thread)
        printf '%s\n' '{{"type":"thread.started","thread_id":"019cfa00-1111-7222-8333-444455556666"}}'
        printf '%s\n' '{{"type":"thread.started","thread_id":"019cfa00-1111-7222-8333-444455556666"}}'
        printf '%s\n' '{{"type":"turn.started"}}'
        printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":10,"output_tokens":5}}}}'
        exit 0
        ;;
    conflicting_thread)
        printf '%s\n' '{{"type":"thread.started","thread_id":"019cfa00-1111-7222-8333-444455556666"}}'
        printf '%s\n' '{{"type":"turn.started","thread_id":"019cfa00-9999-7222-8333-444455556666"}}'
        printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":10,"output_tokens":5}}}}'
        exit 0
        ;;
    turn_failed)
        printf '%s\n' '{{"type":"thread.started","thread_id":"019cfa00-1111-7222-8333-444455556666"}}'
        printf '%s\n' '{{"type":"turn.started"}}'
        printf '%s\n' '{{"type":"turn.failed","error":{{"message":"rate limit or upstream failure"}}}}'
        exit 0
        ;;
    malformed_line)
        printf '%s\n' '{{"type":"thread.started","thread_id":"019cfa00-1111-7222-8333-444455556666"}}'
        printf '%s\n' '{{not-valid-json'
        printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":10,"output_tokens":5}}}}'
        exit 0
        ;;
    nonzero_exit)
        printf '%s\n' '{{"type":"thread.started","thread_id":"019cfa00-1111-7222-8333-444455556666"}}'
        exit 17
        ;;
    hang_with_child)
        printf '%s\n' '{{"type":"thread.started","thread_id":"019cfa00-1111-7222-8333-444455556666"}}'
        sleep 60 &
        CHILD_PID=$!
        printf '%s\n' "$CHILD_PID" > "{child_pid_file}"
        wait "$CHILD_PID"
        ;;
esac
"#,
        mode_file = mode_file.display(),
        child_pid_file = child_pid_file.display(),
    );
    let fake_codex = write_executable_script(&bin_dir, "fake-codex-modes", &script);

    let spec = sample_codex_coder_spec(
        fake_codex,
        repo_dir.clone(),
        base_commit,
        work_area,
        ("gpt-6-luna", "max"),
        BTreeSet::from(["src/lib.rs".to_string()]),
        "Run failure mode check.",
    );

    // 1. Forbidden write outside allowed_write_paths fails post-run diff gate
    fs::write(&mode_file, "forbidden_write").expect("write mode");
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let forbidden_report = execute_worker_task(
        &spec,
        "run-fail-diff",
        "task-fail-diff",
        None,
        &mut cancel_rx,
    )
    .await
    .expect("execute_worker_task forbidden_write");
    assert_eq!(
        forbidden_report.diff_gate.acceptance_state,
        WorkAcceptanceState::RejectedByDiffGate
    );
    assert!(!forbidden_report.diff_gate.passed);
    assert!(forbidden_report
        .diff_gate
        .violations
        .iter()
        .any(|v| v.path == "docs/README.md" && v.kind == DiffViolationKind::OutsideAllowedPaths));

    // 2. Stream/lifecycle failure modes fail closed and never satisfy natural completion evidence
    for (mode, expected_status) in [
        ("missing_terminal", WorkerRunStatus::MissingResultEvidence),
        (
            "missing_turn_started",
            WorkerRunStatus::MissingResultEvidence,
        ),
        (
            "turn_completed_before_thread",
            WorkerRunStatus::SessionMismatch,
        ),
        (
            "duplicate_turn_completed",
            WorkerRunStatus::MissingResultEvidence,
        ),
        (
            "event_after_terminal",
            WorkerRunStatus::MissingResultEvidence,
        ),
        ("truncated_terminal", WorkerRunStatus::MissingResultEvidence),
        ("duplicate_thread", WorkerRunStatus::SessionMismatch),
        ("conflicting_thread", WorkerRunStatus::SessionMismatch),
        ("turn_failed", WorkerRunStatus::ProviderError),
        ("malformed_line", WorkerRunStatus::MalformedStream),
        ("nonzero_exit", WorkerRunStatus::NonZeroExit),
    ] {
        fs::write(&mode_file, mode).expect("write mode");
        let report = run_worker(&spec, &repo_dir)
            .await
            .expect("run_worker failure case");
        assert_eq!(report.status, expected_status, "mode={mode}");
        assert!(
            !report.has_complete_natural_completion_evidence(),
            "mode={mode} must not satisfy natural completion evidence"
        );
    }

    // 3. Cancellation terminates the process group including background child processes
    fs::write(&mode_file, "hang_with_child").expect("write hang mode");
    let (cancel_tx, mut cancel_rx) = watch::channel(false);
    let child_pid_path = child_pid_file.clone();
    tokio::spawn(async move {
        for _ in 0..50 {
            if child_pid_path.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let _ = cancel_tx.send(true);
    });

    let cancel_report = run_worker_with_cancel(&spec, &repo_dir, &mut cancel_rx)
        .await
        .expect("run_worker_with_cancel");
    assert_eq!(cancel_report.status, WorkerRunStatus::Cancelled);
    assert!(!cancel_report.cleanup_incomplete);
    assert!(!cancel_report.has_complete_natural_completion_evidence());

    let pid = fs::read_to_string(&child_pid_file)
        .expect("cancellation fixture must have spawned a descendant")
        .trim()
        .parse::<u32>()
        .expect("descendant PID");
    assert!(
        !is_process_alive(pid),
        "cancelled descendant {pid} must be terminated"
    );

    // Timeout uses the same process-tree cleanup and cannot become natural completion.
    fs::remove_file(&child_pid_file).expect("clear cancellation PID");
    let mut timeout_spec = spec.clone();
    timeout_spec.budgets.timeout_ms = 300;
    let timeout_report = run_worker(&timeout_spec, &repo_dir)
        .await
        .expect("timed worker");
    assert_eq!(timeout_report.status, WorkerRunStatus::TimedOut);
    assert!(!timeout_report.cleanup_incomplete);
    assert!(!timeout_report.has_complete_natural_completion_evidence());
    let pid = fs::read_to_string(&child_pid_file)
        .expect("timeout fixture must have spawned a descendant")
        .trim()
        .parse::<u32>()
        .expect("descendant PID");
    assert!(
        !is_process_alive(pid),
        "timed-out descendant {pid} must be terminated"
    );
}

#[test]
fn codex_lifecycle_rejects_malformed_and_conflicting_identity_without_retained_events() {
    use serde_json::json;
    let resolved = resolve_model_target(WorkerProvider::CodexCli, "gpt-6-luna", Some("max"))
        .expect("resolve target");
    let valid = vec![
        json!({"type":"thread.started","thread_id":"thread-1"}),
        json!({"type":"turn.started"}),
        json!({"type":"item.started","item":{"id":"item-1","type":"agent_message"}}),
        json!({"type":"item.updated","item":{"id":"item-1","type":"agent_message","text":"working"}}),
        json!({"type":"item.completed","item":{"id":"item-1","type":"agent_message","text":"done"}}),
        json!({"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":2,"output_tokens":5}}),
    ];
    let parse = |events: &[serde_json::Value]| {
        let mut parser = NdjsonStreamParser::new_for_provider(
            WorkerProvider::CodexCli,
            resolved.clone(),
            None,
            0,
        );
        for event in events {
            parser.ingest_line(event.to_string().as_bytes());
        }
        parser.finalize()
    };
    let accepted = parse(&valid);
    assert!(accepted.has_valid_codex_turn_lifecycle());
    assert!(accepted.retained_events.is_empty());
    assert_eq!(accepted.observed_usage.as_ref().unwrap().output_tokens, 5);

    for (index, replacement) in [
        (0, json!({"type":"thread.started","session_id":"thread-1"})),
        (
            0,
            json!({"type":"thread.started","thread_id":"thread-1","session_id":"spoof"}),
        ),
        (
            0,
            json!({"type":"thread.started","thread_id":"x".repeat(129)}),
        ),
        (1, json!({"type":"turn.started","thread_id":"thread-2"})),
        (1, json!({"type":"turn.started","thread_id":null})),
        (
            2,
            json!({"type":"item.started","item":{"type":"agent_message"}}),
        ),
        (
            4,
            json!({"type":"item.completed","item":{"id":"item-1","type":"error","message":"failed"}}),
        ),
        (5, json!({"type":"turn.completed","status":12})),
        (5, json!({"type":"turn.completed","status":"failed"})),
        (
            5,
            json!({"type":"turn.completed","usage":{"input_tokens":-1,"output_tokens":5}}),
        ),
        (
            5,
            json!({"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":"5"}}),
        ),
        (5, json!({"type":"turn.completed","truncated":true})),
    ] {
        let mut events = valid.clone();
        events[index] = replacement;
        let rejected = parse(&events);
        assert!(!rejected.has_valid_codex_turn_lifecycle(), "{events:?}");
        assert!(!rejected.saw_result_event, "{events:?}");
        assert!(rejected.observed_usage.is_none(), "{events:?}");
    }
    let mut parser =
        NdjsonStreamParser::new_for_provider(WorkerProvider::CodexCli, resolved, None, 0);
    for event in &valid {
        parser.ingest_line(event.to_string().as_bytes());
    }
    parser.ingest_line(b"not-json-after-completion");
    let rejected = parser.finalize();
    assert!(!rejected.has_valid_codex_turn_lifecycle());
    assert!(!rejected.saw_result_event);
    assert!(rejected.observed_usage.is_none());
}

#[tokio::test]
async fn codex_bare_file_scope_cannot_widen_at_a_derived_base_before_launch() {
    let temp = TempDir::new().expect("tempdir");
    let repo = temp.path().join("repo");
    let base = init_git_repo(&repo);
    let executable = write_executable_script(
        temp.path(),
        "fake-codex",
        "#!/usr/bin/env sh\nprintf launched > LAUNCHED\n",
    );
    let mut spec = sample_codex_coder_spec(
        executable,
        repo.clone(),
        base,
        temp.path().join("work-area"),
        ("gpt-6-luna", "max"),
        BTreeSet::from(["future".to_string()]),
        "Update future.",
    );
    let original_argv = spec
        .build_argv_for_workdir(&repo)
        .expect("absent bare file scope");
    let target = spec.validate().expect("valid spec").resolved_model;
    assert_eq!(
        extract_pinned_codex_task_files_write_scopes(&original_argv, &target),
        Some(BTreeMap::from([(
            "future".to_string(),
            WriteScopeKind::ExactFile
        )]))
    );
    // An accepted predecessor may create a directory in the derived integration base.
    fs::create_dir(repo.join("future")).expect("derived directory");
    fs::write(repo.join("future/child.rs"), "derived child").expect("derived child");
    for args in [
        vec!["add", "."],
        vec![
            "-c",
            "user.name=Threadlane Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-m",
            "Derived predecessor",
        ],
    ] {
        assert!(Command::new("git")
            .args(args)
            .current_dir(&repo)
            .status()
            .expect("git")
            .success());
    }
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(&repo)
        .output()
        .expect("derived SHA");
    assert!(output.status.success());
    spec.base_commit = String::from_utf8(output.stdout)
        .expect("SHA UTF-8")
        .trim()
        .to_string();
    assert_eq!(
        run_worker(&spec, &repo)
            .await
            .expect_err("bare scope may not widen")
            .code(),
        WorkerErrorCode::InvalidSpec
    );
    assert!(
        !repo.join("LAUNCHED").exists(),
        "worker must not launch after scope-kind drift"
    );
    spec.allowed_write_paths = BTreeSet::from(["future/".to_string()]);
    let argv = spec
        .build_argv_for_workdir(&repo)
        .expect("explicit directory remains supported");
    assert_eq!(
        extract_pinned_codex_task_files_write_scopes(&argv, &target),
        Some(BTreeMap::from([(
            "future".to_string(),
            WriteScopeKind::DirectorySubtree
        )]))
    );
}
