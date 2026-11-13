use std::collections::BTreeSet;
use std::ffi::{CString, OsStr};
use std::fs;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{symlink, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};

use tempfile::TempDir;

use threadlane_workers::{
    build_codex_advisory_exact_permission_profile_config, collect_codex_advisory_deny_frontiers,
    is_pinned_codex_read_only_argv, is_pinned_codex_task_files_argv,
    parse_and_validate_codex_advisory_exact_permission_profile_config,
    validate_codex_advisory_workdir, AdvisoryWorkerSpec, ExecutionBudgets, WorkerErrorCode,
    WorkerFilePermissions, WorkerProvider, CODEX_ADVISORY_GLOB_SCAN_DEPTH_MARGIN,
    CODEX_ADVISORY_GLOB_SCAN_MAX_DEPTH, CODEX_ADVISORY_MANDATORY_ROOT_DENY_PATHS,
    CODEX_ADVISORY_MAX_EXACT_DENY_RULES, CODEX_ADVISORY_MAX_WORKDIR_DEPTH,
    CODEX_ADVISORY_MAX_WORKDIR_ENTRIES, CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG,
    MAX_TASK_FILES_SETTINGS_BYTES,
};

/// Exact earlier bounded static read-only profile retained for 26 successful historical planning
/// artifacts. This is a literal evidence vector, not an alias to the current public constant.
const HISTORICAL_BOUNDED_READ_ONLY_PROFILE: &str = r#"permissions.threadlane-advisory={"filesystem"={"glob_scan_max_depth"=64,":minimal"="read",":workspace_roots"={"."="read","**/.env"="deny","**/.env.*"="deny","**/codex.toml"="deny","**/.codex.toml"="deny","**/.git"="deny","**/.git/**"="deny","**/.codex"="deny","**/.codex/**"="deny","**/.agents"="deny","**/.agents/**"="deny","**/.aws"="deny","**/.aws/**"="deny"},"/nix/store"="read"},"network"={"enabled"=false}}"#;

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

fn sample_codex_advisory_spec(
    executable: PathBuf,
    working_directory: PathBuf,
) -> AdvisoryWorkerSpec {
    AdvisoryWorkerSpec {
        provider: WorkerProvider::CodexCli,
        executable,
        model: "gpt-6.1-sol".to_string(),
        effort: Some("xhigh".to_string()),
        allowed_models: BTreeSet::from(["gpt-6.1-sol".to_string()]),
        session_id: None,
        standalone: false,
        working_directory,
        allowed_write_paths: BTreeSet::new(),
        prompt: "Inspect workspace under read-only advisory profile.".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    }
}

fn create_file_at_relative_depth(
    root: &Path,
    first_component: Option<&str>,
    depth: usize,
) -> PathBuf {
    assert!(depth >= 1, "relative component depth must be at least 1");
    let mut current = root.to_path_buf();
    for idx in 1..depth {
        let dir_name = if idx == 1 {
            first_component
                .map(str::to_string)
                .unwrap_or_else(|| format!("d{idx:02}"))
        } else {
            format!("d{idx:02}")
        };
        current = current.join(dir_name);
    }
    fs::create_dir_all(&current).expect("create nested directories");
    let leaf_name = if depth == 1 {
        first_component
            .map(str::to_string)
            .unwrap_or_else(|| "leaf.txt".to_string())
    } else {
        "leaf.txt".to_string()
    };
    let file_path = current.join(leaf_name);
    fs::write(&file_path, "fixture\n").expect("write leaf file");
    file_path
}

fn assert_exact_frontier_has_no_redundant_descendants(frontiers: &BTreeSet<String>) {
    for key in frontiers {
        let mut end = key.len();
        while let Some(slash) = key[..end].rfind('/') {
            let ancestor = &key[..slash];
            assert!(
                !frontiers.contains(ancestor),
                "exact frontier `{key}` redundantly contains descendant of denied ancestor `{ancestor}`"
            );
            end = slash;
        }
    }
}

fn exact_profile(validation: &threadlane_workers::AdvisorySpecValidationReport) -> &str {
    &validation.constructed_argv[16]
}

fn expected_exact_profile(frontiers: &[&str]) -> String {
    let exact_frontiers: BTreeSet<String> =
        frontiers.iter().map(|value| (*value).to_string()).collect();
    build_codex_advisory_exact_permission_profile_config(&exact_frontiers)
        .expect("render expected exact advisory profile")
}

#[test]
fn codex_advisory_profile_pins_glob_scan_max_depth_64_and_rejects_modified_or_missing_depth() {
    assert_eq!(CODEX_ADVISORY_GLOB_SCAN_MAX_DEPTH, 64);
    assert_eq!(CODEX_ADVISORY_GLOB_SCAN_DEPTH_MARGIN, 2);
    assert_eq!(CODEX_ADVISORY_MAX_WORKDIR_DEPTH, 62);
    assert_eq!(CODEX_ADVISORY_MAX_WORKDIR_ENTRIES, 32_768);

    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workspace");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let fake_codex = write_executable_script(&bin_dir, "fake-codex", "#!/usr/bin/env sh\nexit 0\n");
    let spec = sample_codex_advisory_spec(fake_codex, workdir);
    let validation = spec.validate().expect("validate spec");
    let profile = exact_profile(&validation);

    assert!(is_pinned_codex_read_only_argv(
        &validation.constructed_argv,
        &validation.resolved_model
    ));
    assert_eq!(
        profile,
        expected_exact_profile(&[".git"]).as_str(),
        "simple rooted workspace must render deterministic mandatory roots plus exact .git"
    );
    assert!(parse_and_validate_codex_advisory_exact_permission_profile_config(profile).is_ok());

    // Absent `glob_scan_max_depth` in profile must fail exact pinned-argv validation.
    let mut absent_depth_argv = validation.constructed_argv.clone();
    absent_depth_argv[16] = profile.replace("\"glob_scan_max_depth\"=64,", "");
    assert!(!is_pinned_codex_read_only_argv(
        &absent_depth_argv,
        &validation.resolved_model
    ));
    assert!(
        parse_and_validate_codex_advisory_exact_permission_profile_config(&absent_depth_argv[16])
            .is_err()
    );

    // Edited `glob_scan_max_depth` values must fail exact pinned-argv validation.
    for edited_depth in ["8", "62", "63", "65", "0"] {
        let mut edited_argv = validation.constructed_argv.clone();
        edited_argv[16] = profile.replace(
            "\"glob_scan_max_depth\"=64",
            &format!("\"glob_scan_max_depth\"={edited_depth}"),
        );
        assert!(!is_pinned_codex_read_only_argv(
            &edited_argv,
            &validation.resolved_model
        ));
        assert!(
            parse_and_validate_codex_advisory_exact_permission_profile_config(&edited_argv[16])
                .is_err()
        );
    }
}

#[test]
fn codex_advisory_workdir_accepts_exact_max_depth_62_and_rejects_depth_63() {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workspace");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(workdir.join(".git/objects/info")).expect("create .git");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let fake_codex = write_executable_script(&bin_dir, "fake-codex", "#!/usr/bin/env sh\nexit 0\n");
    let spec = sample_codex_advisory_spec(fake_codex, workdir.clone());

    // Relative component depth == 62 (`CODEX_ADVISORY_MAX_WORKDIR_DEPTH`) must succeed.
    let exact_max_file =
        create_file_at_relative_depth(&workdir, None, CODEX_ADVISORY_MAX_WORKDIR_DEPTH);
    assert_eq!(
        exact_max_file
            .strip_prefix(&workdir)
            .expect("relative to workdir")
            .components()
            .count(),
        CODEX_ADVISORY_MAX_WORKDIR_DEPTH
    );
    validate_codex_advisory_workdir(&workdir).expect("entry at exact max depth 62 must pass");
    spec.validate()
        .expect("spec validation at exact max depth 62 must pass");

    // One component deeper (depth == 63) must fail closed before provider invocation.
    let too_deep_file =
        create_file_at_relative_depth(&workdir, None, CODEX_ADVISORY_MAX_WORKDIR_DEPTH + 1);
    assert_eq!(
        too_deep_file
            .strip_prefix(&workdir)
            .expect("relative to workdir")
            .components()
            .count(),
        CODEX_ADVISORY_MAX_WORKDIR_DEPTH + 1
    );
    let err = validate_codex_advisory_workdir(&workdir)
        .expect_err("entry at depth 63 must be rejected before launch");
    assert_eq!(err.code(), WorkerErrorCode::InvalidSpec);
    let msg = err.to_string();
    assert!(
        msg.contains("relative component depth 63")
            && msg.contains("maximum allowed workdir depth 62")
            && msg.contains("glob_scan_max_depth=64"),
        "unexpected depth error message: {msg}"
    );
    assert_eq!(
        spec.validate().unwrap_err().code(),
        WorkerErrorCode::InvalidSpec
    );
}

#[test]
fn codex_advisory_workdir_does_not_prune_git_or_uppercase_git_trees() {
    for git_dir_name in [".git", ".GIT"] {
        let temp = TempDir::new().expect("tempdir");
        let workdir = temp.path().join("workspace");
        let bin_dir = temp.path().join("bin");
        fs::create_dir_all(workdir.join(".git")).expect("create canonical .git root");
        fs::create_dir_all(&bin_dir).expect("create bin_dir");

        let fake_codex =
            write_executable_script(&bin_dir, "fake-codex", "#!/usr/bin/env sh\nexit 0\n");
        let spec = sample_codex_advisory_spec(fake_codex, workdir.clone());

        // `.git`/`.GIT` descendant at exact max depth (62) passes.
        let at_limit = create_file_at_relative_depth(
            &workdir,
            Some(git_dir_name),
            CODEX_ADVISORY_MAX_WORKDIR_DEPTH,
        );
        assert_eq!(
            at_limit
                .strip_prefix(&workdir)
                .expect("relative to workdir")
                .components()
                .count(),
            CODEX_ADVISORY_MAX_WORKDIR_DEPTH
        );
        validate_codex_advisory_workdir(&workdir)
            .expect("git descendant at exact max depth 62 must pass");
        spec.validate()
            .expect("spec with git descendant at exact max depth 62 must pass");

        // `.git`/`.GIT` descendant at depth 63 MUST fail closed (no `.git` pruning).
        let over_limit = create_file_at_relative_depth(
            &workdir,
            Some(git_dir_name),
            CODEX_ADVISORY_MAX_WORKDIR_DEPTH + 1,
        );
        assert_eq!(
            over_limit
                .strip_prefix(&workdir)
                .expect("relative to workdir")
                .components()
                .count(),
            CODEX_ADVISORY_MAX_WORKDIR_DEPTH + 1
        );
        let err = validate_codex_advisory_workdir(&workdir)
            .expect_err("excessively deep .git/.GIT descendant must fail closed");
        assert_eq!(err.code(), WorkerErrorCode::InvalidSpec);
        assert!(err.to_string().contains("relative component depth 63"));
        assert_eq!(
            spec.validate().unwrap_err().code(),
            WorkerErrorCode::InvalidSpec
        );
    }
}

#[test]
fn codex_advisory_workdir_rejects_symlinks_to_protected_deeper_or_external_trees() {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workspace");
    let external_dir = temp.path().join("external-secret-tree");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(workdir.join("src")).expect("create src");
    fs::write(workdir.join(".env"), "SECRET_TOKEN=1\n").expect("write .env");
    fs::create_dir_all(&external_dir).expect("create external_dir");
    fs::write(external_dir.join("credentials.json"), "{}\n").expect("write external file");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let fake_codex = write_executable_script(&bin_dir, "fake-codex", "#!/usr/bin/env sh\nexit 0\n");
    let spec = sample_codex_advisory_spec(fake_codex, workdir.clone());

    validate_codex_advisory_workdir(&workdir).expect("clean workdir without symlinks passes");

    // 1. Symlink inside workspace pointing to protected `.env`
    let env_link = workdir.join("src/leaked_env.txt");
    symlink(workdir.join(".env"), &env_link).expect("create symlink to .env");
    let err_env = validate_codex_advisory_workdir(&workdir)
        .expect_err("symlink to protected file must fail closed");
    assert_eq!(err_env.code(), WorkerErrorCode::InvalidSpec);
    assert!(err_env.to_string().contains("symbolic link"));
    assert_eq!(
        spec.validate().unwrap_err().code(),
        WorkerErrorCode::InvalidSpec
    );
    fs::remove_file(&env_link).expect("remove env symlink");

    // 2. Symlink inside workspace pointing to external directory tree
    let ext_link = workdir.join("src/external_link");
    symlink(&external_dir, &ext_link).expect("create symlink to external tree");
    let err_ext = validate_codex_advisory_workdir(&workdir)
        .expect_err("symlink to external tree must fail closed");
    assert_eq!(err_ext.code(), WorkerErrorCode::InvalidSpec);
    assert!(err_ext.to_string().contains("symbolic link"));
    fs::remove_file(&ext_link).expect("remove external symlink");

    // 3. Symlink inside `.git` directory (proving `.git` is traversed for symlinks as well)
    let git_link = workdir.join(".git/hooks_link");
    symlink(&external_dir, &git_link).expect("create symlink inside .git");
    let err_git_link = validate_codex_advisory_workdir(&workdir)
        .expect_err("symlink inside .git must fail closed");
    assert_eq!(err_git_link.code(), WorkerErrorCode::InvalidSpec);
    assert!(err_git_link.to_string().contains("symbolic link"));
    fs::remove_file(&git_link).expect("remove .git symlink");
}

#[test]
fn codex_advisory_workdir_enforces_exact_entry_budget_32768() {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workspace");
    let bin_dir = temp.path().join("bin");
    // `.git` root directory counts as 1 visited entry.
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let fake_codex = write_executable_script(&bin_dir, "fake-codex", "#!/usr/bin/env sh\nexit 0\n");
    let spec = sample_codex_advisory_spec(fake_codex, workdir.clone());

    // Create 32_767 regular files so total entries (including `.git`) == 32_768.
    let remaining_allowed = CODEX_ADVISORY_MAX_WORKDIR_ENTRIES - 1;
    for idx in 0..remaining_allowed {
        let file_path = workdir.join(format!("entry_{idx:05}.txt"));
        fs::File::create(&file_path).expect("create budget fixture file");
    }

    validate_codex_advisory_workdir(&workdir)
        .expect("workdir with exactly 32,768 entries (including .git) must pass");
    spec.validate()
        .expect("spec with exactly 32,768 entries must pass");

    // Adding one more entry (32,769 total) must fail closed.
    let overflow_entry = workdir.join("entry_overflow.txt");
    fs::File::create(&overflow_entry).expect("create overflow fixture file");

    let err = validate_codex_advisory_workdir(&workdir)
        .expect_err("workdir exceeding 32,768 entries must fail closed");
    assert_eq!(err.code(), WorkerErrorCode::InvalidSpec);
    assert!(
        err.to_string()
            .contains("exceeds maximum preflight entry budget of 32768 entries"),
        "unexpected budget error: {err}"
    );
    assert_eq!(
        spec.validate().unwrap_err().code(),
        WorkerErrorCode::InvalidSpec
    );
}

#[test]
fn codex_advisory_workdir_rejects_fifo_and_special_entries_and_invalid_utf8_names() {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workspace");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let fake_codex = write_executable_script(&bin_dir, "fake-codex", "#!/usr/bin/env sh\nexit 0\n");
    let spec = sample_codex_advisory_spec(fake_codex, workdir.clone());

    // 1. Named FIFO special entry refusal
    let fifo_path = workdir.join("special.fifo");
    let fifo_cstr = CString::new(fifo_path.as_os_str().as_bytes()).expect("fifo CString");
    let mkfifo_rc = unsafe { libc::mkfifo(fifo_cstr.as_ptr(), 0o600) };
    assert_eq!(mkfifo_rc, 0, "libc::mkfifo must succeed in tempdir");

    let fifo_err = validate_codex_advisory_workdir(&workdir)
        .expect_err("FIFO entry inside advisory workdir must fail closed");
    assert_eq!(fifo_err.code(), WorkerErrorCode::InvalidSpec);
    assert!(
        fifo_err
            .to_string()
            .contains("unsupported non-regular filesystem entry"),
        "unexpected FIFO error: {fifo_err}"
    );
    assert_eq!(
        spec.validate().unwrap_err().code(),
        WorkerErrorCode::InvalidSpec
    );
    fs::remove_file(&fifo_path).expect("remove FIFO");

    // 2. Unix domain socket special entry refusal
    let sock_path = workdir.join("special.sock");
    let _listener = UnixListener::bind(&sock_path).expect("bind unix socket in tempdir");
    let sock_err = validate_codex_advisory_workdir(&workdir)
        .expect_err("socket entry inside advisory workdir must fail closed");
    assert_eq!(sock_err.code(), WorkerErrorCode::InvalidSpec);
    assert!(
        sock_err
            .to_string()
            .contains("unsupported non-regular filesystem entry"),
        "unexpected socket error: {sock_err}"
    );
    fs::remove_file(&sock_path).expect("remove socket");

    // 3. Invalid UTF-8 entry name refusal
    let invalid_utf8_name = OsStr::from_bytes(b"invalid-\xff-utf8.txt");
    let invalid_utf8_path = workdir.join(invalid_utf8_name);
    fs::write(&invalid_utf8_path, "non-utf8 filename\n").expect("create non-utf8 file");

    let utf8_err = validate_codex_advisory_workdir(&workdir)
        .expect_err("invalid UTF-8 entry inside advisory workdir must fail closed");
    assert_eq!(utf8_err.code(), WorkerErrorCode::InvalidSpec);
    assert!(
        utf8_err.to_string().contains("is not valid UTF-8"),
        "unexpected invalid UTF-8 error: {utf8_err}"
    );
    assert_eq!(
        spec.validate().unwrap_err().code(),
        WorkerErrorCode::InvalidSpec
    );
    fs::remove_file(&invalid_utf8_path).expect("remove invalid UTF-8 file");

    validate_codex_advisory_workdir(&workdir).expect("clean workdir passes after cleanup");
}

#[test]
fn codex_advisory_output_last_message_argv_pin_preserves_read_only_permission_profile_and_rejects_unsafe_paths(
) {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workspace");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let fake_codex = write_executable_script(&bin_dir, "fake-codex", "#!/usr/bin/env sh\nexit 0\n");
    let spec = sample_codex_advisory_spec(fake_codex, workdir);
    let validation = spec.validate().expect("validate spec");
    let profile = exact_profile(&validation).to_string();

    assert!(validation.allowed_write_paths.is_empty());
    assert!(is_pinned_codex_read_only_argv(
        &validation.constructed_argv,
        &validation.resolved_model
    ));
    assert_eq!(
        threadlane_workers::spec::extract_pinned_codex_read_only_output_last_message_path(
            &validation.constructed_argv,
            &validation.resolved_model
        ),
        None
    );

    // Splicing the supervisor-owned `--output-last-message` flag before `--` keeps the exact
    // rendered read-only profile untouched (`"."="read"`, network disabled).
    let mut spliced_argv = validation.constructed_argv.clone();
    let sep_idx = spliced_argv.len() - 2;
    assert_eq!(spliced_argv[sep_idx], "--");
    let valid_capture_path = "/tmp/threadlane-codex-last-msg-unit/last-message.json";
    spliced_argv.splice(
        sep_idx..sep_idx,
        [
            "--output-last-message".to_string(),
            valid_capture_path.to_string(),
        ],
    );
    assert!(is_pinned_codex_read_only_argv(
        &spliced_argv,
        &validation.resolved_model
    ));
    assert_eq!(
        threadlane_workers::spec::extract_pinned_codex_read_only_output_last_message_path(
            &spliced_argv,
            &validation.resolved_model
        ),
        Some(valid_capture_path)
    );
    assert!(spliced_argv.iter().any(|arg| arg == &profile));

    // Unsafe capture paths, wrong flag aliases, extra flags, or tampered permission profiles fail closed.
    for bad_path in [
        "relative/last-message.json",
        "/tmp/threadlane-codex-last-msg-unit/../last-message.json",
        "/tmp/threadlane-codex-last-msg-unit/./last-message.json",
        "//tmp/threadlane-codex-last-msg-unit/last-message.json",
        "/tmp/threadlane-codex-last-msg-unit//last-message.json",
        "/tmp/threadlane-codex-last-msg-unit/last-message.json/",
        "/tmp/threadlane-codex-last-msg-unit/.",
        "/tmp/threadlane-codex-last-msg-unit/other.json",
        "/last-message.json",
        "/tmp/threadlane\n/last-message.json",
    ] {
        let mut bad_argv = spliced_argv.clone();
        bad_argv[sep_idx + 1] = bad_path.to_string();
        assert!(
            !is_pinned_codex_read_only_argv(&bad_argv, &validation.resolved_model),
            "expected unsafe capture path `{bad_path}` to be rejected"
        );
        assert_eq!(
            threadlane_workers::spec::extract_pinned_codex_read_only_output_last_message_path(
                &bad_argv,
                &validation.resolved_model
            ),
            None
        );
    }

    let mut short_flag_argv = spliced_argv.clone();
    short_flag_argv[sep_idx] = "-o".to_string();
    assert!(!is_pinned_codex_read_only_argv(
        &short_flag_argv,
        &validation.resolved_model
    ));

    let mut tampered_perm_argv = spliced_argv.clone();
    for arg in &mut tampered_perm_argv {
        if arg == &profile {
            *arg = arg.replace("\".\"=\"read\"", "\".\"=\"write\"");
        }
    }
    assert!(!is_pinned_codex_read_only_argv(
        &tampered_perm_argv,
        &validation.resolved_model
    ));
}

#[test]
fn codex_advisory_exact_frontier_collapses_git_and_nested_protected_paths_deterministically() {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workspace");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(workdir.join(".git/objects")).expect("create root .git");
    fs::create_dir_all(workdir.join("nested/.git/objects")).expect("create nested .git");
    fs::create_dir_all(workdir.join(".aws")).expect("create root .aws");
    fs::create_dir_all(workdir.join(".agents/skills")).expect("create root .agents");
    fs::create_dir_all(workdir.join("nested/.aws")).expect("create nested .aws");
    fs::create_dir_all(workdir.join("nested/.agents")).expect("create nested .agents");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    for idx in 0..2_000 {
        fs::write(
            workdir.join(format!(".git/objects/object_{idx:05}")),
            b"protected git object\n",
        )
        .expect("write root git object fixture");
    }
    fs::write(workdir.join(".git/HEAD"), "ref: refs/heads/main\n").expect("write root .git HEAD");
    fs::write(workdir.join("nested/.git/HEAD"), "ref: refs/heads/main\n")
        .expect("write nested .git HEAD");
    fs::write(workdir.join(".aws/credentials"), "root secret\n").expect("write root aws secret");
    fs::write(workdir.join("nested/.aws/credentials"), "nested secret\n")
        .expect("write nested aws secret");
    fs::write(workdir.join(".agents/skills/run.sh"), "#!/bin/sh\n")
        .expect("write root agents script");
    fs::write(
        workdir.join("nested/.agents/config"),
        "nested agent config\n",
    )
    .expect("write nested agents config");
    fs::write(workdir.join(".env"), "ROOT_SECRET=1\n").expect("write root .env");
    fs::write(workdir.join("nested/.env.local"), "NESTED_SECRET=1\n")
        .expect("write nested .env.local");
    fs::write(workdir.join("ordinary[1].txt"), "not protected\n")
        .expect("write ordinary glob-like file outside protected names");

    let fake_codex = write_executable_script(&bin_dir, "fake-codex", "#!/usr/bin/env sh\nexit 0\n");
    let spec = sample_codex_advisory_spec(fake_codex, workdir.clone());
    let validation = spec.validate().expect("validate exact-frontier spec");
    let profile = exact_profile(&validation);
    let parsed = parse_and_validate_codex_advisory_exact_permission_profile_config(profile)
        .expect("parse exact-frontier profile");
    let (canonical_workdir, collected) =
        collect_codex_advisory_deny_frontiers(&workdir).expect("collect deny frontiers");

    assert_eq!(canonical_workdir, validation.canonical_working_directory);
    assert_eq!(parsed, collected);
    assert_eq!(
        profile,
        build_codex_advisory_exact_permission_profile_config(&collected)
            .expect("re-render collected frontiers")
            .as_str()
    );
    assert!(is_pinned_codex_read_only_argv(
        &validation.constructed_argv,
        &validation.resolved_model
    ));

    for required in [
        ".git",
        ".aws",
        ".agents",
        ".codex",
        ".env",
        "codex.toml",
        ".codex.toml",
        "nested/.git",
        "nested/.aws",
        "nested/.agents",
        "nested/.env.local",
    ] {
        assert!(
            parsed.contains(required),
            "exact frontier must contain protected entry `{required}`"
        );
    }
    for forbidden in [
        ".git/HEAD",
        ".git/objects",
        ".aws/credentials",
        ".agents/skills",
        "nested/.git/HEAD",
        "nested/.git/objects",
        "nested/.aws/credentials",
        "nested/.agents/config",
    ] {
        assert!(
            !parsed.contains(forbidden),
            "exact frontier must not expand descendant `{forbidden}` under a denied ancestor"
        );
    }
    assert!(
        !parsed
            .iter()
            .any(|key| key != ".git" && key.starts_with(".git/")),
        "root .git must remain a single exact rule"
    );
    assert!(
        !profile.contains('*')
            && !profile.contains('?')
            && !profile.contains('[')
            && !profile.contains(']'),
        "rendered exact frontier must not contain deny globs: {profile}"
    );
    for key in &parsed {
        assert!(
            !key.chars()
                .any(|ch| matches!(ch, '*' | '?' | '[' | ']' | '{' | '}' | '\\')),
            "exact frontier key `{key}` must not contain glob or injection delimiters"
        );
    }
    assert_exact_frontier_has_no_redundant_descendants(&parsed);

    let second = spec
        .validate()
        .expect("validate same workspace deterministically");
    assert_eq!(second.constructed_argv[16].as_str(), profile);
    assert_eq!(second.constructed_argv, validation.constructed_argv);
}

#[test]
fn codex_advisory_exact_frontier_represents_unicode_and_spaces_without_glob_rules() {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workspace");
    let bin_dir = temp.path().join("bin");
    let unicode_dir = workdir.join("nested ü space");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(unicode_dir.join(".git")).expect("create nested unicode .git");
    fs::create_dir_all(unicode_dir.join(".aws")).expect("create nested unicode .aws");
    fs::create_dir_all(unicode_dir.join(".agents")).expect("create nested unicode .agents");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");
    fs::write(unicode_dir.join(".git/HEAD"), "ref: refs/heads/main\n")
        .expect("write unicode nested .git HEAD");
    fs::write(unicode_dir.join(".aws/credentials"), "secret\n")
        .expect("write unicode nested aws secret");
    fs::write(unicode_dir.join(".agents/config"), "secret\n")
        .expect("write unicode nested agents config");
    fs::write(unicode_dir.join(".env.production"), "SECRET=1\n")
        .expect("write unicode nested .env.production");

    let fake_codex = write_executable_script(&bin_dir, "fake-codex", "#!/usr/bin/env sh\nexit 0\n");
    let spec = sample_codex_advisory_spec(fake_codex, workdir);
    let validation = spec.validate().expect("validate unicode/space workspace");
    let profile = exact_profile(&validation);
    let parsed = parse_and_validate_codex_advisory_exact_permission_profile_config(profile)
        .expect("parse unicode/space exact frontier");

    for required in [
        "nested ü space/.git",
        "nested ü space/.aws",
        "nested ü space/.agents",
        "nested ü space/.env.production",
    ] {
        assert!(
            parsed.contains(required),
            "exact frontier must contain Unicode/space key `{required}`"
        );
        assert!(
            profile.contains(format!("\"{required}\"=\"deny\"").as_str()),
            "rendered profile must safely quote Unicode/space key `{required}`"
        );
    }
    assert!(!parsed.contains("nested ü space/.git/HEAD"));
    assert_exact_frontier_has_no_redundant_descendants(&parsed);

    let escaped_key = "nested/quote\"comma,equal=.env".to_string();
    let escaped_profile =
        build_codex_advisory_exact_permission_profile_config(&BTreeSet::from(
            [escaped_key.clone()],
        ))
        .expect("render safely escaped quoted key");
    assert!(
        escaped_profile.contains("\\\""),
        "quoted key must be TOML-escaped rather than injected: {escaped_profile}"
    );
    let escaped_parsed =
        parse_and_validate_codex_advisory_exact_permission_profile_config(&escaped_profile)
            .expect("parse safely escaped quoted key");
    assert!(escaped_parsed.contains(&escaped_key));

    assert!(is_pinned_codex_read_only_argv(
        &validation.constructed_argv,
        &validation.resolved_model
    ));
}

#[test]
fn codex_advisory_exact_frontier_rejects_unrepresentable_keys_and_profile_bounds() {
    for unrepresentable in [
        "../escape",
        "nested/./.env",
        "nested/../.env",
        "nested/./secret.txt",
        "nested/../secret.txt",
        "/absolute/.git",
        "glob*",
        "question?.env",
        "bracket[0]",
        "close]bracket",
        "brace{0}",
        "paren(0)",
        "back\\slash",
        "new\nline",
        "delete\u{7f}",
    ] {
        let err = build_codex_advisory_exact_permission_profile_config(&BTreeSet::from([
            unrepresentable.to_string(),
        ]))
        .expect_err("unrepresentable exact deny key must fail before launch");
        assert_eq!(err.code(), WorkerErrorCode::InvalidSpec);
    }

    let orphan_descendant_err =
        build_codex_advisory_exact_permission_profile_config(&BTreeSet::from([
            "nested/.git/HEAD".to_string()
        ]))
        .expect_err("orphan descendant of a protected ancestor must fail canonical rendering");
    assert_eq!(orphan_descendant_err.code(), WorkerErrorCode::InvalidSpec);
    assert!(orphan_descendant_err
        .to_string()
        .contains("descendant of a protected ancestor"));

    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workspace");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");
    fs::write(workdir.join(".env.[production]"), "SECRET=1\n")
        .expect("write unrepresentable protected secret fixture");
    let fake_codex = write_executable_script(&bin_dir, "fake-codex", "#!/usr/bin/env sh\nexit 0\n");
    let spec = sample_codex_advisory_spec(fake_codex, workdir);
    let err = spec
        .validate()
        .expect_err("unrepresentable protected file must fail launch rendering");
    assert_eq!(err.code(), WorkerErrorCode::InvalidSpec);
    assert!(
        err.to_string().contains("unrepresentable"),
        "unexpected unrepresentable-key error: {err}"
    );

    let mut too_many = BTreeSet::new();
    for idx in 0..=CODEX_ADVISORY_MAX_EXACT_DENY_RULES {
        too_many.insert(format!("path_{idx:05}/.env"));
    }
    let err = build_codex_advisory_exact_permission_profile_config(&too_many)
        .expect_err("profile deny-rule count bound must fail closed");
    assert_eq!(err.code(), WorkerErrorCode::InvalidSpec);
    assert!(err.to_string().contains("maximum deny-rule count"));

    let mut too_large = BTreeSet::new();
    let long_component = "a".repeat(300);
    for idx in 0..220 {
        too_large.insert(format!("p{idx:04}/{long_component}/.env"));
    }
    let err = build_codex_advisory_exact_permission_profile_config(&too_large)
        .expect_err("profile serialized byte bound must fail closed");
    assert_eq!(err.code(), WorkerErrorCode::InvalidSpec);
    let byte_limit = MAX_TASK_FILES_SETTINGS_BYTES.to_string();
    assert!(err.to_string().contains(byte_limit.as_str()));

    let profile = expected_exact_profile(&[".git"]);
    for malformed in [
        profile.replace(
            "\"codex.toml\"=\"deny\"",
            "\"codex.toml\"=\"deny\",\"nested/./secret.txt\"=\"deny\"",
        ),
        profile.replace(
            "\"codex.toml\"=\"deny\"",
            "\"codex.toml\"=\"deny\",\"nested/../secret.txt\"=\"deny\"",
        ),
        profile.replace("\".git\"=\"deny\"", "\"\\u002egit\"=\"deny\""),
        profile.replace("\".git\"=\"deny\"", "\"bad\u{7f}.git\"=\"deny\""),
        profile.replace("\".git\"=\"deny\"", "\"unterminated"),
    ] {
        assert!(
            parse_and_validate_codex_advisory_exact_permission_profile_config(&malformed).is_err()
        );
    }
}

#[test]
fn codex_advisory_exact_frontier_still_traverses_and_rejects_hazards_under_denied_roots() {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workspace");
    let bin_dir = temp.path().join("bin");
    let external_dir = temp.path().join("external-secret-tree");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");
    fs::create_dir_all(&external_dir).expect("create external_dir");
    let fake_codex = write_executable_script(&bin_dir, "fake-codex", "#!/usr/bin/env sh\nexit 0\n");
    let spec = sample_codex_advisory_spec(fake_codex, workdir.clone());
    spec.validate()
        .expect("baseline denied-root workspace validates");

    // 1. Symlink under denied `.git` is still rejected.
    let git_link = workdir.join(".git/link");
    symlink(&external_dir, &git_link).expect("create symlink inside .git");
    let err = spec
        .validate()
        .expect_err("symlink under denied root must fail closed");
    assert!(err.to_string().contains("symbolic link"));
    fs::remove_file(&git_link).expect("remove .git symlink");

    // 2. Depth overflow under denied `.git` is still rejected.
    let deep =
        create_file_at_relative_depth(&workdir, Some(".git"), CODEX_ADVISORY_MAX_WORKDIR_DEPTH + 1);
    assert_eq!(
        deep.strip_prefix(&workdir)
            .expect("relative to workdir")
            .components()
            .count(),
        CODEX_ADVISORY_MAX_WORKDIR_DEPTH + 1
    );
    let err = spec
        .validate()
        .expect_err("deep entry under denied root must fail closed");
    assert!(err.to_string().contains("relative component depth 63"));
    fs::remove_dir_all(workdir.join(".git")).expect("remove deep .git fixture");
    fs::create_dir(&workdir.join(".git")).expect("recreate empty .git directory");

    // 3. Project Codex config marker under denied `.aws` is still rejected.
    fs::create_dir_all(workdir.join(".aws/.codex")).expect("create project marker under .aws");
    fs::write(workdir.join(".aws/.codex/config.toml"), "hook = true\n")
        .expect("write project marker fixture");
    let err = spec
        .validate()
        .expect_err("project Codex marker under denied root must fail closed");
    assert!(err
        .to_string()
        .contains("project Codex configuration or hooks path"));
    fs::remove_dir_all(workdir.join(".aws")).expect("remove .aws fixture");

    // 4. Invalid UTF-8 under denied `.git` is still rejected.
    let invalid_utf8_path = workdir
        .join(".git")
        .join(OsStr::from_bytes(b"invalid-\xff-utf8"));
    fs::write(&invalid_utf8_path, "non-utf8\n").expect("write non-utf8 fixture");
    let err = spec
        .validate()
        .expect_err("invalid UTF-8 under denied root must fail closed");
    assert!(err.to_string().contains("is not valid UTF-8"));
    fs::remove_file(&invalid_utf8_path).expect("remove invalid UTF-8 fixture");

    // 5. Special file under denied `.git` is still rejected.
    let fifo_path = workdir.join(".git/special.fifo");
    let fifo_cstr = CString::new(fifo_path.as_os_str().as_bytes()).expect("fifo CString");
    let mkfifo_rc = unsafe { libc::mkfifo(fifo_cstr.as_ptr(), 0o600) };
    assert_eq!(mkfifo_rc, 0, "libc::mkfifo must succeed in tempdir");
    let err = spec
        .validate()
        .expect_err("special entry under denied root must fail closed");
    assert!(err
        .to_string()
        .contains("unsupported non-regular filesystem entry"));
    fs::remove_file(&fifo_path).expect("remove FIFO fixture");

    // 6. An unreadable directory under denied `.git` is still rejected when the test process
    // cannot read it (privileged test runners can read mode 0000 and skip this environment-bound
    // assertion without weakening product behavior).
    let unreadable_dir = workdir.join(".git/unreadable");
    fs::create_dir_all(&unreadable_dir).expect("create unreadable .git directory");
    fs::set_permissions(&unreadable_dir, fs::Permissions::from_mode(0o000))
        .expect("chmod unreadable .git directory");
    if fs::read_dir(&unreadable_dir).is_err() {
        let err = spec
            .validate()
            .expect_err("unreadable directory under denied root must fail closed");
        assert!(err.to_string().contains("is not readable"));
    }
    fs::set_permissions(&unreadable_dir, fs::Permissions::from_mode(0o700))
        .expect("restore unreadable .git directory permissions");
    fs::remove_dir(&unreadable_dir).expect("remove unreadable .git fixture");

    spec.validate()
        .expect("clean denied-root workspace validates again");
}

#[test]
fn codex_advisory_exact_frontier_pin_rejects_mutations_and_separates_task_files_and_legacy_static_evidence(
) {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workspace");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");
    let fake_codex = write_executable_script(&bin_dir, "fake-codex", "#!/usr/bin/env sh\nexit 0\n");
    let spec = sample_codex_advisory_spec(fake_codex, workdir);
    let validation = spec.validate().expect("validate exact-frontier spec");
    let profile = exact_profile(&validation).to_string();

    assert!(is_pinned_codex_read_only_argv(
        &validation.constructed_argv,
        &validation.resolved_model
    ));
    assert!(!is_pinned_codex_task_files_argv(
        &validation.constructed_argv,
        &validation.resolved_model
    ));

    // Historical static argv remains readable as legacy evidence, but never parses as a new
    // exact frontier and is not a constructor fallback.
    let mut legacy_argv = validation.constructed_argv.clone();
    legacy_argv[16] = CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG.to_string();
    assert!(is_pinned_codex_read_only_argv(
        &legacy_argv,
        &validation.resolved_model
    ));
    assert!(
        parse_and_validate_codex_advisory_exact_permission_profile_config(
            CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG
        )
        .is_err()
    );

    let mutations = [
        profile.replace("\".\"=\"read\"", "\".\"=\"write\""),
        profile.replace(
            "\"network\"={\"enabled\"=false}",
            "\"network\"={\"enabled\"=true}",
        ),
        profile.replace(
            "\"/nix/store\"=\"read\"",
            "\"/nix/store\"=\"read\",\"/etc\"=\"read\"",
        ),
        profile.replace("\"glob_scan_max_depth\"=64", "\"glob_scan_max_depth\"=128"),
        profile.replace("\".git\"=\"deny\",", ""),
        profile.replace("\".env\"=\"deny\",", ""),
        profile.replace(
            "\".agents\"=\"deny\"",
            "\".agents\"=\"deny\",\".agents\"=\"deny\"",
        ),
        profile.replace(
            "\".agents\"=\"deny\",\".aws\"=\"deny\"",
            "\".aws\"=\"deny\",\".agents\"=\"deny\"",
        ),
        profile.replace(
            "\".git\"=\"deny\"",
            "\".git\"=\"deny\",\".git/HEAD\"=\"deny\"",
        ),
        profile.replace("\".git\"=\"deny\"", "\"../.git\"=\"deny\""),
        profile.replace(
            "\"codex.toml\"=\"deny\"",
            "\"codex.toml\"=\"deny\",\"nested/./secret.txt\"=\"deny\"",
        ),
        profile.replace(
            "\"codex.toml\"=\"deny\"",
            "\"codex.toml\"=\"deny\",\"nested/../secret.txt\"=\"deny\"",
        ),
        profile.replace("\".git\"=\"deny\"", "\"nested/.git/HEAD\"=\"deny\""),
        profile.replace("\".git\"=\"deny\"", "\"**/.git\"=\"deny\""),
        profile.replace("\".git\"=\"deny\"", "\"/absolute/.git\"=\"deny\""),
        profile.replace("\".git\"=\"deny\"", "\"\\u002egit\"=\"deny\""),
    ];
    for mutated in mutations {
        assert_ne!(mutated, profile, "pin mutation must change the profile");
        assert!(
            parse_and_validate_codex_advisory_exact_permission_profile_config(&mutated).is_err(),
            "mutated profile must fail canonical parsing: {mutated}"
        );
        let mut mutated_argv = validation.constructed_argv.clone();
        mutated_argv[16] = mutated;
        assert!(
            !is_pinned_codex_read_only_argv(&mutated_argv, &validation.resolved_model),
            "mutated exact profile must fail the read-only launch pin"
        );
    }

    // Last-message capture splicing remains pinned and keeps the exact rendered profile intact.
    let sep_idx = validation.constructed_argv.len() - 2;
    let capture_path = "/tmp/threadlane-codex-last-msg-unit/last-message.json";
    let mut spliced = validation.constructed_argv.clone();
    spliced.splice(
        sep_idx..sep_idx,
        [
            "--output-last-message".to_string(),
            capture_path.to_string(),
        ],
    );
    assert!(is_pinned_codex_read_only_argv(
        &spliced,
        &validation.resolved_model
    ));
    assert_eq!(
        threadlane_workers::spec::extract_pinned_codex_read_only_output_last_message_path(
            &spliced,
            &validation.resolved_model
        ),
        Some(capture_path)
    );
    assert_eq!(spliced[16].as_str(), profile.as_str());
}

#[test]
fn codex_advisory_known_historical_bounded_static_profile_remains_legacy_evidence_only() {
    let temp = TempDir::new().expect("tempdir");
    let workdir = temp.path().join("workspace");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(workdir.join(".git")).expect("create .git");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");
    let fake_codex = write_executable_script(&bin_dir, "fake-codex", "#!/usr/bin/env sh\nexit 0\n");
    let spec = sample_codex_advisory_spec(fake_codex, workdir);
    let validation = spec.validate().expect("validate exact-frontier spec");

    // New launches always render the current bounded exact frontier, never a legacy literal.
    assert_ne!(
        validation.constructed_argv[16].as_str(),
        HISTORICAL_BOUNDED_READ_ONLY_PROFILE
    );
    assert!(
        parse_and_validate_codex_advisory_exact_permission_profile_config(
            HISTORICAL_BOUNDED_READ_ONLY_PROFILE
        )
        .is_err()
    );

    // The exact earlier bounded literal remains readable as persisted legacy launch evidence.
    let mut historical_argv = validation.constructed_argv.clone();
    historical_argv[16] = HISTORICAL_BOUNDED_READ_ONLY_PROFILE.to_string();
    assert!(is_pinned_codex_read_only_argv(
        &historical_argv,
        &validation.resolved_model
    ));

    // Capture splicing keeps the same historical read-only pin and path extraction contract.
    let sep_idx = historical_argv.len() - 2;
    let capture_path = "/tmp/threadlane-codex-last-msg-unit/last-message.json";
    let mut spliced = historical_argv.clone();
    spliced.splice(
        sep_idx..sep_idx,
        [
            "--output-last-message".to_string(),
            capture_path.to_string(),
        ],
    );
    assert!(is_pinned_codex_read_only_argv(
        &spliced,
        &validation.resolved_model
    ));
    assert_eq!(
        threadlane_workers::spec::extract_pinned_codex_read_only_output_last_message_path(
            &spliced,
            &validation.resolved_model
        ),
        Some(capture_path)
    );

    for weakened in [
        HISTORICAL_BOUNDED_READ_ONLY_PROFILE.replace("\".\"=\"read\"", "\".\"=\"write\""),
        HISTORICAL_BOUNDED_READ_ONLY_PROFILE
            .replace("\":minimal\"=\"read\"", "\":minimal\"=\"write\""),
        HISTORICAL_BOUNDED_READ_ONLY_PROFILE.replace(
            "network\"={\"enabled\"=false}",
            "network\"={\"enabled\"=true}",
        ),
        HISTORICAL_BOUNDED_READ_ONLY_PROFILE.replace(
            "\"/nix/store\"=\"read\"",
            "\"/nix/store\"=\"read\",\"/etc\"=\"read\"",
        ),
        HISTORICAL_BOUNDED_READ_ONLY_PROFILE
            .replace("glob_scan_max_depth\"=64", "glob_scan_max_depth\"=128"),
        HISTORICAL_BOUNDED_READ_ONLY_PROFILE.replace("\"glob_scan_max_depth\"=64,", ""),
        HISTORICAL_BOUNDED_READ_ONLY_PROFILE.replace("\"**/.env\"=\"deny\",", ""),
        HISTORICAL_BOUNDED_READ_ONLY_PROFILE.replace("\"**/.env.*\"=\"deny\",", ""),
        HISTORICAL_BOUNDED_READ_ONLY_PROFILE.replace(
            "\"**/.aws\"=\"deny\",\"**/.aws/**\"=\"deny\"",
            "\"**/.aws\"=\"read\"",
        ),
        HISTORICAL_BOUNDED_READ_ONLY_PROFILE
            .replace("\"**/.git\"=\"deny\"", "\"**/.git\"=\"read\""),
        format!("{HISTORICAL_BOUNDED_READ_ONLY_PROFILE}\n"),
    ] {
        assert_ne!(weakened, HISTORICAL_BOUNDED_READ_ONLY_PROFILE);
        let mut weakened_argv = historical_argv.clone();
        weakened_argv[16] = weakened;
        assert!(
            !is_pinned_codex_read_only_argv(&weakened_argv, &validation.resolved_model),
            "weakened historical profile must fail the read-only launch pin"
        );
    }
}

#[test]
fn codex_advisory_mandatory_root_entries_are_rendered_even_when_absent() {
    let profile = expected_exact_profile(&[]);
    let parsed = parse_and_validate_codex_advisory_exact_permission_profile_config(&profile)
        .expect("empty collected frontier still renders mandatory root entries");
    for root in CODEX_ADVISORY_MANDATORY_ROOT_DENY_PATHS {
        assert!(
            parsed.contains(root),
            "mandatory root `{root}` must be present even when absent on disk"
        );
    }
    assert_eq!(parsed.len(), CODEX_ADVISORY_MANDATORY_ROOT_DENY_PATHS.len());
}
