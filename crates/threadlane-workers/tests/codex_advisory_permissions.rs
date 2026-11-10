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
    is_pinned_codex_read_only_argv, validate_codex_advisory_workdir, AdvisoryWorkerSpec,
    ExecutionBudgets, WorkerErrorCode, WorkerFilePermissions, WorkerProvider,
    CODEX_ADVISORY_GLOB_SCAN_DEPTH_MARGIN, CODEX_ADVISORY_GLOB_SCAN_MAX_DEPTH,
    CODEX_ADVISORY_MAX_WORKDIR_DEPTH, CODEX_ADVISORY_MAX_WORKDIR_ENTRIES,
    CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG,
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

    assert!(is_pinned_codex_read_only_argv(
        &validation.constructed_argv,
        &validation.resolved_model
    ));

    // Absent `glob_scan_max_depth` in profile must fail exact pinned-argv validation.
    let mut absent_depth_argv = validation.constructed_argv.clone();
    absent_depth_argv[16] =
        CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG.replace("\"glob_scan_max_depth\"=64,", "");
    assert_ne!(
        absent_depth_argv[16],
        CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG
    );
    assert!(!is_pinned_codex_read_only_argv(
        &absent_depth_argv,
        &validation.resolved_model
    ));

    // Edited `glob_scan_max_depth` values must fail exact pinned-argv validation.
    for edited_depth in ["8", "62", "63", "65", "0"] {
        let mut edited_argv = validation.constructed_argv.clone();
        edited_argv[16] = CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG.replace(
            "\"glob_scan_max_depth\"=64",
            &format!("\"glob_scan_max_depth\"={edited_depth}"),
        );
        assert!(!is_pinned_codex_read_only_argv(
            &edited_argv,
            &validation.resolved_model
        ));
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
    // read-only `CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG` untouched (`"."="read"`, network disabled).
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
    assert!(spliced_argv
        .iter()
        .any(|arg| arg == CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG));

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
        if arg == CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG {
            *arg = arg.replace("\".\"=\"read\"", "\".\"=\"write\"");
        }
    }
    assert!(!is_pinned_codex_read_only_argv(
        &tampered_perm_argv,
        &validation.resolved_model
    ));
}
