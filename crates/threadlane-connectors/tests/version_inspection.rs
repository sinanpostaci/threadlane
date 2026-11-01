use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::json;
use tempfile::TempDir;
use threadlane_connectors::{
    inspect_tool_version_with_options, inspect_tool_versions, inspect_tool_versions_with_options,
    is_process_group_alive, ordered_system_search_dirs, probe_command_template,
    resolve_system_executable_in_path, version_command_template, ToolAdapter, ToolDefinition,
    VersionInspectionOptions, AVAILABILITY_API_ONLY, AVAILABILITY_DISABLED, AVAILABILITY_FOUND,
    AVAILABILITY_NOT_FOUND, MAX_VERSION_INSPECTION_CONCURRENCY, SELECTION_MODE_CUSTOM,
    SELECTION_MODE_SYSTEM,
};

fn build_tool(
    id: &str,
    label: &str,
    adapter: ToolAdapter,
    executable_path: Option<PathBuf>,
    enabled: bool,
) -> ToolDefinition {
    ToolDefinition {
        id: id.to_string(),
        label: label.to_string(),
        adapter,
        executable_path,
        enabled,
    }
}

fn write_executable_script(dir: &Path, name: &str, body: &str) -> PathBuf {
    fs::create_dir_all(dir).expect("create script directory");
    let path = dir.join(name);
    fs::write(&path, body).expect("write test script");
    let mut perms = fs::metadata(&path).expect("metadata").permissions();
    perms.set_mode(0o700);
    fs::set_permissions(&path, perms).expect("chmod 0700");
    path
}

fn join_path_env(dirs: &[PathBuf]) -> OsString {
    std::env::join_paths(dirs).expect("join PATH entries")
}

#[test]
fn version_command_templates_are_fixed_and_preserve_catalog_probe_templates() {
    assert_eq!(
        version_command_template(&ToolAdapter::CodexCli),
        Some(&["--version"][..])
    );
    assert_eq!(
        version_command_template(&ToolAdapter::OpencodeV2),
        Some(&["--version"][..])
    );
    assert_eq!(
        version_command_template(&ToolAdapter::Antigravity),
        Some(&["--version"][..])
    );
    assert_eq!(
        version_command_template(&ToolAdapter::Git),
        Some(&["--version"][..])
    );
    assert_eq!(
        version_command_template(&ToolAdapter::Codegraph),
        Some(&["--version"][..])
    );
    assert_eq!(
        version_command_template(&ToolAdapter::Tmux),
        Some(&["-V"][..])
    );
    assert_eq!(version_command_template(&ToolAdapter::JevApi), None);
    assert_eq!(
        version_command_template(&ToolAdapter::OpenaiDecisionsApi),
        None
    );

    // Full catalog probe templates remain untouched.
    assert_eq!(
        probe_command_template(&ToolAdapter::CodexCli),
        Some(&["debug", "models", "--bundled"][..])
    );
    assert_eq!(
        probe_command_template(&ToolAdapter::OpencodeV2),
        Some(&["api", "--standalone", "model.list"][..])
    );
    assert_eq!(
        probe_command_template(&ToolAdapter::Antigravity),
        Some(&["models"][..])
    );
}

#[tokio::test]
async fn process_inspection_executes_only_fixed_version_argv_and_redacts_secrets() {
    let temp = TempDir::new().expect("tempdir");
    let bin_dir = temp.path().join("bin");
    let trap_marker = temp.path().join("forbidden-subcommand.marker");

    let specs: &[(&str, &str, ToolAdapter, &str, &str, &str)] = &[
        (
            "codex",
            "codex",
            ToolAdapter::CodexCli,
            "--version",
            "codex-cli 0.165.0-alpha.1.2 (bearer-secret-token-alpha)",
            "0.165.0-alpha.1.2",
        ),
        (
            "opencode",
            "opencode",
            ToolAdapter::OpencodeV2,
            "--version",
            "opencode v2.2.4 user@example.com",
            "2.2.4",
        ),
        (
            "agy",
            "agy",
            ToolAdapter::Antigravity,
            "--version",
            "\u{1b}[32magy version 2.0.9\u{1b}[0m sk-secret-999",
            "2.0.9",
        ),
        (
            "git",
            "git",
            ToolAdapter::Git,
            "--version",
            "git version 2.47.1",
            "2.47.1",
        ),
        (
            "codegraph",
            "codegraph",
            ToolAdapter::Codegraph,
            "--version",
            "codegraph 0.9.3",
            "0.9.3",
        ),
        ("tmux", "tmux", ToolAdapter::Tmux, "-V", "tmux 3.5a", "3.5a"),
    ];

    let mut tools = Vec::new();
    for (tool_id, bin_name, adapter, expected_flag, raw_out, _) in specs {
        let argv_file = temp.path().join(format!("{tool_id}-argv.txt"));
        write_executable_script(
            &bin_dir,
            bin_name,
            &format!(
                r#"#!/bin/sh
printf '%s' "$*" > "{}"
if [ "$1" = "{}" ] && [ $# -eq 1 ]; then
  printf '%s\n' '{}'
  exit 0
fi
touch "{}"
exit 99
"#,
                argv_file.display(),
                expected_flag,
                raw_out,
                trap_marker.display()
            ),
        );
        tools.push(build_tool(tool_id, tool_id, *adapter, None, true));
    }

    let path_env = join_path_env(std::slice::from_ref(&bin_dir));
    let options = VersionInspectionOptions::new().with_path_env(path_env);
    let results = inspect_tool_versions_with_options(&tools, &options).await;

    assert!(
        !trap_marker.exists(),
        "version inspection must never invoke catalog, login, or installer subcommands"
    );
    assert_eq!(results.len(), specs.len());

    let canonical_bin_dir = fs::canonicalize(&bin_dir).expect("canonicalize bin_dir");
    for (inspection, (tool_id, bin_name, _, expected_flag, _, expected_version)) in
        results.iter().zip(specs.iter())
    {
        let argv_file = temp.path().join(format!("{tool_id}-argv.txt"));
        let recorded_argv = fs::read_to_string(&argv_file).expect("read argv log");
        assert_eq!(&recorded_argv, expected_flag);

        let expected_path = canonical_bin_dir
            .join(bin_name)
            .to_string_lossy()
            .into_owned();
        assert_eq!(inspection.tool_id, *tool_id);
        assert_eq!(inspection.availability, AVAILABILITY_FOUND);
        assert_eq!(inspection.selection_mode, SELECTION_MODE_SYSTEM);
        assert_eq!(inspection.configured_path, None);
        assert_eq!(
            inspection.resolved_path.as_deref(),
            Some(expected_path.as_str())
        );
        assert_eq!(
            inspection.system_path.as_deref(),
            Some(expected_path.as_str())
        );
        assert_eq!(inspection.version.as_deref(), Some(*expected_version));
        assert_eq!(
            inspection.system_version.as_deref(),
            Some(*expected_version)
        );
        assert!(!inspection.system_path_differs);
        assert_eq!(inspection.error_code, None);

        let wire = serde_json::to_string(inspection).expect("serialize DTO");
        assert!(!wire.contains("bearer-secret-token-alpha"));
        assert!(!wire.contains("user@example.com"));
        assert!(!wire.contains("sk-secret-999"));
    }
}

#[tokio::test]
async fn stable_profile_symlink_update_wins_over_stale_nix_store_path() {
    let temp = TempDir::new().expect("tempdir");

    // Relative directory with a trap script that must never be resolved even if in PATH.
    let relative_trap_dir = temp.path().join("relative-trap");
    let trap_marker = temp.path().join("relative-trap.marker");
    write_executable_script(
        &relative_trap_dir,
        "codex",
        &format!(
            "#!/bin/sh\ntouch \"{}\"\necho \"codex 9.9.9\"\n",
            trap_marker.display()
        ),
    );

    let store_v1_dir = temp
        .path()
        .join("nix/store/11111111111111111111111111111111-codex-0.164.0/bin");
    let store_v2_dir = temp
        .path()
        .join("nix/store/22222222222222222222222222222222-codex-0.165.0/bin");
    let user_profile_bin = temp.path().join("etc/profiles/per-user/any_operator/bin");
    let system_profile_bin = temp.path().join("run/current-system/sw/bin");
    let fallback_usr_bin = temp.path().join("usr/bin");

    fs::create_dir_all(&user_profile_bin).expect("create user profile bin");
    fs::create_dir_all(&system_profile_bin).expect("create system profile bin");
    fs::create_dir_all(&fallback_usr_bin).expect("create fallback usr bin");

    let v1_bin = write_executable_script(
        &store_v1_dir,
        "codex",
        "#!/bin/sh\necho \"codex-cli 0.164.0\"\n",
    );
    let v2_bin = write_executable_script(
        &store_v2_dir,
        "codex",
        "#!/bin/sh\necho \"codex-cli 0.165.0\"\n",
    );

    // Initially, user profile symlink points to v1.
    let profile_symlink = user_profile_bin.join("codex");
    symlink(&v1_bin, &profile_symlink).expect("symlink v1 into user profile");

    // Construct PATH with relative entries and the stale direct /nix/store/v1/bin BEFORE the stable profile entries.
    let raw_path = format!(
        ":relative-trap:./relative-trap:../relative-trap:{}:{}:{}:{}:{}",
        temp.path().join("usr/../relative-trap").display(),
        store_v1_dir.display(),
        user_profile_bin.display(),
        system_profile_bin.display(),
        fallback_usr_bin.display(),
    );
    let path_os = OsString::from(raw_path);

    let ordered = ordered_system_search_dirs(Some(&path_os));
    assert_eq!(
        ordered,
        vec![
            user_profile_bin.clone(),
            system_profile_bin.clone(),
            store_v1_dir.clone(),
            fallback_usr_bin.clone(),
        ],
        "stable Nix profile dirs must precede direct /nix/store PATH entries"
    );

    let tool = build_tool("codex", "Codex CLI", ToolAdapter::CodexCli, None, true);
    let options = VersionInspectionOptions::new().with_path_env(path_os.clone());

    // First inspection resolves v1 through the profile symlink.
    let first = inspect_tool_version_with_options(&tool, &options).await;
    let canonical_v1 = fs::canonicalize(&v1_bin)
        .expect("canonical v1")
        .to_string_lossy()
        .into_owned();
    assert_eq!(first.availability, AVAILABILITY_FOUND);
    assert_eq!(first.selection_mode, SELECTION_MODE_SYSTEM);
    assert_eq!(first.resolved_path.as_deref(), Some(canonical_v1.as_str()));
    assert_eq!(first.system_path.as_deref(), Some(canonical_v1.as_str()));
    assert_eq!(first.version.as_deref(), Some("0.164.0"));
    assert_eq!(first.system_version.as_deref(), Some("0.164.0"));
    assert!(!first.system_path_differs);

    // Simulate `nup` / `nrs` updating the stable profile symlink to v2 while PATH still has store_v1_dir first.
    let staged_symlink = user_profile_bin.join("codex.new");
    symlink(&v2_bin, &staged_symlink).expect("create staged v2 symlink");
    fs::rename(&staged_symlink, &profile_symlink).expect("atomically replace profile symlink");

    // Second inspection with the exact same ToolDefinition and PATH must resolve v2 afresh.
    let second = inspect_tool_version_with_options(&tool, &options).await;
    let canonical_v2 = fs::canonicalize(&v2_bin)
        .expect("canonical v2")
        .to_string_lossy()
        .into_owned();
    assert_eq!(second.availability, AVAILABILITY_FOUND);
    assert_eq!(second.selection_mode, SELECTION_MODE_SYSTEM);
    assert_eq!(second.resolved_path.as_deref(), Some(canonical_v2.as_str()));
    assert_eq!(second.system_path.as_deref(), Some(canonical_v2.as_str()));
    assert_eq!(second.version.as_deref(), Some("0.165.0"));
    assert_eq!(second.system_version.as_deref(), Some("0.165.0"));
    assert!(!second.system_path_differs);
    assert!(!trap_marker.exists());

    // Verify ordinary PATH precedence is preserved when non-store directories precede profile directories.
    let custom_front_bin = temp.path().join("opt/local/bin");
    let front_exec = write_executable_script(
        &custom_front_bin,
        "codex",
        "#!/bin/sh\necho \"codex-cli 0.170.0\"\n",
    );
    let front_path = join_path_env(&[
        custom_front_bin,
        store_v1_dir,
        user_profile_bin,
        fallback_usr_bin,
    ]);
    let resolved_front = resolve_system_executable_in_path(Some(&front_path), &["codex"]);
    assert_eq!(
        resolved_front,
        Some(fs::canonicalize(&front_exec).expect("canonical front"))
    );
}

#[tokio::test]
async fn custom_pin_comparison_reports_system_diff_without_mutation_or_newer_claims() {
    let temp = TempDir::new().expect("tempdir");
    let pinned_store_dir = temp
        .path()
        .join("nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-codex-0.164.0/bin");
    let system_store_dir = temp
        .path()
        .join("nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-codex-0.165.0/bin");
    let profile_bin = temp.path().join(".nix-profile/bin");
    fs::create_dir_all(&profile_bin).expect("create profile bin");

    let pinned_bin = write_executable_script(
        &pinned_store_dir,
        "codex",
        "#!/bin/sh\necho \"codex-cli 0.164.0\"\n",
    );
    let sys_target_bin = write_executable_script(
        &system_store_dir,
        "codex",
        "#!/bin/sh\necho \"codex-cli 0.165.0\"\n",
    );
    symlink(&sys_target_bin, profile_bin.join("codex")).expect("symlink system codex");

    let path_env = join_path_env(&[pinned_store_dir.clone(), profile_bin.clone()]);
    let options = VersionInspectionOptions::new().with_path_env(path_env);

    let pinned_tool = build_tool(
        "codex",
        "Codex CLI",
        ToolAdapter::CodexCli,
        Some(pinned_bin.clone()),
        true,
    );
    let original_tool = pinned_tool.clone();

    let inspected = inspect_tool_version_with_options(&pinned_tool, &options).await;
    assert_eq!(
        pinned_tool, original_tool,
        "version inspection must never mutate ToolDefinition"
    );

    let canonical_pinned = fs::canonicalize(&pinned_bin)
        .expect("canonical pinned")
        .to_string_lossy()
        .into_owned();
    let canonical_sys = fs::canonicalize(&sys_target_bin)
        .expect("canonical sys")
        .to_string_lossy()
        .into_owned();

    assert_eq!(inspected.tool_id, "codex");
    assert_eq!(inspected.availability, AVAILABILITY_FOUND);
    assert_eq!(inspected.selection_mode, SELECTION_MODE_CUSTOM);
    assert_eq!(
        inspected.configured_path.as_deref(),
        Some(pinned_bin.to_string_lossy().as_ref())
    );
    assert_eq!(
        inspected.resolved_path.as_deref(),
        Some(canonical_pinned.as_str())
    );
    assert_eq!(
        inspected.system_path.as_deref(),
        Some(canonical_sys.as_str())
    );
    assert_eq!(inspected.version.as_deref(), Some("0.164.0"));
    assert_eq!(inspected.system_version.as_deref(), Some("0.165.0"));
    assert!(inspected.system_path_differs);
    assert_eq!(inspected.error_code, None);
    let lower_msg = inspected.message.to_ascii_lowercase();
    assert!(
        !lower_msg.contains("newer") && !lower_msg.contains("outdated"),
        "message must not claim system version is newer purely from a different path: {}",
        inspected.message
    );

    // Custom path pointing to the same canonical target as system resolution sets system_path_differs = false.
    let same_target_tool = build_tool(
        "codex",
        "Codex CLI",
        ToolAdapter::CodexCli,
        Some(profile_bin.join("codex")),
        true,
    );
    let same_inspected = inspect_tool_version_with_options(&same_target_tool, &options).await;
    assert_eq!(same_inspected.selection_mode, SELECTION_MODE_CUSTOM);
    assert_eq!(
        same_inspected.resolved_path.as_deref(),
        Some(canonical_sys.as_str())
    );
    assert_eq!(
        same_inspected.system_path.as_deref(),
        Some(canonical_sys.as_str())
    );
    assert!(!same_inspected.system_path_differs);
    assert_eq!(same_inspected.version.as_deref(), Some("0.165.0"));
    assert_eq!(same_inspected.system_version.as_deref(), Some("0.165.0"));

    // After user selects "Use system" (`executable_path: None`), inspection reports system mode.
    let system_mode_tool = build_tool("codex", "Codex CLI", ToolAdapter::CodexCli, None, true);
    let system_inspected = inspect_tool_version_with_options(&system_mode_tool, &options).await;
    assert_eq!(system_inspected.selection_mode, SELECTION_MODE_SYSTEM);
    assert_eq!(system_inspected.configured_path, None);
    assert_eq!(
        system_inspected.resolved_path.as_deref(),
        Some(canonical_sys.as_str())
    );
    assert_eq!(
        system_inspected.system_path.as_deref(),
        Some(canonical_sys.as_str())
    );
    assert_eq!(system_inspected.version.as_deref(), Some("0.165.0"));
    assert_eq!(system_inspected.system_version.as_deref(), Some("0.165.0"));
    assert!(!system_inspected.system_path_differs);
}

#[tokio::test]
async fn api_only_and_disabled_tools_launch_zero_subprocesses() {
    let temp = TempDir::new().expect("tempdir");
    let marker = temp.path().join("unexpected-spawn.marker");
    let trap_bin = write_executable_script(
        temp.path(),
        "codex",
        &format!(
            "#!/bin/sh\ntouch \"{}\"\necho \"1.0.0\"\n",
            marker.display()
        ),
    );

    let tools = vec![
        build_tool(
            "jev",
            "Jev API",
            ToolAdapter::JevApi,
            Some(trap_bin.clone()),
            true,
        ),
        build_tool(
            "openai",
            "OpenAI Decisions API",
            ToolAdapter::OpenaiDecisionsApi,
            None,
            true,
        ),
        build_tool(
            "codex-disabled-sys",
            "Codex Disabled System",
            ToolAdapter::CodexCli,
            None,
            false,
        ),
        build_tool(
            "codex-disabled-custom",
            "Codex Disabled Custom",
            ToolAdapter::CodexCli,
            Some(trap_bin.clone()),
            false,
        ),
    ];

    let path_env = join_path_env(&[temp.path().to_path_buf()]);
    let options = VersionInspectionOptions::new().with_path_env(path_env);
    let results = inspect_tool_versions_with_options(&tools, &options).await;

    assert!(
        !marker.exists(),
        "API tools and disabled tools must launch zero subprocesses"
    );
    assert_eq!(results.len(), 4);

    assert_eq!(results[0].tool_id, "jev");
    assert_eq!(results[0].availability, AVAILABILITY_API_ONLY);
    assert_eq!(results[0].resolved_path, None);
    assert_eq!(results[0].system_path, None);
    assert_eq!(results[0].version, None);
    assert_eq!(results[0].system_version, None);
    assert!(!results[0].system_path_differs);
    assert_eq!(results[0].error_code, None);

    assert_eq!(results[1].tool_id, "openai");
    assert_eq!(results[1].availability, AVAILABILITY_API_ONLY);
    assert_eq!(results[1].selection_mode, SELECTION_MODE_SYSTEM);
    assert_eq!(results[1].error_code, None);

    assert_eq!(results[2].tool_id, "codex-disabled-sys");
    assert_eq!(results[2].availability, AVAILABILITY_DISABLED);
    assert_eq!(results[2].selection_mode, SELECTION_MODE_SYSTEM);
    assert_eq!(results[2].configured_path, None);
    assert_eq!(results[2].resolved_path, None);
    assert_eq!(results[2].version, None);
    assert_eq!(results[2].error_code, None);

    assert_eq!(results[3].tool_id, "codex-disabled-custom");
    assert_eq!(results[3].availability, AVAILABILITY_DISABLED);
    assert_eq!(results[3].selection_mode, SELECTION_MODE_CUSTOM);
    assert_eq!(
        results[3].configured_path.as_deref(),
        Some(trap_bin.to_string_lossy().as_ref())
    );
    assert_eq!(results[3].resolved_path, None);
    assert_eq!(results[3].version, None);
    assert_eq!(results[3].error_code, None);
}

#[tokio::test]
async fn missing_and_invalid_executable_handling_for_system_and_custom_modes() {
    let temp = TempDir::new().expect("tempdir");
    let sys_bin_dir = temp.path().join("sys-bin");
    let sys_git = write_executable_script(
        &sys_bin_dir,
        "git",
        "#!/bin/sh\necho \"git version 2.47.1\"\n",
    );

    let non_exec_file = temp.path().join("non-executable-git");
    fs::write(&non_exec_file, "#!/bin/sh\necho \"git version 2.47.1\"\n")
        .expect("write non-executable");

    let missing_custom = temp.path().join("does-not-exist-git");

    let tools = vec![
        // 1. System tool not in PATH (codegraph)
        build_tool("codegraph", "CodeGraph", ToolAdapter::Codegraph, None, true),
        // 2. Custom tool missing on disk, while system git exists on PATH
        build_tool(
            "git-missing-custom",
            "Git Missing Custom",
            ToolAdapter::Git,
            Some(missing_custom.clone()),
            true,
        ),
        // 3. Custom tool pointing to non-executable file, while system git exists on PATH
        build_tool(
            "git-non-exec",
            "Git Non-Executable",
            ToolAdapter::Git,
            Some(non_exec_file),
            true,
        ),
    ];

    let options = VersionInspectionOptions::new().with_path_env(join_path_env(&[sys_bin_dir]));
    let results = inspect_tool_versions_with_options(&tools, &options).await;
    let canonical_sys_git = fs::canonicalize(&sys_git)
        .expect("canonical sys_git")
        .to_string_lossy()
        .into_owned();

    // 1. System missing
    assert_eq!(results[0].tool_id, "codegraph");
    assert_eq!(results[0].availability, AVAILABILITY_NOT_FOUND);
    assert_eq!(results[0].selection_mode, SELECTION_MODE_SYSTEM);
    assert_eq!(results[0].configured_path, None);
    assert_eq!(results[0].resolved_path, None);
    assert_eq!(results[0].system_path, None);
    assert_eq!(results[0].version, None);
    assert_eq!(results[0].system_version, None);
    assert!(!results[0].system_path_differs);
    assert_eq!(results[0].error_code, None);

    // 2. Custom missing with valid system fallback reported
    assert_eq!(results[1].tool_id, "git-missing-custom");
    assert_eq!(results[1].availability, AVAILABILITY_NOT_FOUND);
    assert_eq!(results[1].selection_mode, SELECTION_MODE_CUSTOM);
    assert_eq!(
        results[1].configured_path.as_deref(),
        Some(missing_custom.to_string_lossy().as_ref())
    );
    assert_eq!(results[1].resolved_path, None);
    assert_eq!(
        results[1].system_path.as_deref(),
        Some(canonical_sys_git.as_str())
    );
    assert_eq!(results[1].version, None);
    assert_eq!(results[1].system_version.as_deref(), Some("2.47.1"));
    assert!(results[1].system_path_differs);
    assert_eq!(results[1].error_code, None);

    // 3. Custom non-executable reports invalid_executable while still reporting system fallback
    assert_eq!(results[2].tool_id, "git-non-exec");
    assert_eq!(results[2].availability, AVAILABILITY_NOT_FOUND);
    assert_eq!(results[2].selection_mode, SELECTION_MODE_CUSTOM);
    assert_eq!(results[2].resolved_path, None);
    assert_eq!(
        results[2].system_path.as_deref(),
        Some(canonical_sys_git.as_str())
    );
    assert_eq!(results[2].version, None);
    assert_eq!(results[2].system_version.as_deref(), Some("2.47.1"));
    assert!(results[2].system_path_differs);
    assert_eq!(results[2].error_code.as_deref(), Some("invalid_executable"));
}

#[tokio::test]
async fn timeout_overflow_and_failure_supervision_with_process_group_cleanup() {
    let temp = TempDir::new().expect("tempdir");
    let pgid_file = temp.path().join("hang-pgid.txt");

    let hang_script = write_executable_script(
        temp.path(),
        "hang-cli",
        &format!(
            r#"#!/bin/sh
python3 -c 'import os, time; open("{}", "w").write(str(os.getpgrp())); os.spawnlp(os.P_NOWAIT, "sleep", "sleep", "60"); time.sleep(60)' 2>/dev/null || (sleep 60 & sleep 60)
"#,
            pgid_file.display()
        ),
    );

    let overflow_script = write_executable_script(
        temp.path(),
        "overflow-cli",
        "#!/bin/sh\ndd if=/dev/zero bs=65536 count=20 2>/dev/null\n",
    );

    let fail_script = write_executable_script(
        temp.path(),
        "fail-cli",
        "#!/bin/sh\necho \"fatal secret-crash-token-123\" >&2\nexit 1\n",
    );

    let tools = vec![
        build_tool(
            "agy-hang",
            "Hang CLI",
            ToolAdapter::Antigravity,
            Some(hang_script),
            true,
        ),
        build_tool(
            "agy-overflow",
            "Overflow CLI",
            ToolAdapter::Antigravity,
            Some(overflow_script),
            true,
        ),
        build_tool(
            "agy-fail",
            "Fail CLI",
            ToolAdapter::Antigravity,
            Some(fail_script),
            true,
        ),
    ];

    let options = VersionInspectionOptions::new()
        .with_empty_path()
        .with_timeout(Duration::from_millis(150));
    let results = inspect_tool_versions_with_options(&tools, &options).await;

    assert_eq!(results[0].tool_id, "agy-hang");
    assert_eq!(results[0].availability, AVAILABILITY_FOUND);
    assert_eq!(results[0].version, None);
    assert_eq!(results[0].error_code.as_deref(), Some("probe_timed_out"));

    if let Ok(raw_pgid) = fs::read_to_string(&pgid_file) {
        if let Ok(pgid) = raw_pgid.trim().parse::<i32>() {
            assert!(
                !is_process_group_alive(pgid),
                "timed-out version inspection process group {pgid} must be terminated and reaped"
            );
        }
    }

    assert_eq!(results[1].tool_id, "agy-overflow");
    assert_eq!(results[1].availability, AVAILABILITY_FOUND);
    assert_eq!(results[1].version, None);
    assert_eq!(
        results[1].error_code.as_deref(),
        Some("probe_output_overflow")
    );

    assert_eq!(results[2].tool_id, "agy-fail");
    assert_eq!(results[2].availability, AVAILABILITY_FOUND);
    assert_eq!(results[2].version, None);
    assert_eq!(results[2].error_code.as_deref(), Some("probe_failed"));
    assert!(!results[2].message.contains("secret-crash-token-123"));
}

#[tokio::test]
async fn preserves_input_order_and_enforces_max_four_concurrent_subprocesses() {
    let temp = TempDir::new().expect("tempdir");
    let active_dir = temp.path().join("active-slots");
    fs::create_dir_all(&active_dir).expect("create active-slots dir");
    let max_seen_file = temp.path().join("max-seen.txt");

    let mut cli_scripts = Vec::new();
    for idx in 0..6 {
        // Earlier indices sleep slightly longer so completion order differs from input order.
        let delay_sec = if idx < 2 { "0.12" } else { "0.03" };
        let script = write_executable_script(
            temp.path(),
            &format!("cli-{idx}"),
            &format!(
                r#"#!/bin/sh
slot="{}/$$"
touch "$slot"
count=$(ls -1 "{}" | wc -l | tr -d ' ')
printf '%s\n' "$count" >> "{}"
sleep {}
rm -f "$slot"
echo "tool 1.{}.0"
"#,
                active_dir.display(),
                active_dir.display(),
                max_seen_file.display(),
                delay_sec,
                idx
            ),
        );
        cli_scripts.push(script);
    }

    let tools = vec![
        build_tool("t0-disabled", "T0", ToolAdapter::Git, None, false),
        build_tool(
            "t1-cli0",
            "T1",
            ToolAdapter::CodexCli,
            Some(cli_scripts[0].clone()),
            true,
        ),
        build_tool("t2-api", "T2", ToolAdapter::JevApi, None, true),
        build_tool(
            "t3-cli1",
            "T3",
            ToolAdapter::OpencodeV2,
            Some(cli_scripts[1].clone()),
            true,
        ),
        build_tool("t4-missing", "T4", ToolAdapter::Codegraph, None, true),
        build_tool(
            "t5-cli2",
            "T5",
            ToolAdapter::Antigravity,
            Some(cli_scripts[2].clone()),
            true,
        ),
        build_tool(
            "t6-cli3",
            "T6",
            ToolAdapter::Git,
            Some(cli_scripts[3].clone()),
            true,
        ),
        build_tool(
            "t7-cli4",
            "T7",
            ToolAdapter::Tmux,
            Some(cli_scripts[4].clone()),
            true,
        ),
        build_tool(
            "t8-cli5",
            "T8",
            ToolAdapter::Codegraph,
            Some(cli_scripts[5].clone()),
            true,
        ),
    ];

    let options = VersionInspectionOptions::new().with_empty_path();
    let results = inspect_tool_versions_with_options(&tools, &options).await;

    let ids: Vec<&str> = results.iter().map(|r| r.tool_id.as_str()).collect();
    assert_eq!(
        ids,
        vec![
            "t0-disabled",
            "t1-cli0",
            "t2-api",
            "t3-cli1",
            "t4-missing",
            "t5-cli2",
            "t6-cli3",
            "t7-cli4",
            "t8-cli5",
        ],
        "inspect_tool_versions must strictly preserve input slice order"
    );

    assert_eq!(results[0].availability, AVAILABILITY_DISABLED);
    assert_eq!(results[1].version.as_deref(), Some("1.0.0"));
    assert_eq!(results[2].availability, AVAILABILITY_API_ONLY);
    assert_eq!(results[3].version.as_deref(), Some("1.1.0"));
    assert_eq!(results[4].availability, AVAILABILITY_NOT_FOUND);
    assert_eq!(results[5].version.as_deref(), Some("1.2.0"));
    assert_eq!(results[6].version.as_deref(), Some("1.3.0"));
    assert_eq!(results[7].version.as_deref(), Some("1.4.0"));
    assert_eq!(results[8].version.as_deref(), Some("1.5.0"));

    if let Ok(counts_raw) = fs::read_to_string(&max_seen_file) {
        let max_concurrent = counts_raw
            .lines()
            .filter_map(|line| line.trim().parse::<usize>().ok())
            .max()
            .unwrap_or(0);
        assert!(
            max_concurrent <= MAX_VERSION_INSPECTION_CONCURRENCY,
            "observed concurrent processes ({max_concurrent}) exceeded cap ({MAX_VERSION_INSPECTION_CONCURRENCY})"
        );
    }

    // Verify top-level `inspect_tool_versions` wire schema serialization.
    let single = inspect_tool_versions(&tools[0..1]).await;
    let json_val = serde_json::to_value(&single[0]).expect("serialize ToolVersionInspection");
    assert_eq!(
        json_val,
        json!({
            "tool_id": "t0-disabled",
            "availability": "disabled",
            "selection_mode": "system",
            "configured_path": null,
            "resolved_path": null,
            "system_path": null,
            "version": null,
            "system_version": null,
            "system_path_differs": false,
            "message": "Tool is disabled; version inspection was skipped.",
            "error_code": null
        })
    );
}
