#![cfg(unix)]

use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::json;
use tempfile::TempDir;
use threadlane_connectors::{
    derive_stable_profile_bin_dirs, extract_supported_wrapper_exec_target,
    inspect_codex_desktop_launcher, inspect_tool_version_with_options, probe_tool_with_options,
    resolve_codex_system_executable, resolve_codex_system_installation,
    resolve_tool_system_executable_with_options, CodexDiscoveryOptions, CodexResolutionSource,
    ToolAdapter, ToolDefinition, VersionInspectionOptions, AUTH_STATUS_AUTHENTICATED,
    AVAILABILITY_FOUND, AVAILABILITY_NOT_FOUND, MAX_SYMLINK_HOPS, SELECTION_MODE_CUSTOM,
    SELECTION_MODE_SYSTEM,
};

fn build_codex_tool(executable_path: Option<PathBuf>) -> ToolDefinition {
    serde_json::from_value(json!({
        "id": "codex",
        "label": "Codex CLI",
        "adapter": "codex_cli",
        "executable_path": executable_path,
        "enabled": true
    }))
    .expect("ToolDefinition JSON fixture must deserialize")
}

fn write_executable_file(path: &Path, content: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent dirs");
    }
    fs::write(path, content).expect("write executable file");
    let mut perms = fs::metadata(path).expect("metadata").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).expect("chmod 0755");
}

struct NixDesktopBundleFixture {
    wrapper_bin_chatgpt: PathBuf,
    resources_dir: PathBuf,
    bundled_codex: PathBuf,
    wrapper_trap_marker: PathBuf,
    cli_args_log: PathBuf,
}

fn create_nix_desktop_bundle_fixture(
    root: &Path,
    hash_tag: &str,
    pkg_version: &str,
    cli_version: &str,
    model_slug: &str,
) -> NixDesktopBundleFixture {
    let store_dir = root.join("nix/store");
    let wrapper_pkg = store_dir.join(format!("{hash_tag}-chatgpt-{pkg_version}"));
    let unwrapped_pkg = store_dir.join(format!("{hash_tag}-chatgpt-unwrapped-{pkg_version}"));

    let wrapper_bin_chatgpt = wrapper_pkg.join("bin/chatgpt");
    let unwrapped_bin_dir = unwrapped_pkg.join("bin");
    let unwrapped_bin_chatgpt = unwrapped_bin_dir.join("chatgpt");
    let lib_chatgpt_dir = unwrapped_pkg.join("lib/chatgpt");
    let codex_launcher = lib_chatgpt_dir.join("codex-launcher");
    let resources_dir = lib_chatgpt_dir.join("resources");
    let metadata_json = resources_dir.join("linux-package-metadata.json");
    let bundled_codex = resources_dir.join("codex");
    let original_backup = resources_dir.join("codex-threadlane-original");

    fs::create_dir_all(&unwrapped_bin_dir).expect("create unwrapped bin dir");
    fs::create_dir_all(&resources_dir).expect("create resources dir");

    // Launcher binary (`../lib/chatgpt/codex-launcher` symlinked from `bin/chatgpt`).
    let launcher_trap = root.join(format!("launcher-executed-{hash_tag}.marker"));
    write_executable_file(
        &codex_launcher,
        &format!(
            "#!/bin/sh\ntouch \"{}\"\nexit 99\n",
            launcher_trap.display()
        ),
    );
    symlink("../lib/chatgpt/codex-launcher", &unwrapped_bin_chatgpt)
        .expect("symlink unwrapped bin/chatgpt -> ../lib/chatgpt/codex-launcher");

    // Real installed metadata keys: codexAppBrand, codexBuildFlavor, version.
    fs::write(
        &metadata_json,
        serde_json::to_string_pretty(&json!({
            "codexAppBrand": "chatgpt",
            "codexBuildFlavor": "prod",
            "version": pkg_version
        }))
        .expect("serialize metadata"),
    )
    .expect("write linux-package-metadata.json");

    // Active bundled `resources/codex` and ignored `codex-threadlane-original`.
    let cli_args_log = root.join(format!("codex-args-{hash_tag}.log"));
    write_executable_file(
        &bundled_codex,
        &format!(
            r#"#!/bin/sh
printf '%s\n' "$*" >> "{}"
if [ "$1" = "--version" ] && [ $# -eq 1 ]; then
  echo "codex-cli {cli_version}"
  exit 0
fi
if [ "$1" = "debug" ] && [ "$2" = "models" ] && [ "$3" = "--bundled" ] && [ $# -eq 3 ]; then
  printf '{{"models":[{{"slug":"{model_slug}","display_name":"Model {model_slug}","supported_reasoning_levels":[{{"effort":"high"}}],"default_reasoning_level":"high","visibility":"list"}}]}}'
  exit 0
fi
if [ "$1" = "login" ] && [ "$2" = "status" ] && [ $# -eq 2 ]; then
  echo "Logged in using ChatGPT"
  exit 0
fi
exit 2
"#,
            cli_args_log.display()
        ),
    );

    let orig_trap = root.join(format!("original-executed-{hash_tag}.marker"));
    write_executable_file(
        &original_backup,
        &format!(
            "#!/bin/sh\ntouch \"{}\"\necho \"codex-cli 0.0.1-original\"\nexit 0\n",
            orig_trap.display()
        ),
    );

    // Nix shell wrapper (`bin/chatgpt`) with literal `exec "<unwrapped>/bin/chatgpt" "$@"`.
    // Must NEVER be executed during static discovery.
    let wrapper_trap_marker = root.join(format!("wrapper-executed-{hash_tag}.marker"));
    write_executable_file(
        &wrapper_bin_chatgpt,
        &format!(
            "#!/bin/sh\n# Nix wrapper script\nexec \"{}\" --ozone-platform-hint=auto \"$@\"\n",
            unwrapped_bin_chatgpt.display()
        ),
    );

    NixDesktopBundleFixture {
        wrapper_bin_chatgpt,
        resources_dir,
        bundled_codex,
        wrapper_trap_marker,
        cli_args_log,
    }
}

#[tokio::test]
async fn stable_profile_alias_switches_old_to_new_bundle_and_keeps_package_and_cli_versions_distinct(
) {
    let temp = TempDir::new().expect("tempdir");
    let old_bundle = create_nix_desktop_bundle_fixture(
        temp.path(),
        "11111111111111111111111111111111",
        "26.1001.10000",
        "0.150.0",
        "gpt-5.4-old",
    );
    let new_bundle = create_nix_desktop_bundle_fixture(
        temp.path(),
        "22222222222222222222222222222222",
        "26.1027.51404",
        "0.165.0",
        "gpt-5.4-new",
    );

    // Build `/etc/profiles/per-user/testuser/bin/chatgpt` -> Home Manager profile -> wrapper.
    let hm_bin = temp.path().join("nix/store/aaaa-home-manager-path/bin");
    let per_user_bin = temp.path().join("etc/profiles/per-user/testuser/bin");
    fs::create_dir_all(&hm_bin).expect("create hm bin");
    fs::create_dir_all(&per_user_bin).expect("create per-user bin");

    let hm_chatgpt = hm_bin.join("chatgpt");
    let profile_chatgpt = per_user_bin.join("chatgpt");
    let duplicate_alias = per_user_bin.join("codex-desktop");

    symlink(&old_bundle.wrapper_bin_chatgpt, &hm_chatgpt).expect("symlink hm -> old wrapper");
    symlink(&hm_chatgpt, &profile_chatgpt).expect("symlink profile -> hm");
    symlink(&hm_chatgpt, &duplicate_alias).expect("symlink duplicate alias -> hm");

    let discovery = CodexDiscoveryOptions::isolated()
        .with_root_prefix(temp.path())
        .with_user("testuser");

    let resolved_old =
        resolve_codex_system_installation(&discovery).expect("resolve old desktop bundle");
    let expected_old_codex = fs::canonicalize(&old_bundle.bundled_codex).expect("canonical old");
    assert_eq!(resolved_old.executable_path, expected_old_codex);
    assert_eq!(
        resolved_old.source,
        CodexResolutionSource::StableProfileDesktopBundle
    );
    assert_eq!(
        resolved_old
            .desktop_metadata
            .as_ref()
            .expect("desktop metadata")
            .version,
        "26.1001.10000"
    );
    assert!(!old_bundle.wrapper_trap_marker.exists());

    // Atomically switch the Home Manager symlink from the old bundle to the new bundle.
    fs::remove_file(&hm_chatgpt).expect("remove old hm symlink");
    symlink(&new_bundle.wrapper_bin_chatgpt, &hm_chatgpt).expect("symlink hm -> new wrapper");

    let resolved_new =
        resolve_codex_system_installation(&discovery).expect("resolve new desktop bundle");
    let expected_new_codex = fs::canonicalize(&new_bundle.bundled_codex).expect("canonical new");
    assert_eq!(resolved_new.executable_path, expected_new_codex);
    let meta = resolved_new
        .desktop_metadata
        .as_ref()
        .expect("new desktop metadata");
    assert_eq!(meta.brand, "chatgpt");
    assert_eq!(meta.build_flavor, "prod");
    assert_eq!(meta.version, "26.1027.51404");

    // Version inspection reports the CLI `--version` ("0.165.0"), distinct from desktop package version ("26.1027.51404").
    let tool = build_codex_tool(None);
    let options = VersionInspectionOptions::new().with_codex_discovery(discovery);
    let inspection = inspect_tool_version_with_options(&tool, &options).await;
    assert_eq!(inspection.availability, AVAILABILITY_FOUND);
    assert_eq!(inspection.selection_mode, SELECTION_MODE_SYSTEM);
    assert_eq!(inspection.version.as_deref(), Some("0.165.0"));
    assert_eq!(inspection.system_version.as_deref(), Some("0.165.0"));
    assert_ne!(inspection.version.as_deref(), Some("26.1027.51404"));
    assert_eq!(
        inspection.resolved_path.as_deref(),
        Some(expected_new_codex.to_string_lossy().as_ref())
    );
}

#[tokio::test]
async fn stale_inherited_resources_path_loses_to_current_stable_profile_desktop_bundle() {
    let temp = TempDir::new().expect("tempdir");
    let stale_bundle = create_nix_desktop_bundle_fixture(
        temp.path(),
        "aaaa1111111111111111111111111111",
        "26.0900.00000",
        "0.140.0",
        "gpt-5.4-stale",
    );
    let current_bundle = create_nix_desktop_bundle_fixture(
        temp.path(),
        "bbbb2222222222222222222222222222",
        "26.1027.51404",
        "0.165.0",
        "gpt-5.4-current",
    );

    let per_user_bin = temp.path().join("etc/profiles/per-user/operator/bin");
    fs::create_dir_all(&per_user_bin).expect("create per-user bin");
    symlink(
        &current_bundle.wrapper_bin_chatgpt,
        per_user_bin.join("chatgpt"),
    )
    .expect("symlink current chatgpt in stable profile");

    // Inherited PATH puts the stale running desktop bundle's `resources` dir first
    // and omits standalone `codex` from the stable profile.
    let inherited_path =
        std::env::join_paths([&stale_bundle.resources_dir, &per_user_bin]).expect("join PATH");
    let discovery = CodexDiscoveryOptions::isolated()
        .with_path_env(inherited_path)
        .with_root_prefix(temp.path())
        .with_user("operator");

    let expected_current =
        fs::canonicalize(&current_bundle.bundled_codex).expect("canonical current codex");
    assert_eq!(
        resolve_codex_system_executable(&discovery),
        Some(expected_current.clone())
    );

    let tool = build_codex_tool(None);
    let options = VersionInspectionOptions::new().with_codex_discovery(discovery);
    let inspection = inspect_tool_version_with_options(&tool, &options).await;
    assert_eq!(inspection.availability, AVAILABILITY_FOUND);
    assert_eq!(inspection.version.as_deref(), Some("0.165.0"));
    assert_eq!(
        inspection.resolved_path.as_deref(),
        Some(expected_current.to_string_lossy().as_ref())
    );
    assert!(
        !stale_bundle.cli_args_log.exists(),
        "stale inherited resources/codex must not be invoked when a valid stable profile bundle exists"
    );
}

#[tokio::test]
async fn explicit_custom_pin_stays_exact_with_no_fallback_while_system_candidate_is_separate() {
    let temp = TempDir::new().expect("tempdir");
    let pinned_bundle = create_nix_desktop_bundle_fixture(
        temp.path(),
        "cccc1111111111111111111111111111",
        "26.0800.00000",
        "0.130.0",
        "gpt-5.4-pinned",
    );
    let system_bundle = create_nix_desktop_bundle_fixture(
        temp.path(),
        "dddd2222222222222222222222222222",
        "26.1027.51404",
        "0.165.0",
        "gpt-5.4-system",
    );

    let per_user_bin = temp.path().join("etc/profiles/per-user/operator/bin");
    fs::create_dir_all(&per_user_bin).expect("create per-user bin");
    symlink(
        &system_bundle.wrapper_bin_chatgpt,
        per_user_bin.join("chatgpt"),
    )
    .expect("symlink system chatgpt");

    let discovery = CodexDiscoveryOptions::isolated()
        .with_root_prefix(temp.path())
        .with_user("operator");
    let v_options = VersionInspectionOptions::new().with_codex_discovery(discovery.clone());

    // 1. Existing old custom pin stays exact and reports separate system candidate.
    let pinned_tool = build_codex_tool(Some(pinned_bundle.bundled_codex.clone()));
    let inspection = inspect_tool_version_with_options(&pinned_tool, &v_options).await;
    let canonical_pinned = fs::canonicalize(&pinned_bundle.bundled_codex).expect("canonical pin");
    let canonical_system = fs::canonicalize(&system_bundle.bundled_codex).expect("canonical sys");

    assert_eq!(inspection.availability, AVAILABILITY_FOUND);
    assert_eq!(inspection.selection_mode, SELECTION_MODE_CUSTOM);
    assert_eq!(
        inspection.resolved_path.as_deref(),
        Some(canonical_pinned.to_string_lossy().as_ref())
    );
    assert_eq!(
        inspection.system_path.as_deref(),
        Some(canonical_system.to_string_lossy().as_ref())
    );
    assert_eq!(inspection.version.as_deref(), Some("0.130.0"));
    assert_eq!(inspection.system_version.as_deref(), Some("0.165.0"));
    assert!(inspection.system_path_differs);

    let probe = probe_tool_with_options(&pinned_tool, Duration::from_secs(3), &discovery)
        .await
        .expect("probe pinned tool");
    assert_eq!(probe.availability, AVAILABILITY_FOUND);
    assert_eq!(probe.models[0].id, "gpt-5.4-pinned");

    // 2. Missing custom pin has NO automatic fallback in either probe_tool or inspect_tool_version.
    let missing_pin = temp.path().join("nonexistent-pinned-codex");
    let missing_tool = build_codex_tool(Some(missing_pin.clone()));
    let missing_inspection = inspect_tool_version_with_options(&missing_tool, &v_options).await;
    assert_eq!(missing_inspection.availability, AVAILABILITY_NOT_FOUND);
    assert_eq!(missing_inspection.selection_mode, SELECTION_MODE_CUSTOM);
    assert_eq!(missing_inspection.resolved_path, None);
    assert_eq!(missing_inspection.version, None);
    assert_eq!(
        missing_inspection.system_path.as_deref(),
        Some(canonical_system.to_string_lossy().as_ref())
    );
    assert_eq!(
        missing_inspection.system_version.as_deref(),
        Some("0.165.0")
    );
    assert!(missing_inspection.system_path_differs);

    let missing_probe = probe_tool_with_options(&missing_tool, Duration::from_secs(3), &discovery)
        .await
        .expect("probe missing pinned tool");
    assert_eq!(missing_probe.availability, AVAILABILITY_NOT_FOUND);
    assert!(missing_probe.models.is_empty());
}

#[tokio::test]
async fn automatic_full_probe_and_version_resolver_agree_on_none_target() {
    let temp = TempDir::new().expect("tempdir");
    let stale_bundle = create_nix_desktop_bundle_fixture(
        temp.path(),
        "eeee1111111111111111111111111111",
        "26.0900.00000",
        "0.140.0",
        "gpt-5.4-stale",
    );
    let active_bundle = create_nix_desktop_bundle_fixture(
        temp.path(),
        "ffff2222222222222222222222222222",
        "26.1027.51404",
        "0.165.0",
        "gpt-5.4-active",
    );

    let per_user_bin = temp.path().join("etc/profiles/per-user/operator/bin");
    fs::create_dir_all(&per_user_bin).expect("create per-user bin");
    symlink(
        &active_bundle.wrapper_bin_chatgpt,
        per_user_bin.join("chatgpt"),
    )
    .expect("symlink active chatgpt");

    let inherited_path =
        std::env::join_paths([&stale_bundle.resources_dir, &per_user_bin]).expect("join PATH");
    let discovery = CodexDiscoveryOptions::isolated()
        .with_path_env(inherited_path)
        .with_root_prefix(temp.path())
        .with_user("operator");
    let v_options = VersionInspectionOptions::new().with_codex_discovery(discovery.clone());

    let auto_tool = build_codex_tool(None);
    let expected_target =
        fs::canonicalize(&active_bundle.bundled_codex).expect("canonical active codex");

    let resolved_via_helper =
        resolve_tool_system_executable_with_options(&ToolAdapter::CodexCli, &v_options)
            .expect("resolve_tool_system_executable_with_options")
            .expect("must find active bundle");
    assert_eq!(resolved_via_helper, expected_target);

    let inspection = inspect_tool_version_with_options(&auto_tool, &v_options).await;
    assert_eq!(inspection.availability, AVAILABILITY_FOUND);
    assert_eq!(
        inspection.resolved_path.as_deref(),
        Some(expected_target.to_string_lossy().as_ref())
    );
    assert_eq!(inspection.version.as_deref(), Some("0.165.0"));

    let probe = probe_tool_with_options(&auto_tool, Duration::from_secs(3), &discovery)
        .await
        .expect("probe automatic codex tool");
    assert_eq!(probe.availability, AVAILABILITY_FOUND);
    assert_eq!(probe.auth_status, AUTH_STATUS_AUTHENTICATED);
    assert_eq!(probe.models.len(), 1);
    assert_eq!(probe.models[0].id, "gpt-5.4-active");

    let recorded_calls = fs::read_to_string(&active_bundle.cli_args_log).expect("read args log");
    assert!(recorded_calls.contains("--version"));
    assert!(recorded_calls.contains("debug models --bundled"));
    assert!(recorded_calls.contains("login status"));
    assert!(!stale_bundle.cli_args_log.exists());
}

#[test]
fn literal_runtime_setup_is_discovered_without_sourcing_or_launching_it() {
    let temp = TempDir::new().expect("tempdir");
    let bundle = create_nix_desktop_bundle_fixture(
        temp.path(),
        "runtime11111111111111111111111111",
        "26.1027.51404",
        "0.165.0",
        "gpt-fixture",
    );
    let marker = temp.path().join("runtime-script-ran");
    let runtime_script = temp.path().join("patch-runtime.sh");
    fs::write(&runtime_script, format!("touch {}\n", marker.display())).unwrap();
    let wrapper = temp.path().join("chatgpt");
    write_executable_file(&wrapper, &format!(
        "#!/bin/sh\n. {}\nexport DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=${{DOTNET_SYSTEM_GLOBALIZATION_INVARIANT-'1'}}\nexec \"{}\" ${{NIXOS_OZONE_WL:+${{WAYLAND_DISPLAY:+--ozone-platform=wayland}}}} \"$@\"\n",
        runtime_script.display(), bundle.wrapper_bin_chatgpt.display()
    ));
    let installation = inspect_codex_desktop_launcher(&wrapper).expect("literal setup wrapper");
    assert_eq!(
        installation.executable_path,
        fs::canonicalize(bundle.bundled_codex).unwrap()
    );
    assert!(
        !marker.exists(),
        "static discovery must never source runtime code"
    );
    assert!(!bundle.wrapper_trap_marker.exists());
    for setup in [
        ". $RUNTIME",
        ". /tmp/setup.sh arg",
        ". /tmp/../setup.sh",
        "source $(which setup)",
    ] {
        assert!(extract_supported_wrapper_exec_target(&format!(
            "#!/bin/sh\n{setup}\nexec /usr/bin/chatgpt \"$@\"\n"
        ))
        .is_err());
    }
}

#[test]
fn symlink_cycles_and_depth_bounds_are_rejected() {
    let temp = TempDir::new().expect("tempdir");

    // 1. Symlink cycle: a -> b -> a
    let link_a = temp.path().join("cycle-a");
    let link_b = temp.path().join("cycle-b");
    symlink(&link_b, &link_a).expect("symlink a -> b");
    symlink(&link_a, &link_b).expect("symlink b -> a");

    let cycle_err = inspect_codex_desktop_launcher(&link_a).expect_err("cycle must fail");
    assert_eq!(cycle_err.code(), "symlink_cycle");

    // 2. Symlink depth exceeding MAX_SYMLINK_HOPS (16)
    let target = temp.path().join("deep-target");
    write_executable_file(&target, "#!/bin/sh\nexit 0\n");
    let mut prev = target;
    for hop in 0..=MAX_SYMLINK_HOPS {
        let next = temp.path().join(format!("hop-{hop}"));
        symlink(&prev, &next).expect("create hop symlink");
        prev = next;
    }

    let depth_err =
        inspect_codex_desktop_launcher(&prev).expect_err("excessive symlink depth must fail");
    assert_eq!(depth_err.code(), "symlink_depth_exceeded");
}

#[test]
fn unsupported_wrapper_scripts_and_dynamic_exec_targets_are_rejected_without_execution() {
    let invalid_scripts = [
        // Missing shebang
        "exec \"/nix/store/hash-chatgpt-unwrapped-26.1027.51404/bin/chatgpt\" \"$@\"\n",
        // Dynamic variable expansion inside target
        "#!/bin/sh\nexec \"$CODEX_ROOT/bin/chatgpt\" \"$@\"\n",
        "#!/bin/sh\nexec \"${NIX_STORE}/hash-chatgpt/bin/chatgpt\" \"$@\"\n",
        // Command substitution
        "#!/bin/sh\nexec \"$(which chatgpt)\" \"$@\"\n",
        "#!/bin/sh\nexec \"`which chatgpt`\" \"$@\"\n",
        // Relative or traversing target
        "#!/bin/sh\nexec \"../lib/chatgpt/codex-launcher\" \"$@\"\n",
        "#!/bin/sh\nexec \"/nix/store/hash-chatgpt/../evil/bin/chatgpt\" \"$@\"\n",
        // Shell chaining / redirection on exec line
        "#!/bin/sh\nexec \"/nix/store/hash-chatgpt/bin/chatgpt\" \"$@\"; touch /tmp/pwn\n",
        // Arbitrary shell command before exec
        "#!/bin/sh\ntouch /tmp/should-never-run\nexec \"/nix/store/hash-chatgpt/bin/chatgpt\" \"$@\"\n",
        // Statements after exec
        "#!/bin/sh\nexec \"/nix/store/hash-chatgpt/bin/chatgpt\" \"$@\"\necho after\n",
        // No exec line
        "#!/bin/sh\n/nix/store/hash-chatgpt/bin/chatgpt \"$@\"\n",
    ];

    for script in invalid_scripts {
        let err = extract_supported_wrapper_exec_target(script)
            .expect_err("unsupported or dynamic wrapper script must be rejected");
        assert_eq!(
            err.code(),
            "unsupported_wrapper_script",
            "rejected script: {script}"
        );
    }

    // Verify file-level inspection never executes a trap script and blocks fallback to stale PATH.
    let temp = TempDir::new().expect("tempdir");
    let stale_bundle = create_nix_desktop_bundle_fixture(
        temp.path(),
        "99991111111111111111111111111111",
        "26.0900.00000",
        "0.140.0",
        "gpt-5.4-stale",
    );

    let per_user_bin = temp.path().join("etc/profiles/per-user/operator/bin");
    fs::create_dir_all(&per_user_bin).expect("create per-user bin");
    let trap_marker = temp.path().join("trap-executed.marker");
    let bad_wrapper = per_user_bin.join("chatgpt");
    write_executable_file(
        &bad_wrapper,
        &format!(
            "#!/bin/sh\ntouch \"{}\"\nexec \"$DYNAMIC_TARGET/bin/chatgpt\" \"$@\"\n",
            trap_marker.display()
        ),
    );

    let err = inspect_codex_desktop_launcher(&bad_wrapper)
        .expect_err("bad wrapper must fail static inspection");
    assert_eq!(err.code(), "unsupported_wrapper_script");
    assert!(
        !trap_marker.exists(),
        "static discovery must never execute the desktop wrapper script"
    );

    // When the stable profile desktop candidate is broken, system resolution returns None
    // rather than falling back to the stale `/nix/store` resources PATH entry.
    let inherited_path =
        std::env::join_paths([&stale_bundle.resources_dir, &per_user_bin]).expect("join PATH");
    let discovery = CodexDiscoveryOptions::isolated()
        .with_path_env(inherited_path)
        .with_root_prefix(temp.path())
        .with_user("operator");
    assert_eq!(resolve_codex_system_installation(&discovery), None);
    assert!(!trap_marker.exists());
}

#[test]
fn metadata_key_and_version_mismatches_and_missing_or_nonexecutable_backends_are_rejected() {
    let temp = TempDir::new().expect("tempdir");

    // 1. Generic `brand`/`buildFlavor` keys (missing real `codexAppBrand`/`codexBuildFlavor`) must fail.
    let wrong_keys_root = temp.path().join("case-wrong-keys");
    let wrong_keys_bundle = create_nix_desktop_bundle_fixture(
        &wrong_keys_root,
        "abcd1111111111111111111111111111",
        "26.1027.51404",
        "0.165.0",
        "gpt-5.4",
    );
    fs::write(
        wrong_keys_bundle
            .resources_dir
            .join("linux-package-metadata.json"),
        serde_json::to_string(&json!({
            "brand": "chatgpt",
            "buildFlavor": "prod",
            "version": "26.1027.51404"
        }))
        .expect("serialize"),
    )
    .expect("overwrite metadata with generic keys");
    let err = inspect_codex_desktop_launcher(&wrong_keys_bundle.wrapper_bin_chatgpt).expect_err(
        "generic brand/buildFlavor keys without codexAppBrand/codexBuildFlavor must fail",
    );
    assert_eq!(err.code(), "invalid_package_metadata");

    // 2. Mismatched metadata version vs Nix store directory version must fail.
    let mismatch_root = temp.path().join("case-version-mismatch");
    let mismatch_bundle = create_nix_desktop_bundle_fixture(
        &mismatch_root,
        "abcd2222222222222222222222222222",
        "26.1027.51404",
        "0.165.0",
        "gpt-5.4",
    );
    fs::write(
        mismatch_bundle
            .resources_dir
            .join("linux-package-metadata.json"),
        serde_json::to_string(&json!({
            "codexAppBrand": "chatgpt",
            "codexBuildFlavor": "prod",
            "version": "26.9999.00000"
        }))
        .expect("serialize"),
    )
    .expect("overwrite metadata with mismatched version");
    let err = inspect_codex_desktop_launcher(&mismatch_bundle.wrapper_bin_chatgpt)
        .expect_err("mismatched metadata version must fail");
    assert_eq!(err.code(), "invalid_package_metadata");

    // 3. Non-executable `resources/codex` must fail even when `codex-threadlane-original` is executable.
    let nonexec_root = temp.path().join("case-nonexec");
    let nonexec_bundle = create_nix_desktop_bundle_fixture(
        &nonexec_root,
        "abcd3333333333333333333333333333",
        "26.1027.51404",
        "0.165.0",
        "gpt-5.4",
    );
    let mut perms = fs::metadata(&nonexec_bundle.bundled_codex)
        .expect("metadata")
        .permissions();
    perms.set_mode(0o644);
    fs::set_permissions(&nonexec_bundle.bundled_codex, perms).expect("chmod 0644");
    let err = inspect_codex_desktop_launcher(&nonexec_bundle.wrapper_bin_chatgpt)
        .expect_err("non-executable resources/codex must fail");
    assert_eq!(err.code(), "invalid_bundled_executable");

    // 4. Missing `resources/codex` must fail and never fall back to `codex-threadlane-original`.
    fs::remove_file(&nonexec_bundle.bundled_codex).expect("remove resources/codex");
    let err = inspect_codex_desktop_launcher(&nonexec_bundle.wrapper_bin_chatgpt)
        .expect_err("missing resources/codex must fail even when -original exists");
    assert_eq!(err.code(), "invalid_bundled_executable");

    // 5. `resources/codex` symlinked to `codex-threadlane-original` must also be rejected.
    symlink(
        nonexec_bundle
            .resources_dir
            .join("codex-threadlane-original"),
        &nonexec_bundle.bundled_codex,
    )
    .expect("symlink codex -> codex-threadlane-original");
    let err = inspect_codex_desktop_launcher(&nonexec_bundle.wrapper_bin_chatgpt)
        .expect_err("codex pointing to codex-threadlane-original must fail");
    assert_eq!(err.code(), "invalid_bundled_executable");
}

#[test]
fn derive_stable_profile_bin_dirs_rejects_invalid_or_traversing_inputs() {
    let temp = TempDir::new().expect("tempdir");
    let discovery = CodexDiscoveryOptions::isolated()
        .with_root_prefix(temp.path())
        .with_user("../evil")
        .with_home_dir(PathBuf::from("relative/home"))
        .with_profile_bin_dirs([
            PathBuf::from("relative/bin"),
            temp.path().join("etc/profiles/../evil/bin"),
            temp.path().join("etc/profiles/per-user/valid_user/bin"),
        ]);

    let dirs = derive_stable_profile_bin_dirs(&discovery);
    assert!(
        dirs.iter()
            .all(|d| d.is_absolute() && !d.to_string_lossy().contains("evil")),
        "derived profile dirs must exclude relative or traversing paths: {dirs:?}"
    );
    assert!(dirs.contains(&temp.path().join("etc/profiles/per-user/valid_user/bin")));
    assert_eq!(
        CodexDiscoveryOptions::isolated()
            .with_optional_path_env(Some(OsString::from("relative/path")))
            .with_user("valid_user")
            .with_root_prefix(temp.path())
            .user
            .as_deref(),
        Some("valid_user")
    );
}
