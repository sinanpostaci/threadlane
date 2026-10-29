#![deny(unsafe_code)]
#![deny(missing_docs)]

//! `threadlane-connectors`: safe CLI metadata probing and OS keychain credential boundary
//! for Threadlane Milestone 4.
//!
//! ## Key Invariants
//!
//! - **Explicit, fixed CLI templates without a shell**: Probes only execute when explicitly
//!   requested (`probe_tool`), never on passive UI page load, and never invoke a shell or
//!   model inference.
//!   - `codex_cli`: `debug models --bundled` and `login status`
//!   - `opencode_v2`: `models --standalone` (plain v2.2 catalog; never v1 `--verbose`/`--provider`)
//!   - `antigravity`: `models`
//!   - `git` / `codegraph`: `--version`; `tmux`: `-V`
//! - **API adapters never spawn a CLI**: `jev_api` and `openai_decisions_api` return immediately
//!   without executing any subprocess.
//! - **Distinct availability vs authentication**: `availability == "found"` proves only that a
//!   valid executable exists on disk, never login or subscription entitlement. Unverified CLI
//!   authentication states remain `"unknown"`, and unverified catalog efforts remain empty for
//!   manual configuration.
//! - **Bounded process-group supervision**: Enforces a 10-second timeout, 1 MiB stdout/stderr
//!   caps, sanitized Git environment variables, and Unix process-group termination/reaping.
//! - **OS keychain credential boundary**: [`KeyringCredentialStore`] uses `keyring` 4.2.0 (`v1`)
//!   with zero plaintext fallback, zero ambient token migration, and zero secret exposure in
//!   `Debug`, `Display`, or serialized errors.

/// OS keychain credential boundary (`CredentialStore`, `KeyringCredentialStore`, and test vault).
pub mod credentials;
/// Safe static connector error types and wire codes.
pub mod error;
/// Deterministic CLI catalog, login status, and version parsers.
pub mod parsers;
/// Explicit bounded CLI metadata probing (`ToolProbe`, `probe_tool`).
pub mod probe;

pub use credentials::{
    validate_tool_id, CredentialStore, FakeCredentialStore, InMemoryCredentialStore,
    KeyringCredentialStore, SecretStatus, SimulatedVaultState, DEFAULT_KEYRING_SERVICE,
    MAX_SECRET_BYTES, MAX_TOOL_ID_BYTES, SECRET_STATUS_CONFIGURED, SECRET_STATUS_LOCKED,
    SECRET_STATUS_NOT_CONFIGURED, SECRET_STATUS_UNAVAILABLE,
};
pub use error::ConnectorError;
pub use parsers::{
    parse_antigravity_models, parse_cli_version, parse_codex_bundled_models,
    parse_codex_login_status, parse_opencode_models, strip_ansi_codes, validate_effort_token,
    validate_model_id, AUTH_STATUS_AUTHENTICATED, AUTH_STATUS_UNAUTHENTICATED, AUTH_STATUS_UNKNOWN,
    MAX_CATALOG_MODELS, MAX_EFFORT_BYTES, MAX_MODEL_ID_BYTES, MAX_MODEL_LABEL_CHARS,
    MAX_VERSION_BYTES,
};
pub use probe::{
    is_process_group_alive, probe_command_template, probe_tool, probe_tool_with_timeout, ToolProbe,
    AVAILABILITY_API_ONLY, AVAILABILITY_FOUND, AVAILABILITY_NOT_FOUND, DEFAULT_PROBE_TIMEOUT_MS,
    MAX_PROBE_STREAM_BYTES,
};
pub use threadlane_registry::{ModelDefinition, ToolAdapter, ToolDefinition};

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

    fn build_tool_definition(
        id: &str,
        label: &str,
        adapter_snake: &str,
        executable_path: Option<PathBuf>,
    ) -> ToolDefinition {
        serde_json::from_value(json!({
            "id": id,
            "label": label,
            "adapter": adapter_snake,
            "executable_path": executable_path,
            "enabled": true
        }))
        .expect("ToolDefinition JSON fixture must deserialize")
    }

    #[cfg(unix)]
    fn write_executable_script(dir: &Path, name: &str, body: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let path = dir.join(name);
        fs::write(&path, body).expect("write test script");
        let mut perms = fs::metadata(&path).expect("metadata").permissions();
        perms.set_mode(0o700);
        fs::set_permissions(&path, perms).expect("chmod 0700");
        path
    }

    #[test]
    fn in_memory_vault_lifecycle_and_secret_redaction() {
        let vault = InMemoryCredentialStore::new();
        assert_eq!(
            vault.status("jev").expect("initial status"),
            SECRET_STATUS_NOT_CONFIGURED
        );
        assert_eq!(vault.get("jev").expect("initial get"), None);

        let secret = "ts_live_super_secret_token_999";
        vault.set("jev", secret).expect("set secret");
        assert_eq!(
            vault.status("jev").expect("configured status"),
            SECRET_STATUS_CONFIGURED
        );
        assert_eq!(
            vault.get("jev").expect("get secret").as_deref(),
            Some(secret)
        );

        let debug_repr = format!("{vault:?}");
        assert!(
            !debug_repr.contains(secret),
            "Debug representation must never contain secret values: {debug_repr}"
        );

        vault.delete("jev").expect("delete secret");
        assert_eq!(
            vault.status("jev").expect("status after delete"),
            SECRET_STATUS_NOT_CONFIGURED
        );
        assert_eq!(vault.get("jev").expect("get after delete"), None);
        // Deleting again is idempotent.
        vault.delete("jev").expect("idempotent second delete");
    }

    #[test]
    fn in_memory_vault_simulates_locked_and_unavailable_states_safely() {
        let vault = InMemoryCredentialStore::new();
        vault
            .set("openai", "sk-proj-secret-value")
            .expect("set when unlocked");

        vault
            .set_simulated_state(SimulatedVaultState::Locked)
            .expect("lock vault");
        assert_eq!(
            vault.status("openai").expect("locked status"),
            SECRET_STATUS_LOCKED
        );
        let err = vault.get("openai").expect_err("get must fail when locked");
        assert_eq!(err.code(), "credential_store_locked");
        assert!(!err.to_string().contains("sk-proj-secret-value"));

        vault
            .set_simulated_state(SimulatedVaultState::Unavailable)
            .expect("mark unavailable");
        assert_eq!(
            vault.status("openai").expect("unavailable status"),
            SECRET_STATUS_UNAVAILABLE
        );
        let err = vault
            .set("openai", "sk-proj-new-secret")
            .expect_err("set must fail when unavailable");
        assert_eq!(err.code(), "credential_store_unavailable");
        assert!(!err.to_string().contains("sk-proj-new-secret"));
    }

    #[test]
    fn credential_store_rejects_invalid_tool_ids_and_secrets() {
        let vault = InMemoryCredentialStore::new();
        assert!(vault.set("", "secret").is_err());
        assert!(vault.set("-bad", "secret").is_err());
        assert!(vault.set("bad/slash", "secret").is_err());
        assert!(vault.set("jev", "").is_err());
        assert!(vault.set("jev", "   ").is_err());
        assert!(vault.set("jev", "bad\0secret").is_err());

        let keyring_store = KeyringCredentialStore::new();
        assert!(format!("{keyring_store:?}").contains("KeyringCredentialStore"));
        assert!(KeyringCredentialStore::with_service("").is_err());
    }

    #[test]
    fn codex_bundled_parser_filters_hidden_models_and_preserves_exact_ids() {
        let raw_json = r#"{
            "models": [
                {
                    "slug": "gpt-5.4",
                    "display_name": "GPT-5.4",
                    "supported_reasoning_levels": [
                        {"effort": "low", "description": "Fast"},
                        {"effort": "medium", "description": "Balanced"},
                        {"effort": "high", "description": "Deep"}
                    ],
                    "default_reasoning_level": "medium",
                    "visibility": "list"
                },
                {
                    "slug": "gpt-6-sol",
                    "display_name": "GPT-6 Sol",
                    "supported_reasoning_levels": [
                        {"effort": "high"},
                        {"effort": "xhigh"}
                    ],
                    "default_reasoning_level": null,
                    "visibility": "list"
                },
                {
                    "slug": "gpt-internal-preview",
                    "display_name": "Internal Hidden",
                    "supported_reasoning_levels": [{"effort": "high"}],
                    "default_reasoning_level": "high",
                    "visibility": "hide"
                },
                {
                    "slug": "gpt-unlisted",
                    "display_name": "Unlisted",
                    "supported_reasoning_levels": [],
                    "default_reasoning_level": null,
                    "visibility": "none"
                }
            ]
        }"#;

        let models =
            parse_codex_bundled_models("codex", raw_json).expect("parse codex bundled models");
        assert_eq!(models.len(), 2);

        assert_eq!(models[0].id, "gpt-5.4");
        assert_eq!(models[0].tool_id, "codex");
        assert_eq!(models[0].label, "GPT-5.4");
        assert_eq!(models[0].model_arg, "gpt-5.4");
        assert_eq!(models[0].efforts, vec!["low", "medium", "high"]);
        assert!(models[0].allow_no_effort);

        assert_eq!(models[1].id, "gpt-6-sol");
        assert_eq!(models[1].model_arg, "gpt-6-sol");
        assert_eq!(models[1].efforts, vec!["high", "xhigh"]);
        assert!(!models[1].allow_no_effort);
    }

    #[test]
    fn opencode_and_antigravity_parsers_do_not_guess_efforts() {
        let opencode_out = "\
opencode-go/deepseek-v4.1-flash
anthropic/claude-sonnet-4-5
opencode-go/deepseek-v4.1-flash
";
        let oc_models =
            parse_opencode_models("opencode", opencode_out).expect("parse opencode models");
        assert_eq!(oc_models.len(), 2);
        assert_eq!(oc_models[0].id, "opencode-go/deepseek-v4.1-flash");
        assert_eq!(oc_models[0].model_arg, "opencode-go/deepseek-v4.1-flash");
        assert!(oc_models[0].efforts.is_empty());
        assert!(oc_models[0].allow_no_effort);

        let agy_out = "\
Available models:
- gemini-4-argon-high  Gemini 4 Argon (High)
- gemini-4-argon-medium
";
        let agy_models =
            parse_antigravity_models("antigravity", agy_out).expect("parse agy models");
        assert_eq!(agy_models.len(), 2);
        assert_eq!(agy_models[0].id, "gemini-4-argon-high");
        assert_eq!(agy_models[0].label, "Gemini 4 Argon (High)");
        assert!(agy_models[0].efforts.is_empty());
        assert!(agy_models[0].allow_no_effort);
    }

    #[tokio::test]
    async fn probe_codex_cli_runs_fixed_templates_and_redacts_raw_output() {
        let temp = TempDir::new().expect("tempdir");
        let script = write_executable_script(
            temp.path(),
            "fake-codex",
            r#"#!/bin/sh
if [ "$1" = "debug" ] && [ "$2" = "models" ] && [ "$3" = "--bundled" ] && [ $# -eq 3 ]; then
  cat <<'EOF'
{"models":[{"slug":"gpt-5.4","display_name":"GPT-5.4","supported_reasoning_levels":[{"effort":"high"}],"default_reasoning_level":"high","visibility":"list"},{"slug":"hidden-model","display_name":"Hidden","supported_reasoning_levels":[],"default_reasoning_level":null,"visibility":"hide"}]}
EOF
  exit 0
fi
if [ "$1" = "login" ] && [ "$2" = "status" ] && [ $# -eq 2 ]; then
  echo "Logged in using ChatGPT (user@example.com bearer-secret-token-xyz)"
  exit 0
fi
exit 2
"#,
        );

        let tool = build_tool_definition("codex", "Codex CLI", "codex_cli", Some(script));
        let probe = probe_tool(&tool).await.expect("probe codex");

        assert_eq!(probe.tool_id, "codex");
        assert_eq!(probe.availability, AVAILABILITY_FOUND);
        assert_eq!(probe.auth_status, AUTH_STATUS_AUTHENTICATED);
        assert_eq!(probe.models.len(), 1);
        assert_eq!(probe.models[0].id, "gpt-5.4");
        assert!(!probe.message.contains("user@example.com"));
        assert!(!probe.message.contains("bearer-secret-token-xyz"));
    }

    #[tokio::test]
    async fn probe_opencode_v2_uses_exact_standalone_args_and_unknown_auth() {
        let temp = TempDir::new().expect("tempdir");
        let args_log = temp.path().join("args.txt");
        let script = write_executable_script(
            temp.path(),
            "fake-opencode",
            &format!(
                r#"#!/bin/sh
printf '%s' "$*" > "{}"
if [ "$1" = "models" ] && [ "$2" = "--standalone" ] && [ $# -eq 2 ]; then
  echo "opencode-go/deepseek-v4.1-flash"
  exit 0
fi
exit 1
"#,
                args_log.display()
            ),
        );

        let tool = build_tool_definition("opencode", "OpenCode", "opencode_v2", Some(script));
        let probe = probe_tool(&tool).await.expect("probe opencode");

        let recorded_args = fs::read_to_string(&args_log).expect("read recorded args");
        assert_eq!(recorded_args, "models --standalone");
        assert_eq!(probe.availability, AVAILABILITY_FOUND);
        assert_eq!(probe.auth_status, AUTH_STATUS_UNKNOWN);
        assert_eq!(probe.models.len(), 1);
        assert_eq!(probe.models[0].id, "opencode-go/deepseek-v4.1-flash");
        assert!(probe.models[0].efforts.is_empty());
    }

    #[tokio::test]
    async fn probe_api_adapter_never_spawns_cli_process() {
        let temp = TempDir::new().expect("tempdir");
        let marker = temp.path().join("spawned.marker");
        let trap_script = write_executable_script(
            temp.path(),
            "trap-cli",
            &format!("#!/bin/sh\ntouch \"{}\"\nexit 0\n", marker.display()),
        );

        let jev_tool = build_tool_definition("jev", "Jev API", "jev_api", Some(trap_script));
        let probe = probe_tool(&jev_tool).await.expect("probe api tool");

        assert!(
            !marker.exists(),
            "API adapter must never spawn a CLI process"
        );
        assert_eq!(probe.availability, AVAILABILITY_API_ONLY);
        assert_eq!(probe.auth_status, AUTH_STATUS_UNKNOWN);
        assert!(probe.models.is_empty());
    }

    #[tokio::test]
    async fn probe_missing_and_non_executable_paths_distinguished() {
        let temp = TempDir::new().expect("tempdir");
        let missing_path = temp.path().join("does-not-exist");
        let missing_tool =
            build_tool_definition("agy", "Antigravity", "antigravity", Some(missing_path));
        let probe = probe_tool(&missing_tool)
            .await
            .expect("missing executable returns not_found");
        assert_eq!(probe.availability, AVAILABILITY_NOT_FOUND);
        assert_eq!(probe.auth_status, AUTH_STATUS_UNKNOWN);
        assert!(probe.models.is_empty());

        let non_exec_path = temp.path().join("not-executable");
        fs::write(&non_exec_path, "#!/bin/sh\nexit 0\n").expect("write non-executable");
        let bad_tool =
            build_tool_definition("agy", "Antigravity", "antigravity", Some(non_exec_path));
        let err = probe_tool(&bad_tool)
            .await
            .expect_err("non-executable file must be rejected");
        assert_eq!(err.code(), "invalid_executable");
    }

    #[tokio::test]
    async fn probe_timeout_and_overflow_terminate_and_reap_process_group() {
        let temp = TempDir::new().expect("tempdir");
        let pgid_file = temp.path().join("pgid.txt");
        let timeout_script = write_executable_script(
            temp.path(),
            "hang-cli",
            &format!(
                r#"#!/bin/sh
python3 -c 'import os, time; open("{}", "w").write(str(os.getpgrp())); os.spawnlp(os.P_NOWAIT, "sleep", "sleep", "60"); time.sleep(60)' 2>/dev/null || (sleep 60 & sleep 60)
"#,
                pgid_file.display()
            ),
        );

        let tool = build_tool_definition("agy", "Antigravity", "antigravity", Some(timeout_script));
        let err = probe_tool_with_timeout(&tool, Duration::from_millis(150))
            .await
            .expect_err("hanging probe must time out");
        assert_eq!(err.code(), "probe_timed_out");

        if let Ok(raw_pgid) = fs::read_to_string(&pgid_file) {
            if let Ok(pgid) = raw_pgid.trim().parse::<i32>() {
                assert!(
                    !is_process_group_alive(pgid),
                    "timed-out probe process group {pgid} must be terminated and reaped"
                );
            }
        }

        let overflow_script = write_executable_script(
            temp.path(),
            "overflow-cli",
            "#!/bin/sh\ndd if=/dev/zero bs=65536 count=20 2>/dev/null\n",
        );
        let overflow_tool =
            build_tool_definition("agy", "Antigravity", "antigravity", Some(overflow_script));
        let overflow_err = probe_tool(&overflow_tool)
            .await
            .expect_err("overflowing probe must fail");
        assert_eq!(overflow_err.code(), "probe_output_overflow");
    }

    #[tokio::test]
    async fn probe_utility_cli_and_sanitized_git_environment() {
        let temp = TempDir::new().expect("tempdir");
        let env_log = temp.path().join("git-env.txt");
        let git_script = write_executable_script(
            temp.path(),
            "fake-git",
            &format!(
                r#"#!/bin/sh
printf 'PROMPT=%s|NOSYS=%s|GLOBAL=%s|SSH=%s' "$GIT_TERMINAL_PROMPT" "$GIT_CONFIG_NOSYSTEM" "$GIT_CONFIG_GLOBAL" "$GIT_SSH_COMMAND" > "{}"
if [ "$1" = "--version" ]; then
  echo "git version 2.47.1"
  exit 0
fi
exit 1
"#,
                env_log.display()
            ),
        );

        let tool = build_tool_definition("git", "Git", "git", Some(git_script));
        let probe = probe_tool(&tool).await.expect("probe git");
        assert_eq!(probe.availability, AVAILABILITY_FOUND);
        assert_eq!(probe.version.as_deref(), Some("2.47.1"));
        assert_eq!(probe.auth_status, AUTH_STATUS_UNKNOWN);
        assert!(probe.models.is_empty());

        let env_captured = fs::read_to_string(&env_log).expect("read env log");
        assert_eq!(env_captured, "PROMPT=0|NOSYS=1|GLOBAL=/dev/null|SSH=");
    }
}
