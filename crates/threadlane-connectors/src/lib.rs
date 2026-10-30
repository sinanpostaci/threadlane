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
//!   - `opencode_v2`: `api --standalone model.list` (standalone v2.2 catalog with per-model variants; never v1 `--verbose`/`--provider`)
//!   - `antigravity`: `models`
//!   - `git` / `codegraph`: `--version`; `tmux`: `-V`
//! - **API adapters never spawn a CLI**: `jev_api` and `openai_decisions_api` return immediately
//!   without executing any subprocess.
//! - **Distinct availability vs authentication and runtime entitlement**: `availability == "found"`
//!   proves only that a valid executable exists on disk, and discovered catalog models/efforts
//!   reflect installed CLI metadata rather than verified account routes or runtime subscription
//!   entitlement. Unverified CLI authentication states remain `"unknown"`, and models without
//!   evidenced catalog efforts leave `efforts` empty with `allow_no_effort: true`.
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
        assert_eq!(agy_models[0].model_arg, "gemini-4-argon-high");
        assert_eq!(agy_models[0].label, "Gemini 4 Argon (High)");
        assert_eq!(agy_models[0].efforts, vec!["high"]);
        assert!(agy_models[0].allow_no_effort);

        // Bare model row without a parenthesized display label must not infer efforts.
        assert_eq!(agy_models[1].id, "gemini-4-argon-medium");
        assert_eq!(agy_models[1].model_arg, "gemini-4-argon-medium");
        assert_eq!(agy_models[1].label, "gemini-4-argon-medium");
        assert!(agy_models[1].efforts.is_empty());
        assert!(agy_models[1].allow_no_effort);
    }

    #[test]
    fn opencode_model_list_json_discovers_variants_and_redacts_metadata() {
        let raw_json = r#"{
            "location": "/home/alice/.local/share/opencode/secret-location.json",
            "data": [
                {
                    "id": "deepseek-v4.1-flash",
                    "providerID": "opencode-go",
                    "modelID": "deepseek/deepseek-chat-v4.1-alias",
                    "name": "DeepSeek V4.1 Flash",
                    "enabled": true,
                    "type": "chat",
                    "headers": {
                        "Authorization": "Bearer sk-sentinel-model-header-secret"
                    },
                    "variants": [
                        {
                            "id": "low",
                            "settings": {
                                "reasoningEffort": "low",
                                "apiKey": "sk-sentinel-variant-low-secret"
                            }
                        },
                        {
                            "id": "high",
                            "settings": {
                                "reasoningEffort": "high",
                                "headers": {"X-Api-Key": "sk-sentinel-variant-high-secret"}
                            }
                        },
                        {
                            "id": "max",
                            "settings": {
                                "reasoningEffort": "max",
                                "body": {"secret_token": "sk-sentinel-variant-max-secret"}
                            }
                        },
                        {
                            "id": "high",
                            "settings": {"duplicate": true}
                        }
                    ]
                },
                {
                    "id": "deepseek-v4.1-flash",
                    "providerID": "opencode",
                    "modelID": "deepseek-v4.1-flash",
                    "name": "DeepSeek V4.1 Flash",
                    "enabled": true,
                    "type": "chat",
                    "variants": [
                        {"id": "low", "settings": {}},
                        {"id": "high", "settings": {}}
                    ]
                },
                {
                    "id": "claude-sonnet-4-5",
                    "providerID": "anthropic",
                    "modelID": "claude-3-5-sonnet-20241022",
                    "name": "Claude Sonnet 4.5",
                    "enabled": true,
                    "type": "chat",
                    "variants": []
                },
                {
                    "id": "disabled-model",
                    "providerID": "opencode-go",
                    "modelID": "disabled-model",
                    "name": "Disabled Model",
                    "enabled": false,
                    "type": "chat",
                    "variants": [{"id": "high", "settings": {}}]
                },
                {
                    "id": "text-embedding-3-large",
                    "providerID": "openai",
                    "modelID": "text-embedding-3-large",
                    "name": "Text Embedding 3 Large",
                    "enabled": true,
                    "type": "embedding",
                    "variants": []
                },
                {
                    "id": "rerank-v3",
                    "providerID": "cohere",
                    "modelID": "rerank-english-v3.0",
                    "name": "Rerank v3",
                    "enabled": true,
                    "type": "rerank",
                    "variants": []
                }
            ]
        }"#;

        let models =
            parse_opencode_models("opencode", raw_json).expect("parse opencode model.list JSON");
        assert_eq!(models.len(), 3);

        // 1. opencode-go/deepseek-v4.1-flash: selectable providerID/id (NOT modelID alias or bare id)
        assert_eq!(models[0].id, "opencode-go/deepseek-v4.1-flash");
        assert_eq!(models[0].tool_id, "opencode");
        assert_eq!(models[0].model_arg, "opencode-go/deepseek-v4.1-flash");
        assert_ne!(models[0].id, "deepseek/deepseek-chat-v4.1-alias");
        assert_ne!(models[0].model_arg, "deepseek/deepseek-chat-v4.1-alias");
        assert_ne!(models[0].id, "deepseek-v4.1-flash");
        assert_eq!(models[0].label, "DeepSeek V4.1 Flash (opencode-go)");
        assert_eq!(models[0].efforts, vec!["low", "high", "max"]);
        assert!(models[0].allow_no_effort);

        // 2. opencode/deepseek-v4.1-flash: distinct provider route and differentiated label
        assert_eq!(models[1].id, "opencode/deepseek-v4.1-flash");
        assert_eq!(models[1].model_arg, "opencode/deepseek-v4.1-flash");
        assert_eq!(models[1].label, "DeepSeek V4.1 Flash (opencode)");
        assert_ne!(models[0].label, models[1].label);
        assert_eq!(models[1].efforts, vec!["low", "high"]);
        assert!(models[1].allow_no_effort);

        // 3. anthropic/claude-sonnet-4-5: no variants -> empty efforts, allow_no_effort true
        assert_eq!(models[2].id, "anthropic/claude-sonnet-4-5");
        assert_eq!(models[2].model_arg, "anthropic/claude-sonnet-4-5");
        assert_eq!(models[2].label, "Claude Sonnet 4.5 (anthropic)");
        assert!(models[2].efforts.is_empty());
        assert!(models[2].allow_no_effort);

        let serialized = serde_json::to_string(&models).expect("serialize models");
        let debug_repr = format!("{models:?}");
        for sentinel in [
            "secret-location.json",
            "sk-sentinel-model-header-secret",
            "sk-sentinel-variant-low-secret",
            "sk-sentinel-variant-high-secret",
            "sk-sentinel-variant-max-secret",
            "reasoningEffort",
            "deepseek-chat-v4.1-alias",
        ] {
            assert!(
                !serialized.contains(sentinel) && !debug_repr.contains(sentinel),
                "DTO must never expose raw metadata or sentinel {sentinel}"
            );
        }
    }

    #[test]
    fn opencode_model_list_preserves_none_variant_and_namespaced_model_ids() {
        let payload = r#"{"data":[{"providerID":"openrouter","id":"openrouter/free","enabled":true,"type":"chat","variants":[{"id":"none"},{"id":"high"}]}]}"#;
        let models = parse_opencode_models("opencode", payload).expect("valid catalog metadata");
        assert_eq!(models[0].id, "openrouter/openrouter/free");
        assert_eq!(models[0].model_arg, "openrouter/openrouter/free");
        assert_eq!(models[0].efforts, vec!["none", "high"]);
    }

    #[test]
    fn opencode_model_list_rejects_invalid_schema_and_malicious_tokens_safely() {
        let invalid_payloads = [
            r#"{"location": "local"}"#,
            r#"{"location": "local", "data": "not-an-array"}"#,
            r#"{"location": "local", "data": []}"#,
            r#"{"data": ["not-an-object"]}"#,
            r#"{"data": [{"id": "deepseek-v4.1-flash", "enabled": true, "type": "chat"}]}"#,
            r#"{"data": [{"providerID": "opencode-go", "enabled": true, "type": "chat"}]}"#,
            r#"{"data": [{"id": "deepseek-v4.1-flash", "providerID": "opencode/nested", "enabled": true, "type": "chat"}]}"#,
            r#"{"data": [{"id": "deepseek-v4.1-flash", "providerID": "../evil", "enabled": true, "type": "chat"}]}"#,
            r#"{"data": [{"id": "-bad-flag", "providerID": "opencode-go", "enabled": true, "type": "chat"}]}"#,
            r#"{"data": [{"id": "deepseek-v4.1-flash", "providerID": "opencode-go", "enabled": "yes", "type": "chat"}]}"#,
            r#"{"data": [{"id": "deepseek-v4.1-flash", "providerID": "opencode-go", "enabled": true, "type": 123}]}"#,
            r#"{"data": [{"id": "deepseek-v4.1-flash", "providerID": "opencode-go", "enabled": true, "type": "chat", "variants": "high"}]}"#,
            r#"{"data": [{"id": "deepseek-v4.1-flash", "providerID": "opencode-go", "enabled": true, "type": "chat", "variants": ["high"]}]}"#,
            r#"{"data": [{"id": "deepseek-v4.1-flash", "providerID": "opencode-go", "enabled": true, "type": "chat", "variants": [{"settings": {"apiKey": "sk-leak-sentinel-999"}}]}]}"#,
            r#"{"data": [{"id": "deepseek-v4.1-flash", "providerID": "opencode-go", "enabled": true, "type": "chat", "variants": [{"id": ""}]}]}"#,
            r#"{"data": [{"id": "deepseek-v4.1-flash", "providerID": "opencode-go", "enabled": true, "type": "chat", "variants": [{"id": " high "}]}]}"#,
            r#"{"data": [{"id": "deepseek-v4.1-flash", "providerID": "opencode-go", "enabled": true, "type": "chat", "variants": [{"id": "-high"}]}]}"#,
            r#"{"data": [{"id": "deepseek-v4.1-flash", "providerID": "opencode-go", "enabled": true, "type": "chat", "variants": [{"id": "high#inject"}]}]}"#,
            r#"{"data": [{"id": "deepseek-v4.1-flash", "providerID": "opencode-go", "enabled": true, "type": "chat", "variants": [{"id": "high;rm -rf /", "settings": {"token": "sk-leak-sentinel-999"}}]}]}"#,
        ];

        for payload in invalid_payloads {
            let err = parse_opencode_models("opencode", payload)
                .expect_err("invalid or malicious OpenCode JSON payload must fail");
            assert_eq!(err.code(), "parse_error");
            let wire = serde_json::to_string(&err).expect("serialize error");
            assert!(!err.to_string().contains("sk-leak-sentinel-999"));
            assert!(!format!("{err:?}").contains("sk-leak-sentinel-999"));
            assert!(!wire.contains("sk-leak-sentinel-999"));
            assert!(!err.to_string().contains("rm -rf"));
        }

        // Oversized variant count (> 32 distinct efforts) must fail with static ParseError.
        let many_variants: Vec<serde_json::Value> = (0..33)
            .map(|i| json!({"id": format!("level-{i}"), "settings": {}}))
            .collect();
        let oversized_json = serde_json::to_string(&json!({
            "location": "local",
            "data": [{
                "id": "deepseek-v4.1-flash",
                "providerID": "opencode-go",
                "enabled": true,
                "type": "chat",
                "variants": many_variants
            }]
        }))
        .expect("serialize oversized variants");
        let err = parse_opencode_models("opencode", &oversized_json)
            .expect_err("exceeding MAX_EFFORTS_PER_MODEL must fail");
        assert_eq!(err.code(), "parse_error");
    }

    #[test]
    fn antigravity_parser_discovers_mutually_agreeing_efforts_and_leaves_ambiguous_unknown() {
        let agy_out = "\
gemini-4-argon-medium\tGemini 4 Argon (Medium)
gemini-4-argon-high\tGemini 4 Argon (High)
gemini-4-argon-low\tGemini 4 Argon (Low)
gemini-3.8-flash-high\tGemini 3.8 Flash (High)
claude-opus-5-5-high\tClaude Opus 5.5 (High)
gpt-oss-120b-medium\tGPT-OSS 120B (Medium)
gpt-6-sol-xhigh\tGPT-6 Sol (XHigh)
gpt-6-astra-max\tGPT-6 Astra (Max)
claude-sonnet-4-5\tClaude Sonnet 4.5
gemini-4-argon-preview\tGemini 4 Argon (Preview)
gemini-4-argon-conflict-high\tGemini 4 Argon Conflict (Medium)
gemini-4-argon-idonly-high\tGemini 4 Argon IdOnly
gemini-4-argon-labelonly\tGemini 4 Argon LabelOnly (High)
";

        let models =
            parse_antigravity_models("antigravity", agy_out).expect("parse agy models output");
        assert_eq!(models.len(), 13);

        let expected: &[(&str, &str, &[&str])] = &[
            (
                "gemini-4-argon-medium",
                "Gemini 4 Argon (Medium)",
                &["medium"],
            ),
            ("gemini-4-argon-high", "Gemini 4 Argon (High)", &["high"]),
            ("gemini-4-argon-low", "Gemini 4 Argon (Low)", &["low"]),
            (
                "gemini-3.8-flash-high",
                "Gemini 3.8 Flash (High)",
                &["high"],
            ),
            ("claude-opus-5-5-high", "Claude Opus 5.5 (High)", &["high"]),
            ("gpt-oss-120b-medium", "GPT-OSS 120B (Medium)", &["medium"]),
            ("gpt-6-sol-xhigh", "GPT-6 Sol (XHigh)", &["xhigh"]),
            ("gpt-6-astra-max", "GPT-6 Astra (Max)", &["max"]),
            ("claude-sonnet-4-5", "Claude Sonnet 4.5", &[]),
            ("gemini-4-argon-preview", "Gemini 4 Argon (Preview)", &[]),
            (
                "gemini-4-argon-conflict-high",
                "Gemini 4 Argon Conflict (Medium)",
                &[],
            ),
            ("gemini-4-argon-idonly-high", "Gemini 4 Argon IdOnly", &[]),
            (
                "gemini-4-argon-labelonly",
                "Gemini 4 Argon LabelOnly (High)",
                &[],
            ),
        ];

        for (model, (exp_id, exp_label, exp_efforts)) in models.iter().zip(expected.iter()) {
            assert_eq!(model.id, *exp_id);
            assert_eq!(model.model_arg, *exp_id);
            assert_eq!(model.tool_id, "antigravity");
            assert_eq!(model.label, *exp_label);
            let expected_vec: Vec<String> = exp_efforts.iter().map(|s| (*s).to_string()).collect();
            assert_eq!(model.efforts, expected_vec, "mismatch for model {exp_id}");
            assert!(model.allow_no_effort);
        }
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
        assert_eq!(
            probe_command_template(&ToolAdapter::OpencodeV2),
            Some(&["api", "--standalone", "model.list"][..])
        );

        let temp = TempDir::new().expect("tempdir");
        let args_log = temp.path().join("args.txt");
        let script = write_executable_script(
            temp.path(),
            "fake-opencode",
            &format!(
                r#"#!/bin/sh
printf '%s' "$*" > "{}"
if [ "$1" = "api" ] && [ "$2" = "--standalone" ] && [ "$3" = "model.list" ] && [ $# -eq 3 ]; then
  cat <<'EOF'
{{"location":"/tmp/opencode-secret-path","data":[{{"id":"deepseek-v4.1-flash","providerID":"opencode-go","modelID":"deepseek-chat","name":"DeepSeek V4.1 Flash","enabled":true,"type":"chat","variants":[{{"id":"low","settings":{{"apiKey":"sk-secret-probe-token"}}}},{{"id":"high","settings":{{}}}},{{"id":"max","settings":{{}}}}]}}]}}
EOF
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
        assert_eq!(recorded_args, "api --standalone model.list");
        assert_eq!(probe.availability, AVAILABILITY_FOUND);
        assert_eq!(probe.auth_status, AUTH_STATUS_UNKNOWN);
        assert_eq!(probe.models.len(), 1);
        assert_eq!(probe.models[0].id, "opencode-go/deepseek-v4.1-flash");
        assert_eq!(probe.models[0].model_arg, "opencode-go/deepseek-v4.1-flash");
        assert_eq!(probe.models[0].label, "DeepSeek V4.1 Flash (opencode-go)");
        assert_eq!(probe.models[0].efforts, vec!["low", "high", "max"]);
        assert!(probe.models[0].allow_no_effort);
        assert!(!probe.message.contains("manual configuration"));
        assert!(!probe.message.contains("sk-secret-probe-token"));
        assert!(!probe.message.contains("opencode-secret-path"));
    }

    #[tokio::test]
    async fn probe_antigravity_uses_exact_models_args_and_discovers_efforts() {
        assert_eq!(
            probe_command_template(&ToolAdapter::Antigravity),
            Some(&["models"][..])
        );

        let temp = TempDir::new().expect("tempdir");
        let args_log = temp.path().join("agy-args.txt");
        let script = write_executable_script(
            temp.path(),
            "fake-agy",
            &format!(
                r#"#!/bin/sh
printf '%s' "$*" > "{}"
if [ "$1" = "models" ] && [ $# -eq 1 ]; then
  printf 'gemini-4-argon-high\tGemini 4 Argon (High)\ngemini-4-argon-medium\tGemini 4 Argon (Medium)\n'
  exit 0
fi
exit 1
"#,
                args_log.display()
            ),
        );

        let tool = build_tool_definition("agy", "Antigravity", "antigravity", Some(script));
        let probe = probe_tool(&tool).await.expect("probe antigravity");

        let recorded_args = fs::read_to_string(&args_log).expect("read recorded args");
        assert_eq!(recorded_args, "models");
        assert_eq!(probe.availability, AVAILABILITY_FOUND);
        assert_eq!(probe.auth_status, AUTH_STATUS_UNKNOWN);
        assert_eq!(probe.models.len(), 2);
        assert_eq!(probe.models[0].id, "gemini-4-argon-high");
        assert_eq!(probe.models[0].model_arg, "gemini-4-argon-high");
        assert_eq!(probe.models[0].efforts, vec!["high"]);
        assert_eq!(probe.models[1].id, "gemini-4-argon-medium");
        assert_eq!(probe.models[1].model_arg, "gemini-4-argon-medium");
        assert_eq!(probe.models[1].efforts, vec!["medium"]);
        assert!(!probe.message.contains("manual configuration"));
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
