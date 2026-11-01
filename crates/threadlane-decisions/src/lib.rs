#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! `threadlane-decisions`: advisory task-difficulty classification, shadow advice cache,
//! and model catalog client for Jev (`https://api.typesafe.ai/v1/systemone`) and
//! OpenAI Decisions (`https://api.openai.com/v1/decisions`).
//!
//! All provider interactions use fixed HTTPS origins, disabled HTTP redirects, bounded
//! timeouts and response sizes, and vault-backed credentials kept strictly inside Rust.
//! Classifications are advisory only: low-confidence answers and provider refusals
//! require human/orchestrator review and never trigger automatic approval, plan/profile
//! mutation, or model substitution.

mod advice;
mod client;
mod error;
mod schema;
mod transport;
mod types;

pub use advice::{
    classify_advice, list_advice_models, preview_advice, sha256_hex, AdviceClassifyOutput,
    AdviceError, AdviceModelsOutput, AdvicePreviewOutput, AdviceRequest, AdviceScope, AdviceSource,
    ADVICE_CACHE_SCHEMA_VERSION, ADVICE_MODE_SHADOW, ADVICE_PROMPT_VERSION,
    ADVICE_REQUEST_SCHEMA_VERSION, DEFAULT_CACHE_TTL_SECONDS, MAX_CACHE_DIR_ENTRIES,
    MAX_CACHE_RECORDS, MAX_CACHE_RECORD_BYTES, MAX_CACHE_TOTAL_BYTES, MAX_CACHE_TTL_SECONDS,
    MAX_REQUEST_FILE_BYTES, MIN_CACHE_TTL_SECONDS,
};
pub use client::DecisionClient;
pub use error::DecisionError;
pub use schema::{
    JevCatalogEntry, MAX_CATALOG_MODELS, MAX_IDENTIFIER_BYTES, MAX_TASK_SUMMARY_BYTES,
};
pub use transport::{
    ensure_success_status, DecisionTransport, ReqwestTransport, TransportFuture, TransportMethod,
    TransportRequest, TransportResponse, CONNECT_TIMEOUT_SECS, JEV_CLASSIFY_URL, JEV_MODELS_URL,
    MAX_RESPONSE_BYTES, OPENAI_CLASSIFY_URL, OPENAI_MODELS_URL, REQUEST_TIMEOUT_SECS,
};
pub use types::{
    DecisionAdvice, DecisionProvider, DecisionRequest, DEFAULT_JEV_MODEL,
    DEFAULT_OPENAI_DECISION_MODEL, MAX_JS_SAFE_INTEGER, MIN_CONFIDENCE_THRESHOLD,
    SYNTHETIC_TEST_TASK_SUMMARY,
};

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::env;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use fs2::FileExt;
    use threadlane_connectors::{ConnectorError, CredentialStore};
    use threadlane_registry::{
        Difficulty, ModelChoice, ModelDefinition, RegistryChange, RegistryStore, RoleAssignment,
        ToolAdapter, ToolDefinition, WorkflowProfile, WorkflowRole,
    };

    use super::*;
    use crate::advice::{
        classify_advice_with_options, preview_advice_with_options, AdviceRuntimeOptions,
    };

    static TEST_DIR_SEQ: AtomicU64 = AtomicU64::new(1);

    struct ScopedTestWorkspace {
        base: PathBuf,
        pub project_root: PathBuf,
        pub registry_dir: PathBuf,
        pub plans_dir: PathBuf,
        pub cache_dir: PathBuf,
    }

    impl ScopedTestWorkspace {
        fn new(label: &str) -> Self {
            let seq = TEST_DIR_SEQ.fetch_add(1, Ordering::Relaxed);
            let raw_tmp = fs::canonicalize(env::temp_dir()).unwrap_or_else(|_| env::temp_dir());
            let base = raw_tmp.join(format!(
                "threadlane-decisions-{label}-{}-{seq}",
                std::process::id()
            ));
            if base.exists() {
                let _ = fs::remove_dir_all(&base);
            }
            fs::create_dir_all(&base).expect("create test base dir");
            fs::set_permissions(&base, fs::Permissions::from_mode(0o700))
                .expect("chmod 0700 test base");

            let project_root = base.join("project");
            let registry_dir = base.join("registry");
            let plans_dir = base.join("plans");
            let cache_dir = plans_dir.join("advice-cache");

            fs::create_dir_all(&project_root).expect("create project_root");
            fs::set_permissions(&project_root, fs::Permissions::from_mode(0o700))
                .expect("chmod 0700 project_root");

            fs::create_dir_all(&registry_dir).expect("create registry_dir");
            fs::set_permissions(&registry_dir, fs::Permissions::from_mode(0o700))
                .expect("chmod 0700 registry_dir");

            fs::create_dir_all(&plans_dir).expect("create plans_dir");
            fs::set_permissions(&plans_dir, fs::Permissions::from_mode(0o700))
                .expect("chmod 0700 plans_dir");

            Self {
                base,
                project_root,
                registry_dir,
                plans_dir,
                cache_dir,
            }
        }

        fn scope(&self, context_id: &str) -> AdviceScope {
            AdviceScope {
                registry_dir: self.registry_dir.clone(),
                cache_dir: self.cache_dir.clone(),
                project_root: self.project_root.clone(),
                context_id: context_id.to_string(),
                cache_ttl_seconds: DEFAULT_CACHE_TTL_SECONDS,
            }
        }

        fn write_private_file(&self, name: &str, contents: &[u8]) -> PathBuf {
            let path = self.base.join(name);
            fs::write(&path, contents).expect("write private test file");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 private test file");
            path
        }

        fn seed_jev_registry(&self) -> u64 {
            let store =
                RegistryStore::open(self.registry_dir.clone()).expect("open registry store");
            store
                .apply(
                    0,
                    RegistryChange::UpsertTool {
                        tool: ToolDefinition {
                            id: "jev-api".to_string(),
                            label: "TypeSafe Jev API".to_string(),
                            adapter: ToolAdapter::JevApi,
                            executable_path: None,
                            enabled: true,
                        },
                    },
                )
                .expect("upsert jev tool");
            let snap = store
                .apply(
                    1,
                    RegistryChange::UpsertModels {
                        models: vec![
                            ModelDefinition {
                                id: "jev-api:jev-latest".to_string(),
                                tool_id: "jev-api".to_string(),
                                label: "jev-latest".to_string(),
                                model_arg: "jev-latest".to_string(),
                                efforts: Vec::new(),
                                allow_no_effort: true,
                            },
                            ModelDefinition {
                                id: "jev-api:jev-1.14.0".to_string(),
                                tool_id: "jev-api".to_string(),
                                label: "jev-1.14.0".to_string(),
                                model_arg: "jev-1.14.0".to_string(),
                                efforts: Vec::new(),
                                allow_no_effort: true,
                            },
                        ],
                    },
                )
                .expect("upsert jev models");
            snap.revision
        }
    }

    impl Drop for ScopedTestWorkspace {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.base);
        }
    }

    #[derive(Default)]
    struct FakeVault {
        secrets: Mutex<HashMap<String, String>>,
        reads: Mutex<usize>,
        fail_reads: Mutex<bool>,
    }

    impl FakeVault {
        fn with_secret(tool_id: &str, secret: &str) -> Arc<Self> {
            let mut map = HashMap::new();
            map.insert(tool_id.to_string(), secret.to_string());
            Arc::new(Self {
                secrets: Mutex::new(map),
                reads: Mutex::new(0),
                fail_reads: Mutex::new(false),
            })
        }

        fn failing() -> Arc<Self> {
            Arc::new(Self {
                secrets: Mutex::new(HashMap::new()),
                reads: Mutex::new(0),
                fail_reads: Mutex::new(true),
            })
        }

        fn read_count(&self) -> usize {
            *self.reads.lock().expect("vault reads lock")
        }
    }

    impl CredentialStore for FakeVault {
        fn set(&self, tool_id: &str, secret: &str) -> Result<(), ConnectorError> {
            self.secrets
                .lock()
                .expect("vault lock")
                .insert(tool_id.to_string(), secret.to_string());
            Ok(())
        }

        fn get(&self, tool_id: &str) -> Result<Option<String>, ConnectorError> {
            *self.reads.lock().expect("vault reads lock") += 1;
            if *self.fail_reads.lock().expect("fail_reads lock") {
                return Err(ConnectorError::CredentialStoreUnavailable(
                    "OS credential store is unavailable.",
                ));
            }
            Ok(self
                .secrets
                .lock()
                .expect("vault lock")
                .get(tool_id)
                .cloned())
        }

        fn delete(&self, tool_id: &str) -> Result<(), ConnectorError> {
            self.secrets.lock().expect("vault lock").remove(tool_id);
            Ok(())
        }

        fn status(&self, tool_id: &str) -> Result<String, ConnectorError> {
            if *self.fail_reads.lock().expect("fail_reads lock") {
                return Err(ConnectorError::CredentialStoreUnavailable(
                    "OS credential store is unavailable.",
                ));
            }
            let configured = self
                .secrets
                .lock()
                .expect("vault lock")
                .contains_key(tool_id);
            Ok(if configured {
                "configured".to_string()
            } else {
                "not_configured".to_string()
            })
        }
    }

    #[derive(Default)]
    struct FakeTransport {
        requests: Mutex<Vec<TransportRequest>>,
        responses: Mutex<VecDeque<Result<TransportResponse, DecisionError>>>,
        on_execute: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    }

    impl FakeTransport {
        fn with_responses(
            responses: impl IntoIterator<Item = Result<TransportResponse, DecisionError>>,
        ) -> Arc<Self> {
            Arc::new(Self {
                requests: Mutex::new(Vec::new()),
                responses: Mutex::new(responses.into_iter().collect()),
                on_execute: Mutex::new(None),
            })
        }

        fn set_on_execute(&self, hook: impl Fn() + Send + Sync + 'static) {
            *self.on_execute.lock().expect("on_execute lock") = Some(Box::new(hook));
        }

        fn recorded_requests(&self) -> Vec<TransportRequest> {
            self.requests.lock().expect("requests lock").clone()
        }
    }

    impl DecisionTransport for FakeTransport {
        fn execute<'a>(&'a self, request: TransportRequest) -> TransportFuture<'a> {
            Box::pin(async move {
                self.requests.lock().expect("requests lock").push(request);
                if let Some(hook) = self.on_execute.lock().expect("on_execute lock").as_ref() {
                    hook();
                }
                self.responses
                    .lock()
                    .expect("responses lock")
                    .pop_front()
                    .unwrap_or(Err(DecisionError::TransportFailed))
            })
        }
    }

    fn json_response(status: u16, body: &str) -> Result<TransportResponse, DecisionError> {
        Ok(TransportResponse {
            status,
            body: body.as_bytes().to_vec(),
        })
    }

    fn valid_jev_four_way_response(
        model: &str,
        choice: &str,
        confidence: f64,
        probs: (f64, f64, f64, f64),
        input_tokens: u64,
        output_tokens: u64,
    ) -> Result<TransportResponse, DecisionError> {
        let (easy, medium, hard, extreme) = probs;
        let body = format!(
            r#"{{
                "model": "{model}",
                "answers": {{
                    "difficulty": {{
                        "type": "choice",
                        "choice": "{choice}",
                        "confidence": {confidence},
                        "probabilities": {{
                            "easy": {easy},
                            "medium": {medium},
                            "hard": {hard},
                            "extreme": {extreme}
                        }}
                    }}
                }},
                "usage": {{"input_tokens": {input_tokens}, "output_tokens": {output_tokens}}}
            }}"#
        );
        json_response(200, &body)
    }

    #[test]
    fn client_init_does_not_read_vault_or_invoke_network() {
        let vault = FakeVault::with_secret("jev-tool", "secret-jev-key");
        let transport = FakeTransport::with_responses([]);
        let _client =
            DecisionClient::with_transport(vault.clone(), transport.clone()).expect("client init");
        let _prod_client = DecisionClient::new(vault.clone()).expect("prod client init");
        let _test_base_client = DecisionClient::with_test_base_urls(
            vault.clone(),
            "http://127.0.0.1:8080",
            "http://127.0.0.1:8081",
        )
        .expect("test base urls init");
        assert_eq!(
            DecisionClient::with_test_base_urls(vault.clone(), "", "http://127.0.0.1:8081")
                .expect_err("empty base url should fail"),
            DecisionError::InvalidEndpoint
        );

        assert_eq!(vault.read_count(), 0);
        assert!(transport.recorded_requests().is_empty());
    }

    #[tokio::test]
    async fn missing_secret_returns_not_configured_without_network_call() {
        let vault = Arc::new(FakeVault::default());
        let transport = FakeTransport::with_responses([]);
        let client = DecisionClient::with_transport(vault, transport.clone()).expect("client init");

        let err = client
            .models("jev-tool", DecisionProvider::Jev)
            .await
            .expect_err("missing secret must fail");
        assert_eq!(err, DecisionError::NotConfigured);
        assert_eq!(err.code(), "not_configured");
        assert!(transport.recorded_requests().is_empty());
    }

    #[tokio::test]
    async fn jev_models_catalog_returns_only_server_models_and_never_injects_unreturned_versions() {
        let vault = FakeVault::with_secret("jev-tool", "ts-secret-123");
        let transport = FakeTransport::with_responses([
            json_response(
                200,
                r#"{"models":[{"name":"jev-latest","description":"Latest alias","release_date":"2026-09-15"}]}"#,
            ),
            json_response(
                200,
                r#"{"models":[{"name":"jev-latest"},{"name":"jev-latest"}]}"#,
            ),
        ]);
        let client = DecisionClient::with_transport(vault, transport.clone()).expect("client init");

        let models = client
            .models("jev-tool", DecisionProvider::Jev)
            .await
            .expect("jev catalog should succeed");
        assert_eq!(
            models.len(),
            1,
            "catalog must contain only server-returned models without injecting jev-1.14.0"
        );
        assert_eq!(models[0].id, "jev-tool:jev-latest");
        assert_eq!(models[0].model_arg, "jev-latest");
        assert!(models[0].efforts.is_empty());
        assert!(models[0].allow_no_effort);

        let reqs = transport.recorded_requests();
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0].url, JEV_MODELS_URL);
        assert_eq!(reqs[0].method, TransportMethod::Get);
        assert!(!format!("{:?}", reqs[0]).contains("ts-secret-123"));

        let dup_err = client
            .models("jev-tool", DecisionProvider::Jev)
            .await
            .expect_err("duplicate model names must be rejected");
        assert_eq!(dup_err, DecisionError::InvalidResponse);
    }

    #[tokio::test]
    async fn openai_models_catalog_filters_to_supported_decision_model_only() {
        let vault = FakeVault::with_secret("openai-decisions", "sk-proj-secret");
        let transport = FakeTransport::with_responses([json_response(
            200,
            r#"{"object":"list","data":[{"id":"gpt-5"},{"id":"gpt-6-luna"},{"id":"text-embedding-3-large"}]}"#,
        )]);
        let client = DecisionClient::with_transport(vault, transport.clone()).expect("client init");

        let models = client
            .models("openai-decisions", DecisionProvider::Openai)
            .await
            .expect("openai catalog should succeed");
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].id, "openai-decisions:gpt-6-luna");
        assert_eq!(models[0].model_arg, "gpt-6-luna");
        assert!(models[0].efforts.is_empty());
        assert!(models[0].allow_no_effort);
    }

    #[tokio::test]
    async fn jev_classify_parses_four_way_choice_keeps_low_confidence_advisory_and_rejects_noul() {
        let vault = FakeVault::with_secret("jev-tool", "ts-secret-key");
        let transport = FakeTransport::with_responses([
            valid_jev_four_way_response(
                "jev-1.14.0",
                "easy",
                0.95,
                (0.91, 0.05, 0.03, 0.01),
                100,
                10,
            ),
            valid_jev_four_way_response(
                "jev-1.14.0",
                "hard",
                0.62,
                (0.05, 0.20, 0.65, 0.10),
                120,
                12,
            ),
            valid_jev_four_way_response(
                "jev-1.14.0",
                "extreme",
                0.88,
                (0.01, 0.04, 0.15, 0.80),
                140,
                14,
            ),
            // Noul is a yes/no probability primitive, NOT a valid Choice answer!
            json_response(
                200,
                r#"{
                    "model": "jev-1.14.0",
                    "answers": {
                        "difficulty": {
                            "type": "noul",
                            "noul": 0.98
                        }
                    },
                    "usage": {"input_tokens": 90, "output_tokens": 5}
                }"#,
            ),
            // Incomplete probability map (only 2 options instead of all 4) must be rejected.
            json_response(
                200,
                r#"{
                    "model": "jev-1.14.0",
                    "answers": {
                        "difficulty": {
                            "type": "choice",
                            "choice": "easy",
                            "confidence": 0.95,
                            "probabilities": {"easy": 0.97, "hard": 0.03}
                        }
                    },
                    "usage": {"input_tokens": 100, "output_tokens": 10}
                }"#,
            ),
        ]);
        let client = DecisionClient::with_transport(vault, transport.clone()).expect("client init");

        let easy_advice = client
            .test_decision("jev-tool", DecisionProvider::Jev, "jev-1.14.0")
            .await
            .expect("jev easy classification");
        assert_eq!(easy_advice.provider, DecisionProvider::Jev);
        assert_eq!(easy_advice.model, "jev-1.14.0");
        assert_eq!(easy_advice.difficulty, Some(Difficulty::Easy));
        assert_eq!(easy_advice.confidence, Some(0.95));
        assert!(!easy_advice.needs_review);
        assert_eq!(easy_advice.reason_code, "classified");
        assert_eq!(easy_advice.input_tokens, Some(100));
        assert_eq!(easy_advice.output_tokens, Some(10));

        // Verify exact outbound Jev schema.
        let recorded = transport.recorded_requests();
        assert_eq!(recorded[0].url, JEV_CLASSIFY_URL);
        let sent_json: serde_json::Value =
            serde_json::from_str(recorded[0].json_body.as_deref().expect("post body"))
                .expect("valid request json");
        assert_eq!(sent_json["model"], "jev-1.14.0");
        assert_eq!(sent_json["state"]["task"], SYNTHETIC_TEST_TASK_SUMMARY);
        assert_eq!(sent_json["questions"]["difficulty"]["type"], "choice");
        assert!(sent_json["questions"]["difficulty"]["criteria"]["easy"].is_string());
        assert!(sent_json["questions"]["difficulty"]["criteria"]["medium"].is_string());
        assert!(sent_json["questions"]["difficulty"]["criteria"]["hard"].is_string());
        assert!(sent_json["questions"]["difficulty"]["criteria"]["extreme"].is_string());

        // Low-confidence Choice remains valid and advisory (`needs_review = true`, `reason_code = "low_confidence"`).
        let low_conf = client
            .classify(DecisionRequest {
                tool_id: "jev-tool".to_string(),
                provider: DecisionProvider::Jev,
                model: "jev-latest".to_string(),
                task_summary: "Refactor cross-crate lock recovery.".to_string(),
            })
            .await
            .expect("jev low confidence classification");
        assert_eq!(low_conf.model, "jev-1.14.0");
        assert_eq!(low_conf.difficulty, Some(Difficulty::Hard));
        assert_eq!(low_conf.confidence, Some(0.62));
        assert!(low_conf.needs_review);
        assert_eq!(low_conf.reason_code, "low_confidence");

        // Extreme choice is parsed accurately.
        let extreme_advice = client
            .test_decision("jev-tool", DecisionProvider::Jev, "jev-1.14.0")
            .await
            .expect("jev extreme classification");
        assert_eq!(extreme_advice.difficulty, Some(Difficulty::Extreme));
        assert!(!extreme_advice.needs_review);

        // Jev `noul` answer to a `choice` question must be rejected as InvalidResponse.
        let noul_err = client
            .test_decision("jev-tool", DecisionProvider::Jev, "jev-1.14.0")
            .await
            .expect_err("jev noul response to choice request must be rejected");
        assert_eq!(noul_err, DecisionError::InvalidResponse);

        // Two-way probabilities map for a four-way Choice question must be rejected.
        let partial_probs_err = client
            .test_decision("jev-tool", DecisionProvider::Jev, "jev-1.14.0")
            .await
            .expect_err("partial probabilities map must be rejected");
        assert_eq!(partial_probs_err, DecisionError::InvalidResponse);
    }

    #[tokio::test]
    async fn openai_classify_handles_choice_and_refusal_without_fallback_approval() {
        let vault = FakeVault::with_secret("openai-tool", "sk-openai-secret");
        let transport = FakeTransport::with_responses([
            json_response(
                200,
                r#"{
                    "model": "gpt-6-luna",
                    "answers": [{
                        "type": "choice",
                        "name": "difficulty",
                        "choice": "easy",
                        "confidence": 0.95,
                        "probabilities": [
                            {"value": "easy", "probability": 0.97},
                            {"value": "hard", "probability": 0.03}
                        ]
                    }],
                    "usage": {
                        "input_tokens": 100,
                        "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
                        "output_tokens": 0,
                        "output_tokens_details": {"reasoning_tokens": 0},
                        "total_tokens": 100
                    }
                }"#,
            ),
            json_response(
                200,
                r#"{
                    "model": "gpt-6-luna",
                    "answers": [{
                        "type": "refusal",
                        "name": "difficulty"
                    }],
                    "usage": {
                        "input_tokens": 64,
                        "output_tokens": 0,
                        "total_tokens": 64
                    }
                }"#,
            ),
        ]);
        let client = DecisionClient::with_transport(vault, transport.clone()).expect("client init");

        let advice = client
            .test_decision("openai-tool", DecisionProvider::Openai, "gpt-6-luna")
            .await
            .expect("openai choice advice");
        assert_eq!(advice.provider, DecisionProvider::Openai);
        assert_eq!(advice.model, "gpt-6-luna");
        assert_eq!(advice.difficulty, Some(Difficulty::Easy));
        assert_eq!(advice.confidence, Some(0.95));
        assert!(!advice.needs_review);
        assert_eq!(advice.reason_code, "classified");
        assert_eq!(advice.input_tokens, Some(100));
        assert_eq!(advice.output_tokens, Some(0));

        let refusal = client
            .test_decision("openai-tool", DecisionProvider::Openai, "gpt-6-luna")
            .await
            .expect("openai refusal must return safe advisory state");
        assert_eq!(refusal.difficulty, None);
        assert_eq!(refusal.confidence, None);
        assert!(refusal.needs_review);
        assert_eq!(refusal.reason_code, "refusal");
    }

    #[tokio::test]
    async fn schema_validation_rejects_bad_echoes_bounds_disallowed_difficulty_and_duplicates() {
        let vault = FakeVault::with_secret("tool", "secret-token");
        let transport = FakeTransport::with_responses([
            // 1. Mismatched model echo for pinned Jev model
            json_response(
                200,
                r#"{
                    "model": "jev-1.13.0",
                    "answers": {"difficulty": {"type": "choice", "choice": "easy", "confidence": 0.9, "probabilities": {"easy": 0.85, "medium": 0.05, "hard": 0.05, "extreme": 0.05}}}
                }"#,
            ),
            // 2. Confidence > 1.0
            json_response(
                200,
                r#"{
                    "model": "jev-1.14.0",
                    "answers": {"difficulty": {"type": "choice", "choice": "easy", "confidence": 1.05, "probabilities": {"easy": 0.85, "medium": 0.05, "hard": 0.05, "extreme": 0.05}}}
                }"#,
            ),
            // 3. Disallowed difficulty choice
            json_response(
                200,
                r#"{
                    "model": "jev-1.14.0",
                    "answers": {"difficulty": {"type": "choice", "choice": "trivial", "confidence": 0.9, "probabilities": {"easy": 0.85, "medium": 0.05, "hard": 0.05, "extreme": 0.05}}}
                }"#,
            ),
            // 4. Duplicate answer name in OpenAI answers array
            json_response(
                200,
                r#"{
                    "model": "gpt-6-luna",
                    "answers": [
                        {"type": "choice", "name": "difficulty", "choice": "easy", "confidence": 0.9},
                        {"type": "choice", "name": "difficulty", "choice": "hard", "confidence": 0.8}
                    ]
                }"#,
            ),
            // 5. Duplicate value in OpenAI probabilities array
            json_response(
                200,
                r#"{
                    "model": "gpt-6-luna",
                    "answers": [{
                        "type": "choice",
                        "name": "difficulty",
                        "choice": "easy",
                        "confidence": 0.9,
                        "probabilities": [
                            {"value": "easy", "probability": 0.5},
                            {"value": "easy", "probability": 0.5}
                        ]
                    }]
                }"#,
            ),
            // 6. Duplicate key in Jev probabilities object
            json_response(
                200,
                r#"{
                    "model": "jev-1.14.0",
                    "answers": {
                        "difficulty": {
                            "type": "choice",
                            "choice": "easy",
                            "confidence": 0.9,
                            "probabilities": {"easy": 0.4, "easy": 0.4, "medium": 0.1, "hard": 0.1}
                        }
                    }
                }"#,
            ),
        ]);
        let client = DecisionClient::with_transport(vault, transport).expect("client init");

        for idx in 0..3 {
            let err = client
                .test_decision("tool", DecisionProvider::Jev, "jev-1.14.0")
                .await
                .expect_err(&format!("jev invalid case {idx} should fail"));
            assert_eq!(err, DecisionError::InvalidResponse);
        }

        for idx in 3..5 {
            let err = client
                .test_decision("tool", DecisionProvider::Openai, "gpt-6-luna")
                .await
                .expect_err(&format!("openai invalid case {idx} should fail"));
            assert_eq!(err, DecisionError::InvalidResponse);
        }

        let dup_key_err = client
            .test_decision("tool", DecisionProvider::Jev, "jev-1.14.0")
            .await
            .expect_err("duplicate JSON map key must fail");
        assert_eq!(dup_key_err, DecisionError::InvalidResponse);
    }

    #[tokio::test]
    async fn rate_limit_and_server_errors_do_not_retry_and_redact_error_messages() {
        let secret = "super-sensitive-api-key-9999";
        let vault = FakeVault::with_secret("jev-tool", secret);
        let transport = FakeTransport::with_responses([
            json_response(
                529,
                r#"{"error":"overloaded - token super-sensitive-api-key-9999"}"#,
            ),
            json_response(
                401,
                r#"{"error":"invalid token super-sensitive-api-key-9999"}"#,
            ),
        ]);
        let client = DecisionClient::with_transport(vault, transport.clone()).expect("client init");

        let rate_err = client
            .test_decision("jev-tool", DecisionProvider::Jev, "jev-1.14.0")
            .await
            .expect_err("529 must fail immediately");
        assert_eq!(rate_err, DecisionError::RateLimited);
        assert_eq!(rate_err.code(), "rate_limited");
        assert_eq!(
            transport.recorded_requests().len(),
            1,
            "classify must never automatically retry paid requests"
        );
        assert!(!rate_err.to_string().contains(secret));
        assert!(!format!("{rate_err:?}").contains(secret));

        let auth_err = client
            .test_decision("jev-tool", DecisionProvider::Jev, "jev-1.14.0")
            .await
            .expect_err("401 must fail with unauthorized");
        assert_eq!(auth_err, DecisionError::Unauthorized);
        assert_eq!(auth_err.code(), "unauthorized");
        assert_eq!(transport.recorded_requests().len(), 2);
        assert!(!auth_err.to_string().contains(secret));
    }

    struct BlockingVault {
        secret: String,
        entered: Arc<tokio::sync::Notify>,
        release_rx: Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl CredentialStore for BlockingVault {
        fn set(&self, _tool_id: &str, _secret: &str) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn get(&self, _tool_id: &str) -> Result<Option<String>, ConnectorError> {
            self.entered.notify_one();
            self.release_rx
                .lock()
                .expect("release_rx lock")
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("blocking vault should be released after timer progresses");
            Ok(Some(self.secret.clone()))
        }

        fn delete(&self, _tool_id: &str) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn status(&self, _tool_id: &str) -> Result<String, ConnectorError> {
            Ok("configured".to_string())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocking_credential_read_runs_on_spawn_blocking_allowing_timers_to_progress() {
        let entered = Arc::new(tokio::sync::Notify::new());
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let vault = Arc::new(BlockingVault {
            secret: "vault-secret-token".to_string(),
            entered: Arc::clone(&entered),
            release_rx: Mutex::new(release_rx),
        });
        let transport = FakeTransport::with_responses([
            json_response(200, r#"{"models":[{"name":"jev-1.14.0"}]}"#),
            valid_jev_four_way_response(
                "jev-1.14.0",
                "easy",
                0.95,
                (0.91, 0.05, 0.03, 0.01),
                50,
                5,
            ),
        ]);
        let client = DecisionClient::with_transport(vault, transport).expect("client init");

        let entered_wait = entered.notified();
        let client_for_models = client.clone();
        let models_task = tokio::spawn(async move {
            client_for_models
                .models("jev-tool", DecisionProvider::Jev)
                .await
        });

        tokio::time::timeout(std::time::Duration::from_secs(2), entered_wait)
            .await
            .expect("vault.get should be entered on blocking pool");

        let mut timer_ticks = 0usize;
        for _ in 0..3 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            timer_ticks += 1;
        }
        assert_eq!(timer_ticks, 3);
        assert!(
            !models_task.is_finished(),
            "models future must still be waiting on blocked vault read"
        );

        release_tx.send(()).expect("release models vault read");
        let models = models_task
            .await
            .expect("join models task")
            .expect("models should succeed");
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].model_arg, "jev-1.14.0");

        let entered_wait = entered.notified();
        let client_for_classify = client.clone();
        let classify_task = tokio::spawn(async move {
            client_for_classify
                .test_decision("jev-tool", DecisionProvider::Jev, "jev-1.14.0")
                .await
        });

        tokio::time::timeout(std::time::Duration::from_secs(2), entered_wait)
            .await
            .expect("vault.get should be entered for classify");

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(
            !classify_task.is_finished(),
            "classify future must still be waiting on blocked vault read"
        );

        release_tx.send(()).expect("release classify vault read");
        let advice = classify_task
            .await
            .expect("join classify task")
            .expect("classify should succeed");
        assert_eq!(advice.difficulty, Some(Difficulty::Easy));
    }

    // =========================================================================
    // Shadow Advice Core, Cache, Ownership & Security Tests
    // =========================================================================

    #[test]
    fn preview_validates_and_renders_summary_without_creating_cache_or_mutating_stores() {
        let ws = ScopedTestWorkspace::new("preview-offline");
        let reg_rev = ws.seed_jev_registry();
        let scope = ws.scope("550e8400-e29b-41d4-a716-446655440000");

        let summary_text = "Confidential operator task summary for shadow preview only.";
        let req_json = format!(
            r#"{{
                "schema_version": 1,
                "expected_registry_revision": {reg_rev},
                "tool_id": "jev-api",
                "model_id": "jev-api:jev-latest",
                "task_id": "task-alpha",
                "task_revision": 1,
                "task_summary": "{summary_text}",
                "baseline_difficulty": "medium"
            }}"#
        );
        let req_path = ws.write_private_file("request.json", req_json.as_bytes());
        let reg_bytes_before =
            fs::read(ws.registry_dir.join("registry.json")).expect("read registry before");

        assert!(
            !ws.cache_dir.exists(),
            "cache_dir must not exist before preview"
        );

        let preview = preview_advice(&scope, &req_path).expect("preview must succeed offline");
        assert!(preview.untrusted_data);
        assert!(preview.advisory_only);
        assert_eq!(preview.mode, "shadow");
        assert_eq!(preview.context_id, "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(preview.registry_revision, reg_rev);
        assert_eq!(preview.tool_id, "jev-api");
        assert_eq!(preview.model_id, "jev-api:jev-latest");
        assert_eq!(preview.provider, DecisionProvider::Jev);
        assert_eq!(preview.requested_model, "jev-latest");
        assert_eq!(preview.task_id, "task-alpha");
        assert_eq!(preview.task_revision, 1);
        assert_eq!(preview.task_summary, summary_text);
        assert_eq!(preview.summary_sha256, sha256_hex(summary_text.as_bytes()));
        assert_eq!(preview.cache_key.len(), 64);
        assert_eq!(preview.baseline_difficulty, Some(Difficulty::Medium));
        assert!(!preview.provider_call_performed);

        // Debug representation of preview must redact raw task summary.
        assert!(
            !format!("{preview:?}").contains(summary_text),
            "Debug output must never contain raw task_summary"
        );

        // Verify leading/trailing whitespace in task_summary is preserved verbatim in preview
        // output, while summary_sha256 and cache_key use the normalized summary and Debug stays redacted.
        let untrimmed_summary =
            "  \n\t Confidential operator task summary for shadow preview only. \t\r\n  ";
        let untrimmed_req_val = serde_json::json!({
            "schema_version": 1,
            "expected_registry_revision": reg_rev,
            "tool_id": "jev-api",
            "model_id": "jev-api:jev-latest",
            "task_id": "task-alpha",
            "task_revision": 1,
            "task_summary": untrimmed_summary,
            "baseline_difficulty": "medium"
        });
        let untrimmed_req_path = ws.write_private_file(
            "request-untrimmed.json",
            serde_json::to_vec(&untrimmed_req_val)
                .expect("serialize untrimmed request")
                .as_slice(),
        );
        let untrimmed_preview = preview_advice(&scope, &untrimmed_req_path)
            .expect("preview with leading/trailing whitespace must succeed");
        assert_eq!(
            untrimmed_preview.task_summary, untrimmed_summary,
            "preview must return exact original supplied task_summary without trimming"
        );
        assert_eq!(untrimmed_preview.summary_sha256, preview.summary_sha256);
        assert_eq!(untrimmed_preview.cache_key, preview.cache_key);
        let debug_untrimmed = format!("{untrimmed_preview:?}");
        assert!(!debug_untrimmed.contains(summary_text));
        assert!(!debug_untrimmed.contains(untrimmed_summary));

        // Cache directory must NOT be created by preview, and registry must not be mutated.
        assert!(
            !ws.cache_dir.exists(),
            "preview must never create or mutate cache_dir"
        );
        let reg_bytes_after =
            fs::read(ws.registry_dir.join("registry.json")).expect("read registry after");
        assert_eq!(reg_bytes_before, reg_bytes_after);
    }

    #[tokio::test]
    async fn models_command_validates_enabled_jev_tool_and_never_creates_cache_or_mutates_registry()
    {
        let ws = ScopedTestWorkspace::new("models-catalog");
        let reg_rev = ws.seed_jev_registry();
        let scope = ws.scope("550e8400-e29b-41d4-a716-446655440000");

        let vault = FakeVault::with_secret("jev-api", "jev-secret-token-777");
        let transport = FakeTransport::with_responses([json_response(
            200,
            r#"{
                "models": [
                    {
                        "name": "jev-latest",
                        "description": "Alias for the latest Jev model",
                        "release_date": "2026-10-01"
                    },
                    {
                        "name": "jev-2.0.0",
                        "description": "Pinned Jev 2.0 release",
                        "release_date": "2026-10-01"
                    }
                ]
            }"#,
        )]);
        let client =
            DecisionClient::with_transport(vault.clone(), transport.clone()).expect("client");

        let reg_bytes_before =
            fs::read(ws.registry_dir.join("registry.json")).expect("read registry before");

        let out = list_advice_models(&scope, "jev-api", &client)
            .await
            .expect("list_advice_models should succeed");
        assert!(out.untrusted_data);
        assert!(out.advisory_only);
        assert_eq!(out.mode, "shadow");
        assert_eq!(out.registry_revision, reg_rev);
        assert_eq!(out.tool_id, "jev-api");
        assert_eq!(out.provider, DecisionProvider::Jev);
        assert_eq!(out.models.len(), 2);
        assert_eq!(out.models[0].id, "jev-api:jev-latest");
        assert_eq!(out.models[0].name, "jev-latest");
        assert_eq!(out.models[1].id, "jev-api:jev-2.0.0");
        assert_eq!(out.models[1].name, "jev-2.0.0");
        assert!(
            !out.models.iter().any(|m| m.name == "jev-1.14.0"),
            "models output must contain only server-returned models"
        );

        assert!(
            !ws.cache_dir.exists(),
            "models command must never create cache_dir"
        );
        let reg_bytes_after =
            fs::read(ws.registry_dir.join("registry.json")).expect("read registry after");
        assert_eq!(reg_bytes_before, reg_bytes_after);
    }

    #[tokio::test]
    async fn classify_caches_completed_advice_and_serves_cache_hit_with_zero_extra_api_or_vault_calls(
    ) {
        let ws = ScopedTestWorkspace::new("classify-cache-hit");
        let reg_rev = ws.seed_jev_registry();
        let scope = ws.scope("550e8400-e29b-41d4-a716-446655440000");

        let secret = "jev-live-key-do-not-leak-8888";
        let summary_text = "Unique sensitive task summary text 99887766.";
        let vault = FakeVault::with_secret("jev-api", secret);
        let transport = FakeTransport::with_responses([valid_jev_four_way_response(
            "jev-1.14.0",
            "hard",
            0.91,
            (0.02, 0.05, 0.89, 0.04),
            210,
            18,
        )]);
        let client =
            DecisionClient::with_transport(vault.clone(), transport.clone()).expect("client");

        let req_json = format!(
            r#"{{
                "schema_version": 1,
                "expected_registry_revision": {reg_rev},
                "tool_id": "jev-api",
                "model_id": "jev-api:jev-latest",
                "task_id": "task-cache-1",
                "task_revision": 3,
                "task_summary": "{summary_text}",
                "baseline_difficulty": "easy"
            }}"#
        );
        let req_path = ws.write_private_file("req.json", req_json.as_bytes());

        // 1. First classify call performs provider call and creates 0700 cache_dir + 0600 record.
        let opts_t0 = AdviceRuntimeOptions {
            now_ms_override: Some(1_700_000_000_000),
            ..AdviceRuntimeOptions::default()
        };
        let first = classify_advice_with_options(
            &scope,
            &req_path,
            false,
            vault.clone(),
            &client,
            &opts_t0,
        )
        .await
        .expect("first classify should succeed");

        assert!(first.untrusted_data);
        assert!(first.advisory_only);
        assert_eq!(first.mode, "shadow");
        assert_eq!(first.source, AdviceSource::Provider);
        assert!(first.provider_call_performed);
        assert_eq!(first.cache_age_ms, None);
        assert_eq!(first.requested_model, "jev-latest");
        assert_eq!(first.advice.model, "jev-1.14.0");
        assert_eq!(first.advice.difficulty, Some(Difficulty::Hard));
        assert_eq!(first.advice.confidence, Some(0.91));
        assert_eq!(first.advice.input_tokens, Some(210));
        assert_eq!(first.advice.output_tokens, Some(18));
        assert_eq!(first.baseline_difficulty, Some(Difficulty::Easy));
        assert_eq!(first.agrees_with_baseline, Some(false));
        assert_eq!(transport.recorded_requests().len(), 1);
        assert!(vault.read_count() > 0);

        // Verify neither serialized output nor persisted cache file contains raw summary or secret.
        let first_json = serde_json::to_string(&first).expect("serialize first output");
        assert!(!first_json.contains(summary_text));
        assert!(!first_json.contains(secret));
        assert!(!format!("{first:?}").contains(summary_text));
        assert!(!format!("{first:?}").contains(secret));

        let cache_record_path = ws.cache_dir.join(format!("{}.json", first.cache_key));
        let persisted_text =
            fs::read_to_string(&cache_record_path).expect("read persisted cache record");
        assert!(!persisted_text.contains(summary_text));
        assert!(!persisted_text.contains(secret));
        assert!(
            !persisted_text.contains(ws.project_root.to_str().unwrap()),
            "persisted cache record must store project_root_sha256, not raw project_root path"
        );
        let cache_dir_mode = fs::symlink_metadata(&ws.cache_dir)
            .expect("cache_dir meta")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(cache_dir_mode, 0o700);
        let record_mode = fs::symlink_metadata(&cache_record_path)
            .expect("record meta")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(record_mode, 0o600);

        // 2. Second classify call (even with a completely empty/failing vault!) returns cache hit
        //    with zero vault reads and zero network calls.
        let empty_vault = FakeVault::failing();
        let empty_client =
            DecisionClient::with_transport(empty_vault.clone(), transport.clone()).expect("client");
        let opts_t1 = AdviceRuntimeOptions {
            now_ms_override: Some(1_700_000_025_000),
            ..AdviceRuntimeOptions::default()
        };
        let hit = classify_advice_with_options(
            &scope,
            &req_path,
            false,
            empty_vault.clone(),
            &empty_client,
            &opts_t1,
        )
        .await
        .expect("cache hit must succeed without vault or network access");

        assert_eq!(hit.source, AdviceSource::Cache);
        assert!(!hit.provider_call_performed);
        assert_eq!(hit.cache_age_ms, Some(25_000));
        assert_eq!(hit.advice, first.advice);
        assert_eq!(hit.advice.input_tokens, Some(210));
        assert_eq!(hit.advice.output_tokens, Some(18));
        assert_eq!(empty_vault.read_count(), 0);
        assert_eq!(transport.recorded_requests().len(), 1);
    }

    #[tokio::test]
    async fn cache_key_invalidates_when_any_relevant_input_changes() {
        let ws = ScopedTestWorkspace::new("cache-invalidation");
        let reg_rev = ws.seed_jev_registry();
        let scope = ws.scope("ctx-1");

        let make_req = |rev: u64, model_id: &str, task_id: &str, task_rev: u64, summary: &str| {
            format!(
                r#"{{
                    "schema_version": 1,
                    "expected_registry_revision": {rev},
                    "tool_id": "jev-api",
                    "model_id": "{model_id}",
                    "task_id": "{task_id}",
                    "task_revision": {task_rev},
                    "task_summary": "{summary}"
                }}"#
            )
        };

        let base_req = ws.write_private_file(
            "base.json",
            make_req(reg_rev, "jev-api:jev-latest", "task-1", 1, "Base summary.").as_bytes(),
        );
        let base_preview = preview_advice(&scope, &base_req).expect("base preview");

        // 1. Different summary -> different cache_key and summary_sha256
        let diff_summary_req = ws.write_private_file(
            "diff-summary.json",
            make_req(
                reg_rev,
                "jev-api:jev-latest",
                "task-1",
                1,
                "Modified summary.",
            )
            .as_bytes(),
        );
        let diff_summary_preview =
            preview_advice(&scope, &diff_summary_req).expect("diff summary preview");
        assert_ne!(
            diff_summary_preview.summary_sha256,
            base_preview.summary_sha256
        );
        assert_ne!(diff_summary_preview.cache_key, base_preview.cache_key);

        // 2. Different context_id -> different cache_key
        let diff_ctx_scope = ws.scope("ctx-2");
        let diff_ctx_preview =
            preview_advice(&diff_ctx_scope, &base_req).expect("diff ctx preview");
        assert_ne!(diff_ctx_preview.cache_key, base_preview.cache_key);

        // 3. Different project_root -> different cache_key
        let alt_project = ws.base.join("project-alt");
        fs::create_dir_all(&alt_project).expect("create alt project");
        fs::set_permissions(&alt_project, fs::Permissions::from_mode(0o700))
            .expect("chmod alt project");
        let mut diff_proj_scope = scope.clone();
        diff_proj_scope.project_root = alt_project;
        let diff_proj_preview =
            preview_advice(&diff_proj_scope, &base_req).expect("diff project preview");
        assert_ne!(diff_proj_preview.cache_key, base_preview.cache_key);

        // 4. Different task_id -> different cache_key
        let diff_task_req = ws.write_private_file(
            "diff-task.json",
            make_req(reg_rev, "jev-api:jev-latest", "task-2", 1, "Base summary.").as_bytes(),
        );
        let diff_task_preview = preview_advice(&scope, &diff_task_req).expect("diff task preview");
        assert_ne!(diff_task_preview.cache_key, base_preview.cache_key);

        // 5. Different task_revision -> different cache_key
        let diff_task_rev_req = ws.write_private_file(
            "diff-task-rev.json",
            make_req(reg_rev, "jev-api:jev-latest", "task-1", 2, "Base summary.").as_bytes(),
        );
        let diff_task_rev_preview =
            preview_advice(&scope, &diff_task_rev_req).expect("diff task rev preview");
        assert_ne!(diff_task_rev_preview.cache_key, base_preview.cache_key);

        // 6. Different model_id / model_arg -> different cache_key
        let diff_model_req = ws.write_private_file(
            "diff-model.json",
            make_req(reg_rev, "jev-api:jev-1.14.0", "task-1", 1, "Base summary.").as_bytes(),
        );
        let diff_model_preview =
            preview_advice(&scope, &diff_model_req).expect("diff model preview");
        assert_ne!(diff_model_preview.cache_key, base_preview.cache_key);

        // 7. Advancing registry revision -> old expected_registry_revision rejected as stale,
        //    and new expected_registry_revision produces a different cache_key!
        let store = RegistryStore::open(ws.registry_dir.clone()).expect("open registry");
        let snap2 = store
            .apply(
                reg_rev,
                RegistryChange::SetDefaultProfile { profile_id: None },
            )
            .expect("advance registry");
        assert_eq!(
            preview_advice(&scope, &base_req)
                .expect_err("old revision must fail")
                .code(),
            "stale_registry_revision"
        );
        let new_rev_req = ws.write_private_file(
            "new-rev.json",
            make_req(
                snap2.revision,
                "jev-api:jev-latest",
                "task-1",
                1,
                "Base summary.",
            )
            .as_bytes(),
        );
        let new_rev_preview = preview_advice(&scope, &new_rev_req).expect("new rev preview");
        assert_ne!(new_rev_preview.cache_key, base_preview.cache_key);
    }

    #[tokio::test]
    async fn registry_and_request_validation_rejects_disabled_wrong_adapter_wrong_model_and_bad_json_before_api(
    ) {
        let ws = ScopedTestWorkspace::new("validation-refusals");
        let store = RegistryStore::open(ws.registry_dir.clone()).expect("open registry");
        store
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "jev-disabled".to_string(),
                        label: "Disabled Jev".to_string(),
                        adapter: ToolAdapter::JevApi,
                        executable_path: None,
                        enabled: false,
                    },
                },
            )
            .expect("upsert disabled jev");
        store
            .apply(
                1,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "openai-dec".to_string(),
                        label: "OpenAI Decisions".to_string(),
                        adapter: ToolAdapter::OpenaiDecisionsApi,
                        executable_path: None,
                        enabled: true,
                    },
                },
            )
            .expect("upsert openai tool");
        store
            .apply(
                2,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "jev-enabled".to_string(),
                        label: "Enabled Jev".to_string(),
                        adapter: ToolAdapter::JevApi,
                        executable_path: None,
                        enabled: true,
                    },
                },
            )
            .expect("upsert enabled jev");
        let snap = store
            .apply(
                3,
                RegistryChange::UpsertModels {
                    models: vec![
                        ModelDefinition {
                            id: "openai-dec:gpt-6-luna".to_string(),
                            tool_id: "openai-dec".to_string(),
                            label: "gpt-6-luna".to_string(),
                            model_arg: "gpt-6-luna".to_string(),
                            efforts: Vec::new(),
                            allow_no_effort: true,
                        },
                        ModelDefinition {
                            id: "jev-disabled:jev-latest".to_string(),
                            tool_id: "jev-disabled".to_string(),
                            label: "jev-latest".to_string(),
                            model_arg: "jev-latest".to_string(),
                            efforts: Vec::new(),
                            allow_no_effort: true,
                        },
                        ModelDefinition {
                            id: "jev-enabled:jev-latest".to_string(),
                            tool_id: "jev-enabled".to_string(),
                            label: "jev-latest".to_string(),
                            model_arg: "jev-latest".to_string(),
                            efforts: Vec::new(),
                            allow_no_effort: true,
                        },
                    ],
                },
            )
            .expect("upsert models");
        let rev = snap.revision;

        let scope = ws.scope("ctx-val");
        let vault = FakeVault::with_secret("jev-enabled", "secret");
        let transport = FakeTransport::with_responses([]);
        let client =
            DecisionClient::with_transport(vault.clone(), transport.clone()).expect("client");

        // 1. Disabled Jev tool rejected with tool_disabled
        let disabled_req = ws.write_private_file(
            "disabled.json",
            format!(
                r#"{{
                    "schema_version": 1,
                    "expected_registry_revision": {rev},
                    "tool_id": "jev-disabled",
                    "model_id": "jev-disabled:jev-latest",
                    "task_id": "t1",
                    "task_revision": 1,
                    "task_summary": "Summary."
                }}"#
            )
            .as_bytes(),
        );
        let err = classify_advice(&scope, &disabled_req, false, vault.clone(), &client)
            .await
            .expect_err("disabled tool must fail");
        assert_eq!(err.code(), "tool_disabled");

        // 2. OpenAI Decisions tool rejected with unsupported_adapter (shadow advice route is Jev-only)
        let openai_req = ws.write_private_file(
            "openai.json",
            format!(
                r#"{{
                    "schema_version": 1,
                    "expected_registry_revision": {rev},
                    "tool_id": "openai-dec",
                    "model_id": "openai-dec:gpt-6-luna",
                    "task_id": "t1",
                    "task_revision": 1,
                    "task_summary": "Summary."
                }}"#
            )
            .as_bytes(),
        );
        let err = classify_advice(&scope, &openai_req, false, vault.clone(), &client)
            .await
            .expect_err("openai adapter must be rejected on jev advice route");
        assert_eq!(err.code(), "unsupported_adapter");

        // 3. Model belonging to a different tool rejected with model_not_found
        let wrong_model_req = ws.write_private_file(
            "wrong-model.json",
            format!(
                r#"{{
                    "schema_version": 1,
                    "expected_registry_revision": {rev},
                    "tool_id": "jev-enabled",
                    "model_id": "jev-disabled:jev-latest",
                    "task_id": "t1",
                    "task_revision": 1,
                    "task_summary": "Summary."
                }}"#
            )
            .as_bytes(),
        );
        let err = classify_advice(&scope, &wrong_model_req, false, vault.clone(), &client)
            .await
            .expect_err("cross-tool model must fail");
        assert_eq!(err.code(), "model_not_found");

        // 4. Unknown field in request JSON rejected
        let unknown_field_req = ws.write_private_file(
            "unknown-field.json",
            format!(
                r#"{{
                    "schema_version": 1,
                    "expected_registry_revision": {rev},
                    "tool_id": "jev-enabled",
                    "model_id": "jev-enabled:jev-latest",
                    "task_id": "t1",
                    "task_revision": 1,
                    "task_summary": "Summary.",
                    "model_arg_override": "jev-hacked"
                }}"#
            )
            .as_bytes(),
        );
        let err = classify_advice(&scope, &unknown_field_req, false, vault.clone(), &client)
            .await
            .expect_err("unknown field must fail");
        assert_eq!(err.code(), "invalid_request");

        // 5. Duplicate field in request JSON rejected
        let duplicate_field_req = ws.write_private_file(
            "dup-field.json",
            format!(
                r#"{{
                    "schema_version": 1,
                    "expected_registry_revision": {rev},
                    "tool_id": "jev-enabled",
                    "tool_id": "jev-enabled",
                    "model_id": "jev-enabled:jev-latest",
                    "task_id": "t1",
                    "task_revision": 1,
                    "task_summary": "Summary."
                }}"#
            )
            .as_bytes(),
        );
        let err = classify_advice(&scope, &duplicate_field_req, false, vault.clone(), &client)
            .await
            .expect_err("duplicate field must fail");
        assert_eq!(err.code(), "invalid_request");

        // Zero vault reads and zero API requests across all pre-flight refusals, and cache_dir was never created.
        assert_eq!(vault.read_count(), 0);
        assert!(transport.recorded_requests().is_empty());
        assert!(!ws.cache_dir.exists());
    }

    #[tokio::test]
    async fn pending_and_uncertain_attempts_refuse_replay_without_refresh_and_pre_api_refusal_cleans_marker(
    ) {
        let ws = ScopedTestWorkspace::new("pending-uncertain-ownership");
        let reg_rev = ws.seed_jev_registry();
        let scope = ws.scope("ctx-own");

        let req_json = format!(
            r#"{{
                "schema_version": 1,
                "expected_registry_revision": {reg_rev},
                "tool_id": "jev-api",
                "model_id": "jev-api:jev-latest",
                "task_id": "task-own",
                "task_revision": 1,
                "task_summary": "Evaluate ownership state transitions."
            }}"#
        );
        let req_path = ws.write_private_file("req.json", req_json.as_bytes());

        // 1. Pre-API credential refusal (`NotConfigured`) clears its owned pending marker
        //    so a subsequent classify after configuring the secret succeeds without `--refresh`.
        let empty_vault = Arc::new(FakeVault::default());
        let transport = FakeTransport::with_responses([
            Err(DecisionError::Timeout),
            valid_jev_four_way_response(
                "jev-1.14.0",
                "medium",
                0.85,
                (0.05, 0.80, 0.10, 0.05),
                90,
                9,
            ),
            valid_jev_four_way_response(
                "jev-1.14.0",
                "easy",
                0.92,
                (0.88, 0.07, 0.03, 0.02),
                95,
                9,
            ),
        ]);
        let client_empty =
            DecisionClient::with_transport(empty_vault.clone(), transport.clone()).expect("client");

        let not_conf_err = classify_advice(&scope, &req_path, false, empty_vault, &client_empty)
            .await
            .expect_err("missing secret must fail before API");
        assert_eq!(not_conf_err.code(), "not_configured");
        assert!(transport.recorded_requests().is_empty());

        // Configure secret and verify we are NOT blocked by a leftover pending marker.
        let vault = FakeVault::with_secret("jev-api", "jev-secret-token");
        let client =
            DecisionClient::with_transport(vault.clone(), transport.clone()).expect("client");

        // 2. Simulate an abandoned Pending attempt (crash right after sealing Pending before API).
        let abort_opts = AdviceRuntimeOptions {
            abort_after_pending_seal: true,
            now_ms_override: Some(1_700_000_000_000),
            ..AdviceRuntimeOptions::default()
        };
        let _ = classify_advice_with_options(
            &scope,
            &req_path,
            false,
            vault.clone(),
            &client,
            &abort_opts,
        )
        .await
        .expect_err("simulated crash after pending seal");
        assert!(transport.recorded_requests().is_empty());

        // Replay without `--refresh` must refuse with `pending_attempt_requires_refresh` before vault/API!
        let vault_reads_before = vault.read_count();
        let pending_refusal = classify_advice(&scope, &req_path, false, vault.clone(), &client)
            .await
            .expect_err("abandoned pending attempt must refuse without --refresh");
        assert_eq!(pending_refusal.code(), "pending_attempt_requires_refresh");
        assert_eq!(vault.read_count(), vault_reads_before);
        assert!(transport.recorded_requests().is_empty());

        // 3. Explicit `--refresh` proceeds to call provider; first transport response is `Timeout`
        //    (post-call uncertain outcome).
        let timeout_err = classify_advice(&scope, &req_path, true, vault.clone(), &client)
            .await
            .expect_err("timeout response must fail and record uncertain state");
        assert_eq!(timeout_err.code(), "timeout");
        assert_eq!(transport.recorded_requests().len(), 1);

        // 4. Even after cache TTL would have elapsed (e.g. +10 hours), an `Uncertain` attempt
        //    NEVER expires into automatic replay; calling without `--refresh` still refuses!
        let far_future_opts = AdviceRuntimeOptions {
            now_ms_override: Some(1_700_036_000_000),
            ..AdviceRuntimeOptions::default()
        };
        let uncertain_refusal = classify_advice_with_options(
            &scope,
            &req_path,
            false,
            vault.clone(),
            &client,
            &far_future_opts,
        )
        .await
        .expect_err("uncertain attempt must never auto-replay without --refresh");
        assert_eq!(
            uncertain_refusal.code(),
            "uncertain_attempt_requires_refresh"
        );
        assert_eq!(
            transport.recorded_requests().len(),
            1,
            "uncertain refusal must not make a second provider request"
        );

        // 5. Explicit `--refresh` recovers the uncertain record and seals `Completed`.
        let recovered = classify_advice_with_options(
            &scope,
            &req_path,
            true,
            vault.clone(),
            &client,
            &far_future_opts,
        )
        .await
        .expect("explicit --refresh must recover uncertain attempt");
        assert_eq!(recovered.source, AdviceSource::Provider);
        assert_eq!(recovered.advice.difficulty, Some(Difficulty::Medium));
        assert_eq!(transport.recorded_requests().len(), 2);

        // 6. Once a `Completed` entry expires past TTL, a new explicit `classify` invocation
        //    (even without `--refresh`) refreshes it cleanly.
        let expired_opts = AdviceRuntimeOptions {
            now_ms_override: Some(1_700_040_000_000), // > 3600s after 1_700_036_000_000
            ..AdviceRuntimeOptions::default()
        };
        let refreshed_after_expiry = classify_advice_with_options(
            &scope,
            &req_path,
            false,
            vault.clone(),
            &client,
            &expired_opts,
        )
        .await
        .expect("expired completed entry may be refreshed by a new explicit classify call");
        assert_eq!(refreshed_after_expiry.source, AdviceSource::Provider);
        assert_eq!(
            refreshed_after_expiry.advice.difficulty,
            Some(Difficulty::Easy)
        );
        assert_eq!(transport.recorded_requests().len(), 3);
    }

    #[tokio::test]
    async fn concurrent_per_key_lock_busy_refuses_before_vault_or_api() {
        let ws = ScopedTestWorkspace::new("concurrent-lock-busy");
        let reg_rev = ws.seed_jev_registry();
        let scope = ws.scope("ctx-lock");

        let req_json = format!(
            r#"{{
                "schema_version": 1,
                "expected_registry_revision": {reg_rev},
                "tool_id": "jev-api",
                "model_id": "jev-api:jev-latest",
                "task_id": "task-lock",
                "task_revision": 1,
                "task_summary": "Verify nonblocking per-key lock contention."
            }}"#
        );
        let req_path = ws.write_private_file("req.json", req_json.as_bytes());
        let preview = preview_advice(&scope, &req_path).expect("preview");

        // Create cache_dir and hold exclusive fs2 lock on `<cache_key>.lock`.
        fs::create_dir_all(&ws.cache_dir).expect("create cache_dir");
        fs::set_permissions(&ws.cache_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 cache_dir");
        let lock_path = ws.cache_dir.join(format!("{}.lock", preview.cache_key));
        let held_lock = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .expect("open lock file");
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 lock file");
        held_lock
            .try_lock_exclusive()
            .expect("hold exclusive lock in test");

        let vault = FakeVault::with_secret("jev-api", "secret");
        let transport = FakeTransport::with_responses([]);
        let client =
            DecisionClient::with_transport(vault.clone(), transport.clone()).expect("client");

        let err = classify_advice(&scope, &req_path, false, vault.clone(), &client)
            .await
            .expect_err("busy per-key lock must refuse immediately");
        assert_eq!(err.code(), "cache_busy");
        assert_eq!(vault.read_count(), 0);
        assert!(transport.recorded_requests().is_empty());

        held_lock.unlock().expect("unlock");
    }

    #[tokio::test]
    async fn registry_revision_drift_during_api_call_is_rejected_and_not_cached_as_completed() {
        let ws = ScopedTestWorkspace::new("post-api-drift");
        let reg_rev = ws.seed_jev_registry();
        let scope = ws.scope("ctx-drift");

        let req_json = format!(
            r#"{{
                "schema_version": 1,
                "expected_registry_revision": {reg_rev},
                "tool_id": "jev-api",
                "model_id": "jev-api:jev-latest",
                "task_id": "task-drift",
                "task_revision": 1,
                "task_summary": "Detect registry drift after provider call."
            }}"#
        );
        let req_path = ws.write_private_file("req.json", req_json.as_bytes());

        let vault = FakeVault::with_secret("jev-api", "secret");
        let transport = FakeTransport::with_responses([valid_jev_four_way_response(
            "jev-1.14.0",
            "easy",
            0.94,
            (0.90, 0.05, 0.03, 0.02),
            100,
            10,
        )]);
        let reg_dir_for_hook = ws.registry_dir.clone();
        transport.set_on_execute(move || {
            let store = RegistryStore::open(reg_dir_for_hook.clone()).expect("open registry");
            let _ = store
                .apply(
                    reg_rev,
                    RegistryChange::SetDefaultProfile { profile_id: None },
                )
                .expect("advance registry during provider call");
        });

        let client =
            DecisionClient::with_transport(vault.clone(), transport.clone()).expect("client");

        let err = classify_advice(&scope, &req_path, false, vault.clone(), &client)
            .await
            .expect_err("registry drift during API call must be rejected");
        assert_eq!(err.code(), "stale_registry_revision");
        assert_eq!(transport.recorded_requests().len(), 1);
    }

    #[tokio::test]
    async fn filesystem_safety_rejects_symlinks_fifos_insecure_permissions_and_wrong_uid_without_chmod(
    ) {
        let ws = ScopedTestWorkspace::new("fs-safety");
        let reg_rev = ws.seed_jev_registry();
        let scope = ws.scope("ctx-sec");

        let req_json = format!(
            r#"{{
                "schema_version": 1,
                "expected_registry_revision": {reg_rev},
                "tool_id": "jev-api",
                "model_id": "jev-api:jev-latest",
                "task_id": "task-sec",
                "task_revision": 1,
                "task_summary": "Check symlink and permission safety."
            }}"#
        );
        let req_path = ws.write_private_file("req.json", req_json.as_bytes());

        // 1. Insecure request file permissions (0644, 0700 executable, 0400 read-only) and
        //    hardlinked (nlink > 1) request files rejected without repairing/chmodding them.
        for bad_mode in [0o644u32, 0o700u32, 0o400u32] {
            let bad_req = ws.base.join(format!("bad-mode-{bad_mode:o}-req.json"));
            fs::write(&bad_req, req_json.as_bytes()).expect("write bad-mode req");
            fs::set_permissions(&bad_req, fs::Permissions::from_mode(bad_mode))
                .expect("chmod bad-mode req");
            let err = preview_advice(&scope, &bad_req)
                .expect_err("non-0600 request file must fail with security_violation");
            assert_eq!(err.code(), "security_violation");
            let mode_after = fs::symlink_metadata(&bad_req)
                .expect("meta")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(
                mode_after, bad_mode,
                "validator must never chmod/repair an existing unsafe path"
            );
            fs::set_permissions(&bad_req, fs::Permissions::from_mode(0o600))
                .expect("restore 0600 before cleanup");
            fs::remove_file(&bad_req).expect("remove bad-mode req");
        }

        let hardlink_req = ws.base.join("hardlink-req.json");
        fs::hard_link(&req_path, &hardlink_req).expect("create hardlink to req");
        let err = preview_advice(&scope, &hardlink_req)
            .expect_err("multi-link (nlink == 2) request file must fail");
        assert_eq!(err.code(), "security_violation");
        fs::remove_file(&hardlink_req).expect("remove hardlink so req_path returns to nlink == 1");

        // 2. Symlink request file rejected.
        let symlink_req = ws.base.join("symlink-req.json");
        std::os::unix::fs::symlink(&req_path, &symlink_req).expect("create symlink req");
        let err = preview_advice(&scope, &symlink_req).expect_err("symlink request must fail");
        assert_eq!(err.code(), "security_violation");

        // 3. Insecure cache_dir parent permissions (0755) rejected without chmodding it.
        fs::set_permissions(&ws.plans_dir, fs::Permissions::from_mode(0o755))
            .expect("chmod 0755 plans_dir");
        let err = preview_advice(&scope, &req_path).expect_err("0755 cache parent must fail");
        assert_eq!(err.code(), "security_violation");
        assert_eq!(
            fs::symlink_metadata(&ws.plans_dir)
                .expect("meta")
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
        fs::set_permissions(&ws.plans_dir, fs::Permissions::from_mode(0o700))
            .expect("restore 0700 plans_dir");

        // 4. Symlink cache_dir rejected.
        let real_other_dir = ws.base.join("other-dir");
        fs::create_dir_all(&real_other_dir).expect("create other dir");
        fs::set_permissions(&real_other_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 other dir");
        std::os::unix::fs::symlink(&real_other_dir, &ws.cache_dir).expect("symlink cache_dir");
        let err = preview_advice(&scope, &req_path).expect_err("symlink cache_dir must fail");
        assert_eq!(err.code(), "security_violation");
        fs::remove_file(&ws.cache_dir).expect("remove symlink cache_dir");

        // 5. Wrong UID rejected.
        let wrong_uid_opts = AdviceRuntimeOptions {
            expected_uid: rustix::process::geteuid().as_raw().wrapping_add(1),
            ..AdviceRuntimeOptions::default()
        };
        let err = preview_advice_with_options(&scope, &req_path, &wrong_uid_opts)
            .expect_err("mismatched UID must fail");
        assert_eq!(err.code(), "security_violation");

        // 6. Executable 0700 and hardlinked (nlink == 2) cache files and lock files rejected without chmod.
        fs::create_dir_all(&ws.cache_dir).expect("create cache_dir");
        fs::set_permissions(&ws.cache_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 cache_dir");
        let vault = FakeVault::with_secret("jev-api", "secret");
        let transport = FakeTransport::with_responses([]);
        let client =
            DecisionClient::with_transport(vault.clone(), transport.clone()).expect("client");

        // 6a. Executable 0700 cache record file rejected without chmod.
        let rec_path = ws.cache_dir.join(format!("{}.json", "b".repeat(64)));
        fs::write(&rec_path, b"{}").expect("write cache record");
        fs::set_permissions(&rec_path, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 cache record");
        let err = classify_advice(&scope, &req_path, false, vault.clone(), &client)
            .await
            .expect_err("0700 cache record must fail");
        assert_eq!(err.code(), "security_violation");
        assert_eq!(
            fs::symlink_metadata(&rec_path)
                .expect("meta")
                .permissions()
                .mode()
                & 0o777,
            0o700,
            "0700 cache record must not be repaired via chmod"
        );
        fs::set_permissions(&rec_path, fs::Permissions::from_mode(0o600))
            .expect("restore 0600 cache record");

        // 6b. Hardlinked (nlink == 2) cache record file rejected.
        let external_link = ws.base.join("external-record-link.json");
        fs::hard_link(&rec_path, &external_link).expect("hardlink cache record");
        let err = classify_advice(&scope, &req_path, false, vault.clone(), &client)
            .await
            .expect_err("hardlinked cache record must fail");
        assert_eq!(err.code(), "security_violation");
        fs::remove_file(&external_link).expect("remove external hardlink");
        fs::remove_file(&rec_path).expect("remove cache record");

        // 6c. Executable 0700 and hardlinked per-key lock file rejected without chmod.
        let preview = preview_advice(&scope, &req_path).expect("preview");
        let key_lock_path = ws.cache_dir.join(format!("{}.lock", preview.cache_key));
        fs::write(&key_lock_path, b"").expect("write per-key lock");
        fs::set_permissions(&key_lock_path, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 per-key lock");
        let err = classify_advice(&scope, &req_path, false, vault.clone(), &client)
            .await
            .expect_err("0700 per-key lock must fail");
        assert_eq!(err.code(), "security_violation");
        assert_eq!(
            fs::symlink_metadata(&key_lock_path)
                .expect("meta")
                .permissions()
                .mode()
                & 0o777,
            0o700,
            "0700 per-key lock must not be repaired via chmod"
        );
        fs::set_permissions(&key_lock_path, fs::Permissions::from_mode(0o600))
            .expect("restore 0600 per-key lock");
        let external_lock_link = ws.base.join("external-key-lock.lock");
        fs::hard_link(&key_lock_path, &external_lock_link).expect("hardlink per-key lock");
        let err = classify_advice(&scope, &req_path, false, vault.clone(), &client)
            .await
            .expect_err("hardlinked per-key lock must fail");
        assert_eq!(err.code(), "security_violation");
        fs::remove_file(&external_lock_link).expect("remove external lock link");
        fs::remove_file(&key_lock_path).expect("remove per-key lock");

        // 6d. Executable 0700 `.cache.lock` global lock file rejected without chmod.
        let global_lock_path = ws.cache_dir.join(".cache.lock");
        fs::write(&global_lock_path, b"").expect("write global lock");
        fs::set_permissions(&global_lock_path, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 global lock");
        let err = classify_advice(&scope, &req_path, false, vault.clone(), &client)
            .await
            .expect_err("0700 global lock must fail");
        assert_eq!(err.code(), "security_violation");
        assert_eq!(
            fs::symlink_metadata(&global_lock_path)
                .expect("meta")
                .permissions()
                .mode()
                & 0o777,
            0o700,
            "0700 global lock must not be repaired via chmod"
        );
        fs::remove_file(&global_lock_path).expect("remove 0700 global lock");

        // 7. FIFO inside cache_dir rejected.
        let fifo_path = ws.cache_dir.join(format!("{}.json", "a".repeat(64)));
        rustix::fs::mknodat(
            rustix::fs::CWD,
            &fifo_path,
            rustix::fs::FileType::Fifo,
            rustix::fs::Mode::from_raw_mode(0o600),
            0,
        )
        .expect("create fifo in cache_dir");
        let err = classify_advice(&scope, &req_path, false, vault, &client)
            .await
            .expect_err("FIFO in cache_dir must fail");
        assert_eq!(err.code(), "security_violation");
    }

    #[tokio::test]
    async fn enforces_request_summary_and_cache_count_and_byte_bounds() {
        let ws = ScopedTestWorkspace::new("bounds-check");
        let reg_rev = ws.seed_jev_registry();
        let scope = ws.scope("ctx-bounds");

        // 1. Summary > 8192 UTF-8 bytes rejected.
        let long_summary = "x".repeat(MAX_TASK_SUMMARY_BYTES + 1);
        let over_summary_json = format!(
            r#"{{
                "schema_version": 1,
                "expected_registry_revision": {reg_rev},
                "tool_id": "jev-api",
                "model_id": "jev-api:jev-latest",
                "task_id": "task-1",
                "task_revision": 1,
                "task_summary": "{long_summary}"
            }}"#
        );
        let over_summary_path =
            ws.write_private_file("over-summary.json", over_summary_json.as_bytes());
        let err =
            preview_advice(&scope, &over_summary_path).expect_err("summary > 8192 bytes must fail");
        assert_eq!(err.code(), "invalid_request");

        // 2. Request file > 16 KiB rejected with request_too_large.
        let padding = " ".repeat(MAX_REQUEST_FILE_BYTES + 128);
        let over_file_json = format!(
            r#"{{
                "schema_version": 1,
                "expected_registry_revision": {reg_rev},
                "tool_id": "jev-api",
                "model_id": "jev-api:jev-latest",
                "task_id": "task-1",
                "task_revision": 1,
                "task_summary": "Valid summary."{padding}
            }}"#
        );
        let over_file_path = ws.write_private_file("over-file.json", over_file_json.as_bytes());
        let err = preview_advice(&scope, &over_file_path).expect_err("request > 16 KiB must fail");
        assert_eq!(err.code(), "request_too_large");

        // 3. Cache record count bound (MAX_CACHE_RECORDS = 256) enforced before API call.
        fs::create_dir_all(&ws.cache_dir).expect("create cache_dir");
        fs::set_permissions(&ws.cache_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 cache_dir");
        for idx in 0..MAX_CACHE_RECORDS {
            let key = format!("{idx:064x}");
            let path = ws.cache_dir.join(format!("{key}.json"));
            fs::write(&path, b"{}").expect("write dummy record");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 dummy record");
        }

        let valid_req_json = format!(
            r#"{{
                "schema_version": 1,
                "expected_registry_revision": {reg_rev},
                "tool_id": "jev-api",
                "model_id": "jev-api:jev-latest",
                "task_id": "task-257",
                "task_revision": 1,
                "task_summary": "Trigger 257th record check."
            }}"#
        );
        let valid_req_path = ws.write_private_file("valid-req.json", valid_req_json.as_bytes());

        let vault = FakeVault::with_secret("jev-api", "secret");
        let transport = FakeTransport::with_responses([]);
        let client =
            DecisionClient::with_transport(vault.clone(), transport.clone()).expect("client");

        let err = classify_advice(&scope, &valid_req_path, false, vault.clone(), &client)
            .await
            .expect_err("257th unprunable cache record must fail with cache_capacity_exceeded");
        assert_eq!(err.code(), "cache_capacity_exceeded");
        assert_eq!(vault.read_count(), 0);
        assert!(transport.recorded_requests().is_empty());

        // 4. Total cache byte bound (MAX_CACHE_TOTAL_BYTES = 1 MiB) enforced before API call.
        fs::remove_dir_all(&ws.cache_dir).expect("reset cache_dir");
        fs::create_dir_all(&ws.cache_dir).expect("recreate cache_dir");
        fs::set_permissions(&ws.cache_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 cache_dir");
        let chunk = vec![b' '; MAX_CACHE_RECORD_BYTES];
        // 64 files * 16 KiB = 1 MiB exactly.
        for idx in 0..64usize {
            let key = format!("{idx:064x}");
            let path = ws.cache_dir.join(format!("{key}.json"));
            fs::write(&path, &chunk).expect("write 16 KiB record");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 record");
        }
        let err = classify_advice(&scope, &valid_req_path, false, vault.clone(), &client)
            .await
            .expect_err("exceeding 1 MiB cache total bytes must fail before API");
        assert_eq!(err.code(), "cache_capacity_exceeded");
        assert_eq!(vault.read_count(), 0);
        assert!(transport.recorded_requests().is_empty());
    }

    #[tokio::test]
    async fn shadow_advice_never_mutates_existing_profiles_or_model_selections() {
        let ws = ScopedTestWorkspace::new("zero-profile-mutation");
        let rev1 = ws.seed_jev_registry();
        let store = RegistryStore::open(ws.registry_dir.clone()).expect("open registry");
        let snap = store
            .apply(
                rev1,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        id: "user-profile".to_string(),
                        name: "User Explicit Profile".to_string(),
                        assignments: vec![RoleAssignment {
                            role: WorkflowRole::Coder,
                            difficulty: Some(Difficulty::Easy),
                            choice: ModelChoice {
                                model_id: "jev-api:jev-1.14.0".to_string(),
                                effort: None,
                            },
                        }],
                    },
                },
            )
            .expect("upsert user profile");
        let reg_rev = snap.revision;
        let registry_file = ws.registry_dir.join("registry.json");
        let reg_bytes_before = fs::read(&registry_file).expect("read registry before");

        let scope = ws.scope("ctx-shadow");
        let req_json = format!(
            r#"{{
                "schema_version": 1,
                "expected_registry_revision": {reg_rev},
                "tool_id": "jev-api",
                "model_id": "jev-api:jev-1.14.0",
                "task_id": "task-shadow",
                "task_revision": 1,
                "task_summary": "Assess task without changing profile or plan.",
                "baseline_difficulty": "easy"
            }}"#
        );
        let req_path = ws.write_private_file("req.json", req_json.as_bytes());

        let vault = FakeVault::with_secret("jev-api", "secret");
        let transport = FakeTransport::with_responses([valid_jev_four_way_response(
            "jev-1.14.0",
            "extreme",
            0.99,
            (0.00, 0.00, 0.01, 0.99),
            110,
            10,
        )]);
        let client = DecisionClient::with_transport(vault.clone(), transport).expect("client");

        let out = classify_advice(&scope, &req_path, false, vault, &client)
            .await
            .expect("classify should succeed");
        assert_eq!(out.advice.difficulty, Some(Difficulty::Extreme));
        assert_eq!(out.baseline_difficulty, Some(Difficulty::Easy));
        assert_eq!(out.agrees_with_baseline, Some(false));

        let reg_bytes_after = fs::read(&registry_file).expect("read registry after");
        assert_eq!(
            reg_bytes_before, reg_bytes_after,
            "even a 0.99 confident disagreeing classification must never mutate the registry"
        );

        // Plans directory must contain only `advice-cache` and zero plan ledger mutations.
        let plans_entries: Vec<_> = fs::read_dir(&ws.plans_dir)
            .expect("read plans_dir")
            .map(|e| e.expect("entry").file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(plans_entries, vec!["advice-cache".to_string()]);
    }

    struct BarrierTransport {
        in_flight: AtomicU64,
        total_requests: AtomicU64,
        target_in_flight: u64,
        reached_target: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    impl DecisionTransport for BarrierTransport {
        fn execute<'a>(&'a self, _request: TransportRequest) -> TransportFuture<'a> {
            Box::pin(async move {
                self.total_requests.fetch_add(1, Ordering::SeqCst);
                let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                if current >= self.target_in_flight {
                    self.reached_target.notify_waiters();
                }
                self.release.notified().await;
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                valid_jev_four_way_response(
                    "jev-1.14.0",
                    "medium",
                    0.89,
                    (0.05, 0.85, 0.07, 0.03),
                    100,
                    10,
                )
            })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_different_key_admissions_never_exceed_record_or_byte_bounds_and_refused_keys_leave_zero_locks(
    ) {
        let ws = ScopedTestWorkspace::new("concurrent-diff-key-bounds");
        let reg_rev = ws.seed_jev_registry();
        let scope = ws.scope("ctx-conc-bounds");

        fs::create_dir_all(&ws.cache_dir).expect("create cache_dir");
        fs::set_permissions(&ws.cache_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 cache_dir");

        // Seed 254 records so only 2 slots remain before MAX_CACHE_RECORDS (256).
        for idx in 0..(MAX_CACHE_RECORDS - 2) {
            let key = format!("{idx:064x}");
            let path = ws.cache_dir.join(format!("{key}.json"));
            fs::write(&path, b"{}").expect("write seeded record");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 seeded record");
        }

        let reached_two = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let transport = Arc::new(BarrierTransport {
            in_flight: AtomicU64::new(0),
            total_requests: AtomicU64::new(0),
            target_in_flight: 2,
            reached_target: Arc::clone(&reached_two),
            release: Arc::clone(&release),
        });
        let vault = FakeVault::with_secret("jev-api", "secret-conc-key");
        let client =
            DecisionClient::with_transport(vault.clone(), transport.clone()).expect("client");

        let mut req_paths = Vec::new();
        for idx in 0..5usize {
            let req_json = format!(
                r#"{{
                    "schema_version": 1,
                    "expected_registry_revision": {reg_rev},
                    "tool_id": "jev-api",
                    "model_id": "jev-api:jev-latest",
                    "task_id": "task-conc-{idx}",
                    "task_revision": 1,
                    "task_summary": "Concurrent different-key admission test {idx}."
                }}"#
            );
            req_paths.push(ws.write_private_file(&format!("conc-{idx}.json"), req_json.as_bytes()));
        }

        let reached_wait = reached_two.notified();
        let mut handles = tokio::task::JoinSet::new();
        for req_path in req_paths {
            let scope_clone = scope.clone();
            let vault_clone = vault.clone();
            let client_clone = client.clone();
            handles.spawn(async move {
                classify_advice(&scope_clone, &req_path, false, vault_clone, &client_clone).await
            });
        }

        tokio::time::timeout(std::time::Duration::from_secs(5), reached_wait)
            .await
            .expect("two concurrent admissions should reach provider barrier");

        // Await actual refusals while the two admitted requests remain blocked.
        // A 100 ms scheduler deadline is not an admission or filesystem invariant.
        let mut cap_err_count = 0usize;
        for _ in 0..3 {
            let result =
                tokio::time::timeout(std::time::Duration::from_secs(5), handles.join_next())
                    .await
                    .expect("three refused keys must complete without releasing the provider")
                    .expect("a refused task must remain")
                    .expect("join refused task");
            let err = result.expect_err("only refusals can finish before provider release");
            assert_eq!(err.code(), "cache_capacity_exceeded");
            cap_err_count += 1;
        }

        // While 2 requests are in flight at the provider:
        // - `.json` count must be exactly 256 (never > 256)
        // - per-key `.lock` count must be at most 2 (refused keys must not create lock files)
        let mut mid_json_count = 0usize;
        let mut mid_key_lock_count = 0usize;
        for entry in fs::read_dir(&ws.cache_dir).expect("read cache_dir mid-flight") {
            let name = entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .to_string();
            if name.ends_with(".json") {
                mid_json_count += 1;
            } else if name.ends_with(".lock") && name != ".cache.lock" {
                mid_key_lock_count += 1;
            }
        }
        assert_eq!(mid_json_count, MAX_CACHE_RECORDS);
        assert_eq!(
            mid_key_lock_count, 2,
            "refused keys must never create per-key lock files"
        );

        release.notify_waiters();

        let mut ok_count = 0usize;
        while let Some(handle) = handles.join_next().await {
            match handle.expect("join task") {
                Ok(out) => {
                    assert_eq!(out.source, AdviceSource::Provider);
                    ok_count += 1;
                }
                Err(err) => {
                    assert_eq!(err.code(), "cache_capacity_exceeded");
                    cap_err_count += 1;
                }
            }
        }
        assert_eq!(ok_count, 2);
        assert_eq!(cap_err_count, 3);
        assert_eq!(transport.total_requests.load(Ordering::SeqCst), 2);
        assert_eq!(
            vault.read_count(),
            4,
            "only the 2 admitted keys may read the vault (precheck + client.classify)"
        );

        // After the 2 in-flight requests finish, per-key lock count must be 0.
        // Repeated new keys against the full cache must refuse before creating any lock files.
        for extra_idx in 0..15usize {
            let extra_json = format!(
                r#"{{
                    "schema_version": 1,
                    "expected_registry_revision": {reg_rev},
                    "tool_id": "jev-api",
                    "model_id": "jev-api:jev-latest",
                    "task_id": "task-refused-extra-{extra_idx}",
                    "task_revision": 1,
                    "task_summary": "Extra refused key {extra_idx}."
                }}"#
            );
            let extra_path =
                ws.write_private_file(&format!("extra-{extra_idx}.json"), extra_json.as_bytes());
            let err = classify_advice(&scope, &extra_path, false, vault.clone(), &client)
                .await
                .expect_err("cache is full; new key must fail with cache_capacity_exceeded");
            assert_eq!(err.code(), "cache_capacity_exceeded");
        }
        assert_eq!(transport.total_requests.load(Ordering::SeqCst), 2);
        assert_eq!(vault.read_count(), 4);

        let mut final_json_count = 0usize;
        let mut final_key_lock_count = 0usize;
        for entry in fs::read_dir(&ws.cache_dir).expect("read cache_dir after refusals") {
            let name = entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .to_string();
            if name.ends_with(".json") {
                final_json_count += 1;
            } else if name.ends_with(".lock") && name != ".cache.lock" {
                final_key_lock_count += 1;
            }
        }
        assert_eq!(final_json_count, MAX_CACHE_RECORDS);
        assert_eq!(
            final_key_lock_count, 0,
            "repeated refused new keys must leave zero per-key lock files"
        );

        // Part B: Verify `.tmp-advice-*` temporary entries count toward the 1 MiB filesystem bound
        // so concurrent/in-flight temp files cannot permit a second uncounted MiB.
        fs::remove_dir_all(&ws.cache_dir).expect("reset cache_dir");
        fs::create_dir_all(&ws.cache_dir).expect("recreate cache_dir");
        fs::set_permissions(&ws.cache_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 cache_dir");
        let chunk_16k = vec![b' '; MAX_CACHE_RECORD_BYTES];
        for idx in 0..63usize {
            let key = format!("{idx:064x}");
            let path = ws.cache_dir.join(format!("{key}.json"));
            fs::write(&path, &chunk_16k).expect("write 16 KiB record");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 record");
        }
        // 63 * 16384 = 1,032,192 bytes. Add a 16,100-byte `.tmp-advice-*` file -> 1,048,292 bytes
        // (only 284 bytes remaining under 1 MiB, which cannot fit a new Pending record).
        let tmp_path = ws.cache_dir.join(".tmp-advice-concurrent-inflight");
        fs::write(&tmp_path, vec![b' '; 16_100]).expect("write temp file");
        fs::set_permissions(&tmp_path, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 temp file");

        let byte_req_path = ws.write_private_file(
            "byte-bound.json",
            format!(
                r#"{{
                    "schema_version": 1,
                    "expected_registry_revision": {reg_rev},
                    "tool_id": "jev-api",
                    "model_id": "jev-api:jev-latest",
                    "task_id": "task-byte-bound",
                    "task_revision": 1,
                    "task_summary": "Must be refused because .tmp-advice-* counts toward 1 MiB."
                }}"#
            )
            .as_bytes(),
        );
        let err = classify_advice(&scope, &byte_req_path, false, vault.clone(), &client)
            .await
            .expect_err("temp file bytes must count toward 1 MiB bound");
        assert_eq!(err.code(), "cache_capacity_exceeded");
        assert_eq!(transport.total_requests.load(Ordering::SeqCst), 2);
        assert_eq!(vault.read_count(), 4);
    }

    #[tokio::test]
    async fn completion_after_provider_await_uses_fresh_global_byte_accounting_and_preserves_pending_on_overflow(
    ) {
        let ws = ScopedTestWorkspace::new("fresh-post-await-bytes");
        let reg_rev = ws.seed_jev_registry();
        let scope = ws.scope("ctx-post-bytes");

        let req_json = format!(
            r#"{{
                "schema_version": 1,
                "expected_registry_revision": {reg_rev},
                "tool_id": "jev-api",
                "model_id": "jev-api:jev-latest",
                "task_id": "task-post-bytes",
                "task_revision": 1,
                "task_summary": "Verify fresh global scan at post-await completion."
            }}"#
        );
        let req_path = ws.write_private_file("req.json", req_json.as_bytes());
        let preview = preview_advice(&scope, &req_path).expect("preview");

        let vault = FakeVault::with_secret("jev-api", "secret");
        let transport = FakeTransport::with_responses([
            valid_jev_four_way_response(
                "jev-1.14.0",
                "hard",
                0.93,
                (0.02, 0.05, 0.90, 0.03),
                110,
                12,
            ),
            valid_jev_four_way_response(
                "jev-1.14.0",
                "hard",
                0.93,
                (0.02, 0.05, 0.90, 0.03),
                110,
                12,
            ),
        ]);

        // While the provider call is in flight (after Phase 1 sealed Pending when other_disk_bytes was 0),
        // simulate concurrent cache activity filling `other_disk_bytes` to `MAX_CACHE_TOTAL_BYTES - 750`.
        // Here:
        // - `other_disk_bytes + new_completed_len (~670B) <= MAX_CACHE_TOTAL_BYTES` (fits without current!)
        // - `existing_pending_bytes (~530B) + other_disk_bytes <= MAX_CACHE_TOTAL_BYTES` (valid existing dir!)
        // - `existing_pending_bytes + other_disk_bytes + new_completed_len > MAX_CACHE_TOTAL_BYTES` (exceeds!)
        let cache_dir_for_hook = ws.cache_dir.clone();
        transport.set_on_execute(move || {
            let chunk_16k = vec![b' '; MAX_CACHE_RECORD_BYTES];
            for idx in 0..63usize {
                let key = format!("{idx:064x}");
                let path = cache_dir_for_hook.join(format!("{key}.json"));
                fs::write(&path, &chunk_16k).expect("write concurrent 16 KiB record");
                fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                    .expect("chmod 0600 concurrent record");
            }
            let tmp_path = cache_dir_for_hook.join(".tmp-advice-concurrent-writer");
            fs::write(&tmp_path, vec![b' '; MAX_CACHE_RECORD_BYTES - 750])
                .expect("write concurrent temp file");
            fs::set_permissions(&tmp_path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 concurrent temp file");
        });

        let client =
            DecisionClient::with_transport(vault.clone(), transport.clone()).expect("client");

        // Phase 2 completion must perform a fresh global scan under `.cache.lock`, include the
        // existing `Pending` record bytes alongside `other_disk_bytes` and `new_completed_len`,
        // and refuse with `cache_capacity_exceeded`.
        let err = classify_advice(&scope, &req_path, false, vault.clone(), &client)
            .await
            .expect_err(
                "completion must fail when existing_pending + other + new_completed > 1 MiB",
            );
        assert_eq!(err.code(), "cache_capacity_exceeded");
        assert_eq!(transport.recorded_requests().len(), 1);
        assert!(
            !ws.cache_dir
                .join(format!("{}.lock", preview.cache_key))
                .exists(),
            "per-key lock must be cleaned up on completion failure"
        );
        assert!(
            ws.cache_dir
                .join(format!("{}.json", preview.cache_key))
                .exists(),
            "sealed Pending record must never be deleted to make room on completion overflow"
        );

        // Remove the concurrent temp file so space is available again, then verify that
        // because an API call was already made, the record remained sealed as `Pending`
        // and refuses automatic replay without `--refresh`!
        fs::remove_file(ws.cache_dir.join(".tmp-advice-concurrent-writer"))
            .expect("remove concurrent temp file");

        let replay_err = classify_advice(&scope, &req_path, false, vault.clone(), &client)
            .await
            .expect_err("post-API failure must remain Pending and refuse replay without --refresh");
        assert_eq!(replay_err.code(), "pending_attempt_requires_refresh");
        assert_eq!(
            transport.recorded_requests().len(),
            1,
            "refused replay must not perform a second API call"
        );

        // Explicit `--refresh` authorizes recovery and completes cleanly.
        transport.set_on_execute(|| {});
        let recovered = classify_advice(&scope, &req_path, true, vault, &client)
            .await
            .expect("explicit --refresh should recover after space is freed");
        assert_eq!(recovered.source, AdviceSource::Provider);
        assert_eq!(recovered.advice.difficulty, Some(Difficulty::Hard));
        assert_eq!(transport.recorded_requests().len(), 2);
    }

    #[tokio::test]
    async fn atomic_replace_enforces_existing_current_plus_other_plus_new_bound_without_temp_or_lock_growth(
    ) {
        let ws = ScopedTestWorkspace::new("atomic-replace-current-bytes");
        let reg_rev = ws.seed_jev_registry();
        let scope = ws.scope("ctx-replace-bytes");

        let req_json = format!(
            r#"{{
                "schema_version": 1,
                "expected_registry_revision": {reg_rev},
                "tool_id": "jev-api",
                "model_id": "jev-api:jev-latest",
                "task_id": "task-replace-bytes",
                "task_revision": 1,
                "task_summary": "Ensure existing current record bytes are counted during atomic replace."
            }}"#
        );
        let req_path = ws.write_private_file("req.json", req_json.as_bytes());
        let preview = preview_advice(&scope, &req_path).expect("preview");

        let vault = FakeVault::with_secret("jev-api", "secret");
        let transport = FakeTransport::with_responses([valid_jev_four_way_response(
            "jev-1.14.0",
            "easy",
            0.95,
            (0.92, 0.04, 0.03, 0.01),
            100,
            10,
        )]);
        let client =
            DecisionClient::with_transport(vault.clone(), transport.clone()).expect("client");

        // 1. Seed a valid Completed record for `preview.cache_key` via `classify_advice`.
        let initial = classify_advice(&scope, &req_path, false, vault.clone(), &client)
            .await
            .expect("initial classify");
        assert_eq!(initial.source, AdviceSource::Provider);

        let record_path = ws.cache_dir.join(format!("{}.json", preview.cache_key));
        let existing_bytes = fs::read(&record_path).expect("read existing record");
        let existing_current_len = existing_bytes.len();
        assert!(existing_current_len > 500);

        // 2. Populate `other_disk_bytes` so that:
        //    - `other_disk_bytes = MAX_CACHE_TOTAL_BYTES - existing_current_len - 100`
        //    - `other_disk_bytes + new_pending_len (< existing_current_len) <= MAX_CACHE_TOTAL_BYTES` (other + new fits!)
        //    - `existing_current_len + other_disk_bytes = MAX_CACHE_TOTAL_BYTES - 100 <= MAX_CACHE_TOTAL_BYTES`
        //    - `existing_current_len + other_disk_bytes + new_pending_len > MAX_CACHE_TOTAL_BYTES` (exceeds!)
        let chunk_16k = vec![b' '; MAX_CACHE_RECORD_BYTES];
        for idx in 0..63usize {
            let key = format!("{idx:064x}");
            let path = ws.cache_dir.join(format!("{key}.json"));
            fs::write(&path, &chunk_16k).expect("write 16 KiB other record");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 other record");
        }
        let filler_len = MAX_CACHE_RECORD_BYTES - existing_current_len - 100;
        let filler_path = ws.cache_dir.join(format!("{:064x}.json", 63usize));
        fs::write(&filler_path, vec![b' '; filler_len]).expect("write filler record");
        fs::set_permissions(&filler_path, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 filler record");

        let vault_reads_before = vault.read_count();
        for _ in 0..10 {
            let err = classify_advice(&scope, &req_path, true, vault.clone(), &client)
                .await
                .expect_err(
                    "refresh must refuse when existing_current + other + new_pending > 1 MiB",
                );
            assert_eq!(err.code(), "cache_capacity_exceeded");
        }

        assert_eq!(vault.read_count(), vault_reads_before);
        assert_eq!(transport.recorded_requests().len(), 1);
        assert_eq!(
            fs::read(&record_path).expect("existing record must remain intact"),
            existing_bytes,
            "existing current record must never be deleted or modified on capacity refusal"
        );

        // Verify zero `.tmp-advice-*` files and zero per-key `.lock` files exist after repeated refusals.
        for entry in fs::read_dir(&ws.cache_dir).expect("read cache_dir") {
            let name = entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .to_string();
            assert!(
                !name.starts_with(".tmp-advice-"),
                "refusal must not leave temporary files"
            );
            assert!(
                !(name.ends_with(".lock") && name != ".cache.lock"),
                "refusal must not create or leave per-key lock files"
            );
        }
    }

    #[tokio::test]
    async fn refresh_abort_before_api_preserves_prior_uncertain_and_pending_barriers() {
        let ws = ScopedTestWorkspace::new("refresh-pre-api-preserves-barrier");
        let reg_rev = ws.seed_jev_registry();
        let scope = ws.scope("ctx-refresh-barrier");

        let make_req = |name: &str, task_id: &str| -> PathBuf {
            let json = format!(
                r#"{{
                    "schema_version": 1,
                    "expected_registry_revision": {reg_rev},
                    "tool_id": "jev-api",
                    "model_id": "jev-api:jev-latest",
                    "task_id": "{task_id}",
                    "task_revision": 1,
                    "task_summary": "Verify prior barrier survives pre-API --refresh abort for {task_id}."
                }}"#
            );
            ws.write_private_file(name, json.as_bytes())
        };

        let vault = FakeVault::with_secret("jev-api", "jev-valid-secret");
        let empty_vault = Arc::new(FakeVault::default());
        let failing_vault = FakeVault::failing();

        let transport = FakeTransport::with_responses([
            Err(DecisionError::Timeout),
            valid_jev_four_way_response(
                "jev-1.14.0",
                "medium",
                0.90,
                (0.05, 0.85, 0.08, 0.02),
                100,
                10,
            ),
            valid_jev_four_way_response(
                "jev-1.14.0",
                "easy",
                0.94,
                (0.90, 0.05, 0.03, 0.02),
                95,
                9,
            ),
        ]);
        let client_valid =
            DecisionClient::with_transport(vault.clone(), transport.clone()).expect("client");
        let client_empty =
            DecisionClient::with_transport(empty_vault.clone(), transport.clone()).expect("client");
        let client_failing =
            DecisionClient::with_transport(failing_vault.clone(), transport.clone())
                .expect("client");

        // Part A: Prior `Uncertain` barrier must survive `--refresh` with missing or unavailable credentials.
        let req_uncertain = make_req("uncertain-barrier.json", "task-uncertain-barrier");
        let prev_uncertain = preview_advice(&scope, &req_uncertain).expect("preview uncertain");
        let uncertain_path = ws
            .cache_dir
            .join(format!("{}.json", prev_uncertain.cache_key));

        let timeout_err =
            classify_advice(&scope, &req_uncertain, false, vault.clone(), &client_valid)
                .await
                .expect_err("timeout creates uncertain record");
        assert_eq!(timeout_err.code(), "timeout");
        assert_eq!(transport.recorded_requests().len(), 1);
        let uncertain_bytes_before =
            fs::read(&uncertain_path).expect("read persisted uncertain record");

        // Attempt `--refresh` with missing credential -> fails with `not_configured`, zero transport calls.
        let refresh_missing_err = classify_advice(
            &scope,
            &req_uncertain,
            true,
            empty_vault.clone(),
            &client_empty,
        )
        .await
        .expect_err("--refresh with missing credential must fail before transport");
        assert_eq!(refresh_missing_err.code(), "not_configured");
        assert_eq!(transport.recorded_requests().len(), 1);

        // Attempt `--refresh` with unavailable credential store -> fails with `vault_unavailable`, zero transport calls.
        let refresh_unavail_err = classify_advice(
            &scope,
            &req_uncertain,
            true,
            failing_vault.clone(),
            &client_failing,
        )
        .await
        .expect_err("--refresh with unavailable vault must fail before transport");
        assert_eq!(refresh_unavail_err.code(), "vault_unavailable");
        assert_eq!(transport.recorded_requests().len(), 1);

        // Prior `Uncertain` record must still be present on disk, and ordinary `classify`
        // (without `--refresh`) after restoring credentials MUST still refuse!
        assert_eq!(
            fs::read(&uncertain_path).expect("uncertain record must be restored"),
            uncertain_bytes_before
        );
        let ordinary_after_abort =
            classify_advice(&scope, &req_uncertain, false, vault.clone(), &client_valid)
                .await
                .expect_err("ordinary classify after aborted --refresh must still refuse");
        assert_eq!(
            ordinary_after_abort.code(),
            "uncertain_attempt_requires_refresh"
        );
        assert_eq!(transport.recorded_requests().len(), 1);

        // Explicit `--refresh` with valid credential now recovers the uncertain record.
        let recovered_uncertain =
            classify_advice(&scope, &req_uncertain, true, vault.clone(), &client_valid)
                .await
                .expect("explicit --refresh with valid credential must succeed");
        assert_eq!(
            recovered_uncertain.advice.difficulty,
            Some(Difficulty::Medium)
        );
        assert_eq!(transport.recorded_requests().len(), 2);

        // Part B: Prior abandoned `Pending` barrier must also survive `--refresh` with missing/unavailable credentials.
        let req_pending = make_req("pending-barrier.json", "task-pending-barrier");
        let prev_pending = preview_advice(&scope, &req_pending).expect("preview pending");
        let pending_path = ws
            .cache_dir
            .join(format!("{}.json", prev_pending.cache_key));

        let _ = classify_advice_with_options(
            &scope,
            &req_pending,
            false,
            vault.clone(),
            &client_valid,
            &AdviceRuntimeOptions {
                abort_after_pending_seal: true,
                now_ms_override: Some(1_700_000_000_000),
                ..AdviceRuntimeOptions::default()
            },
        )
        .await
        .expect_err("simulated crash after pending seal");
        let pending_bytes_before = fs::read(&pending_path).expect("read persisted pending record");

        // Attempt `--refresh` with missing credential and unavailable vault -> both fail before transport.
        let pend_missing_err = classify_advice(
            &scope,
            &req_pending,
            true,
            empty_vault.clone(),
            &client_empty,
        )
        .await
        .expect_err("--refresh on pending with missing secret must fail");
        assert_eq!(pend_missing_err.code(), "not_configured");

        let pend_unavail_err = classify_advice(
            &scope,
            &req_pending,
            true,
            failing_vault.clone(),
            &client_failing,
        )
        .await
        .expect_err("--refresh on pending with unavailable vault must fail");
        assert_eq!(pend_unavail_err.code(), "vault_unavailable");
        assert_eq!(transport.recorded_requests().len(), 2);

        // Prior `Pending` record is preserved and ordinary `classify` without `--refresh` still refuses!
        assert_eq!(
            fs::read(&pending_path).expect("pending record must be restored"),
            pending_bytes_before
        );
        let ordinary_pending_err =
            classify_advice(&scope, &req_pending, false, vault.clone(), &client_valid)
                .await
                .expect_err("ordinary classify after aborted pending --refresh must still refuse");
        assert_eq!(
            ordinary_pending_err.code(),
            "pending_attempt_requires_refresh"
        );
        assert_eq!(transport.recorded_requests().len(), 2);

        // Explicit `--refresh` with valid credential succeeds.
        let recovered_pending =
            classify_advice(&scope, &req_pending, true, vault.clone(), &client_valid)
                .await
                .expect("explicit --refresh with valid credential must recover pending");
        assert_eq!(recovered_pending.advice.difficulty, Some(Difficulty::Easy));
        assert_eq!(transport.recorded_requests().len(), 3);
    }

    #[tokio::test]
    async fn pruning_evicts_only_expired_completed_records_and_preserves_pending_and_uncertain_entries(
    ) {
        let ws = ScopedTestWorkspace::new("pruning-survival");
        let reg_rev = ws.seed_jev_registry();
        let scope = ws.scope("ctx-prune");

        let vault = FakeVault::with_secret("jev-api", "secret");
        let transport = FakeTransport::with_responses([
            Err(DecisionError::Timeout),
            valid_jev_four_way_response(
                "jev-1.14.0",
                "easy",
                0.94,
                (0.90, 0.05, 0.03, 0.02),
                80,
                8,
            ),
            valid_jev_four_way_response(
                "jev-1.14.0",
                "medium",
                0.91,
                (0.05, 0.88, 0.05, 0.02),
                85,
                9,
            ),
            valid_jev_four_way_response(
                "jev-1.14.0",
                "hard",
                0.96,
                (0.01, 0.03, 0.94, 0.02),
                90,
                10,
            ),
        ]);
        let client =
            DecisionClient::with_transport(vault.clone(), transport.clone()).expect("client");

        let make_req_file = |name: &str, task_id: &str| -> PathBuf {
            let json = format!(
                r#"{{
                    "schema_version": 1,
                    "expected_registry_revision": {reg_rev},
                    "tool_id": "jev-api",
                    "model_id": "jev-api:jev-latest",
                    "task_id": "{task_id}",
                    "task_revision": 1,
                    "task_summary": "Pruning test summary for {task_id}."
                }}"#
            );
            ws.write_private_file(name, json.as_bytes())
        };

        let t0 = 1_700_000_000_000u64;

        // 1. Create an abandoned `Pending` record at t0 (and remove its unlocked `.lock` file
        //    so we test that `Pending` state itself—not just a lock—survives pruning even when old).
        let req_pending = make_req_file("pending.json", "task-pending");
        let prev_pending = preview_advice(&scope, &req_pending).expect("preview pending");
        let _ = classify_advice_with_options(
            &scope,
            &req_pending,
            false,
            vault.clone(),
            &client,
            &AdviceRuntimeOptions {
                now_ms_override: Some(t0),
                abort_after_pending_seal: true,
                ..AdviceRuntimeOptions::default()
            },
        )
        .await
        .expect_err("abort after pending seal");
        let _ = fs::remove_file(
            ws.cache_dir
                .join(format!("{}.lock", prev_pending.cache_key)),
        );

        // 2. Create an `Uncertain` record at t0 (via Timeout).
        let req_uncertain = make_req_file("uncertain.json", "task-uncertain");
        let prev_uncertain = preview_advice(&scope, &req_uncertain).expect("preview uncertain");
        let _ = classify_advice_with_options(
            &scope,
            &req_uncertain,
            false,
            vault.clone(),
            &client,
            &AdviceRuntimeOptions {
                now_ms_override: Some(t0),
                ..AdviceRuntimeOptions::default()
            },
        )
        .await
        .expect_err("timeout produces uncertain record");

        // 3. Create an expired `Completed` record at t0 whose per-key lock IS actively held.
        let req_locked_exp = make_req_file("locked-exp.json", "task-locked-exp");
        let prev_locked_exp =
            preview_advice(&scope, &req_locked_exp).expect("preview locked expired");
        let _ = classify_advice_with_options(
            &scope,
            &req_locked_exp,
            false,
            vault.clone(),
            &client,
            &AdviceRuntimeOptions {
                now_ms_override: Some(t0),
                ..AdviceRuntimeOptions::default()
            },
        )
        .await
        .expect("completed at t0");
        let active_lock_path = ws
            .cache_dir
            .join(format!("{}.lock", prev_locked_exp.cache_key));
        let active_lock_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&active_lock_path)
            .expect("open active lock");
        fs::set_permissions(&active_lock_path, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 active lock");
        active_lock_file
            .try_lock_exclusive()
            .expect("hold exclusive lock on active key");

        // 4. Create an unlocked `Completed` record at t0 (which WILL be expired and eligible for pruning at t1).
        let req_evictable = make_req_file("evictable.json", "task-evictable");
        let prev_evictable = preview_advice(&scope, &req_evictable).expect("preview evictable");
        let _ = classify_advice_with_options(
            &scope,
            &req_evictable,
            false,
            vault.clone(),
            &client,
            &AdviceRuntimeOptions {
                now_ms_override: Some(t0),
                ..AdviceRuntimeOptions::default()
            },
        )
        .await
        .expect("evictable completed at t0");

        // 5. Fill the remaining 252 slots with unprunable records so total records == 256.
        for idx in 0..(MAX_CACHE_RECORDS - 4) {
            let key = format!("{idx:064x}");
            let path = ws.cache_dir.join(format!("{key}.json"));
            fs::write(&path, b"{}").expect("write unprunable record");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 unprunable record");
        }

        // At t1 = t0 + 10_000_000ms (10,000s > 3600s TTL), classify a 257th key:
        // Pruning must evict ONLY `prev_evictable`, while preserving `prev_pending`,
        // `prev_uncertain`, and `prev_locked_exp`!
        let t1 = t0 + 10_000_000;
        let req_new_1 = make_req_file("new-1.json", "task-new-1");
        let prev_new_1 = preview_advice(&scope, &req_new_1).expect("preview new 1");
        let out_new_1 = classify_advice_with_options(
            &scope,
            &req_new_1,
            false,
            vault.clone(),
            &client,
            &AdviceRuntimeOptions {
                now_ms_override: Some(t1),
                ..AdviceRuntimeOptions::default()
            },
        )
        .await
        .expect("257th key must succeed after pruning the 1 unlocked expired Completed record");
        assert_eq!(out_new_1.source, AdviceSource::Provider);

        assert!(
            !ws.cache_dir
                .join(format!("{}.json", prev_evictable.cache_key))
                .exists(),
            "unlocked expired Completed record must be evicted by pruning"
        );
        assert!(
            ws.cache_dir
                .join(format!("{}.json", prev_pending.cache_key))
                .exists(),
            "Pending record must survive pruning even when older than TTL"
        );
        assert!(
            ws.cache_dir
                .join(format!("{}.json", prev_uncertain.cache_key))
                .exists(),
            "Uncertain record must survive pruning even when older than TTL"
        );
        assert!(
            ws.cache_dir
                .join(format!("{}.json", prev_locked_exp.cache_key))
                .exists(),
            "Completed record with active per-key lock must survive pruning"
        );
        assert!(
            ws.cache_dir
                .join(format!("{}.json", prev_new_1.cache_key))
                .exists(),
            "newly admitted record must be persisted"
        );

        // Now all 256 slots are occupied by unprunable records (Pending, Uncertain, locked, or non-expired).
        // A 258th key must fail with `cache_capacity_exceeded` without evicting Pending, Uncertain, or locked entries!
        let req_new_2 = make_req_file("new-2.json", "task-new-2");
        let err_new_2 = classify_advice_with_options(
            &scope,
            &req_new_2,
            false,
            vault.clone(),
            &client,
            &AdviceRuntimeOptions {
                now_ms_override: Some(t1),
                ..AdviceRuntimeOptions::default()
            },
        )
        .await
        .expect_err("258th key must fail when only Pending, Uncertain, and locked records remain");
        assert_eq!(err_new_2.code(), "cache_capacity_exceeded");
        assert!(ws
            .cache_dir
            .join(format!("{}.json", prev_pending.cache_key))
            .exists());
        assert!(ws
            .cache_dir
            .join(format!("{}.json", prev_uncertain.cache_key))
            .exists());
        assert!(ws
            .cache_dir
            .join(format!("{}.json", prev_locked_exp.cache_key))
            .exists());

        active_lock_file.unlock().expect("unlock active lock");
    }
}
