#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! `threadlane-decisions`: advisory task-difficulty classification and model catalog client
//! for Jev (`https://api.typesafe.ai/v1/systemone`) and OpenAI Decisions
//! (`https://api.openai.com/v1/decisions`).
//!
//! All provider interactions use fixed HTTPS origins, disabled HTTP redirects, bounded
//! timeouts and response sizes, and vault-backed credentials kept strictly inside Rust.
//! Classifications are advisory only: uncertainty (`noul`), refusals, and low-confidence
//! answers require human/orchestrator review and never trigger automatic approval or
//! model substitution.

mod client;
mod error;
mod schema;
mod transport;
mod types;

pub use client::DecisionClient;
pub use error::DecisionError;
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
    use std::sync::{Arc, Mutex};

    use threadlane_connectors::{ConnectorError, CredentialStore};
    use threadlane_registry::Difficulty;

    use super::*;

    #[derive(Default)]
    struct FakeVault {
        secrets: Mutex<HashMap<String, String>>,
        reads: Mutex<usize>,
    }

    impl FakeVault {
        fn with_secret(tool_id: &str, secret: &str) -> Arc<Self> {
            let mut map = HashMap::new();
            map.insert(tool_id.to_string(), secret.to_string());
            Arc::new(Self {
                secrets: Mutex::new(map),
                reads: Mutex::new(0),
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
    }

    impl FakeTransport {
        fn with_responses(
            responses: impl IntoIterator<Item = Result<TransportResponse, DecisionError>>,
        ) -> Arc<Self> {
            Arc::new(Self {
                requests: Mutex::new(Vec::new()),
                responses: Mutex::new(responses.into_iter().collect()),
            })
        }

        fn recorded_requests(&self) -> Vec<TransportRequest> {
            self.requests.lock().expect("requests lock").clone()
        }
    }

    impl DecisionTransport for FakeTransport {
        fn execute<'a>(&'a self, request: TransportRequest) -> TransportFuture<'a> {
            Box::pin(async move {
                self.requests.lock().expect("requests lock").push(request);
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
    async fn jev_models_catalog_includes_pinned_version_and_rejects_duplicate_names() {
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
        assert_eq!(models.len(), 2);
        assert_eq!(models[0].id, "jev-tool:jev-latest");
        assert_eq!(models[0].model_arg, "jev-latest");
        assert!(models[0].efforts.is_empty());
        assert!(models[0].allow_no_effort);
        assert_eq!(models[1].id, "jev-tool:jev-1.14.0");
        assert_eq!(models[1].model_arg, "jev-1.14.0");

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
    async fn jev_classify_validates_request_and_parses_choice_low_confidence_and_noul() {
        let vault = FakeVault::with_secret("jev-tool", "ts-secret-key");
        let transport = FakeTransport::with_responses([
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
            json_response(
                200,
                r#"{
                    "model": "jev-1.14.0",
                    "answers": {
                        "difficulty": {
                            "type": "choice",
                            "choice": "hard",
                            "confidence": 0.62,
                            "probabilities": {"easy": 0.25, "hard": 0.75}
                        }
                    },
                    "usage": {"input_tokens": 120, "output_tokens": 12}
                }"#,
            ),
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

        // Low-confidence classification requires review.
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

        // Jev `noul` abstention sets difficulty/confidence to None and requires review.
        let noul_advice = client
            .test_decision("jev-tool", DecisionProvider::Jev, "jev-1.14.0")
            .await
            .expect("jev noul response");
        assert_eq!(noul_advice.difficulty, None);
        assert_eq!(noul_advice.confidence, None);
        assert!(noul_advice.needs_review);
        assert_eq!(noul_advice.reason_code, "noul");
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

        let recorded = transport.recorded_requests();
        assert_eq!(recorded[0].url, OPENAI_CLASSIFY_URL);
        let sent_json: serde_json::Value =
            serde_json::from_str(recorded[0].json_body.as_deref().expect("post body"))
                .expect("valid request json");
        assert_eq!(sent_json["model"], "gpt-6-luna");
        assert_eq!(sent_json["input"], SYNTHETIC_TEST_TASK_SUMMARY);
        assert_eq!(sent_json["questions"][0]["type"], "choice");
        assert_eq!(sent_json["questions"][0]["name"], "difficulty");
        assert_eq!(
            sent_json["questions"][0]["choices"]
                .as_array()
                .expect("choices")
                .len(),
            4
        );

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
                    "answers": {"difficulty": {"type": "choice", "choice": "easy", "confidence": 0.9}}
                }"#,
            ),
            // 2. Confidence > 1.0
            json_response(
                200,
                r#"{
                    "model": "jev-1.14.0",
                    "answers": {"difficulty": {"type": "choice", "choice": "easy", "confidence": 1.05}}
                }"#,
            ),
            // 3. Disallowed difficulty choice
            json_response(
                200,
                r#"{
                    "model": "jev-1.14.0",
                    "answers": {"difficulty": {"type": "choice", "choice": "trivial", "confidence": 0.9}}
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
                            "probabilities": {"easy": 0.5, "easy": 0.5}
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
                    "usage": {"input_tokens": 50, "output_tokens": 5}
                }"#,
            ),
        ]);
        let client = DecisionClient::with_transport(vault, transport).expect("client init");

        // 1. Verify `models` offloads blocking vault read via `spawn_blocking`.
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

        // 2. Verify `classify` also offloads blocking vault read via `spawn_blocking`.
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
}
