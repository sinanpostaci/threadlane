use std::sync::Arc;

use threadlane_connectors::CredentialStore;
use threadlane_registry::ModelDefinition;

use crate::error::DecisionError;
use crate::schema::{
    build_jev_classify_body, build_openai_classify_body, parse_jev_catalog_entries,
    parse_jev_classify_response, parse_jev_models_response, parse_openai_classify_response,
    parse_openai_models_response, validate_request, validate_secret, validate_tool_id,
    JevCatalogEntry,
};
use crate::transport::{
    ensure_success_status, DecisionTransport, ReqwestTransport, TransportMethod, TransportRequest,
    JEV_CLASSIFY_URL, JEV_MODELS_URL, OPENAI_CLASSIFY_URL, OPENAI_MODELS_URL,
};
use crate::types::{
    DecisionAdvice, DecisionProvider, DecisionRequest, SYNTHETIC_TEST_TASK_SUMMARY,
};

/// Client for explicit catalog inspection and advisory task-difficulty classification
/// against Jev (`https://api.typesafe.ai/v1/systemone`) and OpenAI Decisions
/// (`https://api.openai.com/v1/decisions`).
///
/// Initialization performs no network I/O and reads no credentials. Credentials are
/// retrieved from the vault only during explicit [`Self::models`] or [`Self::classify`] calls.
#[derive(Clone)]
pub struct DecisionClient {
    vault: Arc<dyn CredentialStore>,
    transport: Arc<dyn DecisionTransport>,
    jev_models_url: String,
    jev_classify_url: String,
    openai_models_url: String,
    openai_classify_url: String,
}

impl std::fmt::Debug for DecisionClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecisionClient")
            .field("jev_models_url", &self.jev_models_url)
            .field("jev_classify_url", &self.jev_classify_url)
            .field("openai_models_url", &self.openai_models_url)
            .field("openai_classify_url", &self.openai_classify_url)
            .finish()
    }
}

impl DecisionClient {
    /// Creates a new [`DecisionClient`] using the default `rustls`-backed HTTPS transport
    /// with fixed provider endpoints and HTTP redirects disabled.
    pub fn new(vault: Arc<dyn CredentialStore>) -> Result<Self, DecisionError> {
        let transport = ReqwestTransport::new()?;
        Self::with_transport(vault, Arc::new(transport))
    }

    /// Creates a new [`DecisionClient`] with a custom [`DecisionTransport`] implementation
    /// targeting the fixed provider HTTPS endpoints.
    pub fn with_transport(
        vault: Arc<dyn CredentialStore>,
        transport: Arc<dyn DecisionTransport>,
    ) -> Result<Self, DecisionError> {
        Ok(Self {
            vault,
            transport,
            jev_models_url: JEV_MODELS_URL.to_string(),
            jev_classify_url: JEV_CLASSIFY_URL.to_string(),
            openai_models_url: OPENAI_MODELS_URL.to_string(),
            openai_classify_url: OPENAI_CLASSIFY_URL.to_string(),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_test_base_urls(
        vault: Arc<dyn CredentialStore>,
        jev_base_url: &str,
        openai_base_url: &str,
    ) -> Result<Self, DecisionError> {
        let jev_base = jev_base_url.trim().trim_end_matches('/');
        let openai_base = openai_base_url.trim().trim_end_matches('/');
        if jev_base.is_empty() || openai_base.is_empty() {
            return Err(DecisionError::InvalidEndpoint);
        }
        let transport = ReqwestTransport::new_for_test()?;
        Ok(Self {
            vault,
            transport: Arc::new(transport),
            jev_models_url: format!("{jev_base}/v1/models"),
            jev_classify_url: format!("{jev_base}/v1/systemone"),
            openai_models_url: format!("{openai_base}/v1/models"),
            openai_classify_url: format!("{openai_base}/v1/decisions"),
        })
    }

    /// Queries the provider model catalog for the specified `tool_id` and `provider`.
    ///
    /// Performs metadata discovery only and never invokes paid classification inference.
    pub async fn models(
        &self,
        tool_id: &str,
        provider: DecisionProvider,
    ) -> Result<Vec<ModelDefinition>, DecisionError> {
        let validated_tool_id = validate_tool_id(tool_id)?;
        let bearer_token = self.read_bearer_token(validated_tool_id.clone()).await?;

        let url = match provider {
            DecisionProvider::Jev => self.jev_models_url.clone(),
            DecisionProvider::Openai => self.openai_models_url.clone(),
        };

        let response = self
            .transport
            .execute(TransportRequest {
                provider,
                method: TransportMethod::Get,
                url,
                bearer_token,
                json_body: None,
            })
            .await?;

        ensure_success_status(response.status, response.body.len())?;

        match provider {
            DecisionProvider::Jev => parse_jev_models_response(&validated_tool_id, &response.body),
            DecisionProvider::Openai => {
                parse_openai_models_response(&validated_tool_id, &response.body)
            }
        }
    }

    /// Queries the Jev `/v1/models` catalog for the specified `tool_id` and returns the
    /// exact server-reported catalog entries (including optional description and release date).
    ///
    /// Performs metadata discovery only and never injects unreturned models or invokes
    /// classification inference.
    pub async fn jev_catalog_entries(
        &self,
        tool_id: &str,
    ) -> Result<Vec<JevCatalogEntry>, DecisionError> {
        let validated_tool_id = validate_tool_id(tool_id)?;
        let bearer_token = self.read_bearer_token(validated_tool_id.clone()).await?;

        let response = self
            .transport
            .execute(TransportRequest {
                provider: DecisionProvider::Jev,
                method: TransportMethod::Get,
                url: self.jev_models_url.clone(),
                bearer_token,
                json_body: None,
            })
            .await?;

        ensure_success_status(response.status, response.body.len())?;
        parse_jev_catalog_entries(&validated_tool_id, &response.body)
    }

    /// Executes a single advisory difficulty classification request without automatic retries.
    ///
    /// Results are strictly advisory: low-confidence choices and provider refusals set
    /// `needs_review = true` and never trigger fallback models or automatic approval.
    pub async fn classify(
        &self,
        request: DecisionRequest,
    ) -> Result<DecisionAdvice, DecisionError> {
        let validated = validate_request(&request)?;
        let bearer_token = self.read_bearer_token(validated.tool_id).await?;

        let (url, json_body) = match validated.provider {
            DecisionProvider::Jev => (
                self.jev_classify_url.clone(),
                build_jev_classify_body(&validated.model, &validated.task_summary)?,
            ),
            DecisionProvider::Openai => (
                self.openai_classify_url.clone(),
                build_openai_classify_body(&validated.model, &validated.task_summary)?,
            ),
        };

        let response = self
            .transport
            .execute(TransportRequest {
                provider: validated.provider,
                method: TransportMethod::Post,
                url,
                bearer_token,
                json_body: Some(json_body),
            })
            .await?;

        ensure_success_status(response.status, response.body.len())?;

        match validated.provider {
            DecisionProvider::Jev => parse_jev_classify_response(&validated.model, &response.body),
            DecisionProvider::Openai => {
                parse_openai_classify_response(&validated.model, &response.body)
            }
        }
    }

    /// Runs an explicit synthetic decision test using [`SYNTHETIC_TEST_TASK_SUMMARY`].
    pub async fn test_decision(
        &self,
        tool_id: &str,
        provider: DecisionProvider,
        model: &str,
    ) -> Result<DecisionAdvice, DecisionError> {
        self.classify(DecisionRequest {
            tool_id: tool_id.to_string(),
            provider,
            model: model.to_string(),
            task_summary: SYNTHETIC_TEST_TASK_SUMMARY.to_string(),
        })
        .await
    }

    async fn read_bearer_token(&self, tool_id: String) -> Result<String, DecisionError> {
        let vault = Arc::clone(&self.vault);
        let secret = tokio::task::spawn_blocking(move || vault.get(&tool_id))
            .await
            .map_err(|_| DecisionError::VaultUnavailable)?
            .map_err(|_| DecisionError::VaultUnavailable)?;
        match secret {
            Some(raw) => validate_secret(&raw),
            None => Err(DecisionError::NotConfigured),
        }
    }
}
