use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;

use keyring::{Entry, Error as KeyringError};
use serde::{Deserialize, Serialize};

use crate::error::ConnectorError;

/// Default OS keychain service namespace for Threadlane tool credentials.
pub const DEFAULT_KEYRING_SERVICE: &str = "threadlane.connectors.credentials";

/// Maximum permitted byte length for a `tool_id` identifier.
pub const MAX_TOOL_ID_BYTES: usize = 128;

/// Maximum permitted byte length for an API secret stored in the credential vault.
pub const MAX_SECRET_BYTES: usize = 8_192;

/// Status returned when a non-empty secret is stored for the tool.
pub const SECRET_STATUS_CONFIGURED: &str = "configured";

/// Status returned when no secret is stored for the tool.
pub const SECRET_STATUS_NOT_CONFIGURED: &str = "not_configured";

/// Status returned when the OS credential store is locked or requires user unlock.
pub const SECRET_STATUS_LOCKED: &str = "locked";

/// Status returned when no OS credential store backend is available.
pub const SECRET_STATUS_UNAVAILABLE: &str = "unavailable";

/// Allowed safe status values reported by [`CredentialStore::status`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretStatus {
    /// A secret is stored for the requested `tool_id`.
    Configured,
    /// No secret is stored for the requested `tool_id`.
    NotConfigured,
    /// The credential store is locked and cannot be read until unlocked.
    Locked,
    /// The credential store backend is unavailable on this host/session.
    Unavailable,
}

impl SecretStatus {
    /// Returns the canonical `snake_case` status string.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Configured => SECRET_STATUS_CONFIGURED,
            Self::NotConfigured => SECRET_STATUS_NOT_CONFIGURED,
            Self::Locked => SECRET_STATUS_LOCKED,
            Self::Unavailable => SECRET_STATUS_UNAVAILABLE,
        }
    }
}

/// Thread-safe credential boundary for storing and retrieving provider API secrets.
///
/// Implementations must never fall back to plaintext files, migrate ambient CLI tokens,
/// or expose secret values in `Debug`, `Display`, or serialized error payloads.
pub trait CredentialStore: Send + Sync {
    /// Stores or replaces the secret for `tool_id` from explicit user entry.
    fn set(&self, tool_id: &str, secret: &str) -> Result<(), ConnectorError>;

    /// Retrieves the stored secret for `tool_id`, returning `Ok(None)` when not configured.
    fn get(&self, tool_id: &str) -> Result<Option<String>, ConnectorError>;

    /// Deletes any stored secret for `tool_id` idempotently.
    fn delete(&self, tool_id: &str) -> Result<(), ConnectorError>;

    /// Returns the safe credential status (`configured`, `not_configured`, `locked`, or `unavailable`).
    fn status(&self, tool_id: &str) -> Result<String, ConnectorError>;
}

/// Validates a `tool_id` identifier before credential or probe operations.
pub fn validate_tool_id(tool_id: &str) -> Result<(), ConnectorError> {
    if tool_id.is_empty() || tool_id.len() > MAX_TOOL_ID_BYTES || tool_id.trim() != tool_id {
        return Err(ConnectorError::InvalidInput(
            "Tool ID must be a non-empty identifier of at most 128 bytes.",
        ));
    }
    if tool_id.starts_with('-') || tool_id.starts_with('.') {
        return Err(ConnectorError::InvalidInput(
            "Tool ID must not start with '-' or '.'.",
        ));
    }
    if !tool_id
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
    {
        return Err(ConnectorError::InvalidInput(
            "Tool ID contains invalid characters.",
        ));
    }
    Ok(())
}

fn validate_secret_value(secret: &str) -> Result<(), ConnectorError> {
    if secret.trim().is_empty() {
        return Err(ConnectorError::InvalidInput("Secret must not be empty."));
    }
    if secret.len() > MAX_SECRET_BYTES {
        return Err(ConnectorError::InvalidInput(
            "Secret exceeds maximum permitted length.",
        ));
    }
    if secret.contains('\0') {
        return Err(ConnectorError::InvalidInput(
            "Secret must not contain NUL bytes.",
        ));
    }
    Ok(())
}

fn format_account_key(tool_id: &str) -> String {
    format!("tool:{tool_id}")
}

fn classify_keyring_error_status(err: &KeyringError) -> SecretStatus {
    match err {
        KeyringError::NoStorageAccess(_) => SecretStatus::Locked,
        KeyringError::NoDefaultStore | KeyringError::PlatformFailure(_) => {
            SecretStatus::Unavailable
        }
        KeyringError::NoEntry => SecretStatus::NotConfigured,
        _ => SecretStatus::Unavailable,
    }
}

fn map_keyring_error(err: &KeyringError) -> ConnectorError {
    match err {
        KeyringError::NoStorageAccess(_) => {
            ConnectorError::CredentialStoreLocked("OS credential store is locked or denied access.")
        }
        KeyringError::NoDefaultStore | KeyringError::PlatformFailure(_) => {
            ConnectorError::CredentialStoreUnavailable(
                "OS credential store is unavailable on this system.",
            )
        }
        KeyringError::Invalid(_, _) | KeyringError::TooLong(_, _) => {
            ConnectorError::InvalidInput("Credential store rejected invalid identifier or secret.")
        }
        _ => ConnectorError::CredentialStoreError("OS credential store operation failed."),
    }
}

/// OS keychain-backed [`CredentialStore`] using `keyring` 4.2.0 (`v1` API).
///
/// Uses the native OS credential store (Secret Service on Unix, Keychain on macOS,
/// Credential Manager on Windows) and never falls back to plaintext files.
#[derive(Clone)]
pub struct KeyringCredentialStore {
    service: String,
}

impl fmt::Debug for KeyringCredentialStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyringCredentialStore")
            .field("service", &self.service)
            .finish()
    }
}

impl Default for KeyringCredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyringCredentialStore {
    /// Creates a new [`KeyringCredentialStore`] using [`DEFAULT_KEYRING_SERVICE`].
    pub fn new() -> Self {
        Self {
            service: DEFAULT_KEYRING_SERVICE.to_string(),
        }
    }

    /// Creates a [`KeyringCredentialStore`] with a custom service namespace.
    pub fn with_service(service: impl Into<String>) -> Result<Self, ConnectorError> {
        let service = service.into();
        if service.trim().is_empty()
            || service.len() > MAX_TOOL_ID_BYTES
            || service.contains('\0')
            || service.chars().any(char::is_control)
        {
            return Err(ConnectorError::InvalidInput(
                "Keyring service name must be a non-empty safe identifier.",
            ));
        }
        Ok(Self { service })
    }

    fn build_entry(&self, tool_id: &str) -> Result<Entry, KeyringError> {
        let account = format_account_key(tool_id);
        Entry::new(&self.service, &account)
    }
}

impl CredentialStore for KeyringCredentialStore {
    fn set(&self, tool_id: &str, secret: &str) -> Result<(), ConnectorError> {
        validate_tool_id(tool_id)?;
        validate_secret_value(secret)?;
        if let Err(err) = Entry::store_status() {
            return Err(map_keyring_error(err));
        }
        let entry = self
            .build_entry(tool_id)
            .map_err(|e| map_keyring_error(&e))?;
        entry
            .set_password(secret)
            .map_err(|e| map_keyring_error(&e))
    }

    fn get(&self, tool_id: &str) -> Result<Option<String>, ConnectorError> {
        validate_tool_id(tool_id)?;
        if let Err(err) = Entry::store_status() {
            return Err(map_keyring_error(err));
        }
        let entry = self
            .build_entry(tool_id)
            .map_err(|e| map_keyring_error(&e))?;
        match entry.get_password() {
            Ok(secret) => {
                if secret.trim().is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(secret))
                }
            }
            Err(KeyringError::NoEntry) => Ok(None),
            Err(err) => Err(map_keyring_error(&err)),
        }
    }

    fn delete(&self, tool_id: &str) -> Result<(), ConnectorError> {
        validate_tool_id(tool_id)?;
        if let Err(err) = Entry::store_status() {
            return Err(map_keyring_error(err));
        }
        let entry = self
            .build_entry(tool_id)
            .map_err(|e| map_keyring_error(&e))?;
        match entry.delete_credential() {
            Ok(()) | Err(KeyringError::NoEntry) => Ok(()),
            Err(err) => Err(map_keyring_error(&err)),
        }
    }

    fn status(&self, tool_id: &str) -> Result<String, ConnectorError> {
        validate_tool_id(tool_id)?;
        if let Err(err) = Entry::store_status() {
            return Ok(classify_keyring_error_status(err).as_str().to_string());
        }
        let entry = match self.build_entry(tool_id) {
            Ok(entry) => entry,
            Err(err) => return Ok(classify_keyring_error_status(&err).as_str().to_string()),
        };
        match entry.get_password() {
            Ok(secret) => {
                if secret.trim().is_empty() {
                    Ok(SecretStatus::NotConfigured.as_str().to_string())
                } else {
                    Ok(SecretStatus::Configured.as_str().to_string())
                }
            }
            Err(KeyringError::NoEntry) => Ok(SecretStatus::NotConfigured.as_str().to_string()),
            Err(err) => Ok(classify_keyring_error_status(&err).as_str().to_string()),
        }
    }
}

/// Simulated availability state for [`InMemoryCredentialStore`] in deterministic tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimulatedVaultState {
    /// Vault is unlocked and operational.
    Available,
    /// Vault simulates a locked OS keychain.
    Locked,
    /// Vault simulates a missing/unavailable OS keychain backend.
    Unavailable,
}

struct InMemoryVaultInner {
    state: SimulatedVaultState,
    entries: BTreeMap<String, String>,
}

/// Deterministic in-memory [`CredentialStore`] for isolated unit and integration tests.
///
/// Never persists secrets to disk and never formats secret values in [`fmt::Debug`].
pub struct InMemoryCredentialStore {
    inner: Mutex<InMemoryVaultInner>,
}

/// Alias for [`InMemoryCredentialStore`] for test fixtures.
pub type FakeCredentialStore = InMemoryCredentialStore;

impl fmt::Debug for InMemoryCredentialStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.inner.lock() {
            Ok(guard) => f
                .debug_struct("InMemoryCredentialStore")
                .field("state", &guard.state)
                .field("entry_count", &guard.entries.len())
                .finish(),
            Err(_) => f
                .debug_struct("InMemoryCredentialStore")
                .field("poisoned", &true)
                .finish(),
        }
    }
}

impl Default for InMemoryCredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryCredentialStore {
    /// Creates an empty, unlocked in-memory credential store.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(InMemoryVaultInner {
                state: SimulatedVaultState::Available,
                entries: BTreeMap::new(),
            }),
        }
    }

    /// Creates an in-memory credential store that simulates a locked OS keychain.
    pub fn locked() -> Self {
        Self {
            inner: Mutex::new(InMemoryVaultInner {
                state: SimulatedVaultState::Locked,
                entries: BTreeMap::new(),
            }),
        }
    }

    /// Creates an in-memory credential store that simulates an unavailable OS keychain.
    pub fn unavailable() -> Self {
        Self {
            inner: Mutex::new(InMemoryVaultInner {
                state: SimulatedVaultState::Unavailable,
                entries: BTreeMap::new(),
            }),
        }
    }

    /// Updates the simulated vault availability state (`Available`, `Locked`, or `Unavailable`).
    pub fn set_simulated_state(&self, state: SimulatedVaultState) -> Result<(), ConnectorError> {
        let mut guard = self.inner.lock().map_err(|_| {
            ConnectorError::CredentialStoreError("Credential store lock was poisoned.")
        })?;
        guard.state = state;
        Ok(())
    }
}

impl CredentialStore for InMemoryCredentialStore {
    fn set(&self, tool_id: &str, secret: &str) -> Result<(), ConnectorError> {
        validate_tool_id(tool_id)?;
        validate_secret_value(secret)?;
        let mut guard = self.inner.lock().map_err(|_| {
            ConnectorError::CredentialStoreError("Credential store lock was poisoned.")
        })?;
        match guard.state {
            SimulatedVaultState::Available => {
                guard
                    .entries
                    .insert(tool_id.to_string(), secret.to_string());
                Ok(())
            }
            SimulatedVaultState::Locked => Err(ConnectorError::CredentialStoreLocked(
                "OS credential store is locked or denied access.",
            )),
            SimulatedVaultState::Unavailable => Err(ConnectorError::CredentialStoreUnavailable(
                "OS credential store is unavailable on this system.",
            )),
        }
    }

    fn get(&self, tool_id: &str) -> Result<Option<String>, ConnectorError> {
        validate_tool_id(tool_id)?;
        let guard = self.inner.lock().map_err(|_| {
            ConnectorError::CredentialStoreError("Credential store lock was poisoned.")
        })?;
        match guard.state {
            SimulatedVaultState::Available => Ok(guard.entries.get(tool_id).cloned()),
            SimulatedVaultState::Locked => Err(ConnectorError::CredentialStoreLocked(
                "OS credential store is locked or denied access.",
            )),
            SimulatedVaultState::Unavailable => Err(ConnectorError::CredentialStoreUnavailable(
                "OS credential store is unavailable on this system.",
            )),
        }
    }

    fn delete(&self, tool_id: &str) -> Result<(), ConnectorError> {
        validate_tool_id(tool_id)?;
        let mut guard = self.inner.lock().map_err(|_| {
            ConnectorError::CredentialStoreError("Credential store lock was poisoned.")
        })?;
        match guard.state {
            SimulatedVaultState::Available => {
                guard.entries.remove(tool_id);
                Ok(())
            }
            SimulatedVaultState::Locked => Err(ConnectorError::CredentialStoreLocked(
                "OS credential store is locked or denied access.",
            )),
            SimulatedVaultState::Unavailable => Err(ConnectorError::CredentialStoreUnavailable(
                "OS credential store is unavailable on this system.",
            )),
        }
    }

    fn status(&self, tool_id: &str) -> Result<String, ConnectorError> {
        validate_tool_id(tool_id)?;
        let guard = self.inner.lock().map_err(|_| {
            ConnectorError::CredentialStoreError("Credential store lock was poisoned.")
        })?;
        let status = match guard.state {
            SimulatedVaultState::Available => {
                if guard
                    .entries
                    .get(tool_id)
                    .is_some_and(|secret| !secret.trim().is_empty())
                {
                    SecretStatus::Configured
                } else {
                    SecretStatus::NotConfigured
                }
            }
            SimulatedVaultState::Locked => SecretStatus::Locked,
            SimulatedVaultState::Unavailable => SecretStatus::Unavailable,
        };
        Ok(status.as_str().to_string())
    }
}
