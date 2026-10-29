use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use threadlane_core::{
    WakeupClaim, WakeupOutcome, FIXED_WAKEUP_MESSAGE, MAX_ID_LEN, MAX_WAKEUP_ERROR_LEN,
};
use tokio::process::Command;
use tokio::sync::watch;

use crate::descriptor::{ensure_platform_supported, validate_no_symlink_components};
use crate::error::Error;

/// Default bounded timeout (ms) for a single Codex `queue` invocation.
pub const DEFAULT_CODEX_TIMEOUT_MS: u64 = 10_000;

/// Maximum configurable timeout (ms) for a single Codex `queue` invocation.
pub const MAX_CODEX_TIMEOUT_MS: u64 = 300_000;

/// Explicit opt-in configuration for the Codex wake-up queue adapter.
///
/// The adapter is disabled by default. When enabled, the supervisor requires both an
/// absolute path to a non-symlink regular executable and an explicit non-empty allowlist
/// of binding IDs (`notify_bindings`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexWakeupConfig {
    /// Absolute path to the configured Codex executable.
    pub executable: PathBuf,
    /// Explicit allowlist of `binding_id`s eligible for external wake-up notifications.
    pub notify_bindings: BTreeSet<String>,
    /// Bounded timeout for each child process invocation.
    pub timeout: Duration,
}

impl CodexWakeupConfig {
    /// Validates the Codex wake-up configuration prior to starting the supervisor.
    pub fn validate(&self) -> Result<(), Error> {
        ensure_platform_supported()?;

        if self.executable.as_os_str().is_empty() {
            return Err(Error::InvalidConfig(
                "codex executable path must not be empty".to_string(),
            ));
        }
        if !self.executable.is_absolute() {
            return Err(Error::InvalidConfig(format!(
                "codex executable path `{}` must be an absolute path",
                self.executable.display()
            )));
        }

        let _ = validate_no_symlink_components(&self.executable, "codex executable")?;

        let meta = fs::symlink_metadata(&self.executable).map_err(|err| {
            Error::InvalidConfig(format!(
                "cannot stat codex executable `{}`: {err}",
                self.executable.display()
            ))
        })?;
        if meta.file_type().is_symlink() {
            return Err(Error::SecurityViolation(format!(
                "codex executable `{}` must not be a symlink",
                self.executable.display()
            )));
        }
        if !meta.file_type().is_file() {
            return Err(Error::InvalidConfig(format!(
                "codex executable `{}` must be a regular file",
                self.executable.display()
            )));
        }

        if self.notify_bindings.is_empty() {
            return Err(Error::InvalidConfig(
                "at least one `--notify-binding <BINDING_ID>` is required when `--codex` is enabled"
                    .to_string(),
            ));
        }
        for binding_id in &self.notify_bindings {
            validate_binding_id_allowlist_entry(binding_id)?;
        }

        let timeout_ms = self.timeout.as_millis();
        if timeout_ms == 0 || timeout_ms > u128::from(MAX_CODEX_TIMEOUT_MS) {
            return Err(Error::InvalidConfig(format!(
                "codex timeout must be between 1 and {MAX_CODEX_TIMEOUT_MS} ms"
            )));
        }

        Ok(())
    }
}

fn validate_binding_id_allowlist_entry(binding_id: &str) -> Result<(), Error> {
    if binding_id.is_empty()
        || binding_id.len() > MAX_ID_LEN
        || binding_id.trim() != binding_id
        || binding_id.chars().any(char::is_control)
    {
        return Err(Error::InvalidConfig(format!(
            "invalid `--notify-binding` identifier `{binding_id}`"
        )));
    }
    Ok(())
}

/// Returns the exact 5-element argument array passed to the configured Codex executable.
///
/// Never adds model, effort, approval, sandbox, or project flags, and never includes
/// worker prose, task instructions, question prompts, or chat labels.
pub fn build_codex_queue_args(claim: &WakeupClaim) -> [&str; 5] {
    [
        "queue",
        "--thread",
        &claim.thread_uuid,
        "--message",
        FIXED_WAKEUP_MESSAGE,
    ]
}

/// Invokes the configured Codex executable for a durably claimed [`WakeupClaim`].
///
/// Must be called without holding the `Core` mutex.
/// - Zero exit status -> [`WakeupOutcome::Submitted`]
/// - Pre-execution spawn failure -> [`WakeupOutcome::Failed`]
/// - Timeout, non-zero exit, signal termination, or supervisor shutdown during execution ->
///   kills and reaps the child process and returns [`WakeupOutcome::Unknown`].
pub async fn invoke_codex_wakeup(
    config: &CodexWakeupConfig,
    claim: &WakeupClaim,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> WakeupOutcome {
    match fs::symlink_metadata(&config.executable) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return WakeupOutcome::Failed {
                    error: "codex executable path is a symlink".to_string(),
                };
            }
            if !meta.file_type().is_file() {
                return WakeupOutcome::Failed {
                    error: "codex executable path is not a regular file".to_string(),
                };
            }
        }
        Err(err) => {
            return WakeupOutcome::Failed {
                error: truncate_error(&format!("cannot stat codex executable: {err}")),
            };
        }
    }

    let args = build_codex_queue_args(claim);
    let mut cmd = Command::new(&config.executable);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(err) => {
            return WakeupOutcome::Failed {
                error: truncate_error(&format!("failed to spawn codex executable: {err}")),
            };
        }
    };

    tokio::select! {
        wait_res = tokio::time::timeout(config.timeout, child.wait()) => {
            match wait_res {
                Ok(Ok(status)) if status.success() => WakeupOutcome::Submitted,
                Ok(Ok(status)) => WakeupOutcome::Unknown {
                    reason: truncate_error(&format!(
                        "codex queue process terminated with ambiguous status: {status}"
                    )),
                },
                Ok(Err(err)) => {
                    kill_and_reap(&mut child).await;
                    WakeupOutcome::Unknown {
                        reason: truncate_error(&format!(
                            "failed while waiting on codex queue process: {err}"
                        )),
                    }
                }
                Err(_elapsed) => {
                    kill_and_reap(&mut child).await;
                    WakeupOutcome::Unknown {
                        reason: truncate_error(&format!(
                            "codex queue process timed out after {} ms",
                            config.timeout.as_millis()
                        )),
                    }
                }
            }
        }
        _ = shutdown_rx.changed() => {
            kill_and_reap(&mut child).await;
            WakeupOutcome::Unknown {
                reason: "supervisor shutdown while codex queue process was in flight".to_string(),
            }
        }
    }
}

async fn kill_and_reap(child: &mut tokio::process::Child) {
    let _ = child.kill().await;
    let _ = child.wait().await;
}

fn truncate_error(raw: &str) -> String {
    let cleaned: String = raw
        .chars()
        .map(|c| if c == '\0' { ' ' } else { c })
        .take(MAX_WAKEUP_ERROR_LEN / 4)
        .collect();
    if cleaned.trim().is_empty() {
        "codex queue error".to_string()
    } else {
        cleaned
    }
}
