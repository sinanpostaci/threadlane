//! Advisory progress hints and soft deadlines within an unchanged hard runtime ceiling.
//!
//! Provider tool labels are untrusted hints. They never establish file changes, completed
//! checks, useful work, success, acceptance, or permission to enlarge an execution budget.
//! Live hints cover the main supervised provider subprocess. OpenCode session allocation
//! and post-run metadata collection retain their existing absolute deadline and do not
//! provide live tool-progress evidence to this watchdog.

use serde::{Deserialize, Serialize};

use crate::error::WorkerError;
use crate::stream::{NormalizedEventKind, NormalizedWorkerEvent};

/// Optional soft-deadline policy. The invocation's existing timeout remains absolute.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgressWatchdogPolicy {
    /// First advisory deadline, measured from the invocation's monotonic start.
    pub soft_timeout_ms: u64,
    /// Increment applied at a soft deadline when an advancement hint is recent.
    pub extension_ms: u64,
    /// Maximum age of a write, edit, or explicit check tool hint for an extension.
    pub recent_advancement_ms: u64,
}

impl ProgressWatchdogPolicy {
    /// Derives conservative thresholds without increasing `hard_timeout_ms`.
    pub fn derived(hard_timeout_ms: u64) -> Self {
        Self {
            soft_timeout_ms: hard_timeout_ms.saturating_mul(3).saturating_div(4).max(1),
            extension_ms: (hard_timeout_ms / 8).max(1),
            recent_advancement_ms: (hard_timeout_ms / 4).max(1),
        }
    }

    /// Rejects zero, out-of-budget, or unusable explicit soft thresholds.
    pub fn validate(&self, hard_timeout_ms: u64) -> Result<(), WorkerError> {
        if self.soft_timeout_ms == 0
            || self.soft_timeout_ms >= hard_timeout_ms
            || self.extension_ms == 0
            || self.extension_ms > hard_timeout_ms
            || self.recent_advancement_ms == 0
            || self.recent_advancement_ms > hard_timeout_ms
        {
            return Err(WorkerError::InvalidSpec(
                "progress_watchdog requires 0 < soft_timeout_ms < timeout_ms and nonzero extension_ms/recent_advancement_ms <= timeout_ms".to_string(),
            ));
        }
        Ok(())
    }
}

/// Advisory state, independent of the worker's outcome or review acceptance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgressWatchdogStatus {
    /// The initial soft deadline has not been reached.
    Observing,
    /// A recent supported tool hint allowed a bounded soft deadline extension.
    SoftDeadlineExtended,
    /// Progress evidence is uncertain; execution continues within its hard ceiling.
    AttentionRequired,
    /// The unchanged hard runtime ceiling was reached.
    HardLimitReached,
}

/// Fixed advisory reason. None of these states proves that a provider is stuck.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgressAttentionReason {
    /// No supported advancement hint was observed.
    NoAdvancementHints,
    /// Read-only tool activity was observed without a supported advancement hint.
    ReadOnlyActivity,
    /// Earlier supported advancement hints are outside the recent window.
    AdvancementNotRecent,
}

/// Bounded supervision metadata containing only fixed enums, counters, and monotonic times.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgressWatchdogSnapshot {
    /// Current advisory state.
    pub status: ProgressWatchdogStatus,
    /// Reason for the single advisory attention, when emitted.
    pub attention_reason: Option<ProgressAttentionReason>,
    /// Elapsed monotonic invocation time.
    pub elapsed_ms: u64,
    /// Current soft deadline; never later than the existing hard timeout.
    pub soft_deadline_ms: u64,
    /// Existing absolute runtime ceiling.
    pub hard_timeout_ms: u64,
    /// Last write, edit, or explicit check tool hint time, if observed.
    pub last_advancement_ms: Option<u64>,
    /// Count of normalized provider events observed.
    pub events_observed: u64,
    /// Count of supported advancement tool hints (not verified work).
    pub advancement_hints: u64,
    /// Count of read-only tool hints.
    pub read_hints: u64,
    /// Count of provider prose/progress events, which do not extend deadlines.
    pub output_hints: u64,
    /// Count of other tool labels, which do not extend deadlines.
    pub other_tool_hints: u64,
    /// Number of soft extensions within the unchanged hard ceiling.
    pub extensions: u64,
    /// Whether this invocation already attempted its one attention event.
    pub attention_emitted: bool,
}

/// Deterministic advisory state machine; callers supply monotonic elapsed milliseconds.
#[derive(Debug)]
pub struct ProgressWatchdog {
    policy: ProgressWatchdogPolicy,
    snapshot: ProgressWatchdogSnapshot,
}

impl ProgressWatchdog {
    /// Creates an invocation watchdog, deriving thresholds when the policy is absent.
    pub fn new(
        hard_timeout_ms: u64,
        policy: Option<&ProgressWatchdogPolicy>,
    ) -> Result<Self, WorkerError> {
        if hard_timeout_ms == 0 {
            return Err(WorkerError::InvalidSpec(
                "timeout_ms must be nonzero".to_string(),
            ));
        }
        if let Some(policy) = policy {
            policy.validate(hard_timeout_ms)?;
        }
        let policy = policy
            .cloned()
            .unwrap_or_else(|| ProgressWatchdogPolicy::derived(hard_timeout_ms));
        Ok(Self {
            snapshot: ProgressWatchdogSnapshot {
                status: ProgressWatchdogStatus::Observing,
                attention_reason: None,
                elapsed_ms: 0,
                soft_deadline_ms: policy.soft_timeout_ms.min(hard_timeout_ms),
                hard_timeout_ms,
                last_advancement_ms: None,
                events_observed: 0,
                advancement_hints: 0,
                read_hints: 0,
                output_hints: 0,
                other_tool_hints: 0,
                extensions: 0,
                attention_emitted: false,
            },
            policy,
        })
    }

    /// Observes an untrusted normalized event. Prose and generic command tools do not advance.
    pub fn observe(&mut self, event: &NormalizedWorkerEvent, elapsed_ms: u64) {
        self.snapshot.elapsed_ms = self.snapshot.elapsed_ms.max(elapsed_ms);
        if event.line_number == 0 || event.kind == NormalizedEventKind::SupervisorAttention {
            return;
        }
        self.snapshot.events_observed = self.snapshot.events_observed.saturating_add(1);
        match event.kind {
            NormalizedEventKind::Tool => match event.tool_name.as_deref() {
                Some(
                    "write"
                    | "Write"
                    | "write_file"
                    | "write_to_file"
                    | "edit"
                    | "Edit"
                    | "MultiEdit"
                    | "apply_patch"
                    | "replace_file_content"
                    | "multi_replace_file_content"
                    | "run_tests"
                    | "check"
                    | "lint"
                    | "typecheck",
                ) => {
                    self.snapshot.advancement_hints =
                        self.snapshot.advancement_hints.saturating_add(1);
                    self.snapshot.last_advancement_ms = Some(self.snapshot.elapsed_ms);
                }
                Some(
                    "read" | "Read" | "read_file" | "view_file" | "view_file_outline" | "grep"
                    | "Grep" | "glob" | "Glob" | "list_directory" | "find_by_name" | "grep_search"
                    | "search",
                ) => {
                    self.snapshot.read_hints = self.snapshot.read_hints.saturating_add(1);
                }
                _ => {
                    self.snapshot.other_tool_hints =
                        self.snapshot.other_tool_hints.saturating_add(1)
                }
            },
            NormalizedEventKind::Progress => {
                self.snapshot.output_hints = self.snapshot.output_hints.saturating_add(1)
            }
            _ => {}
        }
    }

    /// Advances a deadline. Returns the one advisory attention snapshot, never a kill request.
    pub fn poll(&mut self, elapsed_ms: u64) -> Option<ProgressWatchdogSnapshot> {
        self.snapshot.elapsed_ms = self.snapshot.elapsed_ms.max(elapsed_ms);
        let elapsed_ms = self.snapshot.elapsed_ms;
        if elapsed_ms >= self.snapshot.hard_timeout_ms {
            self.snapshot.status = ProgressWatchdogStatus::HardLimitReached;
            return None;
        }
        if self.snapshot.attention_emitted || elapsed_ms < self.snapshot.soft_deadline_ms {
            return None;
        }
        if self.snapshot.last_advancement_ms.is_some_and(|last| {
            elapsed_ms.saturating_sub(last) <= self.policy.recent_advancement_ms
        }) {
            self.snapshot.soft_deadline_ms = elapsed_ms
                .saturating_add(self.policy.extension_ms)
                .min(self.snapshot.hard_timeout_ms);
            self.snapshot.extensions = self.snapshot.extensions.saturating_add(1);
            self.snapshot.status = ProgressWatchdogStatus::SoftDeadlineExtended;
            return None;
        }
        self.snapshot.status = ProgressWatchdogStatus::AttentionRequired;
        self.snapshot.attention_reason = Some(if self.snapshot.advancement_hints > 0 {
            ProgressAttentionReason::AdvancementNotRecent
        } else if self.snapshot.read_hints > 0 {
            ProgressAttentionReason::ReadOnlyActivity
        } else {
            ProgressAttentionReason::NoAdvancementHints
        });
        self.snapshot.attention_emitted = true;
        Some(self.snapshot.clone())
    }

    /// Returns the next useful wakeup time; attention never causes repeated wakeups.
    pub fn next_deadline_ms(&self) -> u64 {
        if self.snapshot.attention_emitted {
            self.snapshot.hard_timeout_ms
        } else {
            self.snapshot.soft_deadline_ms
        }
    }

    /// Takes a final observation without inventing a deadline transition after process exit.
    pub fn snapshot(&self, elapsed_ms: u64) -> ProgressWatchdogSnapshot {
        let mut snapshot = self.snapshot.clone();
        snapshot.elapsed_ms = snapshot.elapsed_ms.max(elapsed_ms);
        snapshot
    }
}
