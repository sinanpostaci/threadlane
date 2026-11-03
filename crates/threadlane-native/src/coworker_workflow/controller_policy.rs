//! Typed controller execution configuration/envelope, frozen check command specifications,
//! per-task and job-wide correction/attempt/role budgets, declared interface-independence
//! completeness receipts, derived correction attempt receipts, and per-job driver lease state.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use threadlane_authority::validation::validate_canonical_thread_uuid;
use threadlane_authority::{ApprovalRecord, AuthorityStore};
use threadlane_operations::{
    FinalCandidateSubject, IntegrationReceipt, IntegrationStrategy, ReviewReceipt, ReviewSubject,
    TestEvidenceSummary,
};
use threadlane_workers::{
    is_control_config_path, validate_exact_40_hex_sha, validate_executable_file,
    validate_repo_relative_path, validate_sha256_hex_digest, DEFAULT_MAX_ADVISORY_RESPONSE_BYTES,
    DEFAULT_MAX_STDERR_BYTES, DEFAULT_MAX_STDOUT_BYTES, DEFAULT_TIMEOUT_MS,
    MAX_ADVISORY_RESPONSE_BYTES, MAX_STDERR_BYTES, MAX_STDOUT_BYTES, MAX_TIMEOUT_MS,
};
use threadlane_workflow::{DecisionAdvisorMode, ToolAdapter, WorkflowRole};

use crate::conversation_change::UnresolvedBlockingChange;
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    acquire_cross_process_lock, constant_time_str_eq, ensure_platform_supported,
    read_private_file_bounded, sanitize_diagnostic_text, validate_absolute_no_symlink_components,
    validate_bounded_identifier, write_private_file_atomic, MAX_PRIVATE_STATE_FILE_BYTES,
};
use crate::profile_flow::{
    preflight, ExecutionPolicy, PreflightRequest, RoleExecutionRoute, RolePreflightRequest,
    SelectedRoleChoice,
};

use super::contracts::{
    deterministic_prefixed_id, sha256_hex, validate_non_native_worker_choice,
    CandidateCheckEvidence, CanonicalProjectBaseRef, CoworkerJobRecord, CoworkerTaskRecord,
    CoworkerTaskStage, FrozenControllerCapabilityEnvelope, TaskIndependenceProof,
    WorkflowHoldRecord, MAX_ATTEMPTS_PER_TASK, MAX_CHECK_EVIDENCE_ITEMS, MAX_COWORKER_ID_LEN,
    MAX_COWORKER_TEXT_BYTES,
};

/// Maximum in-scope correction attempts permitted per task.
pub const MAX_IN_SCOPE_CORRECTION_ATTEMPTS: u32 = MAX_ATTEMPTS_PER_TASK as u32;

/// Maximum frozen check command specifications permitted per execution envelope.
pub const MAX_CHECK_IDS_PER_ENVELOPE: usize = MAX_CHECK_EVIDENCE_ITEMS;

/// Maximum byte length of sanitized diagnostic and summary strings in controller state.
pub const MAX_COWORKER_SUMMARY_BYTES: usize = MAX_COWORKER_TEXT_BYTES;

/// Alias for [`TaskIndependenceProof`] returned by declared branch-independence evaluation.
pub type IndependenceProof = TaskIndependenceProof;

/// Computes the canonical 64-character lowercase hex SHA-256 digest of a [`FrozenControllerCapabilityEnvelope`].
pub fn compute_capability_envelope_digest(
    cap_env: &FrozenControllerCapabilityEnvelope,
) -> Result<String, NativeError> {
    cap_env.validate(None)?;
    let seed = (
        "threadlane-native:frozen_controller_capability_envelope:v1",
        &cap_env.allowed_reviewer_choices,
        &cap_env.allowed_closure_choices,
        cap_env.max_in_scope_correction_attempts,
        &cap_env.allowed_check_ids,
        cap_env.local_git_integration_allowed,
    );
    let bytes = serde_json::to_vec(&seed).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "Failed to serialize FrozenControllerCapabilityEnvelope for digest calculation.",
        )
    })?;
    Ok(sha256_hex(&bytes))
}

/// Current wire and on-disk schema version for controller policy and runtime state ledgers.
pub const CONTROLLER_POLICY_SCHEMA_VERSION: u32 = 1;

/// Default maximum in-scope correction attempts per task (`<= 2`).
pub const DEFAULT_MAX_IN_SCOPE_CORRECTIONS_PER_TASK: u32 = 2;

/// Default maximum total in-scope correction attempts across an entire job.
pub const DEFAULT_MAX_TOTAL_CORRECTIONS_PER_JOB: u32 = 4;

/// Hard upper ceiling on total in-scope correction attempts across an entire job.
pub const MAX_TOTAL_CORRECTIONS_PER_JOB_CEILING: u32 = 16;

/// Default maximum total task execution attempts (initial + derived corrections) across a job.
pub const DEFAULT_MAX_TOTAL_TASK_ATTEMPTS_PER_JOB: u32 = 16;

/// Hard upper ceiling on total task execution attempts across a job.
pub const MAX_TOTAL_TASK_ATTEMPTS_PER_JOB_CEILING: u32 = 64;

/// Maximum number of arguments permitted in a [`FrozenCheckCommandSpec::argv`].
pub const MAX_CHECK_COMMAND_ARGV_LEN: usize = 32;

/// Maximum UTF-8 byte length of a single argument in [`FrozenCheckCommandSpec::argv`].
pub const MAX_CHECK_COMMAND_ARG_BYTES: usize = 1024;

/// Default controller driver lease time-to-live in milliseconds (`60_000` ms).
pub const DEFAULT_CONTROLLER_LEASE_TTL_MS: u64 = 60_000;

/// Maximum controller driver lease time-to-live in milliseconds (`3_600_000` ms).
pub const MAX_CONTROLLER_LEASE_TTL_MS: u64 = 3_600_000;

/// Maximum number of shadow Jev observations retained per job.
pub const MAX_SHADOW_JEV_OBSERVATIONS_PER_JOB: usize = 32;

/// Disallowed shell/wrapper executable basenames for [`FrozenCheckCommandSpec`].
const DISALLOWED_CHECK_WRAPPER_BASENAMES: &[&str] = &[
    "sh", "bash", "dash", "zsh", "ksh", "csh", "tcsh", "fish", "env", "sudo", "su", "nohup",
    "setsid", "xargs", "python", "python3", "perl", "ruby", "node",
];

/// Classification of a frozen verification check command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrozenCheckKind {
    /// Formatting verification (e.g. `cargo fmt --check`).
    Formatting,
    /// Static analysis / lint verification (e.g. `cargo clippy`).
    Clippy,
    /// Automated test execution (e.g. `cargo test`).
    Tests,
    /// Other bounded verification command explicitly frozen by the operator.
    Custom,
}

impl FrozenCheckKind {
    /// Returns the canonical `snake_case` wire label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Formatting => "formatting",
            Self::Clippy => "clippy",
            Self::Tests => "tests",
            Self::Custom => "custom",
        }
    }
}

/// Exact immutable command specification frozen into a [`FrozenControllerExecutionEnvelope`].
///
/// The controller executes only the canonical `executable` with the exact `argv` array inside
/// `<worktree_checkout>/<repo_relative_cwd>`, never invoking a shell or resolving a live mutable
/// command registry at runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenCheckCommandSpec {
    /// Stable check identifier (must be listed in `allowed_check_ids`).
    pub check_id: String,
    /// Verification check category.
    pub kind: FrozenCheckKind,
    /// Canonical absolute path to a non-symlink executable file.
    pub executable: PathBuf,
    /// Exact argument vector passed to `executable` (excluding `argv[0]`).
    #[serde(default)]
    pub argv: Vec<String>,
    /// Repository-relative working directory inside the target worktree (`"."` for root).
    pub repo_relative_cwd: String,
    /// Finite wall-clock timeout in milliseconds (`1..=MAX_TIMEOUT_MS`).
    pub timeout_ms: u64,
    /// Maximum stdout bytes captured (`1..=MAX_STDOUT_BYTES`).
    pub max_stdout_bytes: usize,
    /// Maximum stderr bytes captured (`1..=MAX_STDERR_BYTES`).
    pub max_stderr_bytes: usize,
    /// Whether this check must pass on each task worktree before Reviewer acceptance.
    pub required_for_task_review: bool,
    /// Whether this check must pass on the combined `RunIntegration` candidate before Closure.
    pub required_for_candidate_closure: bool,
}

impl FrozenCheckCommandSpec {
    /// Validates executable safety, argument bounds, working-directory containment, and finite budgets.
    pub fn validate(&self) -> Result<(), NativeError> {
        validate_bounded_identifier(
            &self.check_id,
            "check_commands.check_id",
            MAX_COWORKER_ID_LEN,
        )?;
        let canonical_exe = validate_executable_file(&self.executable).map_err(|_| {
            NativeError::new(
                NativeErrorCode::SecurityViolation,
                "FrozenCheckCommandSpec.executable must be an existing canonical non-symlink executable file.",
            )
        })?;
        if canonical_exe != self.executable {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "FrozenCheckCommandSpec.executable must match its canonical path.",
            ));
        }
        let exe_name = self
            .executable
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        if DISALLOWED_CHECK_WRAPPER_BASENAMES
            .iter()
            .any(|blocked| exe_name.eq_ignore_ascii_case(blocked))
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "FrozenCheckCommandSpec.executable cannot be a shell or generic command-wrapper binary.",
            ));
        }

        if self.argv.len() > MAX_CHECK_COMMAND_ARGV_LEN {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenCheckCommandSpec.argv exceeds maximum argument count.",
            ));
        }
        for arg in &self.argv {
            if arg.is_empty()
                || arg.len() > MAX_CHECK_COMMAND_ARG_BYTES
                || arg.chars().any(char::is_control)
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "FrozenCheckCommandSpec.argv entries must be non-empty, bounded, and free of control characters.",
                ));
            }
            if arg == "-c" || arg == "/C" || arg == "--eval" {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "FrozenCheckCommandSpec.argv cannot include shell-evaluation flags (`-c`, `--eval`).",
                ));
            }
        }

        if self.repo_relative_cwd != "." {
            validate_repo_relative_path(&self.repo_relative_cwd, "repo_relative_cwd").map_err(
                |_| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "FrozenCheckCommandSpec.repo_relative_cwd must be `.` or a valid repository-relative path.",
                    )
                },
            )?;
            if is_control_config_path(&self.repo_relative_cwd) {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "FrozenCheckCommandSpec.repo_relative_cwd cannot target protected control configuration.",
                ));
            }
        }

        if self.timeout_ms == 0 || self.timeout_ms > MAX_TIMEOUT_MS {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenCheckCommandSpec.timeout_ms must be between 1 and MAX_TIMEOUT_MS.",
            ));
        }
        if self.max_stdout_bytes == 0 || self.max_stdout_bytes > MAX_STDOUT_BYTES {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenCheckCommandSpec.max_stdout_bytes must be between 1 and MAX_STDOUT_BYTES.",
            ));
        }
        if self.max_stderr_bytes == 0 || self.max_stderr_bytes > MAX_STDERR_BYTES {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenCheckCommandSpec.max_stderr_bytes must be between 1 and MAX_STDERR_BYTES.",
            ));
        }
        if !self.required_for_task_review && !self.required_for_candidate_closure {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenCheckCommandSpec must apply to task review, candidate closure, or both.",
            ));
        }
        Ok(())
    }

    /// Computes a deterministic 64-character lowercase hex SHA-256 digest of this check spec.
    pub fn spec_digest(&self) -> Result<String, NativeError> {
        self.validate()?;
        let executable_str = self.executable.to_string_lossy();
        let seed = (
            "threadlane-native:frozen_check_command_spec:v1",
            self.check_id.as_str(),
            self.kind.as_str(),
            executable_str.as_ref(),
            &self.argv,
            self.repo_relative_cwd.as_str(),
            self.timeout_ms,
            self.max_stdout_bytes,
            self.max_stderr_bytes,
            self.required_for_task_review,
            self.required_for_candidate_closure,
        );
        let bytes = serde_json::to_vec(&seed).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "Failed to serialize FrozenCheckCommandSpec for digest calculation.",
            )
        })?;
        Ok(sha256_hex(&bytes))
    }
}

/// Finite per-role call count, wall-clock runtime, and output capture budgets for a job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerRoleExecutionBudget {
    /// Target workflow role (`Coder`, `Reviewer`, or `Closure`).
    pub role: WorkflowRole,
    /// Maximum total invocations of `role` across the entire job (`>= 1`).
    pub max_total_calls: u32,
    /// Maximum cumulative wall-clock runtime in milliseconds across all calls for `role`.
    pub max_total_runtime_ms: u64,
    /// Maximum wall-clock timeout in milliseconds for any single invocation of `role`.
    pub max_timeout_ms_per_call: u64,
    /// Maximum stdout bytes captured per invocation.
    pub max_stdout_bytes_per_call: usize,
    /// Maximum stderr bytes captured per invocation.
    pub max_stderr_bytes_per_call: usize,
    /// Maximum structured advisory response bytes per call (`1..=MAX_ADVISORY_RESPONSE_BYTES`).
    pub max_structured_response_bytes: usize,
}

impl ControllerRoleExecutionBudget {
    /// Constructs a conservative default budget for `role`.
    pub fn default_for_role(role: WorkflowRole, max_total_calls: u32) -> Self {
        Self {
            role,
            max_total_calls: max_total_calls.max(1),
            max_total_runtime_ms: DEFAULT_TIMEOUT_MS.saturating_mul(max_total_calls.max(1) as u64),
            max_timeout_ms_per_call: DEFAULT_TIMEOUT_MS,
            max_stdout_bytes_per_call: DEFAULT_MAX_STDOUT_BYTES,
            max_stderr_bytes_per_call: DEFAULT_MAX_STDERR_BYTES,
            max_structured_response_bytes: DEFAULT_MAX_ADVISORY_RESPONSE_BYTES,
        }
    }

    /// Validates that all budget fields are non-zero and within hard bounds.
    pub fn validate(&self) -> Result<(), NativeError> {
        if !matches!(
            self.role,
            WorkflowRole::Coder | WorkflowRole::Reviewer | WorkflowRole::Closure
        ) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "ControllerRoleExecutionBudget.role must be Coder, Reviewer, or Closure.",
            ));
        }
        if self.max_total_calls == 0
            || self.max_total_calls > MAX_TOTAL_TASK_ATTEMPTS_PER_JOB_CEILING
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "ControllerRoleExecutionBudget.max_total_calls is out of bounds.",
            ));
        }
        if self.max_timeout_ms_per_call == 0 || self.max_timeout_ms_per_call > MAX_TIMEOUT_MS {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "ControllerRoleExecutionBudget.max_timeout_ms_per_call is out of bounds.",
            ));
        }
        if self.max_total_runtime_ms < self.max_timeout_ms_per_call {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "ControllerRoleExecutionBudget.max_total_runtime_ms must be >= max_timeout_ms_per_call.",
            ));
        }
        if self.max_stdout_bytes_per_call == 0 || self.max_stdout_bytes_per_call > MAX_STDOUT_BYTES
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "ControllerRoleExecutionBudget.max_stdout_bytes_per_call is out of bounds.",
            ));
        }
        if self.max_stderr_bytes_per_call == 0 || self.max_stderr_bytes_per_call > MAX_STDERR_BYTES
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "ControllerRoleExecutionBudget.max_stderr_bytes_per_call is out of bounds.",
            ));
        }
        if self.max_structured_response_bytes == 0
            || self.max_structured_response_bytes > MAX_ADVISORY_RESPONSE_BYTES
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "ControllerRoleExecutionBudget.max_structured_response_bytes is out of bounds.",
            ));
        }
        Ok(())
    }
}

/// Cumulative usage counters tracked per role for a job.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerRoleUsageCounters {
    /// Number of role calls reserved before effect.
    pub calls_reserved: u32,
    /// Number of role calls that completed.
    pub calls_completed: u32,
    /// Total observed wall-clock runtime in milliseconds.
    pub total_runtime_ms: u64,
}

/// Trigger condition under which an optional shadow-only Jev observation may be recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowJevTriggerKind {
    /// A task required an in-scope correction after Reviewer rejection.
    RepeatedInScopeCorrection,
    /// A conversational change or hold had ambiguous or incomplete branch independence.
    HoldScopeAmbiguity,
    /// A Reviewer evaluation returned `Unclear` or `Blocked`.
    ReviewerUnclearOrBlocked,
    /// A Closure evaluation returned `Unclear` or `Blocked`.
    ClosureUnclearOrBlocked,
}

impl ShadowJevTriggerKind {
    /// Returns the canonical `snake_case` wire label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RepeatedInScopeCorrection => "repeated_in_scope_correction",
            Self::HoldScopeAmbiguity => "hold_scope_ambiguity",
            Self::ReviewerUnclearOrBlocked => "reviewer_unclear_or_blocked",
            Self::ClosureUnclearOrBlocked => "closure_unclear_or_blocked",
        }
    }
}

/// Explicit opt-in policy for shadow-only Jev decision advisor observations.
///
/// Never grants execution or advancement authority and never triggers hidden external calls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowJevAdvisorPolicy {
    /// Whether shadow Jev observation is enabled for this job.
    pub enabled: bool,
    /// Configured advisor tool identifier.
    pub tool_id: String,
    /// Configured advisor model identifier.
    pub model_id: String,
    /// Configured advisor model argument.
    pub model_arg: String,
    /// Advisor mode (must always be [`DecisionAdvisorMode::Shadow`]).
    pub mode: DecisionAdvisorMode,
    /// Explicit set of triggers permitted to record shadow advisor observations.
    pub allowed_triggers: Vec<ShadowJevTriggerKind>,
    /// Maximum shadow advisor observations permitted across the job.
    pub max_shadow_calls_per_job: u32,
    /// Maximum wall-clock timeout per shadow call in milliseconds.
    pub max_timeout_ms_per_call: u64,
    /// Maximum response bytes per shadow call.
    pub max_response_bytes_per_call: usize,
}

impl ShadowJevAdvisorPolicy {
    /// Validates that the shadow Jev policy is strictly non-authoritative (`Shadow`) and bounded.
    pub fn validate(&self) -> Result<(), NativeError> {
        validate_bounded_identifier(
            &self.tool_id,
            "shadow_jev_policy.tool_id",
            MAX_COWORKER_ID_LEN,
        )?;
        validate_bounded_identifier(
            &self.model_id,
            "shadow_jev_policy.model_id",
            MAX_COWORKER_ID_LEN,
        )?;
        validate_bounded_identifier(
            &self.model_arg,
            "shadow_jev_policy.model_arg",
            MAX_COWORKER_ID_LEN,
        )?;
        if self.mode != DecisionAdvisorMode::Shadow {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "ShadowJevAdvisorPolicy.mode must be DecisionAdvisorMode::Shadow.",
            ));
        }
        if self.enabled {
            if self.allowed_triggers.is_empty() {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Enabled ShadowJevAdvisorPolicy must specify at least one allowed trigger.",
                ));
            }
            if self.max_shadow_calls_per_job == 0
                || self.max_shadow_calls_per_job > MAX_SHADOW_JEV_OBSERVATIONS_PER_JOB as u32
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "ShadowJevAdvisorPolicy.max_shadow_calls_per_job is out of bounds.",
                ));
            }
            if self.max_timeout_ms_per_call == 0 || self.max_timeout_ms_per_call > MAX_TIMEOUT_MS {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "ShadowJevAdvisorPolicy.max_timeout_ms_per_call is out of bounds.",
                ));
            }
            if self.max_response_bytes_per_call == 0
                || self.max_response_bytes_per_call > MAX_ADVISORY_RESPONSE_BYTES
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "ShadowJevAdvisorPolicy.max_response_bytes_per_call is out of bounds.",
                ));
            }
        }
        Ok(())
    }
}

/// Durable shadow-only Jev observation record persisted for diagnostics without affecting advancement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowJevObservationRecord {
    /// Deterministic observation identifier (`jev_<32hex>`).
    pub observation_id: String,
    /// Owning job identifier.
    pub job_id: String,
    /// Trigger condition that authorized this shadow observation.
    pub trigger: ShadowJevTriggerKind,
    /// Optional task identifier associated with the trigger.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// 64-character lowercase hex SHA-256 digest of the observed decision context.
    pub context_digest: String,
    /// Sanitized bounded observation summary.
    pub summary: String,
    /// Timestamp (ms since Unix epoch) when the shadow observation was recorded.
    pub recorded_at_ms: u64,
    /// Always `false`: shadow Jev observations are never authoritative for task or closure advancement.
    pub authoritative_for_advancement: bool,
}

/// Per-task semantic interface-completeness declaration inside a [`DeclaredIndependenceCompletenessReceipt`].
///
/// Current foundation nonempty `owned_interfaces` / `consumed_interfaces` arrays alone do not prove
/// semantic completeness, and empty arrays can legitimately mean "no shared interfaces" when
/// `explicitly_no_interfaces` is `true`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskIndependenceCompletenessEntry {
    /// Approved task identifier.
    pub task_id: String,
    /// 64-character lowercase hex SHA-256 digest of the frozen task specification.
    pub task_digest: String,
    /// Canonical sorted list of logical interfaces owned/defined by `task_id`.
    #[serde(default)]
    pub owned_interfaces: Vec<String>,
    /// Canonical sorted list of logical interfaces consumed by `task_id`.
    #[serde(default)]
    pub consumed_interfaces: Vec<String>,
    /// Must be `true` when both `owned_interfaces` and `consumed_interfaces` are legitimately empty.
    pub explicitly_no_interfaces: bool,
    /// Explicit attestation that the declared dependencies, write scopes, and interfaces for `task_id`
    /// are semantically complete for branch-isolation decisions.
    pub semantic_independence_attested: bool,
}

impl TaskIndependenceCompletenessEntry {
    /// Validates structural bounds and empty-interface attestation consistency.
    pub fn validate(&self) -> Result<(), NativeError> {
        validate_bounded_identifier(&self.task_id, "task_entry.task_id", MAX_COWORKER_ID_LEN)?;
        validate_sha256_hex_digest(&self.task_digest, "task_entry.task_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "TaskIndependenceCompletenessEntry.task_digest must be a 64-hex SHA-256.",
            )
        })?;
        for iface in self
            .owned_interfaces
            .iter()
            .chain(self.consumed_interfaces.iter())
        {
            validate_bounded_identifier(iface, "task_entry.interface", MAX_COWORKER_ID_LEN)?;
        }
        let has_any = !self.owned_interfaces.is_empty() || !self.consumed_interfaces.is_empty();
        if has_any && self.explicitly_no_interfaces {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "explicitly_no_interfaces cannot be true when owned_interfaces or consumed_interfaces is non-empty.",
            ));
        }
        Ok(())
    }

    /// Returns `true` when this task entry is attested as semantically complete (including legitimate empty interfaces).
    pub fn is_complete(&self) -> bool {
        let has_any = !self.owned_interfaces.is_empty() || !self.consumed_interfaces.is_empty();
        self.semantic_independence_attested && (has_any || self.explicitly_no_interfaces)
    }
}

/// Explicit completeness attestation for the impact scope of a hold or conversational change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HoldImpactCompletenessAttestation {
    /// Hold identifier (`hld_...`) or conversation change identifier (`chg_...`).
    pub hold_or_change_id: String,
    /// Directly targeted task identifiers.
    #[serde(default)]
    pub affected_task_ids: Vec<String>,
    /// Directly affected repository-relative write paths.
    #[serde(default)]
    pub affected_write_paths: Vec<String>,
    /// Directly affected logical interfaces.
    #[serde(default)]
    pub affected_interfaces: Vec<String>,
    /// Must be `true` when `affected_interfaces` is legitimately empty because the hold/change affects no shared interfaces.
    pub explicitly_no_shared_interfaces: bool,
    /// Explicit attestation that the impact declaration (`affected_task_ids`, `affected_write_paths`, `affected_interfaces`) is complete.
    pub impact_completeness_attested: bool,
}

impl HoldImpactCompletenessAttestation {
    /// Validates structural bounds and empty-interface attestation consistency.
    pub fn validate(&self) -> Result<(), NativeError> {
        validate_bounded_identifier(
            &self.hold_or_change_id,
            "hold_attestation.hold_or_change_id",
            MAX_COWORKER_ID_LEN,
        )?;
        for tid in &self.affected_task_ids {
            validate_bounded_identifier(
                tid,
                "hold_attestation.affected_task_id",
                MAX_COWORKER_ID_LEN,
            )?;
        }
        for wp in &self.affected_write_paths {
            validate_repo_relative_path(wp, "hold_attestation.affected_write_path").map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "HoldImpactCompletenessAttestation.affected_write_paths entry must be a valid repo-relative path.",
                )
            })?;
        }
        for iface in &self.affected_interfaces {
            validate_bounded_identifier(
                iface,
                "hold_attestation.affected_interface",
                MAX_COWORKER_ID_LEN,
            )?;
        }
        if !self.affected_interfaces.is_empty() && self.explicitly_no_shared_interfaces {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "explicitly_no_shared_interfaces cannot be true when affected_interfaces is non-empty.",
            ));
        }
        Ok(())
    }

    /// Returns `true` when this hold/change impact declaration is attested as semantically complete.
    pub fn is_complete(&self) -> bool {
        let has_scope = !self.affected_task_ids.is_empty()
            || !self.affected_write_paths.is_empty()
            || !self.affected_interfaces.is_empty();
        self.impact_completeness_attested
            && has_scope
            && (!self.affected_interfaces.is_empty() || self.explicitly_no_shared_interfaces)
    }
}

/// Durable completeness receipt governing whether unaffected branches may continue while a sibling
/// branch or conversational change is held.
///
/// File-disjointness alone and nonempty foundation interface arrays alone never prove semantic
/// independence; the controller requires this explicit attested receipt and conservatively holds
/// when completeness is unknown or unattested.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclaredIndependenceCompletenessReceipt {
    /// Deterministic receipt identifier (`icr_<32hex>`).
    pub receipt_id: String,
    /// Bound [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Bound [`ApprovalRecord::approval_id`].
    pub approval_id: String,
    /// Bound 64-hex [`ApprovalRecord::plan_digest`].
    pub plan_digest: String,
    /// Per-task completeness entries keyed by `task_id`.
    pub task_entries: BTreeMap<String, TaskIndependenceCompletenessEntry>,
    /// Per-hold/change impact completeness attestations keyed by `hold_id` or `change_id`.
    #[serde(default)]
    pub hold_attestations: BTreeMap<String, HoldImpactCompletenessAttestation>,
    /// Timestamp (ms since Unix epoch) when this receipt was recorded.
    pub recorded_at_ms: u64,
    /// Must be `true` to authorize branch continuation under active holds.
    pub operator_attested: bool,
    /// Canonical 64-character lowercase hex SHA-256 digest of this receipt.
    pub receipt_digest: String,
}

impl DeclaredIndependenceCompletenessReceipt {
    /// Computes the canonical 64-hex SHA-256 digest of this completeness receipt.
    pub fn compute_digest(&self) -> Result<String, NativeError> {
        let seed = (
            "threadlane-native:independence_completeness_receipt:v1",
            self.receipt_id.as_str(),
            self.job_id.as_str(),
            self.approval_id.as_str(),
            self.plan_digest.as_str(),
            &self.task_entries,
            &self.hold_attestations,
            self.recorded_at_ms,
            self.operator_attested,
        );
        let bytes = serde_json::to_vec(&seed).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "Failed to serialize DeclaredIndependenceCompletenessReceipt for digest calculation.",
            )
        })?;
        Ok(sha256_hex(&bytes))
    }

    /// Validates receipt fields, digests, and alignment with the job's frozen tasks.
    pub fn validate_for_job(&self, job: &CoworkerJobRecord) -> Result<(), NativeError> {
        validate_bounded_identifier(
            &self.receipt_id,
            "completeness_receipt.receipt_id",
            MAX_COWORKER_ID_LEN,
        )?;
        if self.job_id != job.job_id {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "DeclaredIndependenceCompletenessReceipt.job_id does not match job.job_id.",
            ));
        }
        let Some(ref linkage) = job.approval_linkage else {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Cannot validate DeclaredIndependenceCompletenessReceipt on a job without approval_linkage.",
            ));
        };
        if self.approval_id != linkage.approval_id || self.plan_digest != linkage.plan_digest {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "DeclaredIndependenceCompletenessReceipt approval_id or plan_digest does not match job.",
            ));
        }
        if !self.operator_attested {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "DeclaredIndependenceCompletenessReceipt requires operator_attested = true.",
            ));
        }
        for (task_id, entry) in &self.task_entries {
            if task_id != &entry.task_id {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "DeclaredIndependenceCompletenessReceipt task_entries key mismatch.",
                ));
            }
            entry.validate()?;
            let Some(job_task) = job.tasks.iter().find(|t| &t.task_id == task_id) else {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "DeclaredIndependenceCompletenessReceipt references an unknown task_id.",
                ));
            };
            if entry.task_digest != job_task.task_digest
                || entry.owned_interfaces != job_task.owned_interfaces
                || entry.consumed_interfaces != job_task.consumed_interfaces
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "DeclaredIndependenceCompletenessReceipt task entry does not match frozen job task.",
                ));
            }
        }
        for (hold_id, att) in &self.hold_attestations {
            if hold_id != &att.hold_or_change_id {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "DeclaredIndependenceCompletenessReceipt hold_attestations key mismatch.",
                ));
            }
            att.validate()?;
        }
        let computed = self.compute_digest()?;
        if !constant_time_str_eq(&computed, &self.receipt_digest) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "DeclaredIndependenceCompletenessReceipt.receipt_digest mismatch.",
            ));
        }
        Ok(())
    }

    /// Evaluates whether `candidate_task` is proven semantically independent of all active holds,
    /// unresolved blocking changes, and impacted sibling tasks.
    ///
    /// Conservatively returns `Err(reason)` whenever completeness attestation is missing or any
    /// DAG, write-path, or interface overlap exists.
    pub fn prove_branch_continuation_allowed(
        &self,
        candidate_task: &CoworkerTaskRecord,
        all_tasks: &[CoworkerTaskRecord],
        active_holds: &[WorkflowHoldRecord],
        unresolved_changes: &[UnresolvedBlockingChange],
    ) -> Result<IndependenceProof, String> {
        if !self.operator_attested {
            return Err("independence completeness receipt is not operator_attested".to_string());
        }
        let Some(candidate_entry) = self.task_entries.get(&candidate_task.task_id) else {
            return Err(format!(
                "missing completeness receipt entry for candidate task `{}`",
                candidate_task.task_id
            ));
        };
        if !candidate_entry.is_complete() {
            return Err(format!(
                "candidate task `{}` does not have complete semantic independence attestation",
                candidate_task.task_id
            ));
        }

        // Collect all transitive ancestors of `candidate_task`.
        let task_map: BTreeMap<&str, &CoworkerTaskRecord> =
            all_tasks.iter().map(|t| (t.task_id.as_str(), t)).collect();
        let mut ancestors = BTreeSet::new();
        let mut stack: Vec<&str> = candidate_task
            .depends_on
            .iter()
            .map(String::as_str)
            .collect();
        while let Some(dep_id) = stack.pop() {
            if ancestors.insert(dep_id.to_string()) {
                if let Some(dep_task) = task_map.get(dep_id) {
                    for parent in &dep_task.depends_on {
                        stack.push(parent.as_str());
                    }
                }
            }
        }

        let candidate_ifaces: BTreeSet<&str> = candidate_entry
            .owned_interfaces
            .iter()
            .chain(candidate_entry.consumed_interfaces.iter())
            .map(String::as_str)
            .collect();

        // 1. Check all unresolved blocking changes.
        for change in unresolved_changes {
            let Some(ref target_tid) = change.task_id else {
                return Err(format!(
                    "unresolved change `{}` is approval/job-wide",
                    change.change_id
                ));
            };
            if target_tid == &candidate_task.task_id
                || change
                    .affected_dependent_task_ids
                    .iter()
                    .any(|d| d == &candidate_task.task_id)
                || ancestors.contains(target_tid.as_str())
            {
                return Err(format!(
                    "candidate task `{}` is in the DAG impact cone of unresolved change `{}`",
                    candidate_task.task_id, change.change_id
                ));
            }
            let Some(change_att) = self.hold_attestations.get(&change.change_id) else {
                return Err(format!(
                    "unresolved change `{}` lacks an explicit HoldImpactCompletenessAttestation",
                    change.change_id
                ));
            };
            if !change_att.is_complete() {
                return Err(format!(
                    "unresolved change `{}` impact attestation is incomplete",
                    change.change_id
                ));
            }
            if change_att
                .affected_task_ids
                .iter()
                .any(|tid| tid == &candidate_task.task_id || ancestors.contains(tid.as_str()))
            {
                return Err(format!(
                    "candidate task `{}` is targeted by change attestation `{}`",
                    candidate_task.task_id, change.change_id
                ));
            }
            if write_scopes_overlap(
                &candidate_task.write_paths,
                &change_att.affected_write_paths,
            ) {
                return Err(format!(
                    "candidate task `{}` write_paths overlap change `{}` affected_write_paths",
                    candidate_task.task_id, change.change_id
                ));
            }
            if change_att
                .affected_interfaces
                .iter()
                .any(|iface| candidate_ifaces.contains(iface.as_str()))
            {
                return Err(format!(
                    "candidate task `{}` interfaces overlap change `{}` affected_interfaces",
                    candidate_task.task_id, change.change_id
                ));
            }
        }

        // 2. Check all active workflow holds.
        for hold in active_holds.iter().filter(|h| h.active) {
            if hold.holds_entire_job {
                return Err(format!(
                    "active hold `{}` applies to the entire job",
                    hold.hold_id
                ));
            }
            let att = self
                .hold_attestations
                .get(&hold.hold_id)
                .or_else(|| self.hold_attestations.get(&hold.request_key))
                .ok_or_else(|| {
                    format!(
                        "active hold `{}` lacks an explicit HoldImpactCompletenessAttestation",
                        hold.hold_id
                    )
                })?;
            if !att.is_complete() {
                return Err(format!(
                    "active hold `{}` impact attestation is incomplete",
                    hold.hold_id
                ));
            }
            if hold
                .directly_affected_task_ids
                .iter()
                .chain(att.affected_task_ids.iter())
                .any(|tid| tid == &candidate_task.task_id || ancestors.contains(tid.as_str()))
            {
                return Err(format!(
                    "candidate task `{}` is directly or transitively affected by hold `{}`",
                    candidate_task.task_id, hold.hold_id
                ));
            }
            if write_scopes_overlap(&candidate_task.write_paths, &hold.affected_write_paths)
                || write_scopes_overlap(&candidate_task.write_paths, &att.affected_write_paths)
            {
                return Err(format!(
                    "candidate task `{}` write_paths overlap hold `{}` affected_write_paths",
                    candidate_task.task_id, hold.hold_id
                ));
            }
            if hold
                .affected_interfaces
                .iter()
                .chain(att.affected_interfaces.iter())
                .any(|iface| candidate_ifaces.contains(iface.as_str()))
            {
                return Err(format!(
                    "candidate task `{}` interfaces overlap hold `{}` affected_interfaces",
                    candidate_task.task_id, hold.hold_id
                ));
            }
        }

        // 3. Check all impacted sibling tasks.
        for other in all_tasks
            .iter()
            .filter(|t| t.task_id != candidate_task.task_id)
        {
            let sibling_impacted = matches!(
                other.stage,
                CoworkerTaskStage::Held
                    | CoworkerTaskStage::Blocked
                    | CoworkerTaskStage::EffectUnknown
                    | CoworkerTaskStage::Failed
                    | CoworkerTaskStage::Cancelled
            ) || !other.active_hold_ids.is_empty()
                || other.unresolved_outbox_id.is_some()
                || active_holds
                    .iter()
                    .any(|h| h.active && h.directly_affected_task_ids.contains(&other.task_id))
                || unresolved_changes.iter().any(|c| {
                    c.task_id.as_deref() == Some(other.task_id.as_str())
                        || c.affected_dependent_task_ids.contains(&other.task_id)
                });
            if !sibling_impacted {
                continue;
            }
            if ancestors.contains(other.task_id.as_str()) {
                return Err(format!(
                    "candidate task `{}` depends on impacted sibling `{}`",
                    candidate_task.task_id, other.task_id
                ));
            }
            let Some(other_entry) = self.task_entries.get(&other.task_id) else {
                return Err(format!(
                    "impacted sibling `{}` lacks a completeness receipt entry",
                    other.task_id
                ));
            };
            if !other_entry.is_complete() {
                return Err(format!(
                    "impacted sibling `{}` completeness receipt entry is not attested complete",
                    other.task_id
                ));
            }
            if write_scopes_overlap(&candidate_task.write_paths, &other.write_paths) {
                return Err(format!(
                    "candidate task `{}` overlaps write_paths with impacted sibling `{}`",
                    candidate_task.task_id, other.task_id
                ));
            }
            let other_ifaces: BTreeSet<&str> = other_entry
                .owned_interfaces
                .iter()
                .chain(other_entry.consumed_interfaces.iter())
                .map(String::as_str)
                .collect();
            let candidate_owned: BTreeSet<&str> = candidate_entry
                .owned_interfaces
                .iter()
                .map(String::as_str)
                .collect();
            let other_owned: BTreeSet<&str> = other_entry
                .owned_interfaces
                .iter()
                .map(String::as_str)
                .collect();
            if candidate_ifaces
                .iter()
                .any(|iface| other_owned.contains(iface))
                || other_ifaces
                    .iter()
                    .any(|iface| candidate_owned.contains(iface))
            {
                return Err(format!(
                    "candidate task `{}` overlaps interfaces with impacted sibling `{}`",
                    candidate_task.task_id, other.task_id
                ));
            }
        }

        Ok(TaskIndependenceProof {
            candidate_task_id: candidate_task.task_id.clone(),
            independent: true,
            complete_declared_evidence: true,
            disjoint_dependency_closure: true,
            disjoint_write_scopes: true,
            disjoint_interface_ownership: true,
            no_job_wide_hold: true,
            blocking_reasons: Vec::new(),
        })
    }
}

fn write_scopes_overlap(paths_a: &[String], paths_b: &[String]) -> bool {
    for a in paths_a {
        let clean_a = a.trim_end_matches('/');
        for b in paths_b {
            let clean_b = b.trim_end_matches('/');
            if clean_a == clean_b
                || clean_a
                    .strip_prefix(clean_b)
                    .is_some_and(|rest| rest.starts_with('/'))
                || clean_b
                    .strip_prefix(clean_a)
                    .is_some_and(|rest| rest.starts_with('/'))
            {
                return true;
            }
        }
    }
    false
}

/// Request to construct, validate, and preview or record a [`FrozenControllerExecutionEnvelope`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FreezeControllerExecutionEnvelopeRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Expected current `job_revision` of `job_id`.
    pub expected_job_revision: u64,
    /// Governing [`ExecutionPolicy`] snapshot (must match the job's frozen `policy_digest`).
    pub policy: ExecutionPolicy,
    /// Exact headless [`WorkflowRole::Reviewer`] choice to freeze.
    pub reviewer_choice: SelectedRoleChoice,
    /// Exact headless [`WorkflowRole::Closure`] choice to freeze.
    pub closure_choice: SelectedRoleChoice,
    /// Per-role execution budgets (`Coder`, `Reviewer`, `Closure`).
    pub role_budgets: BTreeMap<WorkflowRole, ControllerRoleExecutionBudget>,
    /// Maximum in-scope correction attempts per task (default `<= 2`).
    pub max_in_scope_corrections_per_task: u32,
    /// Maximum total in-scope correction attempts across the entire job.
    pub max_total_corrections_per_job: u32,
    /// Maximum total task execution attempts (initial + corrections) across the entire job.
    pub max_total_task_attempts_per_job: u32,
    /// Exact frozen check command specifications keyed by `check_id`.
    pub check_commands: BTreeMap<String, FrozenCheckCommandSpec>,
    /// Whether the controller is authorized to commit dirty task worktrees after diff-gate verification.
    pub allow_task_worktree_commits: bool,
    /// Whether the controller is authorized to integrate accepted task commits into an owned `RunIntegration` worktree.
    pub allow_run_integration_execution: bool,
    /// Default Git integration strategy for combining accepted task worktrees.
    pub default_integration_strategy: IntegrationStrategy,
    /// Optional explicit semantic completeness receipt for branch continuation under holds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub independence_completeness_receipt: Option<DeclaredIndependenceCompletenessReceipt>,
    /// Optional explicit shadow-only Jev decision advisor policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_jev_policy: Option<ShadowJevAdvisorPolicy>,
    /// Creation timestamp (ms since Unix epoch).
    pub created_at_ms: u64,
    /// Explicit local operator attestation flag (`true` required to record/activate).
    pub operator_attested: bool,
    /// Optional confirmation of the frozen `approval_subject_digest` (required when recording).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirm_approval_subject_digest: Option<String>,
    /// Optional confirmation of the exact full `envelope_digest` from preview (required when recording).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirm_envelope_digest: Option<String>,
}

/// Immutable, operator-attested controller execution envelope bound to a specific admitted job.
///
/// Extends the job's subset [`FrozenControllerCapabilityEnvelope`] with exact frozen check
/// command executables/argv/cwd/budgets, job-wide attempt and correction caps, role call/time/output
/// budgets, task commit and `RunIntegration` powers, and semantic independence completeness receipts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenControllerExecutionEnvelope {
    /// Wire and storage schema version (`1`).
    pub schema_version: u32,
    /// Deterministic envelope identifier (`env_<32hex>`).
    pub envelope_id: String,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Bound [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Bound [`CoworkerJobRecord::work_brief`] identifier.
    pub work_brief_id: String,
    /// Bound 64-hex [`CoworkerJobRecord::work_brief_digest`].
    pub work_brief_digest: String,
    /// Bound [`ApprovalRecord::approval_id`].
    pub approval_id: String,
    /// Bound 64-hex [`ApprovalRecord::subject_digest`].
    pub approval_subject_digest: String,
    /// Bound [`ApprovalRecord::plan_id`].
    pub plan_id: String,
    /// Bound [`ApprovalRecord::plan_revision`].
    pub plan_revision: u64,
    /// Bound 64-hex [`ApprovalRecord::plan_digest`].
    pub plan_digest: String,
    /// Bound [`ApprovalRecord::profile_id`].
    pub profile_id: String,
    /// Bound [`ApprovalRecord::registry_revision`].
    pub registry_revision: u64,
    /// Canonical repository and base commit/tree reference.
    pub project_base: CanonicalProjectBaseRef,
    /// Governing [`ExecutionPolicy`] snapshot.
    pub policy: ExecutionPolicy,
    /// Bound 64-hex SHA-256 digest of `policy`.
    pub policy_digest: String,
    /// Frozen per-task [`WorkflowRole::Coder`] choices keyed by `task_id`.
    pub task_coder_choices: BTreeMap<String, SelectedRoleChoice>,
    /// Frozen headless [`WorkflowRole::Reviewer`] choice.
    pub reviewer_choice: SelectedRoleChoice,
    /// Frozen headless [`WorkflowRole::Closure`] choice.
    pub closure_choice: SelectedRoleChoice,
    /// Per-role execution budgets (`Coder`, `Reviewer`, `Closure`).
    pub role_budgets: BTreeMap<WorkflowRole, ControllerRoleExecutionBudget>,
    /// Maximum in-scope correction attempts per task (`<= 2` by default, `<= capability_envelope.max_in_scope_correction_attempts`).
    pub max_in_scope_corrections_per_task: u32,
    /// Maximum total in-scope correction attempts across the entire job.
    pub max_total_corrections_per_job: u32,
    /// Maximum total task execution attempts (initial + corrections) across the entire job.
    pub max_total_task_attempts_per_job: u32,
    /// Exact frozen check command specifications keyed by `check_id`.
    pub check_commands: BTreeMap<String, FrozenCheckCommandSpec>,
    /// Whether the controller is authorized to commit task worktree changes after diff-gate pass.
    pub allow_task_worktree_commits: bool,
    /// Whether the controller is authorized to integrate accepted task commits into an owned `RunIntegration` worktree.
    pub allow_run_integration_execution: bool,
    /// Default Git integration strategy for combining accepted task worktrees.
    pub default_integration_strategy: IntegrationStrategy,
    /// Optional explicit semantic completeness receipt for branch continuation under holds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub independence_completeness_receipt: Option<DeclaredIndependenceCompletenessReceipt>,
    /// Optional explicit shadow-only Jev decision advisor policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_jev_policy: Option<ShadowJevAdvisorPolicy>,
    /// Creation timestamp (ms since Unix epoch).
    pub created_at_ms: u64,
    /// Explicit local operator attestation flag (`true` required for execution).
    pub operator_attested: bool,
    /// Always `false`: local controller execution envelopes never prove per-call native chat identity.
    pub native_per_call_identity_verified: bool,
    /// Bound 64-hex SHA-256 digest of the job's [`FrozenControllerCapabilityEnvelope`].
    pub capability_envelope_digest: String,
    /// Canonical 64-character lowercase hex SHA-256 digest of this full execution envelope.
    pub envelope_digest: String,
}

impl FrozenControllerExecutionEnvelope {
    /// Computes the canonical 64-character lowercase hex SHA-256 authority digest of this envelope.
    ///
    /// Excludes the receipt-only `operator_attested` flag so an unattested preview (`false`) and
    /// an attested record request (`true`) over identical authority parameters share the exact
    /// same `envelope_digest`.
    pub fn compute_digest(&self) -> Result<String, NativeError> {
        let mut check_digests = BTreeMap::new();
        for (cid, spec) in &self.check_commands {
            check_digests.insert(cid.as_str(), spec.spec_digest()?);
        }
        let seed = (
            (
                "threadlane-native:frozen_controller_execution_envelope:v1",
                self.schema_version,
                self.envelope_id.as_str(),
                self.binding_id.as_str(),
                self.thread_uuid.as_str(),
                self.job_id.as_str(),
                self.work_brief_id.as_str(),
                self.work_brief_digest.as_str(),
                self.approval_id.as_str(),
                self.approval_subject_digest.as_str(),
                self.plan_id.as_str(),
                self.plan_revision,
            ),
            (
                self.plan_digest.as_str(),
                self.profile_id.as_str(),
                self.registry_revision,
                &self.project_base,
                self.policy_digest.as_str(),
                &self.task_coder_choices,
                &self.reviewer_choice,
                &self.closure_choice,
                &self.role_budgets,
                self.max_in_scope_corrections_per_task,
                self.max_total_corrections_per_job,
                self.max_total_task_attempts_per_job,
            ),
            (
                &check_digests,
                self.allow_task_worktree_commits,
                self.allow_run_integration_execution,
                self.default_integration_strategy,
                self.independence_completeness_receipt
                    .as_ref()
                    .map(|r| r.receipt_digest.as_str()),
                &self.shadow_jev_policy,
                self.created_at_ms,
                self.native_per_call_identity_verified,
                self.capability_envelope_digest.as_str(),
            ),
        );
        let bytes = serde_json::to_vec(&seed).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "Failed to serialize FrozenControllerExecutionEnvelope for digest calculation.",
            )
        })?;
        Ok(sha256_hex(&bytes))
    }

    /// Validates all structural, policy, model-choice, command-spec, budget, and digest invariants
    /// of this execution envelope against `job`.
    pub fn validate_against_job(&self, job: &CoworkerJobRecord) -> Result<(), NativeError> {
        if self.schema_version != CONTROLLER_POLICY_SCHEMA_VERSION {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Unsupported FrozenControllerExecutionEnvelope.schema_version.",
            ));
        }
        validate_bounded_identifier(&self.envelope_id, "envelope_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", &self.thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenControllerExecutionEnvelope.thread_uuid must be a canonical non-nil UUID.",
            )
        })?;
        if self.native_per_call_identity_verified {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "FrozenControllerExecutionEnvelope.native_per_call_identity_verified must be false.",
            ));
        }
        if self.binding_id != job.binding_id
            || self.thread_uuid != job.thread_uuid
            || self.job_id != job.job_id
            || self.work_brief_id != job.work_brief.brief_id
            || self.work_brief_digest != job.work_brief_digest
            || self.project_base != job.work_brief.project_base
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenControllerExecutionEnvelope scope or brief/project_base does not match job.",
            ));
        }

        let Some(ref linkage) = job.approval_linkage else {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "Job has no FrozenApprovalLinkage; controller execution is not authorized.",
            ));
        };
        let Some(ref cap_env) = linkage.controller_envelope else {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "Job FrozenApprovalLinkage has no FrozenControllerCapabilityEnvelope.",
            ));
        };
        let expected_cap_digest = compute_capability_envelope_digest(cap_env)?;
        if !constant_time_str_eq(&expected_cap_digest, &self.capability_envelope_digest) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenControllerExecutionEnvelope.capability_envelope_digest does not match job.",
            ));
        }

        let computed_policy_digest = self.policy.digest()?;
        if !constant_time_str_eq(&computed_policy_digest, &self.policy_digest) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenControllerExecutionEnvelope.policy_digest does not match FrozenControllerExecutionEnvelope.policy.",
            ));
        }
        cap_env.validate(Some(&self.policy))?;

        if self.approval_id != linkage.approval_id
            || self.approval_subject_digest != linkage.approval_subject_digest
            || self.plan_id != linkage.plan_id
            || self.plan_revision != linkage.plan_revision
            || self.plan_digest != linkage.plan_digest
            || self.profile_id != linkage.profile_id
            || self.registry_revision != linkage.registry_revision
            || self.policy.policy_id != linkage.policy_id
            || !constant_time_str_eq(&self.policy_digest, &linkage.policy_digest)
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenControllerExecutionEnvelope approval/plan/policy tuple drifted from job linkage.",
            ));
        }

        // Validate Reviewer and Closure choices against non-native headless rules, policy, and capability envelope.
        validate_non_native_worker_choice(WorkflowRole::Reviewer, &self.reviewer_choice)?;
        validate_non_native_worker_choice(WorkflowRole::Closure, &self.closure_choice)?;
        if !self.policy.allows(
            WorkflowRole::Reviewer,
            &self.reviewer_choice.tool_id,
            &self.reviewer_choice.model_id,
            self.reviewer_choice.effort.as_deref(),
        )? {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenControllerExecutionEnvelope.reviewer_choice is not permitted by the frozen ExecutionPolicy.",
            ));
        }
        if !self.policy.allows(
            WorkflowRole::Closure,
            &self.closure_choice.tool_id,
            &self.closure_choice.model_id,
            self.closure_choice.effort.as_deref(),
        )? {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenControllerExecutionEnvelope.closure_choice is not permitted by the frozen ExecutionPolicy.",
            ));
        }
        if !cap_env
            .allowed_reviewer_choices
            .contains(&self.reviewer_choice)
        {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "reviewer_choice is not in the job's FrozenControllerCapabilityEnvelope.allowed_reviewer_choices.",
            ));
        }
        if !cap_env
            .allowed_closure_choices
            .contains(&self.closure_choice)
        {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "closure_choice is not in the job's FrozenControllerCapabilityEnvelope.allowed_closure_choices.",
            ));
        }

        // Verify per-task coder choices match the frozen job tasks exactly.
        if self.task_coder_choices.len() != job.tasks.len() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenControllerExecutionEnvelope.task_coder_choices count does not match job.tasks.",
            ));
        }
        for task in &job.tasks {
            let Some(choice) = self.task_coder_choices.get(&task.task_id) else {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "FrozenControllerExecutionEnvelope is missing coder choice for task `{}`.",
                        task.task_id
                    ),
                ));
            };
            validate_non_native_worker_choice(WorkflowRole::Coder, choice)?;
            if choice != &task.selected_choice {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "FrozenControllerExecutionEnvelope coder choice for task `{}` drifted from job task.",
                        task.task_id
                    ),
                ));
            }
        }

        // Verify role budgets cover Coder, Reviewer, and Closure.
        for required_role in [
            WorkflowRole::Coder,
            WorkflowRole::Reviewer,
            WorkflowRole::Closure,
        ] {
            let Some(budget) = self.role_budgets.get(&required_role) else {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "FrozenControllerExecutionEnvelope.role_budgets is missing required role `{}`.",
                        required_role.as_str()
                    ),
                ));
            };
            if budget.role != required_role {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "FrozenControllerExecutionEnvelope.role_budgets key/role mismatch.",
                ));
            }
            budget.validate()?;
        }

        // Verify per-task and job-wide correction/attempt caps.
        if self.max_in_scope_corrections_per_task > MAX_IN_SCOPE_CORRECTION_ATTEMPTS
            || self.max_in_scope_corrections_per_task > cap_env.max_in_scope_correction_attempts
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "max_in_scope_corrections_per_task exceeds capability envelope or hard maximum.",
            ));
        }
        if self.max_total_corrections_per_job > MAX_TOTAL_CORRECTIONS_PER_JOB_CEILING {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "max_total_corrections_per_job exceeds hard ceiling.",
            ));
        }
        let min_attempts = job.tasks.len() as u32;
        if self.max_total_task_attempts_per_job < min_attempts
            || self.max_total_task_attempts_per_job > MAX_TOTAL_TASK_ATTEMPTS_PER_JOB_CEILING
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "max_total_task_attempts_per_job must be between job.tasks.len() and ceiling.",
            ));
        }

        // Verify frozen check command specifications against allowed_check_ids.
        if self.check_commands.is_empty() || self.check_commands.len() > MAX_CHECK_IDS_PER_ENVELOPE
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenControllerExecutionEnvelope.check_commands must be non-empty and within bounds.",
            ));
        }
        let allowed_ids: BTreeSet<&str> = cap_env
            .allowed_check_ids
            .iter()
            .map(String::as_str)
            .collect();
        let mut has_task_check = false;
        let mut has_closure_check = false;
        for (check_id, spec) in &self.check_commands {
            if check_id != &spec.check_id {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "FrozenControllerExecutionEnvelope.check_commands key mismatch.",
                ));
            }
            if !allowed_ids.contains(check_id.as_str()) {
                return Err(NativeError::new(
                    NativeErrorCode::UnauthorizedOperator,
                    format!(
                        "Check command `{check_id}` is not authorized by FrozenControllerCapabilityEnvelope.allowed_check_ids."
                    ),
                ));
            }
            spec.validate()?;
            if spec.required_for_task_review {
                has_task_check = true;
            }
            if spec.required_for_candidate_closure {
                has_closure_check = true;
            }
        }
        if !has_task_check || !has_closure_check {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenControllerExecutionEnvelope.check_commands must include at least one task-review check and one candidate-closure check.",
            ));
        }

        if self.allow_run_integration_execution && !cap_env.local_git_integration_allowed {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "allow_run_integration_execution requires local_git_integration_allowed = true in capability envelope.",
            ));
        }

        if let Some(ref receipt) = self.independence_completeness_receipt {
            receipt.validate_for_job(job)?;
        }
        if let Some(ref jev) = self.shadow_jev_policy {
            jev.validate()?;
        }

        let computed = self.compute_digest()?;
        if !constant_time_str_eq(&computed, &self.envelope_digest) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenControllerExecutionEnvelope.envelope_digest does not match canonical contents.",
            ));
        }
        Ok(())
    }

    /// Validates all structural, policy, model-choice, command-spec, budget, and digest invariants
    /// of this execution envelope against `job` and requires `operator_attested == true` for
    /// active/recorded execution authority.
    pub fn verify_matches_job(&self, job: &CoworkerJobRecord) -> Result<(), NativeError> {
        if !self.operator_attested {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "FrozenControllerExecutionEnvelope requires operator_attested = true for execution.",
            ));
        }
        self.validate_against_job(job)
    }

    /// Returns the frozen [`ControllerRoleExecutionBudget`] for `role`.
    pub fn budget_for_role(
        &self,
        role: WorkflowRole,
    ) -> Result<&ControllerRoleExecutionBudget, NativeError> {
        self.role_budgets.get(&role).ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "FrozenControllerExecutionEnvelope is missing role budget for `{}`.",
                    role.as_str()
                ),
            )
        })
    }

    /// Returns the frozen [`ControllerRoleExecutionBudget`] for [`WorkflowRole::Coder`].
    pub fn coder_budget(&self) -> Result<&ControllerRoleExecutionBudget, NativeError> {
        self.budget_for_role(WorkflowRole::Coder)
    }

    /// Returns the frozen [`ControllerRoleExecutionBudget`] for [`WorkflowRole::Reviewer`].
    pub fn reviewer_budget(&self) -> Result<&ControllerRoleExecutionBudget, NativeError> {
        self.budget_for_role(WorkflowRole::Reviewer)
    }

    /// Returns the frozen [`ControllerRoleExecutionBudget`] for [`WorkflowRole::Closure`].
    pub fn closure_budget(&self) -> Result<&ControllerRoleExecutionBudget, NativeError> {
        self.budget_for_role(WorkflowRole::Closure)
    }
}

/// Preview report of a [`FrozenControllerExecutionEnvelope`] for trusted-local CLI attestation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerConfigurationPreview {
    /// Deterministic envelope identifier (`env_<32hex>`).
    pub envelope_id: String,
    /// Canonical 64-hex SHA-256 digest of the full execution envelope.
    pub envelope_digest: String,
    /// Bound 64-hex `approval_subject_digest`.
    pub approval_subject_digest: String,
    /// Bound 64-hex `work_brief_digest`.
    pub work_brief_digest: String,
    /// Bound 64-hex `plan_digest`.
    pub plan_digest: String,
    /// Bound 64-hex `policy_digest`.
    pub policy_digest: String,
    /// Bound 64-hex `capability_envelope_digest`.
    pub capability_envelope_digest: String,
    /// Per-check command digests (`check_id -> 64-hex spec_digest`).
    pub check_spec_digests: BTreeMap<String, String>,
    /// Previewed [`FrozenControllerExecutionEnvelope`].
    pub envelope: FrozenControllerExecutionEnvelope,
}

/// Immutable pre-effect receipt authorizing a derived task execution attempt (`attempt_no >= 2`
/// or a task whose upstream predecessor completed via a derived attempt).
///
/// Preserves the original [`ApprovalRecord`] untouched and binds the prior rejected candidate,
/// structured [` super::contracts::StructuredReviewVerdict`] findings, unchanged write scope,
/// and unchanged [`SelectedRoleChoice`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DerivedTaskAttemptReceipt {
    /// Deterministic receipt identifier (`dar_<32hex>`).
    pub receipt_id: String,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Owning [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Original immutable [`ApprovalRecord::approval_id`].
    pub approval_id: String,
    /// Original 64-hex [`ApprovalRecord::subject_digest`].
    pub approval_subject_digest: String,
    /// Governing [`FrozenControllerExecutionEnvelope::envelope_id`].
    pub envelope_id: String,
    /// Governing 64-hex [`FrozenControllerExecutionEnvelope::envelope_digest`].
    pub envelope_digest: String,
    /// Target approved task identifier.
    pub task_id: String,
    /// Original 64-hex task specification digest.
    pub task_digest: String,
    /// 1-based attempt number (`>= 2` for in-scope corrections, or `>= 1` when derived from an upstream corrected predecessor).
    pub attempt_no: u32,
    /// Prior rejected task candidate identifier (`tcd_...`), when `attempt_no >= 2`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_rejected_candidate_id: Option<String>,
    /// Prior structured review verdict identifier (`rvd_...`), when `attempt_no >= 2`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_review_verdict_id: Option<String>,
    /// Prior 64-hex review subject digest, when `attempt_no >= 2`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_review_subject_digest: Option<String>,
    /// Exact required in-scope fixes from the prior [`super::contracts::StructuredReviewVerdict`].
    #[serde(default)]
    pub required_in_scope_fixes: Vec<String>,
    /// Original immutable task instruction from the frozen approval.
    pub frozen_instruction: String,
    /// Derived worker prompt combining `frozen_instruction` with structured reviewer findings.
    pub derived_prompt: String,
    /// Unchanged repository-relative write paths from the frozen approval.
    pub write_paths: Vec<String>,
    /// Unchanged [`SelectedRoleChoice`] from the frozen approval.
    pub coder_choice: SelectedRoleChoice,
    /// Exact 40-hex Git base commit from which the derived attempt worktree was created.
    pub execution_base_commit: String,
    /// Software-generated run identifier (`r...`) owning the derived worktree pair.
    pub run_id: String,
    /// Software-generated worker identifier (`w...`) owning the derived task worktree.
    pub worker_id: String,
    /// Owned `WorktreeKind::Task` worktree identifier (`wt-task-...`).
    pub task_worktree_id: String,
    /// Owned `WorktreeKind::RunIntegration` worktree identifier (`wt-run-...`).
    pub integration_worktree_id: String,
    /// Timestamp (ms since Unix epoch) when this receipt was persisted before effect.
    pub recorded_at_ms: u64,
    /// Canonical 64-character lowercase hex SHA-256 digest of this receipt.
    pub receipt_digest: String,
}

impl DerivedTaskAttemptReceipt {
    /// Computes the canonical 64-hex SHA-256 digest of this derived-attempt receipt.
    pub fn compute_digest(&self) -> Result<String, NativeError> {
        let seed = (
            (
                "threadlane-native:derived_task_attempt_receipt:v1",
                self.receipt_id.as_str(),
                self.binding_id.as_str(),
                self.thread_uuid.as_str(),
                self.job_id.as_str(),
                self.approval_id.as_str(),
                self.approval_subject_digest.as_str(),
                self.envelope_id.as_str(),
                self.envelope_digest.as_str(),
                self.task_id.as_str(),
                self.task_digest.as_str(),
                self.attempt_no,
                self.prior_rejected_candidate_id.as_deref(),
            ),
            (
                self.prior_review_verdict_id.as_deref(),
                self.prior_review_subject_digest.as_deref(),
                &self.required_in_scope_fixes,
                self.frozen_instruction.as_str(),
                self.derived_prompt.as_str(),
                &self.write_paths,
                &self.coder_choice,
                self.execution_base_commit.as_str(),
                self.run_id.as_str(),
                self.worker_id.as_str(),
                self.task_worktree_id.as_str(),
                self.integration_worktree_id.as_str(),
                self.recorded_at_ms,
            ),
        );
        let bytes = serde_json::to_vec(&seed).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "Failed to serialize DerivedTaskAttemptReceipt for digest calculation.",
            )
        })?;
        Ok(sha256_hex(&bytes))
    }

    /// Validates receipt integrity and ensures write scopes and coder model choice match the frozen task.
    pub fn validate_against_task(
        &self,
        task: &CoworkerTaskRecord,
        envelope: &FrozenControllerExecutionEnvelope,
    ) -> Result<(), NativeError> {
        validate_bounded_identifier(
            &self.receipt_id,
            "derived_receipt.receipt_id",
            MAX_COWORKER_ID_LEN,
        )?;
        if self.binding_id != envelope.binding_id
            || self.thread_uuid != envelope.thread_uuid
            || self.job_id != envelope.job_id
            || self.approval_id != envelope.approval_id
            || self.approval_subject_digest != envelope.approval_subject_digest
            || self.envelope_id != envelope.envelope_id
            || self.envelope_digest != envelope.envelope_digest
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "DerivedTaskAttemptReceipt envelope/approval linkage mismatch.",
            ));
        }
        if self.task_id != task.task_id
            || self.task_digest != task.task_digest
            || self.write_paths != task.write_paths
            || self.coder_choice != task.selected_choice
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "DerivedTaskAttemptReceipt cannot alter frozen task_id, task_digest, write_paths, or coder_choice.",
            ));
        }
        if self.attempt_no == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "DerivedTaskAttemptReceipt.attempt_no must be >= 1.",
            ));
        }
        if self.attempt_no >= 2
            && (self.prior_rejected_candidate_id.is_none()
                || self.prior_review_verdict_id.is_none()
                || self.required_in_scope_fixes.is_empty())
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "In-scope correction attempt (attempt_no >= 2) requires prior rejected candidate, review verdict, and non-empty required_in_scope_fixes.",
            ));
        }
        validate_exact_40_hex_sha(&self.execution_base_commit, "execution_base_commit").map_err(
            |_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "DerivedTaskAttemptReceipt.execution_base_commit must be a 40-hex commit SHA.",
                )
            },
        )?;
        let computed = self.compute_digest()?;
        if !constant_time_str_eq(&computed, &self.receipt_digest) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "DerivedTaskAttemptReceipt.receipt_digest mismatch.",
            ));
        }
        Ok(())
    }
}

/// Logical recoverable per-job driver lease record keyed by `instance_id` and `lease_epoch`.
///
/// Note: Because a deterministic `instance_id` can be renewed by a caller supplying the same
/// identifier, this metadata record by itself is not proof of a unique OS process. Physical
/// single-driver process ownership and effect fencing belong to the runtime driver/engine layer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerJobLeaseRecord {
    /// Bound [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Recoverable driver instance identifier (`ctl_<32hex>`).
    pub instance_id: String,
    /// Monotonically increasing 1-based lease epoch.
    pub lease_epoch: u64,
    /// Timestamp (ms since Unix epoch) when the lease was acquired or renewed.
    pub acquired_at_ms: u64,
    /// Timestamp (ms since Unix epoch) when the lease expires if not renewed.
    pub expires_at_ms: u64,
    /// Whether the lease is currently active.
    pub active: bool,
}

impl ControllerJobLeaseRecord {
    /// Returns `true` if the lease is active and has not expired at `now_ms`.
    pub const fn is_active_at(&self, now_ms: u64) -> bool {
        self.active && now_ms < self.expires_at_ms
    }
}

/// Kind of external controller effect tracked in the intent-before-effect outbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingControllerEffectKind {
    /// Initial or derived coder worker dispatch.
    CoderDispatch,
    /// Task worktree Git commit of in-scope coder changes.
    TaskCommit,
    /// Frozen check command execution on a task or integration worktree.
    CheckExecution,
    /// Structured read-only Reviewer worker invocation.
    ReviewerInvocation,
    /// Task worktree integration into an owned `RunIntegration` worktree.
    TaskIntegration,
    /// Structured read-only Closure worker invocation.
    ClosureInvocation,
}

impl PendingControllerEffectKind {
    /// Returns the canonical `snake_case` wire label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CoderDispatch => "coder_dispatch",
            Self::TaskCommit => "task_commit",
            Self::CheckExecution => "check_execution",
            Self::ReviewerInvocation => "reviewer_invocation",
            Self::TaskIntegration => "task_integration",
            Self::ClosureInvocation => "closure_invocation",
        }
    }
}

/// Lifecycle state of a [`PendingControllerEffect`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingControllerEffectStatus {
    /// Intent persisted before crossing the external effect boundary.
    ReservedBeforeEffect,
    /// Effect confirmed completed with verified durable artifacts/receipts.
    EffectConfirmed,
    /// Effect completed locally with a known terminal worker or validation failure (budget consumed, no accepted verdict).
    EffectFailedTerminal,
    /// Effect refused before any external execution occurred.
    RefusedBeforeEffect,
    /// Outcome is unknown after crossing the effect boundary; blocks blind replay until reconciled.
    EffectUnknown,
}

impl PendingControllerEffectStatus {
    /// Returns `true` if this effect is unresolved (`ReservedBeforeEffect` or `EffectUnknown`) and blocks replay.
    pub const fn blocks_replay(self) -> bool {
        matches!(self, Self::ReservedBeforeEffect | Self::EffectUnknown)
    }
}

/// Full bounded closure worker execution artifact retained in private controller runtime state
/// for crash recovery, audit, and fenced late-output preservation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClosureExecutionArtifactRecord {
    /// Closure attempt identifier (`cat_...`).
    pub attempt_id: String,
    /// Idempotency command key bound to the closure invocation.
    pub command_key: String,
    /// Bound 64-hex [`FinalCandidateSubject::subject_digest`].
    pub subject_digest: String,
    /// Bound 40-hex [`FinalCandidateSubject::candidate_tree_sha`].
    pub candidate_tree_sha: String,
    /// Full bounded supervisor execution report ([`threadlane_workers::WorkerExecutionReport`]).
    pub execution_report: threadlane_workers::WorkerExecutionReport,
    /// Full bounded advisory response payload and SHA-256 digest when captured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advisory_response: Option<threadlane_workers::BoundedAdvisoryResponse>,
    /// Observed model identifier from verified stream telemetry, if present (never fabricated).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_model: Option<String>,
    /// Observed reasoning effort from verified stream telemetry, if present (never fabricated).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_effort: Option<String>,
    /// Whether valid late worker output was fenced from promotion by an active hold, stop, revision drift, or subject drift.
    #[serde(default)]
    pub fenced_from_promotion: bool,
    /// Sanitized diagnostic when `fenced_from_promotion == true` or when validation failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
    /// Timestamp (ms since Unix epoch) when this artifact was persisted.
    pub recorded_at_ms: u64,
}

/// Durable intent-before-effect record tracked per job so a restart or reopen inspects pending
/// effects before making any external call and never replays ambiguous work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingControllerEffect {
    /// Deterministic effect identifier (`eff_<32hex>`).
    pub effect_id: String,
    /// Caller/controller idempotency key.
    pub request_key: String,
    /// Owning job identifier.
    pub job_id: String,
    /// Associated task identifier (`None` for job-level candidate checks or closure).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// Associated 1-based task attempt number, if task-scoped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_no: Option<u32>,
    /// Effect category.
    pub kind: PendingControllerEffectKind,
    /// Current durable status.
    pub status: PendingControllerEffectStatus,
    /// 64-character lowercase hex SHA-256 subject digest bound to the effect.
    pub subject_digest: String,
    /// Optional external run, attempt, or receipt correlation identifier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_correlation_id: Option<String>,
    /// Creation timestamp (ms since Unix epoch).
    pub created_at_ms: u64,
    /// Last update timestamp (ms since Unix epoch).
    pub updated_at_ms: u64,
    /// Sanitized diagnostic when `status == EffectUnknown` or `RefusedBeforeEffect`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
}

/// Verified worktree, commit, check, review, and integration artifacts recorded for a specific
/// `(task_id, attempt_no)` execution attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAttemptExecutionArtifacts {
    /// Approved task identifier.
    pub task_id: String,
    /// 1-based attempt number.
    pub attempt_no: u32,
    /// Runtime or derived `run_id`.
    pub run_id: String,
    /// Runtime or derived `worker_id`.
    pub worker_id: String,
    /// Owned `WorktreeKind::Task` worktree identifier.
    pub task_worktree_id: String,
    /// Owned `WorktreeKind::RunIntegration` worktree identifier.
    pub integration_worktree_id: String,
    /// Execution base commit SHA (40-hex).
    pub execution_base_commit: String,
    /// Observed provider identifier from verified worker stream telemetry.
    pub observed_provider: String,
    /// Observed model identifier from verified worker stream telemetry.
    pub observed_model: String,
    /// Observed reasoning effort from verified worker stream telemetry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_effort: Option<String>,
    /// Committed task `HEAD` commit SHA (40-hex) after diff-gate verification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_head_commit: Option<String>,
    /// Committed task Git tree SHA (40-hex).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_tree_sha: Option<String>,
    /// Deterministic candidate identifier (`tcd_<32hex>`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_id: Option<String>,
    /// Executed frozen check evidence on the committed task worktree.
    #[serde(default)]
    pub check_evidence: Vec<CandidateCheckEvidence>,
    /// Aggregated test/check evidence summary bound into [`ReviewSubject`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub test_evidence_summary: Option<TestEvidenceSummary>,
    /// Inspected [`ReviewSubject`] for this attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_subject: Option<ReviewSubject>,
    /// 64-hex SHA-256 digest of the structured Reviewer [`threadlane_workers::BoundedAdvisoryResponse`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewer_response_digest: Option<String>,
    /// Issued [`ReviewReceipt`] when Reviewer verdict is `Accept`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_receipt: Option<ReviewReceipt>,
    /// Issued [`IntegrationReceipt`] after integrating the accepted task worktree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integration_receipt: Option<IntegrationReceipt>,
    /// Whether this attempt was executed via a [`DerivedTaskAttemptReceipt`].
    pub is_derived_attempt: bool,
}

/// Complete durable controller runtime state for a single [`CoworkerJobRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerJobRuntimeState {
    /// Bound [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Monotonically increasing 1-based revision of this controller runtime state.
    pub runtime_revision: u64,
    /// Frozen operator-attested controller execution envelope.
    pub envelope: FrozenControllerExecutionEnvelope,
    /// Current or most recent driver lease record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<ControllerJobLeaseRecord>,
    /// Per-role call and runtime usage counters.
    pub role_usage: BTreeMap<WorkflowRole, ControllerRoleUsageCounters>,
    /// Total task execution attempts (initial + derived) reserved across the job.
    pub total_task_attempts_reserved: u32,
    /// Total in-scope correction attempts reserved across the job.
    pub total_corrections_reserved: u32,
    /// Immutable derived-attempt receipts recorded for this job.
    #[serde(default)]
    pub derived_attempts: Vec<DerivedTaskAttemptReceipt>,
    /// Execution artifacts keyed by `"{task_id}#{attempt_no}"`.
    #[serde(default)]
    pub attempt_artifacts: BTreeMap<String, TaskAttemptExecutionArtifacts>,
    /// Intent-before-effect records keyed by `effect_id`.
    #[serde(default)]
    pub pending_effects: BTreeMap<String, PendingControllerEffect>,
    /// Candidate-level frozen check evidence recorded for the combined `RunIntegration` candidate.
    #[serde(default)]
    pub candidate_check_evidence: Vec<CandidateCheckEvidence>,
    /// Verified [`FinalCandidateSubject`] inspected from `threadlane-operations`, if assembled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_candidate_subject: Option<FinalCandidateSubject>,
    /// Latest closure attempt identifier (`cat_...`) in [` threadlane_operations::FinalCandidateClosureStore`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closure_attempt_id: Option<String>,
    /// Latest closure verdict identifier (`cvd_...`) when terminal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closure_verdict_id: Option<String>,
    /// Retained private closure worker execution artifacts keyed by `attempt_id`.
    #[serde(default)]
    pub closure_artifacts: BTreeMap<String, ClosureExecutionArtifactRecord>,
    /// Shadow-only Jev observations recorded under explicit trigger/budget authority.
    #[serde(default)]
    pub shadow_jev_observations: Vec<ShadowJevObservationRecord>,
    /// Last conversation-change store revision synchronized by the controller.
    pub synced_change_revision: u64,
    /// Whether an explicit operator stop/cancel has been requested for this job's controller loop.
    pub stop_requested: bool,
    /// Optional reason recorded when `stop_requested` is `true` or when blocked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_or_block_reason: Option<String>,
}

impl ControllerJobRuntimeState {
    /// Returns the map key `"{task_id}#{attempt_no}"` used for `attempt_artifacts`.
    pub fn artifact_key(task_id: &str, attempt_no: u32) -> String {
        format!("{task_id}#{attempt_no}")
    }

    /// Looks up the execution artifacts for `(task_id, attempt_no)`.
    pub fn get_attempt_artifacts(
        &self,
        task_id: &str,
        attempt_no: u32,
    ) -> Option<&TaskAttemptExecutionArtifacts> {
        self.attempt_artifacts
            .get(&Self::artifact_key(task_id, attempt_no))
    }

    /// Returns a mutable reference to the execution artifacts for `(task_id, attempt_no)`.
    pub fn get_attempt_artifacts_mut(
        &mut self,
        task_id: &str,
        attempt_no: u32,
    ) -> Option<&mut TaskAttemptExecutionArtifacts> {
        self.attempt_artifacts
            .get_mut(&Self::artifact_key(task_id, attempt_no))
    }

    /// Returns all unresolved [`PendingControllerEffect`] records that block blind replay.
    pub fn unresolved_effects(&self) -> Vec<&PendingControllerEffect> {
        self.pending_effects
            .values()
            .filter(|eff| eff.status.blocks_replay())
            .collect()
    }

    /// Verifies that `role` has remaining call and cumulative runtime budget before reserving a new invocation.
    pub fn ensure_role_budget_available(&self, role: WorkflowRole) -> Result<(), NativeError> {
        let budget = self.envelope.budget_for_role(role)?;
        let usage = self.role_usage.get(&role).cloned().unwrap_or_default();
        if usage.calls_reserved >= budget.max_total_calls {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "Role `{}` call budget exhausted ({} of {} calls reserved).",
                    role.as_str(),
                    usage.calls_reserved,
                    budget.max_total_calls
                ),
            ));
        }
        if usage.total_runtime_ms >= budget.max_total_runtime_ms {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "Role `{}` cumulative runtime budget exhausted ({} ms of {} ms used).",
                    role.as_str(),
                    usage.total_runtime_ms,
                    budget.max_total_runtime_ms
                ),
            ));
        }
        Ok(())
    }

    /// Reserves a call for `role` before crossing the external worker invocation boundary.
    pub fn reserve_role_call(&mut self, role: WorkflowRole) -> Result<u32, NativeError> {
        self.ensure_role_budget_available(role)?;
        let entry = self.role_usage.entry(role).or_default();
        entry.calls_reserved = entry.calls_reserved.saturating_add(1);
        Ok(entry.calls_reserved)
    }

    /// Records completion and observed wall-clock duration (`duration_ms`) for `role`.
    pub fn record_role_completion(&mut self, role: WorkflowRole, duration_ms: u64) {
        let entry = self.role_usage.entry(role).or_default();
        entry.calls_completed = entry.calls_completed.saturating_add(1);
        entry.total_runtime_ms = entry.total_runtime_ms.saturating_add(duration_ms);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedControllerBindingLedger {
    schema_version: u32,
    binding_id: String,
    ledger_revision: u64,
    jobs: BTreeMap<String, ControllerJobRuntimeState>,
}

/// Durable sidecar store for [`FrozenControllerExecutionEnvelope`] and [`ControllerJobRuntimeState`]
/// inside `plans_dir` (`coworker-controller-<binding_tag>.json`, `0600`).
#[derive(Debug, Clone)]
pub struct ControllerPolicyStore {
    plans_dir: PathBuf,
}

impl ControllerPolicyStore {
    /// Opens a [`ControllerPolicyStore`] handle over an existing private (`0700`) `plans_dir`
    /// without creating any files or locks.
    pub fn new(plans_dir: impl AsRef<Path>) -> Result<Self, NativeError> {
        ensure_platform_supported()?;
        let clean = validate_existing_private_dir(plans_dir.as_ref(), "plans_dir")?;
        Ok(Self { plans_dir: clean })
    }

    /// Returns the validated `plans_dir` path.
    pub fn plans_dir(&self) -> &Path {
        &self.plans_dir
    }

    /// Returns the deterministic private ledger path for `binding_id`.
    pub fn ledger_path_for_binding(&self, binding_id: &str) -> Result<PathBuf, NativeError> {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        Ok(self.plans_dir.join(controller_ledger_filename(binding_id)))
    }

    /// Returns the deterministic lockfile path for `binding_id`.
    pub fn lock_path_for_binding(&self, binding_id: &str) -> Result<PathBuf, NativeError> {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        Ok(self.plans_dir.join(controller_lock_filename(binding_id)))
    }

    /// Pure read-only lookup of a job's [`ControllerJobRuntimeState`] without creating files or locks.
    pub fn get_job_runtime_readonly(
        &self,
        binding_id: &str,
        job_id: &str,
    ) -> Result<Option<ControllerJobRuntimeState>, NativeError> {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        let path = self.ledger_path_for_binding(binding_id)?;
        let Some(ledger) = self.read_ledger_unlocked(binding_id, &path)? else {
            return Ok(None);
        };
        Ok(ledger.jobs.get(job_id).cloned())
    }

    /// Alias for [`Self::get_job_runtime_readonly`] returning `Option<ControllerJobRuntimeState>`.
    pub fn read_job_runtime(
        &self,
        binding_id: &str,
        job_id: &str,
    ) -> Result<Option<ControllerJobRuntimeState>, NativeError> {
        self.get_job_runtime_readonly(binding_id, job_id)
    }

    /// Builds, validates, and previews a [`FrozenControllerExecutionEnvelope`] against the live
    /// [`CoworkerJobRecord`], [`AuthorityStore`], [`threadlane_workflow::PlanStore`], and
    /// [`threadlane_workflow::RegistryStore`] without mutating disk state.
    pub fn preview_execution_envelope(
        &self,
        request: &FreezeControllerExecutionEnvelopeRequest,
        job: &CoworkerJobRecord,
        authority: &AuthorityStore,
    ) -> Result<ControllerConfigurationPreview, NativeError> {
        if job.job_revision != request.expected_job_revision {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "expected_job_revision does not match current CoworkerJobRecord.job_revision.",
            ));
        }
        let Some(ref linkage) = job.approval_linkage else {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "Cannot freeze controller execution envelope before binding an ApprovalRecord to the job.",
            ));
        };
        let Some(ref cap_env) = linkage.controller_envelope else {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "Job approval linkage does not include a FrozenControllerCapabilityEnvelope.",
            ));
        };

        // Cross-verify live ApprovalRecord, active published PlanRevision, and RegistryStore revision.
        let approval = authority
            .get_for_operator(&linkage.approval_id)
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Referenced approval_id was not found or failed integrity check in AuthorityStore.",
                )
            })?;
        verify_live_approval_and_registry(&approval, job, &request.policy, authority)?;

        let mut task_coder_choices = BTreeMap::new();
        for task in &job.tasks {
            task_coder_choices.insert(task.task_id.clone(), task.selected_choice.clone());
        }

        let capability_envelope_digest = compute_capability_envelope_digest(cap_env)?;
        let mut check_spec_digests = BTreeMap::new();
        for (cid, spec) in &request.check_commands {
            check_spec_digests.insert(cid.clone(), spec.spec_digest()?);
        }

        let envelope_id = deterministic_prefixed_id(
            "threadlane-native:controller-execution-envelope:v1",
            "env",
            &[
                &request.binding_id,
                &request.job_id,
                &linkage.approval_id,
                &linkage.approval_subject_digest,
                &linkage.policy_digest,
                &capability_envelope_digest,
            ],
        );

        let mut envelope = FrozenControllerExecutionEnvelope {
            schema_version: CONTROLLER_POLICY_SCHEMA_VERSION,
            envelope_id: envelope_id.clone(),
            binding_id: request.binding_id.clone(),
            thread_uuid: request.thread_uuid.clone(),
            job_id: request.job_id.clone(),
            work_brief_id: job.work_brief.brief_id.clone(),
            work_brief_digest: job.work_brief_digest.clone(),
            approval_id: linkage.approval_id.clone(),
            approval_subject_digest: linkage.approval_subject_digest.clone(),
            plan_id: linkage.plan_id.clone(),
            plan_revision: linkage.plan_revision,
            plan_digest: linkage.plan_digest.clone(),
            profile_id: linkage.profile_id.clone(),
            registry_revision: linkage.registry_revision,
            project_base: job.work_brief.project_base.clone(),
            policy: request.policy.clone(),
            policy_digest: linkage.policy_digest.clone(),
            task_coder_choices,
            reviewer_choice: request.reviewer_choice.clone(),
            closure_choice: request.closure_choice.clone(),
            role_budgets: request.role_budgets.clone(),
            max_in_scope_corrections_per_task: request.max_in_scope_corrections_per_task,
            max_total_corrections_per_job: request.max_total_corrections_per_job,
            max_total_task_attempts_per_job: request.max_total_task_attempts_per_job,
            check_commands: request.check_commands.clone(),
            allow_task_worktree_commits: request.allow_task_worktree_commits,
            allow_run_integration_execution: request.allow_run_integration_execution,
            default_integration_strategy: request.default_integration_strategy,
            independence_completeness_receipt: request.independence_completeness_receipt.clone(),
            shadow_jev_policy: request.shadow_jev_policy.clone(),
            created_at_ms: request.created_at_ms,
            operator_attested: request.operator_attested,
            native_per_call_identity_verified: false,
            capability_envelope_digest: capability_envelope_digest.clone(),
            envelope_digest: String::new(),
        };
        envelope.envelope_digest = envelope.compute_digest()?;
        envelope.validate_against_job(job)?;

        Ok(ControllerConfigurationPreview {
            envelope_id,
            envelope_digest: envelope.envelope_digest.clone(),
            approval_subject_digest: envelope.approval_subject_digest.clone(),
            work_brief_digest: envelope.work_brief_digest.clone(),
            plan_digest: envelope.plan_digest.clone(),
            policy_digest: envelope.policy_digest.clone(),
            capability_envelope_digest,
            check_spec_digests,
            envelope,
        })
    }

    /// Validates and durably records an operator-attested [`FrozenControllerExecutionEnvelope`]
    /// for `job`, initializing [`ControllerJobRuntimeState`].
    ///
    /// Requires `request.operator_attested == true`, `request.confirm_approval_subject_digest`
    /// matching the job's frozen `approval_subject_digest`, and `request.confirm_envelope_digest`
    /// matching the recomputed full `envelope_digest`. Once recorded, an envelope is immutable
    /// for that job unless an identical idempotent request is re-submitted.
    pub fn record_execution_envelope(
        &self,
        request: &FreezeControllerExecutionEnvelopeRequest,
        job: &CoworkerJobRecord,
        authority: &AuthorityStore,
    ) -> Result<ControllerJobRuntimeState, NativeError> {
        if !request.operator_attested {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "Recording a FrozenControllerExecutionEnvelope requires explicit operator_attested = true.",
            ));
        }
        let Some(ref confirm_digest) = request.confirm_approval_subject_digest else {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "Recording a FrozenControllerExecutionEnvelope requires confirm_approval_subject_digest.",
            ));
        };
        let Some(ref confirm_envelope_digest) = request.confirm_envelope_digest else {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "Recording a FrozenControllerExecutionEnvelope requires confirm_envelope_digest matching the full previewed envelope_digest.",
            ));
        };
        let preview = self.preview_execution_envelope(request, job, authority)?;
        if !constant_time_str_eq(confirm_digest, &preview.approval_subject_digest) {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "confirm_approval_subject_digest does not match the job's frozen approval_subject_digest.",
            ));
        }
        let recomputed_envelope_digest = preview.envelope.compute_digest()?;
        if !constant_time_str_eq(&recomputed_envelope_digest, &preview.envelope_digest)
            || !constant_time_str_eq(confirm_envelope_digest, &recomputed_envelope_digest)
        {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "confirm_envelope_digest does not match the recomputed FrozenControllerExecutionEnvelope.envelope_digest.",
            ));
        }
        preview.envelope.verify_matches_job(job)?;

        self.transact_binding(&request.binding_id, |ledger| {
            if let Some(existing) = ledger.jobs.get(&job.job_id) {
                existing.envelope.verify_matches_job(job)?;
                if existing.envelope == preview.envelope {
                    return Ok(existing.clone());
                }
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "A different FrozenControllerExecutionEnvelope is already recorded for this job; controller envelopes are immutable.",
                ));
            }

            let mut role_usage = BTreeMap::new();
            role_usage.insert(WorkflowRole::Coder, ControllerRoleUsageCounters::default());
            role_usage.insert(
                WorkflowRole::Reviewer,
                ControllerRoleUsageCounters::default(),
            );
            role_usage.insert(
                WorkflowRole::Closure,
                ControllerRoleUsageCounters::default(),
            );

            let state = ControllerJobRuntimeState {
                job_id: job.job_id.clone(),
                runtime_revision: 1,
                envelope: preview.envelope,
                lease: None,
                role_usage,
                total_task_attempts_reserved: 0,
                total_corrections_reserved: 0,
                derived_attempts: Vec::new(),
                attempt_artifacts: BTreeMap::new(),
                pending_effects: BTreeMap::new(),
                candidate_check_evidence: Vec::new(),
                final_candidate_subject: None,
                closure_attempt_id: None,
                closure_verdict_id: None,
                closure_artifacts: BTreeMap::new(),
                shadow_jev_observations: Vec::new(),
                synced_change_revision: 0,
                stop_requested: false,
                stop_or_block_reason: None,
            };
            ledger.jobs.insert(job.job_id.clone(), state.clone());
            Ok(state)
        })
    }

    /// Acquires or renews the logical per-job lease for `(binding_id, job_id)` with recoverable
    /// `instance_id` (`ctl_...`). Refuses active leases held by a different `instance_id` until
    /// released or expired; same-`instance_id` renewal is logical metadata coordination and does
    /// not by itself prove physical single-process uniqueness.
    pub fn acquire_or_renew_lease(
        &self,
        binding_id: &str,
        job_id: &str,
        instance_id: &str,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<ControllerJobLeaseRecord, NativeError> {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(instance_id, "instance_id", MAX_COWORKER_ID_LEN)?;
        if !instance_id.starts_with("ctl_") {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Controller driver instance_id must start with `ctl_`.",
            ));
        }
        let clamped_ttl = ttl_ms.clamp(1, MAX_CONTROLLER_LEASE_TTL_MS);
        let expires_at_ms = now_ms.saturating_add(clamped_ttl);

        self.transact_binding(binding_id, |ledger| {
            let state = ledger.jobs.get_mut(job_id).ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::NotConfigured,
                    "No FrozenControllerExecutionEnvelope is recorded for this job.",
                )
            })?;

            let next_lease = match state.lease {
                Some(ref current) if current.is_active_at(now_ms) => {
                    if current.instance_id != instance_id {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            format!(
                                "Job `{job_id}` is already leased by active controller driver `{}` (epoch {}).",
                                current.instance_id, current.lease_epoch
                            ),
                        ));
                    }
                    ControllerJobLeaseRecord {
                        job_id: job_id.to_string(),
                        binding_id: binding_id.to_string(),
                        instance_id: instance_id.to_string(),
                        lease_epoch: current.lease_epoch,
                        acquired_at_ms: now_ms,
                        expires_at_ms,
                        active: true,
                    }
                }
                Some(ref expired_or_released) => ControllerJobLeaseRecord {
                    job_id: job_id.to_string(),
                    binding_id: binding_id.to_string(),
                    instance_id: instance_id.to_string(),
                    lease_epoch: expired_or_released.lease_epoch.saturating_add(1),
                    acquired_at_ms: now_ms,
                    expires_at_ms,
                    active: true,
                },
                None => ControllerJobLeaseRecord {
                    job_id: job_id.to_string(),
                    binding_id: binding_id.to_string(),
                    instance_id: instance_id.to_string(),
                    lease_epoch: 1,
                    acquired_at_ms: now_ms,
                    expires_at_ms,
                    active: true,
                },
            };

            state.lease = Some(next_lease.clone());
            state.runtime_revision = state.runtime_revision.saturating_add(1);
            Ok(next_lease)
        })
    }

    /// Explicitly releases an active driver lease held by `(instance_id, expected_lease_epoch)`.
    pub fn release_lease(
        &self,
        binding_id: &str,
        job_id: &str,
        instance_id: &str,
        expected_lease_epoch: u64,
    ) -> Result<ControllerJobLeaseRecord, NativeError> {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(instance_id, "instance_id", MAX_COWORKER_ID_LEN)?;

        self.transact_binding(binding_id, |ledger| {
            let state = ledger.jobs.get_mut(job_id).ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::NotConfigured,
                    "No controller runtime state exists for this job.",
                )
            })?;
            let Some(ref mut lease) = state.lease else {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Job has no controller lease to release.",
                ));
            };
            if lease.instance_id != instance_id || lease.lease_epoch != expected_lease_epoch {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Lease instance_id or expected_lease_epoch does not match current lease owner.",
                ));
            }
            lease.active = false;
            let released = lease.clone();
            state.runtime_revision = state.runtime_revision.saturating_add(1);
            Ok(released)
        })
    }

    /// Executes an atomic read-modify-write mutation on `job_id`'s [`ControllerJobRuntimeState`]
    /// while verifying active lease ownership (`instance_id`, `expected_lease_epoch`).
    pub fn mutate_job_runtime<T, F>(
        &self,
        binding_id: &str,
        job_id: &str,
        instance_id: &str,
        expected_lease_epoch: u64,
        mutate_fn: F,
    ) -> Result<T, NativeError>
    where
        F: FnOnce(&mut ControllerJobRuntimeState) -> Result<T, NativeError>,
    {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(instance_id, "instance_id", MAX_COWORKER_ID_LEN)?;

        self.transact_binding(binding_id, |ledger| {
            let state = ledger.jobs.get_mut(job_id).ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::NotConfigured,
                    "No FrozenControllerExecutionEnvelope is recorded for this job.",
                )
            })?;
            let Some(ref lease) = state.lease else {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Controller must hold an active job lease before mutating runtime state.",
                ));
            };
            if !lease.active
                || lease.instance_id != instance_id
                || lease.lease_epoch != expected_lease_epoch
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Controller lease ownership mismatch or lease is no longer active.",
                ));
            }
            let out = mutate_fn(state)?;
            state.runtime_revision = state.runtime_revision.saturating_add(1);
            Ok(out)
        })
    }

    /// Executes an operator-initiated mutation (such as stop/cancel request or reconciliation)
    /// on `job_id`'s [`ControllerJobRuntimeState`].
    pub fn mutate_job_runtime_for_operator<T, F>(
        &self,
        binding_id: &str,
        job_id: &str,
        mutate_fn: F,
    ) -> Result<T, NativeError>
    where
        F: FnOnce(&mut ControllerJobRuntimeState) -> Result<T, NativeError>,
    {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        self.transact_binding(binding_id, |ledger| {
            let state = ledger.jobs.get_mut(job_id).ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::NotConfigured,
                    "No FrozenControllerExecutionEnvelope is recorded for this job.",
                )
            })?;
            let out = mutate_fn(state)?;
            state.runtime_revision = state.runtime_revision.saturating_add(1);
            Ok(out)
        })
    }

    fn transact_binding<T, F>(&self, binding_id: &str, f: F) -> Result<T, NativeError>
    where
        F: FnOnce(&mut PersistedControllerBindingLedger) -> Result<T, NativeError>,
    {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        let _ = validate_existing_private_dir(&self.plans_dir, "plans_dir")?;
        let lock_name = controller_lock_filename(binding_id);
        let _guard =
            acquire_cross_process_lock(&self.plans_dir, &lock_name, "controller_policy_lock")?;
        let ledger_path = self.ledger_path_for_binding(binding_id)?;
        let mut ledger = self
            .read_ledger_unlocked(binding_id, &ledger_path)?
            .unwrap_or_else(|| PersistedControllerBindingLedger {
                schema_version: CONTROLLER_POLICY_SCHEMA_VERSION,
                binding_id: binding_id.to_string(),
                ledger_revision: 0,
                jobs: BTreeMap::new(),
            });
        let out = f(&mut ledger)?;
        ledger.ledger_revision = ledger.ledger_revision.saturating_add(1);
        self.write_ledger_unlocked(binding_id, &ledger)?;
        Ok(out)
    }

    fn read_ledger_unlocked(
        &self,
        expected_binding_id: &str,
        path: &Path,
    ) -> Result<Option<PersistedControllerBindingLedger>, NativeError> {
        let Some(bytes) = read_private_file_bounded(
            path,
            MAX_PRIVATE_STATE_FILE_BYTES,
            "controller_policy_ledger",
        )?
        else {
            return Ok(None);
        };
        let ledger: PersistedControllerBindingLedger =
            serde_json::from_slice(&bytes).map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "Failed to parse controller policy ledger JSON.",
                )
            })?;
        if ledger.schema_version != CONTROLLER_POLICY_SCHEMA_VERSION
            || ledger.binding_id != expected_binding_id
        {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "Controller policy ledger schema_version or binding_id mismatch.",
            ));
        }
        Ok(Some(ledger))
    }

    fn write_ledger_unlocked(
        &self,
        binding_id: &str,
        ledger: &PersistedControllerBindingLedger,
    ) -> Result<(), NativeError> {
        let bytes = serde_json::to_vec_pretty(ledger).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "Failed to serialize controller policy ledger JSON.",
            )
        })?;
        if bytes.len() > MAX_PRIVATE_STATE_FILE_BYTES {
            return Err(NativeError::new(
                NativeErrorCode::OutputOverflow,
                "Controller policy ledger exceeds maximum private file byte budget.",
            ));
        }
        let filename = controller_ledger_filename(binding_id);
        write_private_file_atomic(
            &self.plans_dir,
            &filename,
            &bytes,
            "controller_policy_ledger",
        )
        .map(|_| ())
    }
}

/// Verifies that the live [`ApprovalRecord`], active published plan revision in [`threadlane_workflow::PlanStore`],
/// [`ExecutionPolicy`], and [`threadlane_workflow::RegistryStore`] snapshot still match `job.approval_linkage` without drift.
pub fn verify_live_approval_and_registry(
    approval: &ApprovalRecord,
    job: &CoworkerJobRecord,
    policy: &ExecutionPolicy,
    authority: &AuthorityStore,
) -> Result<(), NativeError> {
    let Some(ref linkage) = job.approval_linkage else {
        return Err(NativeError::new(
            NativeErrorCode::UnauthorizedOperator,
            "Job has no FrozenApprovalLinkage.",
        ));
    };
    let computed_policy_digest = policy.digest()?;
    if policy.policy_id != linkage.policy_id
        || !constant_time_str_eq(&computed_policy_digest, &linkage.policy_digest)
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ExecutionPolicy does not match the job's FrozenApprovalLinkage policy_id or policy_digest.",
        ));
    }
    if approval.approval_id != linkage.approval_id
        || approval.subject_digest != linkage.approval_subject_digest
        || approval.binding_id != job.binding_id
        || approval.thread_uuid != job.thread_uuid
        || approval.plan_id != linkage.plan_id
        || approval.plan_revision != linkage.plan_revision
        || approval.plan_digest != linkage.plan_digest
        || approval.profile_id != linkage.profile_id
        || approval.registry_revision != linkage.registry_revision
        || approval.repository_path != job.work_brief.project_base.repository_path
        || approval.base_commit != job.work_brief.project_base.base_commit
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ApprovalRecord metadata drifted from the job's FrozenApprovalLinkage.",
        ));
    }

    job.work_brief.project_base.verify_live_repository()?;

    let plan_rev = authority
        .plans()
        .get_revision(&approval.plan_id, approval.plan_revision)
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "Failed to read workflow plan revision from PlanStore.",
            )
        })?
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Referenced plan revision was not found in PlanStore.",
            )
        })?;
    let latest_rev = authority
        .plans()
        .latest_revision(&approval.plan_id)
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "Failed to read latest workflow plan revision from PlanStore.",
            )
        })?
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Referenced workflow plan has no revisions in PlanStore.",
            )
        })?;
    if !plan_rev.status.is_active_published() || latest_rev.revision != approval.plan_revision {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Approved plan revision is no longer the active published revision.",
        ));
    }

    let reg_snap = authority.registry().snapshot().map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "Failed to read workflow registry snapshot.",
        )
    })?;
    if reg_snap.revision != linkage.registry_revision {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "RegistryStore revision drifted from the job's frozen registry_revision.",
        ));
    }

    let mut role_reqs = Vec::new();
    let mut seen_roles = BTreeSet::new();
    for task in &approval.tasks {
        if task.adapter == ToolAdapter::CodexCli
            || RoleExecutionRoute::from_adapter(task.adapter) != RoleExecutionRoute::HeadlessWorker
            || task.role != WorkflowRole::Coder
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Approved task uses an unsupported or native-only execution route for headless Coder execution.",
            ));
        }
        if seen_roles.insert((task.role, task.difficulty)) {
            role_reqs.push(RolePreflightRequest {
                role: task.role,
                difficulty: Some(task.difficulty),
            });
        }
    }

    let report = preflight(
        &reg_snap,
        &PreflightRequest {
            profile_id: Some(approval.profile_id.clone()),
            roles: role_reqs,
        },
        policy,
    )?;
    if !report.ready {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "A1 preflight rejected one or more approved tasks under the frozen execution policy.",
        ));
    }
    Ok(())
}

/// Generates a deterministic logical controller driver instance identifier (`ctl_<32hex>`) for
/// `(binding_id, job_id, driver_tag)`. By itself this deterministic ID is for logical lease
/// recovery and is not proof of a unique OS process.
pub fn deterministic_controller_instance_id(
    binding_id: &str,
    job_id: &str,
    driver_tag: &str,
) -> String {
    deterministic_prefixed_id(
        "threadlane-native:controller-driver-instance:v1",
        "ctl",
        &[binding_id, job_id, driver_tag],
    )
}

/// Sanitizes diagnostic text to a single-line bounded string safe for persistence.
pub fn sanitize_controller_diagnostic(raw: &str) -> String {
    let clean = sanitize_diagnostic_text(raw);
    if clean.len() <= MAX_COWORKER_SUMMARY_BYTES {
        clean
    } else {
        let mut end = MAX_COWORKER_SUMMARY_BYTES;
        while end > 0 && !clean.is_char_boundary(end) {
            end -= 1;
        }
        clean[..end].to_string()
    }
}

fn controller_ledger_filename(binding_id: &str) -> String {
    let tag = deterministic_prefixed_id(
        "threadlane-native:controller-policy-ledger:v1",
        "tag",
        &[binding_id],
    );
    let hex_part = tag.strip_prefix("tag_").unwrap_or(&tag);
    format!("coworker-controller-{hex_part}.json")
}

fn controller_lock_filename(binding_id: &str) -> String {
    let tag = deterministic_prefixed_id(
        "threadlane-native:controller-policy-ledger:v1",
        "tag",
        &[binding_id],
    );
    let hex_part = tag.strip_prefix("tag_").unwrap_or(&tag);
    format!("coworker-controller-{hex_part}.lock")
}

fn validate_existing_private_dir(dir: &Path, label: &str) -> Result<PathBuf, NativeError> {
    if dir.as_os_str().is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            format!("{label} path must not be empty."),
        ));
    }
    let resolved = if dir.is_absolute() {
        dir.to_path_buf()
    } else {
        let cwd = std::env::current_dir().map_err(|_| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!("Failed to resolve current working directory for {label}."),
            )
        })?;
        cwd.join(dir)
    };
    let clean = validate_absolute_no_symlink_components(&resolved, label)?;
    let meta = fs::symlink_metadata(&clean).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            NativeError::new(
                NativeErrorCode::NotConfigured,
                format!("Required {label} directory does not exist."),
            )
        } else {
            NativeError::new(
                NativeErrorCode::IoError,
                format!("Failed to inspect {label} directory metadata."),
            )
        }
    })?;
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!("{label} must be an existing private non-symlink directory."),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if (meta.permissions().mode() & 0o077) != 0 {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!("{label} directory has insecure permissions (expected 0700)."),
            ));
        }
    }
    Ok(clean)
}
