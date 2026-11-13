#![deny(unsafe_code)]
#![deny(missing_docs)]

//! `threadlane-workers`: isolated Milestone 2 worker process supervision, incremental NDJSON
//! evidence parsing, and Git worktree/diff-gate foundation for Threadlane.
//!
//! This crate is intentionally independent of `threadlane-core` and Tauri so it can be
//! exercised and verified in isolation before supervisor/MCP integration.
//!
//! ## Key Boundaries and Invariants
//!
//! - **Explicit trusted inputs**: Provider (`antigravity`, `opencode_v2`, or scoped-task/read-only
//!   `codex_cli`), absolute non-symlink executable path, exact requested model and effort/variant,
//!   canonical repository path, base commit, private work area, and allowed repository-relative
//!   write paths are validated before launch.
//! - **Exact argument arrays**: Never invokes a shell, never invents unsupported flags (such as
//!   `--variant`), and never adds `--dangerously-skip-permissions` or `--auto`.
//! - **Incremental bounded NDJSON & evidence verification**: Captures stdout and stderr in bounded
//!   chunks, parses init/model/session/blocked/error/result evidence, optionally forwards live
//!   sanitized/bounded [`NormalizedWorkerEvent`] records before process exit via nonblocking
//!   `try_send` (`run_worker_with_observer`, `execute_worker_task_with_observer`, recording
//!   undelivered live sends in [`WorkerExecutionReport::dropped_live_events`]), and rejects
//!   exit-zero runs that lack model/effort/result evidence or report mismatches/errors/blocks.
//! - **Unix process-group supervision**: Spawns workers in a dedicated process group (`process_group(0)`),
//!   enforces timeouts, explicit cancellation, and output overflow limits, and terminates/reaps
//!   the entire descendant group so lingering children holding pipes cannot hang supervision.
//! - **Git worktrees and post-run diff gate**: Creates per-run integration and per-task worktrees
//!   with private (`0600`) ownership manifests stored outside the checkout, evaluates post-run
//!   diffs (explicitly labeled as post-run evidence, not preventive OS sandboxing), and enforces
//!   safe ownership-verified cleanup without `--force` or auto-merge/push.

/// Opt-in complete structured advisory response capture and framing provenance.
pub mod advisory_response;
/// Invocation-owned bounded capture and unverified AGY diagnostic hint classifier.
pub mod antigravity_diagnostics;
/// Versioned exact-match AGY catalog model variant metadata.
pub mod antigravity_models;
/// Generic bounded-command execution primitive under isolated Unix process-group supervision.
pub mod bounded_command;
/// Narrow diagnostic CLI parser and command runner (`threadlane-worker`).
pub mod cli;
/// Sealed dependency input policy, manifest verification, and shared lock lease loader.
pub mod dependency;
/// Structured error types, refusal reasons, and wire error envelopes.
pub mod error;
/// Path canonicalization, symlink rejection, private `0700`/`0600` file helpers, and secret redaction.
pub mod fs_safety;
/// Git worktree manager, private ownership manifests, post-run diff gate, and safe cleanup.
pub mod git;
/// Owned OpenCode session allocation and bounded post-run metadata collection.
pub(crate) mod opencode_collector;
/// Pure validation of explicitly supplied owned OpenCode metadata; performs no I/O.
pub mod opencode_metadata;
/// Session-only provider file permission policies and Antigravity `--settings` builders.
pub mod permissions;
/// Advisory progress hints and soft deadlines within the existing hard runtime ceiling.
pub mod progress_watchdog;
/// Cumulative unverified diagnostic snapshots, separate from execution authority.
pub mod provider_health;
/// Typed worker specification, execution budgets, policy allowlists, and exact argv builders.
pub mod spec;
/// Incremental NDJSON stream parser, normalized events, and requested-vs-observed evidence checks.
pub mod stream;
/// Unix process-group lifecycle supervision and bounded concurrent stream capture.
pub mod supervisor;

pub use advisory_response::{
    AdvisoryResponseBudget, AdvisoryResponseError, AdvisoryResponseErrorCode,
    AdvisoryResponseFraming, AdvisoryResponseSource, AdvisoryStructuredExecution,
    BoundedAdvisoryResponse, DEFAULT_MAX_ADVISORY_RESPONSE_BYTES, MAX_ADVISORY_RESPONSE_BYTES,
};
pub use bounded_command::{
    run_bounded_command, run_bounded_command_with_cancel, BoundedCommandBudgets,
    BoundedCommandOverflow, BoundedCommandReport, BoundedCommandSpec, BoundedCommandStatus,
    MAX_BOUNDED_COMMAND_ARGV_LEN, MAX_BOUNDED_COMMAND_ARG_BYTES, MAX_BOUNDED_COMMAND_ENV_OVERRIDES,
};
pub use cli::{load_worker_spec_file, run_cli, CreatedWorktreesReport, WorkerCli, WorkerCommand};
pub use dependency::{
    dependency_input_lock_path, read_dependency_input, validate_dependency_input_policy,
    DependencyInputPolicy, DependencyInputSeal, DependencyPredecessorInput,
    VerifiedDependencyInput, DEPENDENCIES_MANIFEST_SUBDIR, DEPENDENCY_INPUT_SCHEMA_VERSION,
    MAX_DEPENDENCY_INPUT_BYTES, MAX_DEPENDENCY_PREDECESSORS,
};
pub use error::{CleanupRefusalReason, WorkerApiError, WorkerError, WorkerErrorCode};
pub use fs_safety::{
    ensure_platform_supported, ensure_private_dir, open_private_lock_file, read_private_json,
    sanitize_and_redact, truncate_utf8, validate_absolute_no_symlink_components,
    validate_executable_file, validate_existing_canonical_dir, validate_preexisting_private_file,
    write_private_json_atomic,
};
pub use git::{
    begin_worktree_job, cleanup_integrated_worktree, cleanup_worktree,
    ensure_worktree_job_inactive_readonly, evaluate_diff_gate, evaluate_diff_gate_with_limits,
    evaluate_diff_gate_with_scan_limits, evaluate_diff_gate_with_timeout, execute_worker_task,
    execute_worker_task_with_health_observer, execute_worker_task_with_observer,
    inspect_base_commit_path_kind, is_commit_ancestor, is_source_worktree_clean, is_worktree_clean,
    load_and_verify_manifest, open_existing_private_lock_file,
    resolve_allowed_write_scopes_at_base, resolve_exact_commit_sha, resolve_exact_tree_sha,
    run_sanitized_git, run_sanitized_git_allow_nonzero, run_sanitized_git_bytes,
    sanitize_git_command_env, sanitize_git_command_env_from, validate_exact_40_hex_sha,
    validate_sha256_hex_digest, BaseCommitPathKind, ChangedPathEntry, CleanupReport,
    CleanupRequest, DiffChangeKind, DiffGateReport, DiffViolation, DiffViolationKind,
    IntegratedCleanupReport, IntegratedCleanupRequest, IsolationLevel, SupervisedGitOutput,
    TaskExecutionReport, WorkAcceptanceState, WorkAreaOwnershipMarker, WorktreeJobGuard,
    WorktreeKind, WorktreeManager, WorktreeManifest, CHECKOUTS_SUBDIR,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS, DEFAULT_MAX_DIFF_CHANGED_PATHS, DEFAULT_MAX_GIT_OUTPUT_BYTES,
    DEFAULT_MAX_SCAN_DEPTH, DEFAULT_MAX_SCAN_ENTRIES, MANIFESTS_SUBDIR, MANIFEST_SCHEMA_VERSION,
    SANITIZED_GIT_ENV_OVERRIDES, WORK_AREA_MARKER_FILENAME, WORK_AREA_MARKER_VERSION,
};
pub use opencode_metadata::{
    validate_opencode_metadata, validate_opencode_metadata_with_diagnostic,
    OpenCodeMetadataDiagnostic, OpenCodeMetadataError, OpenCodeMetadataEvidence,
    OpenCodeMetadataStage, OwnedOpenCodeSession, MAX_OPENCODE_EXPORT_BYTES,
    MAX_OPENCODE_INFO_BYTES, MAX_OPENCODE_MESSAGES,
};
pub use permissions::{
    build_antigravity_read_only_settings, build_antigravity_read_only_settings_json,
    build_antigravity_task_files_settings_json,
    build_antigravity_task_files_settings_json_for_scopes,
    build_codex_advisory_exact_permission_profile_config,
    build_codex_task_files_permission_profile_config,
    build_codex_task_files_permission_profile_config_for_scopes, build_opencode_config_content,
    build_opencode_config_content_json, collect_codex_advisory_deny_frontiers,
    evaluate_antigravity_settings, evaluate_codex_task_files_permission_profile_config,
    evaluate_opencode_rules, is_unsafe_codex_toml_scope_char, is_unsafe_permission_pattern_char,
    parse_and_validate_codex_advisory_exact_permission_profile_config,
    parse_and_validate_codex_task_files_permission_profile_config,
    validate_advisory_provider_and_mode, validate_advisory_read_only_workdir,
    validate_codex_advisory_workdir, validate_codex_task_files_scope_str,
    validate_codex_task_files_workdir, validate_permission_pattern_path,
    validate_permission_pattern_str, AntigravityPermissionsBlock, AntigravitySessionSettings,
    OpenCodeAgentConfig, OpenCodeConfigContent, OpenCodeModelObject, OpenCodePermissionRule,
    OpenCodeRuleEffect, WorkerFilePermissions, AGY_DENIED_EXECUTION_AND_DELEGATION_TOOLS,
    AGY_PROTECTED_READ_DENY_PATTERNS, AGY_READ_PERMISSION_TOOL, AGY_WRITE_PERMISSION_TOOLS,
    CODEX_ADVISORY_GLOB_SCAN_DEPTH_MARGIN, CODEX_ADVISORY_GLOB_SCAN_MAX_DEPTH,
    CODEX_ADVISORY_MANDATORY_ROOT_DENY_PATHS, CODEX_ADVISORY_MAX_EXACT_DENY_RULES,
    CODEX_ADVISORY_MAX_WORKDIR_DEPTH, CODEX_ADVISORY_MAX_WORKDIR_ENTRIES,
    CODEX_ADVISORY_PROTECTED_DIRECTORY_NAMES, CODEX_ADVISORY_PROTECTED_FILE_NAMES,
    CODEX_PROJECT_CONFIG_MARKERS, CODEX_TASK_FILES_PERMISSION_PROFILE_NAME,
    CODEX_TASK_FILES_PROTECTED_DENY_PATTERNS, MAX_OPENCODE_CONFIG_BYTES,
    MAX_TASK_FILES_SETTINGS_BYTES, MAX_TASK_FILES_WRITE_PATHS, OPENCODE_CONFIG_ENV_VAR,
    OPENCODE_DEFAULT_AGENT, OPENCODE_PINNED_AGENTS, OPENCODE_READ_ONLY_DENIED_PERMISSION_ACTIONS,
    OPENCODE_READ_ONLY_DENIED_TOOL_POLICIES,
};
pub use progress_watchdog::{
    ProgressAttentionReason, ProgressWatchdog, ProgressWatchdogPolicy, ProgressWatchdogSnapshot,
    ProgressWatchdogStatus,
};
pub use provider_health::{ProviderHealthObserver, ProviderHealthSnapshot};
pub use spec::resolve_model_target;
pub use spec::{
    extract_pinned_codex_task_files_write_scopes, is_control_config_path, is_path_allowed,
    is_path_allowed_by_scopes, is_path_allowed_syntactic, is_pinned_codex_read_only_argv,
    is_pinned_codex_task_files_argv, validate_commit_ref, validate_repo_relative_path,
    validate_simple_identifier, AdvisorySpecValidationReport, AdvisoryWorkerSpec, ExecutionBudgets,
    ResolvedModelTarget, SpecValidationReport, WorkerPolicy, WorkerProvider, WorkerSpec,
    WriteScopeKind, CODEX_ADVISORY_DISABLED_FEATURES, CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG,
    CODEX_ADVISORY_PERMISSION_PROFILE_NAME, DEFAULT_MAX_EVENTS, DEFAULT_MAX_LINE_BYTES,
    DEFAULT_MAX_STDERR_BYTES, DEFAULT_MAX_STDOUT_BYTES, DEFAULT_TIMEOUT_MS, MAX_EVENTS,
    MAX_IDENTIFIER_LEN, MAX_LINE_BYTES, MAX_PATH_ENTRY_LEN, MAX_PROMPT_BYTES, MAX_STDERR_BYTES,
    MAX_STDOUT_BYTES, MAX_TIMEOUT_MS,
};
pub use stream::{
    BlockedEvidence, BlockedKind, CodexTurnLifecycleEvidence, EffortEvidenceSource, FieldEvidence,
    MalformedLineDiagnostic, NdjsonStreamParser, NormalizedEventKind, NormalizedWorkerEvent,
    ProviderErrorEvidence, StreamEvidenceSummary, StreamTokenUsage,
};
pub use supervisor::{
    is_process_alive, is_process_group_alive, run_advisory_worker, run_advisory_worker_with_cancel,
    run_advisory_worker_with_health_observer, run_advisory_worker_with_observer,
    run_advisory_worker_with_structured_result,
    run_advisory_worker_with_structured_result_and_cancel,
    run_advisory_worker_with_structured_result_and_health_observer,
    run_advisory_worker_with_structured_result_and_observer, run_worker, run_worker_with_cancel,
    run_worker_with_health_observer, run_worker_with_observer, OutputOverflowKind,
    WorkerExecutionReport, WorkerRunStatus, POST_EXIT_PIPE_DRAIN_TIMEOUT_MS,
};
