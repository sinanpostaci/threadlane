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
//! - **Explicit trusted inputs**: Provider (`antigravity` or `opencode_v2`), absolute non-symlink
//!   executable path, exact requested model and effort/variant, canonical repository path,
//!   base commit, private work area, and allowed repository-relative write paths are validated
//!   before launch.
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

/// Narrow diagnostic CLI parser and command runner (`threadlane-worker`).
pub mod cli;
/// Structured error types, refusal reasons, and wire error envelopes.
pub mod error;
/// Path canonicalization, symlink rejection, private `0700`/`0600` file helpers, and secret redaction.
pub mod fs_safety;
/// Git worktree manager, private ownership manifests, post-run diff gate, and safe cleanup.
pub mod git;
/// Session-only provider file permission policies and Antigravity `--settings` builders.
pub mod permissions;
/// Typed worker specification, execution budgets, policy allowlists, and exact argv builders.
pub mod spec;
/// Incremental NDJSON stream parser, normalized events, and requested-vs-observed evidence checks.
pub mod stream;
/// Unix process-group lifecycle supervision and bounded concurrent stream capture.
pub mod supervisor;

pub use cli::{load_worker_spec_file, run_cli, CreatedWorktreesReport, WorkerCli, WorkerCommand};
pub use error::{CleanupRefusalReason, WorkerApiError, WorkerError, WorkerErrorCode};
pub use fs_safety::{
    ensure_platform_supported, ensure_private_dir, open_private_lock_file, read_private_json,
    sanitize_and_redact, truncate_utf8, validate_absolute_no_symlink_components,
    validate_executable_file, validate_existing_canonical_dir, validate_preexisting_private_file,
    write_private_json_atomic,
};
pub use git::{
    begin_worktree_job, cleanup_integrated_worktree, cleanup_worktree, evaluate_diff_gate,
    evaluate_diff_gate_with_limits, evaluate_diff_gate_with_scan_limits,
    evaluate_diff_gate_with_timeout, execute_worker_task, execute_worker_task_with_observer,
    is_commit_ancestor, is_worktree_clean, load_and_verify_manifest, resolve_exact_commit_sha,
    resolve_exact_tree_sha, run_sanitized_git, run_sanitized_git_allow_nonzero,
    run_sanitized_git_bytes, sanitize_git_command_env, sanitize_git_command_env_from,
    validate_exact_40_hex_sha, validate_sha256_hex_digest, ChangedPathEntry, CleanupReport,
    CleanupRequest, DiffChangeKind, DiffGateReport, DiffViolation, DiffViolationKind,
    IntegratedCleanupReport, IntegratedCleanupRequest, IsolationLevel, SupervisedGitOutput,
    TaskExecutionReport, WorkAcceptanceState, WorkAreaOwnershipMarker, WorktreeJobGuard,
    WorktreeKind, WorktreeManager, WorktreeManifest, CHECKOUTS_SUBDIR,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS, DEFAULT_MAX_DIFF_CHANGED_PATHS, DEFAULT_MAX_GIT_OUTPUT_BYTES,
    DEFAULT_MAX_SCAN_DEPTH, DEFAULT_MAX_SCAN_ENTRIES, MANIFESTS_SUBDIR, MANIFEST_SCHEMA_VERSION,
    SANITIZED_GIT_ENV_OVERRIDES, WORK_AREA_MARKER_FILENAME, WORK_AREA_MARKER_VERSION,
};
pub use permissions::{
    build_antigravity_task_files_settings_json, is_unsafe_permission_pattern_char,
    validate_permission_pattern_path, validate_permission_pattern_str, WorkerFilePermissions,
    AGY_READ_PERMISSION_TOOL, AGY_WRITE_PERMISSION_TOOLS,
};
pub use spec::{
    is_control_config_path, is_path_allowed, validate_commit_ref, validate_repo_relative_path,
    validate_simple_identifier, ExecutionBudgets, ResolvedModelTarget, SpecValidationReport,
    WorkerPolicy, WorkerProvider, WorkerSpec, DEFAULT_MAX_EVENTS, DEFAULT_MAX_LINE_BYTES,
    DEFAULT_MAX_STDERR_BYTES, DEFAULT_MAX_STDOUT_BYTES, DEFAULT_TIMEOUT_MS, MAX_EVENTS,
    MAX_IDENTIFIER_LEN, MAX_LINE_BYTES, MAX_PATH_ENTRY_LEN, MAX_PROMPT_BYTES, MAX_STDERR_BYTES,
    MAX_STDOUT_BYTES, MAX_TIMEOUT_MS,
};
pub use stream::{
    BlockedEvidence, BlockedKind, FieldEvidence, MalformedLineDiagnostic, NdjsonStreamParser,
    NormalizedEventKind, NormalizedWorkerEvent, ProviderErrorEvidence, StreamEvidenceSummary,
};
pub use supervisor::{
    is_process_alive, is_process_group_alive, run_worker, run_worker_with_cancel,
    run_worker_with_observer, OutputOverflowKind, WorkerExecutionReport, WorkerRunStatus,
    POST_EXIT_PIPE_DRAIN_TIMEOUT_MS,
};
