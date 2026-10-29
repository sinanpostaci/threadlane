use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::process::{Command, Stdio};
#[cfg(unix)]
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use threadlane_workers::{
    ensure_platform_supported, is_worktree_clean, load_and_verify_manifest, read_private_json,
    resolve_exact_commit_sha, resolve_exact_tree_sha, sanitize_and_redact,
    sanitize_git_command_env, validate_absolute_no_symlink_components, validate_exact_40_hex_sha,
    validate_executable_file, validate_existing_canonical_dir, validate_sha256_hex_digest,
    validate_simple_identifier, write_private_json_atomic, WorktreeKind,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS, MANIFESTS_SUBDIR, POST_EXIT_PIPE_DRAIN_TIMEOUT_MS,
};

use crate::error::OperationsError;
use crate::review::{sha256_hex, MAX_RECEIPT_BYTES, RECEIPTS_SUBDIR};

/// Schema version for [`CodeGraphIndexReceipt`].
pub const CODEGRAPH_INDEX_RECEIPT_SCHEMA_VERSION: u32 = 1;

/// Maximum byte size captured from an explicit tool version probe (16 KiB).
pub const MAX_TOOL_PROBE_OUTPUT_BYTES: usize = 16_384;

/// Auxiliary local tools reported by [`ToolCapabilities`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuxiliaryToolKind {
    /// Git version control CLI (`git`).
    Git,
    /// Optional terminal multiplexer (`tmux`) for read-only operator attachment.
    Tmux,
    /// Optional CodeGraph CLI (`codegraph`); distinct from the `codegraph_explore` MCP server.
    CodeGraph,
}

impl AuxiliaryToolKind {
    /// Returns the canonical snake_case identifier for this auxiliary tool.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Git => "git",
            Self::Tmux => "tmux",
            Self::CodeGraph => "codegraph",
        }
    }

    /// Returns whether this tool is a mandatory runtime dependency (`false` for `tmux` and `codegraph`).
    pub const fn is_mandatory_os_dependency(self) -> bool {
        false
    }
}

/// Honest availability and verification state for an auxiliary tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolSupportStatus {
    /// Executable path was validated and an explicit operator version probe succeeded.
    SupportedAndVerified,
    /// Executable path exists and passed file/permission validation, but no subprocess probe was run.
    AvailableUnverified,
    /// Tool is not configured or not installed; non-blocking for optional tools (`tmux`, `codegraph`).
    OptionalUnavailable,
    /// Configured executable path does not exist, is a symlink, or is not executable.
    InvalidExecutable,
}

/// Optional explicit executable paths for auxiliary tools.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolPathsConfig {
    /// Optional explicit absolute path to the `git` executable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_executable: Option<PathBuf>,
    /// Optional explicit absolute path to the `tmux` executable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tmux_executable: Option<PathBuf>,
    /// Optional explicit absolute path to the `codegraph` CLI executable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codegraph_executable: Option<PathBuf>,
}

/// Single auxiliary tool capability entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCapabilityReport {
    /// Tool kind (`git`, `tmux`, or `codegraph`).
    pub tool: AuxiliaryToolKind,
    /// Honest support and verification status.
    pub status: ToolSupportStatus,
    /// Whether this tool is required by the OS/runtime (`false` for `tmux` and `codegraph`).
    pub mandatory_os_dependency: bool,
    /// Validated canonical executable path, if configured and valid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable_path: Option<PathBuf>,
    /// Sanitized version string populated only when an explicit operator probe was requested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Safe human-readable explanation of current capability state.
    pub message: String,
}

/// Aggregated capability report for `git`, `tmux`, and `codegraph`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCapabilities {
    /// Capability report for `git`.
    pub git: ToolCapabilityReport,
    /// Capability report for optional `tmux` read-only session attachment.
    pub tmux: ToolCapabilityReport,
    /// Capability report for optional `codegraph` CLI.
    pub codegraph: ToolCapabilityReport,
    /// Explicit distinction between the `codegraph` MCP server and a standalone `codegraph` CLI.
    pub codegraph_surface: CodeGraphSurfaceDistinction,
}

/// Explicit boundary documenting that the `codegraph` MCP tool server (`codegraph_explore`)
/// and any standalone `codegraph` CLI binary are separate interfaces and are never conflated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeGraphSurfaceDistinction {
    /// MCP server tool name (`"codegraph_explore"`).
    pub mcp_tool_name: String,
    /// Always `false`: MCP server availability does not imply CLI availability or vice versa.
    pub mcp_is_cli: bool,
    /// Always `false`: Threadlane never invokes external CodeGraph tools without an explicit operator request.
    pub auto_invoked: bool,
    /// Human-readable boundary note.
    pub note: String,
}

impl Default for CodeGraphSurfaceDistinction {
    fn default() -> Self {
        Self {
            mcp_tool_name: "codegraph_explore".to_string(),
            mcp_is_cli: false,
            auto_invoked: false,
            note: concat!(
                "CodeGraph MCP (`codegraph_explore`) and standalone CodeGraph CLI are distinct ",
                "surfaces (`mcp_is_cli = false`) and are never automatically invoked."
            )
            .to_string(),
        }
    }
}

/// Request to construct an optional read-only `tmux` attachment descriptor for an owned worktree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TmuxDescriptorRequest {
    /// Trusted private work-area root directory.
    pub work_area_root: PathBuf,
    /// Owned worktree identifier.
    pub worktree_id: String,
    /// Optional explicit `tmux` session name; defaults to `tl-ro-<worktree_id>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_name: Option<String>,
    /// Optional explicit absolute `tmux` socket path (`-S <socket_path>`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub socket_path: Option<PathBuf>,
    /// Optional validated `tmux` executable path (defaults to `"tmux"` in `attach_argv[0]`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tmux_executable: Option<PathBuf>,
}

/// Read-only `tmux` attachment descriptor for operator inspection.
///
/// Explicitly marked `read_only = true` and `mandatory_supervisor = false`: Threadlane's
/// Unix process-group supervisor owns worker lifecycle, and `tmux` is never required.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TmuxAttachmentDescriptor {
    /// Owned worktree identifier.
    pub worktree_id: String,
    /// Worktree kind (`run_integration` or `task`).
    pub worktree_kind: WorktreeKind,
    /// Canonical owned checkout path.
    pub checkout_path: PathBuf,
    /// Validated `tmux` session name.
    pub session_name: String,
    /// Optional validated absolute socket path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub socket_path: Option<PathBuf>,
    /// Always `true`: attachment uses `-r` (read-only) so observers cannot inject keystrokes.
    pub read_only: bool,
    /// Always `false`: `tmux` is an optional operator view, never the worker process supervisor.
    pub mandatory_supervisor: bool,
    /// Exact argument vector (including `argv[0]`) for read-only attachment (`attach-session -r -t ...`).
    pub attach_argv: Vec<String>,
    /// Human-readable support note.
    pub support_note: String,
}

/// Explicit index freshness state for a CodeGraph project bound to an owned worktree checkout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum CodeGraphIndexFreshness {
    /// No verified CodeGraph index receipt exists for this worktree checkout.
    Missing,
    /// A verified index receipt exists and matches the worktree's current clean `HEAD` commit and `HEAD^{tree}` SHA.
    Fresh {
        /// Indexed 40-hex `HEAD` commit SHA.
        indexed_commit: String,
        /// Indexed 40-hex `HEAD^{tree}` SHA.
        indexed_tree_sha: String,
        /// Timestamp (ms since Unix epoch) when the index receipt was recorded.
        indexed_at_ms: i64,
    },
    /// An index receipt exists, but the worktree's `HEAD` commit, `HEAD^{tree}`, or working tree cleanliness has changed.
    Stale {
        /// Previously indexed 40-hex commit SHA.
        indexed_commit: String,
        /// Previously indexed 40-hex tree SHA.
        indexed_tree_sha: String,
        /// Current 40-hex `HEAD` commit SHA in the owned worktree checkout.
        current_head_commit: String,
        /// Current 40-hex `HEAD^{tree}` SHA in the owned worktree checkout.
        current_tree_sha: String,
        /// True if the worktree checkout currently has uncommitted, untracked, or ignored modifications.
        dirty_worktree: bool,
    },
    /// Index metadata on disk could not be verified against a valid Threadlane receipt.
    Unverified {
        /// Safe diagnostic detail.
        detail: String,
    },
}

/// Persisted receipt recording when an owned worktree checkout was indexed at a specific commit and tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodeGraphIndexReceipt {
    /// Schema version (`1`).
    pub schema_version: u32,
    /// Owned worktree identifier.
    pub worktree_id: String,
    /// Canonical owned worktree checkout path (`project_path`).
    pub checkout_path: PathBuf,
    /// Exact 40-hex `HEAD` commit SHA when the index was built.
    pub indexed_commit: String,
    /// Exact 40-hex `HEAD^{tree}` SHA when the index was built.
    pub indexed_tree_sha: String,
    /// Operator identifier that recorded the index receipt.
    pub operator_id: String,
    /// Timestamp (ms since Unix epoch).
    pub indexed_at_ms: i64,
    /// Canonical 64-hex SHA-256 digest of this receipt.
    pub receipt_digest: String,
}

impl CodeGraphIndexReceipt {
    /// Computes the canonical 64-character hex SHA-256 digest of this index receipt.
    pub fn compute_digest(&self) -> Result<String, OperationsError> {
        #[derive(Serialize)]
        struct Payload<'a> {
            schema_version: u32,
            worktree_id: &'a str,
            checkout_path: &'a Path,
            indexed_commit: &'a str,
            indexed_tree_sha: &'a str,
            operator_id: &'a str,
            indexed_at_ms: i64,
        }
        let encoded = serde_json::to_vec(&Payload {
            schema_version: self.schema_version,
            worktree_id: &self.worktree_id,
            checkout_path: &self.checkout_path,
            indexed_commit: &self.indexed_commit,
            indexed_tree_sha: &self.indexed_tree_sha,
            operator_id: &self.operator_id,
            indexed_at_ms: self.indexed_at_ms,
        })?;
        Ok(sha256_hex(&encoded))
    }

    /// Validates all fields and verifies `self.receipt_digest`.
    pub fn verify_integrity(&self) -> Result<(), OperationsError> {
        if self.schema_version != CODEGRAPH_INDEX_RECEIPT_SCHEMA_VERSION {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "unsupported codegraph receipt schema_version {}",
                self.schema_version
            )));
        }
        validate_simple_identifier(&self.worktree_id, "worktree_id")?;
        validate_simple_identifier(&self.operator_id, "operator_id")?;
        let _ = validate_exact_40_hex_sha(&self.indexed_commit, "indexed_commit")?;
        let _ = validate_exact_40_hex_sha(&self.indexed_tree_sha, "indexed_tree_sha")?;
        let _ = validate_sha256_hex_digest(&self.receipt_digest, "receipt_digest")?;
        let computed = self.compute_digest()?;
        if computed != self.receipt_digest {
            return Err(OperationsError::ReceiptInvalidated(
                "codegraph index receipt digest mismatch".to_string(),
            ));
        }
        Ok(())
    }
}

/// Verified CodeGraph project status for an owned worktree checkout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeGraphWorktreeStatus {
    /// Owned worktree identifier.
    pub worktree_id: String,
    /// Worktree kind (`run_integration` or `task`).
    pub worktree_kind: WorktreeKind,
    /// Verified canonical owned worktree checkout path used as the CodeGraph `project_path`.
    pub project_path: PathBuf,
    /// Current 40-hex `HEAD` commit SHA of the worktree checkout.
    pub current_head_commit: String,
    /// Current 40-hex `HEAD^{tree}` SHA of the worktree checkout.
    pub current_tree_sha: String,
    /// True if the worktree checkout has uncommitted, untracked, or ignored files.
    pub dirty_worktree: bool,
    /// Explicit index freshness state (`missing`, `fresh`, `stale`, or `unverified`).
    pub freshness: CodeGraphIndexFreshness,
    /// Explicit boundary distinguishing the `codegraph_explore` MCP tool from a standalone CLI.
    pub surface_distinction: CodeGraphSurfaceDistinction,
}

/// Read-only plan for indexing an owned worktree checkout with an explicit CodeGraph CLI executable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeGraphIndexProposal {
    /// Owned worktree identifier.
    pub worktree_id: String,
    /// Verified canonical owned checkout path (`project_path`).
    pub project_path: PathBuf,
    /// Validated canonical CodeGraph CLI executable path.
    pub codegraph_executable: PathBuf,
    /// Current 40-hex `HEAD` commit SHA in the checkout.
    pub head_commit: String,
    /// Current 40-hex `HEAD^{tree}` SHA in the checkout.
    pub tree_sha: String,
    /// Current index freshness prior to indexing.
    pub current_freshness: CodeGraphIndexFreshness,
    /// Exact argument vector (excluding `argv[0]`) planned for CLI indexing.
    pub planned_cli_argv: Vec<String>,
    /// Always `true`: proposal generation never spawns external tools.
    pub read_only: bool,
    /// Explicit MCP vs CLI distinction.
    pub surface_distinction: CodeGraphSurfaceDistinction,
}

/// Passively inspects `git`, `tmux`, and `codegraph` capabilities from `config` without
/// spawning any subprocesses.
pub fn inspect_tool_capabilities_passive(config: &ToolPathsConfig) -> ToolCapabilities {
    ToolCapabilities {
        git: inspect_single_tool_passive(AuxiliaryToolKind::Git, config.git_executable.as_deref()),
        tmux: inspect_single_tool_passive(
            AuxiliaryToolKind::Tmux,
            config.tmux_executable.as_deref(),
        ),
        codegraph: inspect_single_tool_passive(
            AuxiliaryToolKind::CodeGraph,
            config.codegraph_executable.as_deref(),
        ),
        codegraph_surface: CodeGraphSurfaceDistinction::default(),
    }
}

/// Explicitly probes configured auxiliary tools (`git`, `tmux`, `codegraph`) using fixed
/// version argument templates (`--version` / `-V`), sanitized `GIT_*` environment, process-group
/// supervision, and bounded output capture.
///
/// Tools with no configured executable path remain `OptionalUnavailable` without searching
/// untrusted directories or failing the call.
pub fn probe_tool_capabilities_explicit(config: &ToolPathsConfig) -> ToolCapabilities {
    ToolCapabilities {
        git: probe_single_tool_explicit(
            AuxiliaryToolKind::Git,
            config.git_executable.as_deref(),
            &["--version"],
        ),
        tmux: probe_single_tool_explicit(
            AuxiliaryToolKind::Tmux,
            config.tmux_executable.as_deref(),
            &["-V"],
        ),
        codegraph: probe_single_tool_explicit(
            AuxiliaryToolKind::CodeGraph,
            config.codegraph_executable.as_deref(),
            &["--version"],
        ),
        codegraph_surface: CodeGraphSurfaceDistinction::default(),
    }
}

/// Constructs a validated, read-only `tmux` attachment descriptor (`attach-session -r -t ...`)
/// for an owned Threadlane worktree without spawning `tmux` or making `tmux` a mandatory supervisor.
pub fn build_tmux_readonly_descriptor(
    request: &TmuxDescriptorRequest,
) -> Result<TmuxAttachmentDescriptor, OperationsError> {
    let manifest = load_and_verify_manifest(&request.work_area_root, &request.worktree_id)?;
    let canonical_checkout =
        validate_existing_canonical_dir(&manifest.checkout_path, "worktree checkout_path")?;
    if canonical_checkout != manifest.checkout_path {
        return Err(OperationsError::SecurityViolation(
            "worktree checkout_path canonicalization mismatch".to_string(),
        ));
    }

    let session_name = match request.session_name {
        Some(ref name) => {
            validate_simple_identifier(name, "tmux session_name")?;
            name.clone()
        }
        None => format!("tl-ro-{}", manifest.worktree_id),
    };

    let validated_socket = match request.socket_path {
        Some(ref sock) => {
            let checked = validate_absolute_no_symlink_components(sock, "tmux socket_path")?;
            Some(checked)
        }
        None => None,
    };

    let tmux_bin = match request.tmux_executable {
        Some(ref exe) => {
            let canonical_exe = validate_executable_file(exe)?;
            canonical_exe.to_string_lossy().to_string()
        }
        None => "tmux".to_string(),
    };

    let mut attach_argv = Vec::with_capacity(8);
    attach_argv.push(tmux_bin);
    if let Some(ref sock) = validated_socket {
        let sock_str = sock.to_str().ok_or_else(|| {
            OperationsError::InvalidInput("tmux socket_path must be valid UTF-8".to_string())
        })?;
        attach_argv.push("-S".to_string());
        attach_argv.push(sock_str.to_string());
    }
    attach_argv.push("attach-session".to_string());
    attach_argv.push("-r".to_string());
    attach_argv.push("-t".to_string());
    attach_argv.push(session_name.clone());

    Ok(TmuxAttachmentDescriptor {
        worktree_id: manifest.worktree_id,
        worktree_kind: manifest.kind,
        checkout_path: canonical_checkout,
        session_name,
        socket_path: validated_socket,
        read_only: true,
        mandatory_supervisor: false,
        attach_argv,
        support_note: concat!(
            "Optional read-only tmux attachment descriptor (`-r`); ",
            "worker lifecycle remains supervised by Threadlane's Unix process-group supervisor."
        )
        .to_string(),
    })
}

/// Passively inspects an owned worktree checkout for CodeGraph project binding and explicit
/// index freshness.
///
/// Enforces that `candidate_project_path` is the exact canonical `checkout_path` of the
/// verified [`WorktreeManifest`] for `worktree_id` inside `work_area_root` (refusing the
/// primary source repository path, symlinks, or any unowned path). Never spawns external
/// CodeGraph processes or conflates the `codegraph_explore` MCP tool with a CLI binary.
pub fn inspect_codegraph_worktree(
    work_area_root: &Path,
    worktree_id: &str,
    candidate_project_path: &Path,
) -> Result<CodeGraphWorktreeStatus, OperationsError> {
    let manifest = load_and_verify_manifest(work_area_root, worktree_id)?;
    let canonical_checkout =
        validate_existing_canonical_dir(&manifest.checkout_path, "worktree checkout_path")?;
    let canonical_candidate =
        validate_existing_canonical_dir(candidate_project_path, "codegraph project_path")?;

    if canonical_candidate != canonical_checkout {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "codegraph project_path `{}` does not match owned worktree `{worktree_id}` checkout `{}`",
            canonical_candidate.display(),
            canonical_checkout.display()
        )));
    }

    let current_head_commit =
        resolve_exact_commit_sha(&canonical_checkout, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
    let current_tree_sha =
        resolve_exact_tree_sha(&canonical_checkout, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
    let dirty_worktree = !is_worktree_clean(&canonical_checkout, DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;

    let receipt_path = manifest
        .work_area_root
        .join(MANIFESTS_SUBDIR)
        .join(RECEIPTS_SUBDIR)
        .join(format!("codegraph-{}.json", manifest.worktree_id));

    let freshness = if fs::symlink_metadata(&receipt_path).is_err() {
        CodeGraphIndexFreshness::Missing
    } else {
        match read_private_json::<CodeGraphIndexReceipt>(
            &receipt_path,
            MAX_RECEIPT_BYTES,
            "codegraph index receipt",
        ) {
            Ok(receipt) => {
                if let Err(err) = receipt.verify_integrity() {
                    CodeGraphIndexFreshness::Unverified {
                        detail: format!("codegraph index receipt failed integrity check: {err}"),
                    }
                } else if receipt.worktree_id != manifest.worktree_id
                    || receipt.checkout_path != canonical_checkout
                {
                    CodeGraphIndexFreshness::Unverified {
                        detail:
                            "codegraph index receipt worktree_id or checkout_path does not match manifest"
                                .to_string(),
                    }
                } else if !dirty_worktree
                    && receipt.indexed_commit == current_head_commit
                    && receipt.indexed_tree_sha == current_tree_sha
                {
                    CodeGraphIndexFreshness::Fresh {
                        indexed_commit: receipt.indexed_commit,
                        indexed_tree_sha: receipt.indexed_tree_sha,
                        indexed_at_ms: receipt.indexed_at_ms,
                    }
                } else {
                    CodeGraphIndexFreshness::Stale {
                        indexed_commit: receipt.indexed_commit,
                        indexed_tree_sha: receipt.indexed_tree_sha,
                        current_head_commit: current_head_commit.clone(),
                        current_tree_sha: current_tree_sha.clone(),
                        dirty_worktree,
                    }
                }
            }
            Err(err) => CodeGraphIndexFreshness::Unverified {
                detail: format!("failed to read codegraph index receipt: {err}"),
            },
        }
    };

    Ok(CodeGraphWorktreeStatus {
        worktree_id: manifest.worktree_id,
        worktree_kind: manifest.kind,
        project_path: canonical_checkout,
        current_head_commit,
        current_tree_sha,
        dirty_worktree,
        freshness,
        surface_distinction: CodeGraphSurfaceDistinction::default(),
    })
}

/// Builds a read-only [`CodeGraphIndexProposal`] for indexing an owned worktree checkout
/// using an explicitly validated CodeGraph CLI executable.
///
/// Never spawns the executable during proposal construction.
pub fn propose_codegraph_index_plan(
    work_area_root: &Path,
    worktree_id: &str,
    candidate_project_path: &Path,
    codegraph_executable: &Path,
) -> Result<CodeGraphIndexProposal, OperationsError> {
    let status = inspect_codegraph_worktree(work_area_root, worktree_id, candidate_project_path)?;
    let canonical_exe = validate_executable_file(codegraph_executable)
        .map_err(|err| OperationsError::ToolUnavailable(err.to_string()))?;
    let project_str = status.project_path.to_str().ok_or_else(|| {
        OperationsError::InvalidInput("project_path must be valid UTF-8".to_string())
    })?;

    Ok(CodeGraphIndexProposal {
        worktree_id: status.worktree_id,
        project_path: status.project_path.clone(),
        codegraph_executable: canonical_exe,
        head_commit: status.current_head_commit,
        tree_sha: status.current_tree_sha,
        current_freshness: status.freshness,
        planned_cli_argv: vec![
            "index".to_string(),
            "--project".to_string(),
            project_str.to_string(),
        ],
        read_only: true,
        surface_distinction: status.surface_distinction,
    })
}

/// Explicit operator entry point to record a verified [`CodeGraphIndexReceipt`] for an owned,
/// clean worktree checkout at its current `HEAD` commit and `HEAD^{tree}` SHA.
pub fn record_codegraph_index_receipt(
    work_area_root: &Path,
    worktree_id: &str,
    candidate_project_path: &Path,
    operator_id: &str,
    indexed_at_ms: i64,
) -> Result<CodeGraphIndexReceipt, OperationsError> {
    validate_simple_identifier(operator_id, "operator_id")?;
    let status = inspect_codegraph_worktree(work_area_root, worktree_id, candidate_project_path)?;
    if status.dirty_worktree {
        return Err(OperationsError::DirtyWorktree {
            worktree_id: status.worktree_id,
            detail: "cannot record a Fresh CodeGraph index receipt on a dirty worktree checkout"
                .to_string(),
        });
    }

    let canonical_work_area = validate_existing_canonical_dir(work_area_root, "work_area_root")?;
    let mut receipt = CodeGraphIndexReceipt {
        schema_version: CODEGRAPH_INDEX_RECEIPT_SCHEMA_VERSION,
        worktree_id: status.worktree_id.clone(),
        checkout_path: status.project_path,
        indexed_commit: status.current_head_commit,
        indexed_tree_sha: status.current_tree_sha,
        operator_id: operator_id.to_string(),
        indexed_at_ms,
        receipt_digest: String::new(),
    };
    receipt.receipt_digest = receipt.compute_digest()?;
    receipt.verify_integrity()?;

    let receipts_dir = canonical_work_area
        .join(MANIFESTS_SUBDIR)
        .join(RECEIPTS_SUBDIR);
    let filename = format!("codegraph-{}.json", status.worktree_id);
    write_private_json_atomic(
        &receipts_dir,
        &filename,
        &receipt,
        "codegraph index receipt",
    )?;
    Ok(receipt)
}

fn inspect_single_tool_passive(
    tool: AuxiliaryToolKind,
    configured_path: Option<&Path>,
) -> ToolCapabilityReport {
    let Some(path) = configured_path else {
        return ToolCapabilityReport {
            tool,
            status: ToolSupportStatus::OptionalUnavailable,
            mandatory_os_dependency: tool.is_mandatory_os_dependency(),
            executable_path: None,
            version: None,
            message: format!(
                "{} executable is not configured (optional capability; no OS dependency mandatory)",
                tool.as_str()
            ),
        };
    };

    match validate_executable_file(path) {
        Ok(canonical) => ToolCapabilityReport {
            tool,
            status: ToolSupportStatus::AvailableUnverified,
            mandatory_os_dependency: tool.is_mandatory_os_dependency(),
            executable_path: Some(canonical),
            version: None,
            message: format!(
                "{} executable validated passively (version not probed until explicit operator request)",
                tool.as_str()
            ),
        },
        Err(err) => ToolCapabilityReport {
            tool,
            status: ToolSupportStatus::InvalidExecutable,
            mandatory_os_dependency: tool.is_mandatory_os_dependency(),
            executable_path: None,
            version: None,
            message: sanitize_and_redact(&err.to_string(), 256),
        },
    }
}

fn probe_single_tool_explicit(
    tool: AuxiliaryToolKind,
    configured_path: Option<&Path>,
    version_args: &[&str],
) -> ToolCapabilityReport {
    let passive = inspect_single_tool_passive(tool, configured_path);
    let Some(ref canonical_exe) = passive.executable_path else {
        return passive;
    };

    match run_bounded_version_probe(canonical_exe, version_args) {
        Ok(version_str) => ToolCapabilityReport {
            tool,
            status: ToolSupportStatus::SupportedAndVerified,
            mandatory_os_dependency: tool.is_mandatory_os_dependency(),
            executable_path: Some(canonical_exe.clone()),
            version: Some(version_str.clone()),
            message: format!("{} verified ({version_str})", tool.as_str()),
        },
        Err(err) => ToolCapabilityReport {
            tool,
            status: ToolSupportStatus::InvalidExecutable,
            mandatory_os_dependency: tool.is_mandatory_os_dependency(),
            executable_path: Some(canonical_exe.clone()),
            version: None,
            message: sanitize_and_redact(&err.to_string(), 256),
        },
    }
}

fn run_bounded_version_probe(executable: &Path, args: &[&str]) -> Result<String, OperationsError> {
    ensure_platform_supported()?;

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;

        let mut cmd = Command::new(executable);
        sanitize_git_command_env(&mut cmd);
        cmd.args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);

        let mut child = cmd.spawn()?;
        let child_pid = child.id();
        let pgid = i32::try_from(child_pid).map_err(|_| {
            OperationsError::Io(std::io::Error::other("probe PID exceeds i32 range"))
        })?;

        let deadline =
            Instant::now() + Duration::from_millis(DEFAULT_GIT_COMMAND_TIMEOUT_MS.max(1));
        let exit_status = loop {
            if let Some(status) = child.try_wait()? {
                break Some(status);
            }
            if Instant::now() >= deadline {
                // SAFETY: `pgid > 1` is a dedicated process group created via `process_group(0)`.
                let _ = child.kill();
                let _ = child.wait();
                return Err(OperationsError::ToolUnavailable(format!(
                    "version probe for `{}` timed out",
                    executable.display()
                )));
            }
            std::thread::sleep(Duration::from_millis(10));
        };

        let _ = pgid;
        let _ = POST_EXIT_PIPE_DRAIN_TIMEOUT_MS;

        let mut stdout_buf = Vec::new();
        if let Some(stdout) = child.stdout.take() {
            let _ = stdout
                .take((MAX_TOOL_PROBE_OUTPUT_BYTES + 1) as u64)
                .read_to_end(&mut stdout_buf);
        }
        if stdout_buf.len() > MAX_TOOL_PROBE_OUTPUT_BYTES {
            return Err(OperationsError::ToolUnavailable(
                "version probe output exceeded byte cap".to_string(),
            ));
        }

        if !exit_status.is_some_and(|s| s.success()) {
            return Err(OperationsError::ToolUnavailable(format!(
                "version probe exited with non-zero status {:?}",
                exit_status.and_then(|s| s.code())
            )));
        }

        let raw_text = String::from_utf8_lossy(&stdout_buf);
        let first_line = raw_text.lines().next().unwrap_or("").trim();
        let cleaned = sanitize_and_redact(first_line, 128);
        if cleaned.is_empty() {
            return Err(OperationsError::ToolUnavailable(
                "version probe returned empty version string".to_string(),
            ));
        }
        Ok(cleaned)
    }

    #[cfg(not(unix))]
    {
        let _ = (executable, args);
        Err(OperationsError::UnsupportedPlatform(
            std::env::consts::OS.to_string(),
        ))
    }
}
