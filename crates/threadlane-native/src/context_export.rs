//! Safe per-chat v1 trusted-local operator context export for verified managed desktop chats.
//!
//! After `AutomaticPairing::connect` independently verifies the managed Codex parent process,
//! active thread candidate, canonical repository `cwd`, and pairing epoch enrollment, this module
//! exports a private (`0600`) v1 operator context file under an owned private (`0700`) child
//! directory of the pairing store (`<pairing_dir>/operator-contexts/`) and resolves the canonical
//! same-package `threadlane-context` executable.
//!
//! Security and authority invariants:
//! - Only scope and store directory paths are exported; bearer tokens, operator credentials,
//!   raw conversations, and provider logs are never included.
//! - The exported context preserves `route = "trusted_local_operator"`,
//!   `native_per_call_identity_verified = false`, and makes no human approval claim.
//! - Caller-supplied IDs or paths are never accepted as authority; `binding_id` and `thread_uuid`
//!   come from verified enrollment, `project_root` from verified parent/thread evidence, and
//!   stores from actual server configuration.
//! - Export requires all five configured stores (`state_dir`, `registry_dir`, `plans_dir`,
//!   `authority_dir`, `sessions_dir`) to exist as canonical, non-symlink, private (`0700`),
//!   pairwise distinct and non-overlapping directories, and `project_root` to be an existing
//!   canonical non-symlink directory that does not overlap any private store.
//! - Exported files are scoped by `(binding_id, thread_uuid, router_executable, content_sha256)`.
//!   Repeated identical connects reuse the existing `0600` file without rewriting; foreign or
//!   tampered files at the target path fail closed without overwriting.

use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_authority::validation::{validate_canonical_thread_uuid, validate_identifier};

use crate::fs_safety::{
    acquire_cross_process_lock, ensure_platform_supported, prepare_private_dir,
    read_private_file_bounded, validate_absolute_no_symlink_components, validate_private_file_path,
    write_private_file_atomic, MAX_PRIVATE_STATE_FILE_BYTES,
};
use crate::operator_cli::{validate_dirs, OperatorCliError};
use crate::{NativeError, NativeErrorCode};

/// Fixed schema version for the private operator context file (`1`).
pub const OPERATOR_CONTEXT_SCHEMA_VERSION: u32 = 1;

/// Fixed route marker for the private operator context file (`"trusted_local_operator"`).
pub const TRUSTED_LOCAL_OPERATOR_ROUTE: &str = "trusted_local_operator";

/// Owned private child directory name under `pairing_dir` (`"operator-contexts"`).
pub const OPERATOR_CONTEXTS_DIR_NAME: &str = "operator-contexts";

/// Canonical sibling binary name for the context router (`"threadlane-context"`).
pub const CONTEXT_ROUTER_BINARY_NAME: &str = "threadlane-context";

const CONTEXT_EXPORT_LOCK_FILE: &str = ".context-export.lock";

/// Store directory paths embedded in a v1 private operator context JSON file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportedPrivateContextStoresV1 {
    /// Canonical private (`0700`) supervisor runtime state directory.
    pub state_dir: PathBuf,
    /// Canonical private (`0700`) workflow registry directory.
    pub registry_dir: PathBuf,
    /// Canonical private (`0700`) workflow plans directory.
    pub plans_dir: PathBuf,
    /// Canonical private (`0700`) operator authority store directory.
    pub authority_dir: PathBuf,
    /// Canonical private (`0700`) approved host sessions root directory.
    pub sessions_dir: PathBuf,
}

/// Exact v1 private operator context JSON document consumed by `threadlane-context`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportedPrivateContextV1 {
    /// Schema version (must be `1`).
    pub schema_version: u32,
    /// Verified canonical repository root directory (`checked.cwd`).
    pub project_root: PathBuf,
    /// Verified enrolled chat binding identifier.
    pub binding_id: String,
    /// Verified enrolled canonical lowercase hyphenated non-nil native thread UUID.
    pub thread_uuid: String,
    /// Fixed route identifier (`"trusted_local_operator"`).
    pub route: String,
    /// Always `false`: local CLI / paired MCP does not prove per-call conversation identity.
    pub native_per_call_identity_verified: bool,
    /// Canonical private (`0700`) store directories.
    pub stores: ExportedPrivateContextStoresV1,
    /// Optional canonical CLI package root (`<pkg>` when `threadlane-context` resides in `<pkg>/bin`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cli_package: Option<PathBuf>,
}

/// Server-owned store and binary configuration supplied to automatic pairing for context export.
#[derive(Debug, Clone, Copy)]
pub struct OperatorContextServerConfig<'a> {
    /// Private (`0700`) pairing store directory owning the `operator-contexts` child directory.
    pub pairing_dir: &'a Path,
    /// Private (`0700`) supervisor runtime state directory.
    pub state_dir: &'a Path,
    /// Optional `(registry_dir, plans_dir)` from configured workflow stores.
    pub workflow_stores: Option<(&'a Path, &'a Path)>,
    /// Optional `(authority_dir, sessions_dir)` from configured host stores.
    pub host_stores: Option<(&'a Path, &'a Path)>,
    /// Optional anchor executable path overriding `std::env::current_exe()` for same-package sibling resolution.
    pub sibling_anchor_exe: Option<&'a Path>,
}

impl<'a> OperatorContextServerConfig<'a> {
    /// Constructs a server config without `workflow_stores` or `host_stores` (used by minimal pairing setups).
    pub const fn unconfigured(state_dir: &'a Path, pairing_dir: &'a Path) -> Self {
        Self {
            pairing_dir,
            state_dir,
            workflow_stores: None,
            host_stores: None,
            sibling_anchor_exe: None,
        }
    }
}

/// Verified input parameters for exporting a per-chat v1 private operator context.
#[derive(Debug, Clone)]
pub struct OperatorContextExportRequest<'a> {
    /// Verified enrolled chat binding identifier.
    pub binding_id: &'a str,
    /// Verified enrolled canonical native thread UUID.
    pub thread_uuid: &'a str,
    /// Verified canonical repository root directory (`checked.cwd`).
    pub verified_project_root: &'a Path,
    /// Server-owned store and sibling binary configuration.
    pub server_config: OperatorContextServerConfig<'a>,
}

/// Validated result of exporting (or reusing) a per-chat v1 private operator context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportedOperatorContext {
    /// Canonical absolute path to the `0600` private operator context JSON file.
    pub context_path: PathBuf,
    /// Canonical absolute path to the same-package `threadlane-context` router executable.
    pub context_router_executable: PathBuf,
    /// SHA-256 hex digest of the canonical serialized context JSON bytes.
    pub content_sha256: String,
    /// Whether an existing identical `0600` context file was reused without rewriting.
    pub reused_existing: bool,
    /// The validated v1 context payload written to or verified at `context_path`.
    pub document: ExportedPrivateContextV1,
}

/// Additive structured bootstrap status for the per-chat v1 trusted-local operator context
/// returned by `connect_current_chat`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorContextBootstrap {
    /// Whether a validated per-chat v1 private operator context file and canonical sibling
    /// `threadlane-context` executable are available for this connected chat.
    pub available: bool,
    /// High-level availability status (`"ready"`, `"not_configured"`, or `"unavailable"`).
    pub status: String,
    /// Structured error/reason code when `available` is `false` (`None` when `available` is `true`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Truthful, sanitized, actionable explanation when `available` is `false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Context schema version (`Some(1)` when `available` is `true`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_version: Option<u32>,
    /// Always `"trusted_local_operator"`.
    pub route: String,
    /// Always `false`: local CLI and paired MCP do not cryptographically prove per-call chat identity.
    pub native_per_call_identity_verified: bool,
    /// Always `false`: connecting a chat or exporting a context never implies human approval.
    pub human_approval_verified: bool,
    /// Canonical absolute path to the `0600` private operator context JSON file when `available` is `true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_path: Option<PathBuf>,
    /// Canonical absolute path to the same-package `threadlane-context` router executable when `available` is `true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_router_executable: Option<PathBuf>,
    /// Verified canonical repository working directory (`project_root`) when `available` is `true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_root: Option<PathBuf>,
    /// Optional canonical CLI package root (`<pkg>` when `threadlane-context` resides in `<pkg>/bin`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cli_package: Option<PathBuf>,
    /// SHA-256 hex digest of the canonical exported context JSON bytes when `available` is `true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_sha256: Option<String>,
    /// Whether an existing identical `0600` context export was reused without rewriting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reused_existing: Option<bool>,
}

impl OperatorContextBootstrap {
    /// Builds a ready bootstrap descriptor from a validated [`ExportedOperatorContext`].
    pub fn ready(exported: ExportedOperatorContext) -> Self {
        Self {
            available: true,
            status: "ready".to_string(),
            code: None,
            message: None,
            schema_version: Some(exported.document.schema_version),
            route: TRUSTED_LOCAL_OPERATOR_ROUTE.to_string(),
            native_per_call_identity_verified: false,
            human_approval_verified: false,
            context_path: Some(exported.context_path),
            context_router_executable: Some(exported.context_router_executable),
            project_root: Some(exported.document.project_root),
            cli_package: exported.document.cli_package,
            content_sha256: Some(exported.content_sha256),
            reused_existing: Some(exported.reused_existing),
        }
    }

    /// Builds a truthful unavailable/not-configured bootstrap descriptor from a sanitized [`NativeError`].
    pub fn unavailable(error: &NativeError) -> Self {
        let status = if error.error_code() == NativeErrorCode::NotConfigured {
            "not_configured"
        } else {
            "unavailable"
        };
        Self {
            available: false,
            status: status.to_string(),
            code: Some(error.code().to_string()),
            message: Some(error.message().to_string()),
            schema_version: None,
            route: TRUSTED_LOCAL_OPERATOR_ROUTE.to_string(),
            native_per_call_identity_verified: false,
            human_approval_verified: false,
            context_path: None,
            context_router_executable: None,
            project_root: None,
            cli_package: None,
            content_sha256: None,
            reused_existing: None,
        }
    }
}

/// Exports or reuses a per-chat v1 private operator context and returns an [`OperatorContextBootstrap`]
/// suitable for embedding in `connect_current_chat` without breaking ordinary connection compatibility
/// when optional stores or sibling binaries are not configured.
pub fn bootstrap_verified_operator_context(
    request: &OperatorContextExportRequest<'_>,
) -> OperatorContextBootstrap {
    match export_verified_operator_context(request) {
        Ok(exported) => OperatorContextBootstrap::ready(exported),
        Err(err) => OperatorContextBootstrap::unavailable(&err),
    }
}

/// Validates all stores, `project_root`, `(binding_id, thread_uuid)`, and the canonical same-package
/// `threadlane-context` binary, then atomically writes (or reuses without rewriting) the private
/// `0600` context file under `<pairing_dir>/operator-contexts/`.
pub fn export_verified_operator_context(
    request: &OperatorContextExportRequest<'_>,
) -> Result<ExportedOperatorContext, NativeError> {
    ensure_platform_supported()?;

    let checked_binding_id =
        validate_identifier("binding_id", request.binding_id).map_err(|_| {
            NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Verified binding_id failed identifier syntax validation for operator context export.",
        )
        })?;
    let checked_thread_uuid = validate_canonical_thread_uuid("thread_uuid", request.thread_uuid)
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Verified thread_uuid must be a canonical lowercase hyphenated non-nil UUID.",
            )
        })?;

    let Some((registry_dir, plans_dir)) = request.server_config.workflow_stores else {
        return Err(NativeError::new(
            NativeErrorCode::NotConfigured,
            "Operator context export requires configured workflow_stores (registry_dir and plans_dir) on PairedMcpServer.",
        ));
    };
    let Some((authority_dir, sessions_dir)) = request.server_config.host_stores else {
        return Err(NativeError::new(
            NativeErrorCode::NotConfigured,
            "Operator context export requires configured host_stores (authority_dir and sessions_dir) on PairedMcpServer.",
        ));
    };

    // Validate pairing_dir as an existing canonical private 0700 directory without creating or repairing it.
    let validated_pairing = validate_dirs(&[("pairing_dir", request.server_config.pairing_dir)])
        .map_err(|err| map_dir_validation_error("pairing_dir", err))?;
    let clean_pairing_dir = validated_pairing[0].clone();

    // Validate the five required private store directories (existing, canonical, 0700, pairwise disjoint/non-overlapping).
    let validated_stores = validate_dirs(&[
        ("state_dir", request.server_config.state_dir),
        ("registry_dir", registry_dir),
        ("plans_dir", plans_dir),
        ("authority_dir", authority_dir),
        ("sessions_dir", sessions_dir),
    ])
    .map_err(|err| map_dir_validation_error("stores", err))?;

    let clean_state_dir = validated_stores[0].clone();
    let clean_registry_dir = validated_stores[1].clone();
    let clean_plans_dir = validated_stores[2].clone();
    let clean_authority_dir = validated_stores[3].clone();
    let clean_sessions_dir = validated_stores[4].clone();

    for store_dir in &validated_stores {
        if paths_overlap(&clean_pairing_dir, store_dir) {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "Pairing directory must not equal or overlap any private operator store directory.",
            ));
        }
    }

    // Validate verified_project_root as an existing canonical non-symlink directory (need not be 0700, never chmodded).
    let clean_project_root =
        validate_existing_canonical_non_symlink_dir(request.verified_project_root, "project_root")?;

    if paths_overlap(&clean_project_root, &clean_pairing_dir) {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "Verified project_root must not equal or overlap the private pairing directory.",
        ));
    }
    for store_dir in &validated_stores {
        if paths_overlap(&clean_project_root, store_dir) {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "Verified project_root must not equal or overlap any private operator store directory.",
            ));
        }
    }

    // Resolve and validate the canonical same-package threadlane-context executable and optional cli_package root.
    let (router_executable, cli_package) =
        resolve_canonical_context_router_executable(request.server_config.sibling_anchor_exe)?;

    let document = ExportedPrivateContextV1 {
        schema_version: OPERATOR_CONTEXT_SCHEMA_VERSION,
        project_root: clean_project_root,
        binding_id: checked_binding_id.clone(),
        thread_uuid: checked_thread_uuid.clone(),
        route: TRUSTED_LOCAL_OPERATOR_ROUTE.to_string(),
        native_per_call_identity_verified: false,
        stores: ExportedPrivateContextStoresV1 {
            state_dir: clean_state_dir,
            registry_dir: clean_registry_dir,
            plans_dir: clean_plans_dir,
            authority_dir: clean_authority_dir,
            sessions_dir: clean_sessions_dir,
        },
        cli_package,
    };

    let mut serialized = serde_json::to_vec_pretty(&document).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "Failed to serialize v1 operator context JSON.",
        )
    })?;
    serialized.push(b'\n');

    if serialized.len() > MAX_PRIVATE_STATE_FILE_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            "Serialized v1 operator context exceeds maximum private file byte budget.",
        ));
    }

    let content_sha256 = sha256_hex(&serialized);
    let filename = deterministic_context_filename(
        &checked_binding_id,
        &checked_thread_uuid,
        &router_executable,
        &content_sha256,
    );

    let export_dir = ensure_owned_private_export_dir(&clean_pairing_dir)?;
    let _lock = acquire_cross_process_lock(
        &export_dir,
        CONTEXT_EXPORT_LOCK_FILE,
        "operator_context_export_lock",
    )?;

    let target_path = export_dir.join(&filename);
    validate_private_file_path(&target_path, "operator_context")?;

    match fs::symlink_metadata(&target_path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() || !meta.file_type().is_file() {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "Existing operator context export path is not a regular non-symlink file.",
                ));
            }
            let existing_bytes = read_private_file_bounded(
                &target_path,
                MAX_PRIVATE_STATE_FILE_BYTES,
                "operator_context",
            )?
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "Existing operator context export file disappeared during verification.",
                )
            })?;

            if existing_bytes != serialized {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "Existing operator context file contains unexpected or foreign content; refusing to overwrite.",
                ));
            }

            let parsed: ExportedPrivateContextV1 = serde_json::from_slice(&existing_bytes)
                .map_err(|_| {
                    NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "Existing operator context file failed schema verification; refusing to overwrite.",
                    )
                })?;
            if parsed != document {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "Existing operator context file does not match verified chat context; refusing to overwrite.",
                ));
            }

            Ok(ExportedOperatorContext {
                context_path: target_path,
                context_router_executable: router_executable,
                content_sha256,
                reused_existing: true,
                document,
            })
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            let written_path =
                write_private_file_atomic(&export_dir, &filename, &serialized, "operator_context")?;
            Ok(ExportedOperatorContext {
                context_path: written_path,
                context_router_executable: router_executable,
                content_sha256,
                reused_existing: false,
                document,
            })
        }
        Err(_) => Err(NativeError::new(
            NativeErrorCode::IoError,
            "Failed to inspect operator context export file metadata.",
        )),
    }
}

/// Computes the deterministic filename for an exported operator context under
/// `<pairing_dir>/operator-contexts/`.
pub fn deterministic_context_filename(
    binding_id: &str,
    thread_uuid: &str,
    router_executable: &Path,
    content_sha256: &str,
) -> String {
    let binding_tag = short_sha256_tag(
        "threadlane-native:operator-context-binding:v1",
        &[binding_id, thread_uuid],
    );
    let router_str = router_executable.to_string_lossy();
    let package_content_tag = short_sha256_tag(
        "threadlane-native:operator-context-content:v1",
        &[binding_id, thread_uuid, router_str.as_ref(), content_sha256],
    );
    format!("context-{binding_tag}-{package_content_tag}.json")
}

fn ensure_owned_private_export_dir(clean_pairing_dir: &Path) -> Result<PathBuf, NativeError> {
    let export_dir = clean_pairing_dir.join(OPERATOR_CONTEXTS_DIR_NAME);
    match fs::symlink_metadata(&export_dir) {
        Ok(meta) => {
            if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "operator-contexts path under pairing_dir must be a non-symlink directory.",
                ));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if (meta.permissions().mode() & 0o077) != 0 {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "operator-contexts directory has insecure permissions (expected 0700).",
                    ));
                }
            }
            let validated = validate_dirs(&[("operator_contexts_dir", &export_dir)])
                .map_err(|err| map_dir_validation_error("operator_contexts_dir", err))?;
            Ok(validated[0].clone())
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            prepare_private_dir(&export_dir, "operator_contexts_dir")
        }
        Err(_) => Err(NativeError::new(
            NativeErrorCode::IoError,
            "Failed to inspect operator-contexts directory metadata.",
        )),
    }
}

fn resolve_canonical_context_router_executable(
    sibling_anchor_exe: Option<&Path>,
) -> Result<(PathBuf, Option<PathBuf>), NativeError> {
    let anchor_exe = match sibling_anchor_exe {
        Some(custom) => validate_existing_canonical_executable_file(custom, "sibling_anchor_exe")?,
        None => {
            let current = std::env::current_exe().map_err(|_| {
                NativeError::new(
                    NativeErrorCode::NotConfigured,
                    "Failed to determine current executable path for operator context export.",
                )
            })?;
            validate_existing_canonical_executable_file(&current, "current_exe")?
        }
    };

    let bin_dir = anchor_exe.parent().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::NotConfigured,
            "Active executable has no parent directory for sibling resolution.",
        )
    })?;
    let clean_bin_dir = validate_existing_canonical_non_symlink_dir(bin_dir, "binary_dir")?;
    let candidate = clean_bin_dir.join(CONTEXT_ROUTER_BINARY_NAME);

    let meta = match fs::symlink_metadata(&candidate) {
        Ok(meta) => meta,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            return Err(NativeError::new(
                NativeErrorCode::NotConfigured,
                "Canonical sibling threadlane-context executable is not present in the active binary directory.",
            ));
        }
        Err(_) => {
            return Err(NativeError::new(
                NativeErrorCode::IoError,
                "Failed to inspect sibling threadlane-context executable metadata.",
            ));
        }
    };

    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "Sibling threadlane-context path must be a regular non-symlink file.",
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if (meta.permissions().mode() & 0o111) == 0 {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "Sibling threadlane-context file is not marked executable.",
            ));
        }
    }

    let canonical_router = fs::canonicalize(&candidate).map_err(|_| {
        NativeError::new(
            NativeErrorCode::SecurityViolation,
            "Failed to canonicalize sibling threadlane-context executable.",
        )
    })?;
    if canonical_router != candidate || canonical_router.parent() != Some(clean_bin_dir.as_path()) {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "Sibling threadlane-context executable resolved outside the canonical binary directory.",
        ));
    }

    let cli_package = if clean_bin_dir.file_name() == Some(OsStr::new("bin")) {
        if let Some(pkg_parent) = clean_bin_dir.parent() {
            let clean_pkg = validate_existing_canonical_non_symlink_dir(pkg_parent, "cli_package")?;
            if clean_pkg.join("bin") == clean_bin_dir {
                Some(clean_pkg)
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    Ok((canonical_router, cli_package))
}

fn validate_existing_canonical_executable_file(
    path: &Path,
    label: &str,
) -> Result<PathBuf, NativeError> {
    if path.as_os_str().is_empty() || !path.is_absolute() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{label} must be a non-empty absolute file path."),
        ));
    }
    let clean = validate_absolute_no_symlink_components(path, label)?;
    let meta = fs::symlink_metadata(&clean).map_err(|err| {
        if err.kind() == io::ErrorKind::NotFound {
            NativeError::new(
                NativeErrorCode::NotConfigured,
                format!("Required {label} executable does not exist."),
            )
        } else {
            NativeError::new(
                NativeErrorCode::IoError,
                format!("Failed to inspect {label} metadata."),
            )
        }
    })?;
    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!("{label} must be a regular non-symlink file."),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if (meta.permissions().mode() & 0o111) == 0 {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!("{label} is not marked executable."),
            ));
        }
    }
    let canonical = fs::canonicalize(&clean).map_err(|_| {
        NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!("Failed to canonicalize {label} path."),
        )
    })?;
    if canonical != clean {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!("{label} path is not canonical."),
        ));
    }
    Ok(canonical)
}

#[cfg(unix)]
fn validate_existing_canonical_non_symlink_dir(
    path: &Path,
    label: &str,
) -> Result<PathBuf, NativeError> {
    use std::os::unix::ffi::OsStrExt;

    let raw = path.as_os_str().as_bytes();
    if raw.is_empty() || !path.is_absolute() || (raw.len() > 1 && raw.ends_with(b"/")) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{label} must be a normalized absolute directory path."),
        ));
    }

    let mut normal_parts: Vec<&OsStr> = Vec::new();
    let mut saw_root = false;
    for component in path.components() {
        match component {
            Component::RootDir if !saw_root && normal_parts.is_empty() => {
                saw_root = true;
            }
            Component::Normal(part) if saw_root && !part.is_empty() => {
                normal_parts.push(part);
            }
            Component::CurDir | Component::ParentDir => {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    format!("{label} must not contain relative or traversal components."),
                ));
            }
            _ => {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("{label} must be a normalized absolute directory path."),
                ));
            }
        }
    }

    if !saw_root || normal_parts.is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{label} must be a normalized absolute directory path."),
        ));
    }

    let mut reconstructed = PathBuf::from("/");
    for part in &normal_parts {
        reconstructed.push(part);
    }
    if reconstructed.as_os_str() != path.as_os_str() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{label} must be normalized without redundant separators."),
        ));
    }

    let clean = validate_absolute_no_symlink_components(&reconstructed, label)?;
    let meta = fs::symlink_metadata(&clean).map_err(|err| {
        if err.kind() == io::ErrorKind::NotFound {
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
            format!("{label} must be an existing non-symlink directory."),
        ));
    }

    let canonical = fs::canonicalize(&clean).map_err(|_| {
        NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!("Failed to canonicalize {label} directory."),
        )
    })?;
    if canonical != clean {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!("{label} directory path is not canonical."),
        ));
    }
    Ok(canonical)
}

#[cfg(not(unix))]
fn validate_existing_canonical_non_symlink_dir(
    _path: &Path,
    _label: &str,
) -> Result<PathBuf, NativeError> {
    Err(NativeError::new(
        NativeErrorCode::UnsupportedPlatform,
        "Trusted local operator context export is only supported on Unix platforms.",
    ))
}

fn map_dir_validation_error(label: &str, err: OperatorCliError) -> NativeError {
    match err.code {
        "security_violation" => NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!(
                "{label} failed private directory security validation: {}",
                err.message
            ),
        ),
        "io_error" => NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "{label} I/O error during directory validation: {}",
                err.message
            ),
        ),
        "unsupported_platform" => {
            NativeError::new(NativeErrorCode::UnsupportedPlatform, err.message)
        }
        _ => {
            if err.message.contains("does not exist") {
                NativeError::new(
                    NativeErrorCode::NotConfigured,
                    format!("Required {label} directory does not exist."),
                )
            } else {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("{label} failed directory validation: {}", err.message),
                )
            }
        }
    }
}

fn paths_overlap(a: &Path, b: &Path) -> bool {
    a == b || a.starts_with(b) || b.starts_with(a)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn short_sha256_tag(domain: &str, parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain.as_bytes());
    hasher.update([0u8]);
    for part in parts {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
        hasher.update([0u8]);
    }
    let full = format!("{:x}", hasher.finalize());
    full[..16].to_string()
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::{symlink, MetadataExt, PermissionsExt};
    use tempfile::TempDir;

    use super::*;

    const THREAD_A: &str = "0196ed00-0000-7000-8000-0000000000a1";
    const THREAD_B: &str = "0196ed00-0000-7000-8000-0000000000b2";
    const SECRET_SENTINEL: &str = "SECRET_CONTEXT_EXPORT_SENTINEL_99182";

    struct ExportFixture {
        _tmp: TempDir,
        root: PathBuf,
        project_a: PathBuf,
        project_b: PathBuf,
        pairing_dir: PathBuf,
        state_dir: PathBuf,
        registry_dir: PathBuf,
        plans_dir: PathBuf,
        authority_dir: PathBuf,
        sessions_dir: PathBuf,
        pkg_a_dir: PathBuf,
        anchor_a_exe: PathBuf,
        router_a_exe: PathBuf,
        pkg_b_dir: PathBuf,
        anchor_b_exe: PathBuf,
        router_b_exe: PathBuf,
    }

    impl ExportFixture {
        fn new() -> Self {
            let tmp = TempDir::new().expect("tempdir");
            let root = fs::canonicalize(tmp.path()).expect("canonicalize root");
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("chmod root 0700");

            let project_a = root.join("workspace-a");
            let project_b = root.join("workspace-b");
            fs::create_dir_all(&project_a).expect("create project_a");
            fs::create_dir_all(&project_b).expect("create project_b");
            fs::set_permissions(&project_a, fs::Permissions::from_mode(0o755))
                .expect("chmod project_a 0755");
            fs::set_permissions(&project_b, fs::Permissions::from_mode(0o755))
                .expect("chmod project_b 0755");

            let pairing_dir = Self::make_private_dir(&root, "pairing");
            let state_dir = Self::make_private_dir(&root, "state");
            let registry_dir = Self::make_private_dir(&root, "registry");
            let plans_dir = Self::make_private_dir(&root, "plans");
            let authority_dir = Self::make_private_dir(&root, "authority");
            let sessions_dir = Self::make_private_dir(&root, "sessions");

            // Write a secret token inside pairing_dir and state_dir to prove it is never exported.
            fs::write(pairing_dir.join("operator.token"), SECRET_SENTINEL)
                .expect("write operator.token");
            fs::set_permissions(
                pairing_dir.join("operator.token"),
                fs::Permissions::from_mode(0o600),
            )
            .expect("chmod operator.token 0600");

            let pkg_a_dir = root.join("pkg-a");
            let pkg_a_bin_dir = pkg_a_dir.join("bin");
            fs::create_dir_all(&pkg_a_bin_dir).expect("create pkg_a/bin");
            fs::set_permissions(&pkg_a_dir, fs::Permissions::from_mode(0o755))
                .expect("chmod pkg_a 0755");
            fs::set_permissions(&pkg_a_bin_dir, fs::Permissions::from_mode(0o755))
                .expect("chmod pkg_a/bin 0755");
            let anchor_a_exe =
                Self::write_exec(&pkg_a_bin_dir, "threadlane-mcp", "#!/bin/sh\nexit 0\n");
            let router_a_exe =
                Self::write_exec(&pkg_a_bin_dir, "threadlane-context", "#!/bin/sh\nexit 0\n");

            let pkg_b_dir = root.join("pkg-b");
            let pkg_b_bin_dir = pkg_b_dir.join("bin");
            fs::create_dir_all(&pkg_b_bin_dir).expect("create pkg_b/bin");
            fs::set_permissions(&pkg_b_dir, fs::Permissions::from_mode(0o755))
                .expect("chmod pkg_b 0755");
            fs::set_permissions(&pkg_b_bin_dir, fs::Permissions::from_mode(0o755))
                .expect("chmod pkg_b/bin 0755");
            let anchor_b_exe =
                Self::write_exec(&pkg_b_bin_dir, "threadlane-mcp", "#!/bin/sh\nexit 0\n");
            let router_b_exe =
                Self::write_exec(&pkg_b_bin_dir, "threadlane-context", "#!/bin/sh\nexit 0\n");

            Self {
                _tmp: tmp,
                root,
                project_a,
                project_b,
                pairing_dir,
                state_dir,
                registry_dir,
                plans_dir,
                authority_dir,
                sessions_dir,
                pkg_a_dir,
                anchor_a_exe,
                router_a_exe,
                pkg_b_dir,
                anchor_b_exe,
                router_b_exe,
            }
        }

        fn make_private_dir(root: &Path, name: &str) -> PathBuf {
            let dir = root.join(name);
            fs::create_dir_all(&dir).expect("create private dir");
            fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))
                .expect("chmod private dir 0700");
            dir
        }

        fn write_exec(dir: &Path, name: &str, content: &str) -> PathBuf {
            let p = dir.join(name);
            fs::write(&p, content.as_bytes()).expect("write executable");
            fs::set_permissions(&p, fs::Permissions::from_mode(0o755))
                .expect("chmod executable 0755");
            p
        }

        fn server_config<'a>(&'a self) -> OperatorContextServerConfig<'a> {
            OperatorContextServerConfig {
                pairing_dir: &self.pairing_dir,
                state_dir: &self.state_dir,
                workflow_stores: Some((&self.registry_dir, &self.plans_dir)),
                host_stores: Some((&self.authority_dir, &self.sessions_dir)),
                sibling_anchor_exe: Some(&self.anchor_a_exe),
            }
        }
    }

    #[test]
    fn two_chats_export_distinct_private_contexts_and_repeats_reuse_without_rewrite() {
        let fx = ExportFixture::new();

        let req_a = OperatorContextExportRequest {
            binding_id: "bnd_chat_a",
            thread_uuid: THREAD_A,
            verified_project_root: &fx.project_a,
            server_config: fx.server_config(),
        };
        let req_b = OperatorContextExportRequest {
            binding_id: "bnd_chat_b",
            thread_uuid: THREAD_B,
            verified_project_root: &fx.project_b,
            server_config: fx.server_config(),
        };

        let exp_a1 = export_verified_operator_context(&req_a).expect("export chat A");
        assert!(!exp_a1.reused_existing);
        assert_eq!(exp_a1.context_router_executable, fx.router_a_exe);
        assert_eq!(exp_a1.document.schema_version, 1);
        assert_eq!(exp_a1.document.binding_id, "bnd_chat_a");
        assert_eq!(exp_a1.document.thread_uuid, THREAD_A);
        assert_eq!(exp_a1.document.project_root, fx.project_a);
        assert_eq!(exp_a1.document.route, "trusted_local_operator");
        assert!(!exp_a1.document.native_per_call_identity_verified);
        assert_eq!(
            exp_a1.document.cli_package.as_deref(),
            Some(fx.pkg_a_dir.as_path())
        );

        let export_dir = fx.pairing_dir.join(OPERATOR_CONTEXTS_DIR_NAME);
        let export_dir_mode = fs::symlink_metadata(&export_dir)
            .expect("stat export_dir")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(export_dir_mode, 0o700);

        let meta_a1 = fs::symlink_metadata(&exp_a1.context_path).expect("stat context A");
        assert_eq!(meta_a1.permissions().mode() & 0o777, 0o600);
        let ino_a1 = meta_a1.ino();
        let mtime_a1 = (meta_a1.mtime(), meta_a1.mtime_nsec());

        let raw_a = fs::read_to_string(&exp_a1.context_path).expect("read context A");
        assert!(!raw_a.contains(SECRET_SENTINEL));
        assert!(!raw_a.contains("operator.token"));
        assert!(!raw_a.contains("human_approval"));

        // Repeat identical connect for chat A: must reuse identical file without rewriting.
        let exp_a2 = export_verified_operator_context(&req_a).expect("repeat export chat A");
        assert!(exp_a2.reused_existing);
        assert_eq!(exp_a2.context_path, exp_a1.context_path);
        assert_eq!(exp_a2.content_sha256, exp_a1.content_sha256);
        let meta_a2 = fs::symlink_metadata(&exp_a2.context_path).expect("stat context A repeat");
        assert_eq!(
            meta_a2.ino(),
            ino_a1,
            "idempotent repeat must not replace inode"
        );
        assert_eq!(
            (meta_a2.mtime(), meta_a2.mtime_nsec()),
            mtime_a1,
            "idempotent repeat must not rewrite file"
        );

        // Export chat B: distinct file, distinct binding/thread/project_root, chat A untouched.
        let exp_b1 = export_verified_operator_context(&req_b).expect("export chat B");
        assert!(!exp_b1.reused_existing);
        assert_ne!(exp_b1.context_path, exp_a1.context_path);
        assert_eq!(exp_b1.document.binding_id, "bnd_chat_b");
        assert_eq!(exp_b1.document.thread_uuid, THREAD_B);
        assert_eq!(exp_b1.document.project_root, fx.project_b);
    }

    #[test]
    fn package_change_writes_distinct_context_without_overwriting_prior_package_export() {
        let fx = ExportFixture::new();

        let req_pkg_a = OperatorContextExportRequest {
            binding_id: "bnd_chat_a",
            thread_uuid: THREAD_A,
            verified_project_root: &fx.project_a,
            server_config: fx.server_config(),
        };
        let exp_a = export_verified_operator_context(&req_pkg_a).expect("export pkg A");
        let bytes_a_before = fs::read(&exp_a.context_path).expect("read pkg A context");

        let mut cfg_b = fx.server_config();
        cfg_b.sibling_anchor_exe = Some(&fx.anchor_b_exe);
        let req_pkg_b = OperatorContextExportRequest {
            binding_id: "bnd_chat_a",
            thread_uuid: THREAD_A,
            verified_project_root: &fx.project_a,
            server_config: cfg_b,
        };
        let exp_b = export_verified_operator_context(&req_pkg_b).expect("export pkg B");
        assert!(!exp_b.reused_existing);
        assert_ne!(exp_b.context_path, exp_a.context_path);
        assert_eq!(exp_b.context_router_executable, fx.router_b_exe);
        assert_eq!(
            exp_b.document.cli_package.as_deref(),
            Some(fx.pkg_b_dir.as_path())
        );

        let bytes_a_after = fs::read(&exp_a.context_path).expect("re-read pkg A context");
        assert_eq!(
            bytes_a_before, bytes_a_after,
            "package change must never overwrite prior package context file"
        );
    }

    #[test]
    fn foreign_content_symlinks_insecure_permissions_and_missing_stores_fail_closed_without_leaking_secrets(
    ) {
        let fx = ExportFixture::new();

        // 1. Missing workflow_stores or host_stores returns truthful NotConfigured bootstrap.
        let unconfigured_req = OperatorContextExportRequest {
            binding_id: "bnd_chat_a",
            thread_uuid: THREAD_A,
            verified_project_root: &fx.project_a,
            server_config: OperatorContextServerConfig::unconfigured(
                &fx.state_dir,
                &fx.pairing_dir,
            ),
        };
        let bootstrap = bootstrap_verified_operator_context(&unconfigured_req);
        assert!(!bootstrap.available);
        assert_eq!(bootstrap.status, "not_configured");
        assert_eq!(bootstrap.code.as_deref(), Some("not_configured"));
        assert!(bootstrap.context_path.is_none());
        assert!(bootstrap.context_router_executable.is_none());

        // 2. Valid export followed by foreign-content tampering at the target path is refused without overwrite or secret echo.
        let req = OperatorContextExportRequest {
            binding_id: "bnd_chat_a",
            thread_uuid: THREAD_A,
            verified_project_root: &fx.project_a,
            server_config: fx.server_config(),
        };
        let exported = export_verified_operator_context(&req).expect("initial export");
        let foreign_payload = format!(r#"{{"foreign_secret": "{SECRET_SENTINEL}"}}"#);
        fs::write(&exported.context_path, foreign_payload.as_bytes()).expect("tamper context file");
        fs::set_permissions(&exported.context_path, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 tampered file");

        let foreign_err = export_verified_operator_context(&req)
            .expect_err("foreign content at target path must fail closed");
        assert_eq!(foreign_err.error_code(), NativeErrorCode::SecurityViolation);
        assert!(!foreign_err.message().contains(SECRET_SENTINEL));
        assert_eq!(
            fs::read_to_string(&exported.context_path).expect("read preserved foreign file"),
            foreign_payload,
            "foreign file must never be overwritten"
        );

        // 3. Insecure 0644 permissions on existing context file are rejected without repairing permissions.
        fs::set_permissions(&exported.context_path, fs::Permissions::from_mode(0o644))
            .expect("chmod 0644");
        let mode_err =
            export_verified_operator_context(&req).expect_err("0644 context file must fail");
        assert_eq!(mode_err.error_code(), NativeErrorCode::SecurityViolation);
        assert_eq!(
            fs::symlink_metadata(&exported.context_path)
                .expect("stat 0644 file")
                .permissions()
                .mode()
                & 0o777,
            0o644
        );
        fs::remove_file(&exported.context_path).expect("clean up tampered file");

        // 4. Symlink at target context path is rejected without following or overwriting.
        let outside_target = fx.root.join("outside-user-file.json");
        fs::write(&outside_target, SECRET_SENTINEL.as_bytes()).expect("write outside target");
        symlink(&outside_target, &exported.context_path).expect("symlink context path");
        let symlink_err =
            export_verified_operator_context(&req).expect_err("symlink target must fail");
        assert_eq!(symlink_err.error_code(), NativeErrorCode::SecurityViolation);
        assert_eq!(
            fs::read_to_string(&outside_target).expect("outside target preserved"),
            SECRET_SENTINEL
        );
        fs::remove_file(&exported.context_path).expect("remove symlink");

        // 5. Symlink or non-executable sibling threadlane-context is rejected.
        fs::set_permissions(&fx.router_a_exe, fs::Permissions::from_mode(0o600))
            .expect("chmod router 0600");
        let non_exec_err =
            export_verified_operator_context(&req).expect_err("non-executable router must fail");
        assert_eq!(
            non_exec_err.error_code(),
            NativeErrorCode::SecurityViolation
        );
        fs::set_permissions(&fx.router_a_exe, fs::Permissions::from_mode(0o755))
            .expect("restore router 0755");
    }
}
