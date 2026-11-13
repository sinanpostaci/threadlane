//! Safe startup and CLI installer for the bundled user-level Threadlane Codex skill.
//!
//! Manages only `SKILL.md`, `agents/openai.yaml`, and the private ownership marker
//! `.threadlane-managed.json` inside `<skills_dir>/threadlane`. Preserves unrelated user
//! skills, unknown extra files inside the skill directory, parent directory modes, and
//! Codex configuration files.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

use crate::codex_setup::CodexSetupMode;
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::validate_absolute_no_symlink_components;

/// Bundled Threadlane Codex skill version identifier.
pub const THREADLANE_SKILL_VERSION: &str = "2026-11-13.v1";

/// Directory and skill name for the bundled Threadlane Codex skill.
pub const THREADLANE_SKILL_NAME: &str = "threadlane";

const BUNDLED_SKILL_MD: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../skills/threadlane/SKILL.md"
));
const BUNDLED_OPENAI_YAML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../skills/threadlane/agents/openai.yaml"
));

const MARKER_FILE_NAME: &str = ".threadlane-managed.json";
const LOCK_FILE_NAME: &str = ".threadlane-skill-setup.lock";
const AGENTS_DIR_NAME: &str = "agents";
const SKILL_MD_REL_PATH: &str = "SKILL.md";
const OPENAI_YAML_REL_PATH: &str = "agents/openai.yaml";
const MARKER_SCHEMA_VERSION: u32 = 1;
const MARKER_OWNER: &str = "threadlane";
const MAX_MANAGED_FILE_BYTES: u64 = 64 * 1024;
const MAX_VERSION_BYTES: usize = 128;
const CONFLICT_ERROR_CODE: &str = "conflict";

/// High-level outcome state of bundled Threadlane Codex skill inspection or installation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexSkillSetupState {
    /// Bundled skill files and ownership marker were newly installed or adopted.
    Installed,
    /// Previously managed skill files or pending journal were updated to the current bundle.
    Updated,
    /// Bundled skill files and ownership marker already match the current bundle.
    Unchanged,
    /// Target skill directory is absent, empty, or lacks a Threadlane ownership marker in `check` mode.
    NotInstalled,
    /// Managed skill installation exists but requires an update or journal recovery in `check` mode.
    Outdated,
    /// Unmanaged files or user edits conflict with safe automatic installation or update.
    Conflict,
    /// Path validation, marker validation, lock acquisition, or filesystem I/O failed.
    Error,
}

impl CodexSkillSetupState {
    /// Returns the static `snake_case` wire representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Installed => "installed",
            Self::Updated => "updated",
            Self::Unchanged => "unchanged",
            Self::NotInstalled => "not_installed",
            Self::Outdated => "outdated",
            Self::Conflict => "conflict",
            Self::Error => "error",
        }
    }
}

/// Input parameters for inspecting or installing the bundled Threadlane Codex skill.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexSkillSetupRequest {
    /// Parent Codex skills directory (for example `~/.agents/skills`); target is `skills_dir/threadlane`.
    pub skills_dir: PathBuf,
}

/// Sanitized outcome report for bundled Threadlane Codex skill setup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexSkillSetupReport {
    /// High-level outcome state.
    pub state: CodexSkillSetupState,
    /// Inspection or application mode that produced this report.
    pub mode: CodexSetupMode,
    /// Target skill directory path (`<skills_dir>/threadlane`), when available.
    pub skill_path: Option<String>,
    /// Evaluated bundled skill version string.
    pub version: String,
    /// Whether the on-disk bundled skill payload files exactly match the desired bytes.
    ///
    /// Never implies Codex skill activation, live MCP pairing, or execution authority.
    pub installed: bool,
    /// Whether any managed resource file or ownership receipt was modified on disk.
    pub files_modified: bool,
    /// Stable machine-readable error code when `state` is `Conflict` or `Error`.
    pub error_code: Option<String>,
    /// Sanitized fixed diagnostic summary free of file contents, credentials, or config text.
    pub message: String,
}

impl CodexSkillSetupReport {
    /// Constructs a sanitized error report (for example when resolving the user home directory fails).
    pub fn error(
        mode: CodexSetupMode,
        skill_path: Option<String>,
        error_code: &'static str,
        message: &'static str,
    ) -> Self {
        Self {
            state: CodexSkillSetupState::Error,
            mode,
            skill_path,
            version: THREADLANE_SKILL_VERSION.to_string(),
            installed: false,
            files_modified: false,
            error_code: Some(error_code.to_string()),
            message: message.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillOwnershipMarker {
    schema_version: u32,
    owner: String,
    version: String,
    pending: bool,
    files: BTreeMap<String, ManagedFileRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagedFileRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    previous_sha256: Option<String>,
    desired_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileIdentity {
    dev: u64,
    ino: u64,
    len: u64,
    mtime: i64,
    mtime_nsec: i64,
    sha256: String,
}

#[derive(Debug, Clone)]
struct ValidatedPaths {
    skills_dir: PathBuf,
    skill_dir: PathBuf,
    agents_dir: PathBuf,
    skill_md_path: PathBuf,
    openai_yaml_path: PathBuf,
    marker_path: PathBuf,
    lock_path: PathBuf,
}

/// Resolves the default user-level Codex skills directory (`$HOME/.agents/skills`, falling back
/// to `$USERPROFILE/.agents/skills`).
pub fn resolve_codex_skills_dir() -> Result<PathBuf, NativeError> {
    let home = std::env::var_os("HOME");
    let userprofile = std::env::var_os("USERPROFILE");
    resolve_codex_skills_dir_with_env(home.as_deref(), userprofile.as_deref())
}

pub(crate) fn resolve_codex_skills_dir_with_env(
    home: Option<&OsStr>,
    userprofile: Option<&OsStr>,
) -> Result<PathBuf, NativeError> {
    let selected_home = home
        .filter(|val| !val.is_empty())
        .or_else(|| userprofile.filter(|val| !val.is_empty()))
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::InvalidConfig,
                "failed to resolve user home directory from HOME or USERPROFILE",
            )
        })?;
    let home_path = Path::new(selected_home);
    validate_clean_path_syntax(home_path, "user home directory")?;
    let validated_home = validate_absolute_no_symlink_components(home_path, "user home directory")?;
    if validated_home.parent().is_none() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "user home directory must not be the filesystem root",
        ));
    }
    let skills_dir = validated_home.join(".agents").join("skills");
    validate_clean_path_syntax(&skills_dir, "Codex skills directory")?;
    validate_absolute_no_symlink_components(&skills_dir, "Codex skills directory")
}

/// Inspects or installs the compile-time bundled Threadlane Codex skill under `request.skills_dir`.
pub fn run_codex_skill_setup(
    request: &CodexSkillSetupRequest,
    mode: CodexSetupMode,
) -> CodexSkillSetupReport {
    run_with_bundle(
        request,
        mode,
        THREADLANE_SKILL_VERSION,
        BUNDLED_SKILL_MD,
        BUNDLED_OPENAI_YAML,
    )
}

pub(crate) fn run_with_bundle(
    request: &CodexSkillSetupRequest,
    mode: CodexSetupMode,
    version: &str,
    skill_md: &str,
    openai_yaml: &str,
) -> CodexSkillSetupReport {
    let candidate_skill_path = if request.skills_dir.as_os_str().is_empty() {
        None
    } else {
        Some(
            request
                .skills_dir
                .join(THREADLANE_SKILL_NAME)
                .display()
                .to_string(),
        )
    };

    if let Err(err) = validate_bundle_inputs(version, skill_md, openai_yaml) {
        return report_from_error(mode, candidate_skill_path, version, false, err);
    }

    let paths = match validate_skill_paths(&request.skills_dir) {
        Ok(paths) => paths,
        Err(err) => return report_from_error(mode, candidate_skill_path, version, false, err),
    };
    let skill_path = Some(paths.skill_dir.display().to_string());

    let mut files_modified = false;
    match execute_skill_setup(
        &paths,
        mode,
        version,
        skill_md,
        openai_yaml,
        &mut files_modified,
    ) {
        Ok(mut report) => {
            report.files_modified |= files_modified;
            report
        }
        Err(err) => report_from_error(mode, skill_path, version, files_modified, err),
    }
}

fn execute_skill_setup(
    paths: &ValidatedPaths,
    mode: CodexSetupMode,
    version: &str,
    skill_md: &str,
    openai_yaml: &str,
    files_modified: &mut bool,
) -> Result<CodexSkillSetupReport, NativeError> {
    let _lock_guard = match mode {
        CodexSetupMode::Check => None,
        CodexSetupMode::Apply => {
            ensure_directory_chain(&paths.skills_dir, "Codex skills directory")?;
            Some(acquire_setup_lock(&paths.lock_path)?)
        }
    };

    validate_existing_directories(paths)?;

    let desired_skill_md_sha256 = sha256_hex(skill_md.as_bytes());
    let desired_openai_yaml_sha256 = sha256_hex(openai_yaml.as_bytes());

    let initial_marker = read_ownership_marker(&paths.marker_path)?;
    let initial_skill_md =
        read_bounded_regular_file(&paths.skill_md_path, "SKILL.md")?.map(|(id, _)| id);
    let initial_openai_yaml =
        read_bounded_regular_file(&paths.openai_yaml_path, "agents/openai.yaml")?.map(|(id, _)| id);

    let skill_path = Some(paths.skill_dir.display().to_string());
    let both_files_match_desired = initial_skill_md
        .as_ref()
        .map(|id| id.sha256 == desired_skill_md_sha256)
        .unwrap_or(false)
        && initial_openai_yaml
            .as_ref()
            .map(|id| id.sha256 == desired_openai_yaml_sha256)
            .unwrap_or(false);

    if let Some((ref marker_id, ref marker)) = initial_marker {
        if !marker_owns_actual_files(
            marker,
            initial_skill_md.as_ref(),
            initial_openai_yaml.as_ref(),
        ) {
            return Ok(conflict_report(
                mode,
                skill_path,
                version,
                "managed Threadlane skill files were modified or removed outside Threadlane",
            ));
        }

        let marker_matches_desired = !marker.pending
            && marker.version == version
            && marker
                .files
                .get(SKILL_MD_REL_PATH)
                .map(|r| r.desired_sha256 == desired_skill_md_sha256)
                .unwrap_or(false)
            && marker
                .files
                .get(OPENAI_YAML_REL_PATH)
                .map(|r| r.desired_sha256 == desired_openai_yaml_sha256)
                .unwrap_or(false);

        if marker_matches_desired && both_files_match_desired {
            if mode == CodexSetupMode::Apply {
                ensure_directory_chain(&paths.skill_dir, "Threadlane skill directory")?;
                ensure_directory_chain(&paths.agents_dir, "Threadlane skill agents directory")?;
                sync_directory(
                    &paths.skill_dir,
                    SKILL_MD_REL_PATH,
                    NativeErrorCode::PersistenceUncertain,
                )?;
                sync_directory(
                    &paths.agents_dir,
                    OPENAI_YAML_REL_PATH,
                    NativeErrorCode::PersistenceUncertain,
                )?;
                sync_directory(
                    &paths.skill_dir,
                    "ownership marker",
                    NativeErrorCode::PersistenceUncertain,
                )?;
            }
            return Ok(CodexSkillSetupReport {
                state: CodexSkillSetupState::Unchanged,
                mode,
                skill_path,
                version: version.to_string(),
                installed: true,
                files_modified: false,
                error_code: None,
                message: "bundled Threadlane Codex skill is already up to date".to_string(),
            });
        }

        if mode == CodexSetupMode::Check {
            return Ok(CodexSkillSetupReport {
                state: CodexSkillSetupState::Outdated,
                mode,
                skill_path,
                version: version.to_string(),
                installed: both_files_match_desired,
                files_modified: false,
                error_code: None,
                message: "managed Threadlane Codex skill installation requires an update"
                    .to_string(),
            });
        }

        apply_managed_bundle(
            paths,
            version,
            skill_md,
            openai_yaml,
            &desired_skill_md_sha256,
            &desired_openai_yaml_sha256,
            Some(marker_id),
            initial_skill_md.as_ref(),
            initial_openai_yaml.as_ref(),
            files_modified,
        )
    } else {
        if both_files_match_desired {
            if mode == CodexSetupMode::Check {
                return Ok(CodexSkillSetupReport {
                    state: CodexSkillSetupState::NotInstalled,
                    mode,
                    skill_path,
                    version: version.to_string(),
                    installed: true,
                    files_modified: false,
                    error_code: None,
                    message:
                        "bundled Threadlane Codex skill files match desired bytes but lack ownership marker"
                            .to_string(),
                });
            }

            return adopt_matching_bundle(
                paths,
                version,
                &desired_skill_md_sha256,
                &desired_openai_yaml_sha256,
                initial_skill_md.as_ref(),
                initial_openai_yaml.as_ref(),
                files_modified,
            );
        }

        if initial_skill_md.is_some() || initial_openai_yaml.is_some() {
            return Ok(conflict_report(
                mode,
                skill_path,
                version,
                "target skill directory contains unmanaged skill files without a Threadlane ownership marker",
            ));
        }

        if !is_target_genuinely_empty(&paths.skill_dir)? {
            return Ok(conflict_report(
                mode,
                skill_path,
                version,
                "target skill directory is non-empty and lacks a Threadlane ownership marker",
            ));
        }

        if mode == CodexSetupMode::Check {
            return Ok(CodexSkillSetupReport {
                state: CodexSkillSetupState::NotInstalled,
                mode,
                skill_path,
                version: version.to_string(),
                installed: false,
                files_modified: false,
                error_code: None,
                message: "bundled Threadlane Codex skill is not installed".to_string(),
            });
        }

        apply_managed_bundle(
            paths,
            version,
            skill_md,
            openai_yaml,
            &desired_skill_md_sha256,
            &desired_openai_yaml_sha256,
            None,
            None,
            None,
            files_modified,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_managed_bundle(
    paths: &ValidatedPaths,
    version: &str,
    skill_md: &str,
    openai_yaml: &str,
    desired_skill_md_sha256: &str,
    desired_openai_yaml_sha256: &str,
    expected_marker_id: Option<&FileIdentity>,
    expected_skill_md_id: Option<&FileIdentity>,
    expected_openai_yaml_id: Option<&FileIdentity>,
    files_modified: &mut bool,
) -> Result<CodexSkillSetupReport, NativeError> {
    let skill_path = Some(paths.skill_dir.display().to_string());

    ensure_directory_chain(&paths.skill_dir, "Threadlane skill directory")?;
    ensure_directory_chain(&paths.agents_dir, "Threadlane skill agents directory")?;

    if !verify_file_identity(&paths.marker_path, expected_marker_id, "ownership marker")?
        || !verify_file_identity(&paths.skill_md_path, expected_skill_md_id, "SKILL.md")?
        || !verify_file_identity(
            &paths.openai_yaml_path,
            expected_openai_yaml_id,
            "agents/openai.yaml",
        )?
    {
        return Ok(conflict_report(
            CodexSetupMode::Apply,
            skill_path,
            version,
            "concurrent modification detected in Threadlane skill directory before journal write",
        ));
    }

    let pending_marker = build_marker(
        version,
        true,
        expected_skill_md_id.map(|id| id.sha256.clone()),
        desired_skill_md_sha256.to_string(),
        expected_openai_yaml_id.map(|id| id.sha256.clone()),
        desired_openai_yaml_sha256.to_string(),
    );
    let pending_bytes = serialize_marker(&pending_marker)?;
    let pending_marker_id = match atomic_write_checked(
        &paths.skill_dir,
        &paths.marker_path,
        &pending_bytes,
        expected_marker_id,
        "ownership marker",
        files_modified,
    )? {
        Some(id) => id,
        None => {
            return Ok(conflict_report(
                CodexSetupMode::Apply,
                skill_path,
                version,
                "concurrent modification detected while writing pending ownership marker",
            ));
        }
    };

    let current_skill_md_id = if expected_skill_md_id
        .map(|id| id.sha256 == desired_skill_md_sha256)
        .unwrap_or(false)
    {
        if !verify_file_identity(&paths.skill_md_path, expected_skill_md_id, "SKILL.md")? {
            return Ok(conflict_report(
                CodexSetupMode::Apply,
                skill_path,
                version,
                "concurrent modification detected in SKILL.md during skill setup",
            ));
        }
        expected_skill_md_id.cloned().ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "missing expected SKILL.md identity after verification",
            )
        })?
    } else {
        match atomic_write_checked(
            &paths.skill_dir,
            &paths.skill_md_path,
            skill_md.as_bytes(),
            expected_skill_md_id,
            "SKILL.md",
            files_modified,
        )? {
            Some(id) => id,
            None => {
                return Ok(conflict_report(
                    CodexSetupMode::Apply,
                    skill_path,
                    version,
                    "concurrent manual edit detected in SKILL.md before replacement",
                ));
            }
        }
    };

    let current_openai_yaml_id = if expected_openai_yaml_id
        .map(|id| id.sha256 == desired_openai_yaml_sha256)
        .unwrap_or(false)
    {
        if !verify_file_identity(
            &paths.openai_yaml_path,
            expected_openai_yaml_id,
            "agents/openai.yaml",
        )? {
            return Ok(conflict_report(
                CodexSetupMode::Apply,
                skill_path,
                version,
                "concurrent modification detected in agents/openai.yaml during skill setup",
            ));
        }
        expected_openai_yaml_id.cloned().ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "missing expected agents/openai.yaml identity after verification",
            )
        })?
    } else {
        match atomic_write_checked(
            &paths.agents_dir,
            &paths.openai_yaml_path,
            openai_yaml.as_bytes(),
            expected_openai_yaml_id,
            "agents/openai.yaml",
            files_modified,
        )? {
            Some(id) => id,
            None => {
                return Ok(conflict_report(
                    CodexSetupMode::Apply,
                    skill_path,
                    version,
                    "concurrent manual edit detected in agents/openai.yaml before replacement",
                ));
            }
        }
    };

    if !verify_file_identity(&paths.skill_md_path, Some(&current_skill_md_id), "SKILL.md")?
        || !verify_file_identity(
            &paths.openai_yaml_path,
            Some(&current_openai_yaml_id),
            "agents/openai.yaml",
        )?
    {
        return Ok(conflict_report(
            CodexSetupMode::Apply,
            skill_path,
            version,
            "concurrent modification detected in managed skill files before marker finalization",
        ));
    }

    sync_directory(
        &paths.skill_dir,
        SKILL_MD_REL_PATH,
        NativeErrorCode::PersistenceUncertain,
    )?;
    sync_directory(
        &paths.agents_dir,
        OPENAI_YAML_REL_PATH,
        NativeErrorCode::PersistenceUncertain,
    )?;

    let final_marker = build_marker(
        version,
        false,
        None,
        desired_skill_md_sha256.to_string(),
        None,
        desired_openai_yaml_sha256.to_string(),
    );
    let final_bytes = serialize_marker(&final_marker)?;
    if atomic_write_checked(
        &paths.skill_dir,
        &paths.marker_path,
        &final_bytes,
        Some(&pending_marker_id),
        "ownership marker",
        files_modified,
    )?
    .is_none()
    {
        return Ok(conflict_report(
            CodexSetupMode::Apply,
            skill_path,
            version,
            "concurrent modification detected while finalizing ownership marker",
        ));
    }

    let (state, message) = if expected_marker_id.is_some() {
        (
            CodexSkillSetupState::Updated,
            "updated bundled Threadlane Codex skill",
        )
    } else {
        (
            CodexSkillSetupState::Installed,
            "installed bundled Threadlane Codex skill",
        )
    };

    Ok(CodexSkillSetupReport {
        state,
        mode: CodexSetupMode::Apply,
        skill_path,
        version: version.to_string(),
        installed: true,
        files_modified: *files_modified,
        error_code: None,
        message: message.to_string(),
    })
}

fn adopt_matching_bundle(
    paths: &ValidatedPaths,
    version: &str,
    desired_skill_md_sha256: &str,
    desired_openai_yaml_sha256: &str,
    expected_skill_md_id: Option<&FileIdentity>,
    expected_openai_yaml_id: Option<&FileIdentity>,
    files_modified: &mut bool,
) -> Result<CodexSkillSetupReport, NativeError> {
    let skill_path = Some(paths.skill_dir.display().to_string());

    if !verify_file_identity(&paths.marker_path, None, "ownership marker")?
        || !verify_file_identity(&paths.skill_md_path, expected_skill_md_id, "SKILL.md")?
        || !verify_file_identity(
            &paths.openai_yaml_path,
            expected_openai_yaml_id,
            "agents/openai.yaml",
        )?
    {
        return Ok(conflict_report(
            CodexSetupMode::Apply,
            skill_path,
            version,
            "concurrent modification detected while adopting matching skill files",
        ));
    }

    let final_marker = build_marker(
        version,
        false,
        None,
        desired_skill_md_sha256.to_string(),
        None,
        desired_openai_yaml_sha256.to_string(),
    );
    let final_bytes = serialize_marker(&final_marker)?;
    if atomic_write_checked(
        &paths.skill_dir,
        &paths.marker_path,
        &final_bytes,
        None,
        "ownership marker",
        files_modified,
    )?
    .is_none()
    {
        return Ok(conflict_report(
            CodexSetupMode::Apply,
            skill_path,
            version,
            "concurrent modification detected while writing adoption ownership marker",
        ));
    }

    Ok(CodexSkillSetupReport {
        state: CodexSkillSetupState::Installed,
        mode: CodexSetupMode::Apply,
        skill_path,
        version: version.to_string(),
        installed: true,
        files_modified: *files_modified,
        error_code: None,
        message: "adopted matching bundled Threadlane Codex skill".to_string(),
    })
}

fn validate_bundle_inputs(
    version: &str,
    skill_md: &str,
    openai_yaml: &str,
) -> Result<(), NativeError> {
    if version.trim().is_empty() || version.len() > MAX_VERSION_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "bundled skill version string is empty or exceeds maximum length",
        ));
    }
    if skill_md.is_empty() || skill_md.len() as u64 > MAX_MANAGED_FILE_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            "bundled SKILL.md payload is empty or exceeds maximum bounded size",
        ));
    }
    if openai_yaml.is_empty() || openai_yaml.len() as u64 > MAX_MANAGED_FILE_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            "bundled agents/openai.yaml payload is empty or exceeds maximum bounded size",
        ));
    }
    Ok(())
}

fn validate_skill_paths(skills_dir: &Path) -> Result<ValidatedPaths, NativeError> {
    validate_clean_path_syntax(skills_dir, "Codex skills directory")?;
    let clean_skills_dir =
        validate_absolute_no_symlink_components(skills_dir, "Codex skills directory")?;
    if clean_skills_dir.parent().is_none() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "Codex skills directory must not be the filesystem root",
        ));
    }

    let skill_dir = clean_skills_dir.join(THREADLANE_SKILL_NAME);
    let agents_dir = skill_dir.join(AGENTS_DIR_NAME);
    let skill_md_path = skill_dir.join(SKILL_MD_REL_PATH);
    let openai_yaml_path = agents_dir.join("openai.yaml");
    let marker_path = skill_dir.join(MARKER_FILE_NAME);
    let lock_path = clean_skills_dir.join(LOCK_FILE_NAME);

    validate_absolute_no_symlink_components(&skill_dir, "Threadlane skill directory")?;
    validate_absolute_no_symlink_components(&agents_dir, "Threadlane skill agents directory")?;
    validate_absolute_no_symlink_components(&skill_md_path, "Threadlane SKILL.md")?;
    validate_absolute_no_symlink_components(&openai_yaml_path, "Threadlane agents/openai.yaml")?;
    validate_absolute_no_symlink_components(&marker_path, "Threadlane skill ownership marker")?;
    validate_absolute_no_symlink_components(&lock_path, "Threadlane skill setup lock")?;

    validate_existing_ancestor_writable(&clean_skills_dir, "Codex skills directory")?;

    Ok(ValidatedPaths {
        skills_dir: clean_skills_dir,
        skill_dir,
        agents_dir,
        skill_md_path,
        openai_yaml_path,
        marker_path,
        lock_path,
    })
}

fn validate_clean_path_syntax(path: &Path, label: &str) -> Result<(), NativeError> {
    if path.as_os_str().is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            format!("{label} path must not be empty"),
        ));
    }
    if !path.is_absolute() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            format!("{label} path must be an absolute path"),
        ));
    }
    let bytes = path.as_os_str().as_bytes();
    if bytes.contains(&0) {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            format!("{label} path must not contain NUL bytes"),
        ));
    }
    for segment in bytes.split(|&b| b == b'/') {
        if segment == b"." {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!("{label} path must not contain `.` components"),
            ));
        }
        if segment == b".." {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!("{label} path must not contain `..` traversal components"),
            ));
        }
    }
    Ok(())
}

fn validate_existing_ancestor_writable(path: &Path, label: &str) -> Result<(), NativeError> {
    let mut current = Some(path);
    while let Some(candidate) = current {
        match fs::symlink_metadata(candidate) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        format!("{label} path traverses a symlink component"),
                    ));
                }
                if !meta.file_type().is_dir() {
                    return Err(NativeError::new(
                        NativeErrorCode::InvalidConfig,
                        format!("{label} component is not a directory"),
                    ));
                }
                if candidate.parent().is_some() && (meta.mode() & 0o700) != 0o700 {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        format!("{label} directory is read-only or lacks owner rwx permissions"),
                    ));
                }
                return Ok(());
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                current = candidate.parent();
            }
            Err(err) => {
                return Err(NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to inspect {label} ancestor metadata: {}",
                        sanitize_io_error_kind(err.kind())
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn validate_existing_directories(paths: &ValidatedPaths) -> Result<(), NativeError> {
    for (dir, label) in [
        (&paths.skills_dir, "Codex skills directory"),
        (&paths.skill_dir, "Threadlane skill directory"),
        (&paths.agents_dir, "Threadlane skill agents directory"),
    ] {
        match fs::symlink_metadata(dir) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        format!("{label} must not be a symlink"),
                    ));
                }
                if !meta.file_type().is_dir() {
                    return Err(NativeError::new(
                        NativeErrorCode::InvalidConfig,
                        format!("{label} is not a directory"),
                    ));
                }
                if (meta.mode() & 0o700) != 0o700 {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        format!("{label} is read-only or lacks owner rwx permissions"),
                    ));
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to inspect {label} metadata: {}",
                        sanitize_io_error_kind(err.kind())
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn ensure_directory_chain(path: &Path, label: &str) -> Result<(), NativeError> {
    validate_clean_path_syntax(path, label)?;
    let clean = validate_absolute_no_symlink_components(path, label)?;
    let mut current = PathBuf::new();
    for comp in clean.components() {
        match comp {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(comp.as_os_str()),
            Component::Normal(part) => {
                current.push(part);
                match fs::symlink_metadata(&current) {
                    Ok(meta) => {
                        if meta.file_type().is_symlink() {
                            return Err(NativeError::new(
                                NativeErrorCode::SecurityViolation,
                                format!("{label} path traverses a symlink component"),
                            ));
                        }
                        if !meta.file_type().is_dir() {
                            return Err(NativeError::new(
                                NativeErrorCode::InvalidConfig,
                                format!("{label} component is not a directory"),
                            ));
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        if let Some(parent) = current.parent() {
                            if parent.parent().is_some() {
                                let parent_meta = fs::symlink_metadata(parent).map_err(|e| {
                                    NativeError::new(
                                        NativeErrorCode::IoError,
                                        format!(
                                            "failed to inspect {label} parent metadata: {}",
                                            sanitize_io_error_kind(e.kind())
                                        ),
                                    )
                                })?;
                                if (parent_meta.mode() & 0o700) != 0o700 {
                                    return Err(NativeError::new(
                                        NativeErrorCode::SecurityViolation,
                                        format!(
                                            "{label} parent directory is read-only or lacks owner rwx permissions"
                                        ),
                                    ));
                                }
                            }
                        }
                        fs::create_dir(&current).map_err(|create_err| {
                            NativeError::new(
                                NativeErrorCode::IoError,
                                format!(
                                    "failed to create {label}: {}",
                                    sanitize_io_error_kind(create_err.kind())
                                ),
                            )
                        })?;
                    }
                    Err(err) => {
                        return Err(NativeError::new(
                            NativeErrorCode::IoError,
                            format!(
                                "failed to inspect {label} metadata: {}",
                                sanitize_io_error_kind(err.kind())
                            ),
                        ));
                    }
                }

                let meta = fs::symlink_metadata(&current).map_err(|err| {
                    NativeError::new(
                        NativeErrorCode::IoError,
                        format!(
                            "failed to verify {label} metadata: {}",
                            sanitize_io_error_kind(err.kind())
                        ),
                    )
                })?;
                if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        format!("{label} component is not a regular directory"),
                    ));
                }
                if current.parent().is_some() && (meta.mode() & 0o700) != 0o700 {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        format!("{label} directory is read-only or lacks owner rwx permissions"),
                    ));
                }
                if let Some(parent) = current.parent() {
                    if parent.parent().is_some() {
                        sync_directory(parent, label, NativeErrorCode::PersistenceUncertain)?;
                    }
                }
            }
            Component::CurDir | Component::ParentDir => {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    format!("{label} path must not contain relative traversal segments"),
                ));
            }
        }
    }
    Ok(())
}

fn acquire_setup_lock(lock_path: &Path) -> Result<File, NativeError> {
    validate_absolute_no_symlink_components(lock_path, "Threadlane skill setup lock")?;
    match fs::symlink_metadata(lock_path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "skill setup lock path must not be a symlink",
                ));
            }
            if !meta.file_type().is_file() {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "skill setup lock path must be a regular file",
                ));
            }
            if (meta.mode() & 0o600) != 0o600 {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "skill setup lock file is read-only or lacks owner rw permissions",
                ));
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to inspect skill setup lock metadata: {}",
                    sanitize_io_error_kind(err.kind())
                ),
            ));
        }
    }

    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(lock_path)
        .map_err(|err| {
            let code = if err.raw_os_error() == Some(libc::ELOOP) {
                NativeErrorCode::SecurityViolation
            } else {
                NativeErrorCode::IoError
            };
            NativeError::new(
                code,
                format!(
                    "failed to open skill setup lock file: {}",
                    sanitize_io_error_kind(err.kind())
                ),
            )
        })?;

    let opened_meta = lock_file.metadata().map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to inspect opened skill setup lock file: {}",
                sanitize_io_error_kind(err.kind())
            ),
        )
    })?;
    let path_meta = fs::symlink_metadata(lock_path).map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to verify skill setup lock path metadata: {}",
                sanitize_io_error_kind(err.kind())
            ),
        )
    })?;
    if path_meta.file_type().is_symlink()
        || !opened_meta.file_type().is_file()
        || opened_meta.dev() != path_meta.dev()
        || opened_meta.ino() != path_meta.ino()
    {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "skill setup lock file identity changed or is not a regular file",
        ));
    }
    if (opened_meta.mode() & 0o600) != 0o600 {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "skill setup lock file is read-only or lacks owner rw permissions",
        ));
    }

    lock_file.try_lock_exclusive().map_err(|_| {
        NativeError::new(
            NativeErrorCode::IoError,
            "another Threadlane skill setup process currently holds the setup lock",
        )
    })?;

    Ok(lock_file)
}

fn read_bounded_regular_file(
    path: &Path,
    label: &str,
) -> Result<Option<(FileIdentity, Vec<u8>)>, NativeError> {
    validate_absolute_no_symlink_components(path, label)?;
    let path_meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to inspect {label} metadata: {}",
                    sanitize_io_error_kind(err.kind())
                ),
            ));
        }
    };

    if path_meta.file_type().is_symlink() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!("{label} must not be a symlink"),
        ));
    }
    if !path_meta.file_type().is_file() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!("{label} must be a regular file"),
        ));
    }
    if (path_meta.mode() & 0o600) != 0o600 {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!("{label} is read-only or lacks owner rw permissions"),
        ));
    }
    if path_meta.len() > MAX_MANAGED_FILE_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            format!("{label} exceeds maximum bounded size"),
        ));
    }

    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|err| {
            let code = if err.raw_os_error() == Some(libc::ELOOP) {
                NativeErrorCode::SecurityViolation
            } else {
                NativeErrorCode::IoError
            };
            NativeError::new(
                code,
                format!(
                    "failed to open {label}: {}",
                    sanitize_io_error_kind(err.kind())
                ),
            )
        })?;

    let file_meta = file.metadata().map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to inspect opened {label} metadata: {}",
                sanitize_io_error_kind(err.kind())
            ),
        )
    })?;
    if !file_meta.file_type().is_file()
        || file_meta.dev() != path_meta.dev()
        || file_meta.ino() != path_meta.ino()
    {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!("{label} identity changed during open or is not a regular file"),
        ));
    }
    if (file_meta.mode() & 0o600) != 0o600 {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!("{label} is read-only or lacks owner rw permissions"),
        ));
    }
    if file_meta.len() > MAX_MANAGED_FILE_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            format!("{label} exceeds maximum bounded size"),
        ));
    }

    let mut bytes = Vec::with_capacity(file_meta.len() as usize);
    file.take(MAX_MANAGED_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to read {label}: {}",
                    sanitize_io_error_kind(err.kind())
                ),
            )
        })?;
    if bytes.len() as u64 > MAX_MANAGED_FILE_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            format!("{label} exceeds maximum bounded size"),
        ));
    }

    let sha256 = sha256_hex(&bytes);
    let identity = FileIdentity {
        dev: file_meta.dev(),
        ino: file_meta.ino(),
        len: bytes.len() as u64,
        mtime: file_meta.mtime(),
        mtime_nsec: file_meta.mtime_nsec(),
        sha256,
    };
    Ok(Some((identity, bytes)))
}

fn read_ownership_marker(
    marker_path: &Path,
) -> Result<Option<(FileIdentity, SkillOwnershipMarker)>, NativeError> {
    let Some((identity, bytes)) = read_bounded_regular_file(marker_path, "ownership marker")?
    else {
        return Ok(None);
    };
    let marker: SkillOwnershipMarker = serde_json::from_slice(&bytes).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Threadlane skill ownership marker is malformed or contains unknown fields",
        )
    })?;
    validate_ownership_marker(&marker)?;
    Ok(Some((identity, marker)))
}

fn validate_ownership_marker(marker: &SkillOwnershipMarker) -> Result<(), NativeError> {
    if marker.schema_version != MARKER_SCHEMA_VERSION {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "unsupported Threadlane skill ownership marker schema version",
        ));
    }
    if marker.owner != MARKER_OWNER {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "unrecognized Threadlane skill ownership marker owner",
        ));
    }
    if marker.version.trim().is_empty() || marker.version.len() > MAX_VERSION_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "invalid version in Threadlane skill ownership marker",
        ));
    }
    if marker.files.len() != 2
        || !marker.files.contains_key(SKILL_MD_REL_PATH)
        || !marker.files.contains_key(OPENAI_YAML_REL_PATH)
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Threadlane skill ownership marker must contain exact managed file keys",
        ));
    }
    for record in marker.files.values() {
        if !is_valid_sha256_hex(&record.desired_sha256) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Threadlane skill ownership marker has an invalid desired SHA-256 digest",
            ));
        }
        match (&marker.pending, &record.previous_sha256) {
            (true, Some(prev)) if !is_valid_sha256_hex(prev) => {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Threadlane skill ownership marker has an invalid previous SHA-256 digest",
                ));
            }
            (false, Some(_)) => {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "non-pending Threadlane skill ownership marker must not retain previous digests",
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

fn marker_owns_actual_files(
    marker: &SkillOwnershipMarker,
    skill_md: Option<&FileIdentity>,
    openai_yaml: Option<&FileIdentity>,
) -> bool {
    let Some(skill_record) = marker.files.get(SKILL_MD_REL_PATH) else {
        return false;
    };
    let Some(yaml_record) = marker.files.get(OPENAI_YAML_REL_PATH) else {
        return false;
    };
    record_owns_file(marker.pending, skill_record, skill_md)
        && record_owns_file(marker.pending, yaml_record, openai_yaml)
}

fn record_owns_file(
    pending: bool,
    record: &ManagedFileRecord,
    actual: Option<&FileIdentity>,
) -> bool {
    match actual {
        Some(file) => {
            if file.sha256 == record.desired_sha256 {
                return true;
            }
            pending
                && record
                    .previous_sha256
                    .as_deref()
                    .map(|prev| prev == file.sha256)
                    .unwrap_or(false)
        }
        None => pending && record.previous_sha256.is_none(),
    }
}

fn is_target_genuinely_empty(skill_dir: &Path) -> Result<bool, NativeError> {
    let entries = match fs::read_dir(skill_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(err) => {
            return Err(NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to read Threadlane skill directory: {}",
                    sanitize_io_error_kind(err.kind())
                ),
            ));
        }
    };

    let mut empty = true;
    for entry_res in entries {
        let entry = entry_res.map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to inspect Threadlane skill directory entry: {}",
                    sanitize_io_error_kind(err.kind())
                ),
            )
        })?;
        let file_type = entry.file_type().map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to inspect Threadlane skill entry type: {}",
                    sanitize_io_error_kind(err.kind())
                ),
            )
        })?;
        if file_type.is_symlink() {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "Threadlane skill directory contains a symlink entry",
            ));
        }
        if entry.file_name() == OsStr::new(AGENTS_DIR_NAME) && file_type.is_dir() {
            if !is_agents_subdir_empty(&entry.path())? {
                empty = false;
            }
        } else {
            empty = false;
        }
    }
    Ok(empty)
}

fn is_agents_subdir_empty(agents_dir: &Path) -> Result<bool, NativeError> {
    let entries = fs::read_dir(agents_dir).map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to read Threadlane skill agents directory: {}",
                sanitize_io_error_kind(err.kind())
            ),
        )
    })?;
    let mut empty = true;
    for entry_res in entries {
        let entry = entry_res.map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to inspect Threadlane skill agents entry: {}",
                    sanitize_io_error_kind(err.kind())
                ),
            )
        })?;
        let file_type = entry.file_type().map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to inspect Threadlane skill agents entry type: {}",
                    sanitize_io_error_kind(err.kind())
                ),
            )
        })?;
        if file_type.is_symlink() {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "Threadlane skill agents directory contains a symlink entry",
            ));
        }
        empty = false;
    }
    Ok(empty)
}

fn verify_file_identity(
    path: &Path,
    expected: Option<&FileIdentity>,
    label: &str,
) -> Result<bool, NativeError> {
    let current = read_bounded_regular_file(path, label)?.map(|(id, _)| id);
    Ok(current.as_ref() == expected)
}

fn atomic_write_checked(
    parent_dir: &Path,
    target_path: &Path,
    contents: &[u8],
    expected_before: Option<&FileIdentity>,
    label: &str,
    files_modified: &mut bool,
) -> Result<Option<FileIdentity>, NativeError> {
    if contents.len() as u64 > MAX_MANAGED_FILE_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            format!("{label} payload exceeds maximum bounded size"),
        ));
    }
    validate_absolute_no_symlink_components(parent_dir, label)?;
    validate_absolute_no_symlink_components(target_path, label)?;

    let mut temp_file = NamedTempFile::new_in(parent_dir).map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to create temporary file for {label}: {}",
                sanitize_io_error_kind(err.kind())
            ),
        )
    })?;

    temp_file
        .as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to set private permissions on temporary {label}: {}",
                    sanitize_io_error_kind(err.kind())
                ),
            )
        })?;

    temp_file.write_all(contents).map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to write temporary {label}: {}",
                sanitize_io_error_kind(err.kind())
            ),
        )
    })?;
    temp_file.flush().map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to flush temporary {label}: {}",
                sanitize_io_error_kind(err.kind())
            ),
        )
    })?;
    temp_file.as_file().sync_all().map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to sync temporary {label}: {}",
                sanitize_io_error_kind(err.kind())
            ),
        )
    })?;

    #[cfg(test)]
    maybe_inject_concurrent_edit(target_path, label);

    if !verify_file_identity(target_path, expected_before, label)? {
        return Ok(None);
    }

    temp_file.persist(target_path).map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to atomically replace {label}: {}",
                sanitize_io_error_kind(err.error.kind())
            ),
        )
    })?;
    *files_modified = true;

    sync_directory(parent_dir, label, NativeErrorCode::PersistenceUncertain)?;

    let written = read_bounded_regular_file(target_path, label)?
        .map(|(id, _)| id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::PersistenceUncertain,
                format!("{label} was not present after atomic replacement"),
            )
        })?;
    if written.sha256 != sha256_hex(contents) {
        return Err(NativeError::new(
            NativeErrorCode::PersistenceUncertain,
            format!("{label} digest did not match expected bytes after atomic replacement"),
        ));
    }

    Ok(Some(written))
}

fn sync_directory(dir: &Path, label: &str, error_code: NativeErrorCode) -> Result<(), NativeError> {
    #[cfg(test)]
    maybe_inject_sync_dir_failure(dir, label, error_code)?;

    let dir_file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(dir)
        .map_err(|err| {
            NativeError::new(
                error_code,
                format!(
                    "failed to open parent directory to sync {label}: {}",
                    sanitize_io_error_kind(err.kind())
                ),
            )
        })?;
    dir_file.sync_all().map_err(|err| {
        NativeError::new(
            error_code,
            format!(
                "failed to sync parent directory for {label}: {}",
                sanitize_io_error_kind(err.kind())
            ),
        )
    })
}

fn build_marker(
    version: &str,
    pending: bool,
    skill_md_previous: Option<String>,
    skill_md_desired: String,
    openai_yaml_previous: Option<String>,
    openai_yaml_desired: String,
) -> SkillOwnershipMarker {
    let mut files = BTreeMap::new();
    files.insert(
        SKILL_MD_REL_PATH.to_string(),
        ManagedFileRecord {
            previous_sha256: skill_md_previous,
            desired_sha256: skill_md_desired,
        },
    );
    files.insert(
        OPENAI_YAML_REL_PATH.to_string(),
        ManagedFileRecord {
            previous_sha256: openai_yaml_previous,
            desired_sha256: openai_yaml_desired,
        },
    );
    SkillOwnershipMarker {
        schema_version: MARKER_SCHEMA_VERSION,
        owner: MARKER_OWNER.to_string(),
        version: version.to_string(),
        pending,
        files,
    }
}

fn serialize_marker(marker: &SkillOwnershipMarker) -> Result<Vec<u8>, NativeError> {
    let mut bytes = serde_json::to_vec_pretty(marker).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to serialize Threadlane skill ownership marker",
        )
    })?;
    bytes.push(b'\n');
    if bytes.len() as u64 > MAX_MANAGED_FILE_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            "serialized Threadlane skill ownership marker exceeds maximum bounded size",
        ));
    }
    Ok(bytes)
}

fn conflict_report(
    mode: CodexSetupMode,
    skill_path: Option<String>,
    version: &str,
    message: &'static str,
) -> CodexSkillSetupReport {
    CodexSkillSetupReport {
        state: CodexSkillSetupState::Conflict,
        mode,
        skill_path,
        version: version.to_string(),
        installed: false,
        files_modified: false,
        error_code: Some(CONFLICT_ERROR_CODE.to_string()),
        message: message.to_string(),
    }
}

fn report_from_error(
    mode: CodexSetupMode,
    skill_path: Option<String>,
    version: &str,
    files_modified: bool,
    err: NativeError,
) -> CodexSkillSetupReport {
    CodexSkillSetupReport {
        state: CodexSkillSetupState::Error,
        mode,
        skill_path,
        version: version.to_string(),
        installed: false,
        files_modified,
        error_code: Some(err.code.as_str().to_string()),
        message: err.message,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn is_valid_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn sanitize_io_error_kind(kind: std::io::ErrorKind) -> &'static str {
    use std::io::ErrorKind;
    match kind {
        ErrorKind::NotFound => "not_found",
        ErrorKind::PermissionDenied => "permission_denied",
        ErrorKind::AlreadyExists => "already_exists",
        ErrorKind::WouldBlock => "would_block",
        ErrorKind::InvalidInput => "invalid_input",
        ErrorKind::InvalidData => "invalid_data",
        ErrorKind::TimedOut => "timed_out",
        ErrorKind::WriteZero => "write_zero",
        ErrorKind::Interrupted => "interrupted",
        ErrorKind::UnexpectedEof => "unexpected_eof",
        ErrorKind::OutOfMemory => "out_of_memory",
        _ => "io_error",
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
struct TestFaultHooks {
    fail_post_rename_sync_label: Option<&'static str>,
    fail_post_rename_sync_dir: Option<PathBuf>,
    fail_post_rename_sync_skip: usize,
    concurrent_edit_target: Option<(&'static str, Vec<u8>)>,
}

#[cfg(test)]
thread_local! {
    static TEST_FAULT_HOOKS: std::cell::RefCell<TestFaultHooks> =
        std::cell::RefCell::new(TestFaultHooks::default());
}

#[cfg(test)]
fn maybe_inject_sync_dir_failure(
    dir: &Path,
    label: &str,
    error_code: NativeErrorCode,
) -> Result<(), NativeError> {
    if error_code != NativeErrorCode::PersistenceUncertain {
        return Ok(());
    }
    let should_fail = TEST_FAULT_HOOKS.with(|hooks| {
        let mut hooks = hooks.borrow_mut();
        let label_matches = hooks.fail_post_rename_sync_label == Some(label);
        let dir_matches = hooks
            .fail_post_rename_sync_dir
            .as_deref()
            .map(|expected_dir| expected_dir == dir)
            .unwrap_or(true);
        if label_matches && dir_matches {
            if hooks.fail_post_rename_sync_skip > 0 {
                hooks.fail_post_rename_sync_skip -= 1;
                false
            } else {
                hooks.fail_post_rename_sync_label = None;
                hooks.fail_post_rename_sync_dir = None;
                true
            }
        } else {
            false
        }
    });
    if should_fail {
        return Err(NativeError::new(
            NativeErrorCode::PersistenceUncertain,
            format!("injected post-rename directory sync failure for {label}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn maybe_inject_concurrent_edit(target_path: &Path, label: &str) {
    let payload = TEST_FAULT_HOOKS.with(|hooks| {
        let mut hooks = hooks.borrow_mut();
        if hooks
            .concurrent_edit_target
            .as_ref()
            .map(|(target_label, _)| *target_label == label)
            .unwrap_or(false)
        {
            hooks.concurrent_edit_target.take().map(|(_, bytes)| bytes)
        } else {
            None
        }
    });
    if let Some(bytes) = payload {
        let _ = fs::write(target_path, bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use tempfile::TempDir;

    const V1_VERSION: &str = "2026-11-08.v1";
    const V1_SKILL_MD: &str = "---\nname: threadlane\ndescription: \"v1\"\n---\n# Threadlane v1\n";
    const V1_OPENAI_YAML: &str = "interface:\n  display_name: \"Threadlane v1\"\n";

    const V2_VERSION: &str = "2026-11-08.v2";
    const V2_SKILL_MD: &str = "---\nname: threadlane\ndescription: \"v2\"\n---\n# Threadlane v2\n";
    const V2_OPENAI_YAML: &str = "interface:\n  display_name: \"Threadlane v2\"\n";

    const V3_VERSION: &str = "2026-11-08.v3";
    const V3_SKILL_MD: &str = "---\nname: threadlane\ndescription: \"v3\"\n---\n# Threadlane v3\n";
    const V3_OPENAI_YAML: &str = "interface:\n  display_name: \"Threadlane v3\"\n";

    struct FaultHookReset;

    impl Drop for FaultHookReset {
        fn drop(&mut self) {
            TEST_FAULT_HOOKS.with(|hooks| {
                *hooks.borrow_mut() = TestFaultHooks::default();
            });
        }
    }

    fn canonical_tempdir() -> (TempDir, PathBuf) {
        let dir = TempDir::new().expect("create temp dir");
        let canonical = fs::canonicalize(dir.path()).expect("canonicalize temp dir");
        (dir, canonical)
    }

    #[test]
    fn check_on_absent_root_is_read_only_and_creates_nothing() {
        let (_tmp, root) = canonical_tempdir();
        let absent_skills_dir = root.join("nonexistent").join(".agents").join("skills");
        let req = CodexSkillSetupRequest {
            skills_dir: absent_skills_dir.clone(),
        };

        let report = run_codex_skill_setup(&req, CodexSetupMode::Check);
        assert_eq!(report.state, CodexSkillSetupState::NotInstalled);
        assert_eq!(report.mode, CodexSetupMode::Check);
        assert!(!report.installed);
        assert!(!report.files_modified);
        assert_eq!(report.error_code, None);
        assert_eq!(
            report.skill_path,
            Some(
                absent_skills_dir
                    .join(THREADLANE_SKILL_NAME)
                    .display()
                    .to_string()
            )
        );
        assert!(!root.join("nonexistent").exists());
    }

    #[test]
    fn install_check_apply_and_repeated_apply_preserve_identity_and_bytes() {
        let (_tmp, root) = canonical_tempdir();
        let skills_dir = root.join(".agents").join("skills");
        let req = CodexSkillSetupRequest {
            skills_dir: skills_dir.clone(),
        };

        let check_before = run_codex_skill_setup(&req, CodexSetupMode::Check);
        assert_eq!(check_before.state, CodexSkillSetupState::NotInstalled);
        assert!(!check_before.installed);
        assert!(!check_before.files_modified);
        assert!(!skills_dir.exists());

        let install_report = run_codex_skill_setup(&req, CodexSetupMode::Apply);
        assert_eq!(install_report.state, CodexSkillSetupState::Installed);
        assert_eq!(install_report.mode, CodexSetupMode::Apply);
        assert_eq!(install_report.version, THREADLANE_SKILL_VERSION);
        assert!(install_report.installed);
        assert!(install_report.files_modified);
        assert_eq!(install_report.error_code, None);

        let skill_dir = skills_dir.join(THREADLANE_SKILL_NAME);
        let skill_md_path = skill_dir.join("SKILL.md");
        let openai_yaml_path = skill_dir.join("agents").join("openai.yaml");
        let marker_path = skill_dir.join(MARKER_FILE_NAME);

        assert_eq!(
            fs::read_to_string(&skill_md_path).expect("read SKILL.md"),
            BUNDLED_SKILL_MD
        );
        assert_eq!(
            fs::read_to_string(&openai_yaml_path).expect("read openai.yaml"),
            BUNDLED_OPENAI_YAML
        );

        let (marker_id_before, marker) = read_ownership_marker(&marker_path)
            .expect("read marker")
            .expect("marker present");
        assert!(!marker.pending);
        assert_eq!(marker.version, THREADLANE_SKILL_VERSION);

        let skill_md_id_before = read_bounded_regular_file(&skill_md_path, "SKILL.md")
            .expect("inspect SKILL.md")
            .expect("SKILL.md present")
            .0;
        let yaml_id_before = read_bounded_regular_file(&openai_yaml_path, "agents/openai.yaml")
            .expect("inspect openai.yaml")
            .expect("openai.yaml present")
            .0;

        let check_after = run_codex_skill_setup(&req, CodexSetupMode::Check);
        assert_eq!(check_after.state, CodexSkillSetupState::Unchanged);
        assert!(check_after.installed);
        assert!(!check_after.files_modified);
        assert_eq!(check_after.error_code, None);

        let second_apply = run_codex_skill_setup(&req, CodexSetupMode::Apply);
        assert_eq!(second_apply.state, CodexSkillSetupState::Unchanged);
        assert!(second_apply.installed);
        assert!(!second_apply.files_modified);
        assert_eq!(second_apply.error_code, None);

        let marker_id_after = read_ownership_marker(&marker_path)
            .expect("read marker")
            .expect("marker present")
            .0;
        let skill_md_id_after = read_bounded_regular_file(&skill_md_path, "SKILL.md")
            .expect("inspect SKILL.md")
            .expect("SKILL.md present")
            .0;
        let yaml_id_after = read_bounded_regular_file(&openai_yaml_path, "agents/openai.yaml")
            .expect("inspect openai.yaml")
            .expect("openai.yaml present")
            .0;

        assert_eq!(marker_id_before, marker_id_after);
        assert_eq!(skill_md_id_before, skill_md_id_after);
        assert_eq!(yaml_id_before, yaml_id_after);
    }

    #[test]
    fn update_preserves_extras_other_skills_disabled_config_and_parent_modes() {
        let (_tmp, root) = canonical_tempdir();
        let agents_root = root.join(".agents");
        let skills_dir = agents_root.join("skills");
        fs::create_dir_all(&skills_dir).expect("create skills_dir");
        fs::set_permissions(&agents_root, fs::Permissions::from_mode(0o750))
            .expect("chmod .agents");
        fs::set_permissions(&skills_dir, fs::Permissions::from_mode(0o755))
            .expect("chmod skills_dir");

        let other_skill_dir = skills_dir.join("custom-skill");
        fs::create_dir_all(&other_skill_dir).expect("create other skill");
        let other_skill_file = other_skill_dir.join("SKILL.md");
        fs::write(&other_skill_file, "# Custom user skill\n").expect("write other skill");

        let codex_config = root.join("config.toml");
        let disabled_config_text = "[skills.threadlane]\nenabled = false\n";
        fs::write(&codex_config, disabled_config_text).expect("write config.toml");

        let req = CodexSkillSetupRequest {
            skills_dir: skills_dir.clone(),
        };

        let v1_report = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        assert_eq!(v1_report.state, CodexSkillSetupState::Installed);
        assert!(v1_report.installed);
        assert!(v1_report.files_modified);

        let skill_dir = skills_dir.join(THREADLANE_SKILL_NAME);
        fs::set_permissions(&skill_dir, fs::Permissions::from_mode(0o750))
            .expect("chmod skill_dir");
        let extra_root_file = skill_dir.join("extra-notes.txt");
        let extra_agents_file = skill_dir.join("agents").join("custom.yaml");
        fs::write(&extra_root_file, "keep me\n").expect("write extra root file");
        fs::write(&extra_agents_file, "custom: true\n").expect("write extra agents file");

        let check_v2 = run_with_bundle(
            &req,
            CodexSetupMode::Check,
            V2_VERSION,
            V2_SKILL_MD,
            V2_OPENAI_YAML,
        );
        assert_eq!(check_v2.state, CodexSkillSetupState::Outdated);
        assert!(!check_v2.installed);
        assert!(!check_v2.files_modified);

        let apply_v2 = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V2_VERSION,
            V2_SKILL_MD,
            V2_OPENAI_YAML,
        );
        assert_eq!(apply_v2.state, CodexSkillSetupState::Updated);
        assert_eq!(apply_v2.version, V2_VERSION);
        assert!(apply_v2.installed);
        assert!(apply_v2.files_modified);
        assert_eq!(apply_v2.error_code, None);

        assert_eq!(
            fs::read_to_string(skill_dir.join("SKILL.md")).expect("read v2 SKILL.md"),
            V2_SKILL_MD
        );
        assert_eq!(
            fs::read_to_string(skill_dir.join("agents").join("openai.yaml"))
                .expect("read v2 openai.yaml"),
            V2_OPENAI_YAML
        );
        assert_eq!(
            fs::read_to_string(&extra_root_file).expect("read extra root"),
            "keep me\n"
        );
        assert_eq!(
            fs::read_to_string(&extra_agents_file).expect("read extra agents"),
            "custom: true\n"
        );
        assert_eq!(
            fs::read_to_string(&other_skill_file).expect("read other skill"),
            "# Custom user skill\n"
        );
        assert_eq!(
            fs::read_to_string(&codex_config).expect("read config.toml"),
            disabled_config_text
        );
        assert_eq!(
            fs::symlink_metadata(&agents_root)
                .expect("stat .agents")
                .mode()
                & 0o777,
            0o750
        );
        assert_eq!(
            fs::symlink_metadata(&skills_dir)
                .expect("stat skills_dir")
                .mode()
                & 0o777,
            0o755
        );
        assert_eq!(
            fs::symlink_metadata(&skill_dir)
                .expect("stat skill_dir")
                .mode()
                & 0o777,
            0o750
        );
    }

    #[test]
    fn adopts_matching_bytes_without_marker_and_recovers_empty_target_dirs() {
        let (_tmp, root) = canonical_tempdir();
        let skills_dir = root.join("skills");
        let skill_dir = skills_dir.join(THREADLANE_SKILL_NAME);
        let agents_dir = skill_dir.join("agents");
        fs::create_dir_all(&agents_dir).expect("create empty target and agents subdir");

        let req = CodexSkillSetupRequest {
            skills_dir: skills_dir.clone(),
        };

        let empty_check = run_with_bundle(
            &req,
            CodexSetupMode::Check,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        assert_eq!(empty_check.state, CodexSkillSetupState::NotInstalled);
        assert!(!empty_check.installed);
        assert!(!empty_check.files_modified);

        let skill_md_path = skill_dir.join("SKILL.md");
        let openai_yaml_path = agents_dir.join("openai.yaml");
        let extra_path = skill_dir.join("README.local.md");
        fs::write(&skill_md_path, V1_SKILL_MD).expect("seed matching SKILL.md");
        fs::write(&openai_yaml_path, V1_OPENAI_YAML).expect("seed matching openai.yaml");
        fs::write(&extra_path, "local notes\n").expect("seed extra file");

        let skill_md_id_before = read_bounded_regular_file(&skill_md_path, "SKILL.md")
            .expect("inspect SKILL.md")
            .expect("SKILL.md exists")
            .0;
        let yaml_id_before = read_bounded_regular_file(&openai_yaml_path, "agents/openai.yaml")
            .expect("inspect openai.yaml")
            .expect("openai.yaml exists")
            .0;

        let adopt_check = run_with_bundle(
            &req,
            CodexSetupMode::Check,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        assert_eq!(adopt_check.state, CodexSkillSetupState::NotInstalled);
        assert!(adopt_check.installed);
        assert!(!adopt_check.files_modified);
        assert!(!skill_dir.join(MARKER_FILE_NAME).exists());

        let adopt_apply = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        assert_eq!(adopt_apply.state, CodexSkillSetupState::Installed);
        assert!(adopt_apply.installed);
        assert!(adopt_apply.files_modified);
        assert!(skill_dir.join(MARKER_FILE_NAME).exists());
        assert_eq!(
            fs::read_to_string(&extra_path).expect("read extra file"),
            "local notes\n"
        );

        let skill_md_id_after = read_bounded_regular_file(&skill_md_path, "SKILL.md")
            .expect("inspect SKILL.md")
            .expect("SKILL.md exists")
            .0;
        let yaml_id_after = read_bounded_regular_file(&openai_yaml_path, "agents/openai.yaml")
            .expect("inspect openai.yaml")
            .expect("openai.yaml exists")
            .0;
        assert_eq!(skill_md_id_before, skill_md_id_after);
        assert_eq!(yaml_id_before, yaml_id_after);

        let update_v2 = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V2_VERSION,
            V2_SKILL_MD,
            V2_OPENAI_YAML,
        );
        assert_eq!(update_v2.state, CodexSkillSetupState::Updated);
        assert!(update_v2.installed);
        assert!(update_v2.files_modified);
    }

    #[test]
    fn unmanaged_or_user_modified_skill_files_report_conflict_without_overwriting() {
        let (_tmp, root) = canonical_tempdir();
        let skills_dir = root.join("skills");
        let skill_dir = skills_dir.join(THREADLANE_SKILL_NAME);
        fs::create_dir_all(&skill_dir).expect("create skill_dir");

        let unmanaged_note = skill_dir.join("unmanaged.txt");
        fs::write(&unmanaged_note, "user file\n").expect("write unmanaged note");

        let req = CodexSkillSetupRequest {
            skills_dir: skills_dir.clone(),
        };

        for mode in [CodexSetupMode::Check, CodexSetupMode::Apply] {
            let report = run_with_bundle(&req, mode, V1_VERSION, V1_SKILL_MD, V1_OPENAI_YAML);
            assert_eq!(report.state, CodexSkillSetupState::Conflict);
            assert!(!report.installed);
            assert!(!report.files_modified);
            assert_eq!(report.error_code.as_deref(), Some(CONFLICT_ERROR_CODE));
            assert!(!skill_dir.join(MARKER_FILE_NAME).exists());
            assert!(!skill_dir.join("SKILL.md").exists());
            assert!(!skill_dir.join("agents").exists());
        }

        fs::remove_file(&unmanaged_note).expect("remove unmanaged note");
        let install_v1 = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        assert_eq!(install_v1.state, CodexSkillSetupState::Installed);

        let skill_md_path = skill_dir.join("SKILL.md");
        let openai_yaml_path = skill_dir.join("agents").join("openai.yaml");
        let marker_path = skill_dir.join(MARKER_FILE_NAME);

        fs::write(
            &openai_yaml_path,
            "interface:\n  display_name: \"User Edited\"\n",
        )
        .expect("modify openai.yaml");
        let skill_md_id_before = read_bounded_regular_file(&skill_md_path, "SKILL.md")
            .expect("inspect SKILL.md")
            .expect("SKILL.md present")
            .0;
        let marker_id_before = read_ownership_marker(&marker_path)
            .expect("inspect marker")
            .expect("marker present")
            .0;

        for mode in [CodexSetupMode::Check, CodexSetupMode::Apply] {
            let report = run_with_bundle(&req, mode, V2_VERSION, V2_SKILL_MD, V2_OPENAI_YAML);
            assert_eq!(report.state, CodexSkillSetupState::Conflict);
            assert!(!report.installed);
            assert!(!report.files_modified);
            assert_eq!(report.error_code.as_deref(), Some(CONFLICT_ERROR_CODE));
        }

        let skill_md_id_after = read_bounded_regular_file(&skill_md_path, "SKILL.md")
            .expect("inspect SKILL.md")
            .expect("SKILL.md present")
            .0;
        let marker_id_after = read_ownership_marker(&marker_path)
            .expect("inspect marker")
            .expect("marker present")
            .0;
        assert_eq!(skill_md_id_before, skill_md_id_after);
        assert_eq!(marker_id_before, marker_id_after);
        assert_eq!(
            fs::read_to_string(&openai_yaml_path).expect("read user-edited openai.yaml"),
            "interface:\n  display_name: \"User Edited\"\n"
        );

        fs::remove_file(&openai_yaml_path).expect("delete managed openai.yaml");
        let missing_owned = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V2_VERSION,
            V2_SKILL_MD,
            V2_OPENAI_YAML,
        );
        assert_eq!(missing_owned.state, CodexSkillSetupState::Conflict);
        assert!(!missing_owned.installed);
        assert!(!missing_owned.files_modified);
        assert!(!openai_yaml_path.exists());
        assert_eq!(
            fs::read_to_string(&skill_md_path).expect("SKILL.md still v1"),
            V1_SKILL_MD
        );
    }

    #[test]
    fn pending_journal_recovers_partial_states_and_rejects_unowned_partial_files() {
        let (_tmp, root) = canonical_tempdir();
        let skills_dir = root.join("skills");
        let skill_dir = skills_dir.join(THREADLANE_SKILL_NAME);
        let agents_dir = skill_dir.join("agents");
        fs::create_dir_all(&agents_dir).expect("create skill and agents dirs");

        let req = CodexSkillSetupRequest {
            skills_dir: skills_dir.clone(),
        };
        let marker_path = skill_dir.join(MARKER_FILE_NAME);
        let skill_md_path = skill_dir.join("SKILL.md");
        let openai_yaml_path = agents_dir.join("openai.yaml");

        let v1_skill_sha = sha256_hex(V1_SKILL_MD.as_bytes());
        let v1_yaml_sha = sha256_hex(V1_OPENAI_YAML.as_bytes());
        let v2_skill_sha = sha256_hex(V2_SKILL_MD.as_bytes());
        let v2_yaml_sha = sha256_hex(V2_OPENAI_YAML.as_bytes());

        let pending_initial = build_marker(
            V1_VERSION,
            true,
            None,
            v1_skill_sha.clone(),
            None,
            v1_yaml_sha.clone(),
        );
        fs::write(
            &marker_path,
            serialize_marker(&pending_initial).expect("serialize pending initial"),
        )
        .expect("write pending initial marker");
        fs::write(&skill_md_path, V1_SKILL_MD).expect("write partial v1 SKILL.md");

        let check_pending = run_with_bundle(
            &req,
            CodexSetupMode::Check,
            V2_VERSION,
            V2_SKILL_MD,
            V2_OPENAI_YAML,
        );
        assert_eq!(check_pending.state, CodexSkillSetupState::Outdated);
        assert!(!check_pending.installed);
        assert!(!check_pending.files_modified);

        let recover_to_v2 = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V2_VERSION,
            V2_SKILL_MD,
            V2_OPENAI_YAML,
        );
        assert_eq!(recover_to_v2.state, CodexSkillSetupState::Updated);
        assert!(recover_to_v2.installed);
        assert!(recover_to_v2.files_modified);
        assert_eq!(
            fs::read_to_string(&skill_md_path).expect("read recovered SKILL.md"),
            V2_SKILL_MD
        );
        assert_eq!(
            fs::read_to_string(&openai_yaml_path).expect("read recovered openai.yaml"),
            V2_OPENAI_YAML
        );

        let pending_update = build_marker(
            V2_VERSION,
            true,
            Some(v1_skill_sha.clone()),
            v2_skill_sha,
            Some(v1_yaml_sha),
            v2_yaml_sha,
        );
        fs::write(
            &marker_path,
            serialize_marker(&pending_update).expect("serialize pending update"),
        )
        .expect("write pending update marker");
        fs::write(&skill_md_path, V2_SKILL_MD).expect("SKILL.md updated to v2");
        fs::write(&openai_yaml_path, V1_OPENAI_YAML).expect("openai.yaml still at v1");

        let recover_to_v3 = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V3_VERSION,
            V3_SKILL_MD,
            V3_OPENAI_YAML,
        );
        assert_eq!(recover_to_v3.state, CodexSkillSetupState::Updated);
        assert_eq!(recover_to_v3.version, V3_VERSION);
        assert!(recover_to_v3.installed);
        assert!(recover_to_v3.files_modified);
        assert_eq!(
            fs::read_to_string(&skill_md_path).expect("read v3 SKILL.md"),
            V3_SKILL_MD
        );
        assert_eq!(
            fs::read_to_string(&openai_yaml_path).expect("read v3 openai.yaml"),
            V3_OPENAI_YAML
        );
        let (_, final_marker) = read_ownership_marker(&marker_path)
            .expect("read marker")
            .expect("marker present");
        assert!(!final_marker.pending);
        assert_eq!(final_marker.version, V3_VERSION);

        fs::write(
            &marker_path,
            serialize_marker(&pending_update).expect("serialize pending update"),
        )
        .expect("rewrite pending update marker");
        fs::write(&openai_yaml_path, "unrecognized partial content\n")
            .expect("write unknown content");
        let unknown_partial = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V3_VERSION,
            V3_SKILL_MD,
            V3_OPENAI_YAML,
        );
        assert_eq!(unknown_partial.state, CodexSkillSetupState::Conflict);
        assert!(!unknown_partial.installed);
        assert!(!unknown_partial.files_modified);
        assert_eq!(
            fs::read_to_string(&openai_yaml_path).expect("unknown content preserved"),
            "unrecognized partial content\n"
        );
    }

    #[test]
    fn fault_hooks_report_files_modified_on_post_write_error_and_late_concurrent_conflict() {
        let _reset = FaultHookReset;
        let (_tmp, root) = canonical_tempdir();
        let skills_dir = root.join("skills");
        let req = CodexSkillSetupRequest {
            skills_dir: skills_dir.clone(),
        };

        let install_v1 = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        assert_eq!(install_v1.state, CodexSkillSetupState::Installed);

        let skill_dir = skills_dir.join(THREADLANE_SKILL_NAME);
        let marker_path = skill_dir.join(MARKER_FILE_NAME);
        let skill_md_path = skill_dir.join("SKILL.md");
        let openai_yaml_path = skill_dir.join("agents").join("openai.yaml");

        TEST_FAULT_HOOKS.with(|hooks| {
            hooks.borrow_mut().fail_post_rename_sync_label = Some("SKILL.md");
        });

        let sync_err_report = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V2_VERSION,
            V2_SKILL_MD,
            V2_OPENAI_YAML,
        );
        assert_eq!(sync_err_report.state, CodexSkillSetupState::Error);
        assert!(!sync_err_report.installed);
        assert!(sync_err_report.files_modified);
        assert_eq!(
            sync_err_report.error_code.as_deref(),
            Some(NativeErrorCode::PersistenceUncertain.as_str())
        );

        let (_, pending_after_err) = read_ownership_marker(&marker_path)
            .expect("read pending marker")
            .expect("marker exists");
        assert!(pending_after_err.pending);
        assert_eq!(
            fs::read_to_string(&skill_md_path).expect("SKILL.md written before sync error"),
            V2_SKILL_MD
        );
        assert_eq!(
            fs::read_to_string(&openai_yaml_path).expect("openai.yaml still v1"),
            V1_OPENAI_YAML
        );

        let recovered = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V2_VERSION,
            V2_SKILL_MD,
            V2_OPENAI_YAML,
        );
        assert_eq!(recovered.state, CodexSkillSetupState::Updated);
        assert!(recovered.installed);
        assert!(recovered.files_modified);
        let (_, finalized_marker) = read_ownership_marker(&marker_path)
            .expect("read finalized marker")
            .expect("marker exists");
        assert!(!finalized_marker.pending);

        TEST_FAULT_HOOKS.with(|hooks| {
            hooks.borrow_mut().fail_post_rename_sync_label = Some("agents/openai.yaml");
        });
        let yaml_sync_err = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V3_VERSION,
            V3_SKILL_MD,
            V3_OPENAI_YAML,
        );
        assert_eq!(yaml_sync_err.state, CodexSkillSetupState::Error);
        assert!(!yaml_sync_err.installed);
        assert!(yaml_sync_err.files_modified);
        assert_eq!(
            yaml_sync_err.error_code.as_deref(),
            Some(NativeErrorCode::PersistenceUncertain.as_str())
        );
        let (_, pending_after_yaml_err) = read_ownership_marker(&marker_path)
            .expect("read pending marker after yaml sync error")
            .expect("marker exists");
        assert!(pending_after_yaml_err.pending);
        assert_eq!(
            fs::read_to_string(&skill_md_path).expect("SKILL.md updated to v3"),
            V3_SKILL_MD
        );
        assert_eq!(
            fs::read_to_string(&openai_yaml_path)
                .expect("openai.yaml renamed to v3 before dir sync error"),
            V3_OPENAI_YAML
        );

        TEST_FAULT_HOOKS.with(|hooks| {
            hooks.borrow_mut().fail_post_rename_sync_label = Some("agents/openai.yaml");
        });
        let retry_yaml_sync_err = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V3_VERSION,
            V3_SKILL_MD,
            V3_OPENAI_YAML,
        );
        assert_eq!(retry_yaml_sync_err.state, CodexSkillSetupState::Error);
        assert!(!retry_yaml_sync_err.installed);
        assert!(retry_yaml_sync_err.files_modified);
        assert_eq!(
            retry_yaml_sync_err.error_code.as_deref(),
            Some(NativeErrorCode::PersistenceUncertain.as_str())
        );
        let (_, still_pending_marker) = read_ownership_marker(&marker_path)
            .expect("read marker after repeated yaml sync error")
            .expect("marker exists");
        assert!(still_pending_marker.pending);

        let yaml_recovered = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V3_VERSION,
            V3_SKILL_MD,
            V3_OPENAI_YAML,
        );
        assert_eq!(yaml_recovered.state, CodexSkillSetupState::Updated);
        assert!(yaml_recovered.installed);
        assert!(yaml_recovered.files_modified);
        let (_, yaml_finalized_marker) = read_ownership_marker(&marker_path)
            .expect("read finalized marker after yaml recovery")
            .expect("marker exists");
        assert!(!yaml_finalized_marker.pending);
        assert_eq!(
            fs::read_to_string(&skill_md_path).expect("SKILL.md valid after recovery"),
            V3_SKILL_MD
        );
        assert_eq!(
            fs::read_to_string(&openai_yaml_path).expect("openai.yaml valid after recovery"),
            V3_OPENAI_YAML
        );

        let reset_to_v2 = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V2_VERSION,
            V2_SKILL_MD,
            V2_OPENAI_YAML,
        );
        assert_eq!(reset_to_v2.state, CodexSkillSetupState::Updated);

        TEST_FAULT_HOOKS.with(|hooks| {
            let mut hooks = hooks.borrow_mut();
            hooks.fail_post_rename_sync_label = Some("ownership marker");
            hooks.fail_post_rename_sync_dir = Some(skill_dir.clone());
            hooks.fail_post_rename_sync_skip = 1;
        });
        let final_marker_sync_err = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V3_VERSION,
            V3_SKILL_MD,
            V3_OPENAI_YAML,
        );
        assert_eq!(final_marker_sync_err.state, CodexSkillSetupState::Error);
        assert!(!final_marker_sync_err.installed);
        assert!(final_marker_sync_err.files_modified);
        assert_eq!(
            final_marker_sync_err.error_code.as_deref(),
            Some(NativeErrorCode::PersistenceUncertain.as_str())
        );

        let (marker_id_after_final_err, marker_after_final_err) =
            read_ownership_marker(&marker_path)
                .expect("read finalized marker after sync error")
                .expect("marker exists");
        assert!(!marker_after_final_err.pending);
        assert_eq!(marker_after_final_err.version, V3_VERSION);
        let skill_md_id_after_final_err = read_bounded_regular_file(&skill_md_path, "SKILL.md")
            .expect("inspect SKILL.md")
            .expect("SKILL.md exists")
            .0;
        let yaml_id_after_final_err =
            read_bounded_regular_file(&openai_yaml_path, "agents/openai.yaml")
                .expect("inspect openai.yaml")
                .expect("openai.yaml exists")
                .0;

        TEST_FAULT_HOOKS.with(|hooks| {
            let mut hooks = hooks.borrow_mut();
            hooks.fail_post_rename_sync_label = Some("ownership marker");
            hooks.fail_post_rename_sync_dir = Some(skill_dir.clone());
            hooks.fail_post_rename_sync_skip = 0;
        });
        let retry_final_marker_sync_err = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V3_VERSION,
            V3_SKILL_MD,
            V3_OPENAI_YAML,
        );
        assert_eq!(
            retry_final_marker_sync_err.state,
            CodexSkillSetupState::Error
        );
        assert!(!retry_final_marker_sync_err.installed);
        assert!(!retry_final_marker_sync_err.files_modified);
        assert_eq!(
            retry_final_marker_sync_err.error_code.as_deref(),
            Some(NativeErrorCode::PersistenceUncertain.as_str())
        );

        let unchanged_after_sync_repair = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V3_VERSION,
            V3_SKILL_MD,
            V3_OPENAI_YAML,
        );
        assert_eq!(
            unchanged_after_sync_repair.state,
            CodexSkillSetupState::Unchanged
        );
        assert!(unchanged_after_sync_repair.installed);
        assert!(!unchanged_after_sync_repair.files_modified);
        assert_eq!(unchanged_after_sync_repair.error_code, None);
        assert_eq!(
            read_ownership_marker(&marker_path)
                .expect("read marker")
                .expect("marker exists")
                .0,
            marker_id_after_final_err
        );
        assert_eq!(
            read_bounded_regular_file(&skill_md_path, "SKILL.md")
                .expect("inspect SKILL.md")
                .expect("SKILL.md exists")
                .0,
            skill_md_id_after_final_err
        );
        assert_eq!(
            read_bounded_regular_file(&openai_yaml_path, "agents/openai.yaml")
                .expect("inspect openai.yaml")
                .expect("openai.yaml exists")
                .0,
            yaml_id_after_final_err
        );

        let fresh_skills_dir = root.join("fresh_skills");
        let fresh_skill_dir = fresh_skills_dir.join(THREADLANE_SKILL_NAME);
        let fresh_req = CodexSkillSetupRequest {
            skills_dir: fresh_skills_dir.clone(),
        };
        TEST_FAULT_HOOKS.with(|hooks| {
            let mut hooks = hooks.borrow_mut();
            hooks.fail_post_rename_sync_label = Some("Threadlane skill directory");
            hooks.fail_post_rename_sync_dir = Some(fresh_skills_dir.clone());
            hooks.fail_post_rename_sync_skip = 0;
        });
        let mkdir_sync_err = run_with_bundle(
            &fresh_req,
            CodexSetupMode::Apply,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        assert_eq!(mkdir_sync_err.state, CodexSkillSetupState::Error);
        assert!(!mkdir_sync_err.installed);
        assert!(!mkdir_sync_err.files_modified);
        assert_eq!(
            mkdir_sync_err.error_code.as_deref(),
            Some(NativeErrorCode::PersistenceUncertain.as_str())
        );
        assert!(fresh_skill_dir.is_dir());

        TEST_FAULT_HOOKS.with(|hooks| {
            let mut hooks = hooks.borrow_mut();
            hooks.fail_post_rename_sync_label = Some("Threadlane skill directory");
            hooks.fail_post_rename_sync_dir = Some(fresh_skills_dir.clone());
            hooks.fail_post_rename_sync_skip = 0;
        });
        let retry_mkdir_sync_err = run_with_bundle(
            &fresh_req,
            CodexSetupMode::Apply,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        assert_eq!(retry_mkdir_sync_err.state, CodexSkillSetupState::Error);
        assert!(!retry_mkdir_sync_err.installed);
        assert!(!retry_mkdir_sync_err.files_modified);
        assert_eq!(
            retry_mkdir_sync_err.error_code.as_deref(),
            Some(NativeErrorCode::PersistenceUncertain.as_str())
        );

        let fresh_recovered = run_with_bundle(
            &fresh_req,
            CodexSetupMode::Apply,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        assert_eq!(fresh_recovered.state, CodexSkillSetupState::Installed);
        assert!(fresh_recovered.installed);
        assert!(fresh_recovered.files_modified);

        let reset_back_to_v2 = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V2_VERSION,
            V2_SKILL_MD,
            V2_OPENAI_YAML,
        );
        assert_eq!(reset_back_to_v2.state, CodexSkillSetupState::Updated);

        let user_concurrent_yaml = b"interface:\n  display_name: \"Concurrent User Edit\"\n";
        TEST_FAULT_HOOKS.with(|hooks| {
            hooks.borrow_mut().concurrent_edit_target =
                Some(("agents/openai.yaml", user_concurrent_yaml.to_vec()));
        });

        let late_conflict = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V3_VERSION,
            V3_SKILL_MD,
            V3_OPENAI_YAML,
        );
        assert_eq!(late_conflict.state, CodexSkillSetupState::Conflict);
        assert!(!late_conflict.installed);
        assert!(late_conflict.files_modified);
        assert_eq!(
            late_conflict.error_code.as_deref(),
            Some(CONFLICT_ERROR_CODE)
        );
        assert_eq!(
            fs::read_to_string(&openai_yaml_path).expect("user concurrent edit preserved"),
            String::from_utf8_lossy(user_concurrent_yaml)
        );
        assert_eq!(
            fs::read_to_string(&skill_md_path).expect("SKILL.md had advanced to v3"),
            V3_SKILL_MD
        );
        let (_, pending_after_conflict) = read_ownership_marker(&marker_path)
            .expect("read pending marker after conflict")
            .expect("marker exists");
        assert!(pending_after_conflict.pending);

        let retry_with_user_edit = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V3_VERSION,
            V3_SKILL_MD,
            V3_OPENAI_YAML,
        );
        assert_eq!(retry_with_user_edit.state, CodexSkillSetupState::Conflict);
        assert!(!retry_with_user_edit.installed);
        assert!(!retry_with_user_edit.files_modified);
        assert_eq!(
            fs::read_to_string(&openai_yaml_path).expect("user concurrent edit still preserved"),
            String::from_utf8_lossy(user_concurrent_yaml)
        );
    }

    #[test]
    fn rejects_malformed_unknown_and_oversized_markers_and_resources() {
        let (_tmp, root) = canonical_tempdir();
        let skills_dir = root.join("skills");
        let skill_dir = skills_dir.join(THREADLANE_SKILL_NAME);
        let agents_dir = skill_dir.join("agents");
        fs::create_dir_all(&agents_dir).expect("create dirs");

        let req = CodexSkillSetupRequest {
            skills_dir: skills_dir.clone(),
        };
        let marker_path = skill_dir.join(MARKER_FILE_NAME);
        let skill_md_path = skill_dir.join("SKILL.md");
        let openai_yaml_path = agents_dir.join("openai.yaml");

        let invalid_markers = [
            "{".to_string(),
            r#"{"schema_version":1,"owner":"threadlane","version":"v1","pending":false,"unknown":1,"files":{}}"#.to_string(),
            r#"{"schema_version":2,"owner":"threadlane","version":"v1","pending":false,"files":{}}"#.to_string(),
            r#"{"schema_version":1,"owner":"other","version":"v1","pending":false,"files":{}}"#.to_string(),
            r#"{"schema_version":1,"owner":"threadlane","version":"v1","pending":false,"files":{"SKILL.md":{"desired_sha256":"bad"}}}"#.to_string(),
            format!(
                r#"{{"schema_version":1,"owner":"threadlane","version":"v1","pending":false,"files":{{"SKILL.md":{{"previous_sha256":"{h}","desired_sha256":"{h}"}},"agents/openai.yaml":{{"desired_sha256":"{h}"}}}}}}"#,
                h = "a".repeat(64)
            ),
        ];

        for bad_json in invalid_markers {
            fs::write(&marker_path, bad_json).expect("write bad marker");
            let report = run_with_bundle(
                &req,
                CodexSetupMode::Apply,
                V1_VERSION,
                V1_SKILL_MD,
                V1_OPENAI_YAML,
            );
            assert_eq!(report.state, CodexSkillSetupState::Error);
            assert!(!report.installed);
            assert!(!report.files_modified);
            assert_eq!(
                report.error_code.as_deref(),
                Some(NativeErrorCode::ValidationFailed.as_str())
            );
        }

        let oversized = vec![b'x'; (MAX_MANAGED_FILE_BYTES as usize) + 1];
        fs::write(&marker_path, &oversized).expect("write oversized marker");
        let over_marker = run_with_bundle(
            &req,
            CodexSetupMode::Check,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        assert_eq!(over_marker.state, CodexSkillSetupState::Error);
        assert_eq!(
            over_marker.error_code.as_deref(),
            Some(NativeErrorCode::OutputOverflow.as_str())
        );
        fs::remove_file(&marker_path).expect("remove oversized marker");

        fs::write(&skill_md_path, &oversized).expect("write oversized SKILL.md");
        let over_skill = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        assert_eq!(over_skill.state, CodexSkillSetupState::Error);
        assert_eq!(
            over_skill.error_code.as_deref(),
            Some(NativeErrorCode::OutputOverflow.as_str())
        );
        fs::remove_file(&skill_md_path).expect("remove oversized SKILL.md");

        fs::write(&openai_yaml_path, &oversized).expect("write oversized openai.yaml");
        let over_yaml = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        assert_eq!(over_yaml.state, CodexSkillSetupState::Error);
        assert_eq!(
            over_yaml.error_code.as_deref(),
            Some(NativeErrorCode::OutputOverflow.as_str())
        );
    }

    #[test]
    fn rejects_symlinks_nonregular_files_traversal_and_readonly_paths_without_touching_referents() {
        let (_tmp, root) = canonical_tempdir();
        let canary_dir = root.join("canary_dir");
        fs::create_dir_all(&canary_dir).expect("create canary dir");
        let canary_file = canary_dir.join("canary.txt");
        fs::write(&canary_file, "do not touch\n").expect("write canary file");

        for bad_path in [
            PathBuf::new(),
            PathBuf::from("relative/skills"),
            root.join(".").join("skills"),
            root.join("..").join("skills"),
            PathBuf::from("/"),
        ] {
            let report = run_with_bundle(
                &CodexSkillSetupRequest {
                    skills_dir: bad_path,
                },
                CodexSetupMode::Apply,
                V1_VERSION,
                V1_SKILL_MD,
                V1_OPENAI_YAML,
            );
            assert_eq!(report.state, CodexSkillSetupState::Error);
            assert!(!report.installed);
            assert!(!report.files_modified);
        }

        let symlink_skills = root.join("symlink_skills");
        symlink(&canary_dir, &symlink_skills).expect("symlink skills_dir");
        let sym_parent_report = run_with_bundle(
            &CodexSkillSetupRequest {
                skills_dir: symlink_skills,
            },
            CodexSetupMode::Apply,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        assert_eq!(sym_parent_report.state, CodexSkillSetupState::Error);
        assert_eq!(
            sym_parent_report.error_code.as_deref(),
            Some(NativeErrorCode::SecurityViolation.as_str())
        );

        let skills_dir = root.join("real_skills");
        let skill_dir = skills_dir.join(THREADLANE_SKILL_NAME);
        fs::create_dir_all(&skills_dir).expect("create real_skills");
        symlink(&canary_dir, &skill_dir).expect("symlink skill_dir");
        let req = CodexSkillSetupRequest {
            skills_dir: skills_dir.clone(),
        };
        let sym_target_report = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        assert_eq!(sym_target_report.state, CodexSkillSetupState::Error);
        assert_eq!(
            sym_target_report.error_code.as_deref(),
            Some(NativeErrorCode::SecurityViolation.as_str())
        );
        fs::remove_file(&skill_dir).expect("remove symlink skill_dir");

        fs::create_dir_all(skill_dir.join("agents")).expect("create skill_dir/agents");
        for leaf in [
            skill_dir.join("SKILL.md"),
            skill_dir.join("agents").join("openai.yaml"),
            skill_dir.join(MARKER_FILE_NAME),
            skills_dir.join(LOCK_FILE_NAME),
        ] {
            symlink(&canary_file, &leaf).expect("create leaf symlink");
            let report = run_with_bundle(
                &req,
                CodexSetupMode::Apply,
                V1_VERSION,
                V1_SKILL_MD,
                V1_OPENAI_YAML,
            );
            assert_eq!(report.state, CodexSkillSetupState::Error);
            assert_eq!(
                report.error_code.as_deref(),
                Some(NativeErrorCode::SecurityViolation.as_str())
            );
            fs::remove_file(&leaf).expect("remove leaf symlink");
        }
        assert_eq!(
            fs::read_to_string(&canary_file).expect("canary intact"),
            "do not touch\n"
        );

        fs::create_dir(skill_dir.join("SKILL.md")).expect("dir in place of SKILL.md");
        let nonreg_report = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        assert_eq!(nonreg_report.state, CodexSkillSetupState::Error);
        assert_eq!(
            nonreg_report.error_code.as_deref(),
            Some(NativeErrorCode::SecurityViolation.as_str())
        );
        fs::remove_dir(skill_dir.join("SKILL.md")).expect("remove dir SKILL.md");

        fs::set_permissions(&skill_dir, fs::Permissions::from_mode(0o500))
            .expect("chmod skill_dir 0o500");
        let ro_dir_report = run_with_bundle(
            &req,
            CodexSetupMode::Check,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        fs::set_permissions(&skill_dir, fs::Permissions::from_mode(0o700))
            .expect("restore skill_dir 0o700");
        assert_eq!(ro_dir_report.state, CodexSkillSetupState::Error);
        assert_eq!(
            ro_dir_report.error_code.as_deref(),
            Some(NativeErrorCode::SecurityViolation.as_str())
        );

        let installed = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        assert_eq!(installed.state, CodexSkillSetupState::Installed);

        let skill_md_path = skill_dir.join("SKILL.md");
        fs::set_permissions(&skill_md_path, fs::Permissions::from_mode(0o400))
            .expect("chmod SKILL.md 0o400");
        let ro_file_report = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V2_VERSION,
            V2_SKILL_MD,
            V2_OPENAI_YAML,
        );
        fs::set_permissions(&skill_md_path, fs::Permissions::from_mode(0o600))
            .expect("restore SKILL.md 0o600");
        assert_eq!(ro_file_report.state, CodexSkillSetupState::Error);
        assert_eq!(
            ro_file_report.error_code.as_deref(),
            Some(NativeErrorCode::SecurityViolation.as_str())
        );
    }

    #[test]
    fn lock_contention_is_nonblocking_in_apply_and_ignored_by_check() {
        let (_tmp, root) = canonical_tempdir();
        let skills_dir = root.join("skills");
        fs::create_dir_all(&skills_dir).expect("create skills_dir");
        let lock_path = skills_dir.join(LOCK_FILE_NAME);

        let held_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&lock_path)
            .expect("open lock");
        held_lock.try_lock_exclusive().expect("lock exclusively");

        let req = CodexSkillSetupRequest {
            skills_dir: skills_dir.clone(),
        };

        let check_while_locked = run_with_bundle(
            &req,
            CodexSetupMode::Check,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        assert_eq!(check_while_locked.state, CodexSkillSetupState::NotInstalled);

        let apply_while_locked = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        assert_eq!(apply_while_locked.state, CodexSkillSetupState::Error);
        assert!(!apply_while_locked.installed);
        assert!(!apply_while_locked.files_modified);
        assert_eq!(
            apply_while_locked.error_code.as_deref(),
            Some(NativeErrorCode::IoError.as_str())
        );
        assert!(!skills_dir.join(THREADLANE_SKILL_NAME).exists());

        FileExt::unlock(&held_lock).expect("unlock");
        drop(held_lock);

        let apply_after_unlock = run_with_bundle(
            &req,
            CodexSetupMode::Apply,
            V1_VERSION,
            V1_SKILL_MD,
            V1_OPENAI_YAML,
        );
        assert_eq!(apply_after_unlock.state, CodexSkillSetupState::Installed);
        assert!(apply_after_unlock.installed);
    }

    #[test]
    fn resolve_codex_skills_dir_helper_honors_home_and_userprofile_fallbacks() {
        let (_tmp, root) = canonical_tempdir();
        let home_a = root.join("home_a");
        let home_b = root.join("home_b");
        fs::create_dir_all(&home_a).expect("create home_a");
        fs::create_dir_all(&home_b).expect("create home_b");

        let resolved_primary =
            resolve_codex_skills_dir_with_env(Some(home_a.as_os_str()), Some(home_b.as_os_str()))
                .expect("resolve primary HOME");
        assert_eq!(resolved_primary, home_a.join(".agents").join("skills"));

        let resolved_fallback =
            resolve_codex_skills_dir_with_env(Some(OsStr::new("")), Some(home_b.as_os_str()))
                .expect("resolve fallback USERPROFILE");
        assert_eq!(resolved_fallback, home_b.join(".agents").join("skills"));

        let missing_err = resolve_codex_skills_dir_with_env(None, Some(OsStr::new("")))
            .expect_err("missing home must error");
        assert_eq!(missing_err.code, NativeErrorCode::InvalidConfig);

        let relative_err =
            resolve_codex_skills_dir_with_env(Some(OsStr::new("relative/home")), None)
                .expect_err("relative home must error");
        assert_eq!(relative_err.code, NativeErrorCode::InvalidConfig);

        let traversal_err =
            resolve_codex_skills_dir_with_env(Some(root.join("..").as_os_str()), None)
                .expect_err("traversal home must error");
        assert_eq!(traversal_err.code, NativeErrorCode::SecurityViolation);
    }
}
