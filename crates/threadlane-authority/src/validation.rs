use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

use sha2::{Digest, Sha256};
use threadlane_plans::{
    is_protected_control_path, validate_identifier as validate_plan_identifier,
    validate_js_safe_revision,
};
use threadlane_registry::ToolAdapter;
use threadlane_workers::git::WORK_AREA_LOCK_FILENAME;
use threadlane_workers::{
    inspect_base_commit_path_kind, is_control_config_path, resolve_exact_commit_sha,
    validate_absolute_no_symlink_components, validate_exact_40_hex_sha, validate_executable_file,
    validate_existing_canonical_dir, validate_permission_pattern_path,
    validate_permission_pattern_str, validate_repo_relative_path, validate_simple_identifier,
    BaseCommitPathKind, ExecutionBudgets, WorkerFilePermissions, WorkerProvider, WorkerSpec,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS, MAX_PATH_ENTRY_LEN,
};
use uuid::Uuid;

use crate::error::AuthorityError;
use crate::types::{
    ApprovalRecord, ApprovedTaskRecord, AUTHORITY_POLICY_VERSION, AUTHORITY_SCHEMA_VERSION,
    MAX_APPROVAL_MAX_PARALLEL, MAX_APPROVED_TASKS, MAX_APPROVED_WRITE_PATHS,
    MAX_EXECUTABLE_PROFILES_PER_APPROVAL, MIN_APPROVAL_MAX_PARALLEL,
};

/// Validates a non-empty bounded identifier using `threadlane-plans` rules.
pub fn validate_identifier(field: &'static str, value: &str) -> Result<String, AuthorityError> {
    validate_plan_identifier(field, value).map_err(|err| AuthorityError::Validation {
        field: field.to_string(),
        reason: err.to_string(),
    })
}

/// Validates a JS-safe revision number (`<= 9_007_199_254_740_991`).
pub fn validate_revision(field: &'static str, revision: u64) -> Result<u64, AuthorityError> {
    validate_js_safe_revision(field, revision).map_err(|err| AuthorityError::Validation {
        field: field.to_string(),
        reason: err.to_string(),
    })
}

/// Validates that `raw` is a canonical lowercase hyphenated non-nil UUID string.
pub fn validate_canonical_thread_uuid(
    field: &'static str,
    raw: &str,
) -> Result<String, AuthorityError> {
    let parsed = Uuid::parse_str(raw).map_err(|err| {
        AuthorityError::validation(field, format!("`{raw}` is not a valid UUID: {err}"))
    })?;
    if parsed.is_nil() {
        return Err(AuthorityError::validation(
            field,
            "thread UUID must not be the nil UUID (00000000-0000-0000-0000-000000000000)",
        ));
    }
    let canonical = parsed.hyphenated().to_string();
    if raw != canonical {
        return Err(AuthorityError::validation(
            field,
            format!(
                "thread UUID `{raw}` must use canonical lowercase hyphenated form `{canonical}`"
            ),
        ));
    }
    Ok(canonical)
}

/// Validates a 64-character lowercase hexadecimal SHA-256 digest string.
pub fn validate_sha256_hex(field: &'static str, raw: &str) -> Result<String, AuthorityError> {
    if raw.len() != 64 || !raw.chars().all(|ch| matches!(ch, '0'..='9' | 'a'..='f')) {
        return Err(AuthorityError::validation(
            field,
            format!("`{raw}` must be a 64-character lowercase hexadecimal SHA-256 digest"),
        ));
    }
    Ok(raw.to_string())
}

/// Validates that `repository_path` is an existing canonical non-symlink Git repository directory
/// and that `base_commit` is an exact 40-character hexadecimal commit SHA existing in that
/// repository via bounded sanitized read-only Git.
pub fn validate_repository_and_base_commit(
    repository_path: &Path,
    base_commit: &str,
) -> Result<(PathBuf, String), AuthorityError> {
    let canonical_repo = validate_existing_canonical_dir(repository_path, "repository_path")
        .map_err(|err| AuthorityError::InvalidRepository {
            path: repository_path.display().to_string(),
            reason: err.to_string(),
        })?;

    let git_entry = canonical_repo.join(".git");
    let git_meta =
        fs::symlink_metadata(&git_entry).map_err(|err| AuthorityError::InvalidRepository {
            path: canonical_repo.display().to_string(),
            reason: format!("repository does not contain a `.git` entry: {err}"),
        })?;
    if git_meta.file_type().is_symlink() {
        return Err(AuthorityError::SecurityViolation {
            path: git_entry.display().to_string(),
            reason: "repository `.git` entry must not be a symlink".to_string(),
        });
    }

    let normalized_commit =
        validate_exact_40_hex_sha(base_commit, "base_commit").map_err(|err| {
            AuthorityError::InvalidBaseCommit {
                base_commit: base_commit.to_string(),
                reason: err.to_string(),
            }
        })?;

    let resolved_commit = resolve_exact_commit_sha(
        &canonical_repo,
        &normalized_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| AuthorityError::InvalidBaseCommit {
        base_commit: base_commit.to_string(),
        reason: err.to_string(),
    })?;

    if resolved_commit != normalized_commit {
        return Err(AuthorityError::InvalidBaseCommit {
            base_commit: base_commit.to_string(),
            reason: format!(
                "resolved commit `{resolved_commit}` did not match requested 40-hex SHA `{normalized_commit}`"
            ),
        });
    }

    Ok((canonical_repo, normalized_commit))
}

fn validate_write_scope_entry_syntax<'a>(
    entry: &'a str,
    label: &str,
) -> Result<(bool, &'a str), AuthorityError> {
    if entry.is_empty() || entry.len() > MAX_PATH_ENTRY_LEN {
        return Err(AuthorityError::InvalidWriteScope {
            path: entry.to_string(),
            reason: format!("{label} must be between 1 and {MAX_PATH_ENTRY_LEN} bytes"),
        });
    }
    if entry.trim() != entry {
        return Err(AuthorityError::InvalidWriteScope {
            path: entry.to_string(),
            reason: format!("{label} must not have leading or trailing whitespace"),
        });
    }

    validate_repo_relative_path(entry, "allowed_write_paths entry").map_err(|err| {
        AuthorityError::InvalidWriteScope {
            path: entry.to_string(),
            reason: err.to_string(),
        }
    })?;

    let has_trailing_slash = entry.ends_with('/');
    let clean = entry.strip_suffix('/').unwrap_or(entry);
    if clean.is_empty() || clean.ends_with('/') {
        return Err(AuthorityError::InvalidWriteScope {
            path: entry.to_string(),
            reason: format!("{label} must not be empty or end with multiple slashes"),
        });
    }

    validate_permission_pattern_str(clean, label).map_err(|err| {
        AuthorityError::InvalidWriteScope {
            path: entry.to_string(),
            reason: err.to_string(),
        }
    })?;
    if clean.contains('!') {
        return Err(AuthorityError::InvalidWriteScope {
            path: entry.to_string(),
            reason: format!("{label} `{entry}` must not contain `!` pattern negation characters"),
        });
    }

    if is_protected_control_path(clean) || is_control_config_path(clean) {
        return Err(AuthorityError::InvalidWriteScope {
            path: entry.to_string(),
            reason: format!(
                "{label} `{entry}` targets protected repository control or agent configuration files"
            ),
        });
    }

    Ok((has_trailing_slash, clean))
}

/// Validates a repository-relative write scope entry (either an operator allowlist entry or a
/// task `allowed_write_paths` entry) against format, traversal, metacharacter, protected control,
/// symlink, and working-tree file-vs-directory ambiguity rules.
pub fn validate_write_scope_entry(
    entry: &str,
    canonical_repo: &Path,
    label: &str,
) -> Result<String, AuthorityError> {
    let (has_trailing_slash, clean) = validate_write_scope_entry_syntax(entry, label)?;

    let joined = canonical_repo.join(clean);
    validate_absolute_no_symlink_components(&joined, label).map_err(|err| {
        AuthorityError::InvalidWriteScope {
            path: entry.to_string(),
            reason: err.to_string(),
        }
    })?;

    if let Ok(meta) = fs::symlink_metadata(&joined) {
        if meta.file_type().is_symlink() {
            return Err(AuthorityError::InvalidWriteScope {
                path: entry.to_string(),
                reason: format!("{label} `{entry}` targets a symlink in the repository"),
            });
        }
        if has_trailing_slash && !meta.file_type().is_dir() {
            return Err(AuthorityError::InvalidWriteScope {
                path: entry.to_string(),
                reason: format!(
                    "directory write scope `{entry}` (with trailing `/`) targets an existing non-directory in the repository"
                ),
            });
        }
        if !has_trailing_slash && meta.file_type().is_dir() {
            return Err(AuthorityError::InvalidWriteScope {
                path: entry.to_string(),
                reason: format!(
                    "ambiguous write scope `{entry}` targets an existing directory without a trailing `/`; directory scopes must end with `/`"
                ),
            });
        }
    }

    Ok(entry.to_string())
}

/// Validates a repository-relative write scope entry against syntax, traversal, metacharacters,
/// protected control paths, symlink components, working-tree metadata, **and** the immutable
/// pinned `base_commit` tree in `canonical_repo`.
///
/// Rejects bare write scopes (`"foo"`, without trailing `/`) when `"foo"` is a directory (`tree`)
/// at `base_commit` even if the mutable working tree currently has `"foo"` as a regular file or
/// absent, preventing legacy runtime base-commit directory normalization from widening an
/// operator's exact-file approval into a directory subtree.
pub fn validate_write_scope_entry_at_base_commit(
    entry: &str,
    canonical_repo: &Path,
    base_commit: &str,
    label: &str,
) -> Result<String, AuthorityError> {
    let (has_trailing_slash, clean) = validate_write_scope_entry_syntax(entry, label)?;

    let joined = canonical_repo.join(clean);
    validate_absolute_no_symlink_components(&joined, label).map_err(|err| {
        AuthorityError::InvalidWriteScope {
            path: entry.to_string(),
            reason: err.to_string(),
        }
    })?;

    let base_kind = inspect_base_commit_path_kind(
        canonical_repo,
        base_commit,
        clean,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| AuthorityError::InvalidWriteScope {
        path: entry.to_string(),
        reason: format!("failed to inspect `{entry}` at base_commit `{base_commit}`: {err}"),
    })?;

    match base_kind {
        BaseCommitPathKind::Symlink => {
            return Err(AuthorityError::InvalidWriteScope {
                path: entry.to_string(),
                reason: format!(
                    "{label} `{entry}` targets or traverses a symlink at base_commit `{base_commit}`"
                ),
            });
        }
        BaseCommitPathKind::Gitlink => {
            return Err(AuthorityError::InvalidWriteScope {
                path: entry.to_string(),
                reason: format!(
                    "{label} `{entry}` targets or traverses a submodule gitlink at base_commit `{base_commit}`"
                ),
            });
        }
        BaseCommitPathKind::Directory => {
            if !has_trailing_slash {
                return Err(AuthorityError::InvalidWriteScope {
                    path: entry.to_string(),
                    reason: format!(
                        "ambiguous write scope `{entry}` targets a directory at base_commit `{base_commit}` without a trailing `/`; directory scopes must end with `/`"
                    ),
                });
            }
        }
        BaseCommitPathKind::RegularFile => {
            if has_trailing_slash {
                return Err(AuthorityError::InvalidWriteScope {
                    path: entry.to_string(),
                    reason: format!(
                        "directory write scope `{entry}` (with trailing `/`) targets a non-directory file at base_commit `{base_commit}`"
                    ),
                });
            }
        }
        BaseCommitPathKind::Missing => {}
    }

    if let Ok(meta) = fs::symlink_metadata(&joined) {
        if meta.file_type().is_symlink() {
            return Err(AuthorityError::InvalidWriteScope {
                path: entry.to_string(),
                reason: format!("{label} `{entry}` targets a symlink in the repository"),
            });
        }
        if has_trailing_slash
            && !meta.file_type().is_dir()
            && base_kind != BaseCommitPathKind::Directory
        {
            return Err(AuthorityError::InvalidWriteScope {
                path: entry.to_string(),
                reason: format!(
                    "directory write scope `{entry}` (with trailing `/`) targets an existing non-directory in the repository"
                ),
            });
        }
        if !has_trailing_slash && meta.file_type().is_dir() {
            return Err(AuthorityError::InvalidWriteScope {
                path: entry.to_string(),
                reason: format!(
                    "ambiguous write scope `{entry}` targets an existing directory without a trailing `/`; directory scopes must end with `/`"
                ),
            });
        }
    }

    Ok(entry.to_string())
}

/// Checks whether a single validated task write scope entry `task_scope` is contained within
/// at least one entry of `operator_allowlist` componentwise:
/// - Operator entries without a trailing `/` match only the exact same file path (`!task_is_dir && task_scope == op_scope`).
/// - Operator entries with a trailing `/` (`"dir/"`) match descendant files/directories under `"dir"` componentwise.
pub fn is_task_scope_contained_in_operator_allowlist(
    task_scope: &str,
    operator_allowlist: &[String],
) -> bool {
    let task_is_dir = task_scope.ends_with('/');
    let clean_task = task_scope.strip_suffix('/').unwrap_or(task_scope);
    let task_path = Path::new(clean_task);

    operator_allowlist.iter().any(|op_scope| {
        if let Some(clean_op) = op_scope.strip_suffix('/') {
            let op_path = Path::new(clean_op);
            if !task_path.starts_with(op_path) {
                return false;
            }
            if clean_task == clean_op {
                task_is_dir
            } else {
                true
            }
        } else {
            !task_is_dir && task_scope == op_scope
        }
    })
}

/// Resolves a CLI worker tool executable once at approval time to its canonical regular
/// executable file path.
///
/// Supports optional `PATH` lookup when `configured_path` is `None` (or a bare command name) and
/// canonicalizes symlink inputs (such as `/bin/sh` or `/run/current-system/sw/bin/...` on NixOS)
/// to their final target, which must pass [`validate_executable_file`] (existing, absolute,
/// non-symlink regular file with execute permissions). Never spawns or probes the CLI binary.
pub fn resolve_and_validate_worker_executable(
    tool_id: &str,
    task_id: &str,
    adapter: ToolAdapter,
    configured_path: Option<&Path>,
) -> Result<(WorkerProvider, PathBuf), AuthorityError> {
    let (provider, default_bin_name) = match adapter {
        ToolAdapter::Antigravity => (WorkerProvider::Antigravity, "agy"),
        ToolAdapter::OpencodeV2 => (WorkerProvider::OpenCodeV2, "opencode"),
        _ => {
            return Err(AuthorityError::UnsupportedTaskAdapter {
                plan_id: String::new(),
                task_id: task_id.to_string(),
                adapter,
                reason: format!("adapter `{adapter}` is not a headless CLI worker provider"),
            });
        }
    };

    let candidate_path = match configured_path {
        Some(path) => {
            if path.as_os_str().is_empty() {
                return Err(AuthorityError::InvalidExecutable {
                    tool_id: tool_id.to_string(),
                    task_id: task_id.to_string(),
                    reason: "configured executable_path must not be empty".to_string(),
                });
            }
            for comp in path.components() {
                if matches!(comp, Component::CurDir | Component::ParentDir) {
                    return Err(AuthorityError::InvalidExecutable {
                        tool_id: tool_id.to_string(),
                        task_id: task_id.to_string(),
                        reason: format!(
                            "executable_path `{}` must not contain `.` or `..` traversal components",
                            path.display()
                        ),
                    });
                }
            }
            if path.is_absolute() {
                path.to_path_buf()
            } else if path.components().count() == 1 {
                resolve_binary_in_path(path.as_os_str(), tool_id, task_id)?
            } else {
                return Err(AuthorityError::InvalidExecutable {
                    tool_id: tool_id.to_string(),
                    task_id: task_id.to_string(),
                    reason: format!(
                        "relative executable_path `{}` with path separators is forbidden; use an absolute path or a bare command name",
                        path.display()
                    ),
                });
            }
        }
        None => resolve_binary_in_path(std::ffi::OsStr::new(default_bin_name), tool_id, task_id)?,
    };

    let canonical =
        fs::canonicalize(&candidate_path).map_err(|err| AuthorityError::InvalidExecutable {
            tool_id: tool_id.to_string(),
            task_id: task_id.to_string(),
            reason: format!(
                "failed to canonicalize CLI executable `{}`: {err}",
                candidate_path.display()
            ),
        })?;

    let validated = validate_executable_file(&canonical).map_err(|err| {
        AuthorityError::InvalidExecutable {
            tool_id: tool_id.to_string(),
            task_id: task_id.to_string(),
            reason: format!(
                "canonical target `{}` (resolved from `{}`) is not a valid regular executable: {err}",
                canonical.display(),
                candidate_path.display()
            ),
        }
    })?;

    Ok((provider, validated))
}

fn resolve_binary_in_path(
    bin_name: &std::ffi::OsStr,
    tool_id: &str,
    task_id: &str,
) -> Result<PathBuf, AuthorityError> {
    let path_var = std::env::var_os("PATH").ok_or_else(|| AuthorityError::InvalidExecutable {
        tool_id: tool_id.to_string(),
        task_id: task_id.to_string(),
        reason: format!(
            "cannot resolve `{}`: PATH environment variable is not set",
            bin_name.to_string_lossy()
        ),
    })?;

    for dir in std::env::split_paths(&path_var) {
        if !dir.is_absolute() {
            continue;
        }
        let candidate = dir.join(bin_name);
        if let Ok(meta) = fs::metadata(&candidate) {
            if meta.is_file() {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    if meta.permissions().mode() & 0o111 != 0 {
                        return Ok(candidate);
                    }
                }
                #[cfg(not(unix))]
                {
                    return Ok(candidate);
                }
            }
        }
    }

    Err(AuthorityError::InvalidExecutable {
        tool_id: tool_id.to_string(),
        task_id: task_id.to_string(),
        reason: format!(
            "executable `{}` was not found in PATH",
            bin_name.to_string_lossy()
        ),
    })
}

/// Validates a headless worker task's model, effort, canonical executable, repository, base
/// commit, write scope, and budgets at approval time using `threadlane_workers::WorkerSpec`.
// Validate the complete frozen execution tuple without changing any of its fields.
#[allow(clippy::too_many_arguments)]
pub fn validate_worker_task_static(
    provider: WorkerProvider,
    canonical_executable: &Path,
    model_arg: &str,
    effort: Option<&str>,
    canonical_repo: &Path,
    base_commit: &str,
    allowed_write_paths: &[String],
    instruction: &str,
    budgets: &ExecutionBudgets,
    authority_dir: &Path,
) -> Result<(String, Option<String>, String), AuthorityError> {
    let write_set: BTreeSet<String> = allowed_write_paths.iter().cloned().collect();
    let standalone = provider == WorkerProvider::OpenCodeV2;
    let probe_work_area = authority_dir.join("__validation_work_area_preview__");

    let spec = WorkerSpec {
        provider,
        executable: canonical_executable.to_path_buf(),
        model: model_arg.to_string(),
        effort: effort.map(str::to_string),
        allowed_models: BTreeSet::new(),
        session_id: None,
        standalone,
        repository_path: canonical_repo.to_path_buf(),
        base_commit: base_commit.to_string(),
        work_area_root: probe_work_area,
        allowed_write_paths: write_set,
        prompt: instruction.to_string(),
        file_permissions: WorkerFilePermissions::TaskFiles,
        budgets: budgets.clone(),
    };

    let report = spec.validate().map_err(|err| AuthorityError::Validation {
        field: "task_worker_spec".to_string(),
        reason: err.to_string(),
    })?;

    Ok((
        report.resolved_model.base_model,
        report.resolved_model.effort,
        report.resolved_model.cli_model_arg,
    ))
}

/// Validates `work_area_root` during [`crate::AuthorityStore::materialize`] without mutating the
/// filesystem or creating worktrees.
pub fn validate_work_area_for_materialization(
    work_area_root: &Path,
    canonical_repo: &Path,
    authority_dir: &Path,
    registry_dir: &Path,
    plans_dir: &Path,
) -> Result<PathBuf, AuthorityError> {
    let checked = validate_absolute_no_symlink_components(work_area_root, "work_area_root")
        .map_err(|err| AuthorityError::InvalidWorkArea {
            path: work_area_root.display().to_string(),
            reason: err.to_string(),
        })?;

    if checked.parent().is_none() {
        return Err(AuthorityError::InvalidWorkArea {
            path: checked.display().to_string(),
            reason: "work_area_root must not be the filesystem root directory `/`".to_string(),
        });
    }

    // Must not overlap repository_path (or repository_path/.git) or any store directory.
    if checked.starts_with(canonical_repo) || canonical_repo.starts_with(&checked) {
        return Err(AuthorityError::InvalidWorkArea {
            path: checked.display().to_string(),
            reason: format!(
                "work_area_root `{}` must not overlap repository_path `{}` or its `.git` directory",
                checked.display(),
                canonical_repo.display()
            ),
        });
    }
    for (label, store_dir) in [
        ("authority store", authority_dir),
        ("registry store", registry_dir),
        ("plan store", plans_dir),
    ] {
        if checked.starts_with(store_dir) || store_dir.starts_with(&checked) {
            return Err(AuthorityError::InvalidWorkArea {
                path: checked.display().to_string(),
                reason: format!(
                    "work_area_root `{}` must not overlap {label} directory `{}`",
                    checked.display(),
                    store_dir.display()
                ),
            });
        }
    }

    // Verify existing directory or nearest existing operator-owned ancestor directory.
    #[cfg(unix)]
    let expected_uid = {
        use std::os::unix::fs::MetadataExt;
        fs::symlink_metadata(authority_dir).map(|m| m.uid()).ok()
    };

    if let Ok(meta) = fs::symlink_metadata(&checked) {
        if meta.file_type().is_symlink() {
            return Err(AuthorityError::InvalidWorkArea {
                path: checked.display().to_string(),
                reason: "work_area_root must not be a symlink".to_string(),
            });
        }
        if !meta.file_type().is_dir() {
            return Err(AuthorityError::InvalidWorkArea {
                path: checked.display().to_string(),
                reason: "work_area_root must be a directory".to_string(),
            });
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            if let Some(uid) = expected_uid {
                if meta.uid() != uid {
                    return Err(AuthorityError::InvalidWorkArea {
                        path: checked.display().to_string(),
                        reason: format!(
                            "existing work_area_root is owned by uid {}, expected operator uid {uid}",
                            meta.uid()
                        ),
                    });
                }
            }
            let mode = meta.permissions().mode() & 0o777;
            if mode & 0o077 != 0 {
                return Err(AuthorityError::InvalidWorkArea {
                    path: checked.display().to_string(),
                    reason: format!(
                        "existing work_area_root has non-private permissions {:04o} (expected 0700)",
                        mode
                    ),
                });
            }
        }

        let mut entries = Vec::new();
        let read_dir = fs::read_dir(&checked).map_err(|err| AuthorityError::InvalidWorkArea {
            path: checked.display().to_string(),
            reason: format!("failed to inspect existing work_area_root: {err}"),
        })?;
        for entry_res in read_dir {
            let entry = entry_res.map_err(|err| AuthorityError::InvalidWorkArea {
                path: checked.display().to_string(),
                reason: format!("failed to read directory entry in work_area_root: {err}"),
            })?;
            let entry_name = entry.file_name().to_string_lossy().to_string();
            if entry_name == WORK_AREA_LOCK_FILENAME {
                threadlane_workers::validate_preexisting_private_file(
                    &entry.path(),
                    "work area ownership lock",
                )
                .map_err(|err| AuthorityError::InvalidWorkArea {
                    path: checked.display().to_string(),
                    reason: err.to_string(),
                })?;
                let lock_meta = fs::symlink_metadata(entry.path()).map_err(|err| {
                    AuthorityError::InvalidWorkArea {
                        path: checked.display().to_string(),
                        reason: format!("cannot inspect work area ownership lock: {err}"),
                    }
                })?;
                if lock_meta.len() != 0 {
                    return Err(AuthorityError::InvalidWorkArea {
                        path: checked.display().to_string(),
                        reason: "work area ownership lock must be empty".to_string(),
                    });
                }
            }
            entries.push(entry_name);
        }
        if !entries.is_empty() {
            let has_marker = entries.iter().any(|n| n == ".threadlane-work-area.json");
            let has_unexpected = entries.iter().any(|n| {
                !matches!(
                    n.as_str(),
                    ".threadlane-work-area.json"
                        | WORK_AREA_LOCK_FILENAME
                        | "manifests"
                        | "checkouts"
                )
            });
            if !has_marker || has_unexpected {
                return Err(AuthorityError::InvalidWorkArea {
                    path: checked.display().to_string(),
                    reason: "existing non-empty work_area_root is not a valid Threadlane work area"
                        .to_string(),
                });
            }
        }
    } else {
        // Find the nearest existing ancestor directory and verify it is a valid operator-owned directory.
        let mut ancestor = checked.parent();
        let mut found_existing_ancestor = false;
        while let Some(parent) = ancestor {
            if let Ok(meta) = fs::symlink_metadata(parent) {
                if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
                    return Err(AuthorityError::InvalidWorkArea {
                        path: checked.display().to_string(),
                        reason: format!(
                            "work_area_root ancestor `{}` is not a regular non-symlink directory",
                            parent.display()
                        ),
                    });
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt;
                    if let Some(uid) = expected_uid {
                        if meta.uid() != uid {
                            return Err(AuthorityError::InvalidWorkArea {
                                path: checked.display().to_string(),
                                reason: format!(
                                    "work_area_root ancestor `{}` is owned by uid {}, expected operator uid {uid}",
                                    parent.display(),
                                    meta.uid()
                                ),
                            });
                        }
                    }
                }
                found_existing_ancestor = true;
                break;
            }
            ancestor = parent.parent();
        }
        if !found_existing_ancestor {
            return Err(AuthorityError::InvalidWorkArea {
                path: checked.display().to_string(),
                reason: "work_area_root has no existing ancestor directory".to_string(),
            });
        }
    }

    validate_permission_pattern_path(&checked, "work_area_root").map_err(|err| {
        AuthorityError::InvalidWorkArea {
            path: checked.display().to_string(),
            reason: err.to_string(),
        }
    })?;

    Ok(checked)
}

/// Computes the preliminary authority core digest (before populating `profile_id` and `command_key`)
/// so `profile_id` and `command_key` are deterministically derived from the full approval context
/// and task digest.
pub fn compute_authority_core_digest(record: &ApprovalRecord) -> Result<String, AuthorityError> {
    let tasks_json: Vec<serde_json::Value> = record
        .tasks
        .iter()
        .map(|task| {
            serde_json::json!({
                "task_id": task.task_id,
                "title": task.title,
                "instruction": task.instruction,
                "instruction_digest": task.instruction_digest,
                "role": task.role.as_str(),
                "difficulty": task.difficulty.as_str(),
                "tool_id": task.tool_id,
                "adapter": task.adapter.as_str(),
                "provider": task.provider.map(WorkerProvider::as_str),
                "canonical_executable": task.canonical_executable.as_ref().map(|p| p.to_string_lossy().to_string()),
                "model_id": task.model_id,
                "model_arg": task.model_arg,
                "base_model": task.base_model,
                "effort": task.effort,
                "cli_model_arg": task.cli_model_arg,
                "standalone": task.standalone,
                "file_permissions": task.file_permissions.map(WorkerFilePermissions::as_str),
                "depends_on": task.depends_on,
                "allowed_write_paths": task.allowed_write_paths,
                "task_digest": task.task_digest,
            })
        })
        .collect();

    let canonical = serde_json::json!({
        "domain": "threadlane-authority-core-v1",
        "schema_version": AUTHORITY_SCHEMA_VERSION,
        "policy_version": record.policy_version,
        "binding_id": record.binding_id,
        "thread_uuid": record.thread_uuid,
        "plan_id": record.plan_id,
        "plan_revision": record.plan_revision,
        "profile_id": record.profile_id,
        "registry_revision": record.registry_revision,
        "plan_digest": record.plan_digest,
        "repository_path": record.repository_path.to_string_lossy(),
        "base_commit": record.base_commit,
        "approved_task_ids": record.approved_task_ids,
        "approved_write_paths": record.approved_write_paths,
        "max_parallel": record.max_parallel,
        "budgets": {
            "timeout_ms": record.budgets.timeout_ms,
            "max_line_bytes": record.budgets.max_line_bytes,
            "max_stdout_bytes": record.budgets.max_stdout_bytes,
            "max_stderr_bytes": record.budgets.max_stderr_bytes,
            "max_events": record.budgets.max_events,
        },
        "tasks": tasks_json,
    });

    let bytes = serde_json::to_vec(&canonical).map_err(|err| {
        AuthorityError::validation(
            "subject_digest",
            format!("JSON serialization failed: {err}"),
        )
    })?;
    Ok(sha256_hex(&bytes))
}

/// Computes the full 64-character lowercase hex SHA-256 `subject_digest` over all frozen fields
/// of an [`ApprovalRecord`].
pub fn compute_record_subject_digest(record: &ApprovalRecord) -> Result<String, AuthorityError> {
    let tasks_json: Vec<serde_json::Value> = record
        .tasks
        .iter()
        .map(|task| {
            serde_json::json!({
                "task_id": task.task_id,
                "title": task.title,
                "instruction": task.instruction,
                "instruction_digest": task.instruction_digest,
                "role": task.role.as_str(),
                "difficulty": task.difficulty.as_str(),
                "tool_id": task.tool_id,
                "adapter": task.adapter.as_str(),
                "provider": task.provider.map(WorkerProvider::as_str),
                "canonical_executable": task.canonical_executable.as_ref().map(|p| p.to_string_lossy().to_string()),
                "model_id": task.model_id,
                "model_arg": task.model_arg,
                "base_model": task.base_model,
                "effort": task.effort,
                "cli_model_arg": task.cli_model_arg,
                "standalone": task.standalone,
                "file_permissions": task.file_permissions.map(WorkerFilePermissions::as_str),
                "depends_on": task.depends_on,
                "allowed_write_paths": task.allowed_write_paths,
                "task_digest": task.task_digest,
                "profile_id": task.profile_id,
                "command_key": task.command_key,
            })
        })
        .collect();

    let canonical = serde_json::json!({
        "domain": "threadlane-authority-subject-v1",
        "schema_version": AUTHORITY_SCHEMA_VERSION,
        "policy_version": record.policy_version,
        "binding_id": record.binding_id,
        "thread_uuid": record.thread_uuid,
        "plan_id": record.plan_id,
        "plan_revision": record.plan_revision,
        "profile_id": record.profile_id,
        "registry_revision": record.registry_revision,
        "plan_digest": record.plan_digest,
        "repository_path": record.repository_path.to_string_lossy(),
        "base_commit": record.base_commit,
        "approved_task_ids": record.approved_task_ids,
        "approved_write_paths": record.approved_write_paths,
        "max_parallel": record.max_parallel,
        "budgets": {
            "timeout_ms": record.budgets.timeout_ms,
            "max_line_bytes": record.budgets.max_line_bytes,
            "max_stdout_bytes": record.budgets.max_stdout_bytes,
            "max_stderr_bytes": record.budgets.max_stderr_bytes,
            "max_events": record.budgets.max_events,
        },
        "tasks": tasks_json,
    });

    let bytes = serde_json::to_vec(&canonical).map_err(|err| {
        AuthorityError::validation(
            "subject_digest",
            format!("JSON serialization failed: {err}"),
        )
    })?;
    Ok(sha256_hex(&bytes))
}

/// Derives the deterministic `approval_id` (`<= 64` chars) from a 64-hex `subject_digest`.
pub fn derive_approval_id(subject_digest: &str) -> String {
    let prefix = &subject_digest[..32.min(subject_digest.len())];
    format!("apr_{prefix}")
}

/// Derives a deterministic worker `profile_id` (`<= 64` chars) from the authority core digest,
/// `task_id`, and `task_digest`.
pub fn derive_profile_id(authority_core_digest: &str, task_id: &str, task_digest: &str) -> String {
    let material =
        format!("threadlane-authority-profile-v1:{authority_core_digest}:{task_id}:{task_digest}");
    let digest = sha256_hex(material.as_bytes());
    let core_tag = &authority_core_digest[..12.min(authority_core_digest.len())];
    let task_tag = &digest[..20];
    format!("prf_{core_tag}_{task_tag}")
}

/// Derives a deterministic idempotency `command_key` (`<= 64` chars) from the authority core
/// digest, `task_id`, and `task_digest`.
pub fn derive_command_key(authority_core_digest: &str, task_id: &str, task_digest: &str) -> String {
    let material =
        format!("threadlane-authority-cmd-v1:{authority_core_digest}:{task_id}:{task_digest}");
    let digest = sha256_hex(material.as_bytes());
    let core_tag = &authority_core_digest[..12.min(authority_core_digest.len())];
    let task_tag = &digest[..20];
    format!("cmd_{core_tag}_{task_tag}")
}

/// Verifies the full structural and cryptographic integrity of a persisted [`ApprovalRecord`].
pub fn verify_approval_record_integrity(
    record: &ApprovalRecord,
    store_path: &Path,
) -> Result<(), AuthorityError> {
    if record.policy_version != AUTHORITY_POLICY_VERSION {
        return Err(AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: format!(
                "unsupported policy_version {} on approval `{}` (expected {AUTHORITY_POLICY_VERSION})",
                record.policy_version, record.approval_id
            ),
        });
    }

    validate_identifier("binding_id", &record.binding_id).map_err(|err| {
        AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: err.to_string(),
        }
    })?;
    validate_canonical_thread_uuid("thread_uuid", &record.thread_uuid).map_err(|err| {
        AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: err.to_string(),
        }
    })?;
    validate_identifier("plan_id", &record.plan_id).map_err(|err| {
        AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: err.to_string(),
        }
    })?;
    validate_identifier("profile_id", &record.profile_id).map_err(|err| {
        AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: err.to_string(),
        }
    })?;
    validate_sha256_hex("plan_digest", &record.plan_digest).map_err(|err| {
        AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: err.to_string(),
        }
    })?;
    validate_sha256_hex("subject_digest", &record.subject_digest).map_err(|err| {
        AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: err.to_string(),
        }
    })?;
    let normalized_base =
        validate_exact_40_hex_sha(&record.base_commit, "base_commit").map_err(|err| {
            AuthorityError::CorruptStore {
                path: store_path.display().to_string(),
                reason: err.to_string(),
            }
        })?;
    if record.base_commit != normalized_base {
        return Err(AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: format!(
                "approval `{}` base_commit `{}` is not normalized lowercase hex",
                record.approval_id, record.base_commit
            ),
        });
    }
    if !(MIN_APPROVAL_MAX_PARALLEL..=MAX_APPROVAL_MAX_PARALLEL).contains(&record.max_parallel) {
        return Err(AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: format!(
                "approval `{}` max_parallel {} is outside {}..={}",
                record.approval_id,
                record.max_parallel,
                MIN_APPROVAL_MAX_PARALLEL,
                MAX_APPROVAL_MAX_PARALLEL
            ),
        });
    }
    record
        .budgets
        .validate()
        .map_err(|err| AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: err.to_string(),
        })?;
    if record.approved_write_paths.is_empty()
        || record.approved_write_paths.len() > MAX_APPROVED_WRITE_PATHS
    {
        return Err(AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: format!(
                "approval `{}` approved_write_paths count {} is invalid",
                record.approval_id,
                record.approved_write_paths.len()
            ),
        });
    }

    let mut seen_op_scopes = BTreeSet::new();
    for op_path in &record.approved_write_paths {
        validate_stored_write_scope_syntax(
            op_path,
            "approved_write_paths entry",
            &record.approval_id,
            store_path,
        )?;
        if !seen_op_scopes.insert(op_path.clone()) {
            return Err(AuthorityError::CorruptStore {
                path: store_path.display().to_string(),
                reason: format!(
                    "approval `{}` contains duplicate approved_write_paths entry `{op_path}`",
                    record.approval_id
                ),
            });
        }
    }

    if record.tasks.is_empty()
        || record.tasks.len() > MAX_APPROVED_TASKS
        || record.approved_task_ids.len() != record.tasks.len()
    {
        return Err(AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: format!(
                "approval `{}` task list does not match approved_task_ids",
                record.approval_id
            ),
        });
    }
    let worker_profile_count = record.tasks.iter().filter(|t| t.provider.is_some()).count();
    if worker_profile_count > MAX_EXECUTABLE_PROFILES_PER_APPROVAL {
        return Err(AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: format!(
                "approval `{}` executable profile count {} exceeds maximum {}",
                record.approval_id, worker_profile_count, MAX_EXECUTABLE_PROFILES_PER_APPROVAL
            ),
        });
    }

    for (expected_id, task) in record.approved_task_ids.iter().zip(record.tasks.iter()) {
        if expected_id != &task.task_id {
            return Err(AuthorityError::CorruptStore {
                path: store_path.display().to_string(),
                reason: format!(
                    "approval `{}` task order mismatch: expected `{expected_id}`, got `{}`",
                    record.approval_id, task.task_id
                ),
            });
        }
        verify_task_record_structure(
            task,
            &record.approved_write_paths,
            &record.approval_id,
            store_path,
        )?;
    }

    let core_digest =
        compute_authority_core_digest(record).map_err(|err| AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: err.to_string(),
        })?;

    for task in &record.tasks {
        if task.provider.is_some() {
            let expected_profile_id =
                derive_profile_id(&core_digest, &task.task_id, &task.task_digest);
            let expected_command_key =
                derive_command_key(&core_digest, &task.task_id, &task.task_digest);
            if task.profile_id.as_deref() != Some(expected_profile_id.as_str()) {
                return Err(AuthorityError::CorruptStore {
                    path: store_path.display().to_string(),
                    reason: format!(
                        "approval `{}` task `{}` has tampered profile_id",
                        record.approval_id, task.task_id
                    ),
                });
            }
            if task.command_key.as_deref() != Some(expected_command_key.as_str()) {
                return Err(AuthorityError::CorruptStore {
                    path: store_path.display().to_string(),
                    reason: format!(
                        "approval `{}` task `{}` has tampered command_key",
                        record.approval_id, task.task_id
                    ),
                });
            }
        } else if task.profile_id.is_some() || task.command_key.is_some() {
            return Err(AuthorityError::CorruptStore {
                path: store_path.display().to_string(),
                reason: format!(
                    "approval `{}` native task `{}` must not have worker profile_id or command_key",
                    record.approval_id, task.task_id
                ),
            });
        }
    }

    let computed_subject =
        compute_record_subject_digest(record).map_err(|err| AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: err.to_string(),
        })?;
    if record.subject_digest != computed_subject {
        return Err(AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: format!(
                "approval `{}` subject_digest mismatch: stored `{}`, computed `{computed_subject}`",
                record.approval_id, record.subject_digest
            ),
        });
    }

    let expected_approval_id = derive_approval_id(&computed_subject);
    if record.approval_id != expected_approval_id {
        return Err(AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: format!(
                "approval_id mismatch: stored `{}`, expected `{expected_approval_id}`",
                record.approval_id
            ),
        });
    }

    Ok(())
}

fn validate_stored_write_scope_syntax(
    entry: &str,
    label: &str,
    approval_id: &str,
    store_path: &Path,
) -> Result<(), AuthorityError> {
    validate_write_scope_entry_syntax(entry, label)
        .map(|_| ())
        .map_err(|err| AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: format!("approval `{approval_id}` invalid {label} `{entry}`: {err}"),
        })
}

fn verify_task_record_structure(
    task: &ApprovedTaskRecord,
    approved_write_paths: &[String],
    approval_id: &str,
    store_path: &Path,
) -> Result<(), AuthorityError> {
    if task.allowed_write_paths.is_empty() {
        return Err(AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: format!(
                "approval `{approval_id}` task `{}` has empty allowed_write_paths",
                task.task_id
            ),
        });
    }
    for task_path in &task.allowed_write_paths {
        validate_stored_write_scope_syntax(
            task_path,
            "task allowed_write_paths entry",
            approval_id,
            store_path,
        )?;
        if !is_task_scope_contained_in_operator_allowlist(task_path, approved_write_paths) {
            return Err(AuthorityError::CorruptStore {
                path: store_path.display().to_string(),
                reason: format!(
                    "approval `{approval_id}` task `{}` write path `{task_path}` is not contained in approved_write_paths",
                    task.task_id
                ),
            });
        }
    }

    let computed_instruction_digest = sha256_hex(task.instruction.as_bytes());
    if task.instruction_digest != computed_instruction_digest {
        return Err(AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: format!(
                "approval `{approval_id}` task `{}` instruction_digest mismatch",
                task.task_id
            ),
        });
    }
    validate_sha256_hex("task_digest", &task.task_digest).map_err(|err| {
        AuthorityError::CorruptStore {
            path: store_path.display().to_string(),
            reason: err.to_string(),
        }
    })?;
    if let Some(ref profile_id) = task.profile_id {
        validate_simple_identifier(profile_id, "profile_id").map_err(|err| {
            AuthorityError::CorruptStore {
                path: store_path.display().to_string(),
                reason: err.to_string(),
            }
        })?;
    }
    Ok(())
}

/// Computes a 64-character lowercase hexadecimal SHA-256 digest of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}
