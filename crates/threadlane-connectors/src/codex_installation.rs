use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::parsers::MAX_VERSION_BYTES;
use crate::probe::check_resolved_file_executable;
use crate::version_inspection::{
    is_direct_nix_store_path, is_stable_nix_profile_path, ordered_system_search_dirs,
};

/// Maximum number of symlink hops permitted while resolving a candidate path.
pub const MAX_SYMLINK_HOPS: usize = 16;

/// Maximum number of chained shell wrapper scripts permitted during launcher discovery.
pub const MAX_WRAPPER_SCRIPT_HOPS: usize = 4;

/// Maximum number of files inspected while resolving a single desktop launcher candidate.
pub const MAX_INSPECTED_FILES: usize = 32;

/// Maximum byte size permitted for a shell wrapper script (`64 KiB`).
pub const MAX_WRAPPER_SCRIPT_BYTES: usize = 65_536;

/// Maximum byte size permitted for `linux-package-metadata.json` (`64 KiB`).
pub const MAX_PACKAGE_METADATA_BYTES: usize = 65_536;

/// Expected `codexAppBrand` field in `linux-package-metadata.json`.
pub const EXPECTED_DESKTOP_BRAND: &str = "chatgpt";

/// Expected `codexBuildFlavor` field in `linux-package-metadata.json`.
pub const EXPECTED_DESKTOP_BUILD_FLAVOR: &str = "prod";

/// Active bundled Codex CLI binary filename inside `resources/`.
pub const BUNDLED_CODEX_BINARY_NAME: &str = "codex";

/// Backup filename that must never be selected as the active CLI binary for metadata/probing.
pub const IGNORED_ORIGINAL_BINARY_NAME: &str = "codex-threadlane-original";

const DESKTOP_LAUNCHER_NAMES: &[&str] = &["chatgpt", "codex-desktop", "codex-launcher"];

/// Origin of a resolved system Codex CLI executable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexResolutionSource {
    /// Standalone `codex` binary found in a stable Nix profile `bin` directory.
    StableProfileStandalone,
    /// Bundled `resources/codex` resolved from a stable Nix profile desktop launcher (`chatgpt`).
    StableProfileDesktopBundle,
    /// Bundled `resources/codex` resolved from an explicitly supplied desktop launcher or bundle.
    ExplicitDesktopBundle,
    /// Standalone `codex` binary found on `PATH`.
    PathStandalone,
}

/// Validated metadata parsed from a desktop bundle's `linux-package-metadata.json`.
///
/// Expects exact JSON keys `codexAppBrand` (`"chatgpt"`), `codexBuildFlavor` (`"prod"`),
/// and `version` (e.g. `"26.1027.51404"`), which remains distinct from the bundled
/// `codex --version` CLI version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DesktopPackageMetadata {
    /// Package brand identifier from `codexAppBrand` (`"chatgpt"`).
    pub brand: String,
    /// Package build flavor from `codexBuildFlavor` (`"prod"`).
    pub build_flavor: String,
    /// Desktop package version string from `version` (independent of bundled CLI `--version`).
    pub version: String,
}

/// Static discovery result for an installed Codex CLI executable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodexInstallation {
    /// Canonical or validated absolute path to the active `codex` CLI executable.
    pub executable_path: PathBuf,
    /// How the executable was discovered.
    pub source: CodexResolutionSource,
    /// Entry launcher path if resolved through a desktop launcher or profile alias.
    pub launcher_path: Option<PathBuf>,
    /// Validated desktop package metadata when resolved from a Linux/Nix desktop bundle.
    pub desktop_metadata: Option<DesktopPackageMetadata>,
}

/// Structured static error returned when inspecting a candidate Codex desktop launcher or bundle.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CodexDiscoveryError {
    /// Candidate path was empty, relative, contained traversal components, or had control chars.
    #[error("{0}")]
    InvalidCandidatePath(&'static str),

    /// Symlink chain contained a cycle.
    #[error("{0}")]
    SymlinkCycle(&'static str),

    /// Symlink chain exceeded [`MAX_SYMLINK_HOPS`].
    #[error("{0}")]
    SymlinkDepthExceeded(&'static str),

    /// Discovery exceeded [`MAX_WRAPPER_SCRIPT_HOPS`] or [`MAX_INSPECTED_FILES`].
    #[error("{0}")]
    InspectionLimitExceeded(&'static str),

    /// Shell wrapper script did not match the supported single literal `exec` grammar.
    #[error("{0}")]
    UnsupportedWrapperScript(&'static str),

    /// Resolved launcher did not match any known desktop bundle layout.
    #[error("{0}")]
    UnknownBundleLayout(&'static str),

    /// Desktop `linux-package-metadata.json` was missing, oversized, malformed, or mismatched.
    #[error("{0}")]
    InvalidPackageMetadata(&'static str),

    /// Bundled `resources/codex` was missing, not a regular file, or not executable.
    #[error("{0}")]
    InvalidBundledExecutable(&'static str),
}

impl CodexDiscoveryError {
    /// Returns the stable `snake_case` error code for diagnostics and tests.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidCandidatePath(_) => "invalid_candidate_path",
            Self::SymlinkCycle(_) => "symlink_cycle",
            Self::SymlinkDepthExceeded(_) => "symlink_depth_exceeded",
            Self::InspectionLimitExceeded(_) => "inspection_limit_exceeded",
            Self::UnsupportedWrapperScript(_) => "unsupported_wrapper_script",
            Self::UnknownBundleLayout(_) => "unknown_bundle_layout",
            Self::InvalidPackageMetadata(_) => "invalid_package_metadata",
            Self::InvalidBundledExecutable(_) => "invalid_bundled_executable",
        }
    }

    /// Returns the static English error description.
    pub const fn message(&self) -> &'static str {
        match self {
            Self::InvalidCandidatePath(msg)
            | Self::SymlinkCycle(msg)
            | Self::SymlinkDepthExceeded(msg)
            | Self::InspectionLimitExceeded(msg)
            | Self::UnsupportedWrapperScript(msg)
            | Self::UnknownBundleLayout(msg)
            | Self::InvalidPackageMetadata(msg)
            | Self::InvalidBundledExecutable(msg) => msg,
        }
    }
}

/// Injectable environment and candidate configuration for deterministic Codex CLI discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexDiscoveryOptions {
    /// Whether to read ambient environment variables (`PATH`, `USER`, `LOGNAME`, `HOME`,
    /// `XDG_STATE_HOME`, `NIX_PROFILES`) when not explicitly overridden.
    pub use_ambient_env: bool,
    /// Explicit `PATH` override.
    pub path_env: Option<OsString>,
    /// Explicit username (`USER` / `LOGNAME`) for deriving `/etc/profiles/per-user/<user>/bin`.
    pub user: Option<String>,
    /// Explicit `HOME` directory for deriving `~/.nix-profile/bin`.
    pub home_dir: Option<PathBuf>,
    /// Explicit `XDG_STATE_HOME` directory for deriving `$XDG_STATE_HOME/nix/profile/bin`.
    pub xdg_state_home: Option<PathBuf>,
    /// Optional filesystem root prefix for derived profile paths in sandboxed tests (defaults to `/`).
    pub root_prefix: Option<PathBuf>,
    /// Additional explicit stable profile `bin` directories to search.
    pub extra_profile_bin_dirs: Vec<PathBuf>,
    /// Additional explicit desktop launcher or bundle paths to inspect before `PATH` fallback.
    pub candidate_launchers: Vec<PathBuf>,
}

impl Default for CodexDiscoveryOptions {
    fn default() -> Self {
        Self::ambient()
    }
}

impl CodexDiscoveryOptions {
    /// Creates options that read from the current process's ambient environment.
    pub const fn ambient() -> Self {
        Self {
            use_ambient_env: true,
            path_env: None,
            user: None,
            home_dir: None,
            xdg_state_home: None,
            root_prefix: None,
            extra_profile_bin_dirs: Vec::new(),
            candidate_launchers: Vec::new(),
        }
    }

    /// Creates isolated options for deterministic tests that never read ambient host environment.
    pub const fn isolated() -> Self {
        Self {
            use_ambient_env: false,
            path_env: None,
            user: None,
            home_dir: None,
            xdg_state_home: None,
            root_prefix: None,
            extra_profile_bin_dirs: Vec::new(),
            candidate_launchers: Vec::new(),
        }
    }

    /// Sets an explicit `PATH` environment string.
    pub fn with_path_env(mut self, path_env: impl Into<OsString>) -> Self {
        self.path_env = Some(path_env.into());
        self
    }

    /// Sets an optional `PATH` environment string.
    pub fn with_optional_path_env(mut self, path_env: Option<OsString>) -> Self {
        self.path_env = path_env;
        self
    }

    /// Sets an explicit username for deriving per-user Nix profile directories.
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Sets an explicit `HOME` directory for deriving user Nix profile directories.
    pub fn with_home_dir(mut self, home_dir: impl Into<PathBuf>) -> Self {
        self.home_dir = Some(home_dir.into());
        self
    }

    /// Sets an explicit `XDG_STATE_HOME` directory for deriving user Nix profile directories.
    pub fn with_xdg_state_home(mut self, xdg_state_home: impl Into<PathBuf>) -> Self {
        self.xdg_state_home = Some(xdg_state_home.into());
        self
    }

    /// Sets a filesystem root prefix (e.g. a `TempDir` path) for derived system/user profile paths.
    pub fn with_root_prefix(mut self, root_prefix: impl Into<PathBuf>) -> Self {
        self.root_prefix = Some(root_prefix.into());
        self
    }

    /// Appends explicit stable profile `bin` directories to search.
    pub fn with_profile_bin_dirs(mut self, dirs: impl IntoIterator<Item = PathBuf>) -> Self {
        self.extra_profile_bin_dirs.extend(dirs);
        self
    }

    /// Appends explicit candidate desktop launcher or bundle paths to inspect.
    pub fn with_candidate_launchers(
        mut self,
        launchers: impl IntoIterator<Item = PathBuf>,
    ) -> Self {
        self.candidate_launchers.extend(launchers);
        self
    }

    pub(crate) fn effective_path_env(&self) -> Option<OsString> {
        if let Some(ref explicit) = self.path_env {
            return Some(explicit.clone());
        }
        if self.use_ambient_env {
            std::env::var_os("PATH")
        } else {
            None
        }
    }
}

struct InspectionBudget {
    symlink_hops: usize,
    wrapper_hops: usize,
    inspected_files: usize,
}

impl InspectionBudget {
    const fn new() -> Self {
        Self {
            symlink_hops: 0,
            wrapper_hops: 0,
            inspected_files: 0,
        }
    }

    fn record_symlink_hop(&mut self) -> Result<(), CodexDiscoveryError> {
        self.symlink_hops = self.symlink_hops.saturating_add(1);
        if self.symlink_hops > MAX_SYMLINK_HOPS {
            return Err(CodexDiscoveryError::SymlinkDepthExceeded(
                "Symlink resolution exceeded maximum hop depth (16).",
            ));
        }
        Ok(())
    }

    fn record_wrapper_hop(&mut self) -> Result<(), CodexDiscoveryError> {
        self.wrapper_hops = self.wrapper_hops.saturating_add(1);
        if self.wrapper_hops > MAX_WRAPPER_SCRIPT_HOPS {
            return Err(CodexDiscoveryError::InspectionLimitExceeded(
                "Wrapper script chain exceeded maximum hop depth (4).",
            ));
        }
        Ok(())
    }

    fn record_file_inspection(&mut self) -> Result<(), CodexDiscoveryError> {
        self.inspected_files = self.inspected_files.saturating_add(1);
        if self.inspected_files > MAX_INSPECTED_FILES {
            return Err(CodexDiscoveryError::InspectionLimitExceeded(
                "Desktop bundle inspection exceeded maximum file count (32).",
            ));
        }
        Ok(())
    }
}

pub(crate) fn is_safe_lexical_path(path: &Path) -> bool {
    let raw = path.as_os_str().to_string_lossy();
    if raw.is_empty() || raw.contains('\0') || raw.chars().any(char::is_control) {
        return false;
    }
    if !path.is_absolute() {
        return false;
    }
    !path
        .components()
        .any(|c| matches!(c, Component::CurDir | Component::ParentDir))
}

fn is_safe_username_token(user: &str) -> bool {
    if user.is_empty() || user.len() > 64 || user.trim() != user {
        return false;
    }
    if user.starts_with('-') || user.starts_with('.') || user.contains("..") {
        return false;
    }
    user.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

fn is_valid_package_version(version: &str) -> bool {
    if version.is_empty() || version.len() > MAX_VERSION_BYTES || version.trim() != version {
        return false;
    }
    let Some(first) = version.chars().next() else {
        return false;
    };
    if !first.is_ascii_digit() {
        return false;
    }
    version
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '+' | '_'))
}

fn push_unique_safe_dir(out: &mut Vec<PathBuf>, dir: PathBuf) {
    if !is_safe_lexical_path(&dir) || is_direct_nix_store_path(&dir) {
        return;
    }
    if !out.iter().any(|existing| existing == &dir) {
        out.push(dir);
    }
}

/// Derives the ordered, deduplicated list of candidate stable Nix profile `bin` directories
/// from `options` (combining stable profile entries in `PATH`, explicit profile dirs, and
/// bounded environment-derived user/system profile locations).
///
/// Never uses hardcoded usernames and rejects empty, relative, or traversing inputs.
pub fn derive_stable_profile_bin_dirs(options: &CodexDiscoveryOptions) -> Vec<PathBuf> {
    let mut dirs = Vec::new();

    if let Some(path_os) = options.effective_path_env() {
        for dir in std::env::split_paths(&path_os) {
            if is_safe_lexical_path(&dir) && is_stable_nix_profile_path(&dir) {
                push_unique_safe_dir(&mut dirs, dir);
            }
        }
    }

    for extra in &options.extra_profile_bin_dirs {
        push_unique_safe_dir(&mut dirs, extra.clone());
    }

    let root = match options.root_prefix.as_deref() {
        Some(prefix) if is_safe_lexical_path(prefix) => prefix.to_path_buf(),
        Some(_) => return dirs,
        None => PathBuf::from("/"),
    };

    let xdg_state: Option<PathBuf> = options.xdg_state_home.clone().or_else(|| {
        if options.use_ambient_env {
            std::env::var_os("XDG_STATE_HOME").map(PathBuf::from)
        } else {
            None
        }
    });
    if let Some(xdg) = xdg_state {
        if is_safe_lexical_path(&xdg) {
            push_unique_safe_dir(&mut dirs, xdg.join("nix/profile/bin"));
            push_unique_safe_dir(
                &mut dirs,
                xdg.join("home-manager/gcroots/current-home/home-path/bin"),
            );
        }
    }

    let home: Option<PathBuf> = options.home_dir.clone().or_else(|| {
        if options.use_ambient_env {
            std::env::var_os("HOME").map(PathBuf::from)
        } else {
            None
        }
    });
    if let Some(home_path) = home {
        if is_safe_lexical_path(&home_path) {
            push_unique_safe_dir(&mut dirs, home_path.join(".nix-profile/bin"));
            push_unique_safe_dir(&mut dirs, home_path.join(".local/state/nix/profile/bin"));
            push_unique_safe_dir(
                &mut dirs,
                home_path.join(".local/state/home-manager/gcroots/current-home/home-path/bin"),
            );
        }
    }

    let user: Option<String> = options.user.clone().or_else(|| {
        if options.use_ambient_env {
            std::env::var("USER")
                .ok()
                .filter(|u| !u.is_empty())
                .or_else(|| std::env::var("LOGNAME").ok().filter(|u| !u.is_empty()))
        } else {
            None
        }
    });
    if let Some(username) = user {
        if is_safe_username_token(&username) {
            push_unique_safe_dir(
                &mut dirs,
                root.join("etc/profiles/per-user")
                    .join(&username)
                    .join("bin"),
            );
            push_unique_safe_dir(
                &mut dirs,
                root.join("nix/var/nix/profiles/per-user")
                    .join(&username)
                    .join("profile/bin"),
            );
            push_unique_safe_dir(
                &mut dirs,
                root.join("nix/var/nix/profiles/per-user")
                    .join(&username)
                    .join("home-manager/home-path/bin"),
            );
        }
    }

    if options.use_ambient_env {
        if let Some(nix_profiles) = std::env::var_os("NIX_PROFILES") {
            let profiles_lossy = nix_profiles.to_string_lossy();
            let tokens: Vec<&str> = profiles_lossy.split_whitespace().collect();
            for token in tokens.into_iter().rev() {
                let candidate = PathBuf::from(token).join("bin");
                push_unique_safe_dir(&mut dirs, candidate);
            }
        }
    }

    if options.use_ambient_env || options.root_prefix.is_some() {
        push_unique_safe_dir(&mut dirs, root.join("nix/var/nix/profiles/default/bin"));
        push_unique_safe_dir(&mut dirs, root.join("run/current-system/sw/bin"));
    }

    dirs
}

fn resolve_path_symlinks_bounded(
    initial_path: &Path,
    budget: &mut InspectionBudget,
) -> Result<PathBuf, CodexDiscoveryError> {
    if !is_safe_lexical_path(initial_path) {
        return Err(CodexDiscoveryError::InvalidCandidatePath(
            "Candidate path must be an absolute path without traversal components.",
        ));
    }

    let mut pending: Vec<OsString> = initial_path
        .components()
        .filter_map(|c| match c {
            Component::Normal(s) => Some(s.to_os_string()),
            _ => None,
        })
        .collect();
    pending.reverse();

    let mut resolved = PathBuf::from("/");
    let mut visited_links: BTreeSet<PathBuf> = BTreeSet::new();

    while let Some(comp) = pending.pop() {
        if comp.is_empty() || comp == OsStr::new(".") {
            continue;
        }
        if comp == OsStr::new("..") {
            if !resolved.pop() {
                return Err(CodexDiscoveryError::InvalidCandidatePath(
                    "Relative symlink target escaped root directory.",
                ));
            }
            continue;
        }

        let next_path = resolved.join(&comp);
        let meta = fs::symlink_metadata(&next_path).map_err(|_| {
            CodexDiscoveryError::InvalidBundledExecutable(
                "Candidate path does not exist or could not be accessed.",
            )
        })?;

        if meta.file_type().is_symlink() {
            if !visited_links.insert(next_path.clone()) {
                return Err(CodexDiscoveryError::SymlinkCycle(
                    "Detected a symlink cycle while resolving Codex candidate path.",
                ));
            }
            budget.record_symlink_hop()?;
            let link_target = fs::read_link(&next_path).map_err(|_| {
                CodexDiscoveryError::InvalidCandidatePath(
                    "Failed to read symlink target while resolving Codex candidate path.",
                )
            })?;
            let raw_target = link_target.as_os_str().to_string_lossy();
            if raw_target.is_empty()
                || raw_target.contains('\0')
                || raw_target.chars().any(char::is_control)
            {
                return Err(CodexDiscoveryError::InvalidCandidatePath(
                    "Symlink target contained empty or control characters.",
                ));
            }

            let mut target_comps: Vec<OsString> = Vec::new();
            for tc in link_target.components() {
                match tc {
                    Component::RootDir => {
                        resolved = PathBuf::from("/");
                    }
                    Component::CurDir => {}
                    Component::ParentDir => target_comps.push(OsString::from("..")),
                    Component::Normal(s) => target_comps.push(s.to_os_string()),
                    Component::Prefix(_) => {
                        return Err(CodexDiscoveryError::InvalidCandidatePath(
                            "Unsupported path prefix in symlink target.",
                        ));
                    }
                }
            }
            for tc in target_comps.into_iter().rev() {
                pending.push(tc);
            }
        } else {
            resolved = next_path;
        }
    }

    Ok(resolved)
}

fn ensure_regular_executable_file(
    path: &Path,
    budget: &mut InspectionBudget,
) -> Result<(), CodexDiscoveryError> {
    budget.record_file_inspection()?;
    let meta = fs::symlink_metadata(path).map_err(|_| {
        CodexDiscoveryError::InvalidBundledExecutable(
            "Resolved executable target could not be accessed.",
        )
    })?;
    if !meta.file_type().is_file() {
        return Err(CodexDiscoveryError::InvalidBundledExecutable(
            "Resolved executable target must be a regular file.",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if (meta.permissions().mode() & 0o111) == 0 {
            return Err(CodexDiscoveryError::InvalidBundledExecutable(
                "Resolved executable target does not have execute permissions.",
            ));
        }
    }
    Ok(())
}

fn read_bounded_file_bytes(
    path: &Path,
    max_bytes: usize,
    budget: &mut InspectionBudget,
) -> Result<Vec<u8>, CodexDiscoveryError> {
    budget.record_file_inspection()?;
    let meta = fs::symlink_metadata(path).map_err(|_| {
        CodexDiscoveryError::InvalidCandidatePath("File metadata could not be read.")
    })?;
    if !meta.file_type().is_file() {
        return Err(CodexDiscoveryError::InvalidCandidatePath(
            "Inspected target must be a regular file.",
        ));
    }
    if meta.len() > (max_bytes as u64) {
        return Err(CodexDiscoveryError::InspectionLimitExceeded(
            "Inspected file exceeded maximum byte bound.",
        ));
    }
    let file = File::open(path).map_err(|_| {
        CodexDiscoveryError::InvalidCandidatePath("Failed to open file for static inspection.")
    })?;
    let mut buf = Vec::with_capacity((meta.len() as usize).min(max_bytes));
    file.take((max_bytes as u64).saturating_add(1))
        .read_to_end(&mut buf)
        .map_err(|_| {
            CodexDiscoveryError::InvalidCandidatePath("Failed to read file for static inspection.")
        })?;
    if buf.len() > max_bytes {
        return Err(CodexDiscoveryError::InspectionLimitExceeded(
            "Inspected file exceeded maximum byte bound while reading.",
        ));
    }
    Ok(buf)
}

fn is_forbidden_exec_target_char(ch: char) -> bool {
    ch.is_control()
        || matches!(
            ch,
            '$' | '`'
                | '\\'
                | '"'
                | '\''
                | ';'
                | '&'
                | '|'
                | '<'
                | '>'
                | '('
                | ')'
                | '{'
                | '}'
                | '*'
                | '?'
                | '['
                | ']'
                | '~'
                | '!'
        )
}

fn parse_exec_target_from_remainder(rem: &str) -> Result<PathBuf, CodexDiscoveryError> {
    let mut rest = rem.trim();
    if rest.is_empty() {
        return Err(CodexDiscoveryError::UnsupportedWrapperScript(
            "Wrapper exec statement was missing a target executable.",
        ));
    }

    if rest.contains('`')
        || rest.contains("$(")
        || rest.contains(';')
        || rest.contains('|')
        || rest.contains("&&")
        || rest.contains("||")
        || rest.contains('<')
        || rest.contains('>')
    {
        return Err(CodexDiscoveryError::UnsupportedWrapperScript(
            "Wrapper exec statement contained unsupported shell operators or command substitution.",
        ));
    }

    // Support optional `exec -a "$0" "/nix/store/..." "$@"` from Nix `makeWrapper`.
    if let Some(after_flag) = rest.strip_prefix("-a") {
        if after_flag.is_empty() || !after_flag.starts_with(char::is_whitespace) {
            return Err(CodexDiscoveryError::UnsupportedWrapperScript(
                "Wrapper exec statement contained an unsupported flag.",
            ));
        }
        let after_flag = after_flag.trim_start();
        let after_arg = if let Some(quoted) = after_flag.strip_prefix('"') {
            let close = quoted
                .find('"')
                .ok_or(CodexDiscoveryError::UnsupportedWrapperScript(
                    "Wrapper exec -a argument had an unclosed double quote.",
                ))?;
            &quoted[close + 1..]
        } else if let Some(quoted) = after_flag.strip_prefix('\'') {
            let close = quoted
                .find('\'')
                .ok_or(CodexDiscoveryError::UnsupportedWrapperScript(
                    "Wrapper exec -a argument had an unclosed single quote.",
                ))?;
            &quoted[close + 1..]
        } else {
            let end = after_flag.find(char::is_whitespace).ok_or(
                CodexDiscoveryError::UnsupportedWrapperScript(
                    "Wrapper exec -a statement was missing a target path.",
                ),
            )?;
            &after_flag[end..]
        };
        rest = after_arg.trim_start();
    }

    if rest.is_empty() || rest.starts_with('-') {
        return Err(CodexDiscoveryError::UnsupportedWrapperScript(
            "Wrapper exec statement contained an unsupported option or missing target.",
        ));
    }

    let (target_str, after_target) = if let Some(inner_start) = rest.strip_prefix('"') {
        let close_idx =
            inner_start
                .find('"')
                .ok_or(CodexDiscoveryError::UnsupportedWrapperScript(
                    "Wrapper exec target had an unclosed double quote.",
                ))?;
        (&inner_start[..close_idx], &inner_start[close_idx + 1..])
    } else if let Some(inner_start) = rest.strip_prefix('\'') {
        let close_idx =
            inner_start
                .find('\'')
                .ok_or(CodexDiscoveryError::UnsupportedWrapperScript(
                    "Wrapper exec target had an unclosed single quote.",
                ))?;
        (&inner_start[..close_idx], &inner_start[close_idx + 1..])
    } else {
        match rest.find(char::is_whitespace) {
            Some(ws_idx) => (&rest[..ws_idx], &rest[ws_idx..]),
            None => (rest, ""),
        }
    };

    if !after_target.is_empty() && !after_target.starts_with(char::is_whitespace) {
        return Err(CodexDiscoveryError::UnsupportedWrapperScript(
            "Wrapper exec target quote must be followed by whitespace or end of line.",
        ));
    }

    if target_str.is_empty() || target_str.chars().any(is_forbidden_exec_target_char) {
        return Err(CodexDiscoveryError::UnsupportedWrapperScript(
            "Wrapper exec target contained dynamic expansion or forbidden characters.",
        ));
    }

    let target_path = PathBuf::from(target_str);
    if !is_safe_lexical_path(&target_path) {
        return Err(CodexDiscoveryError::UnsupportedWrapperScript(
            "Wrapper exec target must be a clean absolute path without '.' or '..'.",
        ));
    }

    Ok(target_path)
}

fn is_supported_wrapper_preamble_line(line: &str) -> bool {
    if line.contains(';')
        || line.contains('|')
        || line.contains('&')
        || line.contains('`')
        || line.contains("$(")
        || line.contains('<')
        || line.contains('>')
    {
        return false;
    }
    // Nix's desktop wrapper can source a literal runtime environment file.
    // Record only the final literal exec target; never read or source that file.
    if let Some(path) = line
        .strip_prefix(". ")
        .or_else(|| line.strip_prefix("source "))
    {
        let path = path.trim();
        return !path
            .chars()
            .any(|ch| ch.is_whitespace() || is_forbidden_exec_target_char(ch))
            && is_safe_lexical_path(Path::new(path));
    }
    let rest = line
        .strip_prefix("export ")
        .or_else(|| line.strip_prefix("export\t"))
        .map(str::trim_start)
        .unwrap_or(line);
    let Some((ident, _value)) = rest.split_once('=') else {
        return false;
    };
    let mut chars = ident.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Extracts the single literal `exec` target path from a shell wrapper script without
/// executing the script or evaluating shell expressions.
pub fn extract_supported_wrapper_exec_target(
    script_text: &str,
) -> Result<PathBuf, CodexDiscoveryError> {
    if !script_text.starts_with("#!") {
        return Err(CodexDiscoveryError::UnsupportedWrapperScript(
            "Wrapper script must begin with a '#!' shebang line.",
        ));
    }

    let mut found_exec: Option<PathBuf> = None;

    for (idx, raw_line) in script_text.lines().enumerate() {
        if idx == 0 {
            continue;
        }
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if found_exec.is_some() {
            return Err(CodexDiscoveryError::UnsupportedWrapperScript(
                "Wrapper script contained statements after the exec line.",
            ));
        }

        if line == "exec" || line.starts_with("exec ") || line.starts_with("exec\t") {
            let rem = &line["exec".len()..];
            let target = parse_exec_target_from_remainder(rem)?;
            found_exec = Some(target);
            continue;
        }

        if !is_supported_wrapper_preamble_line(line) {
            return Err(CodexDiscoveryError::UnsupportedWrapperScript(
                "Wrapper script contained unsupported statements or commands before exec.",
            ));
        }
    }

    found_exec.ok_or(CodexDiscoveryError::UnsupportedWrapperScript(
        "Wrapper script did not contain a literal exec statement.",
    ))
}

#[derive(Debug)]
enum BundleLayout {
    LinuxResources {
        launcher_dir: PathBuf,
        resources_dir: PathBuf,
    },
    MacOsAppResources {
        resources_dir: PathBuf,
    },
}

fn detect_bundle_layout(resolved_launcher: &Path) -> Option<BundleLayout> {
    // Check if `resolved_launcher` is itself a `.app` bundle directory.
    if resolved_launcher
        .extension()
        .is_some_and(|ext| ext == "app")
    {
        let macos_res = resolved_launcher.join("Contents/Resources");
        if macos_res.is_dir() {
            return Some(BundleLayout::MacOsAppResources {
                resources_dir: macos_res,
            });
        }
    }

    let launcher_dir = resolved_launcher.parent()?;

    // 1. Adjacent `resources/` directory (e.g. `<prefix>/lib/chatgpt/codex-launcher` -> `<prefix>/lib/chatgpt/resources`).
    let adjacent_resources = launcher_dir.join("resources");
    if adjacent_resources.is_dir() {
        return Some(BundleLayout::LinuxResources {
            launcher_dir: launcher_dir.to_path_buf(),
            resources_dir: adjacent_resources,
        });
    }

    // 2. Containing Nix package prefix `<prefix>/lib/chatgpt/resources` when launcher is in `<prefix>/bin/...`
    //    and `<prefix>/lib/chatgpt` exists inside the same package prefix.
    if launcher_dir.file_name() == Some(OsStr::new("bin")) {
        if let Some(prefix) = launcher_dir.parent() {
            let lib_chatgpt = prefix.join("lib/chatgpt");
            let prefix_resources = lib_chatgpt.join("resources");
            if prefix_resources.is_dir() {
                return Some(BundleLayout::LinuxResources {
                    launcher_dir: lib_chatgpt,
                    resources_dir: prefix_resources,
                });
            }
        }
    }

    // 3. macOS `<App>.app/Contents/MacOS/<launcher>` -> `<App>.app/Contents/Resources`.
    if launcher_dir.file_name() == Some(OsStr::new("MacOS")) {
        if let Some(contents_dir) = launcher_dir.parent() {
            if contents_dir.file_name() == Some(OsStr::new("Contents")) {
                if let Some(app_dir) = contents_dir.parent() {
                    if app_dir.extension().is_some_and(|ext| ext == "app") {
                        let macos_res = contents_dir.join("Resources");
                        if macos_res.is_dir() {
                            return Some(BundleLayout::MacOsAppResources {
                                resources_dir: macos_res,
                            });
                        }
                    }
                }
            }
        }
    }

    None
}

fn extract_nix_store_chatgpt_version(path: &Path) -> Option<String> {
    let parts: Vec<&str> = path
        .components()
        .filter_map(|c| match c {
            Component::Normal(s) => s.to_str(),
            _ => None,
        })
        .collect();

    for window in parts.windows(3) {
        if window[0] == "nix" && window[1] == "store" {
            let store_name = window[2];
            for marker in ["-chatgpt-unwrapped-", "-chatgpt-"] {
                if let Some((_, ver)) = store_name.rsplit_once(marker) {
                    if ver.contains('.') && is_valid_package_version(ver) {
                        return Some(ver.to_string());
                    }
                }
            }
        }
    }
    None
}

fn parse_and_validate_linux_metadata(
    metadata_path: &Path,
    expected_store_versions: &[String],
    budget: &mut InspectionBudget,
) -> Result<DesktopPackageMetadata, CodexDiscoveryError> {
    let resolved_meta = resolve_path_symlinks_bounded(metadata_path, budget).map_err(|_| {
        CodexDiscoveryError::InvalidPackageMetadata(
            "Desktop linux-package-metadata.json path could not be resolved.",
        )
    })?;
    let raw_bytes = read_bounded_file_bytes(&resolved_meta, MAX_PACKAGE_METADATA_BYTES, budget)
        .map_err(|_| {
            CodexDiscoveryError::InvalidPackageMetadata(
                "Desktop linux-package-metadata.json could not be read.",
            )
        })?;
    let parsed: Value = serde_json::from_slice(&raw_bytes).map_err(|_| {
        CodexDiscoveryError::InvalidPackageMetadata(
            "Desktop linux-package-metadata.json was not valid JSON.",
        )
    })?;
    let obj = parsed
        .as_object()
        .ok_or(CodexDiscoveryError::InvalidPackageMetadata(
            "Desktop linux-package-metadata.json root must be a JSON object.",
        ))?;

    let brand = obj.get("codexAppBrand").and_then(Value::as_str).ok_or(
        CodexDiscoveryError::InvalidPackageMetadata(
            "Desktop linux-package-metadata.json was missing string field 'codexAppBrand'.",
        ),
    )?;
    if brand != EXPECTED_DESKTOP_BRAND {
        return Err(CodexDiscoveryError::InvalidPackageMetadata(
            "Desktop linux-package-metadata.json 'codexAppBrand' must be 'chatgpt'.",
        ));
    }

    let build_flavor = obj.get("codexBuildFlavor").and_then(Value::as_str).ok_or(
        CodexDiscoveryError::InvalidPackageMetadata(
            "Desktop linux-package-metadata.json was missing string field 'codexBuildFlavor'.",
        ),
    )?;
    if build_flavor != EXPECTED_DESKTOP_BUILD_FLAVOR {
        return Err(CodexDiscoveryError::InvalidPackageMetadata(
            "Desktop linux-package-metadata.json 'codexBuildFlavor' must be 'prod'.",
        ));
    }

    let version = obj.get("version").and_then(Value::as_str).ok_or(
        CodexDiscoveryError::InvalidPackageMetadata(
            "Desktop linux-package-metadata.json was missing string field 'version'.",
        ),
    )?;
    if !is_valid_package_version(version) {
        return Err(CodexDiscoveryError::InvalidPackageMetadata(
            "Desktop linux-package-metadata.json 'version' was empty or malformed.",
        ));
    }

    for store_ver in expected_store_versions {
        if store_ver != version {
            return Err(CodexDiscoveryError::InvalidPackageMetadata(
                "Desktop linux-package-metadata.json 'version' did not match Nix package version.",
            ));
        }
    }

    Ok(DesktopPackageMetadata {
        brand: brand.to_string(),
        build_flavor: build_flavor.to_string(),
        version: version.to_string(),
    })
}

fn validate_bundled_codex_binary(
    resources_dir: &Path,
    budget: &mut InspectionBudget,
) -> Result<PathBuf, CodexDiscoveryError> {
    // Always select active `resources/codex`, never `codex-threadlane-original`.
    let codex_candidate = resources_dir.join(BUNDLED_CODEX_BINARY_NAME);
    let resolved_codex = resolve_path_symlinks_bounded(&codex_candidate, budget)?;

    if resolved_codex.file_name() == Some(OsStr::new(IGNORED_ORIGINAL_BINARY_NAME)) {
        return Err(CodexDiscoveryError::InvalidBundledExecutable(
            "Bundled resources/codex must not resolve to codex-threadlane-original.",
        ));
    }

    ensure_regular_executable_file(&resolved_codex, budget)?;
    let canonical = check_resolved_file_executable(&resolved_codex).map_err(|_| {
        CodexDiscoveryError::InvalidBundledExecutable(
            "Bundled resources/codex failed executable validation.",
        )
    })?;

    if canonical.file_name() == Some(OsStr::new(IGNORED_ORIGINAL_BINARY_NAME)) {
        return Err(CodexDiscoveryError::InvalidBundledExecutable(
            "Bundled resources/codex must not resolve to codex-threadlane-original.",
        ));
    }

    Ok(canonical)
}

fn inspect_resolved_desktop_entry(
    entry_launcher: &Path,
    initial_resolved: &Path,
    source: CodexResolutionSource,
    budget: &mut InspectionBudget,
) -> Result<CodexInstallation, CodexDiscoveryError> {
    let mut current = initial_resolved.to_path_buf();
    let mut seen_targets = BTreeSet::new();
    seen_targets.insert(current.clone());

    let mut store_versions = Vec::new();
    if let Some(ver) = extract_nix_store_chatgpt_version(entry_launcher) {
        store_versions.push(ver);
    }
    if let Some(ver) = extract_nix_store_chatgpt_version(&current) {
        if !store_versions.contains(&ver) {
            store_versions.push(ver);
        }
    }

    loop {
        if let Some(layout) = detect_bundle_layout(&current) {
            return match layout {
                BundleLayout::LinuxResources {
                    launcher_dir,
                    resources_dir,
                } => {
                    ensure_regular_executable_file(&current, budget)?;
                    let meta_in_res = resources_dir.join("linux-package-metadata.json");
                    let meta_in_launcher = launcher_dir.join("linux-package-metadata.json");
                    let meta_path = if fs::symlink_metadata(&meta_in_res).is_ok() {
                        meta_in_res
                    } else if fs::symlink_metadata(&meta_in_launcher).is_ok() {
                        meta_in_launcher
                    } else {
                        return Err(CodexDiscoveryError::InvalidPackageMetadata(
                            "Desktop bundle is missing linux-package-metadata.json.",
                        ));
                    };

                    let desktop_metadata =
                        parse_and_validate_linux_metadata(&meta_path, &store_versions, budget)?;
                    let executable_path = validate_bundled_codex_binary(&resources_dir, budget)?;
                    if let Some(codex_store_ver) =
                        extract_nix_store_chatgpt_version(&executable_path)
                    {
                        if codex_store_ver != desktop_metadata.version {
                            return Err(CodexDiscoveryError::InvalidPackageMetadata(
                                "Bundled resources/codex Nix package version did not match desktop metadata.",
                            ));
                        }
                    }

                    Ok(CodexInstallation {
                        executable_path,
                        source,
                        launcher_path: Some(entry_launcher.to_path_buf()),
                        desktop_metadata: Some(desktop_metadata),
                    })
                }
                BundleLayout::MacOsAppResources { resources_dir } => {
                    if !current.extension().is_some_and(|ext| ext == "app") {
                        ensure_regular_executable_file(&current, budget)?;
                    }
                    let executable_path = validate_bundled_codex_binary(&resources_dir, budget)?;
                    Ok(CodexInstallation {
                        executable_path,
                        source,
                        launcher_path: Some(entry_launcher.to_path_buf()),
                        desktop_metadata: None,
                    })
                }
            };
        }

        ensure_regular_executable_file(&current, budget)?;
        let raw_bytes = read_bounded_file_bytes(&current, MAX_WRAPPER_SCRIPT_BYTES, budget)?;
        if !raw_bytes.starts_with(b"#!") {
            return Err(CodexDiscoveryError::UnknownBundleLayout(
                "Resolved desktop launcher had no known bundle layout and was not a wrapper script.",
            ));
        }

        let script_text = std::str::from_utf8(&raw_bytes).map_err(|_| {
            CodexDiscoveryError::UnsupportedWrapperScript(
                "Wrapper script contained invalid UTF-8 bytes.",
            )
        })?;
        let exec_target = extract_supported_wrapper_exec_target(script_text)?;
        budget.record_wrapper_hop()?;

        if let Some(ver) = extract_nix_store_chatgpt_version(&exec_target) {
            if !store_versions.contains(&ver) {
                store_versions.push(ver);
            }
        }

        let next_resolved = resolve_path_symlinks_bounded(&exec_target, budget)?;
        if let Some(ver) = extract_nix_store_chatgpt_version(&next_resolved) {
            if !store_versions.contains(&ver) {
                store_versions.push(ver);
            }
        }

        if !seen_targets.insert(next_resolved.clone()) {
            return Err(CodexDiscoveryError::SymlinkCycle(
                "Detected a wrapper/symlink loop while resolving desktop launcher.",
            ));
        }
        current = next_resolved;
    }
}

/// Statically inspects an explicit desktop launcher or bundle path (`chatgpt` wrapper,
/// `codex-launcher`, or `.app` bundle) and resolves its active `resources/codex` executable
/// without executing any wrapper script.
pub fn inspect_codex_desktop_launcher(
    candidate_launcher: &Path,
) -> Result<CodexInstallation, CodexDiscoveryError> {
    let mut budget = InspectionBudget::new();
    let resolved = resolve_path_symlinks_bounded(candidate_launcher, &mut budget)?;
    inspect_resolved_desktop_entry(
        candidate_launcher,
        &resolved,
        CodexResolutionSource::ExplicitDesktopBundle,
        &mut budget,
    )
}

/// Resolves the system `codex` CLI installation afresh according to Threadlane's discovery policy:
///
/// 1. Ordinary non-Nix `PATH` entries that appear before any direct `/nix/store` entry.
/// 2. Standalone `codex` in stable Nix user/system profile `bin` directories.
/// 3. Bundled `resources/codex` from a known stable Nix profile desktop app entry (`chatgpt`,
///    `codex-desktop`, `codex-launcher`) or explicitly supplied launcher candidate.
/// 4. If a stable profile desktop candidate exists but fails bundle/metadata/wrapper validation,
///    returns `None` rather than guessing or falling back to stale inherited `/nix/store` `PATH` entries.
/// 5. Otherwise, falls back to remaining `PATH` entries in ordinary precedence order.
pub fn resolve_codex_system_installation(
    options: &CodexDiscoveryOptions,
) -> Option<CodexInstallation> {
    let path_os = options.effective_path_env();
    let mut valid_path_dirs = Vec::new();
    if let Some(ref raw_path) = path_os {
        for dir in std::env::split_paths(raw_path) {
            if is_safe_lexical_path(&dir) && !valid_path_dirs.iter().any(|d| d == &dir) {
                valid_path_dirs.push(dir);
            }
        }
    }

    let first_store_idx = valid_path_dirs
        .iter()
        .position(|d| is_direct_nix_store_path(d))
        .unwrap_or(valid_path_dirs.len());

    // 1. Ordinary non-Nix, non-profile PATH directories placed ahead of any `/nix/store` entry.
    for dir in &valid_path_dirs[..first_store_idx] {
        if !is_stable_nix_profile_path(dir) {
            let candidate = dir.join("codex");
            if let Ok(canonical) = check_resolved_file_executable(&candidate) {
                return Some(CodexInstallation {
                    executable_path: canonical,
                    source: CodexResolutionSource::PathStandalone,
                    launcher_path: None,
                    desktop_metadata: None,
                });
            }
        }
    }

    // 2. Standalone `codex` in stable Nix user/system profile `bin` directories.
    let stable_profile_bins = derive_stable_profile_bin_dirs(options);
    for profile_bin in &stable_profile_bins {
        let candidate = profile_bin.join("codex");
        if let Ok(canonical) = check_resolved_file_executable(&candidate) {
            return Some(CodexInstallation {
                executable_path: canonical,
                source: CodexResolutionSource::StableProfileStandalone,
                launcher_path: Some(candidate),
                desktop_metadata: None,
            });
        }
    }

    // 3. Explicit candidate launchers and stable Nix profile desktop app entries (`chatgpt`, etc.).
    let mut seen_resolved_launchers: BTreeSet<PathBuf> = BTreeSet::new();
    let mut saw_desktop_candidate = false;

    for explicit_candidate in &options.candidate_launchers {
        saw_desktop_candidate = true;
        if !is_safe_lexical_path(explicit_candidate) {
            continue;
        }
        let mut budget = InspectionBudget::new();
        let Ok(resolved_entry) = resolve_path_symlinks_bounded(explicit_candidate, &mut budget)
        else {
            continue;
        };
        if !seen_resolved_launchers.insert(resolved_entry.clone()) {
            continue;
        }
        if let Ok(installation) = inspect_resolved_desktop_entry(
            explicit_candidate,
            &resolved_entry,
            CodexResolutionSource::ExplicitDesktopBundle,
            &mut budget,
        ) {
            return Some(installation);
        }
    }

    for profile_bin in &stable_profile_bins {
        for launcher_name in DESKTOP_LAUNCHER_NAMES {
            let candidate = profile_bin.join(launcher_name);
            match fs::symlink_metadata(&candidate) {
                Ok(_) => {
                    saw_desktop_candidate = true;
                    let mut budget = InspectionBudget::new();
                    let Ok(resolved_entry) = resolve_path_symlinks_bounded(&candidate, &mut budget)
                    else {
                        continue;
                    };
                    if !seen_resolved_launchers.insert(resolved_entry.clone()) {
                        continue;
                    };
                    if let Ok(installation) = inspect_resolved_desktop_entry(
                        &candidate,
                        &resolved_entry,
                        CodexResolutionSource::StableProfileDesktopBundle,
                        &mut budget,
                    ) {
                        return Some(installation);
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(_) => {
                    saw_desktop_candidate = true;
                }
            }
        }
    }

    // 4. If a stable profile or explicit desktop candidate was present and failed static
    //    verification, never guess or fall back to stale direct `/nix/store` PATH entries.
    if saw_desktop_candidate {
        return None;
    }

    // 5. Fallback to remaining PATH entries (non-Nix directories first, then direct `/nix/store` entries).
    for dir in ordered_system_search_dirs(path_os.as_deref()) {
        let candidate = dir.join("codex");
        if let Ok(canonical) = check_resolved_file_executable(&candidate) {
            return Some(CodexInstallation {
                executable_path: canonical,
                source: CodexResolutionSource::PathStandalone,
                launcher_path: None,
                desktop_metadata: None,
            });
        }
    }

    None
}

/// Resolves the canonical filesystem path of the system `codex` CLI executable using `options`.
pub fn resolve_codex_system_executable(options: &CodexDiscoveryOptions) -> Option<PathBuf> {
    resolve_codex_system_installation(options).map(|inst| inst.executable_path)
}
