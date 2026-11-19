//! Read-only, trusted-local physical delivery inspection for a Worker task checkout.
//!
//! [`inspect_delivery`] performs one bounded, offline inspection of an actual task checkout.
//! It requires the actual `HEAD` to exactly equal the expected 40-hex base commit, then reports
//! the actual changed-path set relative to that base, bounded per-file metadata, and fixed
//! machine-readable refusal codes. Optional explicit text is limited to named, small, allowed
//! UTF-8 files and is always marked untrusted.
//!
//! File opens are descriptor-relative with no-follow and nonblocking flags; parent components
//! are opened as directories rather than followed through paths. Configured content filters and
//! suppressed index flags are refused before any content-converting diff. Reads are bounded by
//! remaining time and byte budgets, and every requested file is re-checked for identity, size,
//! mtime, and ctime after all reads and the final Git snapshot. Explicit text is omitted on any
//! refusal or when that evidence is unstable. This is a bounded stable-read check, not a
//! globally atomic filesystem snapshot.
//!
//! The inspector never contacts the supervisor or a provider, never writes Git state,
//! authority, approval, registry, backlog, or session state, and never executes inspected
//! file contents. A successful report is physical evidence only: it grants no execution,
//! review, acceptance, or `Done` authority. Semantic acceptance and a fresh guarded-complete
//! recheck remain separate Coworker responsibilities.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_workers::{
    is_control_config_path, run_sanitized_git_allow_nonzero,
    validate_absolute_no_symlink_components, validate_exact_40_hex_sha,
    validate_repo_relative_path, SupervisedGitOutput, WorkerError,
};

/// Stable schema version for the machine-readable inspection report.
pub const DELIVERY_INSPECTION_SCHEMA_VERSION: u32 = 1;

/// Canonical CLI/tool name embedded in inspection reports.
pub const DELIVERY_INSPECTION_TOOL: &str = "threadlane inspect-delivery";

/// Maximum number of repeated allowed or text file paths accepted in one request.
pub const MAX_DELIVERY_INSPECTION_PATHS: usize = 256;

/// Maximum number of actual changed paths accepted before the inspector refuses.
pub const MAX_DELIVERY_INSPECTION_CHANGED_PATHS: usize = 1_024;

/// Maximum size of one inspected regular file (16 MiB).
pub const MAX_DELIVERY_INSPECTION_FILE_BYTES: u64 = 16 * 1024 * 1024;

/// Maximum total bytes read across all inspected regular files (32 MiB).
pub const MAX_DELIVERY_INSPECTION_TOTAL_READ_BYTES: u64 = 32 * 1024 * 1024;

/// Maximum size of one explicitly requested text file (64 KiB).
pub const MAX_DELIVERY_INSPECTION_TEXT_FILE_BYTES: u64 = 64 * 1024;

/// Maximum total explicitly requested text bytes (256 KiB).
pub const MAX_DELIVERY_INSPECTION_TOTAL_TEXT_BYTES: u64 = 256 * 1024;

/// Maximum compact JSON report size (1 MiB). A cap below the fixed minimal refusal
/// serialization cannot be honored by any report; such values are refused as `OutputLimit`.
pub const MAX_DELIVERY_INSPECTION_OUTPUT_BYTES: usize = 1024 * 1024;

/// Maximum stdout captured from one read-only Git command (1 MiB).
pub const MAX_DELIVERY_INSPECTION_GIT_OUTPUT_BYTES: usize = 1024 * 1024;

/// Per-command timeout for read-only Git inspection (5 seconds).
pub const DELIVERY_INSPECTION_GIT_TIMEOUT_MS: u64 = 5_000;

/// Overall wall-clock budget for one inspection (20 seconds).
pub const MAX_DELIVERY_INSPECTION_DURATION: Duration = Duration::from_secs(20);

/// Finite fixed physical state of one requested inspection file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryFilePhysicalCode {
    /// The path is an existing regular non-symlink file that was read within budget.
    RegularFile,
    /// The path does not exist.
    Missing,
    /// The path is a symbolic link or traverses a symbolic link component.
    Symlink,
    /// The path exists but is not a regular file (for example a directory or socket).
    NotRegularFile,
    /// Metadata could not be read safely.
    Unreadable,
}

/// Finite fixed change classification for one actual changed path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryChangeKind {
    /// A tracked or staged file was added.
    Added,
    /// A tracked file was modified.
    Modified,
    /// A tracked file was deleted.
    Deleted,
    /// A tracked path changed type (for example regular file to symlink).
    TypeChanged,
    /// An untracked, non-ignored file was created.
    Untracked,
    /// An ignored file was created.
    Ignored,
}

/// Fixed status of optional explicit text for one inspection file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryTextStatus {
    /// Text was not requested for this file.
    NotRequested,
    /// Bounded UTF-8 text is present in the report and is untrusted.
    Included,
    /// Explicit text was requested but the file does not exist.
    MissingFile,
    /// Explicit text was requested but the path is not a regular file.
    NotRegularFile,
    /// Explicit text was requested but the file is not valid UTF-8.
    NotUtf8,
    /// Explicit text was requested but the file exceeds a text budget.
    TooLarge,
    /// Explicit text was requested but the total read budget was exhausted.
    ReadLimit,
    /// The file changed or could not be read stably, so text was omitted.
    ChangingRead,
}

/// Fixed machine-readable refusal code for a refused inspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryInspectionRefusalCode {
    /// The request shape, path count, or expected base syntax is invalid.
    InvalidRequest,
    /// The task root is not a clean absolute path with UTF-8 text.
    InvalidRoot,
    /// The task root is not an existing directory.
    RootNotDirectory,
    /// The task root is not the top level of its Git worktree.
    RootNotWorktree,
    /// The expected base is not an exact 40-hex commit SHA.
    InvalidExpectedBase,
    /// The expected base commit does not exist in the task repository.
    UnknownBase,
    /// The actual checkout `HEAD` is not exactly the expected base commit.
    WrongHead,
    /// A read-only Git command failed, was unavailable, or was not supported.
    GitFailed,
    /// The effective repository configuration enables a content-converting filter.
    GitFilterConfigured,
    /// The index carries an unsupported assume-unchanged or skip-worktree flag.
    IndexFlagUnsupported,
    /// A read-only Git command exceeded its bounded child lifetime.
    GitTimeout,
    /// A read-only Git command exceeded its bounded stdout budget.
    GitOutputLimit,
    /// The platform does not support verified bounded Git inspection.
    UnsupportedPlatform,
    /// Git output was malformed or internally inconsistent.
    MalformedGitOutput,
    /// Git emitted a non-UTF-8 path, which is unsupported.
    NonUtf8Path,
    /// A supplied or emitted path was not a clean exact repository-relative file path.
    UnsafePath,
    /// A supplied or emitted path targets protected Git or agent control metadata.
    ProtectedPath,
    /// A path was repeated in the request.
    DuplicatePath,
    /// More requested paths than the configured bound were supplied.
    TooManyPaths,
    /// More actual changed paths than the configured bound were observed.
    TooManyChangedPaths,
    /// A rename or copy was observed; exact allowed file inspection refuses it.
    RenameNotAllowed,
    /// The Git index contains a staged change; inspection requires unchanged index state.
    StagedChange,
    /// Git reported an unmerged path that cannot be inspected as a stable delivery.
    UnmergedChange,
    /// A changed path is outside the exact supplied allowed file set.
    OutOfScopeChange,
    /// A path is a symbolic link or traverses a symbolic link component.
    SymlinkEscape,
    /// An inspected path is not a regular file.
    NotRegularFile,
    /// File metadata or content could not be read within the bounded read.
    FileReadFailed,
    /// An inspected file exceeds the per-file size budget.
    FileSizeLimit,
    /// The total read budget across inspected files would be exceeded.
    ReadLimit,
    /// An explicit text file was not also supplied as an allowed path.
    TextFileNotAllowed,
    /// An explicit text file is not valid UTF-8.
    TextFileNotUtf8,
    /// An explicit text file exceeds a text size budget.
    TextFileTooLarge,
    /// An explicit text file was requested but is missing.
    TextFileMissing,
    /// The checkout or one inspected file changed during the bounded read.
    ChangingRead,
    /// The overall inspection wall-clock budget was exceeded.
    Timeout,
    /// The compact JSON report would exceed its bounded output budget.
    OutputLimit,
}

impl DeliveryInspectionRefusalCode {
    /// Returns the stable `snake_case` machine code.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::InvalidRoot => "invalid_root",
            Self::RootNotDirectory => "root_not_directory",
            Self::RootNotWorktree => "root_not_worktree",
            Self::InvalidExpectedBase => "invalid_expected_base",
            Self::UnknownBase => "unknown_base",
            Self::WrongHead => "wrong_head",
            Self::GitFailed => "git_failed",
            Self::GitFilterConfigured => "git_filter_configured",
            Self::IndexFlagUnsupported => "index_flag_unsupported",
            Self::GitTimeout => "git_timeout",
            Self::GitOutputLimit => "git_output_limit",
            Self::UnsupportedPlatform => "unsupported_platform",
            Self::MalformedGitOutput => "malformed_git_output",
            Self::NonUtf8Path => "non_utf8_path",
            Self::UnsafePath => "unsafe_path",
            Self::ProtectedPath => "protected_path",
            Self::DuplicatePath => "duplicate_path",
            Self::TooManyPaths => "too_many_paths",
            Self::TooManyChangedPaths => "too_many_changed_paths",
            Self::RenameNotAllowed => "rename_not_allowed",
            Self::StagedChange => "staged_change",
            Self::UnmergedChange => "unmerged_change",
            Self::OutOfScopeChange => "out_of_scope_change",
            Self::SymlinkEscape => "symlink_escape",
            Self::NotRegularFile => "not_regular_file",
            Self::FileReadFailed => "file_read_failed",
            Self::FileSizeLimit => "file_size_limit",
            Self::ReadLimit => "read_limit",
            Self::TextFileNotAllowed => "text_file_not_allowed",
            Self::TextFileNotUtf8 => "text_file_not_utf8",
            Self::TextFileTooLarge => "text_file_too_large",
            Self::TextFileMissing => "text_file_missing",
            Self::ChangingRead => "changing_read",
            Self::Timeout => "timeout",
            Self::OutputLimit => "output_limit",
        }
    }
}

/// Bounded inspection budgets; defaults are the production CLI limits.
///
/// Tests and focused callers may supply smaller limits. A refusal is returned instead of an
/// incomplete report when any supplied budget is exhausted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryInspectionLimits {
    /// Maximum repeated allowed paths and maximum repeated text paths.
    pub max_paths: usize,
    /// Maximum actual changed paths.
    pub max_changed_paths: usize,
    /// Maximum bytes in one inspected regular file.
    pub max_file_bytes: u64,
    /// Maximum total bytes read across inspected files.
    pub max_total_read_bytes: u64,
    /// Maximum bytes in one explicitly requested text file.
    pub max_text_file_bytes: u64,
    /// Maximum total explicit text bytes.
    pub max_total_text_bytes: u64,
    /// Maximum compact report bytes before the report is replaced by a minimal refusal.
    pub max_output_bytes: usize,
    /// Maximum stdout bytes captured from one Git command.
    pub max_git_output_bytes: usize,
    /// Per-command Git timeout in milliseconds.
    pub git_timeout_ms: u64,
    /// Overall wall-clock inspection budget.
    pub max_duration: Duration,
}

impl Default for DeliveryInspectionLimits {
    fn default() -> Self {
        Self {
            max_paths: MAX_DELIVERY_INSPECTION_PATHS,
            max_changed_paths: MAX_DELIVERY_INSPECTION_CHANGED_PATHS,
            max_file_bytes: MAX_DELIVERY_INSPECTION_FILE_BYTES,
            max_total_read_bytes: MAX_DELIVERY_INSPECTION_TOTAL_READ_BYTES,
            max_text_file_bytes: MAX_DELIVERY_INSPECTION_TEXT_FILE_BYTES,
            max_total_text_bytes: MAX_DELIVERY_INSPECTION_TOTAL_TEXT_BYTES,
            max_output_bytes: MAX_DELIVERY_INSPECTION_OUTPUT_BYTES,
            max_git_output_bytes: MAX_DELIVERY_INSPECTION_GIT_OUTPUT_BYTES,
            git_timeout_ms: DELIVERY_INSPECTION_GIT_TIMEOUT_MS,
            max_duration: MAX_DELIVERY_INSPECTION_DURATION,
        }
    }
}

/// Input for one trusted-local physical delivery inspection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryInspectionRequest {
    /// Actual task checkout root (absolute, canonical, non-symlink).
    pub root: PathBuf,
    /// Expected frozen base commit from authenticated worker evidence or delivery.
    pub expected_base: String,
    /// Exact allowed repository-relative file paths (repeatable).
    pub allowed_paths: Vec<String>,
    /// Exact allowed repository-relative files whose small UTF-8 contents are included.
    pub text_files: Vec<String>,
}

/// One actual changed path relative to the expected base.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryChangedPath {
    /// Clean repository-relative path.
    pub path: String,
    /// All observed change classes for this path.
    pub change_kinds: Vec<DeliveryChangeKind>,
    /// True when the Git index differs from `HEAD` at this path.
    pub staged: bool,
    /// True when the working tree differs from the index at this path.
    pub unstaged: bool,
    /// True when the path is untracked (including ignored additions).
    pub untracked: bool,
    /// True when the path is ignored by Git.
    pub ignored: bool,
    /// True when the path exactly matches a supplied allowed path.
    pub allowed: bool,
}

/// Bounded physical metadata for one requested allowed file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryFileInspection {
    /// Clean repository-relative path.
    pub path: String,
    /// True when this path appears in the actual changed-path set.
    pub changed: bool,
    /// All observed change classes for this path.
    pub change_kinds: Vec<DeliveryChangeKind>,
    /// Fixed physical state of the path.
    pub physical: DeliveryFilePhysicalCode,
    /// True only for an existing regular non-symlink file.
    pub regular_file: bool,
    /// UTF-8 validity for a present regular file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub utf8: Option<bool>,
    /// True when a present regular file ends with a final `\n` byte.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_lf: Option<bool>,
    /// File size in bytes for a present regular file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// Lowercase SHA-256 hex digest for a present regular file read within budget.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    /// Explicit text inclusion status.
    pub text_status: DeliveryTextStatus,
    /// Explicit untrusted text content, present only when requested and bounded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// True when [`Self::text`] is present.
    pub text_untrusted: bool,
}

/// Machine-readable result of one bounded physical delivery inspection.
///
/// A report with `ok: true` contains observed physical facts only. It is not an acceptance
/// decision, does not execute or endorse file contents, and never grants `Done` authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryInspectionReport {
    /// Stable report schema version.
    pub schema_version: u32,
    /// Canonical tool name (`threadlane inspect-delivery`).
    pub tool: String,
    /// Always `true`: this report is inspection evidence only.
    pub inspection_only: bool,
    /// Always `false`: this report grants no execution, review, acceptance, or `Done` authority.
    pub authority_granted: bool,
    /// True only when the bounded physical inspection completed without refusal.
    pub ok: bool,
    /// Fixed refusal code when `ok` is false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal_code: Option<DeliveryInspectionRefusalCode>,
    /// Safe path associated with the refusal, when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal_path: Option<String>,
    /// Fixed non-content diagnostic summary.
    pub message: String,
    /// Actual task root as supplied.
    pub root: String,
    /// Expected frozen base commit as supplied.
    pub expected_base: String,
    /// Actual current `HEAD`, when resolved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_commit: Option<String>,
    /// True when the expected base is an ancestor of (or equal to) `HEAD`; exact equality is
    /// still required for a successful inspection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_is_ancestor: Option<bool>,
    /// Actual changed-path set relative to the expected base.
    pub changed_paths: Vec<DeliveryChangedPath>,
    /// Per-requested-allowed-file physical metadata.
    pub files: Vec<DeliveryFileInspection>,
    /// True when any untrusted text is present in this report.
    pub text_included: bool,
    /// True when any report content must be treated as untrusted data.
    pub untrusted_data: bool,
    /// True when detailed changed/file evidence was omitted to honor the output budget.
    pub output_omitted: bool,
    /// Optional separately observed source checkout; never an acceptance decision.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceInspection>,
}

/// Physical comparison of one explicitly requested source file with the frozen Git base.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceFileInspection {
    /// Exact repository-relative file path.
    pub path: String,
    /// SHA-256 of the current regular file's exact bytes.
    pub sha256: String,
    /// SHA-256 of the regular file's exact blob bytes at the frozen base.
    pub base_sha256: String,
    /// Whether the observed bytes match the frozen base.
    pub matches_base: bool,
    /// Exact size of explicitly requested source text in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// Whether explicitly requested source text ends in a final `\n` byte.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_lf: Option<bool>,
    /// Explicit untrusted source text, included only in the opt-in inspection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// True when source text is included; absent in a non-text source inspection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text_untrusted: Option<bool>,
}

/// Bounded source checkout facts. A failed observation never means the source matched.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceInspection {
    /// True only when all requested evidence was observed stably.
    pub ok: bool,
    /// Fixed refusal code when the source observation could not be completed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal_code: Option<DeliveryInspectionRefusalCode>,
    /// Canonical source root requested by the caller.
    pub root: String,
    /// Frozen base used to compare source facts.
    pub expected_base: String,
    /// Observed source HEAD.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_commit: Option<String>,
    /// Tree ID of the frozen base.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_tree: Option<String>,
    /// Complete bounded set of tracked, untracked and ignored changes.
    pub changed_paths: Vec<DeliveryChangedPath>,
    /// True only for an observed base HEAD with no changed, untracked or ignored paths.
    pub clean_at_base: bool,
    /// Exact byte comparisons for explicitly named regular files.
    pub files: Vec<SourceFileInspection>,
}

/// Performs one bounded physical inspection with the default production limits.
pub fn inspect_delivery(request: &DeliveryInspectionRequest) -> DeliveryInspectionReport {
    inspect_delivery_with_limits(request, &DeliveryInspectionLimits::default())
}

/// Performs one bounded physical inspection with explicit limits.
///
/// The function is synchronous and offline. It refuses with a fixed code instead of claiming
/// verification when any budget or safety check fails.
pub fn inspect_delivery_with_limits(
    request: &DeliveryInspectionRequest,
    limits: &DeliveryInspectionLimits,
) -> DeliveryInspectionReport {
    let started = Instant::now();
    let mut progress = InspectionProgress::default();
    let mut report = DeliveryInspectionReport {
        schema_version: DELIVERY_INSPECTION_SCHEMA_VERSION,
        tool: DELIVERY_INSPECTION_TOOL.to_string(),
        inspection_only: true,
        authority_granted: false,
        ok: false,
        refusal_code: None,
        refusal_path: None,
        message: "physical delivery inspection did not run".to_string(),
        root: truncate_report_field(&request.root.to_string_lossy(), 4096).to_string(),
        expected_base: truncate_report_field(&request.expected_base, 4096).to_string(),
        head_commit: None,
        base_is_ancestor: None,
        changed_paths: Vec::new(),
        files: Vec::new(),
        text_included: false,
        untrusted_data: false,
        output_omitted: false,
        source: None,
    };

    let mut failure = match validate_limits(limits) {
        Err(failure) => Some(failure),
        Ok(()) => match validate_request(request, limits) {
            Ok(validated) => inspect_validated(&validated, limits, started, &mut progress),
            Err(failure) => Some(failure),
        },
    };

    report.head_commit = progress.head;
    report.base_is_ancestor = progress.base_is_ancestor;
    report.changed_paths = progress.changed;
    report.files = progress.files;
    // Any refusal withholds all explicit text, even when a different primary refusal wins.
    if failure.is_some() {
        omit_unstable_text(&mut report.files);
    }
    report.text_included = report.files.iter().any(|file| file.text.is_some());
    report.untrusted_data = report.text_included;

    if failure.is_none() {
        report.ok = true;
        report.message = "physical delivery inspection completed; report grants no review, acceptance, or Done authority".to_string();
    } else if let Some(primary) = failure.take() {
        report.ok = false;
        report.refusal_code = Some(primary.code);
        report.refusal_path = primary.path;
        let code = primary.code.as_str();
        report.message = format!("physical delivery inspection refused: {code}");
    }

    enforce_output_limit(report, limits.max_output_bytes)
}

/// Adds a bounded source observation to the same physical inspection invocation.
/// The source may be dirty; callers decide whether its facts meet their own task.
pub fn inspect_delivery_with_source(
    request: &DeliveryInspectionRequest,
    source_root: &Path,
    source_files: &[String],
) -> DeliveryInspectionReport {
    inspect_delivery_with_source_and_limits(
        request,
        source_root,
        source_files,
        &DeliveryInspectionLimits::default(),
    )
}

/// Inspects the task and source under one shared deadline, byte and output budget.
pub fn inspect_delivery_with_source_and_limits(
    request: &DeliveryInspectionRequest,
    source_root: &Path,
    source_files: &[String],
    limits: &DeliveryInspectionLimits,
) -> DeliveryInspectionReport {
    inspect_delivery_with_source_mode(request, source_root, source_files, limits, false)
}

/// Inspects explicitly named source files with their bounded, untrusted UTF-8 text.
///
/// Text and the current-file SHA-256 come from the same stable read. Source cleanliness,
/// HEAD and base-byte equality are reported independently and must be checked by the caller.
pub fn inspect_delivery_with_source_text(
    request: &DeliveryInspectionRequest,
    source_root: &Path,
    source_files: &[String],
) -> DeliveryInspectionReport {
    inspect_delivery_with_source_text_and_limits(
        request,
        source_root,
        source_files,
        &DeliveryInspectionLimits::default(),
    )
}

/// Inspects source text under shared task/source deadline, read, text and output budgets.
pub fn inspect_delivery_with_source_text_and_limits(
    request: &DeliveryInspectionRequest,
    source_root: &Path,
    source_files: &[String],
    limits: &DeliveryInspectionLimits,
) -> DeliveryInspectionReport {
    inspect_delivery_with_source_mode(request, source_root, source_files, limits, true)
}

fn inspect_delivery_with_source_mode(
    request: &DeliveryInspectionRequest,
    source_root: &Path,
    source_files: &[String],
    limits: &DeliveryInspectionLimits,
    include_source_text: bool,
) -> DeliveryInspectionReport {
    let started = Instant::now();
    let mut report = inspect_delivery_with_limits(request, limits);
    if report.ok {
        let task_bytes = report.files.iter().filter_map(|file| file.size_bytes).sum();
        let task_text_bytes = report
            .files
            .iter()
            .filter_map(|file| file.text.as_ref())
            .map(|text| text.len() as u64)
            .sum();
        report.source = Some(inspect_source(
            request,
            source_root,
            source_files,
            limits,
            started,
            task_bytes,
            task_text_bytes,
            include_source_text,
        ));
        if report.source.as_ref().is_some_and(|source| !source.ok) {
            report.ok = false;
            report.refusal_code = report
                .source
                .as_ref()
                .and_then(|source| source.refusal_code);
            report.message =
                "source inspection refused; requested evidence is unavailable".to_string();
            omit_unstable_text(&mut report.files);
            report.text_included = false;
            report.untrusted_data = false;
        } else if include_source_text {
            report.text_included |= report
                .source
                .as_ref()
                .is_some_and(|source| source.files.iter().any(|file| file.text.is_some()));
            report.untrusted_data = report.text_included;
        }
    }
    enforce_output_limit(report, limits.max_output_bytes)
}

#[allow(clippy::too_many_arguments)]
fn inspect_source(
    task: &DeliveryInspectionRequest,
    root: &Path,
    files: &[String],
    limits: &DeliveryInspectionLimits,
    started: Instant,
    task_bytes: u64,
    task_text_bytes: u64,
    include_source_text: bool,
) -> SourceInspection {
    let mut report = SourceInspection {
        ok: false,
        refusal_code: None,
        root: truncate_report_field(&root.to_string_lossy(), 4096).to_string(),
        expected_base: task.expected_base.clone(),
        head_commit: None,
        base_tree: None,
        changed_paths: Vec::new(),
        clean_at_base: false,
        files: Vec::new(),
    };
    let result = inspect_source_inner(
        task,
        root,
        files,
        limits,
        started,
        task_bytes,
        task_text_bytes,
        include_source_text,
    );
    match result {
        Ok((head, tree, changes, inspected)) => {
            report.clean_at_base = head == task.expected_base
                && changes.is_empty()
                && inspected.iter().all(|file| file.matches_base);
            report.head_commit = Some(head);
            report.base_tree = Some(tree);
            report.changed_paths = changes;
            report.files = inspected;
            report.ok = true;
        }
        Err(failure) => report.refusal_code = Some(failure.code),
    }
    report
}

#[allow(clippy::too_many_arguments)]
fn inspect_source_inner(
    task: &DeliveryInspectionRequest,
    root: &Path,
    files: &[String],
    limits: &DeliveryInspectionLimits,
    started: Instant,
    task_bytes: u64,
    task_text_bytes: u64,
    include_source_text: bool,
) -> Result<
    (
        String,
        String,
        Vec<DeliveryChangedPath>,
        Vec<SourceFileInspection>,
    ),
    InspectionFailure,
> {
    if root == task.root {
        return Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::InvalidRoot,
        ));
    }
    let source = validate_request(
        &DeliveryInspectionRequest {
            root: root.to_path_buf(),
            expected_base: task.expected_base.clone(),
            allowed_paths: files.to_vec(),
            text_files: if include_source_text {
                files.to_vec()
            } else {
                Vec::new()
            },
        },
        limits,
    )?;
    if read_worktree_top_level(&source.root, limits, started)? != source.root {
        return Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::RootNotWorktree,
        ));
    }
    if resolve_git_commit(&source.root, &source.expected_base, limits, started, true)?
        != source.expected_base
    {
        return Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::UnknownBase,
        ));
    }
    let tree_spec = format!("{}^{{tree}}", source.expected_base);
    let tree_bytes = run_git_bytes(
        &source.root,
        &["rev-parse", "--verify", &tree_spec],
        limits,
        started,
    )?;
    let tree_text = std::str::from_utf8(&tree_bytes)
        .map_err(|_| InspectionFailure::new(DeliveryInspectionRefusalCode::MalformedGitOutput))?;
    let tree = validate_exact_40_hex_sha(tree_text.trim(), "base tree")
        .map_err(|_| InspectionFailure::new(DeliveryInspectionRefusalCode::MalformedGitOutput))?;
    let first = capture_snapshot(&source.root, &source.expected_base, limits, started)?;
    let mut inspected = Vec::new();
    let mut observations = Vec::new();
    let mut total_read = task_bytes;
    let mut total_text = task_text_bytes;
    for path in &source.allowed_paths {
        let (current, failure) = inspect_file(
            &source.root,
            path,
            first.changes.get(path),
            include_source_text,
            limits,
            started,
            &mut total_read,
            &mut total_text,
            &mut observations,
        );
        if let Some(failure) = failure {
            return Err(failure);
        }
        let actual = current.sha256.ok_or_else(|| {
            InspectionFailure::at(DeliveryInspectionRefusalCode::FileReadFailed, path)
        })?;
        // `ls-tree` proves the exact base entry is a regular file, not a symlink or tree.
        let literal = format!(":(literal){path}");
        let entry = run_git_bytes(
            &source.root,
            &["ls-tree", "-z", &source.expected_base, "--", &literal],
            limits,
            started,
        )?;
        let entry_text = std::str::from_utf8(&entry).map_err(|_| {
            InspectionFailure::new(DeliveryInspectionRefusalCode::MalformedGitOutput)
        })?;
        let (metadata, name) = entry_text
            .strip_suffix('\0')
            .and_then(|s| s.split_once('\t'))
            .ok_or_else(|| {
                InspectionFailure::at(DeliveryInspectionRefusalCode::FileReadFailed, path)
            })?;
        let mut fields = metadata.split(' ');
        let mode = fields.next();
        let kind = fields.next();
        let oid = fields.next();
        if !matches!(mode, Some("100644" | "100755"))
            || kind != Some("blob")
            || fields.next().is_some()
            || name != path
        {
            return Err(InspectionFailure::at(
                DeliveryInspectionRefusalCode::FileReadFailed,
                path,
            ));
        }
        let oid =
            validate_exact_40_hex_sha(oid.unwrap_or_default(), "base blob").map_err(|_| {
                InspectionFailure::new(DeliveryInspectionRefusalCode::MalformedGitOutput)
            })?;
        let size_bytes = run_git_bytes(&source.root, &["cat-file", "-s", &oid], limits, started)?;
        let size = std::str::from_utf8(&size_bytes)
            .ok()
            .and_then(|text| text.trim().parse::<u64>().ok())
            .ok_or_else(|| {
                InspectionFailure::new(DeliveryInspectionRefusalCode::MalformedGitOutput)
            })?;
        total_read = total_read
            .checked_add(size)
            .filter(|size| *size <= limits.max_total_read_bytes)
            .ok_or_else(|| InspectionFailure::new(DeliveryInspectionRefusalCode::ReadLimit))?;
        if size > limits.max_git_output_bytes as u64 {
            return Err(InspectionFailure::new(
                DeliveryInspectionRefusalCode::GitOutputLimit,
            ));
        }
        let bytes = run_git_bytes(&source.root, &["cat-file", "blob", &oid], limits, started)?;
        if bytes.len() as u64 != size {
            return Err(InspectionFailure::new(
                DeliveryInspectionRefusalCode::ChangingRead,
            ));
        }
        let base = sha256_hex(&bytes);
        inspected.push(SourceFileInspection {
            path: path.clone(),
            matches_base: actual == base,
            sha256: actual,
            base_sha256: base,
            size_bytes: include_source_text.then_some(current.size_bytes).flatten(),
            final_lf: include_source_text.then_some(current.final_lf).flatten(),
            text: current.text,
            text_untrusted: include_source_text.then_some(current.text_untrusted),
        });
    }
    if capture_snapshot(&source.root, &source.expected_base, limits, started)? != first {
        return Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::ChangingRead,
        ));
    }
    revalidate_observations(&source.root, &observations, limits, started)?;
    let changes = changed_paths(&first.changes, &BTreeSet::new());
    Ok((first.head, tree, changes, inspected))
}

#[derive(Debug)]
struct InspectionFailure {
    code: DeliveryInspectionRefusalCode,
    path: Option<String>,
}

impl InspectionFailure {
    fn new(code: DeliveryInspectionRefusalCode) -> Self {
        Self { code, path: None }
    }

    fn at(code: DeliveryInspectionRefusalCode, path: impl Into<String>) -> Self {
        Self {
            code,
            path: Some(path.into()),
        }
    }
}

struct ValidatedRequest {
    root: PathBuf,
    expected_base: String,
    allowed_paths: Vec<String>,
    allowed: BTreeSet<String>,
    text_files: BTreeSet<String>,
}

#[derive(Default)]
struct InspectionProgress {
    head: Option<String>,
    base_is_ancestor: Option<bool>,
    changed: Vec<DeliveryChangedPath>,
    files: Vec<DeliveryFileInspection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GitSnapshot {
    head: String,
    changes: BTreeMap<String, ChangeAccumulator>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ChangeAccumulator {
    kinds: BTreeSet<DeliveryChangeKind>,
    staged: bool,
    unstaged: bool,
    untracked: bool,
    ignored: bool,
}

#[derive(Debug)]
struct RawChange {
    kind: DeliveryChangeKind,
    path: String,
}

fn validate_limits(limits: &DeliveryInspectionLimits) -> Result<(), InspectionFailure> {
    if limits.max_output_bytes == 0 {
        return Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::OutputLimit,
        ));
    }
    if limits.max_git_output_bytes == 0 {
        return Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::GitOutputLimit,
        ));
    }
    if limits.max_duration.is_zero() {
        return Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::Timeout,
        ));
    }
    Ok(())
}

fn validate_request(
    request: &DeliveryInspectionRequest,
    limits: &DeliveryInspectionLimits,
) -> Result<ValidatedRequest, InspectionFailure> {
    if request.allowed_paths.len() > limits.max_paths || request.text_files.len() > limits.max_paths
    {
        return Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::TooManyPaths,
        ));
    }
    if request.root.as_os_str().is_empty() || request.root.to_str().is_none() {
        return Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::InvalidRoot,
        ));
    }
    if !request.root.is_absolute() {
        return Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::InvalidRoot,
        ));
    }
    for component in request.root.components() {
        if matches!(component, Component::CurDir | Component::ParentDir) {
            return Err(InspectionFailure::new(
                DeliveryInspectionRefusalCode::InvalidRoot,
            ));
        }
    }
    let root = validate_absolute_no_symlink_components(&request.root, "task root").map_err(
        |err| match err {
            WorkerError::SecurityViolation(_) => {
                InspectionFailure::new(DeliveryInspectionRefusalCode::SymlinkEscape)
            }
            WorkerError::UnsupportedPlatform(_) => {
                InspectionFailure::new(DeliveryInspectionRefusalCode::UnsupportedPlatform)
            }
            _ => InspectionFailure::new(DeliveryInspectionRefusalCode::InvalidRoot),
        },
    )?;
    let root_metadata = fs::symlink_metadata(&root)
        .map_err(|_| InspectionFailure::new(DeliveryInspectionRefusalCode::InvalidRoot))?;
    if root_metadata.file_type().is_symlink() {
        return Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::SymlinkEscape,
        ));
    }
    if !root_metadata.file_type().is_dir() {
        return Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::RootNotDirectory,
        ));
    }
    let canonical_root = fs::canonicalize(&root)
        .map_err(|_| InspectionFailure::new(DeliveryInspectionRefusalCode::InvalidRoot))?;
    if canonical_root != root {
        return Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::InvalidRoot,
        ));
    }

    let expected_base = validate_exact_40_hex_sha(&request.expected_base, "expected base")
        .map_err(|_| InspectionFailure::new(DeliveryInspectionRefusalCode::InvalidExpectedBase))?;

    let mut allowed = BTreeSet::new();
    for path in &request.allowed_paths {
        validate_allowed_path(path)?;
        if !allowed.insert(path.clone()) {
            return Err(InspectionFailure::at(
                DeliveryInspectionRefusalCode::DuplicatePath,
                path.clone(),
            ));
        }
    }

    let mut text_files = BTreeSet::new();
    for path in &request.text_files {
        validate_allowed_path(path)?;
        if !allowed.contains(path) {
            return Err(InspectionFailure::at(
                DeliveryInspectionRefusalCode::TextFileNotAllowed,
                path.clone(),
            ));
        }
        if !text_files.insert(path.clone()) {
            return Err(InspectionFailure::at(
                DeliveryInspectionRefusalCode::DuplicatePath,
                path.clone(),
            ));
        }
    }

    Ok(ValidatedRequest {
        root,
        expected_base,
        allowed_paths: request.allowed_paths.clone(),
        allowed,
        text_files,
    })
}

fn validate_allowed_path(path: &str) -> Result<(), InspectionFailure> {
    validate_raw_exact_file_path(path)?;
    validate_repo_relative_path(path, "allowed file path")
        .map_err(|_| InspectionFailure::at(DeliveryInspectionRefusalCode::UnsafePath, path))?;
    if is_control_config_path(path) {
        return Err(InspectionFailure::at(
            DeliveryInspectionRefusalCode::ProtectedPath,
            path,
        ));
    }
    if path.split('/').any(is_protected_credential_component) {
        return Err(InspectionFailure::at(
            DeliveryInspectionRefusalCode::ProtectedPath,
            path,
        ));
    }
    Ok(())
}

/// Validates the literal slash-separated spelling before any file or text read.
///
/// [`Path::components`] normalizes `.` segments, so inspecting only parsed components would
/// accept aliases such as `src/./lib.rs`. This check works on the raw request text and rejects
/// empty, dot, glob, control, and directory aliases explicitly.
fn validate_raw_exact_file_path(path: &str) -> Result<(), InspectionFailure> {
    if path.is_empty()
        || path.starts_with('/')
        || path.ends_with('/')
        || path.contains('\\')
        || path.contains("//")
        || path.contains('\0')
    {
        return Err(InspectionFailure::at(
            DeliveryInspectionRefusalCode::UnsafePath,
            path,
        ));
    }
    for component in path.split('/') {
        if component.is_empty()
            || component == "."
            || component == ".."
            || component.chars().all(|ch| ch == '.')
            || component.chars().any(char::is_control)
            || component
                .chars()
                .any(|ch| matches!(ch, '*' | '?' | '[' | ']' | '{' | '}'))
        {
            return Err(InspectionFailure::at(
                DeliveryInspectionRefusalCode::UnsafePath,
                path,
            ));
        }
    }
    Ok(())
}

fn is_protected_credential_component(component: &str) -> bool {
    matches!(
        component.to_ascii_lowercase().as_str(),
        ".aws"
            | ".ssh"
            | ".gnupg"
            | ".config"
            | ".docker"
            | ".kube"
            | ".azure"
            | ".gcloud"
            | ".password-store"
            | ".netrc"
            | ".npmrc"
            | ".pypirc"
            | ".git-credentials"
            | ".terraform"
            | ".vault-token"
    )
}

fn inspect_validated(
    request: &ValidatedRequest,
    limits: &DeliveryInspectionLimits,
    started: Instant,
    progress: &mut InspectionProgress,
) -> Option<InspectionFailure> {
    if let Err(failure) = check_deadline(started, limits) {
        return Some(failure);
    }

    let top_level = match read_worktree_top_level(&request.root, limits, started) {
        Ok(path) => path,
        Err(failure) => return Some(failure),
    };
    if top_level != request.root {
        return Some(InspectionFailure::new(
            DeliveryInspectionRefusalCode::RootNotWorktree,
        ));
    }

    if let Err(deadline) = check_deadline(started, limits) {
        return Some(deadline);
    }
    let head = match resolve_git_commit(&request.root, "HEAD", limits, started, false) {
        Ok(head) => head,
        Err(err) => return Some(err),
    };
    progress.head = Some(head.clone());
    if let Err(deadline) = check_deadline(started, limits) {
        return Some(deadline);
    }
    let resolved_base =
        match resolve_git_commit(&request.root, &request.expected_base, limits, started, true) {
            Ok(base) => base,
            Err(err) => return Some(err),
        };
    if resolved_base != request.expected_base {
        return Some(InspectionFailure::new(
            DeliveryInspectionRefusalCode::UnknownBase,
        ));
    }

    if let Err(deadline) = check_deadline(started, limits) {
        return Some(deadline);
    }
    if head != request.expected_base {
        let base_is_ancestor = match is_git_ancestor(
            &request.root,
            &request.expected_base,
            &head,
            limits,
            started,
        ) {
            Ok(value) => value,
            Err(err) => return Some(err),
        };
        progress.base_is_ancestor = Some(base_is_ancestor);
        return Some(InspectionFailure::new(
            DeliveryInspectionRefusalCode::WrongHead,
        ));
    }
    progress.base_is_ancestor = Some(true);

    let first = match capture_snapshot(&request.root, &request.expected_base, limits, started) {
        Ok(snapshot) => snapshot,
        Err(failure) => return Some(failure),
    };
    if first.head != head {
        return Some(InspectionFailure::new(
            DeliveryInspectionRefusalCode::ChangingRead,
        ));
    }
    progress.changed = changed_paths(&first.changes, &request.allowed);

    let mut failure = None;
    if let Some(path) = progress
        .changed
        .iter()
        .find(|changed| !changed.allowed)
        .map(|changed| changed.path.clone())
    {
        record_failure(
            &mut failure,
            InspectionFailure::at(DeliveryInspectionRefusalCode::OutOfScopeChange, path),
        );
    }
    if let Some(path) = progress
        .changed
        .iter()
        .find(|changed| changed.staged)
        .map(|changed| changed.path.clone())
    {
        record_failure(
            &mut failure,
            InspectionFailure::at(DeliveryInspectionRefusalCode::StagedChange, path),
        );
    }

    let mut total_read = 0u64;
    let mut total_text = 0u64;
    let mut observations: Vec<FileObservation> = Vec::new();
    for path in &request.allowed_paths {
        if let Err(deadline) = check_deadline(started, limits) {
            record_failure(&mut failure, deadline);
            return failure;
        }
        let text_requested = request.text_files.contains(path);
        let (file, file_failure) = inspect_file(
            &request.root,
            path,
            first.changes.get(path),
            text_requested,
            limits,
            started,
            &mut total_read,
            &mut total_text,
            &mut observations,
        );
        if let Some(file_failure) = file_failure {
            record_failure(&mut failure, file_failure);
        }
        progress.files.push(file);
    }

    if let Err(deadline) = check_deadline(started, limits) {
        record_failure(&mut failure, deadline);
        return failure;
    }
    match capture_snapshot(&request.root, &request.expected_base, limits, started) {
        Ok(second) => {
            if second != first {
                record_failure(
                    &mut failure,
                    InspectionFailure::new(DeliveryInspectionRefusalCode::ChangingRead),
                );
            }
        }
        Err(snapshot_failure) => record_failure(&mut failure, snapshot_failure),
    }
    if let Err(deadline) = check_deadline(started, limits) {
        record_failure(&mut failure, deadline);
    }

    if let Err(unstable) = revalidate_observations(&request.root, &observations, limits, started) {
        record_failure(&mut failure, unstable);
    }
    if let Err(deadline) = check_deadline(started, limits) {
        record_failure(&mut failure, deadline);
    }

    failure
}

fn check_deadline(
    started: Instant,
    limits: &DeliveryInspectionLimits,
) -> Result<(), InspectionFailure> {
    if started.elapsed() >= limits.max_duration {
        return Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::Timeout,
        ));
    }
    Ok(())
}

fn read_worktree_top_level(
    root: &Path,
    limits: &DeliveryInspectionLimits,
    started: Instant,
) -> Result<PathBuf, InspectionFailure> {
    let args = ["rev-parse", "--show-toplevel"];
    let bytes = run_git_bytes(root, &args, limits, started)?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| InspectionFailure::new(DeliveryInspectionRefusalCode::NonUtf8Path))?;
    let trimmed = text.trim_end_matches(['\n', '\r']);
    if trimmed.is_empty() {
        return Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::MalformedGitOutput,
        ));
    }
    let path = PathBuf::from(trimmed);
    if !path.is_absolute() {
        return Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::RootNotWorktree,
        ));
    }
    let canonical = fs::canonicalize(&path)
        .map_err(|_| InspectionFailure::new(DeliveryInspectionRefusalCode::RootNotWorktree))?;
    Ok(canonical)
}

fn capture_snapshot(
    root: &Path,
    base: &str,
    limits: &DeliveryInspectionLimits,
    started: Instant,
) -> Result<GitSnapshot, InspectionFailure> {
    check_deadline(started, limits)?;
    preflight_git_metadata(root, limits, started)?;
    let head = resolve_git_commit(root, "HEAD", limits, started, false)?;
    let mut changes: BTreeMap<String, ChangeAccumulator> = BTreeMap::new();

    let committed = parse_name_status_z(&run_diff(root, false, &[base, "HEAD"], limits, started)?)?;
    for raw in committed {
        add_change(
            &mut changes,
            raw,
            false,
            false,
            false,
            false,
            limits.max_changed_paths,
        )?;
    }
    let staged = parse_name_status_z(&run_diff(root, true, &["HEAD"], limits, started)?)?;
    for raw in staged {
        add_change(
            &mut changes,
            raw,
            true,
            false,
            false,
            false,
            limits.max_changed_paths,
        )?;
    }
    let unstaged = parse_name_status_z(&run_diff(root, false, &[], limits, started)?)?;
    let unstaged_numstat_paths =
        parse_numstat_paths_z(&run_diff_numstat(root, &[], limits, started)?)?;
    for raw in unstaged {
        if !unstaged_numstat_paths.contains(&raw.path) {
            continue;
        }
        add_change(
            &mut changes,
            raw,
            false,
            true,
            false,
            false,
            limits.max_changed_paths,
        )?;
    }
    for path in parse_ls_files_z(&run_ls_files(root, false, limits, started)?)? {
        add_change(
            &mut changes,
            RawChange {
                kind: DeliveryChangeKind::Untracked,
                path,
            },
            false,
            false,
            true,
            false,
            limits.max_changed_paths,
        )?;
    }
    for path in parse_ls_files_z(&run_ls_files(root, true, limits, started)?)? {
        add_change(
            &mut changes,
            RawChange {
                kind: DeliveryChangeKind::Ignored,
                path,
            },
            false,
            false,
            true,
            true,
            limits.max_changed_paths,
        )?;
    }
    check_deadline(started, limits)?;

    Ok(GitSnapshot { head, changes })
}

fn preflight_git_metadata(
    root: &Path,
    limits: &DeliveryInspectionLimits,
    started: Instant,
) -> Result<(), InspectionFailure> {
    let configured_filters = run_git_bounded(
        root,
        &[
            "config",
            "--null",
            "--name-only",
            "--get-regexp",
            r"^filter\..*\.(clean|process|smudge)$",
        ],
        limits,
        started,
        true,
    )?;
    match configured_filters.exit_code {
        Some(0) if configured_filters.stdout_bytes.is_empty() => {}
        Some(0) => {
            return Err(InspectionFailure::new(
                DeliveryInspectionRefusalCode::GitFilterConfigured,
            ))
        }
        Some(1) => {}
        _ => {
            return Err(InspectionFailure::new(
                DeliveryInspectionRefusalCode::GitFailed,
            ))
        }
    }

    let index = run_git_bounded(
        root,
        &["ls-files", "-v", "-z", "--"],
        limits,
        started,
        false,
    )?;
    parse_index_flags_z(&index.stdout_bytes)
}

fn parse_index_flags_z(bytes: &[u8]) -> Result<(), InspectionFailure> {
    let mut rest = bytes;
    while !rest.is_empty() {
        let (entry, after_entry) = take_nul(rest).ok_or_else(malformed_git_output)?;
        rest = after_entry;
        if entry.is_empty() {
            return Err(malformed_git_output());
        }
        if entry.len() < 3 || !matches!(entry[1], b' ' | b'\t') {
            return Err(malformed_git_output());
        }
        match entry[0] {
            b'H' => {}
            b'M' => {
                return Err(InspectionFailure::new(
                    DeliveryInspectionRefusalCode::UnmergedChange,
                ))
            }
            _ => {
                return Err(InspectionFailure::new(
                    DeliveryInspectionRefusalCode::IndexFlagUnsupported,
                ))
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn add_change(
    changes: &mut BTreeMap<String, ChangeAccumulator>,
    raw: RawChange,
    staged: bool,
    unstaged: bool,
    untracked: bool,
    ignored: bool,
    max_changed_paths: usize,
) -> Result<(), InspectionFailure> {
    validate_repo_relative_path(&raw.path, "changed path").map_err(|_| {
        InspectionFailure::at(
            DeliveryInspectionRefusalCode::MalformedGitOutput,
            raw.path.clone(),
        )
    })?;
    if is_control_config_path(&raw.path) {
        return Err(InspectionFailure::at(
            DeliveryInspectionRefusalCode::ProtectedPath,
            raw.path,
        ));
    }
    if !changes.contains_key(&raw.path) && changes.len() >= max_changed_paths {
        return Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::TooManyChangedPaths,
        ));
    }
    let entry = changes.entry(raw.path).or_default();
    entry.kinds.insert(raw.kind);
    entry.staged |= staged;
    entry.unstaged |= unstaged;
    entry.untracked |= untracked;
    entry.ignored |= ignored;
    Ok(())
}

fn changed_paths(
    changes: &BTreeMap<String, ChangeAccumulator>,
    allowed: &BTreeSet<String>,
) -> Vec<DeliveryChangedPath> {
    changes
        .iter()
        .map(|(path, change)| DeliveryChangedPath {
            path: path.clone(),
            change_kinds: change.kinds.iter().copied().collect(),
            staged: change.staged,
            unstaged: change.unstaged,
            untracked: change.untracked,
            ignored: change.ignored,
            allowed: allowed.contains(path),
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileEvidence {
    dev: u64,
    ino: u64,
    len: u64,
    mtime: i64,
    mtime_nsec: i64,
    ctime: i64,
    ctime_nsec: i64,
}

#[derive(Debug)]
struct FileObservation {
    path: String,
    evidence: Option<FileEvidence>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SafeOpenFailure {
    Missing,
    Symlink,
    NotRegularFile,
    Changed,
    Unreadable,
}

#[allow(clippy::too_many_arguments)]
fn inspect_file(
    root: &Path,
    path: &str,
    change: Option<&ChangeAccumulator>,
    text_requested: bool,
    limits: &DeliveryInspectionLimits,
    started: Instant,
    total_read: &mut u64,
    total_text: &mut u64,
    observations: &mut Vec<FileObservation>,
) -> (DeliveryFileInspection, Option<InspectionFailure>) {
    let mut entry = DeliveryFileInspection {
        path: path.to_string(),
        changed: change.is_some(),
        change_kinds: change
            .map(|change| change.kinds.iter().copied().collect())
            .unwrap_or_default(),
        physical: DeliveryFilePhysicalCode::Missing,
        regular_file: false,
        utf8: None,
        final_lf: None,
        size_bytes: None,
        sha256: None,
        text_status: if text_requested {
            DeliveryTextStatus::MissingFile
        } else {
            DeliveryTextStatus::NotRequested
        },
        text: None,
        text_untrusted: false,
    };
    let (mut file, before) = match open_inspection_file(root, path) {
        Ok(opened) => opened,
        Err(SafeOpenFailure::Missing) => {
            observations.push(FileObservation {
                path: path.to_string(),
                evidence: None,
            });
            if text_requested {
                entry.text_status = DeliveryTextStatus::MissingFile;
                return (
                    entry,
                    Some(InspectionFailure::at(
                        DeliveryInspectionRefusalCode::TextFileMissing,
                        path,
                    )),
                );
            }
            if change.is_some_and(expects_present_file) {
                return (
                    entry,
                    Some(InspectionFailure::new(
                        DeliveryInspectionRefusalCode::ChangingRead,
                    )),
                );
            }
            return (entry, None);
        }
        Err(SafeOpenFailure::Symlink) => {
            entry.physical = DeliveryFilePhysicalCode::Symlink;
            if text_requested {
                entry.text_status = DeliveryTextStatus::NotRegularFile;
            }
            return (
                entry,
                Some(InspectionFailure::at(
                    DeliveryInspectionRefusalCode::SymlinkEscape,
                    path,
                )),
            );
        }
        Err(SafeOpenFailure::NotRegularFile) => {
            entry.physical = DeliveryFilePhysicalCode::NotRegularFile;
            if text_requested {
                entry.text_status = DeliveryTextStatus::NotRegularFile;
            }
            return (
                entry,
                Some(InspectionFailure::at(
                    DeliveryInspectionRefusalCode::NotRegularFile,
                    path,
                )),
            );
        }
        Err(SafeOpenFailure::Changed) => {
            if text_requested {
                entry.text_status = DeliveryTextStatus::ChangingRead;
            }
            return (
                entry,
                Some(InspectionFailure::new(
                    DeliveryInspectionRefusalCode::ChangingRead,
                )),
            );
        }
        Err(SafeOpenFailure::Unreadable) => {
            entry.physical = DeliveryFilePhysicalCode::Unreadable;
            if text_requested {
                entry.text_status = DeliveryTextStatus::ChangingRead;
            }
            return (
                entry,
                Some(InspectionFailure::at(
                    DeliveryInspectionRefusalCode::FileReadFailed,
                    path,
                )),
            );
        }
    };

    entry.physical = DeliveryFilePhysicalCode::RegularFile;
    entry.regular_file = true;
    let size = before.len();
    entry.size_bytes = Some(size);
    let observation_index = observations.len();
    observations.push(FileObservation {
        path: path.to_string(),
        evidence: Some(file_evidence(&before)),
    });
    if size > limits.max_file_bytes {
        if text_requested {
            entry.text_status = DeliveryTextStatus::TooLarge;
        }
        return (
            entry,
            Some(InspectionFailure::at(
                DeliveryInspectionRefusalCode::FileSizeLimit,
                path,
            )),
        );
    }
    let next_total = match total_read.checked_add(size) {
        Some(next) if next <= limits.max_total_read_bytes => next,
        _ => {
            if text_requested {
                entry.text_status = DeliveryTextStatus::ReadLimit;
            }
            return (
                entry,
                Some(InspectionFailure::at(
                    DeliveryInspectionRefusalCode::ReadLimit,
                    path,
                )),
            );
        }
    };

    let capacity = match usize::try_from(size) {
        Ok(capacity) => capacity,
        Err(_) => {
            return (
                entry,
                Some(InspectionFailure::at(
                    DeliveryInspectionRefusalCode::FileSizeLimit,
                    path,
                )),
            )
        }
    };
    let mut bytes = Vec::with_capacity(capacity);
    let mut chunk = [0u8; 64 * 1024];
    loop {
        if started.elapsed() >= limits.max_duration {
            if text_requested {
                entry.text_status = DeliveryTextStatus::ChangingRead;
            }
            return (
                entry,
                Some(InspectionFailure::new(
                    DeliveryInspectionRefusalCode::Timeout,
                )),
            );
        }
        let remaining = size
            .saturating_add(1)
            .saturating_sub(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        if remaining == 0 {
            break;
        }
        let want = usize::try_from(remaining.min(chunk.len() as u64)).unwrap_or(chunk.len());
        match file.read(&mut chunk[..want]) {
            Ok(0) => break,
            Ok(read) => bytes.extend_from_slice(&chunk[..read]),
            Err(_) => {
                if text_requested {
                    entry.text_status = DeliveryTextStatus::ChangingRead;
                }
                return (
                    entry,
                    Some(InspectionFailure::at(
                        DeliveryInspectionRefusalCode::FileReadFailed,
                        path,
                    )),
                );
            }
        }
    }
    if bytes.len() as u64 != size {
        if text_requested {
            entry.text_status = DeliveryTextStatus::ChangingRead;
        }
        return (
            entry,
            Some(InspectionFailure::new(
                DeliveryInspectionRefusalCode::ChangingRead,
            )),
        );
    }
    *total_read = next_total;
    let sha256 = sha256_hex(&bytes);
    let utf8 = std::str::from_utf8(&bytes).is_ok();
    entry.sha256 = Some(sha256);
    entry.utf8 = Some(utf8);
    entry.final_lf = Some(bytes.last() == Some(&b'\n'));

    let after_read = match file.metadata() {
        Ok(metadata) => metadata,
        Err(_) => {
            if text_requested {
                entry.text_status = DeliveryTextStatus::ChangingRead;
            }
            return (
                entry,
                Some(InspectionFailure::at(
                    DeliveryInspectionRefusalCode::FileReadFailed,
                    path,
                )),
            );
        }
    };
    let evidence = file_evidence(&after_read);
    if let Some(observation) = observations.get_mut(observation_index) {
        observation.evidence = Some(evidence);
    }
    let stable = after_read.file_type().is_file()
        && same_file_snapshot(&before, &after_read)
        && after_read.len() == size;
    if !stable {
        if text_requested {
            entry.text_status = DeliveryTextStatus::ChangingRead;
        }
        return (
            entry,
            Some(InspectionFailure::new(
                DeliveryInspectionRefusalCode::ChangingRead,
            )),
        );
    }

    if text_requested {
        if !utf8 {
            entry.text_status = DeliveryTextStatus::NotUtf8;
            return (
                entry,
                Some(InspectionFailure::at(
                    DeliveryInspectionRefusalCode::TextFileNotUtf8,
                    path,
                )),
            );
        }
        if size > limits.max_text_file_bytes {
            entry.text_status = DeliveryTextStatus::TooLarge;
            return (
                entry,
                Some(InspectionFailure::at(
                    DeliveryInspectionRefusalCode::TextFileTooLarge,
                    path,
                )),
            );
        }
        let next_text = match total_text.checked_add(size) {
            Some(next) if next <= limits.max_total_text_bytes => next,
            _ => {
                entry.text_status = DeliveryTextStatus::TooLarge;
                return (
                    entry,
                    Some(InspectionFailure::at(
                        DeliveryInspectionRefusalCode::TextFileTooLarge,
                        path,
                    )),
                );
            }
        };
        if let Ok(text) = String::from_utf8(bytes) {
            *total_text = next_text;
            entry.text_status = DeliveryTextStatus::Included;
            entry.text_untrusted = true;
            entry.text = Some(text);
        } else {
            entry.text_status = DeliveryTextStatus::NotUtf8;
            return (
                entry,
                Some(InspectionFailure::at(
                    DeliveryInspectionRefusalCode::TextFileNotUtf8,
                    path,
                )),
            );
        }
    }

    (entry, None)
}

fn expects_present_file(change: &ChangeAccumulator) -> bool {
    change.kinds.iter().any(|kind| {
        matches!(
            kind,
            DeliveryChangeKind::Added
                | DeliveryChangeKind::Modified
                | DeliveryChangeKind::TypeChanged
                | DeliveryChangeKind::Untracked
        )
    }) && !change.kinds.contains(&DeliveryChangeKind::Deleted)
}

#[cfg(unix)]
fn open_inspection_file(root: &Path, path: &str) -> Result<(File, fs::Metadata), SafeOpenFailure> {
    use rustix::fs::{openat, statat, AtFlags, Mode, OFlags};

    const S_IFMT: u32 = 0o170000;
    const S_IFREG: u32 = 0o100000;
    const S_IFDIR: u32 = 0o040000;
    const S_IFLNK: u32 = 0o120000;

    fn stat_failure(err: rustix::io::Errno) -> SafeOpenFailure {
        if err == rustix::io::Errno::NOENT {
            SafeOpenFailure::Missing
        } else {
            SafeOpenFailure::Unreadable
        }
    }

    let root_fd = rustix::fs::open(
        root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| SafeOpenFailure::Unreadable)?;
    let mut dir = root_fd;
    let components: Vec<&str> = path.split('/').collect();
    for (index, component) in components.iter().enumerate() {
        let last = index + 1 == components.len();
        let stat = statat(&dir, *component, AtFlags::SYMLINK_NOFOLLOW).map_err(stat_failure)?;
        let kind = stat.st_mode & S_IFMT;
        if kind == S_IFLNK {
            return Err(SafeOpenFailure::Symlink);
        }
        if last {
            if kind != S_IFREG {
                return Err(SafeOpenFailure::NotRegularFile);
            }
            let fd = openat(
                &dir,
                *component,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|err| {
                if err == rustix::io::Errno::NOENT {
                    SafeOpenFailure::Missing
                } else {
                    SafeOpenFailure::Unreadable
                }
            })?;
            let file = File::from(fd);
            let metadata = file.metadata().map_err(|_| SafeOpenFailure::Unreadable)?;
            if !metadata.file_type().is_file() {
                return Err(SafeOpenFailure::NotRegularFile);
            }
            {
                use std::os::unix::fs::MetadataExt;
                if metadata.dev() != stat.st_dev || metadata.ino() != stat.st_ino {
                    return Err(SafeOpenFailure::Changed);
                }
            }
            return Ok((file, metadata));
        }
        if kind != S_IFDIR {
            return Err(SafeOpenFailure::NotRegularFile);
        }
        dir = openat(
            &dir,
            *component,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(stat_failure)?;
    }
    Err(SafeOpenFailure::Unreadable)
}

#[cfg(not(unix))]
fn open_inspection_file(
    _root: &Path,
    _path: &str,
) -> Result<(File, fs::Metadata), SafeOpenFailure> {
    Err(SafeOpenFailure::Unreadable)
}

fn revalidate_observations(
    root: &Path,
    observations: &[FileObservation],
    limits: &DeliveryInspectionLimits,
    started: Instant,
) -> Result<(), InspectionFailure> {
    check_deadline(started, limits)?;
    for observation in observations {
        check_deadline(started, limits)?;
        match (
            observation.evidence,
            open_inspection_file(root, &observation.path),
        ) {
            (None, Err(SafeOpenFailure::Missing)) => {}
            (Some(expected), Ok((_file, metadata))) => {
                if !metadata.file_type().is_file() || file_evidence(&metadata) != expected {
                    return Err(InspectionFailure::new(
                        DeliveryInspectionRefusalCode::ChangingRead,
                    ));
                }
            }
            _ => {
                return Err(InspectionFailure::new(
                    DeliveryInspectionRefusalCode::ChangingRead,
                ))
            }
        }
        check_deadline(started, limits)?;
    }
    Ok(())
}

/// Clears explicit text whenever the inspection did not complete a stable final pass.
///
/// Any refusal keeps the physical path facts but withholds previously included text so a failed
/// report never carries a stale snippet under an unrelated refusal code.
fn omit_unstable_text(files: &mut [DeliveryFileInspection]) {
    for file in files {
        if file.text.is_some() || file.text_status == DeliveryTextStatus::Included {
            file.text = None;
            file.text_status = DeliveryTextStatus::ChangingRead;
            file.text_untrusted = false;
        }
    }
}

#[cfg(unix)]
fn file_evidence(metadata: &fs::Metadata) -> FileEvidence {
    use std::os::unix::fs::MetadataExt;
    FileEvidence {
        dev: metadata.dev(),
        ino: metadata.ino(),
        len: metadata.len(),
        mtime: metadata.mtime(),
        mtime_nsec: metadata.mtime_nsec(),
        ctime: metadata.ctime(),
        ctime_nsec: metadata.ctime_nsec(),
    }
}

#[cfg(not(unix))]
fn file_evidence(metadata: &fs::Metadata) -> FileEvidence {
    FileEvidence {
        dev: 0,
        ino: 0,
        len: metadata.len(),
        mtime: 0,
        mtime_nsec: 0,
        ctime: 0,
        ctime_nsec: 0,
    }
}

fn run_diff(
    root: &Path,
    cached: bool,
    revisions: &[&str],
    limits: &DeliveryInspectionLimits,
    started: Instant,
) -> Result<Vec<u8>, InspectionFailure> {
    let mut args: Vec<&str> = vec!["diff"];
    if cached {
        args.push("--cached");
    }
    args.extend([
        "--name-status",
        "-z",
        "--find-renames",
        "--no-ext-diff",
        "--no-textconv",
        "--no-color",
    ]);
    args.extend_from_slice(revisions);
    args.push("--");
    run_git_bytes(root, &args, limits, started)
}

fn run_diff_numstat(
    root: &Path,
    revisions: &[&str],
    limits: &DeliveryInspectionLimits,
    started: Instant,
) -> Result<Vec<u8>, InspectionFailure> {
    let mut args: Vec<&str> = vec![
        "diff",
        "--numstat",
        "-z",
        "--find-renames",
        "--no-ext-diff",
        "--no-textconv",
        "--no-color",
    ];
    args.extend_from_slice(revisions);
    args.push("--");
    run_git_bytes(root, &args, limits, started)
}

fn run_ls_files(
    root: &Path,
    ignored: bool,
    limits: &DeliveryInspectionLimits,
    started: Instant,
) -> Result<Vec<u8>, InspectionFailure> {
    let mut args = vec!["ls-files", "-z", "--others", "--exclude-standard"];
    if ignored {
        args.push("--ignored");
    }
    args.push("--");
    run_git_bytes(root, &args, limits, started)
}

fn git_global_args<'a>() -> Vec<&'a str> {
    vec![
        "--no-pager",
        "--no-optional-locks",
        "--no-lazy-fetch",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.untrackedCache=false",
        "-c",
        "core.preloadIndex=false",
        "-c",
        "core.filemode=true",
        "-c",
        "core.symlinks=true",
        "-c",
        "core.ignorecase=false",
        "-c",
        "core.ignoreStat=false",
        "-c",
        "core.trustctime=true",
        "-c",
        "core.checkStat=default",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "diff.autoRefreshIndex=false",
        "-c",
        "gc.auto=0",
    ]
}

fn run_git_bounded(
    root: &Path,
    args: &[&str],
    limits: &DeliveryInspectionLimits,
    started: Instant,
    allow_nonzero: bool,
) -> Result<SupervisedGitOutput, InspectionFailure> {
    check_deadline(started, limits)?;
    let remaining = limits.max_duration.saturating_sub(started.elapsed());
    if remaining.is_zero() {
        return Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::Timeout,
        ));
    }
    let remaining_ms = u64::try_from(remaining.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    let timeout_ms = limits.git_timeout_ms.max(1).min(remaining_ms);
    let mut full_args = git_global_args();
    full_args.extend_from_slice(args);
    let output =
        run_sanitized_git_allow_nonzero(root, &full_args, limits.max_git_output_bytes, timeout_ms)
            .map_err(map_git_error)?;
    check_deadline(started, limits)?;
    if !allow_nonzero && output.exit_code != Some(0) {
        return Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::GitFailed,
        ));
    }
    Ok(output)
}

fn run_git_bytes(
    root: &Path,
    args: &[&str],
    limits: &DeliveryInspectionLimits,
    started: Instant,
) -> Result<Vec<u8>, InspectionFailure> {
    Ok(run_git_bounded(root, args, limits, started, false)?.stdout_bytes)
}

fn resolve_git_commit(
    root: &Path,
    revision: &str,
    limits: &DeliveryInspectionLimits,
    started: Instant,
    allow_unknown: bool,
) -> Result<String, InspectionFailure> {
    let spec = format!("{revision}^{{commit}}");
    let output = run_git_bounded(
        root,
        &["rev-parse", "--verify", "--quiet", &spec],
        limits,
        started,
        true,
    )?;
    if output.exit_code != Some(0) {
        let code = if allow_unknown {
            DeliveryInspectionRefusalCode::UnknownBase
        } else {
            DeliveryInspectionRefusalCode::GitFailed
        };
        return Err(InspectionFailure::new(code));
    }
    let text = std::str::from_utf8(&output.stdout_bytes)
        .map_err(|_| InspectionFailure::new(DeliveryInspectionRefusalCode::MalformedGitOutput))?;
    let sha = text.trim_end_matches(['\n', '\r']).trim();
    validate_exact_40_hex_sha(sha, "resolved commit")
        .map_err(|_| InspectionFailure::new(DeliveryInspectionRefusalCode::MalformedGitOutput))
}

fn is_git_ancestor(
    root: &Path,
    ancestor: &str,
    descendant: &str,
    limits: &DeliveryInspectionLimits,
    started: Instant,
) -> Result<bool, InspectionFailure> {
    let output = run_git_bounded(
        root,
        &["merge-base", "--is-ancestor", "--", ancestor, descendant],
        limits,
        started,
        true,
    )?;
    match output.exit_code {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => Err(InspectionFailure::new(
            DeliveryInspectionRefusalCode::GitFailed,
        )),
    }
}

fn map_git_error(err: WorkerError) -> InspectionFailure {
    match err {
        WorkerError::UnsupportedPlatform(_) => {
            InspectionFailure::new(DeliveryInspectionRefusalCode::UnsupportedPlatform)
        }
        WorkerError::GitCommandFailed { stderr, .. } => {
            let code = if stderr.contains("timed out") {
                DeliveryInspectionRefusalCode::GitTimeout
            } else if stderr.contains("exceeded bounded limit") {
                DeliveryInspectionRefusalCode::GitOutputLimit
            } else {
                DeliveryInspectionRefusalCode::GitFailed
            };
            InspectionFailure::new(code)
        }
        _ => InspectionFailure::new(DeliveryInspectionRefusalCode::GitFailed),
    }
}

fn parse_name_status_z(bytes: &[u8]) -> Result<Vec<RawChange>, InspectionFailure> {
    let mut rest = bytes;
    let mut changes = Vec::new();
    while !rest.is_empty() {
        let (token, after_token) = take_nul(rest).ok_or_else(malformed_git_output)?;
        rest = after_token;
        if token.is_empty() {
            return Err(malformed_git_output());
        }
        let status = token[0];
        if status == b'R' || status == b'C' {
            let (first, after_first) = take_nul(rest).ok_or_else(malformed_git_output)?;
            let (second, _) = take_nul(after_first).ok_or_else(malformed_git_output)?;
            let first = std::str::from_utf8(first)
                .map_err(|_| InspectionFailure::new(DeliveryInspectionRefusalCode::NonUtf8Path))?;
            let second = std::str::from_utf8(second)
                .map_err(|_| InspectionFailure::new(DeliveryInspectionRefusalCode::NonUtf8Path))?;
            let path = if second.is_empty() { first } else { second };
            return Err(InspectionFailure::at(
                DeliveryInspectionRefusalCode::RenameNotAllowed,
                path,
            ));
        }
        if token.len() != 1 {
            return Err(malformed_git_output());
        }
        let (path_bytes, after_path) = take_nul(rest).ok_or_else(malformed_git_output)?;
        rest = after_path;
        if path_bytes.is_empty() {
            return Err(malformed_git_output());
        }
        let path = std::str::from_utf8(path_bytes)
            .map_err(|_| InspectionFailure::new(DeliveryInspectionRefusalCode::NonUtf8Path))?;
        let kind = match status {
            b'A' => DeliveryChangeKind::Added,
            b'M' => DeliveryChangeKind::Modified,
            b'D' => DeliveryChangeKind::Deleted,
            b'T' => DeliveryChangeKind::TypeChanged,
            b'U' => {
                return Err(InspectionFailure::new(
                    DeliveryInspectionRefusalCode::UnmergedChange,
                ))
            }
            _ => return Err(malformed_git_output()),
        };
        changes.push(RawChange {
            kind,
            path: path.to_string(),
        });
    }
    Ok(changes)
}

fn parse_ls_files_z(bytes: &[u8]) -> Result<Vec<String>, InspectionFailure> {
    let mut rest = bytes;
    let mut paths = Vec::new();
    while !rest.is_empty() {
        let (path_bytes, after_path) = take_nul(rest).ok_or_else(malformed_git_output)?;
        rest = after_path;
        if path_bytes.is_empty() {
            return Err(malformed_git_output());
        }
        let path = std::str::from_utf8(path_bytes)
            .map_err(|_| InspectionFailure::new(DeliveryInspectionRefusalCode::NonUtf8Path))?;
        paths.push(path.to_string());
    }
    Ok(paths)
}

fn parse_numstat_paths_z(bytes: &[u8]) -> Result<BTreeSet<String>, InspectionFailure> {
    let mut rest = bytes;
    let mut paths = BTreeSet::new();
    while !rest.is_empty() {
        let (token, after_token) = take_nul(rest).ok_or_else(malformed_git_output)?;
        rest = after_token;
        if token.is_empty() {
            return Err(malformed_git_output());
        }
        let mut fields = token.splitn(3, |byte| *byte == b'\t');
        let added = fields.next().ok_or_else(malformed_git_output)?;
        let deleted = fields.next().ok_or_else(malformed_git_output)?;
        let path_bytes = fields.next().ok_or_else(malformed_git_output)?;
        if !is_numstat_count(added) || !is_numstat_count(deleted) {
            return Err(malformed_git_output());
        }
        if path_bytes.is_empty() {
            let (first, after_first) = take_nul(rest).ok_or_else(malformed_git_output)?;
            let (second, _) = take_nul(after_first).ok_or_else(malformed_git_output)?;
            if first.is_empty() || second.is_empty() {
                return Err(malformed_git_output());
            }
            std::str::from_utf8(first)
                .map_err(|_| InspectionFailure::new(DeliveryInspectionRefusalCode::NonUtf8Path))?;
            let second = std::str::from_utf8(second)
                .map_err(|_| InspectionFailure::new(DeliveryInspectionRefusalCode::NonUtf8Path))?;
            return Err(InspectionFailure::at(
                DeliveryInspectionRefusalCode::RenameNotAllowed,
                second,
            ));
        }
        let path = std::str::from_utf8(path_bytes)
            .map_err(|_| InspectionFailure::new(DeliveryInspectionRefusalCode::NonUtf8Path))?;
        paths.insert(path.to_string());
    }
    Ok(paths)
}

fn is_numstat_count(field: &[u8]) -> bool {
    field == b"-".as_slice() || (!field.is_empty() && field.iter().all(u8::is_ascii_digit))
}

fn take_nul(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    let position = bytes.iter().position(|byte| *byte == 0)?;
    Some((&bytes[..position], &bytes[position + 1..]))
}

fn malformed_git_output() -> InspectionFailure {
    InspectionFailure::new(DeliveryInspectionRefusalCode::MalformedGitOutput)
}

fn record_failure(slot: &mut Option<InspectionFailure>, failure: InspectionFailure) {
    if slot.is_none() {
        *slot = Some(failure);
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let mut hex = String::with_capacity(64);
    for byte in hasher.finalize() {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

#[cfg(unix)]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.len() == right.len() && left.modified().ok() == right.modified().ok()
}

#[cfg(unix)]
fn same_file_snapshot(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    same_file_identity(left, right)
        && left.len() == right.len()
        && left.mtime() == right.mtime()
        && left.mtime_nsec() == right.mtime_nsec()
        && left.ctime() == right.ctime()
        && left.ctime_nsec() == right.ctime_nsec()
}

#[cfg(not(unix))]
fn same_file_snapshot(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    same_file_identity(left, right) && left.modified().ok() == right.modified().ok()
}

fn truncate_utf8_bytes(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn truncate_report_field(text: &str, max_bytes: usize) -> &str {
    truncate_utf8_bytes(text, max_bytes)
}

fn report_within_limit(report: &DeliveryInspectionReport, max_output_bytes: usize) -> bool {
    serde_json::to_vec(report)
        .map(|bytes| bytes.len() <= max_output_bytes)
        .unwrap_or(false)
}

fn enforce_output_limit(
    report: DeliveryInspectionReport,
    max_output_bytes: usize,
) -> DeliveryInspectionReport {
    if report_within_limit(&report, max_output_bytes) {
        return report;
    }

    let refusal_code = report
        .refusal_code
        .unwrap_or(DeliveryInspectionRefusalCode::OutputLimit);
    let code = refusal_code.as_str();
    let build = |root: String,
                 expected_base: String,
                 refusal_path: Option<String>,
                 message: String,
                 output_omitted: bool| DeliveryInspectionReport {
        schema_version: DELIVERY_INSPECTION_SCHEMA_VERSION,
        tool: DELIVERY_INSPECTION_TOOL.to_string(),
        inspection_only: true,
        authority_granted: false,
        ok: false,
        refusal_code: Some(refusal_code),
        refusal_path,
        message,
        root,
        expected_base,
        head_commit: None,
        base_is_ancestor: None,
        changed_paths: Vec::new(),
        files: Vec::new(),
        text_included: false,
        untrusted_data: false,
        output_omitted,
        source: None,
    };

    let bounded = build(
        truncate_report_field(&report.root, 256).to_string(),
        truncate_report_field(&report.expected_base, 80).to_string(),
        report
            .refusal_path
            .as_deref()
            .map(|path| truncate_report_field(path, 256).to_string()),
        format!("physical delivery inspection refused: {code}; detailed evidence omitted"),
        true,
    );
    if report_within_limit(&bounded, max_output_bytes) {
        return bounded;
    }

    let minimal = build(
        String::new(),
        String::new(),
        None,
        format!("physical delivery inspection refused: {code}"),
        true,
    );
    if report_within_limit(&minimal, max_output_bytes) {
        return minimal;
    }

    build(String::new(), String::new(), None, String::new(), true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_status_parser_accepts_regular_entries_and_refuses_rename_and_truncation() {
        let parsed = parse_name_status_z(b"M\0src/lib.rs\0A\0src/new.rs\0")
            .expect("regular name-status output");
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].kind, DeliveryChangeKind::Modified);
        assert_eq!(parsed[0].path, "src/lib.rs");

        let renamed =
            parse_name_status_z(b"R100\0old.rs\0new.rs\0").expect_err("rename must be refused");
        assert_eq!(
            renamed.code,
            DeliveryInspectionRefusalCode::RenameNotAllowed
        );

        let truncated = parse_name_status_z(b"M\0src/lib.rs").expect_err("truncated output");
        assert_eq!(
            truncated.code,
            DeliveryInspectionRefusalCode::MalformedGitOutput
        );
    }

    #[test]
    fn ls_files_parser_accepts_nul_paths_and_refuses_truncation() {
        let parsed = parse_ls_files_z(b"a.txt\0dir/b.txt\0").expect("regular ls-files output");
        assert_eq!(parsed, vec!["a.txt".to_string(), "dir/b.txt".to_string()]);
        let truncated = parse_ls_files_z(b"a.txt").expect_err("truncated ls-files output");
        assert_eq!(
            truncated.code,
            DeliveryInspectionRefusalCode::MalformedGitOutput
        );
    }

    #[test]
    fn numstat_parser_keeps_zero_counts_and_refuses_malformed_or_renamed_records() {
        let parsed = parse_numstat_paths_z(
            b"0\t0\tsrc/mode-only.rs\x00-\t-\tsrc/binary.bin\x001\t2\tsrc/lib.rs\x00",
        )
        .expect("regular numstat output");
        assert!(parsed.contains("src/mode-only.rs"));
        assert!(parsed.contains("src/binary.bin"));
        assert!(parsed.contains("src/lib.rs"));

        let truncated = parse_numstat_paths_z(b"1\t2\tsrc/lib.rs").expect_err("truncated");
        assert_eq!(
            truncated.code,
            DeliveryInspectionRefusalCode::MalformedGitOutput
        );
        let renamed =
            parse_numstat_paths_z(b"1\t2\t\0src/old.rs\0src/new.rs\0").expect_err("rename");
        assert_eq!(
            renamed.code,
            DeliveryInspectionRefusalCode::RenameNotAllowed
        );
    }

    #[test]
    fn index_flag_parser_accepts_normal_entries_and_refuses_suppressed_flags() {
        parse_index_flags_z(b"H src/lib.rs\0H .gitignore\0").expect("ordinary index");
        for entry in [
            b"h src/lib.rs\0".as_slice(),
            b"S src/lib.rs\0".as_slice(),
            b"s src/lib.rs\0".as_slice(),
        ] {
            let failure = parse_index_flags_z(entry).expect_err("suppressed index flag");
            assert_eq!(
                failure.code,
                DeliveryInspectionRefusalCode::IndexFlagUnsupported
            );
        }
        let unmerged = parse_index_flags_z(b"M src/lib.rs\0").expect_err("unmerged entry");
        assert_eq!(unmerged.code, DeliveryInspectionRefusalCode::UnmergedChange);
        let malformed = parse_index_flags_z(b"Hsrc/lib.rs\0").expect_err("malformed entry");
        assert_eq!(
            malformed.code,
            DeliveryInspectionRefusalCode::MalformedGitOutput
        );
    }

    #[test]
    fn git_global_args_keep_network_hook_and_index_safeguards() {
        let args = git_global_args();
        for required in [
            "--no-optional-locks",
            "--no-lazy-fetch",
            "core.fsmonitor=false",
            "core.filemode=true",
            "core.symlinks=true",
            "core.ignorecase=false",
            "core.ignoreStat=false",
            "core.hooksPath=/dev/null",
            "diff.autoRefreshIndex=false",
        ] {
            assert!(
                args.contains(&required),
                "missing hardened Git safeguard `{required}`"
            );
        }
    }

    #[test]
    fn revalidation_detects_an_earlier_file_changing_after_later_reads() {
        let root = tempfile::tempdir().expect("tempdir");
        let first_path = root.path().join("first.txt");
        let second_path = root.path().join("second.txt");
        fs::write(&first_path, "first contents\n").expect("write first");
        fs::write(&second_path, "second contents\n").expect("write second");

        let mut observations = Vec::new();
        for path in ["first.txt", "second.txt"] {
            let (_file, metadata) =
                open_inspection_file(root.path(), path).expect("safe open fixture");
            observations.push(FileObservation {
                path: path.to_string(),
                evidence: Some(file_evidence(&metadata)),
            });
        }

        fs::write(&first_path, "first contents replaced after later reads\n")
            .expect("replace first after evidence capture");
        let limits = DeliveryInspectionLimits::default();
        let failure = revalidate_observations(root.path(), &observations, &limits, Instant::now())
            .expect_err("changed earlier file must fail final evidence revalidation");
        assert_eq!(failure.code, DeliveryInspectionRefusalCode::ChangingRead);
    }

    #[test]
    fn final_revalidation_task_honors_the_shared_remaining_deadline() {
        let root = tempfile::tempdir().expect("tempdir");
        fs::write(root.path().join("first.txt"), "first contents\n").expect("write first");
        let (_file, metadata) =
            open_inspection_file(root.path(), "first.txt").expect("safe open fixture");
        let observations = vec![FileObservation {
            path: "first.txt".to_string(),
            evidence: Some(file_evidence(&metadata)),
        }];
        let limits = DeliveryInspectionLimits {
            max_duration: Duration::ZERO,
            ..DeliveryInspectionLimits::default()
        };
        let failure = revalidate_observations(root.path(), &observations, &limits, Instant::now())
            .expect_err("expired final revalidation must refuse");
        assert_eq!(failure.code, DeliveryInspectionRefusalCode::Timeout);
    }

    #[test]
    fn unstable_evidence_omits_previously_included_text() {
        let mut files = vec![DeliveryFileInspection {
            path: "src/lib.rs".to_string(),
            changed: true,
            change_kinds: vec![DeliveryChangeKind::Modified],
            physical: DeliveryFilePhysicalCode::RegularFile,
            regular_file: true,
            utf8: Some(true),
            final_lf: Some(true),
            size_bytes: Some(4),
            sha256: Some("0".repeat(64)),
            text_status: DeliveryTextStatus::Included,
            text: Some("text".to_string()),
            text_untrusted: true,
        }];
        omit_unstable_text(&mut files);
        assert!(files[0].text.is_none());
        assert!(!files[0].text_untrusted);
        assert_eq!(files[0].text_status, DeliveryTextStatus::ChangingRead);
    }
}
