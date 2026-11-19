use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sha2::{Digest, Sha256};
use tempfile::TempDir;
use threadlane::delivery_inspection::{
    inspect_delivery, inspect_delivery_with_limits, inspect_delivery_with_source,
    inspect_delivery_with_source_and_limits, inspect_delivery_with_source_text,
    inspect_delivery_with_source_text_and_limits, inspect_source_baseline,
    inspect_source_baseline_with_limits, DeliveryChangeKind, DeliveryFileInspection,
    DeliveryFilePhysicalCode, DeliveryInspectionLimits, DeliveryInspectionRefusalCode,
    DeliveryInspectionReport, DeliveryInspectionRequest, DeliveryTextStatus,
};
use threadlane_workers::{
    run_sanitized_git, run_sanitized_git_bytes, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};

struct Fixture {
    _tmp: TempDir,
    root: PathBuf,
    base: String,
}

impl Fixture {
    fn new() -> Self {
        let tmp = TempDir::new().expect("tempdir");
        let tmp_root = fs::canonicalize(tmp.path()).expect("canonical temp root");
        let root = tmp_root.join("repo");
        fs::create_dir_all(root.join("src")).expect("create repo src");
        fs::write(root.join("src/lib.rs"), "pub fn alpha() -> u32 { 1 }\n").expect("write lib");
        fs::write(root.join("src/unchanged.rs"), "pub fn unchanged() {}\n")
            .expect("write unchanged");
        fs::write(root.join(".gitignore"), "ignored.log\n").expect("write gitignore");
        git(&root, &["init", "--quiet"]);
        git(&root, &["config", "user.email", "test@threadlane.local"]);
        git(&root, &["config", "user.name", "Threadlane Test"]);
        git(&root, &["config", "core.autocrlf", "false"]);
        git(&root, &["add", "."]);
        git(&root, &["commit", "--quiet", "-m", "initial"]);
        let base = git(&root, &["rev-parse", "HEAD"]).trim().to_string();
        Self {
            _tmp: tmp,
            root,
            base,
        }
    }

    fn request(&self, allowed: &[&str], text: &[&str]) -> DeliveryInspectionRequest {
        DeliveryInspectionRequest {
            root: self.root.clone(),
            expected_base: self.base.clone(),
            allowed_paths: allowed.iter().map(|path| (*path).to_string()).collect(),
            text_files: text.iter().map(|path| (*path).to_string()).collect(),
        }
    }

    fn write(&self, path: &str, contents: impl AsRef<[u8]>) -> PathBuf {
        let full = self.root.join(path);
        if let Some(parent) = full.parent() {
            fs::create_dir_all(parent).expect("create parent");
        }
        fs::write(&full, contents).expect("write fixture file");
        full
    }

    fn commit_all(&self, message: &str) -> String {
        git(&self.root, &["add", "-A"]);
        git(&self.root, &["commit", "--quiet", "-m", message]);
        git(&self.root, &["rev-parse", "HEAD"]).trim().to_string()
    }
}

fn git(root: &Path, args: &[&str]) -> String {
    run_sanitized_git(root, args, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .unwrap_or_else(|err| panic!("git {args:?} failed: {err}"))
}

fn git_status(root: &Path) -> Vec<u8> {
    run_sanitized_git_bytes(
        root,
        &[
            "--no-optional-locks",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.untrackedCache=false",
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--ignored=matching",
        ],
        1024 * 1024,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("bounded git status")
    .stdout_bytes
}

fn file<'a>(report: &'a DeliveryInspectionReport, path: &str) -> &'a DeliveryFileInspection {
    report
        .files
        .iter()
        .find(|file| file.path == path)
        .unwrap_or_else(|| panic!("missing file report for {path}"))
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

fn add_executable_bits(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(path).expect("read permissions").permissions();
    permissions.set_mode(permissions.mode() | 0o111);
    fs::set_permissions(path, permissions).expect("set executable bits");
}

#[test]
fn source_evidence_compares_named_bytes_and_reports_dirty_and_stale_roots() {
    let fixture = Fixture::new();
    let worker_root = fixture._tmp.path().join("worker");
    let worker_arg = worker_root.to_str().unwrap();
    git(
        &fixture.root,
        &[
            "worktree",
            "add",
            "--quiet",
            "--detach",
            worker_arg,
            &fixture.base,
        ],
    );
    fs::write(worker_root.join("src/lib.rs"), "worker delivery\n").unwrap();
    let mut request = fixture.request(&["src/lib.rs"], &["src/lib.rs"]);
    request.root = worker_root;
    let source_files = vec!["src/lib.rs".to_string(), "src/unchanged.rs".to_string()];

    let report = inspect_delivery_with_source(&request, &fixture.root, &source_files);
    assert!(report.ok, "task evidence: {report:?}");
    let source = report.source.unwrap();
    assert!(
        source.ok && source.clean_at_base,
        "source evidence: {source:?}"
    );
    assert_eq!(source.head_commit.as_deref(), Some(fixture.base.as_str()));
    assert_eq!(
        source.base_tree.as_deref(),
        Some(git(&fixture.root, &["rev-parse", "HEAD^{tree}"]).trim())
    );
    assert!(source.changed_paths.is_empty());
    assert_eq!(source.files.len(), 2);
    assert_eq!(
        source.files[0].sha256,
        sha256_hex(b"pub fn alpha() -> u32 { 1 }\n")
    );
    assert!(source
        .files
        .iter()
        .all(|file| file.matches_base && file.sha256 == file.base_sha256));

    fixture.write("src/unchanged.rs", b"changed after start\n");
    fixture.write("ignored.log", b"ignored addition");
    let changed = inspect_delivery_with_source(&request, &fixture.root, &source_files)
        .source
        .unwrap();
    assert!(changed.ok && !changed.clean_at_base);
    assert!(!changed.files[1].matches_base);
    assert_eq!(
        changed.files[1].sha256,
        sha256_hex(b"changed after start\n")
    );
    assert!(changed
        .changed_paths
        .iter()
        .any(|entry| entry.path == "src/unchanged.rs"));
    assert!(changed
        .changed_paths
        .iter()
        .any(|entry| entry.path == "ignored.log" && entry.ignored));

    fixture.commit_all("source head moved");
    let stale = inspect_delivery_with_source(&request, &fixture.root, &source_files)
        .source
        .unwrap();
    assert!(stale.ok && !stale.clean_at_base);
    assert_ne!(stale.head_commit.as_deref(), Some(fixture.base.as_str()));
    assert!(!stale.files[1].matches_base);

    let same = inspect_delivery_with_source(&request, &request.root, &source_files);
    assert!(!same.ok);
    let same = same.source.unwrap();
    assert!(!same.ok);
    assert_eq!(
        same.refusal_code,
        Some(DeliveryInspectionRefusalCode::InvalidRoot)
    );
    assert!(!same.clean_at_base && same.files.is_empty());
}

#[test]
fn source_and_task_share_the_total_read_budget() {
    let fixture = Fixture::new();
    let worker_root = fixture._tmp.path().join("worker");
    git(
        &fixture.root,
        &[
            "worktree",
            "add",
            "--quiet",
            "--detach",
            worker_root.to_str().unwrap(),
            &fixture.base,
        ],
    );
    let task_text = b"delivered\n";
    fs::write(worker_root.join("src/lib.rs"), task_text).unwrap();
    let mut request = fixture.request(&["src/lib.rs"], &["src/lib.rs"]);
    request.root = worker_root;
    let source_size = fs::metadata(fixture.root.join("src/unchanged.rs"))
        .unwrap()
        .len();
    let limits = DeliveryInspectionLimits {
        max_total_read_bytes: task_text.len() as u64 + source_size * 2 - 1,
        ..DeliveryInspectionLimits::default()
    };
    let report = inspect_delivery_with_source_and_limits(
        &request,
        &fixture.root,
        &["src/unchanged.rs".to_string()],
        &limits,
    );
    assert!(!report.ok);
    let source = report.source.unwrap();
    assert!(!source.ok && !source.clean_at_base && source.files.is_empty());
    assert_eq!(
        source.refusal_code,
        Some(DeliveryInspectionRefusalCode::ReadLimit)
    );
}

#[test]
fn source_baseline_captures_named_hashes_before_task_checkout_without_text() {
    let fixture = Fixture::new();
    let names = vec!["src/lib.rs".to_string(), "src/unchanged.rs".to_string()];
    let baseline = inspect_source_baseline(&fixture.root, &fixture.base, &names);
    assert!(baseline.ok && baseline.clean_at_base, "{baseline:?}");
    assert_eq!(baseline.head_commit.as_deref(), Some(fixture.base.as_str()));
    assert_eq!(
        baseline.base_tree.as_deref(),
        Some(git(&fixture.root, &["rev-parse", "HEAD^{tree}"]).trim())
    );
    assert!(baseline.changed_paths.is_empty());
    assert_eq!(baseline.files.len(), 2);
    let first = &baseline.files[0];
    let contents = "pub fn alpha() -> u32 { 1 }\n";
    assert_eq!(first.path, names[0]);
    assert_eq!(first.sha256, sha256_hex(contents.as_bytes()));
    assert_eq!(first.base_sha256, first.sha256);
    assert!(first.matches_base);
    assert_eq!(first.size_bytes, Some(contents.len() as u64));
    assert!(baseline.files.iter().all(|file| {
        file.text.is_none() && file.text_untrusted.is_none() && file.final_lf.is_none()
    }));
    assert!(!serde_json::to_string(&baseline)
        .unwrap()
        .contains("pub fn alpha"));

    // A later delivery observation can compare its text and same-read hash to the baseline.
    let worker_root = fixture.root.parent().unwrap().join("worker");
    git(
        &fixture.root,
        &[
            "worktree",
            "add",
            "--quiet",
            "--detach",
            worker_root.to_str().unwrap(),
            &fixture.base,
        ],
    );
    fs::write(worker_root.join("src/lib.rs"), "delivery\n").unwrap();
    let mut request = fixture.request(&["src/lib.rs"], &[]);
    request.root = worker_root;
    let later = inspect_delivery_with_source_text(&request, &fixture.root, &names);
    assert!(later.ok, "{later:?}");
    let later = later.source.unwrap();
    assert!(later.clean_at_base);
    for (before, after) in baseline.files.iter().zip(&later.files) {
        assert_eq!(before.sha256, after.sha256);
        assert_eq!(before.base_sha256, after.base_sha256);
        assert_eq!(before.size_bytes, after.size_bytes);
        assert!(after.matches_base);
        assert_eq!(after.text_untrusted, Some(true));
        assert_eq!(
            after.sha256,
            sha256_hex(after.text.as_ref().unwrap().as_bytes())
        );
    }
}

#[test]
fn source_baseline_reports_changed_and_stale_sources_as_unclean() {
    let fixture = Fixture::new();
    let names = vec!["src/lib.rs".to_string()];
    let before = inspect_source_baseline(&fixture.root, &fixture.base, &names);
    assert!(before.ok && before.clean_at_base);
    fixture.write("src/lib.rs", "different source\n");
    fixture.write("ignored.log", "ignored");
    let changed = inspect_source_baseline(&fixture.root, &fixture.base, &names);
    assert!(changed.ok && !changed.clean_at_base, "{changed:?}");
    assert!(!changed.files[0].matches_base);
    assert_ne!(changed.files[0].sha256, before.files[0].sha256);
    assert_eq!(changed.files[0].base_sha256, before.files[0].base_sha256);
    assert!(changed
        .changed_paths
        .iter()
        .any(|path| path.path == "src/lib.rs"));
    assert!(changed
        .changed_paths
        .iter()
        .any(|path| path.path == "ignored.log" && path.ignored));

    fixture.commit_all("source head moved");
    let stale = inspect_source_baseline(&fixture.root, &fixture.base, &names);
    assert!(stale.ok && !stale.clean_at_base, "{stale:?}");
    assert_ne!(stale.head_commit.as_deref(), Some(fixture.base.as_str()));
    assert!(!stale.files[0].matches_base);
}

#[test]
fn source_baseline_fails_closed_on_invalid_shape_files_flags_and_limits() {
    let fixture = Fixture::new();
    let names = vec!["src/lib.rs".to_string()];
    let unknown_base = "0".repeat(40);
    let cases: Vec<(&Path, &str, Vec<String>, DeliveryInspectionRefusalCode)> = vec![
        (
            &fixture.root,
            "invalid",
            names.clone(),
            DeliveryInspectionRefusalCode::InvalidExpectedBase,
        ),
        (
            Path::new("relative"),
            &fixture.base,
            names.clone(),
            DeliveryInspectionRefusalCode::InvalidRoot,
        ),
        (
            &fixture.root,
            &fixture.base,
            vec![],
            DeliveryInspectionRefusalCode::InvalidRequest,
        ),
        (
            &fixture.root,
            &fixture.base,
            vec!["../outside".into()],
            DeliveryInspectionRefusalCode::UnsafePath,
        ),
        (
            &fixture.root,
            &fixture.base,
            vec![".git/config".into()],
            DeliveryInspectionRefusalCode::ProtectedPath,
        ),
        (
            &fixture.root,
            &fixture.base,
            vec![names[0].clone(), names[0].clone()],
            DeliveryInspectionRefusalCode::DuplicatePath,
        ),
        (
            &fixture.root,
            &fixture.base,
            vec!["src/missing.rs".into()],
            DeliveryInspectionRefusalCode::FileReadFailed,
        ),
        (
            &fixture.root,
            &unknown_base,
            names.clone(),
            DeliveryInspectionRefusalCode::UnknownBase,
        ),
    ];
    for (root, base, paths, code) in cases {
        let report = inspect_source_baseline(root, base, &paths);
        assert!(
            !report.ok && !report.clean_at_base && report.files.is_empty(),
            "{report:?}"
        );
        assert_eq!(report.refusal_code, Some(code), "{report:?}");
    }

    let small_file = DeliveryInspectionLimits {
        max_file_bytes: 1,
        ..DeliveryInspectionLimits::default()
    };
    let report =
        inspect_source_baseline_with_limits(&fixture.root, &fixture.base, &names, &small_file);
    assert_eq!(
        report.refusal_code,
        Some(DeliveryInspectionRefusalCode::FileSizeLimit)
    );
    assert!(!report.ok && report.files.is_empty());
    let source_size = fs::metadata(fixture.root.join(&names[0])).unwrap().len();
    let read_limit = DeliveryInspectionLimits {
        max_total_read_bytes: source_size * 2 - 1,
        ..DeliveryInspectionLimits::default()
    };
    let report =
        inspect_source_baseline_with_limits(&fixture.root, &fixture.base, &names, &read_limit);
    assert_eq!(
        report.refusal_code,
        Some(DeliveryInspectionRefusalCode::ReadLimit)
    );
    assert!(!report.ok && report.files.is_empty());
    let output_limit = DeliveryInspectionLimits {
        max_output_bytes: 300,
        ..DeliveryInspectionLimits::default()
    };
    let report =
        inspect_source_baseline_with_limits(&fixture.root, &fixture.base, &names, &output_limit);
    assert_eq!(
        report.refusal_code,
        Some(DeliveryInspectionRefusalCode::OutputLimit)
    );
    assert!(!report.ok && report.files.is_empty());
    assert!(serde_json::to_vec(&report).unwrap().len() <= output_limit.max_output_bytes);

    git(
        &fixture.root,
        &["update-index", "--skip-worktree", &names[0]],
    );
    let report = inspect_source_baseline(&fixture.root, &fixture.base, &names);
    assert_eq!(
        report.refusal_code,
        Some(DeliveryInspectionRefusalCode::IndexFlagUnsupported)
    );
    assert!(!report.ok && report.files.is_empty());
}

#[cfg(unix)]
#[test]
fn source_baseline_refuses_symlinks() {
    use std::os::unix::fs::symlink;
    let fixture = Fixture::new();
    fs::remove_file(fixture.root.join("src/lib.rs")).unwrap();
    symlink("unchanged.rs", fixture.root.join("src/lib.rs")).unwrap();
    let report = inspect_source_baseline(&fixture.root, &fixture.base, &["src/lib.rs".into()]);
    assert_eq!(
        report.refusal_code,
        Some(DeliveryInspectionRefusalCode::SymlinkEscape)
    );
    assert!(!report.ok && report.files.is_empty());
}

fn source_text_fixture() -> (Fixture, DeliveryInspectionRequest) {
    let fixture = Fixture::new();
    let worker_root = fixture.root.parent().unwrap().join("worker");
    git(
        &fixture.root,
        &[
            "worktree",
            "add",
            "--quiet",
            "--detach",
            worker_root.to_str().unwrap(),
            &fixture.base,
        ],
    );
    fs::write(worker_root.join("src/lib.rs"), "ok\n").unwrap();
    let mut request = fixture.request(&["src/lib.rs"], &["src/lib.rs"]);
    request.root = worker_root;
    (fixture, request)
}

fn assert_source_text_refused(
    report: &DeliveryInspectionReport,
    code: DeliveryInspectionRefusalCode,
) {
    assert!(!report.ok, "expected source refusal: {report:?}");
    assert_eq!(report.refusal_code, Some(code), "{report:?}");
    let source = report.source.as_ref().expect("source refusal evidence");
    assert!(!source.ok && !source.clean_at_base && source.files.is_empty());
    assert!(!report.text_included && !report.untrusted_data);
    assert!(report.files.iter().all(|file| file.text.is_none()));
}

#[test]
fn source_text_is_opt_in_exact_and_untrusted_and_old_json_is_unchanged() {
    let (fixture, mut request) = source_text_fixture();
    request.text_files.clear();
    let names = vec!["src/lib.rs".to_string()];
    let old = inspect_delivery_with_source(&request, &fixture.root, &names);
    assert!(old.ok && !old.text_included && !old.untrusted_data);
    let old_json = serde_json::to_value(&old).unwrap();
    let old_file = &old_json["source"]["files"][0];
    for field in ["text", "size_bytes", "final_lf", "text_untrusted"] {
        assert!(
            old_file.get(field).is_none(),
            "unexpected old field {field}"
        );
    }

    let report = inspect_delivery_with_source_text(&request, &fixture.root, &names);
    assert!(report.ok && report.text_included && report.untrusted_data);
    let source = report.source.unwrap();
    assert!(source.ok && source.clean_at_base);
    assert_eq!(source.files.len(), 1);
    let file = &source.files[0];
    let expected = "pub fn alpha() -> u32 { 1 }\n";
    assert_eq!(file.text.as_deref(), Some(expected));
    assert_eq!(file.sha256, sha256_hex(expected.as_bytes()));
    assert_eq!(file.base_sha256, file.sha256);
    assert!(file.matches_base);
    assert_eq!(file.size_bytes, Some(expected.len() as u64));
    assert_eq!(file.final_lf, Some(true));
    assert_eq!(file.text_untrusted, Some(true));
}

#[test]
fn source_text_preserves_dirty_and_stale_source_facts_and_exact_utf8() {
    let (fixture, request) = source_text_fixture();
    let names = vec!["src/lib.rs".to_string()];
    let contents = "özel içerik without final newline";
    fixture.write("src/lib.rs", contents);
    let report = inspect_delivery_with_source_text(&request, &fixture.root, &names);
    assert!(report.ok, "{report:?}");
    let source = report.source.unwrap();
    assert!(!source.clean_at_base && source.head_commit.as_deref() == Some(fixture.base.as_str()));
    let file = &source.files[0];
    assert!(!file.matches_base);
    assert_eq!(file.text.as_deref(), Some(contents));
    assert_eq!(file.sha256, sha256_hex(contents.as_bytes()));
    assert_eq!(file.size_bytes, Some(contents.len() as u64));
    assert_eq!(file.final_lf, Some(false));

    fixture.commit_all("source head moved");
    let report = inspect_delivery_with_source_text(&request, &fixture.root, &names);
    assert!(report.ok, "{report:?}");
    let source = report.source.unwrap();
    assert!(!source.clean_at_base);
    assert_ne!(source.head_commit.as_deref(), Some(fixture.base.as_str()));
    assert!(!source.files[0].matches_base);
}

#[test]
fn source_text_refusals_withhold_all_task_and_partial_source_text() {
    let (fixture, request) = source_text_fixture();
    let names = vec!["src/unchanged.rs".to_string(), "src/lib.rs".to_string()];

    fixture.write("src/lib.rs", [0xff, 0xfe]);
    let report = inspect_delivery_with_source_text(&request, &fixture.root, &names);
    assert_source_text_refused(&report, DeliveryInspectionRefusalCode::TextFileNotUtf8);

    fs::remove_file(fixture.root.join("src/lib.rs")).unwrap();
    let report = inspect_delivery_with_source_text(&request, &fixture.root, &names);
    assert_source_text_refused(&report, DeliveryInspectionRefusalCode::TextFileMissing);

    fixture.write("src/lib.rs", "original\n");
    let duplicate = vec!["src/lib.rs".to_string(), "src/lib.rs".to_string()];
    let report = inspect_delivery_with_source_text(&request, &fixture.root, &duplicate);
    assert_source_text_refused(&report, DeliveryInspectionRefusalCode::DuplicatePath);
    fixture.write("src/new.rs", "not present at the frozen base\n");
    let untracked = vec!["src/unchanged.rs".to_string(), "src/new.rs".to_string()];
    let report = inspect_delivery_with_source_text(&request, &fixture.root, &untracked);
    assert_source_text_refused(&report, DeliveryInspectionRefusalCode::FileReadFailed);
    let protected = vec![".git/config".to_string()];
    let report = inspect_delivery_with_source_text(&request, &fixture.root, &protected);
    assert_source_text_refused(&report, DeliveryInspectionRefusalCode::ProtectedPath);

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        fs::remove_file(fixture.root.join("src/lib.rs")).unwrap();
        symlink("unchanged.rs", fixture.root.join("src/lib.rs")).unwrap();
        let report = inspect_delivery_with_source_text(&request, &fixture.root, &names);
        assert_source_text_refused(&report, DeliveryInspectionRefusalCode::SymlinkEscape);
    }
}

#[test]
fn source_text_enforces_count_and_shares_text_read_and_output_budgets() {
    let (fixture, request) = source_text_fixture();
    let names = vec!["src/lib.rs".to_string(), "src/unchanged.rs".to_string()];
    let count_limits = DeliveryInspectionLimits {
        max_paths: 1,
        ..DeliveryInspectionLimits::default()
    };
    let report = inspect_delivery_with_source_text_and_limits(
        &request,
        &fixture.root,
        &names,
        &count_limits,
    );
    assert_source_text_refused(&report, DeliveryInspectionRefusalCode::TooManyPaths);

    let file_limits = DeliveryInspectionLimits {
        max_text_file_bytes: 8,
        ..DeliveryInspectionLimits::default()
    };
    let report = inspect_delivery_with_source_text_and_limits(
        &request,
        &fixture.root,
        &names[..1],
        &file_limits,
    );
    assert_source_text_refused(&report, DeliveryInspectionRefusalCode::TextFileTooLarge);

    let source_size = fs::metadata(fixture.root.join("src/lib.rs")).unwrap().len();
    let total_limits = DeliveryInspectionLimits {
        max_total_text_bytes: 3 + source_size - 1,
        ..DeliveryInspectionLimits::default()
    };
    let report = inspect_delivery_with_source_text_and_limits(
        &request,
        &fixture.root,
        &names[..1],
        &total_limits,
    );
    assert_source_text_refused(&report, DeliveryInspectionRefusalCode::TextFileTooLarge);

    let read_limits = DeliveryInspectionLimits {
        max_total_read_bytes: 3 + source_size * 2 - 1,
        ..DeliveryInspectionLimits::default()
    };
    let report = inspect_delivery_with_source_text_and_limits(
        &request,
        &fixture.root,
        &names[..1],
        &read_limits,
    );
    assert_source_text_refused(&report, DeliveryInspectionRefusalCode::ReadLimit);

    // The task report alone fits, but the source text makes the combined JSON exceed the cap.
    let task_size = serde_json::to_vec(&inspect_delivery(&request))
        .unwrap()
        .len();
    let output_limits = DeliveryInspectionLimits {
        max_output_bytes: task_size + 1,
        ..DeliveryInspectionLimits::default()
    };
    let report = inspect_delivery_with_source_text_and_limits(
        &request,
        &fixture.root,
        &names[..1],
        &output_limits,
    );
    assert_eq!(
        report.refusal_code,
        Some(DeliveryInspectionRefusalCode::OutputLimit)
    );
    assert!(!report.ok && report.output_omitted && report.source.is_none());
    assert!(!report.text_included && !report.untrusted_data);
    assert!(report.files.is_empty());
}

#[test]
fn source_text_refuses_index_flags_before_exposing_text() {
    for flag in ["--assume-unchanged", "--skip-worktree"] {
        let (fixture, request) = source_text_fixture();
        git(&fixture.root, &["update-index", flag, "src/lib.rs"]);
        let names = vec!["src/lib.rs".to_string()];
        let report = inspect_delivery_with_source_text(&request, &fixture.root, &names);
        assert_source_text_refused(&report, DeliveryInspectionRefusalCode::IndexFlagUnsupported);
    }
}

#[test]
fn reports_actual_metadata_and_only_explicit_untrusted_text() {
    let fixture = Fixture::new();
    let lib_contents = "pub fn alpha() -> u32 { 2 }\n";
    let new_contents = "pub fn new_helper() -> bool { true }\n";
    fixture.write("src/lib.rs", lib_contents);
    fixture.write("src/new.rs", new_contents);

    let request = fixture.request(
        &[
            "src/lib.rs",
            "src/new.rs",
            "src/unchanged.rs",
            "src/missing.rs",
        ],
        &["src/new.rs"],
    );
    let report = inspect_delivery(&request);
    assert!(report.ok, "unexpected refusal: {report:?}");
    assert!(report.inspection_only);
    assert!(!report.authority_granted);
    assert_eq!(report.head_commit.as_deref(), Some(fixture.base.as_str()));
    assert_eq!(report.changed_paths.len(), 2);
    let lib_change = report
        .changed_paths
        .iter()
        .find(|change| change.path == "src/lib.rs")
        .expect("lib change");
    assert!(lib_change.allowed);
    assert!(!lib_change.staged);
    assert!(lib_change.unstaged);
    assert!(lib_change
        .change_kinds
        .contains(&DeliveryChangeKind::Modified));
    let new_change = report
        .changed_paths
        .iter()
        .find(|change| change.path == "src/new.rs")
        .expect("new change");
    assert!(new_change.untracked);
    assert!(new_change
        .change_kinds
        .contains(&DeliveryChangeKind::Untracked));

    let lib = file(&report, "src/lib.rs");
    assert!(lib.changed);
    assert_eq!(lib.physical, DeliveryFilePhysicalCode::RegularFile);
    assert!(lib.regular_file);
    assert_eq!(lib.utf8, Some(true));
    assert_eq!(lib.final_lf, Some(true));
    assert_eq!(lib.size_bytes, Some(lib_contents.len() as u64));
    assert_eq!(lib.sha256, Some(sha256_hex(lib_contents.as_bytes())));
    assert_eq!(lib.text_status, DeliveryTextStatus::NotRequested);
    assert!(lib.text.is_none());

    let new = file(&report, "src/new.rs");
    assert!(new.changed);
    assert_eq!(new.physical, DeliveryFilePhysicalCode::RegularFile);
    assert_eq!(new.text_status, DeliveryTextStatus::Included);
    assert_eq!(new.text.as_deref(), Some(new_contents));
    assert!(new.text_untrusted);
    assert!(report.text_included);
    assert!(report.untrusted_data);

    let unchanged = file(&report, "src/unchanged.rs");
    assert!(!unchanged.changed);
    assert_eq!(unchanged.physical, DeliveryFilePhysicalCode::RegularFile);
    assert!(unchanged.regular_file);
    assert!(unchanged.text.is_none());
    let missing = file(&report, "src/missing.rs");
    assert!(!missing.changed);
    assert_eq!(missing.physical, DeliveryFilePhysicalCode::Missing);
    assert!(!missing.regular_file);
    assert!(missing.sha256.is_none());

    let default_request = fixture.request(
        &[
            "src/lib.rs",
            "src/new.rs",
            "src/unchanged.rs",
            "src/missing.rs",
        ],
        &[],
    );
    let default_report = inspect_delivery(&default_request);
    assert!(default_report.ok, "unexpected refusal: {default_report:?}");
    assert!(!default_report.text_included);
    assert!(!default_report.untrusted_data);
    assert!(default_report
        .files
        .iter()
        .all(|file| file.text.is_none() && file.text_status == DeliveryTextStatus::NotRequested));
    let serialized = serde_json::to_string(&default_report).expect("serialize default report");
    assert!(!serialized.contains(new_contents));
    assert!(!serialized.contains(lib_contents));
    assert!(!serialized.contains("pub fn unchanged"));
}

#[test]
fn refuses_unknown_base_and_wrong_head() {
    let fixture = Fixture::new();
    let mut unknown = fixture.request(&["src/lib.rs"], &[]);
    unknown.expected_base = "0".repeat(40);
    let unknown_report = inspect_delivery(&unknown);
    assert_eq!(
        unknown_report.refusal_code,
        Some(DeliveryInspectionRefusalCode::UnknownBase)
    );

    git(&fixture.root, &["checkout", "--quiet", "-b", "sibling"]);
    fixture.write("src/sibling.rs", "pub fn sibling() {}\n");
    let sibling_base = fixture.commit_all("sibling");
    git(
        &fixture.root,
        &["checkout", "--quiet", fixture.base.as_str()],
    );

    let mut wrong_head = fixture.request(&["src/lib.rs"], &[]);
    wrong_head.expected_base = sibling_base;
    let wrong_head_report = inspect_delivery(&wrong_head);
    assert_eq!(
        wrong_head_report.refusal_code,
        Some(DeliveryInspectionRefusalCode::WrongHead)
    );
    assert_eq!(wrong_head_report.base_is_ancestor, Some(false));
    assert_eq!(
        wrong_head_report.head_commit.as_deref(),
        Some(fixture.base.as_str())
    );
}

#[test]
fn refuses_extra_staged_renamed_and_ignored_paths_and_exact_head_contract() {
    let extra = Fixture::new();
    extra.write("outside.txt", "outside\n");
    let extra_request = extra.request(&["src/lib.rs"], &[]);
    let extra_report = inspect_delivery(&extra_request);
    assert_eq!(
        extra_report.refusal_code,
        Some(DeliveryInspectionRefusalCode::OutOfScopeChange)
    );
    let outside = extra_report
        .changed_paths
        .iter()
        .find(|change| change.path == "outside.txt")
        .expect("outside change");
    assert!(!outside.allowed);
    assert!(outside.untracked);

    let staged = Fixture::new();
    staged.write("src/lib.rs", "pub fn alpha() -> u32 { 3 }\n");
    git(&staged.root, &["add", "src/lib.rs"]);
    let staged_out_of_scope = staged.request(&["src/unchanged.rs"], &[]);
    let staged_report = inspect_delivery(&staged_out_of_scope);
    assert_eq!(
        staged_report.refusal_code,
        Some(DeliveryInspectionRefusalCode::OutOfScopeChange)
    );
    let staged_change = staged_report
        .changed_paths
        .iter()
        .find(|change| change.path == "src/lib.rs")
        .expect("staged change");
    assert!(staged_change.staged);
    assert!(!staged_change.unstaged);

    let staged_allowed = staged.request(&["src/lib.rs"], &[]);
    let staged_allowed_report = inspect_delivery(&staged_allowed);
    assert_eq!(
        staged_allowed_report.refusal_code,
        Some(DeliveryInspectionRefusalCode::StagedChange)
    );
    let allowed_change = staged_allowed_report
        .changed_paths
        .iter()
        .find(|change| change.path == "src/lib.rs")
        .expect("allowed staged change");
    assert!(allowed_change.staged);
    assert!(!allowed_change.unstaged);

    staged.commit_all("committed allowed change");
    let committed_request = staged.request(&["src/lib.rs"], &[]);
    let committed_report = inspect_delivery(&committed_request);
    assert_eq!(
        committed_report.refusal_code,
        Some(DeliveryInspectionRefusalCode::WrongHead),
        "a committed descendant must not satisfy the exact-HEAD contract"
    );
    assert_eq!(committed_report.base_is_ancestor, Some(true));

    let ignored = Fixture::new();
    ignored.write("ignored.log", "ignored\n");
    let ignored_request = ignored.request(&["src/lib.rs"], &[]);
    let ignored_report = inspect_delivery(&ignored_request);
    assert_eq!(
        ignored_report.refusal_code,
        Some(DeliveryInspectionRefusalCode::OutOfScopeChange)
    );
    let ignored_change = ignored_report
        .changed_paths
        .iter()
        .find(|change| change.path == "ignored.log")
        .expect("ignored change");
    assert!(ignored_change.ignored);
    assert!(ignored_change.untracked);
    assert!(!ignored_change.allowed);
}

#[test]
fn refuses_renamed_path_instead_of_misparsing_it() {
    let fixture = Fixture::new();
    git(&fixture.root, &["mv", "src/lib.rs", "src/renamed.rs"]);
    let request = fixture.request(&["src/lib.rs", "src/renamed.rs"], &[]);
    let report = inspect_delivery(&request);
    assert_eq!(
        report.refusal_code,
        Some(DeliveryInspectionRefusalCode::RenameNotAllowed)
    );
}

#[test]
fn reports_missing_files_and_refuses_symlink_and_parent_symlink_escapes() {
    let missing = Fixture::new();
    let missing_request = missing.request(&["src/not-created.rs"], &[]);
    let missing_report = inspect_delivery(&missing_request);
    assert!(missing_report.ok, "unexpected refusal: {missing_report:?}");
    let missing_file = file(&missing_report, "src/not-created.rs");
    assert_eq!(missing_file.physical, DeliveryFilePhysicalCode::Missing);
    assert!(!missing_file.changed);

    let symlink = Fixture::new();
    std::os::unix::fs::symlink("lib.rs", symlink.root.join("src/link.rs"))
        .expect("create file symlink");
    let symlink_request = symlink.request(&["src/link.rs"], &["src/link.rs"]);
    let symlink_report = inspect_delivery(&symlink_request);
    assert_eq!(
        symlink_report.refusal_code,
        Some(DeliveryInspectionRefusalCode::SymlinkEscape)
    );
    let symlink_file = file(&symlink_report, "src/link.rs");
    assert_eq!(symlink_file.physical, DeliveryFilePhysicalCode::Symlink);
    assert!(symlink_file.sha256.is_none());
    assert!(symlink_file.text.is_none());

    let escape = Fixture::new();
    let outside = escape.root.parent().expect("repo parent").join("outside");
    fs::create_dir_all(&outside).expect("create outside");
    fs::write(outside.join("secret.txt"), "outside secret\n").expect("write outside secret");
    std::os::unix::fs::symlink(&outside, escape.root.join("escape"))
        .expect("create parent symlink");
    git(&escape.root, &["add", "escape"]);
    let escape_base = escape.commit_all("tracked parent symlink");
    let mut escape_request = escape.request(&["escape/secret.txt"], &["escape/secret.txt"]);
    escape_request.expected_base = escape_base.clone();
    let escape_report = inspect_delivery(&escape_request);
    assert_eq!(
        escape_report.refusal_code,
        Some(DeliveryInspectionRefusalCode::SymlinkEscape)
    );
    let escape_file = file(&escape_report, "escape/secret.txt");
    assert_eq!(escape_file.physical, DeliveryFilePhysicalCode::Symlink);
    assert!(escape_file.text.is_none());
    assert!(!serde_json::to_string(&escape_report)
        .expect("serialize escape report")
        .contains("outside secret"));

    let root_link_parent = TempDir::new().expect("root link tempdir");
    let root_link = root_link_parent.path().join("root-link");
    std::os::unix::fs::symlink(&escape.root, &root_link).expect("create root symlink");
    let root_link_request = DeliveryInspectionRequest {
        root: root_link,
        expected_base: escape_base,
        allowed_paths: vec!["src/lib.rs".to_string()],
        text_files: Vec::new(),
    };
    let root_link_report = inspect_delivery(&root_link_request);
    assert_eq!(
        root_link_report.refusal_code,
        Some(DeliveryInspectionRefusalCode::SymlinkEscape)
    );
}

#[test]
fn refuses_unsafe_protected_duplicate_and_disallowed_text_paths() {
    let fixture = Fixture::new();
    for unsafe_path in ["../outside", "/etc/passwd", "src/../lib.rs", "src\\lib.rs"] {
        let request = fixture.request(&[unsafe_path], &[]);
        let report = inspect_delivery(&request);
        assert_eq!(
            report.refusal_code,
            Some(DeliveryInspectionRefusalCode::UnsafePath),
            "path `{unsafe_path}` must be unsafe"
        );
    }
    for unsafe_path in [
        "src/./lib.rs",
        "src//lib.rs",
        "src/lib.rs/",
        "src/*.rs",
        "src/?.rs",
        "src/[ab].rs",
        "src/{lib,new}.rs",
        "src/.../lib.rs",
    ] {
        let request = fixture.request(&[unsafe_path], &[]);
        let report = inspect_delivery(&request);
        assert_eq!(
            report.refusal_code,
            Some(DeliveryInspectionRefusalCode::UnsafePath),
            "path `{unsafe_path}` must be rejected as a raw alias"
        );
    }
    let secret = "aws-secret-value\n";
    fixture.write(".aws/credentials", secret);
    let credential_request = fixture.request(&[".aws/credentials"], &[".aws/credentials"]);
    let credential_report = inspect_delivery(&credential_request);
    assert_eq!(
        credential_report.refusal_code,
        Some(DeliveryInspectionRefusalCode::ProtectedPath)
    );
    assert!(!serde_json::to_string(&credential_report)
        .expect("serialize credential refusal")
        .contains(secret.trim()));
    for protected in [
        ".git/config",
        "AGENTS.md",
        ".env",
        ".github/workflows/ci.yml",
    ] {
        let request = fixture.request(&[protected], &[]);
        let report = inspect_delivery(&request);
        assert_eq!(
            report.refusal_code,
            Some(DeliveryInspectionRefusalCode::ProtectedPath),
            "path `{protected}` must be protected"
        );
    }
    let duplicate = fixture.request(&["src/lib.rs", "src/lib.rs"], &[]);
    assert_eq!(
        inspect_delivery(&duplicate).refusal_code,
        Some(DeliveryInspectionRefusalCode::DuplicatePath)
    );
    let text_not_allowed = fixture.request(&["src/lib.rs"], &["src/unchanged.rs"]);
    assert_eq!(
        inspect_delivery(&text_not_allowed).refusal_code,
        Some(DeliveryInspectionRefusalCode::TextFileNotAllowed)
    );

    let binary = Fixture::new();
    binary.write("src/bin.dat", [0xff_u8, 0xfe, 0x00, 0x01]);
    let binary_request = binary.request(&["src/bin.dat"], &["src/bin.dat"]);
    let binary_report = inspect_delivery(&binary_request);
    assert_eq!(
        binary_report.refusal_code,
        Some(DeliveryInspectionRefusalCode::TextFileNotUtf8)
    );
    let binary_file = file(&binary_report, "src/bin.dat");
    assert_eq!(binary_file.utf8, Some(false));
    assert_eq!(binary_file.text_status, DeliveryTextStatus::NotUtf8);
    assert!(binary_file.text.is_none());

    let text_limit = Fixture::new();
    text_limit.write("src/large.txt", "0123456789");
    let text_limit_request = text_limit.request(&["src/large.txt"], &["src/large.txt"]);
    let limits = DeliveryInspectionLimits {
        max_text_file_bytes: 4,
        ..DeliveryInspectionLimits::default()
    };
    let text_limit_report = inspect_delivery_with_limits(&text_limit_request, &limits);
    assert_eq!(
        text_limit_report.refusal_code,
        Some(DeliveryInspectionRefusalCode::TextFileTooLarge)
    );
    let text_limit_file = file(&text_limit_report, "src/large.txt");
    assert_eq!(text_limit_file.text_status, DeliveryTextStatus::TooLarge);
    assert!(text_limit_file.text.is_none());
}

#[test]
fn enforces_path_count_file_size_total_read_output_time_and_git_output_limits() {
    let fixture = Fixture::new();
    let path_limits = DeliveryInspectionLimits {
        max_paths: 1,
        ..DeliveryInspectionLimits::default()
    };
    let too_many = fixture.request(&["src/lib.rs", "src/unchanged.rs"], &[]);
    assert_eq!(
        inspect_delivery_with_limits(&too_many, &path_limits).refusal_code,
        Some(DeliveryInspectionRefusalCode::TooManyPaths)
    );

    let size = Fixture::new();
    size.write("src/too-big.bin", "0123456789");
    let size_request = size.request(&["src/too-big.bin"], &[]);
    let size_limits = DeliveryInspectionLimits {
        max_file_bytes: 8,
        ..DeliveryInspectionLimits::default()
    };
    let size_report = inspect_delivery_with_limits(&size_request, &size_limits);
    assert_eq!(
        size_report.refusal_code,
        Some(DeliveryInspectionRefusalCode::FileSizeLimit)
    );
    assert_eq!(file(&size_report, "src/too-big.bin").size_bytes, Some(10));
    assert!(file(&size_report, "src/too-big.bin").sha256.is_none());

    let read = Fixture::new();
    read.write("src/one.bin", "01234567");
    read.write("src/two.bin", "76543210");
    let read_request = read.request(&["src/one.bin", "src/two.bin"], &[]);
    let read_limits = DeliveryInspectionLimits {
        max_file_bytes: 16,
        max_total_read_bytes: 10,
        ..DeliveryInspectionLimits::default()
    };
    let read_report = inspect_delivery_with_limits(&read_request, &read_limits);
    assert_eq!(
        read_report.refusal_code,
        Some(DeliveryInspectionRefusalCode::ReadLimit)
    );
    assert!(file(&read_report, "src/one.bin").sha256.is_some());
    assert!(file(&read_report, "src/two.bin").sha256.is_none());

    let output = Fixture::new();
    let output_request = output.request(&["src/lib.rs"], &[]);
    let output_limits = DeliveryInspectionLimits {
        max_output_bytes: 1,
        ..DeliveryInspectionLimits::default()
    };
    let output_report = inspect_delivery_with_limits(&output_request, &output_limits);
    assert_eq!(
        output_report.refusal_code,
        Some(DeliveryInspectionRefusalCode::OutputLimit)
    );
    assert!(output_report.output_omitted);

    let zero_output_limits = DeliveryInspectionLimits {
        max_output_bytes: 0,
        ..DeliveryInspectionLimits::default()
    };
    assert_eq!(
        inspect_delivery_with_limits(&output_request, &zero_output_limits).refusal_code,
        Some(DeliveryInspectionRefusalCode::OutputLimit)
    );
    let zero_git_limits = DeliveryInspectionLimits {
        max_git_output_bytes: 0,
        ..DeliveryInspectionLimits::default()
    };
    assert_eq!(
        inspect_delivery_with_limits(&output_request, &zero_git_limits).refusal_code,
        Some(DeliveryInspectionRefusalCode::GitOutputLimit)
    );

    let time = Fixture::new();
    let time_request = time.request(&["src/lib.rs"], &[]);
    let time_limits = DeliveryInspectionLimits {
        max_duration: Duration::ZERO,
        ..DeliveryInspectionLimits::default()
    };
    assert_eq!(
        inspect_delivery_with_limits(&time_request, &time_limits).refusal_code,
        Some(DeliveryInspectionRefusalCode::Timeout)
    );

    let git_output = Fixture::new();
    let git_output_request = git_output.request(&["src/lib.rs"], &[]);
    let git_output_limits = DeliveryInspectionLimits {
        max_git_output_bytes: 1,
        ..DeliveryInspectionLimits::default()
    };
    assert_eq!(
        inspect_delivery_with_limits(&git_output_request, &git_output_limits).refusal_code,
        Some(DeliveryInspectionRefusalCode::GitOutputLimit)
    );
}

#[test]
fn inspection_does_not_mutate_and_refuses_changing_read_evidence() {
    let fixture = Fixture::new();
    fixture.write("src/lib.rs", "pub fn alpha() -> u32 { 4 }\n");
    let request = fixture.request(&["src/lib.rs"], &["src/lib.rs"]);
    let before_status = git_status(&fixture.root);
    let before_contents = fs::read(fixture.root.join("src/lib.rs")).expect("read before");
    let report = inspect_delivery(&request);
    assert!(report.ok, "unexpected refusal: {report:?}");
    let after_status = git_status(&fixture.root);
    let after_contents = fs::read(fixture.root.join("src/lib.rs")).expect("read after");
    assert_eq!(
        before_status, after_status,
        "inspection must not mutate Git status"
    );
    assert_eq!(
        before_contents, after_contents,
        "inspection must not rewrite files"
    );
    assert!(
        !fixture.root.join(".git/index.lock").exists(),
        "inspection must not leave an index lock"
    );

    let changing = Fixture::new();
    let big = changing.write("src/big.bin", "");
    let original_len = 32 * 1024 * 1024;
    let big_file = fs::File::create(&big).expect("create big file");
    big_file.set_len(original_len).expect("size big file");
    drop(big_file);
    let stop = Arc::new(AtomicBool::new(false));
    let first_write = Arc::new(AtomicBool::new(false));
    let writer = {
        let path = big.clone();
        let stop = Arc::clone(&stop);
        let first_write = Arc::clone(&first_write);
        std::thread::spawn(move || {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .open(path)
                .expect("open changing file");
            let block = vec![b'x'; 64 * 1024];
            while !stop.load(Ordering::Relaxed) {
                file.set_len(original_len)
                    .expect("restore changing file length");
                file.seek(SeekFrom::Start(0)).expect("seek changing file");
                file.write_all(&block).expect("write changing file");
                let _ = file.flush();
                file.set_len(original_len - 1)
                    .expect("shrink changing file length");
                first_write.store(true, Ordering::Relaxed);
                std::thread::yield_now();
            }
        })
    };
    while !first_write.load(Ordering::Relaxed) {
        std::thread::yield_now();
    }
    let changing_request = changing.request(&["src/big.bin"], &[]);
    let changing_limits = DeliveryInspectionLimits {
        max_file_bytes: 64 * 1024 * 1024,
        max_total_read_bytes: 64 * 1024 * 1024,
        max_duration: Duration::from_secs(10),
        ..DeliveryInspectionLimits::default()
    };
    let changing_report = inspect_delivery_with_limits(&changing_request, &changing_limits);
    stop.store(true, Ordering::Relaxed);
    writer.join().expect("join changing writer");
    assert_eq!(
        changing_report.refusal_code,
        Some(DeliveryInspectionRefusalCode::ChangingRead)
    );
}

#[test]
fn refuses_special_files_without_reading_them() {
    let fixture = Fixture::new();
    let socket_path = fixture.root.join("src/socket.sock");
    let _listener =
        std::os::unix::net::UnixListener::bind(&socket_path).expect("create unix socket fixture");
    let request = fixture.request(&["src/socket.sock"], &["src/socket.sock"]);
    let report = inspect_delivery(&request);
    assert_eq!(
        report.refusal_code,
        Some(DeliveryInspectionRefusalCode::NotRegularFile)
    );
    let socket_file = file(&report, "src/socket.sock");
    assert_eq!(
        socket_file.physical,
        DeliveryFilePhysicalCode::NotRegularFile
    );
    assert!(socket_file.sha256.is_none());
    assert!(socket_file.text.is_none());
}

#[test]
fn output_refusal_drops_oversized_request_fields_and_text() {
    let fixture = Fixture::new();
    let huge_base = "a".repeat(200_000);
    let mut request = fixture.request(&["src/lib.rs"], &[]);
    request.expected_base = huge_base.clone();
    let limits = DeliveryInspectionLimits {
        max_output_bytes: 1024,
        ..DeliveryInspectionLimits::default()
    };
    let report = inspect_delivery_with_limits(&request, &limits);
    assert_eq!(
        report.refusal_code,
        Some(DeliveryInspectionRefusalCode::InvalidExpectedBase)
    );
    assert!(report.output_omitted);
    let serialized = serde_json::to_string(&report).expect("serialize bounded refusal");
    assert!(
        serialized.len() <= 1024,
        "bounded refusal must honor the output cap: {} bytes",
        serialized.len()
    );
    assert!(!serialized.contains(&huge_base[..1024]));

    let text = "sensitive-text-".repeat(8 * 1024);
    fixture.write("src/large.txt", &text);
    let text_request = fixture.request(&["src/large.txt"], &["src/large.txt"]);
    let text_limits = DeliveryInspectionLimits {
        max_output_bytes: 1024,
        max_file_bytes: 1024 * 1024,
        max_total_read_bytes: 1024 * 1024,
        max_text_file_bytes: 1024 * 1024,
        max_total_text_bytes: 1024 * 1024,
        ..DeliveryInspectionLimits::default()
    };
    let text_report = inspect_delivery_with_limits(&text_request, &text_limits);
    assert_eq!(
        text_report.refusal_code,
        Some(DeliveryInspectionRefusalCode::OutputLimit)
    );
    assert!(text_report.output_omitted);
    assert!(!text_report.text_included);
    assert!(text_report.files.is_empty());
    let serialized = serde_json::to_string(&text_report).expect("serialize text refusal");
    assert!(
        serialized.len() <= 1024,
        "text refusal must honor the output cap: {} bytes",
        serialized.len()
    );
    assert!(!serialized.contains("sensitive-text"));
}

#[test]
fn stale_index_stat_only_change_is_not_reported_and_index_is_not_mutated() {
    let fixture = Fixture::new();
    let lib_path = fixture.root.join("src/lib.rs");
    let index_path = fixture.root.join(".git/index");
    std::thread::sleep(Duration::from_millis(1_100));
    let contents = fs::read(&lib_path).expect("read lib contents");
    fs::write(&lib_path, contents).expect("rewrite byte-identical lib");

    let raw = run_sanitized_git_bytes(
        &fixture.root,
        &[
            "-c",
            "diff.autoRefreshIndex=false",
            "-c",
            "core.filemode=true",
            "diff",
            "--name-status",
            "-z",
            "--no-ext-diff",
            "--no-textconv",
            "--no-color",
            "--",
        ],
        1024 * 1024,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("raw stale-index name status");
    assert_eq!(
        raw.stdout_bytes.as_slice(),
        b"M\0src/lib.rs\0".as_slice(),
        "fixture must expose the stale-index phantom modification"
    );

    let before_index = fs::read(&index_path).expect("read index before inspection");
    let request = fixture.request(&["src/lib.rs"], &["src/lib.rs"]);
    let report = inspect_delivery(&request);
    assert!(report.ok, "unexpected refusal: {report:?}");
    assert!(
        report.changed_paths.is_empty(),
        "stat-only stale-index entry must not be reported as changed: {report:?}"
    );
    let lib = file(&report, "src/lib.rs");
    assert!(!lib.changed);
    assert_eq!(lib.text_status, DeliveryTextStatus::Included);
    assert!(lib.text.is_some());

    let after_index = fs::read(&index_path).expect("read index after inspection");
    assert_eq!(
        before_index, after_index,
        "inspection must not rewrite the index"
    );
}

#[test]
fn forced_filemode_reports_real_chmod_under_local_filemode_false() {
    let fixture = Fixture::new();
    git(&fixture.root, &["config", "core.filemode", "false"]);
    add_executable_bits(&fixture.root.join("src/lib.rs"));

    let request = fixture.request(&["src/lib.rs"], &[]);
    let report = inspect_delivery(&request);
    assert!(report.ok, "unexpected refusal: {report:?}");
    let change = report
        .changed_paths
        .iter()
        .find(|change| change.path == "src/lib.rs")
        .expect("real chmod must remain visible to the inspector");
    assert!(change.unstaged);
    assert_eq!(change.change_kinds, vec![DeliveryChangeKind::Modified]);
}

#[test]
fn refuses_suppressed_index_flags_before_reading_files() {
    for flag in ["--assume-unchanged", "--skip-worktree"] {
        let fixture = Fixture::new();
        git(&fixture.root, &["update-index", flag, "src/lib.rs"]);
        fixture.write("src/lib.rs", "pub fn alpha() -> u32 { 5 }\n");
        let request = fixture.request(&["src/lib.rs"], &["src/lib.rs"]);
        let report = inspect_delivery(&request);
        assert_eq!(
            report.refusal_code,
            Some(DeliveryInspectionRefusalCode::IndexFlagUnsupported),
            "flag `{flag}` must be refused: {report:?}"
        );
        assert!(report.files.is_empty());
        assert!(!report.text_included);
        assert!(!report.untrusted_data);
    }
}

#[test]
fn refuses_configured_filter_before_invoking_its_stub() {
    let fixture = Fixture::new();
    fixture.write(".gitattributes", "* filter=evil\n");
    let base = fixture.commit_all("tracked filter attributes");

    let parent = fixture.root.parent().expect("fixture temp parent");
    let marker = parent.join("filter-invoked");
    let script = parent.join("filter-stub.sh");
    fs::write(
        &script,
        format!(
            "#!/bin/sh\ncat >/dev/null\nprintf invoked >> '{}'\n",
            marker.display()
        ),
    )
    .expect("write filter stub");
    add_executable_bits(&script);
    let filter_command = format!("'{}'", script.display());
    git(
        &fixture.root,
        &["config", "filter.evil.clean", filter_command.as_str()],
    );
    fixture.write("src/lib.rs", "pub fn alpha() -> u32 { 6 }\n");

    let mut request = fixture.request(&["src/lib.rs"], &["src/lib.rs"]);
    request.expected_base = base;
    let report = inspect_delivery(&request);
    assert_eq!(
        report.refusal_code,
        Some(DeliveryInspectionRefusalCode::GitFilterConfigured),
        "configured content converter must be refused: {report:?}"
    );
    assert!(
        !marker.exists(),
        "configured filter stub must not run during metadata preflight"
    );
    assert!(!report.text_included);
}

#[test]
fn refusals_never_retain_explicit_text_or_inclusion_flags() {
    let fixture = Fixture::new();
    fixture.write("outside.txt", "outside\n");
    let request = fixture.request(&["src/lib.rs"], &["src/lib.rs"]);
    let report = inspect_delivery(&request);
    assert_eq!(
        report.refusal_code,
        Some(DeliveryInspectionRefusalCode::OutOfScopeChange)
    );
    let lib = file(&report, "src/lib.rs");
    assert!(lib.text.is_none());
    assert!(!lib.text_untrusted);
    assert_eq!(lib.text_status, DeliveryTextStatus::ChangingRead);
    assert!(!report.text_included);
    assert!(!report.untrusted_data);
}

#[test]
fn operator_docs_keep_byte_budgets_and_exact_head_guidance() {
    let guide = include_str!("../../../docs/MCP-OPERATING-GUIDE.md");
    let quickstart = include_str!("../../../docs/ORDINARY-WORKER-QUICKSTART.md");
    let skill = include_str!("../../../skills/threadlane/SKILL.md");

    assert!(
        guide.len() <= 16_384,
        "operating guide exceeds 16384 UTF-8 bytes: {}",
        guide.len()
    );
    assert!(
        quickstart.len() <= 4_096,
        "ordinary Worker quickstart exceeds 4096 UTF-8 bytes: {}",
        quickstart.len()
    );
    for document in [guide, quickstart, skill] {
        assert!(
            document.contains("equal the expected base"),
            "documentation must state the exact-HEAD contract"
        );
    }
    assert!(guide.contains("not globally atomic"));
    assert!(guide.contains("inspector stays optional"));
    assert!(quickstart.contains("equally bounded physical read"));
    assert!(quickstart.contains("OperatorApprovalRequest"));
    assert!(skill.contains("inspector stays optional"));
}
