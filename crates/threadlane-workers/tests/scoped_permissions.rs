#![cfg(unix)]
#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;
use tempfile::TempDir;
use threadlane_workers::{
    build_antigravity_task_files_settings_json, execute_worker_task, run_worker, ExecutionBudgets,
    FieldEvidence, WorkAcceptanceState, WorkerError, WorkerFilePermissions, WorkerProvider,
    WorkerRunStatus, WorkerSpec, AGY_READ_PERMISSION_TOOL, AGY_WRITE_PERMISSION_TOOLS,
};
use tokio::sync::watch;

struct ScopedTestEnv {
    _temp_dir: TempDir,
    root_dir: PathBuf,
    repo_dir: PathBuf,
    work_area: PathBuf,
    bin_dir: PathBuf,
    base_commit: String,
}

impl ScopedTestEnv {
    fn new() -> Self {
        let temp_dir = TempDir::new().expect("create temp dir");
        let root_dir = fs::canonicalize(temp_dir.path()).expect("canonicalize temp dir");
        let repo_dir = root_dir.join("source-repo");
        let work_area = root_dir.join("work-area");
        let bin_dir = root_dir.join("bin");

        fs::create_dir_all(&repo_dir).expect("create repo_dir");
        fs::create_dir_all(&bin_dir).expect("create bin_dir");
        fs::set_permissions(&bin_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 bin_dir");

        git_cmd(&repo_dir, &["init", "-b", "main"]);
        git_cmd(
            &repo_dir,
            &["config", "--local", "user.name", "Threadlane Test"],
        );
        git_cmd(
            &repo_dir,
            &["config", "--local", "user.email", "test@threadlane.local"],
        );

        let workers_src = repo_dir.join("crates/threadlane-workers/src");
        let core_src = repo_dir.join("crates/threadlane-core/src");
        fs::create_dir_all(&workers_src).expect("create workers_src");
        fs::create_dir_all(&core_src).expect("create core_src");

        fs::write(
            workers_src.join("lib.rs"),
            "pub fn baseline() -> u32 { 1 }\n",
        )
        .expect("write workers lib.rs");
        fs::write(core_src.join("lib.rs"), "pub fn core_only() {}\n").expect("write core lib.rs");

        git_cmd(&repo_dir, &["add", "."]);
        git_cmd(&repo_dir, &["commit", "-m", "Initial commit"]);

        let base_commit = git_stdout(&repo_dir, &["rev-parse", "--verify", "HEAD^{commit}"])
            .trim()
            .to_string();
        assert_eq!(base_commit.len(), 40);

        Self {
            _temp_dir: temp_dir,
            root_dir,
            repo_dir,
            work_area,
            bin_dir,
            base_commit,
        }
    }

    fn write_fake_executable(&self, name: &str, script_body: &str) -> PathBuf {
        let path = self.bin_dir.join(name);
        let full_script = format!("#!/bin/sh\nset -eu\n{script_body}\n");
        fs::write(&path, full_script).expect("write fake executable");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 fake executable");
        path
    }

    fn default_allowed_paths(&self) -> BTreeSet<String> {
        BTreeSet::from([
            "crates/threadlane-workers".to_string(),
            "docs/WORKERS.md".to_string(),
        ])
    }

    fn antigravity_spec(
        &self,
        executable: PathBuf,
        prompt: &str,
        file_permissions: WorkerFilePermissions,
    ) -> WorkerSpec {
        WorkerSpec {
            provider: WorkerProvider::Antigravity,
            executable,
            model: "gemini-4-argon-high".to_string(),
            effort: Some("high".to_string()),
            allowed_models: BTreeSet::from(["gemini-4-argon-high".to_string()]),
            session_id: None,
            standalone: false,
            repository_path: self.repo_dir.clone(),
            base_commit: self.base_commit.clone(),
            work_area_root: self.work_area.clone(),
            allowed_write_paths: self.default_allowed_paths(),
            prompt: prompt.to_string(),
            file_permissions,
            budgets: ExecutionBudgets::default(),
        }
    }

    fn opencode_spec(
        &self,
        executable: PathBuf,
        prompt: &str,
        file_permissions: WorkerFilePermissions,
    ) -> WorkerSpec {
        WorkerSpec {
            provider: WorkerProvider::OpenCodeV2,
            executable,
            model: "opencode/deepseek-4.1-flash".to_string(),
            effort: Some("max".to_string()),
            allowed_models: BTreeSet::from([
                "opencode/deepseek-4.1-flash".to_string(),
                "opencode/deepseek-4.1-flash#max".to_string(),
            ]),
            session_id: Some("sess-201".to_string()),
            standalone: true,
            repository_path: self.repo_dir.clone(),
            base_commit: self.base_commit.clone(),
            work_area_root: self.work_area.clone(),
            allowed_write_paths: self.default_allowed_paths(),
            prompt: prompt.to_string(),
            file_permissions,
            budgets: ExecutionBudgets::default(),
        }
    }
}

fn git_cmd(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .expect("spawn git");
    assert!(
        out.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
}

fn git_stdout(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .expect("spawn git");
    assert!(
        out.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).to_string()
}

#[test]
fn worker_file_permissions_serde_defaults_and_strict_variant_validation() {
    assert_eq!(
        WorkerFilePermissions::default(),
        WorkerFilePermissions::Inherit
    );
    assert_eq!(WorkerFilePermissions::Inherit.as_str(), "inherit");
    assert_eq!(WorkerFilePermissions::TaskFiles.as_str(), "task_files");

    assert_eq!(
        serde_json::to_string(&WorkerFilePermissions::Inherit).expect("serialize inherit"),
        "\"inherit\""
    );
    assert_eq!(
        serde_json::to_string(&WorkerFilePermissions::TaskFiles).expect("serialize task_files"),
        "\"task_files\""
    );

    let parsed_inherit: WorkerFilePermissions =
        serde_json::from_str("\"inherit\"").expect("deserialize inherit");
    assert_eq!(parsed_inherit, WorkerFilePermissions::Inherit);

    let parsed_task_files: WorkerFilePermissions =
        serde_json::from_str("\"task_files\"").expect("deserialize task_files");
    assert_eq!(parsed_task_files, WorkerFilePermissions::TaskFiles);

    for invalid_variant in [
        "\"TaskFiles\"",
        "\"Inherit\"",
        "\"task-files\"",
        "\"taskfiles\"",
        "\"all\"",
        "\"auto\"",
        "\"bypass\"",
        "\"\"",
    ] {
        assert!(
            serde_json::from_str::<WorkerFilePermissions>(invalid_variant).is_err(),
            "variant {invalid_variant} must be rejected by strict serde validation"
        );
    }

    // Deserializing legacy WorkerSpec JSON without `file_permissions` defaults to `Inherit`.
    let env = ScopedTestEnv::new();
    let bin = env.write_fake_executable("noop-agy", "exit 0");
    let spec = env.antigravity_spec(bin, "Legacy fixture check", WorkerFilePermissions::Inherit);
    let mut spec_json = serde_json::to_value(&spec).expect("to_value");
    spec_json
        .as_object_mut()
        .expect("object")
        .remove("file_permissions");
    let roundtrip: WorkerSpec = serde_json::from_value(spec_json).expect("deserialize legacy spec");
    assert_eq!(roundtrip.file_permissions, WorkerFilePermissions::Inherit);
}

#[test]
fn static_validation_and_build_argv_for_workdir_distinguish_inherit_and_task_files() {
    let env = ScopedTestEnv::new();
    let bin = env.write_fake_executable("static-check-agy", "exit 0");
    let workdir = env.root_dir.join("canonical-task-workdir");
    fs::create_dir_all(&workdir).expect("create workdir");

    // 1. Inherit mode preserves static build_argv and identical build_argv_for_workdir
    let inherit_spec = env.antigravity_spec(
        bin.clone(),
        "Inherit prompt",
        WorkerFilePermissions::Inherit,
    );
    let inherit_report = inherit_spec.validate().expect("validate inherit");
    assert_eq!(
        inherit_report.file_permissions,
        WorkerFilePermissions::Inherit
    );
    assert!(!inherit_report.workdir_argv_resolution_required);
    assert!(inherit_report.argv_resolution_note.is_none());

    let inherit_argv = inherit_spec.build_argv().expect("build_argv inherit");
    let inherit_workdir_argv = inherit_spec
        .build_argv_for_workdir(&workdir)
        .expect("build_argv_for_workdir inherit");
    assert_eq!(inherit_argv, inherit_workdir_argv);
    assert_eq!(
        inherit_argv,
        vec![
            "--model",
            "gemini-4-argon-high",
            "--effort",
            "high",
            "--mode",
            "accept-edits",
            "--output-format",
            "stream-json",
            "--print-timeout",
            "0",
            "--print",
            "Inherit prompt",
        ]
    );

    // 2. TaskFiles mode exposes file_permissions and workdir-dependent note on SpecValidationReport,
    // refuses workdir-less build_argv(), and builds exact scoped --settings in build_argv_for_workdir().
    let task_spec =
        env.antigravity_spec(bin, "Task files prompt", WorkerFilePermissions::TaskFiles);
    let task_report = task_spec
        .validate()
        .expect("validate task_files statically");
    assert_eq!(
        task_report.file_permissions,
        WorkerFilePermissions::TaskFiles
    );
    assert!(task_report.workdir_argv_resolution_required);
    assert!(
        task_report
            .argv_resolution_note
            .as_deref()
            .is_some_and(|note| note.contains("build_argv_for_workdir")),
        "SpecValidationReport must document workdir-dependent argv resolution"
    );
    assert!(
        !task_report
            .constructed_argv
            .iter()
            .any(|arg| arg == "--settings"),
        "static validation report must not pretend to synthesize --settings before task workdir exists"
    );

    let no_workdir_err = task_spec
        .build_argv()
        .expect_err("build_argv without workdir must reject TaskFiles");
    assert!(matches!(
        no_workdir_err,
        WorkerError::InvalidSpec(ref msg) if msg.contains("build_argv_for_workdir")
    ));

    let resolved_argv = task_spec
        .build_argv_for_workdir(&workdir)
        .expect("build_argv_for_workdir task_files");
    assert_eq!(resolved_argv.len(), 14);
    assert_eq!(&resolved_argv[..10], &inherit_argv[..10]);
    assert_eq!(resolved_argv[10], "--settings");
    assert_eq!(resolved_argv[12], "--print");
    assert_eq!(resolved_argv[13], "Task files prompt");
    assert!(!resolved_argv
        .iter()
        .any(|a| a.contains("dangerously") || a == "--auto" || a == "--variant"));

    let settings_val: Value =
        serde_json::from_str(&resolved_argv[11]).expect("parse --settings inline JSON");
    let root_obj = settings_val.as_object().expect("settings root object");
    assert_eq!(
        root_obj.len(),
        1,
        "settings JSON must only contain `permissions`"
    );
    let perms_obj = root_obj
        .get("permissions")
        .and_then(Value::as_object)
        .expect("permissions object");
    assert_eq!(
        perms_obj.len(),
        1,
        "permissions object must only contain `allow`"
    );
    let allow_list: Vec<String> = perms_obj
        .get("allow")
        .and_then(Value::as_array)
        .expect("allow array")
        .iter()
        .map(|v| v.as_str().expect("string rule").to_string())
        .collect();

    let w = workdir.to_str().expect("utf8 workdir");
    let mut expected_allow = vec![format!("{AGY_READ_PERMISSION_TOOL}({w}/**)")];
    for rel in ["crates/threadlane-workers", "docs/WORKERS.md"] {
        for tool in AGY_WRITE_PERMISSION_TOOLS {
            expected_allow.push(format!("{tool}({w}/{rel})"));
            expected_allow.push(format!("{tool}({w}/{rel}/**)"));
        }
    }
    assert_eq!(allow_list, expected_allow);

    // Ensure source repository path is NOT in the permissions allowlist.
    let repo_str = env.repo_dir.to_str().expect("utf8 repo");
    assert!(
        allow_list.iter().all(|rule| !rule.contains(repo_str)),
        "permissions must never grant access to source repository_path"
    );
}

#[tokio::test]
async fn execute_worker_task_with_task_files_passes_task_worktree_settings_and_preserves_gates() {
    let env = ScopedTestEnv::new();

    // Fake AGY binary records its received argv to `received_argv.txt`, modifies an allowed file,
    // and emits the real installed AGY stream schema (which omits effort).
    let agy_bin = env.write_fake_executable(
        "task-files-agy",
        r#"
for arg in "$@"; do
    printf '%s\n' "$arg" >> crates/threadlane-workers/received_argv.txt
done
printf 'pub fn edited_with_scoped_permissions() -> u32 { 42 }\n' >> crates/threadlane-workers/src/lib.rs
printf '{"event":"init","conversation_id":"s-scoped-1","init":{"model":"gemini-4-argon-high","cwd":"%s","permission_mode":"accept-edits"}}\n' "$PWD"
printf '{"event":"step_update","step_update":{"conversation_id":"s-scoped-1","step_index":1,"state":"DONE","step_type":"tool_call","usage":{}}}\n'
printf '{"event":"result","result":{"conversation_id":"s-scoped-1","status":"SUCCESS","response":"Updated lib.rs","num_turns":2}}\n'
"#,
    );

    let spec = env.antigravity_spec(
        agy_bin,
        "Edit crates/threadlane-workers/src/lib.rs safely",
        WorkerFilePermissions::TaskFiles,
    );
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let task_report = execute_worker_task(&spec, "run-m2c", "task-1", None, &mut cancel_rx)
        .await
        .expect("execute_worker_task with TaskFiles");

    let canonical_checkout = &task_report.manifest.checkout_path;
    assert_ne!(canonical_checkout, &env.repo_dir);

    let expected_argv = spec
        .build_argv_for_workdir(canonical_checkout)
        .expect("build_argv_for_workdir for task checkout");
    assert_eq!(task_report.worker_report.constructed_argv, expected_argv);

    let received_argv: Vec<String> =
        fs::read_to_string(canonical_checkout.join("crates/threadlane-workers/received_argv.txt"))
            .expect("read received_argv.txt from task worktree")
            .lines()
            .map(str::to_string)
            .collect();
    assert_eq!(received_argv, expected_argv);

    // Verify the settings argument uses the canonical task checkout path, not source_repo.
    let settings_arg = &received_argv[11];
    assert!(settings_arg.contains(canonical_checkout.to_str().expect("utf8 checkout")));
    assert!(!settings_arg.contains(env.repo_dir.to_str().expect("utf8 repo")));

    // Missing observed effort remains MissingEffortEvidence / FinishedNeedsEvidenceAndReview.
    assert_eq!(
        task_report.worker_report.evidence.effort_evidence,
        FieldEvidence::Missing {
            requested: "high".to_string()
        }
    );
    assert_eq!(
        task_report.worker_report.status,
        WorkerRunStatus::MissingEffortEvidence
    );
    assert!(task_report.diff_gate.passed);
    assert_eq!(
        task_report.overall_acceptance_state,
        WorkAcceptanceState::FinishedNeedsEvidenceAndReview
    );

    // Verify that if a worker modifies a path outside allowed_write_paths despite TaskFiles hints,
    // the post-run Git diff gate still rejects the task.
    let rogue_bin = env.write_fake_executable(
        "rogue-task-files-agy",
        r#"
printf '// unauthorized write outside allowed_write_paths\n' >> crates/threadlane-core/src/lib.rs
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-argon-high","effort":"high","result":"done"}\n'
"#,
    );
    let rogue_spec = env.antigravity_spec(
        rogue_bin,
        "Attempt unauthorized edit",
        WorkerFilePermissions::TaskFiles,
    );
    let rogue_report =
        execute_worker_task(&rogue_spec, "run-m2c", "task-rogue", None, &mut cancel_rx)
            .await
            .expect("execute rogue worker task");
    assert_eq!(
        rogue_report.worker_report.status,
        WorkerRunStatus::Succeeded
    );
    assert!(!rogue_report.diff_gate.passed);
    assert_eq!(
        rogue_report.overall_acceptance_state,
        WorkAcceptanceState::RejectedByDiffGate
    );
}

#[tokio::test]
async fn rejects_opencode_task_files_and_unsafe_glob_or_rule_metacharacters_before_spawn() {
    let env = ScopedTestEnv::new();
    let marker_file = env.root_dir.join("must_not_spawn.marker");
    let spy_bin = env.write_fake_executable(
        "spy-worker",
        &format!(
            "printf 'spawned\\n' > '{}'\nexit 0\n",
            marker_file.display()
        ),
    );

    // 1. OpenCodeV2 with TaskFiles must fail validation, build_argv_for_workdir, and execute_worker_task
    // without creating work_area_root or spawning the process.
    let mut oc_task_files = env.opencode_spec(
        spy_bin.clone(),
        "Reject OpenCode TaskFiles",
        WorkerFilePermissions::TaskFiles,
    );
    oc_task_files.work_area_root = env.root_dir.join("uncreated-oc-work-area");
    assert!(matches!(
        oc_task_files.validate(),
        Err(WorkerError::InvalidSpec(ref msg)) if msg.contains("antigravity")
    ));
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    assert!(matches!(
        execute_worker_task(&oc_task_files, "run-oc", "task-oc", None, &mut cancel_rx).await,
        Err(WorkerError::InvalidSpec(_))
    ));
    assert!(
        !oc_task_files.work_area_root.exists(),
        "work_area_root must not be created when OpenCode TaskFiles is rejected"
    );
    assert!(!marker_file.exists(), "worker must not be spawned");

    // 2. Ambiguous glob/rule metacharacters in allowed_write_paths must be rejected for TaskFiles.
    let unsafe_write_entries = [
        "crates/threadlane-*",
        "crates/threadlane-?",
        "crates/[threadlane]",
        "crates/threadlane]",
        "crates/{threadlane}",
        "crates/threadlane}",
        "crates/(threadlane)",
        "crates/threadlane)",
        "crates/thread\\lane",
        "crates/thread\nlane",
        "crates/thread\tlane",
        "crates/thread\x07lane",
    ];
    let clean_workdir = env.root_dir.join("clean-workdir");
    fs::create_dir_all(&clean_workdir).expect("create clean_workdir");

    for bad_entry in unsafe_write_entries {
        let mut bad_spec = env.antigravity_spec(
            spy_bin.clone(),
            "Reject unsafe write path",
            WorkerFilePermissions::TaskFiles,
        );
        bad_spec.work_area_root = env.root_dir.join("uncreated-bad-entry-work-area");
        bad_spec.allowed_write_paths = BTreeSet::from([bad_entry.to_string()]);

        assert!(
            bad_spec.validate().is_err(),
            "allowed_write_paths entry {bad_entry:?} must fail validate() for TaskFiles"
        );
        assert!(
            bad_spec.build_argv_for_workdir(&clean_workdir).is_err(),
            "allowed_write_paths entry {bad_entry:?} must fail build_argv_for_workdir()"
        );
        assert!(
            run_worker(&bad_spec, &clean_workdir).await.is_err(),
            "allowed_write_paths entry {bad_entry:?} must fail run_worker() before spawn"
        );
        assert!(
            !bad_spec.work_area_root.exists(),
            "work_area_root must not be created for rejected entry {bad_entry:?}"
        );
        assert!(!marker_file.exists(), "spy_bin must never be spawned");
    }

    // 3. Ambiguous glob/rule metacharacters in canonical working_directory must be rejected before spawn.
    let unsafe_dir_names = [
        "workdir*glob",
        "workdir?question",
        "workdir[bracket]",
        "workdir{brace}",
        "workdir(paren)",
        "workdir)closeparen",
        "workdir\\backslash",
        "workdir\ttab",
    ];
    let valid_task_spec = env.antigravity_spec(
        spy_bin.clone(),
        "Reject unsafe workdir",
        WorkerFilePermissions::TaskFiles,
    );

    for bad_dir_name in unsafe_dir_names {
        let bad_workdir = env.root_dir.join(bad_dir_name);
        fs::create_dir_all(&bad_workdir).expect("create bad_workdir");
        assert!(
            valid_task_spec
                .build_argv_for_workdir(&bad_workdir)
                .is_err(),
            "working_directory {bad_dir_name:?} must fail build_argv_for_workdir()"
        );
        assert!(
            run_worker(&valid_task_spec, &bad_workdir).await.is_err(),
            "working_directory {bad_dir_name:?} must fail run_worker() before spawn"
        );
        assert!(!marker_file.exists(), "spy_bin must never be spawned");
    }

    // 4. Pre-existing symlink at an allowed_write_paths target inside working_directory must be rejected.
    let symlink_workdir = env.root_dir.join("symlink-target-workdir");
    fs::create_dir_all(symlink_workdir.join("crates")).expect("create crates dir");
    symlink(
        &env.repo_dir,
        symlink_workdir.join("crates/threadlane-workers"),
    )
    .expect("create symlink at allowed_write_paths target");
    assert!(matches!(
        valid_task_spec.build_argv_for_workdir(&symlink_workdir),
        Err(WorkerError::SecurityViolation(_))
    ));
    assert!(
        run_worker(&valid_task_spec, &symlink_workdir)
            .await
            .is_err(),
        "symlink target in working_directory must fail before spawn"
    );
    assert!(!marker_file.exists(), "spy_bin must never be spawned");

    // 5. Root directory `/` must be rejected as a TaskFiles working_directory.
    assert!(
        build_antigravity_task_files_settings_json(Path::new("/"), &env.default_allowed_paths())
            .is_err(),
        "filesystem root `/` must be rejected for TaskFiles"
    );
}

#[tokio::test]
async fn clean_unicode_and_spaces_work_with_task_files_json_serialization() {
    let env = ScopedTestEnv::new();
    let unicode_space_workdir = env
        .root_dir
        .join("task dir with spaces and café_प्रकल्प_日本語");
    let allowed_subpath = "sub folder/модуль_αβγ/file name.rs";
    fs::create_dir_all(unicode_space_workdir.join("sub folder/модуль_αβγ"))
        .expect("create unicode and space dir");

    let echo_bin = env.write_fake_executable(
        "unicode-space-agy",
        r#"
for arg in "$@"; do
    printf '%s\n' "$arg" >> received_unicode_argv.txt
done
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-argon-high","effort":"high","result":"ok"}\n'
"#,
    );

    let mut spec = env.antigravity_spec(
        echo_bin,
        "Unicode & spaces task",
        WorkerFilePermissions::TaskFiles,
    );
    spec.allowed_write_paths = BTreeSet::from([allowed_subpath.to_string()]);

    let built_argv = spec
        .build_argv_for_workdir(&unicode_space_workdir)
        .expect("build_argv_for_workdir with clean Unicode and spaces");
    assert_eq!(built_argv[10], "--settings");

    let parsed: Value =
        serde_json::from_str(&built_argv[11]).expect("parse settings JSON with Unicode and spaces");
    let allow = parsed["permissions"]["allow"]
        .as_array()
        .expect("allow array");
    let w = unicode_space_workdir.to_str().expect("utf8 workdir");
    assert_eq!(
        allow[0].as_str(),
        Some(format!("read_file({w}/**)").as_str())
    );
    assert!(allow
        .iter()
        .any(|v| v.as_str() == Some(format!("edit_file({w}/{allowed_subpath})").as_str())));
    assert!(allow
        .iter()
        .any(|v| v.as_str() == Some(format!("edit_file({w}/{allowed_subpath}/**)").as_str())));

    let report = run_worker(&spec, &unicode_space_workdir)
        .await
        .expect("run_worker with Unicode and spaces workdir");
    assert_eq!(report.status, WorkerRunStatus::Succeeded);
    assert_eq!(report.constructed_argv, built_argv);

    let received_argv: Vec<String> =
        fs::read_to_string(unicode_space_workdir.join("received_unicode_argv.txt"))
            .expect("read received_unicode_argv.txt")
            .lines()
            .map(str::to_string)
            .collect();
    assert_eq!(received_argv, built_argv);
}

#[tokio::test]
async fn enforces_task_files_path_count_and_settings_byte_limits_without_affecting_inherit() {
    let env = ScopedTestEnv::new();
    let marker_file = env.root_dir.join("must_not_spawn_limits.marker");
    let spy_bin = env.write_fake_executable(
        "spy-limits-agy",
        &format!(
            "printf 'spawned\\n' > '{}'\nexit 0\n",
            marker_file.display()
        ),
    );
    let ok_bin = env.write_fake_executable(
        "ok-limits-agy",
        r#"
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-argon-high","effort":"high","result":"ok"}\n'
"#,
    );

    let canonical_workdir = env.root_dir.join("limits-canonical-workdir");
    fs::create_dir_all(&canonical_workdir).expect("create limits-canonical-workdir");

    // 1. >128 allowed_write_paths entries must be rejected for TaskFiles with InvalidSpec before
    // rule allocation or process spawn, while Inherit permits the exact same allowlist.
    let over_limit_paths: BTreeSet<String> = (0..129)
        .map(|i| format!("crates/threadlane-workers/src/module_{i:03}.rs"))
        .collect();
    assert_eq!(over_limit_paths.len(), 129);

    let count_err = WorkerFilePermissions::TaskFiles
        .validate_for_provider_and_paths(WorkerProvider::Antigravity, &over_limit_paths)
        .expect_err(">128 TaskFiles paths must fail validate_for_provider_and_paths");
    assert!(matches!(
        count_err,
        WorkerError::InvalidSpec(ref msg) if msg.contains("128") && msg.contains("129")
    ));

    let mut over_limit_task_spec = env.antigravity_spec(
        spy_bin.clone(),
        "Reject >128 TaskFiles paths",
        WorkerFilePermissions::TaskFiles,
    );
    over_limit_task_spec.work_area_root = env.root_dir.join("uncreated-over-128-work-area");
    over_limit_task_spec.allowed_write_paths = over_limit_paths.clone();

    assert!(matches!(
        over_limit_task_spec.validate(),
        Err(WorkerError::InvalidSpec(ref msg)) if msg.contains("128")
    ));
    assert!(matches!(
        build_antigravity_task_files_settings_json(&canonical_workdir, &over_limit_paths),
        Err(WorkerError::InvalidSpec(ref msg)) if msg.contains("128")
    ));
    assert!(matches!(
        over_limit_task_spec.build_argv_for_workdir(&canonical_workdir),
        Err(WorkerError::InvalidSpec(ref msg)) if msg.contains("128")
    ));
    assert!(matches!(
        run_worker(&over_limit_task_spec, &canonical_workdir).await,
        Err(WorkerError::InvalidSpec(ref msg)) if msg.contains("128")
    ));
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    assert!(matches!(
        execute_worker_task(
            &over_limit_task_spec,
            "run-limits",
            "task-over-128",
            None,
            &mut cancel_rx
        )
        .await,
        Err(WorkerError::InvalidSpec(ref msg)) if msg.contains("128")
    ));
    assert!(
        !over_limit_task_spec.work_area_root.exists(),
        "work_area_root must not be created when >128 TaskFiles paths are rejected"
    );
    assert!(!marker_file.exists(), "spy_bin must never be spawned");

    // Inherit must remain unaffected and accept the exact same >128 allowlist.
    WorkerFilePermissions::Inherit
        .validate_for_provider_and_paths(WorkerProvider::Antigravity, &over_limit_paths)
        .expect("Inherit must allow >128 valid paths");
    let mut inherit_large_spec = env.antigravity_spec(
        ok_bin.clone(),
        "Allow >128 Inherit paths",
        WorkerFilePermissions::Inherit,
    );
    inherit_large_spec.allowed_write_paths = over_limit_paths;
    let inherit_report = inherit_large_spec
        .validate()
        .expect("Inherit spec with >128 paths must validate");
    assert_eq!(inherit_report.allowed_write_paths.len(), 129);
    let inherit_argv = inherit_large_spec
        .build_argv()
        .expect("Inherit build_argv with >128 paths");
    let inherit_workdir_argv = inherit_large_spec
        .build_argv_for_workdir(&canonical_workdir)
        .expect("Inherit build_argv_for_workdir with >128 paths");
    assert_eq!(inherit_argv, inherit_workdir_argv);
    assert!(!inherit_argv.iter().any(|arg| arg == "--settings"));
    let inherit_run = run_worker(&inherit_large_spec, &canonical_workdir)
        .await
        .expect("run_worker Inherit with >128 paths");
    assert_eq!(inherit_run.status, WorkerRunStatus::Succeeded);

    // 2. <=128 legitimate long entries + a real canonical workdir that exceed 65,536 serialized
    // JSON bytes (both via long path segments and via JSON string escaping) must be rejected with
    // InvalidSpec before returning argv or spawning a fake provider.
    let deep_workdir = env
        .root_dir
        .join("canonical_workdir_segment_alpha_0123456789abcdef")
        .join("canonical_workdir_segment_beta_0123456789abcdef");
    fs::create_dir_all(&deep_workdir).expect("create deep_workdir");

    // 2a. 32 legitimate long ASCII entries (<=128) + canonical workdir exceeding 65,536 bytes.
    let a_seg = "a".repeat(80);
    let long_ascii_paths: BTreeSet<String> = (0..32)
        .map(|i| format!("crates/threadlane-workers/{a_seg}/{a_seg}/{a_seg}/mod_{i:02}.rs"))
        .collect();
    assert_eq!(long_ascii_paths.len(), 32);

    let mut long_task_spec = env.antigravity_spec(
        spy_bin.clone(),
        "Reject >64KiB serialized settings from long paths",
        WorkerFilePermissions::TaskFiles,
    );
    long_task_spec.allowed_write_paths = long_ascii_paths.clone();
    long_task_spec
        .validate()
        .expect("<=128 valid long paths pass static count and pattern validation");

    assert!(matches!(
        build_antigravity_task_files_settings_json(&deep_workdir, &long_ascii_paths),
        Err(WorkerError::InvalidSpec(ref msg)) if msg.contains("65536")
    ));
    assert!(matches!(
        long_task_spec.build_argv_for_workdir(&deep_workdir),
        Err(WorkerError::InvalidSpec(ref msg)) if msg.contains("65536")
    ));
    assert!(matches!(
        run_worker(&long_task_spec, &deep_workdir).await,
        Err(WorkerError::InvalidSpec(ref msg)) if msg.contains("65536")
    ));
    assert!(matches!(
        execute_worker_task(
            &long_task_spec,
            "run-limits",
            "task-long-ascii",
            None,
            &mut cancel_rx
        )
        .await,
        Err(WorkerError::InvalidSpec(ref msg)) if msg.contains("65536")
    ));
    assert!(
        !marker_file.exists(),
        "spy_bin must not spawn when serialized settings exceed 65,536 bytes"
    );

    // 2b. 12 legitimate entries (<=128) whose unescaped rule strings are under 65,536 bytes,
    // but exceed 65,536 serialized JSON bytes after serde_json escapes `"` to `\"`.
    let q_seg = "q\"".repeat(45);
    let escaped_overflow_paths: BTreeSet<String> = (0..12)
        .map(|i| format!("crates/threadlane-workers/{q_seg}/{q_seg}/{q_seg}/mod_{i:02}.rs"))
        .collect();
    assert_eq!(escaped_overflow_paths.len(), 12);

    let mut escaped_overflow_spec = env.antigravity_spec(
        spy_bin,
        "Reject >64KiB serialized settings after JSON escaping",
        WorkerFilePermissions::TaskFiles,
    );
    escaped_overflow_spec.allowed_write_paths = escaped_overflow_paths.clone();
    escaped_overflow_spec
        .validate()
        .expect("12 valid paths with quotes pass static validation");

    assert!(matches!(
        build_antigravity_task_files_settings_json(&deep_workdir, &escaped_overflow_paths),
        Err(WorkerError::InvalidSpec(ref msg)) if msg.contains("65536")
    ));
    assert!(matches!(
        escaped_overflow_spec.build_argv_for_workdir(&deep_workdir),
        Err(WorkerError::InvalidSpec(ref msg)) if msg.contains("65536")
    ));
    assert!(matches!(
        run_worker(&escaped_overflow_spec, &deep_workdir).await,
        Err(WorkerError::InvalidSpec(ref msg)) if msg.contains("65536")
    ));
    assert!(
        !marker_file.exists(),
        "spy_bin must not spawn when JSON escaping pushes settings past 65,536 bytes"
    );

    // 3. A normal small TaskFiles payload remains supported and preserves all grants without truncation.
    let small_task_spec = env.antigravity_spec(
        ok_bin,
        "Small TaskFiles payload works",
        WorkerFilePermissions::TaskFiles,
    );
    let small_json = build_antigravity_task_files_settings_json(
        &canonical_workdir,
        &small_task_spec.allowed_write_paths,
    )
    .expect("build small TaskFiles settings JSON");
    assert!(small_json.len() <= 65_536);

    let parsed_small: Value =
        serde_json::from_str(&small_json).expect("parse small TaskFiles settings JSON");
    let small_allow = parsed_small["permissions"]["allow"]
        .as_array()
        .expect("allow array");
    assert_eq!(
        small_allow.len(),
        1 + small_task_spec.allowed_write_paths.len() * AGY_WRITE_PERMISSION_TOOLS.len() * 2,
        "all grants must be preserved without silent truncation"
    );

    let small_argv = small_task_spec
        .build_argv_for_workdir(&canonical_workdir)
        .expect("build_argv_for_workdir for small TaskFiles spec");
    assert_eq!(small_argv[10], "--settings");
    assert_eq!(small_argv[11], small_json);

    let small_run = run_worker(&small_task_spec, &canonical_workdir)
        .await
        .expect("run_worker for small TaskFiles spec");
    assert_eq!(small_run.status, WorkerRunStatus::Succeeded);
}
