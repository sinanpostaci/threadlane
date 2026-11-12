#![cfg(unix)]
#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use threadlane_workers::{
    build_antigravity_task_files_settings_json, execute_worker_task, run_worker,
    AdvisoryWorkerSpec, DiffViolationKind, ExecutionBudgets, FieldEvidence, OpenCodeRuleEffect,
    WorkAcceptanceState, WorkerError, WorkerFilePermissions, WorkerProvider, WorkerRunStatus,
    WorkerSpec, WorktreeManager, AGY_READ_PERMISSION_TOOL, AGY_WRITE_PERMISSION_TOOLS,
    MAX_PROMPT_BYTES,
};
use tokio::sync::watch;

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn read_nul_delimited_argv(path: &Path) -> Vec<Vec<u8>> {
    let mut bytes = fs::read(path).expect("read NUL-delimited argv capture");
    assert_eq!(
        bytes.last(),
        Some(&0),
        "argv capture must be NUL-terminated without a text-line delimiter"
    );
    bytes
        .pop()
        .expect("NUL-terminated argv capture must contain a trailing delimiter");
    if bytes.is_empty() {
        return Vec::new();
    }
    bytes
        .split(|byte| *byte == 0)
        .map(|arg| arg.to_vec())
        .collect()
}

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
            "crates/threadlane-workers/".to_string(),
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
    assert!(
        resolved_argv[13].starts_with("[THREADLANE_AGY_TASK_FILES_CONTRACT] "),
        "TaskFiles argv[13] must start with the session-local AGY TaskFiles contract header"
    );
    let (contract_header, retained_prompt) = resolved_argv[13]
        .split_once("[THREADLANE_TASK_INSTRUCTION] ")
        .expect("TaskFiles argv[13] must contain unambiguous task instruction boundary");
    assert_eq!(retained_prompt, "Task files prompt");
    assert!(resolved_argv[13].ends_with("Task files prompt"));
    assert!(contract_header.contains("`read_file`"));
    assert!(contract_header.contains("`view_file`"));
    assert!(contract_header.contains("`list_dir`"));
    assert!(contract_header.contains("`grep_search`"));
    assert!(contract_header.contains("`edit_file`"));
    assert!(
        contract_header.contains("Never use or request `command`, `run_command`, `bash`, `shell`")
    );
    assert!(contract_header.contains("parent controller runs all checks and Git operations"));
    assert!(contract_header.contains(
        "[`crates/threadlane-workers/` (directory subtree `crates/threadlane-workers` and `crates/threadlane-workers/**`), `docs/WORKERS.md` (exact file only; no descendant `/**`)]"
    ));
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
    for tool in AGY_WRITE_PERMISSION_TOOLS {
        expected_allow.push(format!("{tool}({w}/crates/threadlane-workers)"));
        expected_allow.push(format!("{tool}({w}/crates/threadlane-workers/**)"));
    }
    for tool in AGY_WRITE_PERMISSION_TOOLS {
        expected_allow.push(format!("{tool}({w}/docs/WORKERS.md)"));
    }
    assert_eq!(allow_list, expected_allow);
    assert!(
        !allow_list.iter().any(|rule| {
            rule.starts_with("command(")
                || rule.starts_with("run_command(")
                || rule.starts_with("bash(")
                || rule.starts_with("shell(")
        }),
        "TaskFiles allowlist must never grant command or shell execution permissions"
    );
    for tool in AGY_WRITE_PERMISSION_TOOLS {
        assert!(
            !allow_list.contains(&format!("{tool}({w}/docs/WORKERS.md/**)")),
            "exact file `docs/WORKERS.md` must never be granted descendant `/**` permissions for {tool}"
        );
    }

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
    assert!(
        received_argv[13].ends_with(
            "[THREADLANE_TASK_INSTRUCTION] Edit crates/threadlane-workers/src/lib.rs safely"
        ),
        "child AGY process must receive guarded prompt ending with verbatim task instruction"
    );

    // Verify the settings argument uses the canonical task checkout path, not source_repo.
    let settings_arg = &received_argv[11];
    assert!(settings_arg.contains(canonical_checkout.to_str().expect("utf8 checkout")));
    assert!(!settings_arg.contains(env.repo_dir.to_str().expect("utf8 repo")));

    // The installed AGY schema proves the selected known catalog variant; acceptance still
    // requires the diff gate and an explicit review.
    assert_eq!(
        task_report.worker_report.evidence.effort_evidence,
        FieldEvidence::VerifiedMatch {
            value: "high".to_string()
        }
    );
    assert_eq!(task_report.worker_report.status, WorkerRunStatus::Succeeded);
    assert!(task_report.diff_gate.passed);
    assert_eq!(
        task_report.overall_acceptance_state,
        WorkAcceptanceState::DiffGatePassedPendingReview
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

    // Verify that a zero-exit AGY run reporting `denied_actions` for `command` / `run_command`
    // (matching installed AGY 1.5.3 headless auto-denial) remains Blocked and rejected with no
    // changed task path.
    let blocked_cmd_bin = env.write_fake_executable(
        "blocked-cmd-task-files-agy",
        r#"
printf '{"event":"init","conversation_id":"s-blocked-cmd","init":{"model":"gemini-4-argon-high","cwd":"%s","permission_mode":"accept-edits"}}\n' "$PWD"
printf '{"event":"result","result":{"conversation_id":"s-blocked-cmd","status":"SUCCESS","response":"Command auto-denied in headless mode","denied_actions":[{"action":"command","display_name":"run_command"}],"num_turns":1}}\n'
exit 0
"#,
    );
    let blocked_cmd_spec = env.antigravity_spec(
        blocked_cmd_bin,
        "Do not use command tool",
        WorkerFilePermissions::TaskFiles,
    );
    let blocked_cmd_report = execute_worker_task(
        &blocked_cmd_spec,
        "run-m2c",
        "task-blocked-cmd",
        None,
        &mut cancel_rx,
    )
    .await
    .expect("execute blocked command worker task");
    assert_eq!(
        blocked_cmd_report.worker_report.status,
        WorkerRunStatus::Blocked
    );
    assert!(
        !blocked_cmd_report
            .worker_report
            .evidence
            .blocked_events
            .is_empty(),
        "denied_actions on exit 0 must record a blocked event"
    );
    assert_ne!(
        blocked_cmd_report.overall_acceptance_state,
        WorkAcceptanceState::DiffGatePassedPendingReview
    );
    assert!(
        git_stdout(
            &blocked_cmd_report.manifest.checkout_path,
            &["status", "--porcelain"]
        )
        .trim()
        .is_empty(),
        "blocked command worker must leave no modified or untracked files in task checkout"
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

    // 1. OpenCodeV2 with valid TaskFiles passes static validation and exposes workdir-dependent
    // argv metadata. The static preview keeps the bare caller prompt, while the actual
    // build_argv_for_workdir invocation receives the canonical-cwd prologue without Antigravity
    // `--settings`. Unsafe metacharacters or protected control paths fail before creating
    // work_area_root or spawning the process.
    let clean_workdir = env.root_dir.join("clean-workdir");
    fs::create_dir_all(&clean_workdir).expect("create clean_workdir");

    let valid_oc_task_files = env.opencode_spec(
        spy_bin.clone(),
        "Valid OpenCode TaskFiles",
        WorkerFilePermissions::TaskFiles,
    );
    let oc_report = valid_oc_task_files
        .validate()
        .expect("OpenCodeV2 TaskFiles must pass static validation");
    assert_eq!(oc_report.file_permissions, WorkerFilePermissions::TaskFiles);
    assert!(
        oc_report.workdir_argv_resolution_required,
        "OpenCode TaskFiles actual argv depends on the canonical working directory"
    );
    let oc_note = oc_report
        .argv_resolution_note
        .as_deref()
        .expect("OpenCode TaskFiles must document workdir-dependent argv resolution");
    assert!(oc_note.contains("static base preview"));
    assert!(oc_note.contains("prologue"));
    assert!(oc_note.contains("canonical task working directory"));
    assert!(oc_note.contains("build_argv_for_workdir"));
    assert_eq!(
        oc_report.constructed_argv.last().map(String::as_str),
        Some("Valid OpenCode TaskFiles"),
        "static preview must not pretend the workdir prologue is already resolved"
    );
    assert!(!oc_report
        .constructed_argv
        .iter()
        .any(|arg| arg.contains("THREADLANE_OPENCODE_TASK_FILES_CONTRACT")));
    let static_oc_err = valid_oc_task_files
        .build_argv()
        .expect_err("OpenCode TaskFiles build_argv must require a canonical workdir");
    assert!(matches!(
        static_oc_err,
        WorkerError::InvalidSpec(ref msg)
            if msg.contains("task_files") && msg.contains("build_argv_for_workdir")
    ));

    let clean_workdir_str = clean_workdir.to_str().expect("utf8 clean workdir");
    let oc_argv = valid_oc_task_files
        .build_argv_for_workdir(&clean_workdir)
        .expect("OpenCodeV2 build_argv_for_workdir");
    assert!(!oc_argv.iter().any(|arg| arg == "--settings"));
    let oc_effective_prompt = oc_argv.last().expect("OpenCode effective prompt");
    assert!(oc_effective_prompt.starts_with("[THREADLANE_OPENCODE_TASK_FILES_CONTRACT] "));
    assert!(oc_effective_prompt.contains(&format!("`{clean_workdir_str}`")));
    assert!(oc_effective_prompt.ends_with("[THREADLANE_TASK_INSTRUCTION] Valid OpenCode TaskFiles"));

    let mut oc_bad_task_files = env.opencode_spec(
        spy_bin.clone(),
        "Reject invalid OpenCode TaskFiles",
        WorkerFilePermissions::TaskFiles,
    );
    oc_bad_task_files.work_area_root = env.root_dir.join("uncreated-oc-work-area");
    oc_bad_task_files.allowed_write_paths = BTreeSet::from(["crates/threadlane-*".to_string()]);
    assert!(matches!(
        oc_bad_task_files.validate(),
        Err(WorkerError::InvalidSpec(_))
    ));
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    assert!(matches!(
        execute_worker_task(
            &oc_bad_task_files,
            "run-oc",
            "task-oc",
            None,
            &mut cancel_rx
        )
        .await,
        Err(WorkerError::InvalidSpec(_))
    ));
    assert!(
        !oc_bad_task_files.work_area_root.exists(),
        "work_area_root must not be created when invalid OpenCode TaskFiles is rejected"
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

#[test]
fn opencode_task_files_prologue_uses_exact_canonical_cwd_inside_and_outside_work_area() {
    let env = ScopedTestEnv::new();
    let noop_bin = env.write_fake_executable("opencode-cwd-contract-noop", "exit 0");
    let allowed_write_paths = env.default_allowed_paths();

    let manager = WorktreeManager::new(&env.repo_dir, &env.work_area, &env.base_commit)
        .expect("create WorktreeManager for OpenCode cwd contract test");
    let manifest = manager
        .create_task_worktree(
            "run-opencode-cwd",
            "task-opencode-cwd",
            &allowed_write_paths,
        )
        .expect("create OpenCode cwd contract task worktree");
    let checkout = manifest.checkout_path.clone();
    assert!(
        checkout
            .strip_prefix(&env.work_area)
            .is_ok_and(|relative| !relative.as_os_str().is_empty()),
        "task checkout must be an owned mutable worktree inside work_area_root"
    );

    let outside_dir = env.root_dir.join("outside canonical workdir café 日本語");
    fs::create_dir_all(&outside_dir).expect("create outside canonical workdir");
    assert!(
        outside_dir.strip_prefix(&env.work_area).is_err(),
        "outside fixture must not reside inside work_area_root"
    );

    let caller_prompt =
        "Use the exact canonical cwd.\nNever infer a checkout path from metadata.\n";
    let spec = env.opencode_spec(noop_bin, caller_prompt, WorkerFilePermissions::TaskFiles);
    let spec_json_before =
        serde_json::to_vec(&spec).expect("serialize OpenCode spec before cwd contract");
    let instruction_digest_before = sha256_hex(spec.prompt.as_bytes());

    for (cwd, expected_inside_work_area) in [
        (checkout.as_path(), true),
        (outside_dir.as_path(), false),
        (env.repo_dir.as_path(), false),
    ] {
        let canonical_cwd = fs::canonicalize(cwd).expect("canonicalize cwd fixture");
        assert_eq!(
            canonical_cwd.as_path(),
            cwd,
            "fixture path must already be canonical"
        );
        assert_eq!(
            canonical_cwd
                .strip_prefix(&env.work_area)
                .is_ok_and(|relative| !relative.as_os_str().is_empty()),
            expected_inside_work_area
        );
        let canonical_cwd_str = canonical_cwd.to_str().expect("utf8 canonical cwd");

        let report = spec
            .validate()
            .expect("valid OpenCode TaskFiles spec must validate for every mutable cwd");
        assert!(report.workdir_argv_resolution_required);
        assert!(report
            .argv_resolution_note
            .as_deref()
            .is_some_and(|note| note.contains("prologue")
                && note.contains("canonical task working directory")));
        assert_eq!(
            report.constructed_argv.last().map(String::as_str),
            Some(caller_prompt),
            "static report must remain a bare-prompt preview for every cwd"
        );

        let config_before = spec
            .build_opencode_config_for_workdir(&canonical_cwd)
            .expect("build OpenCode config before cwd argv");
        let argv = spec
            .build_argv_for_workdir(&canonical_cwd)
            .expect("OpenCode TaskFiles must resolve prologue for every valid mutable cwd");
        assert_eq!(argv.len(), 10);
        let effective_prompt = argv.last().expect("OpenCode effective prompt");
        let (contract_header, retained_prompt) = effective_prompt
            .split_once("[THREADLANE_TASK_INSTRUCTION] ")
            .expect("actual OpenCode argv must carry the task instruction boundary");
        assert!(
            contract_header.starts_with("[THREADLANE_OPENCODE_TASK_FILES_CONTRACT] "),
            "every mutable OpenCode TaskFiles cwd must receive the canonical prologue"
        );
        assert_eq!(retained_prompt, caller_prompt);
        assert_eq!(
            contract_header
                .matches("[THREADLANE_OPENCODE_TASK_FILES_CONTRACT]")
                .count(),
            1
        );
        assert!(
            contract_header.contains(&format!("`{canonical_cwd_str}`")),
            "prologue must state the exact canonical cwd `{canonical_cwd_str}`"
        );
        assert_eq!(
            config_before,
            spec.build_opencode_config_for_workdir(&canonical_cwd)
                .expect("rebuild OpenCode config after cwd argv"),
            "prologue resolution must not change model or permission configuration"
        );
    }

    assert_eq!(spec.prompt, caller_prompt);
    assert_eq!(
        spec_json_before,
        serde_json::to_vec(&spec).expect("serialize OpenCode spec after cwd contract"),
        "prologue resolution must not mutate the WorkerSpec"
    );
    assert_eq!(
        instruction_digest_before,
        sha256_hex(spec.prompt.as_bytes()),
        "original task instruction digest must remain unchanged"
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
    assert_eq!(allow.len(), 1 + AGY_WRITE_PERMISSION_TOOLS.len());
    for tool in AGY_WRITE_PERMISSION_TOOLS {
        assert!(allow
            .iter()
            .any(|v| v.as_str() == Some(format!("{tool}({w}/{allowed_subpath})").as_str())));
        assert!(
            !allow
                .iter()
                .any(|v| v.as_str() == Some(format!("{tool}({w}/{allowed_subpath}/**)").as_str())),
            "exact file path with Unicode and spaces must not grant descendant `/**` for {tool}"
        );
    }

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

    // 2a. 32 legitimate long ASCII directory entries (<=128) + canonical workdir exceeding 65,536 bytes.
    let a_seg = "a".repeat(80);
    let long_ascii_paths: BTreeSet<String> = (0..32)
        .map(|i| format!("crates/threadlane-workers/{a_seg}/{a_seg}/{a_seg}/mod_{i:02}/"))
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

    // 2b. 12 legitimate directory entries (<=128) whose unescaped rule strings are under 65,536 bytes,
    // but exceed 65,536 serialized JSON bytes after serde_json escapes `"` to `\"`.
    let q_seg = "q\"".repeat(45);
    let escaped_overflow_paths: BTreeSet<String> = (0..12)
        .map(|i| format!("crates/threadlane-workers/{q_seg}/{q_seg}/{q_seg}/mod_{i:02}/"))
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
    // `crates/threadlane-workers/` (directory -> 2 * 5 rules) + `docs/WORKERS.md` (exact file -> 1 * 5 rules) + 1 read_file rule.
    assert_eq!(
        small_allow.len(),
        1 + AGY_WRITE_PERMISSION_TOOLS.len() * 3,
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

fn parse_allow_rules(settings_json: &str) -> Vec<String> {
    let val: Value = serde_json::from_str(settings_json).expect("parse --settings inline JSON");
    val["permissions"]["allow"]
        .as_array()
        .expect("permissions.allow array")
        .iter()
        .map(|v| v.as_str().expect("string rule").to_string())
        .collect()
}

#[tokio::test]
async fn exact_file_and_absent_file_scopes_never_grant_descendant_wildcards_while_explicit_and_pinned_base_dirs_do(
) {
    let env = ScopedTestEnv::new();
    let noop_bin = env.write_fake_executable("noop-scope-check-agy", "exit 0");
    let workdir = env.root_dir.join("exact-vs-dir-workdir");
    fs::create_dir_all(workdir.join("crates/threadlane-workers/src"))
        .expect("create workdir crates/threadlane-workers/src");
    fs::write(
        workdir.join("crates/threadlane-workers/src/lib.rs"),
        "pub fn baseline() -> u32 { 1 }\n",
    )
    .expect("write existing file in workdir");
    let w = workdir.to_str().expect("utf8 workdir");

    // 1. Existing exact file (`crates/threadlane-workers/src/lib.rs`):
    // Must grant ONLY `<tool>(<w>/crates/threadlane-workers/src/lib.rs)` and NEVER `/**` for any write tool.
    let existing_file_paths = BTreeSet::from(["crates/threadlane-workers/src/lib.rs".to_string()]);
    let mut existing_file_spec = env.antigravity_spec(
        noop_bin.clone(),
        "Existing exact file scope",
        WorkerFilePermissions::TaskFiles,
    );
    existing_file_spec.allowed_write_paths = existing_file_paths.clone();

    let existing_file_argv = existing_file_spec
        .build_argv_for_workdir(&workdir)
        .expect("build_argv_for_workdir for existing exact file");
    assert_eq!(existing_file_argv[10], "--settings");
    let direct_existing_json =
        build_antigravity_task_files_settings_json(&workdir, &existing_file_paths)
            .expect("direct builder for existing exact file");
    assert_eq!(existing_file_argv[11], direct_existing_json);
    assert!(existing_file_argv[13].contains(
        "[`crates/threadlane-workers/src/lib.rs` (exact file only; no descendant `/**`)]"
    ));
    assert!(!existing_file_argv[13].contains("directory subtree"));
    assert!(
        existing_file_argv[13].ends_with("[THREADLANE_TASK_INSTRUCTION] Existing exact file scope")
    );

    let existing_allow = parse_allow_rules(&existing_file_argv[11]);
    let mut expected_existing_allow = vec![format!("{AGY_READ_PERMISSION_TOOL}({w}/**)")];
    for tool in AGY_WRITE_PERMISSION_TOOLS {
        expected_existing_allow.push(format!("{tool}({w}/crates/threadlane-workers/src/lib.rs)"));
    }
    assert_eq!(existing_allow, expected_existing_allow);
    for tool in AGY_WRITE_PERMISSION_TOOLS {
        assert!(
            !existing_allow.iter().any(|rule| rule
                .starts_with(&format!("{tool}({w}/crates/threadlane-workers/src/lib.rs/"))),
            "existing exact file must never receive `{tool}(.../lib.rs/**)` or any child grant"
        );
    }

    // 2. Absent exact file (`new.txt`):
    // Must grant ONLY `<tool>(<w>/new.txt)` and NEVER `<tool>(<w>/new.txt/**)` or any child grant for any write tool.
    let absent_file_paths = BTreeSet::from(["new.txt".to_string()]);
    let mut absent_file_spec = env.antigravity_spec(
        noop_bin.clone(),
        "Absent exact file scope",
        WorkerFilePermissions::TaskFiles,
    );
    absent_file_spec.allowed_write_paths = absent_file_paths.clone();

    let absent_file_argv = absent_file_spec
        .build_argv_for_workdir(&workdir)
        .expect("build_argv_for_workdir for absent exact file");
    assert_eq!(absent_file_argv[10], "--settings");
    let direct_absent_json =
        build_antigravity_task_files_settings_json(&workdir, &absent_file_paths)
            .expect("direct builder for absent exact file");
    assert_eq!(absent_file_argv[11], direct_absent_json);
    assert!(absent_file_argv[13].contains("[`new.txt` (exact file only; no descendant `/**`)]"));
    assert!(!absent_file_argv[13].contains("directory subtree"));
    assert!(absent_file_argv[13].ends_with("[THREADLANE_TASK_INSTRUCTION] Absent exact file scope"));

    let absent_allow = parse_allow_rules(&absent_file_argv[11]);
    let mut expected_absent_allow = vec![format!("{AGY_READ_PERMISSION_TOOL}({w}/**)")];
    for tool in AGY_WRITE_PERMISSION_TOOLS {
        expected_absent_allow.push(format!("{tool}({w}/new.txt)"));
    }
    assert_eq!(absent_allow, expected_absent_allow);
    for tool in AGY_WRITE_PERMISSION_TOOLS {
        assert!(
            !absent_allow
                .iter()
                .any(|rule| rule.starts_with(&format!("{tool}({w}/new.txt/"))),
            "absent exact file `new.txt` must never receive `{tool}(.../new.txt/**)` or any child grant"
        );
    }

    // 3. Explicit trailing-slash directory (`crates/threadlane-workers/` and absent `new_dir/`):
    // Must grant both `<tool>(<w>/<dir>)` and `<tool>(<w>/<dir>/**)` for all write tools.
    let explicit_dir_paths = BTreeSet::from([
        "crates/threadlane-workers/".to_string(),
        "new_dir/".to_string(),
    ]);
    let mut explicit_dir_spec = env.antigravity_spec(
        noop_bin.clone(),
        "Explicit directory scopes",
        WorkerFilePermissions::TaskFiles,
    );
    explicit_dir_spec.allowed_write_paths = explicit_dir_paths.clone();

    let explicit_dir_argv = explicit_dir_spec
        .build_argv_for_workdir(&workdir)
        .expect("build_argv_for_workdir for explicit directories");
    let direct_explicit_dir_json =
        build_antigravity_task_files_settings_json(&workdir, &explicit_dir_paths)
            .expect("direct builder for explicit directories");
    assert_eq!(explicit_dir_argv[11], direct_explicit_dir_json);
    assert!(explicit_dir_argv[13].contains(
        "[`crates/threadlane-workers/` (directory subtree `crates/threadlane-workers` and `crates/threadlane-workers/**`), `new_dir/` (directory subtree `new_dir` and `new_dir/**`)]"
    ));
    assert!(
        explicit_dir_argv[13].ends_with("[THREADLANE_TASK_INSTRUCTION] Explicit directory scopes")
    );

    let explicit_dir_allow = parse_allow_rules(&explicit_dir_argv[11]);
    let mut expected_explicit_dir_allow = vec![format!("{AGY_READ_PERMISSION_TOOL}({w}/**)")];
    for rel_dir in ["crates/threadlane-workers", "new_dir"] {
        for tool in AGY_WRITE_PERMISSION_TOOLS {
            expected_explicit_dir_allow.push(format!("{tool}({w}/{rel_dir})"));
            expected_explicit_dir_allow.push(format!("{tool}({w}/{rel_dir}/**)"));
        }
    }
    assert_eq!(explicit_dir_allow, expected_explicit_dir_allow);

    // Explicit trailing-slash directory targeting an existing regular file must fail closed.
    let file_as_dir_paths = BTreeSet::from(["crates/threadlane-workers/src/lib.rs/".to_string()]);
    let mut file_as_dir_spec = env.antigravity_spec(
        noop_bin.clone(),
        "Reject trailing slash on regular file",
        WorkerFilePermissions::TaskFiles,
    );
    file_as_dir_spec.allowed_write_paths = file_as_dir_paths.clone();
    assert!(matches!(
        file_as_dir_spec.build_argv_for_workdir(&workdir),
        Err(WorkerError::SecurityViolation(_))
    ));
    assert!(matches!(
        build_antigravity_task_files_settings_json(&workdir, &file_as_dir_paths),
        Err(WorkerError::InvalidSpec(_))
    ));

    // 4. Legacy bare directory in `WorkerSpec` (`crates/threadlane-workers` exists as a directory
    // tree in `base_commit`) vs direct builder lacking Git context:
    let legacy_bare_dir_paths = BTreeSet::from(["crates/threadlane-workers".to_string()]);
    let mut legacy_bare_dir_spec = env.antigravity_spec(
        noop_bin.clone(),
        "Legacy bare base directory in WorkerSpec",
        WorkerFilePermissions::TaskFiles,
    );
    legacy_bare_dir_spec.allowed_write_paths = legacy_bare_dir_paths.clone();

    let legacy_spec_argv = legacy_bare_dir_spec
        .build_argv_for_workdir(&workdir)
        .expect("WorkerSpec::build_argv_for_workdir resolves legacy bare base directory via pinned base_commit");
    assert!(legacy_spec_argv[13].contains(
        "[`crates/threadlane-workers/` (directory subtree `crates/threadlane-workers` and `crates/threadlane-workers/**`)]"
    ));
    assert!(legacy_spec_argv[13]
        .ends_with("[THREADLANE_TASK_INSTRUCTION] Legacy bare base directory in WorkerSpec"));

    let multiline_prompt = "Line 1: update module\nLine 2: preserve exact bytes & symbols!\n";
    let mut multiline_spec = env.antigravity_spec(
        noop_bin.clone(),
        multiline_prompt,
        WorkerFilePermissions::TaskFiles,
    );
    multiline_spec.allowed_write_paths = existing_file_paths.clone();
    let multiline_argv = multiline_spec
        .build_argv_for_workdir(&workdir)
        .expect("build_argv_for_workdir with multiline task prompt");
    let (_, extracted_multiline) = multiline_argv[13]
        .split_once("[THREADLANE_TASK_INSTRUCTION] ")
        .expect("split boundary on multiline prompt");
    assert_eq!(extracted_multiline, multiline_prompt);

    let legacy_spec_allow = parse_allow_rules(&legacy_spec_argv[11]);
    let mut expected_legacy_spec_allow = vec![format!("{AGY_READ_PERMISSION_TOOL}({w}/**)")];
    for tool in AGY_WRITE_PERMISSION_TOOLS {
        expected_legacy_spec_allow.push(format!("{tool}({w}/crates/threadlane-workers)"));
        expected_legacy_spec_allow.push(format!("{tool}({w}/crates/threadlane-workers/**)"));
    }
    assert_eq!(legacy_spec_allow, expected_legacy_spec_allow);

    // Direct public builder lacks Git context (`base_commit`) and therefore uses syntactic
    // trailing-slash resolution (bare entry -> exact only).
    let direct_bare_json =
        build_antigravity_task_files_settings_json(&workdir, &legacy_bare_dir_paths)
            .expect("direct builder on bare entry");
    let direct_bare_allow = parse_allow_rules(&direct_bare_json);
    let mut expected_direct_bare_allow = vec![format!("{AGY_READ_PERMISSION_TOOL}({w}/**)")];
    for tool in AGY_WRITE_PERMISSION_TOOLS {
        expected_direct_bare_allow.push(format!("{tool}({w}/crates/threadlane-workers)"));
    }
    assert_eq!(direct_bare_allow, expected_direct_bare_allow);

    // 5. `WorkerFilePermissions::Inherit` argv remains completely unchanged across all allowlists.
    let expected_inherit_argv = vec![
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
        "Inherit unchanged check",
    ];
    for paths in [
        existing_file_paths,
        absent_file_paths,
        explicit_dir_paths,
        legacy_bare_dir_paths,
    ] {
        let mut inherit_spec = env.antigravity_spec(
            noop_bin.clone(),
            "Inherit unchanged check",
            WorkerFilePermissions::Inherit,
        );
        inherit_spec.allowed_write_paths = paths;
        assert_eq!(
            inherit_spec.build_argv().expect("Inherit build_argv"),
            expected_inherit_argv
        );
        assert_eq!(
            inherit_spec
                .build_argv_for_workdir(&workdir)
                .expect("Inherit build_argv_for_workdir"),
            expected_inherit_argv
        );
    }
}

#[tokio::test]
async fn mutable_file_to_directory_replacement_does_not_widen_task_files_settings_and_diff_gate_rejects_children(
) {
    let env = ScopedTestEnv::new();
    let noop_bin = env.write_fake_executable("noop-mutable-check-agy", "exit 0");
    let workdir = env.root_dir.join("mutable-file-to-dir-workdir");
    fs::create_dir_all(&workdir).expect("create workdir");

    // In the mutable task workdir, replace `crates/threadlane-workers/src/lib.rs` (a regular file
    // at `base_commit`) AND `new.txt` (absent at `base_commit`) with directories containing children.
    fs::create_dir_all(workdir.join("crates/threadlane-workers/src/lib.rs"))
        .expect("create directory at lib.rs path in mutable workdir");
    fs::write(
        workdir.join("crates/threadlane-workers/src/lib.rs/nested.rs"),
        "pub fn nested() {}\n",
    )
    .expect("write nested file under lib.rs directory in mutable workdir");
    fs::create_dir_all(workdir.join("new.txt"))
        .expect("create directory at new.txt path in mutable workdir");
    fs::write(workdir.join("new.txt/nested.txt"), "nested\n")
        .expect("write nested file under new.txt directory in mutable workdir");

    // Also mutate `env.repo_dir` working tree AND advance `HEAD` after `env.base_commit` so both
    // paths become directories on `HEAD`, while `spec.base_commit` stays pinned to `env.base_commit`.
    fs::remove_file(env.repo_dir.join("crates/threadlane-workers/src/lib.rs"))
        .expect("remove lib.rs in repo_dir");
    fs::create_dir_all(env.repo_dir.join("crates/threadlane-workers/src/lib.rs"))
        .expect("replace lib.rs with dir in repo_dir");
    fs::write(
        env.repo_dir
            .join("crates/threadlane-workers/src/lib.rs/nested.rs"),
        "pub fn repo_nested() {}\n",
    )
    .expect("write nested.rs in repo_dir");
    fs::create_dir_all(env.repo_dir.join("new.txt")).expect("create new.txt dir in repo_dir");
    fs::write(env.repo_dir.join("new.txt/nested.txt"), "repo nested\n")
        .expect("write new.txt/nested.txt in repo_dir");
    git_cmd(&env.repo_dir, &["add", "."]);
    git_cmd(
        &env.repo_dir,
        &[
            "commit",
            "-m",
            "Mutate HEAD after pinned base_commit to directories",
        ],
    );

    let exact_paths = BTreeSet::from([
        "crates/threadlane-workers/src/lib.rs".to_string(),
        "new.txt".to_string(),
    ]);
    let mut pinned_spec = env.antigravity_spec(
        noop_bin,
        "Pinned base_commit exact file scopes",
        WorkerFilePermissions::TaskFiles,
    );
    pinned_spec.allowed_write_paths = exact_paths.clone();
    assert_eq!(pinned_spec.base_commit, env.base_commit);

    let argv = pinned_spec
        .build_argv_for_workdir(&workdir)
        .expect("build_argv_for_workdir must resolve against immutable pinned base_commit");
    let direct_json = build_antigravity_task_files_settings_json(&workdir, &exact_paths)
        .expect("direct builder must use syntactic scope, not mutable workdir is_dir()");
    assert_eq!(argv[11], direct_json);

    let w = workdir.to_str().expect("utf8 workdir");
    let allow = parse_allow_rules(&argv[11]);
    let mut expected_allow = vec![format!("{AGY_READ_PERMISSION_TOOL}({w}/**)")];
    for rel in ["crates/threadlane-workers/src/lib.rs", "new.txt"] {
        for tool in AGY_WRITE_PERMISSION_TOOLS {
            expected_allow.push(format!("{tool}({w}/{rel})"));
        }
    }
    assert_eq!(allow, expected_allow);
    for rel in ["crates/threadlane-workers/src/lib.rs", "new.txt"] {
        for tool in AGY_WRITE_PERMISSION_TOOLS {
            assert!(
                !allow
                    .iter()
                    .any(|rule| rule.starts_with(&format!("{tool}({w}/{rel}/"))),
                "mutable file->dir replacement must never widen `{rel}` to `{tool}({w}/{rel}/**)`"
            );
        }
    }

    // Verify end-to-end via `execute_worker_task`:
    // 1) Creating exact `new.txt` as a regular file passes with exact-only `--settings` (no `new.txt/**`).
    let exact_new_file_bin = env.write_fake_executable(
        "exact-new-file-agy",
        r#"
printf 'created exact new file\n' > new.txt
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-argon-high","effort":"high","result":"ok"}\n'
"#,
    );
    let mut exact_new_spec = env.antigravity_spec(
        exact_new_file_bin,
        "Create exact new.txt",
        WorkerFilePermissions::TaskFiles,
    );
    exact_new_spec.allowed_write_paths = BTreeSet::from(["new.txt".to_string()]);
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let exact_new_report = execute_worker_task(
        &exact_new_spec,
        "run-exact-new",
        "task-exact-new-ok",
        None,
        &mut cancel_rx,
    )
    .await
    .expect("execute_worker_task creating exact new.txt");
    assert_eq!(
        exact_new_report.worker_report.status,
        WorkerRunStatus::Succeeded
    );
    assert!(exact_new_report.diff_gate.passed);
    assert_eq!(
        exact_new_report.overall_acceptance_state,
        WorkAcceptanceState::DiffGatePassedPendingReview
    );
    let task_checkout_str = exact_new_report
        .manifest
        .checkout_path
        .to_str()
        .expect("utf8 task checkout");
    let task_allow = parse_allow_rules(&exact_new_report.worker_report.constructed_argv[11]);
    for tool in AGY_WRITE_PERMISSION_TOOLS {
        assert!(task_allow.contains(&format!("{tool}({task_checkout_str}/new.txt)")));
        assert!(
            !task_allow
                .iter()
                .any(|rule| rule.starts_with(&format!("{tool}({task_checkout_str}/new.txt/"))),
            "constructed_argv `--settings` for `new.txt` must not grant `{tool}(.../new.txt/**)`"
        );
    }

    // 2) Replacing an exact file scope (`new.txt`) with a directory `new.txt/child.txt` is
    // rejected by the post-run Git diff gate as `OutsideAllowedPaths`.
    let dir_escalation_bin = env.write_fake_executable(
        "dir-escalation-agy",
        r#"
mkdir -p new.txt
printf 'unauthorized child under exact file scope\n' > new.txt/child.txt
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-argon-high","effort":"high","result":"ok"}\n'
"#,
    );
    let mut escalation_spec = env.antigravity_spec(
        dir_escalation_bin,
        "Attempt new.txt/child.txt under exact new.txt scope",
        WorkerFilePermissions::TaskFiles,
    );
    escalation_spec.allowed_write_paths = BTreeSet::from(["new.txt".to_string()]);
    let escalation_report = execute_worker_task(
        &escalation_spec,
        "run-exact-new",
        "task-exact-new-escalate",
        None,
        &mut cancel_rx,
    )
    .await
    .expect("execute_worker_task for new.txt/child.txt escalation");
    assert!(!escalation_report.diff_gate.passed);
    assert_eq!(
        escalation_report.overall_acceptance_state,
        WorkAcceptanceState::RejectedByDiffGate
    );
    assert!(escalation_report.diff_gate.violations.iter().any(|v| {
        v.kind == DiffViolationKind::OutsideAllowedPaths && v.path == "new.txt/child.txt"
    }));
}

#[tokio::test]
async fn moving_legacy_symbolic_ref_from_exact_file_to_directory_does_not_widen_manifest_pinned_launch_settings(
) {
    let env = ScopedTestEnv::new();
    let ok_bin = env.write_fake_executable(
        "symbolic-ref-pin-agy",
        r#"
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-argon-high","effort":"high","result":"ok"}\n'
"#,
    );

    // At commit A (`env.base_commit`), `crates/threadlane-workers/src/lib.rs` is a regular file
    // and `new.txt` is absent. The legacy symbolic ref `main` points to commit A.
    let commit_a = env.base_commit.clone();
    let exact_paths = BTreeSet::from([
        "crates/threadlane-workers/src/lib.rs".to_string(),
        "new.txt".to_string(),
    ]);

    let mut symbolic_spec = env.antigravity_spec(
        ok_bin,
        "Symbolic ref pinned to task manifest base_commit",
        WorkerFilePermissions::TaskFiles,
    );
    symbolic_spec.base_commit = "main".to_string();
    symbolic_spec.allowed_write_paths = exact_paths.clone();

    // 1. `execute_worker_task` with symbolic `base_commit = "main"` resolves `manifest.base_commit`
    // to exact 40-hex `commit_a` without mutating caller `symbolic_spec.base_commit`.
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let exec_report = execute_worker_task(
        &symbolic_spec,
        "run-symref",
        "task-symref-exec",
        None,
        &mut cancel_rx,
    )
    .await
    .expect("execute_worker_task with symbolic base_commit `main`");
    assert_eq!(
        symbolic_spec.base_commit, "main",
        "caller spec must not be mutated by execute_worker_task"
    );
    assert_eq!(exec_report.manifest.base_commit, commit_a);
    assert_eq!(
        exec_report.worker_report.requested_model.base_model,
        "gemini-4-argon-high"
    );
    assert_eq!(
        exec_report.worker_report.requested_model.effort.as_deref(),
        Some("high")
    );

    // 2. Create task manifest A while `main` points to `commit_a`, then deterministically advance
    // `main` to `commit_b` where both `crates/threadlane-workers/src/lib.rs` and `new.txt` are
    // directories in the Git commit tree.
    let manager = WorktreeManager::new(
        &symbolic_spec.repository_path,
        &symbolic_spec.work_area_root,
        &symbolic_spec.base_commit,
    )
    .expect("create WorktreeManager for `main` at commit A");
    assert_eq!(manager.resolved_base_commit(), commit_a);
    let manifest_a = manager
        .create_task_worktree("run-symref", "task-manifest-a", &exact_paths)
        .expect("create task worktree for manifest A");
    assert_eq!(manifest_a.base_commit, commit_a);

    fs::remove_file(env.repo_dir.join("crates/threadlane-workers/src/lib.rs"))
        .expect("remove lib.rs regular file");
    fs::create_dir_all(env.repo_dir.join("crates/threadlane-workers/src/lib.rs"))
        .expect("replace lib.rs with directory on main");
    fs::write(
        env.repo_dir
            .join("crates/threadlane-workers/src/lib.rs/child.rs"),
        "pub fn child() {}\n",
    )
    .expect("write lib.rs/child.rs");
    fs::create_dir_all(env.repo_dir.join("new.txt")).expect("create new.txt directory on main");
    fs::write(env.repo_dir.join("new.txt/child.txt"), "child\n").expect("write new.txt/child.txt");
    git_cmd(&env.repo_dir, &["add", "."]);
    git_cmd(
        &env.repo_dir,
        &[
            "commit",
            "-m",
            "Advance main to commit B where exact file paths are directories",
        ],
    );
    let commit_b = git_stdout(&env.repo_dir, &["rev-parse", "--verify", "main^{commit}"])
        .trim()
        .to_string();
    assert_ne!(commit_b, commit_a);

    // Confirm that unpinned `symbolic_spec` (`base_commit = "main"`) now resolves `main` to
    // `commit_b`, where `lib.rs` is a directory but the `manifest_a` checkout still has a regular
    // file there; the combined exact paths must fail closed, never widen.
    let checkout_str = manifest_a.checkout_path.to_str().expect("utf8 checkout");
    let unpinned_b_error = symbolic_spec
        .build_argv_for_workdir(&manifest_a.checkout_path)
        .expect_err(
            "unpinned symbolic_spec at commit B must fail closed against commit A checkout",
        );
    assert!(
        matches!(
            unpinned_b_error,
            WorkerError::InvalidSpec(ref msg) if msg.contains("non-directory")
        ),
        "expected non-directory InvalidSpec, got {unpinned_b_error:?}"
    );
    assert!(matches!(
        execute_worker_task(
            &symbolic_spec,
            "run-symref",
            "task-manifest-a",
            Some(&manifest_a.worktree_id),
            &mut cancel_rx,
        )
        .await,
        Err(WorkerError::InvalidSpec(ref msg)) if msg.contains("base_commit")
    ));

    // Separate absent-only control: `new.txt` is absent in commit A (no conflicting file
    // metadata) though it becomes a directory in commit B, so an unpinned spec limited to
    // `new.txt` may legitimately widen to descendant grants.
    let mut absent_only_spec = symbolic_spec.clone();
    absent_only_spec.allowed_write_paths = BTreeSet::from(["new.txt".to_string()]);
    let absent_only_argv = absent_only_spec
        .build_argv_for_workdir(&manifest_a.checkout_path)
        .expect("absent-only `new.txt` may broaden against commit A checkout");
    let absent_only_allow = parse_allow_rules(&absent_only_argv[11]);
    for tool in AGY_WRITE_PERMISSION_TOOLS {
        assert!(
            absent_only_allow.contains(&format!("{tool}({checkout_str}/new.txt/**)")),
            "absent-only `new.txt` must widen to `{tool}({checkout_str}/new.txt/**)`"
        );
    }

    // Ephemeral launch spec pinned to `manifest_a.base_commit` (the exact step performed by
    // `execute_worker_task_with_observer` before invoking `run_worker_with_observer`) must keep
    // `crates/threadlane-workers/src/lib.rs` and `new.txt` strictly exact (no `/**` grants),
    // even though `main` now resolves to `commit_b` where both paths are directories.
    let mut launch_spec = symbolic_spec.clone();
    launch_spec.base_commit = manifest_a.base_commit.clone();
    assert_eq!(symbolic_spec.base_commit, "main");
    assert_eq!(launch_spec.base_commit, commit_a);

    let launch_argv = launch_spec
        .build_argv_for_workdir(&manifest_a.checkout_path)
        .expect("build_argv_for_workdir for launch_spec pinned to manifest_a.base_commit");
    assert_eq!(launch_argv[10], "--settings");

    let allow = parse_allow_rules(&launch_argv[11]);
    let mut expected_allow = vec![format!("{AGY_READ_PERMISSION_TOOL}({checkout_str}/**)")];
    for rel in ["crates/threadlane-workers/src/lib.rs", "new.txt"] {
        for tool in AGY_WRITE_PERMISSION_TOOLS {
            expected_allow.push(format!("{tool}({checkout_str}/{rel})"));
        }
    }
    assert_eq!(allow, expected_allow);
    for rel in ["crates/threadlane-workers/src/lib.rs", "new.txt"] {
        for tool in AGY_WRITE_PERMISSION_TOOLS {
            assert!(
                !allow
                    .iter()
                    .any(|rule| rule.starts_with(&format!("{tool}({checkout_str}/{rel}/"))),
                "moving symbolic ref `main` from commit A to directory commit B must never widen manifest-pinned `{rel}` to `{tool}({checkout_str}/{rel}/**)`"
            );
        }
    }

    let manifest_a_exec = execute_worker_task(
        &launch_spec,
        "run-symref",
        "task-manifest-a",
        Some(&manifest_a.worktree_id),
        &mut cancel_rx,
    )
    .await
    .expect("execute_worker_task on manifest_a with pinned launch_spec");
    assert_eq!(
        manifest_a_exec.worker_report.status,
        WorkerRunStatus::Succeeded
    );
    assert_eq!(manifest_a_exec.worker_report.constructed_argv, launch_argv);
}

#[test]
fn task_files_contract_pins_canonical_checkout_against_source_repo_and_approval_metadata() {
    let env = ScopedTestEnv::new();
    let noop_bin = env.write_fake_executable("canonical-contract-agy", "exit 0");
    let allowed_write_paths = env.default_allowed_paths();

    let manager = WorktreeManager::new(&env.repo_dir, &env.work_area, &env.base_commit)
        .expect("create WorktreeManager for canonical task checkout test");
    let manifest = manager
        .create_task_worktree("run-20261109", "task-impl-01", &allowed_write_paths)
        .expect("create canonical task worktree");
    let canonical_checkout = &manifest.checkout_path;
    assert_ne!(
        canonical_checkout, &env.repo_dir,
        "task checkout location must be distinct from source repository location"
    );

    let canonical_checkout_str = canonical_checkout
        .to_str()
        .expect("utf8 canonical task checkout");
    let source_repo_str = env.repo_dir.to_str().expect("utf8 source repo");
    let decoy_approval_checkout = env
        .root_dir
        .join("threadlane-appr-9941-alpha-job-7732-maint/implementation");
    let decoy_approval_checkout_str = decoy_approval_checkout
        .to_str()
        .expect("utf8 decoy approval checkout");
    assert_ne!(canonical_checkout_str, decoy_approval_checkout_str);

    let caller_prompt = format!(
        "Context metadata (untrusted caller notes):\n\
         approval_id: appr-9941-alpha\n\
         job_id: job-7732-maint\n\
         worktree_hint: {decoy_approval_checkout_str}\n\
         source_repository: {source_repo_str}\n\
         Task: inspect crates/threadlane-workers/src/lib.rs and update docs/WORKERS.md.\n"
    );

    let spec = env.antigravity_spec(noop_bin, &caller_prompt, WorkerFilePermissions::TaskFiles);
    let argv = spec
        .build_argv_for_workdir(canonical_checkout)
        .expect("build_argv_for_workdir for canonical task checkout");
    assert_eq!(argv.len(), 14);
    assert_eq!(argv[10], "--settings");
    assert_eq!(argv[12], "--print");

    let (contract_header, retained_prompt) = argv[13]
        .split_once("[THREADLANE_TASK_INSTRUCTION] ")
        .expect("TaskFiles launch prompt must contain [THREADLANE_TASK_INSTRUCTION] boundary");
    assert!(
        contract_header.starts_with("[THREADLANE_AGY_TASK_FILES_CONTRACT] "),
        "authoritative contract prefix must remain intact"
    );
    assert_eq!(
        retained_prompt, caller_prompt,
        "verbatim caller task prompt must be preserved byte-for-byte after the boundary"
    );

    // Authoritative contract header must state the exact canonical task checkout root and never
    // substitute the source repository or misleading approval/job/worktree paths.
    let quoted_canonical_root = format!("`{canonical_checkout_str}`");
    assert!(
        contract_header.contains(&quoted_canonical_root),
        "contract header must state the exact canonical task checkout root `{canonical_checkout_str}`"
    );
    assert!(
        !contract_header.contains(source_repo_str),
        "contract header must never contain source repository path `{source_repo_str}`"
    );
    assert!(
        !contract_header.contains(decoy_approval_checkout_str),
        "contract header must never contain caller approval/job decoy path `{decoy_approval_checkout_str}`"
    );

    // Contract header must guide relative path preference, absolute path construction by joining
    // the exact canonical root with the repo-relative path, non-inference from approval/job/worktree
    // identifiers or source-repo paths, and avoiding command/pwd/list_dir dependency.
    let canonical_join_prefix = format!("{canonical_checkout_str}/");
    assert!(
        contract_header.contains("relative"),
        "contract header must guide preferring repository-relative paths"
    );
    assert!(
        contract_header.contains(&canonical_join_prefix) && contract_header.contains("join"),
        "contract header must explain deriving absolute tool paths only by joining the canonical root with the repo-relative path"
    );
    assert!(
        contract_header.contains("approval")
            && contract_header.contains("job")
            && contract_header.contains("worktree")
            && contract_header.contains("source"),
        "contract header must forbid inferring checkout names from approval/job/worktree identifiers or source-repo paths"
    );
    assert!(
        contract_header.contains("`pwd`")
            && contract_header.contains("`command`")
            && contract_header.contains("`list_dir`"),
        "contract header must avoid depending on command/pwd/list_dir permission to discover the checkout root"
    );

    // Permission JSON must remain unchanged: only canonical read_file(<canonical_checkout>/**)
    // plus exact finite write scopes (directory subtree vs exact file).
    let settings_val: Value =
        serde_json::from_str(&argv[11]).expect("parse --settings inline JSON");
    let perms_obj = settings_val["permissions"]
        .as_object()
        .expect("permissions object");
    assert_eq!(
        perms_obj.len(),
        1,
        "TaskFiles permissions object must only contain `allow`"
    );
    let allow = parse_allow_rules(&argv[11]);
    let mut expected_allow = vec![format!(
        "{AGY_READ_PERMISSION_TOOL}({canonical_checkout_str}/**)"
    )];
    for tool in AGY_WRITE_PERMISSION_TOOLS {
        expected_allow.push(format!(
            "{tool}({canonical_checkout_str}/crates/threadlane-workers)"
        ));
        expected_allow.push(format!(
            "{tool}({canonical_checkout_str}/crates/threadlane-workers/**)"
        ));
    }
    for tool in AGY_WRITE_PERMISSION_TOOLS {
        expected_allow.push(format!("{tool}({canonical_checkout_str}/docs/WORKERS.md)"));
    }
    assert_eq!(allow, expected_allow);
    assert!(
        allow.iter().all(|rule| {
            !rule.contains(source_repo_str) && !rule.contains(decoy_approval_checkout_str)
        }),
        "permission allowlist must never grant access to source repo or decoy approval checkout"
    );
}

#[test]
fn opencode_task_files_prompt_pins_actual_worktree_scopes_without_changing_permissions_or_other_modes(
) {
    let env = ScopedTestEnv::new();
    let noop_bin = env.write_fake_executable("opencode-prompt-contract", "exit 0");
    let allowed_write_paths = env.default_allowed_paths();

    let manager = WorktreeManager::new(&env.repo_dir, &env.work_area, &env.base_commit)
        .expect("create WorktreeManager for OpenCode prompt contract test");
    let manifest = manager
        .create_task_worktree(
            "run-opencode-prompt",
            "task-opencode-prompt",
            &allowed_write_paths,
        )
        .expect("create OpenCode task worktree");
    let checkout = &manifest.checkout_path;
    assert_ne!(
        checkout, &env.repo_dir,
        "task checkout must be distinct from the source repository"
    );

    let checkout_str = checkout
        .to_str()
        .expect("utf8 task checkout for OpenCode prompt test");
    let source_repo_str = env.repo_dir.to_str().expect("utf8 source repository");
    let decoy_checkout = env
        .root_dir
        .join("threadlane-appr-opencode-11-job-22-maint/implementation");
    let decoy_checkout_str = decoy_checkout
        .to_str()
        .expect("utf8 decoy OpenCode checkout");

    let caller_prompt = format!(
        "Context metadata (untrusted caller notes):\n\
         approval_id: appr-opencode-11\n\
         job_id: job-opencode-22\n\
         worktree_hint: {decoy_checkout_str}\n\
         source_repository: {source_repo_str}\n\
         Task: update docs/WORKERS.md and crates/threadlane-workers/src/lib.rs.\n"
    );
    let spec = env.opencode_spec(noop_bin, &caller_prompt, WorkerFilePermissions::TaskFiles);
    let spec_json_before = serde_json::to_vec(&spec).expect("serialize OpenCode spec before argv");
    let instruction_digest_before = sha256_hex(spec.prompt.as_bytes());
    let config_before = spec
        .build_opencode_config_for_workdir(checkout)
        .expect("build OpenCode permission/model config before argv");

    let argv = spec
        .build_argv_for_workdir(checkout)
        .expect("build OpenCode task_files argv");
    assert_eq!(
        spec_json_before,
        serde_json::to_vec(&spec).expect("serialize OpenCode spec after argv"),
        "argv construction must not mutate the WorkerSpec"
    );
    assert_eq!(
        instruction_digest_before,
        sha256_hex(spec.prompt.as_bytes()),
        "original task instruction digest must remain unchanged"
    );
    assert_eq!(
        config_before,
        spec.build_opencode_config_for_workdir(checkout)
            .expect("rebuild OpenCode permission/model config after argv"),
        "generated permission/model configuration must remain identical"
    );

    assert_eq!(argv.len(), 10);
    assert_eq!(argv[0], "--standalone");
    assert_eq!(argv[1], "run");
    assert_eq!(argv[2], "--model");
    assert_eq!(argv[3], "opencode/deepseek-4.1-flash#max");
    assert_eq!(argv[4], "--format");
    assert_eq!(argv[5], "json");
    assert_eq!(argv[6], "--session");
    assert_eq!(argv[7], "sess-201");
    assert_eq!(argv[8], "--");
    assert!(!argv
        .iter()
        .any(|arg| arg == "--settings" || arg == "--auto" || arg.contains("dangerously")));
    let effective_prompt = argv.last().expect("OpenCode effective prompt argument");
    let (contract_header, retained_prompt) = effective_prompt
        .split_once("[THREADLANE_TASK_INSTRUCTION] ")
        .expect("OpenCode task_files prompt must contain the task instruction boundary");
    assert!(
        contract_header.starts_with("[THREADLANE_OPENCODE_TASK_FILES_CONTRACT] "),
        "OpenCode task_files prompt must start with the session-local contract"
    );
    assert_eq!(
        retained_prompt, caller_prompt,
        "caller prompt must remain verbatim after the contract boundary"
    );
    assert_eq!(
        effective_prompt
            .matches("[THREADLANE_TASK_INSTRUCTION] ")
            .count(),
        1,
        "contract must contain exactly one task instruction boundary"
    );

    assert!(
        contract_header.contains(&format!("`{checkout_str}`")),
        "contract must state the exact actual task working directory"
    );
    assert!(
        !contract_header.contains(source_repo_str),
        "contract must never substitute the source repository path"
    );
    assert!(
        !contract_header.contains(decoy_checkout_str),
        "contract must never infer paths from approval/job/worktree identifiers"
    );
    assert!(contract_header.contains("Prefer repository-relative paths"));
    assert!(
        contract_header.contains(&format!("`{checkout_str}/<repo-relative-path>`")),
        "contract must derive absolute paths only by joining the exact task root"
    );
    assert!(
        contract_header.contains(
            "[`crates/threadlane-workers/` (directory subtree `crates/threadlane-workers` and `crates/threadlane-workers/**`), `docs/WORKERS.md` (exact file only; no descendant `/**`)]"
        ),
        "contract must distinguish explicit directory subtrees from exact files"
    );
    assert!(contract_header
        .contains("OpenCode built-in file tools `read`, `glob`, `grep`, `edit`, and `write`"));
    assert!(
        !contract_header.contains("read_file")
            && !contract_header.contains("edit_file")
            && !contract_header.contains("view_file")
            && !contract_header.contains("list_dir"),
        "OpenCode contract must list only OpenCode built-in file tools"
    );
    for forbidden in [
        "shell/terminal",
        "builds",
        "tests",
        "formatters",
        "`git`",
        "subagents",
        "web/browser",
        "MCP tools",
        "parent controller runs all checks and Git operations",
        "temporary files",
        "`AGENTS.md`",
        "`.env*`",
        "outside the declared write scopes",
        "read-only",
    ] {
        assert!(
            contract_header.contains(forbidden),
            "contract must explain `{forbidden}`"
        );
    }

    let config_value: Value =
        serde_json::from_str(&config_before).expect("parse OpenCode permission/model config");
    assert_eq!(config_value["update"], "disable");
    assert_eq!(config_value["default_agent"], "build");
    assert_eq!(config_value["model"]["providerID"], "opencode");
    assert_eq!(config_value["model"]["model"], "deepseek-4.1-flash");
    assert_eq!(config_value["model"]["variant"], "max");
    assert_eq!(
        config_value["agents"]
            .as_object()
            .expect("OpenCode pinned agents")
            .len(),
        4
    );

    let content = spec
        .build_opencode_config_content_for_workdir(checkout)
        .expect("build structured OpenCode config");
    for allowed_edit in [
        "crates/threadlane-workers/src/lib.rs",
        "crates/threadlane-workers/new_module.rs",
        "docs/WORKERS.md",
    ] {
        assert_eq!(
            content.evaluate_permission(None, "edit", allowed_edit),
            OpenCodeRuleEffect::Allow,
            "declared scope `{allowed_edit}` must remain writable"
        );
    }
    for denied_edit in [
        "docs/WORKERS.md/child.md",
        "crates/threadlane-core/src/lib.rs",
        "external_resolution.rs",
        "newmodule.rs",
        "AGENTS.md",
        ".env",
        ".git/config",
        ".opencode/config.json",
        ".local/state",
        "/tmp/outside",
    ] {
        assert_eq!(
            content.evaluate_permission(None, "edit", denied_edit),
            OpenCodeRuleEffect::Deny,
            "unapproved or protected path `{denied_edit}` must remain denied"
        );
        assert_eq!(
            content.evaluate_permission(Some("build"), "edit", denied_edit),
            OpenCodeRuleEffect::Deny,
            "build-agent writes to `{denied_edit}` must remain denied"
        );
    }
    assert_eq!(
        content.evaluate_permission(None, "read", ".env"),
        OpenCodeRuleEffect::Deny
    );
    for denied_action in ["shell", "subagent", "web"] {
        assert_eq!(
            content.evaluate_permission(None, denied_action, "git status"),
            OpenCodeRuleEffect::Deny,
            "OpenCode task_files must not grant `{denied_action}`"
        );
    }

    // Antigravity TaskFiles keeps its existing contract bytes and never receives the OpenCode
    // prologue.
    let agy_bin = env.write_fake_executable("opencode-repair-noop-agy", "exit 0");
    let agy_workdir = env.root_dir.join("opencode-repair-agy-workdir");
    fs::create_dir_all(&agy_workdir).expect("create AGY unaffected workdir");
    let agy_spec = env.antigravity_spec(
        agy_bin,
        "AGY contract unchanged",
        WorkerFilePermissions::TaskFiles,
    );
    let agy_argv = agy_spec
        .build_argv_for_workdir(&agy_workdir)
        .expect("build unchanged AGY task_files argv");
    let agy_prompt = agy_argv.last().expect("AGY prompt argument");
    assert!(agy_prompt.starts_with("[THREADLANE_AGY_TASK_FILES_CONTRACT] "));
    assert!(!agy_prompt.contains("[THREADLANE_OPENCODE_TASK_FILES_CONTRACT]"));
    assert_eq!(
        agy_prompt
            .split_once("[THREADLANE_TASK_INSTRUCTION] ")
            .expect("AGY task instruction boundary")
            .1,
        "AGY contract unchanged"
    );

    // OpenCode Inherit keeps the bare prompt and no permission rules.
    let inherit_bin = env.write_fake_executable("opencode-repair-noop-inherit", "exit 0");
    let inherit_spec = env.opencode_spec(
        inherit_bin,
        "Inherit prompt unchanged",
        WorkerFilePermissions::Inherit,
    );
    let inherit_argv = inherit_spec
        .build_argv_for_workdir(checkout)
        .expect("build unchanged OpenCode inherit argv");
    assert_eq!(
        inherit_argv.last().map(String::as_str),
        Some("Inherit prompt unchanged")
    );
    assert!(!inherit_argv
        .iter()
        .any(|arg| arg.contains("THREADLANE_OPENCODE_TASK_FILES_CONTRACT")));
    let inherit_content = inherit_spec
        .build_opencode_config_content_for_workdir(checkout)
        .expect("build unchanged OpenCode inherit config");
    assert!(inherit_content.permissions.is_none());

    // OpenCode read-only advisory keeps the bare prompt and read-only permissions.
    let advisory_bin = env.write_fake_executable("opencode-repair-noop-advisory", "exit 0");
    let advisory_dir = env.root_dir.join("opencode-repair-advisory");
    fs::create_dir_all(&advisory_dir).expect("create OpenCode advisory workdir");
    let advisory_spec = AdvisoryWorkerSpec {
        provider: WorkerProvider::OpenCodeV2,
        executable: advisory_bin,
        model: "opencode/deepseek-4.1-flash".to_string(),
        effort: Some("max".to_string()),
        allowed_models: BTreeSet::new(),
        session_id: None,
        standalone: true,
        working_directory: advisory_dir.clone(),
        allowed_write_paths: BTreeSet::new(),
        prompt: "Advisory prompt unchanged".to_string(),
        file_permissions: WorkerFilePermissions::ReadOnly,
        budgets: ExecutionBudgets::default(),
    };
    let advisory_argv = advisory_spec
        .build_argv_for_workdir(&advisory_dir)
        .expect("build unchanged OpenCode advisory argv");
    assert_eq!(
        advisory_argv.last().map(String::as_str),
        Some("Advisory prompt unchanged")
    );
    assert!(!advisory_argv
        .iter()
        .any(|arg| arg.contains("THREADLANE_OPENCODE_TASK_FILES_CONTRACT")));
    let advisory_content = advisory_spec
        .build_opencode_config_content()
        .expect("build unchanged OpenCode advisory config");
    assert_eq!(
        advisory_content.evaluate_permission(None, "read", "src/lib.rs"),
        OpenCodeRuleEffect::Allow
    );
    assert_eq!(
        advisory_content.evaluate_permission(None, "edit", "src/lib.rs"),
        OpenCodeRuleEffect::Deny
    );
    assert_eq!(
        advisory_content.evaluate_permission(None, "shell", "git status"),
        OpenCodeRuleEffect::Deny
    );
}

#[tokio::test]
async fn opencode_task_files_effective_prompt_bound_counts_canonical_cwd_and_explicit_scopes_before_provider_spawn(
) {
    let env = ScopedTestEnv::new();
    let boundary_marker = env.root_dir.join("opencode_boundary_spawned.marker");
    let over_marker = env.root_dir.join("opencode_over_bound_spawned.marker");
    let boundary_bin = env.write_fake_executable(
        "opencode-boundary-bin",
        &format!(
            r#"
printf 'spawned\n' > '{}'
printf '{{"type":"step_start","sessionID":"sess-201","part":{{"type":"step-start","model":"opencode/deepseek-4.1-flash#max"}}}}\n'
printf '{{"type":"step_finish","sessionID":"sess-201","part":{{"type":"step-finish","reason":"stop"}}}}\n'
"#,
            boundary_marker.display()
        ),
    );
    let over_bin = env.write_fake_executable(
        "opencode-over-bound-bin",
        &format!(
            r#"
printf 'spawned\n' > '{}'
printf '{{"type":"step_start","sessionID":"sess-201","part":{{"type":"step-start","model":"opencode/deepseek-4.1-flash#max"}}}}\n'
printf '{{"type":"step_finish","sessionID":"sess-201","part":{{"type":"step-finish","reason":"stop"}}}}\n'
"#,
            over_marker.display()
        ),
    );

    let long_workdir = env
        .root_dir
        .join(format!("canonical-cwd-{}", "a".repeat(96)))
        .join(format!("nested-{}", "b".repeat(96)));
    fs::create_dir_all(&long_workdir).expect("create long canonical workdir");
    let long_workdir_str = long_workdir.to_str().expect("utf8 long canonical workdir");

    let mut explicit_scopes = BTreeSet::new();
    for index in 0..4 {
        explicit_scopes.insert(format!(
            "crates/{}/directory-{index}/",
            "scope-dir-".repeat(8)
        ));
        explicit_scopes.insert(format!("docs/{}/file-{index}.md", "scope-file-".repeat(8)));
    }
    assert_eq!(explicit_scopes.len(), 8);
    assert!(explicit_scopes.iter().any(|scope| scope.ends_with('/')));
    assert!(explicit_scopes.iter().any(|scope| !scope.ends_with('/')));

    let mut spec = env.opencode_spec(
        boundary_bin.clone(),
        "probe",
        WorkerFilePermissions::TaskFiles,
    );
    spec.allowed_write_paths = explicit_scopes.clone();

    let probe_argv = spec
        .build_argv_for_workdir(&long_workdir)
        .expect("probe actual OpenCode task_files argv");
    let probe_effective_prompt = probe_argv.last().expect("probe effective prompt");
    assert!(probe_effective_prompt.ends_with("probe"));
    assert!(probe_effective_prompt.contains(long_workdir_str));
    for scope in &explicit_scopes {
        let clean_scope = scope.strip_suffix('/').unwrap_or(scope.as_str());
        assert!(
            probe_effective_prompt.contains(clean_scope),
            "prologue must include explicit scope `{scope}` counted by the effective prompt bound"
        );
    }
    let prologue_and_boundary_bytes = probe_effective_prompt
        .len()
        .checked_sub("probe".len())
        .expect("probe prompt suffix is present");
    let boundary_prompt_len = MAX_PROMPT_BYTES
        .checked_sub(prologue_and_boundary_bytes)
        .expect("canonical prologue and scopes must leave room inside MAX_PROMPT_BYTES");
    let boundary_prompt = "b".repeat(boundary_prompt_len);
    assert!(
        boundary_prompt.len() <= MAX_PROMPT_BYTES,
        "the original prompt stays within the pre-existing prompt cap; only the effective prompt reaches the boundary"
    );

    let mut boundary_spec = spec.clone();
    boundary_spec.executable = boundary_bin;
    boundary_spec.prompt = boundary_prompt.clone();
    let boundary_spec_json_before =
        serde_json::to_vec(&boundary_spec).expect("serialize boundary spec before argv");
    let boundary_digest_before = sha256_hex(boundary_spec.prompt.as_bytes());
    let boundary_config_before = boundary_spec
        .build_opencode_config_for_workdir(&long_workdir)
        .expect("build boundary OpenCode config before argv");

    let boundary_argv = boundary_spec
        .build_argv_for_workdir(&long_workdir)
        .expect("effective prompt exactly at MAX_PROMPT_BYTES must be accepted");
    let boundary_effective_prompt = boundary_argv.last().expect("boundary effective prompt");
    assert_eq!(boundary_effective_prompt.len(), MAX_PROMPT_BYTES);
    let (_, boundary_retained_prompt) = boundary_effective_prompt
        .split_once("[THREADLANE_TASK_INSTRUCTION] ")
        .expect("boundary prompt must retain the task instruction boundary");
    assert_eq!(boundary_retained_prompt, boundary_prompt);

    let boundary_report = run_worker(&boundary_spec, &long_workdir)
        .await
        .expect("boundary-sized effective OpenCode task_files prompt must launch");
    assert_eq!(boundary_report.status, WorkerRunStatus::Succeeded);
    assert!(
        boundary_marker.exists(),
        "boundary-sized effective prompt must reach the provider"
    );
    assert_eq!(boundary_spec.prompt, boundary_prompt);
    assert_eq!(
        boundary_spec_json_before,
        serde_json::to_vec(&boundary_spec).expect("serialize boundary spec after argv")
    );
    assert_eq!(
        boundary_digest_before,
        sha256_hex(boundary_spec.prompt.as_bytes())
    );
    assert_eq!(
        boundary_config_before,
        boundary_spec
            .build_opencode_config_for_workdir(&long_workdir)
            .expect("rebuild boundary OpenCode config after argv")
    );

    let mut over_spec = spec;
    over_spec.executable = over_bin;
    over_spec.prompt = "c".repeat(boundary_prompt_len + 1);
    assert!(
        over_spec.prompt.len() <= MAX_PROMPT_BYTES,
        "the over-bound fixture must fail on the effective prompt cap, not the original prompt cap"
    );
    let over_prompt = over_spec.prompt.clone();
    let over_spec_json_before =
        serde_json::to_vec(&over_spec).expect("serialize over-bound spec before argv");
    let over_digest_before = sha256_hex(over_spec.prompt.as_bytes());

    let over_argv_error = over_spec
        .build_argv_for_workdir(&long_workdir)
        .expect_err("effective prompt one byte over MAX_PROMPT_BYTES must fail before argv");
    assert!(matches!(
        over_argv_error,
        WorkerError::InvalidSpec(ref msg)
            if msg.contains("effective OpenCode task_files prompt")
                && msg.contains(&MAX_PROMPT_BYTES.to_string())
    ));

    let over_run_error = run_worker(&over_spec, &long_workdir)
        .await
        .expect_err("over-bound effective prompt must fail before provider spawn");
    assert!(matches!(over_run_error, WorkerError::InvalidSpec(_)));
    assert!(
        !over_marker.exists(),
        "over-bound effective prompt must never reach the provider"
    );
    assert_eq!(over_spec.prompt, over_prompt);
    assert_eq!(
        over_spec_json_before,
        serde_json::to_vec(&over_spec).expect("serialize over-bound spec after rejection")
    );
    assert_eq!(over_digest_before, sha256_hex(over_spec.prompt.as_bytes()));
}

#[tokio::test]
async fn opencode_task_files_effective_prompt_and_config_reach_fake_cli_in_actual_task_worktree() {
    let env = ScopedTestEnv::new();
    let capture_bin = env.write_fake_executable(
        "opencode-task-files-capture",
        r#"
for arg in "$@"; do
    printf '%s\000' "$arg" >> opencode_received_argv.bin
done
printf '%s' "$OPENCODE_CONFIG_CONTENT" > opencode_received_config.json
printf '{"type":"step_start","sessionID":"sess-201","part":{"type":"step-start","model":"opencode/deepseek-4.1-flash#max"}}\n'
printf '{"type":"step_finish","sessionID":"sess-201","part":{"type":"step-finish","reason":"stop"}}\n'
"#,
    );

    let manager = WorktreeManager::new(&env.repo_dir, &env.work_area, &env.base_commit)
        .expect("create WorktreeManager for OpenCode capture test");
    let manifest = manager
        .create_task_worktree(
            "run-opencode-capture",
            "task-opencode-capture",
            &env.default_allowed_paths(),
        )
        .expect("create OpenCode capture task worktree");
    let checkout = &manifest.checkout_path;
    assert_ne!(
        checkout, &env.repo_dir,
        "fake CLI must run inside the actual task worktree, not the source repository"
    );

    let source_repo_str = env.repo_dir.to_str().expect("utf8 source repository");
    let decoy_checkout = env
        .root_dir
        .join("threadlane-appr-opencode-33-job-44-maint/implementation");
    let decoy_checkout_str = decoy_checkout
        .to_str()
        .expect("utf8 decoy OpenCode capture checkout");
    let caller_prompt = format!(
        "Line 1: update docs/WORKERS.md; worktree_hint: {decoy_checkout_str}; source_repository: {source_repo_str}; preserve exact bytes & symbols!\n\
Line 2 — naïve café, 日本語, кириллица, हिन्दी, emoji 🚀.\n\
Line 3:\ttabs, spaces, quotes \" and backslashes \\\\ stay exact.\n\
Line 4: the final trailing newline follows.\n"
    );
    let spec = env.opencode_spec(
        capture_bin,
        &caller_prompt,
        WorkerFilePermissions::TaskFiles,
    );
    let expected_argv = spec
        .build_argv_for_workdir(checkout)
        .expect("build expected OpenCode task_files argv");
    let expected_config = spec
        .build_opencode_config_for_workdir(checkout)
        .expect("build expected OpenCode permission/model config");

    let report = run_worker(&spec, checkout)
        .await
        .expect("run OpenCode task_files worker with fake CLI");
    assert_eq!(report.status, WorkerRunStatus::Succeeded);
    assert_eq!(
        report.constructed_argv, expected_argv,
        "supervised launch argv must preserve the task_files contract"
    );

    let received_argv_bytes = read_nul_delimited_argv(&checkout.join("opencode_received_argv.bin"));
    let expected_argv_bytes: Vec<Vec<u8>> = expected_argv
        .iter()
        .map(|arg| arg.as_bytes().to_vec())
        .collect();
    assert_eq!(
        received_argv_bytes, expected_argv_bytes,
        "NUL-delimited capture must preserve every actual argv byte exactly, including multiline Unicode prompt bytes"
    );
    let captured_config = fs::read_to_string(checkout.join("opencode_received_config.json"))
        .expect("read captured OPENCODE_CONFIG_CONTENT");
    assert_eq!(
        captured_config, expected_config,
        "child OpenCode process must receive the unchanged permission/model config bytes"
    );

    let effective_prompt_bytes = received_argv_bytes
        .last()
        .expect("captured effective prompt bytes");
    let boundary: &[u8] = b"[THREADLANE_TASK_INSTRUCTION] ";
    let boundary_index = effective_prompt_bytes
        .windows(boundary.len())
        .position(|window| window == boundary)
        .expect("captured prompt must contain the task instruction boundary");
    assert_eq!(
        effective_prompt_bytes
            .windows(boundary.len())
            .filter(|window| *window == boundary)
            .count(),
        1,
        "captured prompt must contain exactly one task instruction boundary"
    );
    let contract_header_bytes = &effective_prompt_bytes[..boundary_index];
    let retained_prompt_bytes = &effective_prompt_bytes[boundary_index + boundary.len()..];
    assert_eq!(
        retained_prompt_bytes,
        caller_prompt.as_bytes(),
        "captured prompt must preserve the original multiline Unicode task instruction byte-for-byte"
    );
    let contract_header =
        std::str::from_utf8(contract_header_bytes).expect("UTF-8 captured contract header");
    assert!(contract_header.starts_with("[THREADLANE_OPENCODE_TASK_FILES_CONTRACT] "));
    assert_eq!(
        contract_header
            .matches("[THREADLANE_OPENCODE_TASK_FILES_CONTRACT]")
            .count(),
        1
    );
    assert!(contract_header.contains(&format!(
        "`{}`",
        checkout
            .to_str()
            .expect("utf8 captured task worktree checkout")
    )));
    assert!(!contract_header.contains(source_repo_str));
    assert!(!contract_header.contains(decoy_checkout_str));
    assert!(contract_header.contains(
        "[`crates/threadlane-workers/` (directory subtree `crates/threadlane-workers` and `crates/threadlane-workers/**`), `docs/WORKERS.md` (exact file only; no descendant `/**`)]"
    ));
}
