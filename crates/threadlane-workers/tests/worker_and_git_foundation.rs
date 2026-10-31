#![cfg(unix)]
#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use threadlane_workers::{
    cleanup_worktree, evaluate_diff_gate, evaluate_diff_gate_with_limits,
    evaluate_diff_gate_with_scan_limits, evaluate_diff_gate_with_timeout, execute_worker_task,
    execute_worker_task_with_observer, is_process_alive, is_process_group_alive,
    load_and_verify_manifest, run_cli, run_worker, run_worker_with_cancel,
    run_worker_with_observer, BlockedKind, CleanupRefusalReason, CleanupRequest, DiffChangeKind,
    DiffViolationKind, ExecutionBudgets, FieldEvidence, IsolationLevel, NormalizedEventKind,
    OutputOverflowKind, WorkAcceptanceState, WorkerCli, WorkerCommand, WorkerError,
    WorkerExecutionReport, WorkerFilePermissions, WorkerPolicy, WorkerProvider, WorkerRunStatus,
    WorkerSpec, WorktreeManager, MAX_PROMPT_BYTES, WORK_AREA_MARKER_FILENAME,
};
use tokio::sync::{mpsc, watch};

struct TestEnv {
    _temp_dir: TempDir,
    root_dir: PathBuf,
    repo_dir: PathBuf,
    work_area: PathBuf,
    bin_dir: PathBuf,
    base_commit: String,
}

impl TestEnv {
    fn new() -> Self {
        let temp_dir = TempDir::new().expect("create temp dir");
        let root_dir = fs::canonicalize(temp_dir.path()).expect("canonicalize temp dir");
        let repo_dir = root_dir.join("repo");
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
            "pub fn initial() -> u32 { 1 }\n",
        )
        .expect("write workers lib.rs");
        fs::write(
            workers_src.join("old_name.rs"),
            "pub const RENAME_ME: &str = \"hello\";\n",
        )
        .expect("write old_name.rs");
        fs::write(core_src.join("lib.rs"), "pub fn core_fn() {}\n").expect("write core lib.rs");
        fs::write(repo_dir.join("AGENTS.md"), "# Control rules\n").expect("write AGENTS.md");
        fs::write(repo_dir.join(".gitignore"), "ignored_secret.txt\n").expect("write .gitignore");

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

    fn antigravity_spec(&self, executable: PathBuf, prompt: &str) -> WorkerSpec {
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
            file_permissions: WorkerFilePermissions::Inherit,
            budgets: ExecutionBudgets::default(),
        }
    }

    fn opencode_spec(&self, executable: PathBuf, prompt: &str) -> WorkerSpec {
        WorkerSpec {
            provider: WorkerProvider::OpenCodeV2,
            executable,
            model: "opencode/deepseek-4.1-flash".to_string(),
            effort: Some("max".to_string()),
            allowed_models: BTreeSet::from([
                "opencode/deepseek-4.1-flash".to_string(),
                "opencode/deepseek-4.1-flash#max".to_string(),
            ]),
            session_id: Some("sess-101".to_string()),
            standalone: true,
            repository_path: self.repo_dir.clone(),
            base_commit: self.base_commit.clone(),
            work_area_root: self.work_area.clone(),
            allowed_write_paths: self.default_allowed_paths(),
            prompt: prompt.to_string(),
            file_permissions: WorkerFilePermissions::Inherit,
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

#[tokio::test]
async fn argv_construction_and_spec_validation_exact_for_antigravity_and_opencode() {
    let env = TestEnv::new();
    let agy_bin = env.write_fake_executable(
        "fake-agy",
        r#"
for arg in "$@"; do
    printf '%s\n' "$arg" >> received_argv.txt
done
printf '{"type":"system","subtype":"init","session_id":"s-agy","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"result","subtype":"success","session_id":"s-agy","model":"gemini-4-argon-high","effort":"high","result":"done"}\n'
"#,
    );

    let agy_spec = env.antigravity_spec(agy_bin.clone(), "Implement task alpha");
    let agy_argv = agy_spec.build_argv().expect("build agy argv");
    assert_eq!(
        agy_argv,
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
            "Implement task alpha",
        ]
    );
    assert!(!agy_argv
        .iter()
        .any(|a| a.contains("dangerously") || a == "--auto" || a == "--variant"));

    // Verify actual process receives the exact argv array without shell splitting on spaces.
    let workdir = env.root_dir.join("agy-run-dir");
    fs::create_dir_all(&workdir).expect("create workdir");
    let agy_report = run_worker(&agy_spec, &workdir).await.expect("run agy");
    assert_eq!(agy_report.status, WorkerRunStatus::Succeeded);
    let recorded_args: Vec<String> = fs::read_to_string(workdir.join("received_argv.txt"))
        .expect("read received_argv.txt")
        .lines()
        .map(str::to_string)
        .collect();
    assert_eq!(recorded_args, agy_argv);

    // OpenCode v2 with standalone + session + effort/variant
    let oc_bin = env.write_fake_executable(
        "fake-opencode",
        r#"
for arg in "$@"; do
    printf '%s\n' "$arg" >> received_oc_argv.txt
done
printf '{"type":"step_start","sessionID":"sess-101","part":{"type":"step-start","model":"opencode/deepseek-4.1-flash#max"}}\n'
printf '{"type":"step_finish","sessionID":"sess-101","part":{"type":"step-finish","reason":"stop"}}\n'
"#,
    );
    let oc_spec = env.opencode_spec(oc_bin.clone(), "Fix bug in parser");
    let oc_argv = oc_spec.build_argv().expect("build opencode argv");
    assert_eq!(
        oc_argv,
        vec![
            "--standalone",
            "run",
            "--model",
            "opencode/deepseek-4.1-flash#max",
            "--format",
            "json",
            "--session",
            "sess-101",
            "--",
            "Fix bug in parser",
        ]
    );
    assert!(!oc_argv.iter().any(|a| a == "--variant" || a == "--effort"));

    let oc_report = run_worker(&oc_spec, &workdir).await.expect("run opencode");
    assert_eq!(oc_report.status, WorkerRunStatus::Succeeded);
    let recorded_oc_args: Vec<String> = fs::read_to_string(workdir.join("received_oc_argv.txt"))
        .expect("read received_oc_argv.txt")
        .lines()
        .map(str::to_string)
        .collect();
    assert_eq!(recorded_oc_args, oc_argv);

    // Arbitrary non-catalog model/effort identifiers must work without silent replacement.
    let mut custom_spec = env.opencode_spec(oc_bin.clone(), "Custom model test");
    custom_spec.model = "custom-org/custom-coder-2026#xhigh".to_string();
    custom_spec.effort = None;
    custom_spec.session_id = None;
    custom_spec.allowed_models.clear();
    let custom_argv = custom_spec
        .build_argv()
        .expect("build custom opencode argv");
    assert_eq!(
        custom_argv,
        vec![
            "--standalone",
            "run",
            "--model",
            "custom-org/custom-coder-2026#xhigh",
            "--format",
            "json",
            "--",
            "Custom model test",
        ]
    );

    // Conflicting inline `#variant` vs `effort` field must fail validation.
    let mut conflict_spec = custom_spec.clone();
    conflict_spec.effort = Some("low".to_string());
    assert!(matches!(
        conflict_spec.validate(),
        Err(WorkerError::InvalidSpec(_))
    ));

    // Disallowed model and disallowed executable must fail policy validation.
    let policy = WorkerPolicy {
        allowed_executables: BTreeSet::from([agy_bin.clone()]),
        allowed_models: BTreeSet::from(["gemini-4-argon-high".to_string()]),
    };
    assert!(matches!(
        custom_spec.validate_against_policy(&policy),
        Err(WorkerError::DisallowedExecutable { .. })
    ));
    let mut disallowed_model_spec = agy_spec.clone();
    disallowed_model_spec.allowed_models = BTreeSet::from(["other-model".to_string()]);
    assert!(matches!(
        disallowed_model_spec.validate(),
        Err(WorkerError::DisallowedModel { .. })
    ));
}

#[tokio::test]
async fn model_mismatch_missing_model_and_effort_mismatch_rejected_even_on_zero_exit() {
    let env = TestEnv::new();
    let workdir = env.root_dir.join("evidence-dir");
    fs::create_dir_all(&workdir).expect("create workdir");

    // 1. Model mismatch (exits 0 with result event, but observed model differs from requested)
    let mismatch_bin = env.write_fake_executable(
        "mismatch-agy",
        r#"
printf '{"type":"system","subtype":"init","model":"gemini-4-flash","effort":"high"}\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-flash","effort":"high","result":"ok"}\n'
"#,
    );
    let spec = env.antigravity_spec(mismatch_bin, "Check model mismatch");
    let report = run_worker(&spec, &workdir)
        .await
        .expect("run mismatch worker");
    assert_eq!(report.exit_code, Some(0));
    assert_eq!(report.status, WorkerRunStatus::ModelMismatch);
    assert!(matches!(
        report.evidence.model_evidence,
        FieldEvidence::Mismatch { .. }
    ));

    // 2. Missing model evidence (exits 0 with result event, but no model reported)
    let no_model_bin = env.write_fake_executable(
        "no-model-agy",
        r#"
printf '{"type":"system","subtype":"init","effort":"high"}\n'
printf '{"type":"result","subtype":"success","effort":"high","result":"ok"}\n'
"#,
    );
    let spec = env.antigravity_spec(no_model_bin, "Check missing model");
    let report = run_worker(&spec, &workdir)
        .await
        .expect("run no-model worker");
    assert_eq!(report.exit_code, Some(0));
    assert_eq!(report.status, WorkerRunStatus::MissingModelEvidence);
    assert!(matches!(
        report.evidence.model_evidence,
        FieldEvidence::Missing { .. }
    ));

    // 3. Effort/variant mismatch on OpenCode v2
    let effort_mismatch_bin = env.write_fake_executable(
        "effort-mismatch-oc",
        r#"
printf '{"type":"step_start","sessionID":"sess-101","part":{"type":"step-start","model":"opencode/deepseek-4.1-flash#low"}}\n'
printf '{"type":"step_finish","sessionID":"sess-101","part":{"type":"step-finish","reason":"stop"}}\n'
"#,
    );
    let oc_spec = env.opencode_spec(effort_mismatch_bin, "Check effort mismatch");
    let report = run_worker(&oc_spec, &workdir)
        .await
        .expect("run effort-mismatch worker");
    assert_eq!(report.exit_code, Some(0));
    assert_eq!(report.status, WorkerRunStatus::EffortMismatch);
    assert!(matches!(
        report.evidence.effort_evidence,
        FieldEvidence::Mismatch { .. }
    ));

    // 4. Missing effort evidence when effort was explicitly requested
    let missing_effort_bin = env.write_fake_executable(
        "missing-effort-agy",
        r#"
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high"}\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-argon-high","result":"ok"}\n'
"#,
    );
    let spec = env.antigravity_spec(missing_effort_bin, "Check missing effort");
    let report = run_worker(&spec, &workdir)
        .await
        .expect("run missing-effort worker");
    assert_eq!(report.exit_code, Some(0));
    assert_eq!(report.status, WorkerRunStatus::MissingEffortEvidence);

    // 5. Missing terminal result event (exits 0 after init)
    let no_result_bin = env.write_fake_executable(
        "no-result-agy",
        r#"
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
"#,
    );
    let spec = env.antigravity_spec(no_result_bin, "Check missing result");
    let report = run_worker(&spec, &workdir)
        .await
        .expect("run missing-result worker");
    assert_eq!(report.exit_code, Some(0));
    assert_eq!(report.status, WorkerRunStatus::MissingResultEvidence);
}

#[tokio::test]
async fn provider_error_with_zero_exit_and_nonzero_exit_are_rejected_and_secrets_redacted() {
    let env = TestEnv::new();
    let workdir = env.root_dir.join("error-dir");
    fs::create_dir_all(&workdir).expect("create workdir");

    // Provider emits error event and still exits with code 0
    let err_zero_bin = env.write_fake_executable(
        "err-zero-agy",
        r#"
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"error","error":{"code":"auth_failed","message":"failed with token=sk-1234567890abcdef and Bearer secret_jwt_token_value"}}\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-argon-high","effort":"high","result":"ok"}\n'
exit 0
"#,
    );
    let spec = env.antigravity_spec(err_zero_bin, "Trigger provider error on zero exit");
    let report = run_worker(&spec, &workdir)
        .await
        .expect("run err-zero worker");
    assert_eq!(report.exit_code, Some(0));
    assert_eq!(report.status, WorkerRunStatus::ProviderError);
    assert_eq!(report.evidence.provider_errors.len(), 1);
    assert_eq!(
        report.evidence.provider_errors[0].code.as_deref(),
        Some("auth_failed")
    );
    assert!(report.evidence.provider_errors[0]
        .message
        .contains("[REDACTED]"));
    assert!(!report.evidence.provider_errors[0]
        .message
        .contains("sk-1234567890abcdef"));
    assert!(!report.evidence.provider_errors[0]
        .message
        .contains("secret_jwt_token_value"));

    // Non-zero exit code without provider error event
    let nonzero_bin = env.write_fake_executable(
        "nonzero-agy",
        r#"
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf 'fatal crash with api_key=AIzaSySecretKeyValue12345\n' >&2
exit 17
"#,
    );
    let spec = env.antigravity_spec(nonzero_bin, "Trigger non-zero exit");
    let report = run_worker(&spec, &workdir)
        .await
        .expect("run nonzero worker");
    assert_eq!(report.exit_code, Some(17));
    assert_eq!(report.status, WorkerRunStatus::NonZeroExit);
    let stderr_summary = report
        .redacted_stderr_summary
        .expect("stderr summary present");
    assert!(stderr_summary.contains("[REDACTED]"));
    assert!(!stderr_summary.contains("AIzaSySecretKeyValue12345"));
}

#[tokio::test]
async fn malformed_ndjson_and_oversized_stdout_line_total_and_stderr_enforced() {
    let env = TestEnv::new();
    let workdir = env.root_dir.join("stream-limits-dir");
    fs::create_dir_all(&workdir).expect("create workdir");

    // 1. Malformed NDJSON line on stdout
    let malformed_bin = env.write_fake_executable(
        "malformed-agy",
        r#"
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf 'THIS IS NOT JSON {{{{\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-argon-high","effort":"high","result":"ok"}\n'
"#,
    );
    let spec = env.antigravity_spec(malformed_bin, "Check malformed NDJSON");
    let report = run_worker(&spec, &workdir)
        .await
        .expect("run malformed worker");
    assert_eq!(report.status, WorkerRunStatus::MalformedStream);
    assert_eq!(report.evidence.malformed_lines_count, 1);

    // 2. Single stdout line exceeds max_line_bytes
    let long_line_bin = env.write_fake_executable(
        "long-line-agy",
        r#"
printf '{"type":"assistant","text":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}\n'
"#,
    );
    let mut line_spec = env.antigravity_spec(long_line_bin, "Check line overflow");
    line_spec.budgets.max_line_bytes = 64;
    line_spec.budgets.max_stdout_bytes = 4096;
    let report = run_worker(&line_spec, &workdir)
        .await
        .expect("run line overflow worker");
    assert_eq!(report.status, WorkerRunStatus::OutputOverflow);
    assert_eq!(
        report.overflow,
        Some(OutputOverflowKind::StdoutLineExceeded { max_line_bytes: 64 })
    );

    // 3. Total stdout exceeds max_stdout_bytes
    let total_stdout_bin = env.write_fake_executable(
        "total-stdout-agy",
        r#"
printf '{"type":"assistant","text":"chunk-one-12345678901234567890"}\n'
printf '{"type":"assistant","text":"chunk-two-12345678901234567890"}\n'
printf '{"type":"assistant","text":"chunk-three-123456789012345678"}\n'
"#,
    );
    let mut total_spec = env.antigravity_spec(total_stdout_bin, "Check total stdout overflow");
    total_spec.budgets.max_line_bytes = 128;
    total_spec.budgets.max_stdout_bytes = 140;
    let report = run_worker(&total_spec, &workdir)
        .await
        .expect("run total stdout overflow worker");
    assert_eq!(report.status, WorkerRunStatus::OutputOverflow);
    assert_eq!(
        report.overflow,
        Some(OutputOverflowKind::StdoutTotalExceeded {
            max_stdout_bytes: 140
        })
    );

    // 4. Total stderr exceeds max_stderr_bytes
    let stderr_overflow_bin = env.write_fake_executable(
        "stderr-overflow-agy",
        r#"
printf 'stderr-overflow-line-12345678901234567890123456789012345678901234567890\n' >&2
printf 'stderr-overflow-line-12345678901234567890123456789012345678901234567890\n' >&2
"#,
    );
    let mut stderr_spec = env.antigravity_spec(stderr_overflow_bin, "Check stderr overflow");
    stderr_spec.budgets.max_stderr_bytes = 80;
    let report = run_worker(&stderr_spec, &workdir)
        .await
        .expect("run stderr overflow worker");
    assert_eq!(report.status, WorkerRunStatus::OutputOverflow);
    assert_eq!(
        report.overflow,
        Some(OutputOverflowKind::StderrTotalExceeded {
            max_stderr_bytes: 80
        })
    );
}

#[tokio::test]
async fn blocked_tool_permission_and_user_input_exposed_as_blocked() {
    let env = TestEnv::new();
    let workdir = env.root_dir.join("blocked-dir");
    fs::create_dir_all(&workdir).expect("create workdir");

    // 1. Antigravity permission block
    let perm_bin = env.write_fake_executable(
        "perm-block-agy",
        r#"
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"permission_request","tool":"run_command","message":"Approval required to execute shell command"}\n'
exit 0
"#,
    );
    let spec = env.antigravity_spec(perm_bin, "Attempt restricted tool");
    let report = run_worker(&spec, &workdir)
        .await
        .expect("run perm-block worker");
    assert_eq!(report.status, WorkerRunStatus::Blocked);
    assert_eq!(report.evidence.blocked_events.len(), 1);
    assert_eq!(
        report.evidence.blocked_events[0].kind,
        BlockedKind::PermissionRequired
    );
    assert_eq!(
        report.evidence.blocked_events[0].tool_name.as_deref(),
        Some("run_command")
    );

    // 2. OpenCode v2 user question / input required block
    let input_bin = env.write_fake_executable(
        "input-block-oc",
        r#"
printf '{"type":"step_start","sessionID":"sess-101","part":{"type":"step-start","model":"opencode/deepseek-4.1-flash#max"}}\n'
printf '{"type":"question","sessionID":"sess-101","question":"Please clarify target module name"}\n'
exit 0
"#,
    );
    let oc_spec = env.opencode_spec(input_bin, "Ambiguous prompt");
    let report = run_worker(&oc_spec, &workdir)
        .await
        .expect("run input-block worker");
    assert_eq!(report.status, WorkerRunStatus::Blocked);
    assert_eq!(report.evidence.blocked_events.len(), 1);
    assert_eq!(
        report.evidence.blocked_events[0].kind,
        BlockedKind::UserInputRequired
    );
}

#[tokio::test]
async fn timeout_and_explicit_cancel_terminate_process_group_and_descendants() {
    let env = TestEnv::new();
    let workdir = env.root_dir.join("timeout-cancel-dir");
    fs::create_dir_all(&workdir).expect("create workdir");

    // 1. Timeout kills both parent and spawned descendant
    let timeout_bin = env.write_fake_executable(
        "timeout-worker",
        r#"
sleep 30 &
desc_pid=$!
printf '%s\n' "$desc_pid" > timeout_descendant.pid
sleep 30
"#,
    );
    let mut timeout_spec = env.antigravity_spec(timeout_bin, "Timeout test");
    timeout_spec.budgets.timeout_ms = 200;
    let report = run_worker(&timeout_spec, &workdir)
        .await
        .expect("run timeout worker");
    assert_eq!(report.status, WorkerRunStatus::TimedOut);

    let desc_pid: u32 = fs::read_to_string(workdir.join("timeout_descendant.pid"))
        .expect("read timeout_descendant.pid")
        .trim()
        .parse()
        .expect("parse descendant pid");
    assert!(
        !is_process_alive(desc_pid),
        "descendant process {desc_pid} must be terminated"
    );
    if let Some(pgid) = report.process_group_id {
        assert!(
            !is_process_group_alive(pgid),
            "process group {pgid} must be terminated"
        );
    }

    // 2. Explicit cancellation kills both parent and spawned descendant
    let cancel_bin = env.write_fake_executable(
        "cancel-worker",
        r#"
sleep 30 &
desc_pid=$!
printf '%s\n' "$desc_pid" > cancel_descendant.pid
sleep 30
"#,
    );
    let mut cancel_spec = env.antigravity_spec(cancel_bin, "Cancel test");
    cancel_spec.budgets.timeout_ms = 10_000;

    let (cancel_tx, mut cancel_rx) = watch::channel(false);
    let pid_file = workdir.join("cancel_descendant.pid");
    tokio::spawn(async move {
        for _ in 0..100 {
            if pid_file.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = cancel_tx.send(true);
    });

    let report = run_worker_with_cancel(&cancel_spec, &workdir, &mut cancel_rx)
        .await
        .expect("run cancelled worker");
    assert_eq!(report.status, WorkerRunStatus::Cancelled);

    let cancel_desc_pid: u32 = fs::read_to_string(workdir.join("cancel_descendant.pid"))
        .expect("read cancel_descendant.pid")
        .trim()
        .parse()
        .expect("parse cancel descendant pid");
    assert!(
        !is_process_alive(cancel_desc_pid),
        "cancelled descendant {cancel_desc_pid} must be terminated"
    );
    if let Some(pgid) = report.process_group_id {
        assert!(
            !is_process_group_alive(pgid),
            "cancelled process group {pgid} must be terminated"
        );
    }
}

#[tokio::test]
async fn descendant_holding_pipes_and_surviving_parent_exit_does_not_hang_and_leaves_no_orphans() {
    let env = TestEnv::new();
    let workdir = env.root_dir.join("pipe-holder-dir");
    fs::create_dir_all(&workdir).expect("create workdir");

    // Parent emits valid NDJSON, spawns a background child that inherits open stdout/stderr
    // pipes and sleeps for 30s, then parent exits 0 immediately.
    let pipe_holder_bin = env.write_fake_executable(
        "pipe-holder-worker",
        r#"
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-argon-high","effort":"high","result":"completed"}\n'
sleep 30 &
desc_pid=$!
printf '%s\n' "$desc_pid" > pipe_descendant.pid
exit 0
"#,
    );

    let mut spec = env.antigravity_spec(pipe_holder_bin, "Pipe holder test");
    spec.budgets.timeout_ms = 5_000;

    let report = run_worker(&spec, &workdir)
        .await
        .expect("run pipe-holder worker without hanging");
    assert_eq!(report.status, WorkerRunStatus::Succeeded);
    assert!(
        report.descendants_terminated_after_exit,
        "supervisor must detect and terminate surviving descendant holding pipes"
    );
    assert!(
        report.duration_ms < 2_500,
        "supervisor must not hang waiting for 30s descendant pipe close (took {} ms)",
        report.duration_ms
    );

    let desc_pid: u32 = fs::read_to_string(workdir.join("pipe_descendant.pid"))
        .expect("read pipe_descendant.pid")
        .trim()
        .parse()
        .expect("parse descendant pid");
    assert!(
        !is_process_alive(desc_pid),
        "surviving pipe-holding descendant {desc_pid} must be terminated and reaped"
    );
    if let Some(pgid) = report.process_group_id {
        assert!(
            !is_process_group_alive(pgid),
            "process group {pgid} must have no remaining alive processes"
        );
    }
}

#[test]
fn path_traversal_symlink_and_dirty_target_rejected_without_data_loss() {
    let env = TestEnv::new();
    let real_bin = env.write_fake_executable("real-bin", "exit 0");

    // 1. Symlink executable must be rejected
    let symlink_bin = env.bin_dir.join("symlink-bin");
    symlink(&real_bin, &symlink_bin).expect("create symlink executable");
    let sym_spec = env.antigravity_spec(symlink_bin, "Symlink exec test");
    assert!(matches!(
        sym_spec.validate(),
        Err(WorkerError::SecurityViolation(_))
    ));

    // 2. Path traversal `..` in allowed_write_paths or work_area_root must be rejected
    let mut traversal_spec = env.antigravity_spec(real_bin.clone(), "Traversal test");
    traversal_spec.allowed_write_paths = BTreeSet::from(["../escape".to_string()]);
    assert!(matches!(
        traversal_spec.validate(),
        Err(WorkerError::SecurityViolation(_))
    ));

    // 3. Control config in allowed_write_paths (`AGENTS.md` or `.git/config`) must be rejected
    let mut control_spec = env.antigravity_spec(real_bin, "Control allowlist test");
    control_spec.allowed_write_paths = BTreeSet::from(["AGENTS.md".to_string()]);
    assert!(matches!(
        control_spec.validate(),
        Err(WorkerError::InvalidSpec(_))
    ));

    // 4. Symlink work_area_root must be rejected by WorktreeManager
    let real_work_area = env.root_dir.join("real-work-area");
    fs::create_dir_all(&real_work_area).expect("create real_work_area");
    let sym_work_area = env.root_dir.join("sym-work-area");
    symlink(&real_work_area, &sym_work_area).expect("create symlink work_area");
    assert!(matches!(
        WorktreeManager::new(&env.repo_dir, &sym_work_area, &env.base_commit),
        Err(WorkerError::SecurityViolation(_))
    ));

    // 5. Dirty / pre-existing target checkout directory must be detected and preserved untouched
    let manager =
        WorktreeManager::new(&env.repo_dir, &env.work_area, &env.base_commit).expect("new manager");
    let preexisting_checkout = env
        .work_area
        .join("checkouts/tasks/wt-task-run1-task1-fixedtok");
    fs::create_dir_all(&preexisting_checkout).expect("create preexisting target");
    let precious_file = preexisting_checkout.join("user_uncommitted.txt");
    fs::write(&precious_file, "do not delete me").expect("write precious_file");

    let err = manager
        .create_task_worktree_with_token("run1", "task1", "fixedtok", &env.default_allowed_paths())
        .expect_err("must refuse pre-existing target directory");
    assert!(matches!(err, WorkerError::TargetAlreadyExists(_)));
    assert_eq!(
        fs::read_to_string(&precious_file).expect("precious_file preserved"),
        "do not delete me"
    );
}

#[test]
fn diff_gate_reports_renames_untracked_deletes_binaries_and_rejects_violations() {
    let env = TestEnv::new();
    let manager =
        WorktreeManager::new(&env.repo_dir, &env.work_area, &env.base_commit).expect("new manager");

    // Part A: Allowed changes (modify, rename, binary untracked, new doc)
    let task_wt = manager
        .create_task_worktree("run-a", "task-1", &env.default_allowed_paths())
        .expect("create task worktree");

    let wt_workers_src = task_wt.checkout_path.join("crates/threadlane-workers/src");
    fs::write(
        wt_workers_src.join("lib.rs"),
        "pub fn initial() -> u32 { 2 }\n",
    )
    .expect("modify lib.rs");

    git_cmd(
        &task_wt.checkout_path,
        &[
            "mv",
            "crates/threadlane-workers/src/old_name.rs",
            "crates/threadlane-workers/src/new_name.rs",
        ],
    );

    let bin_file = task_wt
        .checkout_path
        .join("crates/threadlane-workers/fixture.bin");
    fs::write(&bin_file, [0u8, 159u8, 146u8, 150u8, 0u8]).expect("write binary file");

    let docs_dir = task_wt.checkout_path.join("docs");
    fs::create_dir_all(&docs_dir).expect("create docs dir");
    fs::write(docs_dir.join("WORKERS.md"), "# Workers documentation\n")
        .expect("write docs/WORKERS.md");

    let gate_ok = evaluate_diff_gate(&env.work_area, &task_wt.worktree_id)
        .expect("evaluate passing diff gate");
    assert!(
        gate_ok.passed,
        "expected diff gate to pass: {:?}",
        gate_ok.violations
    );
    assert_eq!(gate_ok.isolation_level, IsolationLevel::PostRunDiffEvidence);
    assert!(!gate_ok.preventive_sandbox);
    assert_eq!(
        gate_ok.acceptance_state,
        WorkAcceptanceState::DiffGatePassedPendingReview
    );

    assert!(gate_ok.changed_paths.iter().any(|e| {
        e.path == "crates/threadlane-workers/src/lib.rs"
            && e.change_kind == DiffChangeKind::Modified
            && !e.is_binary
    }));
    assert!(gate_ok.changed_paths.iter().any(|e| {
        e.path == "crates/threadlane-workers/src/new_name.rs"
            && e.orig_path.as_deref() == Some("crates/threadlane-workers/src/old_name.rs")
            && e.change_kind == DiffChangeKind::Renamed
    }));
    assert!(gate_ok.changed_paths.iter().any(|e| {
        e.path == "crates/threadlane-workers/fixture.bin"
            && e.change_kind == DiffChangeKind::Untracked
            && e.is_binary
    }));
    assert!(gate_ok
        .changed_paths
        .iter()
        .any(|e| { e.path == "docs/WORKERS.md" && e.change_kind == DiffChangeKind::Untracked }));

    // Part B: Violations (outside allowed paths, gitignored untracked file, symlink, control config)
    let bad_wt = manager
        .create_task_worktree("run-b", "task-2", &env.default_allowed_paths())
        .expect("create second task worktree");

    // 1. Modify file outside allowed_write_paths
    fs::write(
        bad_wt
            .checkout_path
            .join("crates/threadlane-core/src/lib.rs"),
        "// unauthorized edit\n",
    )
    .expect("write unauthorized core edit");

    // 2. Create file matching `.gitignore` outside allowed_write_paths
    fs::write(
        bad_wt.checkout_path.join("ignored_secret.txt"),
        "hidden untracked file\n",
    )
    .expect("write ignored file");

    // 3. Create symlink inside allowed_write_paths
    symlink(
        "/etc/hosts",
        bad_wt
            .checkout_path
            .join("crates/threadlane-workers/escape_link"),
    )
    .expect("create symlink inside worktree");

    // 4. Modify control config `AGENTS.md`
    fs::write(bad_wt.checkout_path.join("AGENTS.md"), "# Tampered rules\n")
        .expect("tamper AGENTS.md");

    let gate_bad = evaluate_diff_gate(&env.work_area, &bad_wt.worktree_id)
        .expect("evaluate failing diff gate");
    assert!(!gate_bad.passed);
    assert_eq!(
        gate_bad.acceptance_state,
        WorkAcceptanceState::RejectedByDiffGate
    );

    assert!(gate_bad.violations.iter().any(|v| {
        v.path == "crates/threadlane-core/src/lib.rs"
            && v.kind == DiffViolationKind::OutsideAllowedPaths
    }));
    assert!(gate_bad.violations.iter().any(|v| {
        v.path == "ignored_secret.txt" && v.kind == DiffViolationKind::OutsideAllowedPaths
    }));
    assert!(gate_bad.violations.iter().any(|v| {
        v.path == "crates/threadlane-workers/escape_link"
            && v.kind == DiffViolationKind::SymlinkDetected
    }));
    assert!(gate_bad
        .violations
        .iter()
        .any(|v| { v.path == "AGENTS.md" && v.kind == DiffViolationKind::ControlConfigModified }));

    // Verify files in bad_wt were NOT deleted or reverted (preserved for review)
    assert!(bad_wt
        .checkout_path
        .join("crates/threadlane-core/src/lib.rs")
        .exists());
    assert!(bad_wt.checkout_path.join("ignored_secret.txt").exists());
}

#[tokio::test]
async fn no_global_git_mutation_and_canonical_repo_checkout_untouched() {
    let env = TestEnv::new();

    // Dirty the user's primary repository checkout before any worktree operations.
    let user_dirty_tracked = env.repo_dir.join("crates/threadlane-core/src/lib.rs");
    fs::write(&user_dirty_tracked, "pub fn user_in_progress_edit() {}\n")
        .expect("write user dirty edit");
    let user_untracked = env.repo_dir.join("user_scratch_notes.txt");
    fs::write(&user_untracked, "local notes\n").expect("write user untracked file");

    let branch_before = git_stdout(&env.repo_dir, &["symbolic-ref", "--short", "HEAD"]);
    let head_before = git_stdout(&env.repo_dir, &["rev-parse", "HEAD"]);
    let status_before = git_stdout(&env.repo_dir, &["status", "--porcelain=v1"]);

    let manager =
        WorktreeManager::new(&env.repo_dir, &env.work_area, &env.base_commit).expect("new manager");
    let _run_wt = manager
        .create_run_worktree("run-isolation")
        .expect("create run worktree");

    let worker_bin = env.write_fake_executable(
        "isolated-worker",
        r#"
printf 'pub fn worker_added() {}\n' >> crates/threadlane-workers/src/lib.rs
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-argon-high","effort":"high","result":"done"}\n'
"#,
    );
    let spec = env.antigravity_spec(worker_bin, "Update workers lib.rs");
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let task_report = execute_worker_task(&spec, "run-isolation", "task-1", None, &mut cancel_rx)
        .await
        .expect("execute_worker_task");

    assert_eq!(task_report.worker_report.status, WorkerRunStatus::Succeeded);
    assert!(task_report.diff_gate.passed);
    assert_eq!(
        task_report.overall_acceptance_state,
        WorkAcceptanceState::DiffGatePassedPendingReview
    );

    // Canonical repository checkout must remain 100% untouched.
    let branch_after = git_stdout(&env.repo_dir, &["symbolic-ref", "--short", "HEAD"]);
    let head_after = git_stdout(&env.repo_dir, &["rev-parse", "HEAD"]);
    let status_after = git_stdout(&env.repo_dir, &["status", "--porcelain=v1"]);

    assert_eq!(branch_before, branch_after);
    assert_eq!(head_before, head_after);
    assert_eq!(status_before, status_after);
    assert_eq!(
        fs::read_to_string(&user_dirty_tracked).expect("read user tracked"),
        "pub fn user_in_progress_edit() {}\n"
    );
    assert_eq!(
        fs::read_to_string(&user_untracked).expect("read user untracked"),
        "local notes\n"
    );
}

#[test]
fn safe_cleanup_refuses_dirty_active_and_unexpected_branch_then_succeeds_when_clean() {
    let env = TestEnv::new();
    let manager =
        WorktreeManager::new(&env.repo_dir, &env.work_area, &env.base_commit).expect("new manager");
    let manifest = manager
        .create_task_worktree("run-clean", "task-clean", &env.default_allowed_paths())
        .expect("create task worktree");

    // 1. Unknown worktree_id without private manifest is refused
    let unknown_err = cleanup_worktree(&CleanupRequest {
        work_area_root: env.work_area.clone(),
        worktree_id: "wt-task-nonexistent".to_string(),
    })
    .expect_err("must refuse missing manifest");
    assert!(matches!(
        unknown_err,
        WorkerError::CleanupRefused {
            reason: CleanupRefusalReason::MissingOrInvalidManifest,
            ..
        }
    ));

    // 2. Active job guard held -> refused with ActiveJob
    let guard = manager
        .begin_job(&manifest.worktree_id)
        .expect("acquire job guard");
    let active_err = cleanup_worktree(&CleanupRequest {
        work_area_root: env.work_area.clone(),
        worktree_id: manifest.worktree_id.clone(),
    })
    .expect_err("must refuse active job");
    assert!(matches!(
        active_err,
        WorkerError::CleanupRefused {
            reason: CleanupRefusalReason::ActiveJob,
            ..
        }
    ));
    let _ = guard.finish().expect("finish job guard");

    // 3. Unexpected checked-out branch -> refused with UnexpectedBranchOrOwner
    git_cmd(
        &manifest.checkout_path,
        &["checkout", "-b", "unexpected/branch"],
    );
    let branch_err = cleanup_worktree(&CleanupRequest {
        work_area_root: env.work_area.clone(),
        worktree_id: manifest.worktree_id.clone(),
    })
    .expect_err("must refuse unexpected branch");
    assert!(matches!(
        branch_err,
        WorkerError::CleanupRefused {
            reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
            ..
        }
    ));
    git_cmd(
        &manifest.checkout_path,
        &["checkout", &manifest.branch_name],
    );
    git_cmd(
        &manifest.checkout_path,
        &["branch", "-d", "unexpected/branch"],
    );

    // 4. Dirty worktree (untracked file) -> refused with DirtyWorktree and file preserved
    let dirty_file = manifest
        .checkout_path
        .join("crates/threadlane-workers/unreviewed.txt");
    fs::write(&dirty_file, "unreviewed artifact").expect("write dirty_file");
    let dirty_err = cleanup_worktree(&CleanupRequest {
        work_area_root: env.work_area.clone(),
        worktree_id: manifest.worktree_id.clone(),
    })
    .expect_err("must refuse dirty worktree");
    assert!(matches!(
        dirty_err,
        WorkerError::CleanupRefused {
            reason: CleanupRefusalReason::DirtyWorktree,
            ..
        }
    ));
    assert!(dirty_file.exists(), "dirty artifact must be preserved");

    // 5. Once clean, inactive, and on expected branch -> cleanup succeeds
    fs::remove_file(&dirty_file).expect("remove dirty_file after review");
    let cleanup_report = cleanup_worktree(&CleanupRequest {
        work_area_root: env.work_area.clone(),
        worktree_id: manifest.worktree_id.clone(),
    })
    .expect("cleanup clean worktree");

    assert_eq!(cleanup_report.worktree_id, manifest.worktree_id);
    assert!(
        !manifest.checkout_path.exists(),
        "checkout directory removed"
    );
    assert!(!manifest.manifest_path.exists(), "private manifest removed");
    assert!(
        load_and_verify_manifest(&env.work_area, &manifest.worktree_id).is_err(),
        "manifest must no longer exist"
    );
}

#[tokio::test]
async fn diagnostic_cli_end_to_end_validate_create_run_diff_gate_and_cleanup() {
    let env = TestEnv::new();
    let worker_bin = env.write_fake_executable(
        "cli-worker",
        r#"
mkdir -p docs
printf '# CLI generated doc\n' > docs/WORKERS.md
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-argon-high","effort":"high","result":"done"}\n'
"#,
    );

    let spec = env.antigravity_spec(worker_bin, "Generate docs/WORKERS.md");
    let spec_path = env.root_dir.join("worker-spec.json");
    fs::write(
        &spec_path,
        serde_json::to_vec_pretty(&spec).expect("serialize spec"),
    )
    .expect("write spec file");

    // 1. ValidateSpec
    run_cli(WorkerCli {
        command: WorkerCommand::ValidateSpec {
            spec: spec_path.clone(),
        },
    })
    .await
    .expect("cli validate-spec");

    // 2. CreateWorktrees (run + task)
    let manager =
        WorktreeManager::new(&env.repo_dir, &env.work_area, &env.base_commit).expect("new manager");
    let task_wt = manager
        .create_task_worktree("cli-run", "cli-task", &env.default_allowed_paths())
        .expect("create task worktree");

    // 3. Run worker inside the owned task worktree via CLI
    run_cli(WorkerCli {
        command: WorkerCommand::Run {
            spec: spec_path,
            run_id: "cli-run".to_string(),
            task_id: "cli-task".to_string(),
            worktree_id: Some(task_wt.worktree_id.clone()),
        },
    })
    .await
    .expect("cli run");

    // 4. DiffGate via CLI
    run_cli(WorkerCli {
        command: WorkerCommand::DiffGate {
            work_area: env.work_area.clone(),
            worktree_id: task_wt.worktree_id.clone(),
        },
    })
    .await
    .expect("cli diff-gate");

    // 5. Cleanup via CLI must refuse while `docs/WORKERS.md` is uncommitted in the worktree
    let refuse_res = run_cli(WorkerCli {
        command: WorkerCommand::Cleanup {
            work_area: env.work_area.clone(),
            worktree_id: task_wt.worktree_id.clone(),
        },
    })
    .await;
    assert!(refuse_res.is_err(), "cleanup must refuse dirty worktree");
}

#[tokio::test]
async fn actual_installed_agy_stream_schema_and_denied_actions_and_recoverable_step_error() {
    let env = TestEnv::new();
    let workdir = env.root_dir.join("real-agy-schema-dir");
    fs::create_dir_all(&workdir).expect("create workdir");

    // 1. Real installed AGY stream schema (`event: "init" | "step_update" | "result"`, no effort emitted)
    let real_agy_bin = env.write_fake_executable(
        "real-schema-agy",
        r#"
printf 'pub fn real_schema_edit() {}\n' >> crates/threadlane-workers/src/lib.rs 2>/dev/null || true
printf '{"event":"init","conversation_id":"fixture-session","init":{"model":"gemini-4-argon-high","cwd":"/canonical/worktree","permission_mode":"request-review"}}\n'
printf '{"event":"step_update","step_update":{"conversation_id":"fixture-session","step_index":1,"state":"DONE","step_type":"agent_response","usage":{}}}\n'
printf '{"event":"result","result":{"conversation_id":"fixture-session","status":"SUCCESS","response":"Finished","num_turns":1}}\n'
"#,
    );
    let spec = env.antigravity_spec(real_agy_bin, "Run against real AGY schema");
    let report = run_worker(&spec, &workdir)
        .await
        .expect("run real-schema agy worker");
    assert_eq!(report.exit_code, Some(0));
    assert!(report.evidence.saw_init_event);
    assert!(report.evidence.saw_result_event);
    assert_eq!(
        report.evidence.observed_session_id.as_deref(),
        Some("fixture-session")
    );
    assert_eq!(
        report.evidence.model_evidence,
        FieldEvidence::VerifiedMatch {
            value: "gemini-4-argon-high".to_string(),
        }
    );
    // AGY reports its known concrete catalog variant, not independent reasoning telemetry.
    assert_eq!(
        report.evidence.effort_evidence,
        FieldEvidence::VerifiedMatch {
            value: "high".to_string(),
        }
    );
    assert_eq!(
        report.evidence.effort_evidence_source,
        Some(threadlane_workers::EffortEvidenceSource::AntigravityCatalogModelVariant)
    );
    assert_eq!(report.status, WorkerRunStatus::Succeeded);
    assert!(!report.status.is_completed_pending_effort_evidence());

    // A successful execution and clean scope still require explicit review before acceptance.
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let task_report = execute_worker_task(&spec, "real-agy", "task-1", None, &mut cancel_rx)
        .await
        .expect("execute_worker_task with real-schema agy");
    assert!(task_report.diff_gate.passed);
    assert_eq!(
        task_report.overall_acceptance_state,
        WorkAcceptanceState::DiffGatePassedPendingReview
    );

    // 2. Real AGY `result` with `denied_actions` even when `status: "SUCCESS"` and `response: ""`
    let denied_agy_bin = env.write_fake_executable(
        "denied-actions-agy",
        r#"
printf '{"event":"init","conversation_id":"fixture-session","init":{"model":"gemini-4-argon-high","cwd":"/canonical/worktree","permission_mode":"request-review"}}\n'
printf '{"event":"result","result":{"conversation_id":"fixture-session","status":"SUCCESS","response":"","denied_actions":[{"action":"read_file","display_name":"ReadFile"}]}}\n'
"#,
    );
    let denied_spec = env.antigravity_spec(denied_agy_bin, "Denied action test");
    let denied_report = run_worker(&denied_spec, &workdir)
        .await
        .expect("run denied-actions worker");
    assert_eq!(denied_report.status, WorkerRunStatus::Blocked);
    assert!(!denied_report.evidence.saw_result_event);
    assert_eq!(denied_report.evidence.blocked_events.len(), 1);
    assert_eq!(
        denied_report.evidence.blocked_events[0].kind,
        BlockedKind::PermissionRequired
    );
    assert_eq!(
        denied_report.evidence.blocked_events[0]
            .tool_name
            .as_deref(),
        Some("read_file")
    );
    assert!(denied_report.evidence.blocked_events[0]
        .summary
        .contains("read_file"));

    // 3. Real AGY `result` with nested `status: "FAILURE"`
    let failure_agy_bin = env.write_fake_executable(
        "failure-result-agy",
        r#"
printf '{"event":"init","conversation_id":"fixture-session","init":{"model":"gemini-4-argon-high","cwd":"/canonical/worktree","permission_mode":"request-review"}}\n'
printf '{"event":"result","result":{"conversation_id":"fixture-session","status":"FAILURE","error":"backend quota exceeded","num_turns":1}}\n'
"#,
    );
    let failure_spec = env.antigravity_spec(failure_agy_bin, "Failure result test");
    let failure_report = run_worker(&failure_spec, &workdir)
        .await
        .expect("run failure-result worker");
    assert_eq!(failure_report.status, WorkerRunStatus::ProviderError);
    assert!(!failure_report.evidence.saw_result_event);
    assert_eq!(failure_report.evidence.provider_errors.len(), 1);
    assert!(failure_report.evidence.provider_errors[0]
        .message
        .contains("backend quota exceeded"));

    // 4. Recoverable step tool error in `step_update` does not poison a later valid `result`
    let recoverable_step_bin = env.write_fake_executable(
        "recoverable-step-agy",
        r#"
printf '{"event":"init","conversation_id":"fixture-session","init":{"model":"gemini-4-argon-high","cwd":"/canonical/worktree","permission_mode":"accept-edits"}}\n'
printf '{"event":"step_update","step_update":{"conversation_id":"fixture-session","step_index":1,"state":"ERROR","step_type":"tool_call","error":"transient command failure"}}\n'
printf '{"event":"step_update","step_update":{"conversation_id":"fixture-session","step_index":2,"state":"DONE","step_type":"agent_response","usage":{}}}\n'
printf '{"event":"result","result":{"conversation_id":"fixture-session","status":"SUCCESS","response":"Recovered and completed","num_turns":2}}\n'
"#,
    );
    let rec_spec = env.antigravity_spec(recoverable_step_bin, "Recoverable step test");
    let rec_report = run_worker(&rec_spec, &workdir)
        .await
        .expect("run recoverable-step worker");
    assert_eq!(rec_report.evidence.recoverable_step_errors_count, 1);
    assert!(rec_report.evidence.provider_errors.is_empty());
    assert!(rec_report.evidence.saw_init_event);
    assert!(rec_report.evidence.saw_result_event);
    assert_eq!(rec_report.status, WorkerRunStatus::Succeeded);
}

#[test]
fn existing_unrelated_work_area_root_refused_before_chmod_or_subdir_creation() {
    let env = TestEnv::new();

    // 1. Existing unrelated directory with `keep.txt` and mode 0755 must be refused untouched
    let unrelated_root = env.root_dir.join("unrelated-user-dir");
    fs::create_dir_all(&unrelated_root).expect("create unrelated_root");
    let keep_file = unrelated_root.join("keep.txt");
    fs::write(&keep_file, "sentinel user content").expect("write keep.txt");
    fs::set_permissions(&unrelated_root, fs::Permissions::from_mode(0o755))
        .expect("chmod 0755 unrelated_root");

    let err = WorktreeManager::new(&env.repo_dir, &unrelated_root, &env.base_commit)
        .expect_err("must refuse unrelated non-empty work_area_root");
    assert!(matches!(err, WorkerError::SecurityViolation(_)));

    let mode_after = fs::symlink_metadata(&unrelated_root)
        .expect("stat unrelated_root")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        mode_after, 0o755,
        "unrelated work_area_root permissions must not be chmodded to 0700"
    );
    assert_eq!(
        fs::read_to_string(&keep_file).expect("read keep.txt"),
        "sentinel user content"
    );
    assert!(!unrelated_root.join("manifests").exists());
    assert!(!unrelated_root.join("checkouts").exists());
    assert!(!unrelated_root.join(WORK_AREA_MARKER_FILENAME).exists());

    // 2. Invalid base_commit must fail before creating work_area_root at all
    let untouched_work_area = env.root_dir.join("never-created-work-area");
    assert!(WorktreeManager::new(
        &env.repo_dir,
        &untouched_work_area,
        "nonexistent-commit-ref"
    )
    .is_err());
    assert!(
        !untouched_work_area.exists(),
        "work_area_root must not be created when base_commit resolution fails"
    );

    // 3. Valid owned work_area_root establishes `.threadlane-work-area.json` and can be reused
    // for the same repository, but refuses reuse by another repository or if a nested dir has 0755.
    let manager1 =
        WorktreeManager::new(&env.repo_dir, &env.work_area, &env.base_commit).expect("manager1");
    assert!(env.work_area.join(WORK_AREA_MARKER_FILENAME).exists());
    let _manager2 =
        WorktreeManager::new(&env.repo_dir, manager1.work_area_root(), &env.base_commit)
            .expect("reuse owned work_area_root for same repo");

    let other_repo = env.root_dir.join("other-repo");
    fs::create_dir_all(&other_repo).expect("create other_repo");
    git_cmd(&other_repo, &["init", "-b", "main"]);
    git_cmd(
        &other_repo,
        &["config", "--local", "user.name", "Threadlane Test"],
    );
    git_cmd(
        &other_repo,
        &["config", "--local", "user.email", "test@threadlane.local"],
    );
    fs::write(other_repo.join("README.md"), "other\n").expect("write other README");
    git_cmd(&other_repo, &["add", "."]);
    git_cmd(&other_repo, &["commit", "-m", "Other init"]);
    let other_head = git_stdout(&other_repo, &["rev-parse", "HEAD"])
        .trim()
        .to_string();

    assert!(matches!(
        WorktreeManager::new(&other_repo, &env.work_area, &other_head),
        Err(WorkerError::SecurityViolation(_))
    ));

    // Nested directory with 0755 permissions must not be silently chmodded
    let manifests_dir = env.work_area.join("manifests");
    fs::set_permissions(&manifests_dir, fs::Permissions::from_mode(0o755))
        .expect("chmod 0755 manifests_dir");
    assert!(matches!(
        WorktreeManager::new(&env.repo_dir, &env.work_area, &env.base_commit),
        Err(WorkerError::SecurityViolation(_))
    ));
    let nested_mode = fs::symlink_metadata(&manifests_dir)
        .expect("stat manifests_dir")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(nested_mode, 0o755, "nested dir must not be chmodded");
}

#[tokio::test]
async fn opencode_option_terminator_prompt_ceiling_and_provider_serde_canonical_name() {
    let env = TestEnv::new();
    let oc_bin = env.write_fake_executable(
        "oc-terminator-bin",
        r#"
printf '{"type":"step_start","sessionID":"sess-101","part":{"type":"step-start","model":"opencode/deepseek-4.1-flash#max"}}\n'
printf '{"type":"step_finish","sessionID":"sess-101","part":{"type":"step-finish","reason":"stop"}}\n'
"#,
    );

    // 1. Prompt starting with `--auto` is preceded by `--` option terminator
    let mut flag_prompt_spec = env.opencode_spec(oc_bin.clone(), "--auto");
    flag_prompt_spec.session_id = None;
    let argv = flag_prompt_spec.build_argv().expect("build argv");
    assert_eq!(
        argv,
        vec![
            "--standalone",
            "run",
            "--model",
            "opencode/deepseek-4.1-flash#max",
            "--format",
            "json",
            "--",
            "--auto",
        ]
    );

    // 1b. OpenCode shared-daemon mode (`standalone: false`) must be rejected before launch
    // and before any work_area_root filesystem side effects.
    let mut shared_daemon_spec = flag_prompt_spec.clone();
    shared_daemon_spec.standalone = false;
    shared_daemon_spec.work_area_root = env.root_dir.join("uncreated-shared-daemon-work-area");
    let shared_err = shared_daemon_spec
        .validate()
        .expect_err("opencode_v2 with standalone=false must be rejected");
    assert!(matches!(shared_err, WorkerError::InvalidSpec(ref msg) if msg.contains("standalone")));
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    assert!(matches!(
        execute_worker_task(
            &shared_daemon_spec,
            "run-sd",
            "task-sd",
            None,
            &mut cancel_rx
        )
        .await,
        Err(WorkerError::InvalidSpec(_))
    ));
    assert!(
        !shared_daemon_spec.work_area_root.exists(),
        "work_area_root must not be created when OpenCode standalone=false is rejected"
    );

    // 2. Prompt byte ceiling (`MAX_PROMPT_BYTES == 65_536`) enforced before launch,
    // and 64 KiB prompt launches cleanly on Linux without E2BIG.
    assert_eq!(MAX_PROMPT_BYTES, 65_536);
    let mut over_limit_spec = env.opencode_spec(oc_bin.clone(), &"x".repeat(MAX_PROMPT_BYTES + 1));
    assert!(matches!(
        over_limit_spec.validate(),
        Err(WorkerError::InvalidSpec(_))
    ));
    over_limit_spec.prompt = "x".repeat(262_144);
    assert!(matches!(
        over_limit_spec.validate(),
        Err(WorkerError::InvalidSpec(_))
    ));

    let max_ok_spec = env.opencode_spec(oc_bin, &"a".repeat(MAX_PROMPT_BYTES));
    let workdir = env.root_dir.join("max-prompt-dir");
    fs::create_dir_all(&workdir).expect("create max-prompt-dir");
    let max_report = run_worker(&max_ok_spec, &workdir)
        .await
        .expect("64 KiB prompt must execute without E2BIG");
    assert_eq!(max_report.status, WorkerRunStatus::Succeeded);

    // 3. Canonical serde representation for `WorkerProvider::OpenCodeV2` is `"opencode_v2"`
    let serialized =
        serde_json::to_string(&WorkerProvider::OpenCodeV2).expect("serialize OpenCodeV2");
    assert_eq!(serialized, "\"opencode_v2\"");
    for alias in [
        "\"opencode_v2\"",
        "\"open_code_v2\"",
        "\"opencode-v2\"",
        "\"opencode\"",
    ] {
        let parsed: WorkerProvider = serde_json::from_str(alias).expect("deserialize alias");
        assert_eq!(parsed, WorkerProvider::OpenCodeV2);
    }
}

#[tokio::test]
async fn escaped_pipe_descendant_bounded_join_aborted_future_raii_kill_and_false_cancel_ignored() {
    let env = TestEnv::new();
    let workdir = env.root_dir.join("process-bounds-dir");
    fs::create_dir_all(&workdir).expect("create workdir");

    // 1. Escaped descendant in its own process group (`set -m` background job) holding stdout/stderr open
    let escaped_bin = env.write_fake_executable(
        "escaped-pipe-worker",
        r#"
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-argon-high","effort":"high","result":"done"}\n'
set -m
sleep 30 &
escaped_pid=$!
printf '%s\n' "$escaped_pid" > escaped_pipe.pid
exit 0
"#,
    );
    let mut escaped_spec = env.antigravity_spec(escaped_bin, "Escaped pipe test");
    escaped_spec.budgets.timeout_ms = 5_000;

    let escaped_report = run_worker(&escaped_spec, &workdir)
        .await
        .expect("escaped pipe worker must return within bounded drain timeout");
    let escaped_pid: u32 = fs::read_to_string(workdir.join("escaped_pipe.pid"))
        .expect("read escaped_pipe.pid")
        .trim()
        .parse()
        .expect("parse escaped_pid");

    // Explicitly clean up the escaped fixture process regardless of assertions.
    let _ = Command::new("kill")
        .args(["-9", &escaped_pid.to_string()])
        .status();

    assert!(
        escaped_report.cleanup_incomplete,
        "escaped pipe holder must mark cleanup_incomplete = true"
    );
    assert_eq!(escaped_report.status, WorkerRunStatus::IncompleteCleanup);
    assert!(
        escaped_report.duration_ms < 2_500,
        "bounded drain must not hang for 30s (took {} ms)",
        escaped_report.duration_ms
    );

    // 2. Dropping / aborting the `run_worker` future kills the entire process group via RAII guard
    let abort_bin = env.write_fake_executable(
        "abort-group-worker",
        r#"
sleep 30 &
desc_pid=$!
printf '%s\n' "$$" > abort_parent.pid
printf '%s\n' "$desc_pid" > abort_descendant.pid
sleep 30
"#,
    );
    let mut abort_spec = env.antigravity_spec(abort_bin, "Abort future test");
    abort_spec.budgets.timeout_ms = 30_000;
    let abort_workdir = workdir.clone();

    let join_handle = tokio::spawn(async move { run_worker(&abort_spec, &abort_workdir).await });
    let desc_pid_path = workdir.join("abort_descendant.pid");
    for _ in 0..100 {
        if desc_pid_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(desc_pid_path.exists(), "abort_descendant.pid must exist");
    let abort_parent_pid: i32 = fs::read_to_string(workdir.join("abort_parent.pid"))
        .expect("read abort_parent.pid")
        .trim()
        .parse()
        .expect("parse abort_parent_pid");
    let abort_desc_pid: u32 = fs::read_to_string(&desc_pid_path)
        .expect("read abort_descendant.pid")
        .trim()
        .parse()
        .expect("parse abort_desc_pid");

    join_handle.abort();
    let _ = join_handle.await;

    for _ in 0..40 {
        if !is_process_alive(abort_desc_pid) && !is_process_group_alive(abort_parent_pid) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !is_process_alive(abort_desc_pid),
        "descendant {abort_desc_pid} must be terminated when run_worker future is aborted"
    );
    assert!(
        !is_process_group_alive(abort_parent_pid),
        "process group {abort_parent_pid} must be terminated when run_worker future is aborted"
    );

    // 3. False updates on cancel_rx must not trigger cancellation
    let false_cancel_bin = env.write_fake_executable(
        "false-cancel-worker",
        r#"
sleep 0.1
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-argon-high","effort":"high","result":"ok"}\n'
"#,
    );
    let false_cancel_spec = env.antigravity_spec(false_cancel_bin, "False cancel test");
    let (cancel_tx, mut cancel_rx) = watch::channel(false);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let _ = cancel_tx.send(false);
    });
    let fc_report = run_worker_with_cancel(&false_cancel_spec, &workdir, &mut cancel_rx)
        .await
        .expect("run worker with false cancel update");
    assert_eq!(fc_report.status, WorkerRunStatus::Succeeded);
}

#[test]
fn diff_gate_allows_unchanged_baseline_symlink_and_enforces_diff_budgets() {
    let env = TestEnv::new();

    // Commit a tracked symlink into the repository baseline
    let baseline_link = env.repo_dir.join("crates/threadlane-workers/baseline_link");
    symlink("src/lib.rs", &baseline_link).expect("create baseline symlink");
    git_cmd(&env.repo_dir, &["add", "."]);
    git_cmd(
        &env.repo_dir,
        &["commit", "-m", "Add tracked baseline symlink"],
    );
    let symlink_base_commit =
        git_stdout(&env.repo_dir, &["rev-parse", "--verify", "HEAD^{commit}"])
            .trim()
            .to_string();

    let manager =
        WorktreeManager::new(&env.repo_dir, &env.work_area, &symlink_base_commit).expect("manager");
    let wt = manager
        .create_task_worktree("run-sym", "task-sym", &env.default_allowed_paths())
        .expect("create task worktree");

    // 1. Modify only an allowed regular file; leave tracked `baseline_link` unchanged
    fs::write(
        wt.checkout_path
            .join("crates/threadlane-workers/src/lib.rs"),
        "pub fn updated() -> u32 { 42 }\n",
    )
    .expect("update regular file");

    let gate_ok = evaluate_diff_gate(&env.work_area, &wt.worktree_id).expect("evaluate diff gate");
    assert!(
        gate_ok.passed,
        "unchanged tracked baseline symlink must not fail diff gate: {:?}",
        gate_ok.violations
    );
    assert!(gate_ok.evidence_complete);
    assert!(!gate_ok
        .changed_paths
        .iter()
        .any(|e| e.path.ends_with("baseline_link")));

    // 2. Exceeding `max_changed_paths` or `max_git_output_bytes` fails explicitly with DiffBudgetExceeded
    fs::write(
        wt.checkout_path.join("crates/threadlane-workers/extra.txt"),
        "second changed file\n",
    )
    .expect("write second file");

    let path_budget_report =
        evaluate_diff_gate_with_limits(&env.work_area, &wt.worktree_id, 65_536, 1)
            .expect("evaluate with max_changed_paths = 1");
    assert!(!path_budget_report.passed);
    assert!(!path_budget_report.evidence_complete);
    assert_eq!(
        path_budget_report.acceptance_state,
        WorkAcceptanceState::RejectedByDiffGate
    );
    assert!(path_budget_report
        .violations
        .iter()
        .any(|v| v.kind == DiffViolationKind::DiffBudgetExceeded));

    let byte_budget_report =
        evaluate_diff_gate_with_limits(&env.work_area, &wt.worktree_id, 8, 100)
            .expect("evaluate with max_git_output_bytes = 8");
    assert!(!byte_budget_report.passed);
    assert!(!byte_budget_report.evidence_complete);
    assert!(byte_budget_report
        .violations
        .iter()
        .any(|v| v.kind == DiffViolationKind::DiffBudgetExceeded));

    // 3. Modifying the tracked baseline symlink itself IS reported and rejected as SymlinkDetected
    let wt_link = wt
        .checkout_path
        .join("crates/threadlane-workers/baseline_link");
    fs::remove_file(&wt_link).expect("remove baseline_link");
    symlink("src/old_name.rs", &wt_link).expect("repoint baseline_link");

    let gate_modified_sym =
        evaluate_diff_gate(&env.work_area, &wt.worktree_id).expect("evaluate modified symlink");
    assert!(!gate_modified_sym.passed);
    assert!(gate_modified_sym.violations.iter().any(|v| {
        v.path == "crates/threadlane-workers/baseline_link"
            && v.kind == DiffViolationKind::SymlinkDetected
    }));
}

#[test]
fn source_repository_as_git_worktree_with_git_file_supported() {
    let env = TestEnv::new();

    // Create a linked Git worktree to act as the source `repository_path` (where `.git` is a file)
    let linked_source_repo = env.root_dir.join("linked-source-worktree");
    let linked_source_str = linked_source_repo.to_str().expect("utf8 path");
    git_cmd(
        &env.repo_dir,
        &[
            "worktree",
            "add",
            "-b",
            "source-wt-branch",
            "--",
            linked_source_str,
            &env.base_commit,
        ],
    );
    let dot_git_meta =
        fs::symlink_metadata(linked_source_repo.join(".git")).expect("stat linked .git");
    assert!(
        dot_git_meta.file_type().is_file(),
        "linked source worktree .git must be a regular file"
    );

    let source_branch_before =
        git_stdout(&linked_source_repo, &["symbolic-ref", "--short", "HEAD"]);
    let source_head_before = git_stdout(&linked_source_repo, &["rev-parse", "HEAD"]);

    let linked_work_area = env.root_dir.join("linked-work-area");
    let manager = WorktreeManager::new(&linked_source_repo, &linked_work_area, &env.base_commit)
        .expect("WorktreeManager on linked worktree repository");

    let task_wt = manager
        .create_task_worktree("linked-run", "linked-task", &env.default_allowed_paths())
        .expect("create task worktree from linked source worktree");

    let diff_report = evaluate_diff_gate(&linked_work_area, &task_wt.worktree_id)
        .expect("diff gate on worktree from linked source");
    assert!(diff_report.passed);

    let cleanup_report = cleanup_worktree(&CleanupRequest {
        work_area_root: linked_work_area,
        worktree_id: task_wt.worktree_id.clone(),
    })
    .expect("cleanup_worktree must succeed when repository_path is a worktree with a .git file");
    assert_eq!(cleanup_report.worktree_id, task_wt.worktree_id);
    assert!(!task_wt.checkout_path.exists());

    // Source worktree active branch and HEAD must not have moved
    let source_branch_after = git_stdout(&linked_source_repo, &["symbolic-ref", "--short", "HEAD"]);
    let source_head_after = git_stdout(&linked_source_repo, &["rev-parse", "HEAD"]);
    assert_eq!(source_branch_before, source_branch_after);
    assert_eq!(source_head_before, source_head_after);
}

#[test]
fn git_bounded_supervision_terminates_overflowing_sleeping_and_pipe_holding_hooks() {
    let env = TestEnv::new();
    let manager =
        WorktreeManager::new(&env.repo_dir, &env.work_area, &env.base_commit).expect("manager");

    // Create a clean task worktree first so we can also exercise diff-gate Git supervision on it.
    let wt = manager
        .create_task_worktree("git-bound", "task-clean", &env.default_allowed_paths())
        .expect("create initial task worktree");

    // 1. Control Git command (`git worktree add` via `create_task_worktree`) with a temporary
    // repo-local `.git/hooks/post-checkout` hook that overflows output while a background child
    // holds stderr open for 30s.
    let hooks_dir = env.repo_dir.join(".git/hooks");
    fs::create_dir_all(&hooks_dir).expect("create hooks_dir");
    let post_checkout_hook = hooks_dir.join("post-checkout");
    let hook_child_pid_file = env.root_dir.join("hook_overflow_child.pid");
    fs::write(
        &post_checkout_hook,
        format!(
            "#!/bin/sh\nsleep 30 &\nprintf '%s\\n' \"$!\" > '{}'\nhead -c 70000 /dev/zero | tr '\\0' 'E' >&2\nsleep 30\n",
            hook_child_pid_file.display()
        ),
    )
    .expect("write post-checkout hook");
    fs::set_permissions(&post_checkout_hook, fs::Permissions::from_mode(0o755))
        .expect("chmod 0755 post-checkout hook");

    let start_hook = Instant::now();
    let hook_err = manager
        .create_task_worktree(
            "git-bound",
            "task-hook-overflow",
            &env.default_allowed_paths(),
        )
        .expect_err("post-checkout overflow must fail create_task_worktree");
    let hook_elapsed = start_hook.elapsed();
    fs::remove_file(&post_checkout_hook).expect("remove temporary post-checkout hook");

    let hook_child_pid: u32 = fs::read_to_string(&hook_child_pid_file)
        .expect("read hook_overflow_child.pid")
        .trim()
        .parse()
        .expect("parse hook_child_pid");
    for _ in 0..30 {
        if !is_process_alive(hook_child_pid) {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = Command::new("kill")
        .args(["-9", &hook_child_pid.to_string()])
        .status();

    match hook_err {
        WorkerError::GitCommandFailed { stderr, .. } => {
            assert!(
                stderr.contains("exceeded bounded limit"),
                "expected bounded overflow failure reason, got: {stderr}"
            );
        }
        other => panic!("expected GitCommandFailed, got: {other:?}"),
    }
    assert!(
        hook_elapsed < Duration::from_secs(3),
        "early overflow termination must not wait 30s on held stderr pipe (took {hook_elapsed:?})"
    );
    assert!(
        !is_process_alive(hook_child_pid),
        "owned Git process group child {hook_child_pid} must be terminated on overflow"
    );

    // 2. Diff-gate Git supervision (`evaluate_diff_gate_with_timeout`) using a temporary
    // repo-local clean filter in `<repo>/.git/config` and `<repo>/.git/info/attributes`
    // (never touching user/global Git config or tracked files).
    fs::write(
        wt.checkout_path
            .join("crates/threadlane-workers/src/lib.rs"),
        "pub fn modified_for_filter() -> u32 { 7 }\n",
    )
    .expect("modify tracked file in task worktree");

    let git_info_dir = env.repo_dir.join(".git/info");
    fs::create_dir_all(&git_info_dir).expect("create .git/info");
    fs::write(
        git_info_dir.join("attributes"),
        "crates/threadlane-workers/src/lib.rs filter=boundedtest\n",
    )
    .expect("write .git/info/attributes");

    // 2a. Filter overflows stderr while a background child holds stderr open for 30s
    let filter_overflow_pid_file = env.root_dir.join("filter_overflow_child.pid");
    let filter_overflow_bin = env.write_fake_executable(
        "git-filter-overflow",
        &format!(
            "sleep 30 &\nprintf '%s\\n' \"$!\" > '{}'\nhead -c 70000 /dev/zero | tr '\\0' 'F' >&2\nsleep 30\n",
            filter_overflow_pid_file.display()
        ),
    );
    git_cmd(
        &env.repo_dir,
        &[
            "config",
            "--local",
            "filter.boundedtest.clean",
            filter_overflow_bin.to_str().expect("utf8 path"),
        ],
    );

    let start_overflow = Instant::now();
    let overflow_gate =
        evaluate_diff_gate_with_timeout(&env.work_area, &wt.worktree_id, 65_536, 100, 5_000)
            .expect("evaluate_diff_gate_with_timeout on overflowing filter");
    let overflow_elapsed = start_overflow.elapsed();

    let filter_overflow_pid: u32 = fs::read_to_string(&filter_overflow_pid_file)
        .expect("read filter_overflow_child.pid")
        .trim()
        .parse()
        .expect("parse filter_overflow_pid");
    for _ in 0..30 {
        if !is_process_alive(filter_overflow_pid) {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = Command::new("kill")
        .args(["-9", &filter_overflow_pid.to_string()])
        .status();

    assert!(!overflow_gate.passed);
    assert!(!overflow_gate.evidence_complete);
    assert_eq!(
        overflow_gate.acceptance_state,
        WorkAcceptanceState::RejectedByDiffGate
    );
    assert!(overflow_gate.violations.iter().any(|v| {
        v.kind == DiffViolationKind::DiffBudgetExceeded
            && v.detail.contains("exceeded bounded limit")
    }));
    assert!(
        overflow_elapsed < Duration::from_secs(3),
        "diff gate overflow must not block on held stderr pipe (took {overflow_elapsed:?})"
    );
    assert!(
        !is_process_alive(filter_overflow_pid),
        "filter overflow child {filter_overflow_pid} must be killed with owned Git group"
    );

    // 2b. Filter sleeps past `git_timeout_ms` while holding pipes open
    let filter_timeout_pid_file = env.root_dir.join("filter_timeout_child.pid");
    let filter_timeout_bin = env.write_fake_executable(
        "git-filter-timeout",
        &format!(
            "sleep 30 &\nprintf '%s\\n' \"$!\" > '{}'\nsleep 30\n",
            filter_timeout_pid_file.display()
        ),
    );
    git_cmd(
        &env.repo_dir,
        &[
            "config",
            "--local",
            "filter.boundedtest.clean",
            filter_timeout_bin.to_str().expect("utf8 path"),
        ],
    );

    let start_timeout = Instant::now();
    let timeout_gate =
        evaluate_diff_gate_with_timeout(&env.work_area, &wt.worktree_id, 65_536, 100, 300)
            .expect("evaluate_diff_gate_with_timeout on hanging filter");
    let timeout_elapsed = start_timeout.elapsed();

    let filter_timeout_pid: u32 = fs::read_to_string(&filter_timeout_pid_file)
        .expect("read filter_timeout_child.pid")
        .trim()
        .parse()
        .expect("parse filter_timeout_pid");
    for _ in 0..30 {
        if !is_process_alive(filter_timeout_pid) {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = Command::new("kill")
        .args(["-9", &filter_timeout_pid.to_string()])
        .status();

    assert!(!timeout_gate.passed);
    assert!(!timeout_gate.evidence_complete);
    assert_eq!(
        timeout_gate.acceptance_state,
        WorkAcceptanceState::RejectedByDiffGate
    );
    assert!(
        timeout_gate
            .violations
            .iter()
            .any(|v| v.kind == DiffViolationKind::DiffBudgetExceeded
                && v.detail.contains("timed out"))
    );
    assert!(
        timeout_elapsed < Duration::from_secs(3),
        "diff gate timeout must enforce deadline promptly (took {timeout_elapsed:?})"
    );
    assert!(
        !is_process_alive(filter_timeout_pid),
        "filter timeout child {filter_timeout_pid} must be killed with owned Git group"
    );

    // 2c. Filter spawns an escaped descendant (`set -m`) that keeps stderr open after `git diff` exits 0
    let filter_escaped_pid_file = env.root_dir.join("filter_escaped_child.pid");
    let filter_escaped_bin = env.write_fake_executable(
        "git-filter-escaped",
        &format!(
            "set -m\nsleep 30 </dev/null >/dev/null &\nprintf '%s\\n' \"$!\" > '{}'\ncat\nexit 0\n",
            filter_escaped_pid_file.display()
        ),
    );
    git_cmd(
        &env.repo_dir,
        &[
            "config",
            "--local",
            "filter.boundedtest.clean",
            filter_escaped_bin.to_str().expect("utf8 path"),
        ],
    );

    let start_escaped = Instant::now();
    let escaped_gate =
        evaluate_diff_gate_with_timeout(&env.work_area, &wt.worktree_id, 65_536, 100, 5_000)
            .expect("evaluate_diff_gate_with_timeout on escaped pipe holder");
    let escaped_elapsed = start_escaped.elapsed();

    let filter_escaped_pid: u32 = fs::read_to_string(&filter_escaped_pid_file)
        .expect("read filter_escaped_child.pid")
        .trim()
        .parse()
        .expect("parse filter_escaped_pid");
    // Explicitly clean up the escaped fixture process.
    let _ = Command::new("kill")
        .args(["-9", &filter_escaped_pid.to_string()])
        .status();

    assert!(
        !escaped_gate.passed,
        "escaped descendant holding git pipes must never pass partial/uncertain diff evidence"
    );
    assert!(!escaped_gate.evidence_complete);
    assert_eq!(
        escaped_gate.acceptance_state,
        WorkAcceptanceState::RejectedByDiffGate
    );
    assert!(escaped_gate.violations.iter().any(|v| {
        v.kind == DiffViolationKind::DiffBudgetExceeded && v.detail.contains("incomplete cleanup")
    }));
    assert!(
        escaped_elapsed < Duration::from_secs(3),
        "escaped pipe holder must be bounded by post-exit pipe drain window (took {escaped_elapsed:?})"
    );
}

#[test]
fn diff_gate_preserves_valid_unicode_fffd_rejects_raw_non_utf8_filename_and_bounds_scan() {
    let env = TestEnv::new();
    let mgr = WorktreeManager::new(&env.repo_dir, &env.work_area, &env.base_commit)
        .expect("worktree manager");

    // 1. Allow ONLY the valid UTF-8 filename U+FFFD + ".rs" (bytes ef bf bd 2e 72 73).
    let allowed_fffd_name = "\u{FFFD}.rs".to_string();
    let wt = mgr
        .create_task_worktree(
            "run-utf8",
            "task-utf8",
            &BTreeSet::from([allowed_fffd_name.clone()]),
        )
        .expect("create task worktree for utf8 test");

    fs::write(
        wt.checkout_path.join(&allowed_fffd_name),
        "pub fn valid_unicode_fffd() -> u32 { 7 }\n",
    )
    .expect("write valid U+FFFD filename");

    let valid_gate =
        evaluate_diff_gate(&env.work_area, &wt.worktree_id).expect("evaluate valid U+FFFD gate");
    assert!(
        valid_gate.passed,
        "valid allowed U+FFFD filename must pass diff gate, got violations: {:?}",
        valid_gate.violations
    );
    assert!(valid_gate.evidence_complete);
    assert_eq!(
        valid_gate.acceptance_state,
        WorkAcceptanceState::DiffGatePassedPendingReview
    );
    assert_eq!(valid_gate.changed_paths.len(), 1);
    assert_eq!(valid_gate.changed_paths[0].path, allowed_fffd_name);
    assert_eq!(
        valid_gate.changed_paths[0].change_kind,
        DiffChangeKind::Untracked
    );
    assert!(valid_gate.violations.is_empty());

    // 2. Now create a DIFFERENT raw Unix filename with invalid UTF-8 bytes ff 2e 72 73
    // alongside the valid U+FFFD.rs file in the same worktree.
    let raw_non_utf8_name = OsStr::from_bytes(b"\xff.rs");
    fs::write(
        wt.checkout_path.join(raw_non_utf8_name),
        "pub fn unauthorized_raw_bytes() -> u32 { 99 }\n",
    )
    .expect("write raw non-UTF-8 filename");

    let invalid_utf8_gate = evaluate_diff_gate(&env.work_area, &wt.worktree_id)
        .expect("evaluate diff gate with raw non-UTF-8 filename");
    assert!(
        !invalid_utf8_gate.passed,
        "raw non-UTF-8 filename must never collapse onto U+FFFD.rs or pass diff gate"
    );
    assert!(!invalid_utf8_gate.evidence_complete);
    assert_eq!(
        invalid_utf8_gate.acceptance_state,
        WorkAcceptanceState::RejectedByDiffGate
    );
    assert!(!invalid_utf8_gate.violations.is_empty());
    assert!(
        invalid_utf8_gate
            .violations
            .iter()
            .any(|v| v.kind == DiffViolationKind::PathEscape && v.detail.contains("non-UTF-8")),
        "expected non-UTF-8 PathEscape violation, got: {:?}",
        invalid_utf8_gate.violations
    );

    // 3. Verify bounded checkout scan limits (visited entries and directory depth).
    let wt_scan = mgr
        .create_task_worktree(
            "run-scan-bounds",
            "task-scan-bounds",
            &BTreeSet::from(["crates/threadlane-workers".to_string()]),
        )
        .expect("create task worktree for scan bounds");

    let deep_dir = wt_scan
        .checkout_path
        .join("crates/threadlane-workers/scan_tree/d1/d2/d3/d4");
    fs::create_dir_all(&deep_dir).expect("create nested scan_tree dirs");
    fs::write(deep_dir.join("leaf.rs"), "pub fn leaf() {}\n").expect("write leaf.rs");

    let ok_scan = evaluate_diff_gate_with_scan_limits(
        &env.work_area,
        &wt_scan.worktree_id,
        65_536,
        100,
        200,
        16,
    )
    .expect("evaluate_diff_gate_with_scan_limits within budget");
    assert!(ok_scan.passed);
    assert!(ok_scan.evidence_complete);
    assert_eq!(
        ok_scan.acceptance_state,
        WorkAcceptanceState::DiffGatePassedPendingReview
    );

    let entry_limit_gate = evaluate_diff_gate_with_scan_limits(
        &env.work_area,
        &wt_scan.worktree_id,
        65_536,
        100,
        5,
        16,
    )
    .expect("evaluate_diff_gate_with_scan_limits exceeding max_scan_entries");
    assert!(!entry_limit_gate.passed);
    assert!(!entry_limit_gate.evidence_complete);
    assert_eq!(
        entry_limit_gate.acceptance_state,
        WorkAcceptanceState::RejectedByDiffGate
    );
    assert!(entry_limit_gate.violations.iter().any(|v| {
        v.kind == DiffViolationKind::DiffBudgetExceeded && v.detail.contains("max_scan_entries=5")
    }));

    let depth_limit_gate = evaluate_diff_gate_with_scan_limits(
        &env.work_area,
        &wt_scan.worktree_id,
        65_536,
        100,
        200,
        3,
    )
    .expect("evaluate_diff_gate_with_scan_limits exceeding max_scan_depth");
    assert!(!depth_limit_gate.passed);
    assert!(!depth_limit_gate.evidence_complete);
    assert_eq!(
        depth_limit_gate.acceptance_state,
        WorkAcceptanceState::RejectedByDiffGate
    );
    assert!(depth_limit_gate.violations.iter().any(|v| {
        v.kind == DiffViolationKind::DiffBudgetExceeded
            && v.detail.contains("max_scan_depth budget (3)")
    }));
}

#[tokio::test]
async fn live_observer_receives_sanitized_bounded_progress_while_worker_is_still_running() {
    let env = TestEnv::new();
    let workdir = env.root_dir.join("live-observer-dir");
    fs::create_dir_all(&workdir).expect("create workdir");

    let long_tail = "x".repeat(320);
    let live_bin = env.write_fake_executable(
        "live-observer-worker",
        &format!(
            r#"
printf '%s\n' "$$" > live_worker.pid
printf '{{"type":"system","subtype":"init","session_id":"s-live","model":"gemini-4-argon-high","effort":"high"}}\n'
printf '{{"type":"assistant","text":"progress with Bearer secret_live_token_abc123 and api_key=sk-live1234567890abcdef {long_tail}"}}\n'
while [ ! -f release_worker.flag ]; do
    sleep 0.01
done
printf '{{"type":"result","subtype":"success","session_id":"s-live","model":"gemini-4-argon-high","effort":"high","result":"completed"}}\n'
"#
        ),
    );

    let mut spec = env.antigravity_spec(live_bin, "Observe incremental progress");
    spec.budgets.timeout_ms = 5_000;

    let (progress_tx, mut progress_rx) = mpsc::channel(8);
    let run_workdir = workdir.clone();
    let run_spec = spec.clone();
    let worker_join = tokio::spawn(async move {
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        run_worker_with_observer(&run_spec, &run_workdir, &mut cancel_rx, Some(progress_tx)).await
    });

    let ev1 = tokio::time::timeout(Duration::from_secs(2), progress_rx.recv())
        .await
        .expect("receive init event before timeout")
        .expect("init event present");
    let ev2 = tokio::time::timeout(Duration::from_secs(2), progress_rx.recv())
        .await
        .expect("receive progress event before timeout")
        .expect("progress event present");

    // Verify the worker future and OS child process are still running when progress arrives.
    assert!(
        !worker_join.is_finished(),
        "worker future must still be running when live progress event arrives"
    );
    let live_pid: u32 = fs::read_to_string(workdir.join("live_worker.pid"))
        .expect("read live_worker.pid")
        .trim()
        .parse()
        .expect("parse live_pid");
    assert!(
        is_process_alive(live_pid),
        "worker OS process {live_pid} must still be alive when live progress event arrives"
    );

    assert_eq!(ev1.line_number, 1);
    assert_eq!(ev1.kind, NormalizedEventKind::Init);
    assert_eq!(ev1.model.as_deref(), Some("gemini-4-argon-high"));
    assert_eq!(ev1.effort.as_deref(), Some("high"));

    assert_eq!(ev2.line_number, 2);
    assert_eq!(ev2.kind, NormalizedEventKind::Progress);
    let ev2_summary = ev2.summary.as_deref().expect("progress summary present");
    assert!(
        ev2_summary.len() <= 256,
        "live progress summary must be bounded to 256 bytes (got {})",
        ev2_summary.len()
    );
    assert!(ev2_summary.contains("[REDACTED]"));
    assert!(ev2_summary.ends_with("...[truncated]"));
    assert!(!ev2_summary.contains("secret_live_token_abc123"));
    assert!(!ev2_summary.contains("sk-live1234567890abcdef"));

    // Release the worker so it can emit its terminal result and exit.
    fs::write(workdir.join("release_worker.flag"), "go\n").expect("write release_worker.flag");

    let ev3 = tokio::time::timeout(Duration::from_secs(2), progress_rx.recv())
        .await
        .expect("receive result event before timeout")
        .expect("result event present");
    assert_eq!(ev3.line_number, 3);
    assert_eq!(ev3.kind, NormalizedEventKind::Result);
    assert!(progress_rx.recv().await.is_none());

    let report = worker_join
        .await
        .expect("join worker task")
        .expect("run_worker_with_observer succeeded");
    assert_eq!(report.status, WorkerRunStatus::Succeeded);
    assert_eq!(report.dropped_live_events, 0);
    assert_eq!(report.evidence.dropped_events_count, 0);
    assert_eq!(report.evidence.retained_events, vec![ev1, ev2, ev3]);

    // Also verify `execute_worker_task_with_observer` emits live events before process exit.
    let task_gate_file = env.root_dir.join("release_task_worker.flag");
    let task_pid_file = env.root_dir.join("task_live_worker.pid");
    let task_bin = env.write_fake_executable(
        "task-live-observer-worker",
        &format!(
            r#"
printf 'pub fn live_observed_task() {{}}\n' >> crates/threadlane-workers/src/lib.rs
printf '%s\n' "$$" > '{}'
printf '{{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}}\n'
printf '{{"type":"tool_use","name":"edit_file","message":"updating lib.rs with password=super_secret_pw"}}\n'
while [ ! -f '{}' ]; do
    sleep 0.01
done
printf '{{"type":"result","subtype":"success","model":"gemini-4-argon-high","effort":"high","result":"done"}}\n'
"#,
            task_pid_file.display(),
            task_gate_file.display()
        ),
    );
    let task_spec = env.antigravity_spec(task_bin, "Task live observer test");
    let (task_tx, mut task_rx) = mpsc::channel(8);
    let task_join = tokio::spawn(async move {
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        execute_worker_task_with_observer(
            &task_spec,
            "run-live-obs",
            "task-1",
            None,
            &mut cancel_rx,
            Some(task_tx),
        )
        .await
    });

    let task_ev1 = tokio::time::timeout(Duration::from_secs(2), task_rx.recv())
        .await
        .expect("receive task init")
        .expect("task init event");
    let task_ev2 = tokio::time::timeout(Duration::from_secs(2), task_rx.recv())
        .await
        .expect("receive task tool event")
        .expect("task tool event");

    assert!(!task_join.is_finished());
    let task_pid: u32 = fs::read_to_string(&task_pid_file)
        .expect("read task_live_worker.pid")
        .trim()
        .parse()
        .expect("parse task_pid");
    assert!(is_process_alive(task_pid));
    assert_eq!(task_ev1.kind, NormalizedEventKind::Init);
    assert_eq!(task_ev2.kind, NormalizedEventKind::Tool);
    assert_eq!(task_ev2.tool_name.as_deref(), Some("edit_file"));
    assert!(task_ev2
        .summary
        .as_deref()
        .expect("tool summary")
        .contains("[REDACTED]"));

    fs::write(&task_gate_file, "go\n").expect("write release_task_worker.flag");

    let task_ev3 = tokio::time::timeout(Duration::from_secs(2), task_rx.recv())
        .await
        .expect("receive task result")
        .expect("task result event");
    assert_eq!(task_ev3.kind, NormalizedEventKind::Result);

    let task_report = task_join
        .await
        .expect("join task_join")
        .expect("execute_worker_task_with_observer succeeded");
    assert_eq!(task_report.worker_report.status, WorkerRunStatus::Succeeded);
    assert_eq!(task_report.worker_report.dropped_live_events, 0);
    assert!(task_report.diff_gate.passed);
    assert_eq!(
        task_report.overall_acceptance_state,
        WorkAcceptanceState::DiffGatePassedPendingReview
    );
}

#[tokio::test]
async fn full_and_closed_observer_channels_never_hang_preserve_evidence_and_count_drops() {
    let env = TestEnv::new();
    let workdir = env.root_dir.join("drop-observer-dir");
    fs::create_dir_all(&workdir).expect("create workdir");

    // 1. Full observer channel (`capacity = 2`) with `max_events = 3` across 6 valid events.
    // Proves `dropped_live_events` (4) and `evidence.dropped_events_count` (3) are tracked
    // independently and that terminal `result` evidence is preserved even when observer is full.
    let six_events_bin = env.write_fake_executable(
        "six-events-worker",
        r#"
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"assistant","text":"step 1"}\n'
printf '{"type":"assistant","text":"step 2"}\n'
printf '{"type":"assistant","text":"step 3"}\n'
printf '{"type":"assistant","text":"step 4"}\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-argon-high","effort":"high","result":"done"}\n'
"#,
    );
    let mut full_spec = env.antigravity_spec(six_events_bin, "Full observer test");
    full_spec.budgets.max_events = 3;

    let (full_tx, mut full_rx) = mpsc::channel(2);
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let full_report = run_worker_with_observer(&full_spec, &workdir, &mut cancel_rx, Some(full_tx))
        .await
        .expect("run worker with full observer");

    assert_eq!(full_report.status, WorkerRunStatus::Succeeded);
    assert!(full_report.evidence.saw_init_event);
    assert!(full_report.evidence.saw_result_event);
    assert_eq!(full_report.evidence.valid_json_events, 6);
    assert_eq!(full_report.dropped_live_events, 4);
    assert_eq!(full_report.evidence.retained_events.len(), 3);
    assert_eq!(full_report.evidence.dropped_events_count, 3);

    let buffered_1 = full_rx.recv().await.expect("buffered event 1");
    let buffered_2 = full_rx.recv().await.expect("buffered event 2");
    assert_eq!(buffered_1.line_number, 1);
    assert_eq!(buffered_2.line_number, 2);
    assert!(full_rx.recv().await.is_none());

    // 2. Full observer (`capacity = 1`) with malformed stdout line and redacted secrets.
    let malformed_full_bin = env.write_fake_executable(
        "malformed-full-worker",
        r#"
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf 'NOT_JSON_LINE secret token=sk-malformedsecret123456\n'
printf '{"type":"assistant","text":"progress with Bearer dropped_live_secret_token_999"}\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-argon-high","effort":"high","result":"done"}\n'
"#,
    );
    let mal_spec = env.antigravity_spec(malformed_full_bin, "Malformed + full observer");
    let (mal_tx, _mal_rx) = mpsc::channel(1);
    let mal_report = run_worker_with_observer(&mal_spec, &workdir, &mut cancel_rx, Some(mal_tx))
        .await
        .expect("run malformed worker with full observer");

    assert_eq!(mal_report.status, WorkerRunStatus::MalformedStream);
    assert_eq!(mal_report.evidence.total_lines, 4);
    assert_eq!(mal_report.evidence.valid_json_events, 3);
    assert_eq!(mal_report.evidence.malformed_lines_count, 1);
    assert_eq!(mal_report.dropped_live_events, 2);
    assert!(mal_report.evidence.saw_result_event);
    assert!(mal_report.evidence.malformed_diagnostics[0]
        .redacted_snippet
        .contains("[REDACTED]"));
    assert!(!mal_report.evidence.malformed_diagnostics[0]
        .redacted_snippet
        .contains("sk-malformedsecret123456"));
    assert!(mal_report.evidence.retained_events[1]
        .summary
        .as_deref()
        .expect("retained progress summary")
        .contains("[REDACTED]"));

    // 3. Closed observer (`drop(rx)` before execution) via `execute_worker_task_with_observer`.
    let closed_task_bin = env.write_fake_executable(
        "closed-observer-task-worker",
        r#"
printf 'pub fn closed_observer_edit() {}\n' >> crates/threadlane-workers/src/lib.rs
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"assistant","text":"working while observer receiver is dropped"}\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-argon-high","effort":"high","result":"done"}\n'
"#,
    );
    let closed_spec = env.antigravity_spec(closed_task_bin, "Closed observer task");
    let (closed_tx, closed_rx) = mpsc::channel(4);
    drop(closed_rx);

    let closed_task_report = execute_worker_task_with_observer(
        &closed_spec,
        "run-closed-obs",
        "task-1",
        None,
        &mut cancel_rx,
        Some(closed_tx),
    )
    .await
    .expect("execute_worker_task_with_observer with closed receiver");

    assert_eq!(
        closed_task_report.worker_report.status,
        WorkerRunStatus::Succeeded
    );
    assert_eq!(closed_task_report.worker_report.dropped_live_events, 3);
    assert_eq!(
        closed_task_report
            .worker_report
            .evidence
            .dropped_events_count,
        0
    );
    assert_eq!(
        closed_task_report
            .worker_report
            .evidence
            .retained_events
            .len(),
        3
    );
    assert!(closed_task_report.diff_gate.passed);
    assert_eq!(
        closed_task_report.overall_acceptance_state,
        WorkAcceptanceState::DiffGatePassedPendingReview
    );

    // 4. Output overflow under a full observer does not block overflow termination.
    let overflow_full_bin = env.write_fake_executable(
        "overflow-full-observer",
        r#"
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"assistant","text":"short"}\n'
printf '{"type":"assistant","text":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}\n'
"#,
    );
    let mut overflow_spec = env.antigravity_spec(overflow_full_bin, "Overflow under full observer");
    overflow_spec.budgets.max_line_bytes = 96;
    overflow_spec.budgets.max_stdout_bytes = 4096;
    let (ov_tx, _ov_rx) = mpsc::channel(1);
    let ov_report = run_worker_with_observer(&overflow_spec, &workdir, &mut cancel_rx, Some(ov_tx))
        .await
        .expect("run overflow worker with full observer");
    assert_eq!(ov_report.status, WorkerRunStatus::OutputOverflow);
    assert_eq!(
        ov_report.overflow,
        Some(OutputOverflowKind::StdoutLineExceeded { max_line_bytes: 96 })
    );
    assert_eq!(ov_report.dropped_live_events, 1);
}

#[tokio::test]
async fn timeout_and_cancel_work_promptly_under_slow_full_observer() {
    let env = TestEnv::new();
    let workdir = env.root_dir.join("slow-observer-timeout-cancel-dir");
    fs::create_dir_all(&workdir).expect("create workdir");

    // 1. Timeout when observer channel (capacity 1) is full and never drained during execution.
    let timeout_bin = env.write_fake_executable(
        "slow-obs-timeout-worker",
        r#"
sleep 30 &
desc_pid=$!
printf '%s\n' "$desc_pid" > slow_obs_timeout_desc.pid
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"assistant","text":"event 2"}\n'
printf '{"type":"assistant","text":"event 3"}\n'
printf '{"type":"assistant","text":"event 4"}\n'
sleep 30
"#,
    );
    let mut timeout_spec = env.antigravity_spec(timeout_bin, "Slow observer timeout test");
    timeout_spec.budgets.timeout_ms = 200;

    let (timeout_tx, mut timeout_rx) = mpsc::channel(1);
    let (_cancel_tx, mut noop_cancel_rx) = watch::channel(false);
    let timeout_report = run_worker_with_observer(
        &timeout_spec,
        &workdir,
        &mut noop_cancel_rx,
        Some(timeout_tx),
    )
    .await
    .expect("run timeout worker under slow observer");

    assert_eq!(timeout_report.status, WorkerRunStatus::TimedOut);
    assert!(
        timeout_report.duration_ms < 2_500,
        "timeout under full observer must not hang (took {} ms)",
        timeout_report.duration_ms
    );
    assert_eq!(timeout_report.dropped_live_events, 3);
    assert_eq!(timeout_report.evidence.retained_events.len(), 4);
    assert!(timeout_rx.recv().await.is_some());

    let timeout_desc_pid: u32 = fs::read_to_string(workdir.join("slow_obs_timeout_desc.pid"))
        .expect("read slow_obs_timeout_desc.pid")
        .trim()
        .parse()
        .expect("parse timeout_desc_pid");
    assert!(!is_process_alive(timeout_desc_pid));
    if let Some(pgid) = timeout_report.process_group_id {
        assert!(!is_process_group_alive(pgid));
    }

    // 2. Explicit cancellation when observer channel (capacity 1) is full and never drained.
    let cancel_bin = env.write_fake_executable(
        "slow-obs-cancel-worker",
        r#"
sleep 30 &
desc_pid=$!
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"assistant","text":"event 2"}\n'
printf '{"type":"assistant","text":"event 3"}\n'
printf '%s\n' "$desc_pid" > slow_obs_cancel_desc.pid
sleep 30
"#,
    );
    let mut cancel_spec = env.antigravity_spec(cancel_bin, "Slow observer cancel test");
    cancel_spec.budgets.timeout_ms = 10_000;

    let (cancel_obs_tx, mut cancel_obs_rx) = mpsc::channel(1);
    let (cancel_tx, mut cancel_watch_rx) = watch::channel(false);
    let cancel_pid_file = workdir.join("slow_obs_cancel_desc.pid");
    tokio::spawn(async move {
        for _ in 0..100 {
            if cancel_pid_file.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = cancel_tx.send(true);
    });

    let cancel_report = run_worker_with_observer(
        &cancel_spec,
        &workdir,
        &mut cancel_watch_rx,
        Some(cancel_obs_tx),
    )
    .await
    .expect("run cancelled worker under slow observer");

    assert_eq!(cancel_report.status, WorkerRunStatus::Cancelled);
    assert!(
        cancel_report.duration_ms < 2_500,
        "cancellation under full observer must not hang (took {} ms)",
        cancel_report.duration_ms
    );
    assert_eq!(cancel_report.dropped_live_events, 2);
    assert_eq!(cancel_report.evidence.retained_events.len(), 3);
    assert!(cancel_obs_rx.recv().await.is_some());

    let cancel_desc_pid: u32 = fs::read_to_string(workdir.join("slow_obs_cancel_desc.pid"))
        .expect("read slow_obs_cancel_desc.pid")
        .trim()
        .parse()
        .expect("parse cancel_desc_pid");
    assert!(!is_process_alive(cancel_desc_pid));
    if let Some(pgid) = cancel_report.process_group_id {
        assert!(!is_process_group_alive(pgid));
    }
}

#[tokio::test]
async fn none_observer_preserves_behavior_zero_live_drops_and_unknown_agy_missing_effort() {
    let env = TestEnv::new();
    let workdir = env.root_dir.join("none-observer-dir");
    fs::create_dir_all(&workdir).expect("create workdir");

    // 1. `None` observer with `max_events = 2` across 4 events never inflates `dropped_live_events`.
    let four_events_bin = env.write_fake_executable(
        "none-obs-four-events",
        r#"
printf '{"type":"system","subtype":"init","model":"gemini-4-argon-high","effort":"high"}\n'
printf '{"type":"assistant","text":"msg 1"}\n'
printf '{"type":"assistant","text":"msg 2"}\n'
printf '{"type":"result","subtype":"success","model":"gemini-4-argon-high","effort":"high","result":"done"}\n'
"#,
    );
    let mut spec = env.antigravity_spec(four_events_bin, "None observer test");
    spec.budgets.max_events = 2;

    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let report_none = run_worker_with_observer(&spec, &workdir, &mut cancel_rx, None)
        .await
        .expect("run_worker_with_observer with None");
    let report_wrapper = run_worker(&spec, &workdir).await.expect("run_worker");

    for report in [&report_none, &report_wrapper] {
        assert_eq!(report.status, WorkerRunStatus::Succeeded);
        assert_eq!(report.dropped_live_events, 0);
        assert_eq!(report.evidence.retained_events.len(), 2);
        assert_eq!(report.evidence.dropped_events_count, 2);
    }

    // 2. Serde default compatibility when deserializing legacy JSON without `dropped_live_events`.
    let mut json_val = serde_json::to_value(&report_none).expect("serialize report");
    json_val
        .as_object_mut()
        .expect("report object")
        .remove("dropped_live_events");
    let deserialized: WorkerExecutionReport =
        serde_json::from_value(json_val).expect("deserialize legacy report without field");
    assert_eq!(deserialized.dropped_live_events, 0);

    // 3. Unknown AGY model variants remain `MissingEffortEvidence` /
    // `FinishedNeedsEvidenceAndReview` both with `Some(progress_tx)` and `None`.
    let argon_missing_effort_bin = env.write_fake_executable(
        "argon-missing-effort-obs",
        r#"
printf 'pub fn argon_obs_check() {}\n' >> crates/threadlane-workers/src/lib.rs
printf '{"event":"init","conversation_id":"s-argon","init":{"model":"future-argon-high","cwd":"/canonical/worktree","permission_mode":"accept-edits"}}\n'
printf '{"event":"step_update","step_update":{"conversation_id":"s-argon","step_index":1,"state":"DONE","step_type":"agent_response","usage":{}}}\n'
printf '{"event":"result","result":{"conversation_id":"s-argon","status":"SUCCESS","response":"Completed","num_turns":1}}\n'
"#,
    );
    let mut argon_spec = env.antigravity_spec(argon_missing_effort_bin, "Unknown AGY effort check");
    argon_spec.model = "future-argon-high".to_string();
    argon_spec.allowed_models = BTreeSet::from([argon_spec.model.clone()]);
    let (argon_tx, mut argon_rx) = mpsc::channel(8);
    let argon_task_report = execute_worker_task_with_observer(
        &argon_spec,
        "run-argon-obs",
        "task-1",
        None,
        &mut cancel_rx,
        Some(argon_tx),
    )
    .await
    .expect("execute_worker_task_with_observer on argon missing effort");

    assert_eq!(
        argon_task_report.worker_report.status,
        WorkerRunStatus::MissingEffortEvidence
    );
    assert_eq!(argon_task_report.worker_report.dropped_live_events, 0);
    assert_eq!(
        argon_task_report.overall_acceptance_state,
        WorkAcceptanceState::FinishedNeedsEvidenceAndReview
    );

    let mut observed_kinds = Vec::new();
    while let Some(ev) = argon_rx.recv().await {
        observed_kinds.push(ev.kind);
    }
    assert_eq!(
        observed_kinds,
        vec![
            NormalizedEventKind::Init,
            NormalizedEventKind::Progress,
            NormalizedEventKind::Result,
        ]
    );
}
