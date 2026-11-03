#![cfg(target_os = "linux")]

use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use tempfile::TempDir;
use tokio::sync::watch;

use threadlane_workers::{
    is_process_alive, is_process_group_alive, run_bounded_command, run_bounded_command_with_cancel,
    BoundedCommandBudgets, BoundedCommandOverflow, BoundedCommandSpec, BoundedCommandStatus,
    WorkerError,
};

fn write_executable_script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let staging = dir.join(format!(".{name}.tmp"));
    let final_path = dir.join(name);
    {
        let mut f = File::create(&staging).expect("create staging script");
        f.write_all(body.as_bytes()).expect("write script body");
        f.sync_all().expect("sync script");
    }
    fs::set_permissions(&staging, fs::Permissions::from_mode(0o755)).expect("chmod 0755");
    fs::rename(&staging, &final_path).expect("rename executable into place");
    fs::canonicalize(&final_path).expect("canonicalize executable")
}

#[tokio::test]
async fn exact_argv_cwd_literal_metacharacters_and_multibyte_output_over_256_bytes_succeed() {
    let tmp = TempDir::new().expect("tempdir");
    let root = fs::canonicalize(tmp.path()).expect("canonical root");
    let bin_dir = root.join("bin");
    let work_dir = root.join("work/nested");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");
    fs::create_dir_all(&work_dir).expect("create work_dir");

    let touched_marker = root.join("MUST_NOT_EXIST");
    let script = write_executable_script(
        &bin_dir,
        "fake-check-ok",
        r#"#!/bin/sh
set -eu
pwd_val="$(pwd -P)"
printf 'CWD=%s\n' "$pwd_val"
printf 'ARGC=%d\n' "$#"
idx=1
for arg in "$@"; do
    printf 'ARG%d=%s\n' "$idx" "$arg"
    idx=$((idx + 1))
done
# Emit >256 bytes with multibyte UTF-8 characters (Turkish/CJK/emoji)
printf 'UTF8_BLOCK='
i=0
while [ "$i" -lt 12 ]; do
    printf 'çğıöşü-日本語-🚀-[%02d]|' "$i"
    i=$((i + 1))
done
printf '\n'
printf 'STDERR_DIAG=ok-çğıöşü\n' >&2
exit 0
"#,
    );

    let literal_metachar_arg = format!(
        "literal; touch {} | $(echo pwn) && `id` * > /tmp/nope",
        touched_marker.display()
    );
    let spec = BoundedCommandSpec {
        executable: script.clone(),
        argv: vec![
            "--check".to_string(),
            literal_metachar_arg.clone(),
            "αβγδ-end".to_string(),
        ],
        working_directory: work_dir.clone(),
        env_overrides: vec![("CUSTOM_CHECK_FLAG".to_string(), "enabled".to_string())],
        strip_git_env: true,
        budgets: BoundedCommandBudgets {
            timeout_ms: 5_000,
            max_stdout_bytes: 2_048,
            max_stderr_bytes: 512,
        },
    };

    let report = run_bounded_command(&spec)
        .await
        .expect("run bounded command");
    assert!(report.passed(), "expected passed() == true, got {report:?}");
    assert_eq!(report.status, BoundedCommandStatus::Succeeded);
    assert_eq!(report.exit_code, Some(0));
    assert_eq!(report.signal, None);
    assert!(!report.stdout_overflowed);
    assert!(!report.stderr_overflowed);
    assert!(!report.cleanup_incomplete);
    assert!(
        !touched_marker.exists(),
        "shell metacharacters in argv must remain inert literals"
    );

    assert!(
        report.stdout_bytes_captured > 256,
        "expected >256 stdout bytes, got {}",
        report.stdout_bytes_captured
    );
    assert_eq!(report.stdout_bytes.len(), report.stdout_bytes_captured);
    let stdout_text = String::from_utf8(report.stdout_bytes.clone()).expect("valid UTF-8 stdout");
    assert!(stdout_text.contains(&format!("CWD={}\n", work_dir.display())));
    assert!(stdout_text.contains("ARGC=3\n"));
    assert!(stdout_text.contains("ARG1=--check\n"));
    assert!(stdout_text.contains(&format!("ARG2={literal_metachar_arg}\n")));
    assert!(stdout_text.contains("ARG3=αβγδ-end\n"));
    assert!(stdout_text.contains("çğıöşü-日本語-🚀-[11]|"));

    let stderr_text = String::from_utf8(report.stderr_bytes.clone()).expect("valid UTF-8 stderr");
    assert_eq!(stderr_text, "STDERR_DIAG=ok-çğıöşü\n");
    assert!(report.redacted_stdout_summary.is_some());
    assert!(report.redacted_stderr_summary.is_some());
    if let Some(pgid) = report.process_group_id {
        assert!(!is_process_group_alive(pgid));
    }
}

#[tokio::test]
async fn exact_small_stdout_and_stderr_caps_without_4096_clamp_enforce_overflow_and_reap_group() {
    let tmp = TempDir::new().expect("tempdir");
    let root = fs::canonicalize(tmp.path()).expect("canonical root");
    let bin_dir = root.join("bin");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let script = write_executable_script(
        &bin_dir,
        "fake-check-cap",
        r#"#!/bin/sh
set -eu
mode="$1"
count="$2"
if [ "$mode" = "stdout" ]; then
    head -c "$count" /dev/zero | tr '\0' 'A'
elif [ "$mode" = "stdout_sleep" ]; then
    head -c "$count" /dev/zero | tr '\0' 'B'
    sleep 30
else
    head -c "$count" /dev/zero | tr '\0' 'E' >&2
fi
exit 0
"#,
    );

    // 1. Exact 48-byte stdout cap succeeds when command emits 48 bytes.
    let exact_spec = BoundedCommandSpec {
        executable: script.clone(),
        argv: vec!["stdout".to_string(), "48".to_string()],
        working_directory: root.clone(),
        env_overrides: Vec::new(),
        strip_git_env: true,
        budgets: BoundedCommandBudgets {
            timeout_ms: 5_000,
            max_stdout_bytes: 48,
            max_stderr_bytes: 32,
        },
    };
    let exact_report = run_bounded_command(&exact_spec)
        .await
        .expect("run exact cap");
    assert!(exact_report.passed());
    assert_eq!(exact_report.status, BoundedCommandStatus::Succeeded);
    assert_eq!(exact_report.stdout_bytes_captured, 48);
    assert_eq!(exact_report.stdout_bytes, vec![b'A'; 48]);

    // 2. 49 bytes against 48-byte stdout cap fails with OutputOverflow and kills sleeping process group.
    let stdout_over_spec = BoundedCommandSpec {
        executable: script.clone(),
        argv: vec!["stdout_sleep".to_string(), "49".to_string()],
        working_directory: root.clone(),
        env_overrides: Vec::new(),
        strip_git_env: true,
        budgets: BoundedCommandBudgets {
            timeout_ms: 10_000,
            max_stdout_bytes: 48,
            max_stderr_bytes: 32,
        },
    };
    let stdout_over_report = run_bounded_command(&stdout_over_spec)
        .await
        .expect("run stdout overflow");
    assert!(!stdout_over_report.passed());
    assert_eq!(
        stdout_over_report.status,
        BoundedCommandStatus::OutputOverflow
    );
    assert!(stdout_over_report.stdout_overflowed);
    assert_eq!(
        stdout_over_report.overflow,
        Some(BoundedCommandOverflow::StdoutExceeded {
            max_stdout_bytes: 48
        })
    );
    assert_eq!(stdout_over_report.stdout_bytes_captured, 48);
    assert_eq!(stdout_over_report.stdout_bytes.len(), 48);
    assert!(
        stdout_over_report.duration_ms < 5_000,
        "overflow must terminate sleeping child immediately without waiting for timeout"
    );
    let pgid = stdout_over_report.process_group_id.expect("pgid");
    assert!(!is_process_group_alive(pgid));

    // 3. 33 bytes against 32-byte stderr cap fails with OutputOverflow even if child exits 0.
    let stderr_over_spec = BoundedCommandSpec {
        executable: script,
        argv: vec!["stderr".to_string(), "33".to_string()],
        working_directory: root,
        env_overrides: Vec::new(),
        strip_git_env: true,
        budgets: BoundedCommandBudgets {
            timeout_ms: 5_000,
            max_stdout_bytes: 48,
            max_stderr_bytes: 32,
        },
    };
    let stderr_over_report = run_bounded_command(&stderr_over_spec)
        .await
        .expect("run stderr overflow");
    assert!(!stderr_over_report.passed());
    assert_eq!(
        stderr_over_report.status,
        BoundedCommandStatus::OutputOverflow
    );
    assert!(stderr_over_report.stderr_overflowed);
    assert_eq!(
        stderr_over_report.overflow,
        Some(BoundedCommandOverflow::StderrExceeded {
            max_stderr_bytes: 32
        })
    );
    assert_eq!(stderr_over_report.stderr_bytes_captured, 32);
    assert_eq!(stderr_over_report.stderr_bytes.len(), 32);
}

#[tokio::test]
async fn nonzero_exit_timeout_and_cancellation_never_pass_and_reap_descendants() {
    let tmp = TempDir::new().expect("tempdir");
    let root = fs::canonicalize(tmp.path()).expect("canonical root");
    let bin_dir = root.join("bin");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");

    let pid_file = root.join("grandchild.pid");
    let script = write_executable_script(
        &bin_dir,
        "fake-check-modes",
        r#"#!/bin/sh
set -eu
mode="$1"
pid_out="$2"
if [ "$mode" = "nonzero" ]; then
    printf 'check failed\n' >&2
    exit 17
elif [ "$mode" = "sleep_with_descendant" ]; then
    sleep 30 &
    echo $! > "$pid_out"
    printf 'started\n'
    wait
elif [ "$mode" = "waited_descendant_exit0" ]; then
    (sleep 0.02; printf 'child-done\n') &
    child_pid=$!
    echo "$child_pid" > "$pid_out"
    wait "$child_pid"
    printf 'done-parent\n'
    exit 0
elif [ "$mode" = "lingering_descendant_exit0" ]; then
    sleep 30 &
    echo $! > "$pid_out"
    printf 'done-parent\n'
    exit 0
fi
"#,
    );

    // 1. Non-zero exit
    let nonzero_spec = BoundedCommandSpec {
        executable: script.clone(),
        argv: vec![
            "nonzero".to_string(),
            pid_file.to_string_lossy().to_string(),
        ],
        working_directory: root.clone(),
        env_overrides: Vec::new(),
        strip_git_env: true,
        budgets: BoundedCommandBudgets {
            timeout_ms: 5_000,
            max_stdout_bytes: 512,
            max_stderr_bytes: 512,
        },
    };
    let nonzero_report = run_bounded_command(&nonzero_spec)
        .await
        .expect("nonzero run");
    assert!(!nonzero_report.passed());
    assert_eq!(nonzero_report.status, BoundedCommandStatus::NonZeroExit);
    assert_eq!(nonzero_report.exit_code, Some(17));

    // 2. Timeout kills direct child + descendant in process group
    let timeout_spec = BoundedCommandSpec {
        executable: script.clone(),
        argv: vec![
            "sleep_with_descendant".to_string(),
            pid_file.to_string_lossy().to_string(),
        ],
        working_directory: root.clone(),
        env_overrides: Vec::new(),
        strip_git_env: true,
        budgets: BoundedCommandBudgets {
            timeout_ms: 120,
            max_stdout_bytes: 512,
            max_stderr_bytes: 512,
        },
    };
    let timeout_report = run_bounded_command(&timeout_spec)
        .await
        .expect("timeout run");
    assert!(!timeout_report.passed());
    assert_eq!(timeout_report.status, BoundedCommandStatus::TimedOut);
    let pgid = timeout_report.process_group_id.expect("pgid");
    assert!(!is_process_group_alive(pgid));
    if let Ok(raw_pid) = fs::read_to_string(&pid_file) {
        if let Ok(grandchild_pid) = raw_pid.trim().parse::<u32>() {
            assert!(!is_process_alive(grandchild_pid));
        }
    }

    // 3. Explicit cancellation kills direct child + descendant in process group
    let _ = fs::remove_file(&pid_file);
    let cancel_spec = BoundedCommandSpec {
        executable: script.clone(),
        argv: vec![
            "sleep_with_descendant".to_string(),
            pid_file.to_string_lossy().to_string(),
        ],
        working_directory: root.clone(),
        env_overrides: Vec::new(),
        strip_git_env: true,
        budgets: BoundedCommandBudgets {
            timeout_ms: 10_000,
            max_stdout_bytes: 512,
            max_stderr_bytes: 512,
        },
    };
    let (cancel_tx, mut cancel_rx) = watch::channel(false);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = cancel_tx.send(true);
    });
    let cancel_report = run_bounded_command_with_cancel(&cancel_spec, &mut cancel_rx)
        .await
        .expect("cancel run");
    assert!(!cancel_report.passed());
    assert_eq!(cancel_report.status, BoundedCommandStatus::Cancelled);
    let cancel_pgid = cancel_report.process_group_id.expect("pgid");
    assert!(!is_process_group_alive(cancel_pgid));
    if let Ok(raw_pid) = fs::read_to_string(&pid_file) {
        if let Ok(grandchild_pid) = raw_pid.trim().parse::<u32>() {
            assert!(!is_process_alive(grandchild_pid));
        }
    }

    // 4. Naturally waited child subprocess before parent exit 0 passes cleanly
    let _ = fs::remove_file(&pid_file);
    let waited_spec = BoundedCommandSpec {
        executable: script.clone(),
        argv: vec![
            "waited_descendant_exit0".to_string(),
            pid_file.to_string_lossy().to_string(),
        ],
        working_directory: root.clone(),
        env_overrides: Vec::new(),
        strip_git_env: true,
        budgets: BoundedCommandBudgets {
            timeout_ms: 5_000,
            max_stdout_bytes: 512,
            max_stderr_bytes: 512,
        },
    };
    let waited_report = run_bounded_command(&waited_spec)
        .await
        .expect("waited descendant run");
    assert!(waited_report.passed());
    assert_eq!(waited_report.status, BoundedCommandStatus::Succeeded);
    assert_eq!(waited_report.exit_code, Some(0));
    assert!(!waited_report.descendants_terminated_after_exit);
    assert!(!waited_report.cleanup_incomplete);

    // 5. Lingering descendant in same process group after parent exit 0 is reaped cleanly
    // and classified as DescendantsTerminatedAfterExit (never passing) while preserving exit 0.
    let _ = fs::remove_file(&pid_file);
    let lingering_spec = BoundedCommandSpec {
        executable: script,
        argv: vec![
            "lingering_descendant_exit0".to_string(),
            pid_file.to_string_lossy().to_string(),
        ],
        working_directory: root,
        env_overrides: Vec::new(),
        strip_git_env: true,
        budgets: BoundedCommandBudgets {
            timeout_ms: 5_000,
            max_stdout_bytes: 512,
            max_stderr_bytes: 512,
        },
    };
    let lingering_report = run_bounded_command(&lingering_spec)
        .await
        .expect("lingering descendant run");
    assert!(!lingering_report.passed());
    assert_eq!(
        lingering_report.status,
        BoundedCommandStatus::DescendantsTerminatedAfterExit
    );
    assert_eq!(lingering_report.exit_code, Some(0));
    assert!(lingering_report.descendants_terminated_after_exit);
    assert!(!lingering_report.cleanup_incomplete);
    let lingering_pgid = lingering_report.process_group_id.expect("pgid");
    assert!(!is_process_group_alive(lingering_pgid));
    let raw_pid = fs::read_to_string(&pid_file).expect("read grandchild pid");
    let grandchild_pid: u32 = raw_pid.trim().parse().expect("parse grandchild pid");
    assert!(!is_process_alive(grandchild_pid));
}

#[tokio::test]
async fn validation_rejects_symlink_executable_symlink_workdir_and_git_env_poisoning() {
    let tmp = TempDir::new().expect("tempdir");
    let root = fs::canonicalize(tmp.path()).expect("canonical root");
    let bin_dir = root.join("bin");
    let real_work = root.join("real-work");
    let symlink_work = root.join("symlink-work");
    fs::create_dir_all(&bin_dir).expect("create bin_dir");
    fs::create_dir_all(&real_work).expect("create real_work");
    symlink(&real_work, &symlink_work).expect("create symlink workdir");

    let real_exe = write_executable_script(&bin_dir, "real-exe", "#!/bin/sh\nexit 0\n");
    let symlink_exe = bin_dir.join("symlink-exe");
    symlink(&real_exe, &symlink_exe).expect("create symlink exe");

    let base_budgets = BoundedCommandBudgets {
        timeout_ms: 1_000,
        max_stdout_bytes: 128,
        max_stderr_bytes: 128,
    };

    // Symlink executable is rejected
    let sym_exe_spec = BoundedCommandSpec {
        executable: symlink_exe,
        argv: Vec::new(),
        working_directory: real_work.clone(),
        env_overrides: Vec::new(),
        strip_git_env: true,
        budgets: base_budgets,
    };
    assert!(matches!(
        run_bounded_command(&sym_exe_spec).await,
        Err(WorkerError::SecurityViolation(_))
    ));

    // Symlink working directory is rejected
    let sym_dir_spec = BoundedCommandSpec {
        executable: real_exe.clone(),
        argv: Vec::new(),
        working_directory: symlink_work,
        env_overrides: Vec::new(),
        strip_git_env: true,
        budgets: base_budgets,
    };
    assert!(matches!(
        run_bounded_command(&sym_dir_spec).await,
        Err(WorkerError::SecurityViolation(_))
    ));

    // GIT_* env override when strip_git_env is true is rejected
    let git_env_spec = BoundedCommandSpec {
        executable: real_exe,
        argv: Vec::new(),
        working_directory: real_work,
        env_overrides: vec![("GIT_DIR".to_string(), "/tmp/bad".to_string())],
        strip_git_env: true,
        budgets: base_budgets,
    };
    assert!(matches!(
        run_bounded_command(&git_env_spec).await,
        Err(WorkerError::SecurityViolation(_))
    ));
}
