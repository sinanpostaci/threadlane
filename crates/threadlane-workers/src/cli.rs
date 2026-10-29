use std::fs;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::error::WorkerError;
use crate::fs_safety::validate_absolute_no_symlink_components;
use crate::git::{
    cleanup_worktree, evaluate_diff_gate, execute_worker_task, CleanupRequest, WorkAcceptanceState,
    WorktreeManager, WorktreeManifest,
};
use crate::spec::WorkerSpec;

/// Maximum byte size permitted for a JSON worker spec file loaded by the CLI (512 KiB).
pub const MAX_SPEC_FILE_BYTES: u64 = 524_288;

/// Narrow trusted diagnostic CLI for exercising Threadlane's M2 worker and Git worktree foundation.
///
/// This binary is a local diagnostic tool and is not a public MCP launch interface.
#[derive(Debug, Parser)]
#[command(
    name = "threadlane-worker",
    version,
    about = "Narrow diagnostic CLI for Threadlane M2 worker process supervision and Git worktree isolation"
)]
pub struct WorkerCli {
    /// Diagnostic subcommand to execute.
    #[command(subcommand)]
    pub command: WorkerCommand,
}

/// Subcommands supported by `threadlane-worker`.
#[derive(Debug, Subcommand)]
pub enum WorkerCommand {
    /// Validate a JSON worker spec and output the resolved model and exact constructed argv.
    ValidateSpec {
        /// Path to the JSON worker specification file.
        #[arg(long)]
        spec: PathBuf,
    },
    /// Create an owned per-run integration worktree (and optional per-task worktree) from a spec.
    CreateWorktrees {
        /// Path to the JSON worker specification file.
        #[arg(long)]
        spec: PathBuf,
        /// Run identifier (`[A-Za-z0-9._-]`).
        #[arg(long)]
        run_id: String,
        /// Optional task identifier (`[A-Za-z0-9._-]`) to also create a per-task worktree.
        #[arg(long)]
        task_id: Option<String>,
    },
    /// Run a configured worker inside its owned task worktree and evaluate the post-run diff gate.
    Run {
        /// Path to the JSON worker specification file.
        #[arg(long)]
        spec: PathBuf,
        /// Run identifier used when creating a new task worktree.
        #[arg(long, default_value = "diag-run")]
        run_id: String,
        /// Task identifier used when creating a new task worktree.
        #[arg(long, default_value = "diag-task")]
        task_id: String,
        /// Optional existing owned task `worktree_id` inside `work_area_root` to execute in.
        #[arg(long)]
        worktree_id: Option<String>,
    },
    /// Evaluate the post-run Git diff gate on an existing owned worktree.
    DiffGate {
        /// Absolute path to the private work-area root directory.
        #[arg(long)]
        work_area: PathBuf,
        /// Owned worktree identifier (`<work_area>/manifests/<worktree_id>.json`).
        #[arg(long)]
        worktree_id: String,
    },
    /// Explicitly clean up an owned worktree using its private ownership manifest.
    Cleanup {
        /// Absolute path to the private work-area root directory.
        #[arg(long)]
        work_area: PathBuf,
        /// Owned worktree identifier (`<work_area>/manifests/<worktree_id>.json`).
        #[arg(long)]
        worktree_id: String,
    },
}

/// Structured output emitted by `threadlane-worker create-worktrees`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreatedWorktreesReport {
    /// Newly created per-run integration worktree manifest.
    pub run_worktree: WorktreeManifest,
    /// Newly created per-task worktree manifest, if `--task-id` was provided.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_worktree: Option<WorktreeManifest>,
}

/// Executes a parsed [`WorkerCli`] command and prints its structured JSON report to stdout.
pub async fn run_cli(cli: WorkerCli) -> Result<(), WorkerError> {
    match cli.command {
        WorkerCommand::ValidateSpec { spec } => {
            let worker_spec = load_worker_spec_file(&spec)?;
            let validation = worker_spec.validate()?;
            println!("{}", serde_json::to_string_pretty(&validation)?);
            Ok(())
        }
        WorkerCommand::CreateWorktrees {
            spec,
            run_id,
            task_id,
        } => {
            let worker_spec = load_worker_spec_file(&spec)?;
            let _validation = worker_spec.validate()?;
            let manager = WorktreeManager::new(
                &worker_spec.repository_path,
                &worker_spec.work_area_root,
                &worker_spec.base_commit,
            )?;
            let run_worktree = manager.create_run_worktree(&run_id)?;
            let task_worktree = match task_id {
                Some(ref tid) => Some(manager.create_task_worktree(
                    &run_id,
                    tid,
                    &worker_spec.allowed_write_paths,
                )?),
                None => None,
            };
            let report = CreatedWorktreesReport {
                run_worktree,
                task_worktree,
            };
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        WorkerCommand::Run {
            spec,
            run_id,
            task_id,
            worktree_id,
        } => {
            let worker_spec = load_worker_spec_file(&spec)?;
            let (cancel_tx, mut cancel_rx) = watch::channel(false);
            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    let _ = cancel_tx.send(true);
                }
            });

            let report = execute_worker_task(
                &worker_spec,
                &run_id,
                &task_id,
                worktree_id.as_deref(),
                &mut cancel_rx,
            )
            .await?;

            println!("{}", serde_json::to_string_pretty(&report)?);

            if report.overall_acceptance_state != WorkAcceptanceState::DiffGatePassedPendingReview {
                return Err(WorkerError::ExecutionFailed {
                    status: report.worker_report.status.as_str().to_string(),
                    detail: format!(
                        "overall_acceptance_state is `{:?}` (diff_gate.passed = {})",
                        report.overall_acceptance_state, report.diff_gate.passed
                    ),
                });
            }
            Ok(())
        }
        WorkerCommand::DiffGate {
            work_area,
            worktree_id,
        } => {
            let report = evaluate_diff_gate(&work_area, &worktree_id)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            if !report.passed {
                return Err(WorkerError::ExecutionFailed {
                    status: "diff_gate_failed".to_string(),
                    detail: format!(
                        "diff gate rejected {} violation(s)",
                        report.violations.len()
                    ),
                });
            }
            Ok(())
        }
        WorkerCommand::Cleanup {
            work_area,
            worktree_id,
        } => {
            let report = cleanup_worktree(&CleanupRequest {
                work_area_root: work_area,
                worktree_id,
            })?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
    }
}

/// Loads and deserializes a [`WorkerSpec`] from a JSON file path.
pub fn load_worker_spec_file(path: &Path) -> Result<WorkerSpec, WorkerError> {
    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let checked = validate_absolute_no_symlink_components(&resolved, "worker spec file")?;
    let meta = fs::symlink_metadata(&checked)?;
    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
        return Err(WorkerError::SecurityViolation(format!(
            "worker spec file `{}` must be a regular non-symlink file",
            path.display()
        )));
    }
    if meta.len() > MAX_SPEC_FILE_BYTES {
        return Err(WorkerError::InvalidSpec(format!(
            "worker spec file `{}` exceeds maximum size of {MAX_SPEC_FILE_BYTES} bytes",
            path.display()
        )));
    }
    let bytes = fs::read(&checked)?;
    let spec: WorkerSpec = serde_json::from_slice(&bytes)?;
    Ok(spec)
}
