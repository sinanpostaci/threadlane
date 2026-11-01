#![deny(unsafe_code)]
#![deny(missing_docs)]

//! Trusted local operator workflow CLI (`threadlane-workflow`) for native Codex workflows.
//!
//! Exposes binding-scoped workflow profile catalog inspection (`catalog`), plan listing
//! (`list-plans`), task-paginated plan inspection (`get-plan`), draft creation/update
//! (`save-draft`), and explicit draft publication (`publish`) over the existing
//! [`ScopedWorkflow`] service.
//!
//! ## Security and Authority Boundaries
//!
//! - **Explicit local operator route**: `--binding` and `--thread` attest the operator-selected
//!   target chat scope; they do not authenticate per-call native conversation identity.
//! - **Live target binding confirmation**: Validates all three private store directories and
//!   verifies `(binding_id, thread_uuid)` against the live authenticated local supervisor before
//!   opening workflow stores and again immediately before `save-draft` or `publish`.
//! - **No execution or approval side effects**: Never creates approvals, starts hosts, spawns
//!   workers, or invokes external model providers.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use threadlane_native::operator_cli::{
    emit_error, read_request, validate_dirs, verify_binding, write_result, OperatorCliError,
};
use threadlane_native::{
    open_workflow_stores, NativeError, NativeErrorCode, SavePlanDraft, ScopedWorkflow,
    WorkflowError,
};

#[derive(Debug, Parser)]
#[command(
    name = "threadlane-workflow",
    version,
    about = "Trusted local operator CLI for Threadlane workflow catalog and plan operations"
)]
struct Cli {
    /// Normalized absolute path to the private running supervisor state directory (`supervisor.json`).
    #[arg(long = "state-dir", global = true, value_name = "ABS")]
    state_dir: Option<PathBuf>,

    /// Normalized absolute path to the private global workflow registry store directory (`registry.json`).
    #[arg(long = "registry-dir", global = true, value_name = "ABS")]
    registry_dir: Option<PathBuf>,

    /// Normalized absolute path to the private workflow plan store directory (`plans.json`).
    #[arg(long = "plans-dir", global = true, value_name = "ABS")]
    plans_dir: Option<PathBuf>,

    /// Target control-plane chat binding identifier.
    #[arg(long = "binding", global = true, value_name = "ID")]
    binding: Option<String>,

    /// Target canonical lowercase hyphenated non-nil native Codex thread UUID.
    #[arg(long = "thread", global = true, value_name = "UUID")]
    thread: Option<String>,

    /// Workflow subcommand to execute.
    #[command(subcommand)]
    command: WorkflowCommand,
}

#[derive(Debug, Subcommand)]
#[command(rename_all = "kebab-case")]
enum WorkflowCommand {
    /// List a bounded page of configured workflow profiles from the global registry.
    Catalog {
        /// Optional opaque pagination cursor returned by a previous `catalog` page.
        #[arg(long = "cursor", value_name = "TEXT")]
        cursor: Option<String>,
        /// Optional maximum number of workflow profiles to return (`1..=20`, defaults to `20`).
        #[arg(long = "limit", value_name = "N")]
        limit: Option<usize>,
    },
    /// List a bounded page of plan summaries belonging to the target chat binding.
    ListPlans {
        /// Optional opaque pagination cursor returned by a previous `list-plans` page.
        #[arg(long = "cursor", value_name = "TEXT")]
        cursor: Option<String>,
        /// Optional maximum number of plan summaries to return (`1..=20`, defaults to `20`).
        #[arg(long = "limit", value_name = "N")]
        limit: Option<usize>,
    },
    /// Fetch a plan revision header and a bounded page of full task definitions.
    GetPlan {
        /// Target logical plan identifier belonging to the target chat binding.
        #[arg(long = "plan-id", value_name = "ID")]
        plan_id: String,
        /// Optional 1-based plan revision number (`None` fetches the latest revision).
        #[arg(long = "revision", value_name = "N")]
        revision: Option<u64>,
        /// Optional opaque task pagination cursor returned by a previous `get-plan` page.
        #[arg(long = "cursor", value_name = "TEXT")]
        cursor: Option<String>,
        /// Optional maximum number of full tasks to return (`1..=20`, defaults to `10`).
        #[arg(long = "limit", value_name = "N")]
        limit: Option<usize>,
    },
    /// Create or update a draft plan revision from a private `SavePlanDraft` JSON file.
    SaveDraft {
        /// Normalized absolute path to the private JSON request file (`SavePlanDraft`).
        #[arg(long = "request", value_name = "ABS_PRIVATE_JSON")]
        request: PathBuf,
    },
    /// Explicitly publish a draft plan revision (`PlanChange::Publish` only).
    Publish {
        /// Target logical plan identifier belonging to the target chat binding.
        #[arg(long = "plan-id", value_name = "ID")]
        plan_id: String,
        /// Target 1-based draft plan revision number to publish.
        #[arg(long = "revision", value_name = "N")]
        revision: u64,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(err) => {
            if matches!(
                err.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            ) {
                let _ = err.print();
                return ExitCode::SUCCESS;
            }
            emit_error(&OperatorCliError::invalid_input(
                "invalid or missing command-line arguments",
            ));
            return ExitCode::from(1);
        }
    };

    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            emit_error(&err);
            ExitCode::from(1)
        }
    }
}

async fn run(cli: Cli) -> Result<(), OperatorCliError> {
    let (Some(state_dir), Some(registry_dir), Some(plans_dir), Some(binding_id), Some(thread_uuid)) = (
        cli.state_dir,
        cli.registry_dir,
        cli.plans_dir,
        cli.binding,
        cli.thread,
    ) else {
        return Err(OperatorCliError::invalid_input(
            "invalid or missing command-line arguments",
        ));
    };

    let validated_dirs = validate_dirs(&[
        ("state_dir", &state_dir),
        ("registry_dir", &registry_dir),
        ("plans_dir", &plans_dir),
    ])?;
    let clean_state_dir = &validated_dirs[0];
    let clean_registry_dir = &validated_dirs[1];
    let clean_plans_dir = &validated_dirs[2];

    verify_binding(clean_state_dir, &binding_id, &thread_uuid).await?;

    let Some((registry, plans)) =
        open_workflow_stores(Some(clean_registry_dir), Some(clean_plans_dir))
            .map_err(|err| map_native_error(&err))?
    else {
        return Err(OperatorCliError::invalid_input(
            "workflow registry and plan stores could not be opened",
        ));
    };

    let _ = validate_dirs(&[
        ("state_dir", clean_state_dir),
        ("registry_dir", registry.config_dir()),
        ("plans_dir", plans.dir()),
    ])?;

    let workflow =
        ScopedWorkflow::new(registry, plans, binding_id).map_err(|err| map_workflow_error(&err))?;

    match cli.command {
        WorkflowCommand::Catalog { cursor, limit } => {
            let page = workflow
                .catalog(cursor, limit)
                .map_err(|err| map_workflow_error(&err))?;
            write_result(workflow.binding_id(), &page, false)
        }
        WorkflowCommand::ListPlans { cursor, limit } => {
            let page = workflow
                .list_plans(cursor, limit)
                .map_err(|err| map_workflow_error(&err))?;
            write_result(workflow.binding_id(), &page, false)
        }
        WorkflowCommand::GetPlan {
            plan_id,
            revision,
            cursor,
            limit,
        } => {
            let page = workflow
                .get_plan(&plan_id, revision, cursor, limit)
                .map_err(|err| map_workflow_error(&err))?;
            write_result(workflow.binding_id(), &page, false)
        }
        WorkflowCommand::SaveDraft { request } => {
            let draft: SavePlanDraft = read_request(&request)?;
            verify_binding(clean_state_dir, workflow.binding_id(), &thread_uuid).await?;
            let summary = workflow
                .save_draft(draft)
                .map_err(|err| map_workflow_mutation_error(&err))?;
            write_result(workflow.binding_id(), &summary, true)
        }
        WorkflowCommand::Publish { plan_id, revision } => {
            verify_binding(clean_state_dir, workflow.binding_id(), &thread_uuid).await?;
            let published = workflow
                .publish(&plan_id, revision)
                .map_err(|err| map_workflow_mutation_error(&err))?;
            write_result(workflow.binding_id(), &published, true)
        }
    }
}

const fn map_workflow_error(err: &WorkflowError) -> OperatorCliError {
    OperatorCliError::new(err.code(), err.safe_message())
}

/// Maps [`WorkflowError`] returned after entering a mutating workflow command (`save-draft`
/// or `publish`).
///
/// Because [`ScopedWorkflow::save_draft`] and [`ScopedWorkflow::publish`] can persist a
/// [`threadlane_native::PlanStore`] revision before failing while encoding the response or writing the
/// completion journal—and [`WorkflowError`] does not carry stage provenance—opaque I/O,
/// serialization, and response-size failures inside a mutating call are conservatively
/// reported as `outcome_unknown` so operators reconcile with `list-plans` or `get-plan`
/// instead of blindly replaying. Deterministic pre-effect validation, not-found, stale
/// revision, idempotency, and security refusals retain their original error codes.
const fn map_workflow_mutation_error(err: &WorkflowError) -> OperatorCliError {
    match err {
        WorkflowError::Io(_)
        | WorkflowError::Serialization(_)
        | WorkflowError::ResponseTooLarge(_) => OperatorCliError::new(
            "outcome_unknown",
            "workflow mutation outcome is uncertain after an I/O, serialization, or response error; reconcile with list-plans or get-plan before retrying",
        ),
        WorkflowError::InvalidInput(_)
        | WorkflowError::InvalidCursor(_)
        | WorkflowError::NotFound(_)
        | WorkflowError::StaleRevision(_)
        | WorkflowError::ProfileRevisionMismatch(_)
        | WorkflowError::RegistryResolution(_)
        | WorkflowError::DuplicateTaskId(_)
        | WorkflowError::UnknownDependency(_)
        | WorkflowError::DependencyCycle(_)
        | WorkflowError::InvalidWriteScope(_)
        | WorkflowError::InvalidStateTransition(_)
        | WorkflowError::IdempotencyConflict(_)
        | WorkflowError::CorruptStore(_)
        | WorkflowError::SecurityViolation(_)
        | WorkflowError::UnsupportedPlatform(_) => map_workflow_error(err),
    }
}

fn map_native_error(err: &NativeError) -> OperatorCliError {
    match err.error_code() {
        NativeErrorCode::SecurityViolation => OperatorCliError::new(
            "security_violation",
            "workflow store directory failed private permission or symlink security checks",
        ),
        NativeErrorCode::UnsupportedPlatform => OperatorCliError::new(
            "unsupported_platform",
            "threadlane-workflow is only supported on Unix platforms",
        ),
        NativeErrorCode::IoError => OperatorCliError::new(
            "io_error",
            "filesystem I/O error while opening workflow stores",
        ),
        _ => OperatorCliError::invalid_input("workflow store configuration or state is invalid"),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    use tempfile::TempDir;
    use threadlane::{Supervisor, SupervisorClient, SupervisorConfig};
    use threadlane_native::{
        Difficulty, PlanStatus, PlanStore, PlanTask, RegistryStore, ToolAdapter, WorkflowProfile,
        WorkflowRole,
    };
    use threadlane_workflow::{
        ModelChoice, ModelDefinition, RegistryChange, RoleAssignment, ToolDefinition,
    };

    use super::*;

    const TEST_THREAD_UUID: &str = "0196ed00-0000-7000-8000-000000000011";

    #[test]
    fn global_args_parse_before_and_after_subcommand() {
        let before = Cli::try_parse_from([
            "threadlane-workflow",
            "--state-dir",
            "/tmp/state",
            "--registry-dir",
            "/tmp/registry",
            "--plans-dir",
            "/tmp/plans",
            "--binding",
            "bnd_1",
            "--thread",
            TEST_THREAD_UUID,
            "catalog",
            "--limit",
            "5",
        ])
        .expect("parse global args before subcommand");
        assert_eq!(before.binding.as_deref(), Some("bnd_1"));

        let after = Cli::try_parse_from([
            "threadlane-workflow",
            "get-plan",
            "--plan-id",
            "plan-1",
            "--revision",
            "2",
            "--cursor",
            "cur-1",
            "--limit",
            "10",
            "--state-dir",
            "/tmp/state",
            "--registry-dir",
            "/tmp/registry",
            "--plans-dir",
            "/tmp/plans",
            "--binding",
            "bnd_1",
            "--thread",
            TEST_THREAD_UUID,
        ])
        .expect("parse global args after subcommand");
        assert_eq!(after.binding.as_deref(), Some("bnd_1"));
    }

    #[tokio::test]
    async fn workflow_cli_runs_save_draft_and_publish_against_live_supervisor() {
        let tmp = TempDir::new().expect("tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize tempdir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("chmod root 0700");

        let state_dir = root.join("state");
        let registry_dir = root.join("registry");
        let plans_dir = root.join("plans");
        let req_dir = root.join("requests");
        fs::create_dir(&req_dir).expect("create req_dir");
        fs::set_permissions(&req_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod req_dir 0700");

        let sup = Supervisor::start(SupervisorConfig {
            state_dir: state_dir.clone(),
            tick_interval: Duration::from_millis(50),
            codex: None,
            workers: None,
        })
        .await
        .expect("start supervisor");
        let sup_client = SupervisorClient::from_state_dir(&state_dir).expect("supervisor client");
        let binding = sup_client
            .create_binding(TEST_THREAD_UUID, "Workflow CLI Unit Test")
            .await
            .expect("create binding");

        let registry = RegistryStore::open(&registry_dir).expect("open registry");
        let plans = PlanStore::open(&plans_dir).expect("open plans");

        let s1 = registry
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "opencode".to_string(),
                        label: "OpenCode v2".to_string(),
                        adapter: ToolAdapter::OpencodeV2,
                        executable_path: Some(PathBuf::from("/bin/sh")),
                        enabled: true,
                    },
                },
            )
            .expect("upsert tool");
        let s2 = registry
            .apply(
                s1.revision,
                RegistryChange::UpsertModel {
                    model: ModelDefinition {
                        id: "deepseek-flash".to_string(),
                        tool_id: "opencode".to_string(),
                        label: "DeepSeek Flash".to_string(),
                        model_arg: "opencode-go/deepseek-v4.1-flash".to_string(),
                        efforts: vec!["max".to_string()],
                        allow_no_effort: false,
                    },
                },
            )
            .expect("upsert model");
        let s3 = registry
            .apply(
                s2.revision,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        decision_advisor: None,
                        id: "default-profile".to_string(),
                        name: "Default Profile".to_string(),
                        assignments: vec![RoleAssignment {
                            role: WorkflowRole::Coder,
                            difficulty: Some(Difficulty::Easy),
                            choice: ModelChoice {
                                model_id: "deepseek-flash".to_string(),
                                effort: Some("max".to_string()),
                            },
                        }],
                    },
                },
            )
            .expect("upsert profile");

        let draft = SavePlanDraft {
            command_key: "cmd-unit-1".to_string(),
            plan_id: Some("plan-unit-1".to_string()),
            expected_revision: 0,
            profile_id: "default-profile".to_string(),
            profile_revision: s3.revision,
            summary: "Unit test workflow draft".to_string(),
            tasks: vec![PlanTask {
                id: "task-1".to_string(),
                title: "Task 1".to_string(),
                instruction: "Implement unit test slice".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Easy,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            }],
        };
        let req_path = req_dir.join("draft.json");
        fs::write(&req_path, serde_json::to_vec(&draft).expect("encode draft"))
            .expect("write draft");
        fs::set_permissions(&req_path, fs::Permissions::from_mode(0o600)).expect("chmod 0600");

        let save_cli = Cli {
            state_dir: Some(state_dir.clone()),
            registry_dir: Some(registry_dir.clone()),
            plans_dir: Some(plans_dir.clone()),
            binding: Some(binding.binding_id.clone()),
            thread: Some(TEST_THREAD_UUID.to_string()),
            command: WorkflowCommand::SaveDraft {
                request: req_path.clone(),
            },
        };
        run(save_cli).await.expect("save-draft succeeds");

        let saved_rev = plans
            .get_revision("plan-unit-1", 1)
            .expect("read plan")
            .expect("rev 1 exists");
        assert_eq!(saved_rev.status, PlanStatus::Draft);

        let publish_cli = Cli {
            state_dir: Some(state_dir.clone()),
            registry_dir: Some(registry_dir.clone()),
            plans_dir: Some(plans_dir.clone()),
            binding: Some(binding.binding_id.clone()),
            thread: Some(TEST_THREAD_UUID.to_string()),
            command: WorkflowCommand::Publish {
                plan_id: "plan-unit-1".to_string(),
                revision: 1,
            },
        };
        run(publish_cli).await.expect("publish succeeds");

        let pub_rev = plans
            .get_revision("plan-unit-1", 1)
            .expect("read plan")
            .expect("rev 1 exists");
        assert_eq!(pub_rev.status, PlanStatus::Published);

        sup.shutdown().await;
    }

    #[test]
    fn workflow_read_vs_mutation_error_contracts_preserve_refusals_and_mark_ambiguous_mutations_outcome_unknown(
    ) {
        let ambiguous_cases = [
            (
                WorkflowError::from_plan_error(&threadlane_workflow::PlanError::Io(
                    "/secret/workflow/journal write failure".to_string(),
                )),
                "io_error",
            ),
            (
                WorkflowError::from_plan_error(&threadlane_workflow::PlanError::Serialization(
                    "/secret/workflow/serialization failure".to_string(),
                )),
                "serialization_error",
            ),
            (
                WorkflowError::ResponseTooLarge("workflow response exceeded cap"),
                "response_too_large",
            ),
        ];

        for (err, expected_read_code) in &ambiguous_cases {
            let read_mapped = map_workflow_error(err);
            assert_eq!(read_mapped.code, *expected_read_code);
            assert!(!read_mapped.message.contains("/secret/workflow"));

            let mutation_mapped = map_workflow_mutation_error(err);
            assert_eq!(mutation_mapped.code, "outcome_unknown");
            assert!(
                mutation_mapped.message.contains("reconcile"),
                "mutation outcome_unknown message must instruct reconciliation: {}",
                mutation_mapped.message
            );
            assert!(!mutation_mapped.message.contains("/secret/workflow"));
        }

        let refusal_cases = [
            (WorkflowError::InvalidInput("invalid"), "invalid_input"),
            (WorkflowError::InvalidCursor("cursor"), "invalid_cursor"),
            (WorkflowError::NotFound("not found"), "not_found"),
            (WorkflowError::StaleRevision("stale"), "stale_revision"),
            (
                WorkflowError::ProfileRevisionMismatch("profile mismatch"),
                "profile_revision_mismatch",
            ),
            (
                WorkflowError::RegistryResolution("registry"),
                "registry_resolution_failed",
            ),
            (WorkflowError::DuplicateTaskId("dup"), "duplicate_task_id"),
            (
                WorkflowError::UnknownDependency("dep"),
                "unknown_dependency",
            ),
            (WorkflowError::DependencyCycle("cycle"), "dependency_cycle"),
            (
                WorkflowError::InvalidWriteScope("scope"),
                "invalid_write_scope",
            ),
            (
                WorkflowError::InvalidStateTransition("transition"),
                "invalid_state_transition",
            ),
            (
                WorkflowError::IdempotencyConflict("conflict"),
                "idempotency_conflict",
            ),
            (WorkflowError::CorruptStore("corrupt"), "corrupt_store"),
            (
                WorkflowError::SecurityViolation("security"),
                "security_violation",
            ),
            (
                WorkflowError::UnsupportedPlatform("platform"),
                "unsupported_platform",
            ),
        ];

        for (err, expected_code) in &refusal_cases {
            let read_mapped = map_workflow_error(err);
            let mutation_mapped = map_workflow_mutation_error(err);
            assert_eq!(read_mapped.code, *expected_code);
            assert_eq!(mutation_mapped.code, *expected_code);
            assert_eq!(read_mapped.message, mutation_mapped.message);
        }
    }
}
