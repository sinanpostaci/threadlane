use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, Implementation, ServerCapabilities, ServerConfig},
    schemars, tool, tool_handler, tool_router, ServerHandler, ServiceExt,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use threadlane::descriptor::validate_lazy_state_dir;
use threadlane::mcp::{
    AckEventsParams, AnswerParams, CancelParams, CancelWorkerParams, GetRunParams, GetWorkerParams,
    ListActivityParams, ListRunQuestionsParams, ListRunRevisionsParams, ListRunsParams,
    PullEventsParams, StartDemoParams, StartWorkerParams, StatusParams, SteerParams,
};
use threadlane::{ScopedClient, ThreadlaneMcpServer};
use threadlane_workflow::{
    ensure_mcp_response_budget, Difficulty, PlanStore, PlanTask, RegistryStore, SavePlanDraft,
    ScopedWorkflow, WorkflowError, WorkflowRole,
};

use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::constant_time_str_eq;
use crate::pairing::{
    ConnectionIdentity, EnrollmentRecord, PairingStore, CONNECTION_IDENTITY_TOOL,
};

const WORKFLOW_NOT_CONFIGURED_MESSAGE: &str =
    "Workflow registry and plan stores are not configured for this MCP server.";

/// Parameters for the bootstrap `connection_identity` MCP tool (accepts no arguments).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectionIdentityParams {}

/// Parameters for the `catalog` MCP tool.
///
/// Accepts only optional cursor pagination parameters; caller-supplied binding identifiers,
/// thread identifiers, and filesystem paths are rejected.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CatalogParams {
    /// Optional opaque pagination cursor returned by a previous `catalog` page.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Optional maximum number of workflow profiles to return (`1..=20`, defaults to `20`).
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Parameters for the `list_plans` MCP tool.
///
/// Accepts only optional cursor pagination parameters; scope is derived exclusively from the
/// verified enrolled chat binding.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListPlansParams {
    /// Optional opaque pagination cursor returned by a previous `list_plans` page.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Optional maximum number of plan summaries to return (`1..=20`, defaults to `20`).
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Parameters for the `get_plan` MCP tool.
///
/// Fetches a plan revision header and a bounded page of full untruncated task definitions
/// belonging to the enrolled chat binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetPlanParams {
    /// Target logical plan identifier belonging to the enrolled chat binding.
    pub plan_id: String,
    /// Optional 1-based plan revision number (`None` fetches the latest revision).
    #[serde(default)]
    pub revision: Option<u64>,
    /// Optional opaque task pagination cursor returned by a previous `get_plan` page for this revision.
    #[serde(default)]
    pub task_cursor: Option<String>,
    /// Optional maximum number of full tasks to return (`1..=20`, defaults to `10`).
    #[serde(default)]
    pub task_limit: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
enum McpWorkflowRole {
    Follower,
    Orchestrator,
    Architect,
    Coder,
    Reviewer,
    Closure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
enum McpDifficulty {
    Easy,
    Medium,
    Hard,
    Extreme,
}

/// Schema-validated task node within a [`SaveDraftParams`] request.
///
/// Mirrors [`PlanTask`] with strict `#[serde(deny_unknown_fields)]` and `schemars::JsonSchema`
/// support while deserializing [`WorkflowRole`] and [`Difficulty`] through their canonical
/// enum serde implementations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PlanTaskParams {
    /// Unique task identifier within the plan revision.
    pub id: String,
    /// Short human-readable task title.
    pub title: String,
    /// Workflow role used to resolve the target model from the workflow profile (`follower`, `orchestrator`, `architect`, `coder`, `reviewer`, `closure`).
    #[schemars(with = "McpWorkflowRole")]
    pub role: WorkflowRole,
    /// Task difficulty tier used to resolve the target model from the workflow profile (`easy`, `medium`, `hard`, `extreme`).
    #[schemars(with = "McpDifficulty")]
    pub difficulty: Difficulty,
    /// Untrusted full task instruction prompt (never truncated).
    pub instruction: String,
    /// Upstream task identifiers within the same plan that must complete before this task.
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Explicit repository-relative file or directory paths this task is permitted to modify.
    pub allowed_write_paths: Vec<String>,
}

impl From<PlanTaskParams> for PlanTask {
    fn from(task: PlanTaskParams) -> Self {
        Self {
            id: task.id,
            title: task.title,
            instruction: task.instruction,
            role: task.role,
            difficulty: task.difficulty,
            depends_on: task.depends_on,
            allowed_write_paths: task.allowed_write_paths,
        }
    }
}

impl From<PlanTask> for PlanTaskParams {
    fn from(task: PlanTask) -> Self {
        Self {
            id: task.id,
            title: task.title,
            role: task.role,
            difficulty: task.difficulty,
            instruction: task.instruction,
            depends_on: task.depends_on,
            allowed_write_paths: task.allowed_write_paths,
        }
    }
}

/// Type alias for [`PlanTaskParams`].
pub type WorkflowTaskParams = PlanTaskParams;

/// Type alias for [`PlanTaskParams`].
pub type DraftTaskParams = PlanTaskParams;

/// Parameters for the `save_draft` MCP tool.
///
/// Creates or updates a draft plan revision scoped to the enrolled chat binding. Does not
/// accept caller-supplied `binding_id`, `thread_id`, filesystem store paths, CLI executable
/// paths, or operator approval flags.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SaveDraftParams {
    /// Caller-supplied idempotency key scoped to the enrolled chat binding.
    pub command_key: String,
    /// Optional target plan identifier (`None` derives a deterministic ID from `(binding_id, command_key)`).
    #[serde(default)]
    pub plan_id: Option<String>,
    /// Expected current revision of the plan (`0` when creating a new plan).
    pub expected_revision: u64,
    /// Workflow profile identifier used to resolve task roles and difficulties.
    pub profile_id: String,
    /// Pinned registry revision (`registry_revision`) that must match the current registry state.
    pub profile_revision: u64,
    /// Human-readable plan summary.
    pub summary: String,
    /// Non-empty validated task DAG for the draft revision.
    pub tasks: Vec<PlanTaskParams>,
}

impl From<SaveDraftParams> for SavePlanDraft {
    fn from(params: SaveDraftParams) -> Self {
        Self {
            command_key: params.command_key,
            plan_id: params.plan_id,
            expected_revision: params.expected_revision,
            profile_id: params.profile_id,
            profile_revision: params.profile_revision,
            summary: params.summary,
            tasks: params.tasks.into_iter().map(PlanTask::from).collect(),
        }
    }
}

impl From<SavePlanDraft> for SaveDraftParams {
    fn from(draft: SavePlanDraft) -> Self {
        Self {
            command_key: draft.command_key,
            plan_id: draft.plan_id,
            expected_revision: draft.expected_revision,
            profile_id: draft.profile_id,
            profile_revision: draft.profile_revision,
            summary: draft.summary,
            tasks: draft.tasks.into_iter().map(PlanTaskParams::from).collect(),
        }
    }
}

/// Type alias for [`SaveDraftParams`].
pub type SavePlanDraftParams = SaveDraftParams;

/// Parameters for the `publish` MCP tool.
///
/// Explicitly transitions the current draft plan revision to `published` (`PlanChange::Publish`
/// only). Never advances delivery receipts to `received` or `applied`, never grants operator
/// approval, and never starts workers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PublishParams {
    /// Target logical plan identifier belonging to the enrolled chat binding.
    pub plan_id: String,
    /// Target 1-based draft plan revision number to explicitly publish.
    pub revision: u64,
}

/// Type alias for [`PublishParams`].
pub type PublishPlanParams = PublishParams;

/// Shared operator-selected registry and plan-store handles.
pub type WorkflowStoreHandles = (Arc<RegistryStore>, Arc<PlanStore>);

/// Opens optional private operator [`RegistryStore`] and [`PlanStore`] directories.
///
/// Requires both `registry_dir` and `plans_dir` to be provided together. When both are `None`,
/// returns `Ok(None)` without touching the filesystem. When only one is provided, fails with
/// [`NativeErrorCode::InvalidConfig`].
pub fn open_workflow_stores(
    registry_dir: Option<&Path>,
    plans_dir: Option<&Path>,
) -> Result<Option<WorkflowStoreHandles>, NativeError> {
    match (registry_dir, plans_dir) {
        (None, None) => Ok(None),
        (Some(_), None) | (None, Some(_)) => Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "both --registry-dir and --plans-dir must be provided together to enable workflow tools",
        )),
        (Some(reg_path), Some(plans_path)) => {
            let registry = RegistryStore::open(reg_path).map_err(|err| {
                map_workflow_store_open_error(WorkflowError::from_registry_error(&err))
            })?;
            let plans = PlanStore::open(plans_path)
                .map_err(|err| map_workflow_store_open_error(WorkflowError::from_plan_error(&err)))?;
            Ok(Some((Arc::new(registry), Arc::new(plans))))
        }
    }
}

fn map_workflow_store_open_error(err: WorkflowError) -> NativeError {
    let code = match err {
        WorkflowError::SecurityViolation(_) => NativeErrorCode::SecurityViolation,
        WorkflowError::UnsupportedPlatform(_) => NativeErrorCode::UnsupportedPlatform,
        WorkflowError::Io(_) => NativeErrorCode::IoError,
        _ => NativeErrorCode::InvalidConfig,
    };
    NativeError::new(code, err.safe_message())
}

#[derive(Debug, Clone)]
struct WorkflowStores {
    registry: Arc<RegistryStore>,
    plans: Arc<PlanStore>,
}

#[derive(Debug, Clone)]
struct CachedScopedWorkflow {
    connection_nonce: String,
    epoch_id: String,
    server_pid: u32,
    binding_id: String,
    workflow: ScopedWorkflow,
}

/// Paired stdio MCP server that starts `Unbound` with a unique connection nonce/epoch and
/// delegates scoped runtime and workflow tools only after verified operator enrollment.
#[derive(Debug, Clone)]
pub struct PairedMcpServer {
    state_dir: PathBuf,
    pairing_store: PairingStore,
    connection_nonce: String,
    epoch_id: String,
    server_pid: u32,
    workflow_stores: Option<WorkflowStores>,
    cached_workflow: Arc<Mutex<Option<CachedScopedWorkflow>>>,
    tool_router: ToolRouter<Self>,
}

impl PairedMcpServer {
    /// Creates a new `Unbound` [`PairedMcpServer`] registered in `pairing_store` for the current process.
    pub fn new(
        state_dir: impl AsRef<Path>,
        pairing_store: PairingStore,
    ) -> Result<Self, NativeError> {
        Self::with_pid(state_dir, pairing_store, std::process::id())
    }

    /// Creates a new `Unbound` [`PairedMcpServer`] with an explicit `server_pid` (used for process lifecycle tests).
    pub fn with_pid(
        state_dir: impl AsRef<Path>,
        pairing_store: PairingStore,
        server_pid: u32,
    ) -> Result<Self, NativeError> {
        let clean_state_dir = validate_lazy_state_dir(state_dir.as_ref()).map_err(|err| {
            NativeError::new(
                NativeErrorCode::InvalidConfig,
                format!("invalid Threadlane state directory: {err}"),
            )
        })?;
        let identity = pairing_store.register_unbound_epoch(server_pid)?;
        Ok(Self {
            state_dir: clean_state_dir,
            pairing_store,
            connection_nonce: identity.connection_nonce,
            epoch_id: identity.epoch_id,
            server_pid,
            workflow_stores: None,
            cached_workflow: Arc::new(Mutex::new(None)),
            tool_router: Self::tool_router(),
        })
    }

    /// Attaches trusted operator-configured shared [`RegistryStore`] and [`PlanStore`] handles
    /// to this server while preserving the exact `connection_nonce`, `epoch_id`, and `server_pid`.
    pub fn with_workflow_stores(
        mut self,
        registry: Arc<RegistryStore>,
        plans: Arc<PlanStore>,
    ) -> Self {
        self.workflow_stores = Some(WorkflowStores { registry, plans });
        self.cached_workflow = Arc::new(Mutex::new(None));
        self
    }

    /// Opens private [`RegistryStore`] and [`PlanStore`] directories and attaches them to this
    /// server while preserving the exact `connection_nonce`, `epoch_id`, and `server_pid`.
    pub fn with_workflow_dirs(
        self,
        registry_dir: impl AsRef<Path>,
        plans_dir: impl AsRef<Path>,
    ) -> Result<Self, NativeError> {
        let Some((registry, plans)) =
            open_workflow_stores(Some(registry_dir.as_ref()), Some(plans_dir.as_ref()))?
        else {
            return Err(NativeError::new(
                NativeErrorCode::InvalidConfig,
                "workflow stores could not be initialized",
            ));
        };
        Ok(self.with_workflow_stores(registry, plans))
    }

    /// Returns `true` if operator [`RegistryStore`] and [`PlanStore`] handles are configured.
    pub fn has_workflow_stores(&self) -> bool {
        self.workflow_stores.is_some()
    }

    /// Returns a reference to the configured [`RegistryStore`], if any.
    pub fn registry_store(&self) -> Option<&Arc<RegistryStore>> {
        self.workflow_stores.as_ref().map(|s| &s.registry)
    }

    /// Returns a reference to the configured [`PlanStore`], if any.
    pub fn plan_store(&self) -> Option<&Arc<PlanStore>> {
        self.workflow_stores.as_ref().map(|s| &s.plans)
    }

    /// Returns the CSPRNG connection nonce for this MCP server instance.
    pub fn connection_nonce(&self) -> &str {
        &self.connection_nonce
    }

    /// Returns the CSPRNG connection epoch ID for this MCP server instance.
    pub fn epoch_id(&self) -> &str {
        &self.epoch_id
    }

    /// Returns the [`PairingStore`] backing this server.
    pub fn pairing_store(&self) -> &PairingStore {
        &self.pairing_store
    }

    /// Returns the registered MCP tool definitions exposed by this server's router.
    pub fn list_tools(&self) -> Vec<rmcp::model::Tool> {
        self.tool_router.list_all()
    }

    /// Simulates or performs an MCP connection restart, expiring the current `epoch_id`,
    /// clearing any cached workflow service state, and generating a new `Unbound`
    /// `(connection_nonce, epoch_id)`.
    pub fn restart_epoch(&mut self) -> Result<ConnectionIdentity, NativeError> {
        if let Ok(mut guard) = self.cached_workflow.lock() {
            *guard = None;
        }
        let next = self
            .pairing_store
            .register_restarted_epoch(&self.epoch_id, self.server_pid)?;
        self.connection_nonce = next.connection_nonce.clone();
        self.epoch_id = next.epoch_id.clone();
        Ok(next)
    }

    /// Runs the paired MCP server over standard input and standard output (`rmcp::transport::stdio()`).
    pub async fn run_stdio(self) -> Result<(), NativeError> {
        let running = self.serve(rmcp::transport::stdio()).await.map_err(|err| {
            NativeError::new(
                NativeErrorCode::TransportUnavailable,
                format!("failed to initialize paired stdio MCP server: {err}"),
            )
        })?;
        running.waiting().await.map_err(|err| {
            NativeError::new(
                NativeErrorCode::TransportUnavailable,
                format!("paired stdio MCP server task error: {err}"),
            )
        })?;
        Ok(())
    }

    /// Dispatches a tool call by name and JSON arguments (ignoring any caller `_meta` for identity).
    ///
    /// Used both by tests/fixtures and direct tool invocation helpers.
    pub async fn invoke_tool_json(
        &self,
        tool_name: &str,
        arguments: Value,
        _untrusted_meta: Option<Value>,
    ) -> Result<CallToolResult, CallToolResult> {
        // Explicitly ignore `_untrusted_meta`: caller metadata is never trusted for chat identity
        // or enrollment authority.
        match tool_name {
            CONNECTION_IDENTITY_TOOL => {
                let params: ConnectionIdentityParams =
                    Self::parse_tool_params(tool_name, arguments)?;
                self.connection_identity(Parameters(params)).await
            }
            "enroll" | "bind" | "rebind" | "set_binding" => Err(Self::format_pairing_error(
                tool_name,
                NativeError::new(
                    NativeErrorCode::RebindRejected,
                    "operator enrollment is not model-callable over MCP",
                ),
            )),
            "status" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: StatusParams = Self::parse_tool_params(tool_name, arguments)?;
                self.status(Parameters(params)).await
            }
            "get_run" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: GetRunParams = Self::parse_tool_params(tool_name, arguments)?;
                self.get_run(Parameters(params)).await
            }
            "list_runs" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: ListRunsParams = Self::parse_tool_params(tool_name, arguments)?;
                self.list_runs(Parameters(params)).await
            }
            "list_run_revisions" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: ListRunRevisionsParams = Self::parse_tool_params(tool_name, arguments)?;
                self.list_run_revisions(Parameters(params)).await
            }
            "list_run_questions" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: ListRunQuestionsParams = Self::parse_tool_params(tool_name, arguments)?;
                self.list_run_questions(Parameters(params)).await
            }
            "list_activity" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: ListActivityParams = Self::parse_tool_params(tool_name, arguments)?;
                self.list_activity(Parameters(params)).await
            }
            "start_demo" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: StartDemoParams = Self::parse_tool_params(tool_name, arguments)?;
                self.start_demo(Parameters(params)).await
            }
            "pull_events" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: PullEventsParams = Self::parse_tool_params(tool_name, arguments)?;
                self.pull_events(Parameters(params)).await
            }
            "ack_events" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: AckEventsParams = Self::parse_tool_params(tool_name, arguments)?;
                self.ack_events(Parameters(params)).await
            }
            "answer" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: AnswerParams = Self::parse_tool_params(tool_name, arguments)?;
                self.answer(Parameters(params)).await
            }
            "steer" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: SteerParams = Self::parse_tool_params(tool_name, arguments)?;
                self.steer(Parameters(params)).await
            }
            "cancel" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: CancelParams = Self::parse_tool_params(tool_name, arguments)?;
                self.cancel(Parameters(params)).await
            }
            "start_worker" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: StartWorkerParams = Self::parse_tool_params(tool_name, arguments)?;
                self.start_worker(Parameters(params)).await
            }
            "get_worker" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: GetWorkerParams = Self::parse_tool_params(tool_name, arguments)?;
                self.get_worker(Parameters(params)).await
            }
            "cancel_worker" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: CancelWorkerParams = Self::parse_tool_params(tool_name, arguments)?;
                self.cancel_worker(Parameters(params)).await
            }
            "catalog" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: CatalogParams = Self::parse_tool_params(tool_name, arguments)?;
                self.catalog(Parameters(params)).await
            }
            "list_plans" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: ListPlansParams = Self::parse_tool_params(tool_name, arguments)?;
                self.list_plans(Parameters(params)).await
            }
            "get_plan" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: GetPlanParams = Self::parse_tool_params(tool_name, arguments)?;
                self.get_plan(Parameters(params)).await
            }
            "save_draft" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: SaveDraftParams = Self::parse_tool_params(tool_name, arguments)?;
                self.save_draft(Parameters(params)).await
            }
            "publish" => {
                Self::ensure_no_rebind_keys(tool_name, &arguments)?;
                let _ = self.verify_enrolled_binding_for_tool(tool_name)?;
                let params: PublishParams = Self::parse_tool_params(tool_name, arguments)?;
                self.publish(Parameters(params)).await
            }
            _ => Err(Self::format_pairing_error(
                tool_name,
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "unknown MCP tool requested",
                ),
            )),
        }
    }

    fn ensure_no_rebind_keys(tool_name: &str, arguments: &Value) -> Result<(), CallToolResult> {
        const FORBIDDEN_IDENTITY_KEYS: [&str; 10] = [
            "binding_id",
            "bindingId",
            "thread_id",
            "threadId",
            "session_id",
            "sessionId",
            "connection_nonce",
            "connectionNonce",
            "epoch_id",
            "epochId",
        ];

        if let Some(obj) = arguments.as_object() {
            for forbidden_key in FORBIDDEN_IDENTITY_KEYS {
                if obj.contains_key(forbidden_key) {
                    return Err(Self::format_pairing_error(
                        tool_name,
                        NativeError::new(
                            NativeErrorCode::RebindRejected,
                            format!(
                                "caller-supplied `{forbidden_key}` is forbidden; scope comes exclusively from verified operator enrollment"
                            ),
                        ),
                    ));
                }
            }
            if let Some(tasks) = obj.get("tasks").and_then(Value::as_array) {
                for task in tasks {
                    if let Some(task_obj) = task.as_object() {
                        for forbidden_key in FORBIDDEN_IDENTITY_KEYS {
                            if task_obj.contains_key(forbidden_key) {
                                return Err(Self::format_pairing_error(
                                    tool_name,
                                    NativeError::new(
                                        NativeErrorCode::RebindRejected,
                                        format!(
                                            "caller-supplied `{forbidden_key}` is forbidden; scope comes exclusively from verified operator enrollment"
                                        ),
                                    ),
                                ));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn parse_tool_params<T: for<'de> Deserialize<'de>>(
        tool_name: &str,
        arguments: Value,
    ) -> Result<T, CallToolResult> {
        Self::ensure_no_rebind_keys(tool_name, &arguments)?;
        serde_json::from_value(arguments).map_err(|_| {
            Self::format_pairing_error(
                tool_name,
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "invalid tool arguments or unknown fields supplied",
                ),
            )
        })
    }

    fn verify_enrolled_binding_for_tool(
        &self,
        tool_name: &str,
    ) -> Result<EnrollmentRecord, CallToolResult> {
        let enrollment = match self
            .pairing_store
            .verify_enrolled_binding(&self.connection_nonce, &self.epoch_id)
        {
            Ok(record) => record,
            Err(err) => {
                if let Ok(mut guard) = self.cached_workflow.lock() {
                    *guard = None;
                }
                return Err(Self::format_pairing_error(tool_name, err));
            }
        };

        if enrollment.server_pid != self.server_pid
            || !constant_time_str_eq(&enrollment.connection_nonce, &self.connection_nonce)
            || !constant_time_str_eq(&enrollment.epoch_id, &self.epoch_id)
        {
            if let Ok(mut guard) = self.cached_workflow.lock() {
                *guard = None;
            }
            return Err(Self::format_pairing_error(
                tool_name,
                NativeError::new(
                    NativeErrorCode::ExpiredEpoch,
                    "MCP connection epoch identity or owning PID no longer matches active session",
                ),
            ));
        }

        Ok(enrollment)
    }

    fn enrolled_delegate(&self, tool_name: &str) -> Result<ThreadlaneMcpServer, CallToolResult> {
        let enrollment = self.verify_enrolled_binding_for_tool(tool_name)?;

        let scoped_client = ScopedClient::from_state_dir_lazy(
            &self.state_dir,
            &enrollment.binding_id,
        )
        .map_err(|err| {
            Self::format_pairing_error(
                tool_name,
                NativeError::new(
                    NativeErrorCode::InvalidConfig,
                    format!("failed to construct scoped client for enrolled binding: {err}"),
                ),
            )
        })?;

        Ok(ThreadlaneMcpServer::new(scoped_client))
    }

    fn scoped_workflow_for_enrolled(
        &self,
        stores: &WorkflowStores,
        binding_id: &str,
        tool_name: &str,
    ) -> Result<ScopedWorkflow, CallToolResult> {
        let mut guard = self
            .cached_workflow
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(cached) = guard.as_ref() {
            if cached.server_pid == self.server_pid
                && constant_time_str_eq(&cached.connection_nonce, &self.connection_nonce)
                && constant_time_str_eq(&cached.epoch_id, &self.epoch_id)
                && cached.binding_id == binding_id
            {
                return Ok(cached.workflow.clone());
            }
        }

        let workflow = ScopedWorkflow::new(
            Arc::clone(&stores.registry),
            Arc::clone(&stores.plans),
            binding_id.to_string(),
        )
        .map_err(|err| Self::format_workflow_error(binding_id, tool_name, err))?;

        *guard = Some(CachedScopedWorkflow {
            connection_nonce: self.connection_nonce.clone(),
            epoch_id: self.epoch_id.clone(),
            server_pid: self.server_pid,
            binding_id: binding_id.to_string(),
            workflow: workflow.clone(),
        });

        Ok(workflow)
    }

    async fn run_workflow_blocking<T, F>(
        &self,
        tool_name: &'static str,
        op: F,
    ) -> Result<CallToolResult, CallToolResult>
    where
        T: Serialize + Send + 'static,
        F: FnOnce(ScopedWorkflow) -> Result<T, WorkflowError> + Send + 'static,
    {
        let enrollment = self.verify_enrolled_binding_for_tool(tool_name)?;
        let trusted_binding_id = enrollment.binding_id;

        let Some(stores) = self.workflow_stores.as_ref() else {
            return Err(Self::format_scoped_error(
                &trusted_binding_id,
                tool_name,
                NativeErrorCode::WorkflowNotConfigured.as_str(),
                WORKFLOW_NOT_CONFIGURED_MESSAGE,
            ));
        };

        let workflow = self.scoped_workflow_for_enrolled(stores, &trusted_binding_id, tool_name)?;

        let outcome = tokio::task::spawn_blocking(move || op(workflow))
            .await
            .map_err(|_| {
                Self::format_scoped_error(
                    &trusted_binding_id,
                    tool_name,
                    "io_error",
                    "Workflow background task failed to complete.",
                )
            })?;

        match outcome {
            Ok(data) => Self::format_workflow_result(&trusted_binding_id, tool_name, data),
            Err(err) => Err(Self::format_workflow_error(
                &trusted_binding_id,
                tool_name,
                err,
            )),
        }
    }

    fn format_workflow_result<T: Serialize>(
        binding_id: &str,
        tool_name: &'static str,
        data: T,
    ) -> Result<CallToolResult, CallToolResult> {
        if let Err(err) = ensure_mcp_response_budget(binding_id, tool_name, &data) {
            return Err(Self::format_workflow_error(binding_id, tool_name, err));
        }
        Ok(ThreadlaneMcpServer::build_structured_tool_result(
            binding_id,
            tool_name,
            data,
        ))
    }

    fn format_workflow_error(
        binding_id: &str,
        tool_name: &str,
        err: WorkflowError,
    ) -> CallToolResult {
        Self::format_scoped_error(binding_id, tool_name, err.code(), err.safe_message())
    }

    fn format_scoped_error(
        binding_id: &str,
        tool_name: &str,
        code: &'static str,
        message: &'static str,
    ) -> CallToolResult {
        let payload = json!({
            "untrusted_data": true,
            "demo_only": false,
            "binding_id": binding_id,
            "tool": tool_name,
            "error": {
                "code": code,
                "message": message,
            }
        });
        CallToolResult::structured_error(payload)
    }

    fn format_pairing_error(tool_name: &str, err: NativeError) -> CallToolResult {
        let payload = json!({
            "untrusted_data": true,
            "tool": tool_name,
            "error": {
                "code": err.code(),
                "message": err.message(),
            }
        });
        CallToolResult::structured_error(payload)
    }
}

#[tool_router(router = tool_router)]
impl PairedMcpServer {
    /// Returns the connection nonce and epoch status for trusted operator thread-scoped pairing.
    #[tool(
        name = "connection_identity",
        description = "Return the connection nonce and epoch state for this MCP server instance so the trusted local operator can verify thread ownership. Does not accept caller arguments or grant enrollment authority.",
        annotations(
            title = "Get MCP Connection Identity",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn connection_identity(
        &self,
        Parameters(_params): Parameters<ConnectionIdentityParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        let identity = self
            .pairing_store
            .inspect_epoch(&self.connection_nonce, &self.epoch_id)
            .map_err(|err| Self::format_pairing_error(CONNECTION_IDENTITY_TOOL, err))?;

        let payload = json!({
            "untrusted_data": false,
            "tool": CONNECTION_IDENTITY_TOOL,
            "data": identity,
        });
        Ok(CallToolResult::structured(payload))
    }

    /// Delegated `status` tool (requires verified operator enrollment).
    #[tool(
        name = "status",
        description = "Get compact status for the enrolled chat binding.",
        annotations(
            title = "Get Bound Chat Status",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn status(
        &self,
        Parameters(params): Parameters<StatusParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        let delegate = self.enrolled_delegate("status")?;
        delegate.status(Parameters(params)).await
    }

    /// Delegated `get_run` tool (requires verified operator enrollment).
    #[tool(
        name = "get_run",
        description = "Fetch a constant-size current snapshot for a demo run in the enrolled chat binding.",
        annotations(
            title = "Get Demo Run Detail",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn get_run(
        &self,
        Parameters(params): Parameters<GetRunParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        let delegate = self.enrolled_delegate("get_run")?;
        delegate.get_run(Parameters(params)).await
    }

    /// Delegated `list_runs` tool (requires verified operator enrollment).
    #[tool(
        name = "list_runs",
        description = "List a bounded keyset-paginated page of runs in the enrolled chat binding.",
        annotations(
            title = "List Demo Runs",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn list_runs(
        &self,
        Parameters(params): Parameters<ListRunsParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        let delegate = self.enrolled_delegate("list_runs")?;
        delegate.list_runs(Parameters(params)).await
    }

    /// Delegated `list_run_revisions` tool (requires verified operator enrollment).
    #[tool(
        name = "list_run_revisions",
        description = "List a bounded keyset-paginated page of historical task revisions for a demo run in the enrolled chat binding.",
        annotations(
            title = "List Run Revision History",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn list_run_revisions(
        &self,
        Parameters(params): Parameters<ListRunRevisionsParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        let delegate = self.enrolled_delegate("list_run_revisions")?;
        delegate.list_run_revisions(Parameters(params)).await
    }

    /// Delegated `list_run_questions` tool (requires verified operator enrollment).
    #[tool(
        name = "list_run_questions",
        description = "List a bounded keyset-paginated page of historical questions for a demo run in the enrolled chat binding.",
        annotations(
            title = "List Run Question History",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn list_run_questions(
        &self,
        Parameters(params): Parameters<ListRunQuestionsParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        let delegate = self.enrolled_delegate("list_run_questions")?;
        delegate.list_run_questions(Parameters(params)).await
    }

    /// Delegated `list_activity` tool (requires verified operator enrollment).
    #[tool(
        name = "list_activity",
        description = "List a bounded read-only keyset-paginated page of durable activity events for the enrolled chat binding, with optional after_seq, run_id, and kind filters. Never modifies delivered_seq, acked_seq, or wake-ups; event text is untrusted data.",
        annotations(
            title = "List Bound Chat Activity History",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn list_activity(
        &self,
        Parameters(params): Parameters<ListActivityParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        let delegate = self.enrolled_delegate("list_activity")?;
        delegate.list_activity(Parameters(params)).await
    }

    /// Delegated `start_demo` tool (requires verified operator enrollment).
    #[tool(
        name = "start_demo",
        description = "Start a deterministic simulated demo run in the enrolled chat binding.",
        annotations(
            title = "Start Simulated Demo Run",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn start_demo(
        &self,
        Parameters(params): Parameters<StartDemoParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        let delegate = self.enrolled_delegate("start_demo")?;
        delegate.start_demo(Parameters(params)).await
    }

    /// Delegated `pull_events` tool (requires verified operator enrollment).
    #[tool(
        name = "pull_events",
        description = "Pull a bounded batch of ordered durable events for the enrolled chat binding.",
        annotations(
            title = "Pull Durable Events",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    pub async fn pull_events(
        &self,
        Parameters(params): Parameters<PullEventsParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        let delegate = self.enrolled_delegate("pull_events")?;
        delegate.pull_events(Parameters(params)).await
    }

    /// Delegated `ack_events` tool (requires verified operator enrollment).
    #[tool(
        name = "ack_events",
        description = "Explicitly acknowledge processing of a delivered event batch_id for the enrolled chat binding.",
        annotations(
            title = "Acknowledge Event Batch",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn ack_events(
        &self,
        Parameters(params): Parameters<AckEventsParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        let delegate = self.enrolled_delegate("ack_events")?;
        delegate.ack_events(Parameters(params)).await
    }

    /// Delegated `answer` tool (requires verified operator enrollment).
    #[tool(
        name = "answer",
        description = "Answer the active question on a demo run at expected_revision in the enrolled chat binding.",
        annotations(
            title = "Answer Demo Run Question",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn answer(
        &self,
        Parameters(params): Parameters<AnswerParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        let delegate = self.enrolled_delegate("answer")?;
        delegate.answer(Parameters(params)).await
    }

    /// Delegated `steer` tool (requires verified operator enrollment).
    #[tool(
        name = "steer",
        description = "Update a demo run's instruction at expected_revision in the enrolled chat binding.",
        annotations(
            title = "Steer Demo Run",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn steer(
        &self,
        Parameters(params): Parameters<SteerParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        let delegate = self.enrolled_delegate("steer")?;
        delegate.steer(Parameters(params)).await
    }

    /// Delegated `cancel` tool (requires verified operator enrollment).
    #[tool(
        name = "cancel",
        description = "Terminally cancel a non-terminal demo run in the enrolled chat binding.",
        annotations(
            title = "Cancel Demo Run",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn cancel(
        &self,
        Parameters(params): Parameters<CancelParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        let delegate = self.enrolled_delegate("cancel")?;
        delegate.cancel(Parameters(params)).await
    }

    /// Delegated `start_worker` tool (requires verified operator enrollment).
    #[tool(
        name = "start_worker",
        description = "Admit and queue a real worker run for a startup-configured profile_id in the enrolled chat binding.",
        annotations(
            title = "Start Scoped Worker Run",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn start_worker(
        &self,
        Parameters(params): Parameters<StartWorkerParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        let delegate = self.enrolled_delegate("start_worker")?;
        delegate.start_worker(Parameters(params)).await
    }

    /// Delegated `get_worker` tool (requires verified operator enrollment).
    #[tool(
        name = "get_worker",
        description = "Fetch a compact durable snapshot for a worker run in the enrolled chat binding.",
        annotations(
            title = "Get Scoped Worker Snapshot",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn get_worker(
        &self,
        Parameters(params): Parameters<GetWorkerParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        let delegate = self.enrolled_delegate("get_worker")?;
        delegate.get_worker(Parameters(params)).await
    }

    /// Delegated `cancel_worker` tool (requires verified operator enrollment).
    #[tool(
        name = "cancel_worker",
        description = "Request revision-checked cancellation of a worker run in the enrolled chat binding.",
        annotations(
            title = "Cancel Scoped Worker Run",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn cancel_worker(
        &self,
        Parameters(params): Parameters<CancelWorkerParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        let delegate = self.enrolled_delegate("cancel_worker")?;
        delegate.cancel_worker(Parameters(params)).await
    }

    /// Lists a bounded page of configured workflow profiles from the operator registry (requires verified operator enrollment).
    #[tool(
        name = "catalog",
        description = "List a bounded cursor-paginated page of configured workflow profiles and role/difficulty model assignments from the operator registry for the enrolled chat binding. Omits CLI executable paths and credentials; assignments referencing disabled tools report tool_enabled: false and available: false without model substitution.",
        annotations(
            title = "List Workflow Profile Catalog",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn catalog(
        &self,
        Parameters(params): Parameters<CatalogParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        self.run_workflow_blocking("catalog", move |workflow| {
            workflow.catalog(params.cursor, params.limit)
        })
        .await
    }

    /// Lists a bounded page of workflow plan summaries owned by the enrolled chat binding (requires verified operator enrollment).
    #[tool(
        name = "list_plans",
        description = "List a bounded cursor-paginated page of workflow plan summaries owned exclusively by the enrolled chat binding. Omits task instructions and CLI executable paths. Read-only: never acknowledges or advances delivery receipts.",
        annotations(
            title = "List Bound Chat Workflow Plans",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn list_plans(
        &self,
        Parameters(params): Parameters<ListPlansParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        self.run_workflow_blocking("list_plans", move |workflow| {
            workflow.list_plans(params.cursor, params.limit)
        })
        .await
    }

    /// Fetches a workflow plan revision header and a cursor-paginated page of full task definitions owned by the enrolled chat binding (requires verified operator enrollment).
    #[tool(
        name = "get_plan",
        description = "Fetch a specific workflow plan revision (or the latest revision when revision is omitted) owned by the enrolled chat binding, with cursor-paginated full untruncated task definitions. Read-only: never acknowledges or advances delivery receipts.",
        annotations(
            title = "Get Bound Chat Workflow Plan",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn get_plan(
        &self,
        Parameters(params): Parameters<GetPlanParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        self.run_workflow_blocking("get_plan", move |workflow| {
            workflow.get_plan(
                &params.plan_id,
                params.revision,
                params.task_cursor,
                params.task_limit,
            )
        })
        .await
    }

    /// Creates or updates a draft workflow plan revision owned by the enrolled chat binding (requires verified operator enrollment).
    #[tool(
        name = "save_draft",
        description = "Create or update an unapproved draft workflow plan revision scoped to the enrolled chat binding. Validates task DAG acyclicity, repository-relative non-protected allowed_write_paths, and role/difficulty resolution against the pinned profile_revision before persisting.",
        annotations(
            title = "Save Workflow Plan Draft",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn save_draft(
        &self,
        Parameters(params): Parameters<SaveDraftParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        let draft = SavePlanDraft::from(params);
        self.run_workflow_blocking("save_draft", move |workflow| workflow.save_draft(draft))
            .await
    }

    /// Explicitly publishes a draft workflow plan revision owned by the enrolled chat binding (requires verified operator enrollment).
    #[tool(
        name = "publish",
        description = "Explicitly publish the current draft workflow plan revision (Draft -> Published) for the enrolled chat binding and return its compiled public task routing view and delivery receipt. Never advances receipts to received or applied, never grants operator approval, and never spawns workers.",
        annotations(
            title = "Publish Workflow Plan Revision",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn publish(
        &self,
        Parameters(params): Parameters<PublishParams>,
    ) -> Result<CallToolResult, CallToolResult> {
        self.run_workflow_blocking("publish", move |workflow| {
            workflow.publish(&params.plan_id, params.revision)
        })
        .await
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for PairedMcpServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                "threadlane-native-mcp",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "Threadlane paired MCP bootstrap server. Starts UNBOUND with a fresh connection nonce and epoch; only `connection_identity` is permitted until the trusted local operator enrolls this connection to a single native chat thread. Caller `_meta` and arguments cannot enroll or rebind.",
            )
    }
}
