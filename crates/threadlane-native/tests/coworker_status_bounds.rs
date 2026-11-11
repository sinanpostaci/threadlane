#![cfg(unix)]
#![allow(clippy::expect_used, clippy::unwrap_used)]
//! Regression and wire-budget tests for `coworker_status` (`CoworkerPlanningGateway` and
//! `PairedMcpServer`).
//!
//! Verifies that `DEFAULT_COWORKER_STATUS_LIMIT = 20` and `MAX_COWORKER_STATUS_LIMIT = 20` are
//! preserved while nested `PlanningResultRef` / `task_interfaces` / question / proposal payloads,
//! command `result_ref` / `provider_health` / `diagnostic` fields, and multi-row histories are
//! projected into bounded non-authoritative status summaries whose real `rmcp` `CallToolResult`
//! wire envelope (`structuredContent` + escaped `content[0].text`) never exceeds
//! `MAX_MCP_RESPONSE_BYTES` (`262_144` bytes).

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Serialize;
use serde_json::{json, Value};
use tempfile::TempDir;
use threadlane::{
    Supervisor, SupervisorClient, SupervisorConfig, SupervisorHandle, ThreadlaneMcpServer,
};
use threadlane_authority::{AuthorityStore, OperatorApprovalRequest};
use threadlane_native::coworker_grants::{list_planning_grant_summaries, TrustedGrantScope};
use threadlane_native::coworker_workflow::{
    sha256_hex, ArchitectContextDependencies, ArchitectDecisionStatus, BindJobApprovalRequest,
    CanonicalProjectBaseRef, CoworkerJobOwnershipKey, CoworkerJobOwnershipStore,
    CoworkerPlanningService, CoworkerWorkflowStore, CreateCoworkerJobRequest,
    CreatePlanningGrantRequest, FinalizePlanningCallRequest, OrchestrateWorkBriefPlanRequest,
    OutboxDispatchKind, PlanningCallBudget, PlanningReadOnlyCapabilities,
    PlanningReservationStatus, PlanningResultRef, RecordArchitectAdviceRequest,
    ReservePlanningCallRequest, ReserveTaskOutboxRequest, RevokePlanningGrantRequest,
    RunArchitectAdviceRequest, WorkBrief, WorkBriefKind,
};
use threadlane_native::planning_gateway::{
    truncate_status_text_unicode_safe, CoworkerArchitectAdviceSummary, CoworkerJobSummary,
    CoworkerStatusBriefKindSummary, CoworkerStatusCommandResultKind,
    DEFAULT_COWORKER_STATUS_TEXT_WIRE_BUDGET_BYTES,
};
use threadlane_native::profile_flow::{
    normalize_read_only_advisory_choice, preflight, PreflightRequest, RolePreflightRequest,
    SelectedRoleChoice,
};
use threadlane_native::{
    list_planning_command_summaries_readonly, planning_artifacts_dir, planning_commands_dir,
    CoworkerPlanningGateway, CoworkerStatusParams, CoworkerStatusResponse,
    EnqueuePlanningCommandRequest, ExecutionPolicy, NativeErrorCode, OperatorEnrollRequest,
    PairedMcpServer, PairingStore, PlanStatus, PlanningCommandDriver, PlanningCommandStatus,
    PlanningCommandStore, PlanningCommandSummary, PlanningGrantSummary, PolicyAllowRule,
    RegistryStore, ToolAdapter, WorkflowProfile, WorkflowRole, DEFAULT_COWORKER_STATUS_LIMIT,
    EXECUTION_POLICY_SCHEMA_VERSION, MAX_COWORKER_STATUS_LIMIT,
};
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git, ExecutionBudgets,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::{
    ensure_mcp_response_budget, measure_mcp_response_bytes, ModelChoice, ModelDefinition,
    PlanStore, RegistryChange, RoleAssignment, ToolDefinition, WorkflowError,
    MAX_MCP_RESPONSE_BYTES,
};

const THREAD_UUID: &str = "0196ed00-0000-7000-8000-0000000000b1";
const OTHER_THREAD_UUID: &str = "0196ed00-0000-7000-8000-0000000000b2";
const PROFILE_ID: &str = "profile-status-bounds";
const TOOL_ID: &str = "tool-agy";
const MODEL_ID: &str = "model-argon";
const SECRET_SENTINEL: &str = "THREADLANE_STATUS_BOUNDS_SECRET_SENTINEL_7d4a1e9c";

/// Mirrors the legacy unprojected `coworker_status` shape to prove that the exact same persisted
/// records overflowed `MAX_MCP_RESPONSE_BYTES` (`262_144` bytes) before bounded status projection.
#[derive(Debug, Serialize)]
struct LegacyUnprojectedStatusEnvelope {
    binding_id: String,
    thread_uuid: String,
    workflow_ledger_revision: u64,
    command_ledger_revision: u64,
    active_grant: Option<PlanningGrantSummary>,
    active_grants: Vec<PlanningGrantSummary>,
    grants: Vec<PlanningGrantSummary>,
    jobs: Vec<CoworkerJobSummary>,
    architect_advice: Vec<CoworkerArchitectAdviceSummary>,
    commands: Vec<PlanningCommandSummary>,
    has_more_grants: bool,
    has_more_jobs: bool,
    has_more_architect_advice: bool,
    has_more_commands: bool,
}

struct StatusBoundsFixture {
    _tmp: TempDir,
    root: PathBuf,
    repo_dir: PathBuf,
    project_base: CanonicalProjectBaseRef,
    worker_invocations_path: PathBuf,
    response_json_path: PathBuf,
    health_enabled_path: PathBuf,
    state_dir: PathBuf,
    pairing_dir: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    registry: Arc<RegistryStore>,
    _plans: Arc<PlanStore>,
    authority: AuthorityStore,
    supervisor: Option<SupervisorHandle>,
    binding_id: String,
    registry_revision: u64,
}

impl StatusBoundsFixture {
    async fn start_with_repo_subpath(repo_rel_subpath: &str) -> Self {
        let tmp = TempDir::new().expect("create status bounds tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize tempdir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .expect("chmod fixture root 0700");

        let mut repo_dir = root.clone();
        for segment in repo_rel_subpath.split('/').filter(|s| !s.is_empty()) {
            repo_dir.push(segment);
            create_private_dir(&repo_dir);
        }
        let project_base = init_git_repo(&repo_dir);

        let bin_dir = root.join("bin");
        create_private_dir(&bin_dir);
        let fake_worker_path = bin_dir.join("fake-agy");
        let worker_invocations_path = root.join("worker-invocations.log");
        let response_json_path = root.join("worker-response.json");
        let health_enabled_path = root.join("worker-health-enabled");
        write_fake_status_worker(
            &fake_worker_path,
            &worker_invocations_path,
            &response_json_path,
            &health_enabled_path,
        );

        let state_dir = root.join("runtime");
        let pairing_dir = root.join("pairing");
        let registry_dir = root.join("registry-store");
        let plans_dir = root.join("plans-store");
        let authority_dir = root.join("authority-store");
        let sessions_dir = root.join("sessions-store");
        for dir in [
            &state_dir,
            &pairing_dir,
            &registry_dir,
            &plans_dir,
            &authority_dir,
            &sessions_dir,
        ] {
            create_private_dir(dir);
        }

        let supervisor = Supervisor::start(SupervisorConfig::new(state_dir.clone()))
            .await
            .expect("start disposable supervisor");
        let supervisor_client =
            SupervisorClient::from_state_dir(&state_dir).expect("connect supervisor");
        let binding = supervisor_client
            .create_binding(THREAD_UUID, "status-bounds-chat")
            .await
            .expect("create control binding");

        let registry = Arc::new(RegistryStore::open(&registry_dir).expect("open registry store"));
        let s1 = registry
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: TOOL_ID.to_string(),
                        label: "Antigravity status fixture".to_string(),
                        adapter: ToolAdapter::Antigravity,
                        executable_path: Some(fake_worker_path),
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
                        id: MODEL_ID.to_string(),
                        tool_id: TOOL_ID.to_string(),
                        label: "Gemini 4 Argon High".to_string(),
                        model_arg: "gemini-4-argon-high".to_string(),
                        efforts: vec!["high".to_string()],
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
                        id: PROFILE_ID.to_string(),
                        name: "Status bounds profile".to_string(),
                        assignments: vec![
                            RoleAssignment {
                                role: WorkflowRole::Orchestrator,
                                difficulty: None,
                                choice: ModelChoice {
                                    model_id: MODEL_ID.to_string(),
                                    effort: Some("high".to_string()),
                                },
                            },
                            RoleAssignment {
                                role: WorkflowRole::Architect,
                                difficulty: None,
                                choice: ModelChoice {
                                    model_id: MODEL_ID.to_string(),
                                    effort: Some("high".to_string()),
                                },
                            },
                            RoleAssignment {
                                role: WorkflowRole::Coder,
                                difficulty: None,
                                choice: ModelChoice {
                                    model_id: MODEL_ID.to_string(),
                                    effort: Some("high".to_string()),
                                },
                            },
                        ],
                    },
                },
            )
            .expect("upsert profile");

        let registry_lock = registry_dir.join("registry.lock");
        if registry_lock.exists() {
            fs::remove_file(&registry_lock).expect("remove initial registry.lock");
        }

        let plans = Arc::new(PlanStore::open(&plans_dir).expect("open plan store"));
        let authority =
            AuthorityStore::open(&authority_dir, Arc::clone(&registry), Arc::clone(&plans))
                .expect("open authority store");
        let plans_lock = plans_dir.join("plans.lock");
        if plans_lock.exists() {
            fs::remove_file(&plans_lock).expect("remove initial plans.lock");
        }
        let authority_lock = authority_dir.join("authority.lock");
        if authority_lock.exists() {
            fs::remove_file(&authority_lock).expect("remove initial authority.lock");
        }

        Self {
            _tmp: tmp,
            root,
            repo_dir,
            project_base,
            worker_invocations_path,
            response_json_path,
            health_enabled_path,
            state_dir,
            pairing_dir,
            registry_dir,
            plans_dir,
            authority_dir,
            sessions_dir,
            registry,
            _plans: plans,
            authority,
            supervisor: Some(supervisor),
            binding_id: binding.binding_id,
            registry_revision: s3.revision,
        }
    }

    async fn start() -> Self {
        Self::start_with_repo_subpath("repo").await
    }

    fn gateway(&self) -> CoworkerPlanningGateway {
        self.gateway_for_scope(&self.binding_id, THREAD_UUID)
    }

    fn gateway_for_scope(&self, binding_id: &str, thread_uuid: &str) -> CoworkerPlanningGateway {
        CoworkerPlanningGateway::new(
            &self.state_dir,
            &self.registry_dir,
            &self.plans_dir,
            &self.authority_dir,
            &self.sessions_dir,
            binding_id,
            thread_uuid,
        )
        .expect("create CoworkerPlanningGateway")
        .with_sibling_anchor_exe(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-flow")))
    }

    fn enrolled_mcp(&self) -> PairedMcpServer {
        let pairing_store = PairingStore::open(&self.pairing_dir).expect("open pairing store");
        let operator_token = pairing_store.operator_token().expect("operator token");
        let mcp = PairedMcpServer::new(&self.state_dir, pairing_store.clone())
            .expect("create PairedMcpServer")
            .with_workflow_dirs(&self.registry_dir, &self.plans_dir)
            .expect("with_workflow_dirs")
            .with_host_dirs(&self.authority_dir, &self.sessions_dir)
            .expect("with_host_dirs")
            .with_sibling_anchor_exe(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-flow")));
        pairing_store
            .enroll(
                &operator_token,
                OperatorEnrollRequest {
                    connection_nonce: mcp.connection_nonce().to_string(),
                    epoch_id: mcp.epoch_id().to_string(),
                    binding_id: self.binding_id.clone(),
                    thread_id: THREAD_UUID.to_string(),
                    project_id: None,
                },
            )
            .expect("enroll PairedMcpServer");
        mcp
    }

    fn workflow_store(&self) -> CoworkerWorkflowStore {
        CoworkerWorkflowStore::new(&self.plans_dir).expect("open CoworkerWorkflowStore")
    }

    fn command_store(&self) -> PlanningCommandStore {
        let commands_dir = planning_commands_dir(&self.plans_dir);
        create_private_dir(&commands_dir);
        let artifacts_dir = planning_artifacts_dir(&self.plans_dir);
        create_private_dir(&artifacts_dir);
        let ownership_store =
            CoworkerJobOwnershipStore::new(&commands_dir).expect("open ownership store");
        PlanningCommandStore::new(
            &commands_dir,
            self.workflow_store(),
            &self.registry_dir,
            ownership_store,
        )
        .expect("open PlanningCommandStore")
        .with_artifacts_dir(&artifacts_dir)
        .expect("configure artifacts_dir")
    }

    fn driver(&self) -> PlanningCommandDriver {
        let cmd_store = self.command_store();
        let artifacts_dir = planning_artifacts_dir(&self.plans_dir);
        let service =
            CoworkerPlanningService::new(self.workflow_store(), &self.registry_dir, &artifacts_dir)
                .expect("open CoworkerPlanningService");
        PlanningCommandDriver::new(cmd_store, service).expect("open PlanningCommandDriver")
    }

    fn create_grant(
        &self,
        request_key: &str,
        created_at_ms: u64,
        expires_at_ms: u64,
        max_total_calls: u32,
        max_output_bytes_per_call: usize,
    ) -> String {
        self.workflow_store()
            .create_planning_grant(&CreatePlanningGrantRequest {
                binding_id: self.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                request_key: request_key.to_string(),
                expected_ledger_revision: None,
                project_base: self.project_base.clone(),
                profile_id: PROFILE_ID.to_string(),
                registry_revision: self.registry_revision,
                policy: default_planning_policy(),
                capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
                budget: PlanningCallBudget {
                    max_total_calls,
                    max_orchestrator_calls: max_total_calls,
                    max_architect_calls: max_total_calls,
                    max_timeout_ms_per_call: 60_000,
                    max_output_bytes_per_call,
                },
                created_at_ms,
                expires_at_ms,
                operator_attested: true,
            })
            .expect("create planning grant")
            .grant_id
    }

    fn resolve_advisory_choice(&self, role: WorkflowRole) -> SelectedRoleChoice {
        let snap = self.registry.snapshot().expect("registry snapshot");
        let report = preflight(
            &snap,
            &PreflightRequest {
                profile_id: Some(PROFILE_ID.to_string()),
                roles: vec![RolePreflightRequest::new(role, None)],
            },
            &default_planning_policy(),
        )
        .expect("preflight advisory choice");
        let raw = report.roles[0]
            .selected
            .as_ref()
            .expect("selected role choice");
        normalize_read_only_advisory_choice(role, raw).expect("normalize advisory choice")
    }

    fn reset_worker_invocations(&self) {
        if self.worker_invocations_path.exists() {
            fs::remove_file(&self.worker_invocations_path).expect("clear worker_invocations_path");
        }
    }

    fn worker_invocation_count(&self) -> usize {
        match fs::read_to_string(&self.worker_invocations_path) {
            Ok(text) => text.lines().count(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
            Err(err) => panic!("failed reading worker_invocations_path: {err}"),
        }
    }

    fn build_legacy_envelope(&self, limit: usize) -> LegacyUnprojectedStatusEnvelope {
        let wf_store = self.workflow_store();
        let wf_ledger = wf_store
            .read_binding_ledger(&self.binding_id)
            .expect("read workflow ledger");
        let scope = TrustedGrantScope::new(self.binding_id.clone(), THREAD_UUID.to_string())
            .expect("scope");
        let all_grants =
            list_planning_grant_summaries(&wf_store, &scope).expect("list grant summaries");
        let now_ms = threadlane_native::current_wall_clock_ms().expect("wall clock");
        let active_grants: Vec<PlanningGrantSummary> = all_grants
            .iter()
            .filter(|g| {
                g.status == threadlane_native::coworker_workflow::PlanningGrantStatus::Active
                    && g.revoked_at_ms.is_none()
                    && now_ms >= g.created_at_ms
                    && now_ms < g.expires_at_ms
                    && g.usage.reserved_total_calls < g.budget.max_total_calls
                    && g.usage.unknown_effect_calls == 0
            })
            .cloned()
            .collect();
        let active_grant = active_grants.last().cloned();
        let grants = tail_slice(&all_grants, limit);

        let all_jobs: Vec<CoworkerJobSummary> = wf_ledger
            .jobs
            .iter()
            .map(CoworkerJobSummary::from_record)
            .collect();
        let jobs = tail_slice(&all_jobs, limit);

        let all_advice: Vec<CoworkerArchitectAdviceSummary> = wf_ledger
            .architect_advice
            .iter()
            .map(CoworkerArchitectAdviceSummary::from_record)
            .collect();
        let architect_advice = tail_slice(&all_advice, limit);

        let cmd_page = list_planning_command_summaries_readonly(
            &self.plans_dir,
            &self.registry_dir,
            &self.binding_id,
            THREAD_UUID,
            usize::MAX,
            None,
        )
        .expect("list command summaries");
        let commands = tail_slice(&cmd_page.commands, limit);

        LegacyUnprojectedStatusEnvelope {
            binding_id: self.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            workflow_ledger_revision: wf_ledger.ledger_revision,
            command_ledger_revision: cmd_page.ledger_revision,
            active_grant,
            active_grants: tail_slice(&active_grants, limit),
            grants,
            jobs,
            architect_advice,
            commands,
            has_more_grants: all_grants.len() > limit,
            has_more_jobs: all_jobs.len() > limit,
            has_more_architect_advice: all_advice.len() > limit,
            has_more_commands: cmd_page.commands.len() > limit,
        }
    }

    async fn shutdown(&mut self) {
        if let Some(supervisor) = self.supervisor.take() {
            supervisor.shutdown().await;
        }
    }
}

fn tail_slice<T: Clone>(items: &[T], limit: usize) -> Vec<T> {
    if items.len() <= limit {
        items.to_vec()
    } else {
        items[items.len() - limit..].to_vec()
    }
}

fn default_planning_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
        policy_id: "policy-status-bounds".to_string(),
        allow_rules: vec![PolicyAllowRule {
            tool_id: TOOL_ID.to_string(),
            model_id: MODEL_ID.to_string(),
            efforts: vec!["high".to_string()],
            allow_no_effort: false,
            roles: vec![WorkflowRole::Orchestrator, WorkflowRole::Architect],
        }],
    }
}

fn default_execution_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
        policy_id: "policy-status-bounds-exec".to_string(),
        allow_rules: vec![PolicyAllowRule {
            tool_id: TOOL_ID.to_string(),
            model_id: MODEL_ID.to_string(),
            efforts: vec!["high".to_string()],
            allow_no_effort: false,
            roles: vec![
                WorkflowRole::Orchestrator,
                WorkflowRole::Architect,
                WorkflowRole::Coder,
            ],
        }],
    }
}

fn init_git_repo(repo_dir: &Path) -> CanonicalProjectBaseRef {
    fs::create_dir_all(repo_dir.join("src")).expect("create repo src");
    fs::write(
        repo_dir.join("src/lib.rs"),
        "pub mod exporter;\npub mod metrics;\npub mod parser;\n",
    )
    .expect("write src/lib.rs");
    fs::write(
        repo_dir.join("src/parser.rs"),
        "pub fn parse_frame() -> u32 { 1 }\n",
    )
    .expect("write src/parser.rs");
    fs::write(
        repo_dir.join("src/metrics.rs"),
        "pub fn count_frame() -> u32 { 2 }\n",
    )
    .expect("write src/metrics.rs");
    fs::write(
        repo_dir.join("src/exporter.rs"),
        "pub fn export_frame() -> u32 { 3 }\n",
    )
    .expect("write src/exporter.rs");

    for args in [
        &["init", "--quiet"][..],
        &["config", "user.email", "test@threadlane.local"][..],
        &["config", "user.name", "Threadlane Test"][..],
        &["add", "."][..],
        &["commit", "-m", "initial status bounds fixture", "--quiet"][..],
    ] {
        run_sanitized_git(repo_dir, args, DEFAULT_GIT_COMMAND_TIMEOUT_MS).expect("git init step");
    }
    let commit = resolve_exact_commit_sha(repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve HEAD commit");
    let tree = resolve_exact_tree_sha(repo_dir, &commit, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve HEAD tree");
    CanonicalProjectBaseRef {
        repository_path: repo_dir.to_string_lossy().into_owned(),
        base_commit: commit,
        base_tree_sha: tree,
    }
}

fn create_private_dir(path: &Path) {
    fs::create_dir_all(path).expect("create private directory");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
}

fn directory_entries(path: &Path) -> Vec<String> {
    if !path.exists() {
        return Vec::new();
    }
    let mut names: Vec<String> = fs::read_dir(path)
        .expect("read directory")
        .map(|entry| {
            entry
                .expect("dir entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

fn write_fake_status_worker(
    script_path: &Path,
    invocations_path: &Path,
    response_json_path: &Path,
    health_enabled_path: &Path,
) {
    let script = format!(
        r#"#!/bin/sh
set -eu
if [ -f '{}' ]; then
    prev=''
    for arg in "$@"; do
        if [ "$prev" = '--log-file' ]; then
            printf '%s\n' 'I0101 00:00:01.000000 1 run.go:315] Run: attempt 1 failed (stream reading error: stream idle timeout (5m0s): context deadline exceeded), retrying in 4s.' > "$arg"
            break
        fi
        prev="$arg"
    done
fi
printf 'invoked\n' >> '{}'
printf '{{"event":"init","init":{{"session_id":"agy-status-bounds-1","model":"gemini-4-argon-high"}}}}\n'
esc=$(sed 's/\\/\\\\/g; s/"/\\"/g' '{}' | tr '\n' ' ')
printf '{{"event":"result","result":{{"status":"SUCCESS","response":"%s"}}}}\n' "$esc"
exit 0
"#,
        health_enabled_path.display(),
        invocations_path.display(),
        response_json_path.display(),
    );
    fs::write(script_path, script).expect("write fake worker");
    fs::set_permissions(script_path, fs::Permissions::from_mode(0o700))
        .expect("chmod fake worker 0700");
}

async fn invoke_status_and_verify_wire_envelope(
    mcp: &PairedMcpServer,
    binding_id: &str,
    args: Value,
) -> (CoworkerStatusResponse, Value, usize) {
    let call_result = mcp
        .invoke_tool_json("coworker_status", args, None)
        .await
        .unwrap_or_else(|err| panic!("coworker_status failed: {:?}", err.structured_content));

    let raw_wire_bytes = serde_json::to_vec(&call_result).expect("serialize CallToolResult");
    assert!(
        raw_wire_bytes.len() <= MAX_MCP_RESPONSE_BYTES,
        "real rmcp CallToolResult wire bytes {} exceeded MAX_MCP_RESPONSE_BYTES ({})",
        raw_wire_bytes.len(),
        MAX_MCP_RESPONSE_BYTES
    );

    let wire_str =
        std::str::from_utf8(&raw_wire_bytes).expect("serialized CallToolResult must be UTF-8");
    assert!(
        !wire_str.contains(SECRET_SENTINEL),
        "coworker_status wire envelope must never leak secret or prompt sentinel"
    );
    assert!(
        !wire_str.contains('\u{FFFD}'),
        "coworker_status wire envelope must never contain UTF-8 replacement characters"
    );

    let call_result_val: Value =
        serde_json::from_slice(&raw_wire_bytes).expect("parse CallToolResult JSON");
    let structured = call_result
        .structured_content
        .clone()
        .expect("structured_content must be present");
    assert_eq!(structured["untrusted_data"], json!(true));
    assert_eq!(structured["binding_id"], json!(binding_id));
    assert_eq!(structured["tool"], json!("coworker_status"));

    let text_content = call_result_val["content"][0]["text"]
        .as_str()
        .expect("content[0].text string");
    let parsed_text_json: Value =
        serde_json::from_str(text_content).expect("content[0].text must be valid JSON");
    assert_eq!(
        parsed_text_json, structured,
        "escaped JSON text in content[0].text must match structuredContent"
    );

    let data_val = structured["data"].clone();
    let status: CoworkerStatusResponse =
        serde_json::from_value(data_val.clone()).expect("deserialize CoworkerStatusResponse");
    let measured = measure_mcp_response_bytes(binding_id, "coworker_status", &status)
        .expect("measure_mcp_response_bytes");
    assert_eq!(
        measured,
        raw_wire_bytes.len(),
        "measure_mcp_response_bytes must match actual CallToolResult wire byte length"
    );
    ensure_mcp_response_budget(binding_id, "coworker_status", &status)
        .expect("ensure_mcp_response_budget must pass");

    (status, data_val, raw_wire_bytes.len())
}

async fn invoke_mcp_err_code(mcp: &PairedMcpServer, tool: &str, args: Value) -> String {
    let err = mcp
        .invoke_tool_json(tool, args, None)
        .await
        .expect_err("expected MCP error")
        .structured_content
        .expect("error structured_content");
    let raw = err.to_string();
    assert!(!raw.contains(SECRET_SENTINEL));
    err["error"]["code"]
        .as_str()
        .expect("error.code")
        .to_string()
}

struct SyntheticArchitectContext<'a> {
    choice: &'a SelectedRoleChoice,
    dep_blobs: &'a BTreeMap<String, String>,
    work_brief_digest: &'a str,
    policy_digest: &'a str,
    base_time_ms: u64,
}

struct SyntheticFailedCommand<'a> {
    grant_id: &'a str,
    job_id: &'a str,
    base_time_ms: u64,
    idx: usize,
    diagnostic: &'a str,
}

fn record_synthetic_architect_advice(
    fx: &StatusBoundsFixture,
    grant_id: &str,
    ctx: &SyntheticArchitectContext<'_>,
    idx: usize,
    decision_summary: String,
) -> String {
    let wf_store = fx.workflow_store();
    let deps = ArchitectContextDependencies {
        project_base: fx.project_base.clone(),
        work_brief_digest: ctx.work_brief_digest.to_string(),
        plan_digest: None,
        policy_digest: ctx.policy_digest.to_string(),
        dependency_file_blobs: ctx.dep_blobs.clone(),
        interface_ownership_digests: BTreeMap::from([(
            format!("iface_arch_{idx}"),
            sha256_hex(format!("iface-contract-{idx}").as_bytes()),
        )]),
        accepted_predecessor_trees: BTreeMap::new(),
    };
    let context_digest = deps.context_digest().expect("context_digest");
    let req_key = format!("synth_arch_req_{idx}");
    let offset_ms = idx as u64;
    let res = wf_store
        .reserve_planning_call(&ReservePlanningCallRequest {
            binding_id: fx.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant_id.to_string(),
            request_key: req_key.clone(),
            expected_ledger_revision: None,
            role: WorkflowRole::Architect,
            difficulty: None,
            selected_choice: ctx.choice.clone(),
            context_digest,
            call_project_base: Some(fx.project_base.clone()),
            current_time_ms: ctx.base_time_ms + offset_ms,
        })
        .expect("reserve synthetic architect call");
    let decision_digest = sha256_hex(decision_summary.as_bytes());
    wf_store
        .finalize_planning_call(&FinalizePlanningCallRequest {
            binding_id: fx.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant_id.to_string(),
            reservation_id: res.reservation_id.clone(),
            expected_ledger_revision: None,
            status: PlanningReservationStatus::Completed,
            updated_at_ms: ctx.base_time_ms + 100 + offset_ms,
            run_id: Some(format!("run_synth_arch_{idx}")),
            report_artifact_id: None,
            output_digest: Some(decision_digest.clone()),
            diagnostic: None,
        })
        .expect("finalize synthetic architect call");
    wf_store
        .record_architect_advice(&RecordArchitectAdviceRequest {
            binding_id: fx.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: req_key,
            expected_ledger_revision: None,
            grant_id: grant_id.to_string(),
            reservation_id: res.reservation_id,
            context_dependencies: deps,
            decision_summary,
            decision_digest,
            report_artifact_id: None,
            status: ArchitectDecisionStatus::Proposed,
            created_at_ms: ctx.base_time_ms + 200 + offset_ms,
        })
        .expect("record synthetic architect advice")
        .advice_id
}

fn record_synthetic_known_failed_command(
    fx: &StatusBoundsFixture,
    cmd_store: &PlanningCommandStore,
    spec: SyntheticFailedCommand<'_>,
) -> String {
    let req_key = format!("synth_cmd_fail_{}", spec.idx);
    let enq = cmd_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(
            OrchestrateWorkBriefPlanRequest {
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: spec.grant_id.to_string(),
                job_id: spec.job_id.to_string(),
                request_key: req_key,
                expected_ledger_revision: None,
                difficulty: None,
                architect_difficulty: None,
                plan_id: None,
                expected_plan_revision: 0,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                current_time_ms: spec.base_time_ms + (spec.idx as u64),
            },
        ))
        .expect("enqueue synthetic command");
    let key = CoworkerJobOwnershipKey::new(&fx.binding_id, THREAD_UUID, &enq.command_id)
        .expect("ownership key");
    let guard = cmd_store
        .ownership_store()
        .try_acquire_job(
            &key,
            threadlane_native::current_wall_clock_ms().expect("clock"),
        )
        .expect("acquire ownership")
        .into_guard()
        .expect("uncontended ownership guard");
    cmd_store
        .claim_running(&guard, &fx.binding_id, THREAD_UUID, &enq.command_id, None)
        .expect("claim_running");
    cmd_store
        .record_known_failed(
            &guard,
            &fx.binding_id,
            THREAD_UUID,
            &enq.command_id,
            spec.diagnostic,
            None,
        )
        .expect("record_known_failed");
    enq.command_id
}

#[tokio::test]
async fn coworker_status_single_huge_item_overflows_legacy_envelope_fits_wire_budget_and_preserves_persisted_records(
) {
    let mut fx = StatusBoundsFixture::start().await;
    assert_eq!(DEFAULT_COWORKER_STATUS_LIMIT, 20);
    assert_eq!(MAX_COWORKER_STATUS_LIMIT, 20);

    let now_ms = threadlane_native::current_wall_clock_ms().expect("wall clock");
    let grant_id = fx.create_grant(
        "grant_single_huge_1",
        now_ms.saturating_sub(10_000),
        now_ms + 3_600_000,
        8,
        524_288,
    );

    // Build a single valid job under MAX_PROMPT_BYTES (65,536) whose quote/backslash-dense
    // WorkBrief + PlanningResultRef + completed command result_ref push the legacy unprojected
    // dual-envelope CallToolResult past MAX_MCP_RESPONSE_BYTES (262,144 bytes).
    let escape_chunk = r#"\"\\\"\\"#;
    let open_questions: Vec<String> = (0..18)
        .map(|i| format!("Q{i:02} scope check: {}", escape_chunk.repeat(70)))
        .collect();
    let proposed_additions: Vec<String> = (0..18)
        .map(|i| format!("P{i:02} deferred note: {}", escape_chunk.repeat(70)))
        .collect();
    let long_summary = format!("HUGE-JOB-SUMMARY: {}", escape_chunk.repeat(300));

    let wf_store = fx.workflow_store();
    let brief = WorkBrief {
        brief_id: "brf_single_huge_1".to_string(),
        binding_id: fx.binding_id.clone(),
        thread_uuid: THREAD_UUID.to_string(),
        project_base: fx.project_base.clone(),
        kind: WorkBriefKind::NewJob,
        summary: long_summary.clone(),
        confirmed_requirements: vec![
            format!("Implement parser with secret prompt sentinel excluded from status ({SECRET_SENTINEL}) in src/parser.rs"),
            "Implement metrics in src/metrics.rs".to_string(),
            "Implement exporter in src/exporter.rs".to_string(),
        ],
        proposed_additions: proposed_additions.clone(),
        assumptions: vec!["Preserve exact schema and persisted digests.".to_string()],
        open_questions: open_questions.clone(),
        accepted_architect_decisions: Vec::new(),
    };
    let job = wf_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: fx.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "job_single_huge_1".to_string(),
            expected_ledger_revision: None,
            work_brief: brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create single huge job");

    let parse_ifaces: Vec<String> = (0..8)
        .map(|i| format!("iface_parse_{i:02}_{}", "p".repeat(103)))
        .collect();
    let metrics_ifaces: Vec<String> = (0..8)
        .map(|i| format!("iface_metrics_{i:02}_{}", "m".repeat(101)))
        .collect();
    let export_owned: Vec<String> = (0..8)
        .map(|i| format!("iface_export_{i:02}_{}", "e".repeat(102)))
        .collect();
    let mut export_consumed = parse_ifaces.clone();
    export_consumed.extend(metrics_ifaces.clone());

    let orch_plan_response = json!({
        "outcome": "plan",
        "schema_version": 1,
        "summary": format!("Compiled 3-task plan ({SECRET_SENTINEL})"),
        "tasks": [
            {
                "task_id": "task_parse",
                "title": "Parser implementation",
                "description": "Implement src/parser.rs",
                "role": "coder",
                "difficulty": "medium",
                "depends_on": [],
                "allowed_write_paths": ["src/parser.rs"],
                "owned_interfaces": parse_ifaces,
                "consumed_interfaces": []
            },
            {
                "task_id": "task_metrics",
                "title": "Metrics implementation",
                "description": "Implement src/metrics.rs",
                "role": "coder",
                "difficulty": "easy",
                "depends_on": [],
                "allowed_write_paths": ["src/metrics.rs"],
                "owned_interfaces": metrics_ifaces,
                "consumed_interfaces": []
            },
            {
                "task_id": "task_export",
                "title": "Exporter implementation",
                "description": "Implement src/exporter.rs",
                "role": "coder",
                "difficulty": "hard",
                "depends_on": ["task_parse", "task_metrics"],
                "allowed_write_paths": ["src/exporter.rs"],
                "owned_interfaces": export_owned,
                "consumed_interfaces": export_consumed
            }
        ],
        "check_suggestions": [
            {
                "check_id": "chk_workspace",
                "command_summary": "cargo test --workspace",
                "required": true
            }
        ],
        "unconfirmed_proposal_notes": ["Deferred proposals remain excluded from confirmed tasks."],
        "resolved_open_questions": open_questions,
        "deferred_proposals": proposed_additions
    });
    fs::write(
        &fx.response_json_path,
        serde_json::to_vec(&orch_plan_response).expect("serialize orch response"),
    )
    .expect("write response_json_path");
    fs::write(&fx.health_enabled_path, b"1\n").expect("enable provider health");

    let cmd_store = fx.command_store();
    let enqueued = cmd_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(
            OrchestrateWorkBriefPlanRequest {
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job.job_id.clone(),
                request_key: "cmd_single_huge_plan_1".to_string(),
                expected_ledger_revision: None,
                difficulty: None,
                architect_difficulty: None,
                plan_id: None,
                expected_plan_revision: 0,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                current_time_ms: now_ms,
            },
        ))
        .expect("enqueue single huge plan command");
    let driver = fx.driver();
    let exec_outcome = driver
        .execute_command(&fx.binding_id, THREAD_UUID, &enqueued.command_id)
        .await
        .expect("execute single huge plan command");
    assert_eq!(
        exec_outcome.command.status,
        PlanningCommandStatus::Completed
    );

    // Also approve the published plan and reserve one task outbox so AuthorityStore,
    // FrozenApprovalLinkage, and runtime task/outbox records exist on disk and are verified
    // byte-for-byte untouched by coworker_status.
    let ledger_after_plan = wf_store
        .read_binding_ledger(&fx.binding_id)
        .expect("read ledger after plan");
    let planned_job = ledger_after_plan
        .jobs
        .iter()
        .find(|j| j.job_id == job.job_id)
        .expect("planned job");
    let persisted_pr = planned_job
        .planning_result
        .as_ref()
        .expect("persisted PlanningResultRef")
        .clone();
    let approval = fx
        .authority
        .approve(OperatorApprovalRequest {
            binding_id: fx.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            plan_id: persisted_pr.plan_id.clone(),
            plan_revision: persisted_pr.plan_revision,
            expected_registry_revision: fx.registry_revision,
            expected_plan_digest: persisted_pr.plan_digest.clone(),
            repository_path: fx.repo_dir.clone(),
            base_commit: fx.project_base.base_commit.clone(),
            approved_task_ids: vec![
                "task_parse".to_string(),
                "task_metrics".to_string(),
                "task_export".to_string(),
            ],
            approved_write_paths: vec![
                "src/parser.rs".to_string(),
                "src/metrics.rs".to_string(),
                "src/exporter.rs".to_string(),
            ],
            max_parallel: 2,
            budgets: ExecutionBudgets::default(),
        })
        .expect("approve published plan");
    let bound_job = wf_store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: planned_job.job_revision,
                expected_ledger_revision: None,
                approval_id: approval.approval_id.clone(),
                policy: default_execution_policy(),
                controller_envelope: None,
                task_interfaces: persisted_pr.task_interfaces.clone(),
            },
            &fx.authority,
        )
        .expect("bind job approval");
    wf_store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: fx.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: bound_job.job_revision,
            expected_ledger_revision: None,
            request_key: "outbox_parse_1".to_string(),
            kind: OutboxDispatchKind::TaskAttemptDispatch,
            task_id: "task_parse".to_string(),
            role: WorkflowRole::Coder,
            subject_digest: sha256_hex(b"task_parse_attempt_1"),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect("reserve task outbox");

    // Prove the legacy unprojected envelope for this SINGLE item exceeds 262,144 bytes!
    for legacy_limit in [DEFAULT_COWORKER_STATUS_LIMIT, 1] {
        let legacy_env = fx.build_legacy_envelope(legacy_limit);
        let legacy_wire_bytes =
            measure_mcp_response_bytes(&fx.binding_id, "coworker_status", &legacy_env)
                .expect("measure legacy wire bytes");
        assert!(
            legacy_wire_bytes > MAX_MCP_RESPONSE_BYTES,
            "legacy single-item envelope ({legacy_wire_bytes} bytes) at limit={legacy_limit} must exceed {MAX_MCP_RESPONSE_BYTES}"
        );
        let budget_err = ensure_mcp_response_budget(&fx.binding_id, "coworker_status", &legacy_env)
            .expect_err("legacy single-item envelope must fail ensure_mcp_response_budget");
        assert!(matches!(budget_err, WorkflowError::ResponseTooLarge(_)));
        let raw_legacy = serde_json::to_vec(&ThreadlaneMcpServer::build_structured_tool_result(
            &fx.binding_id,
            "coworker_status",
            &legacy_env,
        ))
        .expect("serialize legacy CallToolResult");
        assert_eq!(raw_legacy.len(), legacy_wire_bytes);
    }

    // Snapshot all persisted files and reset invocation counter before running coworker_status.
    fx.reset_worker_invocations();
    let wf_ledger_path = wf_store
        .ledger_path_for_binding(&fx.binding_id)
        .expect("wf_ledger_path");
    let cmd_ledger_path = cmd_store
        .ledger_path_for_binding(&fx.binding_id)
        .expect("cmd_ledger_path");
    let artifact_path = planning_artifacts_dir(&fx.plans_dir)
        .join(format!("{}.json", persisted_pr.report_artifact_id));
    let approvals_path = fx.authority_dir.join("approvals.json");
    let wf_bytes_before = fs::read(&wf_ledger_path).expect("read wf_ledger before");
    let cmd_bytes_before = fs::read(&cmd_ledger_path).expect("read cmd_ledger before");
    let art_bytes_before = fs::read(&artifact_path).expect("read artifact before");
    let approvals_bytes_before = fs::read(&approvals_path).expect("read approvals.json before");

    let mcp = fx.enrolled_mcp();
    let gw = fx.gateway();

    for (args, direct_limit, expected_limit) in [
        (json!({}), None, 20usize),
        (json!({ "limit": 20 }), Some(20), 20usize),
        (json!({ "limit": 1 }), Some(1), 1usize),
    ] {
        let direct = gw
            .coworker_status(&CoworkerStatusParams {
                limit: direct_limit,
            })
            .expect("direct gateway coworker_status");
        let (mcp_status, mcp_data_json, wire_len) =
            invoke_status_and_verify_wire_envelope(&mcp, &fx.binding_id, args).await;
        assert!(wire_len <= MAX_MCP_RESPONSE_BYTES);
        assert_eq!(direct.effective_limit, expected_limit);
        assert_eq!(mcp_status.effective_limit, expected_limit);
        assert_eq!(direct, mcp_status);

        assert_eq!(
            mcp_status.active_grant_id.as_deref(),
            Some(grant_id.as_str())
        );
        assert!(!mcp_status.active_grant_omitted_by_byte_budget);
        assert!(mcp_status.active_grant.is_some());
        assert!(!mcp_status.rows_omitted_by_byte_budget);
        assert!(mcp_status.details_omitted_or_truncated_by_byte_budget);
        assert!(!mcp_status.has_more_grants);
        assert!(!mcp_status.has_more_active_grants);
        assert!(!mcp_status.has_more_jobs);
        assert!(!mcp_status.has_more_architect_advice);
        assert!(!mcp_status.has_more_commands);

        // Unresolved task outbox is preserved in both scope-wide and row-level reconciliation flags.
        assert!(mcp_status.pending_reconciliation);
        assert_eq!(mcp_status.unresolved_job_outbox_count, 1);
        assert_eq!(mcp_status.unknown_effect_reservation_count, 0);
        assert_eq!(mcp_status.pending_reconciliation_command_count, 0);

        assert_eq!(mcp_status.jobs.len(), 1);
        let j = &mcp_status.jobs[0];
        assert_eq!(j.job_id, job.job_id);
        assert_eq!(j.work_brief_digest, job.work_brief_digest);
        assert!(j.summary_truncated);
        assert!(!j.grants_execution_authority);
        assert_eq!(
            j.approval_id.as_deref(),
            Some(approval.approval_id.as_str())
        );
        assert_eq!(j.task_count, 3);
        assert_eq!(j.unresolved_outbox_count, 1);
        assert!(j.omitted_brief_item_lists);
        assert!(j.omitted_runtime_task_details);
        assert!(!j.ready_for_execution_approval);
        assert!(j.ready_for_execution_approval_with_planning);

        let pr_proj = j
            .planning_result
            .as_ref()
            .expect("projected planning_result");
        assert!(pr_proj.status_projection_only);
        assert!(!pr_proj.grants_execution_authority);
        assert_eq!(pr_proj.plan_id, persisted_pr.plan_id);
        assert_eq!(pr_proj.plan_revision, 1);
        assert_eq!(pr_proj.plan_digest, persisted_pr.plan_digest);
        assert_eq!(pr_proj.output_digest, persisted_pr.output_digest);
        assert_eq!(pr_proj.work_brief_digest, persisted_pr.work_brief_digest);
        assert_eq!(pr_proj.policy_digest, persisted_pr.policy_digest);
        assert_eq!(pr_proj.plan_status, PlanStatus::Published);
        assert_eq!(pr_proj.current_plan_status, Some(PlanStatus::Published));
        assert_eq!(pr_proj.executable, None);
        assert_eq!(pr_proj.task_interface_task_count, 3);
        assert_eq!(pr_proj.owned_interface_count, 24);
        assert_eq!(pr_proj.consumed_interface_count, 16);
        assert_eq!(pr_proj.resolved_open_question_count, 18);
        assert_eq!(pr_proj.deferred_proposal_count, 18);
        assert!(pr_proj.omitted_task_interfaces);
        assert!(pr_proj.omitted_resolved_open_questions);
        assert!(pr_proj.omitted_deferred_proposals);
        assert!(pr_proj.nested_detail_omitted);

        // Projected planning_result must NEVER deserialize as a full authoritative PlanningResultRef.
        assert!(
            serde_json::from_value::<PlanningResultRef>(
                mcp_data_json["jobs"][0]["planning_result"].clone()
            )
            .is_err(),
            "status projection must never deserialize as a full PlanningResultRef"
        );

        assert_eq!(mcp_status.commands.len(), 1);
        let c = &mcp_status.commands[0];
        assert_eq!(c.command_id, enqueued.command_id);
        assert_eq!(c.command_digest, enqueued.command_digest);
        assert_eq!(c.current_plan_status, Some(PlanStatus::Published));
        assert_eq!(c.provider_health_summary.invocation_count, 1);
        assert!(c.provider_health_summary.omitted_records);
        let res_proj = c.result_summary.as_ref().expect("projected result_summary");
        assert!(res_proj.status_projection_only);
        assert!(!res_proj.grants_execution_authority);
        assert!(res_proj.artifact_verified);
        assert_eq!(
            res_proj.kind,
            CoworkerStatusCommandResultKind::OrchestratorPlan
        );
        assert!(res_proj.omitted_nested_detail);
    }

    // Zero provider calls, zero budget consumption, and byte-for-byte unchanged persisted files!
    assert_eq!(fx.worker_invocation_count(), 0);
    assert_eq!(fs::read(&wf_ledger_path).unwrap(), wf_bytes_before);
    assert_eq!(fs::read(&cmd_ledger_path).unwrap(), cmd_bytes_before);
    assert_eq!(fs::read(&artifact_path).unwrap(), art_bytes_before);
    assert_eq!(fs::read(&approvals_path).unwrap(), approvals_bytes_before);
    let reloaded_ledger = wf_store.read_binding_ledger(&fx.binding_id).unwrap();
    let reloaded_pr = reloaded_ledger
        .jobs
        .iter()
        .find(|j| j.job_id == job.job_id)
        .unwrap()
        .planning_result
        .as_ref()
        .unwrap();
    assert_eq!(reloaded_pr.task_interfaces.len(), 3);
    assert_eq!(reloaded_pr.resolved_open_questions.len(), 18);
    assert_eq!(reloaded_pr.deferred_proposals.len(), 18);

    fx.shutdown().await;
}

#[tokio::test]
async fn coworker_status_default_20_explicit_20_limit_1_omission_indicators_and_superseded_reconciliation_semantics(
) {
    let mut fx = StatusBoundsFixture::start().await;
    let now_ms = threadlane_native::current_wall_clock_ms().expect("wall clock");
    let wf_store = fx.workflow_store();
    let cmd_store = fx.command_store();
    let driver = fx.driver();

    // Grant 0 (oldest): will receive an EffectUnknown reservation at the end so we also prove
    // attention/unknown-effect preservation across >20 grants and at limit=1.
    let oldest_attention_grant_id = fx.create_grant(
        "grant_multi_00_attention",
        now_ms.saturating_sub(50_000),
        now_ms + 3_600_000,
        32,
        262_144,
    );
    // Grants 1..=2: revoked grants with long revocation reasons.
    for idx in 1..=2 {
        let gid = fx.create_grant(
            &format!("grant_multi_revoked_{idx:02}"),
            now_ms.saturating_sub(45_000),
            now_ms + 3_600_000,
            8,
            262_144,
        );
        wf_store
            .revoke_planning_grant(&RevokePlanningGrantRequest {
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: gid,
                expected_ledger_revision: None,
                revoked_at_ms: now_ms.saturating_sub(40_000) + idx,
                reason: format!(
                    "Revoked grant #{idx}: {}",
                    "operator-audit-reason-".repeat(150)
                ),
            })
            .expect("revoke grant");
    }
    // Grants 3..=23: 21 currently active grants -> total grants = 24, active grants = 21!
    let mut active_grant_ids = Vec::new();
    for idx in 3..=23 {
        let gid = fx.create_grant(
            &format!("grant_multi_active_{idx:02}"),
            now_ms.saturating_sub(30_000) + (idx * 10),
            now_ms + 3_600_000,
            32,
            262_144,
        );
        active_grant_ids.push(gid);
    }
    let work_grant_id = active_grant_ids
        .last()
        .expect("latest active grant")
        .clone();

    // Job 0: planned at rev 1, then updated to rev 2 so command 0 has `current_plan_status = Superseded`
    // (`executable = Some(false)`) while command 1 has `current_plan_status = Published`.
    let job0 = wf_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: fx.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "job_multi_00".to_string(),
            expected_ledger_revision: None,
            work_brief: WorkBrief {
                brief_id: "brf_multi_00".to_string(),
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                project_base: fx.project_base.clone(),
                kind: WorkBriefKind::NewJob,
                summary: format!("Job 00 summary: {}", "initial-planning-scope-".repeat(120)),
                confirmed_requirements: vec!["Update src/parser.rs".to_string()],
                proposed_additions: Vec::new(),
                assumptions: Vec::new(),
                open_questions: Vec::new(),
                accepted_architect_decisions: Vec::new(),
            },
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create job 00");

    let plan_resp = json!({
        "outcome": "plan",
        "schema_version": 1,
        "summary": "Publish parser plan",
        "tasks": [{
            "task_id": "task_parse",
            "title": "Update parser",
            "description": "Implement parser changes in src/parser.rs",
            "role": "coder",
            "difficulty": "medium",
            "depends_on": [],
            "allowed_write_paths": ["src/parser.rs"],
            "owned_interfaces": ["iface_parser_v1"],
            "consumed_interfaces": []
        }],
        "check_suggestions": [],
        "unconfirmed_proposal_notes": [],
        "resolved_open_questions": [],
        "deferred_proposals": []
    });
    fs::write(
        &fx.response_json_path,
        serde_json::to_vec(&plan_resp).unwrap(),
    )
    .unwrap();

    let cmd_r1 = cmd_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(
            OrchestrateWorkBriefPlanRequest {
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: work_grant_id.clone(),
                job_id: job0.job_id.clone(),
                request_key: "cmd_multi_plan_r1".to_string(),
                expected_ledger_revision: None,
                difficulty: None,
                architect_difficulty: None,
                plan_id: None,
                expected_plan_revision: 0,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                current_time_ms: now_ms,
            },
        ))
        .expect("enqueue r1");
    driver
        .execute_command(&fx.binding_id, THREAD_UUID, &cmd_r1.command_id)
        .await
        .expect("execute r1");

    let published_plan_id = wf_store.read_binding_ledger(&fx.binding_id).unwrap().jobs[0]
        .planning_result
        .as_ref()
        .unwrap()
        .plan_id
        .clone();

    let cmd_r2 = cmd_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(
            OrchestrateWorkBriefPlanRequest {
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: work_grant_id.clone(),
                job_id: job0.job_id.clone(),
                request_key: "cmd_multi_plan_r2".to_string(),
                expected_ledger_revision: None,
                difficulty: None,
                architect_difficulty: None,
                plan_id: Some(published_plan_id.clone()),
                expected_plan_revision: 1,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                current_time_ms: now_ms + 1,
            },
        ))
        .expect("enqueue r2");
    driver
        .execute_command(&fx.binding_id, THREAD_UUID, &cmd_r2.command_id)
        .await
        .expect("execute r2");

    // Clarification command + Consultation command.
    let clarify_resp = json!({
        "outcome": "requires_human_clarification",
        "schema_version": 1,
        "summary": format!("Clarification summary: {}", "clarify-detail-".repeat(180)),
        "questions": (0..10).map(|i| format!("Question {i}: {}", "human-decision-".repeat(60))).collect::<Vec<_>>(),
        "referenced_files": ["src/lib.rs", "src/parser.rs"]
    });
    fs::write(
        &fx.response_json_path,
        serde_json::to_vec(&clarify_resp).unwrap(),
    )
    .unwrap();
    let cmd_clarify = cmd_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(
            OrchestrateWorkBriefPlanRequest {
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: work_grant_id.clone(),
                job_id: job0.job_id.clone(),
                request_key: "cmd_multi_clarify".to_string(),
                expected_ledger_revision: None,
                difficulty: None,
                architect_difficulty: None,
                plan_id: Some(published_plan_id.clone()),
                expected_plan_revision: 2,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                current_time_ms: now_ms + 2,
            },
        ))
        .expect("enqueue clarify");
    driver
        .execute_command(&fx.binding_id, THREAD_UUID, &cmd_clarify.command_id)
        .await
        .expect("execute clarify");

    let consult_resp = json!({
        "outcome": "requires_architect_advice",
        "schema_version": 1,
        "summary": format!("Consultation summary: {}", "architect-check-".repeat(180)),
        "questions": [{
            "question_id": "q_arch_bounds",
            "question_summary": "Verify parser trait stability",
            "dependency_files": ["src/parser.rs", "src/exporter.rs"],
            "interfaces": ["iface_parser_v1"]
        }]
    });
    fs::write(
        &fx.response_json_path,
        serde_json::to_vec(&consult_resp).unwrap(),
    )
    .unwrap();
    let cmd_consult = cmd_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(
            OrchestrateWorkBriefPlanRequest {
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: work_grant_id.clone(),
                job_id: job0.job_id.clone(),
                request_key: "cmd_multi_consult".to_string(),
                expected_ledger_revision: None,
                difficulty: None,
                architect_difficulty: None,
                plan_id: Some(published_plan_id.clone()),
                expected_plan_revision: 2,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                current_time_ms: now_ms + 3,
            },
        ))
        .expect("enqueue consult");
    driver
        .execute_command(&fx.binding_id, THREAD_UUID, &cmd_consult.command_id)
        .await
        .expect("execute consult");

    // Populate 22 jobs total (including Amendment jobs with 3 KiB amendment_reason).
    let mut prev_job_id = job0.job_id.clone();
    let mut prev_job_rev =
        wf_store.read_binding_ledger(&fx.binding_id).unwrap().jobs[0].job_revision;
    for idx in 1..22usize {
        let kind = if idx % 3 == 0 {
            WorkBriefKind::Amendment {
                target_job_id: prev_job_id.clone(),
                target_job_revision: prev_job_rev,
                amendment_reason: format!(
                    "Amendment #{idx}: {}",
                    "scope-refinement-reason-".repeat(120)
                ),
            }
        } else {
            WorkBriefKind::NewJob
        };
        let created = wf_store
            .create_or_amend_job(&CreateCoworkerJobRequest {
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                request_key: format!("job_multi_{idx:02}"),
                expected_ledger_revision: None,
                work_brief: WorkBrief {
                    brief_id: format!("brf_multi_{idx:02}"),
                    binding_id: fx.binding_id.clone(),
                    thread_uuid: THREAD_UUID.to_string(),
                    project_base: fx.project_base.clone(),
                    kind,
                    summary: format!(
                        "Job {idx:02} summary: {}",
                        "detailed-brief-text-".repeat(140)
                    ),
                    confirmed_requirements: vec!["Update src/metrics.rs".to_string()],
                    proposed_additions: vec!["Optional future dashboard".to_string()],
                    assumptions: Vec::new(),
                    open_questions: Vec::new(),
                    accepted_architect_decisions: Vec::new(),
                },
                amended_target_task_ids: Vec::new(),
                amended_write_paths: Vec::new(),
                amended_interfaces: Vec::new(),
            })
            .expect("create multi job");
        prev_job_id = created.job_id;
        prev_job_rev = created.job_revision;
    }

    // Populate 22 ArchitectAdviceRecord entries with 3 KiB decision_summary, distributing
    // reservations across the 21 active grants so each stays within its legal 1..=32 call budget.
    let arch_choice = fx.resolve_advisory_choice(WorkflowRole::Architect);
    let dep_blobs = CoworkerPlanningService::compute_dependency_file_blobs(
        &fx.project_base,
        &["src/lib.rs".to_string(), "src/parser.rs".to_string()],
    )
    .expect("dep blobs");
    let policy_digest = default_planning_policy().digest().expect("policy digest");
    let arch_ctx = SyntheticArchitectContext {
        choice: &arch_choice,
        dep_blobs: &dep_blobs,
        work_brief_digest: &job0.work_brief_digest,
        policy_digest: &policy_digest,
        base_time_ms: now_ms + 100,
    };
    for idx in 0..22usize {
        let target_grant_id = &active_grant_ids[idx % active_grant_ids.len()];
        record_synthetic_architect_advice(
            &fx,
            target_grant_id,
            &arch_ctx,
            idx,
            format!(
                "Architect decision #{idx:02}: {}",
                "interface-boundary-rationale-".repeat(105)
            ),
        );
    }

    // Add 18 KnownFailed commands with 3.5 KiB diagnostics, plus 1 final Unknown command
    // (`pending_reconciliation == true`) -> 23 commands total!
    for idx in 0..18usize {
        let target_grant_id = &active_grant_ids[idx % active_grant_ids.len()];
        let diag = format!(
            "Known failure #{idx:02}: {}",
            "sanitized-worker-diagnostic-".repeat(120)
        );
        record_synthetic_known_failed_command(
            &fx,
            &cmd_store,
            SyntheticFailedCommand {
                grant_id: target_grant_id,
                job_id: &prev_job_id,
                base_time_ms: now_ms + 500,
                idx,
                diagnostic: &diag,
            },
        );
    }
    let unknown_enq = cmd_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_architect(
            RunArchitectAdviceRequest {
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: work_grant_id.clone(),
                request_key: "cmd_multi_unknown_last".to_string(),
                expected_ledger_revision: None,
                difficulty: None,
                context_dependencies: ArchitectContextDependencies {
                    project_base: fx.project_base.clone(),
                    work_brief_digest: job0.work_brief_digest.clone(),
                    plan_digest: None,
                    policy_digest: policy_digest.clone(),
                    dependency_file_blobs: dep_blobs.clone(),
                    interface_ownership_digests: BTreeMap::new(),
                    accepted_predecessor_trees: BTreeMap::new(),
                },
                question_summary: "Check unknown command reconciliation marker".to_string(),
                prior_advice_context: Vec::new(),
                accept_into_brief: false,
                current_time_ms: now_ms + 900,
            },
        ))
        .expect("enqueue unknown command");
    let unknown_guard = cmd_store
        .ownership_store()
        .try_acquire_job(
            &unknown_enq.ownership_key().expect("ownership key"),
            threadlane_native::current_wall_clock_ms().expect("clock"),
        )
        .expect("acquire ownership")
        .into_guard()
        .expect("uncontended ownership guard");
    cmd_store
        .claim_running(
            &unknown_guard,
            &fx.binding_id,
            THREAD_UUID,
            &unknown_enq.command_id,
            None,
        )
        .unwrap();
    cmd_store
        .record_unknown(
            &unknown_guard,
            &fx.binding_id,
            THREAD_UUID,
            &unknown_enq.command_id,
            &format!(
                "Interrupted owner before receipt: {}",
                "pending-reconciliation-detail-".repeat(110)
            ),
            None,
        )
        .unwrap();

    // Mark a reservation on Grant 0 (index 0, oldest grant!) as EffectUnknown so active_grants drops
    // from 21 to 21 (Grant 0 was separate from active_grant_ids, so 20 -> 21: let's check: Grant 0 was
    // active, now Grant 0 has unknown_effect_calls == 1 so active_grants_all has 21 items!).
    let unknown_res = wf_store
        .reserve_planning_call(&ReservePlanningCallRequest {
            binding_id: fx.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: oldest_attention_grant_id.clone(),
            request_key: "res_oldest_grant_unknown".to_string(),
            expected_ledger_revision: None,
            role: WorkflowRole::Architect,
            difficulty: None,
            selected_choice: arch_choice,
            context_digest: sha256_hex(b"oldest-grant-unknown-context"),
            call_project_base: Some(fx.project_base.clone()),
            current_time_ms: now_ms + 950,
        })
        .expect("reserve on oldest grant");
    wf_store
        .finalize_planning_call(&FinalizePlanningCallRequest {
            binding_id: fx.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: oldest_attention_grant_id.clone(),
            reservation_id: unknown_res.reservation_id,
            expected_ledger_revision: None,
            status: PlanningReservationStatus::EffectUnknown,
            updated_at_ms: now_ms + 960,
            run_id: None,
            report_artifact_id: None,
            output_digest: None,
            diagnostic: Some("Simulated crash before receipt".to_string()),
        })
        .expect("finalize oldest grant reservation EffectUnknown");

    // Prove legacy 20-row envelope overflows 262,144 bytes!
    let legacy_20 = fx.build_legacy_envelope(20);
    let legacy_20_bytes =
        measure_mcp_response_bytes(&fx.binding_id, "coworker_status", &legacy_20).unwrap();
    assert!(
        legacy_20_bytes > MAX_MCP_RESPONSE_BYTES,
        "legacy 20-row status envelope ({legacy_20_bytes} bytes) must exceed {MAX_MCP_RESPONSE_BYTES}"
    );

    fx.reset_worker_invocations();
    let mcp = fx.enrolled_mcp();

    // 1. Default (`None`) and explicit `Some(20)` must both apply `effective_limit = 20` (never
    //    secretly 1!), stay <= 262,144 wire bytes, and report count-limit omissions accurately.
    let (status_default, _, wire_default) =
        invoke_status_and_verify_wire_envelope(&mcp, &fx.binding_id, json!({})).await;
    let (status_explicit_20, _, wire_explicit_20) =
        invoke_status_and_verify_wire_envelope(&mcp, &fx.binding_id, json!({ "limit": 20 })).await;
    assert!(wire_default <= MAX_MCP_RESPONSE_BYTES);
    assert!(wire_explicit_20 <= MAX_MCP_RESPONSE_BYTES);
    assert_eq!(status_default.effective_limit, 20);
    assert_eq!(status_explicit_20.effective_limit, 20);
    // The row limit is an upper bound; the full MCP envelope may require fewer rows.
    // A two-row response fits this fixture, so default status must not silently become limit=1.
    let (status_limit_2, _, wire_limit_2) =
        invoke_status_and_verify_wire_envelope(&mcp, &fx.binding_id, json!({ "limit": 2 })).await;
    assert!(wire_limit_2 <= MAX_MCP_RESPONSE_BYTES);
    assert_eq!(status_limit_2.effective_limit, 2);
    for (name, returned, two_row_count, bounds) in [
        (
            "grants",
            status_default.grants.len(),
            status_limit_2.grants.len(),
            &status_default.grants_bounds,
        ),
        (
            "active grants",
            status_default.active_grants.len(),
            status_limit_2.active_grants.len(),
            &status_default.active_grants_bounds,
        ),
        (
            "jobs",
            status_default.jobs.len(),
            status_limit_2.jobs.len(),
            &status_default.jobs_bounds,
        ),
        (
            "architect advice",
            status_default.architect_advice.len(),
            status_limit_2.architect_advice.len(),
            &status_default.architect_advice_bounds,
        ),
        (
            "commands",
            status_default.commands.len(),
            status_limit_2.commands.len(),
            &status_default.commands_bounds,
        ),
    ] {
        assert_eq!(two_row_count, 2, "{name}: the two-row envelope fits");
        assert!(
            returned >= two_row_count,
            "{name}: no hidden limit=1 fallback"
        );
        assert!(returned <= 20, "{name}: respect the requested upper bound");
        assert_eq!(bounds.returned_count, returned, "{name}: returned count");
        let after_limit = bounds.total_count.min(20);
        assert_eq!(
            bounds.omitted_by_limit_count,
            bounds.total_count - after_limit,
            "{name}: count-limit omissions"
        );
        assert_eq!(
            bounds.omitted_by_byte_budget_count,
            after_limit - returned,
            "{name}: byte-budget omissions"
        );
        assert_eq!(bounds.has_more, bounds.total_count > returned, "{name}");
        assert!(bounds.truncated_text_row_count <= returned, "{name}");
        assert!(
            bounds.projected_nested_detail_row_count <= returned,
            "{name}"
        );
    }
    assert_eq!(status_default.grants, status_explicit_20.grants);
    assert_eq!(
        status_default.active_grants,
        status_explicit_20.active_grants
    );
    assert_eq!(status_default.jobs, status_explicit_20.jobs);
    assert_eq!(
        status_default.architect_advice,
        status_explicit_20.architect_advice
    );
    assert_eq!(status_default.commands, status_explicit_20.commands);

    assert!(status_default.has_more_active_grants);
    assert!(status_default.has_more_grants);
    assert!(status_default.has_more_jobs);
    assert!(status_default.has_more_architect_advice);
    assert!(status_default.has_more_commands);
    assert_eq!(status_default.active_grants_bounds.total_count, 21);
    assert_eq!(
        status_default.active_grants_bounds.omitted_by_limit_count,
        1
    );
    assert_eq!(status_default.grants_bounds.total_count, 24);
    assert_eq!(status_default.grants_bounds.omitted_by_limit_count, 4);
    assert_eq!(status_default.jobs_bounds.total_count, 22);
    assert_eq!(status_default.jobs_bounds.omitted_by_limit_count, 2);
    assert_eq!(status_default.architect_advice_bounds.total_count, 22);
    assert_eq!(
        status_default
            .architect_advice_bounds
            .omitted_by_limit_count,
        2
    );
    assert_eq!(status_default.commands_bounds.total_count, 23);
    assert_eq!(status_default.commands_bounds.omitted_by_limit_count, 3);
    assert!(status_default.details_omitted_or_truncated_by_byte_budget);

    // Oldest grant with `unknown_effect_calls == 1` is preserved in `grants` despite 23 newer grants!
    assert!(status_default
        .grants
        .iter()
        .any(|g| g.grant_id == oldest_attention_grant_id && g.usage.unknown_effect_calls == 1));
    assert!(status_default.pending_reconciliation);
    assert_eq!(status_default.unknown_effect_reservation_count, 1);
    assert_eq!(status_default.pending_reconciliation_command_count, 1);

    // Verify `limit: 1` returns 1 row per section while preserving reconciliation attention rows!
    let (status_limit_1, _, wire_limit_1) =
        invoke_status_and_verify_wire_envelope(&mcp, &fx.binding_id, json!({ "limit": 1 })).await;
    assert!(wire_limit_1 <= MAX_MCP_RESPONSE_BYTES);
    assert_eq!(status_limit_1.effective_limit, 1);
    assert_eq!(status_limit_1.active_grants.len(), 1);
    assert_eq!(status_limit_1.grants.len(), 1);
    assert_eq!(status_limit_1.jobs.len(), 1);
    assert_eq!(status_limit_1.architect_advice.len(), 1);
    assert_eq!(status_limit_1.commands.len(), 1);
    assert_eq!(status_limit_1.grants[0].grant_id, oldest_attention_grant_id);
    assert_eq!(
        status_limit_1.commands[0].command_id,
        unknown_enq.command_id
    );
    assert!(status_limit_1.commands[0].pending_reconciliation);
    assert!(status_limit_1.has_more_active_grants);
    assert!(status_limit_1.has_more_grants);
    assert!(status_limit_1.has_more_jobs);
    assert!(status_limit_1.has_more_architect_advice);
    assert!(status_limit_1.has_more_commands);

    // Also verify Superseded vs Published plan status preservation when querying a smaller command
    // set or checking command projections directly via `CoworkerStatusCommandSummary::from_summary`.
    let all_cmd_page = list_planning_command_summaries_readonly(
        &fx.plans_dir,
        &fx.registry_dir,
        &fx.binding_id,
        THREAD_UUID,
        usize::MAX,
        None,
    )
    .unwrap();
    let r1_proj = threadlane_native::planning_gateway::CoworkerStatusCommandSummary::from_summary(
        all_cmd_page
            .commands
            .iter()
            .find(|c| c.command_id == cmd_r1.command_id)
            .unwrap(),
        DEFAULT_COWORKER_STATUS_TEXT_WIRE_BUDGET_BYTES,
    );
    assert_eq!(r1_proj.current_plan_status, Some(PlanStatus::Superseded));
    assert_eq!(r1_proj.executable, Some(false));
    assert_eq!(
        r1_proj
            .result_summary
            .as_ref()
            .unwrap()
            .plan_result
            .as_ref()
            .unwrap()
            .current_plan_status,
        Some(PlanStatus::Superseded)
    );
    let r2_proj = threadlane_native::planning_gateway::CoworkerStatusCommandSummary::from_summary(
        all_cmd_page
            .commands
            .iter()
            .find(|c| c.command_id == cmd_r2.command_id)
            .unwrap(),
        DEFAULT_COWORKER_STATUS_TEXT_WIRE_BUDGET_BYTES,
    );
    assert_eq!(r2_proj.current_plan_status, Some(PlanStatus::Published));
    assert_eq!(r2_proj.executable, None);

    assert_eq!(fx.worker_invocation_count(), 0);
    fx.shutdown().await;
}

#[tokio::test]
async fn coworker_status_byte_budget_row_reduction_sets_has_more_and_omitted_by_byte_budget_counts()
{
    // Construct a valid deep canonical repository path (~1,680 bytes) so 20 projected rows across
    // all 5 sections (~101 CanonicalProjectBaseRef copies + text fields) exceed 262,144 wire bytes,
    // forcing `coworker_status` to reduce row counts by byte budget even when `total_count == 20`!
    let deep_subpath = (0..15)
        .map(|i| format!("seg_{i:02}_{}", "a".repeat(100)))
        .collect::<Vec<_>>()
        .join("/");
    let mut fx = StatusBoundsFixture::start_with_repo_subpath(&deep_subpath).await;
    assert!(fx.project_base.repository_path.len() > 1_550);

    let now_ms = threadlane_native::current_wall_clock_ms().expect("wall clock");
    let wf_store = fx.workflow_store();
    let cmd_store = fx.command_store();

    let mut grant_ids = Vec::with_capacity(20);
    for idx in 0..20usize {
        let gid = fx.create_grant(
            &format!("grant_deep_{idx:02}"),
            now_ms.saturating_sub(20_000) + (idx as u64),
            now_ms + 3_600_000,
            32,
            262_144,
        );
        grant_ids.push(gid);
    }

    let mut last_job_id = String::new();
    let mut first_brief_digest = String::new();
    for idx in 0..20usize {
        let job = wf_store
            .create_or_amend_job(&CreateCoworkerJobRequest {
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                request_key: format!("job_deep_{idx:02}"),
                expected_ledger_revision: None,
                work_brief: WorkBrief {
                    brief_id: format!("brf_deep_{idx:02}"),
                    binding_id: fx.binding_id.clone(),
                    thread_uuid: THREAD_UUID.to_string(),
                    project_base: fx.project_base.clone(),
                    kind: WorkBriefKind::NewJob,
                    summary: format!("Deep job {idx:02}: {}", "summary-payload-".repeat(60)),
                    confirmed_requirements: vec!["Update src/parser.rs".to_string()],
                    proposed_additions: Vec::new(),
                    assumptions: Vec::new(),
                    open_questions: Vec::new(),
                    accepted_architect_decisions: Vec::new(),
                },
                amended_target_task_ids: Vec::new(),
                amended_write_paths: Vec::new(),
                amended_interfaces: Vec::new(),
            })
            .expect("create deep job");
        if idx == 0 {
            first_brief_digest = job.work_brief_digest.clone();
        }
        last_job_id = job.job_id;
    }

    let arch_choice = fx.resolve_advisory_choice(WorkflowRole::Architect);
    let dep_blobs = CoworkerPlanningService::compute_dependency_file_blobs(
        &fx.project_base,
        &["src/lib.rs".to_string()],
    )
    .unwrap();
    let policy_digest = default_planning_policy().digest().unwrap();
    let arch_ctx = SyntheticArchitectContext {
        choice: &arch_choice,
        dep_blobs: &dep_blobs,
        work_brief_digest: &first_brief_digest,
        policy_digest: &policy_digest,
        base_time_ms: now_ms + 100,
    };
    for (idx, grant_id) in grant_ids.iter().enumerate() {
        record_synthetic_architect_advice(
            &fx,
            grant_id,
            &arch_ctx,
            idx,
            format!("Deep advice {idx:02}: {}", "decision-note-".repeat(60)),
        );
    }
    for (idx, grant_id) in grant_ids.iter().enumerate() {
        let diag = format!("Deep diagnostic {idx:02}: {}", "diag-note-".repeat(60));
        record_synthetic_known_failed_command(
            &fx,
            &cmd_store,
            SyntheticFailedCommand {
                grant_id,
                job_id: &last_job_id,
                base_time_ms: now_ms + 500,
                idx,
                diagnostic: &diag,
            },
        );
    }

    let mcp = fx.enrolled_mcp();
    let (status, _, wire_len) =
        invoke_status_and_verify_wire_envelope(&mcp, &fx.binding_id, json!({})).await;
    assert!(wire_len <= MAX_MCP_RESPONSE_BYTES);
    assert_eq!(status.effective_limit, 20);

    // Every section had exact total_count == 20 == effective_limit (so omitted_by_limit_count == 0),
    // yet byte-budget fitting reduced returned rows (< 20 while still > 1!) and accurately marked
    // both `rows_omitted_by_byte_budget` and `has_more_*`!
    assert!(status.rows_omitted_by_byte_budget);
    assert!(status.details_omitted_or_truncated_by_byte_budget);
    assert_eq!(status.active_grants_bounds.total_count, 20);
    assert_eq!(status.grants_bounds.total_count, 20);
    assert_eq!(status.jobs_bounds.total_count, 20);
    assert_eq!(status.architect_advice_bounds.total_count, 20);
    assert_eq!(status.commands_bounds.total_count, 20);
    assert_eq!(status.active_grants_bounds.omitted_by_limit_count, 0);
    assert_eq!(status.grants_bounds.omitted_by_limit_count, 0);
    assert_eq!(status.jobs_bounds.omitted_by_limit_count, 0);
    assert_eq!(status.architect_advice_bounds.omitted_by_limit_count, 0);
    assert_eq!(status.commands_bounds.omitted_by_limit_count, 0);

    let total_byte_omitted = status.active_grants_bounds.omitted_by_byte_budget_count
        + status.grants_bounds.omitted_by_byte_budget_count
        + status.jobs_bounds.omitted_by_byte_budget_count
        + status.architect_advice_bounds.omitted_by_byte_budget_count
        + status.commands_bounds.omitted_by_byte_budget_count;
    assert!(
        total_byte_omitted > 0,
        "20 rows with 1.6 KiB paths must trigger omitted_by_byte_budget_count > 0"
    );
    assert!(
        status.jobs.len() > 1 && status.commands.len() > 1,
        "byte-budget reduction must keep multiple rows rather than collapsing to 1"
    );

    assert_eq!(
        status.has_more_active_grants,
        status.active_grants.len() < 20
    );
    assert_eq!(status.has_more_grants, status.grants.len() < 20);
    assert_eq!(status.has_more_jobs, status.jobs.len() < 20);
    assert_eq!(
        status.has_more_architect_advice,
        status.architect_advice.len() < 20
    );
    assert_eq!(status.has_more_commands, status.commands.len() < 20);
    assert!(status.has_more_active_grants || status.has_more_grants);

    assert_eq!(fx.worker_invocation_count(), 0);
    fx.shutdown().await;
}

#[tokio::test]
async fn coworker_status_unicode_and_json_escaping_truncation_stays_valid_utf8_and_within_wire_budget(
) {
    // 1. Direct boundary sweep across multi-byte UTF-8 and heavy JSON-escape characters.
    let tricky = "ğüşıöçĞÜŞİÖÇ🦀🧪🚀🔒\"quoted\"\\slash\\\nline\ttab日本語".repeat(30);
    for budget in [0, 1, 2, 3, 4, 5, 6, 7, 8, 15, 64, 127, 128, 511, 512, 513] {
        let (out, truncated) = truncate_status_text_unicode_safe(&tricky, budget);
        assert!(truncated);
        assert!(tricky.starts_with(&out));
        let dual_envelope = ThreadlaneMcpServer::build_structured_tool_result(
            "bnd_utf8_check",
            "coworker_status",
            json!({ "t": out }),
        );
        let empty_envelope = ThreadlaneMcpServer::build_structured_tool_result(
            "bnd_utf8_check",
            "coworker_status",
            json!({ "t": "" }),
        );
        let delta = serde_json::to_vec(&dual_envelope).unwrap().len()
            - serde_json::to_vec(&empty_envelope).unwrap().len();
        assert!(
            delta <= budget,
            "actual dual-envelope wire delta {delta} exceeded budget {budget}"
        );
    }

    // 2. End-to-end control-free bounded Unicode + JSON-escape torture across grant, job,
    //    amendment, advice, and command records (<= 4,096 UTF-8 bytes per persisted field).
    let mut fx = StatusBoundsFixture::start().await;
    let now_ms = threadlane_native::current_wall_clock_ms().expect("wall clock");
    let wf_store = fx.workflow_store();
    let cmd_store = fx.command_store();

    let revoked_gid = fx.create_grant(
        "grant_unicode_revoked",
        now_ms.saturating_sub(20_000),
        now_ms + 3_600_000,
        4,
        131_072,
    );
    let unicode_storm = r#"Öncelik 🦀 "kaçış" \ters\ 日本語テスト "#.repeat(50);
    assert!(unicode_storm.len() <= 4_096);
    wf_store
        .revoke_planning_grant(&RevokePlanningGrantRequest {
            binding_id: fx.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: revoked_gid,
            expected_ledger_revision: None,
            revoked_at_ms: now_ms.saturating_sub(10_000),
            reason: unicode_storm.clone(),
        })
        .unwrap();

    let active_gid = fx.create_grant(
        "grant_unicode_active",
        now_ms.saturating_sub(5_000),
        now_ms + 3_600_000,
        8,
        131_072,
    );
    let j1 = wf_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: fx.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "job_unicode_1".to_string(),
            expected_ledger_revision: None,
            work_brief: WorkBrief {
                brief_id: "brf_unicode_1".to_string(),
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                project_base: fx.project_base.clone(),
                kind: WorkBriefKind::NewJob,
                summary: unicode_storm.clone(),
                confirmed_requirements: vec!["Update src/lib.rs".to_string()],
                proposed_additions: Vec::new(),
                assumptions: Vec::new(),
                open_questions: Vec::new(),
                accepted_architect_decisions: Vec::new(),
            },
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .unwrap();
    let j2 = wf_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: fx.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "job_unicode_2_amend".to_string(),
            expected_ledger_revision: None,
            work_brief: WorkBrief {
                brief_id: "brf_unicode_2".to_string(),
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                project_base: fx.project_base.clone(),
                kind: WorkBriefKind::Amendment {
                    target_job_id: j1.job_id.clone(),
                    target_job_revision: j1.job_revision,
                    amendment_reason: unicode_storm.clone(),
                },
                summary: unicode_storm.clone(),
                confirmed_requirements: vec!["Update src/lib.rs".to_string()],
                proposed_additions: Vec::new(),
                assumptions: Vec::new(),
                open_questions: Vec::new(),
                accepted_architect_decisions: Vec::new(),
            },
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .unwrap();

    let arch_choice = fx.resolve_advisory_choice(WorkflowRole::Architect);
    let dep_blobs = CoworkerPlanningService::compute_dependency_file_blobs(
        &fx.project_base,
        &["src/lib.rs".to_string()],
    )
    .unwrap();
    let policy_digest = default_planning_policy().digest().unwrap();
    let arch_ctx = SyntheticArchitectContext {
        choice: &arch_choice,
        dep_blobs: &dep_blobs,
        work_brief_digest: &j2.work_brief_digest,
        policy_digest: &policy_digest,
        base_time_ms: now_ms,
    };
    record_synthetic_architect_advice(&fx, &active_gid, &arch_ctx, 0, unicode_storm.clone());
    record_synthetic_known_failed_command(
        &fx,
        &cmd_store,
        SyntheticFailedCommand {
            grant_id: &active_gid,
            job_id: &j2.job_id,
            base_time_ms: now_ms + 300,
            idx: 0,
            diagnostic: &unicode_storm,
        },
    );

    let mcp = fx.enrolled_mcp();
    let (status, _, wire_len) =
        invoke_status_and_verify_wire_envelope(&mcp, &fx.binding_id, json!({})).await;
    assert!(wire_len <= MAX_MCP_RESPONSE_BYTES);
    assert!(status.grants[0].revoked_reason_truncated);
    assert!(status.jobs[0].summary_truncated);
    assert!(status.jobs[1].summary_truncated);
    assert!(matches!(
        status.jobs[1].kind,
        CoworkerStatusBriefKindSummary::Amendment {
            amendment_reason_truncated: true,
            ..
        }
    ));
    assert!(status.architect_advice[0].decision_summary_truncated);
    assert!(status.commands[0].diagnostic_truncated);

    fx.shutdown().await;
}

#[tokio::test]
async fn coworker_status_rejects_wrong_binding_wrong_thread_and_invalid_limits_without_mutating_stores(
) {
    let mut fx = StatusBoundsFixture::start().await;

    // 1. Pure read-only check on uninitialized stores: zero directories, files, locks, or revisions.
    let empty_plans_dir = fx.root.join("fresh-empty-plans");
    create_private_dir(&empty_plans_dir);
    let gw_empty = CoworkerPlanningGateway::new(
        &fx.state_dir,
        &fx.registry_dir,
        &empty_plans_dir,
        &fx.authority_dir,
        &fx.sessions_dir,
        &fx.binding_id,
        THREAD_UUID,
    )
    .expect("gateway on empty_plans_dir");
    let reg_before = directory_entries(&fx.registry_dir);
    let auth_before = directory_entries(&fx.authority_dir);
    let sess_before = directory_entries(&fx.sessions_dir);
    let empty_status = gw_empty
        .coworker_status(&CoworkerStatusParams::default())
        .expect("coworker_status on empty plans dir");
    assert_eq!(empty_status.workflow_ledger_revision, 0);
    assert_eq!(empty_status.command_ledger_revision, 0);
    assert_eq!(empty_status.effective_limit, 20);
    assert!(directory_entries(&empty_plans_dir).is_empty());
    assert_eq!(directory_entries(&fx.registry_dir), reg_before);
    assert_eq!(directory_entries(&fx.authority_dir), auth_before);
    assert_eq!(directory_entries(&fx.sessions_dir), sess_before);

    // 2. Invalid limit rejection (direct gateway and PairedMcpServer).
    let mcp = fx.enrolled_mcp();
    for bad_limit in [0usize, 21usize, 100usize] {
        assert_eq!(
            gw_empty
                .coworker_status(&CoworkerStatusParams {
                    limit: Some(bad_limit)
                })
                .expect_err("bad limit must fail")
                .code,
            NativeErrorCode::ValidationFailed
        );
        assert_eq!(
            invoke_mcp_err_code(&mcp, "coworker_status", json!({ "limit": bad_limit })).await,
            "validation_failed"
        );
    }
    assert!(directory_entries(&empty_plans_dir).is_empty());

    // 3. Wrong thread_uuid and mismatched ledger binding_id must fail closed.
    let now_ms = threadlane_native::current_wall_clock_ms().expect("wall clock");
    let _grant_id = fx.create_grant(
        "grant_scope_guard_1",
        now_ms.saturating_sub(5_000),
        now_ms + 3_600_000,
        4,
        65_536,
    );

    let gw_wrong_thread = fx.gateway_for_scope(&fx.binding_id, OTHER_THREAD_UUID);
    assert_eq!(
        gw_wrong_thread
            .coworker_status(&CoworkerStatusParams::default())
            .expect_err("wrong thread_uuid must fail closed")
            .code,
        NativeErrorCode::SecurityViolation
    );

    // Copy the persisted ledger under a second binding's filename to simulate a mismatched
    // `binding_id` record and verify fail-closed rejection without mutating or deleting files.
    let wf_store = fx.workflow_store();
    let src_ledger = wf_store.ledger_path_for_binding(&fx.binding_id).unwrap();
    let forged_binding = "bnd_mismatched_target";
    let dst_ledger = wf_store.ledger_path_for_binding(forged_binding).unwrap();
    fs::copy(&src_ledger, &dst_ledger).expect("copy ledger to mismatched binding slot");
    fs::set_permissions(&dst_ledger, fs::Permissions::from_mode(0o600)).unwrap();
    let gw_wrong_binding = fx.gateway_for_scope(forged_binding, THREAD_UUID);
    let binding_mismatch_code = gw_wrong_binding
        .coworker_status(&CoworkerStatusParams::default())
        .expect_err("mismatched binding_id in ledger must fail closed")
        .code;
    assert!(matches!(
        binding_mismatch_code,
        NativeErrorCode::ProtocolError | NativeErrorCode::SecurityViolation
    ));

    assert_eq!(fx.worker_invocation_count(), 0);
    fx.shutdown().await;
}
