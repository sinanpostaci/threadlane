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

use std::collections::{BTreeMap, BTreeSet};
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
use threadlane_core::observation::{
    read_journal_records_readonly, validate_correlation_id, ObservationCallState,
    ObservationJournalRecord, ObservationRecordPhase, MAX_OBSERVATION_JOURNAL_BYTES,
    MAX_OBSERVATION_RECORD_LINE_BYTES, OBSERVATION_JOURNAL_FILE_NAME,
};
use threadlane_native::coworker_grants::{list_planning_grant_summaries, TrustedGrantScope};
use threadlane_native::coworker_workflow::{
    sha256_hex, ArchitectContextDependencies, ArchitectDecisionStatus, BindJobApprovalRequest,
    CanonicalProjectBaseRef, CoworkerJobOwnershipKey, CoworkerJobOwnershipStore,
    CoworkerPlanningService, CoworkerWorkflowStore, CreateCoworkerJobRequest,
    CreatePlanningGrantRequest, FinalizePlanningCallRequest, OrchestrateWorkBriefPlanRequest,
    OutboxDispatchKind, PersistedPlanningCallArtifact, PlanningCallBudget,
    PlanningReadOnlyCapabilities, PlanningReservationStatus, PlanningResultRef,
    RecordArchitectAdviceRequest, ReservePlanningCallRequest, ReserveTaskOutboxRequest,
    RevokePlanningGrantRequest, RunArchitectAdviceRequest, WorkBrief, WorkBriefKind,
};
use threadlane_native::planning_driver::{
    get_planning_command_summary_readonly, read_planning_commands_for_status_readonly,
    read_planning_commands_for_status_readonly_with_hooks, PlanningCommandEvidenceStatus,
    PlanningCommandVerificationIssueKind,
};
use threadlane_native::planning_gateway::{
    truncate_status_text_unicode_safe, CoworkerArchitectAdviceSummary, CoworkerJobSummary,
    CoworkerStatusBriefKindSummary, CoworkerStatusCommandResultKind, PlanningCommandParams,
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
    PairedMcpServer, PairingStore, PlanStatus, PlanningCommandDetailResponse,
    PlanningCommandDriver, PlanningCommandResultRef, PlanningCommandStatus, PlanningCommandStore,
    PlanningCommandSummary, PlanningGrantSummary, PolicyAllowRule, RegistryStore, ToolAdapter,
    WorkflowProfile, WorkflowRole, DEFAULT_COWORKER_STATUS_LIMIT, EXECUTION_POLICY_SCHEMA_VERSION,
    MAX_COWORKER_STATUS_LIMIT,
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

fn snapshot_tree_bytes(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(base: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        if !dir.exists() {
            return;
        }
        let mut entries: Vec<_> = fs::read_dir(dir)
            .expect("read_dir snapshot")
            .map(|e| e.expect("dir entry"))
            .collect();
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let path = entry.path();
            let meta = fs::symlink_metadata(&path).expect("symlink_metadata");
            if meta.file_type().is_dir() {
                walk(base, &path, out);
            } else if meta.file_type().is_file() {
                let rel = path.strip_prefix(base).expect("strip_prefix").to_path_buf();
                out.insert(rel, fs::read(&path).expect("read snapshot file"));
            }
        }
    }

    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

fn copy_private_tree(src: &Path, dst: &Path) {
    create_private_dir(dst);
    let mut entries: Vec<_> = fs::read_dir(src)
        .expect("read_dir copy")
        .map(|e| e.expect("copy entry"))
        .collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        let meta = fs::symlink_metadata(&src_path).expect("symlink_metadata copy");
        if meta.file_type().is_dir() {
            copy_private_tree(&src_path, &dst_path);
        } else if meta.file_type().is_file() {
            fs::copy(&src_path, &dst_path).expect("copy file");
            fs::set_permissions(&dst_path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 copied file");
        }
    }
}

fn first_diff_byte_offset(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right.iter())
        .position(|(a, b)| a != b)
        .unwrap_or_else(|| left.len().min(right.len()))
}

fn assert_tree_bytes_identical(
    before: &BTreeMap<PathBuf, Vec<u8>>,
    after: &BTreeMap<PathBuf, Vec<u8>>,
    stage_label: &str,
) {
    let all_paths: BTreeSet<&PathBuf> = before.keys().chain(after.keys()).collect();
    let mut diffs = Vec::new();
    for rel in all_paths {
        match (before.get(rel), after.get(rel)) {
            (Some(b_bytes), Some(a_bytes)) if b_bytes == a_bytes => {}
            (Some(b_bytes), Some(a_bytes)) => {
                let offset = first_diff_byte_offset(b_bytes, a_bytes);
                let b_hash = sha256_hex(b_bytes);
                let a_hash = sha256_hex(a_bytes);
                diffs.push(format!(
                    "modified path={} before_len={} before_sha256={} after_len={} after_sha256={} first_diff_offset={offset}",
                    rel.display(),
                    b_bytes.len(),
                    b_hash,
                    a_bytes.len(),
                    a_hash
                ));
            }
            (Some(b_bytes), None) => {
                let b_hash = sha256_hex(b_bytes);
                diffs.push(format!(
                    "missing_after path={} before_len={} before_sha256={b_hash}",
                    rel.display(),
                    b_bytes.len()
                ));
            }
            (None, Some(a_bytes)) => {
                let a_hash = sha256_hex(a_bytes);
                diffs.push(format!(
                    "unexpected_added path={} after_len={} after_sha256={a_hash}",
                    rel.display(),
                    a_bytes.len()
                ));
            }
            (None, None) => unreachable!(),
        }
    }
    if !diffs.is_empty() {
        let total = diffs.len();
        let preview: Vec<&str> = diffs.iter().take(16).map(String::as_str).collect();
        panic!(
            "[{stage_label}] expected byte-identical directory tree, found {total} file difference(s): {}",
            preview.join("; ")
        );
    }
}

#[derive(Debug, Clone, Copy)]
struct ExpectedPairedMcpJournalCall<'a> {
    tool_name: &'static str,
    finish_state: ObservationCallState,
    finish_job_id: Option<&'a str>,
    finish_plan_id: Option<&'a str>,
}

impl ExpectedPairedMcpJournalCall<'_> {
    const fn status_ok() -> Self {
        Self {
            tool_name: "coworker_status",
            finish_state: ObservationCallState::Completed,
            finish_job_id: None,
            finish_plan_id: None,
        }
    }
}

#[allow(clippy::too_many_lines)]
fn assert_plans_tree_after_paired_mcp_calls(
    plans_dir: &Path,
    plans_before: &BTreeMap<PathBuf, Vec<u8>>,
    journal_records_before: &[ObservationJournalRecord],
    binding_id: &str,
    thread_uuid: &str,
    expected_calls: &[ExpectedPairedMcpJournalCall<'_>],
    stage_label: &str,
) -> BTreeMap<PathBuf, Vec<u8>> {
    let plans_after = snapshot_tree_bytes(plans_dir);
    let journal_rel = PathBuf::from(OBSERVATION_JOURNAL_FILE_NAME);

    let mut non_journal_before = plans_before.clone();
    non_journal_before.remove(&journal_rel);
    let mut non_journal_after = plans_after.clone();
    non_journal_after.remove(&journal_rel);
    assert_tree_bytes_identical(
        &non_journal_before,
        &non_journal_after,
        &format!("{stage_label}:non_journal_plans"),
    );

    let prior_bytes = plans_before
        .get(&journal_rel)
        .expect("prior observation-journal.jsonl must exist");
    let after_bytes = plans_after
        .get(&journal_rel)
        .expect("after observation-journal.jsonl must exist");
    assert!(
        (after_bytes.len() as u64) <= MAX_OBSERVATION_JOURNAL_BYTES,
        "[{stage_label}] observation journal exceeded MAX_OBSERVATION_JOURNAL_BYTES"
    );
    if !after_bytes.starts_with(prior_bytes) {
        let offset = first_diff_byte_offset(prior_bytes, after_bytes);
        panic!(
            "[{stage_label}] observation-journal.jsonl did not preserve prior prefix: prior_len={} prior_sha256={} after_len={} after_sha256={} first_diff_offset={offset}",
            prior_bytes.len(),
            sha256_hex(prior_bytes),
            after_bytes.len(),
            sha256_hex(after_bytes)
        );
    }

    let full_journal_str = std::str::from_utf8(after_bytes).expect("journal UTF-8");
    assert!(
        !full_journal_str.contains(SECRET_SENTINEL),
        "[{stage_label}] observation journal leaked SECRET_SENTINEL"
    );

    let appended_bytes = &after_bytes[prior_bytes.len()..];
    assert!(
        !appended_bytes.is_empty() && appended_bytes.ends_with(b"\n"),
        "[{stage_label}] appended journal bytes must be non-empty newline-terminated JSONL"
    );
    let appended_str = std::str::from_utf8(appended_bytes).expect("tail UTF-8");
    let appended_lines: Vec<&str> = appended_str
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    assert_eq!(appended_lines.len(), expected_calls.len() * 2);
    for line in &appended_lines {
        assert!(line.len() <= MAX_OBSERVATION_RECORD_LINE_BYTES);
    }

    let (records_after, warnings_after) = read_journal_records_readonly(plans_dir).unwrap();
    assert!(
        warnings_after.is_empty(),
        "[{stage_label}] unexpected observation journal warnings: {warnings_after:?}"
    );
    assert_eq!(
        records_after.len(),
        journal_records_before.len() + expected_calls.len() * 2
    );
    if &records_after[..journal_records_before.len()] != journal_records_before {
        panic!(
            "[{stage_label}] prior {} observation journal record(s) were mutated",
            journal_records_before.len()
        );
    }

    let mut seen_record_ids: BTreeSet<String> = journal_records_before
        .iter()
        .map(|rec| rec.record_id.clone())
        .collect();
    let mut seen_request_ids: BTreeSet<String> = journal_records_before
        .iter()
        .map(|rec| rec.request_id.clone())
        .collect();
    assert_eq!(seen_record_ids.len(), journal_records_before.len());

    let new_records = &records_after[journal_records_before.len()..];
    let call_pairs = new_records.as_chunks::<2>().0.iter().zip(expected_calls);
    for (pair_idx, (pair, expected)) in call_pairs.enumerate() {
        let entry = &pair[0];
        let finish = &pair[1];
        let expected_op = format!("mcp.paired.{}", expected.tool_name);

        assert_eq!(entry.phase, ObservationRecordPhase::Entry);
        assert_eq!(finish.phase, ObservationRecordPhase::Finish);
        assert_eq!(entry.operation, expected_op);
        assert_eq!(finish.operation, expected_op);
        assert_eq!(entry.binding_id.as_deref(), Some(binding_id));
        assert_eq!(finish.binding_id.as_deref(), Some(binding_id));
        assert_eq!(entry.thread_uuid.as_deref(), Some(thread_uuid));
        assert_eq!(finish.thread_uuid.as_deref(), Some(thread_uuid));

        assert!(validate_correlation_id(&entry.record_id).is_some());
        assert!(
            seen_record_ids.insert(entry.record_id.clone()),
            "[{stage_label}] duplicate entry record_id at pair {pair_idx}"
        );
        assert!(validate_correlation_id(&finish.record_id).is_some());
        assert!(
            seen_record_ids.insert(finish.record_id.clone()),
            "[{stage_label}] duplicate finish record_id at pair {pair_idx}"
        );
        assert!(validate_correlation_id(&entry.request_id).is_some());
        assert!(
            seen_request_ids.insert(entry.request_id.clone()),
            "[{stage_label}] duplicate request_id at pair {pair_idx}"
        );
        assert_eq!(finish.request_id, entry.request_id);

        assert_eq!(entry.state, ObservationCallState::Running);
        assert_eq!(finish.state, expected.finish_state);
        assert_ne!(finish.state, ObservationCallState::Unknown);
        if let Some(expected_job_id) = expected.finish_job_id {
            assert_eq!(finish.job_id.as_deref(), Some(expected_job_id));
        }
        if let Some(expected_plan_id) = expected.finish_plan_id {
            assert_eq!(finish.plan_id.as_deref(), Some(expected_plan_id));
        }
    }

    plans_after
}

#[tokio::test]
async fn coworker_status_isolates_corrupted_historical_completed_artifact_without_weakening_strict_verifiers_or_scope_guards(
) {
    let mut fx = StatusBoundsFixture::start().await;
    let now_ms = threadlane_native::current_wall_clock_ms().expect("wall clock");
    let wf_store = fx.workflow_store();
    let cmd_store = fx.command_store();
    let driver = fx.driver();

    let grant_id = fx.create_grant(
        "grant_history_isolation_1",
        now_ms.saturating_sub(15_000),
        now_ms + 3_600_000,
        16,
        262_144,
    );

    // 1. Create three scoped jobs: older completed job, queued sibling job, and healthy current job.
    let job_old = wf_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: fx.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "job_hist_old".to_string(),
            expected_ledger_revision: None,
            work_brief: WorkBrief {
                brief_id: "brf_hist_old".to_string(),
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                project_base: fx.project_base.clone(),
                kind: WorkBriefKind::NewJob,
                summary: "Historical parser job".to_string(),
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
        .expect("create job_old");

    let job_queued = wf_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: fx.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "job_hist_queued".to_string(),
            expected_ledger_revision: None,
            work_brief: WorkBrief {
                brief_id: "brf_hist_queued".to_string(),
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                project_base: fx.project_base.clone(),
                kind: WorkBriefKind::NewJob,
                summary: "Queued metrics sibling job".to_string(),
                confirmed_requirements: vec!["Update src/metrics.rs".to_string()],
                proposed_additions: Vec::new(),
                assumptions: Vec::new(),
                open_questions: Vec::new(),
                accepted_architect_decisions: Vec::new(),
            },
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create job_queued");

    let job_current = wf_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: fx.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "job_hist_current".to_string(),
            expected_ledger_revision: None,
            work_brief: WorkBrief {
                brief_id: "brf_hist_current".to_string(),
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                project_base: fx.project_base.clone(),
                kind: WorkBriefKind::NewJob,
                summary: "Current healthy exporter job".to_string(),
                confirmed_requirements: vec!["Update src/exporter.rs".to_string()],
                proposed_additions: Vec::new(),
                assumptions: Vec::new(),
                open_questions: Vec::new(),
                accepted_architect_decisions: Vec::new(),
            },
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create job_current");

    // Execute older command (index 0, outside usual `limit: 1` tail selection).
    let old_plan_resp = json!({
        "outcome": "plan",
        "schema_version": 1,
        "summary": "Historical parser plan",
        "tasks": [{
            "task_id": "task_hist_parser",
            "title": "Historical parser update",
            "description": "Implement historical parser logic in src/parser.rs",
            "role": "coder",
            "difficulty": "medium",
            "depends_on": [],
            "allowed_write_paths": ["src/parser.rs"],
            "owned_interfaces": ["iface_hist_parser"],
            "consumed_interfaces": []
        }],
        "check_suggestions": [],
        "unconfirmed_proposal_notes": [],
        "resolved_open_questions": [],
        "deferred_proposals": []
    });
    fs::write(
        &fx.response_json_path,
        serde_json::to_vec(&old_plan_resp).unwrap(),
    )
    .unwrap();

    let cmd_old = cmd_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(
            OrchestrateWorkBriefPlanRequest {
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_old.job_id.clone(),
                request_key: "cmd_hist_old_corrupted".to_string(),
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
        .expect("enqueue cmd_old");
    driver
        .execute_command(&fx.binding_id, THREAD_UUID, &cmd_old.command_id)
        .await
        .expect("execute cmd_old");

    let old_completed_record = cmd_store
        .get_command(&fx.binding_id, THREAD_UUID, &cmd_old.command_id)
        .expect("read cmd_old")
        .expect("cmd_old exists");
    let (old_reservation_id, old_artifact_id) = match old_completed_record.result_ref.as_ref() {
        Some(PlanningCommandResultRef::OrchestratorPlan {
            planning_result, ..
        }) => (
            planning_result.reservation_id.clone(),
            planning_result.report_artifact_id.clone(),
        ),
        other => panic!("expected OrchestratorPlan result_ref, got {other:?}"),
    };

    // Corrupt ONLY the older completed command's persisted artifact semantically on the typed
    // `PersistedPlanningCallArtifact` (`verified_response.text`) so JSON schema deserialization
    // still succeeds while full `BoundedAdvisoryResponse` verification fails with the exact
    // umbrella error, embedding `SECRET_SENTINEL` to prove raw artifact text never leaks.
    let old_artifact_path =
        planning_artifacts_dir(&fx.plans_dir).join(format!("{old_artifact_id}.json"));
    let valid_old_artifact: PersistedPlanningCallArtifact =
        serde_json::from_slice(&fs::read(&old_artifact_path).expect("read old artifact"))
            .expect("deserialize valid PersistedPlanningCallArtifact");
    let mut corrupted_old_artifact = valid_old_artifact.clone();
    corrupted_old_artifact
        .verified_response
        .as_mut()
        .expect("completed artifact has verified_response")
        .text = format!("{{\"corrupted_advisory\":\"{SECRET_SENTINEL}\"}}");
    fs::write(
        &old_artifact_path,
        serde_json::to_vec_pretty(&corrupted_old_artifact).unwrap(),
    )
    .expect("overwrite corrupted historical artifact");
    fs::set_permissions(&old_artifact_path, fs::Permissions::from_mode(0o600)).unwrap();

    let persisted_corrupted_bytes =
        fs::read(&old_artifact_path).expect("read corrupted historical artifact bytes");
    assert!(
        String::from_utf8_lossy(&persisted_corrupted_bytes).contains(SECRET_SENTINEL),
        "corrupted artifact on disk must contain SECRET_SENTINEL before status checks"
    );
    let reparsed_corrupted_artifact: PersistedPlanningCallArtifact =
        serde_json::from_slice(&persisted_corrupted_bytes)
            .expect("corrupted artifact deserializes as PersistedPlanningCallArtifact");
    assert_eq!(reparsed_corrupted_artifact.artifact_id, old_artifact_id);

    let strict_err = fx
        .gateway()
        .planning_command(&PlanningCommandParams {
            command_id: cmd_old.command_id.clone(),
        })
        .expect_err(
            "strict planning_command must reject corrupted BoundedAdvisoryResponse artifact",
        );
    assert_eq!(strict_err.code, NativeErrorCode::ProtocolError);
    assert_eq!(
        strict_err.message,
        "OrchestratorOutput artifact failed full BoundedAdvisoryResponse verification under max_output_bytes_per_call"
    );
    assert!(!strict_err.message.contains(SECRET_SENTINEL));

    // Enqueue a Queued sibling (index 1), then enqueue and execute a healthy current Completed
    // command (index 2).
    let cmd_queued = cmd_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(
            OrchestrateWorkBriefPlanRequest {
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_queued.job_id.clone(),
                request_key: "cmd_hist_queued_sibling".to_string(),
                expected_ledger_revision: None,
                difficulty: None,
                architect_difficulty: None,
                plan_id: None,
                expected_plan_revision: 0,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                current_time_ms: now_ms + 10,
            },
        ))
        .expect("enqueue cmd_queued");

    let current_plan_resp = json!({
        "outcome": "plan",
        "schema_version": 1,
        "summary": "Healthy current exporter plan",
        "tasks": [{
            "task_id": "task_current_exporter",
            "title": "Healthy exporter update",
            "description": "Implement exporter changes in src/exporter.rs",
            "role": "coder",
            "difficulty": "medium",
            "depends_on": [],
            "allowed_write_paths": ["src/exporter.rs"],
            "owned_interfaces": ["iface_current_exporter"],
            "consumed_interfaces": []
        }],
        "check_suggestions": [],
        "unconfirmed_proposal_notes": [],
        "resolved_open_questions": [],
        "deferred_proposals": []
    });
    fs::write(
        &fx.response_json_path,
        serde_json::to_vec(&current_plan_resp).unwrap(),
    )
    .unwrap();

    let cmd_current_healthy = cmd_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(
            OrchestrateWorkBriefPlanRequest {
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_current.job_id.clone(),
                request_key: "cmd_hist_current_healthy".to_string(),
                expected_ledger_revision: None,
                difficulty: None,
                architect_difficulty: None,
                plan_id: None,
                expected_plan_revision: 0,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                current_time_ms: now_ms + 20,
            },
        ))
        .expect("enqueue cmd_current_healthy");
    driver
        .execute_command(&fx.binding_id, THREAD_UUID, &cmd_current_healthy.command_id)
        .await
        .expect("execute cmd_current_healthy");

    // Stage A: `[cmd_old (corrupted), cmd_queued, cmd_current_healthy]`.
    // Verify `{}`, `limit: 20`, and `limit: 1` over PairedMcpServer before adding an Unknown sibling:
    // `limit: 1` selects `cmd_current_healthy` (omitting `cmd_old`) while keeping overall
    // `historical_integrity_attention` visible and `pending_reconciliation == false`!
    fx.reset_worker_invocations();
    let stage_a_plans = snapshot_tree_bytes(&fx.plans_dir);
    let (stage_a_journal, stage_a_warnings) = read_journal_records_readonly(&fx.plans_dir).unwrap();
    assert!(stage_a_warnings.is_empty());
    let stage_a_direct = fx
        .gateway()
        .coworker_status(&CoworkerStatusParams::default())
        .expect("stage_a direct gateway coworker_status");
    assert_tree_bytes_identical(
        &stage_a_plans,
        &snapshot_tree_bytes(&fx.plans_dir),
        "stage_a_direct_gateway_plans",
    );
    let mcp = fx.enrolled_mcp();

    let (stage_a_default, _, _) =
        invoke_status_and_verify_wire_envelope(&mcp, &fx.binding_id, json!({})).await;
    let (stage_a_20, _, _) =
        invoke_status_and_verify_wire_envelope(&mcp, &fx.binding_id, json!({ "limit": 20 })).await;
    let (stage_a_1, _, _) =
        invoke_status_and_verify_wire_envelope(&mcp, &fx.binding_id, json!({ "limit": 1 })).await;

    assert_eq!(stage_a_default, stage_a_direct);
    assert_eq!(stage_a_default, stage_a_20);
    assert_eq!(stage_a_default.commands.len(), 3);
    assert_eq!(stage_a_default.commands_bounds.total_count, 3);
    assert_eq!(stage_a_default.commands_bounds.returned_count, 3);
    assert_eq!(stage_a_default.commands_bounds.omitted_by_limit_count, 0);
    assert!(!stage_a_default.has_more_commands);
    assert!(stage_a_default.historical_integrity_attention);
    assert_eq!(stage_a_default.degraded_completed_command_count, 1);
    assert!(!stage_a_default.pending_reconciliation);
    assert_eq!(stage_a_default.pending_reconciliation_command_count, 0);

    assert_eq!(stage_a_1.commands.len(), 1);
    assert_eq!(
        stage_a_1.commands[0].command_id,
        cmd_current_healthy.command_id
    );
    assert!(stage_a_1.commands[0].verification_issue.is_none());
    assert!(stage_a_1.commands[0].result_summary.is_some());
    assert_eq!(stage_a_1.commands_bounds.total_count, 3);
    assert_eq!(stage_a_1.commands_bounds.returned_count, 1);
    assert_eq!(stage_a_1.commands_bounds.omitted_by_limit_count, 2);
    assert!(stage_a_1.has_more_commands);
    assert!(stage_a_1.historical_integrity_attention);
    assert_eq!(stage_a_1.degraded_completed_command_count, 1);
    assert!(!stage_a_1.pending_reconciliation);
    assert_eq!(stage_a_1.pending_reconciliation_command_count, 0);
    let _ = assert_plans_tree_after_paired_mcp_calls(
        &fx.plans_dir,
        &stage_a_plans,
        &stage_a_journal,
        &fx.binding_id,
        THREAD_UUID,
        &[
            ExpectedPairedMcpJournalCall::status_ok(),
            ExpectedPairedMcpJournalCall::status_ok(),
            ExpectedPairedMcpJournalCall::status_ok(),
        ],
        "stage_a_paired_mcp",
    );
    assert_eq!(fx.worker_invocation_count(), 0);
    let stage_a_coherence_plans = fx.root.join("plans-stage-a-coherence");
    copy_private_tree(&fx.plans_dir, &stage_a_coherence_plans);

    // Stage B: Add an `Unknown` (`pending_reconciliation == true`) sibling so the binding holds
    // `[cmd_old (corrupted), cmd_queued, cmd_current_healthy, cmd_unknown]`.
    let dep_blobs = CoworkerPlanningService::compute_dependency_file_blobs(
        &fx.project_base,
        &["src/lib.rs".to_string()],
    )
    .expect("dep blobs");
    let policy_digest = default_planning_policy().digest().expect("policy digest");
    let cmd_unknown = cmd_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_architect(
            RunArchitectAdviceRequest {
                binding_id: fx.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                request_key: "cmd_hist_unknown_sibling".to_string(),
                expected_ledger_revision: None,
                difficulty: None,
                context_dependencies: ArchitectContextDependencies {
                    project_base: fx.project_base.clone(),
                    work_brief_digest: job_current.work_brief_digest.clone(),
                    plan_digest: None,
                    policy_digest,
                    dependency_file_blobs: dep_blobs,
                    interface_ownership_digests: BTreeMap::new(),
                    accepted_predecessor_trees: BTreeMap::new(),
                },
                question_summary: "Verify exporter trait boundary".to_string(),
                prior_advice_context: Vec::new(),
                accept_into_brief: false,
                current_time_ms: now_ms + 30,
            },
        ))
        .expect("enqueue cmd_unknown");
    let unknown_guard = cmd_store
        .ownership_store()
        .try_acquire_job(
            &cmd_unknown.ownership_key().expect("ownership key"),
            threadlane_native::current_wall_clock_ms().expect("clock"),
        )
        .expect("acquire ownership")
        .into_guard()
        .expect("ownership guard");
    cmd_store
        .claim_running(
            &unknown_guard,
            &fx.binding_id,
            THREAD_UUID,
            &cmd_unknown.command_id,
            None,
        )
        .expect("claim_running cmd_unknown");
    cmd_store
        .record_unknown(
            &unknown_guard,
            &fx.binding_id,
            THREAD_UUID,
            &cmd_unknown.command_id,
            "Interrupted before provider receipt",
            None,
        )
        .expect("record_unknown cmd_unknown");
    drop(unknown_guard);

    // Snapshot all stored bytes, ledger revisions, grant usage/reservations, and provider call
    // counts BEFORE exercising `coworker_status` and strict read verifiers.
    fx.reset_worker_invocations();
    let plans_before = snapshot_tree_bytes(&fx.plans_dir);
    let (journal_before, journal_warnings_before) =
        read_journal_records_readonly(&fx.plans_dir).unwrap();
    assert!(journal_warnings_before.is_empty());
    let reg_before = snapshot_tree_bytes(&fx.registry_dir);
    let auth_before = snapshot_tree_bytes(&fx.authority_dir);
    let sess_before = snapshot_tree_bytes(&fx.sessions_dir);
    let wf_before = wf_store
        .read_binding_ledger(&fx.binding_id)
        .expect("wf_before");
    let cmd_before = cmd_store
        .read_binding_ledger(&fx.binding_id)
        .expect("cmd_before");

    let (status_default, _, wire_default) =
        invoke_status_and_verify_wire_envelope(&mcp, &fx.binding_id, json!({})).await;
    let (status_20, _, wire_20) =
        invoke_status_and_verify_wire_envelope(&mcp, &fx.binding_id, json!({ "limit": 20 })).await;
    let (status_2, _, wire_2) =
        invoke_status_and_verify_wire_envelope(&mcp, &fx.binding_id, json!({ "limit": 2 })).await;
    let (status_1, _, wire_1) =
        invoke_status_and_verify_wire_envelope(&mcp, &fx.binding_id, json!({ "limit": 1 })).await;

    assert!(wire_default <= MAX_MCP_RESPONSE_BYTES);
    assert!(wire_20 <= MAX_MCP_RESPONSE_BYTES);
    assert!(wire_2 <= MAX_MCP_RESPONSE_BYTES);
    assert!(wire_1 <= MAX_MCP_RESPONSE_BYTES);
    assert_eq!(status_default, status_20);
    let plans_before_direct_gw = snapshot_tree_bytes(&fx.plans_dir);
    assert_eq!(
        fx.gateway()
            .coworker_status(&CoworkerStatusParams::default())
            .expect("direct gateway coworker_status"),
        status_default
    );
    assert_tree_bytes_identical(
        &plans_before_direct_gw,
        &snapshot_tree_bytes(&fx.plans_dir),
        "stage_b_direct_gateway_plans",
    );

    // Verify full 4-command accounting, degraded row isolation, and strict separation from Unknown!
    assert_eq!(
        status_default.effective_limit,
        DEFAULT_COWORKER_STATUS_LIMIT
    );
    assert_eq!(status_default.commands.len(), 4);
    assert_eq!(status_default.commands_bounds.total_count, 4);
    assert_eq!(status_default.commands_bounds.returned_count, 4);
    assert_eq!(status_default.commands_bounds.omitted_by_limit_count, 0);
    assert_eq!(
        status_default.commands_bounds.omitted_by_byte_budget_count,
        0
    );
    assert!(!status_default.has_more_commands);
    assert!(status_default.historical_integrity_attention);
    assert_eq!(status_default.degraded_completed_command_count, 1);
    assert!(status_default.pending_reconciliation);
    assert_eq!(status_default.pending_reconciliation_command_count, 1);
    assert_eq!(status_default.unknown_effect_reservation_count, 0);
    assert_eq!(status_default.unresolved_job_outbox_count, 0);

    let row_old = &status_default.commands[0];
    assert_eq!(row_old.command_id, old_completed_record.command_id);
    assert_eq!(row_old.command_key, old_completed_record.command_key);
    assert_eq!(
        row_old.service_request_key,
        old_completed_record.service_request_key
    );
    assert_eq!(row_old.binding_id, fx.binding_id);
    assert_eq!(row_old.thread_uuid, THREAD_UUID);
    assert_eq!(row_old.grant_id, grant_id);
    assert_eq!(row_old.context_digest, old_completed_record.context_digest);
    assert_eq!(
        row_old.request_body_digest,
        old_completed_record.request_body_digest
    );
    assert_eq!(row_old.command_digest, old_completed_record.command_digest);
    assert_eq!(row_old.status, PlanningCommandStatus::Completed);
    assert!(!row_old.pending_reconciliation);
    assert_eq!(row_old.executable, Some(false));
    assert!(row_old.result_summary.is_none());
    assert!(row_old.current_plan_status.is_none());
    let issue = row_old
        .verification_issue
        .as_ref()
        .expect("degraded historical row must carry verification_issue");
    assert!(issue.status_diagnostic_only);
    assert!(!issue.grants_execution_authority);
    assert_eq!(
        issue.evidence_status,
        PlanningCommandEvidenceStatus::VerificationFailed
    );
    assert_eq!(
        issue.kind,
        PlanningCommandVerificationIssueKind::BoundedAdvisoryResponseVerificationFailed
    );
    assert_eq!(issue.error_code, NativeErrorCode::ProtocolError);
    assert_eq!(issue.reservation_id, old_reservation_id);
    assert_eq!(issue.report_artifact_id, old_artifact_id);
    assert!(!issue.message.contains(SECRET_SENTINEL));
    assert!(!issue.message.contains(&*fx.root.to_string_lossy()));

    let row_queued = &status_default.commands[1];
    assert_eq!(row_queued.command_id, cmd_queued.command_id);
    assert_eq!(row_queued.status, PlanningCommandStatus::Queued);
    assert!(!row_queued.pending_reconciliation);
    assert!(row_queued.verification_issue.is_none());
    assert!(row_queued.result_summary.is_none());

    let row_healthy = &status_default.commands[2];
    assert_eq!(row_healthy.command_id, cmd_current_healthy.command_id);
    assert_eq!(row_healthy.status, PlanningCommandStatus::Completed);
    assert!(!row_healthy.pending_reconciliation);
    assert!(row_healthy.verification_issue.is_none());
    assert_eq!(row_healthy.current_plan_status, Some(PlanStatus::Published));
    assert_eq!(row_healthy.executable, None);
    let healthy_res = row_healthy
        .result_summary
        .as_ref()
        .expect("healthy completed row must expose bounded result_summary");
    assert!(healthy_res.status_projection_only);
    assert!(!healthy_res.grants_execution_authority);
    assert!(healthy_res.artifact_verified);
    assert_eq!(
        healthy_res.kind,
        CoworkerStatusCommandResultKind::OrchestratorPlan
    );

    let row_unknown = &status_default.commands[3];
    assert_eq!(row_unknown.command_id, cmd_unknown.command_id);
    assert_eq!(row_unknown.status, PlanningCommandStatus::Unknown);
    assert!(row_unknown.pending_reconciliation);
    assert!(row_unknown.verification_issue.is_none());
    assert!(row_unknown.result_summary.is_none());

    // At `limit: 2`, `cmd_old` and `cmd_queued` are omitted, returning `[cmd_current_healthy, cmd_unknown]`
    // while scope-wide integrity attention remains visible!
    assert_eq!(status_2.commands.len(), 2);
    assert_eq!(
        status_2.commands[0].command_id,
        cmd_current_healthy.command_id
    );
    assert_eq!(status_2.commands[1].command_id, cmd_unknown.command_id);
    assert_eq!(status_2.commands_bounds.total_count, 4);
    assert_eq!(status_2.commands_bounds.omitted_by_limit_count, 2);
    assert!(status_2.has_more_commands);
    assert!(status_2.historical_integrity_attention);
    assert_eq!(status_2.degraded_completed_command_count, 1);
    assert_eq!(status_2.pending_reconciliation_command_count, 1);

    // At `limit: 1`, `cmd_old` is omitted and `cmd_unknown` + latest healthy job row are returned,
    // while scope-wide `historical_integrity_attention` and `degraded_completed_command_count` stay visible!
    assert_eq!(status_1.effective_limit, 1);
    assert_eq!(status_1.commands.len(), 1);
    assert_eq!(status_1.commands[0].command_id, cmd_unknown.command_id);
    assert!(status_1.commands[0].verification_issue.is_none());
    assert_eq!(status_1.jobs.len(), 1);
    assert_eq!(status_1.jobs[0].job_id, job_current.job_id);
    assert_eq!(status_1.commands_bounds.total_count, 4);
    assert_eq!(status_1.commands_bounds.returned_count, 1);
    assert_eq!(status_1.commands_bounds.omitted_by_limit_count, 3);
    assert!(status_1.has_more_commands);
    assert!(status_1.historical_integrity_attention);
    assert_eq!(status_1.degraded_completed_command_count, 1);
    assert!(status_1.pending_reconciliation);
    assert_eq!(status_1.pending_reconciliation_command_count, 1);

    // Strict MCP `planning_command`, strict `get_planning_command_summary_readonly`, strict
    // `list_planning_command_summaries_readonly`, and `driver.execute_command` MUST still reject
    // the corrupted artifact, while `planning_command` on `cmd_current_healthy` succeeds!
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "planning_command",
            json!({ "command_id": cmd_old.command_id }),
        )
        .await,
        "protocol_error"
    );
    let healthy_detail_res = mcp
        .invoke_tool_json(
            "planning_command",
            json!({ "command_id": cmd_current_healthy.command_id }),
            None,
        )
        .await
        .expect("planning_command on healthy current command");
    let healthy_detail: PlanningCommandDetailResponse = serde_json::from_value(
        healthy_detail_res
            .structured_content
            .expect("structured_content")["data"]
            .clone(),
    )
    .expect("deserialize PlanningCommandDetailResponse");
    assert_eq!(healthy_detail.status, PlanningCommandStatus::Completed);
    assert!(healthy_detail.orchestrator_plan_summary.is_some());
    let returned_job_id = healthy_detail.job_id.as_deref();
    let returned_plan_id = healthy_detail
        .orchestrator_plan_summary
        .as_ref()
        .map(|summary| summary.plan_id.as_str());
    assert!(returned_job_id.is_some());
    assert!(returned_plan_id.is_some());

    let plans_after_mcp = assert_plans_tree_after_paired_mcp_calls(
        &fx.plans_dir,
        &plans_before,
        &journal_before,
        &fx.binding_id,
        THREAD_UUID,
        &[
            ExpectedPairedMcpJournalCall::status_ok(),
            ExpectedPairedMcpJournalCall::status_ok(),
            ExpectedPairedMcpJournalCall::status_ok(),
            ExpectedPairedMcpJournalCall::status_ok(),
            ExpectedPairedMcpJournalCall {
                tool_name: "planning_command",
                finish_state: ObservationCallState::Failed,
                finish_job_id: None,
                finish_plan_id: None,
            },
            ExpectedPairedMcpJournalCall {
                tool_name: "planning_command",
                finish_state: ObservationCallState::Completed,
                finish_job_id: returned_job_id,
                finish_plan_id: returned_plan_id,
            },
        ],
        "stage_b_paired_mcp",
    );

    assert_eq!(
        get_planning_command_summary_readonly(
            &fx.plans_dir,
            &fx.registry_dir,
            &fx.binding_id,
            THREAD_UUID,
            &cmd_old.command_id,
        )
        .expect_err("strict get_planning_command_summary_readonly must reject corrupted artifact")
        .code,
        NativeErrorCode::ProtocolError
    );
    assert_eq!(
        list_planning_command_summaries_readonly(
            &fx.plans_dir,
            &fx.registry_dir,
            &fx.binding_id,
            THREAD_UUID,
            1,
            None,
        )
        .expect_err(
            "strict list_planning_command_summaries_readonly must reject corrupted artifact",
        )
        .code,
        NativeErrorCode::ProtocolError
    );
    assert_eq!(
        driver
            .execute_command(&fx.binding_id, THREAD_UUID, &cmd_old.command_id)
            .await
            .expect_err("driver.execute_command must reject corrupted completed artifact")
            .code,
        NativeErrorCode::ProtocolError
    );

    // Wrong thread scope still fails closed on both gateway and status-only driver reader.
    assert_eq!(
        fx.gateway_for_scope(&fx.binding_id, OTHER_THREAD_UUID)
            .coworker_status(&CoworkerStatusParams::default())
            .expect_err("wrong thread_uuid must fail closed")
            .code,
        NativeErrorCode::SecurityViolation
    );
    assert_eq!(
        read_planning_commands_for_status_readonly(
            &fx.plans_dir,
            &fx.registry_dir,
            &fx.binding_id,
            OTHER_THREAD_UUID,
        )
        .expect_err("status-only reader wrong thread_uuid must fail closed")
        .code,
        NativeErrorCode::SecurityViolation
    );

    // Byte-for-byte immutability proof across all stores, revisions, receipts, and provider calls!
    assert_tree_bytes_identical(
        &plans_after_mcp,
        &snapshot_tree_bytes(&fx.plans_dir),
        "stage_b_direct_strict_and_scope_plans",
    );
    assert_tree_bytes_identical(
        &reg_before,
        &snapshot_tree_bytes(&fx.registry_dir),
        "stage_b_registry",
    );
    assert_tree_bytes_identical(
        &auth_before,
        &snapshot_tree_bytes(&fx.authority_dir),
        "stage_b_authority",
    );
    assert_tree_bytes_identical(
        &sess_before,
        &snapshot_tree_bytes(&fx.sessions_dir),
        "stage_b_sessions",
    );
    assert_eq!(
        wf_store
            .read_binding_ledger(&fx.binding_id)
            .expect("wf_after"),
        wf_before
    );
    assert_eq!(
        cmd_store
            .read_binding_ledger(&fx.binding_id)
            .expect("cmd_after"),
        cmd_before
    );
    assert_eq!(fx.worker_invocation_count(), 0);

    // Isolated scratch copies prove shared ledger corruption, record digest tampering, and unsafe
    // file permissions still fail closed, while a missing artifact surfaces `Unavailable`.
    let cmd_ledger_rel = cmd_store
        .ledger_path_for_binding(&fx.binding_id)
        .unwrap()
        .strip_prefix(&fx.plans_dir)
        .unwrap()
        .to_path_buf();

    // (a) Corrupt shared command ledger JSON -> fail closed (`ProtocolError`).
    let corrupt_ledger_plans = fx.root.join("plans-corrupt-cmd-ledger");
    copy_private_tree(&fx.plans_dir, &corrupt_ledger_plans);
    let corrupt_cmd_ledger_path = corrupt_ledger_plans.join(&cmd_ledger_rel);
    fs::write(&corrupt_cmd_ledger_path, b"{corrupt-shared-ledger").unwrap();
    fs::set_permissions(&corrupt_cmd_ledger_path, fs::Permissions::from_mode(0o600)).unwrap();
    let gw_corrupt_ledger = CoworkerPlanningGateway::new(
        &fx.state_dir,
        &fx.registry_dir,
        &corrupt_ledger_plans,
        &fx.authority_dir,
        &fx.sessions_dir,
        &fx.binding_id,
        THREAD_UUID,
    )
    .unwrap();
    assert_eq!(
        gw_corrupt_ledger
            .coworker_status(&CoworkerStatusParams::default())
            .expect_err("corrupt shared command ledger must fail closed")
            .code,
        NativeErrorCode::ProtocolError
    );

    // (b) Tampered `command_digest` on `Completed` command record -> fail closed (`ProtocolError`).
    let tampered_record_plans = fx.root.join("plans-tampered-cmd-record");
    copy_private_tree(&fx.plans_dir, &tampered_record_plans);
    let tampered_cmd_ledger_path = tampered_record_plans.join(&cmd_ledger_rel);
    let mut tampered_ledger_val: Value =
        serde_json::from_slice(&fs::read(&tampered_cmd_ledger_path).unwrap()).unwrap();
    tampered_ledger_val["commands"][0]["command_digest"] = json!("0".repeat(64));
    fs::write(
        &tampered_cmd_ledger_path,
        serde_json::to_vec_pretty(&tampered_ledger_val).unwrap(),
    )
    .unwrap();
    fs::set_permissions(&tampered_cmd_ledger_path, fs::Permissions::from_mode(0o600)).unwrap();
    let gw_tampered_record = CoworkerPlanningGateway::new(
        &fx.state_dir,
        &fx.registry_dir,
        &tampered_record_plans,
        &fx.authority_dir,
        &fx.sessions_dir,
        &fx.binding_id,
        THREAD_UUID,
    )
    .unwrap();
    assert_eq!(
        gw_tampered_record
            .coworker_status(&CoworkerStatusParams::default())
            .expect_err("tampered command_digest must fail closed")
            .code,
        NativeErrorCode::ProtocolError
    );

    // (c) Insecure `0644` permissions on persisted artifact file -> fail closed (`SecurityViolation`).
    let unsafe_perm_plans = fx.root.join("plans-unsafe-artifact-perms");
    copy_private_tree(&fx.plans_dir, &unsafe_perm_plans);
    let unsafe_art_path =
        planning_artifacts_dir(&unsafe_perm_plans).join(format!("{old_artifact_id}.json"));
    fs::set_permissions(&unsafe_art_path, fs::Permissions::from_mode(0o644)).unwrap();
    let gw_unsafe_perm = CoworkerPlanningGateway::new(
        &fx.state_dir,
        &fx.registry_dir,
        &unsafe_perm_plans,
        &fx.authority_dir,
        &fx.sessions_dir,
        &fx.binding_id,
        THREAD_UUID,
    )
    .unwrap();
    assert_eq!(
        gw_unsafe_perm
            .coworker_status(&CoworkerStatusParams::default())
            .expect_err("insecure 0644 artifact permissions must fail closed")
            .code,
        NativeErrorCode::SecurityViolation
    );

    // (d) Missing persisted artifact file -> isolated `Unavailable` (`ArtifactMissing`) observation.
    let missing_art_plans = fx.root.join("plans-missing-artifact");
    copy_private_tree(&fx.plans_dir, &missing_art_plans);
    fs::remove_file(
        planning_artifacts_dir(&missing_art_plans).join(format!("{old_artifact_id}.json")),
    )
    .unwrap();
    let gw_missing_art = CoworkerPlanningGateway::new(
        &fx.state_dir,
        &fx.registry_dir,
        &missing_art_plans,
        &fx.authority_dir,
        &fx.sessions_dir,
        &fx.binding_id,
        THREAD_UUID,
    )
    .unwrap();
    let missing_status = gw_missing_art
        .coworker_status(&CoworkerStatusParams::default())
        .expect("missing artifact on old completed command must isolate in coworker_status");
    assert!(missing_status.historical_integrity_attention);
    assert_eq!(missing_status.degraded_completed_command_count, 1);
    assert_eq!(missing_status.pending_reconciliation_command_count, 1);
    let missing_issue = missing_status.commands[0]
        .verification_issue
        .as_ref()
        .expect("missing artifact must produce verification_issue");
    assert_eq!(
        missing_issue.evidence_status,
        PlanningCommandEvidenceStatus::Unavailable
    );
    assert_eq!(
        missing_issue.kind,
        PlanningCommandVerificationIssueKind::ArtifactMissing
    );
    assert!(missing_status.commands[0].result_summary.is_none());
    assert_eq!(missing_status.commands[0].executable, Some(false));

    // (e) Single-ledger-snapshot coherence, append order, and `Running` -> `Unknown` projection
    // without global test serialization: an enqueue after the initial ledger snapshot advances the
    // on-disk command ledger to `R + 1`, while the in-flight status read still returns revision `R`
    // with ONLY revision `R`'s 3 rows (never tagging newer rows with an older revision).
    let coherence_wf_store =
        CoworkerWorkflowStore::new(&stage_a_coherence_plans).expect("coherence_wf_store");
    let coherence_cmds_dir = planning_commands_dir(&stage_a_coherence_plans);
    let coherence_arts_dir = planning_artifacts_dir(&stage_a_coherence_plans);
    let coherence_cmd_store = PlanningCommandStore::open_existing(
        &coherence_cmds_dir,
        coherence_wf_store,
        &fx.registry_dir,
    )
    .expect("coherence_cmd_store")
    .with_artifacts_dir(&coherence_arts_dir)
    .expect("coherence artifacts_dir");
    let initial_coherence_rev = coherence_cmd_store
        .read_binding_ledger(&fx.binding_id)
        .expect("initial coherence ledger")
        .revision;
    let coherence_dep_blobs = CoworkerPlanningService::compute_dependency_file_blobs(
        &fx.project_base,
        &["src/lib.rs".to_string()],
    )
    .expect("coherence dep blobs");
    let coherence_enqueue_req =
        EnqueuePlanningCommandRequest::from_architect(RunArchitectAdviceRequest {
            binding_id: fx.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant_id.clone(),
            request_key: "cmd_hist_coherence_concurrent".to_string(),
            expected_ledger_revision: None,
            difficulty: None,
            context_dependencies: ArchitectContextDependencies {
                project_base: fx.project_base.clone(),
                work_brief_digest: job_current.work_brief_digest.clone(),
                plan_digest: None,
                policy_digest: default_planning_policy().digest().expect("policy digest"),
                dependency_file_blobs: coherence_dep_blobs,
                interface_ownership_digests: BTreeMap::new(),
                accepted_predecessor_trees: BTreeMap::new(),
            },
            question_summary: "Verify single-snapshot status coherence".to_string(),
            prior_advice_context: Vec::new(),
            accept_into_brief: false,
            current_time_ms: now_ms + 40,
        });
    let mut enqueued_mid_read = None;
    let coherent_report = read_planning_commands_for_status_readonly_with_hooks(
        &stage_a_coherence_plans,
        &fx.registry_dir,
        &fx.binding_id,
        THREAD_UUID,
        |observed_rev, observed_len| {
            assert_eq!(observed_rev, initial_coherence_rev);
            assert_eq!(observed_len, 3);
            let cmd_concurrent = coherence_cmd_store
                .enqueue_command(&coherence_enqueue_req)
                .expect("enqueue concurrent command during status read");
            let bumped = coherence_cmd_store
                .read_binding_ledger(&fx.binding_id)
                .expect("read bumped ledger");
            assert_eq!(bumped.revision, initial_coherence_rev + 1);
            assert_eq!(bumped.commands.len(), 4);
            enqueued_mid_read = Some(cmd_concurrent);
            Ok(())
        },
        |_, _| Ok(()),
    )
    .expect("coherent status read across concurrent enqueue");
    let cmd_concurrent = enqueued_mid_read.expect("hook enqueued concurrent command");
    assert_eq!(coherent_report.ledger_revision, initial_coherence_rev);
    assert_eq!(coherent_report.commands.len(), 3);
    assert_eq!(
        coherent_report.commands[0].summary.command_id,
        cmd_old.command_id
    );
    assert!(coherent_report.commands[0].verification_issue.is_some());
    assert_eq!(
        coherent_report.commands[1].summary.command_id,
        cmd_queued.command_id
    );
    assert_eq!(
        coherent_report.commands[2].summary.command_id,
        cmd_current_healthy.command_id
    );
    assert!(coherent_report.commands[2].verification_issue.is_none());

    let post_enqueue_report = read_planning_commands_for_status_readonly(
        &stage_a_coherence_plans,
        &fx.registry_dir,
        &fx.binding_id,
        THREAD_UUID,
    )
    .expect("post-enqueue status read");
    assert_eq!(
        post_enqueue_report.ledger_revision,
        initial_coherence_rev + 1
    );
    assert_eq!(post_enqueue_report.commands.len(), 4);
    assert_eq!(
        post_enqueue_report.commands[3].summary.command_id,
        cmd_concurrent.command_id
    );
    assert_eq!(
        post_enqueue_report.commands[3].summary.status,
        PlanningCommandStatus::Queued
    );

    let live_running_guard = coherence_cmd_store
        .ownership_store()
        .try_acquire_job(
            &cmd_concurrent.ownership_key().expect("ownership key"),
            threadlane_native::current_wall_clock_ms().expect("clock"),
        )
        .expect("acquire concurrent ownership")
        .into_guard()
        .expect("concurrent ownership guard");
    coherence_cmd_store
        .claim_running(
            &live_running_guard,
            &fx.binding_id,
            THREAD_UUID,
            &cmd_concurrent.command_id,
            None,
        )
        .expect("claim_running cmd_concurrent");
    let running_report = read_planning_commands_for_status_readonly(
        &stage_a_coherence_plans,
        &fx.registry_dir,
        &fx.binding_id,
        THREAD_UUID,
    )
    .expect("status read while Running owner is active");
    assert_eq!(running_report.ledger_revision, initial_coherence_rev + 2);
    assert_eq!(
        running_report.commands[3].summary.status,
        PlanningCommandStatus::Running
    );
    assert!(!running_report.commands[3].summary.pending_reconciliation);
    assert!(running_report.commands[3].summary.owner_summary.is_some());
    drop(live_running_guard);

    let abandoned_report = read_planning_commands_for_status_readonly(
        &stage_a_coherence_plans,
        &fx.registry_dir,
        &fx.binding_id,
        THREAD_UUID,
    )
    .expect("status read after Running owner dropped");
    assert_eq!(abandoned_report.ledger_revision, initial_coherence_rev + 2);
    assert_eq!(
        abandoned_report.commands[3].summary.status,
        PlanningCommandStatus::Unknown
    );
    assert!(abandoned_report.commands[3].summary.pending_reconciliation);

    // (f) Policy-invalid typed artifact replacement after initial `ProtocolError` failure ->
    // `isolate_completed_artifact_issue_for_status` must enforce the fatal category guard on
    // reread `validate_against_grant` (`SecurityViolation`) and fail closed without leaking secrets
    // or private paths, while strict detail/get/list verifiers remain fail-closed as well.
    const POLICY_SECRET_MODEL_ID: &str = "model-unpermitted-secret-token-771";
    let stored_grant = wf_store
        .read_binding_ledger(&fx.binding_id)
        .expect("read workflow ledger for grant")
        .planning_grants
        .into_iter()
        .find(|g| g.grant_id == grant_id)
        .expect("standing grant exists");
    let mut policy_invalid_artifact = valid_old_artifact.clone();
    policy_invalid_artifact.selected_choice.model_id = POLICY_SECRET_MODEL_ID.to_string();
    policy_invalid_artifact
        .validate(&old_artifact_id)
        .expect("policy_invalid_artifact stays structurally valid");
    let direct_policy_err = policy_invalid_artifact
        .validate_against_grant(&old_artifact_id, &stored_grant)
        .expect_err("unpermitted choice must fail validate_against_grant");
    assert_eq!(direct_policy_err.code, NativeErrorCode::SecurityViolation);
    assert_eq!(
        direct_policy_err.message,
        "persisted planning artifact selected_choice is not permitted by the standing grant ExecutionPolicy"
    );

    let policy_swap_plans = fx.root.join("plans-policy-invalid-swap");
    copy_private_tree(&fx.plans_dir, &policy_swap_plans);
    let policy_swap_art_path =
        planning_artifacts_dir(&policy_swap_plans).join(format!("{old_artifact_id}.json"));
    let policy_invalid_bytes = serde_json::to_vec_pretty(&policy_invalid_artifact).unwrap();
    let _: PersistedPlanningCallArtifact = serde_json::from_slice(&policy_invalid_bytes)
        .expect("policy_invalid_artifact bytes deserialize cleanly");

    let mut saw_initial_protocol_failure = false;
    let swap_err = read_planning_commands_for_status_readonly_with_hooks(
        &policy_swap_plans,
        &fx.registry_dir,
        &fx.binding_id,
        THREAD_UUID,
        |_, _| Ok(()),
        |failed_cmd_id, initial_strict_err| {
            assert_eq!(failed_cmd_id, cmd_old.command_id);
            assert_eq!(initial_strict_err.code, NativeErrorCode::ProtocolError);
            assert_eq!(
                initial_strict_err.message,
                "OrchestratorOutput artifact failed full BoundedAdvisoryResponse verification under max_output_bytes_per_call"
            );
            saw_initial_protocol_failure = true;
            fs::write(&policy_swap_art_path, &policy_invalid_bytes)
                .expect("swap policy-invalid artifact");
            let mode_600 = fs::Permissions::from_mode(0o600);
            fs::set_permissions(&policy_swap_art_path, mode_600).unwrap();
            Ok(())
        },
    )
    .expect_err("policy-invalid artifact must fail closed");
    assert!(saw_initial_protocol_failure);
    assert_eq!(swap_err.code, NativeErrorCode::SecurityViolation);
    assert_eq!(
        swap_err.message,
        "persisted planning artifact selected_choice is not permitted by the standing grant ExecutionPolicy"
    );
    let root_str = fx.root.to_string_lossy();
    let swap_root_str = policy_swap_plans.to_string_lossy();
    assert!(!swap_err.message.contains(POLICY_SECRET_MODEL_ID));
    assert!(!swap_err.message.contains(SECRET_SENTINEL));
    assert!(!swap_err.message.contains(&*root_str));
    assert!(!swap_err.message.contains(&*swap_root_str));

    let gw_policy_swap = CoworkerPlanningGateway::new(
        &fx.state_dir,
        &fx.registry_dir,
        &policy_swap_plans,
        &fx.authority_dir,
        &fx.sessions_dir,
        &fx.binding_id,
        THREAD_UUID,
    )
    .unwrap();
    let gw_swap_err = gw_policy_swap
        .coworker_status(&CoworkerStatusParams::default())
        .expect_err("gateway status must fail closed on policy violation");
    assert_eq!(gw_swap_err.code, NativeErrorCode::SecurityViolation);
    assert!(!gw_swap_err.message.contains(POLICY_SECRET_MODEL_ID));
    assert!(!gw_swap_err.message.contains(&*root_str));

    assert_eq!(
        gw_policy_swap
            .planning_command(&PlanningCommandParams {
                command_id: cmd_old.command_id.clone(),
            })
            .expect_err("strict planning_command must fail closed")
            .code,
        NativeErrorCode::SecurityViolation
    );
    assert_eq!(
        get_planning_command_summary_readonly(
            &policy_swap_plans,
            &fx.registry_dir,
            &fx.binding_id,
            THREAD_UUID,
            &cmd_old.command_id,
        )
        .expect_err("strict get summary must fail closed")
        .code,
        NativeErrorCode::SecurityViolation
    );
    assert_eq!(
        list_planning_command_summaries_readonly(
            &policy_swap_plans,
            &fx.registry_dir,
            &fx.binding_id,
            THREAD_UUID,
            1,
            None,
        )
        .expect_err("strict list summaries must fail closed")
        .code,
        NativeErrorCode::SecurityViolation
    );

    assert_eq!(fx.worker_invocation_count(), 0);
    fx.shutdown().await;
}
