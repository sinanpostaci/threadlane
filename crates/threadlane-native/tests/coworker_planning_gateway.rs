#![cfg(unix)]
#![allow(clippy::expect_used, clippy::unwrap_used)]
//! Behavioral integration tests for [`CoworkerPlanningGateway`] and the six Paired MCP
//! Coworker planning tools (`coworker_status`, `prepare_work`, `consult_advisor`,
//! retained `consult_architect`, `request_technical_plan`, and `planning_command`).

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tempfile::TempDir;
use threadlane::{Supervisor, SupervisorClient, SupervisorConfig, SupervisorHandle};
use threadlane_native::coworker_workflow::{
    ArchitectContextDependencies, ArchitectDecisionStatus, CanonicalProjectBaseRef,
    CoworkerJobOwnershipStore, CoworkerJobPhase, CoworkerPlanningService, CoworkerWorkflowStore,
    CreatePlanningGrantRequest, PlanningCallBudget, PlanningReadOnlyCapabilities,
    RevokePlanningGrantRequest, RunArchitectAdviceRequest, WorkBriefKind,
};
use threadlane_native::{
    current_wall_clock_ms, planning_artifacts_dir, planning_commands_dir, ConsultArchitectParams,
    CoworkerPlanningGateway, CoworkerStatusParams, CoworkerStatusResponse, Difficulty,
    EnqueuePlanningCommandRequest, ExecutionPolicy, NativeErrorCode, OperatorEnrollRequest,
    PairedMcpServer, PairingStore, PlanStatus, PlanningCommandDetailResponse,
    PlanningCommandResultRef, PlanningCommandStartStatus, PlanningCommandStatus,
    PlanningCommandStore, PlanningCommandSubmissionResponse, PolicyAllowRule, PrepareWorkResponse,
    RegistryStore, ToolAdapter, WorkflowProfile, WorkflowRole, EXECUTION_POLICY_SCHEMA_VERSION,
};
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::{
    ModelChoice, ModelDefinition, RegistryChange, RoleAssignment, ToolDefinition,
};

const THREAD_UUID: &str = "0196ed00-0000-7000-8000-0000000000a1";
const BINDING_LABEL: &str = "gateway-chat";
const PROFILE_ID: &str = "profile-coworker";
const TOOL_ID: &str = "tool-agy";
const MODEL_ID: &str = "model-argon";
const SECRET_SENTINEL: &str = "THREADLANE_GATEWAY_SECRET_SENTINEL_9b3e7f";

struct GatewayFixture {
    _tmp: TempDir,
    repo_dir: PathBuf,
    project_base: CanonicalProjectBaseRef,
    worker_marker: PathBuf,
    release_path: PathBuf,
    mode_path: PathBuf,
    health_enabled_path: PathBuf,
    state_dir: PathBuf,
    pairing_dir: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    supervisor: Option<SupervisorHandle>,
    binding_id: String,
}

impl GatewayFixture {
    async fn start() -> Self {
        let tmp = TempDir::new().expect("create gateway tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize gateway tempdir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .expect("chmod gateway root 0700");

        let repo_dir = root.join("repo");
        let project_base = init_git_repo(&repo_dir);

        let bin_dir = root.join("bin");
        create_private_dir(&bin_dir);
        let fake_worker_path = bin_dir.join("fake-agy");
        let worker_marker = root.join("worker-invoked");
        let started_path = root.join("worker-started");
        let release_path = root.join("worker-release");
        let mode_path = root.join("worker-mode");
        let health_enabled_path = root.join("worker-health-enabled");
        fs::write(&release_path, b"release\n").expect("open release gate by default");
        fs::write(&mode_path, b"architect\n").expect("default worker mode");
        write_fake_worker(
            &fake_worker_path,
            &worker_marker,
            &started_path,
            &release_path,
            &mode_path,
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
            .expect("start disposable gateway supervisor");
        let supervisor_client =
            SupervisorClient::from_state_dir(&state_dir).expect("connect gateway supervisor");
        let binding = supervisor_client
            .create_binding(THREAD_UUID, BINDING_LABEL)
            .await
            .expect("create gateway control binding");

        let registry = RegistryStore::open(&registry_dir).expect("open gateway registry store");
        let s1 = registry
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: TOOL_ID.to_string(),
                        label: "Antigravity gateway fixture".to_string(),
                        adapter: ToolAdapter::Antigravity,
                        executable_path: Some(fake_worker_path.clone()),
                        enabled: true,
                    },
                },
            )
            .expect("upsert gateway tool");
        let s2 = registry
            .apply(
                s1.revision,
                RegistryChange::UpsertModel {
                    model: ModelDefinition {
                        id: MODEL_ID.to_string(),
                        tool_id: TOOL_ID.to_string(),
                        label: "Gemini 4 Argon High".to_string(),
                        model_arg: "gemini-4-argon-high".to_string(),
                        efforts: vec!["high".to_string(), "medium".to_string()],
                        allow_no_effort: false,
                    },
                },
            )
            .expect("upsert gateway model");
        registry
            .apply(
                s2.revision,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        decision_advisor: None,
                        id: PROFILE_ID.to_string(),
                        name: "Gateway fixture profile".to_string(),
                        assignments: vec![
                            RoleAssignment {
                                role: WorkflowRole::Orchestrator,
                                difficulty: Some(Difficulty::Hard),
                                choice: ModelChoice {
                                    model_id: MODEL_ID.to_string(),
                                    effort: Some("high".to_string()),
                                },
                            },
                            RoleAssignment {
                                role: WorkflowRole::Architect,
                                difficulty: Some(Difficulty::Hard),
                                choice: ModelChoice {
                                    model_id: MODEL_ID.to_string(),
                                    effort: Some("high".to_string()),
                                },
                            },
                            RoleAssignment {
                                role: WorkflowRole::Coder,
                                difficulty: Some(Difficulty::Hard),
                                choice: ModelChoice {
                                    model_id: MODEL_ID.to_string(),
                                    effort: Some("high".to_string()),
                                },
                            },
                        ],
                    },
                },
            )
            .expect("upsert gateway profile");

        let registry_lock = registry_dir.join("registry.lock");
        if registry_lock.exists() {
            fs::remove_file(&registry_lock).expect("remove initial registry.lock");
        }

        Self {
            _tmp: tmp,
            repo_dir,
            project_base,
            worker_marker,
            release_path,
            mode_path,
            health_enabled_path,
            state_dir,
            pairing_dir,
            registry_dir,
            plans_dir,
            authority_dir,
            sessions_dir,
            supervisor: Some(supervisor),
            binding_id: binding.binding_id,
        }
    }

    fn gateway(&self) -> CoworkerPlanningGateway {
        CoworkerPlanningGateway::new(
            &self.state_dir,
            &self.registry_dir,
            &self.plans_dir,
            &self.authority_dir,
            &self.sessions_dir,
            &self.binding_id,
            THREAD_UUID,
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

    fn create_grant(&self, request_key: &str, created_at_ms: u64, expires_at_ms: u64) -> String {
        self.create_grant_with_policy(request_key, default_policy(), created_at_ms, expires_at_ms)
    }

    fn create_grant_with_policy(
        &self,
        request_key: &str,
        policy: ExecutionPolicy,
        created_at_ms: u64,
        expires_at_ms: u64,
    ) -> String {
        let workflow_store =
            CoworkerWorkflowStore::new(&self.plans_dir).expect("open CoworkerWorkflowStore");
        workflow_store
            .create_planning_grant(&CreatePlanningGrantRequest {
                binding_id: self.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                request_key: request_key.to_string(),
                expected_ledger_revision: None,
                project_base: self.project_base.clone(),
                profile_id: PROFILE_ID.to_string(),
                registry_revision: 3,
                policy,
                capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
                budget: PlanningCallBudget {
                    max_total_calls: 6,
                    max_orchestrator_calls: 4,
                    max_architect_calls: 2,
                    max_timeout_ms_per_call: 60_000,
                    max_output_bytes_per_call: 65_536,
                },
                created_at_ms,
                expires_at_ms,
                operator_attested: true,
            })
            .expect("create planning grant")
            .grant_id
    }

    async fn shutdown(&mut self) {
        if let Some(supervisor) = self.supervisor.take() {
            supervisor.shutdown().await;
        }
    }
}

fn default_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
        policy_id: "policy-gateway-test".to_string(),
        allow_rules: vec![PolicyAllowRule {
            tool_id: TOOL_ID.to_string(),
            model_id: MODEL_ID.to_string(),
            efforts: vec!["high".to_string()],
            allow_no_effort: false,
            roles: vec![WorkflowRole::Orchestrator, WorkflowRole::Architect],
        }],
    }
}

fn advisor_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
        policy_id: "policy-gateway-advisor-test".to_string(),
        allow_rules: vec![PolicyAllowRule {
            tool_id: TOOL_ID.to_string(),
            model_id: MODEL_ID.to_string(),
            efforts: vec!["high".to_string()],
            allow_no_effort: false,
            roles: vec![WorkflowRole::Orchestrator, WorkflowRole::Advisor],
        }],
    }
}

fn init_git_repo(repo_dir: &Path) -> CanonicalProjectBaseRef {
    fs::create_dir_all(repo_dir.join("src")).expect("create repo src");
    fs::write(
        repo_dir.join("src/lib.rs"),
        "pub fn gateway_seed() -> u32 { 11 }\n",
    )
    .expect("write src/lib.rs");

    for args in [
        &["init", "--quiet"][..],
        &["config", "user.email", "test@threadlane.local"][..],
        &["config", "user.name", "Threadlane Test"][..],
        &["add", "."][..],
        &["commit", "-m", "initial commit", "--quiet"][..],
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
    fs::create_dir_all(path).expect("create private fixture directory");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
}

fn directory_entries(path: &Path) -> Vec<String> {
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

fn write_fake_worker(
    script_path: &Path,
    counter_path: &Path,
    started_path: &Path,
    release_path: &Path,
    mode_path: &Path,
    health_enabled_path: &Path,
) {
    let script = format!(
        r#"#!/bin/sh
set -eu
if [ -f '{}' ]; then
    health_previous=''
    for health_argument in "$@"; do
        if [ "$health_previous" = '--log-file' ]; then
            printf '%s\n' 'I0101 00:00:01.000000 1 run.go:315] Run: attempt 1 failed (stream reading error: stream idle timeout (5m0s): context deadline exceeded), retrying in 4s.' > "$health_argument"
            break
        fi
        health_previous="$health_argument"
    done
fi
printf 'invoked\n' >> '{}'
printf 'started\n' > '{}'
i=0
while [ ! -f '{}' ] && [ "$i" -lt 200 ]; do
    sleep 0.05
    i=$((i + 1))
done
MODE_FILE='{}'
MODE="architect"
if [ -f "$MODE_FILE" ]; then
    MODE=$(cat "$MODE_FILE" | tr -d '[:space:]')
fi
printf '{{"event":"init","init":{{"session_id":"agy-gw-sess-1","model":"gemini-4-argon-high"}}}}\n'
case "$MODE" in
    orch_clarify)
        RESPONSE_JSON='{{\"outcome\":\"requires_human_clarification\",\"schema_version\":1,\"summary\":\"Need human decision before planning\",\"questions\":[\"Should we support multiple commands per batch?\"],\"referenced_files\":[\"src/lib.rs\"]}}'
        ;;
    orch_ask_then_plan)
        printf 'orch_ask_step_arch\n' > "$MODE_FILE"
        RESPONSE_JSON='{{\"outcome\":\"requires_architect_advice\",\"schema_version\":1,\"summary\":\"Need Architect contract check before finalizing plan\",\"questions\":[{{\"question_id\":\"q_arch_1\",\"question_summary\":\"Confirm boundary contract for src/lib.rs\",\"dependency_files\":[\"src/lib.rs\"],\"interfaces\":[\"iface_gateway\"]}}]}}'
        ;;
    orch_ask_step_arch)
        printf 'orch_plan\n' > "$MODE_FILE"
        RESPONSE_JSON='{{\"schema_version\":1,\"decision_summary\":\"Keep strict planning command boundary\",\"rationale\":\"Verify completed command result against stored advice, reservation, and private artifact.\",\"recommended_write_paths\":[\"src/lib.rs\"],\"interface_contracts\":{{\"iface_gateway\":\"Stable contract v1\"}},\"open_questions\":[]}}'
        ;;
    orch_ask_then_clarify)
        printf 'orch_ask_step_clarify\n' > "$MODE_FILE"
        RESPONSE_JSON='{{\"outcome\":\"requires_architect_advice\",\"schema_version\":1,\"summary\":\"Need Advisor contract check before requesting clarification\",\"questions\":[{{\"question_id\":\"q_arch_1\",\"question_summary\":\"Confirm boundary contract for src/lib.rs\",\"dependency_files\":[\"src/lib.rs\"],\"interfaces\":[\"iface_gateway\"]}}]}}'
        ;;
    orch_ask_step_clarify)
        printf 'orch_clarify\n' > "$MODE_FILE"
        RESPONSE_JSON='{{\"schema_version\":1,\"decision_summary\":\"Keep strict planning command boundary\",\"rationale\":\"Verify completed command result against stored advice, reservation, and private artifact.\",\"recommended_write_paths\":[\"src/lib.rs\"],\"interface_contracts\":{{\"iface_gateway\":\"Stable contract v1\"}},\"open_questions\":[]}}'
        ;;
    orch_plan)
        RESPONSE_JSON='{{\"outcome\":\"plan\",\"schema_version\":1,\"summary\":\"Implement gateway slice\",\"tasks\":[{{\"task_id\":\"task_gateway_core\",\"title\":\"Implement planning gateway\",\"description\":\"Wire MCP tools to sibling threadlane-flow.\",\"role\":\"coder\",\"difficulty\":\"hard\",\"depends_on\":[],\"allowed_write_paths\":[\"src/lib.rs\"],\"owned_interfaces\":[\"iface_gateway\"],\"consumed_interfaces\":[]}}],\"check_suggestions\":[{{\"check_id\":\"cargo_test_gateway\",\"command_summary\":\"cargo test -p threadlane-native --test coworker_planning_gateway\",\"required\":true}}],\"unconfirmed_proposal_notes\":[\"Proposal stays unconfirmed\"],\"resolved_open_questions\":[],\"deferred_proposals\":[]}}'
        ;;
    orch_plan_v2_same_iface)
        RESPONSE_JSON='{{\"outcome\":\"plan\",\"schema_version\":1,\"summary\":\"Implement gateway slice revision 2\",\"tasks\":[{{\"task_id\":\"task_gateway_core\",\"title\":\"Implement planning gateway revision 2\",\"description\":\"Refine MCP tool wiring to sibling threadlane-flow without altering interface contract.\",\"role\":\"coder\",\"difficulty\":\"hard\",\"depends_on\":[],\"allowed_write_paths\":[\"src/lib.rs\"],\"owned_interfaces\":[\"iface_gateway\"],\"consumed_interfaces\":[]}}],\"check_suggestions\":[{{\"check_id\":\"cargo_test_gateway\",\"command_summary\":\"cargo test -p threadlane-native --test coworker_planning_gateway\",\"required\":true}}],\"unconfirmed_proposal_notes\":[\"Proposal stays unconfirmed\"],\"resolved_open_questions\":[],\"deferred_proposals\":[]}}'
        ;;
    orch_plan_v2_changed_iface)
        RESPONSE_JSON='{{\"outcome\":\"plan\",\"schema_version\":1,\"summary\":\"Implement gateway slice with changed interface contract\",\"tasks\":[{{\"task_id\":\"task_gateway_core\",\"title\":\"Implement planning gateway v2 contract\",\"description\":\"Change owned interface contract to iface_gateway_v2.\",\"role\":\"coder\",\"difficulty\":\"hard\",\"depends_on\":[],\"allowed_write_paths\":[\"src/lib.rs\"],\"owned_interfaces\":[\"iface_gateway_v2\"],\"consumed_interfaces\":[]}}],\"check_suggestions\":[{{\"check_id\":\"cargo_test_gateway\",\"command_summary\":\"cargo test -p threadlane-native --test coworker_planning_gateway\",\"required\":true}}],\"unconfirmed_proposal_notes\":[\"Proposal stays unconfirmed\"],\"resolved_open_questions\":[],\"deferred_proposals\":[]}}'
        ;;
    *)
        RESPONSE_JSON='{{\"schema_version\":1,\"decision_summary\":\"Keep strict planning command boundary\",\"rationale\":\"Verify completed command result against stored advice, reservation, and private artifact.\",\"recommended_write_paths\":[\"src/lib.rs\"],\"interface_contracts\":{{\"iface_gateway\":\"Stable contract v1\"}},\"open_questions\":[]}}'
        ;;
esac
printf '{{"event":"result","result":{{"status":"SUCCESS","response":"%s"}}}}\n' "$RESPONSE_JSON"
exit 0
"#,
        health_enabled_path.display(),
        counter_path.display(),
        started_path.display(),
        release_path.display(),
        mode_path.display(),
    );
    fs::write(script_path, script).expect("write fake worker");
    fs::set_permissions(script_path, fs::Permissions::from_mode(0o700))
        .expect("chmod fake worker 0700");
}

async fn invoke_mcp_ok<T: DeserializeOwned>(mcp: &PairedMcpServer, tool: &str, args: Value) -> T {
    let payload = mcp
        .invoke_tool_json(tool, args, None)
        .await
        .unwrap_or_else(|err| panic!("{tool} failed: {:?}", err.structured_content))
        .structured_content
        .expect("structured_content");
    assert_eq!(payload["untrusted_data"], json!(true));
    assert_eq!(payload["tool"], json!(tool));
    let raw = payload.to_string();
    assert!(
        !raw.contains(SECRET_SENTINEL),
        "{tool} response must never leak secret sentinel"
    );
    serde_json::from_value(payload["data"].clone()).expect("deserialize tool data")
}

fn provider_invocation_count(path: &Path) -> usize {
    fs::read_to_string(path)
        .map(|text| text.lines().count())
        .unwrap_or(0)
}

async fn invoke_mcp_err_code(mcp: &PairedMcpServer, tool: &str, args: Value) -> String {
    let err = mcp
        .invoke_tool_json(tool, args, None)
        .await
        .expect_err("expected MCP tool error")
        .structured_content
        .expect("error structured_content");
    err["error"]["code"]
        .as_str()
        .expect("error.code string")
        .to_string()
}

fn format_command_detail_context(detail: &PlanningCommandDetailResponse) -> String {
    format!(
        "command_id={} role={:?} status={:?} pending_reconciliation={} diagnostic={:?} result_ref={:?}",
        detail.command_id,
        detail.role,
        detail.status,
        detail.pending_reconciliation,
        detail.diagnostic,
        detail.result_ref,
    )
}

#[tokio::test]
async fn coworker_planning_tools_advertised_routing_guards_and_readonly_status_absence() {
    let mut fixture = GatewayFixture::start().await;

    // 1. Direct gateway `coworker_status` on empty `plans_dir` is pure read-only and creates nothing.
    let plans_before = directory_entries(&fixture.plans_dir);
    let registry_before = directory_entries(&fixture.registry_dir);
    assert!(plans_before.is_empty());
    assert!(!registry_before.contains(&"registry.lock".to_string()));

    let gw = fixture.gateway();
    let direct_status = gw
        .coworker_status(&CoworkerStatusParams::default())
        .expect("direct coworker_status on empty plans_dir");
    assert_eq!(direct_status.binding_id, fixture.binding_id);
    assert_eq!(direct_status.thread_uuid, THREAD_UUID);
    assert_eq!(direct_status.workflow_ledger_revision, 0);
    assert_eq!(direct_status.command_ledger_revision, 0);
    assert!(direct_status.active_grant.is_none());
    assert!(direct_status.grants.is_empty());
    assert!(direct_status.jobs.is_empty());
    assert!(direct_status.architect_advice.is_empty());
    assert!(direct_status.commands.is_empty());
    assert_eq!(directory_entries(&fixture.plans_dir), plans_before);
    assert_eq!(directory_entries(&fixture.registry_dir), registry_before);

    // 2. PairedMcpServer advertises all 6 Coworker planning tools and blocks unbound calls.
    let pairing_store = PairingStore::open(&fixture.pairing_dir).expect("open pairing store");
    let mcp_unbound =
        PairedMcpServer::new(&fixture.state_dir, pairing_store.clone()).expect("new unbound mcp");
    let tools = mcp_unbound.list_tools();
    assert_eq!(tools.len(), 59);
    for (name, expected_ro) in [
        ("coworker_status", true),
        ("prepare_work", false),
        ("consult_advisor", false),
        ("consult_architect", false),
        ("request_technical_plan", false),
        ("planning_command", true),
    ] {
        let tool = tools
            .iter()
            .find(|t| t.name == name)
            .unwrap_or_else(|| panic!("missing tool {name}"));
        assert_eq!(
            tool.annotations.as_ref().and_then(|a| a.read_only_hint),
            Some(expected_ro)
        );
        assert_eq!(
            invoke_mcp_err_code(&mcp_unbound, name, json!({})).await,
            "unbound_connection"
        );
    }

    // 3. Enrolled server without workflow/host store dirs fails closed with `not_configured`.
    let token = pairing_store.operator_token().expect("operator token");
    let mcp_unconfigured =
        PairedMcpServer::new(&fixture.state_dir, pairing_store.clone()).expect("new unconfigured");
    pairing_store
        .enroll(
            &token,
            OperatorEnrollRequest {
                connection_nonce: mcp_unconfigured.connection_nonce().to_string(),
                epoch_id: mcp_unconfigured.epoch_id().to_string(),
                binding_id: fixture.binding_id.clone(),
                thread_id: THREAD_UUID.to_string(),
                project_id: None,
            },
        )
        .expect("enroll unconfigured");
    assert_eq!(
        invoke_mcp_err_code(&mcp_unconfigured, "coworker_status", json!({})).await,
        "not_configured"
    );

    // 4. Configured enrolled server rejects caller-supplied identity/rebind keys and unknown fields.
    let mcp = fixture.enrolled_mcp();
    for forbidden in [
        "binding_id",
        "thread_id",
        "thread_uuid",
        "session_id",
        "connection_nonce",
        "epoch_id",
    ] {
        let mut obj = serde_json::Map::new();
        obj.insert(forbidden.to_string(), json!("forged"));
        assert_eq!(
            invoke_mcp_err_code(&mcp, "coworker_status", Value::Object(obj)).await,
            "rebind_rejected"
        );
    }
    assert_eq!(
        invoke_mcp_err_code(&mcp, "coworker_status", json!({ "unknown_field": 1 })).await,
        "validation_failed"
    );
    assert_eq!(
        invoke_mcp_err_code(&mcp, "coworker_status", json!({ "limit": 0 })).await,
        "validation_failed"
    );

    let mcp_status: CoworkerStatusResponse =
        invoke_mcp_ok(&mcp, "coworker_status", json!({})).await;
    assert_eq!(mcp_status, direct_status);
    assert!(!planning_commands_dir(&fixture.plans_dir).exists());

    fixture.shutdown().await;
}

#[tokio::test]
async fn prepare_work_idempotency_head_progression_proposals_amendment_and_grant_revocation_expiry()
{
    let mut fixture = GatewayFixture::start().await;
    let mcp = fixture.enrolled_mcp();
    let now_ms = current_wall_clock_ms().expect("wall clock");
    let grant_id = fixture.create_grant(
        "req-grant-prep-1",
        now_ms.saturating_sub(5_000),
        now_ms + 3_600_000,
    );
    let commit_1 = fixture.project_base.base_commit.clone();
    let tree_1 = fixture.project_base.base_tree_sha.clone();

    // 1. Prepare initial work brief keeping confirmed_requirements, proposed_additions, assumptions, and open_questions distinct.
    let prep_args_1 = json!({
        "grant_id": grant_id,
        "request_key": "req-prep-job-1",
        "summary": "Structure Coworker planning gateway",
        "confirmed_requirements": ["Expose 5 Paired MCP tools"],
        "proposed_additions": ["Optional command limit"],
        "assumptions": ["Sibling threadlane-flow binary is installed"],
        "open_questions": ["How should dependency blobs be verified?"]
    });
    let prep_1: PrepareWorkResponse =
        invoke_mcp_ok(&mcp, "prepare_work", prep_args_1.clone()).await;
    assert_eq!(prep_1.binding_id, fixture.binding_id);
    assert_eq!(prep_1.thread_uuid, THREAD_UUID);
    assert_eq!(prep_1.grant_id, grant_id);
    assert_eq!(prep_1.job_revision, 1);
    assert_eq!(prep_1.phase, CoworkerJobPhase::Planning);
    assert_eq!(prep_1.kind, WorkBriefKind::NewJob);
    assert_eq!(prep_1.project_base.base_commit, commit_1);
    assert_eq!(prep_1.project_base.base_tree_sha, tree_1);
    assert!(prep_1.ready_for_execution_planning);
    assert!(!prep_1.ready_for_execution_approval);
    assert_eq!(
        prep_1.work_brief.confirmed_requirements,
        vec!["Expose 5 Paired MCP tools"]
    );
    assert_eq!(
        prep_1.work_brief.proposed_additions,
        vec!["Optional command limit"]
    );
    assert_eq!(
        prep_1.work_brief.open_questions,
        vec!["How should dependency blobs be verified?"]
    );

    // 2. Advance Git HEAD in the same canonical repository.
    fs::write(
        fixture.repo_dir.join("src/lib.rs"),
        "pub fn gateway_seed() -> u32 { 22 }\n",
    )
    .expect("update src/lib.rs");
    for args in [
        &["add", "src/lib.rs"][..],
        &["commit", "-m", "second commit", "--quiet"][..],
    ] {
        run_sanitized_git(&fixture.repo_dir, args, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("git commit 2");
    }
    let commit_2 =
        resolve_exact_commit_sha(&fixture.repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve commit_2");
    assert_ne!(commit_1, commit_2);

    // 3. Exact retry of `req-prep-job-1` after HEAD advanced preserves original job and commit_1;
    //    conflicting body with the same `request_key` is rejected.
    let retry_1: PrepareWorkResponse = invoke_mcp_ok(&mcp, "prepare_work", prep_args_1).await;
    assert_eq!(retry_1.job_id, prep_1.job_id);
    assert_eq!(retry_1.work_brief_digest, prep_1.work_brief_digest);
    assert_eq!(retry_1.project_base.base_commit, commit_1);

    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "prepare_work",
            json!({
                "grant_id": grant_id,
                "request_key": "req-prep-job-1",
                "summary": "Changed summary with same request_key",
                "confirmed_requirements": ["Expose 5 Paired MCP tools"]
            })
        )
        .await,
        "validation_failed"
    );

    // 4. New `prepare_work` request under the same standing grant resolves the new current HEAD `commit_2`.
    let prep_2: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-job-2",
            "summary": "Second job at advanced HEAD commit",
            "confirmed_requirements": ["Track current HEAD commit for new work brief"]
        }),
    )
    .await;
    assert_ne!(prep_2.job_id, prep_1.job_id);
    assert_eq!(prep_2.project_base.base_commit, commit_2);
    assert!(prep_2.ready_for_execution_planning);
    assert!(prep_2.ready_for_execution_approval);

    // 5. Amendment CAS on `target_job_revision` and active hold placement on superseded job.
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "prepare_work",
            json!({
                "grant_id": grant_id,
                "request_key": "req-amend-wrong-rev",
                "summary": "Amended summary",
                "confirmed_requirements": ["Updated requirement"],
                "target_job_id": prep_2.job_id,
                "target_job_revision": 99,
                "amendment_reason": "Wrong revision"
            })
        )
        .await,
        "validation_failed"
    );

    let amended: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-amend-job-2",
            "summary": "Amended job 2 summary",
            "confirmed_requirements": ["Track current HEAD commit for new work brief", "Enforce CAS"],
            "amendment": {
                "target_job_id": prep_2.job_id,
                "target_job_revision": prep_2.job_revision,
                "amendment_reason": "Add CAS requirement",
                "amended_write_paths": ["src/lib.rs"]
            }
        }),
    )
    .await;
    assert_eq!(
        amended.supersedes_job_id.as_deref(),
        Some(prep_2.job_id.as_str())
    );
    assert_eq!(amended.target_job_active_hold_count, 1);

    // 6. Expired grant and revoked grant both refuse `prepare_work`.
    let expired_grant_id = fixture.create_grant(
        "req-grant-expired",
        now_ms.saturating_sub(60_000),
        now_ms.saturating_sub(1_000),
    );
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "prepare_work",
            json!({
                "grant_id": expired_grant_id,
                "request_key": "req-after-expiry",
                "summary": "Expired grant",
                "confirmed_requirements": ["Must reject expired grant"]
            })
        )
        .await,
        "validation_failed"
    );

    let wf_store =
        CoworkerWorkflowStore::new(&fixture.plans_dir).expect("open CoworkerWorkflowStore");
    wf_store
        .revoke_planning_grant(&RevokePlanningGrantRequest {
            binding_id: fixture.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant_id.clone(),
            expected_ledger_revision: None,
            revoked_at_ms: current_wall_clock_ms().expect("clock"),
            reason: "Revoked in test".to_string(),
        })
        .expect("revoke grant");

    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "prepare_work",
            json!({
                "grant_id": grant_id,
                "request_key": "req-after-revoke",
                "summary": "Revoked grant",
                "confirmed_requirements": ["Must reject revoked grant"]
            })
        )
        .await,
        "validation_failed"
    );

    fixture.shutdown().await;
}

async fn wait_for_command_completion(
    mcp: &PairedMcpServer,
    command_id: &str,
) -> PlanningCommandDetailResponse {
    let mut last_detail = None;
    for _ in 0..100 {
        let detail: PlanningCommandDetailResponse =
            invoke_mcp_ok(mcp, "planning_command", json!({ "command_id": command_id })).await;
        if detail.status != PlanningCommandStatus::Queued
            && detail.status != PlanningCommandStatus::Running
        {
            return detail;
        }
        last_detail = Some(detail);
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    let last_context = last_detail.as_ref().map_or_else(
        || "<no poll result>".to_string(),
        format_command_detail_context,
    );
    panic!(
        "timed out waiting for planning command `{command_id}` to reach terminal status; last={last_context}"
    );
}

#[tokio::test]
async fn consult_architect_and_request_technical_plan_detached_flow_cli_reopen_and_unknown_reconciliation(
) {
    let mut fixture = GatewayFixture::start().await;
    fs::write(&fixture.health_enabled_path, b"enabled\n").expect("enable fixture health hints");
    let mcp = fixture.enrolled_mcp();
    let now_ms = current_wall_clock_ms().expect("wall clock");
    let grant_id = fixture.create_grant(
        "req-grant-e2e-1",
        now_ms.saturating_sub(5_000),
        now_ms + 3_600_000,
    );

    let prep: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-e2e-1",
            "summary": "Build Paired MCP Coworker planning bridge",
            "confirmed_requirements": ["Use canonical sibling threadlane-flow start-command"],
            "proposed_additions": ["Expose recent commands in coworker_status"],
            "open_questions": ["How should dependency blobs be verified?"]
        }),
    )
    .await;
    assert!(prep.ready_for_execution_planning);
    assert!(!prep.ready_for_execution_approval);

    // 1. Hold fake AGY worker via `release_path` before calling `consult_architect` to prove detached return.
    fs::remove_file(&fixture.release_path).expect("hold fake worker before consult_architect");
    let consult_args = json!({
        "grant_id": grant_id,
        "job_id": prep.job_id,
        "request_key": "req-consult-arch-1",
        "question": format!("How should dependency blobs be verified? {SECRET_SENTINEL}"),
        "dependency_files": ["src/lib.rs"],
        "difficulty": "hard"
    });
    let submit_arch: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "consult_architect", consult_args.clone()).await;
    assert_eq!(submit_arch.binding_id, fixture.binding_id);
    assert_eq!(submit_arch.role, WorkflowRole::Architect);
    assert_eq!(
        submit_arch.submission_status,
        PlanningCommandStartStatus::SubmissionRequested
    );
    assert!(submit_arch.submission_requested);
    assert!(submit_arch.launched);
    assert!(matches!(
        submit_arch.status,
        PlanningCommandStatus::Queued | PlanningCommandStatus::Running
    ));

    let arch_live = wait_for_early_health(&mcp, &submit_arch.command_id).await;
    assert_eq!(arch_live.status, PlanningCommandStatus::Running);
    assert_eq!(arch_live.command.provider_health.len(), 1);
    assert!(!arch_live.command.provider_health[0].terminal_observed);
    assert_eq!(arch_live.command.provider_health[0].snapshot.retry_count, 1);
    assert_eq!(arch_live.command.provider_health[0].snapshot.evidence_class,
        threadlane_workers::provider_health::ProviderHealthEvidenceClass::UnverifiedProviderDiagnosticHint);
    let first_arch_hint = arch_live.command.provider_health[0].snapshot.first_failure;

    // Release the held fake worker and wait for completion.
    fs::write(&fixture.release_path, b"release\n").expect("release held worker");
    let arch_detail = wait_for_command_completion(&mcp, &submit_arch.command_id).await;
    assert_eq!(
        arch_detail.status,
        PlanningCommandStatus::Completed,
        "arch_detail did not complete cleanly: {}",
        format_command_detail_context(&arch_detail)
    );
    assert!(!arch_detail.pending_reconciliation);
    assert!(arch_detail.command.provider_health[0].terminal_observed);
    assert_eq!(
        arch_detail.command.provider_health[0]
            .snapshot
            .first_failure,
        first_arch_hint
    );
    assert_eq!(
        arch_detail
            .structured_architect_advice
            .as_ref()
            .expect("structured_architect_advice")
            .decision_summary,
        "Keep strict planning command boundary"
    );

    // 2. Reopen / repeat `consult_architect` with the same `request_key`: idempotent with invocation counter == 1.
    let repeat_arch: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "consult_architect", consult_args).await;
    assert_eq!(repeat_arch.command_id, submit_arch.command_id);
    assert_eq!(
        repeat_arch.status,
        PlanningCommandStatus::Completed,
        "repeat_arch did not preserve Completed status: command_id={} status={:?} submission_status={:?} diagnostic={:?}",
        repeat_arch.command_id,
        repeat_arch.status,
        repeat_arch.submission_status,
        repeat_arch.command.diagnostic
    );
    assert_eq!(
        repeat_arch.submission_status,
        PlanningCommandStartStatus::AlreadyCompleted
    );
    assert!(!repeat_arch.launched);
    assert_eq!(
        repeat_arch.command.provider_health,
        arch_detail.command.provider_health
    );
    let arch_calls = fs::read_to_string(&fixture.worker_marker)
        .expect("read worker_marker")
        .lines()
        .count();
    assert_eq!(arch_calls, 1);

    let status_after_arch: CoworkerStatusResponse =
        invoke_mcp_ok(&mcp, "coworker_status", json!({})).await;
    assert_eq!(status_after_arch.architect_advice.len(), 1);
    let advice_id = status_after_arch.architect_advice[0].advice_id.clone();

    // 3. Typed Orchestrator outcome 1: `RequiresHumanClarification`.
    fs::write(&fixture.mode_path, b"orch_clarify\n").expect("set mode orch_clarify");
    fs::remove_file(&fixture.release_path).expect("hold Orchestrator");
    let clarify_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": prep.job_id,
            "request_key": "req-tech-plan-clarify-1",
            "difficulty": "hard"
        }),
    )
    .await;
    let orch_live = wait_for_early_health(&mcp, &clarify_submit.command_id).await;
    assert!(matches!(
        orch_live.command.provider_health[0].invocation,
        threadlane_native::provider_health::ProviderHealthInvocation::Planning {
            role: WorkflowRole::Orchestrator,
            ..
        }
    ));
    fs::write(&fixture.release_path, b"release\n").expect("release Orchestrator");
    let clarify_detail = wait_for_command_completion(&mcp, &clarify_submit.command_id).await;
    assert_eq!(
        clarify_detail.status,
        PlanningCommandStatus::Completed,
        "clarify_detail did not complete cleanly: {}",
        format_command_detail_context(&clarify_detail)
    );
    assert!(matches!(
        clarify_detail.result_ref,
        Some(PlanningCommandResultRef::OrchestratorRequiresHumanClarification { .. })
    ));
    assert_eq!(
        clarify_detail
            .clarification
            .as_ref()
            .expect("clarification")
            .questions,
        vec!["Should we support multiple commands per batch?"]
    );

    // 4. Typed Orchestrator outcome 2: `Plan` while open_questions and proposed_additions remain open.
    fs::write(&fixture.mode_path, b"orch_plan\n").expect("set mode orch_plan");
    let plan_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": prep.job_id,
            "request_key": "req-tech-plan-publish-1",
            "architect_advice_ids": [advice_id],
            "difficulty": "hard"
        }),
    )
    .await;
    let plan_detail = wait_for_command_completion(&mcp, &plan_submit.command_id).await;
    assert_eq!(
        plan_detail.status,
        PlanningCommandStatus::Completed,
        "plan_detail did not complete cleanly: {}",
        format_command_detail_context(&plan_detail)
    );
    let orch_summary = plan_detail
        .orchestrator_plan_summary
        .as_ref()
        .expect("orchestrator_plan_summary");
    assert_eq!(orch_summary.plan_revision, 1);
    assert_eq!(orch_summary.plan_status, PlanStatus::Published);
    assert_eq!(orch_summary.task_count, 1);

    let status_after_plan: CoworkerStatusResponse =
        invoke_mcp_ok(&mcp, "coworker_status", json!({})).await;
    let job_summary = status_after_plan
        .jobs
        .iter()
        .find(|j| j.job_id == prep.job_id)
        .expect("job in status");
    assert_eq!(job_summary.phase, CoworkerJobPhase::AwaitingApproval);
    assert!(job_summary.planning_result.is_some());
    assert!(job_summary.ready_for_execution_planning);
    assert!(!job_summary.ready_for_execution_approval);

    // A second command for this job supersedes r1 without invalidating its completed history.
    let update_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": prep.job_id,
            "request_key": "req-tech-plan-publish-2",
            "plan_id": orch_summary.plan_id,
            "expected_plan_revision": 1,
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let update_detail = wait_for_command_completion(&mcp, &update_submit.command_id).await;
    assert_eq!(
        update_detail.status,
        PlanningCommandStatus::Completed,
        "update_detail did not complete cleanly: {}",
        format_command_detail_context(&update_detail)
    );
    assert_eq!(
        update_detail
            .orchestrator_plan_summary
            .as_ref()
            .expect("r2 summary")
            .plan_revision,
        2
    );
    let calls_before_history =
        fs::read(&fixture.worker_marker).expect("provider calls before history");
    for _ in 0..3 {
        let status: CoworkerStatusResponse =
            invoke_mcp_ok(&mcp, "coworker_status", json!({})).await;
        let old = status
            .commands
            .iter()
            .find(|c| c.command_id == plan_submit.command_id)
            .expect("r1 completed command");
        assert_eq!(old.current_plan_status, Some(PlanStatus::Superseded));
        assert_eq!(old.executable, Some(false));
        let old_res = old.result_summary.as_ref().expect("r1 result_summary");
        assert!(old_res.status_projection_only);
        assert!(!old_res.grants_execution_authority);
        assert_eq!(
            old_res.kind,
            threadlane_native::planning_gateway::CoworkerStatusCommandResultKind::OrchestratorPlan
        );
        let old_plan = old_res.plan_result.as_ref().expect("r1 plan_result");
        assert_eq!(old_plan.plan_id, orch_summary.plan_id);
        assert_eq!(old_plan.plan_revision, 1);
        assert_eq!(old_plan.plan_digest, orch_summary.plan_digest);
        assert_eq!(old_plan.plan_status, PlanStatus::Published);
        assert_eq!(old_plan.current_plan_status, Some(PlanStatus::Superseded));
        assert_eq!(old_plan.executable, Some(false));
        let current = status
            .commands
            .iter()
            .find(|c| c.command_id == update_submit.command_id)
            .expect("r2 completed command");
        assert_eq!(current.current_plan_status, Some(PlanStatus::Published));
        let detail: PlanningCommandDetailResponse = invoke_mcp_ok(
            &mcp,
            "planning_command",
            json!({"command_id": plan_submit.command_id}),
        )
        .await;
        assert_eq!(detail.current_plan_status, Some(PlanStatus::Superseded));
        assert_eq!(detail.executable, Some(false));
        assert_eq!(
            detail
                .orchestrator_plan_summary
                .as_ref()
                .expect("frozen summary")
                .plan_status,
            PlanStatus::Published
        );
    }
    assert_eq!(
        fs::read(&fixture.worker_marker).expect("provider calls after history"),
        calls_before_history
    );

    // 5. Simulate interrupted `Running` command -> reopens as `Unknown` (`pending_reconciliation == true`)
    //    and blocks a new `request_key` from bypassing reconciliation.
    let wf_store =
        CoworkerWorkflowStore::new(&fixture.plans_dir).expect("open CoworkerWorkflowStore");
    let wf_ledger = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read wf_ledger");
    let stored_grant = wf_ledger
        .planning_grants
        .iter()
        .find(|g| g.grant_id == grant_id)
        .expect("grant");
    let commands_dir = planning_commands_dir(&fixture.plans_dir);
    let ownership_store =
        CoworkerJobOwnershipStore::new(&commands_dir).expect("open CoworkerJobOwnershipStore");
    let cmd_store = PlanningCommandStore::new(
        &commands_dir,
        wf_store,
        &fixture.registry_dir,
        ownership_store.clone(),
    )
    .expect("open PlanningCommandStore");
    let dep_blobs = CoworkerPlanningService::compute_dependency_file_blobs(
        &prep.project_base,
        &["src/lib.rs".to_string()],
    )
    .expect("dep blobs");
    let enqueued_unknown = cmd_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_architect(
            RunArchitectAdviceRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                request_key: "req-unknown-sim-1".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                context_dependencies: ArchitectContextDependencies {
                    project_base: prep.project_base.clone(),
                    work_brief_digest: prep.work_brief_digest.clone(),
                    plan_digest: None,
                    policy_digest: stored_grant.policy_digest.clone(),
                    dependency_file_blobs: dep_blobs,
                    interface_ownership_digests: BTreeMap::new(),
                    accepted_predecessor_trees: BTreeMap::new(),
                },
                question_summary: "Simulate interrupted Running command".to_string(),
                prior_advice_context: Vec::new(),
                accept_into_brief: false,
                current_time_ms: current_wall_clock_ms().expect("clock"),
            },
        ))
        .expect("enqueue command for unknown simulation");
    let guard = ownership_store
        .try_acquire_job(
            &enqueued_unknown.ownership_key().expect("ownership key"),
            current_wall_clock_ms().expect("clock"),
        )
        .expect("try_acquire_job")
        .into_guard()
        .expect("guard");
    cmd_store
        .claim_running(
            &guard,
            &fixture.binding_id,
            THREAD_UUID,
            &enqueued_unknown.command_id,
            None,
        )
        .expect("claim_running");
    drop(guard);

    let unknown_detail: PlanningCommandDetailResponse = invoke_mcp_ok(
        &mcp,
        "planning_command",
        json!({ "command_id": enqueued_unknown.command_id }),
    )
    .await;
    assert_eq!(unknown_detail.status, PlanningCommandStatus::Unknown);
    assert!(unknown_detail.pending_reconciliation);

    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "consult_architect",
            json!({
                "grant_id": grant_id,
                "job_id": prep.job_id,
                "request_key": "req-bypass-unknown-must-fail",
                "question": "Attempt to bypass Unknown command with a new key",
                "dependency_files": ["src/lib.rs"]
            })
        )
        .await,
        "ambiguous_wakeup_pending_reconciliation"
    );

    let gw_err = fixture
        .gateway()
        .consult_architect(&ConsultArchitectParams {
            grant_id,
            job_id: prep.job_id,
            request_key: "req-bypass-unknown-gw".to_string(),
            expected_ledger_revision: None,
            question: "Attempt via gateway".to_string(),
            dependency_files: vec!["src/lib.rs".to_string()],
            prior_advice_ids: Vec::new(),
            difficulty: Some(Difficulty::Hard),
        })
        .await
        .expect_err("gateway must reject new key while Unknown is pending reconciliation");
    assert_eq!(
        gw_err.error_code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation
    );

    let registry = RegistryStore::open(&fixture.registry_dir).expect("registry for drift");
    let revision = registry.snapshot().expect("current registry").revision;
    registry
        .apply(
            revision,
            RegistryChange::SetDefaultProfile { profile_id: None },
        )
        .expect("advance registry after r2");
    let calls_before_drift_reads = fs::read(&fixture.worker_marker).expect("provider calls");
    let status: CoworkerStatusResponse = invoke_mcp_ok(&mcp, "coworker_status", json!({})).await;
    assert_eq!(
        status
            .commands
            .iter()
            .find(|c| c.command_id == plan_submit.command_id)
            .expect("historical command after registry drift")
            .current_plan_status,
        Some(PlanStatus::Superseded)
    );
    assert_eq!(
        fs::read(&fixture.worker_marker).expect("provider calls after drift reads"),
        calls_before_drift_reads
    );

    fixture.shutdown().await;
}

#[tokio::test]
async fn request_technical_plan_omitted_max_architect_rounds_auto_consults_architect_and_respects_zero_or_budget_limits(
) {
    let mut fixture = GatewayFixture::start().await;
    fs::write(&fixture.health_enabled_path, b"enabled\n").expect("enable fixture health hints");
    let mcp = fixture.enrolled_mcp();
    let now_ms = current_wall_clock_ms().expect("wall clock");
    let grant_id = fixture.create_grant(
        "req-grant-auto-arch-1",
        now_ms.saturating_sub(5_000),
        now_ms + 3_600_000,
    );

    let prep: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-auto-arch-1",
            "summary": "Verify Orchestrator automatic Architect consultation when max_architect_rounds is omitted",
            "confirmed_requirements": ["Consult Architect automatically up to bounded grant/policy limits when Orchestrator asks"]
        }),
    )
    .await;

    // 1. Explicit `max_architect_rounds: 0` when Orchestrator asks for advice must NOT call Architect.
    fs::write(&fixture.mode_path, b"orch_ask_then_plan\n").expect("set mode orch_ask_then_plan");
    let zero_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": prep.job_id,
            "request_key": "req-tech-plan-explicit-zero",
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let zero_detail = wait_for_command_completion(&mcp, &zero_submit.command_id).await;
    assert_eq!(
        zero_detail.status,
        PlanningCommandStatus::Completed,
        "zero_detail did not complete cleanly: {}",
        format_command_detail_context(&zero_detail)
    );
    assert!(matches!(
        zero_detail.result_ref,
        Some(PlanningCommandResultRef::OrchestratorRequiresArchitectAdvice { .. })
    ));
    assert_eq!(
        zero_detail
            .consultation
            .as_ref()
            .expect("consultation")
            .questions
            .len(),
        1
    );

    let status_after_zero: CoworkerStatusResponse =
        invoke_mcp_ok(&mcp, "coworker_status", json!({})).await;
    let grant_after_zero = status_after_zero
        .grants
        .iter()
        .find(|g| g.grant_id == grant_id)
        .expect("grant after explicit zero");
    assert_eq!(grant_after_zero.usage.reserved_orchestrator_calls, 1);
    assert_eq!(grant_after_zero.usage.reserved_architect_calls, 0);
    assert_eq!(grant_after_zero.usage.reserved_total_calls, 1);
    assert!(status_after_zero.architect_advice.is_empty());
    let calls_after_zero = fs::read_to_string(&fixture.worker_marker)
        .expect("read worker_marker")
        .lines()
        .count();
    assert_eq!(calls_after_zero, 1);

    // 2. Omitted `max_architect_rounds` under an active grant that permits Architect defaults to a bounded
    //    allowance (`2`), runs Orchestrator -> Architect -> Orchestrator via detached `threadlane-flow`,
    //    and publishes the typed Plan without any coder execution.
    fs::write(&fixture.mode_path, b"orch_ask_then_plan\n").expect("reset mode orch_ask_then_plan");
    let omitted_args = json!({
        "grant_id": grant_id,
        "job_id": prep.job_id,
        "request_key": "req-tech-plan-omitted-auto-arch",
        "difficulty": "hard"
    });
    let auto_submit: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "request_technical_plan", omitted_args.clone()).await;
    let auto_detail = wait_for_command_completion(&mcp, &auto_submit.command_id).await;
    assert_eq!(
        auto_detail.status,
        PlanningCommandStatus::Completed,
        "auto_detail did not complete cleanly: {}",
        format_command_detail_context(&auto_detail)
    );
    assert!(matches!(
        auto_detail.result_ref,
        Some(PlanningCommandResultRef::OrchestratorPlan { .. })
    ));
    assert_eq!(
        auto_detail.command.provider_health.len(),
        3,
        "every Orchestrator/Architect round has its own invocation"
    );
    assert!(auto_detail
        .command
        .provider_health
        .iter()
        .all(|health| health.terminal_observed && health.snapshot.retry_count == 1));
    assert_eq!(
        auto_detail
            .command
            .provider_health
            .iter()
            .filter(|health| matches!(
                health.invocation,
                threadlane_native::provider_health::ProviderHealthInvocation::Planning {
                    role: WorkflowRole::Architect,
                    ..
                }
            ))
            .count(),
        1
    );
    let health_reservations: std::collections::BTreeSet<_> = auto_detail
        .command
        .provider_health
        .iter()
        .map(|health| health.invocation.key())
        .collect();
    assert_eq!(health_reservations.len(), 3);
    let orch_summary = auto_detail
        .orchestrator_plan_summary
        .as_ref()
        .expect("orchestrator_plan_summary");
    assert_eq!(orch_summary.plan_revision, 1);
    assert_eq!(orch_summary.plan_status, PlanStatus::Published);
    assert_eq!(orch_summary.task_count, 1);

    let status_after_auto: CoworkerStatusResponse =
        invoke_mcp_ok(&mcp, "coworker_status", json!({})).await;
    let grant_after_auto = status_after_auto
        .grants
        .iter()
        .find(|g| g.grant_id == grant_id)
        .expect("grant after omitted auto-arch");
    assert_eq!(grant_after_auto.usage.reserved_orchestrator_calls, 3);
    assert_eq!(grant_after_auto.usage.reserved_architect_calls, 1);
    assert_eq!(grant_after_auto.usage.reserved_total_calls, 4);
    assert_eq!(status_after_auto.architect_advice.len(), 1);
    assert_eq!(
        status_after_auto.architect_advice[0].decision_summary,
        "Keep strict planning command boundary"
    );
    let calls_after_auto = fs::read_to_string(&fixture.worker_marker)
        .expect("read worker_marker")
        .lines()
        .count();
    assert_eq!(calls_after_auto, 4);

    let job_after_auto = status_after_auto
        .jobs
        .iter()
        .find(|j| j.job_id == prep.job_id)
        .expect("job after auto-arch");
    assert_eq!(job_after_auto.phase, CoworkerJobPhase::AwaitingApproval);
    assert!(job_after_auto.planning_result.is_some());
    assert!(job_after_auto.approval_id.is_none());
    assert_eq!(job_after_auto.task_count, 0);

    // Idempotent retry with `max_architect_rounds` omitted after grant usage advanced reuses the completed command.
    let repeat_auto: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "request_technical_plan", omitted_args).await;
    assert_eq!(repeat_auto.command_id, auto_submit.command_id);
    assert_eq!(
        repeat_auto.submission_status,
        PlanningCommandStartStatus::AlreadyCompleted
    );
    assert!(!repeat_auto.launched);
    assert_eq!(
        repeat_auto.command.provider_health,
        auto_detail.command.provider_health
    );

    // 3. When the active grant policy or remaining budget does not permit a headless Architect call,
    //    omitted `max_architect_rounds` defaults to `0` and returns honest `OrchestratorRequiresArchitectAdvice`
    //    without calling Architect.
    let wf_store =
        CoworkerWorkflowStore::new(&fixture.plans_dir).expect("open CoworkerWorkflowStore");
    let orch_only_grant_id = wf_store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: fixture.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "req-grant-orch-only-1".to_string(),
            expected_ledger_revision: None,
            project_base: fixture.project_base.clone(),
            profile_id: PROFILE_ID.to_string(),
            registry_revision: 3,
            policy: ExecutionPolicy {
                schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
                policy_id: "policy-orch-only".to_string(),
                allow_rules: vec![PolicyAllowRule {
                    tool_id: TOOL_ID.to_string(),
                    model_id: MODEL_ID.to_string(),
                    efforts: vec!["high".to_string()],
                    allow_no_effort: false,
                    roles: vec![WorkflowRole::Orchestrator],
                }],
            },
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget: PlanningCallBudget {
                max_total_calls: 4,
                max_orchestrator_calls: 4,
                max_architect_calls: 2,
                max_timeout_ms_per_call: 60_000,
                max_output_bytes_per_call: 65_536,
            },
            created_at_ms: now_ms.saturating_sub(1_000),
            expires_at_ms: now_ms + 3_600_000,
            operator_attested: true,
        })
        .expect("create orch-only grant")
        .grant_id;

    let prep_2: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": orch_only_grant_id,
            "request_key": "req-prep-orch-only-job",
            "summary": "Job under Orchestrator-only policy",
            "confirmed_requirements": ["Return honest consultation outcome when Architect is not permitted by grant policy"]
        }),
    )
    .await;

    fs::write(&fixture.mode_path, b"orch_ask_then_plan\n").expect("reset mode orch_ask_then_plan");
    let no_arch_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": orch_only_grant_id,
            "job_id": prep_2.job_id,
            "request_key": "req-tech-plan-no-arch-permission",
            "difficulty": "hard"
        }),
    )
    .await;
    let no_arch_detail = wait_for_command_completion(&mcp, &no_arch_submit.command_id).await;
    assert_eq!(
        no_arch_detail.status,
        PlanningCommandStatus::Completed,
        "no_arch_detail did not complete cleanly: {}",
        format_command_detail_context(&no_arch_detail)
    );
    assert!(matches!(
        no_arch_detail.result_ref,
        Some(PlanningCommandResultRef::OrchestratorRequiresArchitectAdvice { .. })
    ));

    let status_final: CoworkerStatusResponse =
        invoke_mcp_ok(&mcp, "coworker_status", json!({})).await;
    let orch_only_grant = status_final
        .grants
        .iter()
        .find(|g| g.grant_id == orch_only_grant_id)
        .expect("orch_only_grant");
    assert_eq!(orch_only_grant.usage.reserved_orchestrator_calls, 1);
    assert_eq!(orch_only_grant.usage.reserved_architect_calls, 0);
    assert_eq!(orch_only_grant.usage.reserved_total_calls, 1);
    let calls_final = fs::read_to_string(&fixture.worker_marker)
        .expect("read worker_marker")
        .lines()
        .count();
    assert_eq!(calls_final, 5);

    fixture.shutdown().await;
}

#[tokio::test]
async fn detached_cli_planning_completion_notifies_private_websocket_peer_once() {
    use futures_util::{SinkExt, StreamExt};
    use threadlane::NativeToolOutputConfig;
    use threadlane_native::coworker_workflow::CoworkerJobOwnershipKey;
    use threadlane_native::{PlanningNoticeConfig, PlanningNoticeStore};
    use tokio::net::UnixListener;
    use tokio::sync::oneshot;
    use tokio_tungstenite::accept_async;
    use tokio_tungstenite::tungstenite::Message;

    for disconnect_after_write in [false, true] {
        let mut fixture = GatewayFixture::start().await;
        let mcp = fixture.enrolled_mcp();
        let now_ms = current_wall_clock_ms().expect("wall clock");
        let grant_id = fixture.create_grant(
            "req-notice-grant-1",
            now_ms.saturating_sub(5_000),
            now_ms + 3_600_000,
        );
        let prep: PrepareWorkResponse = invoke_mcp_ok(
            &mcp,
            "prepare_work",
            json!({
                "grant_id": grant_id,
                "request_key": "req-notice-prep-1",
                "summary": "Prove detached planning completion notices",
                "confirmed_requirements": ["Use the private native socket"],
                "proposed_additions": ["Notify after the CLI command completes"],
                "open_questions": ["Is the notice delivered exactly once?"]
            }),
        )
        .await;
        assert!(prep.ready_for_execution_planning);

        create_private_dir(&threadlane_native::planning_commands_dir(
            &fixture.plans_dir,
        ));
        let notice_root = threadlane_native::planning_notice_dir(&fixture.plans_dir);
        create_private_dir(&notice_root);
        let socket_path = notice_root.join("native-notice.sock");
        let listener = UnixListener::bind(&socket_path).expect("bind private notice socket");
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))
            .expect("restrict notice socket permissions");

        let repo = fixture.repo_dir.to_string_lossy().into_owned();
        let (receipt_tx, receipt_rx) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept notice connection");
            let mut websocket = accept_async(stream)
                .await
                .expect("accept private websocket");
            let mut receipt_tx = Some(receipt_tx);
            while let Some(message) = websocket.next().await {
                let message = message.expect("read websocket request");
                let request = match message {
                    Message::Text(text) => text.to_string(),
                    Message::Binary(bytes) => {
                        String::from_utf8(bytes.to_vec()).expect("utf8 binary request")
                    }
                    Message::Ping(payload) => {
                        websocket
                            .send(Message::Pong(payload))
                            .await
                            .expect("send websocket pong");
                        continue;
                    }
                    Message::Close(_) => break,
                    _ => continue,
                };
                let request: Value =
                    serde_json::from_str(&request).expect("parse websocket request");
                let method = request["method"].as_str().unwrap_or_default().to_string();
                if method == "initialized" {
                    continue;
                }
                let is_turn_start = method == "turn/start";
                if is_turn_start && disconnect_after_write {
                    receipt_tx
                        .take()
                        .expect("single turn/start receipt")
                        .send(request)
                        .expect("deliver turn/start receipt");
                    break;
                }
                let result = match method.as_str() {
                    "initialize" => json!({}),
                    "thread/read" => json!({
                        "thread": {
                            "id": THREAD_UUID,
                            "cwd": repo.clone(),
                            "status": {"type": "idle"}
                        }
                    }),
                    "thread/resume" => json!({
                        "thread": {"id": THREAD_UUID, "cwd": repo.clone(), "status": {"type": "idle"}},
                        "cwd": repo.clone()
                    }),
                    "turn/start" => json!({
                        "turn": {
                            "id": "0196ed00-0000-7000-8000-0000000000aa",
                            "status": "inProgress"
                        }
                    }),
                    other => panic!("unexpected websocket request {other}: {request}"),
                };
                let response =
                    json!({"jsonrpc": "2.0", "id": request["id"].clone(), "result": result});
                websocket
                    .send(Message::Text(response.to_string().into()))
                    .await
                    .expect("send websocket response");
                if is_turn_start {
                    receipt_tx
                        .take()
                        .expect("single turn/start receipt")
                        .send(request)
                        .expect("deliver turn/start receipt");
                    break;
                }
            }
        });

        fs::remove_file(&fixture.release_path).expect("hold fake worker");
        let consult_args = json!({
            "grant_id": grant_id,
            "job_id": prep.job_id,
            "request_key": "req-notice-consult-1",
            "question": "How should detached completion be announced?",
            "dependency_files": ["src/lib.rs"],
            "difficulty": "hard"
        });
        let command: PlanningCommandSubmissionResponse =
            invoke_mcp_ok(&mcp, "consult_architect", consult_args.clone()).await;
        assert_eq!(command.binding_id, fixture.binding_id);
        assert_eq!(command.role, WorkflowRole::Architect);
        assert_eq!(
            command.submission_status,
            PlanningCommandStartStatus::SubmissionRequested
        );
        assert!(command.submission_requested);
        assert!(command.launched);

        let ownership_key = CoworkerJobOwnershipKey {
            binding_id: fixture.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            job_id: command.command_id.clone(),
        };
        let target = NativeToolOutputConfig::new_user_message_steer(
            socket_path,
            THREAD_UUID.to_string(),
            fixture.repo_dir.clone(),
        );
        let notice_config = PlanningNoticeConfig::new(
            ownership_key,
            command.command.command_digest.clone(),
            target,
        )
        .expect("valid planning notice configuration");
        let notice_store =
            PlanningNoticeStore::new(&notice_root).expect("open trusted planning notice store");
        notice_store
            .configure(notice_config)
            .expect("configure planning notice target");

        fs::write(&fixture.release_path, b"release\n").expect("release held worker");
        let callback = tokio::time::timeout(Duration::from_secs(10), receipt_rx)
            .await
            .expect("detached CLI callback must arrive within the bounded planning deadline")
            .expect("receive callback request");
        let params = callback["params"]
            .as_object()
            .expect("turn/start params object");
        let text = params["input"][0]["text"]
            .as_str()
            .expect("turn/start input text");
        assert!(text.contains(&command.command_id));
        assert!(text.contains("planning_command"));
        assert!(!params.contains_key("model"));
        assert!(!params.contains_key("reasoningEffort"));
        assert!(!params.contains_key("queue"));
        assert!(!params.contains_key("toolOutput"));
        assert!(!text.contains("toolOutput"));

        let detail = wait_for_command_completion(&mcp, &command.command_id).await;
        assert_eq!(
            detail.status,
            PlanningCommandStatus::Completed,
            "detached notice consult detail did not complete cleanly: {}",
            format_command_detail_context(&detail)
        );
        assert!(!detail.pending_reconciliation);
        assert_eq!(
            detail
                .structured_architect_advice
                .as_ref()
                .expect("structured architect advice")
                .decision_summary,
            "Keep strict planning command boundary"
        );
        let expected_status = if disconnect_after_write {
            "unknown"
        } else {
            "submitted"
        };
        let notified =
            wait_for_planning_notice_delivery(&mcp, &command.command_id, expected_status).await;
        assert_eq!(notified["native_notice"]["configured"], json!(true));
        assert_eq!(notified["native_notice"]["attempt_count"], json!(1));
        assert_eq!(
            notified["native_notice"]["latest"]["status"],
            json!(expected_status)
        );

        let repeat: PlanningCommandSubmissionResponse =
            invoke_mcp_ok(&mcp, "consult_architect", consult_args).await;
        assert_eq!(repeat.command_id, command.command_id);
        assert_eq!(
            repeat.submission_status,
            PlanningCommandStartStatus::AlreadyCompleted
        );
        assert!(!repeat.launched);

        let duplicate_cli = tokio::process::Command::new(env!("CARGO_BIN_EXE_threadlane-flow"))
            .arg("--state-dir")
            .arg(&fixture.state_dir)
            .arg("--registry-dir")
            .arg(&fixture.registry_dir)
            .arg("--plans-dir")
            .arg(&fixture.plans_dir)
            .arg("--authority-dir")
            .arg(&fixture.authority_dir)
            .arg("--sessions-dir")
            .arg(&fixture.sessions_dir)
            .arg("--binding")
            .arg(&fixture.binding_id)
            .arg("--thread")
            .arg(THREAD_UUID)
            .arg("coworker-planning")
            .arg("execute-command")
            .arg("--command-id")
            .arg(&command.command_id)
            .output()
            .await
            .expect("run duplicate detached driver command");
        assert!(
            duplicate_cli.status.success(),
            "duplicate detached driver command failed: {}",
            String::from_utf8_lossy(&duplicate_cli.stderr)
        );

        let provider_calls = fs::read_to_string(&fixture.worker_marker)
            .expect("read worker marker")
            .lines()
            .count();
        assert_eq!(provider_calls, 1);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let after_repeat =
            wait_for_planning_notice_delivery(&mcp, &command.command_id, expected_status).await;
        assert_eq!(after_repeat["native_notice"]["attempt_count"], json!(1));
        assert_eq!(
            after_repeat["native_notice"]["latest"]["status"],
            json!(expected_status)
        );

        peer.await
            .expect("private websocket peer exits after one turn/start");
        fixture.shutdown().await;
    }
}

async fn wait_for_planning_notice_delivery(
    mcp: &PairedMcpServer,
    command_id: &str,
    expected_status: &str,
) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let detail: PlanningCommandDetailResponse =
            invoke_mcp_ok(mcp, "planning_command", json!({"command_id": command_id})).await;
        let value = serde_json::to_value(&detail).expect("serialize planning command detail");
        let notice = &value["native_notice"];
        if notice["configured"] == json!(true)
            && notice["attempt_count"] == json!(1)
            && notice["latest"]["status"] == json!(expected_status)
        {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "planning notice callback was not finalized: {value}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn planning_health_is_durable_before_live_callback_and_terminal_notice_remains_separate() {
    use futures_util::{SinkExt, StreamExt};
    use threadlane::NativeToolOutputConfig;
    use threadlane_native::coworker_notice::{
        planning_notice_dir, NoticeDeliveryStatus, PlanningNoticeConfig, PlanningNoticeStore,
        PlanningNoticeTerminalKind,
    };
    use threadlane_native::coworker_workflow::{
        CoworkerJobLockState, OrchestrateWorkBriefPlanRequest,
    };
    use threadlane_native::planning_driver::open_planning_driver_for_plans_dir;
    use tokio::net::UnixListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    for (role, unknown, automatic_round) in [
        (WorkflowRole::Architect, false, false),
        (WorkflowRole::Orchestrator, false, false),
        (WorkflowRole::Orchestrator, false, true),
        (WorkflowRole::Architect, true, false),
    ] {
        let mut fixture = GatewayFixture::start().await;
        fs::write(&fixture.health_enabled_path, b"enabled\n").expect("enable fixture health hints");
        let mcp = fixture.enrolled_mcp();
        let now = current_wall_clock_ms().unwrap();
        let grant_id = fixture.create_grant("health-planning-grant", now - 1000, now + 3_600_000);
        let prep: PrepareWorkResponse = invoke_mcp_ok(&mcp, "prepare_work", json!({
            "grant_id": grant_id, "request_key": "health-planning-prep",
            "summary": "Offline diagnostic availability", "confirmed_requirements": ["Read src/lib.rs"]
        })).await;
        fs::remove_file(&fixture.release_path).unwrap();
        if role == WorkflowRole::Orchestrator {
            fs::write(
                &fixture.mode_path,
                if automatic_round {
                    &b"orch_ask_then_plan\n"[..]
                } else {
                    &b"orch_plan\n"[..]
                },
            )
            .unwrap();
        }
        let fake_worker = fixture.repo_dir.parent().unwrap().join("bin/fake-agy");
        let script = fs::read_to_string(&fake_worker).unwrap();
        let hint_line = script
            .lines()
            .find(|line| line.contains("retrying in 4s."))
            .unwrap();
        let hints = (1..=5)
            .map(|attempt| {
                hint_line.replace("attempt 1 failed", &format!("attempt {attempt} failed"))
            })
            .collect::<Vec<_>>()
            .join("\n");
        // Each internal invocation waits for its own early callback, not just the root call.
        let script = script.replace(
            "health_previous=''",
            &format!(
                "rm -f '{}'\nhealth_previous=''",
                fixture.release_path.display()
            ),
        );
        fs::write(&fake_worker, script.replace(hint_line, &hints)).unwrap();

        let driver =
            open_planning_driver_for_plans_dir(&fixture.plans_dir, &fixture.registry_dir).unwrap();
        let store = driver.command_store();
        let ledger = store
            .workflow_store()
            .read_binding_ledger(&fixture.binding_id)
            .unwrap();
        let grant = ledger
            .planning_grants
            .iter()
            .find(|grant| grant.grant_id == grant_id)
            .unwrap();
        let request = if role == WorkflowRole::Architect {
            EnqueuePlanningCommandRequest::from_architect(RunArchitectAdviceRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.into(),
                grant_id: grant_id.clone(),
                request_key: "health-planning-call".into(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                context_dependencies: ArchitectContextDependencies {
                    project_base: prep.project_base.clone(),
                    work_brief_digest: prep.work_brief_digest.clone(),
                    plan_digest: None,
                    policy_digest: grant.policy_digest.clone(),
                    dependency_file_blobs: CoworkerPlanningService::compute_dependency_file_blobs(
                        &prep.project_base,
                        &["src/lib.rs".into()],
                    )
                    .unwrap(),
                    interface_ownership_digests: BTreeMap::new(),
                    accepted_predecessor_trees: BTreeMap::new(),
                },
                question_summary: "Inspect the read-only boundary".into(),
                prior_advice_context: Vec::new(),
                accept_into_brief: false,
                current_time_ms: now,
            })
        } else {
            EnqueuePlanningCommandRequest::from_orchestrator(OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.into(),
                grant_id: grant_id.clone(),
                job_id: prep.job_id.clone(),
                request_key: "health-planning-call".into(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: None,
                expected_plan_revision: 0,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: if automatic_round { 1 } else { 0 },
                architect_difficulty: None,
                current_time_ms: now,
            })
        };
        let command = store.enqueue_command(&request).unwrap();
        let key = command.ownership_key().unwrap();
        let notices = planning_notice_dir(&fixture.plans_dir);
        create_private_dir(&notices);
        let socket = notices.join("health.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
        PlanningNoticeStore::new(&notices)
            .unwrap()
            .configure(
                PlanningNoticeConfig::new(
                    key.clone(),
                    command.command_digest.clone(),
                    NativeToolOutputConfig::new_user_message_steer(
                        socket,
                        THREAD_UUID.into(),
                        fixture.repo_dir.clone(),
                    ),
                )
                .unwrap(),
            )
            .unwrap();
        let peer_store = store.clone();
        let peer_key = key.clone();
        let release = fixture.release_path.clone();
        let repo = fixture.repo_dir.to_string_lossy().into_owned();
        let expected_roles = if automatic_round {
            vec![
                WorkflowRole::Orchestrator,
                WorkflowRole::Architect,
                WorkflowRole::Orchestrator,
            ]
        } else {
            vec![role]
        };
        let expected_calls = expected_roles.len();
        let peer = tokio::spawn(async move {
            let mut seen = std::collections::BTreeSet::new();
            // Each live invocation has its own callback; the last connection is terminal.
            for expected_role in expected_roles
                .iter()
                .copied()
                .map(Some)
                .chain(std::iter::once(None))
            {
                let terminal = expected_role.is_none();
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = accept_async(stream).await.unwrap();
                while let Some(frame) = ws.next().await {
                    let Message::Text(text) = frame.unwrap() else {
                        continue;
                    };
                    let request: Value = serde_json::from_str(&text).unwrap();
                    let result = match request["method"].as_str().unwrap() {
                        "initialized" => continue,
                        "initialize" => json!({}),
                        "thread/read" | "thread/resume" => json!({
                            "thread": {"id": THREAD_UUID, "cwd": repo, "status": {"type": "idle"}},
                            "cwd": repo,
                        }),
                        "turn/start" => {
                            let durable = peer_store
                                .get_command(&peer_key.binding_id, THREAD_UUID, &peer_key.job_id)
                                .unwrap()
                                .unwrap();
                            let health = if terminal {
                                durable.provider_health.last().unwrap()
                            } else {
                                durable
                                    .provider_health
                                    .iter()
                                    .find(|health| !health.terminal_observed)
                                    .expect("current reserved invocation")
                            };
                            if let Some(expected_role) = expected_role {
                                let threadlane_native::provider_health::ProviderHealthInvocation::Planning {
                                    role, reservation_id, ..
                                } = &health.invocation else { panic!("expected planning invocation"); };
                                assert_eq!(*role, expected_role);
                                assert!(
                                    seen.insert(reservation_id.clone()),
                                    "no duplicate early callback"
                                );
                            }
                            assert!((1..=5).contains(&health.snapshot.retry_count));
                            if terminal {
                                assert_eq!(health.snapshot.retry_count, 5);
                            }
                            assert_eq!(health.snapshot.evidence_class,
                                threadlane_workers::provider_health::ProviderHealthEvidenceClass::UnverifiedProviderDiagnosticHint);
                            assert_eq!(
                                durable.status,
                                if terminal {
                                    PlanningCommandStatus::Completed
                                } else {
                                    PlanningCommandStatus::Running
                                }
                            );
                            assert_eq!(health.terminal_observed, terminal);
                            if !terminal {
                                assert_eq!(
                                    health.snapshot.availability,
                                    threadlane_workers::provider_health::ProviderDiagnosticAvailability::Available
                                );
                                assert!(health.snapshot.unavailable_reason.is_none());
                            }
                            assert_eq!(health.snapshot.current_failure.is_none(), terminal);
                            let active = matches!(
                                peer_store
                                    .ownership_store()
                                    .inspect_job_lock(&peer_key)
                                    .unwrap(),
                                CoworkerJobLockState::HeldByActiveProcess { .. }
                            );
                            assert_eq!(active, !terminal);
                            let visible = threadlane_native::planning_driver::get_planning_command_summary_readonly(
                                peer_store.workflow_store().plans_dir(), peer_store.registry_dir(),
                                &peer_key.binding_id, THREAD_UUID, &peer_key.job_id,
                            ).unwrap().unwrap();
                            assert_eq!(
                                visible.provider_health_views.iter().any(|view| view.live),
                                !terminal
                            );
                            assert_eq!(
                                request["params"]["input"][0]["text"],
                                threadlane::format_coworker_planning_wakeup_message(
                                    &peer_key.job_id
                                )
                                .unwrap()
                            );
                            let raw = request.to_string();
                            assert!(!raw.contains("retrying in"));
                            assert!(!raw.contains(SECRET_SENTINEL));
                            if !unknown || terminal {
                                ws.send(Message::Text(
                                    json!({"id": request["id"], "result": {
                                        "turn": {"id": "0196ed00-0000-7000-8000-0000000000aa",
                                            "status": "inProgress"}
                                    }})
                                    .to_string()
                                    .into(),
                                ))
                                .await
                                .unwrap();
                            }
                            if !terminal {
                                fs::write(&release, b"release\n").unwrap();
                            }
                            break;
                        }
                        method => panic!("unexpected health callback {method}"),
                    };
                    ws.send(Message::Text(
                        json!({"id": request["id"], "result": result})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
                }
            }
        });
        let outcome = tokio::time::timeout(
            Duration::from_secs(30),
            Box::pin(driver.execute_command(&fixture.binding_id, THREAD_UUID, &command.command_id)),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(outcome.command.status, PlanningCommandStatus::Completed);
        let first = PlanningNoticeStore::inspect(&notices, &key, &command.command_digest).unwrap();
        assert_eq!(first.attempt_count, expected_calls);
        let first = first.latest.unwrap();
        assert_eq!(
            first.terminal_kind,
            PlanningNoticeTerminalKind::DiagnosticAvailable
        );
        assert_eq!(
            first.status,
            if unknown {
                NoticeDeliveryStatus::Unknown
            } else {
                NoticeDeliveryStatus::Submitted
            }
        );
        let terminal = driver
            .notify_terminal_command_if_configured(
                &fixture.binding_id,
                THREAD_UUID,
                &command.command_id,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            terminal.terminal_kind,
            PlanningNoticeTerminalKind::Completed
        );
        assert_ne!(terminal.fingerprint, first.fingerprint);
        assert_eq!(terminal.attempt_count, expected_calls + 1);
        let duplicate = driver
            .notify_terminal_command_if_configured(
                &fixture.binding_id,
                THREAD_UUID,
                &command.command_id,
            )
            .await
            .unwrap()
            .unwrap();
        assert!(duplicate.was_duplicate);
        assert_eq!(duplicate.attempt_count, expected_calls + 1);
        let replay = driver
            .execute_command(&fixture.binding_id, THREAD_UUID, &command.command_id)
            .await
            .unwrap();
        assert!(replay.was_already_completed);
        assert_eq!(
            fs::read_to_string(&fixture.worker_marker)
                .unwrap()
                .lines()
                .count(),
            expected_calls
        );
        tokio::time::timeout(Duration::from_secs(5), peer)
            .await
            .unwrap()
            .unwrap();
        fixture.shutdown().await;
    }
}

/// Waits only inside a bounded fake-provider test; production observation has no polling loop.
async fn wait_for_early_health(
    mcp: &PairedMcpServer,
    command_id: &str,
) -> PlanningCommandDetailResponse {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        let detail: PlanningCommandDetailResponse =
            invoke_mcp_ok(mcp, "planning_command", json!({"command_id": command_id})).await;
        if detail
            .command
            .provider_health
            .iter()
            .any(|h| h.snapshot.retry_count > 0)
        {
            assert!(!detail.command.provider_health[0].terminal_observed);
            return detail;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "missing early scoped diagnostic while provider is held"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn planning_first_closed_lost_or_truncated_hint_is_history_while_cleanup_is_held() {
    use futures_util::poll;
    use threadlane::NativeToolOutputConfig;
    use threadlane_native::coworker_notice::{
        planning_notice_dir, PlanningNoticeConfig, PlanningNoticeStore,
    };
    use threadlane_native::coworker_workflow::{
        OrchestrateWorkBriefPlanRequest, ReservePlanningCallRequest,
    };
    use threadlane_native::planning_driver::{
        get_planning_command_summary_readonly, open_planning_driver_for_plans_dir,
    };
    use threadlane_native::provider_health::{supervise_health, PlanningHealthOwner};
    use threadlane_workers::antigravity_diagnostics::AntigravityDiagnosticParser;
    use threadlane_workers::provider_health::ProviderDiagnosticUnavailableReason as Reason;

    for role in [WorkflowRole::Architect, WorkflowRole::Orchestrator] {
        for reason in [
            Reason::SourceClosed,
            Reason::SourceLost,
            Reason::ResourceLimit,
        ] {
            let mut fixture = GatewayFixture::start().await;
            let now = current_wall_clock_ms().unwrap();
            let grant_id = fixture.create_grant("closed-hint-grant", now - 1000, now + 3_600_000);
            let prep: PrepareWorkResponse = invoke_mcp_ok(
                &fixture.enrolled_mcp(),
                "prepare_work",
                json!({
                    "grant_id": grant_id, "request_key": "closed-hint-prep",
                    "summary": "Bounded final drain", "confirmed_requirements": ["Read src/lib.rs"]
                }),
            )
            .await;
            let driver =
                open_planning_driver_for_plans_dir(&fixture.plans_dir, &fixture.registry_dir)
                    .unwrap();
            let store = driver.command_store();
            let ledger = store
                .workflow_store()
                .read_binding_ledger(&fixture.binding_id)
                .unwrap();
            let grant = ledger
                .planning_grants
                .iter()
                .find(|grant| grant.grant_id == grant_id)
                .unwrap();
            let request = if role == WorkflowRole::Architect {
                EnqueuePlanningCommandRequest::from_architect(RunArchitectAdviceRequest {
                    binding_id: fixture.binding_id.clone(),
                    thread_uuid: THREAD_UUID.into(),
                    grant_id: grant_id.clone(),
                    request_key: "closed-hint-call".into(),
                    expected_ledger_revision: None,
                    difficulty: Some(Difficulty::Hard),
                    context_dependencies: ArchitectContextDependencies {
                        project_base: prep.project_base.clone(),
                        work_brief_digest: prep.work_brief_digest.clone(),
                        plan_digest: None,
                        policy_digest: grant.policy_digest.clone(),
                        dependency_file_blobs:
                            CoworkerPlanningService::compute_dependency_file_blobs(
                                &prep.project_base,
                                &["src/lib.rs".into()],
                            )
                            .unwrap(),
                        interface_ownership_digests: BTreeMap::new(),
                        accepted_predecessor_trees: BTreeMap::new(),
                    },
                    question_summary: "Inspect final drain metadata".into(),
                    prior_advice_context: Vec::new(),
                    accept_into_brief: false,
                    current_time_ms: now,
                })
            } else {
                EnqueuePlanningCommandRequest::from_orchestrator(OrchestrateWorkBriefPlanRequest {
                    binding_id: fixture.binding_id.clone(),
                    thread_uuid: THREAD_UUID.into(),
                    grant_id: grant_id.clone(),
                    job_id: prep.job_id.clone(),
                    request_key: "closed-hint-call".into(),
                    expected_ledger_revision: None,
                    difficulty: Some(Difficulty::Hard),
                    plan_id: None,
                    expected_plan_revision: 0,
                    architect_advice_ids: Vec::new(),
                    max_architect_rounds: 0,
                    architect_difficulty: None,
                    current_time_ms: now,
                })
            };
            let queued = store.enqueue_command(&request).unwrap();
            let key = queued.ownership_key().unwrap();
            let guard = store
                .ownership_store()
                .try_acquire_job(&key, now)
                .unwrap()
                .into_guard()
                .unwrap();
            let command = store
                .claim_running(&guard, &key.binding_id, THREAD_UUID, &key.job_id, None)
                .unwrap();
            let reservation = store
                .workflow_store()
                .reserve_planning_call(&ReservePlanningCallRequest {
                    binding_id: key.binding_id.clone(),
                    thread_uuid: THREAD_UUID.into(),
                    grant_id: grant_id.clone(),
                    request_key: command.service_request_key.clone(),
                    expected_ledger_revision: None,
                    role,
                    difficulty: command.payload.difficulty(),
                    selected_choice: command.selected_choice.clone(),
                    context_digest: command.context_digest.clone(),
                    call_project_base: Some(command.call_project_base.clone()),
                    current_time_ms: now,
                })
                .unwrap();
            let owner = PlanningHealthOwner::new(store, &guard, &command).unwrap();
            let sink = owner.invocation(&reservation).unwrap();
            let directory = planning_notice_dir(&fixture.plans_dir);
            create_private_dir(&directory);
            let socket = directory.join("closed-hint.sock");
            // A real listening endpoint stays available throughout the held cleanup interval.
            let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
            listener.set_nonblocking(true).unwrap();
            fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
            PlanningNoticeStore::new(&directory)
                .unwrap()
                .configure(
                    PlanningNoticeConfig::new(
                        key.clone(),
                        command.command_digest.clone(),
                        NativeToolOutputConfig::new_user_message_steer(
                            socket,
                            THREAD_UUID.into(),
                            fixture.repo_dir.clone(),
                        ),
                    )
                    .unwrap(),
                )
                .unwrap();
            let mut parser = AntigravityDiagnosticParser::new();
            let quiet = parser.snapshot();
            parser.ingest_at(0, b"I0101 00:00:01.000000 1 run.go:315] Run: attempt 7 failed (stream reading error: stream idle timeout (5m0s): context deadline exceeded), retrying in 4s.\n", 10);
            parser.unavailable(reason, 11);
            // Final drain retains SourceLost/ResourceLimit rather than overwriting the reason.
            parser.unavailable(Reason::SourceClosed, 12);
            let hint = parser.snapshot();
            assert_eq!(hint.unavailable_reason, Some(reason));
            assert!(hint.is_valid());
            let expected = hint.clone();
            let (drain_tx, drain_rx) = tokio::sync::oneshot::channel();
            let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
            let mut supervision = Box::pin(supervise_health(
                Some(&sink),
                |observer| async move {
                    observer.send_replace(Some(quiet));
                    drain_rx.await.unwrap();
                    // The FIRST hint is already closed/lost/truncated on watch publication.
                    observer.send_replace(Some(hint.clone()));
                    cleanup_rx.await.unwrap();
                    if role == WorkflowRole::Architect {
                        Err("injected provider error after final drain")
                    } else {
                        Ok((false, hint))
                    }
                },
                |result| Some(&result.1),
            ));
            // Fixed polls drive both watch consumers while oneshots prevent provider completion.
            assert!(poll!(supervision.as_mut()).is_pending());
            assert!(poll!(supervision.as_mut()).is_pending());
            assert!(poll!(supervision.as_mut()).is_pending());
            let quiet_view = get_planning_command_summary_readonly(
                &fixture.plans_dir,
                &fixture.registry_dir,
                &key.binding_id,
                THREAD_UUID,
                &key.job_id,
            )
            .unwrap()
            .unwrap();
            assert!(quiet_view.provider_health_views[0].live);
            assert!(quiet_view.provider_health_views[0]
                .record
                .snapshot
                .current_failure
                .is_none());

            drain_tx.send(()).unwrap();
            assert!(poll!(supervision.as_mut()).is_pending());
            assert!(poll!(supervision.as_mut()).is_pending());
            assert!(poll!(supervision.as_mut()).is_pending());
            guard.validate_before_effect(&key).unwrap();
            let held = get_planning_command_summary_readonly(
                &fixture.plans_dir,
                &fixture.registry_dir,
                &key.binding_id,
                THREAD_UUID,
                &key.job_id,
            )
            .unwrap()
            .unwrap();
            assert_eq!(held.status, PlanningCommandStatus::Running);
            assert_eq!(held.provider_health.len(), 1);
            assert_eq!(held.provider_health[0].snapshot, expected);
            assert!(!held.provider_health[0].terminal_observed);
            assert!(!held.provider_health_views[0].live);
            let notices =
                PlanningNoticeStore::inspect(&directory, &key, &command.command_digest).unwrap();
            assert_eq!(
                notices.attempt_count, 0,
                "no new early intent for {role:?}/{reason:?}"
            );
            assert!(notices.latest.is_none());
            assert_eq!(
                listener.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );

            cleanup_tx.send(()).unwrap();
            let outcome = tokio::time::timeout(Duration::from_secs(5), supervision)
                .await
                .unwrap();
            if role == WorkflowRole::Architect {
                assert_eq!(outcome, Err("injected provider error after final drain"));
            } else {
                let outcome = outcome.unwrap();
                assert!(
                    !outcome.0,
                    "coverage cannot replace the non-success provider result"
                );
                assert_eq!(outcome.1, expected);
            }
            let terminal = store
                .get_command(&key.binding_id, THREAD_UUID, &key.job_id)
                .unwrap()
                .unwrap();
            assert_eq!(terminal.provider_health[0].snapshot, expected);
            assert!(terminal.provider_health[0].terminal_observed);
            assert!(terminal.provider_health[0]
                .snapshot
                .current_failure
                .is_some());
            assert!(terminal.provider_health[0].snapshot.recovery.is_none());
            assert_eq!(terminal.provider_health[0].snapshot.retry_count, 1);
            assert_eq!(
                terminal.provider_health[0].snapshot.retry_high_water,
                Some(7)
            );
            assert_eq!(
                PlanningNoticeStore::inspect(&directory, &key, &command.command_digest)
                    .unwrap()
                    .attempt_count,
                0
            );
            assert_eq!(
                listener.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
            drop(sink);
            drop(guard);
            fixture.shutdown().await;
        }
    }
}

#[tokio::test]
async fn consult_advisor_persists_canonical_identity_refuses_legacy_authority_and_replays_without_redispatch(
) {
    let mut fixture = GatewayFixture::start().await;
    let mcp = fixture.enrolled_mcp();
    let now_ms = current_wall_clock_ms().expect("wall clock");
    let advisor_grant = fixture.create_grant_with_policy(
        "req-grant-advisor",
        advisor_policy(),
        now_ms.saturating_sub(5_000),
        now_ms + 3_600_000,
    );
    let legacy_grant = fixture.create_grant(
        "req-grant-legacy-architect",
        now_ms.saturating_sub(5_000),
        now_ms + 3_600_000,
    );

    let prep: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": advisor_grant.clone(),
            "request_key": "req-prep-advisor",
            "summary": "Advisor canonical identity check",
            "confirmed_requirements": ["Persist the current advisor identity"]
        }),
    )
    .await;
    assert!(prep.ready_for_execution_planning);

    let consult_args = |grant_id: &str, request_key: &str| {
        json!({
            "grant_id": grant_id,
            "job_id": prep.job_id.clone(),
            "request_key": request_key,
            "question": "Which advisory identity must the canonical path store?",
            "dependency_files": ["src/lib.rs"],
            "difficulty": "hard"
        })
    };

    // A pre-transition architect-only grant is never silently retargeted into the canonical
    // advisor identity; the caller must use the retained legacy entrypoint.
    let legacy_err = invoke_mcp_err_code(
        &mcp,
        "consult_advisor",
        consult_args(&legacy_grant, "req-consult-advisor-legacy"),
    )
    .await;
    assert_eq!(legacy_err, "validation_failed");
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 0);

    // A stale caller revision is refused before any provider effect.
    let mut stale_args = consult_args(&advisor_grant, "req-consult-advisor-stale");
    stale_args["expected_ledger_revision"] = json!(9_999);
    let stale_err = invoke_mcp_err_code(&mcp, "consult_advisor", stale_args).await;
    assert_eq!(stale_err, "validation_failed");
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 0);

    // Canonical path stores the current Advisor identity in the command, reservation, and
    // private artifact while retaining the compatibility result/record type names.
    let submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "consult_advisor",
        consult_args(&advisor_grant, "req-consult-advisor"),
    )
    .await;
    assert_eq!(submit.role, WorkflowRole::Advisor);
    let detail = wait_for_command_completion(&mcp, &submit.command_id).await;
    assert_eq!(
        detail.status,
        PlanningCommandStatus::Completed,
        "advisor_detail did not complete cleanly: {}",
        format_command_detail_context(&detail)
    );
    assert_eq!(detail.role, WorkflowRole::Advisor);
    assert_eq!(
        detail
            .structured_architect_advice
            .as_ref()
            .expect("structured advisor advice")
            .decision_summary,
        "Keep strict planning command boundary"
    );
    let (advice_id, reservation_id, report_artifact_id) = match detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::ArchitectAdvice {
            advice_id,
            reservation_id,
            report_artifact_id,
            ..
        }) => (
            advice_id.clone(),
            reservation_id.clone(),
            report_artifact_id.clone(),
        ),
        other => panic!("unexpected advisor result_ref: {other:?}"),
    };

    let store = CoworkerWorkflowStore::new(&fixture.plans_dir).expect("reopen workflow store");
    let ledger = store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read workflow ledger");
    let grant = ledger
        .planning_grants
        .iter()
        .find(|grant| grant.grant_id == advisor_grant)
        .expect("advisor grant");
    assert!(
        grant.policy.explicitly_authorizes_advisor(),
        "the new grant policy must explicitly name the advisor role"
    );
    assert_eq!(grant.reservations.len(), 1);
    assert_eq!(grant.reservations[0].role, WorkflowRole::Advisor);
    assert_eq!(grant.reservations[0].reservation_id, reservation_id);
    assert_eq!(grant.usage.reserved_architect_calls, 1);
    assert_eq!(grant.usage.reserved_total_calls, 1);
    let advice = ledger
        .architect_advice
        .iter()
        .find(|advice| advice.advice_id == advice_id)
        .expect("stored advisor advice record");
    assert_eq!(advice.status, ArchitectDecisionStatus::Proposed);

    let service = CoworkerPlanningService::new(
        store.clone(),
        &fixture.registry_dir,
        planning_artifacts_dir(&fixture.plans_dir),
    )
    .expect("open planning service for artifact inspection");
    let artifact = service
        .read_planning_artifact(&report_artifact_id)
        .expect("read canonical advisor artifact");
    assert_eq!(artifact.role, WorkflowRole::Advisor);
    assert_eq!(artifact.reservation_id, reservation_id);
    assert_eq!(artifact.request_key, submit.command.service_request_key);
    assert!(matches!(
        artifact.outcome,
        threadlane_native::coworker_workflow::PlanningArtifactOutcomeKind::ArchitectAdvice { .. }
    ));

    // Exact replay returns the immutable completed receipt without another provider call.
    let calls_before_replay = provider_invocation_count(&fixture.worker_marker);
    assert_eq!(calls_before_replay, 1);
    let repeat: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "consult_advisor",
        consult_args(&advisor_grant, "req-consult-advisor"),
    )
    .await;
    assert_eq!(repeat.command_id, submit.command_id);
    assert_eq!(repeat.status, PlanningCommandStatus::Completed);
    assert_eq!(
        repeat.submission_status,
        PlanningCommandStartStatus::AlreadyCompleted
    );
    assert!(!repeat.launched);
    assert_eq!(
        provider_invocation_count(&fixture.worker_marker),
        calls_before_replay
    );

    // A different chat/thread scope cannot replay the stored command or reach the provider.
    let other_scope_gateway = CoworkerPlanningGateway::new(
        &fixture.state_dir,
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &fixture.binding_id,
        "0196ed00-0000-7000-8000-0000000000b2",
    )
    .expect("create cross-thread planning gateway")
    .with_sibling_anchor_exe(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-flow")));
    let scope_err = other_scope_gateway
        .consult_advisor(&ConsultArchitectParams {
            grant_id: advisor_grant.clone(),
            job_id: prep.job_id.clone(),
            request_key: "req-consult-advisor-cross-scope".to_string(),
            question: "Cross-scope advisor attempt".to_string(),
            dependency_files: vec!["src/lib.rs".to_string()],
            difficulty: Some(Difficulty::Hard),
            prior_advice_ids: Vec::new(),
            expected_ledger_revision: None,
        })
        .await
        .expect_err("cross-thread advisor scope must be refused");
    assert!(
        [
            NativeErrorCode::ValidationFailed.as_str(),
            NativeErrorCode::SecurityViolation.as_str(),
        ]
        .contains(&scope_err.code()),
        "cross-thread advisor scope must fail closed, got {:?}",
        scope_err.code()
    );
    assert_eq!(
        provider_invocation_count(&fixture.worker_marker),
        calls_before_replay
    );

    fixture.shutdown().await;
}

async fn invoke_mcp_err_code_and_message(
    mcp: &PairedMcpServer,
    tool: &str,
    args: Value,
) -> (String, String) {
    let err = mcp
        .invoke_tool_json(tool, args, None)
        .await
        .expect_err("expected MCP tool error")
        .structured_content
        .expect("error structured_content");
    let code = err["error"]["code"]
        .as_str()
        .expect("error.code string")
        .to_string();
    let message = err["error"]["message"]
        .as_str()
        .expect("error.message string")
        .to_string();
    (code, message)
}

#[tokio::test]
async fn consult_architect_refuses_new_request_on_advisor_only_grant_with_zero_side_effects_and_preserves_exact_replay(
) {
    use threadlane_native::coworker_workflow::{PlanningArtifactOutcomeKind, PlanningGrantStatus};
    use threadlane_native::planning_driver::open_planning_driver_for_plans_dir;

    let mut fixture = GatewayFixture::start().await;
    // Commit a second repository file so dependency_files mutation tests use two valid blobs.
    fs::write(
        fixture.repo_dir.join("src/extra.rs"),
        "pub fn gateway_extra() -> u32 { 22 }\n",
    )
    .expect("write src/extra.rs");
    for args in [
        &["add", "src/extra.rs"][..],
        &["commit", "-m", "add extra dependency file", "--quiet"][..],
    ] {
        run_sanitized_git(&fixture.repo_dir, args, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("commit src/extra.rs");
    }
    let repo_dir = &fixture.repo_dir;
    let commit = resolve_exact_commit_sha(repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve HEAD commit");
    let tree = resolve_exact_tree_sha(repo_dir, &commit, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve HEAD tree");
    fixture.project_base = CanonicalProjectBaseRef {
        repository_path: repo_dir.to_string_lossy().into_owned(),
        base_commit: commit,
        base_tree_sha: tree,
    };

    let mcp = fixture.enrolled_mcp();
    let now_ms = current_wall_clock_ms().expect("wall clock");
    let advisor_grant = fixture.create_grant_with_policy(
        "req-grant-advisor-only-1",
        advisor_policy(),
        now_ms.saturating_sub(5_000),
        now_ms + 3_600_000,
    );
    let advisor_grant_alt = fixture.create_grant_with_policy(
        "req-grant-advisor-only-alt",
        advisor_policy(),
        now_ms.saturating_sub(5_000),
        now_ms + 3_600_000,
    );
    let legacy_grant = fixture.create_grant(
        "req-grant-pre-transition-architect",
        now_ms.saturating_sub(5_000),
        now_ms + 3_600_000,
    );

    let wf_store =
        CoworkerWorkflowStore::new(&fixture.plans_dir).expect("open CoworkerWorkflowStore");
    let advisor_grant_one_call = wf_store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: fixture.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "req-grant-advisor-only-one-call".to_string(),
            expected_ledger_revision: None,
            project_base: fixture.project_base.clone(),
            profile_id: PROFILE_ID.to_string(),
            registry_revision: 3,
            policy: advisor_policy(),
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget: PlanningCallBudget {
                max_total_calls: 1,
                max_orchestrator_calls: 0,
                max_architect_calls: 1,
                max_timeout_ms_per_call: 60_000,
                max_output_bytes_per_call: 65_536,
            },
            created_at_ms: now_ms.saturating_sub(5_000),
            expires_at_ms: now_ms + 3_600_000,
            operator_attested: true,
        })
        .expect("create single-call advisor-only grant")
        .grant_id;

    let prep_primary: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": advisor_grant.clone(),
            "request_key": "req-prep-primary-role-check",
            "summary": "Verify exact role boundary on consult_architect",
            "confirmed_requirements": ["Refuse new legacy architect consults under advisor-only grants"]
        }),
    )
    .await;
    let prep_other: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": advisor_grant.clone(),
            "request_key": "req-prep-other-role-check",
            "summary": "Distinct job for replay isolation",
            "confirmed_requirements": ["Prevent cross-job legacy replay bypass"]
        }),
    )
    .await;
    assert_ne!(prep_primary.job_id, prep_other.job_id);

    // 1. Wrong current entrypoint: a NEW legacy `consult_architect` request on an advisor-only
    //    grant must be refused with clear guidance and ZERO side effects.
    let plans_entries_before = directory_entries(&fixture.plans_dir);
    let ledger_before = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read workflow ledger before refusal");
    assert!(!planning_commands_dir(&fixture.plans_dir).exists());
    assert!(!planning_artifacts_dir(&fixture.plans_dir).exists());

    let (mcp_code, mcp_message) = invoke_mcp_err_code_and_message(
        &mcp,
        "consult_architect",
        json!({
            "grant_id": advisor_grant.clone(),
            "job_id": prep_primary.job_id.clone(),
            "request_key": "req-wrong-entrypoint-mcp",
            "question": "Fresh legacy call under advisor-only authority",
            "dependency_files": ["src/lib.rs"],
            "difficulty": "hard"
        }),
    )
    .await;
    assert_eq!(mcp_code, "validation_failed");
    assert!(
        mcp_message.contains("consult_advisor") && mcp_message.contains("advisor"),
        "expected clear guidance directing caller to consult_advisor, got: {mcp_message}"
    );

    let gw_err = fixture
        .gateway()
        .consult_architect(&ConsultArchitectParams {
            grant_id: advisor_grant.clone(),
            job_id: prep_primary.job_id.clone(),
            request_key: "req-wrong-entrypoint-gw".to_string(),
            expected_ledger_revision: None,
            question: "Fresh legacy gateway call under advisor-only authority".to_string(),
            dependency_files: vec!["src/lib.rs".to_string()],
            prior_advice_ids: Vec::new(),
            difficulty: Some(Difficulty::Hard),
        })
        .await
        .expect_err("gateway consult_architect must refuse new request on advisor-only grant");
    assert_eq!(gw_err.error_code(), NativeErrorCode::ValidationFailed);
    assert!(
        gw_err.message.contains("consult_advisor"),
        "expected clear guidance directing caller to consult_advisor, got: {}",
        gw_err.message
    );

    assert!(!planning_commands_dir(&fixture.plans_dir).exists());
    assert!(!planning_artifacts_dir(&fixture.plans_dir).exists());
    assert_eq!(directory_entries(&fixture.plans_dir), plans_entries_before);
    let ledger_after_refusal = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read workflow ledger after refusal");
    assert_eq!(
        ledger_after_refusal.ledger_revision,
        ledger_before.ledger_revision
    );
    let advisor_grant_after_refusal = ledger_after_refusal
        .planning_grants
        .iter()
        .find(|g| g.grant_id == advisor_grant)
        .expect("advisor grant");
    assert!(advisor_grant_after_refusal.reservations.is_empty());
    assert_eq!(advisor_grant_after_refusal.usage.reserved_total_calls, 0);
    assert_eq!(
        advisor_grant_after_refusal.usage.reserved_architect_calls,
        0
    );
    assert!(ledger_after_refusal.architect_advice.is_empty());
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 0);

    // 2. Canonical `consult_advisor` good path on the advisor-only grant persists `Advisor`, and
    //    `consult_architect` cannot replay that canonical `Advisor` command key.
    let canonical_args = json!({
        "grant_id": advisor_grant.clone(),
        "job_id": prep_primary.job_id.clone(),
        "request_key": "req-canonical-advisor-good",
        "question": "Canonical advisor check under advisor-only authority",
        "dependency_files": ["src/lib.rs"],
        "difficulty": "hard"
    });
    let canonical_submit: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "consult_advisor", canonical_args.clone()).await;
    assert_eq!(canonical_submit.role, WorkflowRole::Advisor);
    let canonical_detail = wait_for_command_completion(&mcp, &canonical_submit.command_id).await;
    assert_eq!(
        canonical_detail.status,
        PlanningCommandStatus::Completed,
        "canonical_detail did not complete cleanly: {}",
        format_command_detail_context(&canonical_detail)
    );
    assert_eq!(canonical_detail.role, WorkflowRole::Advisor);
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 1);

    let (cross_role_code, _) =
        invoke_mcp_err_code_and_message(&mcp, "consult_architect", canonical_args).await;
    assert_eq!(cross_role_code, "validation_failed");
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 1);

    // 3. Pre-transition `architect` grant keeps its valid legacy `consult_architect` path and exact
    //    replay, while still refusing `consult_advisor`.
    let legacy_args = json!({
        "grant_id": legacy_grant.clone(),
        "job_id": prep_primary.job_id.clone(),
        "request_key": "req-legacy-architect-good",
        "question": "Pre-transition architect consultation",
        "dependency_files": ["src/lib.rs"],
        "difficulty": "hard"
    });
    let legacy_submit: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "consult_architect", legacy_args.clone()).await;
    assert_eq!(legacy_submit.role, WorkflowRole::Architect);
    let legacy_detail = wait_for_command_completion(&mcp, &legacy_submit.command_id).await;
    assert_eq!(
        legacy_detail.status,
        PlanningCommandStatus::Completed,
        "legacy_detail did not complete cleanly: {}",
        format_command_detail_context(&legacy_detail)
    );
    assert_eq!(legacy_detail.role, WorkflowRole::Architect);
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 2);

    let legacy_replay: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "consult_architect", legacy_args).await;
    assert_eq!(legacy_replay.command_id, legacy_submit.command_id);
    assert_eq!(legacy_replay.role, WorkflowRole::Architect);
    assert_eq!(
        legacy_replay.submission_status,
        PlanningCommandStartStatus::AlreadyCompleted
    );
    assert!(!legacy_replay.launched);
    assert!(!legacy_replay.submission_requested);
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 2);

    let legacy_advisor_err = invoke_mcp_err_code(
        &mcp,
        "consult_advisor",
        json!({
            "grant_id": legacy_grant.clone(),
            "job_id": prep_primary.job_id.clone(),
            "request_key": "req-advisor-on-legacy-must-fail",
            "question": "Must refuse canonical advisor on architect-only grant",
            "dependency_files": ["src/lib.rs"],
            "difficulty": "hard"
        }),
    )
    .await;
    assert_eq!(legacy_advisor_err, "validation_failed");
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 2);

    // 4. Seed an already-accepted historical confirmed (`Completed`) legacy `Architect` command on
    //    the 1-call advisor-only grant (which exhausts its budget upon completion) via the lower
    //    command store + driver, simulating persisted pre-fix history.
    let driver = open_planning_driver_for_plans_dir(&fixture.plans_dir, &fixture.registry_dir)
        .expect("open planning driver");
    let cmd_store = driver.command_store();
    let wf_ledger_before_seed = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read workflow ledger before seeding historical command");
    let one_call_grant = wf_ledger_before_seed
        .planning_grants
        .iter()
        .find(|g| g.grant_id == advisor_grant_one_call)
        .expect("single-call advisor grant");
    let dep_blobs = CoworkerPlanningService::compute_dependency_file_blobs(
        &prep_primary.project_base,
        &["src/lib.rs".to_string()],
    )
    .expect("compute src/lib.rs dependency blob");
    let historical_key = "req-historical-confirmed-architect-on-advisor-grant";
    let historical_question = "Historical confirmed architect advice accepted before role check";
    let seeded_cmd = cmd_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_architect(
            RunArchitectAdviceRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: advisor_grant_one_call.clone(),
                request_key: historical_key.to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                context_dependencies: ArchitectContextDependencies {
                    project_base: prep_primary.project_base.clone(),
                    work_brief_digest: prep_primary.work_brief_digest.clone(),
                    plan_digest: None,
                    policy_digest: one_call_grant.policy_digest.clone(),
                    dependency_file_blobs: dep_blobs.clone(),
                    interface_ownership_digests: BTreeMap::new(),
                    accepted_predecessor_trees: BTreeMap::new(),
                },
                question_summary: historical_question.to_string(),
                prior_advice_context: Vec::new(),
                accept_into_brief: false,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        ))
        .expect("enqueue historical architect command");
    assert_eq!(seeded_cmd.role, WorkflowRole::Architect);
    let seeded_exec = tokio::time::timeout(
        Duration::from_secs(30),
        Box::pin(driver.execute_command(&fixture.binding_id, THREAD_UUID, &seeded_cmd.command_id)),
    )
    .await
    .expect("historical command execution timeout")
    .expect("historical command execution outcome");
    assert_eq!(seeded_exec.command.status, PlanningCommandStatus::Completed);
    assert_eq!(seeded_exec.command.role, WorkflowRole::Architect);
    let (seeded_reservation_id, seeded_artifact_id) = match seeded_exec.command.result_ref.as_ref()
    {
        Some(PlanningCommandResultRef::ArchitectAdvice {
            reservation_id,
            report_artifact_id,
            ..
        }) => (reservation_id.clone(), report_artifact_id.clone()),
        other => panic!("unexpected historical result_ref: {other:?}"),
    };

    let ledger_after_seed = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read workflow ledger after historical seed");
    let exhausted_one_call = ledger_after_seed
        .planning_grants
        .iter()
        .find(|g| g.grant_id == advisor_grant_one_call)
        .expect("exhausted single-call advisor grant");
    assert_eq!(exhausted_one_call.status, PlanningGrantStatus::Exhausted);
    assert_eq!(exhausted_one_call.reservations.len(), 1);
    assert_eq!(
        exhausted_one_call.reservations[0].role,
        WorkflowRole::Architect
    );
    assert_eq!(
        exhausted_one_call.reservations[0].reservation_id,
        seeded_reservation_id
    );
    let planning_service = CoworkerPlanningService::new(
        wf_store.clone(),
        &fixture.registry_dir,
        planning_artifacts_dir(&fixture.plans_dir),
    )
    .expect("open planning service");
    let seeded_artifact = planning_service
        .read_planning_artifact(&seeded_artifact_id)
        .expect("read historical architect artifact");
    assert_eq!(seeded_artifact.role, WorkflowRole::Architect);
    assert!(matches!(
        seeded_artifact.outcome,
        PlanningArtifactOutcomeKind::ArchitectAdvice { .. }
    ));

    let calls_after_seed = provider_invocation_count(&fixture.worker_marker);
    assert_eq!(calls_after_seed, 3);
    let cmd_ledger_before_replay = cmd_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read command ledger before replay");

    // 5. Genuine EXACT immutable replay of the confirmed historical `Architect` command on the
    //    advisor-only (now Exhausted) grant succeeds via both Paired MCP and direct gateway with
    //    ZERO new provider effects or ledger writes.
    let exact_historical_args = json!({
        "grant_id": advisor_grant_one_call,
        "job_id": prep_primary.job_id,
        "request_key": historical_key,
        "question": historical_question,
        "dependency_files": ["src/lib.rs"],
        "difficulty": "hard"
    });
    let mcp_replay: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "consult_architect", exact_historical_args).await;
    assert_eq!(mcp_replay.command_id, seeded_cmd.command_id);
    assert_eq!(mcp_replay.role, WorkflowRole::Architect);
    assert_eq!(mcp_replay.status, PlanningCommandStatus::Completed);
    assert_eq!(
        mcp_replay.submission_status,
        PlanningCommandStartStatus::AlreadyCompleted
    );
    assert!(!mcp_replay.submission_requested);
    assert!(!mcp_replay.launched);
    assert!(mcp_replay.pid_hint.is_none());

    let gw_replay = fixture
        .gateway()
        .consult_architect(&ConsultArchitectParams {
            grant_id: advisor_grant_one_call.clone(),
            job_id: prep_primary.job_id.clone(),
            request_key: historical_key.to_string(),
            expected_ledger_revision: None,
            question: historical_question.to_string(),
            dependency_files: vec!["src/lib.rs".to_string()],
            prior_advice_ids: Vec::new(),
            difficulty: Some(Difficulty::Hard),
        })
        .await
        .expect("exact gateway replay of confirmed historical architect command must succeed");
    assert_eq!(gw_replay.command_id, seeded_cmd.command_id);
    assert_eq!(gw_replay.role, WorkflowRole::Architect);
    assert_eq!(
        gw_replay.submission_status,
        PlanningCommandStartStatus::AlreadyCompleted
    );
    assert!(!gw_replay.launched);
    assert_eq!(
        provider_invocation_count(&fixture.worker_marker),
        calls_after_seed
    );
    let cmd_ledger_after_replay = cmd_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read command ledger after replay");
    assert_eq!(
        cmd_ledger_after_replay.revision,
        cmd_ledger_before_replay.revision
    );
    assert_eq!(
        cmd_ledger_after_replay.commands.len(),
        cmd_ledger_before_replay.commands.len()
    );
    let wf_ledger_after_replay = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read workflow ledger after replay");
    assert_eq!(
        wf_ledger_after_replay.ledger_revision,
        ledger_after_seed.ledger_revision
    );

    // 6. No bypass for mutated payload, changed key, changed job, changed grant, or foreign thread.
    let mutation_cases = [
        (
            "mutated question",
            json!({
                "grant_id": advisor_grant_one_call,
                "job_id": prep_primary.job_id,
                "request_key": historical_key,
                "question": "Mutated question trying to reuse historical key",
                "dependency_files": ["src/lib.rs"],
                "difficulty": "hard"
            }),
        ),
        (
            "mutated dependency_files",
            json!({
                "grant_id": advisor_grant_one_call,
                "job_id": prep_primary.job_id,
                "request_key": historical_key,
                "question": historical_question,
                "dependency_files": ["src/lib.rs", "src/extra.rs"],
                "difficulty": "hard"
            }),
        ),
        (
            "mutated difficulty",
            json!({
                "grant_id": advisor_grant_one_call,
                "job_id": prep_primary.job_id,
                "request_key": historical_key,
                "question": historical_question,
                "dependency_files": ["src/lib.rs"],
                "difficulty": "medium"
            }),
        ),
        (
            "mutated expected_ledger_revision",
            json!({
                "grant_id": advisor_grant_one_call,
                "job_id": prep_primary.job_id,
                "request_key": historical_key,
                "expected_ledger_revision": ledger_after_seed.ledger_revision,
                "question": historical_question,
                "dependency_files": ["src/lib.rs"],
                "difficulty": "hard"
            }),
        ),
        (
            "different job_id",
            json!({
                "grant_id": advisor_grant_one_call,
                "job_id": prep_other.job_id,
                "request_key": historical_key,
                "question": historical_question,
                "dependency_files": ["src/lib.rs"],
                "difficulty": "hard"
            }),
        ),
        (
            "different advisor-only grant_id",
            json!({
                "grant_id": advisor_grant_alt,
                "job_id": prep_primary.job_id,
                "request_key": historical_key,
                "question": historical_question,
                "dependency_files": ["src/lib.rs"],
                "difficulty": "hard"
            }),
        ),
        (
            "different legacy grant_id with historical key",
            json!({
                "grant_id": legacy_grant,
                "job_id": prep_primary.job_id,
                "request_key": historical_key,
                "question": historical_question,
                "dependency_files": ["src/lib.rs"],
                "difficulty": "hard"
            }),
        ),
        (
            "new request_key with identical payload on advisor-only grant",
            json!({
                "grant_id": advisor_grant,
                "job_id": prep_primary.job_id,
                "request_key": "req-new-key-must-refuse",
                "question": historical_question,
                "dependency_files": ["src/lib.rs"],
                "difficulty": "hard"
            }),
        ),
    ];
    for (label, args) in mutation_cases {
        let err_code = invoke_mcp_err_code(&mcp, "consult_architect", args).await;
        assert_eq!(
            err_code, "validation_failed",
            "expected validation_failed for `{label}`, got `{err_code}`"
        );
    }

    let foreign_gateway = CoworkerPlanningGateway::new(
        &fixture.state_dir,
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &fixture.binding_id,
        "0196ed00-0000-7000-8000-0000000000b9",
    )
    .expect("create foreign-thread gateway")
    .with_sibling_anchor_exe(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-flow")));
    let foreign_err = foreign_gateway
        .consult_architect(&ConsultArchitectParams {
            grant_id: advisor_grant_one_call,
            job_id: prep_primary.job_id.clone(),
            request_key: historical_key.to_string(),
            expected_ledger_revision: None,
            question: historical_question.to_string(),
            dependency_files: vec!["src/lib.rs".to_string()],
            prior_advice_ids: Vec::new(),
            difficulty: Some(Difficulty::Hard),
        })
        .await
        .expect_err("foreign thread must not replay persisted legacy architect command");
    assert!(
        [
            NativeErrorCode::SecurityViolation.as_str(),
            NativeErrorCode::ValidationFailed.as_str(),
        ]
        .contains(&foreign_err.code()),
        "foreign thread replay must fail closed, got {:?}",
        foreign_err.code()
    );
    assert_eq!(
        provider_invocation_count(&fixture.worker_marker),
        calls_after_seed
    );

    // 7. An unconfirmed (`Queued`) legacy `Architect` command on an active advisor-only grant must
    //    never be launched by `consult_architect` (no new provider effect or reservation).
    let active_advisor_grant_record = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read workflow ledger before queued seed")
        .planning_grants
        .into_iter()
        .find(|g| g.grant_id == advisor_grant)
        .expect("active advisor grant");
    let reservations_before_queued = active_advisor_grant_record.reservations.len();
    let queued_key = "req-unconfirmed-queued-architect-on-advisor";
    let queued_question = "Unconfirmed queued architect command must not launch";
    let queued_cmd = cmd_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_architect(
            RunArchitectAdviceRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: advisor_grant.clone(),
                request_key: queued_key.to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                context_dependencies: ArchitectContextDependencies {
                    project_base: prep_primary.project_base,
                    work_brief_digest: prep_primary.work_brief_digest,
                    plan_digest: None,
                    policy_digest: active_advisor_grant_record.policy_digest,
                    dependency_file_blobs: dep_blobs,
                    interface_ownership_digests: BTreeMap::new(),
                    accepted_predecessor_trees: BTreeMap::new(),
                },
                question_summary: queued_question.to_string(),
                prior_advice_context: Vec::new(),
                accept_into_brief: false,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        ))
        .expect("seed queued architect command");
    assert_eq!(queued_cmd.status, PlanningCommandStatus::Queued);

    let (queued_err_code, queued_err_msg) = invoke_mcp_err_code_and_message(
        &mcp,
        "consult_architect",
        json!({
            "grant_id": advisor_grant,
            "job_id": prep_primary.job_id,
            "request_key": queued_key,
            "question": queued_question,
            "dependency_files": ["src/lib.rs"],
            "difficulty": "hard"
        }),
    )
    .await;
    assert_eq!(queued_err_code, "validation_failed");
    assert!(
        queued_err_msg.contains("consult_advisor"),
        "expected guidance to use consult_advisor for unconfirmed queued command, got: {queued_err_msg}"
    );
    assert_eq!(
        provider_invocation_count(&fixture.worker_marker),
        calls_after_seed
    );
    let queued_after = cmd_store
        .get_command(&fixture.binding_id, THREAD_UUID, &queued_cmd.command_id)
        .expect("read queued command after refusal")
        .expect("queued command remains stored");
    assert_eq!(queued_after.status, PlanningCommandStatus::Queued);
    let reservations_after_queued = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read workflow ledger after queued refusal")
        .planning_grants
        .into_iter()
        .find(|g| g.grant_id == advisor_grant)
        .expect("active advisor grant after queued refusal")
        .reservations
        .len();
    assert_eq!(reservations_after_queued, reservations_before_queued);

    fixture.shutdown().await;
}

#[tokio::test]
async fn consult_mixed_policy_explicit_role_rules_route_each_target_precisely() {
    let mut fixture = GatewayFixture::start().await;
    const MODEL_ID_LEGACY: &str = "model-argon-legacy";
    const PROFILE_MIXED: &str = "profile-coworker-mixed";

    let registry = RegistryStore::open(&fixture.registry_dir).expect("open gateway registry");
    let rev = registry.snapshot().expect("registry snapshot").revision;
    let s_model = registry
        .apply(
            rev,
            RegistryChange::UpsertModel {
                model: ModelDefinition {
                    id: MODEL_ID_LEGACY.to_string(),
                    tool_id: TOOL_ID.to_string(),
                    label: "Gemini 4 Argon Legacy Target".to_string(),
                    model_arg: "gemini-4-argon-high".to_string(),
                    efforts: vec!["high".to_string()],
                    allow_no_effort: false,
                },
            },
        )
        .expect("upsert legacy target model");
    let s_profile = registry
        .apply(
            s_model.revision,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    decision_advisor: None,
                    id: PROFILE_MIXED.to_string(),
                    name: "Mixed advisory policy profile".to_string(),
                    assignments: vec![
                        RoleAssignment {
                            role: WorkflowRole::Orchestrator,
                            difficulty: Some(Difficulty::Hard),
                            choice: ModelChoice {
                                model_id: MODEL_ID.to_string(),
                                effort: Some("high".to_string()),
                            },
                        },
                        RoleAssignment {
                            role: WorkflowRole::Advisor,
                            difficulty: Some(Difficulty::Hard),
                            choice: ModelChoice {
                                model_id: MODEL_ID.to_string(),
                                effort: Some("high".to_string()),
                            },
                        },
                        RoleAssignment {
                            role: WorkflowRole::Architect,
                            difficulty: Some(Difficulty::Medium),
                            choice: ModelChoice {
                                model_id: MODEL_ID_LEGACY.to_string(),
                                effort: Some("high".to_string()),
                            },
                        },
                        RoleAssignment {
                            role: WorkflowRole::Coder,
                            difficulty: Some(Difficulty::Hard),
                            choice: ModelChoice {
                                model_id: MODEL_ID.to_string(),
                                effort: Some("high".to_string()),
                            },
                        },
                    ],
                },
            },
        )
        .expect("upsert mixed advisory profile");
    let registry_lock = fixture.registry_dir.join("registry.lock");
    if registry_lock.exists() {
        fs::remove_file(&registry_lock).expect("remove registry.lock");
    }

    let mixed_policy = ExecutionPolicy {
        schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
        policy_id: "policy-gateway-mixed-advisory".to_string(),
        allow_rules: vec![
            PolicyAllowRule {
                tool_id: TOOL_ID.to_string(),
                model_id: MODEL_ID.to_string(),
                efforts: vec!["high".to_string()],
                allow_no_effort: false,
                roles: vec![WorkflowRole::Orchestrator, WorkflowRole::Advisor],
            },
            PolicyAllowRule {
                tool_id: TOOL_ID.to_string(),
                model_id: MODEL_ID_LEGACY.to_string(),
                efforts: vec!["high".to_string()],
                allow_no_effort: false,
                roles: vec![WorkflowRole::Architect],
            },
        ],
    };
    mixed_policy
        .validate()
        .expect("distinct-target explicit mixed policy must be valid");

    let now_ms = current_wall_clock_ms().expect("wall clock");
    let wf_store =
        CoworkerWorkflowStore::new(&fixture.plans_dir).expect("open CoworkerWorkflowStore");
    let mixed_grant = wf_store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: fixture.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "req-grant-mixed-advisory".to_string(),
            expected_ledger_revision: None,
            project_base: fixture.project_base.clone(),
            profile_id: PROFILE_MIXED.to_string(),
            registry_revision: s_profile.revision,
            policy: mixed_policy,
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget: PlanningCallBudget {
                max_total_calls: 6,
                max_orchestrator_calls: 2,
                max_architect_calls: 4,
                max_timeout_ms_per_call: 60_000,
                max_output_bytes_per_call: 65_536,
            },
            created_at_ms: now_ms.saturating_sub(5_000),
            expires_at_ms: now_ms + 3_600_000,
            operator_attested: true,
        })
        .expect("create mixed-policy planning grant")
        .grant_id;

    let mcp = fixture.enrolled_mcp();
    let prep: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": mixed_grant,
            "request_key": "req-prep-mixed-advisory",
            "summary": "Verify explicit mixed-policy rule precision",
            "confirmed_requirements": ["Route each target strictly to its explicitly authorized role"]
        }),
    )
    .await;

    // 1. Wrong entrypoint for each target is refused with zero side effects.
    let (arch_on_advisor_code, arch_on_advisor_msg) = invoke_mcp_err_code_and_message(
        &mcp,
        "consult_architect",
        json!({
            "grant_id": mixed_grant,
            "job_id": prep.job_id,
            "request_key": "req-mixed-arch-on-advisor-target",
            "question": "Must refuse architect on advisor-only target rule",
            "dependency_files": ["src/lib.rs"],
            "difficulty": "hard"
        }),
    )
    .await;
    assert_eq!(arch_on_advisor_code, "validation_failed");
    assert!(
        arch_on_advisor_msg.contains("consult_advisor"),
        "expected guidance to use consult_advisor, got: {arch_on_advisor_msg}"
    );

    let (adv_on_arch_code, adv_on_arch_msg) = invoke_mcp_err_code_and_message(
        &mcp,
        "consult_advisor",
        json!({
            "grant_id": mixed_grant,
            "job_id": prep.job_id,
            "request_key": "req-mixed-adv-on-arch-target",
            "question": "Must refuse advisor on architect-only target rule",
            "dependency_files": ["src/lib.rs"],
            "difficulty": "medium"
        }),
    )
    .await;
    assert_eq!(adv_on_arch_code, "validation_failed");
    assert!(
        adv_on_arch_msg.contains("consult_architect"),
        "expected guidance to use consult_architect, got: {adv_on_arch_msg}"
    );

    assert!(!planning_commands_dir(&fixture.plans_dir).exists());
    assert!(!planning_artifacts_dir(&fixture.plans_dir).exists());
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 0);

    // 2. Each explicitly authorized target succeeds with its exact role identity.
    let advisor_args = json!({
        "grant_id": mixed_grant,
        "job_id": prep.job_id,
        "request_key": "req-mixed-advisor-hard",
        "question": "Canonical advisor target in mixed policy",
        "dependency_files": ["src/lib.rs"],
        "difficulty": "hard"
    });
    let advisor_submit: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "consult_advisor", advisor_args.clone()).await;
    assert_eq!(advisor_submit.role, WorkflowRole::Advisor);
    let advisor_detail = wait_for_command_completion(&mcp, &advisor_submit.command_id).await;
    assert_eq!(
        advisor_detail.status,
        PlanningCommandStatus::Completed,
        "mixed advisor detail did not complete cleanly: {}",
        format_command_detail_context(&advisor_detail)
    );
    assert_eq!(advisor_detail.role, WorkflowRole::Advisor);
    assert_eq!(advisor_detail.selected_choice.model_id, MODEL_ID);
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 1);

    let architect_args = json!({
        "grant_id": mixed_grant,
        "job_id": prep.job_id,
        "request_key": "req-mixed-architect-medium",
        "question": "Explicit legacy architect target in mixed policy",
        "dependency_files": ["src/lib.rs"],
        "difficulty": "medium"
    });
    let architect_submit: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "consult_architect", architect_args.clone()).await;
    assert_eq!(architect_submit.role, WorkflowRole::Architect);
    let architect_detail = wait_for_command_completion(&mcp, &architect_submit.command_id).await;
    assert_eq!(
        architect_detail.status,
        PlanningCommandStatus::Completed,
        "mixed architect detail did not complete cleanly: {}",
        format_command_detail_context(&architect_detail)
    );
    assert_eq!(architect_detail.role, WorkflowRole::Architect);
    assert_eq!(architect_detail.selected_choice.model_id, MODEL_ID_LEGACY);
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 2);

    // Exact replay of both returns immutable receipts without extra provider invocations.
    let advisor_replay: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "consult_advisor", advisor_args).await;
    assert_eq!(advisor_replay.command_id, advisor_submit.command_id);
    assert_eq!(
        advisor_replay.submission_status,
        PlanningCommandStartStatus::AlreadyCompleted
    );
    assert!(!advisor_replay.launched);

    let architect_replay: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "consult_architect", architect_args).await;
    assert_eq!(architect_replay.command_id, architect_submit.command_id);
    assert_eq!(
        architect_replay.submission_status,
        PlanningCommandStartStatus::AlreadyCompleted
    );
    assert!(!architect_replay.launched);
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 2);

    fixture.shutdown().await;
}

#[tokio::test]
async fn direct_same_content_amendment_carries_verified_advisor_decision_explicit_and_implicit_through_publish_and_get_without_extra_reservation(
) {
    use threadlane_native::{open_workflow_stores, ScopedWorkflow};

    let mut fixture = GatewayFixture::start().await;
    let mcp = fixture.enrolled_mcp();
    let now_ms = current_wall_clock_ms().expect("wall clock");
    let wf_store =
        CoworkerWorkflowStore::new(&fixture.plans_dir).expect("open CoworkerWorkflowStore");
    let grant_id = wf_store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: fixture.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "req-grant-amend-advisor-carry".to_string(),
            expected_ledger_revision: None,
            project_base: fixture.project_base.clone(),
            profile_id: PROFILE_ID.to_string(),
            registry_revision: 3,
            policy: advisor_policy(),
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget: PlanningCallBudget {
                max_total_calls: 8,
                max_orchestrator_calls: 4,
                max_architect_calls: 4,
                max_timeout_ms_per_call: 60_000,
                max_output_bytes_per_call: 65_536,
            },
            created_at_ms: now_ms.saturating_sub(5_000),
            expires_at_ms: now_ms + 3_600_000,
            operator_attested: true,
        })
        .expect("create advisor planning grant")
        .grant_id;

    let summary = "Coordinate direct amendment with verified Advisor decision";
    let confirmed_requirements = vec![
        "Preserve verified Advisor provenance across direct amendments",
        "Compile single-task plan for src/lib.rs",
    ];
    let proposed_additions = vec!["Optional summary export"];
    let assumptions = vec!["Repository stays clean at HEAD"];
    let open_questions = vec!["Should the boundary keep one contract interface?"];

    // 1. Prepare initial job (`job_parent`).
    let job_parent: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-parent-advisor-carry",
            "summary": summary,
            "confirmed_requirements": confirmed_requirements,
            "proposed_additions": proposed_additions,
            "assumptions": assumptions,
            "open_questions": open_questions
        }),
    )
    .await;
    assert_eq!(job_parent.job_revision, 1);
    assert_eq!(job_parent.kind, WorkBriefKind::NewJob);

    // 2. Run real `consult_advisor` on `job_parent` via local fake provider.
    let consult_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "consult_advisor",
        json!({
            "grant_id": grant_id,
            "job_id": job_parent.job_id,
            "request_key": "req-consult-parent-advisor-carry",
            "question": "Should the direct amendment preserve strict command boundary?",
            "dependency_files": ["src/lib.rs"],
            "difficulty": "hard"
        }),
    )
    .await;
    assert_eq!(consult_submit.role, WorkflowRole::Advisor);
    let consult_detail = wait_for_command_completion(&mcp, &consult_submit.command_id).await;
    assert_eq!(
        consult_detail.status,
        PlanningCommandStatus::Completed,
        "consult_detail did not complete cleanly: {}",
        format_command_detail_context(&consult_detail)
    );
    let advice_id = match consult_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::ArchitectAdvice {
            advice_id,
            artifact_verified,
            ..
        }) => {
            assert!(*artifact_verified);
            advice_id.clone()
        }
        other => panic!("expected ArchitectAdvice result_ref, got {other:?}"),
    };
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 1);

    let orig_ledger = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read ledger after consult");
    let orig_advice = orig_ledger
        .architect_advice
        .iter()
        .find(|a| a.advice_id == advice_id)
        .expect("stored original Advisor record")
        .clone();
    assert_eq!(
        orig_advice.context_dependencies.work_brief_digest,
        job_parent.work_brief_digest
    );
    assert_eq!(orig_advice.status, ArchitectDecisionStatus::Proposed);

    // 3. Create direct same-content amendment accepting `advice_id` (`amend_explicit`).
    let amend_explicit_args = json!({
        "grant_id": grant_id,
        "request_key": "req-prep-amend-explicit",
        "summary": summary,
        "confirmed_requirements": confirmed_requirements,
        "proposed_additions": proposed_additions,
        "assumptions": assumptions,
        "open_questions": open_questions,
        "accepted_architect_decisions": [advice_id],
        "amendment": {
            "target_job_id": job_parent.job_id,
            "target_job_revision": job_parent.job_revision,
            "amendment_reason": "Incorporate verified Advisor decision into brief"
        }
    });
    let amend_explicit: PrepareWorkResponse =
        invoke_mcp_ok(&mcp, "prepare_work", amend_explicit_args.clone()).await;
    assert_eq!(
        amend_explicit.supersedes_job_id.as_deref(),
        Some(job_parent.job_id.as_str())
    );
    assert_ne!(
        amend_explicit.work_brief_digest,
        job_parent.work_brief_digest
    );
    assert_eq!(
        amend_explicit.work_brief.accepted_architect_decisions,
        vec![advice_id.clone()]
    );

    // Idempotent retry of `prepare_work` returns the exact same amendment job.
    let amend_explicit_retry: PrepareWorkResponse =
        invoke_mcp_ok(&mcp, "prepare_work", amend_explicit_args).await;
    assert_eq!(amend_explicit_retry.job_id, amend_explicit.job_id);
    assert_eq!(
        amend_explicit_retry.work_brief_digest,
        amend_explicit.work_brief_digest
    );

    // 4. `request_technical_plan` with EXPLICIT `architect_advice_ids: [advice_id]` -> publish -> get.
    fs::write(&fixture.mode_path, b"orch_plan\n").expect("set fake worker mode orch_plan");
    let explicit_plan_args = json!({
        "grant_id": grant_id,
        "job_id": amend_explicit.job_id,
        "request_key": "req-tech-plan-amend-explicit",
        "architect_advice_ids": [advice_id],
        "max_architect_rounds": 0,
        "difficulty": "hard"
    });
    let explicit_submit: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "request_technical_plan", explicit_plan_args.clone()).await;
    let explicit_detail = wait_for_command_completion(&mcp, &explicit_submit.command_id).await;
    assert_eq!(
        explicit_detail.status,
        PlanningCommandStatus::Completed,
        "explicit_detail did not complete cleanly: {}",
        format_command_detail_context(&explicit_detail)
    );
    let explicit_pr = match explicit_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::OrchestratorPlan {
            job_id,
            planning_result,
            artifact_verified,
        }) => {
            assert_eq!(job_id, &amend_explicit.job_id);
            assert!(*artifact_verified);
            planning_result.as_ref().clone()
        }
        other => panic!("expected OrchestratorPlan result_ref, got {other:?}"),
    };
    assert_eq!(
        explicit_pr.work_brief_digest,
        amend_explicit.work_brief_digest
    );
    assert_ne!(explicit_pr.work_brief_digest, job_parent.work_brief_digest);
    assert_eq!(explicit_pr.architect_advice_ids, vec![advice_id.clone()]);
    assert_eq!(explicit_pr.plan_status, PlanStatus::Published);
    assert_eq!(explicit_pr.plan_revision, 1);

    let (reg_arc, plans_arc) =
        open_workflow_stores(Some(&fixture.registry_dir), Some(&fixture.plans_dir))
            .expect("open workflow stores")
            .expect("workflow stores configured");
    let scoped = ScopedWorkflow::new(reg_arc, plans_arc, fixture.binding_id.clone())
        .expect("ScopedWorkflow");
    let explicit_published = scoped
        .get_plan(&explicit_pr.plan_id, Some(1), None, None)
        .expect("get published plan for explicit amendment");
    assert_eq!(explicit_published.header.status, PlanStatus::Published);
    assert_eq!(explicit_published.header.revision, 1);
    assert_eq!(
        explicit_published.header.plan_digest.as_deref(),
        Some(explicit_pr.plan_digest.as_str())
    );
    assert_eq!(explicit_published.tasks.len(), 1);

    // Original Advisor record in the ledger must remain completely unmodified.
    let ledger_after_explicit = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read ledger after explicit publish");
    let stored_adv_after_explicit = ledger_after_explicit
        .architect_advice
        .iter()
        .find(|a| a.advice_id == advice_id)
        .expect("original Advisor record retained");
    assert_eq!(stored_adv_after_explicit, &orig_advice);
    let grant_after_explicit = ledger_after_explicit
        .planning_grants
        .iter()
        .find(|g| g.grant_id == grant_id)
        .expect("grant after explicit publish");
    assert_eq!(grant_after_explicit.reservations.len(), 2);
    assert_eq!(grant_after_explicit.usage.reserved_total_calls, 2);
    assert_eq!(grant_after_explicit.usage.reserved_architect_calls, 1);
    assert_eq!(grant_after_explicit.usage.reserved_orchestrator_calls, 1);
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 2);

    // Re-reads (`planning_command`, `coworker_status`, `get_plan`) and idempotent replay (both
    // explicit and omitted `architect_advice_ids` with the same `request_key`) must NEVER create
    // a second reservation or invoke the provider again.
    for _ in 0..3 {
        let reread_detail: PlanningCommandDetailResponse = invoke_mcp_ok(
            &mcp,
            "planning_command",
            json!({ "command_id": explicit_submit.command_id }),
        )
        .await;
        assert_eq!(reread_detail.status, PlanningCommandStatus::Completed);
        assert_eq!(
            reread_detail
                .orchestrator_plan_summary
                .as_ref()
                .expect("explicit plan summary")
                .plan_id,
            explicit_pr.plan_id
        );

        let reread_status: CoworkerStatusResponse =
            invoke_mcp_ok(&mcp, "coworker_status", json!({})).await;
        assert!(reread_status
            .jobs
            .iter()
            .any(|j| j.job_id == amend_explicit.job_id && j.planning_result.is_some()));

        let reread_plan = scoped
            .get_plan(&explicit_pr.plan_id, None, None, None)
            .expect("re-read published plan");
        assert_eq!(reread_plan.header.revision, 1);
    }
    let replay_explicit: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "request_technical_plan", explicit_plan_args).await;
    assert_eq!(replay_explicit.command_id, explicit_submit.command_id);
    assert_eq!(
        replay_explicit.submission_status,
        PlanningCommandStartStatus::AlreadyCompleted
    );
    assert!(!replay_explicit.launched);

    let replay_implicit_same_key: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": amend_explicit.job_id,
            "request_key": "req-tech-plan-amend-explicit",
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    assert_eq!(
        replay_implicit_same_key.command_id,
        explicit_submit.command_id
    );
    assert_eq!(
        replay_implicit_same_key.submission_status,
        PlanningCommandStartStatus::AlreadyCompleted
    );
    assert!(!replay_implicit_same_key.launched);

    let grant_after_rereads = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read ledger after rereads")
        .planning_grants
        .into_iter()
        .find(|g| g.grant_id == grant_id)
        .expect("grant after rereads");
    assert_eq!(grant_after_rereads.reservations.len(), 2);
    assert_eq!(grant_after_rereads.usage.reserved_total_calls, 2);
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 2);

    // 5. Create a second direct same-content amendment (`amend_implicit`, targeting `job_parent`
    //    at its updated revision 2) and call `request_technical_plan` OMITTING
    //    `architect_advice_ids` so it resolves implicitly from `work_brief.accepted_architect_decisions`.
    let amend_implicit: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-amend-implicit",
            "summary": summary,
            "confirmed_requirements": confirmed_requirements,
            "proposed_additions": proposed_additions,
            "assumptions": assumptions,
            "open_questions": open_questions,
            "accepted_architect_decisions": [advice_id],
            "amendment": {
                "target_job_id": job_parent.job_id,
                "target_job_revision": 2,
                "amendment_reason": "Incorporate verified Advisor decision for implicit plan flow"
            }
        }),
    )
    .await;
    assert_eq!(
        amend_implicit.supersedes_job_id.as_deref(),
        Some(job_parent.job_id.as_str())
    );
    assert_ne!(
        amend_implicit.work_brief_digest,
        job_parent.work_brief_digest
    );
    assert_ne!(
        amend_implicit.work_brief_digest,
        amend_explicit.work_brief_digest
    );

    let implicit_plan_args = json!({
        "grant_id": grant_id,
        "job_id": amend_implicit.job_id,
        "request_key": "req-tech-plan-amend-implicit",
        "max_architect_rounds": 0,
        "difficulty": "hard"
    });
    let implicit_submit: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "request_technical_plan", implicit_plan_args.clone()).await;
    let implicit_detail = wait_for_command_completion(&mcp, &implicit_submit.command_id).await;
    assert_eq!(
        implicit_detail.status,
        PlanningCommandStatus::Completed,
        "implicit_detail did not complete cleanly: {}",
        format_command_detail_context(&implicit_detail)
    );
    let implicit_pr = match implicit_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::OrchestratorPlan {
            job_id,
            planning_result,
            artifact_verified,
        }) => {
            assert_eq!(job_id, &amend_implicit.job_id);
            assert!(*artifact_verified);
            planning_result.as_ref().clone()
        }
        other => panic!("expected OrchestratorPlan result_ref, got {other:?}"),
    };
    assert_eq!(
        implicit_pr.work_brief_digest,
        amend_implicit.work_brief_digest
    );
    assert_eq!(implicit_pr.architect_advice_ids, vec![advice_id.clone()]);
    assert_eq!(implicit_pr.plan_status, PlanStatus::Published);
    assert_ne!(implicit_pr.plan_id, explicit_pr.plan_id);

    let implicit_published = scoped
        .get_plan(&implicit_pr.plan_id, Some(1), None, None)
        .expect("get published plan for implicit amendment");
    assert_eq!(implicit_published.header.status, PlanStatus::Published);
    assert_eq!(
        implicit_published.header.plan_digest.as_deref(),
        Some(implicit_pr.plan_digest.as_str())
    );

    // Re-read & replay on the implicit command must not add a reservation.
    let replay_implicit: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "request_technical_plan", implicit_plan_args).await;
    assert_eq!(replay_implicit.command_id, implicit_submit.command_id);
    assert_eq!(
        replay_implicit.submission_status,
        PlanningCommandStartStatus::AlreadyCompleted
    );
    assert!(!replay_implicit.launched);
    let _: PlanningCommandDetailResponse = invoke_mcp_ok(
        &mcp,
        "planning_command",
        json!({ "command_id": implicit_submit.command_id }),
    )
    .await;
    let _: CoworkerStatusResponse = invoke_mcp_ok(&mcp, "coworker_status", json!({})).await;

    let ledger_after_implicit = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read ledger after implicit publish");
    assert_eq!(
        ledger_after_implicit
            .architect_advice
            .iter()
            .find(|a| a.advice_id == advice_id)
            .expect("original Advisor record unchanged after implicit publish"),
        &orig_advice
    );
    let grant_after_implicit = ledger_after_implicit
        .planning_grants
        .iter()
        .find(|g| g.grant_id == grant_id)
        .expect("grant after implicit publish");
    assert_eq!(grant_after_implicit.reservations.len(), 3);
    assert_eq!(grant_after_implicit.usage.reserved_total_calls, 3);
    assert_eq!(grant_after_implicit.usage.reserved_architect_calls, 1);
    assert_eq!(grant_after_implicit.usage.reserved_orchestrator_calls, 2);
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 3);

    // 6. Fresh exact-digest path remains preserved (`consult_advisor` directly on `job_fresh` ->
    //    `request_technical_plan` directly on `job_fresh`).
    fs::write(&fixture.mode_path, b"architect\n").expect("set fake worker mode architect");
    let job_fresh: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-fresh-exact-digest",
            "summary": "Fresh exact-digest Advisor consultation and planning",
            "confirmed_requirements": ["Preserve exact-digest path for unamended jobs"]
        }),
    )
    .await;
    let fresh_consult: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "consult_advisor",
        json!({
            "grant_id": grant_id,
            "job_id": job_fresh.job_id,
            "request_key": "req-consult-fresh-exact-digest",
            "question": "Verify fresh exact-digest consultation boundary?",
            "dependency_files": ["src/lib.rs"],
            "difficulty": "hard"
        }),
    )
    .await;
    let fresh_consult_detail = wait_for_command_completion(&mcp, &fresh_consult.command_id).await;
    assert_eq!(
        fresh_consult_detail.status,
        PlanningCommandStatus::Completed
    );
    let fresh_advice_id = match fresh_consult_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::ArchitectAdvice { advice_id, .. }) => advice_id.clone(),
        other => panic!("expected fresh ArchitectAdvice result_ref, got {other:?}"),
    };
    fs::write(&fixture.mode_path, b"orch_plan\n").expect("set fake worker mode orch_plan");
    let fresh_plan: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_fresh.job_id,
            "request_key": "req-tech-plan-fresh-exact-digest",
            "architect_advice_ids": [fresh_advice_id],
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let fresh_plan_detail = wait_for_command_completion(&mcp, &fresh_plan.command_id).await;
    assert_eq!(fresh_plan_detail.status, PlanningCommandStatus::Completed);
    let fresh_pr = match fresh_plan_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::OrchestratorPlan {
            planning_result, ..
        }) => planning_result.as_ref().clone(),
        other => panic!("expected fresh OrchestratorPlan result_ref, got {other:?}"),
    };
    assert_eq!(fresh_pr.work_brief_digest, job_fresh.work_brief_digest);
    assert_eq!(fresh_pr.architect_advice_ids, vec![fresh_advice_id]);
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 5);

    fixture.shutdown().await;
}

#[tokio::test]
async fn direct_amendment_advice_eligibility_preserves_rejections_across_prepare_enqueue_and_execution(
) {
    use threadlane_native::coworker_workflow::{
        CoworkerLedgerMutationOutcome, CreateCoworkerJobRequest, OrchestrateWorkBriefPlanRequest,
    };
    use threadlane_native::{open_workflow_stores, RequestTechnicalPlanParams, ScopedWorkflow};

    let mut fixture = GatewayFixture::start().await;
    let mcp = fixture.enrolled_mcp();
    let now_ms = current_wall_clock_ms().expect("wall clock");
    let wf_store =
        CoworkerWorkflowStore::new(&fixture.plans_dir).expect("open CoworkerWorkflowStore");

    let grant_id = wf_store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: fixture.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "req-grant-neg-primary".to_string(),
            expected_ledger_revision: None,
            project_base: fixture.project_base.clone(),
            profile_id: PROFILE_ID.to_string(),
            registry_revision: 3,
            policy: advisor_policy(),
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget: PlanningCallBudget {
                max_total_calls: 12,
                max_orchestrator_calls: 6,
                max_architect_calls: 6,
                max_timeout_ms_per_call: 60_000,
                max_output_bytes_per_call: 65_536,
            },
            created_at_ms: now_ms.saturating_sub(5_000),
            expires_at_ms: now_ms + 3_600_000,
            operator_attested: true,
        })
        .expect("create primary advisor grant")
        .grant_id;

    let mut alt_policy = advisor_policy();
    alt_policy.policy_id = "policy-gateway-advisor-alt".to_string();
    let other_policy_grant_id = wf_store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: fixture.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "req-grant-neg-other-policy".to_string(),
            expected_ledger_revision: None,
            project_base: fixture.project_base.clone(),
            profile_id: PROFILE_ID.to_string(),
            registry_revision: 3,
            policy: alt_policy,
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget: PlanningCallBudget {
                max_total_calls: 6,
                max_orchestrator_calls: 4,
                max_architect_calls: 2,
                max_timeout_ms_per_call: 60_000,
                max_output_bytes_per_call: 65_536,
            },
            created_at_ms: now_ms.saturating_sub(5_000),
            expires_at_ms: now_ms + 3_600_000,
            operator_attested: true,
        })
        .expect("create other-policy advisor grant")
        .grant_id;

    let summary = "Validate strict direct amendment Advisor eligibility";
    let reqs = vec![
        "First ordered requirement for src/lib.rs",
        "Second ordered requirement for strict provenance",
    ];
    let proposals = vec!["Optional telemetry summary"];
    let assumptions = vec!["Local checkout stays clean"];
    let questions = vec!["Keep single interface boundary?"];

    let job_parent: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-neg-prep-parent",
            "summary": summary,
            "confirmed_requirements": reqs,
            "proposed_additions": proposals,
            "assumptions": assumptions,
            "open_questions": questions
        }),
    )
    .await;
    let job_foreign: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-neg-prep-foreign",
            "summary": "Separate unrelated job for foreign decision testing",
            "confirmed_requirements": ["Produce an Advisor decision bound only to foreign job"]
        }),
    )
    .await;

    // Produce three genuine artifact-backed Advisor records:
    // - `adv_accepted`: bound to `job_parent`, accepted into `job_valid_amend`
    // - `adv_unaccepted_parent`: bound to `job_parent`, NOT accepted into `job_valid_amend`
    // - `adv_foreign`: bound to `job_foreign`
    let consult_and_get_advice = |job_id: String, req_key: &'static str, question: &'static str| {
        let mcp = &mcp;
        let grant_id = grant_id.clone();
        async move {
            let submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
                mcp,
                "consult_advisor",
                json!({
                    "grant_id": grant_id,
                    "job_id": job_id,
                    "request_key": req_key,
                    "question": question,
                    "dependency_files": ["src/lib.rs"],
                    "difficulty": "hard"
                }),
            )
            .await;
            let detail = wait_for_command_completion(mcp, &submit.command_id).await;
            assert_eq!(detail.status, PlanningCommandStatus::Completed);
            match detail.result_ref.as_ref() {
                Some(PlanningCommandResultRef::ArchitectAdvice {
                    advice_id,
                    report_artifact_id,
                    ..
                }) => (advice_id.clone(), report_artifact_id.clone()),
                other => panic!("expected ArchitectAdvice result_ref, got {other:?}"),
            }
        }
    };

    let (adv_accepted, adv_accepted_artifact_id) = consult_and_get_advice(
        job_parent.job_id.clone(),
        "req-neg-consult-accepted",
        "Primary Advisor decision for parent job",
    )
    .await;
    let (adv_unaccepted_parent, _) = consult_and_get_advice(
        job_parent.job_id.clone(),
        "req-neg-consult-unaccepted",
        "Secondary Advisor decision for parent job that will not be accepted",
    )
    .await;
    let (adv_foreign, _) = consult_and_get_advice(
        job_foreign.job_id.clone(),
        "req-neg-consult-foreign",
        "Foreign Advisor decision bound to unrelated job",
    )
    .await;

    // Also create a job whose plan & interface contracts change AFTER its Advisor consult.
    let job_iface_drift: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-neg-prep-iface-drift",
            "summary": "Verify interface and plan contract drift invalidates prior Advisor decision",
            "confirmed_requirements": ["Detect interface contract changes after consultation"]
        }),
    )
    .await;
    let (adv_pre_plan, _) = consult_and_get_advice(
        job_iface_drift.job_id.clone(),
        "req-neg-consult-pre-plan",
        "Pre-plan Advisor decision before interface ownership exists",
    )
    .await;
    fs::write(&fixture.mode_path, b"orch_plan\n").expect("set fake worker mode orch_plan");
    let pre_plan_pub: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_iface_drift.job_id,
            "request_key": "req-neg-pub-iface-drift",
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let pre_plan_pub_detail = wait_for_command_completion(&mcp, &pre_plan_pub.command_id).await;
    assert_eq!(pre_plan_pub_detail.status, PlanningCommandStatus::Completed);
    let iface_drift_plan_id = pre_plan_pub_detail
        .orchestrator_plan_summary
        .as_ref()
        .expect("iface_drift plan summary")
        .plan_id
        .clone();

    let calls_baseline = provider_invocation_count(&fixture.worker_marker);
    assert_eq!(calls_baseline, 5);
    let reservations_baseline = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("ledger baseline")
        .planning_grants
        .into_iter()
        .find(|g| g.grant_id == grant_id)
        .expect("grant baseline")
        .reservations
        .len();
    assert_eq!(reservations_baseline, 5);

    // 1. `prepare_work` rejects every invalid semantic change, reordering, foreign decision,
    //    non-amendment, revision mismatch, changed policy, declared amended_interfaces, and
    //    post-consult interface/plan drift.
    let prepare_negatives = [
        (
            "modified confirmed_requirements",
            json!({
                "grant_id": grant_id,
                "request_key": "req-neg-prep-mod-req",
                "summary": summary,
                "confirmed_requirements": [
                    "First ordered requirement for src/lib.rs",
                    "MODIFIED requirement text"
                ],
                "proposed_additions": proposals,
                "assumptions": assumptions,
                "open_questions": questions,
                "accepted_architect_decisions": [adv_accepted],
                "amendment": {
                    "target_job_id": job_parent.job_id,
                    "target_job_revision": 1,
                    "amendment_reason": "Modified requirement must fail"
                }
            }),
        ),
        (
            "reordered confirmed_requirements",
            json!({
                "grant_id": grant_id,
                "request_key": "req-neg-prep-reordered-req",
                "summary": summary,
                "confirmed_requirements": [
                    "Second ordered requirement for strict provenance",
                    "First ordered requirement for src/lib.rs"
                ],
                "proposed_additions": proposals,
                "assumptions": assumptions,
                "open_questions": questions,
                "accepted_architect_decisions": [adv_accepted],
                "amendment": {
                    "target_job_id": job_parent.job_id,
                    "target_job_revision": 1,
                    "amendment_reason": "Reordered requirements must fail"
                }
            }),
        ),
        (
            "modified summary",
            json!({
                "grant_id": grant_id,
                "request_key": "req-neg-prep-mod-summary",
                "summary": "Different summary text",
                "confirmed_requirements": reqs,
                "proposed_additions": proposals,
                "assumptions": assumptions,
                "open_questions": questions,
                "accepted_architect_decisions": [adv_accepted],
                "amendment": {
                    "target_job_id": job_parent.job_id,
                    "target_job_revision": 1,
                    "amendment_reason": "Modified summary must fail"
                }
            }),
        ),
        (
            "modified proposed_additions",
            json!({
                "grant_id": grant_id,
                "request_key": "req-neg-prep-mod-proposal",
                "summary": summary,
                "confirmed_requirements": reqs,
                "proposed_additions": ["Different proposal"],
                "assumptions": assumptions,
                "open_questions": questions,
                "accepted_architect_decisions": [adv_accepted],
                "amendment": {
                    "target_job_id": job_parent.job_id,
                    "target_job_revision": 1,
                    "amendment_reason": "Modified proposed_additions must fail"
                }
            }),
        ),
        (
            "modified assumptions",
            json!({
                "grant_id": grant_id,
                "request_key": "req-neg-prep-mod-assumption",
                "summary": summary,
                "confirmed_requirements": reqs,
                "proposed_additions": proposals,
                "assumptions": ["Different assumption"],
                "open_questions": questions,
                "accepted_architect_decisions": [adv_accepted],
                "amendment": {
                    "target_job_id": job_parent.job_id,
                    "target_job_revision": 1,
                    "amendment_reason": "Modified assumptions must fail"
                }
            }),
        ),
        (
            "modified open_questions",
            json!({
                "grant_id": grant_id,
                "request_key": "req-neg-prep-mod-question",
                "summary": summary,
                "confirmed_requirements": reqs,
                "proposed_additions": proposals,
                "assumptions": assumptions,
                "open_questions": ["Different open question?"],
                "accepted_architect_decisions": [adv_accepted],
                "amendment": {
                    "target_job_id": job_parent.job_id,
                    "target_job_revision": 1,
                    "amendment_reason": "Modified open_questions must fail"
                }
            }),
        ),
        (
            "foreign decision on direct amendment",
            json!({
                "grant_id": grant_id,
                "request_key": "req-neg-prep-foreign-decision",
                "summary": summary,
                "confirmed_requirements": reqs,
                "proposed_additions": proposals,
                "assumptions": assumptions,
                "open_questions": questions,
                "accepted_architect_decisions": [adv_foreign],
                "amendment": {
                    "target_job_id": job_parent.job_id,
                    "target_job_revision": 1,
                    "amendment_reason": "Foreign decision must fail"
                }
            }),
        ),
        (
            "non-amendment NewJob accepting parent decision",
            json!({
                "grant_id": grant_id,
                "request_key": "req-neg-prep-new-job-decision",
                "summary": summary,
                "confirmed_requirements": reqs,
                "proposed_additions": proposals,
                "assumptions": assumptions,
                "open_questions": questions,
                "accepted_architect_decisions": [adv_accepted]
            }),
        ),
        (
            "revision mismatch on target_job_revision",
            json!({
                "grant_id": grant_id,
                "request_key": "req-neg-prep-wrong-rev",
                "summary": summary,
                "confirmed_requirements": reqs,
                "proposed_additions": proposals,
                "assumptions": assumptions,
                "open_questions": questions,
                "accepted_architect_decisions": [adv_accepted],
                "amendment": {
                    "target_job_id": job_parent.job_id,
                    "target_job_revision": 99,
                    "amendment_reason": "Stale target_job_revision must fail"
                }
            }),
        ),
        (
            "changed policy grant",
            json!({
                "grant_id": other_policy_grant_id,
                "request_key": "req-neg-prep-other-policy",
                "summary": summary,
                "confirmed_requirements": reqs,
                "proposed_additions": proposals,
                "assumptions": assumptions,
                "open_questions": questions,
                "accepted_architect_decisions": [adv_accepted],
                "amendment": {
                    "target_job_id": job_parent.job_id,
                    "target_job_revision": 1,
                    "amendment_reason": "Different policy_digest must fail"
                }
            }),
        ),
        (
            "declared amended_interfaces",
            json!({
                "grant_id": grant_id,
                "request_key": "req-neg-prep-amended-iface",
                "summary": summary,
                "confirmed_requirements": reqs,
                "proposed_additions": proposals,
                "assumptions": assumptions,
                "open_questions": questions,
                "accepted_architect_decisions": [adv_accepted],
                "amendment": {
                    "target_job_id": job_parent.job_id,
                    "target_job_revision": 1,
                    "amendment_reason": "Declared interface amendment must fail",
                    "amended_interfaces": ["iface_gateway"]
                }
            }),
        ),
        (
            "parent plan and interface contracts changed after consult",
            json!({
                "grant_id": grant_id,
                "request_key": "req-neg-prep-parent-iface-drift",
                "summary": "Verify interface and plan contract drift invalidates prior Advisor decision",
                "confirmed_requirements": ["Detect interface contract changes after consultation"],
                "accepted_architect_decisions": [adv_pre_plan],
                "amendment": {
                    "target_job_id": job_iface_drift.job_id,
                    "target_job_revision": 2,
                    "amendment_reason": "Parent published plan and interface ownership after consult"
                }
            }),
        ),
    ];
    for (label, args) in prepare_negatives {
        let code = invoke_mcp_err_code(&mcp, "prepare_work", args).await;
        assert_eq!(
            code, "validation_failed",
            "prepare_work must reject `{label}`, got `{code}`"
        );
    }

    // Also verify fresh exact-digest `request_technical_plan` rejects `adv_pre_plan` after
    // `job_iface_drift` acquired interface ownership in `iface_drift_plan_id`.
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "request_technical_plan",
            json!({
                "grant_id": grant_id,
                "job_id": job_iface_drift.job_id,
                "request_key": "req-neg-plan-fresh-iface-drift",
                "plan_id": iface_drift_plan_id,
                "expected_plan_revision": 1,
                "architect_advice_ids": [adv_pre_plan],
                "max_architect_rounds": 0,
                "difficulty": "hard"
            })
        )
        .await,
        "validation_failed"
    );

    // 2. Create one legitimate direct amendment `job_valid_amend` accepting ONLY `[adv_accepted]`.
    let job_valid_amend: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-neg-prep-valid-direct-amend",
            "summary": summary,
            "confirmed_requirements": reqs,
            "proposed_additions": proposals,
            "assumptions": assumptions,
            "open_questions": questions,
            "accepted_architect_decisions": [adv_accepted],
            "amendment": {
                "target_job_id": job_parent.job_id,
                "target_job_revision": 1,
                "amendment_reason": "Valid direct amendment for negative boundary checks"
            }
        }),
    )
    .await;

    // 2-hop (grandparent) amendment trying to carry `adv_accepted` across `job_valid_amend` must fail.
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "prepare_work",
            json!({
                "grant_id": grant_id,
                "request_key": "req-neg-prep-grandparent-hop",
                "summary": summary,
                "confirmed_requirements": reqs,
                "proposed_additions": proposals,
                "assumptions": assumptions,
                "open_questions": questions,
                "accepted_architect_decisions": [adv_accepted],
                "amendment": {
                    "target_job_id": job_valid_amend.job_id,
                    "target_job_revision": 1,
                    "amendment_reason": "Two-hop grandparent carry-forward must fail"
                }
            })
        )
        .await,
        "validation_failed"
    );

    // Open direct `CoworkerPlanningService` + `ScopedWorkflow` to verify the execution layer
    // enforces the exact same rule as `prepare_work` and `request_technical_plan` enqueue.
    let (reg_arc, plans_arc) =
        open_workflow_stores(Some(&fixture.registry_dir), Some(&fixture.plans_dir))
            .expect("open workflow stores")
            .expect("workflow stores configured");
    let scoped = ScopedWorkflow::new(reg_arc, plans_arc, fixture.binding_id.clone())
        .expect("ScopedWorkflow");
    let planning_service = CoworkerPlanningService::new(
        wf_store.clone(),
        &fixture.registry_dir,
        planning_artifacts_dir(&fixture.plans_dir),
    )
    .expect("open CoworkerPlanningService");

    let run_direct_exec_err = |g_id: &str, j_id: &str, r_key: &str, advice_ids: Vec<String>| {
        let service = &planning_service;
        let scoped = &scoped;
        let binding_id = fixture.binding_id.clone();
        let req = OrchestrateWorkBriefPlanRequest {
            binding_id,
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: g_id.to_string(),
            job_id: j_id.to_string(),
            request_key: r_key.to_string(),
            expected_ledger_revision: None,
            difficulty: Some(Difficulty::Hard),
            plan_id: None,
            expected_plan_revision: 0,
            architect_advice_ids: advice_ids,
            max_architect_rounds: 0,
            architect_difficulty: None,
            current_time_ms: current_wall_clock_ms().expect("wall clock"),
        };
        async move {
            service
                .orchestrate_brief_to_plan(scoped, &req)
                .await
                .expect_err("direct Orchestrator execution must reject ineligible advice")
        }
    };

    // 3. Unaccepted parent decision (`adv_unaccepted_parent`), foreign decision (`adv_foreign`),
    //    and changed policy grant (`other_policy_grant_id`) are all rejected before enqueue and
    //    before execution on `job_valid_amend`.
    for (label, g_id, advice_ids) in [
        (
            "unaccepted parent decision",
            grant_id.as_str(),
            vec![adv_unaccepted_parent.clone()],
        ),
        (
            "foreign job decision",
            grant_id.as_str(),
            vec![adv_foreign.clone()],
        ),
        (
            "changed policy grant (explicit)",
            other_policy_grant_id.as_str(),
            vec![adv_accepted.clone()],
        ),
    ] {
        assert_eq!(
            invoke_mcp_err_code(
                &mcp,
                "request_technical_plan",
                json!({
                    "grant_id": g_id,
                    "job_id": job_valid_amend.job_id,
                    "request_key": format!("req-neg-plan-{}", label.replace(' ', "-")),
                    "architect_advice_ids": advice_ids,
                    "max_architect_rounds": 0,
                    "difficulty": "hard"
                })
            )
            .await,
            "validation_failed",
            "request_technical_plan must reject `{label}`"
        );
        let exec_err = run_direct_exec_err(
            g_id,
            &job_valid_amend.job_id,
            &format!("exec-neg-{}", label.replace(' ', "-")),
            advice_ids,
        )
        .await;
        assert_eq!(exec_err.error_code(), NativeErrorCode::ValidationFailed);
    }
    // Changed policy grant with IMPLICIT (omitted `architect_advice_ids`) must also fail closed.
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "request_technical_plan",
            json!({
                "grant_id": other_policy_grant_id,
                "job_id": job_valid_amend.job_id,
                "request_key": "req-neg-plan-other-policy-implicit",
                "max_architect_rounds": 0,
                "difficulty": "hard"
            })
        )
        .await,
        "validation_failed"
    );

    // 4. Bypass `prepare_work` by storing malformed/ineligible direct-amendment jobs directly in
    //    `CoworkerWorkflowStore` and prove BOTH enqueue (`request_technical_plan`, explicit +
    //    implicit) AND execution (`orchestrate_brief_to_plan`) enforce the exact same rule:
    //    (a) modified confirmed_requirements, (b) declared amended_interfaces,
    //    (c) missing Advisor record ID, (d) mismatched documented parent amendment hold revision.
    let parent_record = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read ledger for direct store negatives")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job_parent.job_id)
        .expect("parent job record");

    let mut mod_req_brief = parent_record.work_brief.clone();
    mod_req_brief.brief_id = "brf_neg_store_mod_req".to_string();
    mod_req_brief.kind = WorkBriefKind::Amendment {
        target_job_id: job_parent.job_id.clone(),
        target_job_revision: parent_record.job_revision,
        amendment_reason: "Direct store modified requirement".to_string(),
    };
    mod_req_brief.confirmed_requirements[1] =
        "Altered requirement bypassing prepare_work".to_string();
    mod_req_brief.accepted_architect_decisions = vec![adv_accepted.clone()];
    let job_store_mod_req = wf_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: fixture.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "req-neg-store-mod-req".to_string(),
            expected_ledger_revision: None,
            work_brief: mod_req_brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("store modified-requirement amendment job");

    let mut iface_brief = parent_record.work_brief.clone();
    iface_brief.brief_id = "brf_neg_store_iface".to_string();
    iface_brief.kind = WorkBriefKind::Amendment {
        target_job_id: job_parent.job_id.clone(),
        target_job_revision: parent_record.job_revision + 1,
        amendment_reason: "Direct store interface amendment".to_string(),
    };
    iface_brief.accepted_architect_decisions = vec![adv_accepted.clone()];
    let job_store_iface = wf_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: fixture.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "req-neg-store-iface".to_string(),
            expected_ledger_revision: None,
            work_brief: iface_brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: vec!["iface_gateway".to_string()],
        })
        .expect("store amended-interface amendment job");

    let mut missing_src_brief = parent_record.work_brief.clone();
    missing_src_brief.brief_id = "brf_neg_store_missing_src".to_string();
    missing_src_brief.kind = WorkBriefKind::Amendment {
        target_job_id: job_parent.job_id.clone(),
        target_job_revision: parent_record.job_revision + 2,
        amendment_reason: "Direct store missing source advice".to_string(),
    };
    let missing_advice_id = "adv_00000000000000000000000000000000".to_string();
    missing_src_brief.accepted_architect_decisions = vec![missing_advice_id.clone()];
    let job_store_missing_src = wf_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: fixture.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "req-neg-store-missing-src".to_string(),
            expected_ledger_revision: None,
            work_brief: missing_src_brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("store missing-source amendment job");

    let mut hold_mismatch_brief = parent_record.work_brief.clone();
    hold_mismatch_brief.brief_id = "brf_neg_store_hold_mismatch".to_string();
    hold_mismatch_brief.kind = WorkBriefKind::Amendment {
        target_job_id: job_parent.job_id.clone(),
        target_job_revision: parent_record.job_revision + 3,
        amendment_reason: "Direct store hold revision mismatch".to_string(),
    };
    hold_mismatch_brief.accepted_architect_decisions = vec![adv_accepted.clone()];
    let job_store_hold_mismatch = wf_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: fixture.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "req-neg-store-hold-mismatch".to_string(),
            expected_ledger_revision: None,
            work_brief: hold_mismatch_brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("store hold-mismatch amendment job");
    wf_store
        .transact_binding_ledger(&fixture.binding_id, None, |ledger| {
            let parent_mut = ledger
                .jobs
                .iter_mut()
                .find(|j| j.job_id == job_parent.job_id)
                .expect("parent job for hold mismatch");
            let hold = parent_mut
                .holds
                .iter_mut()
                .find(|h| h.request_key == "amend_req-neg-store-hold-mismatch")
                .expect("documented amendment hold");
            hold.request_key = "amend_other_unrelated_key".to_string();
            Ok(CoworkerLedgerMutationOutcome::Mutated(()))
        })
        .expect("break documented parent amendment hold linkage");

    for (label, target_job_id, adv_id) in [
        (
            "store modified requirement",
            &job_store_mod_req.job_id,
            &adv_accepted,
        ),
        (
            "store declared amended_interfaces",
            &job_store_iface.job_id,
            &adv_accepted,
        ),
        (
            "store missing Advisor record",
            &job_store_missing_src.job_id,
            &missing_advice_id,
        ),
        (
            "store missing parent hold proof",
            &job_store_hold_mismatch.job_id,
            &adv_accepted,
        ),
    ] {
        // Explicit `architect_advice_ids` fails closed before enqueue.
        assert_eq!(
            invoke_mcp_err_code(
                &mcp,
                "request_technical_plan",
                json!({
                    "grant_id": grant_id,
                    "job_id": target_job_id,
                    "request_key": format!("req-neg-exp-{}", label.replace(' ', "-")),
                    "architect_advice_ids": [adv_id],
                    "max_architect_rounds": 0,
                    "difficulty": "hard"
                })
            )
            .await,
            "validation_failed",
            "explicit request_technical_plan must reject `{label}`"
        );
        // Implicit (omitted `architect_advice_ids`) ALSO fails closed before enqueue.
        assert_eq!(
            invoke_mcp_err_code(
                &mcp,
                "request_technical_plan",
                json!({
                    "grant_id": grant_id,
                    "job_id": target_job_id,
                    "request_key": format!("req-neg-imp-{}", label.replace(' ', "-")),
                    "max_architect_rounds": 0,
                    "difficulty": "hard"
                })
            )
            .await,
            "validation_failed",
            "implicit request_technical_plan must reject `{label}`"
        );
        // Direct execution layer ALSO fails closed.
        let exec_err = run_direct_exec_err(
            &grant_id,
            target_job_id,
            &format!("exec-store-{}", label.replace(' ', "-")),
            vec![adv_id.clone()],
        )
        .await;
        assert_eq!(
            exec_err.error_code(),
            NativeErrorCode::ValidationFailed,
            "direct execution must reject `{label}`"
        );
    }

    // 5. Missing persisted artifact evidence fails closed across prepare_work, enqueue (explicit +
    //    implicit), and execution.
    let artifact_path =
        planning_artifacts_dir(&fixture.plans_dir).join(format!("{adv_accepted_artifact_id}.json"));
    let saved_artifact = fs::read(&artifact_path).expect("read original Advisor artifact");
    fs::remove_file(&artifact_path).expect("temporarily remove Advisor artifact");

    let current_parent_rev = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read ledger for parent rev")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job_parent.job_id)
        .expect("parent job")
        .job_revision;
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "prepare_work",
            json!({
                "grant_id": grant_id,
                "request_key": "req-neg-prep-missing-artifact",
                "summary": summary,
                "confirmed_requirements": reqs,
                "proposed_additions": proposals,
                "assumptions": assumptions,
                "open_questions": questions,
                "accepted_architect_decisions": [adv_accepted],
                "amendment": {
                    "target_job_id": job_parent.job_id,
                    "target_job_revision": current_parent_rev,
                    "amendment_reason": "Missing artifact must fail"
                }
            })
        )
        .await,
        "validation_failed"
    );
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "request_technical_plan",
            json!({
                "grant_id": grant_id,
                "job_id": job_valid_amend.job_id,
                "request_key": "req-neg-plan-missing-artifact-exp",
                "architect_advice_ids": [adv_accepted],
                "max_architect_rounds": 0,
                "difficulty": "hard"
            })
        )
        .await,
        "validation_failed"
    );
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "request_technical_plan",
            json!({
                "grant_id": grant_id,
                "job_id": job_valid_amend.job_id,
                "request_key": "req-neg-plan-missing-artifact-imp",
                "max_architect_rounds": 0,
                "difficulty": "hard"
            })
        )
        .await,
        "validation_failed"
    );
    assert_eq!(
        run_direct_exec_err(
            &grant_id,
            &job_valid_amend.job_id,
            "exec-neg-missing-artifact",
            vec![adv_accepted.clone()],
        )
        .await
        .error_code(),
        NativeErrorCode::ValidationFailed
    );
    fs::write(&artifact_path, saved_artifact).expect("restore Advisor artifact");
    fs::set_permissions(&artifact_path, fs::Permissions::from_mode(0o600))
        .expect("restore 0600 permissions on Advisor artifact");

    // 6. Rejected and Superseded Advisor decisions fail closed across prepare_work, enqueue
    //    (explicit + implicit), and execution.
    for (status_label, status) in [
        ("rejected", ArchitectDecisionStatus::Rejected),
        ("superseded", ArchitectDecisionStatus::Superseded),
    ] {
        wf_store
            .transact_binding_ledger(&fixture.binding_id, None, |ledger| {
                ledger
                    .architect_advice
                    .iter_mut()
                    .find(|a| a.advice_id == adv_accepted)
                    .expect("adv_accepted")
                    .status = status;
                Ok(CoworkerLedgerMutationOutcome::Mutated(()))
            })
            .expect("update Advisor decision status");

        assert_eq!(
            invoke_mcp_err_code(
                &mcp,
                "prepare_work",
                json!({
                    "grant_id": grant_id,
                    "request_key": format!("req-neg-prep-{status_label}"),
                    "summary": summary,
                    "confirmed_requirements": reqs,
                    "proposed_additions": proposals,
                    "assumptions": assumptions,
                    "open_questions": questions,
                    "accepted_architect_decisions": [adv_accepted],
                    "amendment": {
                        "target_job_id": job_parent.job_id,
                        "target_job_revision": current_parent_rev,
                        "amendment_reason": "Inactive Advisor decision must fail"
                    }
                })
            )
            .await,
            "validation_failed"
        );
        assert_eq!(
            invoke_mcp_err_code(
                &mcp,
                "request_technical_plan",
                json!({
                    "grant_id": grant_id,
                    "job_id": job_valid_amend.job_id,
                    "request_key": format!("req-neg-plan-{status_label}-exp"),
                    "architect_advice_ids": [adv_accepted],
                    "max_architect_rounds": 0,
                    "difficulty": "hard"
                })
            )
            .await,
            "validation_failed"
        );
        assert_eq!(
            invoke_mcp_err_code(
                &mcp,
                "request_technical_plan",
                json!({
                    "grant_id": grant_id,
                    "job_id": job_valid_amend.job_id,
                    "request_key": format!("req-neg-plan-{status_label}-imp"),
                    "max_architect_rounds": 0,
                    "difficulty": "hard"
                })
            )
            .await,
            "validation_failed"
        );
        assert_eq!(
            run_direct_exec_err(
                &grant_id,
                &job_valid_amend.job_id,
                &format!("exec-neg-{status_label}"),
                vec![adv_accepted.clone()],
            )
            .await
            .error_code(),
            NativeErrorCode::ValidationFailed
        );
    }
    wf_store
        .transact_binding_ledger(&fixture.binding_id, None, |ledger| {
            ledger
                .architect_advice
                .iter_mut()
                .find(|a| a.advice_id == adv_accepted)
                .expect("adv_accepted")
                .status = ArchitectDecisionStatus::Proposed;
            Ok(CoworkerLedgerMutationOutcome::Mutated(()))
        })
        .expect("restore Advisor decision status to Proposed");

    // 7. Cross-thread scope fails closed without touching provider or reservations.
    let other_thread_gw = CoworkerPlanningGateway::new(
        &fixture.state_dir,
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &fixture.binding_id,
        "0196ed00-0000-7000-8000-0000000000c4",
    )
    .expect("create other-thread gateway")
    .with_sibling_anchor_exe(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-flow")));
    let other_thread_err = other_thread_gw
        .request_technical_plan(&RequestTechnicalPlanParams {
            grant_id: grant_id.clone(),
            job_id: job_valid_amend.job_id.clone(),
            request_key: "req-neg-other-thread".to_string(),
            difficulty: Some(Difficulty::Hard),
            plan_id: None,
            expected_plan_revision: None,
            architect_advice_ids: vec![adv_accepted.clone()],
            max_architect_rounds: Some(0),
            architect_difficulty: None,
            expected_ledger_revision: None,
        })
        .await
        .expect_err("other thread must fail closed");
    assert!(matches!(
        other_thread_err.error_code(),
        NativeErrorCode::ValidationFailed | NativeErrorCode::SecurityViolation
    ));

    // 8. Changed base commit / tree / dependency blob invalidates carrying `adv_accepted` across
    //    `prepare_work`, `request_technical_plan`, and execution.
    fs::write(
        fixture.repo_dir.join("src/lib.rs"),
        "pub fn gateway_seed() -> u32 { 99 }\n",
    )
    .expect("modify src/lib.rs for base/blob drift");
    for args in [
        &["add", "src/lib.rs"][..],
        &[
            "commit",
            "-m",
            "advance base and change src/lib.rs blob",
            "--quiet",
        ][..],
    ] {
        run_sanitized_git(&fixture.repo_dir, args, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("commit base/blob drift");
    }
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "prepare_work",
            json!({
                "grant_id": grant_id,
                "request_key": "req-neg-prep-changed-base-blob",
                "summary": summary,
                "confirmed_requirements": reqs,
                "proposed_additions": proposals,
                "assumptions": assumptions,
                "open_questions": questions,
                "accepted_architect_decisions": [adv_accepted],
                "amendment": {
                    "target_job_id": job_parent.job_id,
                    "target_job_revision": current_parent_rev,
                    "amendment_reason": "Changed base/tree/blob must fail"
                }
            })
        )
        .await,
        "validation_failed"
    );
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "request_technical_plan",
            json!({
                "grant_id": grant_id,
                "job_id": job_valid_amend.job_id,
                "request_key": "req-neg-plan-changed-base-blob",
                "architect_advice_ids": [adv_accepted],
                "max_architect_rounds": 0,
                "difficulty": "hard"
            })
        )
        .await,
        "validation_failed"
    );
    assert_eq!(
        run_direct_exec_err(
            &grant_id,
            &job_valid_amend.job_id,
            "exec-neg-changed-base-blob",
            vec![adv_accepted],
        )
        .await
        .error_code(),
        NativeErrorCode::ValidationFailed
    );

    // Every single negative check above must have zero provider calls and zero extra reservations.
    assert_eq!(
        provider_invocation_count(&fixture.worker_marker),
        calls_baseline
    );
    let final_grant = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("final ledger")
        .planning_grants
        .into_iter()
        .find(|g| g.grant_id == grant_id)
        .expect("final primary grant");
    assert_eq!(final_grant.reservations.len(), reservations_baseline);

    fixture.shutdown().await;
}

#[tokio::test]
async fn auto_consultation_question_interface_hashes_allow_same_contract_plan_revisions_and_reject_spoofed_or_drifted_contracts(
) {
    use threadlane_native::coworker_workflow::{sha256_hex, OrchestrateWorkBriefPlanRequest};
    use threadlane_native::{open_workflow_stores, ScopedWorkflow};

    let mut fixture = GatewayFixture::start().await;
    let mcp = fixture.enrolled_mcp();
    let now_ms = current_wall_clock_ms().expect("wall clock");
    let wf_store =
        CoworkerWorkflowStore::new(&fixture.plans_dir).expect("open CoworkerWorkflowStore");

    let grant = wf_store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: fixture.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "req-grant-auto-iface-revisions".to_string(),
            expected_ledger_revision: None,
            project_base: fixture.project_base.clone(),
            profile_id: PROFILE_ID.to_string(),
            registry_revision: 3,
            policy: advisor_policy(),
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget: PlanningCallBudget {
                max_total_calls: 24,
                max_orchestrator_calls: 14,
                max_architect_calls: 10,
                max_timeout_ms_per_call: 60_000,
                max_output_bytes_per_call: 65_536,
            },
            created_at_ms: now_ms.saturating_sub(5_000),
            expires_at_ms: now_ms + 3_600_000,
            operator_attested: true,
        })
        .expect("create advisor grant");
    let grant_id = grant.grant_id.clone();

    let (reg_arc, plans_arc) =
        open_workflow_stores(Some(&fixture.registry_dir), Some(&fixture.plans_dir))
            .expect("open workflow stores")
            .expect("workflow stores configured");
    let scoped = ScopedWorkflow::new(reg_arc, plans_arc, fixture.binding_id.clone())
        .expect("ScopedWorkflow");
    let planning_service = CoworkerPlanningService::new(
        wf_store.clone(),
        &fixture.registry_dir,
        planning_artifacts_dir(&fixture.plans_dir),
    )
    .expect("open CoworkerPlanningService");

    // 1. Built-in automatic Orchestrator -> Advisor consultation with `q.interfaces = ["iface_gateway"]`
    //    publishes plan revision 1 and links `adv_auto` whose `interface_ownership_digests` stores
    //    `sha256_hex(b"iface_gateway")` rather than planned-task ownership digests.
    let job_auto: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-auto-prep-job",
            "summary": "Verify built-in auto-consultation advice across legitimate plan revisions",
            "confirmed_requirements": ["Keep iface_gateway contract stable in src/lib.rs"]
        }),
    )
    .await;

    fs::write(&fixture.mode_path, b"orch_ask_then_plan\n").expect("set mode orch_ask_then_plan");
    let rev1_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_auto.job_id,
            "request_key": "req-auto-plan-rev1",
            "max_architect_rounds": 1,
            "difficulty": "hard"
        }),
    )
    .await;
    let rev1_detail = wait_for_command_completion(&mcp, &rev1_submit.command_id).await;
    assert_eq!(
        rev1_detail.status,
        PlanningCommandStatus::Completed,
        "auto-consultation plan rev1 failed: {}",
        format_command_detail_context(&rev1_detail)
    );
    let rev1_pr = match rev1_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::OrchestratorPlan {
            job_id,
            planning_result,
            artifact_verified,
        }) => {
            assert_eq!(job_id, &job_auto.job_id);
            assert!(*artifact_verified);
            planning_result.as_ref().clone()
        }
        other => panic!("expected OrchestratorPlan result_ref for rev1, got {other:?}"),
    };
    assert_eq!(rev1_pr.plan_revision, 1);
    assert_eq!(rev1_pr.architect_advice_ids.len(), 1);
    let adv_auto_id = rev1_pr.architect_advice_ids[0].clone();
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 3);

    let ledger_after_rev1 = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read ledger after rev1");
    let adv_auto = ledger_after_rev1
        .architect_advice
        .iter()
        .find(|a| a.advice_id == adv_auto_id)
        .expect("adv_auto recorded in binding ledger");
    let mut expected_label_digests = BTreeMap::new();
    expected_label_digests.insert("iface_gateway".to_string(), sha256_hex(b"iface_gateway"));
    assert_eq!(
        adv_auto.context_dependencies.interface_ownership_digests,
        expected_label_digests
    );
    assert_eq!(adv_auto.context_dependencies.plan_digest, None);

    // 2. Subsequent legitimate plan revision (`plan@1` -> `plan@2`) under the same decision,
    //    brief, base, policy, and unchanged interface ownership contract MUST SUCCEED.
    fs::write(&fixture.mode_path, b"orch_plan_v2_same_iface\n")
        .expect("set mode orch_plan_v2_same_iface");
    let rev2_args = json!({
        "grant_id": grant_id,
        "job_id": job_auto.job_id,
        "request_key": "req-auto-plan-rev2",
        "plan_id": rev1_pr.plan_id,
        "expected_plan_revision": 1,
        "architect_advice_ids": [adv_auto_id],
        "max_architect_rounds": 0,
        "difficulty": "hard"
    });
    let rev2_submit: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "request_technical_plan", rev2_args.clone()).await;
    let rev2_detail = wait_for_command_completion(&mcp, &rev2_submit.command_id).await;
    assert_eq!(
        rev2_detail.status,
        PlanningCommandStatus::Completed,
        "subsequent legitimate plan rev2 with adv_auto failed: {}",
        format_command_detail_context(&rev2_detail)
    );
    let rev2_pr = match rev2_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::OrchestratorPlan {
            job_id,
            planning_result,
            artifact_verified,
        }) => {
            assert_eq!(job_id, &job_auto.job_id);
            assert!(*artifact_verified);
            planning_result.as_ref().clone()
        }
        other => panic!("expected OrchestratorPlan result_ref for rev2, got {other:?}"),
    };
    assert_eq!(rev2_pr.plan_id, rev1_pr.plan_id);
    assert_eq!(rev2_pr.plan_revision, 2);
    assert_ne!(rev2_pr.plan_digest, rev1_pr.plan_digest);
    assert_eq!(rev2_pr.architect_advice_ids, vec![adv_auto_id.clone()]);
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 4);

    // Verify historical command status marks rev1 Superseded/non-executable and rev2 Published,
    // and idempotent replay performs zero additional provider calls.
    let rev1_history: PlanningCommandDetailResponse = invoke_mcp_ok(
        &mcp,
        "planning_command",
        json!({ "command_id": rev1_submit.command_id }),
    )
    .await;
    assert_eq!(rev1_history.status, PlanningCommandStatus::Completed);
    assert_eq!(
        rev1_history.current_plan_status,
        Some(PlanStatus::Superseded)
    );
    assert_eq!(rev1_history.executable, Some(false));
    assert_eq!(rev2_detail.current_plan_status, Some(PlanStatus::Published));
    assert_eq!(rev2_detail.executable, None);

    let rev2_replay: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "request_technical_plan", rev2_args).await;
    assert_eq!(rev2_replay.command_id, rev2_submit.command_id);
    assert_eq!(
        rev2_replay.submission_status,
        PlanningCommandStartStatus::AlreadyCompleted
    );
    assert!(!rev2_replay.launched);
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 4);

    let published_rev2 = scoped
        .get_plan(&rev2_pr.plan_id, Some(2), None, None)
        .expect("get published plan rev2");
    assert_eq!(published_rev2.header.status, PlanStatus::Published);
    assert_eq!(
        published_rev2.header.plan_digest.as_deref(),
        Some(rev2_pr.plan_digest.as_str())
    );

    // 3. Wrong-job reuse of `adv_auto_id` MUST fail closed across prepare_work, enqueue, and
    //    direct service execution without invoking the provider.
    let job_other: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-auto-prep-wrong-job",
            "summary": "Unrelated job must not reuse job_auto's auto-consultation advice",
            "confirmed_requirements": ["Reject cross-job auto-consultation advice reuse"]
        }),
    )
    .await;
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "request_technical_plan",
            json!({
                "grant_id": grant_id,
                "job_id": job_other.job_id,
                "request_key": "req-auto-wrong-job-plan",
                "architect_advice_ids": [adv_auto_id],
                "max_architect_rounds": 0,
                "difficulty": "hard"
            })
        )
        .await,
        "validation_failed"
    );
    let wrong_job_exec_err = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_other.job_id.clone(),
                request_key: "exec-auto-wrong-job".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: None,
                expected_plan_revision: 0,
                architect_advice_ids: vec![adv_auto_id.clone()],
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect_err("direct execution must reject wrong-job auto-consultation advice");
    assert_eq!(
        wrong_job_exec_err.error_code(),
        NativeErrorCode::ValidationFailed
    );
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "prepare_work",
            json!({
                "grant_id": grant_id,
                "request_key": "req-auto-wrong-new-job",
                "summary": "NewJob accepting foreign auto-consultation advice must fail",
                "confirmed_requirements": ["Must fail closed"],
                "accepted_architect_decisions": [adv_auto_id]
            })
        )
        .await,
        "validation_failed"
    );

    // 4. Spoofed label-hash advice (same `sha256_hex(b"iface_gateway")` digest shape, including
    //    a syntactically forged `_arch_r1_1` key without verified asking Orchestrator
    //    `RequiresArchitectAdvice` provenance) MUST fail closed across enqueue, direct execution,
    //    and direct amendment carry-forward.
    fs::write(&fixture.mode_path, b"architect\n").expect("set mode architect for spoofed advice");
    let dep_blobs = CoworkerPlanningService::compute_dependency_file_blobs(
        &fixture.project_base,
        &["src/lib.rs".to_string()],
    )
    .expect("compute src/lib.rs blob");

    let spoof_plain = planning_service
        .obtain_advisor_advice(&RunArchitectAdviceRequest {
            binding_id: fixture.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant_id.clone(),
            request_key: "req-spoof-label-hash".to_string(),
            expected_ledger_revision: None,
            difficulty: Some(Difficulty::Hard),
            context_dependencies: ArchitectContextDependencies {
                project_base: fixture.project_base.clone(),
                work_brief_digest: job_auto.work_brief_digest.clone(),
                plan_digest: None,
                policy_digest: grant.policy_digest.clone(),
                dependency_file_blobs: dep_blobs.clone(),
                interface_ownership_digests: expected_label_digests.clone(),
                accepted_predecessor_trees: BTreeMap::new(),
            },
            question_summary: "Spoofed plain label hash without Orchestrator question".to_string(),
            prior_advice_context: Vec::new(),
            accept_into_brief: false,
            current_time_ms: current_wall_clock_ms().expect("wall clock"),
        })
        .await
        .expect("record plain spoofed Advisor advice");

    let spoof_forged_key = planning_service
        .obtain_advisor_advice(&RunArchitectAdviceRequest {
            binding_id: fixture.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant_id.clone(),
            request_key: "forged_orch_root_arch_r1_1".to_string(),
            expected_ledger_revision: None,
            difficulty: Some(Difficulty::Hard),
            context_dependencies: ArchitectContextDependencies {
                project_base: fixture.project_base.clone(),
                work_brief_digest: job_other.work_brief_digest.clone(),
                plan_digest: None,
                policy_digest: grant.policy_digest.clone(),
                dependency_file_blobs: dep_blobs,
                interface_ownership_digests: expected_label_digests,
                accepted_predecessor_trees: BTreeMap::new(),
            },
            question_summary: "Spoofed _arch_r1_1 key without Orchestrator reservation".to_string(),
            prior_advice_context: Vec::new(),
            accept_into_brief: false,
            current_time_ms: current_wall_clock_ms().expect("wall clock"),
        })
        .await
        .expect("record forged-key spoofed Advisor advice");
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 6);

    // Spoofed plain advice on planned `job_auto` fails at enqueue and execution.
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "request_technical_plan",
            json!({
                "grant_id": grant_id,
                "job_id": job_auto.job_id,
                "request_key": "req-reject-spoof-plain",
                "plan_id": rev2_pr.plan_id,
                "expected_plan_revision": 2,
                "architect_advice_ids": [spoof_plain.record.advice_id],
                "max_architect_rounds": 0,
                "difficulty": "hard"
            })
        )
        .await,
        "validation_failed"
    );
    let spoof_plain_exec_err = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_auto.job_id.clone(),
                request_key: "exec-reject-spoof-plain".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: Some(rev2_pr.plan_id.clone()),
                expected_plan_revision: 2,
                architect_advice_ids: vec![spoof_plain.record.advice_id.clone()],
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect_err("direct execution must reject spoofed plain label-hash advice");
    assert_eq!(
        spoof_plain_exec_err.error_code(),
        NativeErrorCode::ValidationFailed
    );

    // Spoofed `_arch_r1_1` advice on unplanned `job_other` fails at enqueue, execution, and amendment.
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "request_technical_plan",
            json!({
                "grant_id": grant_id,
                "job_id": job_other.job_id,
                "request_key": "req-reject-spoof-forged-key",
                "architect_advice_ids": [spoof_forged_key.record.advice_id],
                "max_architect_rounds": 0,
                "difficulty": "hard"
            })
        )
        .await,
        "validation_failed"
    );
    let spoof_forged_exec_err = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_other.job_id.clone(),
                request_key: "exec-reject-spoof-forged-key".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: None,
                expected_plan_revision: 0,
                architect_advice_ids: vec![spoof_forged_key.record.advice_id.clone()],
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect_err("direct execution must reject forged-key label-hash advice");
    assert_eq!(
        spoof_forged_exec_err.error_code(),
        NativeErrorCode::ValidationFailed
    );
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "prepare_work",
            json!({
                "grant_id": grant_id,
                "request_key": "req-reject-spoof-forged-amend",
                "summary": "Unrelated job must not reuse job_auto's auto-consultation advice",
                "confirmed_requirements": ["Reject cross-job auto-consultation advice reuse"],
                "accepted_architect_decisions": [spoof_forged_key.record.advice_id],
                "amendment": {
                    "target_job_id": job_other.job_id,
                    "target_job_revision": 1,
                    "amendment_reason": "Spoofed label hash must not carry forward"
                }
            })
        )
        .await,
        "validation_failed"
    );
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 6);

    // 5. When a plan revision changes task interface contracts (`iface_gateway` -> `iface_gateway_v2`),
    //    any subsequent plan revision attaching `adv_auto_id` MUST fail closed.
    fs::write(&fixture.mode_path, b"orch_plan_v2_changed_iface\n")
        .expect("set mode orch_plan_v2_changed_iface");
    let rev3_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_auto.job_id,
            "request_key": "req-auto-plan-rev3-drift-iface",
            "plan_id": rev2_pr.plan_id,
            "expected_plan_revision": 2,
            "architect_advice_ids": [adv_auto_id],
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let rev3_detail = wait_for_command_completion(&mcp, &rev3_submit.command_id).await;
    assert_eq!(
        rev3_detail.status,
        PlanningCommandStatus::Completed,
        "rev3 publishing changed interface contract failed: {}",
        format_command_detail_context(&rev3_detail)
    );
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 7);

    // Now `job_auto` has drifted from baseline `plan@1` (`iface_gateway`) to `iface_gateway_v2`:
    // rev4 attaching `adv_auto_id` MUST fail closed at both enqueue and direct execution.
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "request_technical_plan",
            json!({
                "grant_id": grant_id,
                "job_id": job_auto.job_id,
                "request_key": "req-auto-plan-rev4-after-drift",
                "plan_id": rev2_pr.plan_id,
                "expected_plan_revision": 3,
                "architect_advice_ids": [adv_auto_id],
                "max_architect_rounds": 0,
                "difficulty": "hard"
            })
        )
        .await,
        "validation_failed"
    );
    let drifted_exec_err = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_auto.job_id.clone(),
                request_key: "exec-auto-plan-rev4-after-drift".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: Some(rev2_pr.plan_id.clone()),
                expected_plan_revision: 3,
                architect_advice_ids: vec![adv_auto_id.clone()],
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect_err("direct execution must reject auto-consultation advice after contract drift");
    assert_eq!(
        drifted_exec_err.error_code(),
        NativeErrorCode::ValidationFailed
    );
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 7);

    // 6. Auto-consultation followed by `RequiresHumanClarification` allows verified direct
    //    same-content amendment carry-forward and same-contract plan revisions on the amended job,
    //    while still rejecting contract drift on subsequent revisions.
    let clarify_summary = "Auto-consultation before clarification carried into direct amendment";
    let clarify_reqs = vec!["Preserve iface_gateway boundary across amendment planning"];
    let job_clarify_parent: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-clarify-parent-prep",
            "summary": clarify_summary,
            "confirmed_requirements": clarify_reqs
        }),
    )
    .await;

    fs::write(&fixture.mode_path, b"orch_ask_then_clarify\n")
        .expect("set mode orch_ask_then_clarify");
    let clarify_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_clarify_parent.job_id,
            "request_key": "req-clarify-parent-plan",
            "max_architect_rounds": 1,
            "difficulty": "hard"
        }),
    )
    .await;
    let clarify_detail = wait_for_command_completion(&mcp, &clarify_submit.command_id).await;
    assert_eq!(clarify_detail.status, PlanningCommandStatus::Completed);
    assert!(matches!(
        clarify_detail.result_ref,
        Some(PlanningCommandResultRef::OrchestratorRequiresHumanClarification { .. })
    ));
    assert_eq!(provider_invocation_count(&fixture.worker_marker), 10);

    let adv_clarify_auto_id = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read ledger for adv_clarify_auto")
        .architect_advice
        .into_iter()
        .find(|a| a.context_dependencies.work_brief_digest == job_clarify_parent.work_brief_digest)
        .expect("auto-consultation advice recorded for clarify parent")
        .advice_id;

    let job_clarify_amend: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-clarify-direct-amend",
            "summary": clarify_summary,
            "confirmed_requirements": clarify_reqs,
            "accepted_architect_decisions": [adv_clarify_auto_id],
            "amendment": {
                "target_job_id": job_clarify_parent.job_id,
                "target_job_revision": 1,
                "amendment_reason": "Accept verified auto-consultation decision on same semantic brief"
            }
        }),
    )
    .await;

    // First plan on the amended job (implicit advice resolution from `accepted_architect_decisions`).
    fs::write(&fixture.mode_path, b"orch_plan\n").expect("set mode orch_plan for amend rev1");
    let amend_rev1_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_clarify_amend.job_id,
            "request_key": "req-clarify-amend-plan-rev1",
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let amend_rev1_detail = wait_for_command_completion(&mcp, &amend_rev1_submit.command_id).await;
    assert_eq!(amend_rev1_detail.status, PlanningCommandStatus::Completed);
    let amend_rev1_pr = match amend_rev1_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::OrchestratorPlan {
            planning_result, ..
        }) => planning_result.as_ref().clone(),
        other => panic!("expected OrchestratorPlan for amend rev1, got {other:?}"),
    };
    assert_eq!(
        amend_rev1_pr.architect_advice_ids,
        vec![adv_clarify_auto_id.clone()]
    );

    // Second plan revision on the amended job with unchanged interface contract succeeds.
    fs::write(&fixture.mode_path, b"orch_plan_v2_same_iface\n")
        .expect("set mode orch_plan_v2_same_iface for amend rev2");
    let amend_rev2_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_clarify_amend.job_id,
            "request_key": "req-clarify-amend-plan-rev2",
            "plan_id": amend_rev1_pr.plan_id,
            "expected_plan_revision": 1,
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let amend_rev2_detail = wait_for_command_completion(&mcp, &amend_rev2_submit.command_id).await;
    assert_eq!(amend_rev2_detail.status, PlanningCommandStatus::Completed);

    // Third plan revision changes the interface contract (`iface_gateway` -> `iface_gateway_v2`).
    fs::write(&fixture.mode_path, b"orch_plan_v2_changed_iface\n")
        .expect("set mode orch_plan_v2_changed_iface for amend rev3");
    let amend_rev3_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_clarify_amend.job_id,
            "request_key": "req-clarify-amend-plan-rev3-drift",
            "plan_id": amend_rev1_pr.plan_id,
            "expected_plan_revision": 2,
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let amend_rev3_detail = wait_for_command_completion(&mcp, &amend_rev3_submit.command_id).await;
    assert_eq!(amend_rev3_detail.status, PlanningCommandStatus::Completed);
    let calls_before_amend_drift_reject = provider_invocation_count(&fixture.worker_marker);
    assert_eq!(calls_before_amend_drift_reject, 13);

    // Fourth plan revision on the amended job MUST fail closed (explicit, implicit, and direct).
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "request_technical_plan",
            json!({
                "grant_id": grant_id,
                "job_id": job_clarify_amend.job_id,
                "request_key": "req-clarify-amend-plan-rev4-exp",
                "plan_id": amend_rev1_pr.plan_id,
                "expected_plan_revision": 3,
                "architect_advice_ids": [adv_clarify_auto_id],
                "max_architect_rounds": 0,
                "difficulty": "hard"
            })
        )
        .await,
        "validation_failed"
    );
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "request_technical_plan",
            json!({
                "grant_id": grant_id,
                "job_id": job_clarify_amend.job_id,
                "request_key": "req-clarify-amend-plan-rev4-imp",
                "plan_id": amend_rev1_pr.plan_id,
                "expected_plan_revision": 3,
                "max_architect_rounds": 0,
                "difficulty": "hard"
            })
        )
        .await,
        "validation_failed"
    );
    let amend_drifted_exec_err = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_clarify_amend.job_id.clone(),
                request_key: "exec-clarify-amend-plan-rev4".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: Some(amend_rev1_pr.plan_id.clone()),
                expected_plan_revision: 3,
                architect_advice_ids: vec![adv_clarify_auto_id],
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect_err("direct execution must reject amended job advice after contract drift");
    assert_eq!(
        amend_drifted_exec_err.error_code(),
        NativeErrorCode::ValidationFailed
    );
    assert_eq!(
        provider_invocation_count(&fixture.worker_marker),
        calls_before_amend_drift_reject
    );

    fixture.shutdown().await;
}

fn find_single_prefixed_json(dir: &Path, prefix: &str) -> PathBuf {
    let mut matches: Vec<PathBuf> = fs::read_dir(dir)
        .expect("read store directory")
        .filter_map(|entry| {
            let path = entry.expect("dir entry").path();
            let name = path.file_name()?.to_str()?;
            if name.starts_with(prefix) && name.ends_with(".json") {
                Some(path)
            } else {
                None
            }
        })
        .collect();
    matches.sort();
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one `{prefix}*.json` in {}, got {matches:?}",
        dir.display()
    );
    matches.remove(0)
}

fn write_private_file_0600(path: &Path, bytes: &[u8]) {
    fs::write(path, bytes).expect("write private test file");
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("chmod private file 0600");
}

fn grant_reservation_count(
    wf_store: &CoworkerWorkflowStore,
    binding_id: &str,
    grant_id: &str,
) -> usize {
    wf_store
        .read_binding_ledger(binding_id)
        .expect("read workflow binding ledger")
        .planning_grants
        .into_iter()
        .find(|g| g.grant_id == grant_id)
        .expect("target grant in binding ledger")
        .reservations
        .len()
}

#[tokio::test]
async fn foreign_direct_service_two_revisions_reversed_advice_missing_first_artifact_preserves_target_revision_and_own_evidence_guards(
) {
    use threadlane_native::coworker_workflow::{
        deterministic_prefixed_id, OrchestrateWorkBriefPlanRequest,
    };
    use threadlane_native::{open_workflow_stores, ScopedWorkflow};

    let mut fixture = GatewayFixture::start().await;
    let mcp = fixture.enrolled_mcp();
    let now_ms = current_wall_clock_ms().expect("wall clock");
    let wf_store =
        CoworkerWorkflowStore::new(&fixture.plans_dir).expect("open CoworkerWorkflowStore");

    let grant = wf_store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: fixture.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "req-grant-hist-iso-1".to_string(),
            expected_ledger_revision: None,
            project_base: fixture.project_base.clone(),
            profile_id: PROFILE_ID.to_string(),
            registry_revision: 3,
            policy: advisor_policy(),
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget: PlanningCallBudget {
                max_total_calls: 30,
                max_orchestrator_calls: 20,
                max_architect_calls: 10,
                max_timeout_ms_per_call: 60_000,
                max_output_bytes_per_call: 65_536,
            },
            created_at_ms: now_ms.saturating_sub(5_000),
            expires_at_ms: now_ms + 3_600_000,
            operator_attested: true,
        })
        .expect("create advisor grant");
    let grant_id = grant.grant_id.clone();

    let (reg_arc, plans_arc) =
        open_workflow_stores(Some(&fixture.registry_dir), Some(&fixture.plans_dir))
            .expect("open workflow stores")
            .expect("workflow stores configured");
    let scoped = ScopedWorkflow::new(reg_arc, plans_arc, fixture.binding_id.clone())
        .expect("ScopedWorkflow");
    let artifacts_dir = planning_artifacts_dir(&fixture.plans_dir);
    let planning_service =
        CoworkerPlanningService::new(wf_store.clone(), &fixture.registry_dir, &artifacts_dir)
            .expect("open CoworkerPlanningService");

    // 1. Unrelated job U consults Advisor decisions A and B, accepts them in reversed order [B, A],
    //    and publishes two plans via the DIRECT planning service (writing no gateway command):
    //    first with [B, A], then revision 2 with none ([]), superseding rev 1.
    let u_summary = "Unrelated job U direct-service planning history";
    let u_reqs = vec!["Publish two direct-service revisions for unrelated job U"];
    let job_u_parent: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-u-parent",
            "summary": u_summary,
            "confirmed_requirements": u_reqs
        }),
    )
    .await;

    fs::write(&fixture.mode_path, b"architect\n").expect("set mode architect");
    let consult_a_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "consult_advisor",
        json!({
            "grant_id": grant_id,
            "job_id": job_u_parent.job_id,
            "request_key": "req-u-advisor-a",
            "question": "Advisor decision A for unrelated job U",
            "dependency_files": ["src/lib.rs"],
            "difficulty": "hard"
        }),
    )
    .await;
    let consult_a_detail = wait_for_command_completion(&mcp, &consult_a_submit.command_id).await;
    let adv_a = match consult_a_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::ArchitectAdvice { advice_id, .. }) => advice_id.clone(),
        other => panic!("expected ArchitectAdvice for A, got {other:?}"),
    };

    let consult_b_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "consult_advisor",
        json!({
            "grant_id": grant_id,
            "job_id": job_u_parent.job_id,
            "request_key": "req-u-advisor-b",
            "question": "Advisor decision B for unrelated job U",
            "dependency_files": ["src/lib.rs"],
            "difficulty": "hard"
        }),
    )
    .await;
    let consult_b_detail = wait_for_command_completion(&mcp, &consult_b_submit.command_id).await;
    let adv_b = match consult_b_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::ArchitectAdvice { advice_id, .. }) => advice_id.clone(),
        other => panic!("expected ArchitectAdvice for B, got {other:?}"),
    };

    let job_u: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-u-reversed-decisions",
            "summary": u_summary,
            "confirmed_requirements": u_reqs,
            "accepted_architect_decisions": [adv_b, adv_a],
            "amendment": {
                "target_job_id": job_u_parent.job_id,
                "target_job_revision": 1,
                "amendment_reason": "Accept Advisor decisions [B, A] in reversed order"
            }
        }),
    )
    .await;

    fs::write(&fixture.mode_path, b"orch_plan\n").expect("set mode orch_plan for U rev1");
    let u_rev1_outcome = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_u.job_id.clone(),
                request_key: "direct-u-plan-rev1".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: None,
                expected_plan_revision: 0,
                architect_advice_ids: vec![adv_b.clone(), adv_a.clone()],
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect("direct service U rev1");
    let u_rev1_pr = u_rev1_outcome
        .planning_result
        .clone()
        .expect("U rev1 planning_result");
    assert_eq!(
        u_rev1_pr.architect_advice_ids,
        vec![adv_b.clone(), adv_a.clone()]
    );

    fs::write(&fixture.mode_path, b"orch_plan_v2_same_iface\n")
        .expect("set mode orch_plan_v2_same_iface for U rev2");
    let u_rev2_outcome = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_u.job_id.clone(),
                request_key: "direct-u-plan-rev2".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: Some(u_rev1_pr.plan_id.clone()),
                expected_plan_revision: 1,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect("direct service U rev2");
    let u_rev2_pr = u_rev2_outcome
        .planning_result
        .clone()
        .expect("U rev2 planning_result");
    assert!(u_rev2_pr.architect_advice_ids.is_empty());
    assert_eq!(u_rev2_pr.plan_id, u_rev1_pr.plan_id);
    assert_eq!(u_rev2_pr.plan_revision, 2);

    // Also cover an unrelated direct-service job U2 that publishes rev 1 with an explicit `plan_id`
    // and then publishes a distinct second plan (overwriting U2.planning_result with a new plan_id).
    let job_u2: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-u2-explicit-plan",
            "summary": "Unrelated job U2 with explicit plan_id first revision",
            "confirmed_requirements": ["Verify explicit plan_id receipt isolation"]
        }),
    )
    .await;
    fs::write(&fixture.mode_path, b"orch_plan\n").expect("set mode orch_plan for U2 rev1");
    let u2_rev1_outcome = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_u2.job_id.clone(),
                request_key: "direct-u2-explicit-plan-rev1".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: Some("plan_u2_explicit_custom".to_string()),
                expected_plan_revision: 0,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect("direct service U2 explicit rev1");
    let u2_rev1_pr = u2_rev1_outcome
        .planning_result
        .clone()
        .expect("U2 rev1 planning_result");
    assert_eq!(u2_rev1_pr.plan_id, "plan_u2_explicit_custom");

    fs::write(&fixture.mode_path, b"orch_plan_v2_same_iface\n")
        .expect("set mode orch_plan_v2_same_iface for U2 rev2");
    let u2_rev2_outcome = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_u2.job_id.clone(),
                request_key: "direct-u2-second-plan".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: None,
                expected_plan_revision: 0,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect("direct service U2 second plan");
    let u2_rev2_pr = u2_rev2_outcome
        .planning_result
        .clone()
        .expect("U2 rev2 planning_result");
    assert_ne!(u2_rev2_pr.plan_id, u2_rev1_pr.plan_id);

    // 2. Distinct job J on the same binding, base, and policy has its own complete evidence and
    //    earlier accepted direct-parent Advisor handoff, and publishes J rev 1.
    let j_summary = "Target job J with direct-parent Advisor handoff";
    let j_reqs = vec!["Preserve target job J evidence and baseline guards"];
    let job_j_parent: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-j-parent",
            "summary": j_summary,
            "confirmed_requirements": j_reqs
        }),
    )
    .await;

    fs::write(&fixture.mode_path, b"architect\n").expect("set mode architect for J");
    let consult_j_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "consult_advisor",
        json!({
            "grant_id": grant_id,
            "job_id": job_j_parent.job_id,
            "request_key": "req-j-advisor",
            "question": "Confirm boundary contract for target job J",
            "dependency_files": ["src/lib.rs"],
            "difficulty": "hard"
        }),
    )
    .await;
    let consult_j_detail = wait_for_command_completion(&mcp, &consult_j_submit.command_id).await;
    let adv_j = match consult_j_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::ArchitectAdvice { advice_id, .. }) => advice_id.clone(),
        other => panic!("expected ArchitectAdvice for J, got {other:?}"),
    };

    let prep_j_args = json!({
        "grant_id": grant_id,
        "request_key": "req-prep-j-direct-amend",
        "summary": j_summary,
        "confirmed_requirements": j_reqs,
        "accepted_architect_decisions": [adv_j],
        "amendment": {
            "target_job_id": job_j_parent.job_id,
            "target_job_revision": 1,
            "amendment_reason": "Carry verified Advisor decision into target job J"
        }
    });
    let job_j: PrepareWorkResponse = invoke_mcp_ok(&mcp, "prepare_work", prep_j_args.clone()).await;

    fs::write(&fixture.mode_path, b"orch_plan\n").expect("set mode orch_plan for J rev1");
    let j_rev1_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_j.job_id,
            "request_key": "req-j-plan-rev1",
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let j_rev1_detail = wait_for_command_completion(&mcp, &j_rev1_submit.command_id).await;
    assert_eq!(
        j_rev1_detail.status,
        PlanningCommandStatus::Completed,
        "J rev1 failed: {}",
        format_command_detail_context(&j_rev1_detail)
    );
    let j_rev1_pr = match j_rev1_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::OrchestratorPlan {
            planning_result, ..
        }) => planning_result.as_ref().clone(),
        other => panic!("expected OrchestratorPlan for J rev1, got {other:?}"),
    };
    assert_eq!(j_rev1_pr.architect_advice_ids, vec![adv_j.clone()]);

    // 3. Remove BOTH foreign direct-service first artifacts (`U` rev 1 and `U2` explicit rev 1).
    let u_rev1_artifact_path = artifacts_dir.join(format!("{}.json", u_rev1_pr.report_artifact_id));
    let u2_rev1_artifact_path =
        artifacts_dir.join(format!("{}.json", u2_rev1_pr.report_artifact_id));
    fs::remove_file(&u_rev1_artifact_path).expect("delete U rev1 verified artifact");
    fs::remove_file(&u2_rev1_artifact_path).expect("delete U2 explicit rev1 verified artifact");
    assert!(!u_rev1_artifact_path.exists());
    assert!(!u2_rev1_artifact_path.exists());

    // Confirm the read-only workflow API resolves both implicit superseded and explicit plan_id receipts.
    let readonly_receipts = ScopedWorkflow::read_save_draft_receipts_readonly_at(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.binding_id,
    )
    .expect("read readonly save_draft receipts");
    let u_rev1_save_key = deterministic_prefixed_id(
        "threadlane-native:coworker-plan-draft:v1",
        "cmd",
        &[
            &fixture.binding_id,
            &job_u.job_id,
            &u_rev1_pr.reservation_id,
            &u_rev1_pr.output_digest,
        ],
    );
    let u_rev1_verified = readonly_receipts
        .verify_completed_save_draft(&u_rev1_save_key)
        .expect("verify U rev1 save_draft receipt")
        .expect("U rev1 save_draft receipt present");
    assert_eq!(u_rev1_verified.explicit_plan_id, None);
    assert_eq!(u_rev1_verified.plan_id, u_rev1_pr.plan_id);
    assert_eq!(u_rev1_verified.revision, 1);
    assert_eq!(u_rev1_verified.status, PlanStatus::Superseded);
    assert_eq!(u_rev1_verified.plan_digest, u_rev1_pr.plan_digest);

    let u2_rev1_save_key = deterministic_prefixed_id(
        "threadlane-native:coworker-plan-draft:v1",
        "cmd",
        &[
            &fixture.binding_id,
            &job_u2.job_id,
            &u2_rev1_pr.reservation_id,
            &u2_rev1_pr.output_digest,
        ],
    );
    let u2_rev1_verified = readonly_receipts
        .verify_completed_save_draft(&u2_rev1_save_key)
        .expect("verify U2 explicit rev1 save_draft receipt")
        .expect("U2 explicit rev1 save_draft receipt present");
    assert_eq!(
        u2_rev1_verified.explicit_plan_id.as_deref(),
        Some("plan_u2_explicit_custom")
    );
    assert_eq!(u2_rev1_verified.plan_id, "plan_u2_explicit_custom");
    assert_eq!(u2_rev1_verified.revision, 1);
    assert_eq!(u2_rev1_verified.status, PlanStatus::Published);
    assert_eq!(u2_rev1_verified.plan_digest, u2_rev1_pr.plan_digest);

    // 4. While U's and U2's first artifacts remain missing, J's prepare_work replay, explicit-advice
    //    gateway revision, exact original replay, implicit-advice gateway revision, and direct-service
    //    revision all succeed cleanly.
    let prep_j_replay: PrepareWorkResponse =
        invoke_mcp_ok(&mcp, "prepare_work", prep_j_args.clone()).await;
    assert_eq!(prep_j_replay.job_id, job_j.job_id);

    fs::write(&fixture.mode_path, b"orch_plan_v2_same_iface\n")
        .expect("set mode orch_plan_v2_same_iface for J rev2");
    let j_rev2_args = json!({
        "grant_id": grant_id,
        "job_id": job_j.job_id,
        "request_key": "req-j-plan-rev2-explicit",
        "plan_id": j_rev1_pr.plan_id,
        "expected_plan_revision": 1,
        "architect_advice_ids": [adv_j],
        "max_architect_rounds": 0,
        "difficulty": "hard"
    });
    let j_rev2_submit: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "request_technical_plan", j_rev2_args.clone()).await;
    let j_rev2_detail = wait_for_command_completion(&mcp, &j_rev2_submit.command_id).await;
    assert_eq!(
        j_rev2_detail.status,
        PlanningCommandStatus::Completed,
        "J rev2 explicit failed with missing foreign artifacts: {}",
        format_command_detail_context(&j_rev2_detail)
    );
    let calls_after_j_rev2 = provider_invocation_count(&fixture.worker_marker);

    // Valid original replay returns AlreadyCompleted with zero extra provider calls.
    let j_rev2_replay: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "request_technical_plan", j_rev2_args).await;
    assert_eq!(j_rev2_replay.command_id, j_rev2_submit.command_id);
    assert_eq!(j_rev2_replay.status, PlanningCommandStatus::Completed);
    assert_eq!(
        j_rev2_replay.submission_status,
        PlanningCommandStartStatus::AlreadyCompleted
    );
    assert!(!j_rev2_replay.launched);
    assert_eq!(
        provider_invocation_count(&fixture.worker_marker),
        calls_after_j_rev2
    );

    // Implicit advice reuse (omitted `architect_advice_ids`) on J rev 3.
    let j_rev3_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_j.job_id,
            "request_key": "req-j-plan-rev3-implicit",
            "plan_id": j_rev1_pr.plan_id,
            "expected_plan_revision": 2,
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let j_rev3_detail = wait_for_command_completion(&mcp, &j_rev3_submit.command_id).await;
    assert_eq!(
        j_rev3_detail.status,
        PlanningCommandStatus::Completed,
        "J rev3 implicit failed: {}",
        format_command_detail_context(&j_rev3_detail)
    );

    // Direct-service revision on J rev 4 also succeeds while U's first artifacts remain missing.
    let j_rev4_outcome = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_j.job_id.clone(),
                request_key: "direct-j-plan-rev4".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: Some(j_rev1_pr.plan_id.clone()),
                expected_plan_revision: 3,
                architect_advice_ids: vec![adv_j.clone()],
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect("direct service J rev4 succeeds with missing foreign artifacts");
    assert_eq!(
        j_rev4_outcome
            .planning_result
            .as_ref()
            .expect("J rev4 planning_result")
            .plan_revision,
        4
    );

    // Also verify a distinct non-amendment exact-digest job `J_exact` (whose earlier pre-plan
    // Advisor advice was consulted directly on `J_exact` before rev 1) can revise cleanly while
    // `U` and `U2`'s first artifacts remain missing.
    let job_j_exact: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-j-exact",
            "summary": "Target job J_exact with exact-digest pre-plan Advisor advice",
            "confirmed_requirements": ["Verify exact-digest pre-plan advice baseline isolation"]
        }),
    )
    .await;
    fs::write(&fixture.mode_path, b"architect\n").expect("set mode architect for J_exact");
    let adv_j_exact_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "consult_advisor",
        json!({
            "grant_id": grant_id,
            "job_id": job_j_exact.job_id,
            "request_key": "req-j-exact-advisor",
            "question": "Pre-plan contract advice for exact-digest job J_exact",
            "dependency_files": ["src/lib.rs"],
            "difficulty": "hard"
        }),
    )
    .await;
    let adv_j_exact_detail =
        wait_for_command_completion(&mcp, &adv_j_exact_submit.command_id).await;
    let adv_j_exact = match adv_j_exact_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::ArchitectAdvice { advice_id, .. }) => advice_id.clone(),
        other => panic!("expected ArchitectAdvice for J_exact, got {other:?}"),
    };
    fs::write(&fixture.mode_path, b"orch_plan\n").expect("set mode orch_plan for J_exact rev1");
    let j_exact_rev1_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_j_exact.job_id,
            "request_key": "req-j-exact-plan-rev1",
            "architect_advice_ids": [adv_j_exact],
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let j_exact_rev1_detail =
        wait_for_command_completion(&mcp, &j_exact_rev1_submit.command_id).await;
    let j_exact_rev1_pr = match j_exact_rev1_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::OrchestratorPlan {
            planning_result, ..
        }) => planning_result.as_ref().clone(),
        other => panic!("expected OrchestratorPlan for J_exact rev1, got {other:?}"),
    };
    fs::write(&fixture.mode_path, b"orch_plan_v2_same_iface\n")
        .expect("set mode orch_plan_v2_same_iface for J_exact rev2");
    let j_exact_rev2_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_j_exact.job_id,
            "request_key": "req-j-exact-plan-rev2",
            "plan_id": j_exact_rev1_pr.plan_id,
            "expected_plan_revision": 1,
            "architect_advice_ids": [adv_j_exact],
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let j_exact_rev2_detail =
        wait_for_command_completion(&mcp, &j_exact_rev2_submit.command_id).await;
    assert_eq!(
        j_exact_rev2_detail.status,
        PlanningCommandStatus::Completed,
        "J_exact rev2 failed with missing foreign artifacts: {}",
        format_command_detail_context(&j_exact_rev2_detail)
    );

    // 5. Removing J's OWN historical rev 1 baseline artifact MUST fail closed before any provider
    //    effects even though J's latest rev 4 planning_result artifact is intact.
    let j_rev1_artifact_path = artifacts_dir.join(format!("{}.json", j_rev1_pr.report_artifact_id));
    let saved_j_rev1_artifact = fs::read(&j_rev1_artifact_path).expect("read J rev1 artifact");
    fs::remove_file(&j_rev1_artifact_path).expect("delete J rev1 own baseline artifact");
    let calls_before_own_missing = provider_invocation_count(&fixture.worker_marker);
    let reservations_before_own_missing =
        grant_reservation_count(&wf_store, &fixture.binding_id, &grant_id);

    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "request_technical_plan",
            json!({
                "grant_id": grant_id,
                "job_id": job_j.job_id,
                "request_key": "req-j-plan-rev5-own-missing-exp",
                "plan_id": j_rev1_pr.plan_id,
                "expected_plan_revision": 4,
                "architect_advice_ids": [adv_j],
                "max_architect_rounds": 0,
                "difficulty": "hard"
            })
        )
        .await,
        "validation_failed"
    );
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "request_technical_plan",
            json!({
                "grant_id": grant_id,
                "job_id": job_j.job_id,
                "request_key": "req-j-plan-rev5-own-missing-imp",
                "plan_id": j_rev1_pr.plan_id,
                "expected_plan_revision": 4,
                "max_architect_rounds": 0,
                "difficulty": "hard"
            })
        )
        .await,
        "validation_failed"
    );
    assert_eq!(
        invoke_mcp_err_code(&mcp, "prepare_work", prep_j_args.clone()).await,
        "validation_failed"
    );
    let direct_own_missing_err = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_j.job_id.clone(),
                request_key: "direct-j-plan-rev5-own-missing".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: Some(j_rev1_pr.plan_id.clone()),
                expected_plan_revision: 4,
                architect_advice_ids: vec![adv_j.clone()],
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect_err("direct service must reject missing own historical baseline artifact");
    assert_eq!(
        direct_own_missing_err.error_code(),
        NativeErrorCode::ValidationFailed
    );
    assert_eq!(
        provider_invocation_count(&fixture.worker_marker),
        calls_before_own_missing
    );
    assert_eq!(
        grant_reservation_count(&wf_store, &fixture.binding_id, &grant_id),
        reservations_before_own_missing
    );

    // Restoring ONLY J's own artifact (leaving U's and U2's foreign artifacts missing) makes
    // J's revision pass again.
    write_private_file_0600(&j_rev1_artifact_path, &saved_j_rev1_artifact);
    let j_rev5_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_j.job_id,
            "request_key": "req-j-plan-rev5-restored-own",
            "plan_id": j_rev1_pr.plan_id,
            "expected_plan_revision": 4,
            "architect_advice_ids": [adv_j],
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let j_rev5_detail = wait_for_command_completion(&mcp, &j_rev5_submit.command_id).await;
    assert_eq!(
        j_rev5_detail.status,
        PlanningCommandStatus::Completed,
        "J rev5 failed after restoring own artifact: {}",
        format_command_detail_context(&j_rev5_detail)
    );

    fixture.shutdown().await;
}

#[tokio::test]
async fn normal_gateway_vs_direct_service_unproven_artifacts_and_invalid_receipts_fail_closed_before_provider_effects(
) {
    use threadlane_native::coworker_workflow::{
        OrchestrateWorkBriefPlanRequest, OrchestratorPlanningDecision,
    };
    use threadlane_native::{open_workflow_stores, ScopedWorkflow};

    let mut fixture = GatewayFixture::start().await;
    let mcp = fixture.enrolled_mcp();
    let now_ms = current_wall_clock_ms().expect("wall clock");
    let wf_store =
        CoworkerWorkflowStore::new(&fixture.plans_dir).expect("open CoworkerWorkflowStore");

    let grant = wf_store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: fixture.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "req-grant-hist-iso-2".to_string(),
            expected_ledger_revision: None,
            project_base: fixture.project_base.clone(),
            profile_id: PROFILE_ID.to_string(),
            registry_revision: 3,
            policy: advisor_policy(),
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget: PlanningCallBudget {
                max_total_calls: 30,
                max_orchestrator_calls: 20,
                max_architect_calls: 10,
                max_timeout_ms_per_call: 60_000,
                max_output_bytes_per_call: 65_536,
            },
            created_at_ms: now_ms.saturating_sub(5_000),
            expires_at_ms: now_ms + 3_600_000,
            operator_attested: true,
        })
        .expect("create advisor grant");
    let grant_id = grant.grant_id.clone();

    let (reg_arc, plans_arc) =
        open_workflow_stores(Some(&fixture.registry_dir), Some(&fixture.plans_dir))
            .expect("open workflow stores")
            .expect("workflow stores configured");
    let scoped = ScopedWorkflow::new(reg_arc, plans_arc, fixture.binding_id.clone())
        .expect("ScopedWorkflow");
    let artifacts_dir = planning_artifacts_dir(&fixture.plans_dir);
    let planning_service =
        CoworkerPlanningService::new(wf_store.clone(), &fixture.registry_dir, &artifacts_dir)
            .expect("open CoworkerPlanningService");

    // 1. Normal gateway route for unrelated job U_gw: a non-plan clarification outcome (owned ONLY
    //    via PlanningCommandRecord, no save_draft receipt) plus two published plan revisions.
    let job_u_gw: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-u-gw",
            "summary": "Unrelated job U_gw using normal MCP gateway commands",
            "confirmed_requirements": ["Exercise normal gateway ownership isolation"]
        }),
    )
    .await;

    fs::write(&fixture.mode_path, b"orch_clarify\n").expect("set mode orch_clarify for U_gw");
    let u_gw_clarify_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_u_gw.job_id,
            "request_key": "req-u-gw-clarify",
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let u_gw_clarify_detail =
        wait_for_command_completion(&mcp, &u_gw_clarify_submit.command_id).await;
    let u_gw_clarify_art_id = match u_gw_clarify_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::OrchestratorRequiresHumanClarification {
            report_artifact_id,
            ..
        }) => report_artifact_id.clone(),
        other => panic!("expected OrchestratorRequiresHumanClarification, got {other:?}"),
    };

    fs::write(&fixture.mode_path, b"orch_plan\n").expect("set mode orch_plan for U_gw rev1");
    let u_gw_rev1_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_u_gw.job_id,
            "request_key": "req-u-gw-plan-rev1",
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let u_gw_rev1_detail = wait_for_command_completion(&mcp, &u_gw_rev1_submit.command_id).await;
    let u_gw_rev1_pr = match u_gw_rev1_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::OrchestratorPlan {
            planning_result, ..
        }) => planning_result.as_ref().clone(),
        other => panic!("expected OrchestratorPlan for U_gw rev1, got {other:?}"),
    };

    fs::write(&fixture.mode_path, b"orch_plan_v2_same_iface\n")
        .expect("set mode orch_plan_v2_same_iface for U_gw rev2");
    let u_gw_rev2_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_u_gw.job_id,
            "request_key": "req-u-gw-plan-rev2",
            "plan_id": u_gw_rev1_pr.plan_id,
            "expected_plan_revision": 1,
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let u_gw_rev2_detail = wait_for_command_completion(&mcp, &u_gw_rev2_submit.command_id).await;
    assert_eq!(u_gw_rev2_detail.status, PlanningCommandStatus::Completed);

    // 2. Direct-service non-plan clarification reservation (NO gateway command, NO save_draft receipt):
    //    unproven artifact must stay usable while intact, and MUST refuse if deleted.
    let job_u_unproven: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-u-direct-clarify",
            "summary": "Unrelated job with direct-service non-plan reservation",
            "confirmed_requirements": ["Verify unproven artifacts are never silently skipped"]
        }),
    )
    .await;
    fs::write(&fixture.mode_path, b"orch_clarify\n").expect("set mode orch_clarify for U_unproven");
    let u_unproven_outcome = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_u_unproven.job_id.clone(),
                request_key: "direct-u-unproven-clarify".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: None,
                expected_plan_revision: 0,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect("direct service clarify");
    assert!(matches!(
        u_unproven_outcome.decision,
        OrchestratorPlanningDecision::RequiresHumanClarification(_)
    ));
    let u_unproven_art_id = u_unproven_outcome
        .reservation
        .report_artifact_id
        .clone()
        .expect("unproven artifact id");
    let u_unproven_art_path = artifacts_dir.join(format!("{u_unproven_art_id}.json"));

    // 3. Direct-service two-revision unrelated job U_direct (for invalid-receipt tamper checks).
    let job_u_direct: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-u-direct-plans",
            "summary": "Unrelated job U_direct publishing two direct-service plans",
            "confirmed_requirements": ["Test invalid save_draft receipts without provider calls"]
        }),
    )
    .await;
    fs::write(&fixture.mode_path, b"orch_plan\n").expect("set mode orch_plan for U_direct rev1");
    let u_direct_rev1_pr = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_u_direct.job_id.clone(),
                request_key: "direct-u-plans-rev1".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: None,
                expected_plan_revision: 0,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect("U_direct rev1")
        .planning_result
        .expect("U_direct rev1 planning_result");

    fs::write(&fixture.mode_path, b"orch_plan_v2_same_iface\n")
        .expect("set mode orch_plan_v2_same_iface for U_direct rev2");
    let u_direct_rev2_pr = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_u_direct.job_id.clone(),
                request_key: "direct-u-plans-rev2".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: Some(u_direct_rev1_pr.plan_id.clone()),
                expected_plan_revision: 1,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect("U_direct rev2")
        .planning_result
        .expect("U_direct rev2 planning_result");
    assert_eq!(u_direct_rev2_pr.plan_revision, 2);

    // 4. Target job J with earlier accepted Advisor advice and published rev 1 plan.
    let job_j_parent: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-j2-parent",
            "summary": "Target job J2 for gateway vs direct and invalid-receipt guards",
            "confirmed_requirements": ["Reject unproven missing artifacts and invalid receipts"]
        }),
    )
    .await;
    fs::write(&fixture.mode_path, b"architect\n").expect("set mode architect for J2");
    let adv_j_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "consult_advisor",
        json!({
            "grant_id": grant_id,
            "job_id": job_j_parent.job_id,
            "request_key": "req-j2-advisor",
            "question": "Advisor guidance for target job J2",
            "dependency_files": ["src/lib.rs"],
            "difficulty": "hard"
        }),
    )
    .await;
    let adv_j_detail = wait_for_command_completion(&mcp, &adv_j_submit.command_id).await;
    let adv_j = match adv_j_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::ArchitectAdvice { advice_id, .. }) => advice_id.clone(),
        other => panic!("expected ArchitectAdvice for J2, got {other:?}"),
    };
    let prep_j_args = json!({
        "grant_id": grant_id,
        "request_key": "req-prep-j2-amend",
        "summary": "Target job J2 for gateway vs direct and invalid-receipt guards",
        "confirmed_requirements": ["Reject unproven missing artifacts and invalid receipts"],
        "accepted_architect_decisions": [adv_j],
        "amendment": {
            "target_job_id": job_j_parent.job_id,
            "target_job_revision": 1,
            "amendment_reason": "Attach verified Advisor decision to target job J2"
        }
    });
    let job_j: PrepareWorkResponse = invoke_mcp_ok(&mcp, "prepare_work", prep_j_args.clone()).await;

    fs::write(&fixture.mode_path, b"orch_plan\n").expect("set mode orch_plan for J2 rev1");
    let j_rev1_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_j.job_id,
            "request_key": "req-j2-plan-rev1",
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let j_rev1_detail = wait_for_command_completion(&mcp, &j_rev1_submit.command_id).await;
    let j_rev1_pr = match j_rev1_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::OrchestratorPlan {
            planning_result, ..
        }) => planning_result.as_ref().clone(),
        other => panic!("expected OrchestratorPlan for J2 rev1, got {other:?}"),
    };

    // 5. Delete U_gw's clarify + rev 1 artifacts AND U_direct's rev 1 artifact, while keeping
    //    U_unproven's intact artifact on disk. J rev 2 MUST succeed (proving normal gateway plan,
    //    normal gateway non-plan, direct-service plan, and intact earlier unproven record all work).
    fs::remove_file(artifacts_dir.join(format!("{u_gw_clarify_art_id}.json")))
        .expect("delete U_gw clarify artifact");
    fs::remove_file(artifacts_dir.join(format!("{}.json", u_gw_rev1_pr.report_artifact_id)))
        .expect("delete U_gw rev1 artifact");
    fs::remove_file(artifacts_dir.join(format!("{}.json", u_direct_rev1_pr.report_artifact_id)))
        .expect("delete U_direct rev1 artifact");

    fs::write(&fixture.mode_path, b"orch_plan_v2_same_iface\n")
        .expect("set mode orch_plan_v2_same_iface for J2 rev2");
    let j_rev2_args = json!({
        "grant_id": grant_id,
        "job_id": job_j.job_id,
        "request_key": "req-j2-plan-rev2",
        "plan_id": j_rev1_pr.plan_id,
        "expected_plan_revision": 1,
        "architect_advice_ids": [adv_j],
        "max_architect_rounds": 0,
        "difficulty": "hard"
    });
    let j_rev2_submit: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "request_technical_plan", j_rev2_args.clone()).await;
    let j_rev2_detail = wait_for_command_completion(&mcp, &j_rev2_submit.command_id).await;
    assert_eq!(
        j_rev2_detail.status,
        PlanningCommandStatus::Completed,
        "J2 rev2 failed while intact unproven artifact and missing proven foreign artifacts exist: {}",
        format_command_detail_context(&j_rev2_detail)
    );

    let calls_baseline = provider_invocation_count(&fixture.worker_marker);
    let reservations_baseline = grant_reservation_count(&wf_store, &fixture.binding_id, &grant_id);

    // 6. Deleting the UNPROVEN direct-service clarify artifact MUST refuse before provider effects.
    let saved_unproven_art = fs::read(&u_unproven_art_path).expect("read unproven artifact");
    fs::remove_file(&u_unproven_art_path).expect("delete unproven artifact");
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "request_technical_plan",
            json!({
                "grant_id": grant_id,
                "job_id": job_j.job_id,
                "request_key": "req-j2-rev3-unproven-missing",
                "plan_id": j_rev1_pr.plan_id,
                "expected_plan_revision": 2,
                "architect_advice_ids": [adv_j],
                "max_architect_rounds": 0,
                "difficulty": "hard"
            })
        )
        .await,
        "validation_failed"
    );
    let unproven_direct_err = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_j.job_id.clone(),
                request_key: "direct-j2-rev3-unproven-missing".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: Some(j_rev1_pr.plan_id.clone()),
                expected_plan_revision: 2,
                architect_advice_ids: vec![adv_j.clone()],
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect_err("direct service must never skip unproven missing artifact");
    assert_eq!(
        unproven_direct_err.error_code(),
        NativeErrorCode::ValidationFailed
    );
    assert_eq!(
        provider_invocation_count(&fixture.worker_marker),
        calls_baseline
    );
    assert_eq!(
        grant_reservation_count(&wf_store, &fixture.binding_id, &grant_id),
        reservations_baseline
    );
    write_private_file_0600(&u_unproven_art_path, &saved_unproven_art);

    // 7. Invalid, tampered, corrupt, or conflicting receipts MUST refuse before any provider action.
    let journal_path = find_single_prefixed_json(&fixture.plans_dir, "workflow-commands-");
    let plans_path = fixture.plans_dir.join("plans.json");
    let cmd_ledger_path = find_single_prefixed_json(
        &planning_commands_dir(&fixture.plans_dir),
        "planning-commands-",
    );
    let original_journal = fs::read(&journal_path).expect("read original workflow journal");
    let original_plans = fs::read(&plans_path).expect("read original plans.json");
    let original_cmd_ledger = fs::read(&cmd_ledger_path).expect("read original command ledger");

    let assert_refused_without_provider_effect =
        |label: &str, expected_mcp_code: &str, expected_native_code: NativeErrorCode| {
            let mcp = &mcp;
            let planning_service = &planning_service;
            let scoped = &scoped;
            let wf_store = &wf_store;
            let worker_marker = &fixture.worker_marker;
            let binding_id = fixture.binding_id.clone();
            let grant_id = grant_id.clone();
            let job_id = job_j.job_id.clone();
            let plan_id = j_rev1_pr.plan_id.clone();
            let adv_j = adv_j.clone();
            let label = label.to_string();
            let expected_mcp_code = expected_mcp_code.to_string();
            async move {
                assert_eq!(
                    invoke_mcp_err_code(
                        mcp,
                        "request_technical_plan",
                        json!({
                            "grant_id": grant_id,
                            "job_id": job_id,
                            "request_key": format!("req-j2-invalid-{label}"),
                            "plan_id": plan_id,
                            "expected_plan_revision": 2,
                            "architect_advice_ids": [adv_j],
                            "max_architect_rounds": 0,
                            "difficulty": "hard"
                        })
                    )
                    .await,
                    expected_mcp_code,
                    "MCP request_technical_plan must refuse invalid receipt case `{label}`"
                );
                let direct_err = planning_service
                    .orchestrate_brief_to_plan(
                        scoped,
                        &OrchestrateWorkBriefPlanRequest {
                            binding_id: binding_id.clone(),
                            thread_uuid: THREAD_UUID.to_string(),
                            grant_id: grant_id.clone(),
                            job_id,
                            request_key: format!("direct-j2-invalid-{label}"),
                            expected_ledger_revision: None,
                            difficulty: Some(Difficulty::Hard),
                            plan_id: Some(plan_id),
                            expected_plan_revision: 2,
                            architect_advice_ids: vec![adv_j],
                            max_architect_rounds: 0,
                            architect_difficulty: None,
                            current_time_ms: current_wall_clock_ms().expect("wall clock"),
                        },
                    )
                    .await
                    .expect_err("direct service must refuse invalid receipt case");
                assert_eq!(
                    direct_err.error_code(),
                    expected_native_code,
                    "direct service error code mismatch for `{label}`"
                );
                assert_eq!(
                    provider_invocation_count(worker_marker),
                    calls_baseline,
                    "invalid receipt `{label}` must not invoke provider"
                );
                assert_eq!(
                    grant_reservation_count(wf_store, &binding_id, &grant_id),
                    reservations_baseline,
                    "invalid receipt `{label}` must not create reservations"
                );
            }
        };

    // 7a. Tampered plan_digest on Superseded U_direct rev 1 journal entry.
    {
        let mut journal_json: Value =
            serde_json::from_slice(&original_journal).expect("parse journal json");
        let entry = journal_json["entries"]
            .as_array_mut()
            .expect("entries")
            .iter_mut()
            .find(|e| {
                e["resolved_plan_id"] == u_direct_rev1_pr.plan_id && e["target_revision"] == 1
            })
            .expect("U_direct rev1 journal entry");
        let forged = "0".repeat(64);
        entry["plan_digest"] = json!(forged);
        entry["state"]["plan_digest"] = json!(forged);
        write_private_file_0600(
            &journal_path,
            &serde_json::to_vec_pretty(&journal_json).expect("serialize journal"),
        );
        assert_refused_without_provider_effect(
            "tampered-digest",
            "protocol_error",
            NativeErrorCode::ProtocolError,
        )
        .await;
        write_private_file_0600(&journal_path, &original_journal);
    }

    // 7b. Non-completed Pending and Failed states on U_direct rev 1 journal entry.
    for (case_label, status_str) in [("pending-state", "pending"), ("failed-state", "failed")] {
        let mut journal_json: Value =
            serde_json::from_slice(&original_journal).expect("parse journal json");
        let entry = journal_json["entries"]
            .as_array_mut()
            .expect("entries")
            .iter_mut()
            .find(|e| {
                e["resolved_plan_id"] == u_direct_rev1_pr.plan_id && e["target_revision"] == 1
            })
            .expect("U_direct rev1 journal entry");
        entry["state"] = json!({ "status": status_str });
        write_private_file_0600(
            &journal_path,
            &serde_json::to_vec_pretty(&journal_json).expect("serialize journal"),
        );
        assert_refused_without_provider_effect(
            case_label,
            "validation_failed",
            NativeErrorCode::ValidationFailed,
        )
        .await;
        write_private_file_0600(&journal_path, &original_journal);
    }

    // 7c. Mismatched resolved_plan_id when explicit_plan_id is null.
    {
        let mut journal_json: Value =
            serde_json::from_slice(&original_journal).expect("parse journal json");
        let entry = journal_json["entries"]
            .as_array_mut()
            .expect("entries")
            .iter_mut()
            .find(|e| {
                e["resolved_plan_id"] == u_direct_rev1_pr.plan_id && e["target_revision"] == 1
            })
            .expect("U_direct rev1 journal entry");
        entry["resolved_plan_id"] = json!("plan_mismatched_resolved");
        write_private_file_0600(
            &journal_path,
            &serde_json::to_vec_pretty(&journal_json).expect("serialize journal"),
        );
        assert_refused_without_provider_effect(
            "mismatched-resolved-plan-id",
            "protocol_error",
            NativeErrorCode::ProtocolError,
        )
        .await;
        write_private_file_0600(&journal_path, &original_journal);
    }

    // 7d. Missing PlanRevision in plans.json referenced by U_direct rev 1 completed journal entry.
    {
        let mut plans_json: Value =
            serde_json::from_slice(&original_plans).expect("parse plans json");
        plans_json["revisions"]
            .as_array_mut()
            .expect("revisions")
            .retain(|r| r["plan_id"] != u_direct_rev1_pr.plan_id);
        write_private_file_0600(
            &plans_path,
            &serde_json::to_vec_pretty(&plans_json).expect("serialize plans"),
        );
        assert_refused_without_provider_effect(
            "missing-plan-revision",
            "protocol_error",
            NativeErrorCode::ProtocolError,
        )
        .await;
        write_private_file_0600(&plans_path, &original_plans);
    }

    // 7e. Unpublished Draft status in plans.json for U_direct rev 1.
    {
        let mut plans_json: Value =
            serde_json::from_slice(&original_plans).expect("parse plans json");
        let rev = plans_json["revisions"]
            .as_array_mut()
            .expect("revisions")
            .iter_mut()
            .find(|r| r["plan_id"] == u_direct_rev1_pr.plan_id && r["revision"] == 1)
            .expect("U_direct rev1 in plans.json");
        rev["status"] = json!("draft");
        write_private_file_0600(
            &plans_path,
            &serde_json::to_vec_pretty(&plans_json).expect("serialize plans"),
        );
        assert_refused_without_provider_effect(
            "draft-plan-status",
            "validation_failed",
            NativeErrorCode::ValidationFailed,
        )
        .await;
        write_private_file_0600(&plans_path, &original_plans);
    }

    // 7f. Conflicting gateway OrchestratorPlan command record missing its workflow save_draft receipt.
    {
        let mut journal_json: Value =
            serde_json::from_slice(&original_journal).expect("parse journal json");
        journal_json["entries"]
            .as_array_mut()
            .expect("entries")
            .retain(|e| {
                !(e["resolved_plan_id"] == u_gw_rev1_pr.plan_id && e["target_revision"] == 1)
            });
        write_private_file_0600(
            &journal_path,
            &serde_json::to_vec_pretty(&journal_json).expect("serialize journal"),
        );
        assert_refused_without_provider_effect(
            "missing-gw-plan-draft-receipt",
            "validation_failed",
            NativeErrorCode::ValidationFailed,
        )
        .await;
        write_private_file_0600(&journal_path, &original_journal);
    }

    // 7g. Corrupt workflow journal JSON fails closed without provider effects.
    {
        write_private_file_0600(&journal_path, b"{corrupt-workflow-journal");
        assert_refused_without_provider_effect(
            "corrupt-journal-json",
            "protocol_error",
            NativeErrorCode::ProtocolError,
        )
        .await;
        write_private_file_0600(&journal_path, &original_journal);
    }

    // 7h. Corrupt planning command ledger JSON fails closed on direct service and prepare_work.
    {
        write_private_file_0600(&cmd_ledger_path, b"{corrupt-command-ledger");
        assert_eq!(
            invoke_mcp_err_code(&mcp, "prepare_work", prep_j_args.clone()).await,
            "protocol_error"
        );
        let direct_corrupt_cmd_err = planning_service
            .orchestrate_brief_to_plan(
                &scoped,
                &OrchestrateWorkBriefPlanRequest {
                    binding_id: fixture.binding_id.clone(),
                    thread_uuid: THREAD_UUID.to_string(),
                    grant_id: grant_id.clone(),
                    job_id: job_j.job_id.clone(),
                    request_key: "direct-j2-corrupt-cmd-ledger".to_string(),
                    expected_ledger_revision: None,
                    difficulty: Some(Difficulty::Hard),
                    plan_id: Some(j_rev1_pr.plan_id.clone()),
                    expected_plan_revision: 2,
                    architect_advice_ids: vec![adv_j.clone()],
                    max_architect_rounds: 0,
                    architect_difficulty: None,
                    current_time_ms: current_wall_clock_ms().expect("wall clock"),
                },
            )
            .await
            .expect_err("direct service must fail closed on corrupt command ledger");
        assert_eq!(
            direct_corrupt_cmd_err.error_code(),
            NativeErrorCode::ProtocolError
        );
        assert_eq!(
            provider_invocation_count(&fixture.worker_marker),
            calls_baseline
        );
        assert_eq!(
            grant_reservation_count(&wf_store, &fixture.binding_id, &grant_id),
            reservations_baseline
        );
        write_private_file_0600(&cmd_ledger_path, &original_cmd_ledger);
    }

    // 8. After restoring valid receipts, exact original replays of J2 rev 2 and prepare_work
    //    succeed cleanly with zero extra provider calls or reservations.
    let j_rev2_replay: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "request_technical_plan", j_rev2_args).await;
    assert_eq!(j_rev2_replay.command_id, j_rev2_submit.command_id);
    assert_eq!(j_rev2_replay.status, PlanningCommandStatus::Completed);
    assert_eq!(
        j_rev2_replay.submission_status,
        PlanningCommandStartStatus::AlreadyCompleted
    );
    assert!(!j_rev2_replay.launched);
    let prep_j_replay: PrepareWorkResponse = invoke_mcp_ok(&mcp, "prepare_work", prep_j_args).await;
    assert_eq!(prep_j_replay.job_id, job_j.job_id);
    assert_eq!(
        provider_invocation_count(&fixture.worker_marker),
        calls_baseline
    );
    assert_eq!(
        grant_reservation_count(&wf_store, &fixture.binding_id, &grant_id),
        reservations_baseline
    );

    fixture.shutdown().await;
}

#[tokio::test]
async fn direct_service_crash_between_save_draft_and_publish_for_unrelated_job_does_not_block_target_revision_and_fails_closed_on_contradictions(
) {
    use threadlane_native::coworker_workflow::OrchestrateWorkBriefPlanRequest;
    use threadlane_native::{open_workflow_stores, ScopedWorkflow};
    use threadlane_workflow::SavePlanDraft;

    let mut fixture = GatewayFixture::start().await;
    let mcp = fixture.enrolled_mcp();
    let now_ms = current_wall_clock_ms().expect("wall clock");
    let wf_store =
        CoworkerWorkflowStore::new(&fixture.plans_dir).expect("open CoworkerWorkflowStore");

    let grant = wf_store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: fixture.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "req-grant-draft-crash-1".to_string(),
            expected_ledger_revision: None,
            project_base: fixture.project_base.clone(),
            profile_id: PROFILE_ID.to_string(),
            registry_revision: 3,
            policy: advisor_policy(),
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget: PlanningCallBudget {
                max_total_calls: 30,
                max_orchestrator_calls: 20,
                max_architect_calls: 10,
                max_timeout_ms_per_call: 60_000,
                max_output_bytes_per_call: 65_536,
            },
            created_at_ms: now_ms.saturating_sub(5_000),
            expires_at_ms: now_ms + 3_600_000,
            operator_attested: true,
        })
        .expect("create advisor grant");
    let grant_id = grant.grant_id.clone();

    let (reg_arc, plans_arc) =
        open_workflow_stores(Some(&fixture.registry_dir), Some(&fixture.plans_dir))
            .expect("open workflow stores")
            .expect("workflow stores configured");
    let scoped = ScopedWorkflow::new(reg_arc, plans_arc, fixture.binding_id.clone())
        .expect("ScopedWorkflow");
    let artifacts_dir = planning_artifacts_dir(&fixture.plans_dir);
    let planning_service =
        CoworkerPlanningService::new(wf_store.clone(), &fixture.registry_dir, &artifacts_dir)
            .expect("open CoworkerPlanningService");

    let wf_ledger_path = wf_store
        .ledger_path_for_binding(&fixture.binding_id)
        .expect("workflow ledger path");
    let plans_path = fixture.plans_dir.join("plans.json");

    // Helper to roll back ONLY the post-`save_draft` effects (`publish` + `record_job_planning_result`)
    // so disk state reproduces a real direct-service crash between completed `save_draft` and `publish`.
    let simulate_crash_between_save_draft_and_publish =
        |plan_id: &str,
         plan_revision: u64,
         pre_link_job: &threadlane_native::coworker_workflow::CoworkerJobRecord| {
            let mut plans_json: Value =
                serde_json::from_slice(&fs::read(&plans_path).expect("read plans.json"))
                    .expect("parse plans.json");
            let rev = plans_json["revisions"]
                .as_array_mut()
                .expect("plans revisions")
                .iter_mut()
                .find(|r| r["plan_id"] == plan_id && r["revision"] == plan_revision)
                .expect("target plan revision in plans.json");
            rev["status"] = json!("draft");
            write_private_file_0600(
                &plans_path,
                &serde_json::to_vec_pretty(&plans_json).expect("serialize plans.json"),
            );

            let mut ledger_json: Value =
                serde_json::from_slice(&fs::read(&wf_ledger_path).expect("read workflow ledger"))
                    .expect("parse workflow ledger");
            let job_slot = ledger_json["jobs"]
                .as_array_mut()
                .expect("ledger jobs")
                .iter_mut()
                .find(|j| j["job_id"] == pre_link_job.job_id)
                .expect("target job in workflow ledger");
            *job_slot = serde_json::to_value(pre_link_job).expect("serialize pre-link job");
            write_private_file_0600(
                &wf_ledger_path,
                &serde_json::to_vec_pretty(&ledger_json).expect("serialize workflow ledger"),
            );
        };

    // 1a. Unrelated job U_implicit: direct-service Orchestrator call with implicit `plan_id: None`
    //     that completed `save_draft` and crashed before `publish` and `record_job_planning_result`.
    let job_u_implicit: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-u-imp-crash",
            "summary": "Unrelated job U_implicit crashing between save_draft and publish",
            "confirmed_requirements": ["Produce unpublished implicit first draft for U_implicit"]
        }),
    )
    .await;
    let pre_u_implicit_job = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read ledger before U_implicit plan")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job_u_implicit.job_id)
        .expect("U_implicit pre-link job");
    assert_eq!(pre_u_implicit_job.job_revision, 1);
    assert_eq!(pre_u_implicit_job.phase, CoworkerJobPhase::Planning);
    assert!(pre_u_implicit_job.planning_result.is_none());

    fs::write(&fixture.mode_path, b"orch_plan\n").expect("set mode orch_plan for U_implicit");
    let u_implicit_pr = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_u_implicit.job_id.clone(),
                request_key: "direct-u-imp-crash-rev1".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: None,
                expected_plan_revision: 0,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect("U_implicit direct-service plan")
        .planning_result
        .expect("U_implicit planning_result");
    simulate_crash_between_save_draft_and_publish(&u_implicit_pr.plan_id, 1, &pre_u_implicit_job);

    // 1b. Unrelated job U_explicit: direct-service Orchestrator call with explicit `plan_id` AND
    //     attached Advisor advice `adv_u` that completed `save_draft` and crashed before `publish`.
    let job_u_explicit: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-u-exp-crash",
            "summary": "Unrelated job U_explicit crashing between save_draft and publish",
            "confirmed_requirements": ["Produce unpublished explicit first draft with advice"]
        }),
    )
    .await;
    fs::write(&fixture.mode_path, b"architect\n").expect("set mode architect for U_explicit");
    let adv_u_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "consult_advisor",
        json!({
            "grant_id": grant_id,
            "job_id": job_u_explicit.job_id,
            "request_key": "req-u-exp-advisor",
            "question": "Advisor guidance for U_explicit unpublished first draft",
            "dependency_files": ["src/lib.rs"],
            "difficulty": "hard"
        }),
    )
    .await;
    let adv_u_detail = wait_for_command_completion(&mcp, &adv_u_submit.command_id).await;
    let adv_u = match adv_u_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::ArchitectAdvice { advice_id, .. }) => advice_id.clone(),
        other => panic!("expected ArchitectAdvice for U_explicit, got {other:?}"),
    };
    let pre_u_explicit_job = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read ledger before U_explicit plan")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job_u_explicit.job_id)
        .expect("U_explicit pre-link job");

    fs::write(&fixture.mode_path, b"orch_plan\n").expect("set mode orch_plan for U_explicit");
    let u_explicit_pr = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_u_explicit.job_id.clone(),
                request_key: "direct-u-exp-crash-rev1".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: Some("plan_u_explicit_crash".to_string()),
                expected_plan_revision: 0,
                architect_advice_ids: vec![adv_u.clone()],
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect("U_explicit direct-service plan")
        .planning_result
        .expect("U_explicit planning_result");
    assert_eq!(u_explicit_pr.plan_id, "plan_u_explicit_crash");
    simulate_crash_between_save_draft_and_publish(&u_explicit_pr.plan_id, 1, &pre_u_explicit_job);

    // Verify both crashed jobs' receipts are intact sole unpublished first drafts before running J.
    let journal_path = find_single_prefixed_json(&fixture.plans_dir, "workflow-commands-");
    let find_draft_command_key = |target_plan_id: &str| -> String {
        let j_val: Value =
            serde_json::from_slice(&fs::read(&journal_path).expect("read workflow journal"))
                .expect("parse workflow journal");
        j_val["entries"]
            .as_array()
            .expect("journal entries")
            .iter()
            .find(|e| e["resolved_plan_id"] == target_plan_id && e["target_revision"] == 1)
            .expect("save_draft entry for plan_id")["command_key"]
            .as_str()
            .expect("command_key string")
            .to_string()
    };
    let u_imp_cmd_key = find_draft_command_key(&u_implicit_pr.plan_id);
    let u_exp_cmd_key = find_draft_command_key(&u_explicit_pr.plan_id);
    let initial_receipts = ScopedWorkflow::read_save_draft_receipts_readonly_at(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.binding_id,
    )
    .expect("read readonly save_draft receipts");
    let u_imp_receipt = initial_receipts
        .verify_completed_save_draft(&u_imp_cmd_key)
        .expect("verify U_implicit receipt")
        .expect("U_implicit receipt present");
    assert!(u_imp_receipt.is_sole_unpublished_first_draft());
    assert_eq!(u_imp_receipt.explicit_plan_id, None);
    let u_exp_receipt = initial_receipts
        .verify_completed_save_draft(&u_exp_cmd_key)
        .expect("verify U_explicit receipt")
        .expect("U_explicit receipt present");
    assert!(u_exp_receipt.is_sole_unpublished_first_draft());
    assert_eq!(
        u_exp_receipt.explicit_plan_id.as_deref(),
        Some("plan_u_explicit_crash")
    );

    // 2. Create fully evidenced target job J (amendment with accepted advice `adv_j` + published
    //    rev 1) and exact-digest target job J_exact (pre-plan advice `adv_j_exact` + published rev 1).
    let job_j_parent: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-j-crash-parent",
            "summary": "Target job J revising with advice alongside unrelated crashed U drafts",
            "confirmed_requirements": ["Revise published plan cleanly without being blocked by U"]
        }),
    )
    .await;
    fs::write(&fixture.mode_path, b"architect\n").expect("set mode architect for J");
    let adv_j_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "consult_advisor",
        json!({
            "grant_id": grant_id,
            "job_id": job_j_parent.job_id,
            "request_key": "req-j-crash-advisor",
            "question": "Advisor guidance for target job J revision",
            "dependency_files": ["src/lib.rs"],
            "difficulty": "hard"
        }),
    )
    .await;
    let adv_j_detail = wait_for_command_completion(&mcp, &adv_j_submit.command_id).await;
    let adv_j = match adv_j_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::ArchitectAdvice { advice_id, .. }) => advice_id.clone(),
        other => panic!("expected ArchitectAdvice for J, got {other:?}"),
    };
    let prep_j_args = json!({
        "grant_id": grant_id,
        "request_key": "req-prep-j-crash-amend",
        "summary": "Target job J revising with advice alongside unrelated crashed U drafts",
        "confirmed_requirements": ["Revise published plan cleanly without being blocked by U"],
        "accepted_architect_decisions": [adv_j],
        "amendment": {
            "target_job_id": job_j_parent.job_id,
            "target_job_revision": 1,
            "amendment_reason": "Attach verified Advisor decision to target job J"
        }
    });
    let job_j: PrepareWorkResponse = invoke_mcp_ok(&mcp, "prepare_work", prep_j_args.clone()).await;

    fs::write(&fixture.mode_path, b"orch_plan\n").expect("set mode orch_plan for J rev1");
    let j_rev1_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_j.job_id,
            "request_key": "req-j-crash-plan-rev1",
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let j_rev1_detail = wait_for_command_completion(&mcp, &j_rev1_submit.command_id).await;
    let j_rev1_pr = match j_rev1_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::OrchestratorPlan {
            planning_result, ..
        }) => planning_result.as_ref().clone(),
        other => panic!("expected OrchestratorPlan for J rev1, got {other:?}"),
    };

    let job_j_exact: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-j-exact-crash",
            "summary": "Exact-digest target job J_exact alongside crashed U drafts",
            "confirmed_requirements": ["Verify exact-digest revision succeeds alongside crashed U"]
        }),
    )
    .await;
    fs::write(&fixture.mode_path, b"architect\n").expect("set mode architect for J_exact");
    let adv_j_exact_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "consult_advisor",
        json!({
            "grant_id": grant_id,
            "job_id": job_j_exact.job_id,
            "request_key": "req-j-exact-crash-advisor",
            "question": "Pre-plan contract advice for exact-digest job J_exact",
            "dependency_files": ["src/lib.rs"],
            "difficulty": "hard"
        }),
    )
    .await;
    let adv_j_exact_detail =
        wait_for_command_completion(&mcp, &adv_j_exact_submit.command_id).await;
    let adv_j_exact = match adv_j_exact_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::ArchitectAdvice { advice_id, .. }) => advice_id.clone(),
        other => panic!("expected ArchitectAdvice for J_exact, got {other:?}"),
    };
    fs::write(&fixture.mode_path, b"orch_plan\n").expect("set mode orch_plan for J_exact rev1");
    let j_exact_rev1_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_j_exact.job_id,
            "request_key": "req-j-exact-crash-plan-rev1",
            "architect_advice_ids": [adv_j_exact],
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let j_exact_rev1_detail =
        wait_for_command_completion(&mcp, &j_exact_rev1_submit.command_id).await;
    let j_exact_rev1_pr = match j_exact_rev1_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::OrchestratorPlan {
            planning_result, ..
        }) => planning_result.as_ref().clone(),
        other => panic!("expected OrchestratorPlan for J_exact rev1, got {other:?}"),
    };

    // 3. Revising J and J_exact MUST succeed via BOTH gateway MCP (explicit advice, implicit
    //    advice, prepare_work replay) AND direct service while U_implicit and U_explicit remain
    //    physically intact unpublished first drafts.
    fs::write(&fixture.mode_path, b"orch_plan_v2_same_iface\n")
        .expect("set mode orch_plan_v2_same_iface for revisions");

    // 3a. Gateway MCP explicit advice revision (J rev 2).
    let j_rev2_args = json!({
        "grant_id": grant_id,
        "job_id": job_j.job_id,
        "request_key": "req-j-crash-plan-rev2-exp",
        "plan_id": j_rev1_pr.plan_id,
        "expected_plan_revision": 1,
        "architect_advice_ids": [adv_j],
        "max_architect_rounds": 0,
        "difficulty": "hard"
    });
    let j_rev2_submit: PlanningCommandSubmissionResponse =
        invoke_mcp_ok(&mcp, "request_technical_plan", j_rev2_args.clone()).await;
    let j_rev2_detail = wait_for_command_completion(&mcp, &j_rev2_submit.command_id).await;
    assert_eq!(
        j_rev2_detail.status,
        PlanningCommandStatus::Completed,
        "J rev2 gateway explicit advice failed: {}",
        format_command_detail_context(&j_rev2_detail)
    );
    assert!(!j_rev2_detail.pending_reconciliation);
    let j_rev2_pr = match j_rev2_detail.result_ref.as_ref() {
        Some(PlanningCommandResultRef::OrchestratorPlan {
            job_id,
            planning_result,
            ..
        }) => {
            assert_eq!(job_id, &job_j.job_id);
            planning_result.as_ref().clone()
        }
        other => panic!("expected OrchestratorPlan for J rev2, got {other:?}"),
    };
    assert_eq!(j_rev2_pr.plan_id, j_rev1_pr.plan_id);
    assert_eq!(j_rev2_pr.plan_revision, 2);
    assert_eq!(j_rev2_pr.plan_status, PlanStatus::Published);
    assert_eq!(j_rev2_pr.architect_advice_ids, vec![adv_j.clone()]);

    // 3b. Gateway MCP implicit advice revision (J rev 3).
    let j_rev3_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_j.job_id,
            "request_key": "req-j-crash-plan-rev3-imp",
            "plan_id": j_rev1_pr.plan_id,
            "expected_plan_revision": 2,
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let j_rev3_detail = wait_for_command_completion(&mcp, &j_rev3_submit.command_id).await;
    assert_eq!(
        j_rev3_detail.status,
        PlanningCommandStatus::Completed,
        "J rev3 gateway implicit advice failed: {}",
        format_command_detail_context(&j_rev3_detail)
    );

    // 3c. Direct service revision (J rev 4).
    let j_rev4_req = OrchestrateWorkBriefPlanRequest {
        binding_id: fixture.binding_id.clone(),
        thread_uuid: THREAD_UUID.to_string(),
        grant_id: grant_id.clone(),
        job_id: job_j.job_id.clone(),
        request_key: "direct-j-crash-plan-rev4".to_string(),
        expected_ledger_revision: None,
        difficulty: Some(Difficulty::Hard),
        plan_id: Some(j_rev1_pr.plan_id.clone()),
        expected_plan_revision: 3,
        architect_advice_ids: vec![adv_j.clone()],
        max_architect_rounds: 0,
        architect_difficulty: None,
        current_time_ms: current_wall_clock_ms().expect("wall clock"),
    };
    let j_rev4_outcome = planning_service
        .orchestrate_brief_to_plan(&scoped, &j_rev4_req)
        .await
        .expect("direct service J rev4 succeeds alongside crashed U drafts");
    let j_rev4_pr = j_rev4_outcome
        .planning_result
        .clone()
        .expect("J rev4 planning_result");
    assert_eq!(j_rev4_pr.plan_id, j_rev1_pr.plan_id);
    assert_eq!(j_rev4_pr.plan_revision, 4);
    assert_eq!(j_rev4_pr.plan_status, PlanStatus::Published);
    assert_eq!(j_rev4_pr.architect_advice_ids, vec![adv_j.clone()]);

    // 3d. Gateway MCP and direct service revisions for exact-digest job J_exact (rev 2 & rev 3).
    let j_exact_rev2_submit: PlanningCommandSubmissionResponse = invoke_mcp_ok(
        &mcp,
        "request_technical_plan",
        json!({
            "grant_id": grant_id,
            "job_id": job_j_exact.job_id,
            "request_key": "req-j-exact-crash-plan-rev2",
            "plan_id": j_exact_rev1_pr.plan_id,
            "expected_plan_revision": 1,
            "architect_advice_ids": [adv_j_exact],
            "max_architect_rounds": 0,
            "difficulty": "hard"
        }),
    )
    .await;
    let j_exact_rev2_detail =
        wait_for_command_completion(&mcp, &j_exact_rev2_submit.command_id).await;
    assert_eq!(
        j_exact_rev2_detail.status,
        PlanningCommandStatus::Completed,
        "J_exact rev2 gateway failed: {}",
        format_command_detail_context(&j_exact_rev2_detail)
    );
    let j_exact_rev3_outcome = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_j_exact.job_id.clone(),
                request_key: "direct-j-exact-crash-plan-rev3".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: Some(j_exact_rev1_pr.plan_id.clone()),
                expected_plan_revision: 2,
                architect_advice_ids: vec![adv_j_exact.clone()],
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect("direct service J_exact rev3 succeeds alongside crashed U drafts");
    assert_eq!(
        j_exact_rev3_outcome
            .planning_result
            .as_ref()
            .expect("J_exact rev3 planning_result")
            .plan_revision,
        3
    );

    // 3e. Verify original shape/control: neither U_implicit nor U_explicit was auto-retried,
    //     auto-published, or mutated, and J remains unapproved in AwaitingApproval at rev 4.
    let ledger_after_revs = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read workflow ledger after revisions");
    for expected_u in [&pre_u_implicit_job, &pre_u_explicit_job] {
        let actual_u = ledger_after_revs
            .jobs
            .iter()
            .find(|j| j.job_id == expected_u.job_id)
            .expect("crashed U job present");
        assert_eq!(actual_u, expected_u, "crashed U job must never be mutated");
    }
    let actual_j = ledger_after_revs
        .jobs
        .iter()
        .find(|j| j.job_id == job_j.job_id)
        .expect("target job J present");
    assert_eq!(actual_j.phase, CoworkerJobPhase::AwaitingApproval);
    assert!(actual_j.approval_linkage.is_none());
    assert!(actual_j.tasks.is_empty());
    assert_eq!(
        actual_j
            .planning_result
            .as_ref()
            .expect("J planning_result")
            .plan_revision,
        4
    );
    let post_rev_receipts = ScopedWorkflow::read_save_draft_receipts_readonly_at(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.binding_id,
    )
    .expect("read readonly save_draft receipts after J revisions");
    assert!(post_rev_receipts
        .verify_completed_save_draft(&u_imp_cmd_key)
        .expect("re-verify U_implicit draft")
        .expect("U_implicit draft present")
        .is_sole_unpublished_first_draft());
    assert!(post_rev_receipts
        .verify_completed_save_draft(&u_exp_cmd_key)
        .expect("re-verify U_explicit draft")
        .expect("U_explicit draft present")
        .is_sole_unpublished_first_draft());

    // 3f. Verify superseded J rev 2 history and stable current replays with ZERO additional
    //     provider invocations or grant reservations.
    let calls_baseline = provider_invocation_count(&fixture.worker_marker);
    let reservations_baseline = grant_reservation_count(&wf_store, &fixture.binding_id, &grant_id);
    let assert_superseded_j_rev2_history = || {
        let mcp = &mcp;
        let wf_store = &wf_store;
        let worker_marker = &fixture.worker_marker;
        let binding_id = fixture.binding_id.as_str();
        let grant_id = grant_id.as_str();
        let j_rev2_command_id = j_rev2_submit.command_id.as_str();
        let j_rev2_plan_id = j_rev2_pr.plan_id.as_str();
        let j_rev2_plan_digest = j_rev2_pr.plan_digest.as_str();
        let j_rev2_args = j_rev2_args.clone();
        async move {
            let j_rev2_history: PlanningCommandDetailResponse = invoke_mcp_ok(
                mcp,
                "planning_command",
                json!({ "command_id": j_rev2_command_id }),
            )
            .await;
            assert_eq!(j_rev2_history.command_id, j_rev2_command_id);
            assert_eq!(j_rev2_history.status, PlanningCommandStatus::Completed);
            assert_eq!(
                j_rev2_history.current_plan_status,
                Some(PlanStatus::Superseded)
            );
            assert_eq!(j_rev2_history.executable, Some(false));
            let j_rev2_summary = j_rev2_history
                .orchestrator_plan_summary
                .as_ref()
                .expect("frozen J rev2 orchestrator_plan_summary");
            assert_eq!(j_rev2_summary.plan_id, j_rev2_plan_id);
            assert_eq!(j_rev2_summary.plan_revision, 2);
            assert_eq!(j_rev2_summary.plan_digest, j_rev2_plan_digest);
            assert_eq!(j_rev2_summary.plan_status, PlanStatus::Published);
            assert_eq!(
                invoke_mcp_err_code(mcp, "request_technical_plan", j_rev2_args).await,
                "validation_failed"
            );
            assert_eq!(provider_invocation_count(worker_marker), calls_baseline);
            assert_eq!(
                grant_reservation_count(wf_store, binding_id, grant_id),
                reservations_baseline
            );
        }
    };
    assert_superseded_j_rev2_history().await;
    let prep_j_replay: PrepareWorkResponse =
        invoke_mcp_ok(&mcp, "prepare_work", prep_j_args.clone()).await;
    assert_eq!(prep_j_replay.job_id, job_j.job_id);
    let j_rev4_direct_replay = planning_service
        .orchestrate_brief_to_plan(&scoped, &j_rev4_req)
        .await
        .expect("direct service replay of J rev4");
    assert_eq!(
        j_rev4_direct_replay
            .planning_result
            .as_ref()
            .expect("replayed J rev4 planning_result")
            .plan_digest,
        j_rev4_pr.plan_digest
    );
    assert_eq!(
        j_rev4_direct_replay.reservation.reservation_id,
        j_rev4_outcome.reservation.reservation_id
    );
    assert_eq!(
        provider_invocation_count(&fixture.worker_marker),
        calls_baseline
    );
    assert_eq!(
        grant_reservation_count(&wf_store, &fixture.binding_id, &grant_id),
        reservations_baseline
    );

    // 4. Negative fail-closed checks: missing/corrupt/contradictory U draft evidence, Draft
    //    contradicting a published result, or missing/corrupt/contradictory target J evidence
    //    MUST refuse on BOTH gateway MCP and direct service with zero extra provider effects.
    let cmd_ledger_path = find_single_prefixed_json(
        &planning_commands_dir(&fixture.plans_dir),
        "planning-commands-",
    );
    let u_imp_art_path = artifacts_dir.join(format!("{}.json", u_implicit_pr.report_artifact_id));
    let u_exp_art_path = artifacts_dir.join(format!("{}.json", u_explicit_pr.report_artifact_id));
    let j_rev1_art_path = artifacts_dir.join(format!("{}.json", j_rev1_pr.report_artifact_id));

    let original_journal = fs::read(&journal_path).expect("read original workflow journal");
    let original_plans = fs::read(&plans_path).expect("read original plans.json");
    let original_wf_ledger = fs::read(&wf_ledger_path).expect("read original workflow ledger");
    let original_cmd_ledger = fs::read(&cmd_ledger_path).expect("read original command ledger");
    let original_u_imp_art = fs::read(&u_imp_art_path).expect("read original U_implicit artifact");
    let original_u_exp_art = fs::read(&u_exp_art_path).expect("read original U_explicit artifact");
    let original_j_rev1_art = fs::read(&j_rev1_art_path).expect("read original J rev1 artifact");

    let assert_refused_without_provider_effect =
        |label: &str, expected_mcp_code: &str, expected_native_code: NativeErrorCode| {
            let mcp = &mcp;
            let planning_service = &planning_service;
            let scoped = &scoped;
            let wf_store = &wf_store;
            let worker_marker = &fixture.worker_marker;
            let binding_id = fixture.binding_id.clone();
            let grant_id = grant_id.clone();
            let job_id = job_j.job_id.clone();
            let plan_id = j_rev1_pr.plan_id.clone();
            let adv_j = adv_j.clone();
            let label = label.to_string();
            let expected_mcp_code = expected_mcp_code.to_string();
            async move {
                assert_eq!(
                    invoke_mcp_err_code(
                        mcp,
                        "request_technical_plan",
                        json!({
                            "grant_id": grant_id,
                            "job_id": job_id,
                            "request_key": format!("req-j-crash-neg-{label}"),
                            "plan_id": plan_id,
                            "expected_plan_revision": 4,
                            "architect_advice_ids": [adv_j],
                            "max_architect_rounds": 0,
                            "difficulty": "hard"
                        })
                    )
                    .await,
                    expected_mcp_code,
                    "MCP request_technical_plan must refuse case `{label}`"
                );
                let direct_err = planning_service
                    .orchestrate_brief_to_plan(
                        scoped,
                        &OrchestrateWorkBriefPlanRequest {
                            binding_id: binding_id.clone(),
                            thread_uuid: THREAD_UUID.to_string(),
                            grant_id: grant_id.clone(),
                            job_id,
                            request_key: format!("direct-j-crash-neg-{label}"),
                            expected_ledger_revision: None,
                            difficulty: Some(Difficulty::Hard),
                            plan_id: Some(plan_id),
                            expected_plan_revision: 4,
                            architect_advice_ids: vec![adv_j],
                            max_architect_rounds: 0,
                            architect_difficulty: None,
                            current_time_ms: current_wall_clock_ms().expect("wall clock"),
                        },
                    )
                    .await
                    .expect_err("direct service must refuse negative case");
                assert_eq!(
                    direct_err.error_code(),
                    expected_native_code,
                    "direct service error code mismatch for `{label}`"
                );
                assert_eq!(
                    provider_invocation_count(worker_marker),
                    calls_baseline,
                    "negative case `{label}` must not invoke provider"
                );
                assert_eq!(
                    grant_reservation_count(wf_store, &binding_id, &grant_id),
                    reservations_baseline,
                    "negative case `{label}` must not create reservations"
                );
            }
        };

    // 4a. Missing artifact on unpublished-draft U_implicit MUST fail closed (never skipped).
    fs::remove_file(&u_imp_art_path).expect("remove U_implicit artifact");
    assert_refused_without_provider_effect(
        "missing-u-draft-artifact",
        "validation_failed",
        NativeErrorCode::ValidationFailed,
    )
    .await;
    write_private_file_0600(&u_imp_art_path, &original_u_imp_art);

    // 4b. Corrupt artifact JSON on unpublished-draft U_implicit MUST fail closed.
    write_private_file_0600(&u_imp_art_path, b"{corrupt-u-draft-artifact");
    assert_refused_without_provider_effect(
        "corrupt-u-draft-artifact",
        "protocol_error",
        NativeErrorCode::ProtocolError,
    )
    .await;
    write_private_file_0600(&u_imp_art_path, &original_u_imp_art);

    // 4c. Spoofed `explicit_plan_id` (`null` -> `resolved_plan_id`) on U_implicit's journal receipt
    //     passes plan_id format checks and MUST be caught by context_digest verification.
    {
        let mut journal_json: Value =
            serde_json::from_slice(&original_journal).expect("parse journal json");
        let entry = journal_json["entries"]
            .as_array_mut()
            .expect("entries")
            .iter_mut()
            .find(|e| e["command_key"] == u_imp_cmd_key)
            .expect("U_implicit journal entry");
        entry["explicit_plan_id"] = json!(u_implicit_pr.plan_id);
        write_private_file_0600(
            &journal_path,
            &serde_json::to_vec_pretty(&journal_json).expect("serialize spoofed explicit_plan_id"),
        );
        assert_refused_without_provider_effect(
            "spoofed-u-explicit-plan-id",
            "validation_failed",
            NativeErrorCode::ValidationFailed,
        )
        .await;
        write_private_file_0600(&journal_path, &original_journal);
    }

    // 4d. Removing attached `adv_u` from U_explicit's artifact `architect_advice_ids` passes
    //     standalone artifact schema validation and MUST be caught by context_digest verification.
    {
        let mut art_json: Value =
            serde_json::from_slice(&original_u_exp_art).expect("parse U_explicit artifact");
        art_json["outcome"]["architect_advice_ids"] = json!([]);
        write_private_file_0600(
            &u_exp_art_path,
            &serde_json::to_vec_pretty(&art_json).expect("serialize tampered U_explicit artifact"),
        );
        assert_refused_without_provider_effect(
            "tampered-u-explicit-advice-ids",
            "validation_failed",
            NativeErrorCode::ValidationFailed,
        )
        .await;
        write_private_file_0600(&u_exp_art_path, &original_u_exp_art);
    }

    // 4e. Internally consistent tampered draft summary in BOTH `plans.json` and `workflow-commands`
    //     (recomputed valid `tasks_digest`, `payload_digest`, and `plan_digest`) contradicts
    //     U_implicit's persisted Orchestrator artifact and MUST be refused.
    {
        let tmp_forge = TempDir::new().expect("create temp dir for forged draft");
        let tmp_forge_root = fs::canonicalize(tmp_forge.path()).expect("canonicalize forge dir");
        fs::set_permissions(&tmp_forge_root, fs::Permissions::from_mode(0o700))
            .expect("chmod forge root 0700");
        let tmp_plans_dir = tmp_forge_root.join("plans-forge");
        create_private_dir(&tmp_plans_dir);
        let (tmp_reg, tmp_plans) =
            open_workflow_stores(Some(&fixture.registry_dir), Some(&tmp_plans_dir))
                .expect("open temp workflow stores")
                .expect("temp workflow stores configured");
        let tmp_scoped = ScopedWorkflow::new(tmp_reg, tmp_plans, fixture.binding_id.clone())
            .expect("temp ScopedWorkflow");
        let forged_summary =
            "Contradictory draft summary diverging from persisted Orchestrator artifact"
                .to_string();
        tmp_scoped
            .save_draft(SavePlanDraft {
                command_key: u_imp_receipt.command_key.clone(),
                plan_id: None,
                expected_revision: 0,
                profile_id: u_imp_receipt.profile_id.clone(),
                profile_revision: u_imp_receipt.profile_revision,
                summary: forged_summary,
                tasks: u_imp_receipt.tasks.clone(),
            })
            .expect("save internally consistent forged draft");

        let forged_plans_val: Value = serde_json::from_slice(
            &fs::read(tmp_plans_dir.join("plans.json")).expect("read forged plans.json"),
        )
        .expect("parse forged plans.json");
        let forged_rev = forged_plans_val["revisions"]
            .as_array()
            .expect("forged revisions")
            .iter()
            .find(|r| r["plan_id"] == u_implicit_pr.plan_id && r["revision"] == 1)
            .expect("forged rev 1")
            .clone();

        let forged_journal_path = find_single_prefixed_json(&tmp_plans_dir, "workflow-commands-");
        let forged_journal_val: Value =
            serde_json::from_slice(&fs::read(&forged_journal_path).expect("read forged journal"))
                .expect("parse forged journal");
        let forged_entry = forged_journal_val["entries"]
            .as_array()
            .expect("forged journal entries")
            .iter()
            .find(|e| e["command_key"] == u_imp_cmd_key)
            .expect("forged journal entry")
            .clone();

        let mut patched_plans: Value =
            serde_json::from_slice(&original_plans).expect("parse original plans");
        let rev_slot = patched_plans["revisions"]
            .as_array_mut()
            .expect("revisions")
            .iter_mut()
            .find(|r| r["plan_id"] == u_implicit_pr.plan_id && r["revision"] == 1)
            .expect("U_implicit rev 1 slot");
        *rev_slot = forged_rev;
        write_private_file_0600(
            &plans_path,
            &serde_json::to_vec_pretty(&patched_plans).expect("serialize patched plans"),
        );

        let mut patched_journal: Value =
            serde_json::from_slice(&original_journal).expect("parse original journal");
        let entry_slot = patched_journal["entries"]
            .as_array_mut()
            .expect("entries")
            .iter_mut()
            .find(|e| e["command_key"] == u_imp_cmd_key)
            .expect("U_implicit journal slot");
        *entry_slot = forged_entry;
        write_private_file_0600(
            &journal_path,
            &serde_json::to_vec_pretty(&patched_journal).expect("serialize patched journal"),
        );

        assert_refused_without_provider_effect(
            "artifact-vs-receipt-summary-contradiction",
            "validation_failed",
            NativeErrorCode::ValidationFailed,
        )
        .await;
        write_private_file_0600(&plans_path, &original_plans);
        write_private_file_0600(&journal_path, &original_journal);
    }

    // 4f. Tampered receipt fields on U_implicit's Draft journal entry (`tasks_digest`,
    //     `payload_digest`, `plan_digest`, `created_at_ms`, and non-completed states).
    for (case_label, field, value) in [
        (
            "u-draft-bad-tasks-digest",
            "tasks_digest",
            json!("1".repeat(64)),
        ),
        (
            "u-draft-bad-payload-digest",
            "payload_digest",
            json!("2".repeat(64)),
        ),
    ] {
        let mut journal_json: Value =
            serde_json::from_slice(&original_journal).expect("parse journal");
        let entry = journal_json["entries"]
            .as_array_mut()
            .expect("entries")
            .iter_mut()
            .find(|e| e["command_key"] == u_imp_cmd_key)
            .expect("U_implicit entry");
        entry[field] = value;
        write_private_file_0600(
            &journal_path,
            &serde_json::to_vec_pretty(&journal_json).expect("serialize tampered journal"),
        );
        assert_refused_without_provider_effect(
            case_label,
            "protocol_error",
            NativeErrorCode::ProtocolError,
        )
        .await;
        write_private_file_0600(&journal_path, &original_journal);
    }
    {
        let mut journal_json: Value =
            serde_json::from_slice(&original_journal).expect("parse journal");
        let entry = journal_json["entries"]
            .as_array_mut()
            .expect("entries")
            .iter_mut()
            .find(|e| e["command_key"] == u_imp_cmd_key)
            .expect("U_implicit entry");
        let ts = entry["state"]["created_at_ms"]
            .as_i64()
            .expect("created_at_ms");
        entry["state"]["created_at_ms"] = json!(ts + 1);
        write_private_file_0600(
            &journal_path,
            &serde_json::to_vec_pretty(&journal_json).expect("serialize tampered timestamp"),
        );
        assert_refused_without_provider_effect(
            "u-draft-bad-created-at-ms",
            "protocol_error",
            NativeErrorCode::ProtocolError,
        )
        .await;
        write_private_file_0600(&journal_path, &original_journal);
    }
    {
        let mut journal_json: Value =
            serde_json::from_slice(&original_journal).expect("parse journal");
        let entry = journal_json["entries"]
            .as_array_mut()
            .expect("entries")
            .iter_mut()
            .find(|e| e["command_key"] == u_imp_cmd_key)
            .expect("U_implicit entry");
        let forged = "3".repeat(64);
        entry["plan_digest"] = json!(forged);
        entry["state"]["plan_digest"] = json!(forged);
        write_private_file_0600(
            &journal_path,
            &serde_json::to_vec_pretty(&journal_json).expect("serialize tampered plan_digest"),
        );
        assert_refused_without_provider_effect(
            "u-draft-bad-plan-digest",
            "protocol_error",
            NativeErrorCode::ProtocolError,
        )
        .await;
        write_private_file_0600(&journal_path, &original_journal);
    }
    for (case_label, status_str) in [("u-draft-pending", "pending"), ("u-draft-failed", "failed")] {
        let mut journal_json: Value =
            serde_json::from_slice(&original_journal).expect("parse journal");
        let entry = journal_json["entries"]
            .as_array_mut()
            .expect("entries")
            .iter_mut()
            .find(|e| e["command_key"] == u_imp_cmd_key)
            .expect("U_implicit entry");
        entry["state"] = json!({ "status": status_str });
        write_private_file_0600(
            &journal_path,
            &serde_json::to_vec_pretty(&journal_json).expect("serialize non-completed state"),
        );
        assert_refused_without_provider_effect(
            case_label,
            "validation_failed",
            NativeErrorCode::ValidationFailed,
        )
        .await;
        write_private_file_0600(&journal_path, &original_journal);
    }

    // 4g. Draft contradicting a published result MUST always refuse.
    // 4g-1: Owner job U_implicit has a linked `planning_result` while rev 1 in plans.json is Draft.
    {
        let mut ledger_json: Value =
            serde_json::from_slice(&original_wf_ledger).expect("parse workflow ledger");
        let u_slot = ledger_json["jobs"]
            .as_array_mut()
            .expect("jobs")
            .iter_mut()
            .find(|j| j["job_id"] == job_u_implicit.job_id)
            .expect("U_implicit job slot");
        u_slot["job_revision"] = json!(2);
        u_slot["phase"] = serde_json::to_value(CoworkerJobPhase::AwaitingApproval)
            .expect("serialize AwaitingApproval phase");
        u_slot["planning_result"] =
            serde_json::to_value(&u_implicit_pr).expect("serialize U_implicit planning_result");
        write_private_file_0600(
            &wf_ledger_path,
            &serde_json::to_vec_pretty(&ledger_json).expect("serialize contradicting ledger"),
        );
        assert_refused_without_provider_effect(
            "draft-contradicts-owner-planning-result",
            "validation_failed",
            NativeErrorCode::ValidationFailed,
        )
        .await;
        write_private_file_0600(&wf_ledger_path, &original_wf_ledger);
    }

    // 4g-2: `plans.json` has rev 1 Draft alongside a rev 2 Published for the same `plan_id`
    //       (even with rev 1 artifact still physically intact on disk).
    {
        let mut plans_json: Value =
            serde_json::from_slice(&original_plans).expect("parse plans.json");
        let mut rev2_clone = plans_json["revisions"]
            .as_array()
            .expect("revisions")
            .iter()
            .find(|r| r["plan_id"] == u_implicit_pr.plan_id && r["revision"] == 1)
            .expect("U_implicit rev 1")
            .clone();
        rev2_clone["revision"] = json!(2);
        rev2_clone["status"] = json!("published");
        plans_json["revisions"]
            .as_array_mut()
            .expect("revisions")
            .push(rev2_clone);
        write_private_file_0600(
            &plans_path,
            &serde_json::to_vec_pretty(&plans_json).expect("serialize plans with rev2 published"),
        );
        assert_refused_without_provider_effect(
            "draft-rev1-contradicts-published-rev2",
            "validation_failed",
            NativeErrorCode::ValidationFailed,
        )
        .await;
        write_private_file_0600(&plans_path, &original_plans);
    }

    // 4g-3: A gateway `PlanningCommandRecord` claims `OrchestratorPlan` for U_implicit's `plan_id`
    //       while rev 1 in plans.json is Draft.
    {
        let mut cmd_json: Value =
            serde_json::from_slice(&original_cmd_ledger).expect("parse command ledger");
        let target_cmd = cmd_json["commands"]
            .as_array_mut()
            .expect("commands")
            .iter_mut()
            .find(|c| c["command_id"] == j_exact_rev1_submit.command_id)
            .expect("J_exact rev1 command record");
        target_cmd["result_ref"]["planning_result"]["plan_id"] = json!(u_implicit_pr.plan_id);
        write_private_file_0600(
            &cmd_ledger_path,
            &serde_json::to_vec_pretty(&cmd_json).expect("serialize contradicting command ledger"),
        );
        assert_refused_without_provider_effect(
            "draft-contradicts-gateway-result-ref",
            "validation_failed",
            NativeErrorCode::ValidationFailed,
        )
        .await;
        write_private_file_0600(&cmd_ledger_path, &original_cmd_ledger);
    }

    // 4h. Missing, corrupt, or contradictory TARGET J evidence MUST fail closed even while
    //     intact crashed U drafts exist on disk.
    // 4h-1: Missing J rev 1 baseline artifact.
    fs::remove_file(&j_rev1_art_path).expect("remove J rev1 baseline artifact");
    assert_eq!(
        invoke_mcp_err_code(&mcp, "prepare_work", prep_j_args.clone()).await,
        "validation_failed"
    );
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "request_technical_plan",
            json!({
                "grant_id": grant_id,
                "job_id": job_j.job_id,
                "request_key": "req-j-crash-neg-missing-target-imp",
                "plan_id": j_rev1_pr.plan_id,
                "expected_plan_revision": 4,
                "max_architect_rounds": 0,
                "difficulty": "hard"
            })
        )
        .await,
        "validation_failed"
    );
    assert_refused_without_provider_effect(
        "missing-target-j-baseline-artifact",
        "validation_failed",
        NativeErrorCode::ValidationFailed,
    )
    .await;
    write_private_file_0600(&j_rev1_art_path, &original_j_rev1_art);

    // 4h-2: Corrupt J rev 1 baseline artifact JSON.
    write_private_file_0600(&j_rev1_art_path, b"{corrupt-target-j-artifact");
    assert_refused_without_provider_effect(
        "corrupt-target-j-baseline-artifact",
        "protocol_error",
        NativeErrorCode::ProtocolError,
    )
    .await;
    write_private_file_0600(&j_rev1_art_path, &original_j_rev1_art);

    // 4h-3: Contradictory `job_id` in J rev 1 baseline artifact.
    {
        let mut art_json: Value =
            serde_json::from_slice(&original_j_rev1_art).expect("parse J rev1 artifact");
        art_json["outcome"]["job_id"] = json!(job_u_implicit.job_id);
        write_private_file_0600(
            &j_rev1_art_path,
            &serde_json::to_vec_pretty(&art_json).expect("serialize contradictory J rev1 artifact"),
        );
        assert_refused_without_provider_effect(
            "contradictory-target-j-job-id",
            "validation_failed",
            NativeErrorCode::ValidationFailed,
        )
        .await;
        write_private_file_0600(&j_rev1_art_path, &original_j_rev1_art);
    }

    // 5. After restoring valid state, superseded J rev 2 history and exact current replays still
    //    succeed with zero extra provider calls.
    assert_superseded_j_rev2_history().await;
    let prep_j_replay_after: PrepareWorkResponse =
        invoke_mcp_ok(&mcp, "prepare_work", prep_j_args).await;
    assert_eq!(prep_j_replay_after.job_id, job_j.job_id);
    let j_rev4_replay_after = planning_service
        .orchestrate_brief_to_plan(&scoped, &j_rev4_req)
        .await
        .expect("direct service replay of J rev4 after restore");
    assert_eq!(
        j_rev4_replay_after
            .planning_result
            .as_ref()
            .expect("J rev4 replay planning_result")
            .plan_digest,
        j_rev4_pr.plan_digest
    );
    assert_eq!(
        provider_invocation_count(&fixture.worker_marker),
        calls_baseline
    );
    assert_eq!(
        grant_reservation_count(&wf_store, &fixture.binding_id, &grant_id),
        reservations_baseline
    );

    // 6. A real direct-service crash on a SECOND revision (rev 1 Published + rev 2 Draft) is NOT
    //    a sole unpublished first draft and MUST refuse on both gateway and direct service with
    //    zero additional provider calls or reservations.
    let job_u_rev2: PrepareWorkResponse = invoke_mcp_ok(
        &mcp,
        "prepare_work",
        json!({
            "grant_id": grant_id,
            "request_key": "req-prep-u-rev2-draft-crash",
            "summary": "Unrelated job U_rev2 crashing on second revision draft",
            "confirmed_requirements": ["Verify unpublished rev 2 draft is never excused as first draft"]
        }),
    )
    .await;
    fs::write(&fixture.mode_path, b"orch_plan\n").expect("set mode orch_plan for U_rev2 rev1");
    let u_rev2_r1_pr = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_u_rev2.job_id.clone(),
                request_key: "direct-u-rev2-plan-r1".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: None,
                expected_plan_revision: 0,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect("U_rev2 rev1")
        .planning_result
        .expect("U_rev2 rev1 planning_result");
    let post_u_rev2_r1_job = wf_store
        .read_binding_ledger(&fixture.binding_id)
        .expect("read ledger after U_rev2 rev1")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job_u_rev2.job_id)
        .expect("U_rev2 post-rev1 job");
    fs::write(&fixture.mode_path, b"orch_plan_v2_same_iface\n")
        .expect("set mode orch_plan_v2_same_iface for U_rev2 rev2");
    let u_rev2_r2_pr = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_u_rev2.job_id.clone(),
                request_key: "direct-u-rev2-plan-r2".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: Some(u_rev2_r1_pr.plan_id.clone()),
                expected_plan_revision: 1,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect("U_rev2 rev2")
        .planning_result
        .expect("U_rev2 rev2 planning_result");
    assert_eq!(u_rev2_r2_pr.plan_revision, 2);
    // Roll back rev 2 to Draft and restore rev 1 to Published + job back to post-rev1 state.
    {
        let mut plans_json: Value =
            serde_json::from_slice(&fs::read(&plans_path).expect("read plans.json"))
                .expect("parse plans.json");
        for rev in plans_json["revisions"].as_array_mut().expect("revisions") {
            if rev["plan_id"] == u_rev2_r1_pr.plan_id && rev["revision"] == 1 {
                rev["status"] = json!("published");
            } else if rev["plan_id"] == u_rev2_r1_pr.plan_id && rev["revision"] == 2 {
                rev["status"] = json!("draft");
            }
        }
        write_private_file_0600(
            &plans_path,
            &serde_json::to_vec_pretty(&plans_json).expect("serialize plans.json"),
        );
        let mut ledger_json: Value =
            serde_json::from_slice(&fs::read(&wf_ledger_path).expect("read workflow ledger"))
                .expect("parse workflow ledger");
        let job_slot = ledger_json["jobs"]
            .as_array_mut()
            .expect("jobs")
            .iter_mut()
            .find(|j| j["job_id"] == job_u_rev2.job_id)
            .expect("U_rev2 job slot");
        *job_slot = serde_json::to_value(&post_u_rev2_r1_job).expect("serialize post-rev1 job");
        write_private_file_0600(
            &wf_ledger_path,
            &serde_json::to_vec_pretty(&ledger_json).expect("serialize workflow ledger"),
        );
    }

    let calls_before_rev2_draft = provider_invocation_count(&fixture.worker_marker);
    let reservations_before_rev2_draft =
        grant_reservation_count(&wf_store, &fixture.binding_id, &grant_id);
    assert_eq!(
        invoke_mcp_err_code(
            &mcp,
            "request_technical_plan",
            json!({
                "grant_id": grant_id,
                "job_id": job_j.job_id,
                "request_key": "req-j-crash-neg-u-rev2-draft",
                "plan_id": j_rev1_pr.plan_id,
                "expected_plan_revision": 4,
                "architect_advice_ids": [adv_j],
                "max_architect_rounds": 0,
                "difficulty": "hard"
            })
        )
        .await,
        "validation_failed"
    );
    let rev2_draft_direct_err = planning_service
        .orchestrate_brief_to_plan(
            &scoped,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: fixture.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_j.job_id.clone(),
                request_key: "direct-j-crash-neg-u-rev2-draft".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: Some(j_rev1_pr.plan_id.clone()),
                expected_plan_revision: 4,
                architect_advice_ids: vec![adv_j],
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: current_wall_clock_ms().expect("wall clock"),
            },
        )
        .await
        .expect_err("direct service must reject unpublished rev 2 draft");
    assert_eq!(
        rev2_draft_direct_err.error_code(),
        NativeErrorCode::ValidationFailed
    );
    assert_eq!(
        provider_invocation_count(&fixture.worker_marker),
        calls_before_rev2_draft
    );
    assert_eq!(
        grant_reservation_count(&wf_store, &fixture.binding_id, &grant_id),
        reservations_before_rev2_draft
    );

    fixture.shutdown().await;
}
