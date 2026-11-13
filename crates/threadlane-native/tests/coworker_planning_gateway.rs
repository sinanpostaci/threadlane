#![cfg(unix)]
#![allow(clippy::expect_used, clippy::unwrap_used)]
//! Behavioral integration tests for [`CoworkerPlanningGateway`] and the five Paired MCP
//! Coworker planning tools (`coworker_status`, `prepare_work`, `consult_architect`,
//! `request_technical_plan`, and `planning_command`).

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
    ArchitectContextDependencies, CanonicalProjectBaseRef, CoworkerJobOwnershipStore,
    CoworkerJobPhase, CoworkerPlanningService, CoworkerWorkflowStore, CreatePlanningGrantRequest,
    PlanningCallBudget, PlanningReadOnlyCapabilities, RevokePlanningGrantRequest,
    RunArchitectAdviceRequest, WorkBriefKind,
};
use threadlane_native::{
    current_wall_clock_ms, planning_commands_dir, ConsultArchitectParams, CoworkerPlanningGateway,
    CoworkerStatusParams, CoworkerStatusResponse, Difficulty, EnqueuePlanningCommandRequest,
    ExecutionPolicy, NativeErrorCode, OperatorEnrollRequest, PairedMcpServer, PairingStore,
    PlanStatus, PlanningCommandDetailResponse, PlanningCommandResultRef,
    PlanningCommandStartStatus, PlanningCommandStatus, PlanningCommandStore,
    PlanningCommandSubmissionResponse, PolicyAllowRule, PrepareWorkResponse, RegistryStore,
    ToolAdapter, WorkflowProfile, WorkflowRole, EXECUTION_POLICY_SCHEMA_VERSION,
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
                policy: default_policy(),
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
MODE="architect"
if [ -f '{}' ]; then
    MODE=$(cat '{}' | tr -d '[:space:]')
fi
printf '{{"event":"init","init":{{"session_id":"agy-gw-sess-1","model":"gemini-4-argon-high"}}}}\n'
case "$MODE" in
    orch_clarify)
        RESPONSE_JSON='{{\"outcome\":\"requires_human_clarification\",\"schema_version\":1,\"summary\":\"Need human decision before planning\",\"questions\":[\"Should we support multiple commands per batch?\"],\"referenced_files\":[\"src/lib.rs\"]}}'
        ;;
    orch_ask_then_plan)
        printf 'orch_ask_step_arch\n' > '{}'
        RESPONSE_JSON='{{\"outcome\":\"requires_architect_advice\",\"schema_version\":1,\"summary\":\"Need Architect contract check before finalizing plan\",\"questions\":[{{\"question_id\":\"q_arch_1\",\"question_summary\":\"Confirm boundary contract for src/lib.rs\",\"dependency_files\":[\"src/lib.rs\"],\"interfaces\":[\"iface_gateway\"]}}]}}'
        ;;
    orch_ask_step_arch)
        printf 'orch_plan\n' > '{}'
        RESPONSE_JSON='{{\"schema_version\":1,\"decision_summary\":\"Keep strict planning command boundary\",\"rationale\":\"Verify completed command result against stored advice, reservation, and private artifact.\",\"recommended_write_paths\":[\"src/lib.rs\"],\"interface_contracts\":{{\"iface_gateway\":\"Stable contract v1\"}},\"open_questions\":[]}}'
        ;;
    orch_plan)
        RESPONSE_JSON='{{\"outcome\":\"plan\",\"schema_version\":1,\"summary\":\"Implement gateway slice\",\"tasks\":[{{\"task_id\":\"task_gateway_core\",\"title\":\"Implement planning gateway\",\"description\":\"Wire MCP tools to sibling threadlane-flow.\",\"role\":\"coder\",\"difficulty\":\"hard\",\"depends_on\":[],\"allowed_write_paths\":[\"src/lib.rs\"],\"owned_interfaces\":[\"iface_gateway\"],\"consumed_interfaces\":[]}}],\"check_suggestions\":[{{\"check_id\":\"cargo_test_gateway\",\"command_summary\":\"cargo test -p threadlane-native --test coworker_planning_gateway\",\"required\":true}}],\"unconfirmed_proposal_notes\":[\"Proposal stays unconfirmed\"],\"resolved_open_questions\":[],\"deferred_proposals\":[]}}'
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
        mode_path.display(),
        mode_path.display(),
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

    // 2. PairedMcpServer advertises all 5 Coworker planning tools (52 tools total, including ordinary and guarded backlog tools) and blocks unbound calls.
    let pairing_store = PairingStore::open(&fixture.pairing_dir).expect("open pairing store");
    let mcp_unbound =
        PairedMcpServer::new(&fixture.state_dir, pairing_store.clone()).expect("new unbound mcp");
    let tools = mcp_unbound.list_tools();
    assert_eq!(tools.len(), 52);
    for (name, expected_ro) in [
        ("coworker_status", true),
        ("prepare_work", false),
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
