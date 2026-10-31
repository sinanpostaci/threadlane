#![cfg(unix)]
//! Behavioral end-to-end tests for the packaged `threadlane-flow` CLI.
//!
//! Every fixture uses a disposable local supervisor, private registry/plan/authority/session
//! stores, and a fake worker executable that records any invocation. The tests exercise the real
//! compiled binary for offline help/version, argument failures, hostile request scope, non-private
//! JSON refusal, and read-only preflight. No test starts a provider, invokes the fake worker, or
//! touches a global store.

use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use threadlane::{Supervisor, SupervisorClient, SupervisorConfig, SupervisorHandle};
use threadlane_native::role_flow::CreateRoleFlowCycleRequest;
use threadlane_native::{
    Difficulty, ExecutionPolicy, PolicyAllowRule, PreflightReport, PreflightRequest, RegistryStore,
    RoleExecutionRoute, RolePreflightRequest, ToolAdapter, EXECUTION_POLICY_SCHEMA_VERSION,
};
use threadlane_workflow::{
    ModelChoice, ModelDefinition, RegistryChange, RoleAssignment, ToolDefinition, WorkflowProfile,
    WorkflowRole,
};
use tokio::process::Command;

const THREAD_UUID: &str = "0196ed00-0000-7000-8000-0000000000f1";
const OTHER_THREAD_UUID: &str = "0196ed00-0000-7000-8000-0000000000f2";
const BINDING_LABEL: &str = "flow-cli-chat";
const PROFILE_ID: &str = "profile-flow-cli";
const TOOL_ID: &str = "tool-opencode";
const MODEL_ID: &str = "model-deepseek-flash";
const MAX_IO_BYTES: usize = 1_048_576;
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const SECRET_SENTINEL: &str = "THREADLANE_FLOW_SECRET_SENTINEL_6c1d4a";

#[derive(Debug, Deserialize)]
struct OperatorEnvelope<T> {
    untrusted_data: bool,
    route: String,
    native_per_call_identity_verified: bool,
    binding_id: String,
    data: T,
}

#[derive(Debug, Deserialize)]
struct CliFailure {
    code: String,
    message: String,
}

struct FlowFixture {
    _tmp: TempDir,
    worker_marker: PathBuf,
    state_dir: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    requests_dir: PathBuf,
    supervisor: Option<SupervisorHandle>,
    binding_id: String,
}

impl FlowFixture {
    async fn start() -> Self {
        let tmp = TempDir::new().expect("create flow cli tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize flow cli tempdir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .expect("chmod flow cli root 0700");

        let bin_dir = root.join("bin");
        create_private_dir(&bin_dir);
        let fake_worker = bin_dir.join("fake-opencode");
        let worker_marker = root.join("worker-invoked");
        write_fake_worker(&fake_worker, &worker_marker);

        let state_dir = root.join("runtime");
        let registry_dir = root.join("registry-store");
        let plans_dir = root.join("plans-store");
        let authority_dir = root.join("authority-store");
        let sessions_dir = root.join("sessions-store");
        let requests_dir = root.join("requests");
        for dir in [
            &state_dir,
            &registry_dir,
            &plans_dir,
            &authority_dir,
            &sessions_dir,
            &requests_dir,
        ] {
            create_private_dir(dir);
        }

        let supervisor = Supervisor::start(SupervisorConfig::new(state_dir.clone()))
            .await
            .expect("start disposable flow supervisor");
        let supervisor_client =
            SupervisorClient::from_state_dir(&state_dir).expect("connect flow supervisor");
        let binding = supervisor_client
            .create_binding(THREAD_UUID, BINDING_LABEL)
            .await
            .expect("create flow control binding");

        let registry = RegistryStore::open(&registry_dir).expect("open flow registry store");
        let s1 = registry
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: TOOL_ID.to_string(),
                        label: "OpenCode v2 flow fixture".to_string(),
                        adapter: ToolAdapter::OpencodeV2,
                        executable_path: Some(fake_worker),
                        enabled: true,
                    },
                },
            )
            .expect("upsert flow tool");
        let s2 = registry
            .apply(
                s1.revision,
                RegistryChange::UpsertModel {
                    model: ModelDefinition {
                        id: MODEL_ID.to_string(),
                        tool_id: TOOL_ID.to_string(),
                        label: "DeepSeek v4.1 Flash Max".to_string(),
                        model_arg: "opencode-go/deepseek-v4.1-flash".to_string(),
                        efforts: vec!["max".to_string()],
                        allow_no_effort: false,
                    },
                },
            )
            .expect("upsert flow model");
        registry
            .apply(
                s2.revision,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        id: PROFILE_ID.to_string(),
                        name: "Flow CLI fixture profile".to_string(),
                        assignments: vec![RoleAssignment {
                            role: WorkflowRole::Coder,
                            difficulty: Some(Difficulty::Easy),
                            choice: ModelChoice {
                                model_id: MODEL_ID.to_string(),
                                effort: Some("max".to_string()),
                            },
                        }],
                    },
                },
            )
            .expect("upsert flow profile");

        Self {
            _tmp: tmp,
            worker_marker,
            state_dir,
            registry_dir,
            plans_dir,
            authority_dir,
            sessions_dir,
            requests_dir,
            supervisor: Some(supervisor),
            binding_id: binding.binding_id,
        }
    }

    fn flow_args(&self, command: &str, extra: &[String]) -> Vec<String> {
        let mut args = vec![
            "--state-dir".to_string(),
            self.state_dir.display().to_string(),
            "--registry-dir".to_string(),
            self.registry_dir.display().to_string(),
            "--plans-dir".to_string(),
            self.plans_dir.display().to_string(),
            "--authority-dir".to_string(),
            self.authority_dir.display().to_string(),
            "--sessions-dir".to_string(),
            self.sessions_dir.display().to_string(),
            "--binding".to_string(),
            self.binding_id.clone(),
            "--thread".to_string(),
            THREAD_UUID.to_string(),
            command.to_string(),
        ];
        args.extend(extra.iter().cloned());
        args
    }

    fn write_request<T: Serialize>(&self, name: &str, request: &T) -> PathBuf {
        let path = self.requests_dir.join(name);
        let bytes = serde_json::to_vec(request).expect("serialize flow request");
        write_private_bytes(&path, &bytes);
        path
    }

    async fn shutdown(&mut self) {
        if let Some(supervisor) = self.supervisor.take() {
            supervisor.shutdown().await;
        }
    }
}

fn allowed_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
        policy_id: "policy-flow-cli".to_string(),
        allow_rules: vec![PolicyAllowRule {
            tool_id: TOOL_ID.to_string(),
            model_id: MODEL_ID.to_string(),
            efforts: vec!["max".to_string()],
            allow_no_effort: false,
            roles: vec![WorkflowRole::Coder],
        }],
    }
}

fn preflight_request() -> PreflightRequest {
    PreflightRequest {
        profile_id: Some(PROFILE_ID.to_string()),
        roles: vec![RolePreflightRequest {
            role: WorkflowRole::Coder,
            difficulty: Some(Difficulty::Easy),
        }],
    }
}

fn create_request(binding_id: &str, thread_uuid: &str) -> CreateRoleFlowCycleRequest {
    CreateRoleFlowCycleRequest {
        binding_id: binding_id.to_string(),
        thread_uuid: thread_uuid.to_string(),
        request_key: "flow-create-fixture".to_string(),
        expected_ledger_revision: None,
        approval_id: "apr_0123456789abcdef0123456789abcdef".to_string(),
        task_ids: vec!["task-1".to_string()],
        policy: ExecutionPolicy::deny_all("policy-flow-cli").expect("valid deny-all policy"),
    }
}

fn create_private_dir(path: &Path) {
    fs::create_dir_all(path).expect("create private fixture directory");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|err| panic!("chmod private fixture {} 0700: {err}", path.display()));
}

fn write_fake_worker(path: &Path, marker: &Path) {
    let script = format!(
        "#!/bin/sh\nset -eu\nprintf 'invoked\\n' > '{}'\nexit 97\n",
        marker.display()
    );
    fs::write(path, script).expect("write fake flow worker");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .expect("chmod fake flow worker 0700");
}

fn write_private_bytes(path: &Path, bytes: &[u8]) {
    let parent = path.parent().expect("private request path has a parent");
    create_private_dir(parent);
    fs::write(path, bytes).expect("write private request file");
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .expect("chmod private request file 0600");
}

fn directory_entries(path: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(path)
        .unwrap_or_else(|err| panic!("read directory {}: {err}", path.display()))
        .map(|entry| {
            entry
                .expect("read directory entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

fn sanitized_command(program: PathBuf) -> Command {
    let mut command = Command::new(program);
    command
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_CONFIG_GLOBAL", "/dev/null");
    command
}

async fn run_operator_command(mut command: Command, args: &[String], label: &str) -> Output {
    command.args(args);
    tokio::time::timeout(COMMAND_TIMEOUT, command.output())
        .await
        .unwrap_or_else(|_| panic!("{label} did not finish within {COMMAND_TIMEOUT:?}"))
        .unwrap_or_else(|err| panic!("execute {label}: {err}"))
}

async fn run_flow(args: &[String]) -> Output {
    run_operator_command(
        sanitized_command(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-flow"))),
        args,
        "threadlane-flow",
    )
    .await
}

async fn run_flow_offline(args: &[&str]) -> Output {
    let mut command = Command::new(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-flow")));
    command.args(args).env_clear();
    tokio::time::timeout(COMMAND_TIMEOUT, command.output())
        .await
        .expect("offline command did not finish")
        .expect("execute offline threadlane-flow")
}

fn parse_success<T: DeserializeOwned>(output: &Output, context: &str) -> T {
    assert!(
        output.status.success(),
        "{context} failed with status {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stdout.len() <= MAX_IO_BYTES,
        "{context}: stdout exceeded the bounded 1 MiB result envelope"
    );
    serde_json::from_slice(&output.stdout).unwrap_or_else(|err| {
        panic!(
            "{context}: stdout must contain one JSON operator envelope: {err}\nstdout: {}",
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

fn parse_envelope<T: DeserializeOwned>(output: &Output, context: &str, binding_id: &str) -> T {
    let envelope: OperatorEnvelope<T> = parse_success(output, context);
    assert!(
        envelope.untrusted_data,
        "{context}: result data is untrusted"
    );
    assert_eq!(
        envelope.route, "trusted_local_operator",
        "{context}: unexpected operator route"
    );
    assert!(
        !envelope.native_per_call_identity_verified,
        "{context}: CLI flags must not claim native per-call identity verification"
    );
    assert_eq!(
        envelope.binding_id, binding_id,
        "{context}: result envelope binding scope changed"
    );
    envelope.data
}

fn expect_failure(output: &Output, context: &str) -> CliFailure {
    assert!(
        !output.status.success(),
        "{context}: expected a nonzero exit, got success\nstdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        output.stdout.is_empty(),
        "{context}: failed commands must not write result data to stdout\nstdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        output.stderr.len() <= MAX_IO_BYTES,
        "{context}: stderr exceeded the bounded 1 MiB error envelope"
    );
    let failure: CliFailure = serde_json::from_slice(&output.stderr).unwrap_or_else(|err| {
        panic!(
            "{context}: stderr must contain one static JSON error envelope: {err}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    assert!(
        !failure.code.trim().is_empty(),
        "{context}: error code must not be empty"
    );
    assert!(
        !failure.message.trim().is_empty(),
        "{context}: error message must not be empty"
    );
    failure
}

fn expect_failure_code(output: &Output, context: &str, expected: &str) -> CliFailure {
    let failure = expect_failure(output, context);
    assert_eq!(
        failure.code, expected,
        "{context}: unexpected static error code"
    );
    failure
}

fn assert_no_secret_echo(output: &Output, context: &str) {
    for (stream, bytes) in [
        ("stdout", output.stdout.as_slice()),
        ("stderr", output.stderr.as_slice()),
    ] {
        let text = String::from_utf8_lossy(bytes);
        assert!(
            !text.contains(SECRET_SENTINEL),
            "{context}: raw request sentinel was echoed on {stream}: {text}"
        );
    }
}

#[tokio::test]
async fn help_and_version_are_offline_and_require_no_stores() {
    let help = run_flow_offline(&["--help"]).await;
    assert!(
        help.status.success(),
        "--help failed: {}",
        String::from_utf8_lossy(&help.stderr)
    );
    let help_text = String::from_utf8_lossy(&help.stdout);
    assert!(help_text.contains("threadlane-flow"));
    assert!(help_text.contains("preflight"));
    assert!(help_text.contains("reserve-closure"));

    let version = run_flow_offline(&["--version"]).await;
    assert!(
        version.status.success(),
        "--version failed: {}",
        String::from_utf8_lossy(&version.stderr)
    );
    let version_text = String::from_utf8_lossy(&version.stdout);
    assert!(version_text.contains("threadlane-flow"));
    assert!(version_text.contains(env!("CARGO_PKG_VERSION")));
}

#[tokio::test]
async fn missing_and_invalid_arguments_use_the_bounded_error_envelope() {
    expect_failure_code(
        &run_flow_offline(&[]).await,
        "missing subcommand",
        "invalid_input",
    );
    expect_failure_code(
        &run_flow_offline(&["get"]).await,
        "missing cycle id",
        "invalid_input",
    );
    expect_failure_code(
        &run_flow_offline(&["list", "--limit", "not-a-number"]).await,
        "invalid limit",
        "invalid_input",
    );
}

#[tokio::test]
async fn preflight_is_read_only_and_reports_the_live_bound_scope() {
    let mut fixture = FlowFixture::start().await;
    let request_path = fixture.write_request("preflight.json", &preflight_request());
    let policy_path = fixture.write_request("policy.json", &allowed_policy());
    assert_mode(&request_path, 0o600, "preflight request");
    assert_mode(&policy_path, 0o600, "execution policy");

    let plans_before = directory_entries(&fixture.plans_dir);
    let authority_before = directory_entries(&fixture.authority_dir);
    let sessions_before = directory_entries(&fixture.sessions_dir);
    let registry_before = fs::read(fixture.registry_dir.join("registry.json"))
        .expect("read registry snapshot before preflight");
    let registry_entries_before = directory_entries(&fixture.registry_dir);

    let args = fixture.flow_args(
        "preflight",
        &[
            "--request".to_string(),
            request_path.display().to_string(),
            "--policy".to_string(),
            policy_path.display().to_string(),
        ],
    );
    let output = run_flow(&args).await;
    let report: PreflightReport = parse_envelope(&output, "preflight", &fixture.binding_id);
    assert!(report.ready, "fixture preflight must be ready");
    assert_eq!(report.profile_id.as_deref(), Some(PROFILE_ID));
    assert_eq!(report.roles.len(), 1);
    let role = &report.roles[0];
    assert!(role.ready);
    assert_eq!(role.role, WorkflowRole::Coder);
    let selected = role.selected.as_ref().expect("selected role choice");
    assert_eq!(selected.route, RoleExecutionRoute::HeadlessWorker);
    assert_eq!(selected.model_id, MODEL_ID);
    assert_eq!(selected.model_arg, "opencode-go/deepseek-v4.1-flash");
    assert_eq!(selected.effort.as_deref(), Some("max"));
    assert_eq!(report.policy_id, "policy-flow-cli");
    assert_eq!(report.policy_digest.len(), 64);

    assert_eq!(
        directory_entries(&fixture.plans_dir),
        plans_before,
        "preflight must not create plan or role-flow files"
    );
    assert_eq!(
        directory_entries(&fixture.authority_dir),
        authority_before,
        "preflight must not open or write the authority store"
    );
    assert_eq!(
        directory_entries(&fixture.sessions_dir),
        sessions_before,
        "preflight must not create a host session"
    );
    assert_eq!(
        directory_entries(&fixture.registry_dir),
        registry_entries_before,
        "preflight must not create a registry lock or state file"
    );
    assert_eq!(
        fs::read(fixture.registry_dir.join("registry.json"))
            .expect("read registry snapshot after preflight"),
        registry_before,
        "preflight must not rewrite the registry snapshot"
    );
    assert!(
        !fixture.worker_marker.exists(),
        "preflight must not invoke the configured worker executable"
    );
    assert_no_secret_echo(&output, "preflight");

    fixture.shutdown().await;
}

#[tokio::test]
async fn create_rejects_foreign_scope_and_non_private_json_without_store_writes() {
    let mut fixture = FlowFixture::start().await;
    let plans_before = directory_entries(&fixture.plans_dir);
    let authority_before = directory_entries(&fixture.authority_dir);
    let sessions_before = directory_entries(&fixture.sessions_dir);

    let mut foreign = create_request("bnd_flow_foreign_scope", OTHER_THREAD_UUID);
    foreign.request_key = SECRET_SENTINEL.to_string();
    let foreign_path = fixture.write_request("create-foreign.json", &foreign);
    let foreign_args = fixture.flow_args(
        "create",
        &["--request".to_string(), foreign_path.display().to_string()],
    );
    let foreign_output = run_flow(&foreign_args).await;
    expect_failure_code(&foreign_output, "foreign scope create", "scope_mismatch");
    assert_no_secret_echo(&foreign_output, "foreign scope create");
    assert_eq!(
        directory_entries(&fixture.plans_dir),
        plans_before,
        "foreign scope create must not write role-flow state"
    );
    assert_eq!(
        directory_entries(&fixture.authority_dir),
        authority_before,
        "foreign scope create must not open or write the authority store"
    );
    assert_eq!(
        directory_entries(&fixture.sessions_dir),
        sessions_before,
        "foreign scope create must not create a host session"
    );

    let mut foreign_thread = create_request(&fixture.binding_id, OTHER_THREAD_UUID);
    foreign_thread.request_key = "flow-create-foreign-thread".to_string();
    let foreign_thread_path = fixture.write_request("create-foreign-thread.json", &foreign_thread);
    let foreign_thread_args = fixture.flow_args(
        "create",
        &[
            "--request".to_string(),
            foreign_thread_path.display().to_string(),
        ],
    );
    expect_failure_code(
        &run_flow(&foreign_thread_args).await,
        "foreign thread create",
        "scope_mismatch",
    );

    let fixed = create_request(&fixture.binding_id, THREAD_UUID);
    let insecure_path = fixture.write_request("create-insecure.json", &fixed);
    fs::set_permissions(&insecure_path, fs::Permissions::from_mode(0o644))
        .expect("chmod insecure request 0644");
    let insecure_args = fixture.flow_args(
        "create",
        &["--request".to_string(), insecure_path.display().to_string()],
    );
    let insecure_output = run_flow(&insecure_args).await;
    expect_failure_code(
        &insecure_output,
        "non-private request",
        "security_violation",
    );

    let private_path = fixture.write_request("create-private.json", &fixed);
    let link_path = fixture.requests_dir.join("create-link.json");
    symlink(&private_path, &link_path).expect("create request symlink");
    let link_args = fixture.flow_args(
        "create",
        &["--request".to_string(), link_path.display().to_string()],
    );
    let link_output = run_flow(&link_args).await;
    expect_failure_code(&link_output, "symlinked request", "security_violation");

    assert_eq!(
        directory_entries(&fixture.plans_dir),
        plans_before,
        "refused request files must not write role-flow state"
    );
    assert_eq!(
        directory_entries(&fixture.authority_dir),
        authority_before,
        "refused request files must not write authority state"
    );
    assert!(
        !fixture.worker_marker.exists(),
        "refused requests must not invoke the configured worker executable"
    );

    fixture.shutdown().await;
}

fn assert_mode(path: &Path, expected: u32, label: &str) {
    let mode = fs::symlink_metadata(path)
        .unwrap_or_else(|err| panic!("stat {label} {}: {err}", path.display()))
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, expected, "{label} permissions must be {expected:04o}");
}
