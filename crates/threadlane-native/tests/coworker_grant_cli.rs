#![cfg(unix)]
//! Behavioral CLI integration tests for `threadlane-flow` planning/advice grant subcommands
//! (`grant-prepare`, `grant-record`, `grant-revoke`, `grant-list`, and `grant-get`).

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use threadlane::{Supervisor, SupervisorClient, SupervisorConfig, SupervisorHandle};
use threadlane_native::coworker_grants::{
    LocalOperatorGrantAttestation, PlanningGrantPreview, PlanningGrantPreviewStatus,
    PlanningGrantSummary, PreparePlanningGrantRequest, RecordPlanningGrantRequest,
};
use threadlane_native::coworker_workflow::{
    PlanningCallBudget, PlanningGrantStatus, PlanningReadOnlyCapabilities,
    RevokePlanningGrantRequest,
};
use threadlane_native::{
    Difficulty, ExecutionPolicy, PolicyAllowRule, RegistryStore, RoleExecutionRoute,
    RolePreflightRequest, ToolAdapter, WorkflowProfile, WorkflowRole,
    EXECUTION_POLICY_SCHEMA_VERSION,
};
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::{
    ModelChoice, ModelDefinition, RegistryChange, RoleAssignment, ToolDefinition,
};
use tokio::process::Command;

const THREAD_UUID: &str = "0196ed00-0000-7000-8000-0000000000a1";
const OTHER_THREAD_UUID: &str = "0196ed00-0000-7000-8000-0000000000a2";
const BINDING_LABEL: &str = "grant-cli-chat";
const PROFILE_ID: &str = "profile-grant-cli";
const TOOL_ID: &str = "tool-agy";
const MODEL_ID: &str = "model-argon";
const MAX_IO_BYTES: usize = 1_048_576;
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const SECRET_SENTINEL: &str = "THREADLANE_GRANT_CLI_SECRET_SENTINEL_9b3e7f";
const WRONG_DIGEST: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

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

struct GrantCliFixture {
    _tmp: TempDir,
    repo_dir: PathBuf,
    base_commit: String,
    base_tree_sha: String,
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

impl GrantCliFixture {
    async fn start() -> Self {
        let tmp = TempDir::new().expect("create grant cli tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize grant cli tempdir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .expect("chmod grant cli root 0700");

        let repo_dir = root.join("repo");
        let (base_commit, base_tree_sha) = init_git_repo(&repo_dir);

        let bin_dir = root.join("bin");
        create_private_dir(&bin_dir);
        let fake_worker = bin_dir.join("fake-agy");
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
            .expect("start disposable grant supervisor");
        let supervisor_client =
            SupervisorClient::from_state_dir(&state_dir).expect("connect grant supervisor");
        let binding = supervisor_client
            .create_binding(THREAD_UUID, BINDING_LABEL)
            .await
            .expect("create grant control binding");

        let registry = RegistryStore::open(&registry_dir).expect("open grant registry store");
        let s1 = registry
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: TOOL_ID.to_string(),
                        label: "Antigravity grant fixture".to_string(),
                        adapter: ToolAdapter::Antigravity,
                        executable_path: Some(fake_worker),
                        enabled: true,
                    },
                },
            )
            .expect("upsert grant tool");
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
            .expect("upsert grant model");
        registry
            .apply(
                s2.revision,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        decision_advisor: None,
                        id: PROFILE_ID.to_string(),
                        name: "Grant CLI fixture profile".to_string(),
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
                        ],
                    },
                },
            )
            .expect("upsert grant profile");

        let registry_lock = registry_dir.join("registry.lock");
        if registry_lock.exists() {
            fs::remove_file(&registry_lock).expect("remove initial registry.lock");
        }

        Self {
            _tmp: tmp,
            repo_dir,
            base_commit,
            base_tree_sha,
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
        self.flow_args_for(&self.binding_id, THREAD_UUID, command, extra)
    }

    fn flow_args_for(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        command: &str,
        extra: &[String],
    ) -> Vec<String> {
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
            binding_id.to_string(),
            "--thread".to_string(),
            thread_uuid.to_string(),
            command.to_string(),
        ];
        args.extend(extra.iter().cloned());
        args
    }

    fn write_request<T: Serialize>(&self, name: &str, request: &T) -> PathBuf {
        let path = self.requests_dir.join(name);
        let bytes = serde_json::to_vec(request).expect("serialize grant request");
        write_private_bytes(&path, &bytes);
        path
    }

    fn sample_prepare_request(&self, request_key: &str) -> PreparePlanningGrantRequest {
        PreparePlanningGrantRequest {
            request_key: request_key.to_string(),
            binding_id: self.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            repository_path: self.repo_dir.clone(),
            base_commit: self.base_commit.clone(),
            expected_base_tree_sha: Some(self.base_tree_sha.clone()),
            profile_id: PROFILE_ID.to_string(),
            expected_registry_revision: Some(3),
            roles: vec![
                RolePreflightRequest::new(WorkflowRole::Orchestrator, Some(Difficulty::Hard)),
                RolePreflightRequest::new(WorkflowRole::Architect, Some(Difficulty::Hard)),
            ],
            policy: ExecutionPolicy {
                schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
                policy_id: "policy-grant-cli".to_string(),
                allow_rules: vec![PolicyAllowRule {
                    tool_id: TOOL_ID.to_string(),
                    model_id: MODEL_ID.to_string(),
                    efforts: vec!["high".to_string()],
                    allow_no_effort: false,
                    roles: vec![WorkflowRole::Orchestrator, WorkflowRole::Architect],
                }],
            },
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget: PlanningCallBudget {
                max_total_calls: 6,
                max_orchestrator_calls: 4,
                max_architect_calls: 2,
                max_timeout_ms_per_call: 60_000,
                max_output_bytes_per_call: 65_536,
            },
            created_at_ms: 1_760_000_000_000,
            expires_at_ms: 1_760_003_600_000,
        }
    }

    async fn shutdown(&mut self) {
        if let Some(supervisor) = self.supervisor.take() {
            supervisor.shutdown().await;
        }
    }
}

fn init_git_repo(repo_dir: &Path) -> (String, String) {
    fs::create_dir_all(repo_dir.join("src")).expect("create repo src");
    fs::write(
        repo_dir.join("src/lib.rs"),
        "pub fn grant_seed() -> u32 { 11 }\n",
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
    (commit, tree)
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
    fs::write(path, script).expect("write fake grant worker");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .expect("chmod fake grant worker 0700");
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

async fn run_flow_with_env(args: &[String], envs: &[(&str, &str)]) -> Output {
    let mut command = sanitized_command(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-flow")));
    command.args(args);
    for (k, v) in envs {
        command.env(k, v);
    }
    tokio::time::timeout(COMMAND_TIMEOUT, command.output())
        .await
        .expect("threadlane-flow did not finish within timeout")
        .expect("execute threadlane-flow")
}

async fn run_flow(args: &[String]) -> Output {
    run_flow_with_env(args, &[]).await
}

async fn run_flow_offline(args: &[&str]) -> Output {
    let mut command = Command::new(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-flow")));
    command.args(args).env_clear();
    tokio::time::timeout(COMMAND_TIMEOUT, command.output())
        .await
        .expect("offline command did not finish")
        .expect("execute offline threadlane-flow")
}

fn parse_envelope<T: DeserializeOwned>(output: &Output, context: &str, binding_id: &str) -> T {
    assert!(
        output.status.success(),
        "{context} failed with status {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stdout.len() <= MAX_IO_BYTES,
        "{context}: stdout exceeded bounded 1 MiB result envelope"
    );
    let envelope: OperatorEnvelope<T> =
        serde_json::from_slice(&output.stdout).unwrap_or_else(|err| {
            panic!(
                "{context}: invalid operator JSON envelope: {err}\nstdout: {}",
                String::from_utf8_lossy(&output.stdout)
            )
        });
    assert!(
        envelope.untrusted_data,
        "{context}: untrusted_data must be true"
    );
    assert_eq!(envelope.route, "trusted_local_operator");
    assert!(
        !envelope.native_per_call_identity_verified,
        "{context}: local operator route must never claim per-call native identity"
    );
    assert_eq!(envelope.binding_id, binding_id);
    envelope.data
}

fn expect_failure_code(output: &Output, context: &str, expected: &str) -> CliFailure {
    assert!(
        !output.status.success(),
        "{context}: expected nonzero exit, got success\nstdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        output.stdout.is_empty(),
        "{context}: failed command must not write result data to stdout"
    );
    assert!(output.stderr.len() <= MAX_IO_BYTES);
    let failure: CliFailure = serde_json::from_slice(&output.stderr).unwrap_or_else(|err| {
        panic!(
            "{context}: stderr must contain static JSON error envelope: {err}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    assert_eq!(failure.code, expected, "{context}: unexpected error code");
    assert!(!failure.message.trim().is_empty());
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
            "{context}: secret sentinel echoed on {stream}: {text}"
        );
    }
}

#[tokio::test]
async fn help_and_invalid_grant_cli_arguments_use_bounded_error_envelope() {
    let help = run_flow_offline(&["--help"]).await;
    assert!(help.status.success());
    let help_text = String::from_utf8_lossy(&help.stdout);
    for cmd in [
        "grant-prepare",
        "grant-record",
        "grant-revoke",
        "grant-list",
        "grant-get",
    ] {
        assert!(help_text.contains(cmd), "--help must list `{cmd}`");
    }

    let record_help = run_flow_offline(&["grant-record", "--help"]).await;
    assert!(record_help.status.success());
    let record_help_text = String::from_utf8_lossy(&record_help.stdout);
    assert!(record_help_text.contains("--confirm-preview"));
    assert!(record_help_text.contains("--operator-approved"));

    expect_failure_code(
        &run_flow_offline(&["grant-prepare"]).await,
        "missing grant-prepare --request",
        "invalid_input",
    );
    expect_failure_code(
        &run_flow_offline(&["grant-record"]).await,
        "missing grant-record --request",
        "invalid_input",
    );
    expect_failure_code(
        &run_flow_offline(&["grant-revoke"]).await,
        "missing grant-revoke --request",
        "invalid_input",
    );
    expect_failure_code(
        &run_flow_offline(&["grant-get"]).await,
        "missing grant-get --grant-id",
        "invalid_input",
    );
}

#[tokio::test]
async fn grant_prepare_list_and_get_are_read_only_even_with_parent_observation_env() {
    let mut fixture = GrantCliFixture::start().await;
    let prepare_req = fixture.sample_prepare_request("req-grant-preview-1");
    let prepare_path = fixture.write_request("grant-prepare.json", &prepare_req);

    let plans_before = directory_entries(&fixture.plans_dir);
    let registry_before = directory_entries(&fixture.registry_dir);
    let registry_bytes_before =
        fs::read(fixture.registry_dir.join("registry.json")).expect("read registry.json");
    let authority_before = directory_entries(&fixture.authority_dir);
    let sessions_before = directory_entries(&fixture.sessions_dir);
    assert!(plans_before.is_empty(), "plans_dir starts empty");
    assert!(
        !registry_before.contains(&"registry.lock".to_string()),
        "registry.lock absent before preview"
    );

    let parent_envs = [
        ("THREADLANE_PARENT_REQUEST_ID", "obsreq_parent_grant_check"),
        ("THREADLANE_PLAN_ID", "plan_parent_grant_check"),
    ];

    let prepare_args = fixture.flow_args(
        "grant-prepare",
        &["--request".to_string(), prepare_path.display().to_string()],
    );
    let prepare_out = run_flow_with_env(&prepare_args, &parent_envs).await;
    let preview: PlanningGrantPreview =
        parse_envelope(&prepare_out, "grant-prepare", &fixture.binding_id);
    assert_eq!(
        preview.status,
        PlanningGrantPreviewStatus::ProposedCandidateNotGranted
    );
    assert!(!preview.grant_active);
    assert!(!preview.capabilities.code_execution_approval_granted);
    assert!(preview.capabilities.read_only_filesystem);
    assert_eq!(preview.request_key, "req-grant-preview-1");
    assert_eq!(preview.binding_id, fixture.binding_id);
    assert_eq!(preview.thread_uuid, THREAD_UUID);
    assert_eq!(preview.profile_id, PROFILE_ID);
    assert_eq!(preview.registry_revision, 3);
    assert_eq!(preview.preview_digest.len(), 64);
    assert_eq!(preview.frozen_role_choices.len(), 2);
    for choice in &preview.frozen_role_choices {
        assert_eq!(
            choice.selected_choice.route,
            RoleExecutionRoute::HeadlessWorker
        );
        assert_eq!(choice.selected_choice.model_id, MODEL_ID);
        assert_eq!(choice.selected_choice.model_arg, "gemini-4-argon-high");
        assert_eq!(choice.selected_choice.effort.as_deref(), Some("high"));
    }

    let list_args = fixture.flow_args("grant-list", &[]);
    let list_out = run_flow_with_env(&list_args, &parent_envs).await;
    let listed: Vec<PlanningGrantSummary> =
        parse_envelope(&list_out, "grant-list empty", &fixture.binding_id);
    assert!(listed.is_empty());

    let get_args = fixture.flow_args(
        "grant-get",
        &[
            "--grant-id".to_string(),
            "pgr_00000000000000000000000000000000".to_string(),
        ],
    );
    let get_out = run_flow_with_env(&get_args, &parent_envs).await;
    expect_failure_code(&get_out, "grant-get missing", "grant_not_found");

    assert_eq!(
        directory_entries(&fixture.plans_dir),
        plans_before,
        "preview/list/get must not create store, lock, or journal files in plans_dir even with parent observation env"
    );
    assert_eq!(
        directory_entries(&fixture.registry_dir),
        registry_before,
        "preview/list/get must not create registry.lock"
    );
    assert_eq!(
        fs::read(fixture.registry_dir.join("registry.json")).expect("read registry.json"),
        registry_bytes_before
    );
    assert_eq!(directory_entries(&fixture.authority_dir), authority_before);
    assert_eq!(directory_entries(&fixture.sessions_dir), sessions_before);
    assert!(!fixture.worker_marker.exists());

    fixture.shutdown().await;
}

#[tokio::test]
async fn grant_record_rejects_file_only_attestation_digest_mismatch_and_wrong_scope() {
    let mut fixture = GrantCliFixture::start().await;
    let prepare_req = fixture.sample_prepare_request("req-grant-guard-1");
    let prepare_path = fixture.write_request("prepare-guard.json", &prepare_req);
    let preview: PlanningGrantPreview = parse_envelope(
        &run_flow(&fixture.flow_args(
            "grant-prepare",
            &["--request".to_string(), prepare_path.display().to_string()],
        ))
        .await,
        "prepare preview for guard test",
        &fixture.binding_id,
    );

    // 1. Wrong CLI --binding or --thread is rejected by verify_binding before any file or journal write.
    let bad_thread_args = fixture.flow_args_for(
        &fixture.binding_id,
        OTHER_THREAD_UUID,
        "grant-prepare",
        &["--request".to_string(), prepare_path.display().to_string()],
    );
    expect_failure_code(
        &run_flow(&bad_thread_args).await,
        "wrong CLI thread on grant-prepare",
        "thread_uuid_mismatch",
    );
    assert!(
        directory_entries(&fixture.plans_dir).is_empty(),
        "verify_binding failure must leave plans_dir completely empty"
    );

    // 2. File-only attestation (`operator_attested: true` and `confirmed_preview_digest` in JSON)
    //    must be rejected unless BOTH `--operator-approved` and `--confirm-preview` CLI flags are passed.
    let self_attested_record = RecordPlanningGrantRequest {
        request_key: preview.request_key.clone(),
        expected_ledger_revision: Some(0),
        confirmed_preview_digest: preview.preview_digest.clone(),
        preview: preview.clone(),
        attestation: LocalOperatorGrantAttestation::attested(),
    };
    let record_path = fixture.write_request("record-self-attested.json", &self_attested_record);

    let no_flags_args = fixture.flow_args(
        "grant-record",
        &["--request".to_string(), record_path.display().to_string()],
    );
    expect_failure_code(
        &run_flow(&no_flags_args).await,
        "file-only attestation without CLI flags",
        "unauthorized_operator",
    );

    let missing_approved_args = fixture.flow_args(
        "grant-record",
        &[
            "--request".to_string(),
            record_path.display().to_string(),
            "--confirm-preview".to_string(),
            preview.preview_digest.clone(),
        ],
    );
    expect_failure_code(
        &run_flow(&missing_approved_args).await,
        "missing --operator-approved flag",
        "unauthorized_operator",
    );

    let missing_digest_args = fixture.flow_args(
        "grant-record",
        &[
            "--request".to_string(),
            record_path.display().to_string(),
            "--operator-approved".to_string(),
        ],
    );
    expect_failure_code(
        &run_flow(&missing_digest_args).await,
        "missing --confirm-preview flag",
        "invalid_input",
    );

    let malformed_digest_args = fixture.flow_args(
        "grant-record",
        &[
            "--request".to_string(),
            record_path.display().to_string(),
            "--operator-approved".to_string(),
            "--confirm-preview".to_string(),
            "not-a-valid-sha256".to_string(),
        ],
    );
    expect_failure_code(
        &run_flow(&malformed_digest_args).await,
        "malformed --confirm-preview flag",
        "invalid_input",
    );

    // 3. Digest override mismatch: CLI `--confirm-preview` disagrees with the preview digest.
    let wrong_digest_args = fixture.flow_args(
        "grant-record",
        &[
            "--request".to_string(),
            record_path.display().to_string(),
            "--operator-approved".to_string(),
            "--confirm-preview".to_string(),
            WRONG_DIGEST.to_string(),
        ],
    );
    expect_failure_code(
        &run_flow(&wrong_digest_args).await,
        "CLI --confirm-preview override mismatch",
        "validation_failed",
    );

    // 4. Wrong fixed binding/thread in JSON request body is rejected with `scope_mismatch` before
    //    creating any grant ledger or lockfile, and never echoes secret request keys.
    let mut foreign_prepare = prepare_req.clone();
    foreign_prepare.binding_id = "bnd_foreign_grant_scope".to_string();
    foreign_prepare.request_key = SECRET_SENTINEL.to_string();
    let foreign_prepare_path =
        fixture.write_request("prepare-foreign-scope.json", &foreign_prepare);
    let foreign_prepare_out = run_flow(&fixture.flow_args(
        "grant-prepare",
        &[
            "--request".to_string(),
            foreign_prepare_path.display().to_string(),
        ],
    ))
    .await;
    expect_failure_code(
        &foreign_prepare_out,
        "foreign binding in grant-prepare",
        "scope_mismatch",
    );
    assert_no_secret_echo(&foreign_prepare_out, "foreign binding in grant-prepare");

    let mut foreign_record = self_attested_record.clone();
    foreign_record.preview.thread_uuid = OTHER_THREAD_UUID.to_string();
    foreign_record.request_key = SECRET_SENTINEL.to_string();
    let foreign_record_path = fixture.write_request("record-foreign-thread.json", &foreign_record);
    let foreign_record_out = run_flow(&fixture.flow_args(
        "grant-record",
        &[
            "--request".to_string(),
            foreign_record_path.display().to_string(),
            "--operator-approved".to_string(),
            "--confirm-preview".to_string(),
            preview.preview_digest.clone(),
        ],
    ))
    .await;
    expect_failure_code(
        &foreign_record_out,
        "foreign thread in grant-record",
        "scope_mismatch",
    );
    assert_no_secret_echo(&foreign_record_out, "foreign thread in grant-record");

    let foreign_revoke = RevokePlanningGrantRequest {
        binding_id: "bnd_foreign_grant_scope".to_string(),
        thread_uuid: THREAD_UUID.to_string(),
        grant_id: "pgr_0123456789abcdef0123456789abcdef".to_string(),
        expected_ledger_revision: Some(1),
        revoked_at_ms: 1_760_000_050_000,
        reason: SECRET_SENTINEL.to_string(),
    };
    let foreign_revoke_path = fixture.write_request("revoke-foreign-scope.json", &foreign_revoke);
    let foreign_revoke_out = run_flow(&fixture.flow_args(
        "grant-revoke",
        &[
            "--request".to_string(),
            foreign_revoke_path.display().to_string(),
        ],
    ))
    .await;
    expect_failure_code(
        &foreign_revoke_out,
        "foreign binding in grant-revoke",
        "scope_mismatch",
    );
    assert_no_secret_echo(&foreign_revoke_out, "foreign binding in grant-revoke");

    // Verify no coworker-workflow ledger or lockfile was created in plans_dir, and grant-list is empty.
    for entry in directory_entries(&fixture.plans_dir) {
        assert!(
            !entry.starts_with("coworker-workflow-"),
            "rejected grant operations must not create coworker-workflow ledger/lock files, found {entry}"
        );
    }
    let listed: Vec<PlanningGrantSummary> = parse_envelope(
        &run_flow(&fixture.flow_args("grant-list", &[])).await,
        "grant-list after rejected mutations",
        &fixture.binding_id,
    );
    assert!(listed.is_empty());
    assert!(!fixture.worker_marker.exists());

    fixture.shutdown().await;
}

#[tokio::test]
async fn grant_record_list_get_and_revoke_lifecycle_with_cas_and_stale_registry_refusal() {
    let mut fixture = GrantCliFixture::start().await;
    let prepare_req = fixture.sample_prepare_request("req-grant-lifecycle-1");
    let prepare_path = fixture.write_request("prepare-lifecycle.json", &prepare_req);
    let preview: PlanningGrantPreview = parse_envelope(
        &run_flow(&fixture.flow_args(
            "grant-prepare",
            &["--request".to_string(), prepare_path.display().to_string()],
        ))
        .await,
        "grant-prepare lifecycle",
        &fixture.binding_id,
    );

    // Record using unattested/empty file fields + explicit CLI `--operator-approved` and `--confirm-preview`.
    let record_req = RecordPlanningGrantRequest {
        request_key: preview.request_key.clone(),
        expected_ledger_revision: Some(0),
        confirmed_preview_digest: String::new(),
        preview: preview.clone(),
        attestation: LocalOperatorGrantAttestation::unattested(),
    };
    let record_path = fixture.write_request("record-lifecycle.json", &record_req);
    let record_args = fixture.flow_args(
        "grant-record",
        &[
            "--request".to_string(),
            record_path.display().to_string(),
            "--confirm-preview".to_string(),
            preview.preview_digest.clone(),
            "--operator-approved".to_string(),
        ],
    );
    let recorded: PlanningGrantSummary = parse_envelope(
        &run_flow(&record_args).await,
        "grant-record lifecycle",
        &fixture.binding_id,
    );
    assert!(recorded.grant_id.starts_with("pgr_"));
    assert_eq!(recorded.ledger_revision, 1);
    assert_eq!(recorded.status, PlanningGrantStatus::Active);
    assert!(recorded.operator_attested);
    assert!(!recorded.native_per_call_identity_verified);
    assert_eq!(recorded.reservation_count, 0);

    // Query via `grant-list` and `grant-get` (verifying no new lockfile or journal entries on queries).
    let journal_path = fixture.plans_dir.join("observation-journal.jsonl");
    let journal_before_queries =
        fs::read_to_string(&journal_path).expect("observation journal after grant-record");
    assert!(journal_before_queries.contains("flow.grant-record"));

    let parent_envs = [("THREADLANE_PARENT_REQUEST_ID", "obsreq_parent_query")];
    let listed: Vec<PlanningGrantSummary> = parse_envelope(
        &run_flow_with_env(&fixture.flow_args("grant-list", &[]), &parent_envs).await,
        "grant-list after record",
        &fixture.binding_id,
    );
    assert_eq!(listed, vec![recorded.clone()]);

    let fetched: PlanningGrantSummary = parse_envelope(
        &run_flow_with_env(
            &fixture.flow_args(
                "grant-get",
                &["--grant-id".to_string(), recorded.grant_id.clone()],
            ),
            &parent_envs,
        )
        .await,
        "grant-get after record",
        &fixture.binding_id,
    );
    assert_eq!(fetched, recorded);
    assert_eq!(
        fs::read_to_string(&journal_path).expect("journal unchanged after queries"),
        journal_before_queries,
        "grant-list and grant-get must not append observation journal entries"
    );

    // Prepare a second preview, then mutate the registry before recording: stale registry must fail closed.
    let second_prepare = fixture.sample_prepare_request("req-grant-stale-registry");
    let second_prepare_path = fixture.write_request("prepare-second.json", &second_prepare);
    let second_preview: PlanningGrantPreview = parse_envelope(
        &run_flow(&fixture.flow_args(
            "grant-prepare",
            &[
                "--request".to_string(),
                second_prepare_path.display().to_string(),
            ],
        ))
        .await,
        "prepare second preview",
        &fixture.binding_id,
    );
    let registry = RegistryStore::open(&fixture.registry_dir).expect("open registry");
    registry
        .apply(
            3,
            RegistryChange::SetDefaultProfile {
                profile_id: Some(PROFILE_ID.to_string()),
            },
        )
        .expect("bump registry revision to 4");
    let stale_record = RecordPlanningGrantRequest {
        request_key: second_preview.request_key.clone(),
        expected_ledger_revision: Some(1),
        confirmed_preview_digest: second_preview.preview_digest.clone(),
        preview: second_preview.clone(),
        attestation: LocalOperatorGrantAttestation::attested(),
    };
    let stale_record_path = fixture.write_request("record-stale.json", &stale_record);
    expect_failure_code(
        &run_flow(&fixture.flow_args(
            "grant-record",
            &[
                "--request".to_string(),
                stale_record_path.display().to_string(),
                "--confirm-preview".to_string(),
                second_preview.preview_digest.clone(),
                "--operator-approved".to_string(),
            ],
        ))
        .await,
        "stale registry grant-record",
        "validation_failed",
    );

    // Revoke with stale expected_ledger_revision fails closed; rightful revoke succeeds.
    let stale_revoke = RevokePlanningGrantRequest {
        binding_id: fixture.binding_id.clone(),
        thread_uuid: THREAD_UUID.to_string(),
        grant_id: recorded.grant_id.clone(),
        expected_ledger_revision: Some(99),
        revoked_at_ms: 1_760_000_060_000,
        reason: "Stale CAS check".to_string(),
    };
    let stale_revoke_path = fixture.write_request("revoke-stale.json", &stale_revoke);
    expect_failure_code(
        &run_flow(&fixture.flow_args(
            "grant-revoke",
            &[
                "--request".to_string(),
                stale_revoke_path.display().to_string(),
            ],
        ))
        .await,
        "stale revision grant-revoke",
        "validation_failed",
    );

    let rightful_revoke = RevokePlanningGrantRequest {
        binding_id: fixture.binding_id.clone(),
        thread_uuid: THREAD_UUID.to_string(),
        grant_id: recorded.grant_id.clone(),
        expected_ledger_revision: Some(1),
        revoked_at_ms: 1_760_000_060_000,
        reason: "Operator revoked standing planning grant".to_string(),
    };
    let revoke_path = fixture.write_request("revoke-rightful.json", &rightful_revoke);
    let revoked: PlanningGrantSummary = parse_envelope(
        &run_flow(&fixture.flow_args(
            "grant-revoke",
            &["--request".to_string(), revoke_path.display().to_string()],
        ))
        .await,
        "grant-revoke rightful",
        &fixture.binding_id,
    );
    assert_eq!(revoked.status, PlanningGrantStatus::Revoked);
    assert_eq!(revoked.ledger_revision, 2);
    assert_eq!(
        revoked.revoked_reason.as_deref(),
        Some("Operator revoked standing planning grant")
    );
    assert!(!fixture.worker_marker.exists());

    fixture.shutdown().await;
}
