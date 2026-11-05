use super::*;
use serde_json::Value;
use threadlane::{Supervisor, SupervisorClient, SupervisorConfig};
use threadlane_native::coworker_workflow::CoworkerJobLockState;

fn write_gated_fake_agy_script(
    script_path: &Path,
    coder_count_file: &Path,
    planning_count_file: &Path,
    prompt_file: &Path,
    plan_response_file: &Path,
    coder_started_file: &Path,
    gate_release_file: &Path,
) {
    let script = format!(
        r#"#!/bin/sh
set -eu
trap '' PIPE
trap 'exit 0' TERM INT
CODER_COUNT_FILE="{coder_count_file}"
PLANNING_COUNT_FILE="{planning_count_file}"
PROMPT_FILE="{prompt_file}"
PLAN_RESPONSE_FILE="{plan_response_file}"
CODER_STARTED_FILE="{coder_started_file}"
GATE_RELEASE_FILE="{gate_release_file}"

MODEL=""
EFFORT=""
AGY_MODE=""
PROMPT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --model)
            MODEL="$2"
            shift 2
            ;;
        --effort)
            EFFORT="$2"
            shift 2
            ;;
        --mode)
            AGY_MODE="$2"
            shift 2
            ;;
        --print|--prompt|-p)
            PROMPT="$2"
            shift 2
            ;;
        *)
            shift 1
            ;;
    esac
done

if [ "$AGY_MODE" = "plan" ]; then
    pc=0
    if [ -f "$PLANNING_COUNT_FILE" ]; then
        pc=$(cat "$PLANNING_COUNT_FILE")
    fi
    pc=$((pc + 1))
    printf "%s" "$pc" > "$PLANNING_COUNT_FILE"
    printf '{{"event":"init","init":{{"session_id":"sess-agy-plan-%s","model":"%s"}}}}\n' "$pc" "$MODEL"
    esc=$(sed 's/\\/\\\\/g; s/"/\\"/g' "$PLAN_RESPONSE_FILE" | tr '\n' ' ')
    printf '{{"event":"result","result":{{"status":"SUCCESS","response":"%s"}}}}\n' "$esc"
    exit 0
fi

c=0
if [ -f "$CODER_COUNT_FILE" ]; then
    c=$(cat "$CODER_COUNT_FILE")
fi
c=$((c + 1))
printf "%s" "$c" > "$CODER_COUNT_FILE"

printf "%s" "$PROMPT" > "$PROMPT_FILE"
printf '{{"type":"init","session_id":"sess-agy-ctrl","model":"%s","effort":"%s"}}\n' "$MODEL" "$EFFORT"
printf "started\n" > "$CODER_STARTED_FILE"

while [ ! -f "$GATE_RELEASE_FILE" ]; do
    sleep 0.05
done

if [ -d "src" ]; then
    case "$PROMPT" in
        *CTRL_TASK_A*)
            printf "pub fn a() -> u32 {{ 100 }}\n" > "src/a.rs"
            ;;
        *)
            printf "pub fn a() -> u32 {{ 42 }}\n" > "src/a.rs"
            ;;
    esac
fi

printf '{{"type":"result","subtype":"success","session_id":"sess-agy-ctrl","model":"%s","effort":"%s"}}\n' "$MODEL" "$EFFORT" || true
printf "done\n" > "${{GATE_RELEASE_FILE}}.done" || true
exit 0
"#,
        coder_count_file = coder_count_file.display(),
        planning_count_file = planning_count_file.display(),
        prompt_file = prompt_file.display(),
        plan_response_file = plan_response_file.display(),
        coder_started_file = coder_started_file.display(),
        gate_release_file = gate_release_file.display(),
    );
    let tmp_script = script_path.with_extension("tmp");
    write_executable_script(&tmp_script, &script);
    fs::rename(&tmp_script, script_path).expect("atomically replace fake-agy script");
}

async fn run_flow_cli(
    state_dir: &Path,
    fixture: &ControllerTestFixture,
    binding_id: &str,
    thread_uuid: &str,
    subcommand_args: &[&str],
) -> Value {
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_threadlane-flow"))
        .arg("--state-dir")
        .arg(state_dir)
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--plans-dir")
        .arg(&fixture.plans_dir)
        .arg("--authority-dir")
        .arg(&fixture.authority_dir)
        .arg("--sessions-dir")
        .arg(&fixture.sessions_dir)
        .arg("--binding")
        .arg(binding_id)
        .arg("--thread")
        .arg(thread_uuid)
        .args(subcommand_args)
        .output()
        .await
        .expect("spawn threadlane-flow subprocess");
    assert!(
        output.status.success(),
        "threadlane-flow {:?} failed with status {:?}\nstdout: {}\nstderr: {}",
        subcommand_args,
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let envelope: Value =
        serde_json::from_slice(&output.stdout).expect("parse threadlane-flow stdout JSON envelope");
    assert_eq!(envelope["untrusted_data"], Value::Bool(true));
    assert_eq!(
        envelope["route"],
        Value::String("trusted_local_operator".to_string())
    );
    assert_eq!(
        envelope["binding_id"],
        Value::String(binding_id.to_string())
    );
    envelope["data"].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn cli_code_driver_ensure_reuses_live_owner_survives_default_hold_reconciles_phase1_and_cancels_cleanly(
) {
    let fixture = setup_fixture();
    let root = fs::canonicalize(fixture._tmp.path()).expect("canonicalize temp root");
    let state_dir = root.join("runtime");
    fs::create_dir_all(&state_dir).expect("create runtime state_dir");
    fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 runtime state_dir");

    let supervisor = Supervisor::start(SupervisorConfig::new(state_dir.clone()))
        .await
        .expect("start disposable supervisor");
    let supervisor_client =
        SupervisorClient::from_state_dir(&state_dir).expect("create supervisor client");
    let binding = supervisor_client
        .create_binding(THREAD_UUID_A, "code-driver-cli-test")
        .await
        .expect("create actual supervisor binding");
    let binding_id = binding.binding_id;
    let thread_uuid = binding.thread_uuid;

    let tasks = vec![PlanTask {
        id: "task-cli-a".to_string(),
        title: "Task CLI A".to_string(),
        instruction: "CTRL_TASK_A update src/a.rs".to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Hard,
        depends_on: Vec::new(),
        allowed_write_paths: vec!["src/a.rs".to_string()],
    }];

    let (job, approval_id) = create_planned_approved_and_frozen_job_for_scope(
        &fixture,
        &binding_id,
        &thread_uuid,
        "brief-cli-driver-1",
        "plan-cli-driver-1",
        tasks,
        BTreeMap::new(),
        vec!["src/".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;

    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);

    let coder_started_file = root.join("coder_started.flag");
    let gate_release_file = root.join("coder_gate_release.flag");
    write_gated_fake_agy_script(
        &fixture.fake_agy_exe,
        &fixture.agy_count_file,
        &fixture.agy_planning_count_file,
        &fixture.agy_prompt_file,
        &fixture.agy_plan_response_file,
        &coder_started_file,
        &gate_release_file,
    );

    // 1. First `code-driver ensure` in explicit diagnostic Phase 1 mode must return while fake Coder is still held on the private gate.
    let ensure_first = run_flow_cli(
        &state_dir,
        &fixture,
        &binding_id,
        &thread_uuid,
        &[
            "code-driver",
            "ensure",
            "--job-id",
            &job.job_id,
            "--execution-mode",
            "phase1_initial_dispatch_only",
        ],
    )
    .await;
    assert!(
        !gate_release_file.exists(),
        "first code-driver ensure must return while fake Coder is held on gate"
    );
    assert_eq!(ensure_first["disposition"], "spawned_new_owner");
    assert_eq!(ensure_first["status"]["owner_state"], "live_verified_owner");
    assert_eq!(
        ensure_first["status"]["execution_mode"],
        "phase1_initial_dispatch_only"
    );
    // Readiness proves physical ownership; the logical lease is checked below once the Coder runs.
    let owner_instance_id = ensure_first["status"]["verified_owner_summary"]["instance_id"]
        .as_str()
        .expect("verified_owner_summary.instance_id string")
        .to_string();
    assert!(owner_instance_id.starts_with("cwo_"));

    let started_deadline = Instant::now() + Duration::from_secs(5);
    while !coder_started_file.exists() {
        assert!(
            Instant::now() < started_deadline,
            "timed out waiting for gated fake Coder to start"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    // Status while held shows the actual owned instance and the running task.
    // The owner can answer status while the first pipeline step is still pending,
    // so wait for the actual step report snapshot to become visible.
    let readiness_deadline = Instant::now() + Duration::from_secs(5);
    let status_running = loop {
        let status = run_flow_cli(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &["code-driver", "status", "--job-id", &job.job_id],
        )
        .await;
        if !status["step_report"].is_null() && status["step_report"]["active_running_count"] == 1 {
            break status;
        }
        assert!(
            Instant::now() < readiness_deadline,
            "timed out after 5s waiting for non-null step_report with active_running_count == 1; last status: {status}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(status_running["owner_state"], "live_verified_owner");
    assert_eq!(
        status_running["verified_owner_summary"]["instance_id"],
        owner_instance_id
    );
    assert_eq!(status_running["lease_active"], true);
    assert_eq!(status_running["host_lock_held"], true);
    assert_eq!(status_running["overall_status"], "running_tasks");
    assert_eq!(status_running["step_report"]["active_running_count"], 1);
    let running_host_task_count = status_running["tasks"]
        .as_array()
        .expect("status_running.tasks array")
        .iter()
        .filter(|task| task["host_status"] == "running")
        .count();
    assert_eq!(running_host_task_count, 1);
    assert_eq!(status_running["tasks"][0]["task_id"], "task-cli-a");
    assert_eq!(status_running["tasks"][0]["stage"], "running");
    assert_eq!(status_running["tasks"][0]["host_status"], "running");

    // 2. Concurrent duplicate `ensure` calls must reuse that same live owner instance and keep a single provider call.
    let ensure_args = ["code-driver", "ensure", "--job-id", &job.job_id];
    let (dup_a, dup_b) = tokio::join!(
        run_flow_cli(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &ensure_args,
        ),
        run_flow_cli(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &ensure_args,
        ),
    );
    for dup in [&dup_a, &dup_b] {
        assert_eq!(dup["disposition"], "reused_live_owner");
        assert_eq!(dup["status"]["owner_state"], "live_verified_owner");
        assert_eq!(
            dup["status"]["verified_owner_summary"]["instance_id"],
            owner_instance_id
        );
        assert_eq!(dup["status"]["overall_status"], "running_tasks");
    }
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    // 3. After 6.6s under the default request, the same owner remains live (no implicit 6.4s cutoff).
    tokio::time::sleep(Duration::from_millis(6_600)).await;
    assert!(
        !gate_release_file.exists(),
        "gate must still be closed after 6.6s hold check"
    );
    let status_after_hold = run_flow_cli(
        &state_dir,
        &fixture,
        &binding_id,
        &thread_uuid,
        &["code-driver", "status", "--job-id", &job.job_id],
    )
    .await;
    assert_eq!(status_after_hold["owner_state"], "live_verified_owner");
    assert_eq!(
        status_after_hold["verified_owner_summary"]["instance_id"],
        owner_instance_id
    );
    assert_eq!(status_after_hold["lease_active"], true);
    assert_eq!(status_after_hold["host_lock_held"], true);
    assert_eq!(status_after_hold["overall_status"], "running_tasks");
    assert_eq!(status_after_hold["tasks"][0]["task_id"], "task-cli-a");
    assert_eq!(status_after_hold["tasks"][0]["stage"], "running");
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    // 4. Release the gate and await actual Phase 1 AwaitingChecksOrReview (and not Completed).
    fs::write(&gate_release_file, "release\n").expect("write gate release file");

    let reconcile_deadline = Instant::now() + Duration::from_secs(10);
    let status_awaiting = loop {
        let current = run_flow_cli(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &["code-driver", "status", "--job-id", &job.job_id],
        )
        .await;
        if current["overall_status"] == "awaiting_checks_or_review" {
            break current;
        }
        assert!(
            Instant::now() < reconcile_deadline,
            "timed out waiting for Phase 1 awaiting_checks_or_review; last status: {current}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    assert_eq!(
        status_awaiting["overall_status"],
        "awaiting_checks_or_review"
    );
    assert_ne!(status_awaiting["job_phase"], "completed");
    assert_eq!(status_awaiting["job_phase"], "executing");
    assert_eq!(
        status_awaiting["pipeline_boundary"],
        "phase1_initial_dispatch_and_reconcile_only"
    );
    assert_eq!(status_awaiting["owner_state"], "live_verified_owner");
    assert_eq!(
        status_awaiting["verified_owner_summary"]["instance_id"],
        owner_instance_id
    );
    assert_eq!(status_awaiting["tasks"][0]["task_id"], "task-cli-a");
    assert_eq!(status_awaiting["tasks"][0]["stage"], "ready_for_review");
    assert_eq!(
        status_awaiting["tasks"][0]["observed_model"],
        "gemini-4-argon-high"
    );
    assert_eq!(status_awaiting["tasks"][0]["observed_effort"], "high");
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    // 5. Cancel/stop the exact owned driver, shutting down its disposable host and releasing ownership.
    let cancel_report = run_flow_cli(
        &state_dir,
        &fixture,
        &binding_id,
        &thread_uuid,
        &[
            "code-driver",
            "cancel",
            "--job-id",
            &job.job_id,
            "--reason",
            "cli_test_cleanup",
        ],
    )
    .await;
    assert_eq!(
        cancel_report["durable_stop_recorded_before_host_cancel"],
        true
    );
    assert_eq!(cancel_report["notified_live_owner"], true);
    assert_eq!(cancel_report["status"]["stop_requested"], true);
    assert_eq!(cancel_report["status"]["overall_status"], "stopped");
    assert_eq!(cancel_report["status"]["lease_active"], false);
    assert!(cancel_report["status"]["verified_owner_summary"].is_null());
    assert_eq!(cancel_report["status"]["host_lock_held"], false);
    assert_ne!(cancel_report["status"]["job_phase"], "completed");

    let actual_driver_ownership_store = CoworkerJobOwnershipStore::new(
        threadlane_native::coworker_driver::coworker_driver_dir(&fixture.plans_dir),
    )
    .expect("open actual CLI code-driver ownership store");
    let ownership_key = CoworkerJobOwnershipKey::new(&binding_id, &thread_uuid, &job.job_id)
        .expect("construct ownership key");
    assert!(
        !matches!(
            actual_driver_ownership_store
                .inspect_job_lock(&ownership_key)
                .expect("inspect actual driver job lock after cancel"),
            CoworkerJobLockState::HeldByActiveProcess { .. }
        ),
        "physical job lock in coworker_driver_dir(plans_dir) must not be held after cancel"
    );
    assert!(
        !threadlane_host::is_host_lock_held(&fixture.sessions_dir.join(&approval_id))
            .expect("inspect host lock after cancel"),
        "disposable host lock must not be held after cancel"
    );

    supervisor.shutdown().await;
}

async fn run_flow_cli_expect_err(
    state_dir: &Path,
    fixture: &ControllerTestFixture,
    binding_id: &str,
    thread_uuid: &str,
    subcommand_args: &[&str],
) -> Value {
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_threadlane-flow"))
        .arg("--state-dir")
        .arg(state_dir)
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--plans-dir")
        .arg(&fixture.plans_dir)
        .arg("--authority-dir")
        .arg(&fixture.authority_dir)
        .arg("--sessions-dir")
        .arg(&fixture.sessions_dir)
        .arg("--binding")
        .arg(binding_id)
        .arg("--thread")
        .arg(thread_uuid)
        .args(subcommand_args)
        .output()
        .await
        .expect("spawn threadlane-flow subprocess");
    assert!(
        !output.status.success(),
        "expected threadlane-flow {:?} to fail, but it succeeded with stdout: {}",
        subcommand_args,
        String::from_utf8_lossy(&output.stdout),
    );
    assert!(
        output.stdout.is_empty(),
        "failed threadlane-flow command must not emit stdout result envelope: {}",
        String::from_utf8_lossy(&output.stdout),
    );
    serde_json::from_slice(&output.stderr).unwrap_or_else(|err| {
        panic!(
            "failed to parse stderr OperatorCliError JSON ({err}): {}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn write_private_json_file(path: &Path, value: &Value) {
    let bytes = serde_json::to_vec_pretty(value).expect("serialize private JSON file");
    fs::write(path, bytes).expect("write private JSON file");
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 private JSON file");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn cli_code_driver_cancel_active_held_job_records_durable_stop_releases_locks_and_isolates_sibling_and_scope(
) {
    let fixture = setup_fixture();
    let root = fs::canonicalize(fixture._tmp.path()).expect("canonicalize temp root");
    let state_dir = root.join("runtime");
    fs::create_dir_all(&state_dir).expect("create runtime state_dir");
    fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 runtime state_dir");

    let supervisor = Supervisor::start(SupervisorConfig::new(state_dir.clone()))
        .await
        .expect("start disposable supervisor");
    let supervisor_client =
        SupervisorClient::from_state_dir(&state_dir).expect("create supervisor client");
    let binding_a = supervisor_client
        .create_binding(THREAD_UUID_A, "code-driver-cli-iso-a")
        .await
        .expect("create actual supervisor binding A");
    let binding_b = supervisor_client
        .create_binding(THREAD_UUID_B, "code-driver-cli-iso-b")
        .await
        .expect("create actual supervisor binding B");
    let binding_a_id = binding_a.binding_id;
    let thread_a_uuid = binding_a.thread_uuid;
    let binding_b_id = binding_b.binding_id;
    let thread_b_uuid = binding_b.thread_uuid;

    let tasks_a = vec![PlanTask {
        id: "task-cancel-a".to_string(),
        title: "Task Cancel A".to_string(),
        instruction: "CTRL_TASK_A update src/a.rs".to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Hard,
        depends_on: Vec::new(),
        allowed_write_paths: vec!["src/a.rs".to_string()],
    }];
    let (job_a, approval_a_id) = create_planned_approved_and_frozen_job_for_scope(
        &fixture,
        &binding_a_id,
        &thread_a_uuid,
        "brief-cli-cancel-a",
        "plan-cli-cancel-a",
        tasks_a,
        BTreeMap::new(),
        vec!["src/".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;

    let tasks_sibling = vec![PlanTask {
        id: "task-sibling-b".to_string(),
        title: "Task Sibling B".to_string(),
        instruction: "CTRL_TASK_B update src/b.rs".to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Hard,
        depends_on: Vec::new(),
        allowed_write_paths: vec!["src/b.rs".to_string()],
    }];
    let (job_sibling, sibling_approval_id) = create_planned_approved_and_frozen_job_for_scope(
        &fixture,
        &binding_a_id,
        &thread_a_uuid,
        "brief-cli-sibling-b",
        "plan-cli-sibling-b",
        tasks_sibling,
        BTreeMap::new(),
        vec!["src/".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;

    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 2);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);

    let sibling_job_before = fixture
        .workflow_store
        .read_binding_ledger(&binding_a_id)
        .expect("read binding A ledger before cancel")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job_sibling.job_id)
        .expect("find sibling job before cancel");
    let sibling_runtime_before = fixture
        .policy_store
        .get_job_runtime_readonly(&binding_a_id, &job_sibling.job_id)
        .expect("read sibling runtime before cancel")
        .expect("sibling runtime exists before cancel");
    let sibling_approval_before = fixture
        .authority
        .get_for_operator(&sibling_approval_id)
        .expect("read sibling approval before cancel");

    let coder_started_file = root.join("coder_cancel_started.flag");
    let gate_release_file = root.join("coder_cancel_gate_release.flag");
    write_gated_fake_agy_script(
        &fixture.fake_agy_exe,
        &fixture.agy_count_file,
        &fixture.agy_planning_count_file,
        &fixture.agy_prompt_file,
        &fixture.agy_plan_response_file,
        &coder_started_file,
        &gate_release_file,
    );

    // 1. Wrong binding, wrong thread, and mismatched request identity must fail before any launch or budget charge.
    let wrong_binding_err = run_flow_cli_expect_err(
        &state_dir,
        &fixture,
        &binding_b_id,
        &thread_b_uuid,
        &["code-driver", "ensure", "--job-id", &job_a.job_id],
    )
    .await;
    assert_eq!(wrong_binding_err["code"], "security_violation");

    let wrong_thread_err = run_flow_cli_expect_err(
        &state_dir,
        &fixture,
        &binding_a_id,
        THREAD_UUID_B,
        &["code-driver", "ensure", "--job-id", &job_a.job_id],
    )
    .await;
    assert_eq!(wrong_thread_err["code"], "thread_uuid_mismatch");

    let bad_scope_req_path = root.join("bad_scope_driver_request.json");
    write_private_json_file(
        &bad_scope_req_path,
        &serde_json::json!({
            "schema_version": 1,
            "binding_id": binding_b_id,
            "thread_uuid": thread_a_uuid,
            "job_id": job_a.job_id,
        }),
    );
    let typed_scope_err = threadlane_native::CoworkerDriverRequest::load_from_private_file(
        &bad_scope_req_path,
        &binding_a_id,
        &thread_a_uuid,
    )
    .expect_err("typed CoworkerDriverRequest loader must reject mismatched binding_id");
    assert_eq!(
        typed_scope_err.error_code(),
        NativeErrorCode::SecurityViolation
    );

    let cli_scope_err = run_flow_cli_expect_err(
        &state_dir,
        &fixture,
        &binding_a_id,
        &thread_a_uuid,
        &[
            "code-driver",
            "ensure",
            "--request",
            bad_scope_req_path.to_str().expect("utf8 path"),
        ],
    )
    .await;
    assert_eq!(cli_scope_err["code"], "scope_mismatch");

    let valid_req_path = root.join("valid_scope_driver_request.json");
    write_private_json_file(
        &valid_req_path,
        &serde_json::json!({
            "schema_version": 1,
            "binding_id": binding_a_id,
            "thread_uuid": thread_a_uuid,
            "job_id": job_a.job_id,
        }),
    );
    let conflicting_job_id_err = run_flow_cli_expect_err(
        &state_dir,
        &fixture,
        &binding_a_id,
        &thread_a_uuid,
        &[
            "code-driver",
            "ensure",
            "--request",
            valid_req_path.to_str().expect("utf8 path"),
            "--job-id",
            &job_sibling.job_id,
        ],
    )
    .await;
    assert_eq!(conflicting_job_id_err["code"], "invalid_input");

    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);
    let runtime_a_pre = fixture
        .policy_store
        .get_job_runtime_readonly(&binding_a_id, &job_a.job_id)
        .expect("read job_a runtime pre-launch")
        .expect("job_a runtime exists");
    assert_eq!(runtime_a_pre.total_task_attempts_reserved, 0);

    // 2. Launch only `job_a` in explicit diagnostic Phase 1 mode and wait until its fake Coder is held on the private gate.
    let ensure_a = run_flow_cli(
        &state_dir,
        &fixture,
        &binding_a_id,
        &thread_a_uuid,
        &[
            "code-driver",
            "ensure",
            "--job-id",
            &job_a.job_id,
            "--execution-mode",
            "phase1_initial_dispatch_only",
        ],
    )
    .await;
    assert_eq!(ensure_a["disposition"], "spawned_new_owner");
    assert_eq!(ensure_a["status"]["owner_state"], "live_verified_owner");
    assert_eq!(
        ensure_a["status"]["execution_mode"],
        "phase1_initial_dispatch_only"
    );

    let started_deadline = Instant::now() + Duration::from_secs(5);
    while !coder_started_file.exists() {
        assert!(
            Instant::now() < started_deadline,
            "timed out waiting for job_a fake Coder to start"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    let mut status_a_running = run_flow_cli(
        &state_dir,
        &fixture,
        &binding_a_id,
        &thread_a_uuid,
        &["code-driver", "status", "--job-id", &job_a.job_id],
    )
    .await;
    assert_eq!(status_a_running["overall_status"], "running_tasks");

    // The live owner's cached step report can reach `running_tasks` after host dispatch
    // confirmation but before the claimed worker's owned worktree snapshot is recorded.
    // The fake started marker does not make that cached report current, so wait for the
    // owner's next status step to expose the concrete task_worktree_id.
    let worktree_id_deadline = Instant::now() + Duration::from_secs(5);
    let task_worktree_id = loop {
        if let Some(task_worktree_id) = status_a_running["tasks"][0]["task_worktree_id"]
            .as_str()
            .filter(|id| !id.is_empty())
        {
            break task_worktree_id.to_string();
        }
        assert!(
            Instant::now() < worktree_id_deadline,
            "timed out after 5s waiting for running task_worktree_id; last status: {status_a_running}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        status_a_running = run_flow_cli(
            &state_dir,
            &fixture,
            &binding_a_id,
            &thread_a_uuid,
            &["code-driver", "status", "--job-id", &job_a.job_id],
        )
        .await;
        assert_eq!(status_a_running["overall_status"], "running_tasks");
    };

    // 3. Cancel active held `job_a` and verify durable stop precedes narrow host cancellation and releases all locks.
    let cancel_a = run_flow_cli(
        &state_dir,
        &fixture,
        &binding_a_id,
        &thread_a_uuid,
        &[
            "code-driver",
            "cancel",
            "--job-id",
            &job_a.job_id,
            "--reason",
            "operator_cancel_active_held_job",
        ],
    )
    .await;
    assert_eq!(cancel_a["durable_stop_recorded_before_host_cancel"], true);
    assert_eq!(cancel_a["notified_live_owner"], true);
    assert_eq!(cancel_a["narrow_host_cancellation_attempted"], true);
    assert_eq!(
        cancel_a["cancelled_host_tasks"]
            .as_array()
            .expect("cancelled_host_tasks array")
            .len(),
        1
    );
    assert_eq!(
        cancel_a["cancelled_host_tasks"][0]["task_id"],
        "task-cancel-a"
    );
    assert_eq!(cancel_a["status"]["stop_requested"], true);
    assert_eq!(cancel_a["status"]["overall_status"], "stopped");
    assert_eq!(cancel_a["status"]["lease_active"], false);
    assert!(cancel_a["status"]["verified_owner_summary"].is_null());
    assert_eq!(cancel_a["status"]["host_lock_held"], false);

    let actual_driver_ownership_store = CoworkerJobOwnershipStore::new(
        threadlane_native::coworker_driver::coworker_driver_dir(&fixture.plans_dir),
    )
    .expect("open actual driver ownership store");
    let ownership_key_a =
        CoworkerJobOwnershipKey::new(&binding_a_id, &thread_a_uuid, &job_a.job_id)
            .expect("construct ownership_key_a");
    assert!(
        !matches!(
            actual_driver_ownership_store
                .inspect_job_lock(&ownership_key_a)
                .expect("inspect driver lock after cancel"),
            CoworkerJobLockState::HeldByActiveProcess { .. }
        ),
        "driver owner lock must be released after cancel"
    );
    assert!(
        !threadlane_host::is_host_lock_held(&fixture.sessions_dir.join(&approval_a_id))
            .expect("inspect host lock after cancel"),
        "disposable host lock must be released after cancel"
    );
    let work_area_root = fixture
        .sessions_dir
        .join(&approval_a_id)
        .join(threadlane_host::WORK_AREA_SUBDIR);
    let task_manifest =
        threadlane_workers::load_and_verify_manifest(&work_area_root, &task_worktree_id)
            .expect("load task worktree manifest after cancel");
    threadlane_workers::ensure_worktree_job_inactive_readonly(&task_manifest)
        .expect("worker worktree job lock must be released after cancel");

    // 4. `ensure` after durable stop must not restart work or increment provider/attempt counters.
    let ensure_after_stop = run_flow_cli(
        &state_dir,
        &fixture,
        &binding_a_id,
        &thread_a_uuid,
        &["code-driver", "ensure", "--job-id", &job_a.job_id],
    )
    .await;
    assert_eq!(
        ensure_after_stop["disposition"],
        "already_quiescent_verified"
    );
    assert_eq!(ensure_after_stop["status"]["stop_requested"], true);
    assert_eq!(ensure_after_stop["status"]["overall_status"], "stopped");
    assert_eq!(ensure_after_stop["status"]["lease_active"], false);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
    let runtime_a_post = fixture
        .policy_store
        .get_job_runtime_readonly(&binding_a_id, &job_a.job_id)
        .expect("read job_a runtime after post-stop ensure")
        .expect("job_a runtime exists");
    assert_eq!(runtime_a_post.total_task_attempts_reserved, 1);

    // 5. Sibling job's CoworkerJobRecord, runtime, envelope, and approval remain completely unchanged.
    let sibling_job_after = fixture
        .workflow_store
        .read_binding_ledger(&binding_a_id)
        .expect("read binding A ledger after cancel")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job_sibling.job_id)
        .expect("find sibling job after cancel");
    let sibling_runtime_after = fixture
        .policy_store
        .get_job_runtime_readonly(&binding_a_id, &job_sibling.job_id)
        .expect("read sibling runtime after cancel")
        .expect("sibling runtime exists after cancel");
    let sibling_approval_after = fixture
        .authority
        .get_for_operator(&sibling_approval_id)
        .expect("read sibling approval after cancel");

    assert_eq!(sibling_job_after, sibling_job_before);
    assert_eq!(sibling_runtime_after, sibling_runtime_before);
    assert_eq!(
        sibling_runtime_after.envelope,
        sibling_runtime_before.envelope
    );
    assert_eq!(sibling_approval_after, sibling_approval_before);

    fs::write(&gate_release_file, "release\n").expect("release gate file");
    supervisor.shutdown().await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn cli_code_driver_owner_crash_reports_stale_evidence_refuses_blind_replay_and_rejects_copied_metadata(
) {
    use std::os::unix::fs::MetadataExt;

    let fixture = setup_fixture();
    let root = fs::canonicalize(fixture._tmp.path()).expect("canonicalize temp root");
    let state_dir = root.join("runtime");
    fs::create_dir_all(&state_dir).expect("create runtime state_dir");
    fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 runtime state_dir");

    let supervisor = Supervisor::start(SupervisorConfig::new(state_dir.clone()))
        .await
        .expect("start disposable supervisor");
    let supervisor_client =
        SupervisorClient::from_state_dir(&state_dir).expect("create supervisor client");
    let binding = supervisor_client
        .create_binding(THREAD_UUID_A, "code-driver-cli-crash-test")
        .await
        .expect("create actual supervisor binding");
    let binding_id = binding.binding_id;
    let thread_uuid = binding.thread_uuid;

    let tasks = vec![PlanTask {
        id: "task-crash-a".to_string(),
        title: "Task Crash A".to_string(),
        instruction: "CTRL_TASK_A update src/a.rs".to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Hard,
        depends_on: Vec::new(),
        allowed_write_paths: vec!["src/a.rs".to_string()],
    }];
    let (job, approval_id) = create_planned_approved_and_frozen_job_for_scope(
        &fixture,
        &binding_id,
        &thread_uuid,
        "brief-cli-crash-1",
        "plan-cli-crash-1",
        tasks,
        BTreeMap::new(),
        vec!["src/".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;

    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);

    let coder_started_file = root.join("coder_crash_started.flag");
    let gate_release_file = root.join("coder_crash_gate_release.flag");
    write_gated_fake_agy_script(
        &fixture.fake_agy_exe,
        &fixture.agy_count_file,
        &fixture.agy_planning_count_file,
        &fixture.agy_prompt_file,
        &fixture.agy_plan_response_file,
        &coder_started_file,
        &gate_release_file,
    );

    let host_config = ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &approval_id,
    );
    let host_handle = ApprovedHost::start_fresh_only(host_config)
        .await
        .expect("start fresh approved host before code-driver ensure");

    let ensure_first = run_flow_cli(
        &state_dir,
        &fixture,
        &binding_id,
        &thread_uuid,
        &[
            "code-driver",
            "ensure",
            "--job-id",
            &job.job_id,
            "--execution-mode",
            "phase1_initial_dispatch_only",
        ],
    )
    .await;
    assert_eq!(ensure_first["disposition"], "spawned_new_owner");
    assert_eq!(ensure_first["status"]["owner_state"], "live_verified_owner");
    assert_eq!(
        ensure_first["status"]["execution_mode"],
        "phase1_initial_dispatch_only"
    );
    assert_eq!(
        ensure_first["status"]["host_mode"],
        "reused_verified_ready_host"
    );

    let started_deadline = Instant::now() + Duration::from_secs(5);
    while !coder_started_file.exists() {
        assert!(
            Instant::now() < started_deadline,
            "timed out waiting for gated fake Coder to start before crash"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        !gate_release_file.exists(),
        "controlled fake gate must remain closed before owner crash"
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    let running_deadline = Instant::now() + Duration::from_secs(5);
    let status_before_crash = loop {
        let current = run_flow_cli(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &["code-driver", "status", "--job-id", &job.job_id],
        )
        .await;
        if current["tasks"][0]["host_status"] == "running"
            && current["tasks"][0]["stage"] == "running"
            && current["step_report"]["active_running_count"] == 1
        {
            break current;
        }
        assert!(
            Instant::now() < running_deadline,
            "timed out waiting for status_before_crash to observe running task; last: {current}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(status_before_crash["owner_state"], "live_verified_owner");
    assert_eq!(status_before_crash["overall_status"], "running_tasks");
    assert_eq!(
        status_before_crash["step_report"]["active_running_count"],
        1
    );

    let verified_summary = &status_before_crash["verified_owner_summary"];
    let owner_instance_id = verified_summary["instance_id"]
        .as_str()
        .expect("verified_owner_summary.instance_id")
        .to_string();
    let owner_pid = u32::try_from(
        verified_summary["pid_hint"]
            .as_u64()
            .expect("verified_owner_summary.pid_hint"),
    )
    .expect("pid_hint fits u32");

    let driver_dir = threadlane_native::coworker_driver::coworker_driver_dir(&fixture.plans_dir);
    let actual_driver_ownership_store =
        CoworkerJobOwnershipStore::new(&driver_dir).expect("open actual driver ownership store");
    let ownership_key = CoworkerJobOwnershipKey::new(&binding_id, &thread_uuid, &job.job_id)
        .expect("construct ownership key");

    match actual_driver_ownership_store
        .inspect_job_lock(&ownership_key)
        .expect("inspect live job lock before crash")
    {
        CoworkerJobLockState::HeldByActiveProcess {
            recorded_summary: Some(summary),
            ..
        } => {
            assert_eq!(summary.instance_id, owner_instance_id);
            assert_eq!(summary.pid_hint, owner_pid);
        }
        other => panic!("expected HeldByActiveProcess before crash, got {other:?}"),
    }

    // Verify the real sealed TaskRunReceipt dispatch mapping (`task_id -> run_id, worker_id`)
    // and prove completion uncertainty while the fake Coder is held on the closed gate.
    let session_dir = fixture.sessions_dir.join(&approval_id);
    let approval_record = fixture
        .authority
        .get_for_operator(&approval_id)
        .expect("load approval record for crash test");
    let host_manifest =
        threadlane_host::read_host_manifest_optional(&session_dir, &approval_record)
            .expect("read host manifest before crash")
            .expect("host manifest must exist while Ready");
    assert_eq!(host_manifest.phase, HostPhase::Ready);
    let session_binding_id = host_manifest
        .session_binding_id
        .as_deref()
        .expect("Ready host manifest must record session_binding_id")
        .to_string();

    let sealed_receipt = threadlane_host::read_task_receipt(
        &session_dir,
        &approval_record,
        &session_binding_id,
        "task-crash-a",
    )
    .expect("read task receipt while Coder is held")
    .expect("sealed TaskRunReceipt dispatch mapping must exist once worker is running");
    assert_eq!(sealed_receipt.task_id, "task-crash-a");
    assert_eq!(
        sealed_receipt.task_digest,
        approval_record.tasks[0].task_digest
    );
    assert_eq!(
        Some(sealed_receipt.command_key.as_str()),
        approval_record.tasks[0].command_key.as_deref()
    );
    assert!(!sealed_receipt.run_id.is_empty());
    assert!(!sealed_receipt.worker_id.is_empty());
    assert!(sealed_receipt.recorded_at_ms > 0);
    assert_eq!(
        status_before_crash["tasks"][0]["run_id"].as_str(),
        Some(sealed_receipt.run_id.as_str())
    );
    assert_eq!(
        status_before_crash["tasks"][0]["worker_id"].as_str(),
        Some(sealed_receipt.worker_id.as_str())
    );

    let runtime_before_crash = fixture
        .policy_store
        .get_job_runtime_readonly(&binding_id, &job.job_id)
        .expect("read runtime before crash")
        .expect("runtime exists before crash");
    assert!(
        runtime_before_crash
            .get_attempt_artifacts("task-crash-a", 1)
            .is_none(),
        "no completed TaskAttemptExecutionArtifacts may exist while fake Coder is held on gate"
    );
    assert_eq!(runtime_before_crash.total_task_attempts_reserved, 1);
    assert_eq!(runtime_before_crash.unresolved_effects().len(), 1);

    let job_before_crash = fixture
        .workflow_store
        .read_binding_ledger(&binding_id)
        .expect("read binding ledger before crash")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job.job_id)
        .expect("find job before crash");
    assert_eq!(job_before_crash.tasks[0].stage, CoworkerTaskStage::Running);
    assert!(job_before_crash.tasks[0].candidates.is_empty());
    let attempt_before_crash = job_before_crash.tasks[0]
        .attempts
        .last()
        .expect("running task has attempt 1 record");
    assert_eq!(
        attempt_before_crash.run_id.as_deref(),
        Some(sealed_receipt.run_id.as_str())
    );
    assert_eq!(
        attempt_before_crash.worker_id.as_deref(),
        Some(sealed_receipt.worker_id.as_str())
    );
    assert!(
        attempt_before_crash.report_artifact_id.is_none()
            && attempt_before_crash.head_commit.is_none()
            && attempt_before_crash.tree_sha.is_none(),
        "no completed report promotion or commit artifact may exist while fake Coder is held on gate"
    );

    // Validate Linux `/proc/<owner_pid>` identity before signalling ONLY that owned PID.
    assert!(owner_pid > 1 && owner_pid != std::process::id());
    let proc_dir = PathBuf::from(format!("/proc/{owner_pid}"));
    let proc_uid = fs::metadata(&proc_dir)
        .expect("stat /proc/<owner_pid>")
        .uid();
    let self_uid = fs::metadata("/proc/self")
        .expect("stat /proc/self process directory")
        .uid();
    assert_eq!(
        self_uid,
        rustix::process::geteuid().as_raw(),
        "/proc/self resolved directory UID must match current effective UID"
    );
    assert_eq!(proc_uid, self_uid, "owner PID must belong to current UID");

    let proc_exe = fs::read_link(proc_dir.join("exe")).expect("read /proc/<owner_pid>/exe");
    let expected_flow_exe = fs::canonicalize(env!("CARGO_BIN_EXE_threadlane-flow"))
        .expect("canonicalize threadlane-flow binary");
    assert_eq!(
        proc_exe, expected_flow_exe,
        "owner PID executable must match disposable threadlane-flow binary"
    );

    let cmdline_raw = fs::read(proc_dir.join("cmdline")).expect("read /proc/<owner_pid>/cmdline");
    let argv: Vec<String> = cmdline_raw
        .split(|&b| b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8_lossy(s).into_owned())
        .collect();
    assert!(
        argv.windows(2).any(|w| w == ["code-driver", "run-owner"]),
        "owner argv must contain `code-driver run-owner`: {argv:?}"
    );
    assert!(
        argv.windows(2).any(|w| w == ["--job-id", &job.job_id]),
        "owner argv must contain `--job-id {}`: {argv:?}",
        job.job_id
    );
    assert!(
        argv.windows(2).any(|w| w == ["--binding", &binding_id]),
        "owner argv must contain `--binding {binding_id}`: {argv:?}"
    );
    assert!(
        argv.windows(2).any(|w| w == ["--thread", &thread_uuid]),
        "owner argv must contain `--thread {thread_uuid}`: {argv:?}"
    );

    let typed_pid = rustix::process::Pid::from_raw(i32::try_from(owner_pid).expect("i32 pid"))
        .expect("non-zero Pid");
    rustix::process::kill_process(typed_pid, rustix::process::Signal::KILL)
        .expect("send SIGKILL to verified run-owner PID");

    let crash_unlock_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if matches!(
            actual_driver_ownership_store
                .inspect_job_lock(&ownership_key)
                .expect("inspect job lock after SIGKILL"),
            CoworkerJobLockState::UnlockedStaleFile { .. }
        ) {
            break;
        }
        assert!(
            Instant::now() < crash_unlock_deadline,
            "timed out waiting for kernel to release crashed owner's FD lock"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(
        !gate_release_file.exists(),
        "controlled fake gate must still be closed immediately after owner crash"
    );
    let receipt_after_crash = threadlane_host::read_task_receipt(
        &session_dir,
        &approval_record,
        &session_binding_id,
        "task-crash-a",
    )
    .expect("read task receipt immediately after owner crash")
    .expect("sealed TaskRunReceipt dispatch mapping must survive owner crash");
    assert_eq!(receipt_after_crash, sealed_receipt);

    // Status must report stale physical owner evidence while preserving the surviving task->run/worker mapping.
    let status_after_crash = run_flow_cli(
        &state_dir,
        &fixture,
        &binding_id,
        &thread_uuid,
        &["code-driver", "status", "--job-id", &job.job_id],
    )
    .await;
    assert_eq!(
        status_after_crash["owner_state"],
        "stale_crash_artifact_detected"
    );
    assert!(status_after_crash["verified_owner_summary"].is_null());
    assert_eq!(status_after_crash["lease_active"], true);
    assert_eq!(status_after_crash["host_lock_held"], true);
    assert_eq!(status_after_crash["blocked_on_unknown_effect"], true);
    assert_eq!(status_after_crash["overall_status"], "running_tasks");
    assert_eq!(
        status_after_crash["tasks"][0]["run_id"].as_str(),
        Some(sealed_receipt.run_id.as_str())
    );
    assert_eq!(
        status_after_crash["tasks"][0]["worker_id"].as_str(),
        Some(sealed_receipt.worker_id.as_str())
    );

    // Copied metadata or a forged instance_id in persisted driver state without a held FD lock never grants ownership.
    for entry in fs::read_dir(&driver_dir).expect("read driver_dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|s| s.to_str()) == Some("json") {
            let mut state_val: Value =
                serde_json::from_slice(&fs::read(&path).expect("read driver state json"))
                    .expect("parse driver state json");
            state_val["owner_instance_id"] = Value::String(format!("cwo_{}", "f".repeat(64)));
            state_val["owner_active"] = Value::Bool(true);
            write_private_json_file(&path, &state_val);
        }
    }
    let status_forged_meta = run_flow_cli(
        &state_dir,
        &fixture,
        &binding_id,
        &thread_uuid,
        &["code-driver", "status", "--job-id", &job.job_id],
    )
    .await;
    assert_eq!(
        status_forged_meta["owner_state"],
        "stale_crash_artifact_detected"
    );
    assert!(status_forged_meta["verified_owner_summary"].is_null());

    // Reconcile while the controlled fake gate is still closed:
    // - Reconciles using the surviving sealed TaskRunReceipt (`run_id` / `worker_id`)
    // - Makes no new provider call and charges no new attempt/role budget
    // - Does NOT invent completed inference while the worker is still held on the closed gate
    assert!(
        !gate_release_file.exists(),
        "controlled fake gate must still be closed during post-crash reconcile"
    );
    let reconcile_while_held = run_flow_cli(
        &state_dir,
        &fixture,
        &binding_id,
        &thread_uuid,
        &["code-driver", "reconcile", "--job-id", &job.job_id],
    )
    .await;
    assert_eq!(reconcile_while_held["owner_state"], "no_active_owner");
    assert_eq!(reconcile_while_held["lease_active"], false);
    assert_eq!(
        reconcile_while_held["host_mode"],
        "reused_verified_ready_host"
    );
    assert_eq!(reconcile_while_held["overall_status"], "running_tasks");
    assert_eq!(
        reconcile_while_held["step_report"]["dispatched_task_ids"],
        serde_json::json!([])
    );
    assert_eq!(
        reconcile_while_held["step_report"]["reconciled_ready_for_review_task_ids"],
        serde_json::json!([])
    );
    assert_eq!(
        reconcile_while_held["step_report"]["active_running_count"],
        1
    );
    assert_eq!(reconcile_while_held["tasks"][0]["stage"], "running");
    assert_eq!(reconcile_while_held["tasks"][0]["host_status"], "running");
    assert_eq!(
        reconcile_while_held["tasks"][0]["run_id"].as_str(),
        Some(sealed_receipt.run_id.as_str())
    );
    assert_eq!(
        reconcile_while_held["tasks"][0]["worker_id"].as_str(),
        Some(sealed_receipt.worker_id.as_str())
    );

    assert_eq!(
        read_invocation_count(&fixture.agy_count_file),
        1,
        "reconcile after crash must reuse surviving dispatch mapping without calling provider again"
    );
    let runtime_after_held_reconcile = fixture
        .policy_store
        .get_job_runtime_readonly(&binding_id, &job.job_id)
        .expect("read runtime after held reconcile")
        .expect("runtime exists after held reconcile");
    assert_eq!(runtime_after_held_reconcile.total_task_attempts_reserved, 1);
    assert_eq!(
        runtime_after_held_reconcile
            .role_usage
            .get(&WorkflowRole::Coder)
            .expect("Coder usage")
            .calls_reserved,
        1
    );
    assert!(
        runtime_after_held_reconcile
            .get_attempt_artifacts("task-crash-a", 1)
            .is_none(),
        "reconcile while gate is closed must not invent completed TaskAttemptExecutionArtifacts"
    );
    assert_eq!(runtime_after_held_reconcile.unresolved_effects().len(), 1);

    let job_after_held_reconcile = fixture
        .workflow_store
        .read_binding_ledger(&binding_id)
        .expect("read binding ledger after held reconcile")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job.job_id)
        .expect("find job after held reconcile");
    assert_eq!(
        job_after_held_reconcile.tasks[0].stage,
        CoworkerTaskStage::Running
    );
    let attempt_after_held_reconcile = job_after_held_reconcile.tasks[0]
        .attempts
        .last()
        .expect("attempt 1 record after held reconcile");
    assert!(
        attempt_after_held_reconcile.report_artifact_id.is_none()
            && attempt_after_held_reconcile.head_commit.is_none(),
        "reconcile while gate is closed must not invent report promotion or head_commit"
    );
    let receipt_after_held_reconcile = threadlane_host::read_task_receipt(
        &session_dir,
        &approval_record,
        &session_binding_id,
        "task-crash-a",
    )
    .expect("read task receipt after held reconcile")
    .expect("sealed TaskRunReceipt still present after held reconcile");
    assert_eq!(receipt_after_held_reconcile, sealed_receipt);

    // Release the controlled gate so the held fake Coder finishes, then reconcile completion
    // using the same surviving dispatch mapping without any new provider call or budget charge.
    fs::write(&gate_release_file, "release\n").expect("release gate file after crash test");
    let coder_done_file = PathBuf::from(format!("{}.done", gate_release_file.display()));
    let done_deadline = Instant::now() + Duration::from_secs(5);
    while !coder_done_file.exists() {
        assert!(
            Instant::now() < done_deadline,
            "timed out waiting for held fake Coder to exit after gate release"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let reconcile_complete_deadline = Instant::now() + Duration::from_secs(10);
    let reconcile_after_gate = loop {
        let current = run_flow_cli(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &["code-driver", "reconcile", "--job-id", &job.job_id],
        )
        .await;
        if current["overall_status"] == "awaiting_checks_or_review" {
            break current;
        }
        assert!(
            Instant::now() < reconcile_complete_deadline,
            "timed out waiting for post-gate reconcile to reach awaiting_checks_or_review; last: {current}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(
        reconcile_after_gate["overall_status"],
        "awaiting_checks_or_review"
    );
    assert_eq!(
        reconcile_after_gate["tasks"][0]["stage"],
        "ready_for_review"
    );
    assert_eq!(
        reconcile_after_gate["tasks"][0]["run_id"].as_str(),
        Some(sealed_receipt.run_id.as_str())
    );
    assert_eq!(
        reconcile_after_gate["tasks"][0]["worker_id"].as_str(),
        Some(sealed_receipt.worker_id.as_str())
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    let runtime_after_completion = fixture
        .policy_store
        .get_job_runtime_readonly(&binding_id, &job.job_id)
        .expect("read runtime after completion reconcile")
        .expect("runtime exists after completion reconcile");
    assert_eq!(runtime_after_completion.total_task_attempts_reserved, 1);
    assert_eq!(
        runtime_after_completion
            .role_usage
            .get(&WorkflowRole::Coder)
            .expect("Coder usage")
            .calls_reserved,
        1
    );
    assert!(runtime_after_completion.unresolved_effects().is_empty());
    let completed_artifacts = runtime_after_completion
        .get_attempt_artifacts("task-crash-a", 1)
        .expect(
            "completed TaskAttemptExecutionArtifacts must exist after gate release + reconcile",
        );
    assert_eq!(completed_artifacts.run_id, sealed_receipt.run_id);
    assert_eq!(completed_artifacts.worker_id, sealed_receipt.worker_id);

    host_handle
        .shutdown()
        .await
        .expect("shut down approved host after crash recovery test");
    supervisor.shutdown().await;
}
