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

printf '{{"type":"result","subtype":"success","session_id":"sess-agy-ctrl","model":"%s","effort":"%s"}}\n' "$MODEL" "$EFFORT"
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

    // 1. First `code-driver ensure` must return while fake Coder is still held on the private gate.
    let ensure_first = run_flow_cli(
        &state_dir,
        &fixture,
        &binding_id,
        &thread_uuid,
        &["code-driver", "ensure", "--job-id", &job.job_id],
    )
    .await;
    assert!(
        !gate_release_file.exists(),
        "first code-driver ensure must return while fake Coder is held on gate"
    );
    assert_eq!(ensure_first["disposition"], "spawned_new_owner");
    assert_eq!(ensure_first["status"]["owner_state"], "live_verified_owner");
    assert_eq!(ensure_first["status"]["lease_active"], true);
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
    let status_running = run_flow_cli(
        &state_dir,
        &fixture,
        &binding_id,
        &thread_uuid,
        &["code-driver", "status", "--job-id", &job.job_id],
    )
    .await;
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
