#![cfg(target_os = "linux")]

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use tempfile::TempDir;

use threadlane_native::coworker_workflow::{
    build_task_review_prompt, run_task_review_with_claim, run_task_review_with_guard,
    CoworkerJobOwnershipKey, CoworkerJobOwnershipStore, ExpectedTaskReviewIdentity,
    ReviewVerdictOutcome, TaskReviewExecutionRequest, TaskReviewExecutionStatus,
    TaskReviewFailureKind, MAX_TASK_REVIEW_RESPONSE_BYTES, TASK_REVIEW_RESPONSE_SCHEMA_VERSION,
};
use threadlane_operations::{
    commit_task_worktree_changes, inspect_review_subject_readonly, CommitTaskChangesRequest,
    InspectReviewSubjectRequest, ReviewPolicyBinding, ReviewSubject, TestEvidenceSummary,
    RECEIPTS_SUBDIR,
};
use threadlane_workers::{
    resolve_exact_commit_sha, run_sanitized_git, ExecutionBudgets, WorkerFilePermissions,
    WorkerProvider, WorkerRunStatus, WorktreeManager, WorktreeManifest,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS, MANIFESTS_SUBDIR,
};

const TEST_THREAD_UUID: &str = "123e4567-e89b-42d3-a456-426614174000";
const POLICY_DIGEST_HEX: &str = "6666666666666666666666666666666666666666666666666666666666666666";

struct ReviewTestFixture {
    _tmp: TempDir,
    repo_path: PathBuf,
    work_area_root: PathBuf,
    bin_dir: PathBuf,
    state_dir: PathBuf,
    manager: WorktreeManager,
    ownership_store: CoworkerJobOwnershipStore,
}

impl ReviewTestFixture {
    fn new() -> Self {
        let tmp = TempDir::new().expect("tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize tempdir");
        let repo_path = root.join("repo");
        let work_area_root = root.join("work-area");
        let lock_root_dir = root.join("ownership-locks");
        let bin_dir = root.join("bin");
        let state_dir = root.join("state");

        fs::create_dir_all(repo_path.join("crates/sub/src")).expect("create repo subdir");
        fs::create_dir_all(&bin_dir).expect("create bin_dir");
        fs::create_dir_all(&state_dir).expect("create state_dir");
        fs::create_dir_all(&lock_root_dir).expect("create lock_root_dir");
        fs::set_permissions(&lock_root_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 lock_root_dir");

        for args in [
            &["init", "--quiet"][..],
            &["config", "user.name", "Threadlane Review Test"][..],
            &["config", "user.email", "review@threadlane.local"][..],
        ] {
            run_sanitized_git(&repo_path, args, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
                .expect("git init config");
        }

        fs::write(
            repo_path.join("crates/sub/src/lib.rs"),
            "pub fn answer() -> u32 { 42 }\n",
        )
        .expect("write baseline file");
        run_sanitized_git(&repo_path, &["add", "."], DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("git add");
        run_sanitized_git(
            &repo_path,
            &["commit", "-m", "initial", "--quiet"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git commit");

        let base_commit =
            resolve_exact_commit_sha(&repo_path, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
                .expect("resolve HEAD commit");
        let manager =
            WorktreeManager::new(&repo_path, &work_area_root, &base_commit).expect("worktree mgr");
        let ownership_store =
            CoworkerJobOwnershipStore::new(&lock_root_dir).expect("ownership store");

        Self {
            _tmp: tmp,
            repo_path,
            work_area_root: manager.work_area_root().to_path_buf(),
            bin_dir,
            state_dir,
            manager,
            ownership_store,
        }
    }

    fn write_executable(&self, name: &str, body: &str) -> PathBuf {
        let staging = self.bin_dir.join(format!(".{name}.tmp"));
        let final_path = self.bin_dir.join(name);
        {
            let mut file = File::create(&staging).expect("create staging script");
            file.write_all(body.as_bytes()).expect("write script");
            file.sync_all().expect("sync script");
        }
        fs::set_permissions(&staging, fs::Permissions::from_mode(0o755)).expect("chmod 0755");
        fs::rename(&staging, &final_path).expect("rename executable");
        fs::canonicalize(&final_path).expect("canonicalize executable")
    }

    fn create_committed_task_subject(
        &self,
        run_id: &str,
        compact_manifest_task_id: &str,
        token: &str,
    ) -> (WorktreeManifest, ReviewSubject) {
        let allowed = BTreeSet::from(["crates/sub/src/lib.rs".to_string()]);
        let manifest = self
            .manager
            .create_task_worktree_with_token(run_id, compact_manifest_task_id, token, &allowed)
            .expect("create task worktree");

        fs::write(
            manifest.checkout_path.join("crates/sub/src/lib.rs"),
            "pub fn answer() -> u32 { 42 }\npub fn reviewed_helper() -> u32 { 7 }\n",
        )
        .expect("modify task file");

        let _commit_report = commit_task_worktree_changes(&CommitTaskChangesRequest {
            work_area_root: self.work_area_root.clone(),
            task_worktree_id: manifest.worktree_id.clone(),
            commit_message: "Implement reviewed_helper in allowed scope".to_string(),
            worker_status: Some(WorkerRunStatus::Succeeded),
            allow_missing_effort_evidence: false,
        })
        .expect("commit task worktree changes");

        let subject = inspect_review_subject_readonly(&InspectReviewSubjectRequest {
            work_area_root: self.work_area_root.clone(),
            task_worktree_id: manifest.worktree_id.clone(),
            policy: ReviewPolicyBinding {
                policy_id: "pol-coworker-review".to_string(),
                codex_launch_report: None,
                codex_source_report_sha256: None,
                requested_provider: Some("antigravity".to_string()),
                requested_model: Some("gemini-4-argon-high".to_string()),
                requested_effort: Some("high".to_string()),
                observed_model: Some("gemini-4-argon-high".to_string()),
                observed_effort: Some("high".to_string()),
                file_permissions: WorkerFilePermissions::TaskFiles,
                require_effort_evidence: true,
                require_passing_tests: true,
            },
            test_evidence: TestEvidenceSummary {
                formatting_passed: true,
                clippy_passed: true,
                tests_passed: true,
                passed_test_count: 3,
                failed_test_count: 0,
                evidence_summary: "cargo fmt, clippy, and 3 unit tests passed".to_string(),
            },
            worker_status: Some(WorkerRunStatus::Succeeded),
        })
        .expect("inspect_review_subject_readonly");

        (manifest, subject)
    }

    fn write_fake_agy_reviewer(&self, name: &str) -> PathBuf {
        let response_file = self.state_dir.join("response.json");
        let mode_file = self.state_dir.join("mode.txt");
        let recorded_prompt = self.state_dir.join("last_prompt.txt");
        let recorded_argv = self.state_dir.join("last_argv.txt");

        let script = format!(
            r#"#!/bin/sh
set -eu
RESP_FILE="{}"
MODE_FILE="{}"
PROMPT_OUT="{}"
ARGV_OUT="{}"

printf "%s\n" "$@" > "$ARGV_OUT"

MODEL="gemini-4-argon-high"
PROMPT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --model)
            MODEL="$2"
            shift 2
            ;;
        --mode|--output-format|--settings)
            shift 2
            ;;
        --prompt|-p)
            PROMPT="$2"
            shift 2
            ;;
        *)
            PROMPT="$1"
            shift 1
            ;;
    esac
done

printf "%s" "$PROMPT" > "$PROMPT_OUT"

MODE="valid"
if [ -f "$MODE_FILE" ]; then
    MODE=$(cat "$MODE_FILE")
fi

if [ "$MODE" = "wrong_model" ]; then
    printf '{{"event":"init","init":{{"session_id":"agy-rev-sess-1","model":"gemini-4-flash"}}}}\n'
else
    printf '{{"event":"init","init":{{"session_id":"agy-rev-sess-1","model":"%s"}}}}\n' "$MODEL"
fi

if [ "$MODE" = "mutate_worktree" ]; then
    printf "// rogue mutation during read-only review\n" >> crates/sub/src/lib.rs
fi

if [ "$MODE" = "prose_response" ]; then
    printf '{{"event":"result","result":{{"status":"SUCCESS","response":"LGTM! Everything passes review."}}}}\n'
    exit 0
fi

esc=$(sed 's/\\/\\\\/g; s/"/\\"/g' "$RESP_FILE" | tr '\n' ' ')
printf '{{"event":"result","result":{{"status":"SUCCESS","response":"%s"}}}}\n' "$esc"
exit 0
"#,
            response_file.display(),
            mode_file.display(),
            recorded_prompt.display(),
            recorded_argv.display(),
        );
        self.write_executable(name, &script)
    }

    fn write_response_json(&self, identity: &ExpectedTaskReviewIdentity, outcome: &str) {
        let payload = serde_json::json!({
            "schema_version": TASK_REVIEW_RESPONSE_SCHEMA_VERSION,
            "task_id": identity.task_id,
            "attempt_no": identity.attempt_no,
            "candidate_id": identity.candidate_id,
            "review_subject_digest": identity.review_subject_digest,
            "base_commit": identity.base_commit,
            "task_head_commit": identity.task_head_commit,
            "task_tree_sha": identity.task_tree_sha,
            "diff_digest": identity.diff_digest,
            "policy_digest": identity.policy_digest,
            "outcome": outcome,
            "findings": ["In-scope helper matches contract."],
            "required_in_scope_fixes": [],
            "conflicting_paths_or_interfaces": [],
            "summary": "Reviewed diff and test evidence; candidate is ready."
        });
        fs::write(
            self.state_dir.join("response.json"),
            serde_json::to_vec(&payload).expect("serialize response json"),
        )
        .expect("write response.json");
    }
}

#[tokio::test]
async fn supervised_reviewer_returns_verified_accept_verdict_proposal_without_minting_receipt() {
    let fixture = ReviewTestFixture::new();
    let (_manifest, subject) =
        fixture.create_committed_task_subject("run-alpha", "wt-compact-01", "tok01");
    assert_eq!(subject.repository_path, fixture.repo_path);
    let fake_agy = fixture.write_fake_agy_reviewer("fake-agy-review");

    // Logical workflow task_id differs from the compact worktree manifest task_id (`wt-compact-01`).
    let expected_identity = ExpectedTaskReviewIdentity::from_subject(
        &subject,
        "logical_task_telemetry_helper",
        1,
        "cand_alpha_01",
        POLICY_DIGEST_HEX,
    );
    assert_ne!(subject.task_id, expected_identity.task_id);

    fixture.write_response_json(&expected_identity, "accept");
    fs::write(fixture.state_dir.join("mode.txt"), "valid").expect("write mode");

    let key = CoworkerJobOwnershipKey::new("bind-alpha", TEST_THREAD_UUID, "job-alpha")
        .expect("ownership key");
    let guard = fixture
        .ownership_store
        .try_acquire_job(&key, 1_700_000_000_000)
        .expect("try_acquire_job")
        .into_guard()
        .expect("acquired guard");
    let claim = guard.issue_driver_claim();

    let request = TaskReviewExecutionRequest {
        binding_id: "bind-alpha".to_string(),
        thread_uuid: TEST_THREAD_UUID.to_string(),
        job_id: "job-alpha".to_string(),
        subject: subject.clone(),
        expected_identity: expected_identity.clone(),
        provider: WorkerProvider::Antigravity,
        executable: fake_agy,
        model_arg: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        budgets: ExecutionBudgets {
            timeout_ms: 10_000,
            ..ExecutionBudgets::default()
        },
        max_response_bytes: MAX_TASK_REVIEW_RESPONSE_BYTES,
        frozen_instructions: Some(
            "Verify reviewed_helper stays within crates/sub/src/lib.rs.".to_string(),
        ),
        untrusted_candidate_context: Some("Candidate note: added reviewed_helper.".to_string()),
    };

    let prompt = build_task_review_prompt(&request).expect("build prompt");
    assert!(prompt.contains(&subject.subject_digest));
    assert!(prompt.contains("logical_task_telemetry_helper"));

    let (_cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);
    let report = run_task_review_with_claim(
        &request,
        &guard,
        Some(&claim),
        Some(guard.summary()),
        &mut cancel_rx,
    )
    .await
    .expect("run_task_review_with_claim");

    assert_eq!(report.status, TaskReviewExecutionStatus::VerifiedVerdict);
    assert_eq!(report.failure_kind, None);
    assert!(report.can_promote_verdict());
    assert!(report.is_accept_proposal());
    assert!(!report.fenced_from_promotion);
    assert!(report.ownership_verified_post_run);
    assert!(report.post_run_subject_verified);
    assert!(report.post_run_worktree_clean);
    assert_eq!(
        report.observed_model.as_deref(),
        Some("gemini-4-argon-high")
    );
    assert_eq!(report.observed_effort.as_deref(), Some("high"));
    assert!(report.advisory_response().is_some());
    report.validate_integrity().expect("report integrity");

    let verdict = report
        .promote_verified_verdict(&subject, &expected_identity)
        .expect("promote verified verdict");
    assert_eq!(verdict.outcome, ReviewVerdictOutcome::Accept);
    assert_eq!(verdict.task_id, "logical_task_telemetry_helper");
    assert_eq!(verdict.task_tree_sha, subject.task_tree_sha);
    assert_eq!(verdict.review_subject_digest, subject.subject_digest);
    assert!(!verdict.prose_may_advance_acceptance);

    // Verify read-only `--mode plan` launch and that no ReviewReceipt was minted on disk.
    let argv_log =
        fs::read_to_string(fixture.state_dir.join("last_argv.txt")).expect("read argv log");
    assert!(argv_log.contains("plan"));
    assert!(argv_log.contains("--settings"));
    let receipts_dir = fixture
        .work_area_root
        .join(MANIFESTS_SUBDIR)
        .join(RECEIPTS_SUBDIR);
    assert!(!receipts_dir.exists());
}

#[tokio::test]
async fn supervised_reviewer_refuses_wrong_model_prose_and_mismatched_subject_or_mutated_worktree()
{
    let fixture = ReviewTestFixture::new();
    let (_manifest, subject) =
        fixture.create_committed_task_subject("run-beta", "wt-compact-02", "tok02");
    let fake_agy = fixture.write_fake_agy_reviewer("fake-agy-review-neg");

    let expected_identity = ExpectedTaskReviewIdentity::from_subject(
        &subject,
        "logical_task_beta",
        2,
        "cand_beta_02",
        POLICY_DIGEST_HEX,
    );
    fixture.write_response_json(&expected_identity, "accept");

    let key = CoworkerJobOwnershipKey::new("bind-beta", TEST_THREAD_UUID, "job-beta")
        .expect("ownership key");
    let guard = fixture
        .ownership_store
        .try_acquire_job(&key, 1_700_000_000_100)
        .expect("try_acquire_job")
        .into_guard()
        .expect("acquired guard");

    let base_request = TaskReviewExecutionRequest {
        binding_id: "bind-beta".to_string(),
        thread_uuid: TEST_THREAD_UUID.to_string(),
        job_id: "job-beta".to_string(),
        subject: subject.clone(),
        expected_identity: expected_identity.clone(),
        provider: WorkerProvider::Antigravity,
        executable: fake_agy,
        model_arg: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        budgets: ExecutionBudgets {
            timeout_ms: 10_000,
            ..ExecutionBudgets::default()
        },
        max_response_bytes: MAX_TASK_REVIEW_RESPONSE_BYTES,
        frozen_instructions: None,
        untrusted_candidate_context: None,
    };

    // 1. Mismatched ownership guard key is refused before spawning.
    let mut wrong_job_req = base_request.clone();
    wrong_job_req.job_id = "job-other".to_string();
    assert!(run_task_review_with_guard(&wrong_job_req, &guard)
        .await
        .is_err());

    // 2. Wrong observed model in provider stream -> KnownTerminalFailure (ModelEvidenceFailed).
    fs::write(fixture.state_dir.join("mode.txt"), "wrong_model").expect("write mode");
    let model_mismatch_report = run_task_review_with_guard(&base_request, &guard)
        .await
        .expect("run wrong_model");
    assert_eq!(
        model_mismatch_report.status,
        TaskReviewExecutionStatus::KnownTerminalFailure
    );
    assert_eq!(
        model_mismatch_report.failure_kind,
        Some(TaskReviewFailureKind::ModelEvidenceFailed)
    );
    assert!(model_mismatch_report.verified_verdict.is_none());
    assert!(!model_mismatch_report.can_promote_verdict());
    assert!(model_mismatch_report
        .promote_verified_verdict(&subject, &expected_identity)
        .is_err());

    // 3. Prose terminal response instead of raw JSON object -> AdvisoryResponseInvalid.
    fs::write(fixture.state_dir.join("mode.txt"), "prose_response").expect("write mode");
    let prose_report = run_task_review_with_guard(&base_request, &guard)
        .await
        .expect("run prose_response");
    assert_eq!(
        prose_report.status,
        TaskReviewExecutionStatus::KnownTerminalFailure
    );
    assert_eq!(
        prose_report.failure_kind,
        Some(TaskReviewFailureKind::AdvisoryResponseInvalid)
    );
    assert!(prose_report.verified_verdict.is_none());

    // 4. Mismatched subject/tree SHA echoed in JSON response -> VerdictValidationFailed.
    let mut wrong_tree_identity = expected_identity.clone();
    wrong_tree_identity.task_tree_sha = "f".repeat(40);
    fixture.write_response_json(&wrong_tree_identity, "accept");
    fs::write(fixture.state_dir.join("mode.txt"), "valid").expect("write mode");

    let mismatch_report = run_task_review_with_guard(&base_request, &guard)
        .await
        .expect("run mismatched identity");
    assert_eq!(
        mismatch_report.status,
        TaskReviewExecutionStatus::KnownTerminalFailure
    );
    assert_eq!(
        mismatch_report.failure_kind,
        Some(TaskReviewFailureKind::VerdictValidationFailed)
    );
    assert!(mismatch_report.advisory_response().is_some());
    assert!(mismatch_report.verified_verdict.is_none());

    // 5. Worktree mutated during execution despite ReadOnly settings -> SubjectMutatedOrDrifted (fenced).
    fixture.write_response_json(&expected_identity, "accept");
    fs::write(fixture.state_dir.join("mode.txt"), "mutate_worktree").expect("write mode");

    let fenced_report = run_task_review_with_guard(&base_request, &guard)
        .await
        .expect("run mutate_worktree");
    assert_eq!(
        fenced_report.status,
        TaskReviewExecutionStatus::SubjectMutatedOrDrifted
    );
    assert_eq!(
        fenced_report.failure_kind,
        Some(TaskReviewFailureKind::SubjectMutatedOrDrifted)
    );
    assert!(fenced_report.fenced_from_promotion);
    assert!(!fenced_report.post_run_subject_verified);
    assert!(!fenced_report.post_run_worktree_clean);
    assert!(fenced_report.verified_verdict.is_none());
    assert!(!fenced_report.can_promote_verdict());
    assert!(fenced_report.advisory_response().is_some());
    fenced_report
        .validate_integrity()
        .expect("fenced report integrity");
    assert!(fenced_report
        .promote_verified_verdict(&subject, &expected_identity)
        .is_err());
}
